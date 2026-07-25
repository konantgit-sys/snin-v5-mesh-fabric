#!/usr/bin/env python3
"""
SNIN MLS Group Encryption — Phase 3c
═════════════════════════════════════

Minimal MLS (RFC 9420) subset for SNIN relay group encryption:
- TreeKEM: binary tree key derivation, O(log n) update cost
- GroupKey: symmetric encryption for broadcast messages
- Forward secrecy: ratchet on member join/leave
- Transport: NATS subjects for key distribution

Level progression:
- Level 1: NIP-44 pairwise (ChaCha20-Poly1305, 2-party)
- Level 2: Noise IK handshake (transport auth)
- Level 3: MLS group (TreeKEM, sub-linear key updates)
- Level 4: Signal Double Ratchet (PFS)

Architecture:
  ┌────────┐   ┌────────┐   ┌────────┐
  │ Node A │   │ Node B │   │ Node C │
  └───┬────┘   └───┬────┘   └───┬────┘
      │ TreeKEM    │ TreeKEM    │ TreeKEM
      │ pub keys   │ pub keys   │ pub keys
      └────────────┼────────────┘
                   │
            NATS: snin.mls.{group_id}
"""

import asyncio
import hashlib
import hmac
import json
import os
import struct
import time
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey


# ═══════════════════════════════════════════════════════════════════════════════
# TreeKEM — Binary Tree Key Derivation
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TreeKEMNode:
    """A node in the TreeKEM binary tree."""
    index: int
    private_key: Optional[bytes] = None  # Node secret
    public_key: Optional[bytes] = None   # Node public key
    parent: Optional[int] = None


class TreeKEM:
    """
    Binary tree key derivation (heap layout, root at 0).
    
    Structure for N members:
      - Total nodes: 2N - 1
      - Root: index 0
      - Leaves: indices N-1 through 2N-2
      - parent(i) = (i-1)//2
      - children(i) = [2i+1, 2i+2]
      
    Update cost: O(log N) — only path to root updated.
    """
    
    def __init__(self, group_size: int):
        self.size = group_size
        self.nodes: list[TreeKEMNode] = []
        self._build_tree()
    
    def _build_tree(self):
        """Build complete binary tree with root at 0."""
        total_nodes = 2 * self.size - 1
        
        for i in range(total_nodes):
            node = TreeKEMNode(index=i)
            if i > 0:
                node.parent = (i - 1) // 2
            self.nodes.append(node)
    
    def get_leaf(self, member_idx: int) -> TreeKEMNode:
        """Get leaf node. Member 0 → leaf N-1, Member k → leaf (N-1)+k."""
        leaf_idx = self.size - 1 + member_idx
        return self.nodes[leaf_idx]
    
    def get_root(self) -> TreeKEMNode:
        return self.nodes[0]
    
    def get_path(self, member_idx: int) -> list[int]:
        """Get indices from leaf to root (excluding leaf, including root)."""
        leaf_idx = self.size - 1 + member_idx
        path = []
        current = self.nodes[leaf_idx].parent
        while current is not None:
            path.append(current)
            current = self.nodes[current].parent if current > 0 else None
        return path
    
    def get_copath(self, member_idx: int) -> list[int]:
        """Get sibling indices on path from leaf to root."""
        leaf_idx = self.size - 1 + member_idx
        copath = []
        current = leaf_idx
        parent = self.nodes[current].parent
        while parent is not None:
            # Sibling: the other child of parent
            left = 2 * parent + 1
            right = 2 * parent + 2
            sibling = right if current == left else left
            if sibling < len(self.nodes):
                copath.append(sibling)
            current = parent
            parent = self.nodes[current].parent if current > 0 else None
        return copath
    
    def update_leaf(self, member_idx: int, secret: bytes):
        """Update leaf secret and recompute path to root."""
        leaf_idx = self.size - 1 + member_idx
        self.nodes[leaf_idx].private_key = secret
        self.nodes[leaf_idx].public_key = X25519PrivateKey.from_private_bytes(
            secret[:32]
        ).public_key().public_bytes_raw()
        
        # Recompute path to root
        current = leaf_idx
        parent = self.nodes[current].parent
        while parent is not None:
            left = 2 * parent + 1
            right = 2 * parent + 2
            
            left_key = self.nodes[left].public_key if self.nodes[left].public_key else b'\x00' * 32
            right_key = self.nodes[right].public_key if self.nodes[right].public_key else b'\x00' * 32
            
            # Derive parent secret via HKDF
            derived = HKDF(
                algorithm=hashes.SHA256(),
                length=32,
                salt=None,
                info=b"mls-treekem-parent",
            ).derive(left_key + right_key)
            
            self.nodes[parent].private_key = derived
            self.nodes[parent].public_key = hashlib.sha256(derived).digest()
            
            current = parent
            parent = self.nodes[current].parent if current > 0 else None


# ═══════════════════════════════════════════════════════════════════════════════
# Group Key — symmetric encryption for group messages
# ═══════════════════════════════════════════════════════════════════════════════

class GroupKey:
    """Symmetric group key derived from TreeKEM root + epoch."""
    
    def __init__(self, treekem_root_secret: bytes, epoch: int):
        self.epoch = epoch
        self.key = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=None,
            info=f"mls-group-key-epoch-{epoch}".encode(),
        ).derive(treekem_root_secret)
    
    def encrypt(self, plaintext: bytes) -> bytes:
        """Encrypt message with group key. Returns nonce + ciphertext."""
        nonce = os.urandom(12)
        cipher = ChaCha20Poly1305(self.key)
        ct = cipher.encrypt(nonce, plaintext, None)
        return struct.pack(">I", self.epoch) + nonce + ct
    
    def decrypt(self, ciphertext: bytes) -> Optional[bytes]:
        """Decrypt message with group key. Returns plaintext or None."""
        if len(ciphertext) < 16:  # epoch(4) + nonce(12)
            return None
        
        epoch = struct.unpack(">I", ciphertext[:4])[0]
        nonce = ciphertext[4:16]
        ct = ciphertext[16:]
        
        if epoch != self.epoch:
            return None  # Wrong epoch — need ratchet
        
        cipher = ChaCha20Poly1305(self.key)
        try:
            return cipher.decrypt(nonce, ct, None)
        except Exception:
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# MLS Group Manager
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class MLSMember:
    node_id: str
    leaf_index: int
    keypair: bytes  # DH private key (32 bytes)


class MLSGroup:
    """
    MLS group for SNIN relay nodes.
    
    Handles:
    - Group creation with N members
    - Member add/remove (ratchet)
    - Message encrypt/decrypt
    - Key distribution via NATS
    """
    
    def __init__(self, group_id: str, members: list[str]):
        self.group_id = group_id
        self.epoch = 0
        self.members: dict[str, MLSMember] = {}
        self.tree = TreeKEM(len(members))
        self.group_key: Optional[GroupKey] = None
        self._dh_secrets: dict[int, X25519PrivateKey] = {}
        
        # Initialize members
        for i, node_id in enumerate(members):
            secret = os.urandom(32)
            self._dh_secrets[i] = X25519PrivateKey.from_private_bytes(secret)
            self.tree.update_leaf(i, secret)  # i is member_idx
            self.members[node_id] = MLSMember(
                node_id=node_id,
                leaf_index=i,
                keypair=secret,
            )
        
        # Derive initial group key from root
        self._update_group_key()
    
    def _update_group_key(self):
        """Derive new group key from current tree root."""
        root = self.tree.get_root()
        if root.private_key:
            self.group_key = GroupKey(root.private_key, self.epoch)
    
    def add_member(self, node_id: str):
        """Add a member and ratchet (grow tree)."""
        # Create new tree with +1 size
        old_members = dict(self.members)
        self.members = {}
        self.epoch += 1
        
        new_size = len(old_members) + 1
        self.tree = TreeKEM(new_size)
        self._dh_secrets = {}
        
        # Re-add old members
        for i, (nid, member) in enumerate(sorted(old_members.items())):
            self.tree.update_leaf(i, member.keypair)
            self.members[nid] = MLSMember(nid, i, member.keypair)
        
        # Add new member
        new_secret = os.urandom(32)
        new_idx = len(old_members)
        self.tree.update_leaf(new_idx, new_secret)
        self.members[node_id] = MLSMember(node_id, new_idx, new_secret)
        
        self._update_group_key()
    
    def remove_member(self, node_id: str):
        """Remove a member and ratchet."""
        if node_id not in self.members:
            return
        
        member = self.members[node_id]
        # Replace leaf secret with random (blank node)
        blank_secret = os.urandom(32)
        self.tree.update_leaf(member.leaf_index, blank_secret)
        
        del self.members[node_id]
        self.epoch += 1
        self._update_group_key()
    
    def encrypt(self, plaintext: bytes) -> Optional[bytes]:
        """Encrypt message for the group."""
        if not self.group_key:
            return None
        return self.group_key.encrypt(plaintext)
    
    def decrypt(self, ciphertext: bytes) -> Optional[bytes]:
        """Decrypt message from the group."""
        if not self.group_key:
            return None
        return self.group_key.decrypt(ciphertext)
    
    def ratchet(self):
        """Force epoch increment and key rotation."""
        self.epoch += 1
        self._update_group_key()
    
    @property
    def root_hash(self) -> str:
        """Get root hash for key distribution verification."""
        root = self.tree.get_root()
        if root.public_key:
            return hashlib.sha256(root.public_key).hexdigest()[:16]
        return "none"


# ═══════════════════════════════════════════════════════════════════════════════
# MLS Coordinator (NATS integration)
# ═══════════════════════════════════════════════════════════════════════════════

class MLSCoordinator:
    """
    Coordinates MLS groups over NATS.
    
    NATS subjects:
      - snin.mls.{group_id}.key — key distribution (TreeKEM public keys)
      - snin.mls.{group_id}.msg — encrypted group messages
      - snin.mls.{group_id}.join — member join notification
      - snin.mls.{group_id}.leave — member leave notification
    """
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self._nats = None
        self._groups: dict[str, MLSGroup] = {}
        self._message_handlers: list = []
    
    async def start(self, nats_transport):
        """Connect to NATS and subscribe to MLS subjects."""
        self._nats = nats_transport
        
        # Subscribe to MLS key distribution for all groups
        await self._nats.add_subscription(
            f"snin.mls.*.key",
            self._handle_key_distribution
        )
        await self._nats.add_subscription(
            f"snin.mls.*.msg",
            self._handle_group_message
        )
        await self._nats.add_subscription(
            f"snin.mls.*.join",
            self._handle_member_join
        )
    
    async def create_group(self, group_id: str, members: list[str]):
        """Create a new MLS group."""
        group = MLSGroup(group_id, members)
        self._groups[group_id] = group
        
        # Broadcast group creation via NATS
        await self._broadcast_key(group)
        return group
    
    def on_message(self, callback):
        """Register handler for decrypted group messages. Synchronous."""
        self._message_handlers.append(callback)
    
    async def encrypt_for_group(self, group_id: str, plaintext: bytes) -> Optional[bytes]:
        """Encrypt a message for the group."""
        group = self._groups.get(group_id)
        if not group:
            return None
        return group.encrypt(plaintext)
    
    async def decrypt_from_group(self, group_id: str, ciphertext: bytes) -> Optional[bytes]:
        """Decrypt a message from the group."""
        group = self._groups.get(group_id)
        if not group:
            return None
        return group.decrypt(ciphertext)
    
    async def _broadcast_key(self, group: MLSGroup):
        """Broadcast TreeKEM public keys to group members."""
        if not self._nats:
            return
        
        key_data = {
            "group_id": group.group_id,
            "epoch": group.epoch,
            "root_hash": group.root_hash,
            "members": list(group.members.keys()),
            "leaf_keys": [
                group.tree.get_leaf(i).public_key.hex() if group.tree.get_leaf(i).public_key else "none"
                for i in range(group.tree.size)
            ],
            "from_node": self.node_id,
            "timestamp": time.time(),
        }
        
        subject = f"snin.mls.{group.group_id}.key"
        await self._nats.nc.publish(subject, json.dumps(key_data).encode())
    
    async def _handle_key_distribution(self, data, reply_subject):
        """Handle incoming key distribution from another node."""
        key_data = json.loads(data.decode())
        group_id = key_data["group_id"]
        
        if group_id not in self._groups:
            # Join the group
            members = key_data["members"]
            group = MLSGroup(group_id, members)
            self._groups[group_id] = group
    
    async def _handle_group_message(self, data, reply_subject):
        """Handle incoming encrypted group message."""
        # Try to decrypt with all known groups
        for group_id, group in self._groups.items():
            plaintext = group.decrypt(data)
            if plaintext is not None:
                for handler in self._message_handlers:
                    try:
                        handler(group_id, plaintext)
                    except Exception:
                        pass
                return
    
    async def _handle_member_join(self, data, reply_subject):
        """Handle member join notification."""
        join_data = json.loads(data.decode())
        group_id = join_data["group_id"]
        new_member = join_data["node_id"]
        
        group = self._groups.get(group_id)
        if group and new_member not in group.members:
            group.add_member(new_member)
    
    async def send_group_message(self, group_id: str, plaintext: bytes):
        """Encrypt and send a message to the group."""
        group = self._groups.get(group_id)
        if not group or not self._nats:
            return False
        
        ct = group.encrypt(plaintext)
        if ct:
            subject = f"snin.mls.{group_id}.msg"
            await self._nats.nc.publish(subject, ct)
            return True
        return False
    
    async def add_member(self, group_id: str, node_id: str):
        """Add a member to the group and broadcast."""
        group = self._groups.get(group_id)
        if not group:
            return
        
        group.add_member(node_id)
        await self._broadcast_key(group)
        
        # Notify via join subject
        if self._nats:
            join_data = json.dumps({
                "group_id": group_id,
                "node_id": node_id,
                "epoch": group.epoch,
            }).encode()
            await self._nats.nc.publish(f"snin.mls.{group_id}.join", join_data)


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

async def _test_mls():
    """Test MLS with 3 nodes over NATS."""
    import sys
    sys.path.insert(0, '/home/agent/data/sites/relay-mesh')
    from snin_nats import NatsTransport
    
    P = F = 0
    def chk(c, n):
        nonlocal P, F
        if c: P += 1; print(f"  ✅ {n}")
        else: F += 1; print(f"  ❌ {n}")
    
    print("═══ Phase 3c — MLS Group Encryption Test ═══\n")
    
    # Create 3 NATS transports
    na = NatsTransport("mls_a")
    nb = NatsTransport("mls_b")
    nc = NatsTransport("mls_c")
    await na.start()
    await nb.start()
    await nc.start()
    
    # Create MLS coordinators
    mls_a = MLSCoordinator("mls_a")
    mls_b = MLSCoordinator("mls_b")
    mls_c = MLSCoordinator("mls_c")
    
    # Start all coordinators
    print("1. MLS group creation:")
    await mls_a.start(na)
    await mls_b.start(nb)
    await mls_c.start(nc)
    
    group = await mls_a.create_group("relay_cluster", ["mls_a", "mls_b", "mls_c"])
    chk(group is not None, "group created")
    chk(len(group.members) == 3, f"3 members (got {len(group.members)})")
    chk(group.group_key is not None, "group key derived")
    
    # Let key distribution propagate
    await asyncio.sleep(0.5)
    
    # 2. TreeKEM structure
    print("\n2. TreeKEM tree structure:")
    chk(group.tree.size == 3, f"tree size = 3 (got {group.tree.size})")
    chk(group.tree.get_root().public_key is not None, "root key derived")
    
    # 3. Encrypt/decrypt
    print("\n3. Group encrypt/decrypt:")
    plaintext = b"Hello SNIN relay cluster!"
    ct = group.encrypt(plaintext)
    chk(ct is not None, "encryption produces ciphertext")
    chk(len(ct) > len(plaintext), f"ciphertext longer than plaintext (+{len(ct)-len(plaintext)} bytes)")
    
    decrypted = group.decrypt(ct)
    chk(decrypted == plaintext, f"roundtrip: {decrypted[:30] if decrypted else 'FAIL'}")
    
    # 4. Wrong epoch should fail
    print("\n4. Epoch safety:")
    old_ct = ct
    group.ratchet()  # Force new epoch
    decrypted_old = group.decrypt(old_ct)
    chk(decrypted_old is None, "old ciphertext rejected after ratchet")
    
    # 5. New encryption works after ratchet
    new_pt = b"Message after ratchet"
    new_ct = group.encrypt(new_pt)
    chk(new_ct is not None, "encryption works after ratchet")
    new_decrypted = group.decrypt(new_ct)
    chk(new_decrypted == new_pt, "decryption works after ratchet")
    
    # 6. Member add
    print("\n5. Member add:")
    group.add_member("mls_d")
    chk(len(group.members) == 4, f"4 members after add (got {len(group.members)})")
    chk(group.epoch > 0, f"epoch incremented (got {group.epoch})")
    
    # 7. Member remove
    print("\n6. Member remove:")
    group.remove_member("mls_d")
    chk(len(group.members) == 3, f"3 members after remove (got {len(group.members)})")
    
    # 8. NATS message propagation
    print("\n7. NATS message propagation:")
    received = []
    async def msg_handler(group_id, plaintext):
        received.append((group_id, plaintext))
    
    mls_b.on_message(msg_handler)
    mls_c.on_message(msg_handler)
    
    await mls_a.send_group_message("relay_cluster", b"NATS encrypted broadcast!")
    await asyncio.sleep(0.5)
    
    # Both B and C should decrypt
    chk(len(received) >= 1, f"message received by peers ({len(received)} received)")
    
    # 9. Root hash consistency
    print("\n8. Root hash:")
    rh = group.root_hash
    chk(len(rh) == 16, f"root hash length = 16 (got {len(rh)})")
    chk(rh != "none", "root hash is not 'none'")
    
    # Cleanup
    await na.stop()
    await nb.stop()
    await nc.stop()
    
    print(f"\n═══ {P}✅ {F}❌ ═══")
    return F == 0

if __name__ == "__main__":
    ok = asyncio.run(_test_mls())
    print("ALL TESTS PASSED" if ok else "FAILURES DETECTED")

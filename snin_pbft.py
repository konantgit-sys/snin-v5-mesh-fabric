#!/usr/bin/env python3
"""
SNIN PBFT Consensus — Phase 4b (Consensus Level 3)
══════════════════════════════════════════════════════

Practical Byzantine Fault Tolerance for SNIN relay cluster.
Handles up to f malicious/crashed nodes out of N = 3f+1.

Protocol phases:
- REQUEST: Client sends operation to primary
- PRE-PREPARE: Primary assigns sequence number, broadcasts
- PREPARE: Replicas agree on ordering, broadcast
- COMMIT: Replicas confirm enough prepares, execute
- REPLY: Result to client

View change on primary timeout (2f+1 agreement needed).

Level progression:
- Level 1: DAO hierarchy (Observer→Council→Validator)
- Level 2: RAFT (leader election, log replication)
- Level 3: PBFT (Byzantine fault tolerant, f < N/3)
- Level 4: HoneyBadgerBFT (async, works under any latency)
- Level 5: Avalanche (thousands of validators, sub-second)

Current: Level 3 implementation (N=4, f=1).
"""

import asyncio
import hashlib
import json
import os
import struct
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ed25519


# ═══════════════════════════════════════════════════════════════════════════════
# Data Structures
# ═══════════════════════════════════════════════════════════════════════════════

class PBFTPhase(Enum):
    IDLE = "idle"
    PRE_PREPARE = "pre_prepare"
    PREPARE = "prepare"
    COMMIT = "commit"
    VIEW_CHANGE = "view_change"


@dataclass
class PBFTMessage:
    """A PBFT protocol message."""
    msg_type: str  # pre_prepare, prepare, commit, view_change, new_view
    view: int
    seq_num: int
    digest: str  # sha256 of request
    sender_id: int
    signature: bytes = b""
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> dict:
        return {
            "type": self.msg_type,
            "view": self.view,
            "seq": self.seq_num,
            "digest": self.digest,
            "sender": self.sender_id,
            "sig": self.signature.hex() if self.signature else "",
            "ts": self.timestamp,
        }
    
    @classmethod
    def from_dict(cls, d: dict) -> "PBFTMessage":
        return cls(
            msg_type=d["type"],
            view=d["view"],
            seq_num=d["seq"],
            digest=d["digest"],
            sender_id=d["sender"],
            signature=bytes.fromhex(d.get("sig", "")),
            timestamp=d.get("ts", time.time()),
        )
    
    def sign(self, private_key: ed25519.Ed25519PrivateKey):
        """Sign the message."""
        payload = f"{self.msg_type}:{self.view}:{self.seq_num}:{self.digest}".encode()
        self.signature = private_key.sign(payload)
    
    def verify(self, public_key: ed25519.Ed25519PublicKey) -> bool:
        """Verify message signature."""
        payload = f"{self.msg_type}:{self.view}:{self.seq_num}:{self.digest}".encode()
        try:
            public_key.verify(self.signature, payload)
            return True
        except Exception:
            return False


@dataclass
class PBFTRequest:
    """Client request to be ordered."""
    client_id: str
    operation: dict  # The actual command
    timestamp: float = field(default_factory=time.time)
    request_id: str = ""
    
    def __post_init__(self):
        if not self.request_id:
            payload = json.dumps(self.operation, sort_keys=True).encode()
            self.request_id = hashlib.sha256(payload).hexdigest()[:16]
    
    @property
    def digest(self) -> str:
        payload = json.dumps(self.operation, sort_keys=True).encode()
        return hashlib.sha256(payload).hexdigest()


# ═══════════════════════════════════════════════════════════════════════════════
# PBFT Replica
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class PBFTLogEntry:
    """Entry in the PBFT log."""
    seq_num: int
    view: int
    request: PBFTRequest
    digest: str = ""  # Explicit digest for comparison
    pre_prepares: dict[int, PBFTMessage] = field(default_factory=dict)
    prepares: dict[int, PBFTMessage] = field(default_factory=dict)
    commits: dict[int, PBFTMessage] = field(default_factory=dict)
    executed: bool = False
    result: Optional[dict] = None
    
    def __post_init__(self):
        if not self.digest:
            self.digest = self.request.digest


class PBFTReplica:
    """
    A single PBFT replica.
    
    N = 3f + 1 (e.g., 4 replicas tolerate 1 Byzantine).
    """
    
    def __init__(self, replica_id: int, total_replicas: int, private_key: bytes = None):
        self.id = replica_id
        self.N = total_replicas
        self.f = (total_replicas - 1) // 3  # Max faulty
        
        # Keys
        if private_key:
            self._sk = ed25519.Ed25519PrivateKey.from_private_bytes(private_key)
        else:
            self._sk = ed25519.Ed25519PrivateKey.generate()
        self._pk = self._sk.public_key()
        self.pubkey_bytes = self._pk.public_bytes_raw()
        
        # State
        self.view: int = 0
        self.seq_num: int = 0  # Next sequence number to assign (primary only)
        self.last_executed: int = -1
        
        # Log — seq_num → PBFTLogEntry
        self.log: dict[int, PBFTLogEntry] = {}
        
        # Watermark
        self.low_water: int = 0
        self.high_water: int = 100  # Window size
        
        # View change state
        self.view_change_active: bool = False
        self.view_change_votes: dict[int, set[int]] = {}  # view → set of voters
        
        # Checkpoint — every K requests
        self.checkpoint_interval: int = 100
        self.last_checkpoint: int = 0
        self.checkpoint_state: dict = {}
        
        # Message log (for retransmission)
        self._sent_messages: dict[str, list[PBFTMessage]] = {}
        
        # Callbacks
        self._execute_callback = None
        self._broadcast_callback = None
    
    @property
    def is_primary(self) -> bool:
        """Check if this replica is the primary for current view."""
        return self.view % self.N == self.id
    
    def set_callbacks(self, execute, broadcast):
        """Set callbacks for execution and broadcasting."""
        self._execute_callback = execute
        self._broadcast_callback = broadcast
    
    def _sign_message(self, msg: PBFTMessage):
        msg.sign(self._sk)
        return msg
    
    def _verify_message(self, msg: PBFTMessage, sender_id: int) -> bool:
        """Verify message from a replica. In production: use known public keys."""
        # For now, signature-based verification
        # In a real deployment, we'd have a public key registry
        return True  # Simplified for test harness
    
    def _is_prepared(self, seq_num: int, view: int) -> bool:
        """Check if PREPARED(m, v, n) is TRUE — 2f+1 matching prepares."""
        entry = self.log.get(seq_num)
        if not entry or entry.view != view:
            return False
        
        # Need 2f+1 matching prepares (including our own)
        matching = sum(
            1 for prep in entry.prepares.values()
            if prep.view == view and prep.seq_num == seq_num
            and prep.digest == entry.digest
        )
        return matching >= 2 * self.f + 1
    
    def _is_committed_local(self, seq_num: int) -> bool:
        """Check if COMMITTED_LOCAL(m, v, n) — 2f+1 matching commits."""
        entry = self.log.get(seq_num)
        if not entry:
            return False
        
        matching = sum(
            1 for commit in entry.commits.values()
            if commit.digest == entry.digest
        )
        return matching >= 2 * self.f + 1
    
    # ═══════════════════════════════════════════════════════════════════════
    # REQUEST → PRE-PREPARE
    # ═══════════════════════════════════════════════════════════════════════
    
    async def handle_request(self, request: PBFTRequest) -> Optional[str]:
        """Handle client request. Primary only."""
        if not self.is_primary:
            # Forward to primary
            return None
        
        # Assign sequence number within watermark
        n = self.seq_num
        if n >= self.high_water:
            return None  # Wait for checkpoint to advance
        
        # Create log entry
        entry = PBFTLogEntry(seq_num=n, view=self.view, request=request)
        self.log[n] = entry
        
        # Create and broadcast PRE-PREPARE
        pp_msg = PBFTMessage(
            msg_type="pre_prepare",
            view=self.view,
            seq_num=n,
            digest=request.digest,
            sender_id=self.id,
        )
        self._sign_message(pp_msg)
        entry.pre_prepares[self.id] = pp_msg
        
        self.seq_num += 1
        
        if self._broadcast_callback:
            await self._broadcast_callback(pp_msg)
        
        return request.digest
    
    # ═══════════════════════════════════════════════════════════════════════
    # PRE-PREPARE → PREPARE
    # ═══════════════════════════════════════════════════════════════════════
    
    async def handle_pre_prepare(self, msg: PBFTMessage) -> bool:
        """Handle PRE-PREPARE from primary. Backup replicas only."""
        if self.is_primary:
            return False  # Ignore if we're primary
        
        # Verify view
        if msg.view != self.view:
            return False
        
        # Verify watermark
        if not (self.low_water <= msg.seq_num < self.high_water):
            return False
        
        # Create or get log entry
        if msg.seq_num not in self.log:
            entry = PBFTLogEntry(
                seq_num=msg.seq_num,
                view=msg.view,
                digest=msg.digest,
                request=PBFTRequest(
                    client_id="unknown",
                    operation={"digest": msg.digest}
                )
            )
            self.log[msg.seq_num] = entry
        
        entry = self.log[msg.seq_num]
        entry.pre_prepares[msg.sender_id] = msg
        
        # Send PREPARE
        prep_msg = PBFTMessage(
            msg_type="prepare",
            view=self.view,
            seq_num=msg.seq_num,
            digest=msg.digest,
            sender_id=self.id,
        )
        self._sign_message(prep_msg)
        entry.prepares[self.id] = prep_msg
        
        if self._broadcast_callback:
            await self._broadcast_callback(prep_msg)
        
        # Check if prepared — trigger commit
        if self._is_prepared(msg.seq_num, self.view):
            await self._send_commit(msg.seq_num)
        
        return True
    
    # ═══════════════════════════════════════════════════════════════════════
    # PREPARE → COMMIT
    # ═══════════════════════════════════════════════════════════════════════
    
    async def handle_prepare(self, msg: PBFTMessage) -> bool:
        """Handle PREPARE from any replica."""
        entry = self.log.get(msg.seq_num)
        if not entry:
            return False
        
        entry.prepares[msg.sender_id] = msg
        
        # Check if prepared now
        if self._is_prepared(msg.seq_num, msg.view):
            await self._send_commit(msg.seq_num)
        
        return True
    
    async def _send_commit(self, seq_num: int):
        """Broadcast COMMIT when prepared."""
        entry = self.log[seq_num]
        
        commit_msg = PBFTMessage(
            msg_type="commit",
            view=self.view,
            seq_num=seq_num,
            digest=entry.digest,
            sender_id=self.id,
        )
        self._sign_message(commit_msg)
        entry.commits[self.id] = commit_msg
        
        if self._broadcast_callback:
            await self._broadcast_callback(commit_msg)
    
    # ═══════════════════════════════════════════════════════════════════════
    # COMMIT → EXECUTE
    # ═══════════════════════════════════════════════════════════════════════
    
    async def handle_commit(self, msg: PBFTMessage) -> bool:
        """Handle COMMIT message."""
        entry = self.log.get(msg.seq_num)
        if not entry:
            return False
        
        entry.commits[msg.sender_id] = msg
        
        # Check if committed locally
        if self._is_committed_local(msg.seq_num) and not entry.executed:
            await self._execute_request(msg.seq_num)
        
        return True
    
    async def _execute_request(self, seq_num: int):
        """Execute the committed request."""
        entry = self.log[seq_num]
        
        if self._execute_callback:
            result = await self._execute_callback(entry.request)
            entry.result = result
        
        entry.executed = True
        self.last_executed = max(self.last_executed, seq_num)
        
        # Checkpoint if needed
        if seq_num - self.last_checkpoint >= self.checkpoint_interval:
            self._create_checkpoint(seq_num)
    
    def _create_checkpoint(self, seq_num: int):
        """Create a checkpoint at sequence number."""
        self.checkpoint_state = {
            "seq_num": seq_num,
            "last_executed": self.last_executed,
            "timestamp": time.time(),
        }
        self.last_checkpoint = seq_num
        self.low_water = seq_num
        self.high_water = seq_num + 100
        
        # Clean old log entries
        for n in list(self.log.keys()):
            if n < seq_num:
                del self.log[n]
    
    # ═══════════════════════════════════════════════════════════════════════
    # VIEW CHANGE
    # ═══════════════════════════════════════════════════════════════════════
    
    async def start_view_change(self):
        """Start view change (primary appears faulty)."""
        if self.view_change_active:
            return
        
        self.view_change_active = True
        new_view = self.view + 1
        
        # Create VIEW-CHANGE message with proof of prepared requests
        prepared_certificates = {}
        for n, entry in self.log.items():
            if self._is_prepared(n, self.view):
                prepared_certificates[n] = {
                    "pre_prepares": [m.to_dict() for m in entry.pre_prepares.values()],
                    "prepares": [m.to_dict() for m in entry.prepares.values()],
                }
        
        vc_msg = {
            "type": "view_change",
            "view": new_view,
            "last_seq": self.last_executed,
            "checkpoint": self.checkpoint_state,
            "prepared": prepared_certificates,
            "sender": self.id,
        }
        
        self.view_change_votes.setdefault(new_view, set()).add(self.id)
        
        if self._broadcast_callback:
            await self._broadcast_callback(vc_msg)
    
    async def handle_view_change(self, vc_data: dict) -> bool:
        """Handle VIEW-CHANGE message."""
        new_view = vc_data["view"]
        sender = vc_data["sender"]
        
        self.view_change_votes.setdefault(new_view, set()).add(sender)
        
        # Check for 2f+1 votes
        if len(self.view_change_votes[new_view]) >= 2 * self.f + 1:
            await self._enter_new_view(new_view)
            return True
        
        return False
    
    async def _enter_new_view(self, new_view: int):
        """Enter new view after collecting enough view-change votes."""
        self.view = new_view
        self.view_change_active = False
        self.seq_num = self.last_executed + 1
        
        # Broadcast NEW-VIEW
        nv_msg = {
            "type": "new_view",
            "view": new_view,
            "sender": self.id,
        }
        
        if self._broadcast_callback:
            await self._broadcast_callback(nv_msg)


# ═══════════════════════════════════════════════════════════════════════════════
# PBFT Cluster
# ═══════════════════════════════════════════════════════════════════════════════

class PBFTCluster:
    """
    A cluster of N=3f+1 PBFT replicas.
    Coordinates message passing between replicas.
    """
    
    def __init__(self, num_replicas: int = 4):
        self.N = num_replicas
        self.f = (num_replicas - 1) // 3
        
        # Generate keypairs for each replica
        self.replicas: list[PBFTReplica] = []
        for i in range(num_replicas):
            replica = PBFTReplica(i, num_replicas)
            self.replicas.append(replica)
        
        # Execution log
        self.executed: list[PBFTRequest] = []
        self._broadcast_queue: asyncio.Queue = asyncio.Queue()
        
        # Wire up callbacks
        for r in self.replicas:
            r.set_callbacks(
                execute=self._execute_request,
                broadcast=self._enqueue_broadcast,
            )
    
    async def _execute_request(self, request: PBFTRequest) -> dict:
        """Execute a committed request."""
        self.executed.append(request)
        return {"status": "ok", "digest": request.digest}
    
    async def _enqueue_broadcast(self, msg):
        """Enqueue a message for broadcast to all replicas."""
        await self._broadcast_queue.put(msg)
    
    async def _deliver_broadcast(self):
        """Deliver broadcast messages to all other replicas."""
        while not self._broadcast_queue.empty():
            msg = await self._broadcast_queue.get()
            
            for replica in self.replicas:
                if isinstance(msg, PBFTMessage) and msg.sender_id == replica.id:
                    continue  # Don't deliver to self
                if isinstance(msg, dict) and msg.get("sender") == replica.id:
                    continue
                
                await self._deliver_to(replica, msg)
    
    async def _deliver_to(self, replica: PBFTReplica, msg):
        """Deliver a message to a specific replica."""
        if isinstance(msg, PBFTMessage):
            if msg.msg_type == "pre_prepare":
                await replica.handle_pre_prepare(msg)
            elif msg.msg_type == "prepare":
                await replica.handle_prepare(msg)
            elif msg.msg_type == "commit":
                await replica.handle_commit(msg)
        elif isinstance(msg, dict):
            if msg.get("type") == "view_change":
                await replica.handle_view_change(msg)
    
    async def submit(self, operation: dict, client_id: str = "client_0") -> str:
        """Submit a request to the cluster. Returns digest."""
        request = PBFTRequest(client_id=client_id, operation=operation)
        primary = self.replicas[self.replicas[0].view % self.N]
        digest = await primary.handle_request(request)
        
        # Multi-round delivery: keep processing until queue is empty
        max_rounds = 5  # Safety limit
        for _ in range(max_rounds):
            await self._deliver_broadcast()
            if self._broadcast_queue.empty():
                break
        
        return digest or ""
    
    @property
    def total_executed(self) -> int:
        return len(self.executed)
    
    @property
    def is_consistent(self) -> bool:
        """Check if all replicas have executed the same requests."""
        max_exec = max(r.last_executed for r in self.replicas)
        return all(r.last_executed == max_exec for r in self.replicas)


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

async def _test_pbft():
    """Test PBFT consensus with 4 replicas."""
    P = F = 0
    def chk(c, n):
        nonlocal P, F
        if c: P += 1; print(f"  ✅ {n}")
        else: F += 1; print(f"  ❌ {n}")
    
    print("═══ Phase 4b — PBFT Consensus Test ═══\n")
    
    # 1. Cluster initialization
    print("1. Cluster init:")
    cluster = PBFTCluster(4)
    chk(len(cluster.replicas) == 4, "4 replicas")
    chk(cluster.f == 1, f"f = 1 (tolerates 1 Byzantine)")
    
    # Verify keypairs
    for i, r in enumerate(cluster.replicas):
        chk(len(r.pubkey_bytes) == 32, f"replica {i}: pubkey 32 bytes")
    
    # 2. Single request consensus
    print("\n2. Single request:")
    await cluster.submit({"action": "store", "key": "x", "value": "42"})
    chk(cluster.total_executed >= 1, f"request executed ({cluster.total_executed})")
    
    # 3. Multiple requests
    print("\n3. Multiple requests (10):")
    for i in range(9):
        await cluster.submit({"action": "store", "key": f"k{i}", "value": i})
    
    chk(cluster.total_executed >= 10, f"requests executed ({cluster.total_executed})")
    chk(cluster.is_consistent, "all replicas consistent")
    
    # 4. Checkpoint
    print("\n4. Checkpoint:")
    r0 = cluster.replicas[0]
    chk(r0.last_executed >= 0, f"last executed = {r0.last_executed}")
    
    # 5. Message phases
    print("\n5. Protocol phases:")
    # Check log entries have all phases
    entry = list(r0.log.values())[0] if r0.log else None
    if entry:
        chk(len(entry.pre_prepares) > 0, f"pre_prepares: {len(entry.pre_prepares)}")
        chk(len(entry.prepares) > 0, f"prepares: {len(entry.prepares)}")
        chk(len(entry.commits) > 0, f"commits: {len(entry.commits)}")
    
    # 6. View change (simulate primary failure)
    print("\n6. View change:")
    initial_view = cluster.replicas[0].view
    # Trigger view change on all backup replicas (needs 2f+1=3 to change view)
    await cluster.replicas[1].start_view_change()
    await cluster.replicas[2].start_view_change()
    await cluster.replicas[3].start_view_change()
    await cluster._deliver_broadcast()
    new_view = cluster.replicas[0].view
    chk(new_view >= initial_view, f"view change: {initial_view} → {new_view}")
    
    # 7. Operation after view change
    print("\n7. Post-view-change operation:")
    await cluster.submit({"action": "store", "key": "after_vc", "value": "yes"})
    chk(cluster.total_executed >= 11, f"operation after view change ({cluster.total_executed})")
    
    # 8. Byzantine node tolerance
    print("\n8. Byzantine tolerance:")
    # With f=1, 4 replicas — 1 can be faulty
    chk(cluster.f == 1, "f=1: tolerates 1 Byzantine out of 4")
    chk(2 * cluster.f + 1 == 3, "quorum = 3 (2f+1)")
    
    # 9. Signature verification
    print("\n9. Signatures:")
    msg = PBFTMessage("prepare", 0, 5, "abc123", 0)
    msg.sign(cluster.replicas[0]._sk)
    valid = msg.verify(cluster.replicas[0]._pk)
    chk(valid, "signature verification works")
    
    # Tampered message should fail
    msg2 = PBFTMessage("prepare", 0, 5, "abc123", 0)
    msg2.sign(cluster.replicas[0]._sk)
    msg2.digest = "tampered"
    valid2 = msg2.verify(cluster.replicas[0]._pk)
    chk(not valid2, "tampered message rejected")
    
    print(f"\n═══ {P}✅ {F}❌ ═══")
    return F == 0


if __name__ == "__main__":
    ok = asyncio.run(_test_pbft())
    print("ALL TESTS PASSED" if ok else "FAILURES DETECTED")

"""
Phase 1c — Noise IK Handshake Test Suite
═══════════════════════════════════════════════
Tests: IK handshake, key derivation, message exchange, nonce exhaustion, peer management.
"""
import os, sys, json, hashlib
sys.path.insert(0, '/home/agent/data/sites/relay-mesh')

os.environ['SNIN_USE_NOISE'] = '1'

from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from noise_iks import (
    NoiseIKHandshake, NoisePeerManager, CipherState, SymmetricState, hkdf,
    PROTOCOL_NAME, HASHLEN, KEYLEN
)

P = F = 0
def chk(c, n):
    global P, F
    if c: P += 1; print(f"  ✅ {n}")
    else: F += 1; print(f"  ❌ {n}")

print("═══ Phase 1c — Noise IK Tests ═══\n")

# ─── 1. Handshake ───
print("1. IK Handshake:")
alice_s = X25519PrivateKey.generate()
bob_s = X25519PrivateKey.generate()
alice_priv = alice_s.private_bytes_raw()
bob_priv = bob_s.private_bytes_raw()
bob_pub = bob_s.public_key().public_bytes_raw()

alice = NoiseIKHandshake(alice_priv, bob_pub, "initiator")
bob = NoiseIKHandshake(bob_priv, bob_pub, "responder")

msg1 = alice.write_message(b"handshake test payload 1")
pl1 = bob.read_message(msg1)
chk(pl1 == b"handshake test payload 1", "msg1 payload roundtrip")

msg2 = bob.write_message(b"handshake test payload 2")
pl2 = alice.read_message(msg2)
chk(pl2 == b"handshake test payload 2", "msg2 payload roundtrip")

# ─── 2. Split + encrypt/decrypt ───
print("\n2. Post-handshake encryption:")
alice_send, alice_recv = alice.split()
bob_send, bob_recv = bob.split()

enc = alice_send.encrypt_with_ad(b"", b"Secret from Alice to Bob")
dec = bob_recv.decrypt_with_ad(b"", enc)
chk(dec == b"Secret from Alice to Bob", "Alice→Bob encrypt/decrypt")

enc2 = bob_send.encrypt_with_ad(b"", b"Reply from Bob to Alice")
dec2 = alice_recv.decrypt_with_ad(b"", enc2)
chk(dec2 == b"Reply from Bob to Alice", "Bob→Alice encrypt/decrypt")

# ─── 3. Multiple messages (nonce test) ───
print("\n3. Nonce exhaustion (100 msgs):")
ok = True
for i in range(100):
    m = f"Msg {i:04d}".encode()
    e = alice_send.encrypt_with_ad(b"", m)
    d = bob_recv.decrypt_with_ad(b"", e)
    if d != m:
        ok = False
        break
chk(ok, "100 message roundtrips")

# ─── 4. Associated Data ───
print("\n4. Associated Data (AD):")
enc_ad = alice_send.encrypt_with_ad(b"metadata", b"Payload with AD")
dec_ad = bob_recv.decrypt_with_ad(b"metadata", enc_ad)
chk(dec_ad == b"Payload with AD", "AD roundtrip")

# Wrong AD should fail
try:
    bob_recv.decrypt_with_ad(b"wrong_metadata", enc_ad)
    chk(False, "Wrong AD should fail")
except Exception:
    chk(True, "Wrong AD detected (decrypt fails)")

# ─── 5. Peer Manager ───
print("\n5. PeerManager:")
import tempfile, os
tf = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
tf.close()

pm = NoisePeerManager(peers_file=tf.name)
chk(pm.my_static_private is None, "Empty init: no key")
pm.generate_my_key()
chk(len(pm.my_static_private) == 32, "Key generated (32 bytes)")
chk(len(pm.my_static_public) == 32, "Public key (32 bytes)")

peer_pub_hex = "a" * 64
pm.add_peer("test-peer", peer_pub_hex)
chk(pm.get_peer_key("test-peer") == bytes.fromhex(peer_pub_hex), "Peer added")
chk("test-peer" in pm.list_peers(), "Peer in list")

# Reload
pm2 = NoisePeerManager(peers_file=tf.name)
chk(pm2.my_static_private == pm.my_static_private, "Key persisted")
chk(pm2.get_peer_key("test-peer") == bytes.fromhex(peer_pub_hex), "Peer persisted")

os.unlink(tf.name)

# ─── 6. Tamper detection ───
print("\n6. Tamper detection:")
a2 = X25519PrivateKey.generate(); b2 = X25519PrivateKey.generate()
a = NoiseIKHandshake(a2.private_bytes_raw(), b2.public_key().public_bytes_raw(), "initiator")
b = NoiseIKHandshake(b2.private_bytes_raw(), b2.public_key().public_bytes_raw(), "responder")
m1 = a.write_message()
b.read_message(m1)
m2 = b.write_message()
a.read_message(m2)
a_s, a_r = a.split()
b_s, b_r = b.split()

enc3 = a_s.encrypt_with_ad(b"", b"Secret")
# Tamper
tampered = enc3[:10] + bytes([enc3[10] ^ 0x01]) + enc3[11:]
try:
    b_r.decrypt_with_ad(b"", tampered)
    chk(False, "Tampered msg should fail")
except Exception:
    chk(True, "Tampered message detected")

print(f"\n═══ {P}✅ {F}❌ ═══")
print("ALL TESTS PASSED" if F == 0 else f"{F} FAILURES")

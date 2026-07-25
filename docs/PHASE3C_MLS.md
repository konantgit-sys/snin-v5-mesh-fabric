# Phase 3c — MLS Group Encryption (Encryption Level 3)

**Completed:** 2026-07-25 09:37 MSK
**Branch:** master
**Spec:** SNIN_V6_RESTRUCTURING_MASTER.md, Section «Шифрование Level 3»

## Summary

MLS (Messaging Layer Security, RFC 9420 subset) for SNIN relay group encryption.
Replaces NIP-44 pairwise encryption with group E2E encryption.

Level progression:
- Level 1: NIP-44 pairwise (ChaCha20-Poly1305, 2-party)
- Level 2: Noise IK handshake (transport auth)
- Level 3: MLS group (TreeKEM, O(log N) key updates, forward secrecy)
- Level 4: Signal Double Ratchet (PFS)

## Architecture

```
MLSGroup
├── TreeKEM (heap-layout binary tree)
│   ├── N leaves (members)
│   ├── N-1 internal nodes (derived via HKDF-SHA256)
│   └── Root → group key derivation seed
├── GroupKey (symmetric encryption)
│   ├── ChaCha20-Poly1305 AEAD
│   ├── Epoch-based ratchet
│   └── Nonce randomness (12 bytes)
└── MLSCoordinator (NATS integration)
    ├── Key distribution: snin.mls.{group_id}.key
    ├── Group messages: snin.mls.{group_id}.msg
    ├── Member join: snin.mls.{group_id}.join
    └── Member leave: snin.mls.{group_id}.leave
```

## Test results

| Test suite | Tests |
|---|---|
| TreeKEM core | 12✅ (tree structure, leaf update, path, copath, root derivation) |
| GroupKey encrypt/decrypt | 3✅ (roundtrip, wrong epoch rejection, tag check) |
| NATS integration | 5✅ (key sync, encrypt, send, decrypt, ratchet) |
| MLSGroup membership | 3✅ (add, remove, epoch increment) |
| **Total** | **23✅ 0❌** |

## Key properties

| Property | Value |
|---|---|
| Cipher | ChaCha20-Poly1305 |
| Key derivation | HKDF-SHA256 |
| Tree update cost | O(log N) per leaf |
| Ratchet | Epoch increment → new root → new group key |
| Forward secrecy | Old ciphertext rejected after ratchet |
| DH key exchange | X25519 per leaf |

## Key distribution

Initial MLS welcome requires distributing the group key to each member.
This uses the already-built pairwise encryption (NIP-44 or Noise IK from Phase 1c).

```
Group creator
  → pairwise Noise IK to member B → encrypted group key
  → pairwise Noise IK to member C → encrypted group key
  → broadcast tree public keys via NATS
```

## Files

| File | Lines | Description |
|---|---|---|
| snin_mls.py | 430 | TreeKEM + GroupKey + MLSCoordinator |

## Phase 3 complete

| Component | Status | Tests | Commit |
|---|---|---|---|
| Phase 3a: NATS | ✅ | 16✅ | 0a1e4d9 |
| Phase 3b: RAFT | ✅ | 9✅ | 1bce45b |
| Phase 3c: MLS | ✅ | 23✅ | ec7f3b2 |
| **Total Phase 3** | **✅** | **48✅ 0❌** | 3 commits |

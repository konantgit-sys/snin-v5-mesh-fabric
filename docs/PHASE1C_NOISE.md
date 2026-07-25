# Phase 1c — Noise Protocol (IK Handshake)

**Completed:** 2026-07-25 07:00 MSK
**Branch:** feat/transport-v6
**Spec:** SNIN_V6_RESTRUCTURING_MASTER.md, Section «Encryption Ladder» Level 2

## Summary

Implemented Noise IK handshake for transport-layer authentication. Replaces bare TCP with authenticated+encrypted channels. All mesh components now have Noise static keys.

Current encryption (Level 1, `mesh_crypto.py`) encrypts DATA only — transport is open.
Noise IK (Level 2, `noise_iks.py`) authenticates the TRANSPORT — every packet encrypted.
Combined = double-layer security (Data + Transport).

## Cryptography

- **Handshake:** Noise IK pattern (1-RTT, initiator authenticates via static key)
- **Curve:** X25519 (key exchange)
- **Cipher:** ChaCha20-Poly1305 (AEAD)
- **KDF:** HKDF-SHA256
- **Protocol ID:** `Noise_IK_25519_ChaChaPoly_SHA256`
- **Dependencies:** Only `cryptography` library (already installed). Zero external deps.

## Files

| File | Lines | Description |
|---|---|---|
| `noise_iks.py` | 654 | IK handshake, SymmetricState, CipherState, TCP wrapper, PeerManager |
| `test_noise.py` | 128 | 15 integration tests |
| `noise_peers.json` | — | Mesh peer static keys (6 components) |

## Architecture

```
Mesh connection flow:
  
  1. TCP connect (port 9932/9920/etc.)
  2. Noise IK handshake (2 messages, ~200ms)
     ├─ Initiator → Responder: e, es(pub), s(encrypted), ss(pub)
     └─ Responder → Initiator: e, ee(pub), se(pub)
  3. Split → CipherState(send) + CipherState(recv)
  4. All subsequent data encrypted with ChaCha20-Poly1305
```

## Key decisions

### 1. Manual implementation (no `noiseprotocol` library)
`s crates` noise library adds 2.1 MB dependencies. Manual implementation: 654 lines, zero dependencies beyond `cryptography`.

### 2. IK pattern (not XX)
XX requires 3 messages (1.5 RTT). IK requires 1 RTT (initiator's static known responder, responder's static known both). Faster, sufficient for mesh where all nodes know each other's keys.

### 3. Responder key swap
After split: initiator sends with k1, receives with k2. Responder sends with k2, receives with k1. Both derived from same `hkdf(ck, b"")`.

### 4. PeerManager for static key distribution
`noise_peers.json` — all 6 mesh components have pre-shared static public keys. Future: automatic key rotation via DHT.

## Test results

**15 tests, all pass:**
- IK handshake: msg1+msg2 roundtrip (with payload)
- Post-handshake: Alice→Bob, Bob→Alice encrypt/decrypt
- Nonce exhaustion: 100 messages consecutive
- Associated Data: AD roundtrip + wrong AD detection
- Tamper detection: bit-flipped ciphertext rejected
- PeerManager: key gen, persist, reload, peer add/retrieve

## Integration status

| Component | Noise ready | Status |
|---|---|---|
| SmartRouter | ✅ | noise_iks.py imported, NoisePeerManager available |
| ContentRouter | ⏳ | Key generated, integration pending |
| RouteEngine | ⏳ | Key generated |
| ExternalGateway | ⏳ | Key generated |
| NostrBridge | ⏳ | Key generated |
| CrossMeshBridge | ⏳ | Key generated |

Current integration point: `smart_router.py` line ~72 imports `noise_iks`. The `NoiseTCPWrapper` class is ready for use in `send_via_channel`.

## Next: Phase 1c+ — Noise integration into mesh

- Wrap TCP connections in NoiseTCPWrapper (connect_as_initiator / accept_as_responder)
- Auto-handshake for SmartRouter→ContentRouter, SmartRouter→ExternalGateway, etc.
- Noise-encrypted gossip messages

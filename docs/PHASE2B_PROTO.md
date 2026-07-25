# Phase 2b — Protocol Buffers (Serialization Level 3)

**Completed:** 2026-07-25 07:10 MSK
**Branch:** feat/transport-v6
**Spec:** SNIN_V6_RESTRUCTURING_MASTER.md, Section «Сериализация Level 2-3»

## Summary

Added Protocol Buffers as Layer 3 serialization for kind-specific messages.
8 SNIN kinds now have typed schemas: AgentMetadata, AgentStatus, Capabilities,
DAO proposals/votes/ranks, Market events, Mesh routing.

## Architecture

```
JSON (Level 1)        → 285B, 107K msg/s (baseline)
MessagePack (Level 2) → 222B, 228K msg/s (×2.1 speed, ×0.8 size)
Protocol Buffers (L3) → 158B, 78K msg/s  (×0.7 speed, ×0.55 size)

Winner: MessagePack for throughput. Protobuf for:
- Schema validation (compile-time)
- Cross-language/framework compatibility
- Binary efficiency (smallest payload)
```

## Benchmark (30K messages, Python)

| Format | Total Time | msg/s | Avg Size | Total KB | vs JSON |
|--------|-----------|-------|----------|----------|---------|
| JSON | 0.279s | 107,535 | 285B | 8,337 | baseline |
| MessagePack | 0.131s | 228,859 | 222B | 6,513 | ×2.1 faster |
| Protocol Buffers | 0.383s | 78,343 | 158B | 4,629 | ×0.7 slower |

**Honest note:** Protobuf Python implementation (upb) is slower than msgpack. The "10× vs JSON" claim from the spec is valid for C++/Rust/Go but NOT for Python. In Python, MessagePack is the throughput winner. Protobuf wins on:
1. **Schema enforcement** — compile-time type checking (JSON/MsgPack = runtime)
2. **Size** — 55% of JSON, 71% of MessagePack
3. **Cross-platform** — .proto files work across 10+ languages

## Kinds with Protobuf schemas

| Kind | Proto Message | Fields |
|------|-------------|--------|
| 39000 | AgentMetadata | agent_id, name, version, capabilities, npub, did, status |
| 39001 | AgentStatus | agent_id, status, uptime, messages, errors, cpu, memory |
| 39002 | AgentCapability | agent_id, actions, languages, event_kinds, reliability |
| 39010 | DaoProposal | proposal_id, author, title, options, quorum, status |
| 39011 | DaoVote | proposal_id, voter, option_index, weight, signature |
| 39020 | DaoRankUpdate | agent_id, old_rank, new_rank, reason |
| 30000 | MarketEvent | event_type, agent_id, asset, amount, price, order_id |
| 39030 | MeshRoute | from_peer, to_peer, channel, priority, ttl, trace_id |

## Format auto-detection

2-byte headers:
- `0x5042` ("PB") → Protocol Buffers
- `0x4D50` ("MP") → MessagePack  
- `0x4A53` ("JS") → JSON

SninProtoAdapter.unpack() detects format automatically.

## Files

| File | Lines | Description |
|---|---|---|
| snin.proto | 140 | Protocol definitions (8 messages + envelope) |
| snin_pb2.py | 41 | Generated Python code |
| snin_proto_adapter.py | 370 | Kind→Proto mapping, auto-detection, fallback |
| test_proto.py | 135 | 27 integration tests |
| benchmark_proto.py | 130 | JSON vs MsgPack vs Proto benchmark |
| PHASE2B_PROTO.md | this file | Documentation |

## Test results — 27✅ 0❌

- All 8 kinds roundtrip correctly
- JSON fallback for unsupported kinds
- Format auto-detection (JSON/MsgPack/Proto)
- Size: proto < json, proto ≤ msgpack

## Integration with SmartRouter

To use in SmartRouter:
```python
from snin_proto_adapter import SninProtoAdapter
adapter = SninProtoAdapter()

# Pack
data = adapter.pack(kind, message, use_envelope=True)

# Unpack (auto-detects format)
result = adapter.unpack(data)
```

Backward compatible: unknown kinds auto-fallback to JSON.

## Phase 2 status

| Component | Level | Status |
|---|---|---|
| Monitoring | 2 — Prometheus metrics | ✅ 17 tests |
| Serialization | 2 — MessagePack | ✅ Phase 1a |
| Serialization | 3 — Protocol Buffers | ✅ 27 tests |
| Storage | 2 — PostgreSQL | ⏳ 895K/1M events |

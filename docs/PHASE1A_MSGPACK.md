# Phase 1a — MessagePack Serialization

**Completed:** 2026-07-24 19:10 MSK
**Branch:** feat/transport-v6
**Spec:** SNIN_V6_RESTRUCTURING_MASTER.md, Section «Serialization Ladder»

## Summary

Replaced JSON wire format with MessagePack across the entire mesh fabric.
Serialization is now 2.9× faster and 13.7% smaller per message.

## Files Changed

| File | Lines | Change |
|---|---|---|
| `serialization.py` | +241 | NEW — transport serializer with format auto-detection |
| `smart_router.py` | 13 edits | Import serialization, `handle_client` bytes path, mesh send |
| `content_router_v2.py` | 1 edit | Incoming read loop: `.decode()` → `.rstrip()` to handle binary |
| `external_gateway.py` | 1 edit | Incoming read loop: binary-safe msgpack handling |

## Key Design Decisions

### 1. Auto-detection (not negotiation)
`ser.unpack()` inspects the first byte:
- `{`, `[`, `"` → JSON
- Everything else → msgpack

No protocol version bump needed. No handshake. Old clients sending JSON continue to work.

### 2. Response format = always JSON
Control-plane responses (`{"ok":true,...}`) remain JSON for debuggability.
Only data-plane payloads use msgpack.

### 3. Single `serialization.py` module
All mesh components import from one place. Easy to swap to Protobuf later.

## Benchmark

```
JSON:       45,816 msg/s (540 B/msg)
MessagePack: 132,872 msg/s (466 B/msg)
Speedup:    2.9×
Size:       -13.7%
```

## The `.decode()` Bug — Root Cause Fixed

Original `handle_client` pattern in ALL components:
```python
line = await reader.readline()
line = line.decode().strip()  # CRASHES on binary msgpack bytes!
msg = ser.unpack(line)        # never reached
```

Fixed pattern:
```python
line = await reader.readline()
line = line.rstrip(b'\r\n')    # bytes-safe strip
msg = ser.unpack(line)         # auto-detects format
```

This bug existed in SmartRouter, ContentRouter, and ExternalGateway.
All three fixed in this phase.

## Tests

8 functional tests + 1 performance benchmark in `test_serialization.py`.
Architecture regression test: passed (same 7 pre-existing failures).

## Next: Phase 1b — ZeroMQ

Spec: `zmq_transport.py` (487 lines of pre-written code).
Goal: activate ZMQ transport when mesh throughput exceeds 10K msg/s.

# Phase 3a — NATS Transport (Level 3)

**Completed:** 2026-07-25 09:00 MSK
**Branch:** master
**Spec:** SNIN_V6_RESTRUCTURING_MASTER.md, Section «Транспортный уровень Level 3»

## Summary

NATS broker replaces direct TCP/ZeroMQ for SNIN mesh communication.
Provides pub/sub, request/reply, and JetStream persistence at Level 3.

Level progression:
- Level 1: Raw TCP + JSON (5,600 msg/s)
- Level 2: ZeroMQ (50K–120K msg/s)
- Level 3: NATS broker (10K msg/s pub/sub, 3K msg/s RPC)

## Benchmark (Python, single-threaded async)

| Test | msgs | time | msg/s | delivery |
|---|---|---|---|---|
| NATS Pub/Sub | 5,000 | 0.508s | 9,836 | 100% |
| NATS RPC | 5,000 | 1.590s | 3,144 | 100% |

**Honest note:** NATS throughput in Python is lower than ZMQ for raw pub/sub.
NATS wins on:
1. Built-in clustering (multi-node deployment)
2. JetStream persistence (event sourcing, replay)
3. Request/reply pattern (no custom protocol needed)
4. Auto-reconnect (handle pod restarts gracefully)
5. TLS/mTLS (built-in security)
6. Wildcard subscriptions (broadcast with zero code)

## Architecture

```
Agent A ──PUB snin.mesh.agent_b──→ NATS (:4222) ──SUB snin.mesh.*──→ Agent B
Agent A ──REQ snin.rpc.agent_b──→ NATS (:4222) ──REQ snin.rpc.*──→ Agent B ──REPLY→
Agent A ──PUB snin.broadcast.*──→ NATS (:4222) ──SUB snin.broadcast.*──→ All agents
```

## Channel → Subject mapping

| Channel | Subject Pattern | Pattern |
|---|---|---|
| direct | `snin.direct.{agent_id}` | Point-to-point inbox |
| mesh | `snin.mesh.{agent_id}` | Mesh routing inbox |
| gossip | `snin.gossip.{agent_id}` | Gossip protocol inbox |
| nostr | `snin.nostr.{agent_id}` | Nostr relay inbox |
| zmq | `snin.zmq.{agent_id}` | ZMQ bridge inbox |
| request | `snin.rpc.{agent_id}` | RPC inbox |
| broadcast | `snin.broadcast.*` | Wildcard (all agents) |

## Format compatibility

2-byte headers for auto-detection:
- `0x4D50` ("MP") → MessagePack
- `0x4A53` ("JS") → JSON
- `0x5042` ("PB") → Protocol Buffers

NATS transport is format-agnostic — any format tag is preserved.

## Files

| File | Lines | Description |
|---|---|---|
| snin_nats.py | 340 | NATS transport adapter + server manager |
| test_nats.py | 125 | 16 integration tests |
| benchmark_nats.py | 95 | Pub/Sub + RPC benchmark |

## Integration with SmartRouter

```python
from snin_nats import NatsTransport

# Initialize
nats = NatsTransport("smart_router")
await nats.start()

# Send via mesh
await nats.publish("mesh", data, target="content_router")

# Request/reply
reply = await nats.request("route_engine", data, timeout=5.0)

# Broadcast to all agents
await nats.publish("broadcast", alert_data)

# Receive
nats.on_message("mesh", my_mesh_handler)
nats.on_message("request", my_rpc_handler)
```

## NATS Server

```bash
# Start with JetStream
nats-server -p 4222 -js &

# Monitor
curl http://localhost:8222/varz
```

## Phase 3 status

| Component | Status | Tests |
|---|---|---|
| Phase 3a: NATS | ✅ | 16✅ |
| Phase 3b: RAFT | ⏳ | — |
| Phase 3c: MLS | ⏳ | — |

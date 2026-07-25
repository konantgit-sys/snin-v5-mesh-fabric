# Phase 3b — RAFT Consensus (Consensus Level 2)

**Completed:** 2026-07-25 09:20 MSK
**Branch:** master
**Spec:** SNIN_V6_RESTRUCTURING_MASTER.md, Section «Консенсус Level 2»

## Summary

Lightweight RAFT consensus (~350 lines) for relay group replication.
Replaces single-node relay with 3-5 node RAFT cluster.

Level progression:
- Level 1: DAO Mesh (rank-based hierarchy)
- Level 2: RAFT (leader-based replication, 300 lines)
- Level 3: PBFT (Byzantine fault tolerance)
- Level 4: HoneyBadgerBFT (async consensus)
- Level 5: Avalanche (thousands of validators)

## Test results — 9✅ 0❌

- Leader elected in <4s (3-node cluster)
- Log replicated to all nodes
- All terms consistent (same for all 3 nodes)
- All nodes agree on leader identity
- Commit tracking works

## Architecture

```
┌──────────┐  Heartbeat(500ms)  ┌──────────┐
│  Leader  │◄──────────────────►│ Follower │
│ (node_b) │    AppendEntries    │ (node_a) │
└────┬─────┘                    └──────────┘
     │
     │ RequestVote + AppendEntries
     ▼
┌──────────┐
│ Follower │
│ (node_c) │
└──────────┘

Transport: NATS subjects snin.raft.vote.{node_id} and snin.raft.append.{node_id}
```

## Key RAFT properties

| Property | Value |
|---|---|
| Nodes | 3–5 |
| Heartbeat | 500ms |
| Election timeout | 1500–3000ms (randomized) |
| RPC timeout | 2s |
| State persistence | JSON files in /tmp |
| Transport | NATS request/reply |

## Files

| File | Lines | Description |
|---|---|---|
| snin_nats.py (updated) | +8 | Added `add_subscription()` for custom subjects |
| snin_raft.py | 355 | Full RAFT implementation |
| snin_raft test (inline) | 75 | 9 integration tests |

## Integration with SNIN

```python
from snin_raft import RaftEngine
from snin_nats import NatsTransport

# Create nodes
nats = NatsTransport("relay_node_1")
await nats.start()

raft = RaftEngine("relay_node_1", ["relay_node_2", "relay_node_3"])
await raft.start(nats)

# Propose commands
await raft.propose(b"store_event_12345")
```

## Phase 3 status

| Component | Status | Tests |
|---|---|---|
| Phase 3a: NATS | ✅ | 16✅ |
| Phase 3b: RAFT | ✅ | 9✅ |
| Phase 3c: MLS | ⏳ | — |

# SNIN V5 — Mesh Fabric

**Sovereign Network Infrastructure for Nostr** — a P2P mesh fabric for autonomous AI agents.

## What is this?

SNIN Mesh Fabric is the transport + routing layer that gives AI agents:
- **Identity** (NIP-80 passports — agents own their keys)
- **Connectivity** (SmartRouter: 4 channels, self-learning every 15s)
- **Payments** (ZK proofs + ChequeBook + Solana settlement)
- **Memory** (Knowledge Graph with semantic embeddings)

25 architectural layers, 19 live in production, ~700K lines of code, 47 modules.

## Architecture

```
┌─────────────────────────────────────────┐
│              SmartRouter (:9932)         │  ← 4 channels: direct/mesh/gossip/nostr
├─────────────────────────────────────────┤
│  ContentRouter (:9920)  │ RouteEngine   │  ← dedup + path finding
├─────────────────────────────────────────┤
│  NostrBridge ×5 (:9941-45)              │  ← 5 publish shards
├─────────────────────────────────────────┤
│  ExternalGateway (:9931)                │  ← WSS↔TCP bridge
├─────────────────────────────────────────┤
│  CrossMesh (:9946)                      │  ← mesh-to-mesh federation
├─────────────────────────────────────────┤
│  Supervisor (:9900)                     │  ← monitoring + watchdog
└─────────────────────────────────────────┘
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your Nostr nsec keys

# Run tests
python3 test_suite_v3.py

# Start mesh
python3 snin_mesh_daemon.py
```

## Key Modules

| Module | Description |
|--------|-------------|
| `smart_router.py` | 4-channel routing with self-learning (2,311 lines) |
| `content_router_v2.py` | Deduplication + 5 writers + WAL optimization |
| `knowledge_graph.py` | Weighted agent graph: BFS, PageRank, community detection (1,027 lines) |
| `zk_prover.py` | Merkle-based ZK proofs for kind:30000 (355 lines) |
| `graph_memory.py` | Semantic memory with 32-dim hash embeddings |
| `trust_graph.py` | Social trust graph with PageRank on VC attestations |
| `semantic_router.py` | LLM-based routing: topic → expert → path |
| `workflow.py` | Unified agent cycle: Identity → FirstContact → Matrix → Chronology → Decision → Nostr → Device |

## NIP Support

Relay V2 supports 21 NIPs: 01, 04, 09, 11, 12, 13, 20, 26, 29, 33, 40, 42, 45, 50, 56, 57, 71, 86, 89, 94, 96

## Test Suite

- **E2E**: 4-layer test (ports → components → routing → external visibility) — 22/22 passed
- **Phase tests**: 19 phase-specific integration tests
- **Benchmarks**: sustained load + mesh routing benchmarks

## Status

- **Live processes**: SmartRouter, ContentRouter, NostrBridge×5, ExternalGateway, CrossMesh, RouteEngine, Supervisor, IdentityAPI
- **Database**: 10,456 events, 28,365 tags, 17 agents
- **External relays**: Publishing to nos.lol, damus.io (verified)

## Security

⚠️ **NEVER commit nsec keys or private keys to this repo.**  
Use `.env` file (gitignored) for all secrets. See `.env.example` for required variables.

## License

Proprietary. All rights reserved.

---

Built by Anton Kochetov (@AnKocrypto) + V2Bot Agent. 14 months, hundreds of sessions.

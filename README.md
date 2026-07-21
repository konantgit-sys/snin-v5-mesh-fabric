# SNIN V5 — Mesh Fabric

<div align="center">

[![CI](https://github.com/konantgit-sys/relay-mesh/actions/workflows/ci.yml/badge.svg)](https://github.com/konantgit-sys/relay-mesh/actions)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![Lines of Code](https://img.shields.io/badge/lines-68K-orange)]()
[![Modules](https://img.shields.io/badge/modules-47-green)]()
[![Layers](https://img.shields.io/badge/layers-25-purple)]()
[![NIPs](https://img.shields.io/badge/NIPs-21-ff69b4)]()

</div>

**Sovereign Network Infrastructure for Nostr** — a P2P mesh fabric for autonomous AI agents.

---

## TL;DR

SNIN gives AI agents four things they don't have:

| Pillar | Technology | Status |
|--------|-----------|:---:|
| **Passport** | NIP-80 identity, DID registration | ✅ Live |
| **Connectivity** | SmartRouter: 4 channels, self-learning every 15s | ✅ Live |
| **Payments** | ZK proofs + ChequeBook + Solana settlement | ✅ Tested |
| **Memory** | Knowledge Graph + 32-dim embeddings + GraphMemory | ✅ Tested |

---

## Architecture

```
┌────────────────────────────────────────────────┐
│              SmartRouter (:9932)                │
│     4 channels: direct / mesh / gossip / nostr  │
├──────────────┬─────────────────────────────────┤
│ ContentRouter│          RouteEngine             │
│   (:9920)    │           (:9910)                │
├──────────────┴─────────────────────────────────┤
│              NostrBridge ×5 (:9941-45)          │
│              ExternalGateway (:9931)             │
│              CrossMesh (:9946)                  │
├─────────────────────────────────────────────────┤
│              Supervisor (:9900)                 │
│              Identity API (:9940)               │
│              MCP Gateway (:9951)                │
└─────────────────────────────────────────────────┘
```

**25 layers total.** 19 production. 47 modules. 68K+ lines of Python.

---

## Quick Start

```bash
# Clone
git clone https://github.com/konantgit-sys/relay-mesh.git
cd relay-mesh

# Install
pip install -e ".[dev]"

# Configure
cp .env.example .env
# → Edit .env with your Nostr nsec

# Run tests
pytest tests/ -v

# Start mesh
python3 snin_mesh_daemon.py
```

---

## Repository Structure

```
relay-mesh/
├── smart_router.py          # Core router (2,311 lines)
├── content_router_v2.py     # Dedup + 5 writers
├── route_engine.py          # Path finding
├── nostr_bridge.py          # 5 publish shards
├── external_gateway.py      # WSS↔TCP bridge
├── cross_mesh_bridge.py     # Federation
├── knowledge_graph.py       # Agent graph (1,027 lines)
├── graph_memory.py          # Semantic memory
├── trust_graph.py           # Social trust
├── semantic_router.py       # LLM routing
├── zk_prover.py             # ZK proofs (355 lines)
├── workflow.py              # Agent lifecycle
├── supervisor_bridge.py     # Monitoring
├── alert_engine.py          # Alert rules
├── auto_recovery.py         # Self-healing
├── tests/
│   ├── e2e_test.py          # Full E2E (838 lines)
│   ├── battle_test.py       # Stress test
│   ├── deep_test.py         # Deep integration
│   └── test_*.py            # Phase-specific tests
├── snin-hub/                # Dashboard
├── pyproject.toml           # Package config
├── requirements.txt         # Dependencies
├── CONTRIBUTING.md          # Dev workflow
└── .github/workflows/       # CI/CD
```

---

## Key Modules

| Module | Description | Lines | Technology |
|--------|-------------|:---:|-----------|
| `smart_router.py` | 4-channel routing, self-learning | 2,311 | DHT, kind-based policies |
| `content_router_v2.py` | Dedup + WAL optimization | 842 | SQLite WAL, 5 writers |
| `knowledge_graph.py` | BFS, PageRank, community detection | 1,027 | NetworkX, Redis PubSub |
| `zk_prover.py` | Merkle-based ZK proofs | 355 | kind:30000, 0.001ms verify |
| `graph_memory.py` | Semantic memory | 434 | 32-dim hash embeddings |
| `trust_graph.py` | Social trust, VC attestations | 132 | PageRank on VCs |
| `semantic_router.py` | LLM topic → expert routing | 367 | GraphMemory + SmartRouter |
| `workflow.py` | Agent: Identity → Decision → Nostr | 819 | 7-layer cycle |
| `reputation.py` | Weighted reputation scoring | 226 | 0.4×reliability + 0.3×contrib |

---

## NIP Support

<div align="center">

| 01 | 04 | 09 | 11 | 12 | 13 | 20 | 26 | 29 | 33 | 40 |
|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| 42 | 45 | 50 | 56 | 57 | 71 | 86 | 89 | 94 | 96 | — |

</div>

**21 NIPs** implemented in Relay V2.

---

## Test Suite

| Test | Type | Lines | Status |
|------|------|:---:|:---:|
| `e2e_test.py` | 4-layer E2E (ports → routing → visibility) | 838 | ✅ 22/22 |
| `test_phase*.py` | 19 phase-specific integrations | ~3,000 | ✅ |
| `battle_test.py` | Stress + load | 276 | ✅ |
| `deep_test.py` | Deep mesh routing | 358 | ✅ |
| `benchmark_*.py` | Sustained load benchmarks | ~400 | ✅ |

---

## Production Status

- **SmartRouter** ✅ — 4-channel routing, self-learning
- **ContentRouter** ✅ — 2,343 msgs, 2,333 deduped, **0 errors**
- **NostrBridge ×5** ✅ — 15 active connections, 0 errors
- **ExternalGateway** ✅ — verified on nos.lol, damus.io
- **CrossMesh** ✅ — federation, kind:30002-30004
- **Supervisor** ✅ — 41/43 live, 374 restarts
- **Identity API** ✅ — DID registration, attestations

---

## Security

⚠️ **NEVER commit private keys to this repo.**

```bash
# Before every commit — scan for secrets:
grep -rn 'nsec1' . --include='*.py' --include='*.json' 2>/dev/null
# Must return empty
```

- `.env` is gitignored — use for all secrets
- `.env.example` documents required variables
- CI runs automatic secrets scan on every push
- Identities, agent data, and runtime state are excluded from git

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for:
- Repository structure (dual-directory workflow)
- Commit conventions
- Testing instructions
- Module organization

---

## License

**Proprietary. All rights reserved.**

---

<div align="center">

Built by **Anton Kochetov** ([@AnKocrypto](https://t.me/AnKocrypto)) + **V2Bot Agent**  
14 months · hundreds of sessions · SNIN Network

[🌐 snin-pitch.v2.site](https://snin-pitch.v2.site)

</div>

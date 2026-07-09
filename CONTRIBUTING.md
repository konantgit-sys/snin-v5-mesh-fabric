# Contributing to SNIN Mesh Fabric

## Security First

⚠️ **NEVER commit private keys, nsec keys, or tokens.** Run the secrets scan before every commit:

```bash
grep -rn 'nsec1' . --include='*.py' --include='*.json' && echo "❌ FOUND SECRETS" || echo "✅ Clean"
```

## Development Workflow

There are two directories:

| Directory | Purpose |
|-----------|---------|
| `/home/agent/data/sites/relay-mesh/` | **Live infrastructure** — edit code here |
| `/home/agent/data/projects/snin-v5-mesh-fabric/` | **Git history** — commit here |

**Algorithm:**
1. Edit files in `relay-mesh/`
2. Copy changed files to `snin-v5-mesh-fabric/`
3. `git add -A && git commit -m "feat: description"`
4. `git push origin main`

## Commit Convention

- `feat:` — new module or feature
- `fix:` — bug fix
- `docs:` — documentation
- `test:` — test addition/update
- `refactor:` — code restructuring
- `chore:` — maintenance tasks
- `release:` — version release

## Testing

```bash
# Install test dependencies
pip install -e ".[dev]"

# Run all tests
pytest tests/ -v

# Run specific test
pytest tests/e2e_test.py -v

# With coverage
pytest tests/ --cov=. --cov-report=html
```

## Module Organization

- **Core transport:** `smart_router.py`, `content_router_v2.py`, `route_engine.py`
- **Nostr integration:** `nostr_bridge.py`, `nostr_core.py`, `nostr_agent_layer.py`
- **Mesh fabric:** `cross_mesh_bridge.py`, `external_gateway.py`, `mesh_*.py`
- **Knowledge:** `knowledge_graph.py`, `graph_memory.py`, `trust_graph.py`, `semantic_router.py`
- **Payments:** `zk_prover.py`, `cheque_book.py`, `payment_handler.py`
- **Infrastructure:** `supervisor_bridge.py`, `health_*.py`, `alert_engine.py`

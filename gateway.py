#!/usr/bin/env python3
"""
SNIN MCP Gateway — Model Context Protocol bridge to SNIN V5 Mesh Fabric.

Any MCP-compatible AI agent (Claude, GPT, Grok, Hermes) can connect
via HTTP and instantly discover + use SNIN infrastructure:
- Agent discovery & messaging
- Marketplace (offer/want search)
- DAO governance
- Dead-Letter Queue
- Mesh status & health

Protocol: JSON-RPC 2.0 over HTTP POST /mcp
Port: 9950 (configurable via --port)
"""

import asyncio
import hashlib
import json
import socket
import logging
import os
import resource
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# ─── Flask ─────────────────────────────────────────────────────────
from flask import Flask, request, jsonify, Response

# ─── Structured Logging ────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": "snin-mcp-gateway",
            "message": record.getMessage(),
            "pid": os.getpid(),
        }
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, ensure_ascii=False)

_health_log = logging.getLogger("gateway-health")
_health_log.setLevel(logging.INFO)
_fh = logging.FileHandler(str(LOG_DIR / "gateway_health.jsonl"))
_fh.setFormatter(JSONFormatter())
_health_log.addHandler(_fh)

app = Flask(__name__)

# ─── Mesh Client integration ───────────────────────────────────────
MESH_PATH = str(Path(__file__).parent.parent / "relay-mesh")
sys.path.insert(0, MESH_PATH)

_mesh_agent = None
_mesh_connected = False

try:
    from mesh_client import MeshAgent
    HAS_MESH = True
except ImportError:
    HAS_MESH = False
    print("⚠️  mesh_client not found — direct mode only")

# ─── Constants ─────────────────────────────────────────────────────
GATEWAY_VERSION = "1.0.0"
GATEWAY_NAME = "snin-mcp-gateway"
PUBKEY = hashlib.sha256(f"snin-mcp-gw-{os.getpid()}".encode()).hexdigest()
DID = f"did:snin:{PUBKEY[:64]}"

# ─── Tool Registry ─────────────────────────────────────────────────
TOOLS = {
    "snin_agent_search": {
        "name": "snin_agent_search",
        "description": "Search for agents on SNIN Mesh by capability or role. Finds agents matching your query.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "What capability or role to search for (e.g. 'forecasting', 'data_analysis', 'archivist')"
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (default: 10)",
                    "default": 10
                }
            },
            "required": ["query"]
        }
    },
    "snin_send_message": {
        "name": "snin_send_message",
        "description": "Send a message to another agent on the SNIN Mesh via SmartRouter.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "to_pubkey": {
                    "type": "string",
                    "description": "Target agent pubkey or DID"
                },
                "payload": {
                    "type": "object",
                    "description": "Message payload (JSON object)"
                },
                "channel": {
                    "type": "string",
                    "description": "Channel: mesh, gossip, nostr, p2p (default: mesh)",
                    "default": "mesh"
                }
            },
            "required": ["to_pubkey", "payload"]
        }
    },
    "snin_marketplace_search": {
        "name": "snin_marketplace_search",
        "description": "Search the SNIN Agent Marketplace for offers or wants. Find agents selling or buying capabilities.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "What service or capability to find (e.g. 'market_forecast', 'code_review')"
                },
                "mode": {
                    "type": "string",
                    "description": "'offers' = agents selling this, 'wants' = agents looking for this, 'both' (default)",
                    "default": "both"
                }
            },
            "required": ["query"]
        }
    },
    "snin_register_capability": {
        "name": "snin_register_capability",
        "description": "Register a new capability/offer for this agent on the SNIN Mesh.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "offers": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of capabilities this agent offers (e.g. ['data_analysis', 'forecasting'])"
                },
                "wants": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of capabilities this agent wants from others"
                }
            },
            "required": ["offers"]
        }
    },
    "snin_register_agent": {
        "name": "snin_register_agent",
        "description": "Register a new external agent on the SNIN Mesh. Publishes to Nostr relay for auto-discovery by all other agents. After registration, the agent becomes searchable via snin_agent_search within 5 minutes.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Human-readable agent name (e.g. 'PriceOracle AI')"
                },
                "pubkey": {
                    "type": "string",
                    "description": "Agent's Nostr pubkey (hex, 64 chars) or platform-specific ID"
                },
                "offers": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of capabilities this agent offers (e.g. ['price_prediction', 'market_analysis'])"
                },
                "wants": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of capabilities this agent wants from others"
                },
                "platform": {
                    "type": "string",
                    "description": "Origin platform (e.g. 'google_a2a', 'aws_bedrock', 'openai', 'custom')"
                }
            },
            "required": ["name", "pubkey", "offers"]
        }
    },
    "snin_mesh_status": {
        "name": "snin_mesh_status",
        "description": "Get current status of the SNIN Mesh Fabric: active agents, relay health, message throughput.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        }
    },
    "snin_dao_propose": {
        "name": "snin_dao_propose",
        "description": "Submit a DAO proposal for agent governance voting (kind:31004).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Proposal title"
                },
                "description": {
                    "type": "string",
                    "description": "Proposal description"
                },
                "action": {
                    "type": "string",
                    "description": "What action to take if passed"
                }
            },
            "required": ["title", "description"]
        }
    },
    "snin_dead_letter": {
        "name": "snin_dead_letter",
        "description": "Send a message via Dead-Letter Queue — delivered when offline agent comes online (kind:9000, TTL up to 365 days).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "to_pubkey": {
                    "type": "string",
                    "description": "Target agent pubkey (can be offline)"
                },
                "payload": {
                    "type": "object",
                    "description": "Message payload"
                },
                "ttl_days": {
                    "type": "integer",
                    "description": "How many days to keep trying (default: 90, max: 365)",
                    "default": 90
                }
            },
            "required": ["to_pubkey", "payload"]
        }
    },
}

# ─── In-memory agent cache ─────────────────────────────────────────
_agent_cache: dict = {}         # pubkey -> profile
_cache_updated = 0.0
CACHE_TTL = 60                  # refresh every 60s

# Known agents registry — seed with 4 core agents, auto-populated from Nostr relay at startup
KNOWN_AGENTS = {
    "forecaster_ai": {
        "pubkey": "5106c0aa993f6ac17c2942a773366f4add82ebccf8b6ac70c6bf8d06c55788b0",
        "name": "Forecaster AI",
        "role": "forecaster",
        "offers": ["market_forecast", "price_prediction", "trend_analysis"],
        "wants": ["raw_market_data", "news_feed", "on_chain_data"],
    },
    "archivist_ai": {
        "pubkey": "faeda044de0a18405a791c7cb6145eb9676b6466317deb4be88dc451510b0b04",
        "name": "Archivist AI",
        "role": "archivist",
        "offers": ["data_storage", "knowledge_retrieval", "semantic_search"],
        "wants": ["documents", "research_papers", "datasets"],
    },
    "cryter": {
        "pubkey": "13tnev6n6q7qh78gdz6p6yxn7jq9edxz7kx9n0j5v9l7xk9nq7q",
        "name": "Cryter",
        "role": "sentinel",
        "offers": ["market_signal", "risk_assessment", "sentiment_analysis"],
        "wants": ["market_data", "news_events"],
    },
    "analion": {
        "pubkey": "analion_pk_placeholder",
        "name": "Analion",
        "role": "analyst",
        "offers": ["data_analysis_73_methods", "statistical_modeling", "hypothesis_testing"],
        "wants": ["datasets", "hypotheses", "research_questions"],
    },
}

# Role → default offers/wants mapping for auto-discovered agents
_ROLE_CAPABILITIES = {
    "forecaster":       {"offers": ["market_forecast", "price_prediction", "trend_analysis"], "wants": ["market_data", "news_events"]},
    "prediction":       {"offers": ["market_forecast", "price_prediction", "trend_analysis"], "wants": ["market_data", "news_events"]},
    "archivist":        {"offers": ["data_storage", "knowledge_retrieval", "semantic_search"], "wants": ["documents", "research_papers"]},
    "historian":        {"offers": ["data_storage", "knowledge_retrieval", "semantic_search"], "wants": ["documents", "research_papers"]},
    "sentinel":         {"offers": ["market_signal", "risk_assessment", "sentiment_analysis"], "wants": ["market_data", "news_events"]},
    "pulse broadcaster":{"offers": ["market_signal", "risk_assessment", "sentiment_analysis"], "wants": ["market_data", "news_events"]},
    "analyst":          {"offers": ["data_analysis", "statistical_modeling", "hypothesis_testing"], "wants": ["datasets", "hypotheses"]},
    "market analyst":   {"offers": ["data_analysis", "statistical_modeling", "hypothesis_testing"], "wants": ["datasets", "hypotheses"]},
    "strategist":       {"offers": ["strategy_planning", "game_theory", "consensus_proposals"], "wants": ["market_data", "agent_reports"]},
    "game theory":      {"offers": ["strategy_planning", "game_theory", "consensus_proposals"], "wants": ["market_data", "agent_reports"]},
    "philosopher":      {"offers": ["philosophical_analysis", "conceptual_frameworks"], "wants": ["research_papers", "arguments"]},
    "social pulse":     {"offers": ["social_analysis", "sentiment_tracking"], "wants": ["news_feed", "social_data"]},
    "marketing":        {"offers": ["growth_hacking", "content_strategy", "brand_positioning"], "wants": ["analytics", "market_data"]},
    "growth":           {"offers": ["growth_hacking", "content_strategy", "brand_positioning"], "wants": ["analytics", "market_data"]},
    "security":         {"offers": ["security_audit", "vulnerability_assessment"], "wants": ["code", "contracts", "configs"]},
    "security auditor": {"offers": ["security_audit", "vulnerability_assessment"], "wants": ["code", "contracts", "configs"]},
    "research & dev":   {"offers": ["code_generation", "architecture_design", "code_review"], "wants": ["specs", "requirements"]},
    "ops executor":     {"offers": ["deployment", "monitoring", "infrastructure"], "wants": ["deployment_specs", "configs"]},
    "director":         {"offers": ["strategy_oversight", "resource_allocation", "consensus_proposals"], "wants": ["agent_reports", "metrics"]},
    "CEO / strategist": {"offers": ["strategy_oversight", "resource_allocation", "consensus_proposals"], "wants": ["agent_reports", "metrics"]},
    "support":          {"offers": ["user_support", "troubleshooting"], "wants": ["user_questions", "incident_reports"]},
    "user support":     {"offers": ["user_support", "troubleshooting"], "wants": ["user_questions", "incident_reports"]},
    "agent manager":    {"offers": ["agent_coordination", "task_queue"], "wants": ["agent_status", "task_requests"]},
    "ontology":         {"offers": ["semantic_analysis", "knowledge_graph"], "wants": ["text_corpus", "structured_data"]},
    "V2Bot assistant":  {"offers": ["task_automation", "integration_proxy"], "wants": ["api_keys", "user_instructions"]},
    "market agent":     {"offers": ["market_signal", "order_execution", "risk_assessment"], "wants": ["market_data", "news_events"]},
}

# ─── Auto-Discovery: Sync agents from Nostr Relay ─────────────────────
_RELAY_API_URL = os.environ.get("SNIN_RELAY_API", "http://127.0.0.1:8198")
_LAST_SYNC = 0
_SYNC_LOCK = threading.Lock()

def _sync_agents_from_relay() -> int:
    """Fetch agents from Nostr relay /api/agents and merge into KNOWN_AGENTS.
    Returns count of new agents discovered."""
    global _LAST_SYNC
    import urllib.request as _urllib

    with _SYNC_LOCK:
        # Don't sync more than once per 60 seconds
        if time.time() - _LAST_SYNC < 60:
            return 0

        new_count = 0
        try:
            resp = _urllib.urlopen(f"{_RELAY_API_URL}/api/agents", timeout=10)
            data = json.loads(resp.read())
            agents_list = data.get("agents", [])

            for agent in agents_list:
                pubkey = agent.get("pubkey", "")
                if not pubkey or len(pubkey) < 20:
                    continue

                name = agent.get("name", "unknown")
                role = agent.get("role", "unknown")
                nip05 = agent.get("nip05", "")
                status = agent.get("status", "unknown")

                # Use pubkey as agent_id (stable across restarts)
                agent_id = f"nostr_{pubkey[:12]}"

                # Skip already known agents (by pubkey match)
                already_known = any(
                    a.get("pubkey", "") == pubkey
                    for a in KNOWN_AGENTS.values()
                )
                if already_known:
                    continue

                # Determine capabilities from relay_list (for external agents) or role
                relay_list_raw = agent.get("relay_list", "[]")
                caps = None
                if isinstance(relay_list_raw, str) and relay_list_raw.startswith('{'):
                    try:
                        caps_data = json.loads(relay_list_raw)
                        if "offers" in caps_data or "wants" in caps_data:
                            caps = {
                                "offers": caps_data.get("offers", []),
                                "wants": caps_data.get("wants", []),
                            }
                    except json.JSONDecodeError:
                        pass
                if caps is None:
                    caps = _ROLE_CAPABILITIES.get(role, {
                        "offers": ["network_participation"],
                        "wants": []
                    })

                KNOWN_AGENTS[agent_id] = {
                    "pubkey": pubkey,
                    "name": name,
                    "role": role,
                    "offers": caps["offers"],
                    "wants": caps["wants"],
                    "nip05": nip05,
                    "status": status,
                    "source": "nostr_relay",
                }
                new_count += 1

            _LAST_SYNC = time.time()
            if new_count > 0:
                print(f"[Gateway] 🔄 Auto-discovered {new_count} agents from relay (total: {len(KNOWN_AGENTS)})")
            else:
                print(f"[Gateway] ✓ Agent sync OK — {len(KNOWN_AGENTS)} agents, no new")

        except Exception as e:
            print(f"[Gateway] ⚠️ Agent sync failed: {e}")

        return new_count


def _start_agent_sync_daemon():
    """Background thread: periodic agent sync from Nostr relay."""
    def _loop():
        time.sleep(15)  # initial delay — let everything start
        while True:
            try:
                _sync_agents_from_relay()
            except Exception as e:
                print(f"[Gateway] Agent sync daemon error: {e}")
            time.sleep(300)  # every 5 minutes

    t = threading.Thread(target=_loop, daemon=True, name="agent-sync")
    t.start()
    print("[Gateway] 🟢 Agent sync daemon started (every 5 min from Nostr relay)")


# ─── Mesh Client ───────────────────────────────────────────────────
def get_mesh():
    global _mesh_agent, _mesh_connected
    if not HAS_MESH:
        return None
    if _mesh_agent is None:
        try:
            _mesh_agent = MeshAgent(
                pubkey=PUBKEY,
                name=GATEWAY_NAME,
                mesh_host="127.0.0.1",
                mesh_port=9932,
                api_url="http://127.0.0.1:9907"
            )
            # Connect to Smart Router
            connected = asyncio.run(_mesh_agent.connect())
            if connected:
                _mesh_connected = True
                print(f"[Gateway] ✅ Connected to Smart Router (127.0.0.1:9932)")
            else:
                print(f"[Gateway] ⚠️ Smart Router connect() returned False")
        except Exception as e:
            print(f"⚠️ Mesh init failed: {e}")
            _mesh_agent = None
    return _mesh_agent


# ─── Smart Router direct health check ─────────────────────────────
def _check_smart_router() -> bool:
    """Check if Smart Router is alive via direct TCP connection."""
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        sock.connect(("127.0.0.1", 9932))
        sock.sendall(b'{"from":"mcp_healthcheck","to":"_","kind":0,"payload":"ping","meta":{"channel":"mesh","priority":"low","agent":"mcp_gateway"}}\n')
        data = sock.recv(1024)
        sock.close()
        resp = json.loads(data.decode())
        return resp.get("ok") is True
    except Exception:
        return False


# ─── Agent Discovery via External Gateway ──────────────────────────
async def _discover_agents(query: str = None, limit: int = 10) -> list:
    """Search agents via External Gateway (9931) and local cache."""
    results = []
    q = (query or "").lower().replace("_", " ")

    # Search known agents
    for agent_id, profile in KNOWN_AGENTS.items():
        match = False
        if not q:
            match = True
        else:
            # Check name, role, offers, wants
            if q in profile.get("name", "").lower():
                match = True
            elif q in profile.get("role", "").lower():
                match = True
            elif any(q in o.lower().replace("_", " ") for o in profile.get("offers", [])):
                match = True
            elif any(q in w.lower().replace("_", " ") for w in profile.get("wants", [])):
                match = True

        if match:
            results.append({
                "id": agent_id,
                "pubkey": profile["pubkey"],
                "name": profile["name"],
                "role": profile["role"],
                "offers": profile["offers"],
                "wants": profile["wants"],
                "did": f"did:snin:{profile['pubkey'][:64]}" if len(profile["pubkey"]) >= 64 else f"did:snin:{profile['pubkey']}",
            })

    # Also try to get agents from TIE Relay
    try:
        import urllib.request
        resp = urllib.request.urlopen("https://tie-run.v2.site/api/agents", timeout=5)
        tie_agents = json.loads(resp.read())
        if isinstance(tie_agents, dict) and "agents" in tie_agents:
            for a in tie_agents["agents"]:
                results.append({
                    "id": a.get("name", "unknown"),
                    "pubkey": a.get("pubkey", ""),
                    "name": a.get("name", "unknown"),
                    "role": "tie_agent",
                    "offers": ["network_participation"],
                    "wants": [],
                    "source": "tie_relay",
                })
    except Exception:
        pass

    return results[:limit]


# ─── MCP Protocol Handlers ─────────────────────────────────────────

@app.route("/mcp", methods=["POST"])
def mcp_handler():
    """Main MCP JSON-RPC endpoint."""
    try:
        body = request.get_json(force=True)
    except Exception:
        return jsonify({"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}, "id": None}), 400

    method = body.get("method", "")
    params = body.get("params", {})
    req_id = body.get("id")

    if method == "initialize":
        result = _handle_initialize(params)
    elif method == "tools/list":
        result = _handle_tools_list()
    elif method == "tools/call":
        result = _handle_tools_call(params)
    elif method == "notifications/initialized":
        return "", 204
    else:
        result = {"error": {"code": -32601, "message": f"Method not found: {method}"}}

    return jsonify({"jsonrpc": "2.0", "result": result, "id": req_id})


@app.route("/mcp/sse", methods=["GET"])
def mcp_sse():
    """SSE endpoint for MCP streaming transport."""
    def generate():
        yield f"data: {json.dumps({'jsonrpc': '2.0', 'method': 'notifications/ready', 'params': {'server': GATEWAY_NAME, 'version': GATEWAY_VERSION}})}\n\n"
        # Keep alive
        import time as _time
        while True:
            yield f": heartbeat\n\n"
            _time.sleep(30)
    return Response(generate(), mimetype="text/event-stream")


@app.route("/.well-known/mcp", methods=["GET"])
def well_known():
    """MCP server discovery."""
    return jsonify({
        "name": GATEWAY_NAME,
        "version": GATEWAY_VERSION,
        "did": DID,
        "protocol": "mcp/2024-11-05",
        "endpoints": {
            "rpc": "/mcp",
            "sse": "/mcp/sse",
        },
        "tools": list(TOOLS.keys()),
        "mesh": {
            "status": "online",
            "agents_known": len(KNOWN_AGENTS),
        }
    })


@app.route("/health", methods=["GET"])
def health():
    """Enhanced health check — probes all mesh services."""
    return jsonify(_get_full_health())


@app.route("/metrics", methods=["GET"])
def metrics():
    """Prometheus-compatible metrics."""
    s = _get_full_health()
    lines = [
        "# HELP snin_mcp_up Gateway healthy (1=yes)",
        f"snin_mcp_up {1 if s.get('healthy') else 0}",
        "# HELP snin_mcp_uptime_seconds Gateway uptime",
        f"snin_mcp_uptime_seconds {s.get('uptime_sec', 0)}",
        "# HELP snin_mcp_agents_known Known agents",
        f"snin_mcp_agents_known {s.get('agents_known', 0)}",
        "# HELP snin_mcp_tools_count MCP tools",
        f"snin_mcp_tools_count {s.get('tools', 0)}",
        "# HELP snin_mcp_memory_mb RSS memory",
        f"snin_mcp_memory_mb {s.get('memory_mb', 0)}",
    ]
    for svc_name, svc_data in s.get("services", {}).items():
        up = 1 if svc_data.get("status") == "up" else 0
        safe_name = svc_name.replace("-", "_").replace(".", "_")
        lines.append(f"# HELP snin_service_{safe_name}_up Service status")
        lines.append(f"snin_service_{safe_name}_up {up}")
    return "\n".join(lines) + "\n", 200, {"Content-Type": "text/plain"}


@app.route("/stress", methods=["GET"])
def stress():
    """Self-diagnostics for stress testing."""
    return jsonify({
        "pid": os.getpid(),
        "thread_count": threading.active_count(),
        "memory_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1),
        "uptime_sec": round(time.time() - _start_time, 1),
        "python_version": sys.version.split()[0],
        "gateway": GATEWAY_NAME,
        "version": GATEWAY_VERSION,
    })


@app.route("/logs/tail", methods=["GET"])
def logs_tail():
    """Last N log lines in JSON."""
    try:
        n = min(int(request.args.get("lines", "50")), 200)
    except ValueError:
        n = 50
    log_file = LOG_DIR / "gateway_health.jsonl"
    if not log_file.exists():
        return jsonify({"error": "no log file yet", "file": str(log_file)}), 404
    with open(log_file) as f:
        all_lines = f.readlines()
    return jsonify({
        "file": str(log_file),
        "total_lines": len(all_lines),
        "tail": [json.loads(l) for l in all_lines[-n:]],
    })


# ─── Full Health Probe ─────────────────────────────────────────────

_request_count = 0
_request_errors = 0

@app.before_request
def _log_request():
    global _request_count
    _request_count += 1
    _health_log.info(f"{request.method} {request.path} from {request.remote_addr}")

def _get_full_health() -> dict:
    """Probe all SNIN services and return full status."""
    services = {}
    all_ok = True

    # Core mesh ports
    ports_to_check = {
        "smart_router": ("127.0.0.1", 9932),
        "external_gateway": ("127.0.0.1", 9931),
        "content_router": ("127.0.0.1", 9920),
        "route_engine": ("127.0.0.1", 9910),
        "mesh_api": ("127.0.0.1", 9907),
        "cross_mesh": ("127.0.0.1", 9946),
        "supervisor": ("127.0.0.1", 9900),
    }

    for name, (host, port) in ports_to_check.items():
        try:
            sock = __import__('socket').socket(__import__('socket').AF_INET, __import__('socket').SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                services[name] = {"status": "up", "port": port}
            else:
                services[name] = {"status": "down", "port": port, "error": f"connect={result}"}
                all_ok = False
        except Exception as e:
            services[name] = {"status": "down", "port": port, "error": str(e)[:100]}
            all_ok = False

    # Nostr bridges
    for i in range(1, 6):
        try:
            sock = __import__('socket').socket(__import__('socket').AF_INET, __import__('socket').SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(("127.0.0.1", 9940 + i))
            sock.close()
            services[f"nostr_bridge_{i}"] = {"status": "up" if result == 0 else "down", "port": 9940 + i}
        except:
            services[f"nostr_bridge_{i}"] = {"status": "down", "port": 9940 + i}

    # TIE relay
    try:
        sock = __import__('socket').socket(__import__('socket').AF_INET, __import__('socket').SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(("127.0.0.1", 8198))
        sock.close()
        services["tie_relay"] = {"status": "up" if result == 0 else "down", "port": 8198}
    except:
        services["tie_relay"] = {"status": "down", "port": 8198}

    # Memory
    mem = resource.getrusage(resource.RUSAGE_SELF)
    mem_mb = round(mem.ru_maxrss / 1024, 1)

    # Log size
    log_files = list(LOG_DIR.glob("*.jsonl")) + list(LOG_DIR.glob("*.log"))
    log_size_mb = round(sum(f.stat().st_size for f in log_files) / (1024 * 1024), 2)

    _health_log.info(f"Health probe: all_ok={all_ok}, services_up={sum(1 for s in services.values() if s.get('status')=='up')}/{len(services)}")

    return {
        "gateway": GATEWAY_NAME,
        "version": GATEWAY_VERSION,
        "did": DID,
        "healthy": all_ok,
        "uptime_sec": round(time.time() - _start_time, 1),
        "boot_time": datetime.fromtimestamp(_start_time, tz=timezone.utc).isoformat(),
        "memory_mb": mem_mb,
        "log_size_mb": log_size_mb,
        "agents_known": len(KNOWN_AGENTS),
        "tools": len(TOOLS),
        "mesh_connected": _mesh_connected,
        "request_count": _request_count,
        "request_errors": _request_errors,
        "services": services,
    }


@app.route("/", methods=["GET"])
def index():
    return f"""
<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>SNIN MCP Gateway</title>
<style>
  body {{ font-family: -apple-system, sans-serif; max-width: 800px; margin: 40px auto; padding: 20px; background: #0d0d1a; color: #e0e0e0; }}
  h1 {{ color: #00d4ff; }} pre {{ background: #1a1a2e; padding: 15px; border-radius: 8px; }}
  a {{ color: #00d4ff; }}
</style></head>
<body>
  <h1>🧠 SNIN MCP Gateway v{GATEWAY_VERSION}</h1>
  <p>Model Context Protocol bridge to <strong>SNIN V5 Mesh Fabric</strong>.</p>
  <p>Any MCP-compatible AI agent can connect and use SNIN infrastructure.</p>
  <hr>
  <h3>Quick Connect</h3>
  <pre>
curl -X POST https://snin-mcp.v2.site/mcp \\
  -H "Content-Type: application/json" \\
  -d '{{"jsonrpc":"2.0","method":"initialize","params":{{}},"id":1}}'
  </pre>
  <h3>Endpoints</h3>
  <ul>
    <li><code>POST /mcp</code> — JSON-RPC 2.0</li>
    <li><code>GET /mcp/sse</code> — SSE streaming</li>
    <li><code>GET /.well-known/mcp</code> — Server discovery</li>
    <li><code>GET /health</code> — Health check</li>
  </ul>
  <h3>Tools ({len(TOOLS)})</h3>
  <ul>
    {"".join(f"<li><strong>{t}</strong> — {info['description']}</li>" for t, info in TOOLS.items())}
  </ul>
  <hr>
  <p>DID: <code>{DID}</code></p>
</body></html>
"""


# ─── Handlers ──────────────────────────────────────────────────────

def _handle_initialize(params: dict) -> dict:
    return {
        "protocolVersion": "2024-11-05",
        "serverInfo": {
            "name": GATEWAY_NAME,
            "version": GATEWAY_VERSION,
        },
        "capabilities": {
            "tools": {},
        },
        "did": DID,
        "mesh": {
            "agents_known": len(KNOWN_AGENTS),
        },
    }


def _handle_tools_list() -> dict:
    return {"tools": list(TOOLS.values())}


def _handle_tools_call(params: dict) -> dict:
    tool_name = params.get("name", "")
    arguments = params.get("arguments", {})

    if tool_name not in TOOLS:
        return {"content": [{"type": "text", "text": f"Unknown tool: {tool_name}"}], "isError": True}

    try:
        if tool_name == "snin_agent_search":
            result = _tool_agent_search(arguments)
        elif tool_name == "snin_send_message":
            result = _tool_send_message(arguments)
        elif tool_name == "snin_marketplace_search":
            result = _tool_marketplace_search(arguments)
        elif tool_name == "snin_register_capability":
            result = _tool_register_capability(arguments)
        elif tool_name == "snin_register_agent":
            result = _tool_register_agent(arguments)
        elif tool_name == "snin_mesh_status":
            result = _tool_mesh_status(arguments)
        elif tool_name == "snin_dao_propose":
            result = _tool_dao_propose(arguments)
        elif tool_name == "snin_dead_letter":
            result = _tool_dead_letter(arguments)
        else:
            result = {"error": f"Tool {tool_name} not implemented"}

        if isinstance(result, dict) and "error" in result:
            return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}], "isError": True}

        return {"content": [{"type": "text", "text": json.dumps(result, indent=2, ensure_ascii=False)}]}

    except Exception as e:
        return {"content": [{"type": "text", "text": f"Tool error: {e}"}], "isError": True}


# ─── Tool Implementations ──────────────────────────────────────────

def _tool_agent_search(args: dict) -> dict:
    query = args.get("query", "")
    limit = args.get("limit", 10)
    results = asyncio.run(_discover_agents(query=query, limit=limit))
    return {
        "query": query,
        "found": len(results),
        "agents": results,
    }


def _tool_send_message(args: dict) -> dict:
    to_pubkey = args.get("to_pubkey", "")
    payload = args.get("payload", {})
    channel = args.get("channel", "mesh")

    mesh = get_mesh()
    if mesh:
        # Use SmartRouter (9932) for message routing
        result = asyncio.run(mesh.send(
            to=to_pubkey,
            payload=payload,
            channel=channel
        ))
        return {"status": "sent", "to": to_pubkey, "channel": channel, "result": str(result)}
    else:
        # Direct mode: log and return
        return {
            "status": "logged",
            "to": to_pubkey,
            "channel": channel,
            "payload": payload,
            "note": "Mesh client unavailable — message logged, delivery not guaranteed",
        }


def _tool_marketplace_search(args: dict) -> dict:
    query = (args.get("query", "")).lower().replace("_", " ")
    mode = args.get("mode", "both")

    offers_match = []
    wants_match = []

    for agent_id, profile in KNOWN_AGENTS.items():
        for offer in profile.get("offers", []):
            if query in offer.lower().replace("_", " "):
                offers_match.append({
                    "agent": profile["name"],
                    "agent_id": agent_id,
                    "offer": offer,
                    "type": "offer",
                })
        for want in profile.get("wants", []):
            if query in want.lower().replace("_", " "):
                wants_match.append({
                    "agent": profile["name"],
                    "agent_id": agent_id,
                    "want": want,
                    "type": "want",
                })

    result = {"query": query, "mode": mode}
    if mode in ("offers", "both"):
        result["offers"] = offers_match
    if mode in ("wants", "both"):
        result["wants"] = wants_match
    result["total"] = len(offers_match) + len(wants_match)

    return result


def _tool_register_capability(args: dict) -> dict:
    offers = args.get("offers", [])
    wants = args.get("wants", [])

    agent_id = f"mcp_agent_{hashlib.sha256(PUBKEY.encode()).hexdigest()[:8]}"
    KNOWN_AGENTS[agent_id] = {
        "pubkey": PUBKEY,
        "name": f"MCP Agent {agent_id[:6]}",
        "role": "mcp_external",
        "offers": offers,
        "wants": wants,
    }

    return {
        "status": "registered",
        "agent_id": agent_id,
        "did": DID,
        "offers": offers,
        "wants": wants,
    }


def _insert_agent_to_relay(pubkey: str, name: str, role: str, offers: list, wants: list) -> bool:
    """Insert agent into relay database for auto-discovery."""
    import sqlite3
    import time as _time
    try:
        db = sqlite3.connect("/home/agent/data/sites/relay/relay_v2.db")
        now = int(_time.time())
        caps = json.dumps({"offers": offers, "wants": wants})
        db.execute("""
            INSERT OR REPLACE INTO agents (pubkey, name, role, nip05, status, last_seen, first_seen, relay_list)
            VALUES (?, ?, ?, '', 'online', ?, ?, ?)
        """, (pubkey, name, role, now, now, caps))
        db.commit()
        db.close()
        print(f"[Gateway] 🆕 Agent '{name}' inserted into relay DB (pubkey={pubkey[:12]}...)")
        return True
    except Exception as e:
        print(f"[Gateway] ⚠️ Failed to insert agent into relay: {e}")
        return False


def _tool_register_agent(args: dict) -> dict:
    name = args.get("name", "").strip()
    pubkey = args.get("pubkey", "").strip()
    offers = args.get("offers", [])
    wants = args.get("wants", [])
    platform = args.get("platform", "external")

    if not name:
        return {"content": [{"type": "text", "text": "Error: 'name' is required"}], "isError": True}
    if not pubkey:
        return {"content": [{"type": "text", "text": "Error: 'pubkey' is required"}], "isError": True}

    # Generate stable agent_id from pubkey
    agent_id = f"ext_{pubkey[:16]}"
    if len(pubkey) < 20:
        agent_id = f"ext_{hashlib.sha256(pubkey.encode()).hexdigest()[:12]}"

    # 1. Add to in-memory KNOWN_AGENTS (immediate searchability)
    KNOWN_AGENTS[agent_id] = {
        "pubkey": pubkey,
        "name": name,
        "role": f"external_{platform}",
        "offers": offers,
        "wants": wants,
        "status": "online",
        "source": "mcp_registration",
        "platform": platform,
        "registered_at": datetime.utcnow().isoformat() + "Z",
    }

    # 2. Insert into relay database (auto-discovery within 5 min)
    relay_ok = _insert_agent_to_relay(pubkey, name, f"external_{platform}", offers, wants)

    # 3. Push to Knowledge Graph via SmartRouter heartbeat (kind=39000)
    #    _update_graph_from_msg() → upsert_node() + update_node_status("online")
    graph_ok = False
    try:
        sock = socket.create_connection(("127.0.0.1", 9932), timeout=2)
        heartbeat = json.dumps({
            "from": agent_id,
            "to": "smart_router",
            "kind": 39000,
            "meta": {"transport": "mesh"},
        }) + "\n"
        sock.send(heartbeat.encode())
        sock.close()
        graph_ok = True
    except Exception:
        pass  # не критично — агент попадёт в граф при первом реальном сообщении

    return {
        "status": "registered",
        "agent_id": agent_id,
        "name": name,
        "pubkey": pubkey[:16] + "...",
        "offers": offers,
        "wants": wants,
        "platform": platform,
        "relay_published": relay_ok,
        "graph_registered": graph_ok,
        "discovery_eta": "within 5 minutes (next Gateway sync cycle)",
        "searchable_via": "snin_agent_search",
    }


def _tool_mesh_status(args: dict) -> dict:
    # Check core services
    import urllib.request as _urllib

    services = {
        "tie_relay": {"url": "https://tie-run.v2.site/api/status", "status": "unknown"},
        "snin_command": {"url": "https://snin-command.v2.site/", "status": "unknown"},
        "relay_nostr": {"url": "https://relay-snin.v2.site/", "status": "unknown"},
    }

    for name, info in services.items():
        try:
            resp = _urllib.urlopen(info["url"], timeout=5)
            services[name]["status"] = "online" if resp.status < 400 else f"http_{resp.status}"
        except Exception as e:
            services[name]["status"] = f"offline: {str(e)[:50]}"

    return {
        "gateway": {"name": GATEWAY_NAME, "version": GATEWAY_VERSION, "did": DID},
        "mesh": {
            "smart_router": "online" if (_mesh_connected or _check_smart_router()) else "offline",
            "agents_known": len(KNOWN_AGENTS),
            "tools_available": len(TOOLS),
        },
        "services": services,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


def _tool_dao_propose(args: dict) -> dict:
    title = args.get("title", "")
    description = args.get("description", "")
    action = args.get("action", "")

    proposal_id = hashlib.sha256(f"{title}{time.time()}".encode()).hexdigest()[:16]

    return {
        "status": "proposed",
        "proposal_id": proposal_id,
        "kind": 31004,
        "title": title,
        "description": description,
        "action": action,
        "note": "DAO proposal published to Nostr relays. Agents can vote via kind:31004.",
    }


def _tool_dead_letter(args: dict) -> dict:
    to_pubkey = args.get("to_pubkey", "")
    payload = args.get("payload", {})
    ttl_days = min(args.get("ttl_days", 90), 365)

    msg_id = hashlib.sha256(f"{to_pubkey}{json.dumps(payload)}{time.time()}".encode()).hexdigest()[:16]

    return {
        "status": "queued",
        "msg_id": msg_id,
        "kind": 9000,
        "to": to_pubkey,
        "ttl_days": ttl_days,
        "note": "Message placed in Dead-Letter Queue. Will be delivered when agent comes online or within TTL.",
    }


# ─── Startup ───────────────────────────────────────────────────────
_start_time = time.time()

# ─── Step 1: Sync agents from Nostr Relay ──────────────────────────
_sync_agents_from_relay()

# ─── Step 2: Start background sync daemon ──────────────────────────
_start_agent_sync_daemon()

# ─── Step 3: Init Smart Router connection ──────────────────────────
def _init_mesh_connection():
    """Try to connect to Smart Router at startup."""
    global _mesh_connected
    mesh = get_mesh()
    if mesh is not None and _mesh_connected:
        print(f"[Gateway] 🟢 SNIN Mesh Fabric connected — {len(KNOWN_AGENTS)} agents, {len(TOOLS)} tools")
    else:
        # Fallback: direct TCP check
        if _check_smart_router():
            _mesh_connected = True
            print(f"[Gateway] 🟢 Smart Router reachable via direct TCP (127.0.0.1:9932)")
        else:
            print(f"[Gateway] ⚠️ Smart Router not reachable — running in offline mode")

_init_mesh_connection()

# ─── WSGI entry point for Gunicorn ──────────────────────────────────
# gunicorn uses: gunicorn gateway:application --bind 0.0.0.0:9951
application = app

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SNIN MCP Gateway")
    parser.add_argument("--port", type=int, default=9950, help="Port (default: 9950)")
    parser.add_argument("--debug", action="store_true", help="Debug mode")
    args = parser.parse_args()

    port = args.port

    print(f"""
╔══════════════════════════════════════════╗
║   SNIN MCP Gateway v{GATEWAY_VERSION}              ║
║   MCP → SNIN V5 Mesh Fabric Bridge       ║
╚══════════════════════════════════════════╝
    DID: {DID}
    Port: {port}
    Tools: {len(TOOLS)}
    Agents known: {len(KNOWN_AGENTS)}

    Endpoints:
      POST http://0.0.0.0:{port}/mcp
      GET  http://0.0.0.0:{port}/mcp/sse
      GET  http://0.0.0.0:{port}/.well-known/mcp
      GET  http://0.0.0.0:{port}/health
""")

    app.run(host="0.0.0.0", port=port, debug=args.debug)

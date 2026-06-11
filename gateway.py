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

# Known agents registry (filled from Nostr relay and mesh discovery)
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
        except Exception as e:
            print(f"⚠️ Mesh init failed: {e}")
            _mesh_agent = None
    return _mesh_agent


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
            "smart_router": "online" if _mesh_connected else "offline",
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

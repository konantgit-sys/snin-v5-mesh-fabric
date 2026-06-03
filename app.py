"""SNIN Mesh HTTP API — NIP-42 AUTH + WebSocket + Agents/Devices.

Endpoints:
  GET  /health           — cluster + agents status
  GET  /mesh/stats       — mesh performance
  GET  /agents           — list registered agents
  POST /agents/register  — register an agent  
  POST /agents/<pk>/ping — agent heartbeat
  POST /mesh/send        — send message via SmartRouter
  GET  /devices          — list IoT devices
  POST /devices/register — register a device
  WS   /ws               — NIP-42 WebSocket (AUTH required for some ops)
"""
import json, orjson, os, sys, time
from functools import lru_cache
from flask import Flask, jsonify, request
from flask_sock import Sock
from pathlib import Path

# NIP-42 AUTH module
sys.path.insert(0, str(Path.home() / "data" / "sites" / "relay-mesh"))
from nip42_auth import (
    generate_challenge, validate_challenge, create_session, validate_session,
    is_valid_auth_event, get_auth_pubkey,
    format_auth_message, format_auth_ok, format_notice, parse_message,
    RATE_LIMIT_ANON, RATE_LIMIT_AUTH, cleanup_sessions
)

# NIP-65 Discovery
from nip65_discovery import get_relay_info

# Middleware (Phase 4) — unified rate limit + circuit breaker
from middleware import cb_check, cb_record_error, cb_reset, cb_status, check_rate_limit_simple

# Phase 20 — Payment Handler (kind:30000)
from payment_handler import (
    init_payments, validate_payment_event, get_stats as get_pay_stats
)

# Phase 21 — Cheque Book (25M tx/s)
import requests as _cheque_requests
CHEQUE_API = "http://127.0.0.1:9916"

# Phase 21.1 — Payment Integrator (auto-route cheque/optimistic + accounting DB)
import sys as _pay_sys
_pay_sys.path.insert(0, "/home/agent/data/sites/relay-mesh")
from pay_integrator import route_payment, get_accounting_stats, record_payment, update_payment_status

# Phase 22 — ZK Proof (Merkle-based, in-process, 0 демонов)
from zk_prover import (
    init_ledger, credit_agent, prove_balance, verify_zk_proof,
    get_balance, get_ledger_stats
)

app = Flask(__name__)
sock = Sock(app)

BASE = Path.home() / "data" / "sites" / "relay-mesh"
AGENTS_FILE = str(BASE / "agents.json")
DEVICES_FILE = str(BASE / "devices.json")

# ─── Agent Registry ───
agents: dict = {}

def _load_agents():
    global agents
    try:
        with open(AGENTS_FILE) as f:
            agents = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        agents = {}

def _save_agents():
    with open(AGENTS_FILE, "w") as f:
        json.dump(agents, f, indent=2, ensure_ascii=False)

_load_agents()

# ─── Device Registry ───
devices: dict = {}

def _load_devices():
    global devices
    try:
        with open(DEVICES_FILE) as f:
            devices = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        devices = {}

def _save_devices():
    with open(DEVICES_FILE, "w") as f:
        json.dump(devices, f, indent=2, ensure_ascii=False)

_load_devices()

# ─── Cluster health (cached 5s) ───
@lru_cache(maxsize=1)
def _get_health():
    try:
        import urllib.request
        req = urllib.request.Request("http://127.0.0.1:9933/health")
        resp = urllib.request.urlopen(req, timeout=2)
        return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}

# ─── Routes ───
@app.route("/")
def root():
    return jsonify({
        "service": "SNIN Mesh API",
        "version": "v2 (NIP-42)",
        "endpoints": [
            "/health", "/agents", "/devices",
            "/mesh/stats", "/mesh/send",
            "/ws (WebSocket NIP-42 AUTH)", "/nip65 (Relay Discovery)", "/network/snapshot", "/agents/capabilities", "/system/degradation"
        ],
        "agents": len(agents),
        "devices": len(devices),
        "sessions": len(nip42_auth.sessions) if 'nip42_auth' in dir() else 0,
    })

@app.route("/health")
def health():
    h = _get_health()
    _get_health.cache_clear()
    h["agents"] = len(agents)
    h["devices"] = len(devices)
    h["service"] = "snin-mesh-api"
    h["auth"] = {
        "sessions_active": len(__import__("nip42_auth").sessions),
        "challenges_pending": len(__import__("nip42_auth").challenges),
    }
    return jsonify(h)

@app.route("/mesh/stats")
def mesh_stats():
    h = _get_health()
    return jsonify({
        "workers": h.get("pools", {}).get("workers_alive", 0),
        "version": h.get("version", "?"),
        "mode": h.get("mode", "?"),
        "status": h.get("status", "error"),
        "forwarded": h.get("stats", {}).get("forwarded", 0),
        "agents": len(agents),
        "devices": len(devices),
    })

@app.route("/agents")
def list_agents():
    pubkey = get_auth_pubkey(request.headers)
    # Authenticated users see all agents; anonymous see count only
    if pubkey:
        return jsonify({"count": len(agents), "agents": agents})
    return jsonify({"count": len(agents), "auth_required": "send Authorization: NIP-42 <token>"})


@app.route("/agents/<pubkey>/ping", methods=["POST"])
def agent_ping(pubkey):
    if pubkey in agents:
        agents[pubkey]["last_seen"] = time.time()
        _save_agents()
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "unknown"}), 404


@app.route("/agents/gossip", methods=["POST"])
def register_gossip():
    """Зарегистрировать gossip-адрес агента (IP:port для p2p)."""
    data = request.get_json(force=True)
    pubkey = data.get("pubkey", "")
    if not pubkey:
        return jsonify({"ok": False, "error": "pubkey required"}), 400
    
    name = data.get("name", pubkey[:16])
    gossip_host = data.get("gossip_host", "127.0.0.1")
    gossip_port = int(data.get("gossip_port", 0))
    
    if pubkey not in agents:
        agents[pubkey] = {"name": name, "pubkey": pubkey, "first_seen": time.time()}
    
    agents[pubkey]["name"] = name
    agents[pubkey]["gossip_host"] = gossip_host
    agents[pubkey]["gossip_port"] = gossip_port
    agents[pubkey]["last_seen"] = time.time()
    _save_agents()
    
    print(f"[API] ✅ Gossip registered: {pubkey[:16]} → {gossip_host}:{gossip_port}")
    return jsonify({"ok": True, "pubkey": pubkey, "gossip": f"{gossip_host}:{gossip_port}"})


@app.route("/agents/gossip/peers")
def list_gossip_peers():
    """Список всех агентов с gossip-адресами."""
    peers = []
    now = time.time()
    for pk, info in agents.items():
        if info.get("gossip_port", 0) > 0:
            peers.append({
                "pubkey": pk,
                "name": info.get("name", pk[:16]),
                "gossip_host": info.get("gossip_host", "127.0.0.1"),
                "gossip_port": info.get("gossip_port", 0),
                "last_seen": info.get("last_seen", 0),
                "alive": (now - info.get("last_seen", 0)) < 30,
            })
    return jsonify({"count": len(peers), "peers": peers})

@app.route("/devices")
def list_devices():
    return jsonify({"count": len(devices), "devices": devices})

@app.route("/devices/register", methods=["POST"])
def register_device():
    data = request.get_json(force=True)
    device_id = data.get("device_id", "")
    if not device_id:
        return jsonify({"ok": False, "error": "device_id required"}), 400
    devices[device_id] = {
        "device_type": data.get("device_type", "custom"),
        "name": data.get("name", device_id),
        "protocol": data.get("protocol", "tcp"),
        "address": data.get("address", ""),
        "port": data.get("port", 0),
        "latency_ms": data.get("latency_ms", -1),
        "tier": data.get("tier", 4),
        "config": data.get("config", {}),
        "pubkey": data.get("pubkey", f"device_{device_id}"),
        "status": data.get("status", "registered"),
        "registered_at": time.time(),
        "last_seen": time.time(),
    }
    _save_devices()
    return jsonify({"ok": True, "device": devices[device_id]})

@app.route("/mesh/send", methods=["POST"])
def mesh_send():
    """Forward a message to SmartRouter. Authenticated clients get higher priority."""
    data = request.get_json(force=True)
    pubkey = get_auth_pubkey(request.headers)
    
    # Rate limit (via middleware)
    client_ip = request.remote_addr or "unknown"
    max_rate = RATE_LIMIT_AUTH if pubkey else RATE_LIMIT_ANON
    if not check_rate_limit_simple(client_ip, max_rate):
        return jsonify({"ok": False, "error": "rate_limit"}), 429
    
    msg = {
        "kind": data.get("kind", 39002),
        "pubkey": data.get("pubkey", pubkey or "?"),
        "from": data.get("from", data.get("pubkey", pubkey or "?")),
        "to": data.get("to", "broadcast"),
        "meta": data.get("meta", {"channel": "auto"}),
        "payload": data.get("payload", {}),
    }
    if pubkey:
        msg["meta"]["auth"] = pubkey  # mark as authenticated
    
    import asyncio
    async def _send():
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", 9932), timeout=3)
            w.write(orjson.dumps(msg) + b"\n")
            await asyncio.wait_for(w.drain(), timeout=3)
            w.close()
            return {"ok": True, "authenticated": bool(pubkey)}
        except Exception as e:
            return {"ok": False, "error": str(e)}
    result = asyncio.run(_send())
    return jsonify(result)

@app.route("/mesh/send/<to>", methods=["POST"])
def mesh_send_to(to):
    """Send to specific agent via URL path."""
    data = request.get_json(force=True)
    data["to"] = to
    return mesh_send()

# ─── Route Engine Ingest WebSocket ───



# ─── NIP-42 WebSocket Endpoint ───

@sock.route("/ws")
def ws_handler(ws):
    """WebSocket endpoint with NIP-42 AUTH handshake.
    
    Protocol (NIP-42):
      1. Server: ["AUTH", "<challenge>"]
      2. Client: ["AUTH", {"kind": 22242, "pubkey": "...", "content": "<challenge>", ...}]
      3. Server: ["AUTH", "OK"]  -- on success, connection closes on failure
    
    After auth:
      - Client sends: ["EVENT", {...}] for authenticated message forwarding
      - Client receives: ["EVENT", {...}] for incoming mesh events
    """
    import asyncio
    
    pubkey = None
    authenticated = False
    challenge = None
    
    # 1. Send AUTH challenge
    challenge = generate_challenge()
    ws.send(format_auth_message(challenge))
    
    # 2. Wait for auth response (or first message)
    while True:
        try:
            raw = ws.receive(timeout=30)
        except Exception:
            break  # timeout or disconnect
        
        if raw is None:
            break
        
        msg = parse_message(raw)
        if msg is None:
            ws.send(format_notice("invalid message format"))
            continue
        
        msg_type = msg[0]
        
        if msg_type == "AUTH" and len(msg) >= 2:
            # NIP-42 AUTH response
            event = msg[1]
            if not isinstance(event, dict):
                ws.send(format_notice("AUTH: event must be object"))
                continue
            
            if not validate_challenge(challenge or event.get("content", "")):
                ws.send(format_notice("AUTH: challenge expired or invalid"))
                # Send new challenge
                challenge = generate_challenge()
                ws.send(format_auth_message(challenge))
                continue
            
            if is_valid_auth_event(event, event.get("content", "")):
                pubkey = event["pubkey"]
                token = create_session(pubkey)
                authenticated = True
                ws.send(format_auth_ok())
                # Also send the session token as a NOTICE for convenience
                ws.send(format_notice(f"authenticated as {pubkey[:16]}... token={token[:16]}..."))
            else:
                ws.send(format_notice("AUTH: invalid signature"))
                challenge = generate_challenge()
                ws.send(format_auth_message(challenge))
        
        elif msg_type == "EVENT" and len(msg) >= 2:
            # Event forwarding
            event = msg[1]
            if not isinstance(event, dict):
                ws.send(format_notice("EVENT: must be object"))
                continue
            
            # Rate limit (via middleware)
            max_rate = RATE_LIMIT_AUTH if authenticated else RATE_LIMIT_ANON
            if not check_rate_limit_simple(f"ws:{pubkey or ws}", max_rate):
                ws.send(format_notice("rate limit exceeded"))
                continue
            
            # Forward to SmartRouter
            mesh_msg = {
                "kind": event.get("kind", 39002),
                "pubkey": event.get("pubkey", pubkey or "?"),
                "from": event.get("pubkey", pubkey or "?"),
                "to": event.get("tags", [{}])[0].get("relay", "broadcast") if event.get("tags") else "broadcast",
                "meta": {"channel": event.get("tags", [{}])[0].get("relay", "mesh") if event.get("tags") else "mesh"},
                "payload": event.get("content", ""),
            }
            if authenticated:
                mesh_msg["meta"]["auth"] = pubkey
            
            # Send to SmartRouter (non-blocking)
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                r, w = loop.run_until_complete(
                    asyncio.wait_for(
                        asyncio.open_connection("127.0.0.1", 9932), timeout=3))
                w.write(orjson.dumps(mesh_msg) + b"\n")
                loop.run_until_complete(asyncio.wait_for(w.drain(), timeout=3))
                w.close()
                loop.close()
                ws.send(format_notice("ok"))
            except Exception as e:
                ws.send(format_notice(f"forward error: {e}"))
        
        elif msg_type == "PING":
            ws.send(json.dumps(["PONG"]))
        
        elif msg_type == "CLOSE":
            break
        
        else:
            ws.send(format_notice(f"unknown message type: {msg_type}"))

@app.route("/nip65")
def nip65_discovery():
    """NIP-65 Relay List Metadata endpoint."""
    info = get_relay_info()
    return jsonify(info)

# ═══ Веха 7: First Contact — Capability Discovery ═══

@app.route("/agents/capabilities")
def list_capabilities():
    """Get agent capabilities (Phase 14.1)."""
    pubkey = get_auth_pubkey(request.headers) if hasattr(request, 'headers') else None
    result = get_agent_capabilities()
    return jsonify({"count": len(result), "capabilities": result if pubkey else {}})

@app.route("/agents/register", methods=["POST"])
def register_agent_v2():
    """Register agent WITH capabilities (Phase 14.1)."""
    data = request.get_json(force=True)
    pubkey = data.get("pubkey", "")
    if not pubkey:
        return jsonify({"ok": False, "error": "pubkey required"}), 400
    
    caps = data.get("capabilities", data.get("caps", ["gossip"]))
    
    # Register agent
    agents[pubkey] = {
        "name": data.get("name", pubkey[:16]),
        "registered_at": time.time(),
        "last_seen": time.time(),
        "meta": data.get("meta", {}),
        "capabilities": caps,
    }
    _save_agents()
    
    # Register capabilities
    caps_info = register_capabilities(pubkey, caps)
    
    return jsonify({
        "ok": True,
        "agent": agents[pubkey],
        "capabilities": caps_info,
    })

@app.route("/network/snapshot")
def network_snapshot():
    """Full network topology snapshot (Phase 14.2)."""
    snapshot = compute_network_snapshot(agents, devices, shard_count=5)
    return jsonify(snapshot)

@app.route("/agents/<pubkey>/heartbeat", methods=["POST"])
def agent_heartbeat(pubkey):
    """Agent health-check heartbeat (Phase 14.3, kind:39005)."""
    result = process_heartbeat(pubkey, agents)
    if result.get("ok"):
        _save_agents()
        return jsonify(result)
    return jsonify(result), 404

@app.route("/system/degradation")
def degradation_status():
    """Circuit breaker + degradation report (via middleware Phase 4)."""
    cb_data = cb_status()
    return jsonify({
        "circuit_breakers": cb_data.get("channels", {}),
        "middleware_uptime": cb_data.get("uptime_sec", 0),
    })

@app.route("/system/circuit-breaker/<channel>/reset", methods=["POST"])
def reset_cb(channel):
    """Reset circuit breaker for a channel (via middleware)."""
    cb_reset(channel)
    return jsonify({"ok": True, "channel": channel, "state": "closed"})

# ═══ Phase 20: S1 — Payment Gateway ═══

@app.route("/api/payments/stats")
def payment_stats():
    """Статистика платёжного модуля."""
    try:
        with open("/dev/shm/verifier_status.json") as f:
            verifier = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        verifier = {"stats": {"total_processed": 0, "valid": 0, "invalid": 0, "errors": 0}}
    
    return jsonify({
        "handler": get_pay_stats(),
        "verifier": verifier.get("stats", verifier),
        "status": "optimistic_verify_later",
    })
# ═══ Phase 21: S4 — Cheque Book ═══

@app.route("/api/chequebook/stats")
def chequebook_stats():
    """Статистика чековых книжек."""
    try:
        r = _cheque_requests.get(f"{CHEQUE_API}/stats", timeout=1)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e), "available": False}), 503

@app.route("/api/chequebook/issue", methods=["POST"])
def chequebook_issue():
    """Выпустить чековую книжку агенту."""
    data = request.get_json(force=True)
    try:
        r = _cheque_requests.post(
            f"{CHEQUE_API}/issue",
            json=data,
            timeout=2
        )
        return jsonify(r.json()), r.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 503

@app.route("/api/chequebook/agent/<pubkey>")
def chequebook_agent(pubkey):
    """Получить книжки агента."""
    try:
        r = _cheque_requests.get(f"{CHEQUE_API}/agent/{pubkey}", timeout=1)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 503

# ═══ Phase 22: S5 — ZK Proof ═══

@app.route("/api/zk/stats")
def zk_stats():
    """Статистика ZK-системы."""
    return jsonify(get_ledger_stats())

@app.route("/api/zk/credit", methods=["POST"])
def zk_credit():
    """Зачислить средства агенту (депозит)."""
    data = request.get_json(force=True)
    result = credit_agent(
        pubkey=data.get("agent", ""),
        amount=float(data.get("amount", 0)),
    )
    return jsonify(result)

@app.route("/api/zk/balance/<pubkey>")
def zk_balance(pubkey):
    """Баланс агента."""
    return jsonify(get_balance(pubkey))

@app.route("/api/zk/prove/<pubkey>")
def zk_prove(pubkey):
    """Создать ZK-proof для агента."""
    result = prove_balance(pubkey)
    return jsonify(result)

@app.route("/api/zk/spend", methods=["POST"])
def zk_spend():
    """Потратить средства через ZK-proof (kind:30000)."""
    data = request.get_json(force=True)
    result = verify_zk_proof(data, event_id=data.get("id", ""))
    code = 200 if result.get("accepted") else 400
    
    # Запись в accounting
    if result.get("accepted"):
        tags = data.get("tags", [])
        p_tag = ""
        for tag in tags:
            if isinstance(tag, list) and len(tag) > 1 and tag[0] == "p":
                p_tag = tag[1]
                break
        record_payment(
            event_id=data.get("id", f"zk_{time.time_ns()}"),
            kind=30000,
            pubkey=data.get("pubkey", ""),
            recipient=p_tag,
            amount=float(data.get("amount", 0)),
            token="SNIN",
            solana_tx=data.get("root", ""),
            method="zk",
            status="verified",
        )
    
    return jsonify(result), code

@app.route("/api/payments/submit", methods=["POST"])
def submit_payment():
    """Принять kind:30000 — авто-выбор канала (cheque > optimistic)."""
    data = request.get_json(force=True)
    
    # Auto-route between cheque and optimistic
    result = route_payment(data)
    
    if data.get("cheque_only") and result.get("method") != "cheque":
        result = {"accepted": False, "reason": "cheque required but not available"}
        return jsonify(result), 400
    
    code = 200 if result.get("accepted") else 400
    return jsonify(result), code

@app.route("/api/payments/accounting")
def payment_accounting():
    """Статистика accounting DB."""
    return jsonify(get_accounting_stats())

if __name__ == "__main__":
    print("[SNIN Mesh API] Starting on port 9907 (NIP-42 AUTH enabled)...", flush=True)
    # Periodic cleanup of expired sessions
    import threading
    def cleanup_loop():
        while True:
            time.sleep(300)  # every 5 minutes
            cleanup_sessions()
    t = threading.Thread(target=cleanup_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=9907)

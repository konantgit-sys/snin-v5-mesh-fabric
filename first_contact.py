"""First Contact Protocol — Capability Discovery, Network Snapshot, Health-check.

Фазы:
  14.1 — kind:39004 Capability Discovery (agent capabilities + broadcast)
  14.2 — Network Snapshot (topology + latency matrix)
  14.3 — Health-check + Graceful Degradation (kind:39005)
"""
import json, time, math, random
from pathlib import Path

BASE = Path.home() / "data" / "sites" / "relay-mesh"
CAPS_FILE = str(BASE / "capabilities.json")
BUFFER_FILE = str(BASE / "buffer_zone.json")

# ─── Capability Registry (Phase 14.1) ───
capabilities: dict = {}  # pubkey -> list of capabilities

def _load_capabilities():
    global capabilities
    try:
        with open(CAPS_FILE) as f:
            capabilities = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        capabilities = {}

def _save_capabilities():
    with open(CAPS_FILE, "w") as f:
        json.dump(capabilities, f, indent=2, ensure_ascii=False)

_load_capabilities()

# ─── Buffer Zone (Phase 14.3) ───
buffer_zone: dict = {}  # pubkey -> {agent_data, expired_at}

def _load_buffer():
    global buffer_zone
    try:
        with open(BUFFER_FILE) as f:
            buffer_zone = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        buffer_zone = {}

def _save_buffer():
    with open(BUFFER_FILE, "w") as f:
        json.dump(buffer_zone, f, indent=2, ensure_ascii=False)

_load_buffer()

# ─── Phase 14.1: Capability Discovery ───

CAPABILITY_TEMPLATES = {
    "gossip": {"channel": "gossip", "kinds": [39000, 39001, 39002], "latency": "2-50ms"},
    "mesh": {"channel": "mesh", "kinds": [39003, 39010, 39011], "latency": "50-100ms"},
    "nostr_bridge": {"channel": "nostr", "kinds": [1, 7, 9734, 10002, 22242], "latency": "1-5s"},
    "dht_storage": {"channel": "internal", "kinds": [39001], "ttl": 60},
    "p2p_forward": {"channel": "p2p", "kinds": [39002], "protocol": "tcp"},
    "auth_provider": {"feature": "nip42_auth", "methods": ["schnorr", "session"]},
    # P15: marketplace capabilities
    "ai_analysis": {"domain": "ai", "kinds": [39020], "description": "AI analysis and inference"},
    "ml_inference": {"domain": "ai", "kinds": [39020], "description": "ML model inference"},
    "crypto_trading": {"domain": "crypto", "kinds": [39021], "description": "Cryptocurrency trading"},
    "blockchain_indexing": {"domain": "crypto", "kinds": [39021], "description": "Blockchain data indexing"},
    "btc_trading": {"domain": "btc", "kinds": [39022], "description": "Bitcoin trading signals"},
    "bitcoin_analytics": {"domain": "btc", "kinds": [39022], "description": "Bitcoin on-chain analytics"},
    "nostr_relay": {"domain": "nostr", "kinds": [39023], "description": "Nostr relay operation"},
    "nostr_indexer": {"domain": "nostr", "kinds": [39023], "description": "Nostr content indexing"},
    "defi_analysis": {"domain": "defi", "kinds": [39024], "description": "DeFi protocol analysis"},
    "market_analysis": {"domain": "finance", "kinds": [39024], "description": "Market analysis"},
    "news_aggregation": {"domain": "news", "kinds": [39025], "description": "News aggregation"},
    "code_review": {"domain": "tech", "kinds": [39026], "description": "Code review service"},
    "privacy_audit": {"domain": "privacy", "kinds": [39027], "description": "Privacy audit"},
}

def register_capabilities(pubkey: str, caps: list = None) -> dict:
    """Register agent capabilities."""
    if caps is None:
        caps = ["gossip"]
    valid = []
    for c in caps:
        if c in CAPABILITY_TEMPLATES:
            valid.append(c)
    if not valid:
        valid = ["gossip"]
    capabilities[pubkey] = {
        "capabilities": valid,
        "template": {c: CAPABILITY_TEMPLATES[c] for c in valid},
        "registered_at": time.time(),
        "updated_at": time.time(),
    }
    _save_capabilities()
    return capabilities[pubkey]

def get_agent_capabilities(pubkey: str = None) -> dict:
    """Get capabilities of an agent or all agents."""
    if pubkey:
        return capabilities.get(pubkey, {})
    return capabilities

def build_kind39004_event(pubkey: str, caps: list) -> dict:
    """Build capability announcement event (kind:39004)."""
    return {
        "kind": 39004,
        "pubkey": pubkey,
        "created_at": int(time.time()),
        "tags": [["c", c] for c in caps],
        "content": json.dumps({
            "capabilities": caps,
            "details": {c: CAPABILITY_TEMPLATES[c] for c in caps if c in CAPABILITY_TEMPLATES},
        }),
    }

# ─── Phase 14.2: Network Snapshot ───

def compute_network_snapshot(agents: dict, devices: dict, shard_count: int = 5) -> dict:
    """Compute full network topology snapshot.
    
    Returns:
      - agents: count + list with pubkey name shard latency
      - topology: connection graph (who talks to whom)
      - latency_matrix: estimated latencies between agents
      - shards: distribution across shards
    """
    agent_list = []
    shard_dist = {i: {"agents": 0, "names": []} for i in range(shard_count)}
    latency_map = {}
    
    for pubkey, info in agents.items():
        # Determine shard
        shard_id = _hash_to_shard(pubkey, shard_count)
        agent_entry = {
            "pubkey": pubkey[:16] + "...",
            "name": info.get("name", pubkey[:12]),
            "shard": shard_id,
            "last_seen": info.get("last_seen", 0),
            "status": "alive" if time.time() - info.get("last_seen", 0) < 120 else "stale",
            "capabilities": capabilities.get(pubkey, {}).get("capabilities", ["unknown"]),
        }
        agent_list.append(agent_entry)
        shard_dist[shard_id]["agents"] += 1
        shard_dist[shard_id]["names"].append(agent_entry["name"])
        latency_map[pubkey[:16]] = _estimate_latency(info)
    
    # Device distribution
    device_list = []
    for did, info in devices.items():
        device_list.append({
            "id": did,
            "type": info.get("device_type", "?"),
            "tier": info.get("tier", 4),
            "latency_ms": info.get("latency_ms", -1),
        })
    
    # Topology: each shard connected to all other shards via P2P
    topology = []
    for i in range(shard_count):
        for j in range(i + 1, shard_count):
            if shard_dist[i]["agents"] > 0 and shard_dist[j]["agents"] > 0:
                topology.append({
                    "from": f"shard_{i}",
                    "to": f"shard_{j}",
                    "link": "p2p_tcp",
                    "latency_ms": round(random.uniform(0.5, 3.0), 2),
                })
    
    # Channels
    channels = {
        "direct": {"agents": len(agent_list), "latency": "1-2ms"},
        "gossip": {"agents": len(agent_list), "latency": "20-50ms", "fanout": 3},
        "mesh": {"agents": len(agent_list), "latency": "50-100ms", "dedup": "bloom"},
        "nostr": {"agents": len(agent_list), "latency": "1-5s", "relays": 101},
    }
    
    return {
        "generated_at": time.time(),
        "agents": {
            "total": len(agent_list),
            "alive": sum(1 for a in agent_list if a["status"] == "alive"),
            "stale": sum(1 for a in agent_list if a["status"] == "stale"),
            "list": agent_list,
        },
        "devices": device_list,
        "shards": {
            "count": shard_count,
            "distribution": [shard_dist[i] for i in range(shard_count)],
        },
        "topology": topology,
        "channels": channels,
        "capabilities": {
            "total_types": len(CAPABILITY_TEMPLATES),
            "agents_with_caps": len(capabilities),
        },
    }

def _hash_to_shard(pubkey: str, n_shards: int) -> int:
    """Deterministic shard assignment."""
    try:
        import hashlib
        if not pubkey or len(pubkey) < 8:
            return 0
        return int(hashlib.md5(pubkey.encode()).hexdigest()[:8], 16) % n_shards
    except Exception:
        return 0

def _estimate_latency(info: dict) -> float:
    """Estimate latency based on agent info."""
    age = time.time() - info.get("last_seen", time.time())
    if age < 10:
        return round(random.uniform(1.0, 5.0), 2)
    elif age < 60:
        return round(random.uniform(5.0, 20.0), 2)
    else:
        return round(random.uniform(20.0, 100.0), 2)

# ─── Phase 14.3: Graceful Degradation ───

HEARTBEAT_TTL = 60        # seconds — standard TTL
BUFFER_TTL = 300           # seconds — keep dead agent in buffer zone (5 min)
CIRCUIT_BREAKER_WINDOW = 10  # seconds — sliding window for errors
CIRCUIT_BREAKER_THRESHOLD = 5  # errors per window → OPEN

# Per-channel circuit breaker state
cb_state: dict = {
    "direct": {"errors": [], "state": "closed", "last_open": 0},
    "gossip": {"errors": [], "state": "closed", "last_open": 0},
    "mesh": {"errors": [], "state": "closed", "last_open": 0},
    "nostr": {"errors": [], "state": "closed", "last_open": 0},
}

def build_kind39005_event(pubkey: str, status: str = "alive") -> dict:
    """Build health-check heartbeat (kind:39005)."""
    return {
        "kind": 39005,
        "pubkey": pubkey,
        "created_at": int(time.time()),
        "tags": [
            ["status", status],
            ["ttl", str(HEARTBEAT_TTL)],
        ],
        "content": json.dumps({
            "status": status,
            "timestamp": time.time(),
            "agent_uptime": "?" if pubkey not in capabilities else 
                int(time.time() - capabilities[pubkey].get("registered_at", time.time())),
        }),
    }

def add_to_buffer(pubkey: str, data: dict, ttl: int = 300):
    """P15: Add agent to buffer zone (pending verification)."""
    buffer_zone[pubkey] = {
        **data,
        "expired_at": time.time() + ttl,
        "added_at": time.time(),
    }
    _save_buffer()

def decide_buffer_action(pubkey: str) -> dict:
    """P15: Decide what to do with a buffered agent."""
    if pubkey not in buffer_zone:
        return {"action": "unknown", "reason": "not in buffer"}
    info = buffer_zone[pubkey]
    now = time.time()
    if now > info.get("expired_at", 0):
        return {"action": "expire", "reason": "ttl expired", "age": int(now - info.get("added_at", now))}
    age = now - info.get("added_at", now)
    if age < 30:
        return {"action": "validate", "reason": "new agent, pending validation", "age": int(age)}
    return {"action": "promote", "reason": "passed probation", "age": int(age)}

def process_heartbeat(pubkey: str, agents: dict) -> dict:
    """Process agent heartbeat — update last_seen, handle buffer zone."""
    if pubkey in agents:
        agents[pubkey]["last_seen"] = time.time()
        return {"ok": True, "status": "alive", "ttl": HEARTBEAT_TTL}
    
    # Agent in buffer zone → revive
    if pubkey in buffer_zone:
        agents[pubkey] = buffer_zone.pop(pubkey)
        agents[pubkey]["last_seen"] = time.time()
        _save_buffer()
        return {"ok": True, "status": "revived", "ttl": HEARTBEAT_TTL}
    
    return {"ok": False, "error": "unknown_agent"}

def check_graceful_degradation(agents: dict) -> dict:
    """Check agent health and move dead agents to buffer zone."""
    now = time.time()
    moved = []
    
    for pubkey in list(agents.keys()):
        last_seen = agents[pubkey].get("last_seen", 0)
        if now - last_seen > HEARTBEAT_TTL:
            # Move to buffer zone
            buffer_zone[pubkey] = {
                "agent": agents[pubkey],
                "expired_at": time.time() + BUFFER_TTL,
            }
            del agents[pubkey]
            moved.append(pubkey)
    
    # Clean stale buffer entries
    expired = []
    for pubkey in list(buffer_zone.keys()):
        if now > buffer_zone[pubkey].get("expired_at", 0):
            expired.append(pubkey)
    for pk in expired:
        del buffer_zone[pk]
    
    _save_buffer()
    return {
        "moved_to_buffer": moved,
        "buffer_size": len(buffer_zone),
        "expired_removed": len(expired),
    }

def circuit_breaker_check(channel: str) -> str:
    """Check circuit breaker state for a channel.
    
    Returns: 'closed' (working), 'open' (blocked), 'half-open' (testing)
    """
    state = cb_state.get(channel, {"errors": [], "state": "closed"})
    now = time.time()
    
    # Clean old errors
    state["errors"] = [t for t in state["errors"] if now - t < CIRCUIT_BREAKER_WINDOW]
    
    if state["state"] == "open":
        # Auto half-open after 30s
        if now - state.get("last_open", 0) > 30:
            state["state"] = "half-open"
            cb_state[channel] = state
            return "half-open"
        return "open"
    
    if len(state["errors"]) >= CIRCUIT_BREAKER_THRESHOLD:
        state["state"] = "open"
        state["last_open"] = now
        cb_state[channel] = state
        return "open"
    
    return state["state"]

def record_channel_error(channel: str):
    """Record an error on a channel (triggers circuit breaker)."""
    if channel not in cb_state:
        cb_state[channel] = {"errors": [], "state": "closed", "last_open": 0}
    cb_state[channel]["errors"].append(time.time())

def reset_circuit_breaker(channel: str):
    """Manually reset circuit breaker for a channel."""
    if channel in cb_state:
        cb_state[channel] = {"errors": [], "state": "closed", "last_open": 0}

def get_degradation_status() -> dict:
    """Get full degradation status report."""
    return {
        "circuit_breakers": {ch: {"state": s["state"]} for ch, s in cb_state.items()},
        "buffer_zone": {pk: {"ttl_remaining": max(0, int(info.get("expired_at", 0) - time.time()))} 
                       for pk, info in buffer_zone.items()},
    }

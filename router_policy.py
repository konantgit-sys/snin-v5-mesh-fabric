"""
router_policy.py — Политики маршрутизации, Circuit Breaker, Traffic Classification
Выделено из smart_router.py (Фаза 2 рефакторинга)
"""

import asyncio
import hashlib
import random
import time
from collections import defaultdict, deque

try:
    import orjson as json
except ImportError:
    import json

from mesh_config import config

# ─── REDIS KEYS ───
ROUTE_HISTORY_KEY = "route:history:{}:{}"
ROUTE_BEST_KEY = "route:best:{}"
ROUTE_STATS_KEY = "route:stats:{}:{}"
POLICY_KEY = "policy:routes"
TC_POLICY_KEY = "tc:policy:{}"
TC_HISTORY_KEY = "tc:history:{}:{}"
TC_BEST_KEY = "tc:best:{}"
TC_STATS_KEY = "tc:stats:{}:{}"

# ─── CB (Redis-backed, legacy) ───
CB_INCIDENT_KEY = "cb:incident:{}:{}"
CB_BLOCKED_KEY = "cb:blocked:{}"
CB_INCIDENT_LIMIT = 3
CB_INCIDENT_WINDOW = 60
CB_BLOCK_TTL = 30

# ─── TRAFFIC CLASSES ───
TRAFFIC_CLASSES = {
    "agent-to-agent": {"gossip": 0.6, "mesh": 0.4},
    "iot":            {"content_router": 0.8, "mesh": 0.2},
    "gossip_data":    {"gossip_data": 1.0, "mesh": 0.5, "gossip": 0.3},
    "nostr-out":      {"nostr": 0.9, "mesh": 0.1},
    "content":        {"mesh": 0.5, "content_router": 0.5},
    "payment":        {"chequebook": 0.5, "mesh": 0.4, "gossip": 0.1},
    "dao":            {"mesh": 1.0, "chequebook": 0.1},
}

KIND_TO_TRAFFIC_CLASS = {
    39000: "agent-to-agent",
    39001: "agent-to-agent",
    39003: "iot",
    39004: "gossip_data",
    39005: "agent-to-agent",
    39006: "agent-to-agent",
    39010: "dao",
    30000: "payment",
    1:     "nostr-out",
    42:    "nostr-out",
    7:     "nostr-out",
}

# ─── BACKPRESSURE ───
BP_MAX_CONCURRENT = 100
BP_MAX_QUEUE_TIME = 0.5
BP_RETRY_AFTER_SEC = 5

# ─── REDIS CLIENT (lazy singleton) ───
_REDIS_CLIENT = None


async def aredis():
    """Ленивый Redis-asyncio клиент."""
    global _REDIS_CLIENT
    if _REDIS_CLIENT is None:
        try:
            import redis.asyncio as redis_py
            _REDIS_CLIENT = redis_py.Redis(
                host='localhost', port=6379, db=0,
                socket_connect_timeout=1, socket_timeout=2,
                decode_responses=True
            )
            await _REDIS_CLIENT.ping()
            print(f"[Redis] aredis: connected to localhost:6379 db=0")
        except Exception as e:
            print(f"[Redis] aredis: FAILED - {e}")
            pass
    return _REDIS_CLIENT


# ─── REPUTATION ───
try:
    from reputation import get_reputation_for_pubkey, get_all_reputations
    REPUTATION_ENABLED = True
except Exception:
    REPUTATION_ENABLED = False


def get_reputation_weight(pubkey: str) -> float:
    """Получить вес репутации для pubkey. 0.0-1.0."""
    if not REPUTATION_ENABLED:
        return 0.5
    try:
        rep = get_reputation_for_pubkey(pubkey)
        return rep.get("score", 0.5)
    except Exception:
        return 0.5


# ─── CIRCUIT BREAKER (In-Memory) ───
class InMemoryCircuitBreaker:
    """Pure Python CB — никакого Redis на горячем пути.
    
    Sliding window: deque(timestamps) per channel.
    Incident: >500ms latency → запись в deque.
    Block: 3+ incidents в окне 60s → блок на 30s.
    """
    
    def __init__(self):
        self._incidents: dict[str, deque] = {}
        self._blocked_until: dict[str, float] = {}
        self.latency_threshold_ms = 500
        self.mesh_latency_threshold_ms = 1000
        self.incident_limit = 3
        self.incident_window = 60
        self.block_ttl = 30
    
    def record_incident(self, channel: str) -> bool:
        now = time.time()
        if channel not in self._incidents:
            self._incidents[channel] = deque(maxlen=100)
        inc = self._incidents[channel]
        inc.append(now)
        while inc and inc[0] < now - self.incident_window:
            inc.popleft()
        if len(inc) >= self.incident_limit:
            self._blocked_until[channel] = now + self.block_ttl
            inc.clear()
            return True
        return False
    
    def is_blocked(self, channel: str) -> bool:
        if channel not in self._blocked_until:
            return False
        if time.time() < self._blocked_until[channel]:
            return True
        del self._blocked_until[channel]
        return False
    
    def get_blocked(self) -> list[str]:
        now = time.time()
        return [ch for ch, until in self._blocked_until.items() if now < until]
    
    def force_recovery(self, channel: str):
        self._blocked_until.pop(channel, None)
    
    def reset(self, channel: str):
        self._incidents.pop(channel, None)
        self._blocked_until.pop(channel, None)


# ─── TRAFFIC CLASSIFICATION ───
def classify_traffic(kind: int, meta: dict = None) -> str:
    """Классифицировать тип трафика по kind + meta."""
    if meta and isinstance(meta, dict):
        tc = meta.get("traffic_class", "")
        if tc in TRAFFIC_CLASSES:
            return tc
    return KIND_TO_TRAFFIC_CLASS.get(kind, "content")


# ─── SHAED FOR GOSSIP ───
def gossip_shard_for(pubkey: str, n_shards: int = 5) -> int:
    """Выбрать шард по pubkey через consistent hashing."""
    if not pubkey or pubkey == "?" or len(pubkey) < 8:
        return random.randint(0, n_shards - 1)
    h = hashlib.md5(pubkey.encode()).hexdigest()
    return int(h[:8], 16) % n_shards


# ─── POLICY ENGINE (Redis-backed) ───
async def apply_policies():
    """Записать дефолтные политики в Redis, если их нет."""
    r = await aredis()
    if not r:
        return
    defaults = {
        "39000": json.dumps({"gossip": 0.9, "mesh": 0.1}),
        "39001": json.dumps({"gossip": 0.7, "direct": 0.3}),
        "39002": json.dumps({"mesh": 1.0}),
        "39003": json.dumps({"mesh": 0.5, "gossip": 0.5}),
        "39004": json.dumps({"mesh": 0.8, "gossip_data": 1.0, "gossip": 0.3}),
        "39010_39025": json.dumps({"mesh": 1.0}),
        "30000": json.dumps({"chequebook": 0.5, "mesh": 0.4, "gossip": 0.1}),
        "1": json.dumps({"nostr": 1.0}),
        "default": json.dumps({"mesh": 0.7, "gossip": 0.3}),
    }
    for kind_range, weights in defaults.items():
        exists = await r.hget(POLICY_KEY, kind_range)
        if exists is None:
            await r.hset(POLICY_KEY, kind_range, weights)
    for tc, weights in TRAFFIC_CLASSES.items():
        key = TC_POLICY_KEY.format(tc)
        exists = await r.get(key)
        if exists is None:
            await r.setex(key, 86400, json.dumps(weights))


async def get_policy_for_kind(kind: int) -> dict:
    """Вернуть словарь каналов с весами для данного kind."""
    r = await aredis()
    if r:
        raw = await r.hget(POLICY_KEY, str(kind))
        if raw:
            return json.loads(raw)
        all_policies = await r.hgetall(POLICY_KEY)
        for key, raw in all_policies.items():
            if "_" in key:
                parts = key.split("_")
                try:
                    lo, hi = int(parts[0]), int(parts[1])
                    if lo <= kind <= hi:
                        return json.loads(raw)
                except (ValueError, IndexError):
                    continue
        raw = await r.hget(POLICY_KEY, "default")
        if raw:
            return json.loads(raw)
    return {"mesh": 1.0}


async def pick_channel_from_policy(policy: dict, agent_id: str) -> str:
    """Выбрать канал из политики по весам (с учётом self-learning)."""
    r = await aredis()
    best = None
    if r:
        best = await r.get(ROUTE_BEST_KEY.format(agent_id))
    if best and best in policy:
        return best
    channels = list(policy.keys())
    weights = [policy[c] for c in channels]
    return random.choices(channels, weights=weights, k=1)[0]


async def get_best_channel(agent_id: str) -> str:
    """Лучший канал для агента."""
    r = await aredis()
    if r:
        best = await r.get(ROUTE_BEST_KEY.format(agent_id))
        if best:
            return best
    return "mesh"


async def record_route(agent_id: str, channel: str, latency_ms: float, success: bool):
    r = await aredis()
    if r is None:
        return
    key = ROUTE_HISTORY_KEY.format(agent_id, channel)
    now = time.time()
    await r.zadd(key, {f"{now}:{latency_ms:.1f}": latency_ms})
    await r.zremrangebyrank(key, 0, -101)
    await r.expire(key, 86400)
    scores = await r.zrange(key, 0, -1, withscores=True)
    avg = sum(s[1] for s in scores) / len(scores) if scores else latency_ms
    stats_key = ROUTE_STATS_KEY.format(agent_id, channel)
    if success:
        await r.hincrby(stats_key, "sent", 1)
    else:
        await r.hincrby(stats_key, "failed", 1)
    await r.hset(stats_key, "avg_latency", f"{avg:.1f}")
    await r.expire(stats_key, 86400)
    current_best = await r.get(ROUTE_BEST_KEY.format(agent_id))
    if current_best is None:
        await r.set(ROUTE_BEST_KEY.format(agent_id), channel)
        await r.expire(ROUTE_BEST_KEY.format(agent_id), 86400)
    elif success and current_best != channel:
        curr_avg = await r.hget(ROUTE_STATS_KEY.format(agent_id, current_best), "avg_latency")
        if curr_avg:
            try:
                if avg < float(curr_avg) * 0.8:
                    await r.set(ROUTE_BEST_KEY.format(agent_id), channel)
            except ValueError:
                pass


# ─── CIRCUIT BREAKER (Redis-backed, legacy) ───
async def cb_record_incident(channel: str) -> bool:
    r = await aredis()
    if not r:
        return False
    now = time.time()
    key = CB_INCIDENT_KEY.format(channel, "all")
    await r.zadd(key, {f"{now}": now})
    await r.zremrangebyscore(key, 0, now - CB_INCIDENT_WINDOW)
    await r.expire(key, 300)
    count = await r.zcard(key)
    if count >= CB_INCIDENT_LIMIT:
        block_key = CB_BLOCKED_KEY.format(channel)
        await r.setex(block_key, CB_BLOCK_TTL, "1")
        await r.delete(key)
        return True
    return False


async def cb_is_blocked(channel: str) -> bool:
    r = await aredis()
    if not r:
        return False
    return (await r.exists(CB_BLOCKED_KEY.format(channel))) > 0


async def cb_get_blocked_channels() -> list:
    r = await aredis()
    if not r:
        return []
    keys = await r.keys(CB_BLOCKED_KEY.format("*"))
    return [k.split(":")[-1] for k in keys]

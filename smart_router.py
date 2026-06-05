"""Smart Router v2 — 4 канала, маршрутные политики, самообучение + L5T Dead-Letter.

Каналы доставки:
  direct  — TCP напрямую к агенту (IP из DHT)          — ~2ms
  mesh    — Content Router → Route Engine → relay-mesh   — ~100ms
  gossip  — Broadcast через 5 gossip шардов (fan-out ×3)  — ~50ms  
  nostr   — Nostr Gateway → 21 публичный релей           — ~1-5s

Маршрутные политики (правила из Redis policy:routes:{kind}):
  kind:39000 (heartbeat)    → gossip:0.9, mesh:0.1
  kind:39001 (DHT)          → gossip:0.7, direct:0.3
  kind:39002 (content)      → mesh:0.6, nostr:0.4
  kind:39010-39025 (DAO)    → mesh:1.0 (только надёжный)
  kind:1 (Nostr text)       → nostr:1.0
  kind:30000 (market)       → mesh:0.5, gossip:0.5

Формат на вход (TCP :9932, line-based JSON):
  {
    "from": "agent_name",
    "to": "target_agent",
    "kind": 39002,
    "payload": "message text",
    "meta": {
        "channel": "nostr",
        "priority": "high",
        "agent": "forecaster_ai",
    }
  }
"""


# ═══ L5T: Dead-Letter Middleware ═══
try:
    from l5t_middleware import create_l5t_middleware, L5TMiddleware, HeartbeatTracker
    L5T_AVAILABLE = True
except ImportError:
    L5T_AVAILABLE = False
    print("[Router] ⚠️ L5T middleware not available")


import asyncio
import orjson as json
import time
import os
import sys
import hashlib
import random
from collections import defaultdict, deque

from mesh_config import config
from gossip_stream import GossipStream
from graceful_degradation import GracefulDegradation
from rate_limiter import RateLimiter
from message_sequencer import SeqNumTracker, ReorderBuffer, reorder_timeout_loop, reorder_cleanup_loop
from message_deduplicator import MessageDeduplicator, dedup_cleanup_loop
from priority_queue import PriorityQueue

# Level 2: CPU-bound crypto в ProcessPool
sys.path.insert(0, "/home/agent/data/sites/relay-mesh")
from cpu_worker import verify_ed25519_processpool_async, shutdown_pools

# Router modules (Phase 2 refactoring)
from router_policy import (
    InMemoryCircuitBreaker, aredis, apply_policies, get_policy_for_kind,
    pick_channel_from_policy, get_best_channel, record_route,
    classify_traffic, get_reputation_weight, gossip_shard_for,
    TRAFFIC_CLASSES, KIND_TO_TRAFFIC_CLASS, ROUTE_STATS_KEY,
    ROUTE_HISTORY_KEY, ROUTE_BEST_KEY, TC_STATS_KEY, TC_HISTORY_KEY,
    TC_BEST_KEY, TC_POLICY_KEY, BP_MAX_CONCURRENT, BP_MAX_QUEUE_TIME,
    POLICY_KEY,
)

# ─── Настройки (из mesh_config.yaml) ──────────────────────────────────
LISTEN_HOST = config.get("transport.smart_router.host", "0.0.0.0")
LISTEN_PORT = config.get("transport.smart_router.port", 9932)

# Health endpoint (mesh_health на порту +10000)
from mesh_health import start_health
start_health(LISTEN_PORT, "smart_router")

# Адреса каналов
CR_HOST = "127.0.0.1"
CR_PORT = config.get("transport.content_router_v2.port", 9920)
NOSTR_GW_HOST = "127.0.0.1"
NOSTR_GW_PORTS = [9941, 9942, 9943, 9944, 9945]  # Фаза 2: все 5 шардов — gateway

# ═══ L5T Constants ═══
HEARTBEAT_KIND = 39000
DEAD_LETTER_KIND = 9000
GOSSIP_PORTS = [9100, 9101, 9102, 9103, 9104]
CR_V2_PORT = CR_PORT

# Unix sockets
UNIX_SOCK_DIR = config.get("global.unix_socket_dir", "/tmp/snin")
UNIX_CR_SOCK = f"{UNIX_SOCK_DIR}/cr.sock"
UNIX_NOSTR_SOCK = f"{UNIX_SOCK_DIR}/nostr.sock"
UNIX_GOSSIP_SOCKS = [f"{UNIX_SOCK_DIR}/gossip_{i}.sock" for i in range(5)]
UNIX_SR_SOCK = f"{UNIX_SOCK_DIR}/sr.sock"
ACK_CONNECT_TIMEOUT = 5
HEALTH_PORT = config.get("transport.smart_router.health_port", 9933)

# Redis (lazy import в aredis())
REDIS_CLIENT = None
_GLOBAL_ROUTER = None  # глобальный синглтон SmartRouter (in-memory best_channel)

# ═══ Phase 2: Relay Signing — подписанные релеи (L5 Identity) ═══
SIGNED_RELAYS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data", "signed_relays.json"
)
_signed_relays_cache: dict[str, dict] = {}


def _load_signed_relays():
    """Загрузить список подписанных релеев из файла."""
    global _signed_relays_cache
    try:
        with open(SIGNED_RELAYS_FILE) as f:
            _signed_relays_cache = json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError, Exception):
        _signed_relays_cache = {}


def is_relay_signed(relay_url: str) -> bool:
    """Проверить, подписан ли релей через L5 Identity."""
    return relay_url in _signed_relays_cache


def get_signed_relay_count() -> int:
    """Количество подписанных релеев."""
    return len(_signed_relays_cache)


# Загрузка при старте
_load_signed_relays()


class SmartRouter:
    def __init__(self):
        self.stats = defaultdict(int)
        self.stats["start_time"] = time.time()
        self._deg = GracefulDegradation()
        self._cr_writer = None
        self._nostr_writers = [None] * 5  # 5 bridge shards, initialized with None placeholders
        self._gossip_writers = []
        self._gossip_stream = None  # V8: GossipStream instance
        # Self-learning
        self._last_learning = time.time()
        self._learning_interval = 15  # сек
        self._channel_health = {
            "mesh": {"ok": 0, "fail": 0, "avg_ms": 0},
            "gossip": {"ok": 0, "fail": 0, "avg_ms": 0},
            "nostr": {"ok": 0, "fail": 0, "avg_ms": 0},
            "direct": {"ok": 0, "fail": 0, "avg_ms": 0},
            "fire-and-forget": {"ok": 0, "fail": 0, "avg_ms": 0},
        }
        # ═══ Фаза 2: Circuit Breaker + Backpressure ═══
        self._cb = InMemoryCircuitBreaker()
        self._concurrent = 0
        self._bp_threshold_reached = False
        # ═══ Фаза 6.2: In-memory Policy Cache ═══
        self._policy_cache: dict[str, dict] = {}
        self._policy_cache_loaded = False
        self._best_channel: dict[str, str] = {}
        # ═══ Фаза 6.3: Deferred Route Stats ═══
        self._rs_sent: dict[str, dict[str, int]] = {}
        self._rs_failed: dict[str, dict[str, int]] = {}
        self._rs_lat: dict[str, dict[str, list[float]]] = {}
        self._rs_last_flush = time.time()
        # ═══ Вектор 3: Traffic Class Stats (in-memory) ═══
        self._tc_sent: dict[str, int] = {}
        self._tc_failed: dict[str, int] = {}
        self._tc_lat: dict[str, list[float]] = {}
        # ═══ Фаза 6.7: Batch Mesh Drain ═══
        self._mesh_buf = bytearray()
        self._last_mesh_drain = 0.0
        self._mesh_drain_interval = 0.01  # 10ms
        # ═══ Фаза 1.1: Pending queue для mesh при отвале CR ═══
        self._pending_mesh_queue: list[dict] = []  # накопленные сообщения
        self._pending_mesh_max = 1000               # лимит очереди
        # ═══ Фаза 1.2: CB recovery counter ═══
        self._cb_recovery_count: dict[str, int] = {}  # channel → успешных drain подряд
        self._cb_recovery_threshold = 5                # после скольких снять блокировку
        self._cr_v2_writer = None  # Content Router v2 (:9920)
        self._last_cr_reconnect = 0.0  # rate-limit reconnect
        # ═══ Фаза 1: DHT Kademlia ═══
        self._dht = None
        # ═══ L5T: Dead-Letter Queue Middleware ═══
        self.l5t = create_l5t_middleware() if L5T_AVAILABLE else None
        self._l5t_heartbeat_task: asyncio.Task = None  # type: ignore
        if self.l5t:
            print("[Router] 📮 L5T Dead-Letter middleware initialized")
        # ═══ Фаза 8: Event subscribers (push-канал для агентов) ═══
        self._event_subscribers: dict[int, tuple] = {}  # id → (writer, agent_name)
        self._sub_next_id = 0
        # ═══ Фаза 1 Rate Limiter (token bucket) ═══
        self._rate_limiter = RateLimiter(rate=100.0, burst=200)
        # ═══ Фаза 3 Message Ordering (seq_num + reorder buffer) ═══
        self._seq_tracker = SeqNumTracker()
        self._reorder = ReorderBuffer()
        # ═══ Фаза 4 Message Deduplication ═══
        self._dedup = MessageDeduplicator()
        # ═══ Фаза 5 Message Prioritization ═══
        self._priority_queue = PriorityQueue()
        self._pq_workers = 2  # кол-во worker-корутин (мало = CRITICAL не ждёт долго)

    # ═══ Фаза 6.2: In-memory Policy Cache ═══
    async def _load_policy_cache(self):
        """Загрузить все политики из Redis в in-memory cache."""
        r = await aredis()
        if not r:
            print("[PolicyCache] aredis returned None!")
            return
        try:
            all_raw = await r.hgetall(POLICY_KEY)
            # DEBUG: show raw default
            raw_default = all_raw.get("default", "MISSING")
            print(f"[PolicyCache] RAW default: {raw_default}")
            cache = {}
            for key, raw in all_raw.items():
                try:
                    data = raw.encode() if isinstance(raw, str) else raw
                    cache[key] = json.loads(data)
                except Exception as e:
                    print(f"[PolicyCache] ERROR key={key}: {e}")
                    cache[key] = {"mesh": 1.0}
            self._policy_cache = cache
            self._policy_cache_loaded = True
            # 🔒 Gossip-first enforcement: gossip ≥ mesh in all policies
            for key in cache:
                w = cache[key]
                # Ensure gossip exists in all policies
                if "gossip" not in w:
                    w["gossip"] = 1.0  # add gossip with high weight
                # Gossip must be at least as heavy as mesh
                mesh_w = w.get("mesh", 0)
                gossip_w = w.get("gossip", 0)
                if gossip_w < mesh_w:
                    w["gossip"] = mesh_w  # equalize
                # Normalize
                total = sum(w.values())
                if total > 0:
                    for ch in w:
                        w[ch] = round(w[ch] / total, 3)
                cache[key] = w
            self._policy_cache = cache
            print(f"[Router] Policy cache loaded: {len(cache)} rules, default={cache.get('default', 'N/A')}")
        except Exception as e:
            print(f"[PolicyCache] Error loading: {e}")

    def get_policy(self, kind: int) -> dict:
        """In-memory: вернуть политику для kind без Redis.
        Exact match → Range match → Default.
        Gossip-first enforced inline.
        """
        sk = str(kind)
        result = None

        # Exact match
        if sk in self._policy_cache:
            result = dict(self._policy_cache[sk])
        else:
            # Range match (39010_39025)
            for key, weights in self._policy_cache.items():
                if "_" in key:
                    parts = key.split("_")
                    try:
                        lo, hi = int(parts[0]), int(parts[1])
                        if lo <= kind <= hi:
                            result = dict(weights)
                            break
                    except (ValueError, IndexError):
                        continue
        
        if result is None:
            result = self._policy_cache.get("default", {"gossip": 1.0, "mesh": 0.5})
        
        # 🔒 Gossip-first: ensure gossip ≥ mesh
        result = dict(result)  # copy to avoid mutating cache
        if "gossip" not in result:
            result["gossip"] = 1.0
        if result.get("gossip", 0) < result.get("mesh", 0):
            result["gossip"] = result["mesh"]
            total = sum(result.values())
            for ch in result:
                result[ch] = round(result[ch] / total, 3)
        
        return result

    async def _sync_best_channels(self):
        """Раз в 60 сек: синхронизировать best_channel из Redis.
        Только для агентов с >10 маршрутами — не на горячем пути.
        """
        r = await aredis()
        if not r:
            return
        try:
            keys = await r.keys("route:best:*")
            for k in keys:
                agent = k.split(":")[-1]
                if agent:
                    ch = await r.get(k)
                    if ch:
                        self._best_channel[agent] = ch
        except Exception as e:
            print(f"[SyncBest] Error: {e}")

    # ═══ Фаза 6.3: Deferred Route Stats ═══
    # ═══ Вектор 3: Self-Learning по traffic_class ═══
    async def _get_tc_best(self, traffic_class: str) -> str | None:
        """Лучший канал для данного traffic_class из Redis."""
        r = await aredis()
        if r:
            return await r.get(TC_BEST_KEY.format(traffic_class))
        return None
    
    def _record_tc(self, traffic_class: str, channel: str, latency_ms: float, success: bool):
        """Сохранить статистику по traffic_class (in-memory, flush в Redis)."""
        key = f"tc:{traffic_class}:{channel}"
        if success:
            self._tc_sent[key] = self._tc_sent.get(key, 0) + 1
            lats = self._tc_lat.get(key)
            if lats is None:
                self._tc_lat[key] = lats = []
            if len(lats) < 50:
                lats.append(latency_ms)
        else:
            self._tc_failed[key] = self._tc_failed.get(key, 0) + 1
    
    def _record_route(self, agent_id: str, channel: str, latency_ms: float, success: bool):
        """In-memory: синхронный append в буфер, 0 Redis на горячем пути.
        Флашится раз в 30 сек через _flush_route_stats.
        """
        key = f"{agent_id}:{channel}"
        if success:
            self._rs_sent[key] = self._rs_sent.get(key, 0) + 1
            lats = self._rs_lat.get(key)
            if lats is None:
                self._rs_lat[key] = lats = []
            if len(lats) < 50:
                lats.append(latency_ms)
        else:
            self._rs_failed[key] = self._rs_failed.get(key, 0) + 1

    async def _flush_route_stats(self):
        """Раз в 30 сек: batch запись накопленной статистики в Redis."""
        r = await aredis()
        if not r or (not self._rs_sent and not self._rs_failed):
            self._rs_last_flush = time.time()
            return
        try:
            pipe = r.pipeline()
            for key, count in self._rs_sent.items():
                agent, ch = key.split(":", 1)
                pipe.hincrby(ROUTE_STATS_KEY.format(agent, ch), "sent", count)
            for key, count in self._rs_failed.items():
                agent, ch = key.split(":", 1)
                pipe.hincrby(ROUTE_STATS_KEY.format(agent, ch), "failed", count)
            for key, lats in self._rs_lat.items():
                if lats:
                    agent, ch = key.split(":", 1)
                    avg = sum(lats) / len(lats)
                    pipe.hset(ROUTE_STATS_KEY.format(agent, ch), "avg_latency", f"{avg:.1f}")
                    pipe.expire(ROUTE_STATS_KEY.format(agent, ch), 86400)
                    # ═══ Дыра C fix: сохраняем route:history и route:best ═══
                    now = time.time()
                    pipe.zadd(ROUTE_HISTORY_KEY.format(agent, ch), {f"{now}:{avg:.1f}": avg})
                    pipe.zremrangebyrank(ROUTE_HISTORY_KEY.format(agent, ch), 0, -101)
                    pipe.expire(ROUTE_HISTORY_KEY.format(agent, ch), 86400)
            await pipe.execute()
            
            # route:best — лучший канал по avg_latency
            for key in self._rs_lat:
                if key:
                    agent, ch = key.split(":", 1)
                    lats = self._rs_lat[key]
                    if lats:
                        avg = sum(lats) / len(lats)
                        current_best = await r.get(ROUTE_BEST_KEY.format(agent))
                        if current_best is None:
                            await r.setex(ROUTE_BEST_KEY.format(agent), 86400, ch)
                        elif ch == current_best:
                            pass
                        else:
                            curr_avg = await r.hget(ROUTE_STATS_KEY.format(agent, current_best), "avg_latency")
                            if curr_avg and float(curr_avg) > 0:
                                if avg < float(curr_avg) * 0.8:
                                    await r.setex(ROUTE_BEST_KEY.format(agent), 86400, ch)
            
            # ═══ Вектор 3: Flush TC stats в Redis ═══
            tc_pipe = r.pipeline()
            for key, count in self._tc_sent.items():
                # key = tc:{traffic_class}:{channel}
                parts = key.split(":", 2)
                if len(parts) == 3:
                    traffic_class, ch = parts[1], parts[2]
                    tc_pipe.hincrby(TC_STATS_KEY.format(traffic_class, ch), "sent", count)
            for key, count in self._tc_failed.items():
                parts = key.split(":", 2)
                if len(parts) == 3:
                    traffic_class, ch = parts[1], parts[2]
                    tc_pipe.hincrby(TC_STATS_KEY.format(traffic_class, ch), "failed", count)
            for key, lats in self._tc_lat.items():
                if lats:
                    parts = key.split(":", 2)
                    if len(parts) == 3:
                        traffic_class, ch = parts[1], parts[2]
                        avg = sum(lats) / len(lats)
                        tc_pipe.hset(TC_STATS_KEY.format(traffic_class, ch), "avg_latency", f"{avg:.1f}")
                        tc_pipe.expire(TC_STATS_KEY.format(traffic_class, ch), 86400)
                        # TC history (sorted set для self-learning)
                        now = time.time()
                        tc_pipe.zadd(TC_HISTORY_KEY.format(traffic_class, ch), {f"{now}:{avg:.1f}": avg})
                        tc_pipe.zremrangebyrank(TC_HISTORY_KEY.format(traffic_class, ch), 0, -101)
                        tc_pipe.expire(TC_HISTORY_KEY.format(traffic_class, ch), 86400)
            await tc_pipe.execute()
            
            # Обновляем TC_BEST в self_learning_loop (сравниваем avg_latency по каналам)
            tc_best_updates = {}
            for key in self._tc_lat:
                parts = key.split(":", 2)
                if len(parts) == 3:
                    traffic_class, ch = parts[1], parts[2]
                    lats = self._tc_lat[key]
                    if lats:
                        avg = sum(lats) / len(lats)
                        if traffic_class not in tc_best_updates or avg < tc_best_updates[traffic_class][1]:
                            tc_best_updates[traffic_class] = (ch, avg)
            for tc, (best_ch, avg_lat) in tc_best_updates.items():
                current_best = await r.get(TC_BEST_KEY.format(tc))
                if current_best != best_ch:
                    await r.setex(TC_BEST_KEY.format(tc), 86400, best_ch)
                    print(f"[TC] {tc} → {best_ch} ({avg_lat:.0f}ms)")
            
            self._rs_sent.clear()
            self._rs_failed.clear()
            self._rs_lat.clear()
            self._tc_sent.clear()
            self._tc_failed.clear()
            self._tc_lat.clear()
        except Exception as e:
            print(f"[FlushRoute] Error: {e}")
        self._rs_last_flush = time.time()

    async def connect_channel(self, host, port, name, unix_path=None):
        """Connect to channel: Unix socket (Phase 3) → TCP fallback.

        Быстрое подключение: 1 попытка + 1 таймаут для non-critical каналов (gossip).
        На-demand переподключение — через _reconnect_gossip_shard().
        """
        # Unix first
        if unix_path:
            for _ in range(2):
                try:
                    r, w = await asyncio.wait_for(
                        asyncio.open_unix_connection(unix_path),
                        timeout=1)  # 1 сек вместо ACK_CONNECT_TIMEOUT
                    print(f"[Router] ✅ Channel '{name}' (Unix)")
                    return w
                except (FileNotFoundError, ConnectionRefusedError, asyncio.TimeoutError, OSError):
                    if _ == 0:
                        print(f"[Router] ⏳ '{name}' Unix not ready, fallback TCP...")
                    await asyncio.sleep(0.5)

        # TCP fallback — быстро, без спама
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=1.5
            )
            print(f"[Router] ✅ Channel '{name}' ({host}:{port})")
            return w
        except (ConnectionRefusedError, asyncio.TimeoutError, OSError):
            print(f"[Router] ⏳ '{name}' not ready ({host}:{port}) — will retry on-demand")
            return None

    async def ensure_channels(self):
        """Параллельное подключение ко всем каналам (Unix → TCP fallback)."""
        channels = [
            self.connect_channel(CR_HOST, CR_PORT, "mesh", UNIX_CR_SOCK),
        ]
        # Gossip shards
        for i, (port, usock) in enumerate(zip(GOSSIP_PORTS, UNIX_GOSSIP_SOCKS)):
            channels.append(self.connect_channel("127.0.0.1", port, f"gossip:{i}", usock))
        
        results = await asyncio.gather(*channels)
        
        self._cr_writer = results[0]
        self._gossip_writers = [w for w in results[1:] if w is not None]
        
        # ═══ Connect to all 5 nostr bridge shards ═══
        # Инициализируем список с None для всех 5 шардов, чтобы индексы совпадали
        self._nostr_writers = [None] * 5
        connected_count = 0
        for i, port in enumerate(NOSTR_GW_PORTS):
            try:
                r, w = await asyncio.wait_for(
                    asyncio.open_connection(NOSTR_GW_HOST, port), timeout=2
                )
                self._nostr_writers[i] = w  # Замещаем None на writer
                connected_count += 1
                print(f"[Router] ✅ nostr shard-{i} connected (:{port})")
            except Exception:
                print(f"[Router] ⏳ nostr shard-{i} not ready (:{port}) — will retry on-demand")
                # Шард остаётся None в списке
        
        # Content Router v2 (:9920)
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", CR_V2_PORT), timeout=2
            )
            self._cr_v2_writer = w
            print(f"[Router] ✅ CR v2 connected (:{CR_V2_PORT})")
        except Exception as e:
            print(f"[Router] ⚠️ CR v2 not available: {e}")
        
        alive_nostr = len([w for w in self._nostr_writers if w is not None])
        print(f"[Router]    Channels: mesh {'✓' if self._cr_writer else '✗'} "
              f"nostr({alive_nostr}/5) "
              f"gossip({len(self._gossip_writers)}/5) direct ✓")
    
    async def _reconnect_nostr_shard(self, shard_idx: int):
        """Переподключение к nostr шарду. Замещает элемент на месте, не добавляет дубль."""
        if shard_idx < 0 or shard_idx >= len(NOSTR_GW_PORTS):
            return
        port = NOSTR_GW_PORTS[shard_idx]
        print(f"[Router] 🔄 Reconnecting nostr shard {shard_idx} (:{port})...")
        for attempt in range(5):
            try:
                r, w = await asyncio.wait_for(
                    asyncio.open_connection(NOSTR_GW_HOST, port), timeout=2
                )
                # Убедиться, что список достаточно длинный
                while len(self._nostr_writers) <= shard_idx:
                    self._nostr_writers.append(None)
                self._nostr_writers[shard_idx] = w
                print(f"[Router] ✅ nostr shard {shard_idx} reconnected (:{port})")
                return
            except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
                await asyncio.sleep(2)
        print(f"[Router] ⚠️ nostr shard {shard_idx} reconnect failed")
        # На месте остаётся None, будет пропущен при записи

    async def _reconnect_gossip_shard(self, shard_idx: int):
        """Переподключение к gossip шарду при падении writer."""
        if shard_idx < 0 or shard_idx >= 5:
            return
        port = 9100 + shard_idx
        print(f"[Router] 🔄 Reconnecting gossip shard {shard_idx} (:{port})...")
        for attempt in range(5):
            try:
                r, w = await asyncio.wait_for(
                    asyncio.open_connection("127.0.0.1", port), timeout=2
                )
                self._gossip_writers[shard_idx] = w
                print(f"[Router] ✅ gossip shard {shard_idx} reconnected")
                return
            except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
                await asyncio.sleep(2)
        print(f"[Router] ⚠️ gossip shard {shard_idx} reconnect failed")
        self._gossip_writers[shard_idx] = None

    async def _reconnect_mesh(self):
        """Переподключение к mesh (CRV2 Unix socket) с exponential backoff.
        
        Backoff: 1 → 2 → 4 → 8 → 15 → 30 (cap) секунд.
        После восстановления — отправляет все накопленные сообщения.
        """
        backoff = [1, 2, 4, 8, 15, 30]
        print(f"[Router] 🔄 Reconnecting mesh (CRV2 Unix socket)...")
        for attempt, delay in enumerate(backoff):
            try:
                r, w = await asyncio.wait_for(
                    asyncio.open_unix_connection("/tmp/snin/cr.sock"), timeout=3
                )
                self._cr_writer = w
                print(f"[Router] ✅ mesh (CRV2) reconnected (attempt {attempt + 1})")
                # ═══ CB разблокировка и сброс health при реконнекте ═══
                self._channel_health["mesh"] = {"ok": 0, "fail": 0, "avg_ms": 0}
                self._cb_recovery_count["mesh"] = self._cb_recovery_threshold  # моментальное восстановление
                if self._cb.is_blocked("mesh"):
                    self._cb._blocked_until.pop("mesh", None)
                    print(f"[Router] 🩺 CB mesh unblocked on reconnect")
                # Сбросить накопленные сообщения
                await self._flush_pending_queue()
                return
            except (FileNotFoundError, ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
                if attempt == 0:
                    print(f"[Router] ⏳ mesh (CRV2) not ready, retrying...")
                elif attempt < len(backoff) - 1:
                    print(f"[Router] ⏳ mesh retry {attempt + 1}/{len(backoff)} in {delay}s...")
                await asyncio.sleep(delay)
        print(f"[Router] ⚠️ mesh (CRV2) reconnect failed after {len(backoff)} attempts")
        self._cr_writer = None

    async def _flush_pending_queue(self):
        """Отправить все накопленные сообщения после восстановления mesh."""
        if not self._pending_mesh_queue:
            return
        count = len(self._pending_mesh_queue)
        print(f"[Router] 📦 Flushing {count} pending mesh messages...")
        to_send = list(self._pending_mesh_queue)
        self._pending_mesh_queue.clear()
        
        if self._cr_writer:
            payload = b"".join(json.dumps(m) + b"\n" for m in to_send)
            try:
                self._cr_writer.write(payload)
                await asyncio.wait_for(self._cr_writer.drain(), timeout=5)
                print(f"[Router] ✅ {count} pending messages flushed")
            except Exception as e:
                self._pending_mesh_queue.extend(to_send)
                print(f"[Router] ⚠️ Flush failed: {e}, {len(self._pending_mesh_queue)} in queue")
                self._cr_writer = None
                asyncio.ensure_future(self._reconnect_mesh())

    # ═══ V8: DHT → GossipStream peer discovery ═══
    async def _dht_scan_peers(self):
        """Сканировать DHT в Redis на агентов с relay_addr.
        Для каждого найденного пира — подключить GossipStream.
        """
        if not self._gossip_stream:
            return

        r = await aredis()
        if not r:
            return

        try:
            all_agents = await r.hgetall("dht:agents")
            connected = 0
            for pk_hex, raw in all_agents.items():
                try:
                    agent = json.loads(raw)
                except:
                    continue

                relay_addr = agent.get("relay_addr", "")
                if not relay_addr or relay_addr == "127.0.0.1:9105":
                    continue

                peer_id = f"agent:{pk_hex[:16]}"
                if peer_id in self._gossip_stream.pools:
                    continue

                host, port_str = relay_addr.rsplit(":", 1)
                port = int(port_str) if port_str.isdigit() else 9105

                print(f"[Router] 🔗 DHT → GossipStream: {peer_id} ({host}:{port})")
                ok = await self._gossip_stream.add_peer(peer_id, host, port)
                if ok:
                    connected += 1

            if connected:
                print(f"[Router] ✅ GossipStream подключил {connected} пиров из DHT")
        except Exception as e:
            print(f"[Router] ⚠️ DHT scan: {e}")

    async def _dht_scan_loop(self):
        """Периодический DHT scan (каждые 30 сек)."""
        while True:
            await self._dht_scan_peers()
            await asyncio.sleep(30)

    async def send_via_channel(self, channel: str, message: dict) -> dict:
        start = time.time()
        # Rate-limit send_via_channel log: каждые 50 сообщений
        self._send_counter = getattr(self, '_send_counter', 0) + 1
        if self._send_counter % 50 == 0:
            print(f"[Router] 🚀 send_via_channel({channel}) [#{self._send_counter}]")
        result = {"ok": False, "latency_ms": 0, "error": ""}

        # ═══ Фаза 2: проверка Circuit Breaker ═══
        if self._cb.is_blocked(channel):
            self.stats["cb_blocked"] += 1
            result["error"] = f"circuit_breaker: {channel} blocked"
            latency_ms = (time.time() - start) * 1000
            result["latency_ms"] = round(latency_ms, 1)
            return result

        try:
            if channel == "mesh" and self._cr_writer:
                # Фаза 6.7: Batch drain — буферизируем, drain каждые 10ms
                self._mesh_buf.extend(json.dumps(message) + b"\n")
                # ═══ Сообщение буферизировано — считаем успехом, даже если drain не сейчас ═══
                # Если не поставить ok=True, fail_rate всегда = 100% (сообщения сыпятся быстрее 10ms)
                result["ok"] = True
                now = time.time()
                if now - self._last_mesh_drain >= self._mesh_drain_interval:
                    try:
                        self._cr_writer.write(bytes(self._mesh_buf))
                        self._mesh_buf.clear()
                        await asyncio.wait_for(self._cr_writer.drain(), timeout=3)
                        self._last_mesh_drain = now
                        result["ok"] = True
                        # ═══ CB recovery: увеличить счётчик успешных ═══
                        self._cb_recovery_count["mesh"] = self._cb_recovery_count.get("mesh", 0) + 1
                        if self._cb_recovery_count["mesh"] >= self._cb_recovery_threshold:
                            if self._cb.is_blocked("mesh"):
                                self._cb._blocked_until.pop("mesh", None)
                                print(f"[Router] 🩺 CB mesh auto-recovered ({self._cb_recovery_count['mesh']} successful)")
                            self._cb_recovery_count["mesh"] = 0
                    except (ConnectionResetError, BrokenPipeError, OSError, asyncio.TimeoutError) as _eb:
                        # Не чистим буфер — сохраняем сообщение в pending queue
                        pending_msg = json.loads(self._mesh_buf.decode())
                        self._mesh_buf.clear()
                        self._cr_writer = None
                        self.stats["mesh_error"] += 1
                        result["error"] = f"mesh writer dead: {_eb}"
                        # ═══ Сохраняем в очередь неотправленных ═══
                        if len(self._pending_mesh_queue) < self._pending_mesh_max:
                            self._pending_mesh_queue.append(pending_msg)
                            print(f"[Router] 📥 Saved to pending queue ({len(self._pending_mesh_queue)})")
                        else:
                            print(f"[Router] ⚠️ Pending queue full, dropping message")
                        asyncio.ensure_future(self._reconnect_mesh())
                
                # ═══ Форвард подписчикам (агентам) — ВСЕГДА, не только при успешном drain ═══
                if self._event_subscribers:
                    await self._push_to_subscribers(message)
            elif channel == "mesh":  # _cr_writer is None, пробуем восстановить
                result = {"ok": False, "error": "mesh writer not connected"}
                asyncio.ensure_future(self._reconnect_mesh())

            elif channel == "nostr":
                # ═══ Multi-shard: write to all connected nostr bridges ═══
                # Список может содержать None для отключённых шардов

                # Убедиться, что есть хотя бы один живой шард
                alive_shards = [i for i, w in enumerate(self._nostr_writers) if w is not None]
                print(f"[Router] 🟣 nostr: {len(alive_shards)}/{len(NOSTR_GW_PORTS)} shards alive")
                if not alive_shards:
                    print(f"[Router] 🔴 All nostr shards dead, reconnecting publisher...")
                    # ⚡ BLOCKING reconnect (не ensure_future — ждём результат)
                    await self._reconnect_nostr_shard(0)
                    alive_shards = [i for i, w in enumerate(self._nostr_writers) if w is not None]
                    if not alive_shards:
                        self.stats["chan_fail:nostr"] += 1
                        result["error"] = "all nostr shards dead, reconnect failed"
                        return result

                # 🔧 Рекурсивная конвертация bytes→str во всей структуре
                def _bytes_to_str(v):
                    if isinstance(v, bytes):
                        return v.decode("utf-8", errors="replace")
                    elif isinstance(v, dict):
                        return {k: _bytes_to_str(val) for k, val in v.items()}
                    elif isinstance(v, list):
                        return [_bytes_to_str(item) for item in v]
                    elif isinstance(v, tuple):
                        return tuple(_bytes_to_str(item) for item in v)
                    return v
                
                # Конвертируем всё message в чистый dict без bytes
                clean_msg = _bytes_to_str(message)
                
                # Извлекаем поля из очищенного сообщения
                content_str = ""
                payload = clean_msg.get("payload", {})
                if isinstance(payload, dict):
                    try:
                        content_str_bytes = json.dumps(payload)
                        content_str = content_str_bytes.decode() if isinstance(content_str_bytes, bytes) else content_str_bytes
                    except Exception:
                        content_str = json.dumps(payload, default=str)
                        if isinstance(content_str, bytes):
                            content_str = content_str.decode()
                elif isinstance(payload, str):
                    content_str = payload
                else:
                    content_str = str(payload)
                
                nostr_msg = {
                    "kind": 39002,
                    "pubkey": clean_msg.get("pubkey", "router"),
                    "payload": {"text": content_str},
                    "content": content_str,
                    "tags": clean_msg.get("tags", []),
                    "created_at": int(time.time()),
                }
                # Bridge принимает kind:39002 → подписывает → публикует как kind:1 в Nostr
                try:
                    payload_bytes = json.dumps(nostr_msg) + b"\n"
                except Exception as e:
                    import traceback, sys
                    tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                    print(f"[Router] 🔴 nostr json.dumps CRASHED: {e}\n{tb}")
                    import json as _std_json
                    payload_bytes = _std_json.dumps(nostr_msg, default=str).encode() + b"\n"

                # Пишем во все живые шарды, отмечаем мёртвые по индексам
                ok_count = 0
                dead_shards = []
                alive_count = len([w for w in self._nostr_writers if w is not None])
                print(f"[Router] 🟣 nostr send: {alive_count}/{len(NOSTR_GW_PORTS)} shards alive, payload={len(payload_bytes)}b")
                for i, w in enumerate(self._nostr_writers):
                    if w is None:
                        # Уже мёртвый шард, пропускаем но пытаемся переподключить
                        asyncio.ensure_future(self._reconnect_nostr_shard(i))
                        continue

                    try:
                        w.write(payload_bytes)
                        await asyncio.wait_for(w.drain(), timeout=3)
                        ok_count += 1
                    except (BrokenPipeError, ConnectionResetError, asyncio.TimeoutError, OSError) as e:
                        dead_shards.append(i)
                        self._nostr_writers[i] = None  # Отмечаем как None, не удаляем
                        print(f"[Router] ⚠️ nostr shard-{i} writer died: {type(e).__name__}: {e}")
                        asyncio.ensure_future(self._reconnect_nostr_shard(i))
                    except Exception as e:
                        dead_shards.append(i)
                        self._nostr_writers[i] = None
                        print(f"[Router] 🔴 nostr shard-{i} UNCAUGHT: {type(e).__name__}: {e}")
                        asyncio.ensure_future(self._reconnect_nostr_shard(i))

                if ok_count > 0:
                    result["ok"] = True
                    result["shards_ok"] = ok_count
                    result["shards_total"] = len(self._nostr_writers)
                    result["shards_dead"] = len(dead_shards)
                    print(f"[Router] ✅ nostr sent to {ok_count} shards OK")
                else:
                    self.stats["chan_fail:nostr"] += 1
                    result["error"] = "all nostr shards unreachable"
                    print(f"[Router] 🔴 nostr send FAILED: {alive_count} alive, {len(dead_shards)} new dead")

            elif channel == "content_router" and self._cr_v2_writer:
                payload = json.dumps(message) + b"\n"
                try:
                    self._cr_v2_writer.write(payload)
                    await asyncio.wait_for(self._cr_v2_writer.drain(), timeout=3)
                    result["ok"] = True
                except Exception as e:
                    self._cr_v2_writer = None
                    self.stats["chan_fail:cr_v2"] += 1
                    result["error"] = str(e)
            
            # ═══ Вектор 4: ChequeBook канал (payment) ═══
            elif channel == "chequebook":
                kind = message.get("kind", 0)
                if kind == 30000:
                    try:
                        import httpx
                        async with httpx.AsyncClient(timeout=10) as c:
                            # send to ChequeBook API
                            resp = await c.post(
                                "http://127.0.0.1:9916/api/v1/payment",
                                json=message, headers={"Content-Type": "application/json"}
                            )
                            if resp.status_code == 200:
                                result["ok"] = True
                                result["msg"] = "payment processed"
                            else:
                                result["error"] = f"chequebook: {resp.status_code}"
                    except ImportError:
                        # fallback: direct HTTP
                        import urllib.request
                        payload = json.dumps(message)  # orjson returns bytes
                        req = urllib.request.Request(
                            "http://127.0.0.1:9916/api/v1/payment",
                            data=payload,
                            headers={"Content-Type": "application/json"}
                        )
                        try:
                            with urllib.request.urlopen(req, timeout=5) as resp:
                                if resp.status == 200:
                                    result["ok"] = True
                        except Exception as e:
                            result["error"] = f"chequebook: {e}"
                result["ok"] = True  # optimistic — если нет handler на платежи
            
            elif channel == "gossip" and self._gossip_writers:
                gossip_msg = dict(message)
                if "meta" not in gossip_msg:
                    gossip_msg["meta"] = {}
                if isinstance(gossip_msg["meta"], dict):
                    gossip_msg["meta"] = {**gossip_msg["meta"], "origin": "smart_router"}

                # Фаза 3: Consistent Hashing — выбираем шард по pubkey если есть
                target_pubkey = message.get("to") or message.get("pubkey", "")
                gossip_payload = json.dumps(gossip_msg) + b"\n"
                if target_pubkey and len(target_pubkey) > 8:
                    # Directed: шлём в 1 шард (по хешу pubkey)
                    shard_idx = gossip_shard_for(target_pubkey)
                    if shard_idx < len(self._gossip_writers):
                        w = self._gossip_writers[shard_idx]
                        try:
                            w.write(gossip_payload)
                            await asyncio.wait_for(w.drain(), timeout=2)
                            result["ok"] = True
                            self.stats[f"ch_shard:{shard_idx}"] += 1
                        except (asyncio.TimeoutError, Exception) as e:
                            self.stats["gossip_error"] += 1
                            result["error"] = str(e)
                            # Reconnect мёртвого шарда
                            if "closed" in str(e).lower() or "reset" in str(e).lower() or "broken" in str(e).lower():
                                self._gossip_writers[shard_idx] = None
                                asyncio.ensure_future(self._reconnect_gossip_shard(shard_idx))
                    else:
                        result["error"] = f"invalid shard {shard_idx}"
                else:
                    # Broadcast: batch write во все 5 → concurrent drain
                    to_drain = []
                    for idx, w in enumerate(self._gossip_writers):
                        if w is None:
                            continue
                        try:
                            w.write(gossip_payload)
                            to_drain.append(w)
                            result["ok"] = True
                        except Exception as e:
                            self.stats["gossip_error"] += 1
                            if "closed" in str(e).lower() or "reset" in str(e).lower() or "broken" in str(e).lower():
                                self._gossip_writers[idx] = None
                                asyncio.ensure_future(self._reconnect_gossip_shard(idx))
                    # Concurrent drain всех шардов
                    if to_drain:
                        await asyncio.gather(
                            *[asyncio.wait_for(w.drain(), timeout=2) for w in to_drain],
                            return_exceptions=True
                        )
                    self.stats["gossip_broadcast"] += 1

            elif channel == "fire-and-forget" and self._gossip_writers:
                # ═══ Fire-and-Forget: broadcast без confirm, без drain ═══
                gossip_msg = dict(message)
                if "meta" not in gossip_msg:
                    gossip_msg["meta"] = {}
                if isinstance(gossip_msg["meta"], dict):
                    gossip_msg["meta"] = {**gossip_msg["meta"], "origin": "smart_router", "channel": "fire-and-forget"}
                gossip_payload = json.dumps(gossip_msg) + b"\n"
                
                ok_count = 0
                for idx, w in enumerate(self._gossip_writers):
                    if w is None:
                        continue
                    try:
                        w.write(gossip_payload)
                        ok_count += 1
                    except Exception:
                        self._gossip_writers[idx] = None
                        asyncio.ensure_future(self._reconnect_gossip_shard(idx))
                
                # Fire-and-forget: не ждём drain — возвращаем сразу
                result["ok"] = ok_count > 0
                result["shards_ok"] = ok_count
                result["latency_ms"] = round((time.time() - start) * 1000, 1)
                self.stats["fire_forget_sent"] += 1
                if ok_count == 0:
                    self.stats["fire_forget_fail"] += 1
                    result["error"] = "all gossip shards dead"

            elif channel == "gossip_data":
                # V8: GossipStream — data channel между реле
                if self._gossip_stream:
                    payload = message.get("payload", message.get("content", {}))
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload)
                        except:
                            payload = {"text": payload}
                    bcast = await self._gossip_stream.broadcast(payload)
                    result["ok"] = any(bcast.values()) if bcast else False
                    if bcast:
                        self.stats["gossip_data_sent"] += sum(1 for v in bcast.values() if v)
                else:
                    result["error"] = "gossip_stream not initialized"
                    self.stats["chan_fail:gossip_data"] += 1

            elif channel == "direct":
                to = message.get("to", "")
                r = await aredis()
                if r and to:
                    # DHT lookup: сначала Redis hash, потом старые ключи
                    agent_data = await r.hget(f"dht:agents", to)
                    if not agent_data:
                        agent_data = await r.hget(f"dht:agents", to[:16])
                    if not agent_data:
                        agent_data = await r.get(f"dht:agent:{to[:16]}")
                    if not agent_data:
                        agent_data = await r.get(f"dht:{to[:16]}")
                    if agent_data:
                        info = json.loads(agent_data)
                        ip = info.get("ip", "127.0.0.1")
                        port = info.get("tcp_port", info.get("port", 9932))
                        try:
                            r2, w2 = await asyncio.wait_for(
                                asyncio.open_connection(ip, int(port)), timeout=2
                            )
                            w2.write(json.dumps(message) + b"\n")
                            await w2.drain()
                            w2.close()
                            result["ok"] = True
                        except Exception as e:
                            result["error"] = f"conn: {e}"
                    else:
                        result["error"] = "agent not in DHT"
                else:
                    result["error"] = "no DHT"
            else:
                result["error"] = f"unknown channel '{channel}'"

        except asyncio.TimeoutError:
            result["error"] = f"{channel}_timeout"
            self.stats[f"timeout:{channel}"] += 1
        except Exception as e:
            result["error"] = str(e)
            import traceback
            print(f"[Router] 💥 send_via_channel({channel}) exception: {type(e).__name__}: {e}")
            traceback.print_exc()

        latency_ms = (time.time() - start) * 1000
        result["latency_ms"] = round(latency_ms, 1)

        # ═══ Фаза 2: Circuit Breaker — инциденты ═══
        # Только если сообщение ДОСТАВЛЕНО, но медленно
        # mesh: выше порог (1000ms вместо 500ms)
        threshold = self._cb.mesh_latency_threshold_ms if channel == "mesh" else self._cb.latency_threshold_ms
        if result["ok"] and latency_ms > threshold:
            self._cb.record_incident(channel)
            self.stats[f"cb_incident:{channel}"] += 1
        
        # Ошибка отправки (buffer full, drain timeout) — НЕ инцидент CB
        # Это нормальная backpressure при высокой нагрузке
        # CB сработает отдельно при fail_rate > 50% в self_learning_loop

        # Фаза 1: обновляем health канала в реальном времени
        health = self._channel_health.get(channel)
        if health and channel in ("mesh", "gossip", "nostr", "direct"):
            if result["ok"]:
                health["ok"] += 1
            else:
                health["fail"] += 1
            if result["ok"]:
                prev = health["avg_ms"]
                health["avg_ms"] = prev * 0.9 + latency_ms * 0.1 if prev else latency_ms

        return result

    # ─── Фаза 1: Self-Learning ────────────────────────────────────────
    # ─── Фаза 1: Self-Learning (Redis + in-memory fallback) ────────────
    async def self_learning_loop(self):
        """Каждые N секунд: анализирует маршруты через Redis (или in-memory).
        
        Двухуровневая архитектура:
        1. Redis доступен → используем route:history и tc:stats для точного выбора
        2. Redis недоступен → in-memory _channel_health как fallback
        """
        while True:
            await asyncio.sleep(self._learning_interval)
            try:
                now = time.time()
                r = await aredis()
                changes = []

                # ═══ Блок 1: Circuit Breaker + Channel Health (всегда in-memory) ═══
                for ch, h in self._channel_health.items():
                    total = h["ok"] + h["fail"]
                    if total > 5:
                        fail_rate = h["fail"] / total
                        if fail_rate > 0.5:
                            self._cb.record_incident(ch)
                            changes.append(f"🚨 CB {ch} fail_rate={fail_rate:.0%}")
                        elif fail_rate > 0.3:
                            changes.append(f"❌ {ch} fail_rate={fail_rate:.0%} → congestion")
                        elif h["avg_ms"] > 0 and h["avg_ms"] > 200:
                            changes.append(f"⚠️ {ch} slow={h['avg_ms']:.0f}ms")

                # CB recovery + сброс health чтобы не ре-блокировать
                for ch in list(self._cb._blocked_until.keys()):
                    if not self._cb.is_blocked(ch):
                        changes.append(f"🔓 CB {ch} → unblocked")
                        self._channel_health[ch] = {"ok": 0, "fail": 0, "avg_ms": 0}

                # ═══ Блок 2: Анализ маршрутов ═══
                if r:
                    # ─── Tier 1: Redis — per-агент статистика ───
                    all_agents = set()
                    for key in await r.keys("route:history:*:*"):
                        parts = key.split(":")
                        if len(parts) >= 4:
                            all_agents.add(parts[2])

                    for agent in list(all_agents)[:50]:
                        best_ch = None
                        best_avg = 9999
                        for ch in ("mesh", "gossip", "nostr", "direct"):
                            stats_key = ROUTE_STATS_KEY.format(agent, ch)
                            stats = await r.hgetall(stats_key)
                            if not stats:
                                continue
                            avg_lat = stats.get("avg_latency")
                            if avg_lat and avg_lat != "?":
                                try:
                                    avg_f = float(avg_lat)
                                    if avg_f < best_avg:
                                        best_avg = avg_f
                                        best_ch = ch
                                except ValueError:
                                    continue
                        if best_ch:
                            current_best = await r.get(ROUTE_BEST_KEY.format(agent))
                            if current_best != best_ch:
                                await r.setex(ROUTE_BEST_KEY.format(agent), 86400, best_ch)
                                self._best_channel[agent] = best_ch
                                changes.append(f"↪️ {agent[:8]} → {best_ch} ({best_avg:.0f}ms) [Redis]")

                    # Traffic Class stats
                    tc_keys = await r.keys("tc:stats:*:*")
                    tc_analysed = set()
                    for tc_key in tc_keys:
                        parts = tc_key.split(":")
                        if len(parts) >= 4:
                            traffic_class = parts[2]
                            if traffic_class in tc_analysed:
                                continue
                            tc_analysed.add(traffic_class)
                            best_tc_ch = None
                            best_tc_avg = 9999
                            for ch in ("mesh", "gossip", "nostr", "content_router", "direct"):
                                stats = await r.hgetall(TC_STATS_KEY.format(traffic_class, ch))
                                if not stats:
                                    continue
                                avg_lat = stats.get("avg_latency")
                                if avg_lat and avg_lat != "?":
                                    try:
                                        avg_f = float(avg_lat)
                                        if avg_f < best_tc_avg:
                                            best_tc_avg = avg_f
                                            best_tc_ch = ch
                                    except ValueError:
                                        continue
                            if best_tc_ch:
                                current = await r.get(TC_BEST_KEY.format(traffic_class))
                                if current != best_tc_ch:
                                    await r.setex(TC_BEST_KEY.format(traffic_class), 86400, best_tc_ch)
                                    changes.append(f"🔀 tc:{traffic_class} → {best_tc_ch} ({best_tc_avg:.0f}ms) [Redis]")

                    # Auto-tuning весов политик
                    health_scores = {}
                    for ch, h in self._channel_health.items():
                        total = h["ok"] + h["fail"]
                        if total >= 10:
                            safe_avg = max(1.0, h["avg_ms"])
                            score = h["ok"] / max(1, total) * 100 / safe_avg
                            health_scores[ch] = score
                        else:
                            # Неиспользованные каналы — даём нейтральный скор (не penalize!)
                            health_scores[ch] = 50.0  # neutral, не 0
                    
                    # Gossip-first: boost gossip by default
                    if "gossip" in health_scores and health_scores.get("gossip", 0) < 30:
                        health_scores["gossip"] = max(health_scores["gossip"], 50.0)
                    
                    if health_scores:
                        max_score = max(health_scores.values())
                        if max_score > 0:
                            for kind_range in await r.hkeys(POLICY_KEY):
                                raw = await r.hget(POLICY_KEY, kind_range)
                                if not raw:
                                    continue
                                weights = json.loads(raw)
                                old_weights = dict(weights)
                                for ch in list(weights.keys()):
                                    ch_score = health_scores.get(ch, 0)
                                    rel_score = ch_score / max_score
                                    if rel_score < 0.3:
                                        weights[ch] = round(weights.get(ch, 0.5) * 0.5, 2)
                                        if weights[ch] < 0.05:
                                            weights[ch] = 0.05
                                    elif rel_score > 0.8:
                                        weights[ch] = round(weights.get(ch, 0.5) * 1.2, 2)
                                        if weights[ch] > 0.95:
                                            weights[ch] = 0.95
                                total_w = sum(weights.values())
                                if total_w > 0:
                                    for ch in weights:
                                        weights[ch] = round(weights[ch] / total_w, 3)
                                # 🔒 Gossip-first enforcement: gossip ≥ mesh
                                if "gossip" not in weights:
                                    weights["gossip"] = 1.0
                                if weights.get("gossip", 0) < weights.get("mesh", 0):
                                    weights["gossip"] = weights["mesh"]
                                    total_w = sum(weights.values())
                                    for ch in weights:
                                        weights[ch] = round(weights[ch] / total_w, 3)
                                if weights != old_weights:
                                    await r.hset(POLICY_KEY, kind_range, json.dumps(weights))
                                    changes.append(f"⚙️ policy {kind_range}: {old_weights} → {weights}")

                else:
                    # ─── Tier 2: In-memory fallback ───
                    healthiest = min(self._channel_health.items(),
                                     key=lambda x: x[1]["avg_ms"] if x[1]["avg_ms"] > 0 else 9999)
                    if healthiest and healthiest[1]["avg_ms"] > 0:
                        best_ch = healthiest[0]
                        for agent in set(self._best_channel.keys()) | {"default"}:
                            current = self._best_channel.get(agent)
                            if current != best_ch:
                                self._best_channel[agent] = best_ch
                                changes.append(f"↪️ best_channel → {best_ch} ({healthiest[1]['avg_ms']:.0f}ms) [in-mem]")
                                break

                # ═══ Блок 3: Сервисные операции ═══
                # Сброс health раз в 5 циклов
                if int(now / self._learning_interval) % 5 == 0:
                    for ch in self._channel_health:
                        self._channel_health[ch] = {"ok": 0, "fail": 0, "avg_ms": 0}

                if changes:
                    for c in changes:
                        print(f"[Router] 📊 {c}")
                    self.stats["learning_actions"] += len(changes)

                # Policy cache reload + sync best channels
                if int(now / self._learning_interval) % 4 == 0:
                    await self._load_policy_cache()
                    await self._sync_best_channels()

                # Flush route stats every ~30s
                if now - self._rs_last_flush >= 30:
                    await self._flush_route_stats()

                # Mesh buffer flush
                if self._mesh_buf and self._cr_writer:
                    try:
                        self._cr_writer.write(bytes(self._mesh_buf))
                        self._mesh_buf.clear()
                        await asyncio.wait_for(self._cr_writer.drain(), timeout=3)
                    except (ConnectionResetError, BrokenPipeError, OSError, asyncio.TimeoutError) as _eb:
                        print(f"[SelfLearn] ⚠️ Mesh writer dead ({_eb}), resetting")
                        self._cr_writer = None
                        self._mesh_buf.clear()

                self._last_learning = now

            except Exception as e:
                print(f"[SelfLearn] Error: {e}")

    async def route_message(self, msg: dict) -> dict:
        self.stats["received"] += 1

        meta = msg.get("meta", {})
        from_agent = msg.get("from", msg.get("pubkey", "?"))[:16]
        to_agent = msg.get("to", "broadcast")[:16]
        kind = msg.get("kind", 39002)
        priority = meta.get("priority", "normal")
        channel_pref = meta.get("channel", "auto")

        # ═══ Rate Limiter: token bucket per agent ═══
        if not self._rate_limiter.allow(from_agent):
            self.stats["rate_limited"] += 1
            return {"ok": False, "error": "rate_limited", "channel": "drop"}

        # ═══ L5T: Agent-to-Agent → проверка online/offline ═══
        to_full = msg.get("to", "")
        if to_full and to_full != "broadcast" and self.l5t:
            # Это agent-to-agent сообщение — проверяем статус получателя
            is_online = await self.l5t.heartbeat.is_online(to_full)
            print(f"[Router] L5T: agent msg to={to_full[:16]}... online={is_online}")
            if not is_online:
                # Получатель офлайн → Dead-Letter Queue
                self.stats["l5t:queued"] += 1
                try:
                    result = await self.l5t.queue_to_dlq(msg)
                    result["channel"] = "deadletter"
                    if "seq" in msg:
                        result["seq"] = msg["seq"]
                    print(f"[Router] L5T: ✅ queued to DLQ hash={result.get('dlq_hash','?')[:8]}...")
                    return result
                except Exception as e:
                    print(f"[Router] L5T: ❌ DLQ error: {e}")
                    import traceback; traceback.print_exc()
                    result = {"ok": False, "error": f"dlq_failed: {e}"}
                    if "seq" in msg:
                        result["seq"] = msg["seq"]
                    return result
            self.stats["l5t:direct_agent"] += 1

        # ═══ Heartbeat обработка ═══
        if kind == HEARTBEAT_KIND and self.l5t:
            from_full = msg.get("from", msg.get("pubkey", ""))
            hb_result = await self.l5t.process_incoming_heartbeat(from_full, meta)
            return {
                "ok": True,
                "channel": "heartbeat",
                "status": "ack",
                "online": hb_result.get("online", True),
                "agent": hb_result.get("name", ""),
                "pending_messages": hb_result.get("pending_messages", 0),
            }
        
        # ═══ Вектор 3: Self-Learning по traffic_class ═══
        traffic_class = classify_traffic(kind, meta)
        self.stats[f"tc:{traffic_class}"] += 1

        # Шаг 1: выбор канала (с учётом traffic_class + self-learning)
        if channel_pref == "auto":
            # Базовая политика по kind
            policy = self.get_policy(kind)
            
            # Self-learning best канал для traffic_class
            tc_best = await self._get_tc_best(traffic_class)
            
            if tc_best and tc_best in policy:
                # 🔀 Multi-channel: bias toward best but preserve diversity
                chs = list(policy.keys())
                weights = [policy[c] * (3.0 if c == tc_best else 1.0) for c in chs]
                channel = random.choices(chs, weights=weights, k=1)[0]
            else:
                # Используем policy weights напрямую (не перезаписываем через TRAFFIC_CLASSES)
                chs = list(policy.keys())
                weights = [policy[c] for c in chs]
                channel = random.choices(chs, weights=weights, k=1)[0]
                if random.random() < 0.1:  # log 10% for debug
                    print(f"[Router] DEBUG auto-pick: policy={policy} chs={chs} w={weights} → {channel}")
            
            # Фаза 2: если канал зациркуичен — перевыбираем
            if self._cb.is_blocked(channel):
                self.stats["cb_reroute"] += 1
                alt = [c for c in policy if not self._cb.is_blocked(c)]
                channel = alt[0] if alt else "mesh"
            # Фаза 1: если выбранный канал перегружен — перевыбираем
            health = self._channel_health.get(channel)
            if health and health["ok"] + health["fail"] > 5:
                fail_rate = health["fail"] / max(1, health["ok"] + health["fail"])
                if fail_rate > 0.3:
                    alt_channels = [c for c in policy if c != channel and not self._cb.is_blocked(c)]
                    if alt_channels:
                        alt_weights = [policy[c] for c in alt_channels]
                        channel = random.choices(alt_channels, weights=alt_weights, k=1)[0]
                        self.stats["congestion_reroute"] += 1
                elif health["avg_ms"] > 200:
                    self.stats["congestion_slow"] += 1
        elif channel_pref in ("direct", "mesh", "gossip", "nostr", "content_router", "chequebook", "gossip_data", "nostr_data", "fire-and-forget"):
            channel = channel_pref
            # Фаза 2: если явно запрошенный канал зациркуичен — mesh fallback
            if self._cb.is_blocked(channel):
                self.stats["cb_reroute_explicit"] += 1
                channel = "mesh"
        else:
            channel = "mesh"
        
        # ═══ Фаза 1: Reputation-weighted override ═══
        try:
            sender_pubkey = event.get("pubkey", "")
            if sender_pubkey:
                rep_weight = _get_reputation_weight(sender_pubkey)
                if rep_weight < 0.3:
                    # Низкая репутация → только mesh (контролируемый канал)
                    if channel in ("nostr", "gossip", "gossip_data", "nostr_data"):
                        self.stats["rep_low_reroute"] += 1
                        channel = "mesh"
        except Exception:
            pass
        
        # Шаг 2: собираем каналы для отправки (исключая CB-blocked)
        policy = self.get_policy(kind)
        channels_to_try = [ch for ch in [channel] if not self._cb.is_blocked(ch)]
        # ⭐ Fallback: если nostr в каналах — добавляем mesh как резерв
        if 'nostr' in channels_to_try and 'mesh' not in channels_to_try:
            channels_to_try.append('mesh')
        if not channels_to_try:
            # mesh заблокирован — пробуем другие каналы
            fallbacks = [c for c in ("nostr", "gossip", "gossip_data", "chequebook") if not self._cb.is_blocked(c)]
            if fallbacks:
                channels_to_try = fallbacks
                print(f"[Router] ⚡ CB mesh blocked, fallback to: {fallbacks}")
            else:
                channels_to_try = ["mesh"]  # все каналы заблокированы — последняя надежда
                print(f"[Router] ⚠️ All channels blocked, forcing mesh")
        
        # ═══ Вектор 4: kind:30000 → обязательно шлём в ChequeBook ═══
        if kind == 30000 and "chequebook" not in channels_to_try and not self._cb.is_blocked("chequebook"):
            channels_to_try.append("chequebook")

        # Мультиканал для высокого приоритета
        if priority == "high":
            extra_channels = [c for c in policy if c != channel and not self._cb.is_blocked(c)]
            if not extra_channels:
                extra_channels = [c for c in ("mesh", "gossip", "gossip_data", "nostr", "chequebook") if c != channel and not self._cb.is_blocked(c)]
            channels_to_try.extend(extra_channels[:1])

        self.stats[f"channel:{channel}"] += 1
        self.stats[f"priority:{priority}"] += 1
        self.stats[f"kind:{kind}"] += 1

        # Шаг 3: отправка
        print(f"[Router] 📨 route_message: channel={channel} channels_to_try={channels_to_try} priority={priority}")
        best_result = {"ok": False, "latency_ms": 9999, "error": "no channel"}
        ok_count = 0
        for ch in channels_to_try:
            result = await self.send_via_channel(ch, msg)
            if result["ok"]:
                self._record_route(from_agent, ch, result["latency_ms"], True)
                self._record_tc(traffic_class, ch, result["latency_ms"], True)
                self.stats[f"chan_ok:{ch}"] += 1
                ok_count += 1
                if result["latency_ms"] < best_result["latency_ms"]:
                    best_result = result
                    best_result["channel"] = ch
            else:
                self._record_route(from_agent, ch, result["latency_ms"], False)
                self._record_tc(traffic_class, ch, result["latency_ms"], False)
                self.stats[f"chan_fail:{ch}"] += 1

        # ═══ Content Router мультикаст: все kind дублируются в CR ═══
        if self._cr_v2_writer is None:
            now = time.time()
            if now - self._last_cr_reconnect < 30:
                pass  # rate-limit: не дёргать чаще раза в 30 сек
            else:
                self._last_cr_reconnect = now
                print(f"[Router] ⚠️ CR v2 writer is None, reconnecting...")
                try:
                    r, w = await asyncio.wait_for(
                        asyncio.open_connection("127.0.0.1", CR_V2_PORT), timeout=2
                    )
                    self._cr_v2_writer = w
                    print(f"[Router] ✅ CR v2 reconnected (:{CR_V2_PORT})")
                except Exception as e:
                    print(f"[Router] ❌ CR reconnect failed: {e}")
                    # Закрываем reader/writer если открылись, чтобы не текли FDs
                    try:
                        if 'r' in dir() and r: r.fp.close()
                    except: pass
                    try:
                        if 'w' in dir() and w: w.close()
                    except: pass
        if self._cr_v2_writer:
            print(f"[Router] 🔁 CR multicast for kind={msg.get('kind',0)} from={msg.get('from','?')[:12]}")
            try:
                cr_payload = json.dumps(msg) + b"\n"  # orjson returns bytes, no .encode()
                self._cr_v2_writer.write(cr_payload)
                await asyncio.wait_for(self._cr_v2_writer.drain(), timeout=1)
                self.stats["cr_v2_multicast"] += 1
            except Exception as ex:
                print(f"[Router] ❌ CR multicast FAIL: {type(ex).__name__}: {ex}")
                self.stats["cr_v2_multicast_fail"] += 1
                self._cr_v2_writer = None
                # Попробуем переподключиться (с rate-limit)
                now = time.time()
                if now - self._last_cr_reconnect >= 30:
                    self._last_cr_reconnect = now
                    try:
                        r, w = await asyncio.wait_for(
                            asyncio.open_connection("127.0.0.1", CR_V2_PORT), timeout=2
                        )
                        self._cr_v2_writer = w
                    except Exception:
                        pass
        
        # Если ничего не сработало — mesh как последняя надежда
        if not best_result["ok"] and "mesh" not in channels_to_try:
            r = await self.send_via_channel("mesh", msg)
            if r["ok"]:
                best_result = r
                best_result["channel"] = "mesh"
                self.stats["fallback_to_mesh"] += 1

        if best_result["ok"]:
            self.stats["forwarded"] += 1
            best_result["channels_used"] = ok_count
        else:
            self.stats["failed"] += 1
            # ═══ L5T: Dead-Letter Queue — если получатель офлайн ═══
            to_agent_full = msg.get("to", "")
            if to_agent_full and to_agent_full != "broadcast" and to_agent_full != "?":
                try:
                    from dead_letter import get_dlq
                    dlq = get_dlq()
                    dlq_result = await dlq.push(
                        from_pubkey=msg.get("from", msg.get("pubkey", "")),
                        to_pubkey=to_agent_full,
                        content=msg.get("content", json.dumps(msg).decode() if hasattr(json.dumps(msg), 'decode') else str(json.dumps(msg))),
                        kind=msg.get("kind", 39002),
                        priority=meta.get("priority", "normal"),
                    )
                    if dlq_result["ok"]:
                        self.stats["dlq_queued"] += 1
                        best_result["ok"] = True
                        best_result["dlq"] = True
                        best_result["dlq_hash"] = dlq_result["hash"]
                        print(f"[Router] 📥 DLQ queued for {to_agent_full[:12]}:{dlq_result['hash']}")
                except Exception as ex:
                    self.stats["dlq_error"] += 1

        # Add seq info to response
        if "seq" in msg:
            best_result["seq"] = msg["seq"]

        return best_result

    async def handle_client(self, reader, writer):
        peer = writer.get_extra_info("peername", ("?", 0))
        addr = f"{peer[0]}:{peer[1]}"

        # ═══ Фаза 2: Backpressure ═══
        self._concurrent += 1
        self.stats["connections"] += 1
        if self._concurrent > BP_MAX_CONCURRENT:
            self._bp_threshold_reached = True
            self.stats["backpressure_rejected"] += 1
            try:
                bp = {"retry_after": BP_RETRY_AFTER_SEC, "error": "backpressure",
                      "concurrent": self._concurrent, "max": BP_MAX_CONCURRENT}
                writer.write(json.dumps(bp) + b"\n")
                await writer.drain()
            except Exception:
                pass
            writer.close()
            self._concurrent -= 1
            self.stats["disconnects"] += 1
            return
        if self._concurrent > BP_MAX_CONCURRENT * 0.8:
            self.stats["backpressure_warning"] += 1

        try:
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=30)
                if not line:
                    break
                line = line.decode().strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    self.stats["bad_json"] += 1
                    continue

                # ═══ Фаза 0: verify_sig (optional, 0.05ms) ═══
                sig = msg.get("sig", "")
                pk = msg.get("pubkey", "")
                if sig and pk:
                    payload = msg.get("payload", {})
                # Level 2: Ed25519 verify в ProcessPool — гарантированно не блокирует event loop
                    sig_ok = await verify_ed25519_processpool_async(pk, payload, sig)
                    if sig_ok:
                        self.stats["signed_ok"] += 1
                        # DHT: регистрируем/обновляем подписанного агента
                        if self._dht:
                            agent_meta = {
                                "role": msg.get("from", "anonymous"),
                                "kind": msg.get("kind", 0),
                                "ip": msg.get("meta", {}).get("ip", "127.0.0.1"),
                                "tcp_port": int(msg.get("meta", {}).get("tcp_port", 9932)),
                            }
                            asyncio.ensure_future(self._safe_dht_refresh(pk, agent_meta))
                    else:
                        self.stats["signed_fail"] += 1
                        writer.write(b'{"ok":false,"error":"invalid sig"}\n')
                        try:
                            await writer.drain()
                        except Exception:
                            pass
                        continue
                else:
                    self.stats["unsigned_packets"] += 1

                # ═══ Фаза 8: Subscribe/Unsubscribe от агентов ═══
                kind = msg.get("kind", 0)
                if kind == "subscribe":
                    sub_id = self._sub_next_id
                    self._sub_next_id += 1
                    agent_name = msg.get("from", "anonymous")
                    self._event_subscribers[sub_id] = (writer, agent_name)
                    self.stats["subscribers"] = len(self._event_subscribers)
                    writer.write(json.dumps({"ok": True, "subscribed": True, "sub_id": sub_id}) + b"\n")
                    await writer.drain()
                    print(f"[Router] ✅ Agent '{agent_name}' subscribed (id={sub_id}, total={len(self._event_subscribers)})")
                    
                    # ═══ L5T: Flush pending DLQ messages to subscriber ═══
                    if self.l5t:
                        try:
                            pending = await self.l5t.sync_for_agent(agent_name, since=0)
                            if pending:
                                for dlq_msg in pending:
                                    try:
                                        content = dlq_msg.content
                                        if isinstance(content, (bytes, str)):
                                            payload = json.loads(content) if content else {}
                                    except:
                                        payload = {"raw": str(dlq_msg.content)[:100]}
                                    push_event = {
                                        "type": "push",
                                        "kind": dlq_msg.kind,
                                        "from": dlq_msg.from_pubkey,
                                        "to": agent_name,
                                        "meta": {"channel": "deadletter_sync", "dlq": True},
                                        "payload": payload,
                                        "dlq_hash": dlq_msg.hash,
                                    }
                                    data = json.dumps(push_event) + b"\n"
                                    writer.write(data)
                                await writer.drain()
                                print(f"[Router] 📬 Flushed {len(pending)} DLQ messages to {agent_name[:16]}...")
                        except Exception as e:
                            print(f"[Router] ⚠️ DLQ flush error: {e}")
                            import traceback; traceback.print_exc()
                    
                    continue
                elif kind == "unsubscribe":
                    sub_id = msg.get("sub_id", -1)
                    agent_name = msg.get("from", "unknown")
                    if sub_id in self._event_subscribers:
                        del self._event_subscribers[sub_id]
                        self.stats["subscribers"] = len(self._event_subscribers)
                        print(f"[Router] ❌ Agent '{agent_name}' unsubscribed (id={sub_id})")
                    writer.write(json.dumps({"ok": True, "unsubscribed": True}) + b"\n")
                    await writer.drain()
                    continue
                
                # ═══ Фаза 9: Push всем подписанным агентам (кроме отправителя) ═══
                from_agent = msg.get("from", "")
                
                # ═══ Фаза 4: Dedup check (до seq assignment — по payload) ═══
                is_dup = await self._dedup.is_duplicate(msg)
                if is_dup:
                    self.stats["dedup_dropped"] += 1
                    writer.write(json.dumps({"ok": False, "error": "duplicate", "channel": "dedup_drop"}) + b"\n")
                    await writer.drain()
                    continue
                
                # ═══ Фаза 3: seq_num assignment (before push) ═══
                to_full = msg.get("to", "")
                if to_full and to_full != "broadcast":
                    seq = await self._seq_tracker.next_seq(from_agent, to_full)
                    msg["seq"] = seq
                    if "meta" not in msg:
                        msg["meta"] = {}
                    msg["meta"]["seq"] = seq
                    self.stats["seq_assigned"] += 1
                
                event_for_push = {
                    "type": "push",
                    "kind": kind,
                    "from": from_agent,
                    "to": msg.get("to", ""),
                    "pubkey": msg.get("pubkey", ""),
                    "payload": msg.get("payload", msg.get("content", "")),
                    "meta": msg.get("meta", {}),
                    "seq": msg.get("seq"),
                    "ts": time.time(),
                }
                await self._push_to_subscribers(event_for_push, exclude_writer=writer)
                
                # pipeline_feed от RE — только push, без роутинга (избегаем цикла RE→SR→CRV2→RE)
                if kind == "pipeline_feed":
                    continue

                # ═══ Фаза 5: Priority Queue (вместо прямого route_message) ═══
                qm = await self._priority_queue.put(msg)
                # Клиент получает мгновенное подтверждение
                writer.write(json.dumps({
                    "ok": True,
                    "channel": "queued",
                    "priority": qm.priority.name,
                    "seq": msg.get("seq"),
                    "queue_depth": self._priority_queue.sizes["total"]
                }) + b"\n")
                await writer.drain()
                continue

        except asyncio.TimeoutError:
            self.stats["client_timeout"] += 1
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            self.stats["errors"] += 1
        finally:
            # Удаляем подписку этого клиента если была
            dead_ids = [sid for sid, (w, _) in self._event_subscribers.items() if w is writer]
            for sid in dead_ids:
                del self._event_subscribers[sid]
                self.stats["subscribers"] = len(self._event_subscribers)
            writer.close()
            self._concurrent -= 1
            self.stats["disconnects"] += 1

    async def _push_to_subscribers(self, event: dict, exclude_writer=None):
        """Разослать событие всем подписанным агентам.
        
        Фаза 3: agent-to-agent сообщения проходят через reorder buffer.
        Фаза 4: dedup-проверка перед отправкой.
        Broadcast идут напрямую.
        """
        if not self._event_subscribers:
            return
        
        # ═══ Фаза 4: Dedup check ═══
        if await self._dedup.is_duplicate(event):
            self.stats["dedup_push_dropped"] += 1
            return
        
        to_agent = event.get("to", "")
        from_agent = event.get("from", event.get("pubkey", "?"))[:16]
        seq = event.get("seq")
        
        # ═══ Фаза 3: Reorder buffer для agent-to-agent ═══
        if to_agent and to_agent != "broadcast" and seq is not None:
            ready_msgs = await self._reorder.deliver(from_agent, to_agent, seq, event)
            if not ready_msgs:
                # Сообщение буферизировано — не доставляем пока
                return
            # Доставляем готовые сообщения
            for ready_msg in ready_msgs:
                await self._push_single(ready_msg, exclude_writer)
        else:
            # Broadcast или без seq → доставляем сразу
            await self._push_single(event, exclude_writer)
    
    async def _push_single(self, event: dict, exclude_writer=None):
        """Отправить одно сообщение всем подписчикам."""
        dead_ids = []
        payload = json.dumps(event) + b"\n"
        for sid, (w, name) in list(self._event_subscribers.items()):
            if w is exclude_writer:
                continue
            try:
                w.write(payload)
                await w.drain()
            except (BrokenPipeError, ConnectionResetError, OSError):
                dead_ids.append(sid)
        for sid in dead_ids:
            del self._event_subscribers[sid]
            self.stats["subscribers"] = len(self._event_subscribers)

    async def _safe_dht_refresh(self, pubkey: str, meta: dict):
        """Безопасный DHT refresh в фоне (catch ошибок)."""
        try:
            if self._dht:
                await self._dht.refresh_agent(pubkey, meta)
        except Exception as e:
            print(f"[DHT] refresh error: {type(e).__name__}: {e}")

    # ═══ Фаза 3: Reorder background loops ═══
    async def _reorder_timeout_loop(self):
        """Фоновый цикл: проверка тайм-аутов reorder buffer."""
        while True:
            await asyncio.sleep(1.0)
            try:
                timed_out = await self._reorder.check_timeouts()
                if timed_out:
                    for from_agent, to_agent, msg in timed_out:
                        await self._push_single(msg)
            except Exception as e:
                print(f"[Reorder] timeout check error: {e}")

    async def _reorder_cleanup_loop(self):
        """Фоновый цикл: очистка старых пар reorder buffer."""
        while True:
            await asyncio.sleep(300.0)
            try:
                removed = self._reorder.cleanup_old_pairs()
                if removed:
                    print(f"[Reorder] Cleaned {removed} stale pairs")
            except Exception as e:
                print(f"[Reorder] cleanup error: {e}")

    # ═══ Фаза 4: Dedup background loop ═══
    async def _dedup_cleanup_loop(self):
        """Фоновый цикл: очистка dedup-кеша."""
        while True:
            await asyncio.sleep(120.0)
            try:
                await self._dedup.cleanup()
            except Exception as e:
                print(f"[Dedup] cleanup error: {e}")

    # ═══ Фаза 5: Priority Queue workers + aging ═══
    async def _priority_worker(self, worker_id: int):
        """Worker: вытаскивает из priority queue и обрабатывает.
        
        Стратегия: после каждой обработки проверяет CRITICAL очередь.
        Это гарантирует, что CRITICAL не ждёт за NORMAL/HIGH.
        """
        print(f"[PQ] Worker #{worker_id} started")
        while True:
            try:
                # ═══ СНАЧАЛА проверяем CRITICAL (мгновенно, без ожидания) ═══
                qm = await self._priority_queue.try_get_critical()
                if qm is None:
                    # Нет CRITICAL — берём любое из очереди
                    qm = await self._priority_queue.get()
                

                await self.route_message(qm.msg)
                
                wait = qm.age
                if wait > 1.0:
                    print(f"[PQ] Worker #{worker_id}: p={qm.priority.name} wait={wait:.2f}s")
                
                self.stats["pq_processed"] += 1
                
            except Exception as e:
                self.stats["pq_errors"] += 1
                print(f"[PQ] Worker #{worker_id} error: {e}")
                await asyncio.sleep(0.1)

    async def _priority_aging_loop(self):
        """Фоновый цикл: проверка aging сообщений."""
        while True:
            await asyncio.sleep(5.0)
            try:
                promoted = await self._priority_queue.age_messages()
                if promoted:
                    print(f"[PQ] Aged up {promoted} messages")
            except Exception as e:
                print(f"[PQ] Aging error: {e}")

    async def run(self):
        global _GLOBAL_ROUTER
        _GLOBAL_ROUTER = self
        await apply_policies()
        await self.ensure_channels()

        # Фаза 6.2: загружаем policy cache при старте
        await self._load_policy_cache()
        await self._sync_best_channels()
        print(f"[Router]    Policy cache: {len(self._policy_cache)} rules (in-memory)")

        # ═══ Фаза 1: DHT Kademlia ═══
        try:
            from dht_node import DHTNode, DHT_PORT
            self._dht = DHTNode(
                port=DHT_PORT,
                agent_pubkey="smart_router",
                agent_meta={"ip": "127.0.0.1", "tcp_port": 9932, "role": "router",
                            "relay_addr": f"127.0.0.1:{self._gossip_stream.listen_port if self._gossip_stream else 9105}"}
            )
            await self._dht.start()
            print(f"[Router] ✅ DHT node ready (agents={len(await self._dht.list_agents())})")
        except Exception as e:
            print(f"[Router] ⚠️ DHT init error: {e}")

        n_gossip = len(self._gossip_writers)
        server = await asyncio.start_server(self.handle_client, LISTEN_HOST, LISTEN_PORT)
        addr = server.sockets[0].getsockname()
        print(f"[Router] 🧠 Smart Router v2 — {addr[0]}:{addr[1]}")
        print(f"[Router]    Channels: mesh ✓ nostr ✓ gossip({n_gossip}/5) direct ✓ gossip_data {'✓' if self._gossip_stream else '✗'}")
        r = await aredis()
        n_policies = len(await r.hkeys(POLICY_KEY)) if r else 0
        print(f"[Router]    Policies: {n_policies} rules in Redis")
        print(f"[Router]    Route-learning: ON")
        print(f"[Router]    Phase 4: orjson + Health :{HEALTH_PORT}")
        print(f"[Router]    Phase 3: Message Ordering ENABLED (seq_num + reorder)")
        print(f"[Router]    Phase 4: Message Deduplication ENABLED")
        print(f"[Router]    Phase 5: Priority Queue ENABLED ({self._pq_workers} workers, aging)")

        async def health_check(reader, writer):
            """HTTP endpoint: /health — общая статистика, /dht — детали DHT."""
            try:
                request_line = await reader.readline()
                path = request_line.decode().split(" ")[1] if b" " in request_line else "/"
                n_clients = len(getattr(self, 'agent_states', {}))
                n_conc = self._concurrent
                r = await aredis()
                redis_ok = r is not None

                if path == "/dht" and self._dht:
                    agents = await self._dht.list_agents()
                    nodes = await self._dht.list_nodes()
                    body = json.dumps({
                        "running": True,
                        "node_id": self._dht.server.node.id.hex() if self._dht.server and self._dht.server.node else "?",
                        "node_pubkey": "smart_router",
                        "agents": agents,
                        "nodes": nodes,
                        "n_agents": len(agents),
                        "n_nodes": len(nodes),
                    })
                elif path == "/dht":
                    body = json.dumps({"running": False})
                else:
                    body = json.dumps({
                    "status": "ok",
                    "uptime": int(time.time() - self.stats.get("start_time", time.time())),
                    "channels": {
                        "mesh": self._cr_writer is not None,
                        "nostr": len(self._nostr_writers),
                        "gossip": sum(1 for w in self._gossip_writers if w is not None),
                        "direct": True,
                    },
                    "clients": n_clients,
                    "concurrent": n_conc,
                    "redis": redis_ok,
                    "dht": {
                        "running": self._dht is not None and self._dht.is_running(),
                        "n_agents": len(await self._dht.list_agents()) if self._dht else 0,
                        "n_nodes": len(await self._dht.list_nodes()) if self._dht else 0,
                    } if self._dht else {"running": False},
                    "stats": {k: v for k, v in self.stats.items()
                              if isinstance(v, (int, float))},
                    "version": "2.4.0",
                })
                resp = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: application/json\r\n"
                    b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                    b"Access-Control-Allow-Origin: *\r\n"
                    b"Connection: close\r\n"
                    b"\r\n" + body
                )
                writer.write(resp)
                await writer.drain()
            except Exception as e:
                err = str(e).encode()
                resp = (
                    b"HTTP/1.1 500 ERROR\r\n"
                    b"Content-Type: text/plain\r\n"
                    b"Content-Length: " + str(len(err)).encode() + b"\r\n"
                    b"\r\n" + err
                )
                writer.write(resp)
                await writer.drain()
            finally:
                writer.close()
        
        health = await asyncio.start_server(health_check, "127.0.0.1", HEALTH_PORT)
        
        async with server, health:
            await asyncio.gather(
                server.serve_forever(),
                health.serve_forever(),
                self.self_learning_loop(),
            )

# ═══ Entry point — запуск напрямую или через router_api.py ═══
if __name__ == "__main__":
    print("[SmartRouter] Starting via __main__...")
    from router_api import run_router
    asyncio.run(run_router())

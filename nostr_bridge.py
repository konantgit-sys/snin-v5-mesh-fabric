#!/usr/bin/env python3
"""Nostr Bridge — двусторонний Nostr ↔ Mesh мост.

Архитектура:
  Nostr (WebSocket, 101 релей)
    ↓ kind:1 → kind:39002  (Nostr → Mesh)
    ↑ kind:39002 → kind:1  (Mesh → Nostr)

Протоколы:
  NIP-42 (AUTH) — подпись challenge при подключении
  NIP-65 (Relay List) — публикация kind:10002 с нашими релеями
  NIP-01 (Basic protocol) — kind:1 публикации

Всё идёт через SmartRouter (9932) → выбор канала → дальше по конвейеру.

Запуск:
  python3 nostr_bridge.py
  
Зависимости:
  pip install websockets
"""

import asyncio
import hashlib
import json
import orjson
import os
import sys
import time
import signal
import argparse

# ═══ Monkey-patch: websockets connection_lost при SSL/сетевых ошибках ═══
import websockets.asyncio.connection as _ws_conn
_ws_orig_connection_lost = _ws_conn.Connection.connection_lost
def _ws_safe_connection_lost(self, exc):
    if not hasattr(self, 'recv_messages'):
        # connection_made не был вызван — SSL/сетевая ошибка на handshake
        return
    return _ws_orig_connection_lost(self, exc)
_ws_conn.Connection.connection_lost = _ws_safe_connection_lost

import websockets

sys.path.insert(0, "/home/agent/data/sites/relay-mesh")

from ttl_cache import TTLCache
from cpu_worker import hash_sha256_async, sign_event_full_async

# ═══ Shard CLI parser ═══
_shard_parser = argparse.ArgumentParser()
_shard_parser.add_argument("--shard-id", type=int, default=0, help="Shard index (0-based)")
_shard_parser.add_argument("--total-shards", type=int, default=1, help="Total shard count")
_shard_args, _ = _shard_parser.parse_known_args()
SHARD_ID = _shard_args.shard_id
TOTAL_SHARDS = _shard_args.total_shards

# ─── Настройки ────────────────────────────────────────────────────────
SMART_ROUTER_HOST = "127.0.0.1"
SMART_ROUTER_PORT = 9932

# Gateway port — каждый шард на своём порту
GATEWAY_PORT = 9941 + SHARD_ID

# Nostr ключи агентов для подписи (берём из agents.json)
AGENTS_FILE = "/home/agent/data/sites/relay-mesh/agents.json"

# Наши релеи для публикации (NIP-65) — один на шард
_OUR_RELAYS_ALL = [
    "ws://127.0.0.1:8198",
    "wss://relay.primal.net",
    "wss://relay.damus.io",
    "wss://purplepag.es",
    "wss://relay.azzamo.net",   # ★ 67 NIP
    "wss://nostr.bond",
]

# Релеи для чтения (сканирования) — обновлено 2026-05-18 V2
_SCAN_RELAYS_ALL = [
    "ws://127.0.0.1:8198",                    # ← Локальный TIE Relay (приоритетный)
    "wss://top.testrelay.top/juliet-oscar",
    "wss://asia.azzamo.net/kilo-yonder",
    "wss://shu03.shugur.net/papa-nexus-uniform",
    "wss://relay.cloistr.xyz/sable-titan",
    "wss://relay.homeinhk.xyz",
    "wss://rele.speyhard.fi/nostr/oscar",
    "wss://nostr-01.uid.ovh",
    "wss://relay.laantungir.net",
    "wss://sendit.nosflare.com",
    "wss://chat.bitcoinwalk.org/alpha-haven",
    "wss://orly-relay.imwald.eu/lima",
    "wss://nostr.vulpem.com",
    "wss://nostr.reelnetwork.eu",
    "wss://nostr1.bananabit.net",
    "wss://spatia-arcana.com/nox",
    "wss://nostr.primz.org",
    "wss://prl.plus",
    "wss://creatr.nostr.wine",
    "wss://relay.spacetomatoes.net",
    "wss://xmr.usenostr.org",
    "wss://relay.damus.io",
    "wss://relay.primal.net",
    "wss://purplepag.es",
    "wss://nos.lol",
    "wss://relay.azzamo.net",
]

# Relay tiers для graceful degradation (TIER 1 = стабильные, TIER 2 = средние, TIER 3 = остальные)
RELAY_TIERS = {
    "ws://127.0.0.1:8198": 1,                 # ← Локальный TIE Relay
    "wss://relay.primal.net": 1,
    "wss://relay.damus.io": 1,
    "wss://purplepag.es": 1,
    "wss://relay.nos.lol": 1,
    "wss://nos.lol": 1,
    "wss://relay.azzamo.net": 1,   # ★ 67 NIP
    "wss://nostr.bond": 2,
    "wss://sendit.nosflare.com": 2,
    "wss://relay.homeinhk.xyz": 2,
    "wss://prl.plus": 2,
    "wss://nostr.vulpem.com": 2,
}

# Shard slicing
if TOTAL_SHARDS > 1:
    OUR_RELAYS = [_OUR_RELAYS_ALL[SHARD_ID]] if SHARD_ID < len(_OUR_RELAYS_ALL) else []
    chunk = len(_SCAN_RELAYS_ALL) // TOTAL_SHARDS
    start = SHARD_ID * chunk
    end = start + chunk if SHARD_ID < TOTAL_SHARDS - 1 else len(_SCAN_RELAYS_ALL)
    SCAN_RELAYS = _SCAN_RELAYS_ALL[start:end]
else:
    OUR_RELAYS = _OUR_RELAYS_ALL[:]
    SCAN_RELAYS = _SCAN_RELAYS_ALL[:]

# Интервалы
RELAY_LIST_INTERVAL = 3600       # публикация NIP-65 раз в час
SCAN_INTERVAL = 120              # сканирование Nostr ленты
PUBLISH_QUEUE_INTERVAL = 30      # отправка накопленных mesh событий в Nostr

# ─── Статистика ───────────────────────────────────────────────────────
stats = {
    "nostr_to_mesh": 0,
    "mesh_to_nostr": 0,
    "relay_list_discovered": 0,
    "auth_challenges": 0,
    "auth_success": 0,
    "published": 0,       # отправлено (может не дойти)
    "confirmed": 0,       # OK=true от релея
    "rejected": 0,        # OK=false от релея
    "errors": 0,
    "started_at": time.time(),
}


# ═══════════════════════════════════════════════════════════════
#  NIP-42: AUTH challenge/response
# ═══════════════════════════════════════════════════════════════

async def sign_event_async(pubkey_hex: str, private_key_hex: str, content: str,
                          kind: int, tags: list = None, created_at: int = 0) -> dict:
    """
    Async версия sign_event — SHA256 в thread pool.
    Используется для горячего пути (listen_sr).
    """
    ts = created_at or int(time.time())
    tags = tags or []

    event = {
        "id": "",
        "pubkey": pubkey_hex,
        "created_at": ts,
        "kind": kind,
        "tags": tags,
        "content": content,
        "sig": "",
    }

    serialized = orjson.dumps([0, pubkey_hex, ts, kind, tags, content]).decode()
    event_id = await hash_sha256_async(serialized)
    event["id"] = event_id

    # Пытаемся подписать через nostr библиотеку
    try:
        from nostr.key import PrivateKey
        from nostr.event import Event

        if len(private_key_hex) == 64:
            key = PrivateKey(bytes.fromhex(private_key_hex))
        else:
            key = PrivateKey.from_nsec(private_key_hex)

        ev = Event(
            content=content,
            public_key=pubkey_hex,
            kind=kind,
            tags=tags,
            created_at=ts,
        )
        key.sign_event(ev)
        event["id"] = ev.id
        event["sig"] = ev.signature
    except Exception as e:
        print(f"[Sign] ⚠️ Cannot sign (will need external signer): {e}")
        event["sig"] = "sig_needs_external_signer"

    return event


def sign_event(pubkey_hex: str, private_key_hex: str, content: str,
               kind: int, tags: list = None, created_at: int = 0) -> dict:
    """
    Подписать Nostr событие (синхронная версия).

    Используется nostr SDK если доступен, иначе возвращает
    событие без подписи (будет подписано позже через ключ агента).

    Для горячего пути (async контекст) используй sign_event_async().
    """
    ts = created_at or int(time.time())
    tags = tags or []

    event = {
        "id": "",
        "pubkey": pubkey_hex,
        "created_at": ts,
        "kind": kind,
        "tags": tags,
        "content": content,
        "sig": "",
    }

    serialized = orjson.dumps([0, pubkey_hex, ts, kind, tags, content])
    event_id = hashlib.sha256(serialized).hexdigest()
    event["id"] = event_id
    
    # Пытаемся подписать через nostr библиотеку
    try:
        from nostr.key import PrivateKey
        from nostr.event import Event
        
        if len(private_key_hex) == 64:
            key = PrivateKey(bytes.fromhex(private_key_hex))
        else:
            key = PrivateKey.from_nsec(private_key_hex)
        
        ev = Event(
            content=content,
            public_key=pubkey_hex,
            kind=kind,
            tags=tags,
            created_at=ts,
        )
        key.sign_event(ev)
        event["id"] = ev.id
        event["sig"] = ev.signature
    except Exception as e:
        print(f"[Sign] ⚠️ Cannot sign (will need external signer): {e}")
        event["sig"] = "sig_needs_external_signer"
    
    return event


def make_auth_event(pubkey_hex: str, privkey_hex: str, challenge: str,
                    relay_url: str) -> dict:
    """
    Сформировать NIP-42 AUTH событие (kind:22242).
    
    Релей шлёт challenge → агент подписывает → релей разрешает запись.
    """
    return sign_event(
        pubkey_hex=pubkey_hex,
        private_key_hex=privkey_hex,
        content=challenge,
        kind=22242,
        tags=[["relay", relay_url]],
    )


def make_relay_list_event(pubkey_hex: str, privkey_hex: str,
                          read_relays: list, write_relays: list) -> dict:
    """
    Сформировать NIP-65 relay list metadata (kind:10002).
    
    Публикуется раз в час, описывает какие релеи агент использует.
    Позволяет другим агентам/клиентам найти наши посты.
    """
    tags = []
    for r in read_relays:
        tags.append(["r", r, "read"])
    for w in write_relays:
        tags.append(["r", w, "write"])
    
    return sign_event(
        pubkey_hex=pubkey_hex,
        private_key_hex=privkey_hex,
        content="",
        kind=10002,
        tags=tags,
    )


# ═══════════════════════════════════════════════════════════════
#  Nostr WebSocket клиент (NIP-01 + NIP-42)
# ═══════════════════════════════════════════════════════════════

# ── Сохранение discovered релеев для Health Daemon ──
_DISCOVERED_FILE = "/home/agent/data/sites/relay-mesh/logs/discovered_relays.json"

def _save_discovered_relays(relays: set):
    try:
        os.makedirs(os.path.dirname(_DISCOVERED_FILE), exist_ok=True)
        with open(_DISCOVERED_FILE, "w") as f:
            json.dump(sorted(relays), f, indent=2)
    except Exception as e:
        print(f"[Bridge] ⚠️ Cannot save discovered relays: {e}")


class CircuitBreaker:
    """Per-relay circuit breaker: 3 strikes → disconnect, cooldown, retry."""
    
    STATES = {"CLOSED": 0, "OPEN": 1, "HALF_OPEN": 2}
    
    def __init__(self, relay_url: str, max_failures: int = 3, cooling: int = 60, on_open=None):
        self.url = relay_url
        self.max_failures = max_failures
        self.cooling = cooling
        self.max_total_failures = 10  # после 10-го OPEN — перманентная смерть
        self.permanently_dead = False
        self.on_open = on_open  # callback при OPEN (для graceful degradation)
        self.state = "CLOSED"
        self.failures = 0
        self.cooling_until = 0.0
        self.total_failures = 0
        self.total_restores = 0
    
    def record_failure(self):
        """Record a failure. If max reached → OPEN."""
        self.failures += 1
        self.total_failures += 1
        # Перманентная смерть после max_total_failures
        if self.total_failures >= self.max_total_failures:
            self.permanently_dead = True
            self.state = "DEAD"
            print(f"[⚡{self.url}] Circuit PERMANENTLY DEAD ({self.total_failures} total failures)")
            if self.on_open:
                self.on_open()
            return
        if self.failures >= self.max_failures:
            self.state = "OPEN"
            self.cooling_until = time.time() + self.cooling
            print(f"[⚡{self.url}] Circuit OPEN ({self.failures} failures, cooling {self.cooling}s)")
            if self.on_open:
                self.on_open()
    
    def record_success(self):
        """On success → CLOSED, reset counter."""
        if self.state in ("OPEN", "HALF_OPEN"):
            self.state = "CLOSED"
            self.total_restores += 1
            print(f"[⚡{self.url}] Circuit CLOSED (restored after {self.failures} failures)")
        self.failures = 0
    
    def can_connect(self) -> bool:
        """Can we try to connect?"""
        if self.permanently_dead:
            return False
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN" and time.time() >= self.cooling_until:
            self.state = "HALF_OPEN"
            print(f"[⚡{self.url}] Circuit HALF_OPEN — trying reconnect")
            return True
        return False
    
    def status_str(self) -> str:
        remaining = max(0, int(self.cooling_until - time.time())) if self.state == "OPEN" else 0
        return f"{self.state}(f={self.failures},cool={remaining}s)"


class NostrRelayClient:
    """
    Подключение к одному Nostr релею через WebSocket.
    
    - Поддерживает NIP-42 AUTH
    - Подписывается на kind:1
    - Принимает mesh события для публикации
    - Отслеживает OK/FAIL ответы на публикации, retry при auth-required
    
    Каждый релей — отдельное asyncio-соединение.
    """
    
    def __init__(self, relay_url: str, bridge: "NostrBridge"):
        self.url = relay_url
        self.bridge = bridge
        self.ws = None
        self.connected = False
        self.sub_id = f"snin_mesh_{int(time.time())}_{hash(relay_url) % 10000}"
        self.reconnect_delay = 1
        self._running = False
        
        # Circuit Breaker
        self.cb = CircuitBreaker(relay_url, max_failures=3, cooling=60,
                                on_open=lambda: bridge._on_relay_open(relay_url))
        
        # AUTH-защита от бесконечных циклов
        self._auth_cycles = 0          # сколько раз AUTH прошел но publish rejected
        self._consecutive_rejects = 0  # последовательные reject-ы без успеха
        
        # OK-трекинг: event_id → {event, retries}
        self._pending_oks: dict[str, dict] = {}
        # Retry-очередь: event_id → event (ждут AUTH)
        self._retry_queue: asyncio.Queue = asyncio.Queue()
    
    async def connect(self):
        """Подключиться к релею. С AUTH если требуется. Учитывает Circuit Breaker."""
        if not self.cb.can_connect():
            remaining = max(0, int(self.cb.cooling_until - time.time()))
            print(f"[⏳{self.url}] CB blocked ({self.cb.state}), {remaining}s left")
            self.connected = False
            return False
        
        try:
            ssl_ctx = None
            if self.url.startswith("wss://"):
                import ssl as _ssl_module
                ssl_ctx = _ssl_module.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = _ssl_module.CERT_NONE
            self.ws = await websockets.connect(
                self.url, 
                max_size=1_000_000,
                ping_interval=30,
                ping_timeout=10,
                ssl=ssl_ctx,
            )
            self.connected = True
            self.reconnect_delay = 1
            self.cb.record_success()
            
            # Отправляем SUBSCRIBE на kind:1 + NIP-65 kind:10002 для Discovery
            sub = orjson.dumps(["REQ", self.sub_id, {"kinds": [1, 10002], "limit": 10}]).decode()
            await self.ws.send(sub)
            
            print(f"  ✅ {self.url} — connected")
            return True
            
        except Exception as e:
            self.connected = False
            self.cb.record_failure()
            print(f"  ❌ {self.url} — {e}")
            return False
    
    async def listen(self):
        """
        Слушать сообщения от релея с авто-переподключением.
        
        Цикл: connect → listen events → reconnect (с CB) → ...
        Пока `_running = True` и CB не заблокировал навсегда.
        """
        self._running = True
        while self._running:
            # Если не подключены — пробуем
            if not self.ws or not self.connected:
                ok = await self.connect()
                if not ok:
                    # CB заблокировал — ждём
                    if self.cb.state == "OPEN":
                        remaining = max(1, int(self.cb.cooling_until - time.time()))
                        await asyncio.sleep(min(remaining, 30))
                    else:
                        delay = min(self.reconnect_delay, 30)
                        self.reconnect_delay = min(self.reconnect_delay * 2, 60)
                        await asyncio.sleep(delay)
                    continue  # try connect again
            
            # Подключены — слушаем
            try:
                msg = await asyncio.wait_for(self.ws.recv(), timeout=120)
                data = orjson.loads(msg)
                
                if not isinstance(data, list) or len(data) < 2:
                    continue
                
                msg_type = data[0]
                
                # ── EVENT: kind:1 пост из Nostr ──
                if msg_type == "EVENT" and len(data) >= 3:
                    event = data[2]
                    kind = event.get("kind", 0)
                    
                    if kind == 1:  # Nostr note
                        await self.bridge.on_nostr_event(event)
                        stats["nostr_to_mesh"] += 1
                    
                    elif kind == 10002:  # NIP-65 Relay List Discovery
                        await self.bridge._handle_relay_list_event(event)
                        stats["relay_list_discovered"] += 1
                
                # ── EOSE: конец initial snapshot ──
                elif msg_type == "EOSE":
                    pass  # Initial sync complete
                
                # ── NOTICE ──
                elif msg_type == "NOTICE":
                    notice = data[1]
                    if "auth" in notice.lower() or "rate" in notice.lower():
                        print(f"[{self.url}] ⚠️ {notice[:100]}")
                
                # ── AUTH: NIP-42 challenge ──
                elif msg_type == "AUTH":
                    self._auth_cycles += 1
                    
                    # Защита от бесконечных AUTH-циклов
                    if self._auth_cycles >= 3:
                        print(f"[{self.url}] 🔐 AUTH loop ({self._auth_cycles}x), CB OPEN")
                        self.cb.record_failure()
                        self.ws = None
                        self.connected = False
                        continue
                    
                    challenge = data[1]  # challenge string
                    stats["auth_challenges"] += 1
                    
                    # Подписываем и отправляем AUTH ответ
                    pubkey = self.bridge.get_pubkey()
                    privkey = self.bridge.get_privkey()
                    if pubkey and privkey:
                        auth_event = make_auth_event(pubkey, privkey, challenge, self.url)
                        auth_msg = orjson.dumps(["AUTH", auth_event])
                        await self.ws.send(auth_msg)
                        stats["auth_success"] += 1
                        print(f"[{self.url}] 🔐 AUTH completed (#{self._auth_cycles})")
                        
                        # После AUTH — скинуть retry-очередь
                        await self._drain_retry_queue()
                        # Очистить stale pendings (>5 мин)
                        self._cleanup_stale_pendings()
                
                # ── OK: подтверждение публикации ──
                elif msg_type == "OK":
                    event_id = data[1]
                    success = data[2] if len(data) > 2 else False
                    message = data[3] if len(data) > 3 else ""
                    
                    if success:
                        stats["confirmed"] += 1
                        # Убрать из pending
                        self._pending_oks.pop(event_id, None)
                        # Успешная публикация = успех для CB
                        self.cb.record_success()
                        self._consecutive_rejects = 0  # сброс счётчика reject-ов
                    else:
                        stats["rejected"] += 1
                        # auth-required → retry
                        if "auth" in message.lower() and event_id in self._pending_oks:
                            info = self._pending_oks[event_id]
                            if info["retries"] < 3:
                                info["retries"] += 1
                                await self._retry_queue.put(info["event"])
                                print(f"[{self.url}] 🔄 retry {info['retries']}/3 (auth-required)")
                            else:
                                print(f"[{self.url}] ❌ gave up after {info['retries']} retries")
                                stats["errors"] += 1
                                self._pending_oks.pop(event_id, None)
                                self.cb.record_failure()  # exhausted retries = CB hit
                        else:
                            print(f"[{self.url}] ⚠️ publish rejected: {message[:100]}")
                            self._pending_oks.pop(event_id, None)
                            # Protocol-level reject = CB failure
                            self._consecutive_rejects += 1
                            self.cb.record_failure()
                
            except asyncio.TimeoutError:
                self._cleanup_stale_pendings(max_age=300)
                try:
                    await self.ws.send(orjson.dumps(["REQ", f"{self.sub_id}_hb", 
                        {"kinds": [1], "limit": 1, "since": int(time.time())}]))
                except:
                    self.connected = False
                    self.cb.record_failure()
                    self.ws = None
            except websockets.exceptions.ConnectionClosed:
                print(f"[{self.url}] ❌ Connection closed")
                self.connected = False
                self.cb.record_failure()
                self.ws = None
            except Exception as e:
                print(f"[{self.url}] ⚠️ {type(e).__name__}: {e}")
                stats["errors"] += 1
                self.connected = False
                self.cb.record_failure()
                self.ws = None
    
    async def publish(self, event: dict):
        """Опубликовать событие в релей. Отслеживает OK-ответ. Уважает Circuit Breaker."""
        if not self.connected or not self.ws:
            return False
        if self.cb.state == "OPEN":
            return False  # Не пытаемся публиковать в отключённый релей
        try:
            event_id = event.get("id", "")
            msg = orjson.dumps(["EVENT", event])
            await self.ws.send(msg)
            stats["published"] += 1  # отправлено (может быть не подтверждено)
            
            # Запомнить для OK-трекинга
            if event_id:
                self._pending_oks[event_id] = {
                    "event": event,
                    "ts": time.time(),
                    "retries": 0,
                }
            return True
        except Exception as e:
            stats["errors"] += 1
            self.cb.record_failure()
            return False
    
    async def close(self):
        self._running = False
        if self.ws:
            await self.ws.close()
    
    async def _drain_retry_queue(self):
        """Отослать все накопленные retry-события из очереди."""
        retried = 0
        while not self._retry_queue.empty():
            try:
                event = self._retry_queue.get_nowait()
                event_id = event.get("id", "")
                msg = orjson.dumps(["EVENT", event])
                await self.ws.send(msg)
                stats["published"] += 1
                if event_id:
                    self._pending_oks[event_id] = {
                        "event": event,
                        "ts": time.time(),
                        "retries": self._pending_oks.get(event_id, {}).get("retries", 0),
                    }
                retried += 1
            except Exception as e:
                stats["errors"] += 1
                break
        if retried:
            print(f"[{self.url}] 🔄 Retried {retried} events after AUTH")
    
    def _cleanup_stale_pendings(self, max_age: float = 300):
        """Удалить события из pending, по которым не пришёл OK за max_age сек."""
        now = time.time()
        stale = [eid for eid, info in self._pending_oks.items()
                 if now - info.get("ts", 0) > max_age]
        for eid in stale:
            self._pending_oks.pop(eid, None)
        if stale:
            print(f"[{self.url}] 🧹 Dropped {len(stale)} stale pending OKs")


# ═══════════════════════════════════════════════════════════════
#  Nostr Bridge — управляет всеми соединениями
# ═══════════════════════════════════════════════════════════════

class NostrBridge:
    """
    Двусторонний мост между Nostr и Mesh.
    
    ╔══════════════════════╗     ╔══════════════════════╗
    ║     NOSTR NETWORK    ║     ║     SNIN MESH        ║
    ║  101 релей через WS  ║ ←─→ ║  SmartRouter :9932   ║
    ║  kind:1 публикации   ║     ║  kind:39002 события  ║
    ╚══════════════════════╝     ╚══════════════════════╝
              │                             │
              │  NIP-42 AUTH                │  FirstContact
              │  NIP-65 Relay List          │  Channel Ranker
              │                             │  Matrix Exchange
    """
    
    def __init__(self, pubkey_hex: str = "", privkey_hex: str = ""):
        self.pubkey = pubkey_hex
        self.privkey = privkey_hex
        
        # Подключения к релеям
        self.clients: list[NostrRelayClient] = []
        self._publish_queue: asyncio.Queue = asyncio.Queue(maxsize=500)  # backpressure: очередь ограничена
        self._published_cache: TTLCache = TTLCache(maxsize=5000, ttl=600)  # Level 3: event_id dedup
        
        # Самоконтроль памяти — при превышении RSS сам умирает
        self._rss_limit_mb = 500  # порог срабатывания
        
        # Graceful degradation: relay pool по tiers
        self._relay_pool: dict[int, list[str]] = {}  # tier → [urls]
        for url, tier in RELAY_TIERS.items():
            self._relay_pool.setdefault(tier, []).append(url)
        self._dead_relays: dict[str, float] = {}  # url → время смерти (для повторной проверки)
        
        # Rate limiter на входящие события от Nostr релеев
        self._rate_events_per_window = 500    # макс событий за окно (x5 — NIP-65 flood)
        self._rate_window = 30                # окно в секундах (~17/сек)
        self._rate_tokens = self._rate_events_per_window
        self._rate_last_refill = time.time()
        
        # Подключение к SmartRouter (Mesh)
        self.sr_reader = None
        self.sr_writer = None
        self.sr_connected = False
        
        # Флаги
        self._running = False
        self._relay_list_task = None
        self._sr_listener_task = None
        self._publisher_task = None
    
    def get_pubkey(self) -> str:
        return self.pubkey
    
    def get_privkey(self) -> str:
        return self.privkey
    
    # ── SmartRouter (Mesh) ──
    
    async def connect_sr(self):
        """Подключиться к SmartRouter для получения mesh-событий."""
        for attempt in range(10):
            try:
                self.sr_reader, self.sr_writer = await asyncio.open_connection(
                    SMART_ROUTER_HOST, SMART_ROUTER_PORT
                )
                self.sr_connected = True
                print(f"\n[Bridge] ✅ Connected to SmartRouter ({SMART_ROUTER_HOST}:{SMART_ROUTER_PORT})")
                return True
            except ConnectionRefusedError:
                delay = min(0.5 * (2 ** attempt), 30)
                print(f"[Bridge] ⏳ SR connect attempt {attempt+1}/10, retry in {delay:.0f}s...")
                await asyncio.sleep(delay)
        return False
    
    async def _sr_reconnect_loop(self):
        """Фоновое переподключение к SR. Никогда не завершается — держит NB живым."""
        print(f"[Bridge] 🔄 SR reconnect loop started (retry every 5s)")
        while self._running:
            if not self.sr_connected:
                ok = await self.connect_sr()
                if ok:
                    print(f"[Bridge] ✅ SR reconnected in background")
                    # Запускаем listen_sr если ещё не запущен
                    if self._sr_pending_listener and self._sr_listener_task is None:
                        self._sr_listener_task = asyncio.create_task(self.listen_sr())
                        self._sr_pending_listener = False
            await asyncio.sleep(5)
        print(f"[Bridge] ⏹ SR reconnect loop ended")
    
    async def listen_sr(self):
        """Слушать mesh-события из SmartRouter и публиковать в Nostr."""
        print(f"[Bridge] 📡 Listening mesh events → Nostr")
        
        while self._running:
            # Если не подключены — переподключаемся
            if not self.sr_connected:
                print(f"[Bridge] 🔄 Reconnecting to SmartRouter...")
                ok = await self.connect_sr()
                if not ok:
                    await asyncio.sleep(5)
                    continue
            
            try:
                line = await asyncio.wait_for(
                    self.sr_reader.readline(), timeout=30
                )
                if not line:
                    self.sr_connected = False
                    continue
                
                event = orjson.loads(line.strip())
                kind = event.get("kind", 0)
                
                # kind:39002 = mesh событие → публикуем как kind:1 в Nostr
                if kind == 39002:
                    pubkey = event.get("pubkey", self.pubkey)
                    content = event.get("payload", {}).get("text", "")
                    if not content:
                        content = event.get("content", "")
                    
                    # Антицикл: эхо нашей публикации — дроп
                    ev_id = event.get("id", "")
                    seen_key = ev_id or (pubkey, content[:200])
                    if ev_id and not self._published_cache.add(ev_id):
                        continue
                    if not ev_id and not self._published_cache.add(seen_key):
                        continue
                    
                    if content:
                        # Level 2: SHA256 + Schnorr sign в ProcessPool — event loop не блокируется
                        nostr_event = await sign_event_full_async(
                            pubkey or self.pubkey,
                            self.privkey,
                            content,
                            1,
                            event.get("tags", []),
                        )
                        try:
                            self._publish_queue.put_nowait(nostr_event)
                        except asyncio.QueueFull:
                            stats["dropped"] = stats.get("dropped", 0) + 1
                            if stats["dropped"] % 100 == 1:
                                print(f"[Bridge] ⚠️ Publish queue full (listen_sr), dropped #{stats['dropped']}")
                        stats["mesh_to_nostr"] += 1
                        print(f"[Bridge] 📤 queued for Nostr: {nostr_event.get('content','')[:60]}")
                
            except asyncio.TimeoutError:
                continue
            except (BrokenPipeError, ConnectionResetError):
                self.sr_connected = False
                print(f"[Bridge] ⚠️ SR connection lost, reconnecting...")
                continue
            except ValueError as e:
                print(f"[Bridge] ⚠️ SR listener: {e}")
                stats["errors"] += 1
                self.sr_connected = False
                await asyncio.sleep(2)
    
    # ── Nostr события ──
    
    async def on_nostr_event(self, event: dict):
        """
        Получен kind:1 из Nostr → отправляем в Mesh как kind:39002.
        
        Маршрутизация: через SmartRouter → ContentRouter → агентам.
        """
        pubkey = event.get("pubkey", "")
        content = event.get("content", "")
        
        # Rate limiter: token bucket на входящие события
        now = time.time()
        elapsed = now - self._rate_last_refill
        self._rate_tokens = min(
            self._rate_events_per_window,
            self._rate_tokens + elapsed * (self._rate_events_per_window / self._rate_window)
        )
        self._rate_last_refill = now
        if self._rate_tokens < 1:
            stats["rate_limited"] = stats.get("rate_limited", 0) + 1
            if stats["rate_limited"] % 100 == 1:
                print(f"[Bridge] ⚠️ Rate limited {stats['rate_limited']} events so far")
            return
        self._rate_tokens -= 1
        
        # Level 3: dedup by event_id
        ev_id = event.get("id", "")
        if ev_id:
            if not self._published_cache.add(ev_id):
                return
        else:
            seen_key = (pubkey, content[:200])
            if not self._published_cache.add(seen_key):
                return
        
        created_at = event.get("created_at", 0)
        tags = event.get("tags", [])
        
        mesh_msg = {
            "kind": 39002,
            "id": event.get("id", ""),            # ← ФИКС: сохраняем оригинальный Nostr event ID для CR dedup
            "pubkey": pubkey,
            "from": f"nostr:{pubkey[:16]}",
            "to": "broadcast",
            "payload": {
                "text": content[:500],
                "origin": "nostr_bridge",
                "relay": self._get_relay_for_event(event),
            },
            "tags": tags,
            "created_at": created_at,
            "meta": {
                "origin": "nostr_bridge",
                "channel": "nostr",
                "priority": "normal",
                "tier": 4,
            }
        }
        
        if self.sr_connected and self.sr_writer:
            try:
                self.sr_writer.write(orjson.dumps(mesh_msg) + b"\n")
                await self.sr_writer.drain()
            except (BrokenPipeError, ConnectionResetError):
                self.sr_connected = False
                stats["errors"] += 1
            except Exception as e:
                print(f"[Bridge] ⚠️ Send to mesh: {e}")
                stats["errors"] += 1
    
    def _get_relay_for_event(self, event: dict) -> str:
        """Определить какой релей прислал событие."""
        return event.get("meta", {}).get("relay", "unknown")
    
    # ── Публикация в Nostr ──
    
    async def publish_loop(self):
        """Event-driven: публикует каждое событие немедленно через открытый WS."""
        cb_report_counter = 0
        while self._running:
            try:
                event = await asyncio.wait_for(self._publish_queue.get(), timeout=60)
            except asyncio.TimeoutError:
                # Каждые 60 сек — отчёт CB статуса
                cb_report_counter += 1
                if cb_report_counter >= 5 and self.clients:
                    cb_report_counter = 0
                    open_relays = [c.url for c in self.clients if c.cb.state == "OPEN"]
                    closed_relays = [c.url for c in self.clients if c.cb.state == "CLOSED"]
                    half_relays = [c.url for c in self.clients if c.cb.state == "HALF_OPEN"]
                    print(f"[Bridge] 📊 CB: {len(closed_relays)}✅ {len(open_relays)}⛔ {len(half_relays)}🔄"
                          f" connected={sum(1 for c in self.clients if c.connected)}/{len(self.clients)}")
                    if open_relays:
                        for url in open_relays[:3]:
                            c = next((x for x in self.clients if x.url == url), None)
                            if c:
                                print(f"         ⛔ {url} cooling={max(0,int(c.cb.cooling_until-time.time()))}s")
                continue
            
            if not self.clients:
                continue
            
            # Публикуем на всех connected релеях сразу
            tasks = []
            for client in self.clients:
                if client.connected:
                    tasks.append(client.publish(event))
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                # Level 3: dedup по event_id
                ev_id = event.get("id", "")
                if ev_id:
                    self._published_cache.add(ev_id)
                print(f"[Bridge] 📤 Published event to {len(tasks)} relays")
    
    # ── NIP-65: Relay list ──
    
    async def publish_relay_list(self):
        """Публикует NIP-65 kind:10002 раз в час."""
        if not self.pubkey or not self.privkey:
            print("[Bridge] ⚠️ No keys, skipping NIP-65")
            return
        
        while self._running:
            event = make_relay_list_event(
                pubkey_hex=self.pubkey,
                privkey_hex=self.privkey,
                read_relays=SCAN_RELAYS,
                write_relays=OUR_RELAYS,
            )
            
            # Публикуем на всех connected релеях
            tasks = []
            for client in self.clients:
                if client.connected:
                    tasks.append(client.publish(event))
            
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                success = sum(1 for r in results if r is True)
                print(f"[Bridge] 📋 NIP-65 relay list published to {success}/{len(tasks)} relays")
            
            await asyncio.sleep(RELAY_LIST_INTERVAL)
    
    # ── NIP-65: Relay List Discovery ──
    _discovered_relays: set = set()  # динамически найденные релеи
    
    async def _handle_relay_list_event(self, event: dict):
        """
        Обрабатывает kind:10002 от других пользователей.
        Парсит теги ['r', '<url>', '<read|write>'] и добавляет релеи в SCAN_RELAYS.
        """
        # Level 3: стоп после 100 релеев — хватит
        if len(self._discovered_relays) >= 100:
            return
        
        tags = event.get("tags", [])
        added = 0
        for t in tags:
            if len(t) >= 2 and t[0] == "r":
                url = t[1]
                # Нормализация: убираем лишние слеши, проверяем wss://
                if not url.startswith("wss://"):
                    continue
                url = url.rstrip("/")
                if url not in self._discovered_relays and url not in SCAN_RELAYS:
                    if len(self._discovered_relays) >= 100:
                        return
                    self._discovered_relays.add(url)
                    added += 1
                    if added <= 3:  # не засоряем лог
                        print(f"[Bridge] 🌐 Discovered relay via NIP-65: {url}")
        
        if added:
            # Сохраняем discovered релеи для Health Daemon
            _save_discovered_relays(self._discovered_relays)
            
            # Добавляем в глобальный список SCAN_RELAYS (через append)
            _new = [u for u in self._discovered_relays if u not in SCAN_RELAYS]
            SCAN_RELAYS.extend(_new[:10])  # макс 10 за раз (было 20)
            print(f"[Bridge] 🌐 NIP-65 Discovery: +{added} new relays (total: {len(self._discovered_relays)})")
    
    # ── Запуск ──
    
    async def start(self):
        """Запустить мост: подключиться к SR и Nostr релеям."""
        print(f"\n{'='*50}")
        print(f"  SNIN Nostr ↔ Mesh Bridge")
        print(f"{'='*50}")
        
        self._running = True
        
        # 1. Gateway mode: слушаем свой порт СРАЗУ
        self._gateway_task = asyncio.create_task(self._gateway_loop())
        
        # 1b. Local Nostr Relay (NIP-01 WS) для Cryter
        self._local_relay_task = asyncio.create_task(self._local_relay_server())
        await asyncio.sleep(0.5)
        
        # 2. Подключаемся к SmartRouter (не фатально если не готов — переподключимся)
        sr_ok = await self.connect_sr()
        if not sr_ok:
            print("[Bridge] ❌ Cannot connect to SmartRouter — gateway stays up, retrying in bg")
            # Запускаем фоновое переподключение вместо выхода
            self._sr_reconnect_task = asyncio.create_task(self._sr_reconnect_loop())
        
        # 3. Подключаемся к Nostr релеям
        print(f"\n[Bridge] 🔌 Connecting to {len(SCAN_RELAYS)} Nostr relays...")
        for url in SCAN_RELAYS:
            client = NostrRelayClient(url, self)
            ok = await client.connect()
            self.clients.append(client)
            await asyncio.sleep(0.1)  # небольшая задержка между подключениями
        
        connected = sum(1 for c in self.clients if c.connected)
        print(f"\n[Bridge] ✅ {connected}/{len(self.clients)} relays connected")
        
        # 4. Запускаем фоновые задачи
        print(f"[Bridge] 🔄 Starting background loops...")
        
        # Слушаем Nostr события от каждого релея
        nostr_tasks = [asyncio.create_task(client.listen()) for client in self.clients]
        
        # Слушаем mesh события из SR (только если SR подключён)
        if sr_ok:
            self._sr_listener_task = asyncio.create_task(self.listen_sr())
        else:
            print("[Bridge] ⏳ SR listener deferred — will start after reconnect")
            self._sr_listener_task = None
            self._sr_pending_listener = True
        
        # Публикуем mesh → Nostr
        self._publisher_task = asyncio.create_task(self.publish_loop())
        
        # NIP-65 relay list
        self._relay_list_task = asyncio.create_task(self.publish_relay_list())
        
        # Самоконтроль памяти — проверка RSS каждые 60 сек
        self._memory_check_task = asyncio.create_task(self._memory_self_check())
        
        print(f"[Bridge] ✅ Running. {connected} Nostr relays ↔ Mesh")
        
        # Собираем таски для ожидания (ВСЕ бесконечные — ни один не должен завершиться)
        all_tasks = [self._publisher_task, self._relay_list_task, self._memory_check_task]
        all_tasks.extend(nostr_tasks)
        if self._sr_listener_task:
            all_tasks.append(self._sr_listener_task)
        if self._gateway_task:
            all_tasks.append(self._gateway_task)
        if hasattr(self, '_local_relay_task'):
            all_tasks.append(self._local_relay_task)
        if hasattr(self, '_sr_reconnect_task'):
            all_tasks.append(self._sr_reconnect_task)
        
        # asyncio.FIRST_EXCEPTION: если хоть одна таска упала — логируем
        done, pending = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in done:
            try:
                exc = task.exception()
                if exc:
                    print(f"[Bridge] ❌ Task died: {type(exc).__name__}: {exc}")
            except asyncio.CancelledError:
                pass
        
        # Если мы сюда дошли — одна из тасок умерла. Ошибка. Ждём remaining.
        print(f"[Bridge] 🔴 One of the main tasks died! Remaining tasks: {len(pending)}")
        if pending:
            await asyncio.wait(pending, return_when=asyncio.ALL_COMPLETED)
    
    async def _gateway_loop(self):
        """TCP gateway: port GATEWAY_PORT — принимает mesh события от SR, публикует в Nostr."""
        # Level 3: ретрай при address already in use
        for attempt in range(5):
            try:
                server = await asyncio.start_server(
                    self._gateway_handler, "127.0.0.1", GATEWAY_PORT
                )
                async with server:
                    print(f"[Bridge] 📡 Shard-{SHARD_ID} Gateway listening on {GATEWAY_PORT} (mesh→Nostr)")
                    await server.serve_forever()
            except OSError as e:
                if "address already in use" in str(e).lower() and attempt < 4:
                    print(f"[GW] ⚠️ Port {GATEWAY_PORT} busy (attempt {attempt+1}/5), freeing...")
                    proc = await asyncio.create_subprocess_shell(
                        f"fuser -k {GATEWAY_PORT}/tcp 2>/dev/null; sleep 1",
                        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
                    )
                    await proc.wait()
                    await asyncio.sleep(2)
                    continue
                print(f"[GW] ❌ Port {GATEWAY_PORT}: {e}")
                return
    
    async def _gateway_handler(self, reader, writer):
        """Обработать одно TCP-соединение от SR (nostr channel). Не закрывать по таймауту."""
        peer = writer.get_extra_info('peername')
        print(f"[GW] ⚡ Connection from {peer}")
        try:
            while True:
                line = await reader.readline()
                if not line:
                    print(f"[GW] ⚠️ EOF from {peer}")
                    break
                raw = line.decode().strip()
                print(f"[GW] 📩 {raw[:120]}")
                event = orjson.loads(raw)
                kind = event.get("kind", 0)
                
                # kind:39002 mesh → публикуем как kind:1 в Nostr
                if kind == 39002:
                    content = event.get("payload", {}).get("text", "")
                    if not content:
                        content = event.get("content", "")
                    if content:
                        # Level 3: dedup по event_id
                        ev_id = event.get("id", "")
                        seen_key = ev_id or (event.get("pubkey", ""), content[:200])
                        if ev_id:
                            if not self._published_cache.add(ev_id):
                                continue
                        else:
                            if not self._published_cache.add(seen_key):
                                continue
                        # Level 2: подпись в ProcessPool, чтобы event id + sig были валидны
                        nostr_event = await sign_event_full_async(
                            event.get("pubkey", self.pubkey),
                            self.privkey,
                            content,
                            1,
                            event.get("tags", []),
                        )
                        try:
                            self._publish_queue.put_nowait(nostr_event)
                        except asyncio.QueueFull:
                            stats["dropped"] = stats.get("dropped", 0) + 1
                            if stats["dropped"] % 100 == 1:
                                print(f"[Bridge] ⚠️ Publish queue full (gateway), dropped #{stats['dropped']}")
                        stats["mesh_to_nostr"] += 1
                        print(f"[GW] ✅ queued for Nostr: {content[:60]}")
                    else:
                        print(f"[GW] ⚠️ empty content in kind:39002")
                else:
                    # unknown kind — тихо пропускаем, не спамим в логи
                    stats["unknown_kind"] = stats.get("unknown_kind", 0) + 1
        except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError, ValueError) as e:
            print(f"[GW] ⚠️ {e}")
        finally:
            writer.close()
            print(f"[GW] 🔒 closed {peer}")
    
    def _get_fallback_relay(self, dead_url: str) -> str | None:
        """
        Graceful degradation: при OPEN на relay найти замену из того же или нижнего TIER.
        Возвращает URL запасного релея или None.
        """
        dead_tier = RELAY_TIERS.get(dead_url)
        if dead_tier is None:
            return None
        
        # Пробуем тот же TIER
        same_tier = self._relay_pool.get(dead_tier, [])
        alive_same = [u for u in same_tier if u != dead_url and u not in self._dead_relays]
        if alive_same:
            chosen = alive_same[0]
            print(f"[Bridge] ⚡ Fallback: {dead_url} → {chosen} (same TIER {dead_tier})")
            return chosen
        
        # Пробуем TIER ниже (с запасом)
        for tier in sorted(self._relay_pool.keys()):
            if tier > dead_tier:
                candidates = [u for u in self._relay_pool[tier] if u not in self._dead_relays]
                if candidates:
                    chosen = candidates[0]
                    print(f"[Bridge] ⚡ Fallback: {dead_url} → {chosen} (TIER {tier} reserve)")
                    return candidates[0]
        
        return None  # нет замены
    
    def _on_relay_open(self, dead_url: str):
        """
        Callback при OPEN на relay. Логирует dead relay и запускает fallback в фоне.
        """
        self._dead_relays[dead_url] = time.time()
        fallback = self._get_fallback_relay(dead_url)
        if fallback:
            print(f"[Bridge] ⚡ Graceful degradation: {dead_url} → {fallback}")
            # Создаём новый клиент для fallback релея
            new_client = NostrRelayClient(fallback, self)
            self.clients.append(new_client)
            # Запускаем его в фоне
            asyncio.create_task(new_client.listen())
    
    async def _memory_self_check(self):
        """Проверка RSS процесса каждые 60 сек. При превышении — self-terminate."""
        import os as _os
        while self._running:
            try:
                with open("/proc/self/status") as _f:
                    for _line in _f:
                        if _line.startswith("VmRSS:"):
                            _rss_kb = int(_line.split()[1])
                            _rss_mb = _rss_kb // 1024
                            if _rss_mb > self._rss_limit_mb:
                                print(f"[Bridge] ⛔ RSS {_rss_mb}MB > {self._rss_limit_mb}MB — self-terminating")
                                _os._exit(1)
                            break
            except Exception:
                pass
            await asyncio.sleep(60)
    
    # ── Local Nostr Relay (NIP-01 WebSocket) ──
    # Принимает уже подписанные kind:1 от локальных агентов (Cryter)
    # и отправляет их во внешние релеи без переподписи.
    
    LOCAL_RELAY_PORT = 9961 + SHARD_ID
    
    async def _local_relay_server(self):
        """WebSocket Nostr relay (NIP-01) для локальных агентов."""
        async def handler(ws):
            peer = ws.remote_address
            print(f"[LR] ⚡ WS connection from {peer}")
            try:
                async for raw in ws:
                    try:
                        msg = orjson.loads(raw)
                    except ValueError:
                        continue
                    
                    if not isinstance(msg, list) or len(msg) < 2:
                        continue
                    
                    msg_type = msg[0]
                    
                    # NIP-01: ["EVENT", {"id":..., "pubkey":..., ...}]
                    if msg_type == "EVENT":
                        event = msg[1]
                        ev_kind = event.get("kind", 1)
                        ev_id = event.get("id", "")
                        
                        if ev_kind == 1:
                            try:
                                self._publish_queue.put_nowait(event)
                            except asyncio.QueueFull:
                                await ws.send(orjson.dumps(["OK", ev_id, False, "rate-limited"]))
                                continue
                            
                            stats["mesh_to_nostr"] = stats.get("mesh_to_nostr", 0) + 1
                            content_preview = event.get("content", "")[:60]
                            print(f"[LR] ✅ received signed event {ev_id[:16]}... «{content_preview}»")
                            await ws.send(orjson.dumps(["OK", ev_id, True, ""]))
                        else:
                            await ws.send(orjson.dumps(["OK", ev_id, False, "unsupported kind"]))
                    
                    elif msg_type == "REQ":
                        sub_id = msg[1]
                        await ws.send(orjson.dumps(["EOSE", sub_id]))
                    
                    elif msg_type == "CLOSE":
                        break
                        
            except websockets.exceptions.ConnectionClosed:
                pass
            except Exception as e:
                print(f"[LR] ⚠️ WS error: {e}")
            finally:
                print(f"[LR] 🔒 closed {peer}")
        
        # Level 3: ретрай при address already in use
        for attempt in range(5):
            try:
                async with websockets.serve(handler, "127.0.0.1", self.LOCAL_RELAY_PORT):
                    print(f"[Bridge] 📡 Local Nostr Relay on ws://127.0.0.1:{self.LOCAL_RELAY_PORT} (NIP-01)")
                    await asyncio.Future()  # run forever
            except OSError as e:
                if "address already in use" in str(e).lower() and attempt < 4:
                    print(f"[LR] ⚠️ Port {self.LOCAL_RELAY_PORT} busy (attempt {attempt+1}/5), killing occupant...")
                    # Прибиваем процесс на порту
                    proc = await asyncio.create_subprocess_shell(
                        f"fuser -k {self.LOCAL_RELAY_PORT}/tcp 2>/dev/null; sleep 1",
                        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
                    )
                    await proc.wait()
                    await asyncio.sleep(2)
                    continue
                print(f"[LR] ❌ Port {self.LOCAL_RELAY_PORT}: {e}")
                raise
    
    async def stop(self):
        self._running = False
        for client in self.clients:
            await client.close()
        if self.sr_writer:
            self.sr_writer.close()
    
    def get_stats(self) -> str:
        uptime = int(time.time() - stats["started_at"])
        return (
            f"[Bridge] Uptime: {uptime}s | "
            f"Nostr→Mesh: {stats['nostr_to_mesh']} | "
            f"Mesh→Nostr: {stats['mesh_to_nostr']} | "
            f"AUTH: {stats['auth_success']}/{stats['auth_challenges']} | "
            f"Published: {stats['published']} (✅{stats['confirmed']} ❌{stats['rejected']}) | "
            f"Errors: {stats['errors']}"
        )


# ═══════════════════════════════════════════════════════════════
#  API для интеграции с FirstContact
# ═══════════════════════════════════════════════════════════════

class NostrBridgeLayer:
    """
    Слой Nostr в архитектуре First Contact.
    
    После сканирования каналов (Фаза A) мост подключается
    к Nostr релеям (Фаза B) и начинает двусторонний обмен.
    """
    
    def __init__(self):
        self.bridge = None
        self._running = False
    
    async def start(self, pubkey_hex: str = "", privkey_hex: str = ""):
        """Запустить мост как часть First Contact."""
        self.bridge = NostrBridge(
            pubkey_hex=pubkey_hex or "npub1snin_mesh_bridge",
            privkey_hex=privkey_hex or "",
        )
        self._running = True
        
        # Запускаем в фоне (не блокируем First Contact)
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self.bridge.start())
        
        return {"ok": True, "note": "Nostr Bridge started in background"}
    
    async def stop(self):
        if self.bridge:
            await self.bridge.stop()
        self._running = False
    
    def status(self) -> dict:
        if not self.bridge:
            return {"ok": False, "error": "not started"}
        return {
            "ok": True,
            "connected_relays": sum(1 for c in self.bridge.clients if c.connected),
            "total_relays": len(self.bridge.clients),
            "stats": self.bridge.get_stats() if self.bridge else "N/A",
        }



# ─── Main ───
if __name__ == "__main__":
    print("[Nostr Bridge] Starting...")
    
    # Пытаемся загрузить ключи из agents.json
    pubkey = "npub1snin_mesh_bridge"
    privkey = ""
    
    try:
        with open(AGENTS_FILE) as f:
            agents = json.load(f)
            for pk, info in agents.items():
                if info.get("name") == "archivist_ai":
                    pubkey = info.get("meta", {}).get("nostr_pubkey", pk)
                    # privkey берётся из meta если есть
                    privkey = info.get("meta", {}).get("nostr_privkey", "")
                    break
    except Exception as e:
        print(f"[Bridge] ⚠️ Cannot load agents.json: {e}")
    
    bridge = NostrBridge(pubkey_hex=pubkey, privkey_hex=privkey)
    
    try:
        asyncio.run(bridge.start())
    except KeyboardInterrupt:
        print("\n[Bridge] Stopping...")
        asyncio.run(bridge.stop())

"""
nostr_core.py — Nostr WebSocket клиент + криптография + Circuit Breaker
Выделено из nostr_bridge.py (Фаза 3 рефакторинга)

Содержит:
- sign_event_async() / sign_event() — подпись событий NIP-01
- make_auth_event() — NIP-42 авторизация
- make_relay_list_event() — NIP-65 relay list
- CircuitBreaker — per-relay circuit breaker
- NostrRelayClient — WebSocket клиент к одному релею
"""

import asyncio
import hashlib
import json
import orjson
import time
from typing import TYPE_CHECKING

import websockets

from cpu_worker import hash_sha256_async, sign_event_full_async

if TYPE_CHECKING:
    from nostr_bridge import NostrBridge


# ═══════════════════════════════════════════════════════════════
#  NIP-01: Подпись событий
# ═══════════════════════════════════════════════════════════════

async def sign_event_async(pubkey_hex: str, private_key_hex: str, content: str,
                          kind: int, tags: list = None, created_at: int = 0) -> dict:
    """Подписать событие Nostr (async, через ProcessPool)."""
    # Делегируем полную подпись (хеширование + Schnorr) в sign_event_full_async
    return await sign_event_full_async(pubkey_hex, private_key_hex, content, kind, tags, created_at)


def sign_event(pubkey_hex: str, private_key_hex: str, content: str,
              kind: int, tags: list = None, created_at: int = 0) -> dict:
    """Подписать событие Nostr (sync, для простых случаев)."""
    if created_at == 0:
        created_at = int(time.time())
    if tags is None:
        tags = []
    # Strip compressed/uncompressed prefix — Nostr expects bare 32-byte pubkey
    if len(pubkey_hex) == 66 and pubkey_hex[:2] in ('02', '03'):
        bare_pubkey = pubkey_hex[2:]
    elif len(pubkey_hex) == 130 and pubkey_hex[:2] == '04':
        bare_pubkey = pubkey_hex[2:]
    else:
        bare_pubkey = pubkey_hex
    import secp256k1
    serialized = json.dumps(
        [0, bare_pubkey, created_at, kind, tags, content],
        ensure_ascii=False, separators=(',', ':')
    )
    event_id = hashlib.sha256(serialized.encode()).hexdigest()
    privkey_bytes = bytes.fromhex(private_key_hex)
    pk = secp256k1.PrivateKey(privkey_bytes)
    sig = pk.schnorr_sign(bytes.fromhex(event_id), None, raw=True)
    return {
        "id": event_id,
        "pubkey": bare_pubkey,
        "created_at": created_at,
        "kind": kind,
        "tags": tags,
        "content": content,
        "sig": sig.hex(),
    }


# ═══════════════════════════════════════════════════════════════
#  NIP-42: AUTH challenge/response
# ═══════════════════════════════════════════════════════════════

def make_auth_event(pubkey_hex: str, privkey_hex: str, challenge: str,
                    relay_url: str = "") -> dict:
    """Создать подписанное AUTH событие (NIP-42)."""
    tags = [["challenge", challenge]]
    if relay_url:
        tags.append(["relay", relay_url])
    return sign_event(
        pubkey_hex=pubkey_hex,
        private_key_hex=privkey_hex,
        content="",
        kind=22242,
        tags=tags,
    )


# ═══════════════════════════════════════════════════════════════
#  NIP-65: Relay List
# ═══════════════════════════════════════════════════════════════

def make_relay_list_event(pubkey_hex: str, privkey_hex: str,
                          read_relays: list, write_relays: list) -> dict:
    """Сформировать NIP-65 relay list metadata (kind:10002)."""
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
#  Per-Relay Circuit Breaker
# ═══════════════════════════════════════════════════════════════

class CircuitBreaker:
    """Per-relay circuit breaker: 3 strikes → disconnect, cooldown, retry."""

    STATES = {"CLOSED": 0, "OPEN": 1, "HALF_OPEN": 2}

    def __init__(self, relay_url: str, max_failures: int = 3, cooling: int = 60, on_open=None):
        self.url = relay_url
        self.max_failures = max_failures
        self.cooling = cooling
        self.max_total_failures = 10
        self.permanently_dead = False
        self.on_open = on_open
        self.state = "CLOSED"
        self.failures = 0
        self.cooling_until = 0.0
        self.total_failures = 0
        self.total_restores = 0

    def record_failure(self):
        self.failures += 1
        self.total_failures += 1
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
        if self.state in ("OPEN", "HALF_OPEN"):
            self.state = "CLOSED"
            self.total_restores += 1
            print(f"[⚡{self.url}] Circuit CLOSED (restored after {self.failures} failures)")
        self.failures = 0

    def can_connect(self) -> bool:
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


# ═══════════════════════════════════════════════════════════════
#  Nostr WebSocket клиент (NIP-01 + NIP-42)
# ═══════════════════════════════════════════════════════════════

class NostrRelayClient:
    """
    Подключение к одному Nostr релею через WebSocket.

    - Поддерживает NIP-42 AUTH
    - Подписывается на kind:1
    - Принимает mesh события для публикации
    - Отслеживает OK/FAIL ответы на публикации, retry при auth-required
    """

    def __init__(self, relay_url: str, bridge: "NostrBridge"):
        self.url = relay_url
        self.bridge = bridge
        self.ws = None
        self.connected = False
        self.sub_id = f"snin_mesh_{int(time.time())}_{hash(relay_url) % 10000}"
        self.reconnect_delay = 1
        self._running = False
        self.cb = CircuitBreaker(relay_url, max_failures=3, cooling=60,
                                on_open=lambda: bridge._on_relay_open(relay_url))
        self._auth_cycles = 0
        self._consecutive_rejects = 0
        self._pending_oks: dict[str, dict] = {}
        self._retry_queue: asyncio.Queue = asyncio.Queue()

    async def connect(self):
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
                self.url, ssl=ssl_ctx, ping_interval=30, ping_timeout=10,
                max_size=2**22, open_timeout=15, close_timeout=5
            )
            self.connected = True
            self.reconnect_delay = 1
            self.cb.record_success()
            print(f"[🔗{self.url}] Connected")
            return True
        except Exception as e:
            self.connected = False
            self.cb.record_failure()
            err_msg = str(e)[:80]
            print(f"[✗{self.url}] Connect fail: {err_msg}")
            await asyncio.sleep(self.reconnect_delay)
            self.reconnect_delay = min(self.reconnect_delay * 2, 60)
            return False

    async def listen(self):
        self._running = True
        while self._running:
            try:
                msg = await asyncio.wait_for(self.ws.recv(), timeout=300)
                data = orjson.loads(msg)
                if isinstance(data, list):
                    msg_type = data[0]
                    if msg_type == "EVENT":
                        sub_id, event = data[1], data[2]
                        kind = event.get("kind", -1)
                        if kind == 1 or kind == 39002:
                            await self.bridge.on_nostr_event(event)
                    elif msg_type == "OK":
                        event_id, ok, note = data[1], data[2], data[3] if len(data) > 3 else ""
                        await self._handle_ok(event_id, ok, note)
                    elif msg_type == "AUTH":
                        challenge = data[1]
                        await self._handle_auth(challenge)
                    elif msg_type == "NOTICE":
                        pass
            except asyncio.TimeoutError:
                try:
                    await self.ws.ping()
                except:
                    await self._reconnect()
            except websockets.exceptions.ConnectionClosed:
                await self._reconnect()
            except Exception as e:
                err_str = str(e)[:60]
                print(f"[✗{self.url}] Listen error: {err_str}")
                if self._running:
                    await self._reconnect()

    async def _handle_auth(self, challenge: str):
        pk = getattr(self.bridge, 'pubkey', getattr(self.bridge, '_pubkey_hex', ''))
        sk = getattr(self.bridge, "privkey", getattr(self.bridge, "_privkey_hex", ""))
        print(f"[AUTH] bridge.pubkey={'SET' if pk else 'EMPTY'} bridge.privkey={'SET' if sk else 'EMPTY'} privkey_len={len(sk) if sk else 'NONE'}")
        auth_event = make_auth_event(
            pubkey_hex=getattr(self.bridge, 'pubkey', getattr(self.bridge, '_pubkey_hex', '')),
            privkey_hex=getattr(self.bridge, "privkey", getattr(self.bridge, "_privkey_hex", "")),
            challenge=challenge,
            relay_url=self.url,
        )
        await self.ws.send(orjson.dumps(["AUTH", auth_event]).decode())
        self.bridge.stats["auth_challenges"] += 1
        self.bridge.stats["auth_success"] += 1

    async def _handle_ok(self, event_id: str, ok: bool, note: str):
        if ok:
            self.bridge.stats["confirmed"] += 1
            self._consecutive_rejects = 0
            self._pending_oks.pop(event_id, None)
            print(f"[✅{self.url[-35:]}] CONFIRMED id={event_id[:16]}...")
        else:
            self.bridge.stats["rejected"] += 1
            self._consecutive_rejects += 1
            self.bridge.stats["errors"] += 1
            print(f"[⛔{self.url[-40:]}] REJECTED id={event_id[:16]}... reason={note[:80]}")
            if "auth-required" in note.lower() or "restricted" in note.lower():
                if self._auth_cycles < 3:
                    self._auth_cycles += 1
                    pending = self._pending_oks.pop(event_id, None)
                    if pending:
                        await self._retry_queue.put(pending["event"])
                else:
                    self._pending_oks.pop(event_id, None)
            else:
                self._pending_oks.pop(event_id, None)

    async def publish(self, event: dict):
        self.bridge.stats["published"] += 1
        self._pending_oks[event["id"]] = {"event": event, "retries": 0}
        try:
            payload = orjson.dumps(["EVENT", event]).decode()
            await self.ws.send(payload)
            self.cb.record_success()
            print(f"[Publish] ✅ {self.url[-30:]} id={event.get('id','?')[:12]}...")
        except Exception as e:
            self.cb.record_failure()
            self.bridge.stats["errors"] += 1
            print(f"[Publish] ❌ {self.url[-30:]}: {type(e).__name__}: {e}")

    async def close(self):
        self._running = False
        if self.ws:
            try:
                await self.ws.close()
            except:
                pass
        self.connected = False

    async def _drain_retry_queue(self):
        while self._running:
            try:
                event = await asyncio.wait_for(self._retry_queue.get(), timeout=1)
                await self.publish(event)
            except asyncio.TimeoutError:
                continue

    async def _reconnect(self):
        self.connected = False
        delay = self.reconnect_delay
        if self.ws:
            try:
                await self.ws.close()
            except:
                pass
        self.ws = None
        print(f"[🔄{self.url}] Reconnecting in {delay}s...")
        await asyncio.sleep(delay)
        self.reconnect_delay = min(delay * 2, 60)
        await self.connect()

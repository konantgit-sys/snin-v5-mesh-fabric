"""External Gateway — внешние устройства в mesh.

Два входа:
  1. TCP Gateway (9931) — для ESP32, curl, любых TCP-клиентов
# import uvloop (disabled)
# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
  2. Nostr Gateway — подписка на 101 релей, kind:1 → mesh kind:39002

Всё отправляется в Smart Router (localhost:9932) → выбор канала → дальше по конвейеру.
"""

import asyncio
import json
import os
import sys
import time
import hashlib
from collections import defaultdict

sys.path.insert(0, "/home/agent/data/sites/relay-mesh")
from cpu_worker import make_nostr_id_async

# ─── Настройки ──────────────────────────────────────────────────────────
TCP_HOST = "0.0.0.0"
TCP_PORT = 9931

# Health endpoint
from mesh_health import start_health
start_health(TCP_PORT, "external_gateway")
# P0-E: через Smart Router (не напрямую в CR)
SMART_ROUTER_HOST = "127.0.0.1"
SMART_ROUTER_PORT = 9932

# Phase 3: Unix sockets
UNIX_SOCK_DIR = "/tmp/snin"
UNIX_GW_SOCK = f"{UNIX_SOCK_DIR}/nostr.sock"

GATEWAY_ID = os.path.basename(__file__).replace(".py", "")

# Nostr релеи (обновлено 2026-05-17 по результатам скана 7931 relay)
# Критерии: живые NIP-11, разные домены, разный софт, гео-баланс
NOSTR_RELAYS = [
    "wss://relay.damus.io",
    "wss://relay.primal.net",
    "wss://relay.nostr.info",
    "wss://nostr.wine",
    "wss://nostr.oxtr.dev",
    "wss://nostr-pub.wellorder.net",
    "wss://relay.f7z.io",
    "wss://relay.nostrati.com",
    "wss://relay.azzamo.net",              # 67 NIP — лучший в мире
    "wss://relay.nostrcheck.me",           # 28 NIP — khatru
    "wss://relay.nostriches.club",         # 28 NIP
    "wss://relay.npubhaus.com",            # 28 NIP
    "wss://relay.nosflare.com",            # 19 NIP
    "wss://relay.mostro.network",          # 16 NIP — mostro
    "wss://relay.nostr.moe",              # 19 NIP
    "wss://nostr.bond",                    # 33 NIP — shugur
    "wss://relay.aidatanorge.no",          # 20 NIP — Норвегия
    "wss://nostr.einundzwanzig.space",     # Германия
    "wss://soloco.nl",                     # Нидерланды
    "wss://relay.degmods.com",             # EU
    "wss://relay.nostrplebs.com",          # US
    "wss://purplepag.es",                  # US
    "wss://relay.minibits.cash",           # US
    "wss://nostr.mom",                     # Япония/Азия
    "wss://airchat.nostr1.com",            # nostr1.com (1 из 759)
]

# ─── Счётчики ──────────────────────────────────────────────────────────
stats = defaultdict(int)
start_time = time.time()


# ─── Утилиты ────────────────────────────────────────────────────────────
def make_nostr_id(pubkey: str, content: str, kind: int, ts: int) -> str:
    """Генерация Nostr event id."""
    raw = json.dumps([0, pubkey, ts, kind, [], content], separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def mesh_event(pubkey: str, content: str, kind: int = 39002,
               created_at: int = 0, sig: str = "") -> dict:
    """Сформировать событие для Smart Router (формат с meta.origin)."""
    ts = created_at or int(time.time())
    ev = {
        "id": make_nostr_id(pubkey, content, kind, ts),
        "kind": kind,
        "pubkey": pubkey,
        "content": content,
        "created_at": ts,
        "sig": sig or "gw_" + hashlib.md5(pubkey.encode()).hexdigest()[:32],
        "meta": {
            "origin": "external_gateway",
            "channel": "auto",
            "priority": "normal",
        },
    }
    return ev


async def mesh_event_async(pubkey: str, content: str, kind: int = 39002,
                          created_at: int = 0, sig: str = "") -> dict:
    """Async версия mesh_event — SHA256 в thread pool (Level 1)."""
    ts = created_at or int(time.time())
    event_id = await make_nostr_id_async(pubkey, content, kind, ts)
    ev = {
        "id": event_id,
        "kind": kind,
        "pubkey": pubkey,
        "content": content,
        "created_at": ts,
        "sig": sig or "gw_" + hashlib.md5(pubkey.encode()).hexdigest()[:32],
        "meta": {
            "origin": "external_gateway",
            "channel": "auto",
            "priority": "normal",
        },
    }
    return ev


# ─── TCP Gateway ────────────────────────────────────────────────────────
class TCPGateway:
    """TCP сервер для внешних устройств (ESP32, curl, любой клиент).
    
    Принимает строчный JSON на порту 9931.
    Формат: {"kind": 1, "pubkey": "hex", "content": "текст", ...}
    Отправляет в Smart Router на 9932.
    """

    def __init__(self):
        self.sr_writer = None
        self.sr_reader = None
        self.sr_connected = False
        self.stats = defaultdict(int)

    async def connect_sr(self):
        """Подключение к Smart Router (exponential backoff — Фаза 3)."""
        for attempt in range(10):
            try:
                self.sr_reader, self.sr_writer = await asyncio.open_connection(
                    SMART_ROUTER_HOST, SMART_ROUTER_PORT
                )
                self.sr_connected = True
                print(f"[TCPGateway] ✅ Connected to Smart Router ({SMART_ROUTER_HOST}:{SMART_ROUTER_PORT})")
                return True
            except ConnectionRefusedError:
                self.sr_connected = False
                delay = min(0.5 * (2 ** attempt), 30)  # 0.5, 1, 2, 4, 8, 16, 30...
                print(f"[TCPGateway] ⏳ SR connect attempt {attempt+1}/10, retry in {delay:.0f}s...")
                await asyncio.sleep(delay)
        print(f"[TCPGateway] ❌ Cannot connect to Smart Router after 10 attempts")
        return False

    async def send_to_sr(self, event: dict):
        """Отправить событие в Smart Router (TCP line-based JSON)."""
        if not self.sr_connected or self.sr_writer is None:
            self.stats["sr_errors"] += 1
            return False
        try:
            line = json.dumps(event) + "\n"
            self.sr_writer.write(line.encode())
            await self.sr_writer.drain()
            self.stats["sent_to_sr"] += 1
            return True
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.sr_connected = False
            self.sr_writer = None
            self.stats["sr_errors"] += 1
            print(f"[TCPGateway] ⚠️ Lost connection to SR, reconnecting...")
            asyncio.create_task(self.connect_sr())
            return False

    async def handle_client(self, reader, writer):
        """Обработка одного TCP-клиента (ESP32, curl, ...)."""
        peer = writer.get_extra_info("peername", ("?", 0))
        addr = f"{peer[0]}:{peer[1]}"
        self.stats["connections"] += 1
        self.stats["clients"] += 1

        while True:
            try:
                line = await reader.readline()
                if not line:
                    break
                line = line.decode().strip()
                if not line:
                    continue

                # Парсинг JSON
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    self.stats["bad_json"] += 1
                    continue

                # Nostr протокол: данные могут быть списком ["EVENT", {...}]
                if isinstance(data, list):
                    if len(data) >= 2 and isinstance(data[1], dict):
                        data = data[1]
                    else:
                        self.stats["bad_json"] += 1
                        continue
                # Если не dict — пропускаем
                if not isinstance(data, dict):
                    self.stats["bad_json"] += 1
                    continue

                kind = data.get("kind", 39002)
                pubkey = data.get("pubkey", "")
                content = data.get("content", "")
                ts = data.get("created_at", 0)
                sig = data.get("sig", "")

                if not pubkey:
                    pubkey = f"ext_{hashlib.md5(addr.encode()).hexdigest()[:16]}"
                if not content:
                    continue

                # Нормализация kind:1 (Nostr текст) → kind:39002 (mesh content)
                if kind == 1:
                    mesh_content = json.dumps({
                        "from": f"ext_{pubkey[:8]}",
                        "seq": self.stats["kind1_received"],
                        "payload": {
                            "type": "nostr_bridge",
                            "text": content[:1000],
                            "source": addr,
                        },
                    })
                    event = await mesh_event_async(pubkey, mesh_content, 39002, ts, sig)
                else:
                    # kind:39002 напрямую
                    event = await mesh_event_async(pubkey, content, kind, ts, sig)

                # Отправка в Smart Router
                ok = await self.send_to_sr(event)
                self.stats["received"] += 1
                if ok:
                    self.stats["forwarded"] += 1
                    if kind == 1:
                        self.stats["kind1_forwarded"] += 1
                else:
                    self.stats["errors"] += 1

                # P0-E: ack клиенту
                ack = {"ok": ok, "id": event.get("id", "")[:16]}
                try:
                    writer.write(json.dumps(ack) + b"\n")
                    await writer.drain()
                except Exception:
                    pass

            except (ConnectionResetError, BrokenPipeError):
                break
            except Exception as e:
                self.stats["errors"] += 1
                print(f"[TCPGateway] ⚠️ Error from {addr}: {e}")
                break

        writer.close()
        self.stats["clients"] -= 1
        self.stats["disconnects"] += 1

    async def run(self):
        """Запуск TCP сервера + Unix socket (Phase 3)."""
        # Phase 3: Unix socket
        os.makedirs(UNIX_SOCK_DIR, exist_ok=True)
        try:
            os.unlink(UNIX_GW_SOCK)
        except FileNotFoundError:
            pass
        unix_server = await asyncio.start_unix_server(
            self.handle_client, UNIX_GW_SOCK)
        print(f"[TCPGateway] Unix socket {UNIX_GW_SOCK}")
        
        # TCP для внешних клиентов
        server = await asyncio.start_server(
            self.handle_client,
            TCP_HOST,
            TCP_PORT,
        )
        addr = server.sockets[0].getsockname()
        print(f"[TCPGateway] 🚀 Listening on TCP {addr[0]}:{addr[1]}")
        print(f"[TCPGateway]    Forward → Smart Router ({SMART_ROUTER_HOST}:{SMART_ROUTER_PORT})")

        # Подключаемся к SR в фоне
        asyncio.create_task(self.connect_sr())

        async with server, unix_server:
            await server.serve_forever()


# ─── Nostr Gateway ──────────────────────────────────────────────────────
class NostrGateway:
    """Подписка на Nostr kind:1 (посты) → mesh kind:39002 (content).
    
    Читает посты с публичных релеев, пересылает в mesh.
    """

    def __init__(self):
        self.tcp_gw = None
        self.stats = defaultdict(int)

    def set_tcp_gateway(self, tcp_gw: TCPGateway):
        """Привязать TCP Gateway для отправки в mesh."""
        self.tcp_gw = tcp_gw

    async def subscribe_relay(self, relay_url: str):
        """Подписка на один Nostr relay (с exponential backoff — Фаза 3)."""
        attempt = 0
        while True:
            attempt += 1
            
            # Если релей не отвечает 10+ раз — в чёрный список
            if attempt > 10:
                print(f"[NostrGW] ⛔ {relay_url} — превышен лимит подключений (10), в чёрный список")
                self.stats["relays_blocked"] = self.stats.get("relays_blocked", 0) + 1
                return
            
            try:
                import websockets

                ws = await asyncio.wait_for(
                    websockets.connect(relay_url, max_size=1_000_000, ping_interval=15),
                    timeout=10,
                )
                attempt = 0  # сброс на успешном
                print(f"[NostrGW] ✅ Connected to {relay_url}")

                sub_msg = json.dumps(["REQ", GATEWAY_ID, {
                    "kinds": [1, 7],
                    "limit": 10,
                }])
                await ws.send(sub_msg)

                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        try:
                            pong = await ws.ping()
                            await asyncio.wait_for(pong, timeout=3)
                        except Exception:
                            break
                        continue

                    try:
                        parsed = json.loads(msg)
                    except json.JSONDecodeError:
                        continue

                    if not isinstance(parsed, list) or len(parsed) < 2:
                        continue

                    msg_type = parsed[0]

                    if msg_type == "EVENT" and len(parsed) >= 3:
                        event = parsed[2]
                        kind = event.get("kind", 1)
                        pubkey = event.get("pubkey", "")
                        content = event.get("content", "")
                        created_at = event.get("created_at", 0)

                        if not pubkey or not content:
                            continue

                        if kind == 1:
                            short = content[:500]
                            mesh_content = json.dumps({
                                "from": f"nostr_{pubkey[:8]}",
                                "seq": self.stats["nostr_events"],
                                "payload": {
                                    "type": "nostr_post",
                                    "text": short,
                                    "original_kind": kind,
                                    "original_pubkey": pubkey,
                                },
                            })
                            event_mesh = await mesh_event_async(pubkey, mesh_content, 39002, created_at)
                            if self.tcp_gw:
                                ok = await self.tcp_gw.send_to_sr(event_mesh)
                                if ok:
                                    self.stats["forwarded"] += 1
                            self.stats["nostr_events"] += 1

                        elif kind == 7:
                            short = content[:200]
                            mesh_content = json.dumps({
                                "from": f"nostr_{pubkey[:8]}",
                                "seq": self.stats["nostr_events"],
                                "payload": {
                                    "type": "nostr_reaction",
                                    "reaction": short,
                                    "original_kind": kind,
                                },
                            })
                            event_mesh = await mesh_event_async(pubkey, mesh_content, 39003, created_at)
                            if self.tcp_gw:
                                ok = await self.tcp_gw.send_to_sr(event_mesh)
                                if ok:
                                    self.stats["forwarded"] += 1
                            self.stats["nostr_events"] += 1

                    elif msg_type == "EOSE":
                        print(f"[NostrGW] ✅ EOSE from {relay_url}, now streaming...")

                await ws.close()
                print(f"[NostrGW] Disconnected from {relay_url}")

            except (asyncio.TimeoutError, Exception) as e:
                print(f"[NostrGW] ⚠️ {relay_url}: {e}")

            # Exponential backoff: 1s, 2s, 4s, 8s, 16s, 30s max
            delay = min(1 * (2 ** min(attempt - 1, 5)), 30)
            print(f"[NostrGW] 🔄 Reconnect {relay_url} in {delay:.0f}s (attempt {attempt})")
            await asyncio.sleep(delay)

    async def run(self):
        """Запуск подписки на все релеи."""
        tasks = []
        for url in NOSTR_RELAYS:
            tasks.append(self.subscribe_relay(url))
            await asyncio.sleep(0.1)  # разнести старты

        print(f"[NostrGW] 📡 Subscribing to {len(NOSTR_RELAYS)} relays")
        await asyncio.gather(*tasks, return_exceptions=True)


# ─── Статистика ─────────────────────────────────────────────────────────
async def print_stats(tcp_gw: TCPGateway, nostr_gw: NostrGateway):
    """Печать статистики каждые 15 секунд."""
    while True:
        await asyncio.sleep(15)
        elapsed = int(time.time() - start_time)
        print(f"\n[Gateway] {'='*50}")
        print(f"[Gateway] Uptime: {elapsed}s")
        print(f"[Gateway] TCP Gateway:")
        print(f"  connections: {tcp_gw.stats['connections']} | "
              f"current: {tcp_gw.stats['clients']} | "
              f"received: {tcp_gw.stats['received']} | "
              f"forwarded: {tcp_gw.stats['forwarded']} | "
              f"errors: {tcp_gw.stats['errors']}")
        print(f"  kind:1 forwarded: {tcp_gw.stats['kind1_forwarded']} | "
              f"bad JSON: {tcp_gw.stats['bad_json']} | "
              f"SR errors: {tcp_gw.stats['sr_errors']}")
        print(f"[Gateway] Nostr Gateway:")
        print(f"  events: {nostr_gw.stats['nostr_events']} | "
              f"forwarded: {nostr_gw.stats['forwarded']} | "
              f"relays: {len(NOSTR_RELAYS)}")
        rate = tcp_gw.stats['forwarded'] / max(elapsed, 1)
        print(f"[Gateway] Forward rate: {rate:.1f}/sec")
        print(f"[Gateway] {'='*50}\n")


# ─── Main ───────────────────────────────────────────────────────────────
async def main():
    tcp_gw = TCPGateway()
    nostr_gw = NostrGateway()
    nostr_gw.set_tcp_gateway(tcp_gw)

    await asyncio.gather(
        tcp_gw.run(),
        nostr_gw.run(),
        print_stats(tcp_gw, nostr_gw),
    )


if __name__ == "__main__":
    print(f"[Gateway] External Gateway v1 — {len(NOSTR_RELAYS)} Nostr relays + TCP {TCP_PORT}")
    print(f"[Gateway] Forward target: {SMART_ROUTER_HOST}:{SMART_ROUTER_PORT} (Smart Router)")
    
    import signal
    signal.signal(signal.SIGTERM, lambda s, f: (print(f"\n[Gateway] SIGTERM — shutdown. Total forwarded: {stats['forwarded']}"), sys.exit(0)))
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[Gateway] Shutdown. Total forwarded: {stats['forwarded']}")

#!/usr/bin/env python3
"""Nostr Bridge — двусторонний Nostr ↔ Mesh мост (Фаза 3: refactored).

Архитектура:
  Nostr (WebSocket, 101 релей)
    ↓ kind:1 → kind:39002  (Nostr → Mesh)
    ↑ kind:39002 → kind:1  (Mesh → Nostr)

Запуск:
  python3 nostr_bridge.py --shard-id 0 --total-shards 5

Импортирует:
  - nostr_core: NostrRelayClient, CircuitBreaker, signing
  - nostr_relay_list: relay config, shard slicing
"""

import asyncio
import gc
import json
import orjson
import os
import signal
import sys
import time
import argparse

# ═══ Monkey-patch: websockets connection_lost ═══
import websockets.asyncio.connection as _ws_conn
_ws_orig_connection_lost = _ws_conn.Connection.connection_lost
def _ws_safe_connection_lost(self, exc):
    if not hasattr(self, 'recv_messages'):
        return
    return _ws_orig_connection_lost(self, exc)
_ws_conn.Connection.connection_lost = _ws_safe_connection_lost

import websockets

sys.path.insert(0, "/home/agent/data/sites/relay-mesh")

from ttl_cache import TTLCache
from cpu_worker import sign_event_full_async
from mesh_config import config
from mesh_health import start_health

# ─── Nostr core + relay list (Phase 3 modules) ───
from nostr_core import (
    NostrRelayClient, CircuitBreaker,
    sign_event_async, sign_event, make_auth_event, make_relay_list_event,
)
from nostr_relay_list import (
    init as init_relay_list, get_our_relays, get_scan_relays, save_discovered_relays,
    RELAY_TIERS, RELAY_LIST_INTERVAL, SCAN_INTERVAL, PUBLISH_QUEUE_INTERVAL,
    AGENTS_FILE, DISCOVERED_FILE,
)

init_relay_list(config)

# ═══ Shard CLI ═══
_shard_parser = argparse.ArgumentParser()
_shard_parser.add_argument("--shard-id", type=int, default=0)
_shard_parser.add_argument("--total-shards", type=int, default=1)
_shard_args, _ = _shard_parser.parse_known_args()
SHARD_ID = _shard_args.shard_id
TOTAL_SHARDS = _shard_args.total_shards
PUBLISHER_SHARD_ID = 0
IS_PUBLISHER = SHARD_ID == PUBLISHER_SHARD_ID

# ─── Настройки ───
SMART_ROUTER_HOST = config.get("nostr.smart_router_host", "127.0.0.1")
SMART_ROUTER_PORT = config.get("transport.smart_router.port", 9932)
GATEWAY_BASE = config.get("nostr.bridge_base_port", 9941)
GATEWAY_PORT = GATEWAY_BASE + SHARD_ID

start_health(GATEWAY_PORT, f"nostr_bridge_{SHARD_ID}")

# ─── Релеи шарда ───
OUR_RELAYS = get_our_relays(SHARD_ID, TOTAL_SHARDS)
SCAN_RELAYS = get_scan_relays(SHARD_ID, TOTAL_SHARDS)

# ─── Статистика ───
stats = {
    "nostr_to_mesh": 0, "mesh_to_nostr": 0,
    "relay_list_discovered": 0, "auth_challenges": 0, "auth_success": 0,
    "published": 0, "confirmed": 0, "rejected": 0, "errors": 0,
    "started_at": time.time(),
}
# ───── NostrBridge class ─────

class NostrBridge:
    """
    Двусторонний мост между Nostr и Mesh.
    - SmartRouter (:9932) ↔ Mesh события (kind:39002)
    - Nostr релеи (WS) ↔ kind:1 публикации
    - TCP Gateway (GATEWAY_PORT) для mesh→Nostr
    - Local WS Relay (:9961+shard) для Cryter
    """

    def __init__(self, pubkey_hex: str = "", privkey_hex: str = ""):
        self.pubkey = pubkey_hex
        self.privkey = privkey_hex
        self.clients: list[NostrRelayClient] = []
        self._publish_queue: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._published_cache: TTLCache = TTLCache(maxsize=5000, ttl=600)
        self._rss_limit_mb = 500
        self._relay_pool: dict[int, list[str]] = {}
        for url, tier in RELAY_TIERS.items():
            self._relay_pool.setdefault(tier, []).append(url)
        self._dead_relays: dict[str, float] = {}
        self._rate_events_per_window = 500
        self._rate_window = 30
        self._rate_tokens = self._rate_events_per_window
        self._rate_last_refill = time.time()
        self.sr_reader = None
        self.sr_writer = None
        self.sr_connected = False
        self._running = False
        self._relay_list_task = None
        self._sr_listener_task = None
        self._publisher_task = None
        self._discovered_relays: set = set()

    def get_pubkey(self) -> str:
        return self.pubkey

    def get_privkey(self) -> str:
        return self.privkey

    # ── SmartRouter ──

    async def connect_sr(self):
        try:
            self.sr_reader, self.sr_writer = await asyncio.wait_for(
                asyncio.open_connection(SMART_ROUTER_HOST, SMART_ROUTER_PORT),
                timeout=10
            )
            self.sr_connected = True
            print(f"[Bridge] ✅ Connected to SmartRouter :{SMART_ROUTER_PORT}")
            return True
        except Exception as e:
            self.sr_connected = False
            print(f"[Bridge] ❌ SmartRouter :{SMART_ROUTER_PORT}: {e}")
            return False

    async def _sr_reconnect_loop(self):
        delay = 5
        while self._running and not self.sr_connected:
            await asyncio.sleep(delay)
            if await self.connect_sr():
                if self._sr_pending_listener:
                    self._sr_listener_task = asyncio.create_task(self.listen_sr())
                    self._sr_pending_listener = False
            delay = min(delay * 1.5, 60)

    async def listen_sr(self):
        """Читать события из SmartRouter (mesh→Nostr)."""
        buffer = ""
        while self._running:
            try:
                chunk = await asyncio.wait_for(self.sr_reader.read(65536), timeout=60)
                if not chunk:
                    print(f"[Bridge] ⚠️ SR connection closed")
                    self.sr_connected = False
                    break
                buffer += chunk.decode()
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = orjson.loads(line)
                    except (orjson.JSONDecodeError, ValueError):
                        continue
                    kind = msg.get("kind", 0)
                    # kind:39002 → публикуем в Nostr как kind:1
                    if kind == 39002:
                        await self._handle_mesh_event(msg)
                    # kind:39000 (heartbeat) → relay list event
                    elif kind == 39000:
                        tags = msg.get("tags", [])
                        relay_tags = [t[1] for t in tags if isinstance(t, list) and len(t) >= 2 and t[0] == "r"]
                        if relay_tags:
                            for url in relay_tags:
                                if url not in self._discovered_relays:
                                    self._discovered_relays.add(url)
            except asyncio.TimeoutError:
                continue
            except (ConnectionResetError, BrokenPipeError):
                print(f"[Bridge] ⚠️ SR connection lost")
                self.sr_connected = False
                break

    async def _handle_mesh_event(self, msg: dict):
        """Обработать mesh событие kind:39002 → kind:1 Nostr."""
        content = msg.get("payload", {}).get("text", "") or msg.get("content", "")
        if not content:
            return
        ev_id = msg.get("id", "")
        seen_key = ev_id or (msg.get("pubkey", ""), content[:200])
        if ev_id:
            if not self._published_cache.add(ev_id):
                return
        elif not self._published_cache.add(seen_key):
            return
        nostr_event = await sign_event_async(
            pubkey_hex=self.pubkey,
            private_key_hex=self.privkey,
            content=content,
            kind=1,
            tags=msg.get("tags", []),
        )
        try:
            self._publish_queue.put_nowait(nostr_event)
            stats["mesh_to_nostr"] += 1
        except asyncio.QueueFull:
            stats["dropped"] = stats.get("dropped", 0) + 1

    # ── Nostr события ──

    async def on_nostr_event(self, event: dict):
        """Обработать событие из Nostr → отправить в SmartRouter."""
        if event.get("kind") not in (1, 39002):
            return
        # Rate limit
        now = time.time()
        elapsed = now - self._rate_last_refill
        self._rate_tokens += elapsed * (self._rate_events_per_window / self._rate_window)
        if self._rate_tokens > self._rate_events_per_window:
            self._rate_tokens = self._rate_events_per_window
        self._rate_last_refill = now
        if self._rate_tokens < 1:
            return
        self._rate_tokens -= 1
        self._discovered_relays.add(self._get_relay_for_event(event))
        if len(self._discovered_relays) % 100 == 0:
            save_discovered_relays(self._discovered_relays)
        # NIP-65: kind:10002 relay list
        if event.get("kind") == 10002:
            await self._handle_relay_list_event(event)
            return
        # kind:1 → mesh kind:39002
        text = event.get("content", "")
        if not text:
            return
        # Dups
        pubkey = event.get("pubkey", "")
        tag_e = [t[1] for t in event.get("tags", []) if isinstance(t, list) and len(t) >= 2 and t[0] == "e"]
        dedup_key = f"{pubkey}:{text[:80]}:{':'.join(tag_e[:2])}"
        if not self._published_cache.add(dedup_key):
            return
        mesh_msg = orjson.dumps({
            "from": "nostr",
            "to": "mesh",
            "kind": 39002,
            "pubkey": pubkey,
            "payload": {"text": text, "source": f"nostr:{event.get('id','')}"},
            "meta": {"channel": "mesh", "route": "nostr"},
        }).decode() + "\n"
        if self.sr_writer and self.sr_connected:
            try:
                self.sr_writer.write(mesh_msg.encode())
                await self.sr_writer.drain()
                stats["nostr_to_mesh"] += 1
            except Exception:
                pass

    def _get_relay_for_event(self, event: dict) -> str:
        tags = event.get("tags", [])
        for t in tags:
            if isinstance(t, list) and len(t) >= 2 and t[0] == "r" and t[1].startswith("wss://"):
                return t[1]
        return "wss://relay.primal.net"

    # ── Публикация ──

    async def publish_loop(self):
        while self._running:
            try:
                event = await asyncio.wait_for(self._publish_queue.get(), timeout=1)
                if not self.clients:
                    await asyncio.sleep(0.5)
                    continue
                alive = [c for c in self.clients if c.connected and not c.cb.permanently_dead]
                if not alive:
                    continue
                for c in alive[:3]:
                    asyncio.create_task(c.publish(event))
                self._published_cache.add(event.get("id", ""))
            except asyncio.TimeoutError:
                continue

    async def publish_relay_list(self):
        """Публиковать NIP-65 relay list раз в час."""
        while self._running:
            event = make_relay_list_event(
                pubkey_hex=self.pubkey, privkey_hex=self.privkey,
                read_relays=SCAN_RELAYS + list(self._discovered_relays)[:50],
                write_relays=OUR_RELAYS,
            )
            for c in self.clients:
                if c.connected and not c.cb.permanently_dead:
                    await c.publish(event)
            await asyncio.sleep(RELAY_LIST_INTERVAL)

    async def _handle_relay_list_event(self, event: dict):
        tags = event.get("tags", [])
        read_relays = [t[1] for t in tags if isinstance(t, list) and len(t) >= 2 and t[1].startswith(("ws://", "wss://"))]
        added = 0
        for url in read_relays:
            if url not in self._discovered_relays:
                self._discovered_relays.add(url)
                added += 1
        if added:
            stats["relay_list_discovered"] += added
            save_discovered_relays(self._discovered_relays)
            print(f"[Bridge] 🌐 NIP-65 Discovery: +{added} new relays (total: {len(self._discovered_relays)})")

    # ── Запуск ──

    async def start(self):
        self._running = True
        print(f"{'='*50}\n  SNIN Nostr ↔ Mesh Bridge (shard {SHARD_ID}/{TOTAL_SHARDS})\n{'='*50}")
        if IS_PUBLISHER:
            self._gateway_task = asyncio.create_task(self._gateway_loop())
        else:
            print(f"[Bridge] 📡 Shard-{SHARD_ID} = scanner mode (no gateway)")
        self._local_relay_task = asyncio.create_task(self._local_relay_server())
        await asyncio.sleep(0.5)
        sr_ok = await self.connect_sr()
        if not sr_ok:
            print("[Bridge] ❌ Cannot connect to SmartRouter — retrying in bg")
            self._sr_pending_listener = True
            self._sr_reconnect_task = asyncio.create_task(self._sr_reconnect_loop())
        print(f"\n[Bridge] 🔌 Connecting to {len(SCAN_RELAYS)} Nostr relays...")
        for url in SCAN_RELAYS:
            client = NostrRelayClient(url, self)
            ok = await client.connect()
            self.clients.append(client)
            await asyncio.sleep(0.1)
        connected = sum(1 for c in self.clients if c.connected)
        print(f"\n[Bridge] ✅ {connected}/{len(self.clients)} relays connected")
        print(f"[Bridge] 🔄 Starting background loops...")
        nostr_tasks = [asyncio.create_task(client.listen()) for client in self.clients]
        if sr_ok:
            self._sr_listener_task = asyncio.create_task(self.listen_sr())
        else:
            self._sr_pending_listener = True
        if IS_PUBLISHER:
            self._publisher_task = asyncio.create_task(self.publish_loop())
            self._relay_list_task = asyncio.create_task(self.publish_relay_list())
        else:
            print(f"[Bridge] 🔇 Shard-{SHARD_ID} = no publishing")
        self._memory_check_task = asyncio.create_task(self._memory_self_check())
        print(f"[Bridge] ✅ Running. {connected} Nostr relays ↔ Mesh")
        all_tasks = []
        if self._publisher_task:
            all_tasks.append(self._publisher_task)
        if self._relay_list_task:
            all_tasks.append(self._relay_list_task)
        if self._sr_listener_task:
            all_tasks.append(self._sr_listener_task)
        if self._gateway_task:
            all_tasks.append(self._gateway_task)
        all_tasks.extend(nostr_tasks)
        all_tasks.append(self._memory_check_task)
        try:
            await asyncio.gather(*all_tasks)
        except Exception as e:
            print(f"[Bridge] 💀 {e}")
            raise

    # ── Gateway ──

    async def _gateway_loop(self):
        for attempt in range(5):
            try:
                server = await asyncio.start_server(
                    self._gateway_handler, "127.0.0.1", GATEWAY_PORT
                )
                async with server:
                    print(f"[Bridge] 📡 Gateway listening on {GATEWAY_PORT} (mesh→Nostr)")
                    await server.serve_forever()
            except OSError as e:
                if "address already in use" in str(e).lower() and attempt < 4:
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
        peer = writer.get_extra_info('peername')
        print(f"[GW] ⚡ Connection from {peer}")
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                raw = line.decode().strip()
                event = orjson.loads(raw)
                if event.get("kind") == 39002:
                    content = event.get("payload", {}).get("text", "") or event.get("content", "")
                    if content:
                        ev_id = event.get("id", "")
                        if ev_id and not self._published_cache.add(ev_id):
                            continue
                        nostr_event = await sign_event_async(
                            pubkey_hex=self.pubkey, private_key_hex=self.privkey,
                            content=content, kind=1, tags=event.get("tags", []),
                        )
                        try:
                            self._publish_queue.put_nowait(nostr_event)
                        except asyncio.QueueFull:
                            stats["dropped"] = stats.get("dropped", 0) + 1
                        stats["mesh_to_nostr"] += 1
        except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError, ValueError) as e:
            print(f"[GW] ⚠️ {e}")
        finally:
            writer.close()
    # ── Graceful Degradation ──

    def _get_fallback_relay(self, dead_url: str) -> str | None:
        dead_tier = RELAY_TIERS.get(dead_url)
        if dead_tier is None:
            return None
        same_tier = self._relay_pool.get(dead_tier, [])
        alive_same = [u for u in same_tier if u != dead_url and u not in self._dead_relays]
        if alive_same:
            print(f"[Bridge] ⚡ Fallback: {dead_url} → {alive_same[0]} (same TIER {dead_tier})")
            return alive_same[0]
        for tier in sorted(self._relay_pool.keys()):
            if tier > dead_tier:
                candidates = [u for u in self._relay_pool[tier] if u not in self._dead_relays]
                if candidates:
                    print(f"[Bridge] ⚡ Fallback: {dead_url} → {candidates[0]} (TIER {tier} reserve)")
                    return candidates[0]
        return None

    def _on_relay_open(self, dead_url: str):
        self._dead_relays[dead_url] = time.time()
        if len(self.clients) >= 30:
            return
        evicted = [c for c in self.clients if c.cb.permanently_dead]
        for c in evicted:
            self.clients.remove(c)
        fallback = self._get_fallback_relay(dead_url)
        if fallback:
            new_client = NostrRelayClient(fallback, self)
            self.clients.append(new_client)
            asyncio.create_task(new_client.listen())

    async def _memory_self_check(self):
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

    # ── Local Nostr Relay (NIP-01 WS) для Cryter ──

    LOCAL_RELAY_PORT = 9961 + SHARD_ID

    async def _local_relay_server(self):
        async def handler(ws):
            peer = ws.remote_address
            try:
                async for raw in ws:
                    try:
                        msg = orjson.loads(raw)
                    except ValueError:
                        continue
                    if not isinstance(msg, list) or len(msg) < 2:
                        continue
                    msg_type = msg[0]
                    if msg_type == "EVENT":
                        event = msg[1]
                        if event.get("kind") == 1:
                            try:
                                self._publish_queue.put_nowait(event)
                            except asyncio.QueueFull:
                                await ws.send(orjson.dumps(["OK", event.get("id", ""), False, "rate-limited"]))
                                continue
                            stats["mesh_to_nostr"] += 1
                            await ws.send(orjson.dumps(["OK", event.get("id", ""), True, ""]))
                        else:
                            await ws.send(orjson.dumps(["OK", event.get("id", ""), False, "unsupported kind"]))
                    elif msg_type == "REQ":
                        await ws.send(orjson.dumps(["EOSE", msg[1]]))
                    elif msg_type == "CLOSE":
                        break
            except websockets.exceptions.ConnectionClosed:
                pass
            except Exception as e:
                print(f"[LR] ⚠️ WS error: {e}")

        for attempt in range(5):
            try:
                async with websockets.serve(handler, "127.0.0.1", self.LOCAL_RELAY_PORT):
                    print(f"[Bridge] 📡 Local Nostr Relay on ws://127.0.0.1:{self.LOCAL_RELAY_PORT}")
                    await asyncio.Future()
            except OSError as e:
                if "address already in use" in str(e).lower() and attempt < 4:
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


# ───── NostrBridgeLayer — API для FirstContact ─────

class NostrBridgeLayer:
    def __init__(self):
        self.bridge = None
        self._running = False

    async def start(self, pubkey_hex: str = "", privkey_hex: str = ""):
        self.bridge = NostrBridge(
            pubkey_hex=pubkey_hex or "npub1snin_mesh_bridge",
            privkey_hex=privkey_hex or "",
        )
        self._running = True
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


# ───── Main ─────

if __name__ == "__main__":
    print("[Nostr Bridge] Starting...")
    pubkey = "npub1snin_mesh_bridge"
    privkey = ""
    try:
        with open(AGENTS_FILE) as f:
            agents = json.load(f)
            for pk, info in agents.items():
                if info.get("name") == "archivist_ai":
                    pubkey = info.get("meta", {}).get("nostr_pubkey", pk)
                    privkey = info.get("meta", {}).get("nostr_privkey", "")
                    break
    except Exception as e:
        print(f"[Bridge] ⚠️ Cannot load agents.json: {e}")

    bridge = NostrBridge(pubkey_hex=pubkey, privkey_hex=privkey)
    _shutting_down = False

    def _handle_sigterm():
        global _shutting_down
        if _shutting_down:
            return
        _shutting_down = True
        print(f"\n[Bridge] SIGTERM received — graceful shutdown...")
        if hasattr(bridge, 'stop'):
            asyncio.run(bridge.stop())

    signal.signal(signal.SIGTERM, lambda s, f: _handle_sigterm())
    try:
        asyncio.run(bridge.start())
    except KeyboardInterrupt:
        print("\n[Bridge] Stopping...")
        asyncio.run(bridge.stop())

#!/usr/bin/env python3
"""Route Engine — Маршрутизатор между P2P bridge и relay-mesh.

Классифицирует события по kind:
  - kind:39000 с type=heartbeat → bypass→файл (не в relay)
  - kind:39001 (DHT) → batch → POST /api/ingest/batch
  - kind:39010+ (DAO) → immediate → POST /api/ingest
  - kind:39000/39002/39003 (mesh) → batch → POST /api/ingest/batch

Запуск:
    python3 route_engine.py

Принимает Nostr-события через TCP (localhost:9910) в формате JSON.
Каждое событие — одна строка JSON с переводом строки.
"""

import asyncio
# import uvloop (disabled)
# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
import orjson
import json as _json
import time
import os
import sys
from collections import defaultdict

import httpx

RELAY_MESH = "http://localhost:9907"
SMART_ROUTER_HOST = "127.0.0.1"
SMART_ROUTER_PORT = 9932
HEARTBEAT_LOG = "/home/agent/data/heartbeat.log"
BATCH_WINDOW = 0.1  # сек — накопление batch (10 flushes/sec)
LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 9910

# Phase 3: Unix sockets для внутренней коммуникации
UNIX_SOCK_DIR = "/tmp/snin"
UNIX_SOCK_PATH = f"{UNIX_SOCK_DIR}/re.sock"


class RouteEngine:
    """Классификатор + батчер + bypass."""

    def __init__(self):
        self.batches = defaultdict(list)  # type -> list of events
        self.last_flush = time.time()
        self._http = None  # httpx.AsyncClient (lazy init)
        self.stats = {
            "received": 0,
            "heartbeat_bypassed": 0,
            "dao_immediate": 0,
            "batched": 0,
            "dht_redis": 0,
            "errors": 0,
            "flushes": 0,
            "ws_flushes": 0,
        }

    async def _get_http(self):
        """Ленивая инициализация httpx клиента (один на всё время жизни)."""
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=httpx.Timeout(5.0, connect=2.0),
                limits=httpx.Limits(max_keepalive_connections=4, max_connections=8),
            )
        return self._http

    async def _get_http(self):
        """Ленивая инициализация httpx клиента (один на всё время жизни)."""
        if self._http is None or self._http.is_closed:
            import httpx
            self._http = httpx.AsyncClient(
                timeout=httpx.Timeout(5.0, connect=2.0),
                limits=httpx.Limits(max_keepalive_connections=4, max_connections=8),
            )
        return self._http

    def classify(self, event: dict) -> str:
        """Вернуть тип маршрута: heartbeat / dht / dao / mesh / unknown."""
        kind = event.get("kind")
        if kind == 39000:
            try:
                content = _json.loads(event.get("content", "{}"))
                if content.get("type") == "heartbeat":
                    return "heartbeat"
                if content.get("type") == "hello":
                    return "heartbeat"  # hello = одноразовый heartbeat
            except (_json.JSONDecodeError, TypeError):
                pass
            return "mesh"
        if kind == 39001:
            return "dht"
        if kind in (39010, 39011, 39012, 39013):
            return "dao"
        if kind in (39020, 39021):
            return "nft"
        if kind == 30000:
            return "solana"
        if kind in (39002, 39003):
            return "mesh"
        return "unknown"

    async def add(self, event: dict):
        """Добавить событие в очередь. Классифицирует и маршрутизирует."""
        rtype = self.classify(event)
        self.stats["received"] += 1

        if rtype == "heartbeat":
            self._bypass_heartbeat(event)
            self.stats["heartbeat_bypassed"] += 1
            return

        if rtype == "dao":
            await self._send_immediate(event)
            self.stats["dao_immediate"] += 1
            return

        # Всё остальное — в batch
        self.batches[rtype].append(event)
        self.stats["batched"] += 1

    def _bypass_heartbeat(self, event: dict):
        """Heartbeat пишем в файл, не в relay."""
        try:
            content = _json.loads(event.get("content", "{}"))
            line = _json.dumps({
                "ts": time.time(),
                "pubkey": event.get("pubkey", "?"),
                "kind": event.get("kind"),
                "from": content.get("from", "?"),
                "counter": content.get("counter", 0),
                "uptime": content.get("uptime", 0),
                "id": event.get("id", "")[:16],
            }) + "\n"
            with open(HEARTBEAT_LOG, "a") as f:
                f.write(line)
        except Exception as e:
            self.stats["errors"] += 1

    async def _send_immediate(self, event: dict):
        """DAO — сразу в relay-mesh (async HTTP)."""
        try:
            http = await self._get_http()
            r = await http.post(
                f"{RELAY_MESH}/api/ingest",
                json=event,
            )
            if r.status_code != 200:
                self.stats["errors"] += 1
        except Exception:
            self.stats["errors"] += 1

    async def _flush_batch(self):
        """Отправить все накопленные batch — в SmartRouter через TCP."""
        now = time.time()
        total_events = sum(len(v) for v in self.batches.values())
        if total_events == 0:
            return

        # Собираем все события в один массив
        all_events = []
        for rtype, events in list(self.batches.items()):
            if events:
                all_events.extend(events)
                self.batches[rtype] = []

        if not all_events:
            return

        # Отправляем в SmartRouter через TCP (потоковый push)
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection(SMART_ROUTER_HOST, SMART_ROUTER_PORT), timeout=3
            )
            # Шлём батч как одно сообщение с kind pipeline_feed
            msg = orjson.dumps({
                "kind": "pipeline_feed",
                "from": "route_engine",
                "pubkey": "route_engine",
                "payload": {"events": all_events, "count": len(all_events)},
                "meta": {"channel": "mesh", "priority": "high", "pipeline": True}
            }) + b"\n"
            w.write(msg)
            await asyncio.wait_for(w.drain(), timeout=3)
            w.close()
            self.stats["flushes"] += 1
            self.stats["ws_flushes"] += 1
            self.last_flush = now
        except Exception as e:
            self.stats["errors"] += 1
            print(f"[RouteEngine] ⚠️ SR send error: {e}")
            # Возвращаем события в batch для повторной отправки
            for ev in all_events:
                self.batches["recovery"].append(ev)

        self.last_flush = now

    async def tick(self):
        """Тик — проверка и flush batch раз в BATCH_WINDOW."""
        while True:
            await asyncio.sleep(BATCH_WINDOW)
            await self._flush_batch()

    def stats_report(self) -> dict:
        return dict(self.stats)


class RouteEngineServer:
    """TCP сервер, принимающий Nostr-события построчно."""

    def __init__(self, engine: RouteEngine):
        self.engine = engine

    async def handle_client(self, reader, writer):
        while True:
            try:
                line = await reader.readline()
                if not line:
                    break
                line = line.decode().strip()
                if not line:
                    continue
                event = _json.loads(line)
                await self.engine.add(event)
            except (_json.JSONDecodeError, ConnectionResetError, BrokenPipeError):
                self.engine.stats["errors"] += 1
                break
            except Exception:
                self.engine.stats["errors"] += 1
                break
        writer.close()

    async def run(self):
        # Phase 3: Unix socket (для CR)
        os.makedirs(UNIX_SOCK_DIR, exist_ok=True)
        try:
            os.unlink(UNIX_SOCK_PATH)
        except FileNotFoundError:
            pass
        unix_server = await asyncio.start_unix_server(
            self.handle_client, UNIX_SOCK_PATH)
        print(f"[RouteEngine] Unix socket {UNIX_SOCK_PATH}")

        server = await asyncio.start_server(
            self.handle_client,
            LISTEN_HOST,
            LISTEN_PORT,
        )
        addr = server.sockets[0].getsockname()
        print(f"[RouteEngine] TCP {addr[0]}:{addr[1]}")
        print(f"[RouteEngine] Relay: {RELAY_MESH}")
        print(f"[RouteEngine] Heartbeat log: {HEARTBEAT_LOG}")
        print(f"[RouteEngine] Batch window: {BATCH_WINDOW}s")

        async with server, unix_server:
            await asyncio.gather(
                server.serve_forever(),
                unix_server.serve_forever(),
            )



async def main():
    engine = RouteEngine()
    server = RouteEngineServer(engine)

    # Печатаем статистику каждые 10 сек
    async def print_stats():
        while True:
            await asyncio.sleep(10)
            s = engine.stats_report()
            print(f"[RouteEngine] Stats: recv={s['received']} hb_bypass={s['heartbeat_bypassed']} "
                  f"dao={s['dao_immediate']} batch={s['batched']} flush={s['flushes']} "
                  f"ws={s['ws_flushes']} err={s['errors']}")
            # Сброс счётчиков
            for k in s:
                engine.stats[k] = 0

    await asyncio.gather(
        server.run(),
        engine.tick(),
        print_stats(),
    )


if __name__ == "__main__":
    asyncio.run(main())

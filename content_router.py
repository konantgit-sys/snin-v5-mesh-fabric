#!/usr/bin/env python3
"""Content Router — активный стрим статуса агентов 1 раз в секунду.

Отличие от heartbeat:
  - heartbeat: "я жив" (1/30 сек) → bypass → Redis
  - content: "вот мои данные" (1/1 сек) → gossip + bridge → Route Engine

Kind: 39002 (content stream)
Content: {
  "type": "content",
  "from": "cryter",
  "ts": 1234567890.0,
  "seq": 42,
  "payload": {
    "state": "idle|busy|thinking",
    "n_peers": 3,
    "buffer_size": 5,
    "pending_tasks": ["analyze", "post", "vote"],
    "last_decisions": {"vote_for": 12, "vote_against": 3},
    "sentiment": 0.42,
    "uptime": 3600,
  }
}

Запуск:
  python3 content_router.py --port 9920

Принимает content от gossip шардов, анализирует:
  - dedup по (from + seq)
  - агрегация: сколько уникальных content/сек
  - форвард в Route Engine если изменения > порога
"""

import asyncio
import json
import time
import os
import sys
import argparse
from collections import defaultdict, deque

ROUTE_ENGINE_HOST = "127.0.0.1"
ROUTE_ENGINE_PORT = 9910
DEDUP_WINDOW = 5  # сек — окно дедупликации
CHANGE_THRESHOLD = 0.15  # 15% изменение состояния = форвард


class ContentRouter:
    """Принимает content stream, дедуплицирует, агрегирует, форвардит изменения."""

    def __init__(self, port: int):
        self.port = port
        self.route_writer = None
        # Дедупликация: {agent_id: {seq: ts}}
        self.seen: dict[str, dict[int, float]] = defaultdict(dict)
        # Последнее известное состояние каждого агента
        self.states: dict[str, dict] = {}
        # Статистика
        self.stats = {
            "received": 0,
            "deduped": 0,
            "forwarded": 0,
            "changes": 0,
            "errors": 0,
        }
        # Агенты и их throughput
        self.agents: dict[str, float] = {}  # agent_id -> last_seen
        # Периодический drain (счётчик)
        self._drain_counter = 0

    async def connect_route_engine(self):
        """Подключение к Route Engine."""
        for attempt in range(5):
            try:
                reader, writer = await asyncio.open_connection(
                    ROUTE_ENGINE_HOST, ROUTE_ENGINE_PORT
                )
                self.route_writer = writer
                print(f"[ContentRouter] Connected to Route Engine")
                return
            except (ConnectionRefusedError, OSError):
                await asyncio.sleep(1)
        print(f"[ContentRouter] Route Engine not available")

    def _has_real_change(self, agent_id: str, new: dict) -> bool:
        """Проверить, изменилось ли состояние значимо."""
        old = self.states.get(agent_id)
        if old is None:
            return True

        old_state = old.get("state", "")
        new_state = new.get("state", "")
        if old_state != new_state:
            return True

        old_tasks = set(old.get("pending_tasks", []))
        new_tasks = set(new.get("pending_tasks", []))
        if old_tasks != new_tasks:
            return True

        old_buffer = old.get("buffer_size", 0)
        new_buffer = new.get("buffer_size", 0)
        if abs(new_buffer - old_buffer) > max(1, old_buffer * CHANGE_THRESHOLD):
            return True

        old_sentiment = old.get("sentiment", 0.0)
        new_sentiment = new.get("sentiment", 0.0)
        if abs(new_sentiment - old_sentiment) > CHANGE_THRESHOLD:
            return True

        return False

    async def process(self, event: dict):
        """Обработать content stream событие."""
        self.stats["received"] += 1

        content = event.get("content", "{}")
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except (json.JSONDecodeError, TypeError):
                content = {}

        agent_id = content.get("from", "?")
        seq = content.get("seq", 0)
        payload = content.get("payload", {})

        # Дедупликация
        seen_seqs = self.seen[agent_id]
        if seq in seen_seqs:
            self.stats["deduped"] += 1
            return

        # Очистка старых seq (старше DEDUP_WINDOW)
        now = time.time()
        old_seqs = [s for s, t in seen_seqs.items() if now - t > DEDUP_WINDOW]
        for s in old_seqs:
            del seen_seqs[s]

        seen_seqs[seq] = now
        self.agents[agent_id] = now

        # Проверка изменений
        if self._has_real_change(agent_id, payload):
            self.stats["changes"] += 1
            self.states[agent_id] = payload

            # Форвард в Route Engine
            await self._forward(event)

    async def _forward(self, event: dict):
        """Форвард в Route Engine (async TCP, drain раз в 20)."""
        if self.route_writer and not self.route_writer.is_closing():
            try:
                self.route_writer.write((json.dumps(event) + "\n").encode())
                self._drain_counter += 1
                self.stats["forwarded"] += 1
                # drain раз в 20 событий — не блокировать поток
                if self._drain_counter >= 20:
                    await self.route_writer.drain()
                    self._drain_counter = 0
            except Exception:
                self.stats["errors"] += 1
                self.route_writer = None
                asyncio.create_task(self.connect_route_engine())

    async def handle_event(self, reader, writer):
        """TCP handler — принимает events от gossip шардов."""
        while True:
            try:
                line = await reader.readline()
                if not line:
                    break
                line = line.decode().strip()
                if not line:
                    continue
                event = json.loads(line)
                await self.process(event)
            except (json.JSONDecodeError, ConnectionResetError, BrokenPipeError):
                break
            except Exception:
                self.stats["errors"] += 1
                break
        writer.close()

    async def clean_stale(self):
        """Очистка мёртвых агентов (не было content > 30 сек)."""
        while True:
            await asyncio.sleep(10)
            now = time.time()
            stale = [aid for aid, last in self.agents.items() if now - last > 30]
            for aid in stale:
                del self.agents[aid]
                if aid in self.seen:
                    del self.seen[aid]
                if aid in self.states:
                    del self.states[aid]

    async def print_stats(self):
        """Статистика каждые 10 сек."""
        while True:
            await asyncio.sleep(10)
            s = self.stats
            print(f"[ContentRouter] Agents:{len(self.agents)} "
                  f"recv:{s['received']} dedup:{s['deduped']} "
                  f"chg:{s['changes']} fwd:{s['forwarded']} err:{s['errors']}")
            # Сброс счётчиков
            for k in s:
                self.stats[k] = 0

    async def run(self):
        await self.connect_route_engine()

        server = await asyncio.start_server(
            self.handle_event,
            "127.0.0.1",
            self.port,
        )
        print(f"[ContentRouter] Listening on TCP {self.port}")
        print(f"[ContentRouter] Dedup window: {DEDUP_WINDOW}s")
        print(f"[ContentRouter] Change threshold: {CHANGE_THRESHOLD*100}%")

        async with server:
            await asyncio.gather(
                server.serve_forever(),
                self.clean_stale(),
                self.print_stats(),
            )


def main():
    parser = argparse.ArgumentParser(description="Content Router")
    parser.add_argument("--port", type=int, default=9920, help="TCP port")
    args = parser.parse_args()
    router = ContentRouter(args.port)
    asyncio.run(router.run())


if __name__ == "__main__":
    main()

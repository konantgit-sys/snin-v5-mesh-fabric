#!/usr/bin/env python3
"""Тест Content Router: 500 агентов, content 1/сек, замер throughput.

Каждый агент:
  - content stream (kind:39002, type:"content") раз в 1 сек
  - состояние меняется случайно раз в 30 сек (триггерит change→forward)

Запуск:
  python3 content_bench.py --count 500 --rate 1
"""

import asyncio
import json
import time
import random
import sys
import os

CONTENT_ROUTER_HOST = "127.0.0.1"
CONTENT_ROUTER_PORT = 9920
RUN_TIME = 60  # сек — длительность теста


async def agent(name: str, shard_writer, stats: dict):
    """Один агент — content stream 1/сек."""
    seq = 0
    start_ts = time.time()
    state = "idle"
    buffer_size = random.randint(0, 10)
    tasks = random.sample(["analyze", "post", "vote", "train", "sync"], k=random.randint(1, 3))

    try:
        while True:
            now = time.time()
            uptime = now - start_ts

            # Случайная смена состояния раз в 30 сек
            if seq > 0 and seq % 30 == 0:
                state = random.choice(["idle", "busy", "thinking"])
                buffer_size = random.randint(0, 20)
                tasks = random.sample(["analyze", "post", "vote", "train", "sync"], k=random.randint(1, 4))

            content = {
                "type": "content",
                "from": name,
                "ts": now,
                "seq": seq,
                "payload": {
                    "state": state,
                    "n_peers": 3,
                    "buffer_size": buffer_size,
                    "pending_tasks": tasks,
                    "sentiment": round(random.uniform(-1, 1), 2),
                    "uptime": round(uptime, 1),
                },
            }

            event = {
                "id": f"{name}_c{seq}_{int(now*1e6)}",
                "kind": 39002,
                "pubkey": f"vp_{name}",
                "content": json.dumps(content),
                "created_at": int(now),
                "sig": f"csig_{name}_{seq}",
            }

            try:
                shard_writer.write((json.dumps(event) + "\n").encode())
                await shard_writer.drain()
                stats["sent_total"] += 1
                if seq % 30 == 0:
                    stats["changes"] += 1
            except Exception:
                stats["errors"] += 1

            seq += 1
            await asyncio.sleep(1)

    except asyncio.CancelledError:
        pass


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rate", type=int, default=1)
    args = parser.parse_args()

    n_agents = args.count
    target_rate = args.rate  # msg/sec per agent

    print(f"[ContentBench] {n_agents} agents, {target_rate} content/sec each")
    print(f"[ContentBench] Total throughput: {n_agents * target_rate} msg/sec")
    print(f"[ContentBench] Target: {CONTENT_ROUTER_HOST}:{CONTENT_ROUTER_PORT}")
    print(f"[ContentBench] Duration: {RUN_TIME}s")
    print()

    # Подключение к Content Router
    try:
        reader, writer = await asyncio.open_connection(
            CONTENT_ROUTER_HOST, CONTENT_ROUTER_PORT
        )
        print("[ContentBench] Connected to Content Router")
    except ConnectionRefusedError:
        print("[ContentBench] ❌ Content Router not available")
        return

    stats = {"sent_total": 0, "changes": 0, "errors": 0}

    # Запуск агентов (все через одно TCP соединение)
    agents_tasks = []
    for i in range(n_agents):
        name = f"vag_{i:04d}"
        task = asyncio.create_task(agent(name, writer, stats))
        agents_tasks.append(task)

    # Статистика каждые 5 сек
    try:
        for tick in range(RUN_TIME // 5):
            await asyncio.sleep(5)
            elapsed = (tick + 1) * 5
            rate = stats["sent_total"] / max(elapsed, 1)
            print(f"[{elapsed}s] sent={stats['sent_total']} "
                  f"rate={rate:.0f}/sec "
                  f"changes={stats['changes']} "
                  f"err={stats['errors']}")
    except KeyboardInterrupt:
        print("\n[ContentBench] Interrupted")
    finally:
        # Стоп
        for t in agents_tasks:
            t.cancel()
        await asyncio.gather(*agents_tasks, return_exceptions=True)
        writer.close()

        total = stats["sent_total"]
        elapsed = RUN_TIME
        print(f"\n[ContentBench] === Results ===")
        print(f"[ContentBench] Agents: {n_agents}")
        print(f"[ContentBench] Total sent: {total}")
        print(f"[ContentBench] Avg rate: {total/elapsed:.0f} msg/sec")
        print(f"[ContentBench] Changes: {stats['changes']}")
        print(f"[ContentBench] Errors: {stats['errors']}")


if __name__ == "__main__":
    asyncio.run(main())

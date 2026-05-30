#!/usr/bin/env python3
"""virtual_agents.py — имитация N агентов для тестирования gossip шарда.

Каждый виртуальный агент:
  - подключается к gossip шарду через TCP
  - шлёт heartbeat раз в 30 сек
  - шлёт DHT signal раз в 5 мин
  - шлёт mesh data раз в 2.5 ч
  - принимает gossip от шарда (печатает статистику)

Запуск:
    python3 virtual_agents.py --shard localhost:9100 --count 25
"""

import asyncio
import json
import time
import random
import argparse
import string


class VirtualAgent:
    """Один виртуальный агент."""

    def __init__(self, agent_id: int, shard_host: str, shard_port: int):
        self.agent_id = agent_id
        self.name = f"vagent_{agent_id:04d}"
        self.shard_host = shard_host
        self.shard_port = shard_port
        self.pubkey = f"vp_{agent_id:04d}_{''.join(random.choices(string.hexdigits, k=32))}"[:64]
        self.reader = None
        self.writer = None
        self.connected = False
        self.stats = {"sent": 0, "recv": 0, "hb": 0, "dht": 0, "dao": 0}
        self.counter = 0
        self.start_ts = time.time()

    async def connect(self):
        for attempt in range(5):
            try:
                self.reader, self.writer = await asyncio.open_connection(
                    self.shard_host, self.shard_port
                )
                self.connected = True
                print(f"[{self.name}] Connected to shard")
                return True
            except (ConnectionRefusedError, OSError) as e:
                await asyncio.sleep(1)
        print(f"[{self.name}] FAILED to connect")
        return False

    async def send(self, kind: int, msg_type: str, data: dict = None):
        """Отправить сообщение в шард."""
        if not self.connected or not self.writer or self.writer.is_closing():
            return

        content = {
            "type": msg_type,
            "from": self.name,
            "peer_id": f"vagent_{self.agent_id}",
            "ts": time.time(),
            "counter": self.counter,
            "uptime": round(time.time() - self.start_ts, 1),
        }
        if data:
            content.update(data)

        event = {
            "id": f"{self.name}_{self.counter}_{int(time.time()*1000000)}",
            "kind": kind,
            "pubkey": self.pubkey,
            "content": json.dumps(content),
            "created_at": int(time.time()),
            "sig": f"vsig_{self.name}_{self.counter}",
        }

        try:
            self.writer.write((json.dumps(event) + "\n").encode())
            await self.writer.drain()
            self.stats["sent"] += 1
            self.counter += 1
            return True
        except Exception:
            self.connected = False
            return False

    async def heartbeat(self):
        """Heartbeat раз в 30 сек."""
        ok = await self.send(39000, "heartbeat")
        if ok:
            self.stats["hb"] += 1

    async def dht_signal(self):
        """DHT signal раз в 5 мин."""
        ok = await self.send(39001, "dht", {"op": "put", "key": self.name, "value": {"port": self.shard_port}})
        if ok:
            self.stats["dht"] += 1

    async def dao_vote(self):
        """DAO vote раз в 24 ч (для теста — раз в 60 сек)."""
        ok = await self.send(39011, "dao_vote", {
            "proposal_id": f"prop_{int(time.time())}",
            "vote": random.choice(["for", "against", "abstain"]),
            "weight": 100,
        })
        if ok:
            self.stats["dao"] += 1

    async def listen(self):
        """Слушать входящие gossip сообщения."""
        while self.connected and self.writer and not self.writer.is_closing():
            try:
                line = await asyncio.wait_for(self.reader.readline(), timeout=5)
                if line:
                    self.stats["recv"] += 1
            except asyncio.TimeoutError:
                continue
            except (ConnectionResetError, BrokenPipeError, Exception):
                break
        self.connected = False

    async def run(self):
        if not await self.connect():
            return

        # Запускаем слушатель
        listen_task = asyncio.create_task(self.listen())

        # Hello при подключении
        await self.send(39000, "hello", {"pubkey": self.pubkey})

        # Первый heartbeat сразу
        await self.heartbeat()

        # Регулярные задачи
        try:
            cycle = 0
            while True:
                await asyncio.sleep(30)
                cycle += 1

                # Heartbeat каждые 30 сек
                await self.heartbeat()

                # DHT каждые 5 мин
                if cycle % 10 == 0:
                    await self.dht_signal()

                # DAO каждые 2 мин (для теста)
                if cycle % 4 == 0:
                    await self.dao_vote()

        except asyncio.CancelledError:
            pass
        finally:
            listen_task.cancel()
            if self.writer and not self.writer.is_closing():
                self.writer.close()


async def main():
    parser = argparse.ArgumentParser(description="Virtual Agents")
    parser.add_argument("--shard", default="localhost:9100", help="Shard address host:port")
    parser.add_argument("--count", type=int, default=25, help="Number of virtual agents")
    args = parser.parse_args()

    host, port_str = args.shard.split(":")
    port = int(port_str)

    print(f"[VirtualAgents] Creating {args.count} agents → {args.shard}")
    print(f"[VirtualAgents] Each: heartbeat/30s, DHT/5min, DAO/2min")

    agents = []
    for i in range(args.count):
        agent = VirtualAgent(i, host, port)
        agents.append(agent)

    # Запуск всех агентов
    tasks = [asyncio.create_task(agent.run()) for agent in agents]
    print(f"[VirtualAgents] {args.count} agents launched")

    # Статистика каждые 10 сек
    try:
        while True:
            await asyncio.sleep(10)
            total_sent = sum(a.stats["sent"] for a in agents)
            total_recv = sum(a.stats["recv"] for a in agents)
            total_hb = sum(a.stats["hb"] for a in agents)
            connected = sum(1 for a in agents if a.connected)
            print(f"[VirtualAgents] Connected:{connected}/{args.count} "
                  f"sent:{total_sent} recv:{total_recv} hb:{total_hb}")
    except KeyboardInterrupt:
        print("\nShutting down...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())

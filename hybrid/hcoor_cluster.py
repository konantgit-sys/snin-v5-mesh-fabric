#!/usr/bin/env python3
"""
HCOOR Cluster Daemon — запускает 3 P2P агентов, подключает к координатору.
Каждый агент с реальным IP:PORT и P2P каналом — заменяет 6 мёртвых модулей.
"""
import asyncio, json, os, sys, signal
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from p2p_agent import P2PAgent

AGENTS = [
    {
        "pubkey": "f9c3a1b2d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0",
        "name": "Cryter",
        "capabilities": ["publish", "analyze", "engage", "forecast"],
    },
    {
        "pubkey": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1",
        "name": "Forecaster",
        "capabilities": ["forecast", "analyze", "predict"],
    },
    {
        "pubkey": "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
        "name": "Archivist",
        "capabilities": ["archive", "search", "attest", "store"],
    },
]


class HcoorCluster:
    def __init__(self):
        self.agents: list[P2PAgent] = []
        self._stop = asyncio.Event()

    async def start(self):
        print(f"🚀 HCOOR Cluster — {len(AGENTS)} agents")
        for cfg in AGENTS:
            agent = P2PAgent(cfg["pubkey"], cfg["name"], cfg["capabilities"])
            await agent.start()
            self.agents.append(agent)

        print(f"\n✅ All {len(self.agents)} agents online with P2P channels")

        # Keep alive: ping + refresh peers
        while not self._stop.is_set():
            try:
                await asyncio.sleep(25)
                for a in self.agents:
                    if a._hcoor_writer and not a._hcoor_writer.is_closing():
                        a._hcoor_writer.write(
                            json.dumps({"type": "ping", "pubkey": a.pubkey}).encode() + b"\n"
                        )
                        await a._hcoor_writer.drain()
                        # Read pong
                        await asyncio.wait_for(a._hcoor_reader.readline(), 3)
            except Exception:
                pass

    async def stop(self):
        self._stop.set()
        for a in self.agents:
            await a.stop()
        print("⏹️ Cluster stopped")


async def main():
    cluster = HcoorCluster()
    loop = asyncio.get_running_loop()

    def shutdown():
        asyncio.ensure_future(cluster.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)

    await cluster.start()


if __name__ == "__main__":
    asyncio.run(main())

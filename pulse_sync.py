#!/usr/bin/env python3
"""
SNIN Pulse Sync Module v2.4

Фаза 2.4: NTP-синхронизация + Heartbeat между сервисами

Логика:
  - Heartbeat сервер (:9930) отправляет pulse каждые 5 сек
  - Все сервисы синхронизируют время через NTP
  - Детектирует несинхронизированные узлы (clock skew >1 сек)
  - Используется для distributed tracing, load balancing, consensus

Pulse формат: {ts, ntp_offset, healthy_nodes: []}
"""

import asyncio
import json
import logging
import ntplib
import os
import socket
import time
from datetime import datetime
from typing import Dict, List, Optional

LOG_DIR = "/home/agent/data/logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [PULSE] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "pulse_sync.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("PulseSync")

PULSE_STATUS_FILE = "/home/agent/data/sites/relay-mesh/pulse_status.json"
NTP_SERVERS = ["pool.ntp.org", "time.nist.gov", "time.google.com"]

class PulseNode:
    """Один узел в сети, отправляющий pulse"""
    def __init__(self, name: str, port: int):
        self.name = name
        self.port = port
        self.last_heartbeat = None
        self.ntp_offset = 0.0  # Разница между local time и NTP time
        self.is_healthy = True
        self.latency_ms = 0.0

    def to_dict(self):
        return {
            "name": self.name,
            "port": self.port,
            "is_healthy": self.is_healthy,
            "ntp_offset": round(self.ntp_offset, 3),
            "latency_ms": round(self.latency_ms, 2),
            "last_heartbeat": self.last_heartbeat
        }

class PulseSyncManager:
    def __init__(self):
        self.nodes: Dict[str, PulseNode] = {}
        self.local_ntp_offset = 0.0
        self.pulse_sequence = 0
        self.setup_nodes()

    def setup_nodes(self):
        """Инициализирует узлы"""
        nodes_config = [
            ("smart_router", 9932),
            ("route_engine", 9910),
            ("content_router", 9920),
            ("nostr_bridge_0", 9941),
            ("nostr_bridge_1", 9942),
            ("nostr_bridge_2", 9943),
            ("nostr_bridge_3", 9944),
            ("external_gateway", 9931),
            ("identity_api", 9940),
        ]
        for name, port in nodes_config:
            self.nodes[name] = PulseNode(name, port)

    def sync_ntp(self) -> float:
        """Синхронизируется с NTP, возвращает offset в секундах"""
        for server in NTP_SERVERS:
            try:
                client = ntplib.NTPClient()
                response = client.request(server, version=3, timeout=2)
                offset = response.offset
                logger.info(f"✓ NTP sync: {server} → offset {offset:.3f}s")
                return offset
            except Exception as e:
                logger.warning(f"✗ NTP {server} failed: {e}")
                continue

        logger.error("Could not sync with any NTP server")
        return 0.0

    async def broadcast_heartbeat(self):
        """Отправляет heartbeat (pulse) всем узлам"""
        self.pulse_sequence += 1
        now = time.time()

        pulse_data = {
            "sequence": self.pulse_sequence,
            "timestamp": now,
            "ntp_offset": self.local_ntp_offset,
            "healthy_nodes": self.get_healthy_nodes(),
        }

        # Отправляем heartbeat всем узлам параллельно
        tasks = []
        for name, node in self.nodes.items():
            task = self._send_heartbeat_to_node(node, pulse_data)
            tasks.append(task)

        await asyncio.gather(*tasks)
        logger.info(f"📡 Pulse #{self.pulse_sequence} broadcasted")

    async def _send_heartbeat_to_node(self, node: PulseNode, pulse: Dict):
        """Отправляет heartbeat одному узлу"""
        start = time.time()
        url = f"http://127.0.0.1:{node.port}/api/pulse"

        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=pulse, timeout=aiohttp.ClientTimeout(total=1)) as resp:
                    elapsed = (time.time() - start) * 1000
                    node.latency_ms = elapsed

                    if resp.status == 200:
                        node.is_healthy = True
                        node.last_heartbeat = datetime.utcnow().isoformat()
                    else:
                        node.is_healthy = False
                        logger.warning(f"Pulse to {node.name} returned HTTP {resp.status}")
        except asyncio.TimeoutError:
            node.is_healthy = False
            logger.warning(f"Pulse to {node.name} timeout")
        except Exception as e:
            node.is_healthy = False
            logger.warning(f"Pulse to {node.name} failed: {e}")

    def get_healthy_nodes(self) -> List[str]:
        """Список живых узлов"""
        return [name for name, node in self.nodes.items() if node.is_healthy]

    def detect_clock_skew(self) -> Dict[str, float]:
        """Детектирует узлы с большой разницей в часах"""
        skewed = {}
        for name, node in self.nodes.items():
            if abs(node.ntp_offset) > 1.0:  # Больше 1 секунды — проблема
                skewed[name] = node.ntp_offset
                logger.warning(f"⏰ {name} clock skew: {node.ntp_offset:.3f}s")
        return skewed

    def get_status(self):
        """Возвращает статус пульса"""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "pulse_sequence": self.pulse_sequence,
            "local_ntp_offset": round(self.local_ntp_offset, 3),
            "healthy_nodes": self.get_healthy_nodes(),
            "clock_skew": self.detect_clock_skew(),
            "nodes": {name: node.to_dict() for name, node in self.nodes.items()},
        }

    def save_status(self):
        """Сохраняет статус"""
        with open(PULSE_STATUS_FILE, 'w') as f:
            json.dump(self.get_status(), f, indent=2)

    async def run_pulse_loop(self):
        """Главный loop: синхронизируемся и отправляем pulse"""
        logger.info("🚀 Pulse Sync Manager started")

        # Начальная синхронизация с NTP
        self.local_ntp_offset = self.sync_ntp()

        while True:
            try:
                # Отправляем heartbeat
                await self.broadcast_heartbeat()

                # Сохраняем статус
                self.save_status()

                # Re-sync с NTP каждые 5 минут
                if self.pulse_sequence % 60 == 0:
                    self.local_ntp_offset = self.sync_ntp()

                # Ждём 5 секунд перед следующим pulse
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Pulse loop error: {e}")
                await asyncio.sleep(5)

async def start_pulse_http_server():
    """Запускает HTTP сервер для /api/pulse/status"""
    from aiohttp import web

    manager = PulseSyncManager()

    async def pulse_status(request):
        return web.json_response(manager.get_status())

    async def pulse_health(request):
        """Endpoint для получения heartbeat от других сервисов"""
        pulse_data = await request.json()
        logger.info(f"Received pulse #{pulse_data['sequence']}")
        return web.json_response({"ok": True})

    app = web.Application()
    app.router.add_get("/api/pulse/status", pulse_status)
    app.router.add_post("/api/pulse", pulse_health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 9930)
    await site.start()

    logger.info("Pulse HTTP server listening on :9930")

    # Запускаем pulse loop в фоне
    asyncio.create_task(manager.run_pulse_loop())

    # Держим сервер живым
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(start_pulse_http_server())

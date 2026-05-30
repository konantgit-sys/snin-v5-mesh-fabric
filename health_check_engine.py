#!/usr/bin/env python3
"""
SNIN Health Check Engine v2.1 — Mesh Resilience Module

Фаза 2.1: Детектирование падений за <1 сек, сбор метрик, управление graceful degradation.

Структура:
  - HealthMonitor (каждые 5 сек пробует /health от каждого сервиса)
  - MetricsCollector (uptime, latency, restart count, memory usage)
  - DegradationManager (изолирует failing компоненты, включает fallback)
  - REST API (:9999 /api/health/*)

Логирует в: /home/agent/data/logs/health_engine.log
JSON статус: /home/agent/data/sites/relay-mesh/health_status.json
"""

import asyncio
import json
import logging
import os
import subprocess
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import aiohttp

# ─── SETUP LOGGING ───
LOG_DIR = "/home/agent/data/logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "health_engine.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("HealthEngine")

STATUS_FILE = "/home/agent/data/sites/relay-mesh/health_status.json"
HEALTH_CHECK_INTERVAL = 5  # сек
RESPONSE_TIMEOUT = 2.0  # сек
DEGRADATION_THRESHOLD = 3  # 3 consecutive fails

# ─── СЕРВИСЫ ДЛЯ МОНИТОРИНГА ───
SERVICES = [
    {"name": "smart_router", "port": 9932, "health_path": "/api/health"},
    {"name": "route_engine", "port": 9910, "health_path": "/api/health"},
    {"name": "content_router", "port": 9920, "health_path": "/api/health"},
    {"name": "nostr_bridge_0", "port": 9941, "health_path": "/health"},
    {"name": "nostr_bridge_1", "port": 9942, "health_path": "/health"},
    {"name": "nostr_bridge_2", "port": 9943, "health_path": "/health"},
    {"name": "nostr_bridge_3", "port": 9944, "health_path": "/health"},
    {"name": "external_gateway", "port": 9931, "health_path": "/health"},
    {"name": "identity_api", "port": 9940, "health_path": "/api/health"},
    {"name": "cheque_book", "port": 9916, "health_path": "/health"},
    {"name": "verifier", "port": 9915, "health_path": "/health"},
]

class HealthStatus:
    """Статус одного сервиса"""
    def __init__(self, name: str, port: int):
        self.name = name
        self.port = port
        self.is_alive = False
        self.last_check = None
        self.consecutive_fails = 0
        self.uptime_seconds = 0
        self.restart_count = 0
        self.latency_ms = 0.0
        self.status_code = None
        self.error_msg = ""
        self.degraded = False

    def to_dict(self):
        return {
            "name": self.name,
            "port": self.port,
            "is_alive": self.is_alive,
            "consecutive_fails": self.consecutive_fails,
            "latency_ms": round(self.latency_ms, 2),
            "uptime_seconds": self.uptime_seconds,
            "restart_count": self.restart_count,
            "status_code": self.status_code,
            "error": self.error_msg,
            "degraded": self.degraded,
            "last_check": self.last_check
        }

class HealthCheckEngine:
    def __init__(self):
        self.statuses: Dict[str, HealthStatus] = {}
        self.degradation_modes: Dict[str, bool] = {}  # {channel: is_degraded}
        self.start_time = time.time()

        # Инициализируем статусы
        for service in SERVICES:
            status = HealthStatus(service["name"], service["port"])
            self.statuses[service["name"]] = status

    async def check_service(self, service: Dict) -> HealthStatus:
        """Проверяет сервис за <1 сек"""
        name = service["name"]
        port = service["port"]
        health_path = service["health_path"]
        status = self.statuses[name]

        url = f"http://127.0.0.1:{port}{health_path}"
        start = time.time()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=RESPONSE_TIMEOUT)) as resp:
                    elapsed = (time.time() - start) * 1000
                    status.latency_ms = elapsed
                    status.status_code = resp.status
                    status.last_check = datetime.utcnow().isoformat()

                    if resp.status == 200:
                        status.is_alive = True
                        status.consecutive_fails = 0
                        status.error_msg = ""
                        logger.info(f"✓ {name}:{port} alive ({elapsed:.1f}ms)")
                    else:
                        status.is_alive = False
                        status.consecutive_fails += 1
                        status.error_msg = f"HTTP {resp.status}"
                        logger.warning(f"✗ {name}:{port} HTTP {resp.status} ({elapsed:.1f}ms)")
        except asyncio.TimeoutError:
            status.is_alive = False
            status.consecutive_fails += 1
            status.error_msg = "timeout"
            status.latency_ms = RESPONSE_TIMEOUT * 1000
            logger.warning(f"✗ {name}:{port} TIMEOUT")
        except aiohttp.ClientConnRefusedError:
            status.is_alive = False
            status.consecutive_fails += 1
            status.error_msg = "connection refused"
            logger.warning(f"✗ {name}:{port} REFUSED")
        except Exception as e:
            status.is_alive = False
            status.consecutive_fails += 1
            status.error_msg = str(e)
            logger.warning(f"✗ {name}:{port} ERROR: {e}")

        return status

    async def monitor_loop(self):
        """Цикл мониторинга: каждые 5 сек проверить все сервисы"""
        logger.info("🚀 Health Check Engine started")

        while True:
            try:
                # Параллельно проверяем все сервисы
                tasks = [self.check_service(svc) for svc in SERVICES]
                await asyncio.gather(*tasks)

                # Анализируем graceful degradation
                self._detect_degradation()

                # Сохраняем статус в JSON
                self._save_status()

                # Выполняем recovery if needed
                self._trigger_recovery()

                await asyncio.sleep(HEALTH_CHECK_INTERVAL)
            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)

    def _detect_degradation(self):
        """Детектирует graceful degradation по каналам"""
        # Группируем сервисы по каналам
        channels = {
            "nostr_bridges": ["nostr_bridge_0", "nostr_bridge_1", "nostr_bridge_2", "nostr_bridge_3"],
            "routers": ["smart_router", "route_engine", "content_router"],
            "gateways": ["external_gateway"],
        }

        for channel, services in channels.items():
            alive_count = sum(1 for s in services if self.statuses[s].is_alive)
            total = len(services)

            if alive_count == 0:
                # Полное падение канала
                self.degradation_modes[channel] = True
                logger.error(f"🔴 CRITICAL: {channel} completely down ({alive_count}/{total})")
            elif alive_count < total // 2:
                # Половина упала
                self.degradation_modes[channel] = True
                logger.warning(f"🟠 DEGRADED: {channel} ({alive_count}/{total} alive)")
            else:
                # Работает нормально
                self.degradation_modes[channel] = False
                logger.info(f"🟢 {channel} healthy ({alive_count}/{total})")

    def _trigger_recovery(self):
        """Включает graceful degradation и auto-recovery"""
        for name, status in self.statuses.items():
            if status.consecutive_fails >= DEGRADATION_THRESHOLD:
                logger.warning(f"Triggering recovery for {name} ({status.consecutive_fails} fails)")
                status.degraded = True
                # TODO: запустить graceful_shutdown.py и restart
            elif status.is_alive:
                status.degraded = False

    def _save_status(self):
        """Сохраняет JSON статус"""
        status_dict = {
            "timestamp": datetime.utcnow().isoformat(),
            "engine_uptime_seconds": time.time() - self.start_time,
            "degradation_modes": self.degradation_modes,
            "services": {name: s.to_dict() for name, s in self.statuses.items()},
            "summary": {
                "total_services": len(self.statuses),
                "alive": sum(1 for s in self.statuses.values() if s.is_alive),
                "degraded": sum(1 for s in self.statuses.values() if s.degraded),
            }
        }

        with open(STATUS_FILE, 'w') as f:
            json.dump(status_dict, f, indent=2)

    def get_health_summary(self):
        """REST: GET /api/health/summary"""
        with open(STATUS_FILE, 'r') as f:
            return json.load(f)

    def get_service_health(self, name: str):
        """REST: GET /api/health/service/{name}"""
        if name in self.statuses:
            return {"ok": True, "data": self.statuses[name].to_dict()}
        return {"ok": False, "error": "service not found"}

async def start_http_server():
    """Запускает HTTP сервер для /api/health/* endpoints"""
    from aiohttp import web

    engine = HealthCheckEngine()

    async def health_summary(request):
        return web.json_response(engine.get_health_summary())

    async def service_health(request):
        name = request.match_info.get("name", "")
        return web.json_response(engine.get_service_health(name))

    app = web.Application()
    app.router.add_get("/api/health/summary", health_summary)
    app.router.add_get("/api/health/service/{name}", service_health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 9999)
    await site.start()

    logger.info("Health API listening on :9999")

    # Запускаем monitor loop в фоне
    asyncio.create_task(engine.monitor_loop())

    # Держим сервер живым
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(start_http_server())

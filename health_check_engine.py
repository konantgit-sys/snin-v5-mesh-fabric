#!/usr/bin/env python3
"""
SNIN Health Check Engine v3.2 — Mesh Resilience Module
Config-driven: читает список сервисов из mesh_config.yaml.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, List

import aiohttp
from mesh_config import config

# ─── SETUP ───
LOG_DIR = config.get("global.log_dir", "/home/agent/data/logs")
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

STATUS_FILE = config.get("orchestration.health_engine.status_file",
                         "/home/agent/data/sites/relay-mesh/health_status.json")
HEALTH_CHECK_INTERVAL = config.get("orchestration.health_engine.interval", 10)
RESPONSE_TIMEOUT = config.get("orchestration.health_engine.timeout", 2.0)
DEGRADATION_THRESHOLD = config.get("orchestration.health_engine.degradation_threshold", 3)

# ─── СЕРВИСЫ ИЗ КОНФИГА ───
# Авто-генерация списка сервисов для health check
def _build_services() -> List[dict]:
    svcs = []

    # Mesh-сервисы (transport + bridge + mesh_fabric) — через mesh_health на 199xx
    mesh_entries = [
        ("transport.smart_router",      "smart_router"),
        ("transport.route_engine",      "route_engine"),
        ("transport.content_router_v2", "content_router"),
        ("mesh_fabric.external_gateway","external_gateway"),
        ("bridge.cross_mesh",           "cross_mesh_bridge"),
    ]
    for cfg_key, name in mesh_entries:
        port = config.get(f"{cfg_key}.port")
        if port:
            hp = port + config.get("global.health_port_offset", 10000)
            svcs.append({"name": name, "health_port": hp})

    # Nostr bridges (bridge_count штук)
    bridge_base = config.get("nostr.bridge_base_port", 9941)
    bridge_count = config.get("nostr.bridge_count", 5)
    for i in range(bridge_count):
        hp = (bridge_base + i) + config.get("global.health_port_offset", 10000)
        svcs.append({"name": f"nostr_bridge_{i}", "health_port": hp})

    # HTTP-сервисы (identity, verifier, supervisor)
    http_entries = [
        ("identity.identity_api_port", "identity_api", 9940),
        ("identity.verifier.port",     "verifier",     9915),
        ("orchestration.supervisor",   "supervisor",   9900),
    ]
    for cfg_key, name, default_port in http_entries:
        port = config.get(cfg_key) or default_port
        svcs.append({"name": name, "port": port, "path": "/health"})

    # TCP-fallback сервисы
    tcp_entries = [
        ("orchestration.relay_mesh_api", "relay_mesh_api", 9907),
        ("orchestration.relay_v2",       "relay_v2",       9905),
    ]
    for cfg_key, name, default_port in tcp_entries:
        port = config.get(cfg_key) or default_port
        svcs.append({"name": name, "port": port, "type": "tcp"})

    return svcs


SERVICES = _build_services()


class ServiceStatus:
    def __init__(self, svc: dict):
        self.name = svc["name"]
        self.health_port = svc.get("health_port")
        self.port = svc.get("port")
        self.svc_type = svc.get("type", "auto")
        self.path = svc.get("path", "/health")
        self.is_alive = False
        self.last_check = None
        self.consecutive_fails = 0
        self.uptime_seconds = 0
        self.restart_count = 0
        self.latency_ms = 0.0
        self.status_code = None
        self.error_msg = ""
        self.degraded = False
        self._start_time = None

    def mark_alive(self, latency_ms: float):
        self.is_alive = True
        self.consecutive_fails = 0
        self.error_msg = ""
        self.latency_ms = round(latency_ms, 1)
        self.status_code = 200
        self.last_check = datetime.utcnow().isoformat()
        if not self._start_time:
            self._start_time = time.time()

    def mark_dead(self, error: str, latency_ms: float = 0):
        self.is_alive = False
        self.consecutive_fails += 1
        self.error_msg = error
        self.latency_ms = round(latency_ms, 1)
        self.status_code = None
        self.last_check = datetime.utcnow().isoformat()
        if self.consecutive_fails >= DEGRADATION_THRESHOLD:
            self.degraded = True

    def to_dict(self):
        uptime = 0
        if self._start_time and self.is_alive:
            uptime = int(time.time() - self._start_time)
        port = self.health_port or self.port or 0
        return {
            "name": self.name,
            "port": port,
            "is_alive": self.is_alive,
            "consecutive_fails": self.consecutive_fails,
            "latency_ms": self.latency_ms,
            "uptime_seconds": uptime,
            "restart_count": self.restart_count,
            "status_code": self.status_code,
            "error": self.error_msg,
            "degraded": self.degraded,
            "last_check": self.last_check
        }


class HealthCheckEngine:
    def __init__(self):
        self.statuses: Dict[str, ServiceStatus] = {}
        self.degradation_modes: Dict[str, bool] = {}
        self.start_time = time.time()
        for svc in SERVICES:
            self.statuses[svc["name"]] = ServiceStatus(svc)
        logger.info(f"📋 Monitoring {len(SERVICES)} services from mesh_config.yaml")

    async def check_http(self, svc: dict, health_port: int = None) -> ServiceStatus:
        status = self.statuses[svc["name"]]
        port = health_port or svc.get("port")
        path = svc.get("path", "/health")
        if not port:
            status.mark_dead("no port")
            return status
        url = f"http://127.0.0.1:{port}{path}"
        start = time.time()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=RESPONSE_TIMEOUT)) as resp:
                    latency = (time.time() - start) * 1000
                    if resp.status == 200:
                        status.mark_alive(latency)
                    else:
                        status.mark_dead(f"HTTP {resp.status}", latency)
        except asyncio.TimeoutError:
            status.mark_dead("timeout", RESPONSE_TIMEOUT * 1000)
        except aiohttp.ClientConnectionError:
            status.mark_dead("connection refused")
        except Exception as e:
            status.mark_dead(str(e))
        return status

    async def check_tcp(self, svc: dict) -> ServiceStatus:
        status = self.statuses[svc["name"]]
        port = svc.get("port")
        if not port:
            status.mark_dead("no port")
            return status
        start = time.time()
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", port),
                timeout=RESPONSE_TIMEOUT
            )
            latency = (time.time() - start) * 1000
            writer.close()
            await writer.wait_closed()
            status.mark_alive(latency)
        except asyncio.TimeoutError:
            status.mark_dead("timeout", RESPONSE_TIMEOUT * 1000)
        except ConnectionRefusedError:
            status.mark_dead("connection refused")
        except Exception as e:
            status.mark_dead(str(e))
        return status

    async def check_service(self, svc: dict) -> ServiceStatus:
        if svc.get("health_port"):
            return await self.check_http(svc, health_port=svc["health_port"])
        if svc.get("path") and svc.get("port"):
            return await self.check_http(svc)
        return await self.check_tcp(svc)

    async def monitor_loop(self):
        logger.info("🚀 Health Check Engine v3.2 (config-driven) started")
        await asyncio.sleep(3)
        while True:
            try:
                tasks = [self.check_service(svc) for svc in SERVICES]
                await asyncio.gather(*tasks)
                self._detect_degradation()
                self._save_status()
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)
            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)

    def _detect_degradation(self):
        channels = {
            "nostr_bridges": [f"nostr_bridge_{i}" for i in range(config.get("nostr.bridge_count", 5))],
            "routers": ["smart_router", "route_engine", "content_router"],
            "gateways": ["external_gateway", "cross_mesh_bridge"],
            "identity": ["identity_api", "verifier"],
        }
        for channel, services in channels.items():
            alive = sum(1 for s in services if s in self.statuses and self.statuses[s].is_alive)
            total = len([s for s in services if s in self.statuses])
            if alive == 0:
                self.degradation_modes[channel] = True
                logger.error(f"🔴 {channel} completely down ({alive}/{total})")
            elif alive < max(total // 2, 1):
                self.degradation_modes[channel] = True
                logger.warning(f"🟠 {channel} degraded ({alive}/{total} alive)")
            else:
                self.degradation_modes[channel] = False

    def _save_status(self):
        alive = sum(1 for s in self.statuses.values() if s.is_alive)
        degraded = sum(1 for s in self.statuses.values() if s.degraded)
        total = len(self.statuses)
        status_dict = {
            "timestamp": datetime.utcnow().isoformat(),
            "engine_uptime_seconds": int(time.time() - self.start_time),
            "degradation_modes": self.degradation_modes,
            "services": {name: s.to_dict() for name, s in self.statuses.items()},
            "summary": {
                "total_services": total,
                "alive": alive,
                "degraded": degraded,
                "health_pct": round(alive / total * 100, 1) if total else 0,
            }
        }
        with open(STATUS_FILE, 'w') as f:
            json.dump(status_dict, f, indent=2)

    def get_health_summary(self):
        try:
            with open(STATUS_FILE, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"error": "status not yet available"}

    def get_service_health(self, name: str):
        if name in self.statuses:
            return {"ok": True, "data": self.statuses[name].to_dict()}
        return {"ok": False, "error": "service not found"}


async def start_http_server():
    from aiohttp import web
    engine = HealthCheckEngine()

    async def health_summary(request):
        return web.json_response(engine.get_health_summary())

    async def service_health(request):
        name = request.match_info.get("name", "")
        return web.json_response(engine.get_service_health(name))

    async def health_ping(request):
        return web.json_response({
            "status": "ok",
            "engine": "HealthEngine v3.2",
            "config_driven": True,
            "monitored_services": len(SERVICES)
        })

    app = web.Application()
    app.router.add_get("/api/health/summary", health_summary)
    app.router.add_get("/api/health/service/{name}", service_health)
    app.router.add_get("/api/health/ping", health_ping)
    app.router.add_get("/api/status", health_summary)
    app.router.add_get("/status", health_summary)
    app.router.add_get("/health", health_ping)

    # ═══ L5T: Dead-Letter Sync API ═══
    async def dlq_sync(request):
        try:
            from dead_letter import get_dlq
            data = await request.json()
            to_pubkey = data.get("pubkey", "")
            since = data.get("since", 0)
            if not to_pubkey:
                return web.json_response({"ok": False, "error": "pubkey required"}, status=400)
            dlq = get_dlq()
            messages = await dlq.sync(to_pubkey, since)
            return web.json_response({
                "ok": True,
                "count": len(messages),
                "messages": [m.to_dict() for m in messages],
            })
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=500)

    app.router.add_post("/api/v1/deadletter/sync", dlq_sync)

    async def dlq_stats(request):
        try:
            from dead_letter import get_dlq
            dlq = get_dlq()
            return web.json_response({"ok": True, **dlq.stats()})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=500)

    app.router.add_get("/api/v1/deadletter/stats", dlq_stats)

    engine_port = config.get("orchestration.health_engine.port", 9999)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", engine_port)
    await site.start()
    logger.info(f"✅ Health API listening on :{engine_port}")

    asyncio.create_task(engine.monitor_loop())
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(start_http_server())

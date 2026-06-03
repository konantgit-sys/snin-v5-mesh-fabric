#!/usr/bin/env python3
"""
SNIN Health Check Engine v3.5 L15 — Mesh Resilience + WS + Alerts + History + Alert Engine + Auto-Recovery
Config-driven: читает список сервисов из mesh_config.yaml.

L13 фичи:
  - WebSocket stream live-статусов (/api/v1/health/ws)
  - SQLite история проверок (health_history.db)
  - Dashboard API (/api/v1/health/dashboard)
  - Telegram/Nostr алерты (alert_engine.py)
  - Purge старой истории раз в час

L14 фичи:
  - YAML-driven Alert Engine с правилами (alert_config.yaml)
  - Multi-channel dispatch: Telegram, Nostr, Webhook
  - Эскалация с таймерами (уровни 0→1→2→3)
  - /ack сброс эскалации
  - SQLite alert_log + events

L15 фичи:
  - YAML-driven Auto-Recovery (recovery_config.yaml, 5 стратегий)
  - Анализ причины падения (логи, метрики, история)
  - 3-4 уровня попыток: restart → clear_cache → reload_layer → escalate
  - Supervisor bridge (HTTP API + fallback kill+nohup)
  - Rate limits: cooldown, max daily, slot health threshold
  - API: /api/v1/recovery/stats, /api/v1/recovery/events, /api/v1/recovery/analysis, /api/v1/recovery/reset/{svc}
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
from datetime import datetime
from typing import Dict, List

import aiohttp
from mesh_config import config
from health_ws import get_broadcaster
from alert_engine import get_alert_engine
from auto_recovery import get_auto_recovery

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

# ─── L13: SQLite история ───
HEALTH_DB = config.get("orchestration.health_engine.history_db",
                       "/home/agent/data/sites/relay-mesh/logs/health_history.db")
HISTORY_RETENTION = config.get("orchestration.health_engine.history_retention", 86400 * 7)  # 7 дней

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

        # L13: SQLite история
        self._db_conn = sqlite3.connect(HEALTH_DB)
        self._init_db()
        self._last_broadcast_state = {name: s.to_dict() for name, s in self.statuses.items()}

        # L13: WebSocket + Alerts
        self._broadcaster = get_broadcaster()
        self._broadcaster.set_state_getter(self._get_full_state_for_ws)
        self._alert_engine = get_alert_engine()

        # L15: Auto-Recovery
        self._auto_recovery = get_auto_recovery()

    def _init_db(self):
        """Создаёт таблицы если не существуют."""
        self._db_conn.execute("""
            CREATE TABLE IF NOT EXISTS health_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_name TEXT NOT NULL,
                status TEXT NOT NULL,
                checked_at INTEGER NOT NULL,
                latency_ms REAL DEFAULT 0,
                uptime_seconds INTEGER DEFAULT 0
            )
        """)
        self._db_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_health_svc_time 
            ON health_history(service_name, checked_at)
        """)
        self._db_conn.commit()

    def _log_to_db(self, name: str, status: str, latency: float, uptime: int):
        """Пишет одну запись в health_history."""
        try:
            self._db_conn.execute(
                "INSERT INTO health_history (service_name, status, checked_at, latency_ms, uptime_seconds) "
                "VALUES (?, ?, ?, ?, ?)",
                (name, status, int(time.time()), latency, uptime)
            )
            self._db_conn.commit()
        except Exception as e:
            logger.warning(f"DB log error ({name}): {e}")

    def _purge_old_history(self):
        """Удаляет записи старше HISTORY_RETENTION."""
        try:
            cutoff = int(time.time()) - HISTORY_RETENTION
            self._db_conn.execute("DELETE FROM health_history WHERE checked_at < ?", (cutoff,))
            self._db_conn.commit()
            logger.info(f"🧹 History purged (before {cutoff})")
        except Exception as e:
            logger.warning(f"Purge error: {e}")

    def _get_full_state_for_ws(self) -> Dict:
        """Возвращает состояние для отправки через WS при коннекте."""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "engine_uptime": int(time.time() - self.start_time),
            "services": {name: s.to_dict() for name, s in self.statuses.items()},
            "degradation": self.degradation_modes,
        }

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
            result = await self.check_http(svc, health_port=svc["health_port"])
        elif svc.get("path") and svc.get("port"):
            result = await self.check_http(svc)
        else:
            result = await self.check_tcp(svc)

        # L13: пишем в SQLite историю
        self._log_to_db(
            result.name,
            "alive" if result.is_alive else "dead",
            result.latency_ms,
            int(time.time() - result._start_time) if result._start_time else 0
        )

        return result

    async def monitor_loop(self):
        logger.info("🚀 Health Check Engine v3.5 (L15: Auto-Recovery + Alert Engine) started")
        await asyncio.sleep(3)
        purge_counter = 0
        while True:
            try:
                tasks = [self.check_service(svc) for svc in SERVICES]
                await asyncio.gather(*tasks)
                self._detect_degradation()

                # L13: WebSocket broadcast изменений
                current_state = {name: s.to_dict() for name, s in self.statuses.items()}
                for name, cur in current_state.items():
                    prev = self._last_broadcast_state.get(name, {})
                    if cur.get("is_alive") != prev.get("is_alive"):
                        await self._broadcaster.broadcast_status_change(name, prev, cur)
                self._last_broadcast_state = current_state

                # L13: Alert engine check
                await self._alert_engine.evaluate(current_state)

                # L15: Auto-Recovery — для dead сервисов (3+ consecutive fails)
                for name, st in current_state.items():
                    if not st.get("is_alive"):
                        # Передаём full state для slot health check
                        st["_all_statuses"] = current_state
                        await self._auto_recovery.on_service_dead(name, st)

                self._save_status()

                # L13: Purge раз в час
                purge_counter += 1
                if purge_counter >= 360:  # ~раз в час (10 сек * 360 = 3600 сек)
                    self._purge_old_history()
                    purge_counter = 0

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
            "engine": "HealthEngine v3.5 L15",
            "config_driven": True,
            "monitored_services": len(SERVICES),
            "ws_enabled": True,
            "alert_engine": True,
            "history_db": HEALTH_DB,
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

    # ═══ L13: Dashboard API + WebSocket ═══
    async def health_dashboard(request):
        """GET /api/v1/health/dashboard — JSON для frontend-виджета."""
        summary = engine.get_health_summary()
        services = summary.get("services", {})
        total = len(services)
        alive = sum(1 for s in services.values() if s.get("is_alive"))
        dead = total - alive

        # Иерархия по слоям
        layers = {}
        for name, s in services.items():
            layer = "other"
            if name.startswith("nostr_bridge"):
                layer = "nostr"
            elif name in ("smart_router", "route_engine", "content_router"):
                layer = "routing"
            elif name in ("external_gateway", "cross_mesh_bridge"):
                layer = "gateway"
            elif name in ("identity_api", "verifier"):
                layer = "identity"
            elif name in ("supervisor", "relay_mesh_api", "relay_v2"):
                layer = "infra"
            layers.setdefault(layer, []).append(s)

        return web.json_response({
            "overall": "healthy" if dead == 0 else "degraded" if dead <= 3 else "critical",
            "summary": {
                "total": total,
                "alive": alive,
                "dead": dead,
                "degraded": sum(1 for s in services.values() if s.get("degraded")),
                "health_pct": summary.get("summary", {}).get("health_pct", 0),
            },
            "engine": {
                "uptime": summary.get("engine_uptime_seconds", 0),
                "version": "3.5",
            },
            "degradation": summary.get("degradation_modes", {}),
            "layers": {
                name: {
                    "alive": sum(1 for s in svcs if s.get("is_alive")),
                    "total": len(svcs),
                    "services": [s.get("name") for s in svcs],
                }
                for name, svcs in layers.items()
            },
            "timestamp": datetime.utcnow().isoformat(),
        })

    app.router.add_get("/api/v1/health/dashboard", health_dashboard)

    # L13: WebSocket endpoint
    broadcaster = get_broadcaster()
    app.router.add_get("/api/v1/health/ws", broadcaster.ws_handler)

    # L13: History endpoint (последние N записей по сервису)
    async def health_history(request):
        name = request.query.get("service", "")
        limit = int(request.query.get("limit", 100))

        if not name:
            return web.json_response({"ok": False, "error": "service param required"}, status=400)

        try:
            cur = engine._db_conn.execute(
                "SELECT checked_at, status, latency_ms, uptime_seconds "
                "FROM health_history WHERE service_name = ? "
                "ORDER BY checked_at DESC LIMIT ?",
                (name, limit)
            )
            rows = [
                {
                    "time": r[0],
                    "status": r[1],
                    "latency_ms": r[2],
                    "uptime": r[3],
                }
                for r in cur.fetchall()
            ]
            return web.json_response({"ok": True, "service": name, "history": rows})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=500)

    app.router.add_get("/api/v1/health/history", health_history)

    # ═══ L14: Alert Engine API ═══
    alert_engine = get_alert_engine()

    async def alerts_list(request):
        limit = int(request.query.get("limit", 20))
        active_only = request.query.get("active", "").lower() == "true"
        alerts = alert_engine.get_alerts(limit=limit, active_only=active_only)
        return web.json_response({"ok": True, "count": len(alerts), "alerts": alerts})

    async def alerts_active(request):
        alerts = alert_engine.get_active_alerts()
        return web.json_response({"ok": True, "count": len(alerts), "alerts": alerts})

    async def alerts_ack(request):
        alert_id = request.match_info.get("alert_id", "")
        if not alert_id:
            return web.json_response({"ok": False, "error": "alert_id required"}, status=400)
        ok = await alert_engine.acknowledge(alert_id)
        return web.json_response({"ok": ok, "alert_id": alert_id})

    async def alerts_reload(request):
        alert_engine.reload_rules()
        return web.json_response({"ok": True, "rules_count": len(alert_engine.rules)})

    app.router.add_get("/api/v1/alerts", alerts_list)
    app.router.add_get("/api/v1/alerts/active", alerts_active)
    app.router.add_post("/api/v1/alerts/ack/{alert_id}", alerts_ack)
    app.router.add_post("/api/v1/alerts/reload", alerts_reload)

    # ═══ L15: Auto-Recovery API ═══
    recovery = get_auto_recovery()

    async def recovery_stats(request):
        return web.json_response({
            "ok": True,
            "stats": recovery.get_stats(),
            "strategies": list(recovery.strategies.keys()),
            "daily_count": recovery._daily_count,
            "daily_limit": recovery.config.get("max_daily_total", 30),
        })

    async def recovery_events(request):
        service = request.query.get("service", "")
        limit = int(request.query.get("limit", 20))
        events = recovery.get_recovery_events(service_name=service, limit=limit)
        return web.json_response({"ok": True, "count": len(events), "events": events})

    async def recovery_analysis(request):
        analysis = recovery.get_analysis()
        return web.json_response({"ok": True, "count": len(analysis), "analysis": analysis})

    async def recovery_reset(request):
        service = request.match_info.get("service", "")
        if not service:
            return web.json_response({"ok": False, "error": "service required"}, status=400)
        recovery.reset_service(service)
        return web.json_response({"ok": True, "service": service})

    async def recovery_reload(request):
        recovery.reload_config()
        return web.json_response({"ok": True, "strategies": len(recovery.strategies)})

    app.router.add_get("/api/v1/recovery/stats", recovery_stats)
    app.router.add_get("/api/v1/recovery/events", recovery_events)
    app.router.add_get("/api/v1/recovery/analysis", recovery_analysis)
    app.router.add_post("/api/v1/recovery/reset/{service}", recovery_reset)
    app.router.add_post("/api/v1/recovery/reload", recovery_reload)

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

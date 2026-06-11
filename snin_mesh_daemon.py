"""
P19: Production Release — SNIN Mesh Fabric unified daemon.

Entry point for running the full SNIN V5 Mesh Fabric stack.
Starts all services, exposes health API, handles graceful shutdown.

Services started:
  - KnowledgeGraph (Redis-backed)
  - SmartRouter (route optimization)
  - SemanticRouter (topic classification)
  - ContentRouter (event routing pipeline P12)
  - FederationDiscovery (cross-mesh P18)
  - RewardLedger (DAO rewards P17)
  - ChequeMeshRouter (payments P16)
  - CronScheduler (agent scheduling P16)
  - CrossMeshBridge (legacy P2P)
  - HealthCheck API (port 9997)

Usage:
  python3 snin_mesh_daemon.py                # run all services
  python3 snin_mesh_daemon.py --service X    # run specific service
  python3 snin_mesh_daemon.py --check         # health check only
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
import threading
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler

# ─── Paths ──────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MESH-DAEMON] %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_DIR / "mesh_daemon.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("mesh-daemon")

# ─── Service Registry ───────────────────────────────────

class Service:
    def __init__(self, name: str, start_fn, stop_fn=None, depends: list = None):
        self.name = name
        self.start_fn = start_fn
        self.stop_fn = stop_fn
        self.depends = depends or []
        self.started = False
        self.status = "pending"
        self.error = None
        self.start_time = None

    def start(self):
        try:
            self.start_time = time.time()
            result = self.start_fn()
            self.started = True
            self.status = "running"
            return result
        except Exception as e:
            self.error = str(e)
            self.status = "failed"
            log.error(f"[{self.name}] Failed to start: {e}")
            return None

    def stop(self):
        if self.stop_fn:
            try:
                self.stop_fn()
            except Exception as e:
                log.warning(f"[{self.name}] Stop error: {e}")
        self.status = "stopped"
        self.started = False


class ServiceRegistry:
    def __init__(self):
        self.services: dict[str, Service] = {}
        self.start_order: list[str] = []

    def register(self, service: Service):
        self.services[service.name] = service

    def set_order(self, order: list[str]):
        self.start_order = order

    def start_all(self) -> bool:
        """Start services in dependency order."""
        started = set()

        for name in self.start_order:
            if name not in self.services:
                log.warning(f"Unknown service: {name}")
                continue

            svc = self.services[name]

            # Check dependencies
            deps_ok = True
            for dep in svc.depends:
                if dep not in started:
                    log.error(f"[{name}] Dependency not started: {dep}")
                    deps_ok = False

            if not deps_ok:
                svc.status = "failed"
                svc.error = "dependency not met"
                continue

            log.info(f"Starting: {name}...")
            try:
                svc.start()
                if svc.started:
                    started.add(name)
                    log.info(f"[{name}] ✅ started")
                else:
                    log.error(f"[{name}] ❌ failed: {svc.error}")
            except Exception as e:
                svc.status = "failed"
                svc.error = str(e)
                log.error(f"[{name}] ❌ exception: {e}")

        return all(svc.started for svc in self.services.values())

    def stop_all(self):
        """Stop services in reverse order."""
        for name in reversed(self.start_order):
            if name in self.services:
                svc = self.services[name]
                if svc.started:
                    log.info(f"Stopping: {name}...")
                    svc.stop()

    def get_status(self) -> dict:
        return {
            name: {
                "status": svc.status,
                "started": svc.started,
                "uptime_sec": round(time.time() - svc.start_time, 1) if svc.start_time else 0,
                "error": svc.error,
            }
            for name, svc in self.services.items()
        }

    def is_healthy(self) -> bool:
        return all(svc.started for svc in self.services.values())


# ─── Service Factories ─────────────────────────────────

def _start_knowledge_graph():
    """Initialize Redis-backed knowledge graph."""
    import redis
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    r.ping()
    from knowledge_graph import KnowledgeGraph
    return KnowledgeGraph(r)


def _start_smart_router():
    """Initialize smart router."""
    from smart_router import SmartRouter
    return SmartRouter()


def _start_semantic_router():
    """Initialize semantic router tied to KG+SmartRouter."""
    import redis
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    kg = _start_knowledge_graph()
    sr = _start_smart_router()
    from semantic_router import create_semantic_router
    return create_semantic_router(kg, sr, r)


def _start_content_router():
    """Initialize content router pipeline."""
    sem = _start_semantic_router()
    from content_router import create_content_router
    return create_content_router(sem)


def _start_reward_ledger():
    """Initialize DAO reward ledger."""
    from dao_rewards import RewardLedger
    return RewardLedger()


def _start_cheque_mesh():
    """Initialize ChequeMesh payment router."""
    from cheque_mesh import ChequeMeshRouter
    return ChequeMeshRouter()


def _start_cron_scheduler():
    """Initialize agent cron scheduler."""
    from agent_cron import CronScheduler
    cs = CronScheduler()
    threading.Thread(target=cs.run_forever, daemon=True, name="cron-scheduler").start()
    return cs


def _start_federation_discovery():
    """Initialize federation discovery."""
    from federation_discovery import (
        create_local_topology, FederationDiscovery,
        make_federation_announce_handler, make_federation_scan_handler,
    )
    topo = create_local_topology("snin-v5-mesh-fabric")
    fd = FederationDiscovery(topo)
    # Register cron handlers
    cs = _start_cron_scheduler()
    cs.register("federation", "announce", 600,
                make_federation_announce_handler(fd))
    cs.register("federation", "scan", 300,
                make_federation_scan_handler(fd))
    # Initial announce
    fd.announce()
    return fd


def _start_health_api():
    """Start health check HTTP API on port 9997."""
    registry_ref = []  # will be filled by main

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            status = registry.get_status() if (registry := registry_ref[0] if registry_ref else None) else {}
            data = {
                "service": "snin-mesh-fabric",
                "version": "P19",
                "healthy": all(s.get("started") for s in status.values()),
                "services": status,
                "uptime_sec": round(time.time() - boot_time, 1),
                "python_files": len(list(BASE_DIR.glob("*.py"))),
            }
            body = json.dumps(data, ensure_ascii=False, indent=2)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body.encode())

        def log_message(self, fmt, *args):
            pass  # silent

    boot_time = time.time()
    server = HTTPServer(("0.0.0.0", 9997), HealthHandler)
    log.info("[HealthAPI] Listening on :9997")
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="health-api")
    thread.start()
    return server


# ─── Main ──────────────────────────────────────────────

def main():
    global registry
    import redis

    # Verify Redis
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.ping()
    except Exception as e:
        log.error(f"Redis not available: {e}")
        log.error("Start Redis: redis-server --daemonize yes")
        sys.exit(1)

    registry = ServiceRegistry()

    # Register services with dependencies
    services = [
        Service("knowledge_graph", _start_knowledge_graph),
        Service("smart_router", _start_smart_router),
        Service("semantic_router", _start_semantic_router,
                depends=["knowledge_graph", "smart_router"]),
        Service("content_router", _start_content_router,
                depends=["semantic_router"]),
        Service("reward_ledger", _start_reward_ledger),
        Service("cheque_mesh", _start_cheque_mesh),
        Service("cron_scheduler", _start_cron_scheduler),
        Service("federation_discovery", _start_federation_discovery,
                depends=["reward_ledger", "cron_scheduler", "content_router"]),
    ]

    for svc in services:
        registry.register(svc)

    registry.set_order([s.name for s in services])

    # Store registry for health API
    _health_registry = registry
    globals()['_health_registry'] = registry

    # Start health API first
    import http.server
    health_server = _start_health_api()

    # Import to make registry accessible in handler
    http.server.registry = registry

    # Start all services
    log.info("═══ SNIN Mesh Fabric P19 — Starting ═══")
    ok = registry.start_all()

    if ok:
        log.info("═══ All services started ✅ ═══")
    else:
        failed = [n for n, s in registry.services.items() if not s.started]
        log.warning(f"═══ Started with errors: {failed} ═══")

    # Print status
    status = registry.get_status()
    log.info(f"Services: {json.dumps({n: s['status'] for n, s in status.items()}, indent=2)}")

    # Setup signal handlers
    def shutdown(sig, frame):
        log.info("Shutting down...")
        registry.stop_all()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # Keep alive
    try:
        while True:
            time.sleep(60)
            # Periodic health log
            running = sum(1 for s in registry.services.values() if s.started)
            total = len(registry.services)
            log.info(f"[Heartbeat] {running}/{total} services running")
    except KeyboardInterrupt:
        shutdown(None, None)


# ─── CLI ───────────────────────────────────────────────

def cli_check():
    """Health check mode — just print status and exit."""
    import urllib.request
    try:
        resp = urllib.request.urlopen("http://localhost:9997")
        data = json.loads(resp.read())
        print(json.dumps(data, indent=2, ensure_ascii=False))
        healthy = data.get("healthy", False)
        sys.exit(0 if healthy else 1)
    except Exception as e:
        print(f"Health check failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if "--check" in sys.argv:
        cli_check()
    else:
        main()

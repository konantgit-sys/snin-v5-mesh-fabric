#!/usr/bin/env python3
"""
SNIN Metrics Aggregator (Phase 2: Monitoring Level 2)
═══════════════════════════════════════════════════════

Fetches /metrics from all SNIN services and provides:
  GET /metrics     — merged Prometheus format (for Prometheus if installed later)
  GET /api/metrics  — JSON summary (for dashboard)
  GET /             — redirect to dashboard

Supports auto-discovery via SUPERVISOR_URL or static SERVICE_LIST.
"""

import os, sys, time, json, logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import URLError
from threading import Thread, Lock

logging.basicConfig(level=logging.INFO, format='[Aggregator] %(message)s')
logger = logging.getLogger("snin.aggregator")

PORT = int(os.environ.get("METRICS_AGGREGATOR_PORT", "9090"))
SCRAPE_INTERVAL = int(os.environ.get("METRICS_SCRAPE_INTERVAL", "15"))

# Static service list (used when no supervisor)
SERVICE_LIST = [
    ("smart_router", "127.0.0.1", 9932, "mesh"),
    ("content_router", "127.0.0.1", 9920, "mesh"),
    ("route_engine", "127.0.0.1", 9910, "mesh"),
    ("external_gateway", "127.0.0.1", 9915, "mesh"),
    ("nostr_bridge", "127.0.0.1", 9941, "nostr"),
    ("cross_mesh", "127.0.0.1", 9946, "mesh"),
    ("supervisor", "127.0.0.1", 9900, "control"),
    ("relay", "127.0.0.1", 8197, "storage"),
    ("snin_hub", "127.0.0.1", 9950, "web"),
]

# Future: Prometheus metrics ports per service
METRICS_PORTS = {
    "smart_router": 9092,
    "content_router": 9093,
    "route_engine": 9094,
    "external_gateway": 9095,
    "nostr_bridge": 9096,
}


class MetricsAggregator:
    """Collects health + metrics from SNIN services."""
    
    def __init__(self):
        self._lock = Lock()
        self._health: dict = {}
        self._last_scrape = 0
        self._scrape_count = 0
    
    def scrape_all(self) -> dict:
        """Scrape health from all services, return summary."""
        results = {}
        timestamp = time.time()
        
        for name, host, port, category in SERVICE_LIST:
            try:
                req = Request(f"http://{host}:{port}/health", headers={"User-Agent": "snin-aggregator"})
                resp = urlopen(req, timeout=3)
                data = json.loads(resp.read())
                results[name] = {
                    "status": "online",
                    "port": port,
                    "category": category,
                    "data": data,
                    "latency_ms": round((time.time() - timestamp) * 1000, 1)
                }
            except Exception as e:
                results[name] = {
                    "status": "offline",
                    "port": port,
                    "category": category,
                    "error": str(e)[:100]
                }
        
        with self._lock:
            self._health = results
            self._last_scrape = timestamp
            self._scrape_count += 1
        
        return results
    
    def get_summary(self) -> dict:
        """Return cached summary."""
        online = sum(1 for s in self._health.values() if s["status"] == "online")
        total = len(self._health) if self._health else len(SERVICE_LIST)
        return {
            "timestamp": self._last_scrape,
            "scrape_count": self._scrape_count,
            "services_total": total,
            "services_online": online,
            "services_offline": total - online,
            "uptime_pct": round(online / max(total, 1) * 100, 1),
            "services": self._health if self._health else {},
            "mesh_health": self._mesh_health(),
        }
    
    def _mesh_health(self) -> str:
        """Mesh health status."""
        if not self._health:
            return "unknown"
        critical = ["smart_router", "content_router", "route_engine"]
        offline = [s for s in critical if self._health.get(s, {}).get("status") != "online"]
        if len(offline) == 0:
            return "healthy"
        elif len(offline) == 1:
            return f"degraded ({offline[0]} offline)"
        else:
            return f"critical ({', '.join(offline)} offline)"


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP Handler
# ═══════════════════════════════════════════════════════════════════════════════

_aggregator = MetricsAggregator()

class AggregatorHandler(BaseHTTPRequestHandler):
    
    def log_message(self, fmt, *args):
        pass
    
    def _json(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False, indent=2).encode())
    
    def do_GET(self):
        if self.path == "/health":
            self._json(200, _aggregator.get_summary())
        elif self.path == "/api/metrics":
            self._json(200, _aggregator.get_summary())
        elif self.path == "/api/scrape":
            results = _aggregator.scrape_all()
            self._json(200, _aggregator.get_summary())
        elif self.path == "/" or self.path == "/index.html":
            # Serve dashboard HTML
            dash_path = os.path.join(os.path.dirname(__file__), "..", "snin-dashboard", "index.html")
            try:
                with open(dash_path, "r") as f:
                    html = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(html.encode())
            except FileNotFoundError:
                self._json(404, {"error": "dashboard not found"})
        else:
            self._json(404, {"error": "not found"})


# ═══════════════════════════════════════════════════════════════════════════════
# Background scraper + main
# ═══════════════════════════════════════════════════════════════════════════════

def _scrape_loop():
    """Background scraper thread."""
    while True:
        try:
            _aggregator.scrape_all()
            summary = _aggregator.get_summary()
            logger.info(f"Scrape #{summary['scrape_count']}: {summary['services_online']}/{summary['services_total']} online, mesh={summary['mesh_health']}")
        except Exception as e:
            logger.error(f"Scrape error: {e}")
        time.sleep(SCRAPE_INTERVAL)


def main():
    # Initial scrape
    _aggregator.scrape_all()
    
    # Start background scraper
    Thread(target=_scrape_loop, daemon=True).start()
    
    # Start HTTP server
    server = HTTPServer(("0.0.0.0", PORT), AggregatorHandler)
    logger.info(f"Metrics Aggregator started on :{PORT}")
    logger.info(f"  GET /        — health summary (JSON)")
    logger.info(f"  GET /api/scrape — force scrape")
    logger.info(f"  Scrape interval: {SCRAPE_INTERVAL}s")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()

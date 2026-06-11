"""
P19.1: Production Health API — external-ready monitoring service.

Exposes:
  GET /health      — JSON status of all services
  GET /metrics     — Prometheus-style metrics
  GET /stress      — self-diagnostic: endpoint count, uptime, memory
  GET /logs/tail   — last 50 log lines

Runs on port 8085, designed for full-proxy behind *.v2.site.
Structured JSON logging to file + stdout.
"""

import json
import logging
import os
import sys
import time
import threading
import resource
from datetime import datetime, timezone
from pathlib import Path
from flask import Flask, jsonify, request

BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ─── Structured JSON Logging ──────────────────────

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": record.name,
            "message": record.getMessage(),
            "pid": os.getpid(),
        }
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, ensure_ascii=False)

log = logging.getLogger("health-api")
log.setLevel(logging.INFO)

# File handler — structured JSON
fh = logging.FileHandler(str(LOG_DIR / "health_api.jsonl"))
fh.setFormatter(JSONFormatter())
log.addHandler(fh)

# Console handler — human-readable
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s %(message)s"))
log.addHandler(ch)

app = Flask(__name__)
boot_time = time.time()
request_count = 0
error_count = 0

# ─── Service Registry (lazy imports) ─────────────

_services_status = {}
_available = False

def _probe_services():
    """Probe all services and update status. Non-blocking — failures logged."""
    global _services_status, _available
    status = {}
    all_ok = True

    # 1. Redis
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, decode_responses=True, socket_connect_timeout=2)
        r.ping()
        status["redis"] = {"status": "up", "latency_ms": round((time.time() - time.time()) * 1000)}
    except Exception as e:
        status["redis"] = {"status": "down", "error": str(e)[:100]}
        all_ok = False

    # 2. Python modules
    modules = [
        "knowledge_graph", "smart_router", "semantic_router",
        "content_router", "dao_rewards", "cheque_mesh",
        "agent_cron", "federation_discovery",
    ]
    for mod in modules:
        try:
            __import__(mod)
            status[mod] = {"status": "importable"}
        except Exception as e:
            status[mod] = {"status": "broken", "error": str(e)[:100]}
            all_ok = False

    # 3. File count
    py_files = len(list(BASE_DIR.glob("*.py")))
    test_files = len(list(BASE_DIR.glob("test_phase*.py")))

    # 4. Log size
    log_files = list(LOG_DIR.glob("*.jsonl")) + list(LOG_DIR.glob("*.log"))
    log_size_mb = round(sum(f.stat().st_size for f in log_files) / (1024 * 1024), 2)

    # 5. Memory
    mem = resource.getrusage(resource.RUSAGE_SELF)
    mem_mb = round(mem.ru_maxrss / 1024, 1)

    _services_status = {
        "service": "snin-mesh-fabric",
        "version": "P19.1",
        "healthy": all_ok,
        "uptime_sec": round(time.time() - boot_time, 1),
        "boot_time": datetime.fromtimestamp(boot_time, tz=timezone.utc).isoformat(),
        "request_count": request_count,
        "error_count": error_count,
        "memory_mb": mem_mb,
        "log_size_mb": log_size_mb,
        "code": {
            "python_files": py_files,
            "test_files": test_files,
        },
        "services": status,
    }
    _available = True

# Probe immediately, then every 30s
_probe_services()
threading.Thread(target=lambda: ([time.sleep(30), _probe_services()] for _ in iter(int, 1)), daemon=True).start()

# ─── Routes ──────────────────────────────────────

@app.route("/health")
def health():
    global request_count
    request_count += 1
    if not _available:
        return jsonify({"status": "initializing", "healthy": False}), 503
    return jsonify(_services_status), 200 if _services_status["healthy"] else 503


@app.route("/metrics")
def metrics():
    global request_count
    request_count += 1
    s = _services_status
    lines = [
        "# HELP snin_mesh_up Whether the mesh is healthy (1=yes, 0=no)",
        f"snin_mesh_up {1 if s.get('healthy') else 0}",
        "# HELP snin_mesh_uptime_seconds Uptime in seconds",
        f"snin_mesh_uptime_seconds {s.get('uptime_sec', 0)}",
        "# HELP snin_mesh_requests_total Total HTTP requests",
        f"snin_mesh_requests_total {request_count}",
        "# HELP snin_mesh_errors_total Total HTTP errors",
        f"snin_mesh_errors_total {error_count}",
        "# HELP snin_mesh_memory_mb RSS memory in MB",
        f"snin_mesh_memory_mb {s.get('memory_mb', 0)}",
        "# HELP snin_mesh_log_size_mb Log size in MB",
        f"snin_mesh_log_size_mb {s.get('log_size_mb', 0)}",
        f"# HELP snin_mesh_python_files Python file count",
        f"snin_mesh_python_files {s.get('code', {}).get('python_files', 0)}",
    ]
    # Per-service metrics
    for svc_name, svc in s.get("services", {}).items():
        up = 1 if svc.get("status") in ("up", "importable") else 0
        clean_name = svc_name.replace("-", "_").replace(".", "_")
        lines.append(f"# HELP snin_service_{clean_name}_up Service status")
        lines.append(f"snin_service_{clean_name}_up {up}")
    return "\n".join(lines) + "\n", 200, {"Content-Type": "text/plain"}


@app.route("/stress")
def stress_info():
    """Self-diagnostics for stress testing."""
    global request_count
    request_count += 1
    return jsonify({
        "endpoint": "/stress",
        "pid": os.getpid(),
        "thread_count": threading.active_count(),
        "memory_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1),
        "uptime_sec": round(time.time() - boot_time, 1),
        "requests_served": request_count,
        "errors": error_count,
        "python_version": sys.version,
    })


@app.route("/logs/tail")
def logs_tail():
    global request_count
    request_count += 1
    lines_param = request.args.get("lines", "50")
    try:
        n = min(int(lines_param), 200)
    except ValueError:
        n = 50

    log_file = LOG_DIR / "health_api.jsonl"
    if not log_file.exists():
        return jsonify({"error": "no log file yet"}), 404

    with open(log_file) as f:
        all_lines = f.readlines()
    return jsonify({
        "file": str(log_file),
        "total_lines": len(all_lines),
        "tail": [json.loads(l) for l in all_lines[-n:]],
    })


@app.errorhandler(500)
def handle_500(e):
    global error_count
    error_count += 1
    log.error(f"Internal error: {e}")
    return jsonify({"error": "internal", "request_id": str(time.time())}), 500


@app.before_request
def log_request():
    log.info(f"{request.method} {request.path} from {request.remote_addr}")


# ─── Main ────────────────────────────────────────

if __name__ == "__main__":
    log.info("═══ SNIN Health API vP19.1 starting on :8085 ═══")
    log.info(f"PID: {os.getpid()}, Python: {sys.version.split()[0]}")
    log.info(f"Log dir: {LOG_DIR}")

    # Warm up
    _probe_services()
    healthy_count = sum(1 for s in _services_status.get("services", {}).values()
                        if s.get("status") in ("up", "importable"))
    total_count = len(_services_status.get("services", {}))
    log.info(f"Initial probe: {healthy_count}/{total_count} services healthy")

    app.run(host="0.0.0.0", port=8085, debug=False, threaded=True)

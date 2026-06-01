#!/usr/bin/env python3
"""
SNIN L9 — Orchestration Layer :9900

Управление всей сетью. Надстройка над supervisor.
  - Supervisor (статусы всех сервисов)
  - Auto-healing (рестарт упавших)
  - Layer topology (какие сервисы к каким слоям)
  - Dependency graph (кто от кого зависит)
  - Performance metrics (CPU/RAM/uptime по слоям)
"""

import asyncio
import json
import os
import socket
import sys
import time
import signal
import subprocess
import http.server
from datetime import datetime
from pathlib import Path

# ─── Конфиг ───
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9900
SUPERVISOR_STATUS = "/home/agent/data/sites/snin-hub/supervisor_status.json"
SUPERVISOR_PY = "/home/agent/data/sites/snin-hub/supervisor.py"
LOG_DIR = "/home/agent/data/logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Слои и их сервисы
LAYER_MAP = {
    "L0 Protocol":    ["relay_v2", "p2p_dash", "snin_network", "relay_frontend"],
    "L1.5 Bridge":    ["cross_mesh", "l1_5_bridge", "cross_mesh_bridge",
                       "mesh_nostr_bridge_0","mesh_nostr_bridge_1",
                       "mesh_nostr_bridge_2","mesh_nostr_bridge_3"],
    "L2 Transport":   ["l2_transport", "mesh_simple_agent"],
    "L2.5 Encryption":["encryption_layer"],
    "L3 Mesh":        ["mesh_api", "mesh_smart_router", "mesh_chrono",
                       "mesh_relay", "mesh_route_engine", "mesh_content_router",
                       "mesh_external_gate", "l3_mesh_core"],
    "L3.5 ZK":        ["zk_layer"],
    "L4 Payment":     ["l4_payment", "snin_pay", "snin_tracker",
                       "cheque_book", "verifier", "dao_mesh","snin_dao"],
    "L4.5 Privacy":   ["privacy_layer"],
    "L5 Identity":    ["identity_api", "forecaster_ai", "archivist_ai"],
    "L6 Agents":      ["l6_network", "scc_agent"],
    "L8 Application": ["app_layer", "hub_api"],
    "L9 Orchestration":["api_gateway", "l9_orchestration"],
    "Other":          ["sninbot", "snin_cmd", "snin_upload",
                       "analion", "esp32_bridge", "esp32_bridge_v3",
                       "science_mesh", "city_mesh", "trading_mesh",
                       "defi_mesh", "crowd_mesh", "chain_mesh", "energy_mesh"],
}

L9_SVC_NAME = "l9_orchestration"
SERVER_PIDFILE = f"/tmp/snin_{L9_SVC_NAME}.pid"


def port_open(host="127.0.0.1", port=9900, timeout=1.5) -> bool:
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True
    except: return False


def read_supervisor_status() -> dict:
    try:
        with open(SUPERVISOR_STATUS) as f:
            return json.load(f)
    except: return {"alive": 0, "dead": 0, "total_services": 0, "services": {}}


def read_process_stats() -> dict:
    """Системные метрики."""
    try:
        load = open("/proc/loadavg").read().split()[:3]
        mem = subprocess.run(
            "free -m | awk 'NR==2{print $2,$3,$4,$7}'", shell=True,
            capture_output=True, text=True).stdout.strip().split()
        uptime = open("/proc/uptime").read().split()[0]
        disk = subprocess.run(
            "df -h / | tail -1 | awk '{print $2,$3,$4,$5}'", shell=True,
            capture_output=True, text=True).stdout.strip().split()
        return {
            "load": load,
            "mem_total_mb": int(mem[0]) if len(mem) > 0 else 0,
            "mem_used_mb": int(mem[1]) if len(mem) > 1 else 0,
            "mem_free_mb": int(mem[2]) if len(mem) > 2 else 0,
            "mem_avail_mb": int(mem[3]) if len(mem) > 3 else 0,
            "uptime_s": int(float(uptime)),
            "disk_total": disk[0] if len(disk) > 0 else "?",
            "disk_used": disk[1] if len(disk) > 1 else "?",
            "disk_free": disk[2] if len(disk) > 2 else "?",
            "disk_pct": disk[3] if len(disk) > 3 else "?",
        }
    except: return {}


def build_layer_status(status: dict) -> list:
    """Группировка сервисов по слоям."""
    services = status.get("services", {})
    layers = []
    for layer_name, svc_names in LAYER_MAP.items():
        layer_svcs = []
        alive = 0
        for name in svc_names:
            svc = services.get(name, {})
            is_alive = svc.get("alive", False)
            if is_alive: alive += 1
            layer_svcs.append({
                "name": name,
                "alive": is_alive,
                "port": svc.get("port", 0),
                "restarts": svc.get("restarts", 0),
            })
        layers.append({
            "layer": layer_name,
            "alive": alive,
            "total": len(svc_names),
            "services": layer_svcs,
        })
    return layers


def find_dead_critical(status: dict) -> list:
    """Какие критичные сервисы упали."""
    services = status.get("services", {})
    critical_map = {}
    try:
        with open(SUPERVISOR_PY) as f:
            code = f.read()
        import re
        for m in re.finditer(r'\{"name":\s*"([^"]+)",[^}]*"critical":\s*(True|false)', code):
            name = m.group(1)
            critical = m.group(2) == "True"
            if critical:
                critical_map[name] = True
    except: pass

    dead = []
    for name, svc in services.items():
        if not svc.get("alive", False) and critical_map.get(name, False):
            dead.append(name)
    return dead


def build_dependency_graph() -> dict:
    """Граф зависимостей: какие слои зависят от каких."""
    layers_order = [
        "L0 Protocol", "L1.5 Bridge", "L2 Transport", "L2.5 Encryption",
        "L3 Mesh", "L3.5 ZK", "L4 Payment", "L4.5 Privacy",
        "L5 Identity", "L6 Agents", "L8 Application", "L9 Orchestration",
    ]
    deps = {}
    for i, layer in enumerate(layers_order):
        upstream = [l for l in layers_order[:i]] if i > 0 else []
        downstream = [l for l in layers_order[i+1:]] if i < len(layers_order)-1 else []
        deps[layer] = {
            "depends_on": upstream,
            "depended_by": downstream,
            "level": i,
        }
    return {"order": layers_order, "dependencies": deps, "total_layers": len(layers_order)}


# ═══════════════════════════════════════════
# HTTP API
# ═══════════════════════════════════════════

class L9Handler(http.server.BaseHTTPRequestHandler):

    def _json(self, data: dict, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2, default=str).encode())

    def _error(self, msg: str, status=404):
        self._json({"error": msg}, status)

    def do_GET(self):
        path = self.path.rstrip("/")

        if path == "/" or path == "":
            sup = read_supervisor_status()
            self._json({
                "service": "SNIN L9 — Orchestration Layer",
                "version": "1.0",
                "total_services": sup.get("total_services", 0),
                "alive": sup.get("alive", 0),
                "dead": sup.get("dead", 0),
                "layers": len(LAYER_MAP),
                "uptime_s": sup.get("uptime_sec", 0),
                "endpoints": [
                    "/health — общий статус",
                    "/layers — группировка по слоям",
                    "/topology — граф зависимостей",
                    "/services — все сервисы",
                    "/dead — упавшие сервисы",
                    "/restart/{name} — рестарт сервиса",
                    "/metrics — система/процессы",
                ]
            })

        elif path == "/health":
            sup = read_supervisor_status()
            dead = find_dead_critical(sup)
            self._json({
                "status": "ok" if not dead else "degraded",
                "layer": "L9 — Orchestration",
                "alive": sup.get("alive", 0),
                "dead": sup.get("dead", 0),
                "total": sup.get("total_services", 0),
                "total_restarts": sup.get("total_restarts", 0),
                "critical_dead": dead,
                "layers_alive": sum(1 for l in build_layer_status(sup) if l["alive"] > 0),
                "layers_total": len(LAYER_MAP),
            })

        elif path == "/layers":
            sup = read_supervisor_status()
            layers = build_layer_status(sup)
            self._json({
                "layers": layers,
                "summary": {
                    "total": len(layers),
                    "healthy": sum(1 for l in layers if l["alive"] == l["total"]),
                    "degraded": sum(1 for l in layers if 0 < l["alive"] < l["total"]),
                    "dead": sum(1 for l in layers if l["alive"] == 0),
                }
            })

        elif path == "/topology":
            self._json(build_dependency_graph())

        elif path == "/services":
            sup = read_supervisor_status()
            svcs = sup.get("services", {})
            self._json({"services": svcs, "count": len(svcs)})

        elif path == "/dead":
            sup = read_supervisor_status()
            svcs = sup.get("services", {})
            dead_list = [{"name": n, "port": s.get("port"),
                          "fails": s.get("fails"), "restarts": s.get("restarts")}
                         for n, s in svcs.items() if not s.get("alive")]
            critical_dead = find_dead_critical(sup)
            self._json({
                "dead_count": len(dead_list),
                "critical_dead": critical_dead,
                "dead": dead_list,
            })

        elif path == "/metrics":
            sys_stats = read_process_stats()
            sup = read_supervisor_status()
            self._json({
                "system": sys_stats,
                "orchestration": {
                    "total_services": sup.get("total_services", 0),
                    "alive": sup.get("alive", 0),
                    "dead": sup.get("dead", 0),
                    "total_restarts": sup.get("total_restarts", 0),
                    "uptime_s": sup.get("uptime_sec", 0),
                }
            })

        elif path == "/l9/health":
            self._json({"status": "ok", "service": "L9 Orchestration", "port": PORT})

        else:
            self._error(f"not found: {path}")

    def do_POST(self):
        path = self.path.rstrip("/")

        if path.startswith("/restart/"):
            name = path.split("/restart/")[1]
            sup = read_supervisor_status()
            svcs = sup.get("services", {})
            if name not in svcs:
                self._error(f"service '{name}' not found", 404)
                return

            # Kill and restart
            port = svcs[name].get("port", 0)
            start_script = None
            with open(SUPERVISOR_PY) as f:
                for line in f:
                    if f'"{name}"' in line:
                        m = __import__('re').search(r'"start":\s*"([^"]+)"', line)
                        if m: start_script = m.group(1)

            try:
                if port: subprocess.run(["fuser", "-k", f"{port}/tcp"],
                                        capture_output=True, timeout=5)
                time.sleep(1)
                if start_script and os.path.isfile(start_script):
                    subprocess.run(["bash", start_script], timeout=30,
                                   capture_output=True, text=True)
                    time.sleep(2)
                    alive = port_open(port=port) if port else False
                    self._json({"restarted": name, "alive": alive, "port": port})
                else:
                    self._json({"restarted": name, "alive": False,
                                "error": f"no start.sh for {name}"})
            except Exception as e:
                self._error(f"restart failed: {str(e)[:60]}")

        else:
            self._error(f"not found: {path}")

    def log_message(self, fmt, *args):
        pass  # тихо


def main():
    # Проверка дубля
    if os.path.isfile(SERVER_PIDFILE):
        with open(SERVER_PIDFILE) as f:
            old_pid = int(f.read().strip())
        try:
            os.kill(old_pid, 0)
            print(f"[L9] Already running (PID {old_pid})")
            return
        except OSError:
            pass

    with open(SERVER_PIDFILE, "w") as f:
        f.write(str(os.getpid()))

    print(f"[L9] Orchestration Layer :{PORT}")
    server = http.server.HTTPServer(("0.0.0.0", PORT), L9Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("[L9] Shutdown")
        if os.path.isfile(SERVER_PIDFILE): os.remove(SERVER_PIDFILE)


if __name__ == "__main__":
    main()

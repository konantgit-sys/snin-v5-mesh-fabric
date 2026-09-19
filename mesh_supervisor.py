#!/usr/bin/env python3
"""
SNIN Mesh Supervisor v1.0 — мониторинг и автоподъём mesh-ядра.

Стандарт SNIN_PORT_REGISTRY: supervisor → :9909
(порт 9900 занят graphify, supervisor перенесён на 9909).

Логика:
  - Каждые 15 сек проверяет живость каждого сервиса (TCP-порт или pgrep)
  - 2 consecutive fail → restart (subprocess.Popen, start_new_session)
  - Защита от дублей: перед запуском проверяет, что порт реально мёртв
  - PYTHONPATH=/home/agent/data/.local_lib — пакеты переживают рестарт пода
  - HTTP API: GET /health → {alive,total,dead,total_restarts}
             GET /status  → детально по сервисам
             POST /restart/{name} → рестарт конкретного сервиса
  - Лог: /home/agent/data/sites/relay-mesh/logs/mesh_supervisor.log
"""

import json
import logging
import os
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime

MESH_DIR = "/home/agent/data/sites/relay-mesh"
LOG_DIR = os.path.join(MESH_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "mesh_supervisor.log")
logging.basicConfig(
    filename=LOG_FILE, level=logging.INFO,
    format="%(asctime)s [supervisor] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

CHECK_INTERVAL = 15   # сек
MAX_FAILS = 2         # фейлов подряд до рестарта

# ─── Реестр сервисов (реальность, сверено с start.sh и ps 2026-09-02) ───
# type: "port" — проверка TCP-connect; "proc" — pgrep по сигнатуре
SERVICES = {
    "content_router": {
        "type": "port", "port": 9920,
        "cmd": ["python3", "-u", f"{MESH_DIR}/content_router_v2.py", "9920"],
        "log": f"{LOG_DIR}/cr_v2.log",
    },
    "route_engine": {
        "type": "port", "port": 9910,
        "cmd": ["python3", "-u", f"{MESH_DIR}/route_engine.py"],
        "log": f"{LOG_DIR}/route_engine.log",
    },
    "smart_router": {
        "type": "port", "port": 9932,
        "cmd": ["python3", "-u", f"{MESH_DIR}/smart_router.py"],
        "log": f"{LOG_DIR}/smart_router.log",
        "env": {"SNIN_USE_ZMQ": "0"},  # ZMQ выключен: pyzmq нет, порты 9960-9965 заняты релеями шардов (ZMQ, если включать, идёт в блок 9260-9269)
    },
    "external_gateway": {
        "type": "port", "port": 9931,
        "cmd": ["python3", "-u", f"{MESH_DIR}/external_gateway.py"],
        "log": f"{LOG_DIR}/external_gateway.log",
    },
    "cross_mesh": {
        "type": "port", "port": 9946,
        "cmd": ["python3", "-u", f"{MESH_DIR}/cross_mesh_bridge.py", "9946"],
        "log": f"{LOG_DIR}/cross_mesh.log",
    },
    "mesh_status": {
        "type": "port", "port": 8085,
        "cmd": ["python3", f"{MESH_DIR}/mesh_status.py"],
        "log": f"{LOG_DIR}/mesh_status.log",
    },
    "audit_daemon": {
        "type": "proc", "sig": "proof_mesh/audit_daemon",
        "cmd": ["python3", f"{MESH_DIR}/proof_mesh/audit_daemon.py"],
        "log": f"{LOG_DIR}/audit_daemon.log",
    },
}

# nostr_bridge шарды 0-4
for i in range(5):
    SERVICES[f"nostr_bridge_{i}"] = {
        "type": "port", "port": 9941 + i,
        "cmd": ["python3", "-u", f"{MESH_DIR}/nostr_bridge.py",
                "--shard-id", str(i), "--total-shards", "5"],
        "log": f"{LOG_DIR}/nostr_bridge_shard{i}.log",
    }

# ─── Состояние ───
state = {name: {"fails": 0, "restarts": 0, "alive": False,
                "last_check": None, "last_start": None}
         for name in SERVICES}


def port_alive(port: int, timeout: float = 1.0) -> bool:
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        s.close()
        return True
    except OSError:
        return False


def proc_alive(sig: str) -> bool:
    try:
        r = subprocess.run(["pgrep", "-f", sig], capture_output=True, timeout=3)
        return r.returncode == 0
    except Exception:
        return False


def is_alive(name: str) -> bool:
    svc = SERVICES[name]
    if svc["type"] == "port":
        return port_alive(svc["port"])
    return proc_alive(svc["sig"])


def start_service(name: str) -> bool:
    """Запустить сервис. Сначала double-check что он мёртв (защита от дублей)."""
    svc = SERVICES[name]
    if is_alive(name):
        return False  # уже жив — не плодим дубль
    env = os.environ.copy()
    env["PYTHONPATH"] = "/home/agent/data/.local_lib" + (
        ":" + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env.update(svc.get("env", {}))
    logf = open(svc["log"], "a")
    try:
        subprocess.Popen(
            svc["cmd"], cwd=MESH_DIR, env=env,
            stdout=logf, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        state[name]["last_start"] = datetime.now().isoformat(timespec="seconds")
        state[name]["restarts"] += 1
        logging.info(f"🔄 рестарт {name} (после {state[name]['fails']} фейлов)")
        return True
    except Exception as e:
        logging.error(f"❌ не удалось запустить {name}: {e}")
        return False


def check_loop():
    """Фоновый цикл мониторинга."""
    while True:
        for name in SERVICES:
            alive = is_alive(name)
            state[name]["alive"] = alive
            state[name]["last_check"] = datetime.now().isoformat(timespec="seconds")
            if alive:
                state[name]["fails"] = 0
            else:
                state[name]["fails"] += 1
                if state[name]["fails"] >= MAX_FAILS:
                    logging.info(f"⚠️ {name} мёртв ({state[name]['fails']} фейлов) — поднимаю")
                    start_service(name)
                    state[name]["fails"] = 0
        time.sleep(CHECK_INTERVAL)


def health_json() -> dict:
    total = len(SERVICES)
    alive = sum(1 for s in state.values() if s["alive"])
    dead = total - alive
    restarts = sum(s["restarts"] for s in state.values())
    return {"alive": alive, "total": total, "dead": dead,
            "total_restarts": restarts}


class Handler(BaseHTTPRequestHandler):
    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._json(health_json())
        elif self.path == "/status":
            detail = {}
            for name, s in state.items():
                svc = SERVICES[name]
                detail[name] = {
                    "alive": s["alive"],
                    "check": svc["type"],
                    "port": svc.get("port"),
                    "fails": s["fails"],
                    "restarts": s["restarts"],
                    "last_start": s["last_start"],
                    "cmd": " ".join(svc["cmd"][2:]),
                }
            self._json({"health": health_json(), "services": detail,
                        "ts": datetime.now().isoformat(timespec="seconds")})
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self):
        if self.path.startswith("/restart/"):
            name = self.path.split("/")[-1]
            if name not in SERVICES:
                self._json({"error": f"unknown service: {name}"}, 404)
                return
            started = start_service(name)
            self._json({"name": name, "restarted": started})
        else:
            self._json({"error": "not found"}, 404)

    def log_message(self, fmt, *args):  # тишина в stdout
        pass


def main():
    port = 9909
    # защита от дублей supervisor'а
    if port_alive(port):
        logging.error(f":{port} уже занят — supervisor уже запущен? Выход.")
        print(f"[supervisor] :{port} уже занят — выход (дубль)")
        return

    t = threading.Thread(target=check_loop, daemon=True)
    t.start()

    # первичная проверка: кто уже жив — зафиксировать; кто мёртв — поднять
    for name in SERVICES:
        alive = is_alive(name)
        state[name]["alive"] = alive
        if not alive:
            logging.info(f"старт: {name} не запущен — поднимаю")
            start_service(name)

    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    logging.info(f"Mesh Supervisor на :{port}, сервисов: {len(SERVICES)}")
    print(f"[supervisor] :{port} | сервисов: {len(SERVICES)}")
    srv.serve_forever()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""SNIN Watchdog — автоперезапуск критических сервисов."""

import os
import sys
import time
import socket
import subprocess
import json
from datetime import datetime

LOG_FILE = "/home/agent/data/sites/snin-hub/watchdog.log"
STATUS_FILE = "/home/agent/data/sites/snin-hub/watchdog_status.json"
CHECK_INTERVAL = 60  # секунд

SERVICES = [
    {
        "name": "relay_v2",
        "port": 8198,
        "description": "Nostr Relay v2 (основной)",
        "start_cmd": ["bash", "/home/agent/data/sites/relay/start.sh"],
        "health_url": "http://localhost:8198/nip11",
    },
    {
        "name": "relay_nostr",
        "port": 8443,
        "description": "Nostr Relay (websocket)",
        "start_cmd": [
            "bash", "-c",
            "cd /home/agent/data/sites/relay && python3 -m relay.server --port 8443 --host 0.0.0.0 >> /tmp/relay_server.log 2>&1 &"
        ],
    },
    {
        "name": "dao_dht",
        "port": 8082,
        "description": "P2P DAO / DHT слой",
        "start_cmd": ["bash", "-c",
            "cd /home/agent/data/p2p-agent-mesh && python3 dao_api.py 8082 >> /tmp/dao_dht.log 2>&1 &"
        ],
        "health_url": "http://localhost:8082/health",
    },
]


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def port_is_open(port, host="127.0.0.1"):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(3)
    try:
        s.connect((host, port))
        s.close()
        return True
    except:
        try:
            s.close()
        except:
            pass
        return False


def save_status(services):
    status = {
        "timestamp": datetime.now().isoformat(),
        "services": {},
    }
    for s in services:
        status["services"][s["name"]] = {
            "alive": s.get("alive", False),
            "last_check": s.get("last_check", ""),
            "restarts": s.get("restarts", 0),
        }
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f, indent=2)


def check_and_restart(service):
    name = service["name"]
    port = service["port"]
    alive = port_is_open(port)
    service["alive"] = alive
    service["last_check"] = datetime.now().isoformat()
    service.setdefault("restarts", 0)
    service.setdefault("consecutive_fails", 0)

    if alive:
        service["consecutive_fails"] = 0
        return True

    service["consecutive_fails"] += 1
    fails = service["consecutive_fails"]

    log(f"⚠️  {name} (порт {port}) НЕ ОТВЕЧАЕТ. Попытка #{fails}")

    if fails >= 2:  # restart after 2 consecutive fails
        log(f"🚑  Перезапуск {name}...")
        try:
            # Graceful shutdown перед убийством
            if name in ("relay_v2", "relay_nostr", "dao_dht"):
                subprocess.run(
                    ["python3", "/home/agent/data/graceful_shutdown.py"],
                    timeout=15, capture_output=True,
                )
                log(f"  graceful_shutdown вызван для {name}")
        except: pass

        try:
            # Kill old process on that port
            subprocess.run(
                ["bash", "-c", f"fuser -k {port}/tcp 2>/dev/null"],
                timeout=5,
            )
            time.sleep(1)
        except:
            pass

        try:
            result = subprocess.run(
                service["start_cmd"],
                timeout=30,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                service["restarts"] += 1
                service["consecutive_fails"] = 0
                log(f"✅  {name} перезапущен успешно (всего: {service['restarts']})")
                time.sleep(3)
                # Verify
                if port_is_open(port):
                    log(f"✅  {name} подтвердил работу на порту {port}")
                else:
                    log(f"⚠️  {name} запущен, но порт {port} ещё не слушается")
            else:
                log(f"❌  Ошибка перезапуска {name}: {result.stderr[:200]}")
        except Exception as e:
            log(f"❌  Исключение при перезапуске {name}: {e}")

        time.sleep(5)
        service["alive"] = port_is_open(port)
        return service["alive"]

    return False


def main():
    log("=" * 50)
    log("🛡️  SNIN Watchdog запущен")
    log(f"  Проверка каждые {CHECK_INTERVAL} сек")
    log(f"  Сервисов под наблюдением: {len(SERVICES)}")
    for s in SERVICES:
        log(f"    {s['name']} → порт {s['port']} ({s['description']})")
    log("=" * 50)

    cycles = 0
    while True:
        cycles += 1
        ts = datetime.now().strftime("%H:%M:%S")
        all_alive = True

        for svc in SERVICES:
            alive = check_and_restart(svc)
            if not alive:
                all_alive = False

        save_status(SERVICES)

        status_emoji = "✅" if all_alive else "⚠️"
        restarts_total = sum(s.get("restarts", 0) for s in SERVICES)
        log(f"[{ts}] Цикл #{cycles}: {status_emoji} | рестартов: {restarts_total}")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()

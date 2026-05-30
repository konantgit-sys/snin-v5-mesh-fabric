#!/usr/bin/env python3
"""
Relay Monitor V2 — проверяет релеи каждые N минут, пишет лог + статус.
Версия: V4
Дата: 2026-05-23

Использует: relay_list.txt (101 релей)
Публикует: relay_monitor_status.json (читается supervisor)
API: GET /api/v1/relays (через hub_api)
"""

import json
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime

# ─── Пути ───
BASE = os.path.dirname(__file__)
RELAY_LIST = os.path.join(BASE, "relay_list.txt")
LOG_DIR = os.path.join(BASE, "logs")
STATUS_FILE = "/home/agent/data/sites/snin-hub/relay_monitor_status.json"
PIDFILE = "/tmp/snin_relay_monitor.pid"

# ─── Конфигурация ───
CHECK_INTERVAL = 600  # 10 минут
DEAD_THRESHOLD = 3    # 3 раза подряд мёртв = critical
HTTP_TIMEOUT = 5
CONCURRENT = 10       # параллельных проверок

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [RELAY] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "relay_monitor.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("relay_monitor")


def load_relays() -> list:
    """Загрузить список релеев из relay_list.txt."""
    if not os.path.exists(RELAY_LIST):
        logger.warning(f"⚠️ {RELAY_LIST} не найден")
        return []
    with open(RELAY_LIST) as f:
        relays = []
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and line.startswith("wss://"):
                relays.append(line)
    return relays


def check_relay(url: str) -> tuple:
    """Проверить один релей."""
    host = url.replace("wss://", "").split("/")[0]
    try:
        r = subprocess.run(
            ["curl", "-s", "--max-time", str(HTTP_TIMEOUT),
             "-o", "/dev/null", "-w", "%{http_code}",
             "-H", "Accept: application/nostr+json",
             f"https://{host}/"],
            capture_output=True, text=True, timeout=HTTP_TIMEOUT + 3,
        )
        code = r.stdout.strip()
        alive = code in ("200", "403", "401")
        latency = None
        return (url, alive, code, None)
    except Exception as e:
        return (url, False, "timeout", str(e)[:40])


def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    
    relays = load_relays()
    if not relays:
        logger.error("❌ Нет релеев для проверки")
        return None
    
    # Загружаем состояние мёртвых
    dead_count = {}
    dead_file = os.path.join(LOG_DIR, "relay_dead.json")
    if os.path.exists(dead_file):
        try:
            with open(dead_file) as f:
                dead_count = json.load(f)
        except Exception:
            dead_count = {}
    
    logger.info(f"📡 Проверка {len(relays)} релеев...")
    
    # Проверяем последовательно (curl форкает процесс — параллельность не спасёт)
    results = []
    ok = fail = 0
    for url in relays:
        res = check_relay(url)
        results.append(res)
        if res[1]:
            ok += 1
            dead_count[url] = 0
        else:
            fail += 1
            dead_count[url] = dead_count.get(url, 0) + 1
    
    # Сводка
    entry = {
        "ts": datetime.now().isoformat(),
        "total": len(relays),
        "alive": ok,
        "dead": fail,
        "pct": round(ok / len(relays) * 100, 1) if relays else 0,
        "dead_relays": [r[0] for r in results if not r[1]],
    }
    
    # Лог
    log_file = os.path.join(LOG_DIR, "relay_monitor.jsonl")
    with open(log_file, "a") as f:
        f.write(json.dumps(entry) + "\n")
    
    # Сохраняем счётчики dead
    with open(dead_file, "w") as f:
        json.dump(dead_count, f)
    
    # Critical — 3+ раза подряд
    critical = [url for url, cnt in dead_count.items() if cnt >= DEAD_THRESHOLD]
    
    logger.info(f"  ✅ {ok}/{len(relays)} alive ({entry['pct']}%)")
    if fail > 0:
        logger.info(f"  ❌ Dead: {fail}")
    if critical:
        logger.warning(f"  🚨 CRITICAL: {len(critical)} relays dead {DEAD_THRESHOLD}+ times")
    
    # Сохраняем статус для supervisor
    status = {
        "timestamp": entry["ts"],
        "total": entry["total"],
        "alive": entry["alive"],
        "dead": entry["dead"],
        "pct": entry["pct"],
        "critical_dead": len(critical),
        "dead_list": entry["dead_relays"][:20],  # первые 20
        "check_interval": CHECK_INTERVAL,
    }
    os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f, indent=2)
    
    return entry


def run_loop():
    """Бесконечный цикл проверки."""
    logger.info("=" * 50)
    logger.info("🚀 Relay Monitor V2 запущен")
    logger.info(f"   Релеев: {len(load_relays())}")
    logger.info(f"   Интервал: {CHECK_INTERVAL // 60} мин")
    logger.info("=" * 50)
    
    # Первая проверка сразу
    main()
    
    while True:
        time.sleep(CHECK_INTERVAL)
        main()


if __name__ == "__main__":
    # PID file
    pid = str(os.getpid())
    with open(PIDFILE, "w") as f:
        f.write(pid)
    
    def _handle_signal(sig, frame):
        logger.info("👋 Завершение")
        if os.path.exists(PIDFILE):
            os.remove(PIDFILE)
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    
    if "--once" in sys.argv:
        main()
    else:
        run_loop()

#!/usr/bin/env python3
"""
Memory Guard v1.0 — защита от утечек памяти в контейнере 8 ГБ.

Логика:
  - Раз в 30 сек проверяет RSS всех критических процессов
  - При превышении порога → SIGTERM (graceful shutdown)
  - start.sh поднимет процесс заново
  - Логи в /home/agent/data/logs/memory_guard.log

Пороги (в MB RSS):
  nostr_bridge        300  — утекает быстрее всех
  relay_server_v2     400  — основной relay
  smart_router        200  — мозг маршрутизации
  route_engine        200  — маршруты
  cross_mesh_bridge   200  — меж-сетевой мост
  l2_* / l4_* / l6_*  150  — слои SNIN
  Остальные           200  — app.py, hub_api и т.д.
"""

import os
import re
import sys
import time
import signal
import logging
import subprocess
from datetime import datetime

# ─── Конфиг ───────────────────────────────────────────────────────────
CHECK_INTERVAL = 30       # секунд между проверками
PIDFILE = "/tmp/snin_memory_guard.pid"
LOG_FILE = "/home/agent/data/logs/memory_guard.log"

# Пороги: имя_процесса → max RSS в MB
# Максимум копий процессов (защита от размножения дубликатов)
MAX_INSTANCES = {
    "nostr_bridge": 6,  # максимум 1 мастер + 5 шардов
    "cross_mesh_bridge": 1,  # только 1 экземпляр
    "nip65_publisher": 1,
    "relay_monitor": 1,
}
# (имя берётся из /proc/<pid>/cmdline — достаточно частичного совпадения)
LIMITS = {
    "nostr_bridge": 500,
    "relay_server_v2": 400,
    "relay.server": 400,
    "smart_router": 200,
    "route_engine": 200,
    "cross_mesh_bridge": 200,
    "l2_encryption_layer": 150,
    "l2_transport_layer": 150,
    "l4_payment_layer": 150,
    "l4_privacy_layer": 150,
    "l6_agent_network": 150,
    "l3_zk_layer": 150,
    "l8_app_layer": 150,
    "identity_api": 150,
    "forecaster_dash": 150,
    "hub_api": 150,
    "gateway": 200,
    "dao_api": 200,
    "app.py": 200,
}

# Процессы, которые НЕ ТРОГАЕМ (системные)
EXEMPT = [
    "engine_c.py",       # движок VK/Telegram
    "api_server_v2.py",  # системный API ассистента
    "memory_guard",      # себя не трогаем
    "redis-server",
]

# ─── Логирование ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("memory_guard")


def get_all_processes() -> list[dict]:
    """Прочитать все процессы из /proc."""
    procs = []
    for pid_str in os.listdir("/proc"):
        if not pid_str.isdigit():
            continue
        try:
            pid = int(pid_str)
            with open(f"/proc/{pid}/status") as f:
                status = f.read()
            
            # VmRSS
            m = re.search(r"VmRSS:\s+(\d+)\s+kB", status)
            if not m:
                continue
            rss_kb = int(m.group(1))
            rss_mb = rss_kb // 1024
            
            # cmdline
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as f:
                    raw = f.read().replace(b"\x00", b" ").decode("utf-8", errors="replace")
            except:
                raw = ""
            
            if not raw.strip():
                continue
            
            procs.append({
                "pid": pid,
                "rss_kb": rss_kb,
                "rss_mb": rss_mb,
                "cmdline": raw.strip(),
            })
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
    
    return procs


def match_limit(cmdline: str, limits: dict) -> int | None:
    """Вернуть порог для процесса по cmdline, или None если exempt."""
    # Проверка exempt
    for ex in EXEMPT:
        if ex in cmdline:
            return None  # не трогать
    
    # Проверка лимитов (от более специфичных к общим)
    for name, limit in limits.items():
        if name in cmdline:
            return limit
    
    return None  # нет лимита → не контролируем


def kill_process(pid: int, cmdline: str, rss_mb: int, limit: int):
    """SIGTERM процессу с превышением. Если не умер за 5 сек → SIGKILL."""
    name = cmdline.split("/")[-1].split(" ")[0] if cmdline else f"pid:{pid}"
    log.warning(f"RSS {rss_mb}MB > {limit}MB — killing {name} (pid={pid})")
    log.warning(f"  CMD: {cmdline[:200]}")
    
    try:
        os.kill(pid, signal.SIGTERM)
        # Ждём 5 секунд
        for _ in range(5):
            time.sleep(1)
            try:
                os.kill(pid, 0)  # проверка жив ли
            except ProcessLookupError:
                log.info(f"  ✅ {name}({pid}) terminated gracefully")
                return
        # Если всё ещё жив — SIGKILL
        log.warning(f"  ⚠️ {name}({pid}) not responding to SIGTERM — SIGKILL")
        os.kill(pid, signal.SIGKILL)
        time.sleep(1)
        log.info(f"  ✅ {name}({pid}) killed with SIGKILL")
    except ProcessLookupError:
        log.info(f"  ✅ {name}({pid}) already dead")
    except Exception as e:
        log.error(f"  ❌ Error killing {pid}: {e}")


def main():
    log.info("=" * 60)
    log.info("Memory Guard v1.0 — STARTED")
    log.info("Limits: " + ", ".join(f"{k}={v}MB" for k, v in sorted(LIMITS.items())))
    log.info("Exempt: " + ", ".join(EXEMPT))
    log.info("=" * 60)
    
    while True:
        try:
            procs = get_all_processes()
            
            kills = []  # (pid, cmdline, rss_mb, limit)
            for p in procs:
                limit = match_limit(p["cmdline"], LIMITS)
                if limit is None:
                    continue
                if p["rss_mb"] > limit:
                    kills.append((p["pid"], p["cmdline"], p["rss_mb"], limit))
            
            # 🛡 Защита от дубликатов: если процесса > MAX_INSTANCES — убиваем лишние
            for name, max_count in MAX_INSTANCES.items():
                matched = [p for p in procs if name in p["cmdline"]]
                if len(matched) > max_count:
                    # Сортируем по RAM (большие = старые/утекшие) и убиваем лишние
                    excess = sorted(matched, key=lambda x: x["rss_mb"], reverse=True)[max_count:]
                    for ep in excess:
                        kill_process(ep["pid"], ep["cmdline"], ep["rss_mb"], 0)
                        log.warning(f"  🛡 Duplicate kill: {name} x{len(matched)} > {max_count} — killed PID {ep['pid']}")
            
            for pid, cmdline, rss_mb, limit in kills:
                kill_process(pid, cmdline, rss_mb, limit)
            
            # Краткий статус раз в 2 минуты (4 итерации)
            if int(time.time()) % 120 < CHECK_INTERVAL:
                total_mb = sum(p["rss_mb"] for p in procs)
                top = sorted(procs, key=lambda x: x["rss_mb"], reverse=True)[:5]
                top_str = "; ".join(f"{p['rss_mb']}MB {p['cmdline'][:60]}" for p in top)
                log.info(f"Status: {len(procs)} procs, ~{total_mb}MB total | Top: {top_str}")
            
            time.sleep(CHECK_INTERVAL)
        except KeyboardInterrupt:
            log.info("Memory Guard — STOPPED")
            break
        except Exception as e:
            log.error(f"Error in main loop: {e}")
            time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    # PID file
    try:
        with open(PIDFILE) as f:
            old_pid = int(f.read().strip())
            try:
                os.kill(old_pid, 0)
                print(f"Memory Guard already running (pid={old_pid})")
                sys.exit(0)
            except ProcessLookupError:
                pass
    except FileNotFoundError:
        pass
    
    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))
    
    main()

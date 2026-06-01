#!/usr/bin/env python3
"""
SNIN Supervisor v1.0 — управление всеми демонами сети.

Замена watchdog.py (который смотрел только 3 порта).
Теперь supervisor знает о ВСЕХ сервисах, их start.sh, port.txt.

Логика:
  - Каждые 15 сек проверяет порты всех сервисов
  - 2 consecutive fail → graceful_shutdown → restart через start.sh
  - Контроль дублей: pidfile в /tmp/snin_<name>.pid
  - Единый лог: /home/agent/data/logs/supervisor.log
  - Статус: /home/agent/data/sites/snin-hub/supervisor_status.json
"""

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime

LOG_DIR = "/home/agent/data/logs"
os.makedirs(LOG_DIR, exist_ok=True)

STATUS_FILE = "/home/agent/data/sites/snin-hub/supervisor_status.json"
PIDFILE = "/tmp/snin_supervisor.pid"
LOG_FILE = os.path.join(LOG_DIR, "supervisor.log")
GRACEFUL_SCRIPT = "/home/agent/data/graceful_shutdown.py"

CHECK_INTERVAL = 15  # сек
MAX_FAILS = 2         # попыток перед рестартом

# ─── RSS лимиты для memory guard (дублирующая защита) ───
RSS_LIMITS = {
    "nostr_bridge": 1024,
    "mesh_nostr_bridge_0": 1024,
    "mesh_nostr_bridge_1": 1024,
    "mesh_nostr_bridge_2": 1024,
    "mesh_nostr_bridge_3": 1024,
    "mesh_nostr_bridge_4": 1024,
    "relay_server_v2": 1024,
    "smart_router": 200,
    "mesh_smart_router": 200,
    "mesh_route_engine": 200,
    "mesh_content_router": 200,
    "mesh_external_gate": 200,
    "route_engine": 200,
    "cross_mesh_bridge": 200,
    "app.py": 200,
    "l2_encryption_layer": 150,
    "l2_transport_layer": 150,
    "l3_zk_layer": 150,
    "l4_payment_layer": 150,
    "l4_privacy_layer": 150,
    "l6_agent_network": 150,
    "l8_app_layer": 150,
    "identity_api": 150,
    "forecaster_dash": 150,
    "hub_api": 150,
    "dao_api": 200,
}

EXEMPT_NAMES = ["engine_c.py", "api_server_v2.py", "memory_guard", "redis-server", "supervisor"]

# ═══════════════════════════════════════════
# Конфигурация всех сервисов
# ═══════════════════════════════════════════

SERVICES = [
    # ─── Mesh Core (открытый стек) ───
    {"name": "mesh_simple_agent", "port": 9908, "start": "/home/agent/data/sites/mesh-agent-lite/start.sh", "critical": True},
    {"name": "mesh_relay",        "port": 8443, "start": None, "critical": True},  # часть start.sh

    # ─── Relay V2 (Nostr) ───
    {"name": "relay_v2",          "port": 8198, "start": "/home/agent/data/sites/relay/start.sh", "critical": True},
    {"name": "relay_frontend",    "port": 8086, "start": "/home/agent/data/sites/relay-dash/start.sh", "critical": False},

    # ─── P2P / DHT ───
    {"name": "p2p_dash",          "port": 8090, "start": "/home/agent/data/sites/p2p-dash/start.sh", "critical": False},
    {"name": "snin_dao",          "port": 9510, "start": "/home/agent/data/sites/relay-mesh/dao_mesh.sh", "critical": True},

    # ─── Mesh relay-mesh (V4 — 10 компонентов) ───
    # Каждый компонент запускается точечно (как watchdog safe_start),
    # а не через монолитный start.sh (чтобы не ронять живые при падении одного)
    {"name": "mesh_smart_router", "port": 9932,
     "cmd": "cd /home/agent/data/sites/relay-mesh && python3 -u smart_router.py",
     "critical": True},
    {"name": "mesh_route_engine", "port": 9910,
     "cmd": "cd /home/agent/data/sites/relay-mesh && python3 -u route_engine.py",
     "critical": True},
    {"name": "mesh_content_router","port": 9920,
     "cmd": "cd /home/agent/data/sites/relay-mesh && python3 -u content_router_v2.py 9920",
     "critical": True},
    {"name": "mesh_nostr_bridge_0","port": 9941,
     "cmd": "cd /home/agent/data/sites/relay-mesh && python3 -u nostr_bridge.py --shard-id 0 --total-shards 5",
     "critical": True},
    {"name": "mesh_nostr_bridge_1","port": 9942,
     "cmd": "cd /home/agent/data/sites/relay-mesh && python3 -u nostr_bridge.py --shard-id 1 --total-shards 5",
     "critical": True},
    {"name": "mesh_nostr_bridge_2","port": 9943,
     "cmd": "cd /home/agent/data/sites/relay-mesh && python3 -u nostr_bridge.py --shard-id 2 --total-shards 5",
     "critical": True},
    {"name": "mesh_nostr_bridge_3","port": 9944,
     "cmd": "cd /home/agent/data/sites/relay-mesh && python3 -u nostr_bridge.py --shard-id 3 --total-shards 5",
     "critical": True},
    {"name": "mesh_nostr_bridge_4","port": 9945,
     "cmd": "cd /home/agent/data/sites/relay-mesh && python3 -u nostr_bridge.py --shard-id 4 --total-shards 5",
     "critical": True},
    {"name": "mesh_external_gate","port": 9931,
     "cmd": "cd /home/agent/data/sites/relay-mesh && python3 -u external_gateway.py",
     "critical": True},
    {"name": "cross_mesh_bridge",  "port": 9946,
     "cmd": "cd /home/agent/data/sites/relay-mesh && python3 -u cross_mesh_bridge.py 9946",
     "critical": False},
    
    # ─── V4 Phase 4 — DAO Mesh :9510 + Science Mesh :9650 ───
    {"name": "science_mesh",      "port": 9650, "start": "/home/agent/data/sites/relay-mesh/science_mesh.sh", "critical": False},
    
    # ─── V4 Phase 6 — Smart City Mesh :9660 ───
    {"name": "city_mesh",         "port": 9660, "start": "/home/agent/data/sites/relay-mesh/city_mesh.sh", "critical": False},
    
    # ─── V4 Phase 7 — Trading Signal Mesh :9670 ───
    {"name": "trading_mesh",      "port": 9670, "start": "/home/agent/data/sites/relay-mesh/trading_mesh.sh", "critical": False},
    
    # ─── V4 Phase 8 — DeFi Oracle Mesh :9680 ───
    {"name": "defi_mesh",         "port": 9680, "start": "/home/agent/data/sites/relay-mesh/defi_mesh.sh", "critical": False},
    
    # ─── V4 Phase 9 — Crowdfunding :9690, Supply Chain :9720, Energy Grid :9710 ───
    {"name": "crowd_mesh",        "port": 9690, "start": "/home/agent/data/sites/relay-mesh/crowd_mesh.sh", "critical": False},
    {"name": "chain_mesh",        "port": 9720, "start": "/home/agent/data/sites/relay-mesh/chain_mesh.sh", "critical": False},
    {"name": "energy_mesh",       "port": 9710, "start": "/home/agent/data/sites/relay-mesh/energy_mesh.sh", "critical": False},

    # ─── AI Agents ───
    {"name": "identity_api",      "port": 9940, "start": "/home/agent/data/sites/identity-api/start.sh", "critical": False},

    # ─── Payments / DAO (V4 — cheque_book + verifier) ───
    {"name": "cheque_book",       "port": 9916, "start": "/home/agent/data/sites/relay-mesh/payments_start.sh", "critical": True},
    {"name": "verifier",          "port": 9915, "start": "/home/agent/data/sites/relay-mesh/payments_start.sh", "critical": True},
    {"name": "snin_pay",          "port": 8191, "start": "/home/agent/data/sites/snin-pay/start.sh", "critical": False},
    {"name": "snin_tracker",      "port": 8192, "start": "/home/agent/data/sites/snin-tracker/start.sh", "critical": False},
    {"name": "scc_agent",         "port": 8196, "start": "/home/agent/data/sites/scc-agent/start.sh", "critical": False},

    # ─── Gateway ───
    {"name": "api_gateway",       "port": 8083, "start": "/home/agent/data/sites/api-gateway/start.sh", "critical": True},
    {"name": "hub_api",           "port": 9950, "start": "/home/agent/data/sites/snin-hub/start.sh", "critical": True},

    # ─── L2.5 Encryption Layer ───
    {"name": "encryption_layer",   "port": 9600, "start": "/home/agent/data/sites/encryption-layer/start.sh", "critical": True},
    # ─── L2 Transport Layer ───
    {"name": "l2_transport",       "port": 9500, "start": "/home/agent/data/sites/l2-transport/start.sh", "critical": True},
    # ─── L8 Application Layer ───
    {"name": "app_layer",          "port": 9800, "start": "/home/agent/data/sites/app-layer/start.sh", "critical": True},
    # ─── L4.5 Privacy Layer ───
    {"name": "privacy_layer",      "port": 9700, "start": "/home/agent/data/sites/privacy-layer/start.sh", "critical": True},
    # ─── L3.5 ZK Layer ───
    {"name": "zk_layer",           "port": 9250, "start": "/home/agent/data/sites/zk-layer/start.sh", "critical": True},
    # ─── L6 Agent Network ───
    {"name": "l6_network",         "port": 9400, "start": "/home/agent/data/sites/l6-network/start.sh", "critical": True},
    # ─── L4 Payment Layer ───
    {"name": "l4_payment",        "port": 9200, "start": "/home/agent/data/sites/l4-payment/start.sh", "critical": True},

    # L9 Orchestration Layer
    {"name": "l9_orchestration",     "port": 9900, "start": "", "critical": True},
    # L3 Mesh Core Layer
    {"name": "l3_mesh_core",         "port": 9300, "start": "/home/agent/data/sites/snin-hub/l3_start.sh", "critical": True},
    # L1.5 Cross-Mesh Bridge
    {"name": "l1_5_bridge",        "port": 8202, "start": "/home/agent/data/sites/bridge/start.sh", "critical": False},
]



# ═══════════════════════════════════════════
# Supervisor
# ═══════════════════════════════════════════

class SNINSupervisor:
    def __init__(self):
        self.services = {s["name"]: dict(s, alive=False, fails=0, restarts=0,
                                          last_check="", pid=None) for s in SERVICES}
        self.running = True
        self.loop = None
        self._cleanup_done = False

    def log(self, msg):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(LOG_FILE, "a") as f:
            f.write(f"[{ts}] {msg}\n")
        print(f"[{ts}] {msg}", flush=True)

    # ─── Port check ───
    def port_open(self, host: str, port: int, timeout: float = 2) -> bool:
        try:
            s = socket.create_connection((host, port), timeout=timeout)
            s.close()
            return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False

    # ─── Health check (HTTP /health на порту+10000) ───
    def health_check(self, svc: dict) -> bool:
        if not svc.get("cmd"):
            return self.port_open("127.0.0.1", svc["port"])
        health_port = svc["port"] + 10000
        try:
            import http.client
            conn = http.client.HTTPConnection("127.0.0.1", health_port, timeout=2)
            conn.request("GET", "/health")
            resp = conn.getresponse()
            if resp.status == 200:
                data = json.loads(resp.read())
                return data.get("status") == "ok"
            return False
        except Exception:
            return self.port_open("127.0.0.1", svc["port"])

    # ─── Kill by port ───
    def kill_port(self, port: int):
        try:
            subprocess.run(["fuser", "-k", f"{port}/tcp"],
                           capture_output=True, timeout=5)
            time.sleep(1)
        except:
            pass

    # ─── Graceful shutdown перед рестартом ───
    def graceful_shutdown(self, name: str):
        # Только для критичных mesh сервисов
        critical_graceful = {"relay_v2", "mesh_api", "mesh_smart_router", "snin_dao"}
        if name in critical_graceful and os.path.isfile(GRACEFUL_SCRIPT):
            try:
                subprocess.run(["python3", GRACEFUL_SCRIPT],
                               timeout=15, capture_output=True)
                self.log(f"  graceful_shutdown вызван для {name}")
            except:
                pass

    # ─── Restart service ───
    def restart(self, name: str, svc: dict):
        # Если уже 20+ рестартов — ставим табу на вечные попытки
        if svc["restarts"] >= 20 and not svc.get("critical", False):
            self.log(f"  ⛔ {name} — превышен лимит рестартов (20), помечен как dead")
            svc["_dead"] = True
            return False

        port = svc["port"]
        cmd = svc.get("cmd")
        start_script = svc.get("start")

        self.log(f"🚑 Restart {name} (порт {port})...")

        # 1. Graceful shutdown
        self.graceful_shutdown(name)

        # 2. Kill old процесс на порту
        self.kill_port(port)

        # 3. Запуск
        if cmd:
            # Точечный рестарт (mesh-компоненты) — асинхронный, без ожидания завершения
            try:
                process = subprocess.Popen(
                    ["bash", "-c", cmd],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            except Exception as e:
                self.log(f"  ❌ {name} — Popen ошибка: {str(e)[:60]}")
                return False
            # Даём 5 секунд на открытие порта
            port_ok = False
            for _ in range(10):
                if self.health_check(svc) or self.port_open("127.0.0.1", port):
                    port_ok = True
                    break
                time.sleep(0.5)
            if port_ok:
                svc["restarts"] += 1
                svc["fails"] = 0
                self.log(f"  ✅ {name} — запущен (PID={process.pid}, рестартов: {svc['restarts']})")
                return True
            else:
                self.log(f"  ⚠️ {name} — запущен (PID={process.pid}) но порт не открыт за 5с — ждём health-check")
        elif start_script and os.path.isfile(start_script):
            # Монолитный рестарт через start.sh (для не-mesh сервисов)
            try:
                result = subprocess.run(
                    ["bash", start_script],
                    timeout=120, capture_output=True, text=True
                )
                if result.returncode == 0:
                    svc["restarts"] += 1
                    svc["fails"] = 0
                    self.log(f"  ✅ {name} — перезапущен (рестартов: {svc['restarts']})")
                    return True
                else:
                    self.log(f"  ❌ {name} — start.sh вернул {result.returncode}: {result.stderr[:100]}")
            except subprocess.TimeoutExpired:
                self.log(f"  ⏰ {name} — timeout при запуске")
            except Exception as e:
                self.log(f"  ❌ {name} — ошибка: {str(e)[:60]}")
        else:
            self.log(f"  ⚠️  {name} — нет ни cmd, ни start.sh")
        return False

    # ─── Check one service ───
    async def check_service(self, name: str, svc: dict):
        # Пропускаем мёртвые сервисы (превышен лимит рестартов)
        if svc.get("_dead"):
            if svc.get("alive"):
                svc["_dead"] = False  # воскрес
            else:
                return
        
        port = svc["port"]
        alive = self.health_check(svc)
        svc["alive"] = alive
        svc["last_check"] = datetime.now().isoformat()

        if alive:
            svc["fails"] = 0
            return

        svc["fails"] += 1
        self.log(f"⚠️  {name} (:{port}) — не отвечает (fail #{svc['fails']})")

        if svc["fails"] >= MAX_FAILS:
            await asyncio.get_event_loop().run_in_executor(None, self.restart, name, svc)
            svc["fails"] = 0  # сброс после попытки

    # ─── Save status (POST to hub + file fallback) ───
    def save_status(self):
        now = datetime.now().isoformat()
        ram_data = self._collect_ram()
        data = {
            "timestamp": now,
            "uptime_sec": int(time.time() - self.started_at),
            "total_services": len(self.services),
            "alive": sum(1 for s in self.services.values() if s["alive"]),
            "dead": sum(1 for s in self.services.values() if not s["alive"]),
            "total_restarts": sum(s["restarts"] for s in self.services.values()),
            "services": {n: {
                "alive": s["alive"],
                "port": s["port"],
                "restarts": s["restarts"],
                "fails": s["fails"],
                "last_check": s["last_check"],
                "ram_mb": ram_data.get(n, 0),
            } for n, s in self.services.items()}
        }
        # POST в Hub API
        try:
            import urllib.request
            req = urllib.request.Request(
                "http://127.0.0.1:9950/api/supervisor/status",
                data=json.dumps(data).encode(),
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            urllib.request.urlopen(req, timeout=3)
        except Exception:
            pass  # fallback на файл
        # Файл (всегда пишем)
        os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)
        with open(STATUS_FILE, "w") as f:
            json.dump(data, f, indent=2)

    def _collect_ram(self):
        """Собрать RAM (RSS) для каждого сервиса через ps aux."""
        import subprocess, re
        try:
            result = subprocess.run(
                ["ps", "aux", "--sort=-%mem"],
                capture_output=True, text=True, timeout=5
            )
            ps_lines = result.stdout.split('\n')
        except Exception:
            return {n: 0 for n in self.services}
        
        ram = {n: 0 for n in self.services}
        
        # For each service, find the matching process
        for name in self.services:
            # Build keywords to match in ps output
            keywords = [name]
            # Also try common variations
            if name.endswith('_mesh'):
                keywords.append(name.replace('_mesh', ''))
            if name.startswith('l'):
                keywords.append(name.replace('_', ''))
            
            for line in ps_lines:
                if not line.strip() or 'PID' in line:
                    continue
                # Check if any keyword matches
                for kw in keywords:
                    if kw in line:
                        parts = line.split()
                        if len(parts) >= 6:
                            try:
                                rss_kb = int(parts[5])  # RSS column in ps aux
                                if rss_kb > ram.get(name, 0):
                                    ram[name] = rss_kb // 1024  # kB → MB
                            except ValueError:
                                pass
                        break
        return ram

    # ─── Summary ───
    def summary(self):
        alive = sum(1 for s in self.services.values() if s["alive"])
        total = len(self.services)
        restarts = sum(s["restarts"] for s in self.services.values())
        dead_list = [n for n, s in self.services.items() if not s["alive"]]
        line = f"📊 {alive}/{total} alive | {restarts} restarts"
        if dead_list:
            line += f" | 💀 {', '.join(dead_list[:5])}"
            if len(dead_list) > 5:
                line += f" +{len(dead_list)-5}"
        return line

    # ─── Signal handler ───
    def stop(self):
        self.log("🛑 Supervisor получил сигнал остановки")
        self.running = False
        self.save_status()

    async def _rss_check(self):
        """Проверить RSS всех процессов, превысивших лимит, и убить их."""
        import re
        for pid_str in os.listdir("/proc"):
            if not pid_str.isdigit():
                continue
            try:
                pid = int(pid_str)
                with open(f"/proc/{pid}/status") as f:
                    status = f.read()
                m = re.search(r"VmRSS:\s+(\d+)\s+kB", status)
                if not m:
                    continue
                rss_kb = int(m.group(1))
                rss_mb = rss_kb // 1024

                try:
                    with open(f"/proc/{pid}/cmdline", "rb") as f:
                        raw = f.read().replace(b"\x00", b" ").decode("utf-8", errors="replace")
                except:
                    continue

                if not raw.strip():
                    continue

                # Проверка exempt
                if any(x in raw for x in EXEMPT_NAMES):
                    continue

                # Проверка лимитов
                for name, limit in RSS_LIMITS.items():
                    if name in raw and rss_mb > limit:
                        self.log(f"⛔ RSS {rss_mb}MB > {limit}MB — killing {name} (pid={pid})")
                        try:
                            os.kill(pid, signal.SIGTERM)
                        except ProcessLookupError:
                            pass
                        break
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                continue

    # ─── Main loop ───
    async def run(self):
        self.started_at = time.time()
        self.log("=" * 50)
        self.log("🚀 SNIN Supervisor v1.0 запущен")
        self.log(f"   Сервисов под наблюдением: {len(self.services)}")
        self.log(f"   Интервал проверки: {CHECK_INTERVAL} сек")
        self.log("=" * 50)

        # Первая проверка сразу
        for name, svc in self.services.items():
            alive = self.health_check(svc)
            svc["alive"] = alive
            icon = "🟢" if alive else "🔴"
            self.log(f"  {icon} {name} (:{svc['port']})")

        self.save_status()
        self.log(f"\n{self.summary()}\n")

        # Цикл проверок
        while self.running:
            await asyncio.sleep(CHECK_INTERVAL)
            for name, svc in self.services.items():
                await self.check_service(name, svc)
            await self._rss_check()   # проверка утечек памяти
            self.save_status()
            self.log(self.summary())


def main():
    # Проверка дубля
    if os.path.isfile(PIDFILE):
        with open(PIDFILE) as f:
            old_pid = int(f.read().strip())
        try:
            os.kill(old_pid, 0)
            print(f"❌ Supervisor уже запущен (PID {old_pid})")
            sys.exit(1)
        except OSError:
            os.remove(PIDFILE)

    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))

    sup = SNINSupervisor()

    def signal_handler(sig, frame):
        sup.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(sup.run())
    except KeyboardInterrupt:
        sup.stop()
    finally:
        if os.path.isfile(PIDFILE):
            os.remove(PIDFILE)
        sup.save_status()
        sup.log("👋 Supervisor остановлен")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
SNIN Daemon Collector v2 — сбор ВСЕХ процессов сервера с категоризацией
Вызывается из hub_fastapi через /api/daemons
"""
import os, time, pwd, json, subprocess
from pathlib import Path

SNIN_HOME = "/home/agent/data"

# Категории процессов с ключевыми словами
CATEGORIES = {
    "ai_agents": {
        "emoji": "🧠",
        "label": "AI Agents",
        "keywords": [
            "cryter_v10_daemon", "cryter_pulse.py", "nostr_auto_reply",
            "daemon_v3.py", "creator_agent", "archivist", "forecaster"
        ]
    },
    "snin": {
        "emoji": "🧩",
        "label": "SNIN Core",
        "keywords": [
            "hub_fastapi", "hub_ws", "simple_agent", "supervisor", "memory_guard",
            "l1_5_bridge", "l2_transport", "l2_encryption", "l3_mesh_core", "l3_zk",
            "l4_payment", "l4_privacy", "l6_agent", "l8_app", "l9_orchestr",
            "dao_api", "snin_pay", "api_gateway", "identity_api",
            "p2p-agent-mesh", "secret_invite"
        ]
    },
    "relay": {
        "emoji": "📡",
        "label": "Relay / Mesh",
        "keywords": [
            "relay_server", "relay_health", "relay-mesh", "nostr_bridge",
            "smart_router", "content_router", "cross_mesh", "relay_mesh_api"
        ]
    },
    "backend": {
        "emoji": "⚙️",
        "label": "Backend / Bots",
        "keywords": [
            "backend/app.py", "agent_scheduler", "cryter_v7_dash",
            "notex", "brain"
        ]
    },
    "site": {
        "emoji": "🌐",
        "label": "Sites / Apps",
        "keywords": [
            "sites/", "webapp"
        ]
    },
    "system": {
        "emoji": "🖥",
        "label": "System",
        "keywords": [
            "dockerd", "containerd", "sshd", "cron", "systemd", "rsyslog"
        ]
    }
}

KNOW_APPS = {
    "relay_server_v2.py": {"name":"📡 Relay Server","desc":"Релейный сервер — маршрутизация сообщений между нодами и ретрансляция пакетов"},
    "relay_health_daemon.py": {"name":"❤️ Relay Health","desc":"Мониторинг здоровья релейной сети — пинг нод, сбор метрик доступности"},
    "nostr_bridge.py": {"name":"🌉 Nostr Bridge","desc":"Мост к Nostr — публикация/подписка на децентрализованные события (shard)"},
    "smart_router.py": {"name":"🧭 Smart Router","desc":"Умный маршрутизатор — выбор оптимального пути для сообщений в mesh"},
    "content_router_v2.py": {"name":"📨 Content Router","desc":"Маршрутизация контента по шардам — распределение нагрузки"},
    "cross_mesh_bridge.py": {"name":"🔄 Cross-Mesh Br","desc":"Кросс-мост между mesh-кластерами — объединение независимых сетей"},
    "hub_fastapi.py": {"name":"📡 Hub API","desc":"Основное API хаба — точка входа REST API SNIN"},
    "mesh_light.py": {"name":"🔗 Hub WS (legacy)","desc":"WebSocket лёгкий хаб — для обратной совместимости"},
    "simple_agent": {"name":"🤖 Simple Agent","desc":"Базовый p2p-агент — точка входа в децентрализованную сеть SNIN"},
    "supervisor.py": {"name":"👁 Supervisor","desc":"Супервизор процессов — мониторинг и автоперезапуск системных демонов"},
    "memory_guard.py": {"name":"🧠 Memory Guard","desc":"Страж памяти — мониторинг утечек и защита от переполнения RAM"},
    "l1_5_bridge.py": {"name":"🌉 L1.5 Bridge","desc":"L1.5 Bridge — связующее звено между уровнями SNIN"},
    "l2_transport_layer.py": {"name":"📦 L2 Transport","desc":"Транспортный уровень L2 — шифрованная передача данных"},
    "l2_encryption_layer.py": {"name":"🔐 L2 Encryption","desc":"Уровень шифрования L2 — защита каналов связи"},
    "l3_mesh_core.py": {"name":"🔀 L3 Mesh Core","desc":"Ядро mesh-сети L3 — управление топологией и маршрутизацией"},
    "l3_zk_layer.py": {"name":"🔬 L3 ZK Layer","desc":"Zero-Knowledge уровень L3 — доказательства с нулевым разглашением"},
    "l4_payment_layer.py": {"name":"💰 L4 Payment","desc":"Платежный слой L4 — обработка транзакций SNIN"},
    "l4_privacy_layer.py": {"name":"🛡 L4 Privacy","desc":"Уровень приватности L4 — анонимизация транзакций"},
    "l6_agent_network.py": {"name":"🤖 L6 Agent Net","desc":"Сеть агентов L6 — координация и коммуникация AI-агентов"},
    "l8_app_layer.py": {"name":"📱 L8 App Layer","desc":"Прикладной уровень L8 — пользовательские приложения"},
    "l9_orchestration.py": {"name":"🎯 L9 Orchestr","desc":"Оркестратор L9 — управление жизненным циклом процессов"},
    "dao_api.py": {"name":"🗳 DAO API","desc":"DAO API — голосования, управление сообществом SNIN"},
    "snin_pay.py": {"name":"💳 SNIN Pay","desc":"SNIN Pay — платёжный шлюз для токена SNIN"},
    "api_gateway.py": {"name":"🚪 API Gateway","desc":"API Gateway — единая точка входа для внешних запросов"},
    "identity_api_v2.py": {"name":"🆔 Identity API","desc":"Identity API — управление идентификацией и ключами"},
    "frontend.py": {"name":"🖥 Relay Frontend","desc":"Фронтенд Relay — веб-интерфейс релейной сети"},
    "relay_mesh_api.py": {"name":"🌐 Relay Mesh API","desc":"API Relay Mesh — управление mesh-инфраструктурой"},
    "forecaster_dash.py": {"name":"📊 Forecaster","desc":"Forecaster — AI-прогнозирование метрик сети"},
    "agent_scheduler.py": {"name":"⏱ Agent Scheduler","desc":"Планировщик задач — распределение нагрузки агентов"},
    "app.py": {"name":"📱 App","desc":"Приложение — пользовательское приложение/сервер"},
    "gossip_shard.py": {"name":"📣 Gossip Shard","desc":"Шард gossip-протокола — распространение информации в mesh"},
    "bot.py": {"name":"🤖 Bot","desc":"Telegram бот — пользовательский интерфейс в Telegram"},
    "engine_c.py": {"name":"⚡ Engine C","desc":"Engine C — основной движок обработки запросов"},
    "dashboard.py": {"name":"📊 Dashboard","desc":"Дашборд — веб-панель мониторинга метрик"},
    "relay_mesh_gossip.py": {"name":"📣 Mesh Gossip","desc":"Gossip протокол mesh — синхронизация состояния нод"},
    "route_engine.py": {"name":"🛣 Route Engine","desc":"Маршрутизатор — вычисление путей в mesh-сети"},
    "scheduler_daemon.py": {"name":"⏰ Scheduler","desc":"Планировщик — запуск задач по расписанию"},
    "external_gateway.py": {"name":"🚪 Ext Gateway","desc":"Внешний шлюз — интеграция с внешними сетями"},
    "chequebook.py": {"name":"📒 ChequeBook","desc":"Чековая книга — учёт и верификация платежей"},
    "verifier.py": {"name":"✅ Verifier","desc":"Верификатор — проверка подлинности транзакций"},
    # AI Agents
    "cryter_v10_daemon.py": {"name":"🤖 Cryter V10","desc":"Cryter V10 — AI-агент контента (цикл #N, граф знаний, автономный)"},
    "cryter_pulse.py": {"name":"💓 Pulse","desc":"Pulse — пульс Cryter, публикация коротких постов EN+RU каждые 12-49 мин"},
    "nostr_auto_reply.py": {"name":"💬 Nostr Auto Reply","desc":"Auto Reply — авто-ответы на комментарии в Nostr (87/87 answered)"},
    "daemon_v3.py": {"name":"🧠 Creator Agent","desc":"Creator — мозг сети SNIN, наблюдает и публикует мысли в @octopus_valet"},
}

# Описания для OTHER процессов по ключевым словам
OTHER_DESCRIPTIONS = {
    "api_server": {"name":"🌐 API Server", "desc":"API сервер — REST/WebSocket сервер для внешних запросов"},
    "cryter/dashboard": {"name":"🤖 Cryter Dash", "desc":"Cryter Dashboard — старая панель управления Cryter"},
    "cryter": {"name":"🤖 Cryter", "desc":"Cryter — AI-агент контента и аналитики"},
    "archivist": {"name":"🗄 Archivist", "desc":"Archivist — AI-архивариус, анализ исторических данных"},
    "archivist-ai": {"name":"🗄 Archivist AI", "desc":"Archivist AI — веб-интерфейс архивариуса"},
    "brain": {"name":"🧠 SNIN Brain", "desc":"SNIN Brain — RAG база знаний агентов"},
    "chrono": {"name":"⏳ Chrono", "desc":"Chrono — временная БД и управление временными рядами"},
    "snin-brain": {"name":"🧠 SNIN Brain", "desc":"SNIN Brain — AI база знаний для принятия решений"},
    "p2p-dash": {"name":"🔗 P2P Dashboard", "desc":"P2P Dashboard — мониторинг p2p-сети SNIN"},
    "scc-agent": {"name":"🤖 SCC Agent", "desc":"SCC Agent — AI-агент для умных контрактов"},
    "snin-launch": {"name":"🚀 SNIN Launch", "desc":"SNIN Launch — лаунчер для развертывания сервисов"},
    "snin-network": {"name":"🌐 SNIN Network", "desc":"SNIN Network — веб-интерфейс сети SNIN"},
    "snin-tracker": {"name":"📡 SNIN Tracker", "desc":"SNIN Tracker — отслеживание метрик сети"},
    "upload": {"name":"📤 Upload Site", "desc":"Upload Site — сервис загрузки файлов"},
    "snin-command": {"name":"📟 SNIN Command", "desc":"SNIN Command — бэкенд командного управления"},
    "relay-mesh/routes": {"name":"🛣 Routes", "desc":"Маршрутизация mesh — обновление таблиц маршрутизации"},
    "engine_c.py": {"name":"⚡ Engine C", "desc":"Engine C — основной движок обработки запросов V2Bot"},
    "dashboard.py": {"name":"📊 Dashboard", "desc":"Дашборд — веб-панель мониторинга метрик"},
    "cryter_v7": {"name":"🤖 Cryter Dash", "desc":"Cryter Dashboard — панель управления Cryter AI"},
    # AI Agents — отдельная категория
    "cryter_v10": {"name":"🤖 Cryter V10","desc":"Cryter V10 — AI-агент контента (цикл #N, 101 релей, граф знаний)"},
    "cryter_pulse": {"name":"💓 Pulse","desc":"Pulse — пульс Cryter, публикация коротких постов EN+RU каждые 12-49 мин"},
    "nostr_auto_reply": {"name":"💬 Nostr Auto Reply","desc":"Auto Reply — авто-ответы на комментарии в Nostr (87/87)"},
    "daemon_v3": {"name":"🧠 Creator Agent","desc":"Creator — мозг сети SNIN, публикует мысли в @octopus_valet каждые 30 мин"},
}

def get_username():
    return pwd.getpwuid(os.getuid()).pw_name

def classify_process(cmdline, cwd=""):
    """Классифицировать процесс по категории"""
    cl = cmdline.lower()
    for cat, info in CATEGORIES.items():
        for kw in info["keywords"]:
            if kw.lower() in cl:
                return cat
    # Проверка по CWD только для app.py (у которых скрипт не содержит категорийных ключей)
    if cwd and ('/app.py' in cl or ' app.py' in cl):
        cwd_lower = cwd.lower()
        for cat, info in CATEGORIES.items():
            for kw in info["keywords"]:
                if kw.lower() in cwd_lower:
                    return cat
    return "other"

def get_readable_name(cmdline, cwd=""):
    """Получить читаемое имя процесса + описание"""
    # Для app.py — сначала проверяем CWD в OTHER_DESCRIPTIONS
    if 'app.py' in cmdline and cwd:
        cwd_lower = cwd.lower()
        for kw, info in OTHER_DESCRIPTIONS.items():
            if kw in cwd_lower:
                return info["name"], info["desc"]
    
    for key, val in KNOW_APPS.items():
        if key in cmdline or key.replace('.py','') in cmdline:
            return val["name"], val.get("desc","")
    # Из пути: последний значимый элемент
    parts = cmdline.split()
    if len(parts) >= 2 and parts[0].endswith('python3'):
        # Пропускаем флаги (-u, -m, -c) после python3
        script_idx = 1
        while script_idx < len(parts) and parts[script_idx].startswith('-'):
            script_idx += 1
        script = parts[script_idx] if script_idx < len(parts) else parts[-1]
        name = script.split('/')[-1]
        # Для python3 -m module.name — извлекаем имя модуля
        if '-m' in parts:
            m_idx = parts.index('-m')
            if m_idx + 1 < len(parts):
                name = parts[m_idx + 1].split('.')[-1] + ' (module)'
        # Проверяем OTHER_DESCRIPTIONS по cmdline
        for kw, info in OTHER_DESCRIPTIONS.items():
            if kw in cmdline.lower():
                return info["name"], info["desc"]
        # Проверяем по CWD
        if cwd:
            cwd_lower = cwd.lower()
            for kw, info in OTHER_DESCRIPTIONS.items():
                if kw in cwd_lower:
                    return info["name"], info["desc"]
        return name[:40], ""
    # Из пути: последний значимый элемент (не python)
    for kw, info in OTHER_DESCRIPTIONS.items():
        if kw in cmdline.lower():
            return info["name"], info["desc"]
    base = parts[0].split('/')[-1][:40] if parts else "?"
    return base, ""

def get_port(cmdline, parts=None):
    """Извлечь порт из cmdline"""
    if parts is None:
        parts = cmdline.split()
    port = 0
    
    # --port N
    if "--port" in cmdline:
        for i, p in enumerate(parts):
            if p == "--port" and i+1 < len(parts):
                try: return int(parts[i+1])
                except: pass
    
    # python3 script.py PORT
    for i, p in enumerate(parts):
        if p.endswith('.py') and i+1 < len(parts):
            try:
                c = int(parts[i+1])
                if 1024 <= c <= 65535 and c not in (8443, 8080, 8081):
                    return c
            except: pass
    
    # Любое число 1024-65535
    for p in parts:
        try:
            n = int(p)
            if 1024 <= n <= 65535:
                port = n
        except: pass
    
    return port

def collect_processes():
    """Собрать ВСЕ процессы сервера с категоризацией"""
    processes = []
    group_counts = {}
    group_ram = {}
    seen_readable = {}
    
    try:
        for pid_dir in Path("/proc").iterdir():
            if not pid_dir.name.isdigit():
                continue
            pid = pid_dir.name
            
            try:
                cmdline = (pid_dir / "cmdline").read_text().replace("\0", " ").strip()
                if not cmdline or len(cmdline) < 5:
                    continue
                
                # Только python3, node, dockerd, containerd — пользовательские процессы
                if not any(x in cmdline for x in ["python3", "node ", "dockerd", "containerd"]):
                    continue
                
                # Пропускаем системные python (apt, snap, pip)
                if any(x in cmdline for x in ["/usr/lib/python3", "/snap/", "dpkg", "apt-get", "pip3"]):
                    continue
                
                status = (pid_dir / "status").read_text()
                rss_kb = 0
                for line in status.split("\n"):
                    if line.startswith("VmRSS:"):
                        rss_kb = int(line.split()[1])
                        break
                if rss_kb < 512:  # меньше 0.5 MB — не интересно
                    continue
                
                parts = cmdline.split()
                
                # Рабочая директория процесса — для разных app.py в разных папках
                try:
                    cwd = os.readlink(f"/proc/{pid}/cwd") or ""
                except:
                    cwd = ""
                
                readable, desc = get_readable_name(cmdline, cwd)
                port = get_port(cmdline, parts)
                group = classify_process(cmdline, cwd)
                
                # CPU
                cpu = 0.0
                try:
                    stat = (pid_dir / "stat").read_text()
                    sp = stat.split()
                    utime = int(sp[13]); stime = int(sp[14])
                    total = utime + stime
                    if total > 0:
                        uptime = float(open("/proc/uptime").read().split()[0])
                        cpu = round(total / (uptime * 100) * 100, 1)
                except: pass
                
                # Duplicate detection — по пути скрипта + порту + CWD (не по readable имени!)
                # Разные app.py в разных папках — НЕ дубликаты
                # Одинаковый скрипт на одном порту — дубликат
                script_path = ""
                for i, p in enumerate(parts):
                    if p.endswith('.py') or p.endswith('.pyc'):
                        script_path = p
                        break
                if not script_path:
                    script_path = parts[-1] if parts else ""
                # Исключаем spawn_main (multiprocessing воркеры) — они не дубликаты
                if 'spawn_main' in cmdline or 'multiprocessing.' in cmdline or 'resource_tracker' in cmdline:
                    is_duplicate = False
                else:
                    # Для bridge с --shard-id — включаем shard в ключ (разные шарды ≠ дубликаты)
                    shard_match = __import__('re').search(r'--shard-id\s+(\d+)', cmdline)
                    shard_suffix = ":" + shard_match.group(1) if shard_match else ""
                    dup_key = group + ":" + script_path + ":" + str(port) + ":" + cwd + shard_suffix
                    prev = seen_readable.get(dup_key, [])
                    is_duplicate = len(prev) > 0
                prev.append(pid)
                seen_readable[dup_key] = prev
                
                processes.append({
                    "pid": int(pid),
                    "name": readable,
                    "desc": desc,
                    "cwd": cwd,
                    "cmd": cmdline[:120],
                    "port": port,
                    "rss_mb": round(rss_kb / 1024, 1),
                    "cpu": cpu,
                    "is_duplicate": is_duplicate,
                    "group": group,
                })
                
                # Собираем статистику
                group_counts[group] = group_counts.get(group, 0) + 1
                group_ram[group] = group_ram.get(group, 0) + round(rss_kb / 1024, 1)
                
            except (FileNotFoundError, PermissionError, ValueError):
                continue
    except Exception as e:
        return {"error": str(e), "processes": []}
    
    # Сортировка по группе
    group_order = {"system":0,"snin":1,"relay":2,"backend":3,"site":4,"other":5}
    processes.sort(key=lambda p: (group_order.get(p["group"], 99), -p["rss_mb"]))
    
    # Собираем breakdown
    breakdown = {}
    for cat, info in CATEGORIES.items():
        breakdown[cat] = {
            "count": group_counts.get(cat, 0),
            "ram_mb": round(group_ram.get(cat, 0), 1),
            "emoji": info["emoji"],
            "label": info["label"]
        }
    # Other
    breakdown["other"] = {
        "count": group_counts.get("other", 0),
        "ram_mb": round(group_ram.get("other", 0), 1),
        "emoji": "📦",
        "label": "Other"
    }
    
    total_ram = round(sum(p["rss_mb"] for p in processes), 1)
    
    return {
        "processes": processes,
        "total": len(processes),
        "total_ram_mb": total_ram,
        "breakdown": breakdown,
        "collected_at": time.time()
    }

def collect_libs():
    """Собрать все установленные библиотеки через pip3"""
    libs = {}
    total = 0
    try:
        r = subprocess.run(["pip3", "list", "--format=json"], capture_output=True, text=True, timeout=5)
        packages = json.loads(r.stdout)
        for p in packages:
            name = p.get("name", "")
            version = p.get("version", "")
            if name and version:
                libs[name.lower()] = version
        total = len(packages)
    except:
        libs = {"error": "can't read packages"}
        total = 0
    
    # Топ RAM библиотек — размер папок site-packages
    top_ram = {}
    try:
        sp = Path("/home/agent/.local/lib/python3.11/site-packages")
        if sp.exists():
            for d in sorted(sp.iterdir()):
                if d.is_dir():
                    size = sum(f.stat().st_size for f in d.rglob('*') if f.is_file()) / 1024 / 1024
                    if size > 0.5:
                        top_ram[d.name] = round(size, 1)
        top_ram = dict(sorted(top_ram.items(), key=lambda x: -x[1])[:10])
    except: pass
    
    return {"libs": libs, "total": total, "top_ram_mb": top_ram}

def collect_caches():
    """Собрать информацию о БД и кэшах"""
    dbs = []
    log_sizes = {}
    
    for f in Path(SNIN_HOME).rglob("*.db"):
        if any(x in str(f).lower() for x in ["cache", "tmp", "backup", "mypy", "mypy_cache"]):
            continue
        size = f.stat().st_size
        if size > 1024:  # > 1 KB
            dbs.append({"path": str(f).replace(SNIN_HOME, "~"), "size_mb": round(size/1024/1024, 1)})
    
    dbs.sort(key=lambda x: x["size_mb"], reverse=True)
    
    # Логи
    for log_dir_name in ["snin-hub", "cryter", "relay", "relay-mesh"]:
        log_dir = Path(SNIN_HOME) / "sites" / log_dir_name
        if log_dir.exists():
            for f in log_dir.glob("*.log"):
                log_sizes[log_dir_name + "/" + f.name] = round(f.stat().st_size / 1024, 1)
    
    return {"databases": dbs[:20], "total_db_size_mb": round(sum(d["size_mb"] for d in dbs), 1), 
            "logs": log_sizes}

if __name__ == "__main__":
    import json
    data = {
        "libs": collect_libs(),
        "caches": collect_caches()
    }
    procs = collect_processes()
    data["processes"] = procs
    print(json.dumps(data, indent=2, ensure_ascii=False))

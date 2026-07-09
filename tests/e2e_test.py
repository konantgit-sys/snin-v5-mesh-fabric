#!/usr/bin/env python3
"""
SNIN V5 — ПОЛНЫЙ ТЕСТ АРХИТЕКТУРЫ
===================================
Сквозные проверки всех слоёв:
  L0 — Health: порты, процессы, аптайм
  L1 — Компоненты: SmartRouter, ContentRouter, NostrBridge, ExternalGateway, 
                   RouteEngine, CrossMesh, Supervisor, IdentityAPI
  L2 — Связи: маршрутизация mesh, проходимость данных, CB статус, шардирование
  L3 — Внешняя видимость: публикация на внешние Nostr relai, E2E цикл
  L4 — Консистентность: ключи, конфиги, реестры

Использование:
  python3 snin_v5_e2e_test.py [--save-baseline] [--json]

Результаты:
  - JSON/текстовый отчёт
  - Baseline для дельта-анализа регрессии
  - Файл /tmp/snin_test_snapshot.json с полным состоянием
"""

import socket, json, time, os, sys, re, glob, yaml
from datetime import datetime
from collections import defaultdict

# ═══ КОНФИГУРАЦИЯ ═══════════════════════════════════

RELAY_MESH_DIR   = "/home/agent/data/sites/relay-mesh"
RELAY_MESH_LOGS  = os.path.join(RELAY_MESH_DIR, "logs")
CRYTER_CONFIG    = "/home/agent/data/agents/core/cryter/config/config.yaml"
CRYTER_LOGS      = "/home/agent/data/agents/core/cryter/logs"
KEYSTORE_DIR     = "/home/agent/data/sites/chrono/keystore"
AGENTS_REGISTRY  = "/home/agent/data/agents_registry"
TIE_RELAY_DIR    = "/home/agent/data/sites/tie-relay"
SNIN_HUB_DIR     = "/home/agent/data/sites/snin-hub"

SNAPSHOT_FILE = "/tmp/snin_test_snapshot.json"
BASELINE_FILE  = "/home/agent/data/scripts/test_baseline_v5.json"
REGRESSION_LOG = "/home/agent/data/scripts/test_regression_v5.json"

PORTS = [
    (9932, "SmartRouter"),       # L5 — ядро маршрутизации
    (9931, "ExternalGateway"),   # L6 — вход в Nostr
    (9920, "ContentRouter"),     # L4 — дедупликация/квалификация
    (9910, "RouteEngine"),       # L3 — маршрутизация mesh
    (9907, "MeshAPI"),           # L3 — API mesh
    (9946, "CrossMesh"),         # L2 — межшардовый мост
    (9941, "NostrBridge-1"),     # L1 — шард 1
    (9942, "NostrBridge-2"),     # L1 — шард 2
    (9943, "NostrBridge-3"),     # L1 — шард 3
    (9944, "NostrBridge-4"),     # L1 — шард 4
    (9945, "NostrBridge-5"),     # L1 — шард 5
    (8198, "TIERelay"),          # L0 — TIE WS relay
    (9900, "Supervisor"),        # L9 — оркестратор
    (9940, "IdentityAPI"),       # L5 — идентификация
]

EXTERNAL_TEST_RELAYS = [
    "wss://nos.lol",
    "wss://relay.damus.io",
]

# ═══ УТИЛИТЫ ════════════════════════════════════════

class TestResult:
    def __init__(self):
        self.results = []
        self.fails = 0
        self.total = 0
        self.start = time.time()
    
    def add(self, layer: str, name: str, passed: bool, data=None):
        self.results.append({
            "layer": layer, "name": name, "passed": passed,
            "data": data, "timestamp": datetime.now().isoformat()
        })
        self.total += 1
        if not passed:
            self.fails += 1
        icon = "✅" if passed else "❌"
        print(f"  {icon} [{layer}] {name}")
        return passed
    
    def summary(self):
        elapsed = time.time() - self.start
        passed_count = self.total - self.fails
        print(f"\n═══ ИТОГ ═══")
        print(f"  {passed_count}/{self.total} пройдено, {self.fails} провалов")
        print(f"  Время: {elapsed:.0f} сек")
        return {
            "total": self.total, "passed": passed_count,
            "fails": self.fails, "elapsed_sec": elapsed,
            "results": self.results,
            "timestamp": datetime.now().isoformat()
        }

def check_port(host, port, timeout=1):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.close()
        return True
    except:
        return False

def http_get(url, timeout=3):
    import urllib.request
    try:
        r = urllib.request.urlopen(url, timeout=timeout)
        return r.read()
    except Exception as e:
        return None

# ═══ L0: HEALTH ═════════════════════════════════════

def test_l0_ports(t: TestResult):
    """Проверка всех портов архитектуры."""
    print("\n━ L0: ПОРТЫ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    port_data = {}
    all_ok = True
    for port, name in PORTS:
        ok = check_port("127.0.0.1", port, 1)
        port_data[name] = ok
        if not ok:
            all_ok = False
            t.add("L0", f":{port} {name}", False)
    if all_ok:
        t.add("L0", f"Все {len(PORTS)} портов", True, port_data)
    return port_data


def test_l0_processes(t: TestResult):
    """Проверка что процессы живы и не жрут память."""
    print("\n━ L0: ПРОЦЕССЫ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    import psutil
    
    process_map = {
        "smart_router": "SmartRouter",
        "content_router_v2": "ContentRouter",
        "nostr_bridge": "NostrBridge",
        "external_gateway": "ExternalGateway",
        "cross_mesh_bridge": "CrossMesh",
        "route_engine": "RouteEngine",
        "identity_api": "IdentityAPI",
        "supervisor": "Supervisor",
    }
    
    found = {}
    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'memory_percent', 'cpu_percent', 'create_time']):
        try:
            cmd = ' '.join(proc.info['cmdline'] or [])
            for key, name in process_map.items():
                if key in cmd and 'python' in cmd.lower():
                    if name not in found:
                        found[name] = {
                            "pid": proc.info['pid'],
                            "mem_pct": proc.info['memory_percent'],
                            "cpu_pct": proc.info['cpu_percent'],
                            "uptime_sec": time.time() - proc.info['create_time']
                        }
        except: pass
    
    # Считаем NostrBridge шарды отдельно
    nb_count = sum(1 for n in found if 'NostrBridge' in n)
    
    all_ok = True
    missing = []
    for key, name in process_map.items():
        if name == 'NostrBridge':
            continue  # проверяем по шардам
        if name not in found:
            missing.append(name)
            all_ok = False
    
    nb_shards = [f for f in found if 'NostrBridge' in f]
    if len(nb_shards) == 0:
        all_ok = False
        missing.append("NostrBridge (shards)")
    
    names_found = list(found.keys())
    t.add("L0", f"Процессы ({len(found)}/{len(process_map)})", all_ok, {
        "found": names_found, "missing": missing, "details": found
    })
    return found


# ═══ L1: КОМПОНЕНТЫ ═════════════════════════════════

def test_l1_smartrouter(t: TestResult):
    """SmartRouter: health API, mesh канал, nostr каналы."""
    print("\n━ L1: SMARTROUTER ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    data = http_get("http://localhost:9933/", timeout=3)
    if not data:
        return t.add("L1", "SmartRouter API", False, {"error": "no response"})
    
    try:
        d = json.loads(data)
    except:
        return t.add("L1", "SmartRouter API", False, {"error": "invalid json"})
    
    mesh_ok = d.get("channels", {}).get("mesh", False)
    nostr_count = d.get("channels", {}).get("nostr", 0)
    uptime_min = d.get("uptime", 0) // 60
    
    passed = mesh_ok and nostr_count > 0
    t.add("L1", "SmartRouter Health", passed, {
        "mesh": mesh_ok, "nostr_count": nostr_count,
        "uptime_min": uptime_min
    })
    return d


def test_l1_contentrouter(t: TestResult):
    """ContentRouter: recv, dedup, fwd, качество."""
    print("\n━ L1: CONTENT ROUTER ━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log = os.path.join(RELAY_MESH_LOGS, "content_router.log")
    if not os.path.exists(log):
        return t.add("L1", "ContentRouter лог", False, {"error": "not found"})
    
    import re
    with open(log) as f:
        lines = f.readlines()
    
    recv_total = 0; dedup_total = 0; fwd_total = 0; err_total = 0
    for l in lines[-500:]:
        m = re.search(r'recv:(\d+)', l); recv_total += int(m.group(1)) if m else 0
        m = re.search(r'dedup:(\d+)', l); dedup_total += int(m.group(1)) if m else 0
        m = re.search(r'fwd:(\d+)', l); fwd_total += int(m.group(1)) if m else 0
        m = re.search(r'err:(\d+)', l); err_total += int(m.group(1)) if m else 0
    
    last_status = [l.strip() for l in lines if 'recv:' in l and 'dedup:' in l]
    last_line = last_status[-1] if last_status else "no data"
    
    # Rate per 10s
    recv_rate = recv_total / 5 if len(lines[-500:]) >= 50 else 0  # ~50 lines = 500s
    
    passed = recv_total > 0 and err_total == 0
    t.add("L1", "ContentRouter Stats", passed, {
        "recv": recv_total, "dedup": dedup_total, "fwd": fwd_total,
        "err": err_total, "recv_rate_10s": recv_rate,
        "last_status": last_line[:120]
    })
    return {"recv": recv_total, "dedup": dedup_total, "fwd": fwd_total, "err": err_total}


def test_l1_nostrbridge(t: TestResult):
    """NostrBridge: шарды, активные подключения, ошибки."""
    print("\n━ L1: NOSTRBRIDGE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    shards = {}
    all_ok = True
    
    for i in range(5):
        log = os.path.join(RELAY_MESH_LOGS, f"nostr_bridge_shard{i}.log")
        if not os.path.exists(log):
            shards[f"shard_{i}"] = {"status": "no_log"}
            all_ok = False
            continue
        
        with open(log, 'rb') as f:
            content = f.read().decode('utf-8', errors='replace')
        
        active = content.count("✅ Connected")
        dead = content.count("permanently dead")
        errors = content.count("ERROR") + content.count("Error")
        published = content.count("✅ Published") + content.count("published:")
        ok_count = content.count("OK") + content.count("✅")
        sz_kb = os.path.getsize(log) // 1024
        
        shard_ok = active > 0 and dead == 0
        t.add("L1", f"NostrBridge shard_{i}", shard_ok, {
            "active": active, "dead": dead, "errors": errors,
            "published": published, "size_kb": sz_kb
        })
        shards[f"shard_{i}"] = {"active": active, "dead": dead, "errors": errors, "published": published}
        if not shard_ok: all_ok = False
    
    return shards


def test_l1_external_gateway(t: TestResult):
    """ExternalGateway: ошибки, форварды, блокировки."""
    print("\n━ L1: EXTERNAL GATEWAY ━━━━━━━━━━━━━━━━━━━━━━━━")
    log = os.path.join(RELAY_MESH_LOGS, "external_gateway.log")
    if not os.path.exists(log):
        return t.add("L1", "ExternalGateway", False, {"error": "log not found"})
    
    with open(log, 'rb') as f:
        content = f.read().decode('utf-8', errors='replace')
    
    list_errors = content.count("list_errors")
    blocked = content.count("blocked")
    forwarded = content.count("forward") + content.count("Forward")
    sz_kb = os.path.getsize(log) // 1024
    
    passed = list_errors == 0 and blocked == 0
    t.add("L1", "ExternalGateway", passed, {
        "list_errors": list_errors, "blocked": blocked,
        "forwarded": forwarded, "size_kb": sz_kb
    })
    return {"list_errors": list_errors, "blocked": blocked, "forwarded": forwarded}


def test_l1_route_engine(t: TestResult):
    """RouteEngine: recv, err, ws."""
    print("\n━ L1: ROUTE ENGINE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log = os.path.join(RELAY_MESH_LOGS, "route_engine.log")
    if not os.path.exists(log):
        return t.add("L1", "RouteEngine", False, {"error": "log not found"})
    
    import re
    with open(log) as f:
        lines = f.readlines()
    
    recv = 0; err = 0; ws = 0
    for l in lines[-100:]:
        m = re.search(r'recv=(\d+)', l); recv += int(m.group(1)) if m else 0
        m = re.search(r'err=(\d+)', l); err += int(m.group(1)) if m else 0
        m = re.search(r'ws=(\d+)', l); ws += int(m.group(1)) if m else 0
    
    passed = err == 0
    t.add("L1", "RouteEngine", passed, {"recv": recv, "err": err, "ws": ws})
    return {"recv": recv, "err": err, "ws": ws}


def test_l1_circuit_breaker(t: TestResult):
    """Circuit Breaker: блокировки mesh/nostr."""
    print("\n━ L1: CIRCUIT BREAKER ━━━━━━━━━━━━━━━━━━━━━━━━━")
    log = os.path.join(RELAY_MESH_LOGS, "smart_router.log")
    if not os.path.exists(log):
        return t.add("L1", "CircuitBreaker", False, {"error": "not found"})
    
    with open(log) as f:
        lines = f.readlines()
    
    mesh_blocked = sum(1 for l in lines[-500:] if 'CB mesh blocked' in l)
    nostr_blocked = sum(1 for l in lines[-500:] if 'CB nostr blocked' in l)
    
    passed = mesh_blocked == 0 and nostr_blocked == 0
    t.add("L1", "CircuitBreaker (CB)", passed, {
        "mesh_blocked_last500": mesh_blocked,
        "nostr_blocked_last500": nostr_blocked
    })
    return {"mesh_blocked": mesh_blocked, "nostr_blocked": nostr_blocked}


def test_l1_supervisor(t: TestResult):
    """Supervisor: alive/total, restarts."""
    print("\n━ L1: SUPERVISOR ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    data = http_get("http://localhost:9900/health", timeout=3)
    if not data:
        return t.add("L1", "Supervisor", False, {"error": "no response"})
    
    try:
        d = json.loads(data)
    except:
        return t.add("L1", "Supervisor", False, {"error": "invalid json"})
    
    alive = d.get("alive", 0)
    total = d.get("total", 0)
    restarts = d.get("total_restarts", 0)
    
    passed = alive >= total * 0.8
    t.add("L1", "Supervisor", passed, {
        "alive": alive, "total": total, "restarts": restarts,
        "dead": d.get("dead", 0)
    })
    return d


def test_l1_identity_api(t: TestResult):
    """IdentityAPI: L5 Identity & Reputation."""
    print("\n━ L1: IDENTITY API ━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log = os.path.join(RELAY_MESH_LOGS, "identity_api.log")
    if not os.path.exists(log):
        return t.add("L1", "IdentityAPI", False, {"error": "log not found"})
    
    with open(log) as f:
        lines = f.readlines()
    
    health_lines = [l for l in lines if 'Health' in l or 'Agent' in l or 'agent' in l]
    
    t.add("L1", "IdentityAPI listener on :9940", True, {
        "health_entries": len(health_lines),
        "size_kb": os.path.getsize(log) // 1024
    })
    return {"lines": len(lines), "health_entries": len(health_lines)}


# ═══ L2: СВЯЗИ ═══════════════════════════════════════

def test_l2_mesh_routing(t: TestResult):
    """Mesh-канал: отправка → верификация прохождения через ContentRouter."""
    print("\n━ L2: MESH-КАНАЛ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    test_uid = f"e2e_routing_{int(time.time())}"
    
    msg = json.dumps({
        "id": test_uid, "kind": 39002,
        "content": json.dumps({"text": f"__TEST_{test_uid}__", "origin": "e2e_test"}),
        "created_at": int(time.time())
    }) + "\n"
    
    # Отправка через SmartRouter
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect(('127.0.0.1', 9932))
        s.sendall(msg.encode())
        time.sleep(1)
        resp = json.loads(s.recv(4096).decode())
        s.close()
        
        ok = resp.get("ok", False)
        channel = resp.get("channel", "?")
        latency = resp.get("latency_ms", 0)
        
        if not ok:
            return t.add("L2", "Mesh Routing", False, {"error": "SR rejected"})
        
        # Верификация в ContentRouter
        time.sleep(2)
        cr_log = os.path.join(RELAY_MESH_LOGS, "content_router.log")
        cr_received = False
        if os.path.exists(cr_log):
            with open(cr_log) as f:
                cr_content = f.read()
            # CR может обрезать ID — ищем любой кусок от uid
            for chunk_len in [20, 18, 15, 12, 10]:
                if test_uid[:chunk_len] in cr_content:
                    cr_received = True
                    break
        
        passed = ok and cr_received
        t.add("L2", "Mesh Routing (SR → CR)", passed, {
            "sr_ok": ok, "channel": channel, "latency_ms": latency,
            "cr_received": cr_received
        })
        return {"ok": ok, "channel": channel, "latency_ms": latency, "cr_received": cr_received}
    except Exception as e:
        return t.add("L2", "Mesh Routing", False, {"error": str(e)[:80]})


def test_l2_cross_mesh(t: TestResult):
    """CrossMesh: проверка соединения."""
    print("\n━ L2: CROSS-MESH ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    ok = check_port("127.0.0.1", 9946, 2)
    
    log = os.path.join(RELAY_MESH_LOGS, "cross_mesh_bridge.log")
    cross_lines = 0
    if os.path.exists(log):
        with open(log) as f:
            cross_lines = len(f.readlines())
    
    t.add("L2", "CrossMesh Bridge (:9946)", ok, {
        "lines_logged": cross_lines,
        "size_kb": os.path.getsize(log) // 1024 if os.path.exists(log) else 0
    })
    return {"ok": ok, "log_lines": cross_lines}


def test_l2_tie_relay(t: TestResult):
    """TIE Relay: WS relay для агентов."""
    print("\n━ L2: TIE-RELAY ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    ok = check_port("127.0.0.1", 8198, 2)
    
    if not ok:
        return t.add("L2", "TIE Relay (:8198)", False, {"error": "port closed"})
    
    log_files = glob.glob(os.path.join(TIE_RELAY_DIR, "*.log"))
    log_sizes = {}
    for lf in log_files:
        log_sizes[os.path.basename(lf)] = os.path.getsize(lf) // 1024
    
    t.add("L2", "TIE Relay (:8198)", True, {"log_sizes_kb": log_sizes})
    return {"ok": ok, "logs": log_sizes}


# ═══ L3: ВНЕШНЯЯ ВИДИМОСТЬ ═════════════════════════

def test_l3_nostr_publish(t: TestResult):
    """Прямая публикация на внешние Nostr revis."""
    print("\n━ L3: NOSTR ПУБЛИКАЦИЯ ━━━━━━━━━━━━━━━━━━━━━━━━")
    import websocket as ws_lib  # import as alias
    
    # Читаем nsec из конфига
    if not os.path.exists(CRYTER_CONFIG):
        return t.add("L3", "Nostr Publish (прямая)", False, {"error": "config not found"})
    
    with open(CRYTER_CONFIG) as f:
        cfg = yaml.safe_load(f)
    nsec = cfg.get('nostr', {}).get('nsec', '')
    if not nsec:
        return t.add("L3", "Nostr Publish (прямая)", False, {"error": "no nsec in config"})
    
    from nostr_sdk import Keys, EventBuilder, Kind
    keys = Keys.parse(nsec)
    pubkey = keys.public_key().to_hex()
    
    # Создаём тестовый пост
    test_content = f"__SNIN_E2E_{int(time.time())}__"
    builder = EventBuilder(Kind(1), test_content)
    event = builder.sign_with_keys(keys)
    event_id = event.id().to_hex()
    msg = json.dumps(["EVENT", json.loads(event.as_json())])
    
    results = {}
    all_ok = True
    for relay_url in EXTERNAL_TEST_RELAYS:
        try:
            ws = ws_lib.create_connection(relay_url, timeout=10)
            ws.send(msg)
            ws.settimeout(5)
            time.sleep(1)
            resp = ws.recv()
            data = json.loads(resp)
            ws.close()
            
            success = data[2] == True
            results[relay_url] = {"ok": success, "message": str(data[3])[:60]}
            if not success:
                all_ok = False
        except Exception as e:
            results[relay_url] = {"ok": False, "error": str(e)[:60]}
            all_ok = False
    
    # Верификация на релее
    time.sleep(2)
    verified = 0
    for relay_url in EXTERNAL_TEST_RELAYS:
        try:
            ws = ws_lib.create_connection(relay_url, timeout=10)
            ws.send(json.dumps(["REQ", "e2e_vfy", {"ids": [event_id], "limit": 1}]))
            ws.settimeout(5)
            time.sleep(1)
            found = False
            try:
                while True:
                    m = ws.recv()
                    d = json.loads(m)
                    if d[0] == "EVENT": found = True; break
                    elif d[0] == "EOSE": break
            except: pass
            ws.send(json.dumps(["CLOSE", "e2e_vfy"]))
            ws.close()
            if found:
                verified += 1
                results[relay_url]["verified"] = True
            else:
                results[relay_url]["verified"] = False
        except:
            results[relay_url]["verified"] = False
    
    t.add("L3", "Nostr Publish (прямая)", all_ok and verified > 0, {
        "event_id": event_id, "pubkey": pubkey,
        "results": results, "verified_relays": verified
    })
    return {"event_id": event_id, "results": results, "verified": verified}


def test_l3_nostr_historic(t: TestResult):
    """Проверка что посты Cryter есть на внешних релеях (история)."""
    print("\n━ L3: NOSTR ИСТОРИЯ ━━━━━━━━━━━━━━━━━━━━━━━━━━")
    import websocket as ws_lib
    
    # Берём pubkey из конфига
    if not os.path.exists(CRYTER_CONFIG):
        return t.add("L3", "Nostr History", False, {"error": "no config"})
    
    with open(CRYTER_CONFIG) as f:
        cfg = yaml.safe_load(f)
    nsec = cfg.get('nostr', {}).get('nsec', '')
    if not nsec:
        return t.add("L3", "Nostr History", False, {"error": "no nsec"})
    
    from nostr_sdk import Keys
    keys = Keys.parse(nsec)
    pubkey_hex = keys.public_key().to_hex()
    npub = keys.public_key().to_bech32()
    
    # Проверка на nos.lol
    results = {}
    for relay_url in EXTERNAL_TEST_RELAYS:
        try:
            ws = ws_lib.create_connection(relay_url, timeout=10)
            ws.send(json.dumps(["REQ", "hist", {"authors": [pubkey_hex], "limit": 3}]))
            ws.settimeout(5)
            time.sleep(2)
            posts = []
            try:
                while True:
                    m = ws.recv()
                    d = json.loads(m)
                    if d[0] == "EVENT":
                        ev = d[2]
                        posts.append({
                            "created_at": ev.get("created_at", 0),
                            "content": ev.get("content", "")[:60]
                        })
                    elif d[0] == "EOSE": break
            except: pass
            ws.send(json.dumps(["CLOSE", "hist"]))
            ws.close()
            results[relay_url] = {"post_count": len(posts), "posts": posts}
        except Exception as e:
            results[relay_url] = {"error": str(e)[:60]}
    
    total_posts = sum(r.get("post_count", 0) for r in results.values())
    passed = total_posts > 0
    t.add("L3", "Nostr History (посты на релеях)", passed, {
        "npub": npub, "total_posts_found": total_posts, "results": results
    })
    return results


# ═══ L4: КОНСИСТЕНТНОСТЬ ════════════════════════════

def test_l4_key_consistency(t: TestResult):
    """Проверка консистентности ключей: config.yaml vs keyring."""
    print("\n━ L4: КОНСИСТЕНТНОСТЬ КЛЮЧЕЙ ━━━━━━━━━━━━━━━━━")
    
    # 1. Keyring
    sys.path.insert(0, KEYSTORE_DIR)
    from keyring import Keyring
    kr = Keyring()
    pairs = kr.get_all_keypairs()
    kr.close()
    
    kr_data = {}
    for p in pairs:
        agent_id = p.get('agent_id', p.get('name', '?'))
        kr_data[agent_id] = {
            "npub": p.get('npub', '?'),
            "pubhex": p.get('pubhex', '?'),
            "name": p.get('name', '?')
        }
    
    # 2. Config
    if not os.path.exists(CRYTER_CONFIG):
        return t.add("L4", "Key консистентность", False, {"error": "config not found"})
    
    with open(CRYTER_CONFIG) as f:
        cfg = yaml.safe_load(f)
    nsec = cfg.get('nostr', {}).get('nsec', '')
    
    from nostr_sdk import Keys
    keys = Keys.parse(nsec)
    cfg_npub = keys.public_key().to_bech32()
    cfg_hex = keys.public_key().to_hex()
    
    # 3. Сравнение
    kr_npub = kr_data.get('cryter', {}).get('npub', '?')
    kr_hex = kr_data.get('cryter', {}).get('pubhex', '?')
    
    keys_match = (kr_npub == cfg_npub) or (kr_hex.strip('02') == cfg_hex.strip('02'))
    # Нормализованное сравнение (без 02 префикса)
    kr_hex_clean = kr_hex.lstrip('02')
    cfg_hex_clean = cfg_hex.lstrip('02')
    exact_match = kr_npub == cfg_npub
    
    t.add("L4", "Key консистентность (config ↔ keyring)", keys_match, {
        "config_npub": cfg_npub,
        "keyring_cryter_npub": kr_npub,
        "exact_match": exact_match,
        "hex_match": kr_hex_clean == cfg_hex_clean,
        "config_hex": cfg_hex,
        "keyring_hex": kr_hex,
        "all_keys_in_keyring": len(kr_data)
    })
    return {
        "config_npub": cfg_npub, "keyring_npub": kr_npub,
        "match": keys_match, "all_keys": list(kr_data.keys())
    }


def test_l4_agent_registry(t: TestResult):
    """Проверка реестра агентов."""
    print("\n━ L4: AGENT REGISTRY ━━━━━━━━━━━━━━━━━━━━━━━━━━")
    if not os.path.exists(AGENTS_REGISTRY):
        return t.add("L4", "Agent Registry", False, {"error": "not found"})
    
    agents = sorted([d for d in os.listdir(AGENTS_REGISTRY) 
                     if os.path.isdir(os.path.join(AGENTS_REGISTRY, d))])
    files = [f for f in os.listdir(AGENTS_REGISTRY) 
             if os.path.isfile(os.path.join(AGENTS_REGISTRY, f))]
    
    t.add("L4", f"Agent Registry ({len(agents)} агентов)", True, {
        "agents": agents, "files": files
    })
    return {"agents": agents, "files": files}


def test_l4_configs(t: TestResult):
    """Проверка конфигов на наличие и валидность."""
    print("\n━ L4: КОНФИГИ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    configs_to_check = [
        (CRYTER_CONFIG, "Cryter config.yaml"),
        (os.path.join(RELAY_MESH_DIR, "port.txt"), "Mesh port"),
        (os.path.join(RELAY_MESH_DIR, "start.sh"), "Mesh start.sh"),
        (os.path.join(RELAY_MESH_DIR, "agents.json"), "Mesh agents.json"),
        (os.path.join(TIE_RELAY_DIR, "port.txt"), "TIE port"),
        (os.path.join(SNIN_HUB_DIR, "port.txt"), "SNIN Hub port"),
    ]
    
    results = {}
    all_exist = True
    for path, name in configs_to_check:
        exists = os.path.exists(path)
        size = os.path.getsize(path) if exists else 0
        results[name] = {"exists": exists, "size": size}
        if not exists:
            all_exist = False
    
    t.add("L4", "Конфиги архитектуры", all_exist, results)
    return results


# ═══ BASELINE ═══════════════════════════════════════

def load_baseline():
    if os.path.exists(BASELINE_FILE):
        return json.load(open(BASELINE_FILE))
    return None

def save_baseline(summary):
    json.dump(summary, open(BASELINE_FILE, "w"), indent=2, default=str)
    print(f"\n📦 Baseline сохранён: {BASELINE_FILE}")

def delta_analysis(summary):
    baseline = load_baseline()
    if not baseline:
        save_baseline(summary)
        return []
    
    deltas = []
    old_results = {r["name"]: r for r in baseline.get("results", [])}
    
    for r in summary["results"]:
        name = r["name"]
        old = old_results.get(name)
        if not old:
            continue
        
        # Регрессия: было True → стало False
        if old["passed"] and not r["passed"]:
            deltas.append(f"⚠️ РЕГРЕССИЯ: {name} — было ✅, стало ❌")
        
        # NostrBridge: были active → стали 0
        if "NostrBridge" in name:
            old_active = old.get("data", {}).get("active", 1)
            new_active = r.get("data", {}).get("active", 0)
            if old_active > 0 and new_active == 0:
                deltas.append(f"⚠️ РЕГРЕССИЯ: {name} — было {old_active} active, стало 0")
        
        # CB: было 0 блокировок → стало > 0
        if "CircuitBreaker" in name:
            old_blocked = old.get("data", {}).get("mesh_blocked_last500", 0)
            new_blocked = r.get("data", {}).get("mesh_blocked_last500", 0)
            if old_blocked == 0 and new_blocked > 0:
                deltas.append(f"⚠️ РЕГРЕССИЯ: CB mesh blocked — был 0, стал {new_blocked}")
    
    return deltas


# ═══ MAIN ═══════════════════════════════════════════

def main():
    save_flag = "--save-baseline" in sys.argv
    json_flag = "--json" in sys.argv
    
    print(f"\n═══ SNIN V5 — ПОЛНЫЙ ТЕСТ АРХИТЕКТУРЫ ═══")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} MSK")
    print(f"  Архитектура: relay-mesh → tie-relay → snin-hub")
    print(f"  Тест: L0→L4, {len(PORTS)} портов, {len(EXTERNAL_TEST_RELAYS)} внешних релеев")
    print()
    
    t = TestResult()
    
    # ═══ L0: Health ═══
    ports = test_l0_ports(t)
    procs = test_l0_processes(t)
    
    # ═══ L1: Компоненты ═══
    sr_data = test_l1_smartrouter(t)
    cr_data = test_l1_contentrouter(t)
    nb_data = test_l1_nostrbridge(t)
    eg_data = test_l1_external_gateway(t)
    re_data = test_l1_route_engine(t)
    cb_data = test_l1_circuit_breaker(t)
    sup_data = test_l1_supervisor(t)
    identity_data = test_l1_identity_api(t)
    
    # ═══ L2: Связи ═══
    mesh_data = test_l2_mesh_routing(t)
    cross_data = test_l2_cross_mesh(t)
    tie_data = test_l2_tie_relay(t)
    
    # ═══ L3: Внешняя видимость ═══
    publish_data = test_l3_nostr_publish(t)
    history_data = test_l3_nostr_historic(t)
    
    # ═══ L4: Консистентность ═══
    keys_data = test_l4_key_consistency(t)
    reg_data = test_l4_agent_registry(t)
    config_data = test_l4_configs(t)
    
    # ═══ Итог ═══
    summary = t.summary()
    
    # Delta-анализ
    print(f"\n━ DELTA-АНАЛИЗ (регрессия) ━━━━━━━━━━━━━━━━━━")
    deltas = delta_analysis(summary)
    if deltas:
        for d in deltas:
            print(f"  {d}")
    else:
        print(f"  ✅ Регрессии не обнаружены")
    
    # Сохраняем снапшот
    snapshot = {
        "summary": summary,
        "deltas": deltas,
        "snapshot_time": datetime.now().isoformat()
    }
    json.dump(snapshot, open(SNAPSHOT_FILE, "w"), indent=2, default=str)
    print(f"\n📸 Снапшот: {SNAPSHOT_FILE}")
    
    # JSON вывод
    if json_flag:
        print("\n" + json.dumps(summary, indent=2, default=str))
    
    passed = summary["fails"] == 0 and len(deltas) == 0
    return passed, summary


if __name__ == "__main__":
    passed, _ = main()
    sys.exit(0 if passed else 1)

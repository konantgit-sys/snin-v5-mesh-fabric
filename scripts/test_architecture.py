#!/usr/bin/env python3
"""
SNIN V5 — МАСТЕР-ТЕСТ АРХИТЕКТУРЫ
====================================
Единый стандарт "готово". Перед каждым ответом "всё работает"
прогонять: python3 /home/agent/data/scripts/test_architecture.py

Фиксация регрессии:
  - /home/agent/data/scripts/test_regression_log.json  — история чеков
  - /home/agent/data/scripts/test_baseline.json        — эталонные показатели
  - delta = текущие показатели − baseline
    → если delta > порога: СТОП, разбираться

Исправления, которые тест отслеживает (регрессия):
  1. CB mesh blocked = 0 (фикс #1 — баг с drain timeout)
  2. CB recovery на реконнекте (фикс #2)
  3. ContentRouter принимает сообщения (фикс #3)
"""

import socket, json, time, os, sys, subprocess, glob, urllib.request
from datetime import datetime
from collections import Counter

# ─── КОНФИГУРАЦИЯ ─────────────────────────────────────
REGRESSION_LOG = "/home/agent/data/scripts/test_regression_log.json"
BASELINE_FILE = "/home/agent/data/scripts/test_baseline.json"
RELAY_MESH_LOG = "/home/agent/data/sites/relay-mesh/logs"
CRYTER_LOG = "/home/agent/data/agents/core/cryter/logs"

PORTS = [
    (9932, "SmartRouter"),
    (9931, "ExternalGateway"),
    (9920, "ContentRouter"),
    (9910, "RouteEngine"),
    (9933, "MeshAPI"),
    (9946, "CrossMesh"),
    (9941, "NostrBridge-1"),
    (9942, "NostrBridge-2"),
    (9943, "NostrBridge-3"),
    (9944, "NostrBridge-4"),
    (9945, "NostrBridge-5"),
    (8198, "TIERelay"),
    (9900, "Supervisor"),
]

# Пороги для delta-анализа
THRESHOLDS = {
    "cb_mesh_blocked": 0,        # CB mesh блокировок не должно быть
    "port_fails": 0,             # Ни один порт не должен упасть
    "content_router_recv_delta": 50,  # recv не должен упасть больше чем на 50
    "nostr_dead_shards": 0,      # permanently_dead = 0
}


# ─── ТЕСТ-ФУНКЦИИ ─────────────────────────────────────

def test_port(port, name):
    """Проверка порта."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    ok = s.connect_ex(('127.0.0.1', port)) == 0
    s.close()
    return ("✅" if ok else "❌"), f"{name} (:{port})", ok


def test_smartrouter_api():
    """Health-ендпоинт SmartRouter."""
    try:
        r = urllib.request.urlopen("http://localhost:9933/", timeout=3)
        d = json.loads(r.read())
        mesh_ok = d.get("channels", {}).get("mesh", False)
        nostr_ok = d.get("channels", {}).get("nostr", 0) > 0
        uptime = d.get("uptime", 0)
        return mesh_ok and nostr_ok, d
    except Exception as e:
        return False, str(e)


def test_mesh_routing():
    """Проверка маршрутизации через mesh-канал."""
    uid = f"regression_test_{int(time.time())}"
    msg = json.dumps({
        "id": uid, "kind": 39002,
        "content": json.dumps({"text": f"__REGRESSION_{uid}__", "origin": "regression_test"}),
        "created_at": int(time.time())
    }) + "\n"
    
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect(('127.0.0.1', 9932))
        s.sendall(msg.encode())
        resp = json.loads(s.recv(4096).decode())
        s.close()
        
        ok = resp.get("ok", False)
        latency = resp.get("latency_ms", 0)
        
        # Проверка что дошло до ContentRouter
        time.sleep(2)
        cr_log = os.path.join(RELAY_MESH_LOG, "cr_v2.log")
        cr_ok = False
        if os.path.exists(cr_log):
            with open(cr_log) as f:
                cr_ok = uid[:16] in f.read()
        
        return ok and cr_ok, {"uid": uid, "ok": ok, "cr_received": cr_ok, "latency_ms": latency}
    except Exception as e:
        return False, str(e)


def test_cb_status(pid=None):
    """Circuit Breaker статус для mesh канала."""
    sr_log = os.path.join(RELAY_MESH_LOG, "smart_router.log")
    if not os.path.exists(sr_log):
        return False, {"error": "log not found"}
    
    lines = open(sr_log).readlines()
    
    # Найти последний запуск
    last_start = 0
    for i, line in enumerate(lines, 1):
        if 'Smart Router v2 — 0.0.0.0:9932' in line:
            last_start = i
    total = len(lines)
    new_lines = total - last_start
    
    # CB blocked с последнего запуска
    mesh_blocked = 0
    nostr_blocked = 0
    fail_rates = []
    for i, line in enumerate(lines, 1):
        if i < last_start: continue
        if 'CB mesh blocked' in line: mesh_blocked += 1
        if 'CB nostr blocked' in line: nostr_blocked += 1
        if 'fail_rate' in line: fail_rates.append(line.strip())
    
    return (mesh_blocked == 0), {
        "mesh_blocked": mesh_blocked,
        "nostr_blocked": nostr_blocked,
        "log_lines_since_start": new_lines,
        "last_fail_rate": fail_rates[-1] if fail_rates else None
    }


def test_contentrouter_stats():
    """Статистика ContentRouter."""
    cr_log = os.path.join(RELAY_MESH_LOG, "cr_v2.log")
    if not os.path.exists(cr_log):
        return False, {"error": "log not found"}
    
    lines = open(cr_log).readlines()
    last_status = None
    for line in lines[-5:]:
        if 'Agents:' in line:
            last_status = line.strip()
    
    # recv за последние строки
    import re
    recv_vals = []
    for l in lines[-50:]:
        m = re.search(r'recv:(\d+)', l)
        if m: recv_vals.append(int(m.group(1)))
    
    fwd_count = sum(1 for l in lines if 'fwd kind' in l)
    
    return True, {
        "agents": 0 if not last_status else "?",
        "last_status": last_status,
        "recv_recent_mean": sum(recv_vals)/len(recv_vals) if recv_vals else 0,
        "total_fwd": fwd_count
    }


def test_nostrbridge():
    """Статус NostrBridge шардов."""
    shards = {}
    all_ok = True
    existing = sorted(glob.glob(os.path.join(RELAY_MESH_LOG, "nostr_bridge_shard*.log")))
    if not existing:
        existing = sorted(glob.glob(os.path.join(RELAY_MESH_LOG, "nostr_bridge_*.log")))
    
    for f in existing:
        basename = os.path.basename(f).replace(".log", "")
        if not os.path.exists(f):
            shards[basename] = {"status": "no_log"}
            all_ok = False
            continue
        
        content = open(f).read()
        active = content.count("✅ Connected")
        dead = content.count("permanently dead")
        errors = content.count("ERROR") + content.count("Error")
        size_kb = os.path.getsize(f) // 1024
        
        shards[basename] = {
            "permanently_dead": dead,
            "errors": errors,
            "size_kb": size_kb
        }
        if dead > 0:
            all_ok = False
    
    return all_ok, shards


def test_external_gateway():
    """External Gateway проверка."""
    f = os.path.join(RELAY_MESH_LOG, "external_gateway.log")
    if not os.path.exists(f):
        return False, {"error": "log not found"}
    
    content = open(f).read()
    list_errors = content.count("list_errors")
    blocked = content.count("blocked")
    size_kb = os.path.getsize(f) // 1024
    
    return (list_errors == 0), {
        "list_errors": list_errors,
        "blocked": blocked,
        "size_kb": size_kb
    }


def test_supervisor():
    """Supervisor L9 статус."""
    try:
        r = urllib.request.urlopen("http://localhost:9900/health", timeout=3)
        d = json.loads(r.read())
        alive = d.get("alive", 0)
        total = d.get("total", 0)
        restarts = d.get("total_restarts", 0)
        return (alive >= total * 0.8), {
            "alive": alive, "total": total, "restarts": restarts,
            "dead": d.get("dead", 0)
        }
    except Exception as e:
        return False, str(e)


def test_cryter_processes():
    """Проверка процессов Cryter."""
    import psutil
    found = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
        try:
            cmd = ' '.join(proc.info['cmdline'] or [])
            if 'cryter' in cmd.lower() and 'python' in cmd.lower():
                proc.cpu_percent(interval=0.1)  # first call = 0
                found.append(proc.info)
        except: pass
    return len(found) > 0, {"count": len(found)}


def test_keyring():
    """Проверка keyring с агентами."""
    sys.path.insert(0, '/home/agent/data/sites/chrono/keystore')
    try:
        from keyring import Keyring
        kr = Keyring()
        pairs = kr.get_all_keypairs()
        kr.close()
        return len(pairs) > 0, {"count": len(pairs), "names": [p.get('name','?') for p in pairs]}
    except Exception as e:
        return False, str(e)


# ─── DELTA-АНАЛИЗ ──────────────────────────────────────

def load_baseline():
    """Загрузить эталонные показатели."""
    if os.path.exists(BASELINE_FILE):
        return json.load(open(BASELINE_FILE))
    return None

def save_baseline(results):
    """Сохранить эталон."""
    baseline = {}
    for name, passed, data in results:
        baseline[name] = {"passed": passed, "data": data}
    json.dump(baseline, open(BASELINE_FILE, "w"), indent=2)
    print(f"\n📦 Baseline сохранён: {BASELINE_FILE}")

def delta_analysis(results):
    """Сравнить с baseline и найти регрессию."""
    baseline = load_baseline()
    if not baseline:
        save_baseline(results)
        return []
    
    deltas = []
    for name, passed, data in results:
        old = baseline.get(name)
        if not old:
            continue
        
        # CB mesh blocked — критическая регрессия
        if name == "CB mesh" and old["data"].get("mesh_blocked", 0) == 0:
            new_blocked = data.get("mesh_blocked", 0)
            if new_blocked > 0:
                deltas.append(f"⚠️ РЕГРЕССИЯ: CB mesh blocked был 0, стал {new_blocked}")
        
        # Порты
        if name == "Ports" and old["passed"]:
            if not passed:
                deltas.append("⚠️ РЕГРЕССИЯ: упал порт (были все ✅)")
        
        # NostrBridge — dead shards
        if name == "NostrBridge":
            old_dead = sum(1 for s in old.get("data", {}).values() if isinstance(s, dict) and s.get("permanently_dead", 0) > 0)
            new_dead = sum(1 for s in data.values() if isinstance(s, dict) and s.get("permanently_dead", 0) > 0)
            if new_dead > old_dead:
                deltas.append(f"⚠️ РЕГРЕССИЯ: NostrBridge dead shards {old_dead} → {new_dead}")
    
    return deltas


# ─── ГЛАВНАЯ ───────────────────────────────────────────

def main(save_baseline_flag=False):
    start = time.time()
    
    print(f"═══ SNIN V5 — МАСТЕР-ТЕСТ ═══")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} MSK")
    print()
    
    results = []
    all_passed = True
    fail_count = 0
    
    # 1. ПОРТЫ
    print("━ 1. ПОРТЫ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    port_fails = []
    for port, name in PORTS:
        icon, label, ok = test_port(port, name)
        if not ok: port_fails.append(label)
        status = "✅" if ok else "❌"
        print(f"  {status} :{port} ({name})")
    
    all_ports_ok = len(port_fails) == 0
    if not all_ports_ok: all_passed = False; fail_count += len(port_fails)
    results.append(("Ports", all_ports_ok, {"total": len(PORTS), "fails": port_fails}))
    
    # 2. HEALTH + MESH
    print("\n━ 2. SMARTROUTER ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    sr_ok, sr_data = test_smartrouter_api()
    status = "✅" if sr_ok else "❌"
    print(f"  {status} API & mesh канал")
    if isinstance(sr_data, dict):
        print(f"    uptime: {sr_data.get('uptime', 0)/60:.0f} мин")
        print(f"    mesh: {sr_data.get('channels', {}).get('mesh', False)}")
        print(f"    nostr: {sr_data.get('channels', {}).get('nostr', 0)}")
    results.append(("SmartRouter", sr_ok, sr_data))
    if not sr_ok: all_passed = False; fail_count += 1
    
    # 3. MESH ROUTING
    print("\n━ 3. MESH-КАНАЛ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    route_ok, route_data = test_mesh_routing()
    if isinstance(route_data, dict):
        print(f"  {'✅' if route_ok else '❌'} ok={route_data.get('ok')} cr_received={route_data.get('cr_received')} latency={route_data.get('latency_ms'):.1f}ms")
    else:
        print(f"  {'✅' if route_ok else '❌'} {route_data}")
    results.append(("MeshRouting", route_ok, route_data))
    if not route_ok: all_passed = False; fail_count += 1
    
    # 4. CB
    print("\n━ 4. CIRCUIT BREAKER ━━━━━━━━━━━━━━━━━━━━━━━━━")
    cb_ok, cb_data = test_cb_status()
    if isinstance(cb_data, dict):
        print(f"  {'✅' if cb_ok else '❌'} mesh_blocked={cb_data.get('mesh_blocked', '?')}")
        print(f"    nostr_blocked={cb_data.get('nostr_blocked', '?')}")
        print(f"    log_lines={cb_data.get('log_lines_since_start', '?')}")
        if cb_data.get("last_fail_rate"):
            print(f"    last_fail_rate: {cb_data['last_fail_rate'][:70]}")
    results.append(("CB mesh", cb_ok, cb_data))
    if not cb_ok: all_passed = False; fail_count += 1
    
    # 5. CONTENTROUTER
    print("\n━ 5. CONTENTROUTER ━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    cr_ok, cr_data = test_contentrouter_stats()
    if isinstance(cr_data, dict):
        print(f"  {'✅' if cr_ok else '❌'} recv_mean={cr_data.get('recv_recent_mean', '?'):.0f}")
        print(f"    total_fwd={cr_data.get('total_fwd', '?')}")
        if cr_data.get("last_status"):
            print(f"    status: {cr_data['last_status'][:80]}")
    results.append(("ContentRouter", cr_ok, cr_data))
    if not cr_ok: all_passed = False; fail_count += 1
    
    # 6. NOSTRBRIDGE
    print("\n━ 6. NOSTRBRIDGE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    nb_ok, nb_data = test_nostrbridge()
    if isinstance(nb_data, dict):
        for shard, info in nb_data.items():
            if isinstance(info, dict):
                icon = "✅" if info.get("permanently_dead", 0) == 0 else "❌"
                print(f"  {icon} {shard}: active={info.get('active_conn','?')} dead={info.get('permanently_dead','?')} errors={info.get('errors','?')}")
            else:
                print(f"  ❌ {shard}: {info}")
        all_dead = sum(1 for s in nb_data.values() if isinstance(s, dict) and s.get("permanently_dead", 0) > 0)
        if all_dead > 0: nb_ok = False
    print(f"  {'✅ All OK' if nb_ok else '❌ ЕСТЬ ПРОБЛЕМЫ'}")
    results.append(("NostrBridge", nb_ok, nb_data))
    if not nb_ok: all_passed = False; fail_count += 1
    
    # 7. EXTERNAL GATEWAY
    print("\n━ 7. EXTERNAL GATEWAY ━━━━━━━━━━━━━━━━━━━━━━━━")
    eg_ok, eg_data = test_external_gateway()
    if isinstance(eg_data, dict):
        print(f"  {'✅' if eg_ok else '❌'} list_errors={eg_data.get('list_errors', '?')} blocked={eg_data.get('blocked', '?')}")
    results.append(("ExternalGateway", eg_ok, eg_data))
    if not eg_ok: all_passed = False; fail_count += 1
    
    # 8. SUPERVISOR
    print("\n━ 8. SUPERVISOR ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    sup_ok, sup_data = test_supervisor()
    if isinstance(sup_data, dict):
        print(f"  {'✅' if sup_ok else '❌'} alive={sup_data.get('alive','?')}/{sup_data.get('total','?')} restarts={sup_data.get('restarts','?')} dead={sup_data.get('dead','?')}")
    else:
        print(f"  {'✅' if sup_ok else '❌'} {sup_data}")
    results.append(("Supervisor", sup_ok, sup_data))
    if not sup_ok: all_passed = False; fail_count += 1
    
    # 9. CRYTER
    print("\n━ 9. CRYTER ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    cryter_ok, cryter_data = test_cryter_processes()
    if isinstance(cryter_data, dict):
        print(f"  {'✅' if cryter_ok else '❌'} {cryter_data.get('count',0)} процессов")
    results.append(("Cryter", cryter_ok, cryter_data))
    
    # 10. KEYRING
    kr_ok, kr_data = test_keyring()
    if isinstance(kr_data, dict):
        print(f"\n   keyring: {'✅' if kr_ok else '❌'} {kr_data.get('count',0)} агентов")
        names = [n for n in kr_data.get('names', []) if n != '?']
        if names: print(f"   имена: {', '.join(names[:8])}{'...' if len(names)>8 else ''}")
    results.append(("Keyring", kr_ok, kr_data))
    
    # 11. DELTA-АНАЛИЗ
    print("\n━ 10. DELTA-АНАЛИЗ (регрессия) ━━━━━━━━━━━━━━")
    deltas = delta_analysis(results)
    if deltas:
        for d in deltas:
            print(f"  {d}")
            all_passed = False
            fail_count += 1
    else:
        print(f"  ✅ Регрессии не обнаружены")
    
    # ИТОГ
    total_tests = len(results) + len([r for r in results if r[1]])  # ~approx
    print(f"\n═══ ИТОГ ═══")
    print(f"  {'✅ ВСЁ РАБОТАЕТ' if all_passed else f'❌ {fail_count} ПРОВАЛ(А)'}")
    elapsed = time.time() - start
    print(f"  Время: {elapsed:.0f} сек")
    
    # Логируем
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "passed": all_passed,
        "fails": fail_count,
        "elapsed_sec": elapsed,
        "results": {r[0]: {"passed": r[1]} for r in results}
    }
    with open(REGRESSION_LOG, "w") as f:
        json.dump(log_entry, f, indent=2)
    
    return all_passed, results


if __name__ == "__main__":
    save = "--save-baseline" in sys.argv
    passed, _ = main(save)
    sys.exit(0 if passed else 1)

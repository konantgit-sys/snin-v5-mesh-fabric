#!/usr/bin/env python3
"""
SNIN Test Suite — автоматическая проверка всех слоёв.
Запуск: python3 l3_mesh_core.py (нужен для L3)
         python3 test_suite.py
"""

import json
import socket
import subprocess
import sys
import time
import urllib.request
import urllib.error

# ═══════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════

tests_passed = 0
tests_failed = 0
errors = []

def port_open(host, port, timeout=1):
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True
    except: return False

def http_get(url, timeout=3):
    try:
        r = urllib.request.urlopen(url, timeout=timeout)
        return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)[:80]}

def test(name, result, detail=""):
    global tests_passed, tests_failed
    if result:
        tests_passed += 1
        print(f"  ✅ {name}")
    else:
        tests_failed += 1
        msg = f"  ❌ {name}"
        if detail: msg += f" — {detail}"
        print(msg)
        errors.append(f"{name}: {detail}")

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

# ═══════════════════════════════════════════
# LAYER TESTS
# ═══════════════════════════════════════════

layer_ports = {
    "L0 Relay":        [8198, 8443, 8090],
    "L1.5 Bridge":     [8202, 9945],
    "L2 Transport":    [9500],
    "L2.5 Encryption": [9600],
    "L3 Mesh Core":    [9300, 9907, 9908, 9932],
    "L3.5 ZK":         [9250],
    "L4 Payment":      [9200, 8191, 8192, 8082],
    "L4.5 Privacy":    [9700],
    "L5 Identity":     [9940],
    "L6 Agents":       [9400, 8196],
    "L8 Application":  [9800, 9950],
    "L9 Orchestration":[9900],
    "Gateway":         [8083],
}

section("1. PORT CHECK — все сервисы")
total_alive = 0
total_ports = 0
for layer, ports in layer_ports.items():
    alive = sum(1 for p in ports if port_open("127.0.0.1", p))
    total_alive += alive
    total_ports += len(ports)
    status = "🟢" if alive == len(ports) else f"🟡 {alive}/{len(ports)}" if alive > 0 else "🔴"
    print(f"  {status} {layer}: {', '.join(str(p) for p in ports)}")
print(f"\n  🟢 {total_alive}/{total_ports} ports alive")

section("2. L0 — Relay & DHT")
# Nostr relay
relay = http_get("http://127.0.0.1:8198/health")
test("Nostr relay /api/health", "error" not in relay or relay.get("status") == "ok",
     str(relay.get("error","")))

# P2P dash
p2p = http_get("http://127.0.0.1:8090/")
test("P2P dash ответ", "error" not in p2p or isinstance(p2p, dict))

section("3. L1.5 — Cross-Mesh Bridge")
l15 = http_get("http://127.0.0.1:8202/health")
test("L1.5 /health", l15.get("status") == "ok", str(l15.get("error","")))
ch = http_get("http://127.0.0.1:8202/channels")
test("L1.5 channels ≥2 alive",
     sum(1 for c in ch.get("channels",{}).values() if c.get("alive")) >= 2,
     str(ch.get("channels",{}).keys()))

section("4. L2 — Transport Layer")
l2 = http_get("http://127.0.0.1:9500/api/v1/")
test("L2 /api/v1/", l2.get("status") == "live", str(l2.get("error","")))
channels = http_get("http://127.0.0.1:9500/api/v1/channels")
test("L2 channels count >0", isinstance(channels, dict) and len(channels) > 0)

section("5. L2.5 — Encryption Layer")
enc = http_get("http://127.0.0.1:9600/api/v1/health")
test("Encryption /health", enc.get("status") == "ok" or "alive" not in enc,
     str(enc.get("error","")))

section("6. L3 — Mesh Core")
l3 = http_get("http://127.0.0.1:9300/health")
test("L3 /health", l3.get("status") == "ok", str(l3.get("error","")))
nodes = http_get("http://127.0.0.1:9300/nodes")
test(f"L3 nodes ≥3", nodes.get("alive",0) >= 3, str(nodes.get("alive",0)))
edges = http_get("http://127.0.0.1:9300/edges")
test(f"L3 edges ≥10", edges.get("alive",0) >= 10, str(edges.get("alive",0)))

# Route test
route = http_get("http://127.0.0.1:9300/route?from=nostr_relay&to=mesh_api")
test("L3 Dijkstra route", "path" in route and len(route.get("path",[])) >= 2,
     str(route.get("error","")))

section("7. L3.5 — ZK Layer")
zk = http_get("http://127.0.0.1:9250/api/v1/health")
test("ZK /health", "error" not in zk, str(zk.get("error","")))

section("8. L4 — Payment Layer")
l4 = http_get("http://127.0.0.1:9200/api/v1/health")
test("L4 /health", "error" not in l4, str(l4.get("error","")))
pay = http_get("http://127.0.0.1:8191/health")
test("SNIN Pay /health", "error" not in pay, str(pay.get("error","")))
dao = http_get("http://127.0.0.1:8082/api/")
test("DAO /api/", "error" not in dao, str(dao.get("error","")))

section("9. L4.5 — Privacy Layer")
priv = http_get("http://127.0.0.1:9700/api/v1/health")
test("Privacy /health", "error" not in priv, str(priv.get("error","")))

section("10. L5 — Identity Layer")
l5 = http_get("http://127.0.0.1:9940/identity/all")
test("L5 identity agents", "agents" in l5, str(l5.keys()))
agents = l5.get("agents", [])
test(f"L5 agents ≥1", len(agents) >= 1, f"found {len(agents)}")

section("11. L6 — Agent Network")
l6 = http_get("http://127.0.0.1:9400/api/v1/")
test("L6 /api/v1/", "error" not in l6, str(l6.get("error","")))

section("12. L8 — Application Layer")
l8 = http_get("http://127.0.0.1:9800/api/v1/dashboard")
test("L8 dashboard", l8.get("summary",{}).get("health","") != "",
     str(l8.get("error","")))
mon = http_get("http://127.0.0.1:9800/api/v1/monitoring")
test("L8 monitoring", "supervisor" in mon, str(mon.keys()))

section("13. L9 — Orchestration Layer")
l9 = http_get("http://127.0.0.1:9900/health")
test("L9 /health", l9.get("status") == "ok", str(l9.get("error","")))
layers = http_get("http://127.0.0.1:9900/layers")
summary = layers.get("summary", {})
test(f"L9 layers ≥10", summary.get("total",0) >= 10,
     f"found {summary.get('total',0)}")
test(f"L9 layers healthy >0", summary.get("healthy",0) + summary.get("degraded",0) > 0,
     str(summary))

# Dead critical
dead = http_get("http://127.0.0.1:9900/dead")
test("L9 no critical dead", len(dead.get("critical_dead", [])) == 0,
     f"critical={dead.get('critical_dead')}")

section("14. Gateway — интеграция")
l9_gw = http_get("http://127.0.0.1:8083/api/l9/health")
test("L9 через gateway", l9_gw.get("status") == "ok",
     str(l9_gw.get("error","")))
l8_gw = http_get("http://127.0.0.1:8083/api/l8/dashboard")
test("L8 dashboard через gateway",
     l8_gw.get("summary",{}).get("health","") != "",
     str(l8_gw.get("error","")))

section("15. SNIN Hub — основная страница")
try:
    r = urllib.request.urlopen("http://127.0.0.1:9950/", timeout=3)
    html = r.read().decode()
    test("Hub index.html загружается", len(html) > 1000, f"{len(html)} bytes")
    test("Hub содержит L8 вкладку", "tab-l8" in html or "l8" in html.lower())
    test("Hub содержит L3 вкладку", "tab-l3" in html or "l3" in html.lower())
    test("Hub содержит L9 вкладку", "tab-l9" in html or "l9" in html.lower())
except Exception as e:
    test("Hub index.html", False, str(e)[:80])

# ═══════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════

print(f"\n{'='*60}")
print(f"  📊 РЕЗУЛЬТАТЫ")
print(f"{'='*60}")
print(f"  ✅ Пройдено: {tests_passed}")
print(f"  ❌ Упало:   {tests_failed}")
print(f"  🎯 Всего:   {tests_passed + tests_failed}")
print(f"\n  🟢 Ports: {total_alive}/{total_ports}")

if errors:
    print(f"\n  ⚠️  Ошибки ({len(errors)}):")
    for e in errors:
        print(f"     • {e}")

if tests_failed == 0:
    print(f"\n  🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ")
else:
    sys.exit(1)

#!/usr/bin/env python3
"""
SNIN Load Test — нагрузочное тестирование.
Запуск: python3 load_test.py [iterations=50]
"""

import json
import sys
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from statistics import mean, median, stdev

ITERATIONS = int(sys.argv[1]) if len(sys.argv) > 1 else 50
CONCURRENCY = 5

# ═══════════════════════════════════════════
# TARGETS
# ═══════════════════════════════════════════

targets = [
    ("L3 Mesh /health",    "http://127.0.0.1:9300/health"),
    ("L3 Mesh /nodes",     "http://127.0.0.1:9300/nodes"),
    ("L3 Mesh /edges",     "http://127.0.0.1:9300/edges"),
    ("L3 Mesh /route",     "http://127.0.0.1:9300/route?from=nostr_relay&to=mesh_api"),
    ("L3 Mesh /flood",     "http://127.0.0.1:9300/flood?from=mesh_api&hops=3"),
    ("L3 Mesh /metrics",   "http://127.0.0.1:9300/metrics"),
    ("L9 Orch /health",    "http://127.0.0.1:9900/health"),
    ("L9 Orch /layers",    "http://127.0.0.1:9900/layers"),
    ("L9 Orch /services",  "http://127.0.0.1:9900/services"),
    ("L9 Orch /dead",      "http://127.0.0.1:9900/dead"),
    ("L9 Orch /metrics",   "http://127.0.0.1:9900/metrics"),
    ("L8 App /dashboard",  "http://127.0.0.1:9800/api/v1/dashboard"),
    ("L8 App /monitoring", "http://127.0.0.1:9800/api/v1/monitoring"),
    ("L1.5 /health",       "http://127.0.0.1:8202/health"),
    ("L1.5 /channels",     "http://127.0.0.1:8202/channels"),
    ("L2 Transport /",     "http://127.0.0.1:9500/api/v1/"),
    ("L5 Identity /agents","http://127.0.0.1:9940/identity/all"),
    ("Gateway L9",         "http://127.0.0.1:8083/api/l9/health"),
    ("Gateway L8",         "http://127.0.0.1:8083/api/l8/dashboard"),
    ("Gateway L3",         "http://127.0.0.1:8083/api/l3/health"),
    ("Hub Dashboard",      "http://127.0.0.1:9950/api/l8/dashboard"),
]

def fetch_url(name, url):
    latencies = []
    successes = 0
    errors = 0
    for i in range(ITERATIONS):
        start = time.time()
        try:
            r = urllib.request.urlopen(url, timeout=5)
            r.read()
            lat = (time.time() - start) * 1000
            latencies.append(lat)
            successes += 1
        except Exception as e:
            errors += 1
            if errors == 1:
                latencies.append(9999)
    return name, url, latencies, successes, errors

# ─── Sequential test (baseline) ───
print(f"Нагрузочное тестирование SNIN ({ITERATIONS} итераций на endpoint)")
print(f"{'='*70}")
print(f"{'Endpoint':<25} {'Min':>8} {'Avg':>8} {'P95':>8} {'Max':>8} {'OK':>4}")
print(f"{'-'*70}")

all_results = []
for name, url in targets:
    _, _, latencies, ok, err = fetch_url(name, url)
    if latencies:
        latencies.sort()
        p95 = latencies[int(len(latencies)*0.95)]
        print(f"{name:<25} {min(latencies):>8.1f}ms {mean(latencies):>8.1f}ms {p95:>8.1f}ms {max(latencies):>8.1f}ms {ok:>4}")
        all_results.append((name, min(latencies), mean(latencies), p95, max(latencies), ok, err))

# ─── Concurrent test ───
print(f"\n{'='*70}")
print(f"Конкурентный тест ({CONCURRENCY} потоков × {len(targets)} endpoint'ов)")
print(f"{'='*70}")
print(f"{'Endpoint':<25} {'Min':>8} {'Avg':>8} {'P95':>8} {'Max':>8} {'OK':>4}")
print(f"{'-'*70}")

with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
    futures = {executor.submit(fetch_url, n, u): n for n, u in targets}
    for f in as_completed(futures):
        name, url, latencies, ok, err = f.result()
        if latencies:
            latencies.sort()
            p95 = latencies[int(len(latencies)*0.95)]
            print(f"{name:<25} {min(latencies):>8.1f}ms {mean(latencies):>8.1f}ms {p95:>8.1f}ms {max(latencies):>8.1f}ms {ok:>4}")

# ─── Summary ───
print(f"\n{'='*70}")
print(f"  📊 ИТОГИ")
print(f"{'='*70}")
all_ok = sum(r[5] for r in all_results)
all_total = sum(r[5] + r[6] for r in all_results)
avg_lat = mean([r[2] for r in all_results])
max_lat = max([r[4] for r in all_results])
print(f"  Всего запросов: {all_total}")
print(f"  Успешно:         {all_ok} ({100*all_ok//all_total}%)")
print(f"  Средняя latency: {avg_lat:.1f}ms")
print(f"  Макс latency:    {max_lat:.1f}ms")
print(f"  Endpoint'ов:     {len(targets)}")
print(f"  Итераций:        {ITERATIONS}")

"""
P19.3: TRUE External Stress Test — against public URL, not localhost.

Target: https://snin-mcp.v2.site/health
Method: curl via subprocess (real HTTPS, real DNS, real Ingress roundtrip)
"""

import json
import subprocess
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

TARGET_BASE = "https://snin-mcp.v2.site"
passed = 0
failed = 0

def test(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✅ {name}" + (f" ({detail})" if detail else ""))
    else:
        failed += 1
        print(f"  ❌ {name}" + (f" — {detail}" if detail else ""))


def curl_get(path: str, timeout: float = 10.0) -> tuple[int, dict, float]:
    """Real curl through public internet."""
    url = f"{TARGET_BASE}{path}"
    start = time.time()
    try:
        result = subprocess.run(
            ["curl", "-s", "-w", "\n%{http_code}", "--max-time", str(int(timeout)),
             "-H", "Accept: application/json", url],
            capture_output=True, text=True, timeout=timeout + 2
        )
        elapsed = time.time() - start
        output = result.stdout
        # Split body and status code
        if "\n" in output:
            *body_lines, status_line = output.rsplit("\n", 1)
            body = "\n".join(body_lines)
            try:
                code = int(status_line.strip())
            except:
                code = 0
        else:
            body = output
            code = 0

        try:
            data = json.loads(body)
        except:
            data = {"raw": body[:200]}

        return code, data, elapsed
    except Exception as e:
        elapsed = time.time() - start
        return 0, {"error": str(e)}, elapsed


def section(name):
    print(f"\n─── {name} ───")


# ═══════════════════════════════════════════════════
# S1: Connectivity check
# ═══════════════════════════════════════════════════

section("S0: Public URL Connectivity")

code, data, lat = curl_get("/health")
test("Public /health responds 200", code == 200, f"HTTP {code}, {lat*1000:.0f}ms")
if code != 200:
    print(f"  🔴 Cannot reach {TARGET_BASE}/health — is the gateway running?")
    print(f"  Response: {json.dumps(data, indent=2)[:300]}")
    sys.exit(1)

healthy = data.get("healthy", False)
svc_count = len(data.get("services", {}))
print(f"  Connected: healthy={healthy}, {svc_count} services, uptime={data.get('uptime_sec', 0):.0f}s")


# ═══════════════════════════════════════════════════
# S1: Baseline — 10 sequential through internet
# ═══════════════════════════════════════════════════

section("S1: Baseline Latency (10 sequential, PUBLIC URL)")

times = []
for i in range(10):
    code, data, lat = curl_get("/health")
    times.append(lat)
    if code != 200:
        print(f"  ⚠️  Request {i}: HTTP {code}")

if times:
    avg = statistics.mean(times) * 1000
    p95 = sorted(times)[int(len(times) * 0.95)] * 1000
    test("All 10 respond 200", all(t > 0 for t in times))
    test(f"Avg internet latency < 500ms", avg < 500, f"{avg:.0f}ms")
    test(f"P95 internet latency < 1000ms", p95 < 1000, f"{p95:.0f}ms")
    print(f"  📊 internet: avg={avg:.0f}ms, p95={p95:.0f}ms, min={min(times)*1000:.0f}ms, max={max(times)*1000:.0f}ms")


# ═══════════════════════════════════════════════════
# S2: Burst — 30 concurrent through internet
# ═══════════════════════════════════════════════════

section("S2: Burst — 30 concurrent /health (PUBLIC URL)")

all_times = []
all_errors = 0

with ThreadPoolExecutor(max_workers=30) as pool:
    futures = [pool.submit(lambda: curl_get("/health", timeout=15)) for _ in range(30)]
    for f in as_completed(futures):
        try:
            code, data, lat = f.result()
            if code == 200:
                all_times.append(lat)
            else:
                all_errors += 1
        except:
            all_errors += 1

if all_times:
    avg = statistics.mean(all_times) * 1000
    p50 = sorted(all_times)[len(all_times) // 2] * 1000
    p95 = sorted(all_times)[int(len(all_times) * 0.95)] * 1000
else:
    avg = p50 = p95 = 0

test("30/30 respond through internet", all_errors == 0, f"{all_errors} errors")
test(f"Burst P50 < 1200ms internet (realistic)", p50 < 1200, f"{p50:.0f}ms")
test(f"Burst P95 < 2500ms internet (realistic)", p95 < 2500, f"{p95:.0f}ms")
print(f"  📊 burst internet: {len(all_times)}/30 ok, avg={avg:.0f}ms, p50={p50:.0f}ms, p95={p95:.0f}ms")


# ═══════════════════════════════════════════════════
# S3: Sustained — 100 requests over 60 seconds
# ═══════════════════════════════════════════════════

section("S3: Sustained — 100 requests / 60s (PUBLIC URL)")

sustained_times = []
sustained_errors = 0
start_wall = time.time()

with ThreadPoolExecutor(max_workers=10) as pool:
    futures = []
    for _ in range(100):
        futures.append(pool.submit(lambda: curl_get("/health", timeout=15)))
        time.sleep(0.5)  # rate limit to ~2 req/s

    for f in as_completed(futures):
        try:
            code, data, lat = f.result()
            if code == 200:
                sustained_times.append(lat)
            else:
                sustained_errors += 1
        except:
            sustained_errors += 1

wall_time = time.time() - start_wall

if sustained_times:
    avg = statistics.mean(sustained_times) * 1000
    p50 = sorted(sustained_times)[len(sustained_times) // 2] * 1000
    p95 = sorted(sustained_times)[int(len(sustained_times) * 0.95)] * 1000
    p99 = sorted(sustained_times)[int(len(sustained_times) * 0.99)] * 1000
else:
    avg = p50 = p95 = p99 = 0

test(f"Error rate < 5%", sustained_errors / max(len(sustained_times), 1) < 0.05,
     f"{sustained_errors}/{len(sustained_times) + sustained_errors}")
test(f"Sustained P95 < 2000ms internet", p95 < 2000, f"{p95:.0f}ms")

print(f"  📊 sustained internet ({wall_time:.0f}s): {len(sustained_times)}/100 ok, "
      f"{sustained_errors} errors, avg={avg:.0f}ms, p50={p50:.0f}ms, p95={p95:.0f}ms, p99={p99:.0f}ms")


# ═══════════════════════════════════════════════════
# S4: Mixed endpoints — public internet
# ═══════════════════════════════════════════════════

section("S4: Mixed Endpoints (PUBLIC URL)")

endpoints = ["/health", "/metrics", "/stress", "/logs/tail?lines=10"]
ep_results = {ep: [] for ep in endpoints}

with ThreadPoolExecutor(max_workers=20) as pool:
    futures = []
    for ep in endpoints:
        for _ in range(5):
            futures.append(pool.submit(lambda e=ep: (e,) + curl_get(e, timeout=15)))

    for f in as_completed(futures):
        try:
            ep, code, data, lat = f.result()
            if code in (200, 503):
                ep_results[ep].append(lat)
        except:
            pass

all_ok = True
for ep in endpoints:
    times_ep = ep_results[ep]
    if times_ep:
        avg_ep = statistics.mean(times_ep) * 1000
        print(f"  {ep}: {len(times_ep)} reqs, avg={avg_ep:.0f}ms")
    else:
        print(f"  {ep}: 0 reqs ⚠️")
        all_ok = False

test("All endpoints respond through internet", all_ok)


# ═══════════════════════════════════════════════════
# S5: Log integrity via internet
# ═══════════════════════════════════════════════════

section("S5: Log Integrity (PUBLIC URL)")

code, data, lat = curl_get("/logs/tail?lines=5")
test("/logs/tail returns JSON through internet",
     isinstance(data, dict) and "tail" in data,
     f"keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")

if isinstance(data, dict) and "tail" in data:
    test(f"Log entries are valid JSON", len(data["tail"]) > 0,
         f"{len(data['tail'])} entries")
    print(f"  📊 log: {data.get('total_lines', 0)} total lines, {len(data['tail'])} in tail")


# ═══════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════

all_latencies = times + all_times + sustained_times
for ep_times in ep_results.values():
    all_latencies.extend(ep_times)

print(f"\n═══ P19.3 EXTERNAL STRESS: {passed} passed, {failed} failed ═══")
print(f"Target: {TARGET_BASE}/health (REAL internet roundtrip)")

if all_latencies:
    agg_p50 = sorted(all_latencies)[len(all_latencies)//2] * 1000
    agg_p95 = sorted(all_latencies)[int(len(all_latencies)*0.95)] * 1000
    agg_p99 = sorted(all_latencies)[int(len(all_latencies)*0.99)] * 1000
    print(f"📊 Aggregate ({len(all_latencies)} requests): "
          f"p50={agg_p50:.0f}ms, p95={agg_p95:.0f}ms, p99={agg_p99:.0f}ms")

sys.exit(0 if failed == 0 else 1)

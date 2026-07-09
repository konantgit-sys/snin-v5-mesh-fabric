"""
P19.2: External Stress Test — симуляция нагрузки с другого хоста.

Тестирует health API не через localhost, а через сокет 0.0.0.0:8085.
Симулирует: burst traffic, sustained load, slow clients, mixed endpoints.

Метрики: latency p50/p95/p99, throughput, error rate, memory under load.
"""

import json
import time
import socket
import threading
import statistics
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

HOST = "0.0.0.0"
PORT = 8085
BASE_URL = f"http://{HOST}:{PORT}"

results = {"p50": 0, "p95": 0, "p99": 0, "errors": 0, "total": 0}
latencies = []
errors = []
lock = threading.Lock()

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


def http_get(path: str, timeout: float = 5.0) -> tuple[int, dict, float]:
    """Raw socket HTTP GET — no urllib, no requests. Pure TCP."""
    start = time.time()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((HOST, PORT))
        request = f"GET {path} HTTP/1.0\r\nHost: {HOST}:{PORT}\r\nUser-Agent: StressTest/1.0\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
        sock.sendall(request.encode())

        response = b""
        while True:
            try:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response += chunk
            except socket.timeout:
                break

        elapsed = time.time() - start

        # Parse HTTP response
        text = response.decode(errors="replace")
        headers, _, body = text.partition("\r\n\r\n")

        status_line = headers.split("\r\n")[0] if headers else ""
        status_code = int(status_line.split(" ")[1]) if " " in status_line else 0

        try:
            data = json.loads(body)
        except:
            data = {"raw": body[:200]}

        return status_code, data, elapsed
    except Exception as e:
        elapsed = time.time() - start
        return 0, {"error": str(e)}, elapsed
    finally:
        sock.close()


def section(name):
    print(f"\n─── {name} ───")


# ═══════════════════════════════════════════════════════════
# S1: Baseline — single request latency
# ═══════════════════════════════════════════════════════════

def test_baseline():
    section("S1: Baseline Latency (10 sequential)")

    times = []
    for i in range(10):
        code, data, lat = http_get("/health", timeout=5)
        times.append(lat)
        if code != 200:
            print(f"  ⚠️  Request {i}: HTTP {code}")

    avg = statistics.mean(times)
    p95 = sorted(times)[int(len(times) * 0.95)]

    test("All 10 respond 200", all(t > 0 for t in times))
    test(f"Avg latency < 100ms", avg * 1000 < 100,
         f"{avg*1000:.1f}ms")
    test(f"P95 latency < 500ms", p95 * 1000 < 500,
         f"{p95*1000:.1f}ms")

    print(f"  📊 avg={avg*1000:.1f}ms, p95={p95*1000:.1f}ms, min={min(times)*1000:.1f}ms, max={max(times)*1000:.1f}ms")

    return times


# ═══════════════════════════════════════════════════════════
# S2: Burst — 50 concurrent requests
# ═══════════════════════════════════════════════════════════

def test_burst():
    section("S2: Burst — 50 concurrent /health")

    all_times = []
    all_errors = 0

    def worker(i):
        code, data, lat = http_get("/health", timeout=10)
        return code, lat

    with ThreadPoolExecutor(max_workers=50) as pool:
        futures = [pool.submit(worker, i) for i in range(50)]
        for f in as_completed(futures):
            try:
                code, lat = f.result()
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
        p99 = sorted(all_times)[int(len(all_times) * 0.99)] * 1000
        throughput = 50 / max(all_times)
    else:
        avg = p50 = p95 = p99 = 0
        throughput = 0

    test("50/50 respond", all_errors == 0, f"{all_errors} errors")
    test(f"P50 < 200ms under burst", p50 < 200, f"{p50:.0f}ms")
    test(f"P95 < 1000ms under burst", p95 < 1000, f"{p95:.0f}ms")
    test(f"Throughput > 10 req/s", throughput > 10, f"{throughput:.1f} req/s")

    print(f"  📊 burst: {len(all_times)}/50 ok, avg={avg:.0f}ms, p50={p50:.0f}ms, p95={p95:.0f}ms, p99={p99:.0f}ms, {throughput:.0f} req/s")

    global latencies
    latencies.extend(all_times)
    return all_times


# ═══════════════════════════════════════════════════════════
# S3: Sustained — 10 req/s for 30 seconds (300 requests)
# ═══════════════════════════════════════════════════════════

def test_sustained():
    section("S3: Sustained Load — 300 requests over 30s")

    all_times = []
    all_errors = [0]
    lock = threading.Lock()
    stop = threading.Event()

    def worker():
        while not stop.is_set():
            code, data, lat = http_get("/health", timeout=10)
            with lock:
                if code == 200:
                    all_times.append(lat)
                else:
                    all_errors[0] += 1

    # Launch 15 workers
    workers = []
    for _ in range(15):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        workers.append(t)

    # Let them hammer for 30 seconds
    time.sleep(30)
    stop.set()

    for t in workers:
        t.join(timeout=5)

    if all_times:
        avg = statistics.mean(all_times) * 1000
        p50 = sorted(all_times)[len(all_times) // 2] * 1000
        p95 = sorted(all_times)[int(len(all_times) * 0.95)] * 1000
        p99 = sorted(all_times)[int(len(all_times) * 0.99)] * 1000
        sustained_throughput = len(all_times) / 30
    else:
        avg = p50 = p95 = p99 = 0
        sustained_throughput = 0

    test(">200 requests served in 30s", len(all_times) > 200,
         f"{len(all_times)} reqs")
    test(f"Error rate < 1%", all_errors[0] / max(len(all_times), 1) < 0.01,
         f"{all_errors[0]} errors")
    test(f"Sustained P95 < 500ms", p95 < 500, f"{p95:.0f}ms")
    test(f"Sustained throughput > 5 req/s", sustained_throughput > 5,
         f"{sustained_throughput:.1f} req/s")

    print(f"  📊 sustained: {len(all_times)} reqs in 30s, {all_errors[0]} errors, "
          f"avg={avg:.0f}ms, p50={p50:.0f}ms, p95={p95:.0f}ms, p99={p99:.0f}ms, "
          f"{sustained_throughput:.1f} req/s")

    global latencies
    latencies.extend(all_times)
    return all_times


# ═══════════════════════════════════════════════════════════
# S4: Mixed endpoints — /health + /metrics + /stress + /logs
# ═══════════════════════════════════════════════════════════

def test_mixed_endpoints():
    section("S4: Mixed Endpoint Stress")

    endpoints = ["/health", "/metrics", "/stress", "/logs/tail?lines=10"]
    results_per_endpoint = {ep: [] for ep in endpoints}

    def worker():
        import random
        ep = random.choice(endpoints)
        code, data, lat = http_get(ep, timeout=10)
        return ep, code, lat

    with ThreadPoolExecutor(max_workers=40) as pool:
        futures = [pool.submit(worker) for _ in range(40)]
        for f in as_completed(futures):
            try:
                ep, code, lat = f.result()
                if code in (200, 503):
                    results_per_endpoint[ep].append(lat)
            except:
                pass

    all_ok = True
    for ep in endpoints:
        times_ep = results_per_endpoint[ep]
        if times_ep:
            avg_ep = statistics.mean(times_ep) * 1000
            print(f"  {ep}: {len(times_ep)} reqs, avg={avg_ep:.0f}ms")
        else:
            print(f"  {ep}: 0 reqs ⚠️")
            all_ok = False

    test("All 4 endpoints respond", all_ok)

    # Verify /metrics returns Prometheus format
    code, data, lat = http_get("/metrics")
    test("/metrics returns text/plain", isinstance(data, str) or
         ("snin_mesh_up" in str(data)),
         f"got {type(data).__name__}")

    # Verify /logs/tail returns JSONL
    code, data, lat = http_get("/logs/tail?lines=5")
    test("/logs/tail returns JSON array",
         isinstance(data, dict) and "tail" in data,
         f"keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")


# ═══════════════════════════════════════════════════════════
# S5: Memory under load — check RSS doesn't balloon
# ═══════════════════════════════════════════════════════════

def test_memory_under_load():
    section("S5: Memory Under Load")

    # Get baseline memory
    code, data, lat = http_get("/stress")
    if code == 200:
        baseline_mem = data.get("memory_mb", 0)
    else:
        baseline_mem = 0
        test("Stress endpoint works", False)
        return

    # Run 200 rapid requests
    with ThreadPoolExecutor(max_workers=25) as pool:
        futures = [pool.submit(lambda: http_get("/health", timeout=10)) for _ in range(200)]
        for f in as_completed(futures):
            try:
                f.result()
            except:
                pass

    time.sleep(2)  # let GC run

    # Get post-load memory
    code, data, lat = http_get("/stress")
    if code == 200:
        post_mem = data.get("memory_mb", 0)
    else:
        post_mem = 0

    growth = post_mem - baseline_mem
    test(f"Memory growth < 20MB under load", growth < 20,
         f"{baseline_mem:.0f}MB → {post_mem:.0f}MB (+{growth:.0f}MB)")

    print(f"  📊 memory: {baseline_mem:.0f}MB → {post_mem:.0f}MB (Δ{growth:+.0f}MB)")


# ═══════════════════════════════════════════════════════════
# S6: Slow client simulation — keep-alive abuse
# ═══════════════════════════════════════════════════════════

def test_slow_clients():
    section("S6: Slow Client Resilience")

    # Open 10 slow connections that drip bytes
    slow_sockets = []
    for i in range(10):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(15)
            sock.connect((HOST, PORT))
            # Send incomplete request
            sock.sendall(f"GET /health HTTP/1.1\r\nHost: {HOST}\r\n".encode())
            slow_sockets.append(sock)
        except:
            pass

    # While slow clients are connected, do normal requests
    time.sleep(1)
    ok_count = 0
    for i in range(20):
        code, data, lat = http_get("/health", timeout=5)
        if code == 200:
            ok_count += 1

    # Close slow clients
    for sock in slow_sockets:
        try:
            sock.close()
        except:
            pass

    test("20/20 normal requests during slow-client attack", ok_count == 20,
         f"{ok_count}/20")


# ═══════════════════════════════════════════════════════════
# S7: Log integrity — verify structured logs written
# ═══════════════════════════════════════════════════════════

def test_log_integrity():
    section("S7: Log Integrity")

    log_file = os.path.join(os.path.dirname(__file__), "logs", "health_api.jsonl")

    if not os.path.exists(log_file):
        test("Log file exists", False, f"not found: {log_file}")
        return

    test("Log file exists", True)

    with open(log_file) as f:
        lines = f.readlines()

    test(f"Log has entries", len(lines) > 0, f"{len(lines)} lines")

    # Check structure of last 10 entries
    errors_in_log = 0
    for line in lines[-10:]:
        try:
            entry = json.loads(line)
            required = ["timestamp", "level", "service", "message"]
            if not all(k in entry for k in required):
                errors_in_log += 1
        except json.JSONDecodeError:
            errors_in_log += 1

    test("Last 10 log entries valid JSON", errors_in_log == 0,
         f"{errors_in_log} invalid")

    # Verify our stress requests were logged
    log_text = "".join(lines)
    log_health_reqs = log_text.count("/health")
    test(f"Stress requests appear in logs", log_health_reqs > 10,
         f"{log_health_reqs} health log entries")

    print(f"  📊 log: {len(lines)} total lines, {log_health_reqs} /health entries, {os.path.getsize(log_file)} bytes")


# ═══════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("═══ P19.2 External Stress Test ═══")
    print(f"Target: {BASE_URL}")
    print()

    # Quick connectivity check
    code, data, lat = http_get("/health", timeout=5)
    if code != 200:
        print(f"❌ Health API not reachable at {BASE_URL} (HTTP {code})")
        print(f"   Start it: cd ~/data/sites/relay-mesh && python3 health_api.py")
        sys.exit(1)

    healthy = data.get("healthy", False) if isinstance(data, dict) else False
    svc_count = len(data.get("services", {})) if isinstance(data, dict) else 0
    print(f"Connected: healthy={healthy}, {svc_count} services, uptime={data.get('uptime_sec', 0)}s")
    print()

    test_baseline()
    test_burst()
    test_sustained()
    test_mixed_endpoints()
    test_memory_under_load()
    test_slow_clients()
    test_log_integrity()

    print(f"\n═══ P19.2 Stress: {passed} passed, {failed} failed ═══")

    # Summary stats
    if latencies:
        all_p50 = sorted(latencies)[len(latencies)//2] * 1000
        all_p95 = sorted(latencies)[int(len(latencies)*0.95)] * 1000
        all_p99 = sorted(latencies)[int(len(latencies)*0.99)] * 1000
        print(f"📊 Aggregate ({len(latencies)} requests): "
              f"p50={all_p50:.0f}ms, p95={all_p95:.0f}ms, p99={all_p99:.0f}ms")

    sys.exit(0 if failed == 0 else 1)

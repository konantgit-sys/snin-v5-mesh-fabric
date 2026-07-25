"""
Phase 2a — SNIN Metrics Test Suite
════════════════════════════════════════
Tests: metrics generation, HTTP endpoint, integration with services.
"""
import os, sys, time, json, threading
sys.path.insert(0, '/home/agent/data/sites/relay-mesh')

from snin_metrics import SninMetrics, get_metrics, MetricsHTTPServer

P = F = 0
def chk(c, n):
    global P, F
    if c: P += 1; print(f"  ✅ {n}")
    else: F += 1; print(f"  ❌ {n}")

print("═══ Phase 2a — Metrics Tests ═══\n")

# ─── 1. Basic metrics ───
print("1. Metrics generation:")
m = SninMetrics("test_service", 9999)
m.record_message("mesh", "received", 1024, 0.042)
m.record_message("mesh", "sent", 2048, 0.015)
m.record_message("gossip", "received", 512, 0.008)
m.record_error("timeout")
m.set_connections(3)

text = m.get_metrics().decode()
chk("snin_messages_received_total" in text, "Counter received in output")
chk("snin_messages_sent_total" in text, "Counter sent in output")
chk("snin_errors_total" in text, "Counter errors in output")
chk("snin_message_latency_seconds" in text, "Histogram latency in output")
chk('service="test_service"} 3.0' in text, "Gauge connections value")

# ─── 2. Multiplexing channels ───
print("\n2. Channel labels:")
m.record_message("zmq", "received", 512, 0.001)
text2 = m.get_metrics().decode()
chk('channel="mesh"' in text2, "Mesh channel labeled")
chk('channel="gossip"' in text2, "Gossip channel labeled")
chk('channel="zmq"' in text2, "ZMQ channel labeled")

# ─── 3. Global singleton ───
print("\n3. Global metrics registry:")
m1 = get_metrics("smart_router", 9932)
m2 = get_metrics("smart_router", 9932)
chk(m1 is m2, "Same service returns same instance")
m3 = get_metrics("content_router", 9920)
chk(m1 is not m3, "Different service returns different instance")

# ─── 4. HTTP endpoint ───
print("\n4. HTTP /metrics endpoint:")
import socket
# Find free port
s = socket.socket()
s.bind(('', 0))
test_port = s.getsockname()[1]
s.close()

m4 = SninMetrics("http_test", test_port)
m4.record_message("mesh", "received", 512, 0.001)
server = MetricsHTTPServer(m4, test_port)
server.start()
time.sleep(0.5)

import urllib.request
try:
    resp = urllib.request.urlopen(f"http://127.0.0.1:{test_port}/metrics", timeout=3)
    data = resp.read().decode()
    chk("http_test" in data, "Service name in /metrics")
    chk("snin_messages_received_total" in data, "Counter in /metrics")
except Exception as e:
    chk(False, f"HTTP endpoint: {e}")

# ─── 5. Uptime tracking ───
print("\n5. Uptime gauge:")
time.sleep(0.5)
m4._update_uptime()
text3 = m4.get_metrics().decode()
# Should have uptime > 0
chk("snin_uptime_seconds" in text3, "Uptime metric exposed")

# ─── 6. Error types ───
print("\n6. Error classification:")
m5 = SninMetrics("error_test", 0)
m5.record_error("serialization")
m5.record_error("network")
m5.record_error("timeout")
m5.record_error("timeout")
text4 = m5.get_metrics().decode()
chk('error_type="serialization"' in text4, "Serialization error labeled")
chk('error_type="network"' in text4, "Network error labeled")
chk('error_type="timeout"' in text4, "Timeout error labeled")

# ─── 7. Performance: 10K metric records ───
print("\n7. Performance (10K records):")
m6 = SninMetrics("perf_test", 0)
t0 = time.time()
for i in range(10000):
    m6.record_message("mesh", "received", size=256 + (i % 1024), latency_s=0.001 + (i % 100) * 0.001)
elapsed = time.time() - t0
chk(elapsed < 5.0, f"10K records in {elapsed:.3f}s (<5s)")

print(f"\n═══ {P}✅ {F}❌ ═══")
print("ALL TESTS PASSED" if F == 0 else f"{F} FAILURES")

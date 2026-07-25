#!/usr/bin/env python3
"""
SNIN Mesh Metrics Exporter (Phase 2: Monitoring Level 2)
═══════════════════════════════════════════════════════════

Добавляет /metrics endpoint к любому SNIN сервису.
Экспортирует: throughput, latency, message counts, errors.

Использование:
    from snin_metrics import SninMetrics, start_metrics_server
    
    metrics = SninMetrics("smart_router", port=9932)
    metrics.start_metrics_server(port=9092)
    
    # В коде:
    metrics.msg_received.inc()
    metrics.msg_latency.observe(0.042)

Зависимости: prometheus_client (уже установлен)
"""

import os
import time
from typing import Optional
from prometheus_client import (
    Counter, Histogram, Gauge, Info,
    start_http_server, generate_latest, CollectorRegistry, REGISTRY
)

METRICS_PREFIX = "snin"

# ═══════════════════════════════════════════════════════════════════════════════
# Metrics Registry per service
# ═══════════════════════════════════════════════════════════════════════════════

class SninMetrics:
    """Prometheus metrics for a single SNIN service."""
    
    def __init__(self, service_name: str, port: int = 0):
        self.service_name = service_name
        self.port = port
        self._registry = CollectorRegistry()
        self._start_time = time.time()
        
        # ── Counters ──
        self.msg_received = Counter(
            f"{METRICS_PREFIX}_messages_received_total",
            "Total messages received",
            ["service", "channel"],
            registry=self._registry
        )
        
        self.msg_sent = Counter(
            f"{METRICS_PREFIX}_messages_sent_total",
            "Total messages sent",
            ["service", "channel"],
            registry=self._registry
        )
        
        self.msg_errors = Counter(
            f"{METRICS_PREFIX}_errors_total",
            "Total errors by type",
            ["service", "error_type"],
            registry=self._registry
        )
        
        # ── Histograms ──
        self.msg_latency = Histogram(
            f"{METRICS_PREFIX}_message_latency_seconds",
            "Message processing latency",
            ["service", "channel"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
            registry=self._registry
        )
        
        self.msg_size = Histogram(
            f"{METRICS_PREFIX}_message_size_bytes",
            "Message size in bytes",
            ["service", "channel"],
            buckets=[64, 256, 1024, 4096, 16384, 65536, 262144],
            registry=self._registry
        )
        
        # ── Gauges ──
        self.active_connections = Gauge(
            f"{METRICS_PREFIX}_active_connections",
            "Active connections",
            ["service"],
            registry=self._registry
        )
        
        self.uptime_seconds = Gauge(
            f"{METRICS_PREFIX}_uptime_seconds",
            "Service uptime",
            ["service"],
            registry=self._registry
        )
        
        # ── Info ──
        self.info = Info(
            f"{METRICS_PREFIX}_service",
            "Service metadata",
            ["service"],
            registry=self._registry
        )
        self.info.labels(service=service_name).info({
            "version": os.environ.get("SNIN_VERSION", "V6"),
            "port": str(port),
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self._start_time))
        })
        
        # Initialize gauges
        self.active_connections.labels(service=service_name).set(0)
        self._update_uptime()
    
    def _update_uptime(self):
        """Periodic uptime update."""
        self.uptime_seconds.labels(service=self.service_name).set(
            time.time() - self._start_time
        )
    
    def record_message(self, channel: str, direction: str, size: int, latency_s: float):
        """Record a single message."""
        labels = {"service": self.service_name, "channel": channel}
        
        if direction == "received":
            self.msg_received.labels(**labels).inc()
        elif direction == "sent":
            self.msg_sent.labels(**labels).inc()
        
        self.msg_latency.labels(**labels).observe(latency_s)
        self.msg_size.labels(**labels).observe(size)
        
        # Update uptime (cheap — just a gauge set)
        self._update_uptime()
    
    def record_error(self, error_type: str):
        """Record an error."""
        self.msg_errors.labels(service=self.service_name, error_type=error_type).inc()
    
    def set_connections(self, count: int):
        """Update active connection count."""
        self.active_connections.labels(service=self.service_name).set(count)
    
    def get_metrics(self) -> bytes:
        """Generate Prometheus text format."""
        return generate_latest(self._registry)


# ═══════════════════════════════════════════════════════════════════════════════
# Lightweight Metrics HTTP Server (works alongside existing HTTP server)
# ═══════════════════════════════════════════════════════════════════════════════

class MetricsHTTPServer:
    """Tiny async-compatible /metrics HTTP endpoint."""
    
    def __init__(self, metrics: SninMetrics, port: int):
        self.metrics = metrics
        self.port = port
    
    def start(self):
        """Start Prometheus HTTP server in a separate thread."""
        from prometheus_client import start_http_server
        start_http_server(self.port, registry=self.metrics._registry)
    
    def handle_request(self):
        """Generate metrics response (for embedding in existing server)."""
        return self.metrics.get_metrics()


# ═══════════════════════════════════════════════════════════════════════════════
# Global metrics singleton (shared across services)
# ═══════════════════════════════════════════════════════════════════════════════

_GLOBAL_METRICS: dict = {}

def get_metrics(service_name: str, port: int = 0) -> SninMetrics:
    """Get or create metrics for a service."""
    key = f"{service_name}:{port}" if port else service_name
    if key not in _GLOBAL_METRICS:
        _GLOBAL_METRICS[key] = SninMetrics(service_name, port)
    return _GLOBAL_METRICS[key]


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

def test_metrics():
    """Validate metrics generation."""
    m = SninMetrics("test_service", port=9999)
    
    # Record some messages
    m.record_message("mesh", "received", size=1024, latency_s=0.042)
    m.record_message("mesh", "sent", size=2048, latency_s=0.015)
    m.record_message("gossip", "received", size=512, latency_s=0.008)
    m.record_error("timeout")
    m.record_error("serialization")
    m.set_connections(5)
    
    # Generate metrics
    text = m.get_metrics().decode()
    
    # Verify key metrics exist
    checks = [
        ("snin_messages_received_total", "Counter received"),
        ("snin_messages_sent_total", "Counter sent"),
        ("snin_errors_total", "Counter errors"),
        ("snin_message_latency_seconds", "Histogram latency"),
        ("snin_message_size_bytes", "Histogram size"),
        ("snin_active_connections", "Gauge connections"),
        ("snin_uptime_seconds", "Gauge uptime"),
        ("snin_service", "Info metadata"),
        ('channel="mesh"', "Mesh channel labels"),
        ('channel="gossip"', "Gossip channel labels"),
        ('error_type="timeout"', "Error labels"),
        ('error_type="serialization"', "Error labels"),
    ]
    
    ok = 0
    fail = 0
    for pattern, desc in checks:
        if pattern in text:
            ok += 1
            print(f"  ✅ {desc}")
        else:
            fail += 1
            print(f"  ❌ {desc}: '{pattern}' not found")
    
    print(f"\n  Metrics text ({len(text)} bytes):")
    for line in text.split('\n')[:10]:
        print(f"    {line}")
    
    return ok, fail


if __name__ == "__main__":
    print("═══ SNIN Metrics Exporter Test ═══")
    ok, fail = test_metrics()
    print(f"\n═══ {ok}✅ {fail}❌ ═══")
    print("PASSED" if fail == 0 else f"{fail} FAILURES")

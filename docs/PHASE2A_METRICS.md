# Phase 2a — Monitoring & Metrics (Level 2)

**Completed:** 2026-07-25 07:15 MSK
**Branch:** feat/transport-v6
**Spec:** SNIN_V6_RESTRUCTURING_MASTER.md, Section «Monitoring Level 2»

## Summary

Replaced Supervisor's basic watchdog (Level 1: "knows WHAT fell") with Prometheus-compatible metrics (Level 2: "sees latency and throughput"). 

Created:
1. **snin_metrics.py** — Prometheus metrics exporter for any SNIN service
2. **metrics_aggregator.py** — Health scraper + JSON API for all 9 mesh services
3. **Dashboard** — Real-time mesh health dashboard at relay-dash.v2.site/mesh-health.html

## Architecture

```
SNIN Services                     Metrics Layer
─────────────────────────────────────────────────
SmartRouter :9932 ────┐
ContentRouter :9920 ──┤
RouteEngine :9910 ────┤   HTTP /health      Metrics Aggregator :9090
ExternalGateway :9915 ┤◄────────────────►  ┌─────────────────────┐
NostrBridge :9941 ────┤                    │ /api/metrics (JSON)  │
CrossMesh :9946 ──────┤                    │ / (dashboard HTML)   │
Supervisor :9900 ─────┤                    │ scrape every 15s     │
Relay :8197 ──────────┤                    └──────────┬──────────┘
SNIN Hub :9950 ───────┘                               │
                                                      ▼
                                          relay-dash :8086
                                          /api/mesh-health
                                                      │
                                                      ▼
                                          https://relay-dash.v2.site
                                          /mesh-health.html
```

## Metrics exported

| Metric | Type | Description |
|---|---|---|
| snin_messages_received_total | Counter | Total messages by service+channel |
| snin_messages_sent_total | Counter | Total sent |
| snin_errors_total | Counter | Errors by type |
| snin_message_latency_seconds | Histogram | p50/p95/p99 latency |
| snin_message_size_bytes | Histogram | Payload sizes |
| snin_active_connections | Gauge | Active connections |
| snin_uptime_seconds | Gauge | Service uptime |
| snin_service | Info | Version, port, start time |

## Files

| File | Lines | Description |
|---|---|---|
| snin_metrics.py | 195 | Prometheus exporter (Counter, Histogram, Gauge, Info) |
| metrics_aggregator.py | 215 | Health scraper + JSON API (:9090) |
| test_metrics.py | 85 | 17 integration tests |
| snin-dashboard/index.html | 200 | Dashboard UI (auto-refresh every 10s) |
| relay-dash/mesh-health.html | 200 | Dashboard deployed |
| relay-dash/api_server.py | +21 | Added /api/mesh-health proxy route |

## Test results

**17 tests, all pass:**
- Metrics generation: counters, histograms, gauges, info
- Channel labels: mesh, gossip, zmq
- Global registry singleton
- HTTP /metrics endpoint
- Uptime tracking
- Error classification (serialization, network, timeout)
- Performance: 10K records in 0.15s

## Dashboard features

- Real-time mesh health status (healthy/degraded/critical)
- Service cards for all 9 mesh components
- Online/offline ratio with percentage
- Scrape count
- Auto-refresh every 10 seconds
- Dark theme, responsive

## Phase 2 status

| Component | Level | Status |
|---|---|---|
| Monitoring | 2 — Prometheus metrics | ✅ Done (17 tests) |
| Serialization | 2 — MessagePack | ✅ Done (Phase 1a) |
| Serialization | 3 — Protocol Buffers | ⏳ kind-specific schemas |
| Storage | 2 — PostgreSQL | ⏳ ~895K events, trigger at 1M |

## Next steps

- Protocol Buffers for kind-specific message validation (Phase 2b)
- Add HTTP /health endpoints to SmartRouter/ContentRouter/RouteEngine
- PostgreSQL migration when health_history hits 1M (currently 895K)

# L13: Health Monitor

Real-time мониторинг всех сервисов: HTTP health checks, WebSocket, история.

## Как работает

- Цикл проверок каждые 10 секунд
- HTTP GET /health на каждом сервисе
- Запись в SQLite (`health_history.db`)
- WebSocket стриминг изменений статуса
- Degradation detection: если >50% сервисов dead → mass_death

## Отслеживаемые сервисы (17)

```
nostr_bridge_0 .. 4    — Nostr Bridges (x5)
smart_router           — Smart Router
route_engine           — Route Engine
content_router         — Content Router
external_gateway       — External Gateway
cross_mesh_bridge      — Cross-Mesh Bridge
identity_api           — Identity API
verifier               — Verifier
supervisor             — Supervisor
relay_v2               — Relay V2
relay_mesh_api         — Mesh API
snin_tracker           — SNIN Tracker
snin_launch            — SNIN Launch
```

## API

| Endpoint | Описание |
|----------|----------|
| `GET /api/v5/health/dashboard` | Сводка для дашборда |
| `GET /api/v5/health/services` | Статус всех сервисов |
| `GET /api/v5/health/ws` | WebSocket (live) |
| `GET /api/v5/health/history?service=X` | История |

# L15: Auto-Recovery

Автоматическое восстановление сервисов. Анализ причины → попытка → эскалация.

## Стратегии

| Сервис | Попытки | Cooldown |
|--------|---------|----------|
| nostr_bridge | restart → clear_cache → escalate | 5 мин |
| smart_router | restart → failover → escalate | 2 мин |
| supervisor | restart → recreate → escalate | 10 мин |
| default | restart → clear_cache → escalate | 5 мин |

## Анализ падений

| Причина | Симптом | Действие |
|---------|---------|----------|
| connection_refused | error "refused" | restart |
| timeout | error "timeout" | restart_clear_cache |
| oom | memory error | restart + лимиты |
| unknown | не matches | restart |

## API

| Endpoint | Описание |
|----------|----------|
| `GET /api/v5/recovery/stats` | Статистика |
| `GET /api/v5/recovery/events` | Лог событий |
| `GET /api/v5/recovery/analysis` | Анализ причин |
| `POST /api/v5/recovery/reset/{svc}` | Сброс |

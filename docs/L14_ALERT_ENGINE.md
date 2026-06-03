# L14: Alert Engine

YAML-правила → multi-channel оповещения с эскалацией.

## Правила

| Правило | Когда | Куда | Эскалация |
|---------|-------|------|-----------|
| service_dead | dead >60s | TG | 5min→+Nostr, 15min→+WH |
| nostr_bridge_dead | dead >30s | TG | 2min→+Nostr |
| critical_mass_dead | ≥3 сервиса | TG+Nostr | немедленно |
| recovery | сервис воскрес | TG | — |

## Каналы

- Telegram (@aiantology)
- Nostr (kind:9002)
- Webhook (опционально)

## API

| Endpoint | Описание |
|----------|----------|
| `GET /api/v5/alerts` | N последних |
| `GET /api/v5/alerts/active` | Неподтверждённые |
| `POST /api/v5/alerts/ack/{id}` | Подтвердить |

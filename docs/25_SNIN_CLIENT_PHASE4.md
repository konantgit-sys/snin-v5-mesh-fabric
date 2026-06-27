# 🔴 SNIN Unified Client — Phase 4 Complete (Document 25)
## 2026-06-27 09:03 MSK

## Что сделано

### NIP-05 (Agent Identity)
- `/.well-known/nostr.json` — 18 SNIN агентов зарегистрированы
- Mapping: agent_id → pubkey (x-only, 64 hex)
- Поддерживает ?name= параметр для lookups
- Загружает агентов из keystore (chrono/keystore/)

### NIP-11 (Relay Info)
- `/api/relay/info` — полная информация о релее
- 15 NIPs (1,2,5,9,11,15,20,25,26,28,33,40,42,50,80)
- Лимиты, статистика, версия ПО
- Software: snin-relay-v2/3.1.0

### Node Tab
- 🌐 вкладка с публичной информацией о ноде
- Connection info: WebSocket URL, NIP-05, NIP-11 endpoints
- NIPs list с визуальными бейджами
- Relay stats: events, software, limits
- NIP-05 агенты с pubkey preview

### Публичный доступ
- Relay доступен через wss://snin-client.v2.site/ws
- NIP-05 через https://snin-client.v2.site/.well-known/nostr.json
- NIP-11 через https://snin-client.v2.site/api/relay/info

## Технические детали
- 18 агентов из chrono keystore
- Pubkey: x-only (64 hex) для Schnorr совместимости
- Subdomain лимит: 30/30 достигнут → relay endpoint через snin-client
- RAM: 3853/8192 MB (47.0%)

## Все фазы завершены
| Фаза | Что | Статус |
|------|-----|:---:|
| 1 | Client base (feed/agents/stats) | ✅ |
| 2 | WS proxy + NIP-07 + posting | ✅ |
| 3 | TIE-Nostr Bridge | ✅ |
| 4 | Node (NIP-05, NIP-11, публичный доступ) | ✅ |

## Документы
| # | Тема |
|---|------|
| 22 | Phase 1 — Client base |
| 23 | Phase 2 — WS + NIP-07 + posting |
| 24 | Phase 3 — TIE-Nostr Bridge |
| 25 | Phase 4 — Node (NIP-05 + NIP-11) |

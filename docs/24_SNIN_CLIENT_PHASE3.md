# 🔴 SNIN Unified Client — Phase 3 Complete (Document 24)
## 2026-06-27 08:04 MSK

## Что сделано

### TIE-Nostr Bridge v2.0
- Полный цикл: опрос TIE relay → публикация агентов на Nostr
- Реализован NIP-42 AUTH (Schnorr подпись + challenge-response)
- Использует x-only pubkey (64 hex) для совместимости с релеем
- Агенты публикуются как kind:1 с тегом "tie-agent"
- **2 TIE агента синхронизированы** (alice_test, bob_node)

### TIE Tab в SNIN Client
- Вкладка 🔗 TIE с двумя секциями:
  - Synced to Nostr (агенты из релея Nostr, kind:1 с TIE тегом)
  - On TIE Relay (агенты из кэша bridge)
- Статус TIE-релея (tie-run.v2.site)
- Время последней синхронизации

### Backend (app.py v3.0)
- GET /api/tie — запрос агентов из БД (tags_json LIKE '%tie-agent%')
- Автоматическое определение имени агента из content

### Ключевые находки при разработке
- Релей SNIN использует Schnorr (BIP-340), не ECDSA
- kind:39000 требует "h" тег (NIP-29 group)
- kind:30000 резервирован под Solana Payments
- x-only pubkey (64 hex) — обязателен для Schnorr верификации
- NIP-42 AUTH работает через ["AUTH", {event_dict}], не через to_message()

## Технические детали
- Bridge: асинхронный, aiohttp + nostr library
- Кеширование: tie_cache.json + БД релея
- RAM: 3837/8192 MB (46.8%)

## Следующий шаг: Phase 4 (стать нодой, 3-5ч)
- Настройка внешнего релея (публичный доступ к :8198)
- DNS и SSL через v2.site
- NIP-05 идентификация
- Статус "ноды" в дашбордах

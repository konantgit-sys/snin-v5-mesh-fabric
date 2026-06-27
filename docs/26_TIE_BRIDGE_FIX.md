# 🔴 TIE Bridge Fix — Document 26
## 2026-06-27 08:15 MSK

## Проблема
- snin-client /api/tie показывал 0 peers
- TIE Bridge процесс не был запущен

## Решение
- Перезапущен tie_nostr_bridge.py v2
- Установлен cron: каждые 10 минут синхронизация TIE → Nostr
- Endpoints исправлены: /register→/api/create_agent, /peers→/api/site_agents

## Результат
- 2 TIE агента живы: alice_test, bob_node
- 2 агента в кэше snin-client
- 6 событий синхронизированы в Nostr (kind:1 с тегом tie-agent)
- TIE relay uptime: 249K+ сек (~2.9 дней)

## Открытые проблемы
| Проблема | Статус |
|----------|:---:|
| Supervisor: 4 dead slots (32/36) | ⚠️ допустимо (≥80%) |
| tie-infra start.sh disabled | ⚠️ не восстановится после рестарта |

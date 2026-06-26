# 🔴 14-й ПРОГОН — RELAY REVIVAL
**2026-06-26 23:06 MSK**

## Диагноз

| Проблема | Причина | Решение |
|----------|--------|---------|
| relay_server_v2 (8198) crash-loop | `pynostr` не установлен | `pip3 install pynostr` |
| snin_adapter (8443) crash-loop | `orjson` не установлен | `pip3 install orjson` |
| frontend (8199) bind error | Старый процесс PID 24210 | `kill 24210` |

## Результат

| Компонент | Порт | Статус | PID | RAM |
|-----------|:---:|:---:|-----|-----|
| relay_server_v2 | 8198 | LIVE | 457911 | 80 MB |
| frontend | 8199 | LIVE | 457942 | 37 MB |
| snin_adapter | — | LIVE | 457957 | 38 MB |
| supervisor | — | LIVE | 5093 | 25 MB |
| smart_router | — | LIVE | 24537 | 55 MB |
| content_router_v2 | 9920 | LIVE | 5307 | 25 MB |
| external_gateway | — | LIVE | 5377 | 31 MB |

**Итого:** 7/7 SNIN-процессов живы. RAM: 3895/8192 MB (47.5%).

## Relay V2 — характеристики

- **12,169 событий**, 134 уникальных pubkey
- **20 NIPs**: 1,4,9,11,12,13,20,26,29,33,40,42,45,50,56,71,86,89,94,96
- **SNIN Protocol**: кинды 8010-8016, 19000, 39000
- **Последнее событие**: 2026-06-24 10:09 UTC (релей молчал 52 часа до revival)
- **snin_adapter**: зарегистрирован на damus.io, purplepag.es, offchain.pub

## Топ-kinds в БД

| Kind | Count | Назначение |
|------|------:|-----------|
| 1 | 7,229 | Text notes |
| 10002 | 2,931 | Relay list metadata |
| 9000 | 999 | Internal SNIN |
| 39000 | 572 | SNIN mesh events |
| 19000 | 222 | SNIN protocol events |
| 8010 | 107 | SNIN NIP-80 |

## Следующие шаги

- [x] Релей поднят
- [ ] SNIN Dashboard (snin-dashboard.v2.site)
- [ ] SNIN Client Phase 1
- [ ] Telegram bot с inline-командами
- [ ] Pulse Sync (Phase 2.4)
- [ ] Fanout (Phase 3.1)

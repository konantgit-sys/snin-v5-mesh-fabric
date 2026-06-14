# SNIN Infrastructure Snapshot — 2026-06-14 23:24 YEKT

## Текущее состояние

| Показатель | До чистки | После чистки |
|------------|-----------|-------------|
| RAM used | 8025 MB (97.9%) | **6944 MB (84.8%)** |
| RAM free | 167 MB | **1248 MB** |
| Python-процессов | ~85 | **~58** |
| RSS python | ~4260 MB | ~3200 MB |

## Что живо (критическое)

| Сервис | Процессов | RAM | Роль |
|--------|-----------|-----|------|
| 🐙 Cryter Agent | 9 | 930 MB | Ядро — Nostr + Telegram posting |
| 🔌 ApiServer | 2 | 468 MB | Основной API (агенты, relay) |
| 📡 RelayServer | 1 | 427 MB | Nostr relay (:8198) |
| 🌉 NostrBridge | 5 | 187 MB | Мост Nostr ↔ Mesh (5 шардов) |
| 🤖 Agent Engine | 3 | 161 MB | Движок агента (этот чат) |
| 📨 Telegram Bots | 3 | 164 MB | Telegram боты |
| ⏳ Chrono API | 1 | 115 MB | Keystore (:8190) |
| 💬 TIE Messenger | 2 | 73 MB | Фронтенд + WebSocket |
| 📊 Hub Dashboard | 1 | 68 MB | snin-hub.v2.site |
| 🔧 V2Bot Daemon | 1 | 65 MB | Демон V2Bot |
| 🆔 Identity API | 1 | 49 MB | DID + NIP-05 |
| 💳 ChequeBook | 1 | 29 MB | Чеки |
| 👁️ Supervisor | 1 | 24 MB | Мониторинг 34 сервисов |
| 🖥️ Frontend | 1 | 36 MB | Статический фронтенд |
| 🌐 Web Backends | 7 | 349 MB | Разные бэкенды |

## Что вырублено (start.sh → start.sh.disabled)

### L2-L8 Архитектурные слои
- l2_transport_layer.py (:9500) — `/sites/l2-transport/start.sh.disabled`
- l2_encryption_layer.py (:9600) — `/sites/encryption-layer/start.sh.disabled`
- l3_zk_layer.py (:9250) — `/sites/zk-layer/start.sh.disabled`
- l4_privacy_layer.py (:9700) — `/sites/privacy-layer/start.sh.disabled`
- l4_payment_layer.py (:9200) — `/sites/l4-payment/start.sh.disabled`
- l6_agent_network.py (:9400) — `/sites/l6-network/start.sh.disabled`
- l8_app_layer.py (:9800) — `/sites/app-layer/start.sh.disabled`
- l1_5_bridge.py (:8202) — `/sites/bridge/start.sh.disabled`

### Mesh Fabric
- snin_mesh_daemon.py — `/sites/snin-mesh/start.sh.disabled`
- cross_mesh_bridge.py (:9945, :9946) — `/sites/cross-mesh/start.sh.disabled`
- external_gateway.py — `/sites/external-gateway/start.sh.disabled`
- relay_mesh_api.py (:9907) — `/sites/snin-gossip/start.sh.disabled`

### Мониторинг (дубли)
- dashboard.py (:8086) — `/sites/relay-dash/start.sh.disabled`

### Неиспользуемые сервисы
- relay_v2.py (:9905) — `/sites/tie-relay/start.sh.disabled`
- snin_pay (:8191) — `/sites/snin-pay/start.sh.disabled`
- dao_api (:8082) — `/sites/snin-dao/start.sh.disabled`
- api_gateway.py — `/sites/api-gateway/start.sh.disabled`
- tie-infra, tie-run, scaler-engine

### Mesh-сервисы (без start.sh, убиты вручную)
- trading_mesh, defi_mesh, energy_mesh, crowd_mesh, chain_mesh, city_mesh, research_mesh
- verifier.py (:9915) ×2
- simple_agent.py (:9908)
- route_engine.py, content_router_v2.py (:9920)
- l9_orchestration.py (:9900)

## Как восстановить

Для любого вырубленного сервиса:
```bash
cd /home/agent/data/sites/ИМЯ_СЕРВИСА
mv start.sh.disabled start.sh
```
Система автоматически запустит сервис в течение 60 секунд.

## Dashboards (живые)

- **Hub:** https://snin-hub.v2.site — показывает cgroup RAM (8 GB), не хост
- **TIE Messenger:** https://tie-app.v2.site — 14 вкладок, войти `demo`

## Ключевые порты

| Порт | Сервис | Статус |
|------|--------|--------|
| 8198 | Relay Server (Nostr) | ✅ |
| 8190 | Chrono API | ✅ |
| 8099 | TIE WebSocket | ✅ |
| 9950 | Hub Dashboard | ✅ |
| 9900 | Supervisor | ✅ |
| 9940 | Identity API | ✅ |
| 9931-9932 | NostrBridge | ✅ |

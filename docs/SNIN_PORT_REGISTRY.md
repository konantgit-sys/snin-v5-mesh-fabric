# SNIN PORT REGISTRY — единый стандарт портов

*Сгенерировано: 2026-09-01 20:40 · Аудит: 2026-09-02 14:20 (все порты и port.txt сверены с реальностью) · Версия стандарта: 1.2*

## Правила
1. **Один порт = один сервис.** Назначение порта — в этой таблице, не в port.txt.
2. **Имя сервиса = имя файла = имя процесса = имя в реестре.** Без синонимов.
3. `status: cert` — сертифицирован (проверен, слушается); `cfg` — назначен по конфигу (сервис не запущен, порт зафиксирован на будущее); `system` — служебный; `unknown` — не назначен.
4. Изменение порта — только через правку этой таблицы (registry) + аудит.

## Диапазоны (стандарт)
| Диапазон | Слой | Назначение |
|----------|------|------------|
| 8080-8123 | web | Веб-приложения и дашборды |
| 8190-8199 | L0 | Релеи Nostr |
| 9100-9109 | L1 | Gossip-шарды |
| 9200 | L4 | Платежи |
| 9767-9880 | web | Знаниевые сервисы |
| 9900-9955 | L3 | Шлюзы, хабы, дашборды |
| 9910-9946 | L1 | Mesh-ядро (маршрутизация) |
| 9960-9965 | L2 | Публикация/ZMQ шардов |
| 19900-19999 | web | Вспомогательные API (urantia — исторические, не активны) |
| 39001 | L1 | DHT |

## Реестр (аудит 2026-09-02 14:20)

| Порт | Сервис | Файл | Назначение | Слой | Статус |
|------|--------|------|------------|------|--------|
| :6379 | redis | redis-server | Graph Memory / кэш SR | system | cert |
| :8080 | system-platform | api_server_v2.py (agent/) | служебный порт платформы (НЕ занимать) | system | cert |
| :8082 | relay-sol | relay_core.py (cfg) | SNIN Relay Pro (коммерческий relay, не запущен) | L0 | cfg |
| :8083 | api-gateway | api_gateway.py | REST API Gateway | L3 | cert |
| :8085 | relay-mesh-site | mesh_status.py | Дашборд mesh-статуса | web | cert |
| :8086 | relay-dash | api_server.py | Дашборд релея | web | cert |
| :8089 | triplet-test | app.py | Тестовая тройка | web | cert |
| :8090 | p2p-dash | app.py | P2P Agent Mesh дашборд (+ :39001) | web | cert |
| :8091 | archivist | app.py | Archivist (закрытая база) | web | cert |
| :8092 | factory | app.py 8092 | V2Bot Content Factory | web | cert |
| :8095 | snin-client | uvicorn app:app | Единый клиент (NWC-код) | web | cert |
| :8096 | cryter-dash-legacy | app.py (cryter/dashboard) | ДУБЛЬ api_server.py:9902, поддоменом НЕ используется | web | cert |
| :8100 | urantia-crossref | src/ (cfg) | Urantia crossref (полный проект, не запущен) | web | cfg |
| :8101 | confluence | uvicorn src.api.main | Confluence: Урантия·Библия·Коран | web | cert |
| :8107 | test-crossref | (cfg) | ПЕРЕНЕСЁН с :8100 (уступил urantia-crossref) | web | cfg |
| :8111 | analion | app.py (sites/analion/) | Сайт Analion (analion.v2.site) | web | cert |
| :8123 | snin-mail | uvicorn app:app | SNIN Mail (uvicorn) | web | cert |
| :8177 | simple-api | app.py (cfg) | Простой API (не запущен) | web | cfg |
| :8178 | analion-site | (cfg) | ДУБЛЬ analion (sites/analion-site НЕ зарегистрирован) | web | cfg |
| :8191 | snin-pay | api/server.py | SNIN Payment Gateway v0.1.0 | L4/экономика | cert |
| :8197 | snin-relay | relay_gateway.py 8197 | Nostr relay gateway | L0 | cert |
| :8198 | relay-v2 | snin_nostr_relay.py | Nostr relay (snin-relay/) | L0 | cert |
| :9105 | gossip (внутр.) | smart_router.py | Порт SmartRouter (gossip), НЕ отдельный демон | L1 | cert |
| :9200 | l4-payment | l4_payment_layer.py 9200 | L4 Payment layer | L4 | cert |
| :9767 | mesh-relay-test | relay.py | Тестовая пара релеев | web | cert |
| :9770 | lenin-book | uvicorn api_v2:app | Ленин — архитектор | web | cert |
| :9776 | passport-api | uvicorn api.passport_api | API паспортов | web | cert |
| :9777 | mesh-hub | relay.py | Mesh Hub | web | cert |
| :9780 | lenin-oracle | app.py | Оракул Ленина | web | cert |
| :9880 | api-lenin | app.py | API-ключи Ленина | web | cert |
| :9900 | graphify-api | api.py | Визуальный граф кода API (graphify-snin/) | L3 | cert |
| :9901 | cobalt-dash | app.py | Cobalt Dashboard — BRING World Manager | L3 | cert |
| :9902 | cryter-dash | api_server.py 9902 | Дашборд Cryter (cryter-dash.v2.site) | L3 | cert |
| :9903 | remora-dash | app.py | Remora Dashboard (ПЕРЕЕХАЛ с :9901) | L3 | cert |
| :9904 | tie-mesh | app.py 9904 | TIE Unified Mesh (ПЕРЕЕХАЛ с :9901) | L3 | cert |
| :9905 | tie-infra | gateway.py (cfg) | TIE Gateway (НАЗНАЧЕН, disabled, был :9901) | L3 | cfg |
| :9906 | upload | app.py (cfg) | Upload-сервис (НАЗНАЧЕН, disabled, был :9901) | L3 | cfg |
| :9907 | (свободен) | — | gossip документный — реально у smart_router :9105 | L3 | free |
| :9909 | mesh-supervisor | mesh_supervisor.py | Supervisor mesh (ПЕРЕЕХАЛ с :9900) | L3 | cert |
| :9910 | route-engine | route_engine.py | Поиск кратчайшего пути, выбор канала | L1 | cert |
| :9920 | content-router | content_router_v2.py 9920 | Дедупликация, семантическая маршрутизация | L1 | cert |
| :9931 | external-gateway | external_gateway.py | WSS↔TCP мост, Nostr→mesh (kind 39002/39003) | L1/L3 | cert |
| :9932 | smart-router | smart_router.py | ЕДИНСТВЕННАЯ точка входа, 4 канала | L1 | cert |
| :9933 | mesh-api (внутр.) | smart_router.py | HTTP-API порт SmartRouter, НЕ отдельный демон | L1 | cert |
| :9941 | nostr-bridge-0 | nostr_bridge.py --shard 0 | Публикация шард 0/5 | L1/L3 | cert |
| :9942 | nostr-bridge-1 | nostr_bridge.py --shard 1 | Публикация шард 1/5 | L1/L3 | cert |
| :9943 | nostr-bridge-2 | nostr_bridge.py --shard 2 | Публикация шард 2/5 | L1/L3 | cert |
| :9944 | nostr-bridge-3 | nostr_bridge.py --shard 3 | Публикация шард 3/5 | L1/L3 | cert |
| :9945 | nostr-bridge-4 | nostr_bridge.py --shard 4 | Публикация шард 4/5 | L1/L3 | cert |
| :9946 | cross-mesh | cross_mesh_bridge.py 9946 | Mesh-to-mesh федерация (kind 30002-30004) | L1 | cert |
| :9950 | snin-hub | hub_fastapi.py --port 9950 | Единый дашборд + API (/api/spm) | L3 | cert |
| :9951 | snin-mcp | gateway.py --port 9951 | MCP Gateway (внешние AI-агенты) | L3 | cert |
| :9960 | zmq (SR) | smart_router.py | ZeroMQ-порт SmartRouter | L2 | cert |
| :9961 | zmq (SR-2) | smart_router.py | Второй ZeroMQ-порт SmartRouter | L2 | cert |
| :9962 | nostr-bridge-1-aux | nostr_bridge.py shard 1 | Вторичный порт шарда 1 (ИСПРАВЛЕНО: не zmq_transport) | L2 | cert |
| :9963 | nostr-bridge-2-aux | nostr_bridge.py shard 2 | Вторичный порт шарда 2 (ИСПРАВЛЕНО: не zmq_transport) | L2 | cert |
| :9964 | nostr-bridge-3-aux | nostr_bridge.py shard 3 | Вторичный порт шарда 3 (ИСПРАВЛЕНО: не zmq_transport) | L2 | cert |
| :9965 | nostr-bridge-4-aux | nostr_bridge.py shard 4 | Вторичный порт шарда 4 (ИСПРАВЛЕНО: не zmq_transport) | L2 | cert |
| :9970 | hcoor | hybrid.hcoor --port 9970 | Гибридный координатор | L3 | cert |
| :9980 | snin-network | app.py | Админ-панель SNIN Network | L3 | cert |
| :12345 | system-hidden | невидим для ss | слушает неизвестный слушатель (инфраструктура?) — НЕ назначать | system | hold |
| :39001 | p2p-dash-2 | app.py (p2p-dash) | Второй порт p2p-dash (DHT) | L1 | cert |

*Удалены как неактивные:* :19910-19946 (urantia-crossref исторические тестовые запуски — НЕ слушаются, не назначать).

## Конфликты port.txt — ВСЕ ЗАКРЫТЫ (аудит 2026-09-02 14:20)

| Порт | Сервисы (директории) | Решение | Статус |
|------|----------------------|---------|--------|
| :8082 | relay-sol, snin-dao | relay-sol (есть код relay_core.py); snin-dao — ПУСТ (только log, кода нет) → исключён | ✅ решено |
| :8085 | relay-mesh, snin-health-api | relay-mesh на 8085 (mesh_status.py), health-api не запущен | ✅ решено |
| :8100 | test-crossref, urantia-crossref | urantia-crossref (полный проект src/); test-crossref → :8107 | ✅ решено |
| :8177 | analion-site, simple-api | simple-api на 8177; analion-site — ДУБЛЬ analion (analion.v2.site = sites/analion/:8111) → :8178 | ✅ решено |
| :8198 | relay, relay-ws | реально snin_nostr_relay.py (snin-relay), relay-ws нет | ✅ решено |
| :9767 | mesh-relay-test, peer-relay | mesh-relay-test (peer-relay не запущен) | ✅ решено |
| :9777 | mesh-hub, mesh55 | mesh-hub (mesh55 не запущен) | ✅ решено |
| :9901 | cobalt-dash, remora-dash, tie-infra, tie-mesh, upload | cobalt-dash владелец; remora-dash→:9903, tie-mesh→:9904 (СДЕЛАНО); tie-infra→:9905, upload→:9906 (НАЗНАЧЕНО, disabled) | ✅ решено |
| :9951 | snin-mcp, ws-hub | snin-mcp (gateway.py --port 9951), ws-hub не запущен | ✅ решено |

## Расхождения документ ↔ реальность

| Сущность | В документах | Реально | Решение |
|----------|--------------|---------|---------|
| External Gateway | :5377, :9931, :9951 | :9931 | стандарт: :9931 |
| Mesh API | :9907 (memory), :9908 WS, :9911 REST, :9912 gRPC | :9933 (порт smart_router) | стандарт: :9933 |
| Supervisor | :9900 | graphify на :9900, supervisor мёртв | ✅ supervisor → :9909 (mesh_supervisor.py) |
| snin-pay | :8191 OFF (KB) | :8191 ЖИВ (api/server.py) | статус: cert |
| SmartRouter (конфиг) | router_api.py | smart_router.py | ✅ конфиг исправлен |
| MeshAPI/gossip | отдельные демоны | порты smart_router.py (9933/9105/9960/9961) | ✅ реестр обновлён |
| :9962-9965 | zmq_transport.py | nostr_bridge шарды 1-4 (aux-порты) | ✅ реестр обновлён |
| :8096 | app-8096 (unknown) | cryter dashboard app.py (дубль :9902) | ✅ идентифицирован |
| :12345 | unknown | слушает невидимое (ss не показывает) | ✅ hold, не назначать |
| analion | analion-site :8177 | analion.v2.site = sites/analion/ :8111 | ✅ analion-site → :8178 (дубль) |

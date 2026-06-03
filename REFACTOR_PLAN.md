# SNIN V5 Mesh Fabric — План рефакторинга (5 проблем)

⏱ Общая длительность: 5 фаз, ~14 дней
🎯 Цель: уменьшить дублирование, централизовать контроль, подготовить к масштабированию

---

## Фаза 0: Подготовка (1 день)
**До начала любых изменений — зафиксировать текущее состояние**

- [ ] Создать папку `/home/agent/data/sites/relay-mesh/_archive/`
- [ ] Перенести мёртвый код: sr_master.py, sr_master_v2.py, sr_master_v3.py, sr_master_v51.py, first_contact_old.py, identity_api.py, ws_server.py.archived
- [ ] Создать бэкап всей папки: `cp -r relay-mesh relay-mesh.BACKUP.$(date +%Y%m%d)`
- [ ] Проверить, что все сервисы alive (health engine :9999)
- [ ] Сохранить список PID'ов всех процессов

**Результат:** Чистая директория + страховочный бэкап

---

## Фаза 1: Config — единый конфиг (2 дня)
**Базовый слой — все остальные фазы будут на него опираться**

### Шаг 1.1 — Создать mesh_config.yaml
```yaml
# /home/agent/data/sites/relay-mesh/mesh_config.yaml
version: "3.1"

global:
  log_dir: /home/agent/data/logs
  health_port_base: 10000  # + service_port = health_port

layers:
  transport:
    smart_router: 9932
    route_engine: 9910
    content_router: 9920
  mesh_fabric:
    external_gateway: 9931
    cross_mesh_bridge: 9946
    anti_ddos: 9970
    dht_node: 9934
  nostr:
    bridge_base: 9941
    bridge_count: 5
    relay_list_path: relays.json
  identity:
    identity_api: 9940
    verifier: 9915
    cheque_book: 9916
  orchestration:
    supervisor: 9900
    health_engine: 9999
    relay_mesh_api: 9907
    relay_v2: 9905
```

### Шаг 1.2 — Написать mesh_config.py (загрузчик)
- Читает YAML, отдаёт `config.get("layers.transport.smart_router") → 9932`
- Авто-генерация health_port: `config.health_port_for("smart_router") → 19932`
- Проверка на дубли портов при старте

### Шаг 1.3 — Встроить в 3 ключевых сервиса (пилот)
- `smart_router.py` — вместо `LISTEN_PORT = 9932` → `from mesh_config import config; LISTEN_PORT = config.get("layers.transport.smart_router")`
- `nostr_bridge.py` — вместо `GATEWAY_PORT = 9941 + SHARD_ID` → `config.get("layers.nostr.bridge_base") + SHARD_ID`
- `health_check_engine.py` — прочитать весь список сервисов из конфига

**Контрольная точка:** curl :9999/api/health/summary — 15/15 alive

---

## Фаза 2: Smart Router — декомпозиция ✅ (завершено)
**2040 строк → 3 модуля (1542 + 314 + 159 = 2015 строк)**

Итоговая структура:
- `smart_router.py` (1542 строк) — только SmartRouter class + config
- `router_policy.py` (314 строк) — InMemoryCircuitBreaker, policy engine, traffic classification
- `router_api.py` (159 строк) — entry point, health endpoint, status printer

### Шаг 2.1 — Выделить router_policy.py ✅ (~314 строк)
Перенести:
- `apply_policies()` — политики маршрутизации
- `pick_channel_from_policy()` — выбор канала
- `get_best_channel()` — лучший канал для агента
- `classify_traffic()` — классификация по kind
- `_get_reputation_weight()` — вес репутации
- Все policy-константы и маппинги kind→channel

Интерфейс: `class RouterPolicy: def pick(event, agent_id) → channel`

### Шаг 2.2 — Выделить router_dht.py (~300 строк)
Перенести:
- DHT-логику (поиск агентов)
- Bloom filter sync
- Redis-backed routing table

Интерфейс: `class RouterDHT: def find_agent(pubkey) → agent_info`

### Шаг 2.3 — Выделить router_api.py (~200 строк)
Перенести:
- HTTP/Health endpoint в самом Smart Router
- `/api/v3/stats` и другие REST методы
- Форматирование ответов

### Шаг 2.4 — Ядро smart_router.py (~600 строк)
Оставить только:
- Основной event loop
- Вызов policy → DHT → channel
- Запись в лог/метрики
- Инициализация модулей

### Шаг 2.5 — Интеграция и тест
- Собрать всё вместе
- Проверить через supervisor :9900/health что smart_router не упал
- Проверить те же метрики, что и до рефакторинга

**Контрольная точка:** Smart router работает, ответы идентичны до/после

---

## Фаза 3: Nostr Bridge — core + обёртки ✅ (завершено)
**1473 строк → 3 модуля (105 + 349 + 613 = 1067 строк)**

Итоговая структура:
- `nostr_relay_list.py` (105 строк) — relay списки, TIER'ы, shard slicing
- `nostr_core.py` (349 строк) — NostrRelayClient, CircuitBreaker, signing, NIP-42/65
- `nostr_bridge.py` (613 строк) — NostrBridge оркестрация + NostrBridgeLayer + main

### Шаг 3.1 — Выделить nostr_core.py ✅ (349 строк)
Общий функционал (сейчас дублирован в каждом шарде):
- `NostrClient` — подключение к релею (websocket)
- `sign_event()` — подпись событий
- `make_auth_event()` — NIP-42 авторизация
- `RateLimiter` — per-relay rate limit
- `ConnectionPool` — reconnect с exponential backoff
- `RelayTier` — классификация релеев по стабильности

### Шаг 3.2 — nostr_relay_list.py
Вынести конфигурацию релеев:
- 101 релей из 4 TIER'ов
- Список шардовых релеев (NIP-65)
- Scan relays

### Шаг 3.3 — Переписать nostr_bridge.py как легковесную обёртку
```
class NostrBridgeShard:
    def __init__(self, shard_id, total_shards):
        self.core = NostrClientPool(shard_id, total_shards)
        self.relays = RelayList.for_shard(shard_id)
    
    async def run(self):
        await self.core.connect_all(self.relays)
        await self.core.listen_loop()
```

### Шаг 3.4 — Тест всех 5 шардов
- Запустить, проверить что все 5 health endpoints (19941-19945) отвечают
- Сравнить uptime до/после

**Контрольная точка:** 5/5 nostr bridges alive, публикация работает

---

## Фаза 4: Middleware — централизация cross-cutting ✅ (завершено)
**Circuit breaker + rate limit + auth → единый pipeline**

Итоговая структура:
- `middleware.py` (403 строк) — RateLimiter + CircuitBreakerManager + RequestPipeline + sync shortcuts
- `anti_ddos.py` (77 строк) — thin wrapper вокруг middleware.RateLimiter

### Шаг 4.1 — Спроектировать middleware.py ✅ (403 строк)
```
flow:
  client_request → RateLimiter → CircuitBreaker → Auth → Router
```

Компоненты:
- `RateLimiter` — per-IP (100/мин), per-pubkey (50/мин), Redis-backed
- `CircuitBreakerCheck` — проверка статуса канала перед маршрутизацией
- `RequestPipeline` — композиция middleware

### Шаг 4.2 — Интегрировать в smart_router
- Вырезать rate limit логику из app.py и anti_ddos.py
- Вырезать circuit breaker вызовы из smart_router.py и external_gateway.py
- Встроить единый вызов `await pipeline.process(request)` в начало обработки

### Шаг 4.3 — anti_ddos_daemon.py переписать как thin wrapper
- Убрать дублирование логики с app.py
- Оставить как standalone сервис (:9970) для статистики

### Шаг 4.4 — Тест circuit breaker после миграции
- Проверить circuit_breaker_status.json — все 4 канала CLOSED
- Симулировать отказ канала → проверить auto half-open

**Контрольная точка:** Rate limit + CB работают из одного места, метрики не изменились

---

## Фаза 5: Интеграция, автотесты, документация ✅ (завершено)

**Результат:**
- `Makefile` — `make test` запускает 78 тестов (53 middleware + 25 phase 1-2)
- `test_middleware.py` — 53 теста на RateLimiter, CircuitBreaker, Pipeline, sync shortcuts
- `ARCHITECTURE.md` обновлён до V5 — добавлен Middleware Layer, обновлены все 5 фаз
- Бэкап оставлен как reference
- Health engine: 41/41 сервисов alive (основная система)

**Итого по 5 фазам:**

| Фаза | Было | Стало | Строк |
|:----:|:----:|:-----:|:-----:|
| 1 — Config | 20+ хардкодов | 1 config.yaml (15 сервисов) | 60 |
| 2 — Smart Router | 1 файл, 2037 строк | 3 модуля, 2015 строк | -22 |
| 3 — Nostr Bridge | 7350 строк (×5 шарды) | 1067 строк (3 модуля) | -6283 |
| 4 — Middleware | 4+ rate limit + 2 CB | 1 pipeline (403 строк) | -450 |
| 5 — Tests | 0 системных тестов | 78 тестов, Makefile | +1 файл |

---

## Итого: что меняется

| Проблема | Было | Стало | Эффект |
|----------|------|-------|--------|
| Smart Router | 1 файл, 2037 строк | 4 файла, ~600 ядро | Чинить в 4 раза быстрее |
| Nostr Bridge | 1470×5 = 7350 строк | ~1000 core + 500 обёртки | -80% дублирования |
| Config | хардкод в 20+ файлах | 1 config.yaml | Поменять порт = 1 строка |
| Legacy | 1700 строк мусора | _archive | Чистый поиск |
| Middleware | 4+ файла | 1 pipeline | Жёсткий контроль |

Жду команды — с какой фазы начинаем?

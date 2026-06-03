# SNIN Relay Mesh — V5 Architecture (May 2026)

## Статус (AS-IS, 5 фаз рефакторинга завершены)

```
                          ВНЕШНИЙ МИР
                              │
                    Gateway (:8083) / Hub (:9950)
                              │
                     ┌─── Smart Router (:9932) ───┐
                     │                              │
               Route Engine (:9910)          Content Router (:9920)
                     │                              │
          ┌──────────┼──────────┬──────────┬────────┼──────────┐
          │          │          │          │        │          │
      Direct      Mesh     Nostr×5    Gossip    Ext.Gate  Identity
      (TCP)    (mesh net) (:9941-45) (V8 stream) (:9931)  (:9940)
                                                          │
                                                    Cheque Book (:9916)
                                                    Verifier (:9915)
                     │
              Middleware Layer (Phase 4) ←── РАТЕ ЛИМИТ + CIRCUIT BREAKER
                     │
              Supervisor L9 (:9900)
          35+ сервисов под контролем
```

## Ключевые компоненты

| Компонент | Порт | Статус | Назначение |
|-----------|:----:|:------:|------------|
| Smart Router | 9932 | 🟢 | Маршрутизация по 4 каналам, DHT, политики |
| Route Engine | 9910 | 🟢 | Классификация kind, batch |
| Content Router V2 | 9920 | 🟢 | Bloom+Redis dedup, URL priority |
| Nostr Bridge ×5 | 9941-9945 | 🟢🟢🟢🟢🟢 | Шлюзы Nostr (замена gossip :9100-9104) |
| External Gateway | 9931 | 🟢 | TCP + Nostr bridge (101 relay) |
| Identity API v2 | 9940 | 🟢 | Регистрация агентов |
| Cross-Mesh Bridge | 9945 | 🟢 | Federation между mesh сетями |
| Cheque Book | 9916 | 🟢 | Чековые книги (15 книг, 8 агентов) |
| Verifier | 9915 | 🟢 | Верификация платежей (test mode) |

## Middleware Layer — Phase 4 ✅ (`middleware.py`)

**Единый pipeline для cross-cutting задач:**

```
                  Request
                     │
               RateLimiter
           per-IP (100/60s), per-pubkey (50/60s),
           per-session (10/s anon, 100/s auth)
           blacklist (10 violations → 300s TTL)
           max event size (64 KB)
                     │
            CircuitBreakerManager
           direct (3 strikes / 30s cooldown)
           mesh   (5 strikes / 30s cooldown)
           nostr  (3 strikes / 60s cooldown)
           gossip (5 strikes / 30s cooldown)
                     │
               Auth (NIP-42)
                     │
                  Router
```

### Компоненты middleware

| Класс | Строк | Назначение |
|-------|:-----:|------------|
| `RateLimiter` | 120 | Многоуровневый rate limit + blacklist |
| `ChannelCB` | 70 | Circuit breaker для одного канала |
| `CircuitBreakerManager` | 90 | Управление 4 каналами, JSON persistence |
| `RequestPipeline` | 60 | Композиция middleware, sync wrappers |
| Shortcut functions | 30 | `cb_check()`, `cb_reset()`, `check_rate_limit_simple()` |

### Заменённые файлы

| Файл | Было | Стало |
|------|:----:|:-----:|
| `anti_ddos.py` | 163 строк (дублирование RateLimiter) | 77 строк (thin wrapper) |
| `app.py` | 3 разных rate limit + 2 CB вызова | всё через middleware |
| `circuit_breaker.py` | 232 строк (свой CB) | `from middleware import *` |
| `nip42_auth.py` | свой rate limit в AUTH | делегирует в middleware |
| Дублирование | 3 rate limiter'а, 2 CB | 1 pipeline |

### Эндпоинты мониторинга

- `/system/degradation` — статус всех circuit breaker'ов
- `/system/circuit-breaker/<channel>/reset` — сброс CB
- `anti_ddos_daemon.py` (:9970) — статистика rate limiter'а

## Graceful degradation (V5)

- Circuit breaker: 4 канала (direct/mesh/gossip/nostr), auto half-open 30s
- JSON persistence: `circuit_breaker_status.json` — состояние сохраняется на диск
- Nostr Bridge health monitor: проверка каждые 15 сек
- External Gateway probe: fallback Nostr канала
- Content Router: in-memory dedup без Redis

## Anti-DDoS (V5)

- Max event size: 64 KB
- Rate limiter: per-IP (100/60s), per-pubkey (50/60s), per-session (10/s anon, 100/s auth)
- Blacklist: 10 нарушений → 300s блокировка
- Статистика: rejected/size, rejected/blacklist, rejected/rate

## CRC / Pipeline — Phase 2 ✅ (`router_policy.py`)

Smart Router декомпозирован:
- `smart_router.py` (1542 строк) — ядро маршрутизации
- `router_policy.py` (314 строк) — InMemoryCircuitBreaker, политики, классификация трафика
- `router_api.py` (159 строк) — entry point, health, статус

## Nostr Bridge — Phase 3 ✅ (`nostr_core.py`)

- `nostr_relay_list.py` (105 строк) — relay списки, TIER'ы, shard slicing
- `nostr_core.py` (349 строк) — NostrRelayClient, CircuitBreaker, signing
- `nostr_bridge.py` (613 строк) — оркестрация 5 шардов, NostrBridgeLayer

## Быстродействие

- Throughput: 36,873 msg/s sustained, 194,388 burst
- Каналы: mesh ⚡ nostr™ gossip™ direct
- Язык: CPython (решение сохранять до Phase 15)

## Ссылки

- SNIN Hub: https://snin-hub.v2.site
- NOSTR Relay: https://snin-relay.v2.site
- Supervisor (health): http://127.0.0.1:9900/health
- Degradation status: /system/degradation
- Makefile: `make test` (53 + 25 тестов)

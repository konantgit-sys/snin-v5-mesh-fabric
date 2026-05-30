# SNIN Relay Mesh — V4 Architecture (23 May 2026)

## Текущий статус (AS-IS, 33 сервиса)

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
              Supervisor L9 (:9900)
          35 сервисов под контролем
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

## Graceful degradation (V4)

- Circuit breaker: 4 канала (direct/mesh/gossip/nostr), auto half-open 30s
- Redis fallback: in-memory DHT кэш (graceful_degradation.py)
- Nostr Bridge health monitor: проверка каждые 15 сек
- External Gateway probe: fallback Nostr канала
- Content Router: in-memory dedup без Redis

## Anti-DDoS (V4)

- Max event size: 64 KB
- Rate limiter: per-IP (100/мин), per-pubkey (50/мин)
- Blacklist: мусорные pubkey с TTL 5 мин
- Signature gate: reject без подписи

## Быстродействие

- Throughput: 36,873 msg/s sustained, 194,388 burst
- Каналы: mesh ⚡ nostr™ gossip™ direct
- Язык: CPython (решение сохранять до Phase 15)

## Ссылки

- SNIN Hub: https://snin-hub.v2.site
- NOSTR Relay: https://snin-relay.v2.site
- Supervisor (health): http://127.0.0.1:9900/health
- Degradation status: /system/degradation
- Anti-DDoS stats: /api/v3/stats (в составе)

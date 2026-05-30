# SNIN Mesh — Спецификация Layer 8 v2.2 (финальная)

> Дата: 2026-05-16 | Статус: Утверждена

---

## Итоговая архитектура (после всех правок)

```
АГЕНТЫ ──► Smart Router (:9932) ──► {mesh, gossip, nostr} ──► CR (:9920, Redis dedup) ──► RE (:9910) ──► Relay Mesh (:9907) ──► {SQLite, Redis}
                                              ↑
                                         Heartbeat — ТОЛЬКО из Relay Mesh
```

## Убраны 3 дубля

| Было | Стало |
|------|-------|
| Gossip → RE напрямую (обход CR) | Gossip → CR (через дедубликацию) |
| Heartbeat из 3 мест (Gossip, RE, Mesh) | Heartbeat из 1 места (Relay Mesh) |
| 2 входа для агентов (SR + Gossip) | 1 вход — Smart Router |

## Добавлены

- Redis dedup в CR (вместо in-memory) — персистентная дедубликация
- Circuit Breaker (>500ms → бан 30 сек)
- Consistent hashing для Gossip (1 pubkey → 1 shard)
- Backpressure (очередь >100 → retry_after)
- SQLite WAL tuning + VACUUM
- ESP32 reconnection (exponential backoff)

## Что может агент

| Канал | Куда | Скорость | Размер | Дубль |
|-------|------|:--------:|:------:|:-----:|
| direct | 1:1 | ~2ms | 64 KB | 1x |
| mesh | CR→RE | ~100ms | 500 B | 1-2x |
| gossip | всем | ~50ms | 1 KB | ×15 |
| nostr | 21 relay | ~1-5s | 64 KB | 1x |

За 1 сек: 100 msg (mesh) / 500 msg (gossip)
Приоритет: low=1 канал, normal=best, high=2 канала
Выбор: self-learning + policy + congestion

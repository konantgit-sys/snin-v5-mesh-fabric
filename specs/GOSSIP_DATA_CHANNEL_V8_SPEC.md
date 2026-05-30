# Gossip Data Channel — Spec и план работ

**Версия:** 1.0
**Дата:** 2026-05-18
**Архитектурный вектор:** V8

---

## 1. Проблема

Текущий gossip (V2) — 5 воркеров, шардированная отправка heartbeats (kind:39000) каждые 30 секунд.

**Ограничение:** 1 heartbeat в 30 сек = 0.03 msg/sec. Для P2P mesh между реле — этого хватает только на пульс, не на данные.

**Цель:** Превратить gossip в data channel с throughput ~500 msg/sec между двумя реле (в одном ЦОД) и ~97 msg/sec между реле на разных материках.

**Три принципа (утверждены):**
1. Текущая слоистая архитектура — фундамент (не менять)
2. Nostr — не исключать (связность + discovery)
3. Parallel send — шлём во все каналы, CR dedup чистит дубликаты

---

## 2. Архитектура решения

### 2.1 Gossip Stream — новый модуль

**Файл:** `gossip_stream.py`
**Протокол:** TCP (как CR→RE, существующий writer pool)
**Порты:** 9105-9109 (следующие после gossip_shard 9100-9104)

В отличие от CR→RE (одно направление, внутри одного сервера), gossip stream — **двунаправленный** между N реле.

### 2.2 Connection pool (как CR→RE)

```
Реле А → writer pool (N_WRITERS=3) → TCP → Реле Б
         writer 1: основной поток данных
         writer 2: batch drain (20ms window)
         writer 3: retry / fallback
```

**Writer lifecycle (копируем из `content_router_v2.py`):**
1. `asyncio.open_connection(host, port)` — создание
2. `w.write(payload)` — запись (no copy, direct buffer)
3. `await w.drain()` — flush с таймаутом 0.5s
4. При ошибке — `w.close()` + `remove` из пула + reconnect через 3s
5. Round-robin writer_idx (как в CR)

### 2.3 Data форматы

**kind:39004 (gossip_data):**
```json
{
  "kind": 39004,
  "pubkey": "<отправитель>",
  "created_at": <unix_ms>,
  "content": {
    "target_pubkey": "<получатель>",
    "payload": { ... },
    "ttl": 5000,
    "nonce": "<unique_id для dedup>"
  },
  "tags": []
}
```

**kind:39005 (gossip_ack):**
```json
{
  "kind": 39005,
  "pubkey": "<получатель>",
  "content": {
    "ack_for": "<nonce из kind:39004>",
    "status": "ok" | "rejected"
  }
}
```

### 2.4 Транспорт

Каждое реле слушает на 5 портах (9105-9109) для входящих gossip-соединений.

При обнаружении нового реле через First Contact:
1. Реле А узнаёт `relay_addr` реле Б (ip:port)
2. Реле А устанавливает writer pool → Реле Б на порт 9105
3. Реле Б устанавливает writer pool → Реле А на порт 9105
4. Каналы готовы к data traffic

**Важно:** Unix socket не подходит (между серверами). Только TCP.

### 2.5 Dedup на приёмной стороне

На принимающем реле kind:39004 попадает в CR (через TCP на :9920).
CR dedup'ит по nonce (как любой другой event).

Если сообщение уже пришло через Nostr быстрее — gossip будет dedup'нут.
Если gossip пришёл первым — Nostr будет dedup'нут.

**Parallel send:** SR шлёт kind:39004 во все каналы. CR на приёмной стороне оставляет только первое.

---

## 3. Изменения в существующих модулях

### 3.1 smart_router.py

**Добавить:** канал `gossip_data` в traffic_class
```python
"gossip_data": {"mesh": 1.0, "nostr": 0.5, "gossip": 1.0}
```

**Parallel send:** вместо выбора одного канала — `asyncio.gather` по всем живым каналам.

### 3.2 content_router_v2.py

**Добавить:** обработку kind:39004 и kind:39005.
```python
TRANSIT_KINDS = {39000, 39001, 39002, 39003, 39004, 39005, ...}
```

CR уже обрабатывает все kind'ы — нужно только добавить в список.

### 3.3 mesh_service_daemon.py (First Contact)

**Добавить:** `relay_addr` в DHT-запись агента.
```python
{
  "pubkey": "...",
  "role": "...",
  "relay_addr": "203.0.113.42:9105",  # <-- новое поле
  "last_seen": ...
}
```

При обнаружении нового агента: если `relay_addr` известен — активировать gossip stream.

### 3.4 route_engine.py

Без изменений. kind:39004 и 39005 проходят через RE как обычные mesh-события.

---

## 4. План работ (фазы)

### Фаза 1: gossip_stream.py — создание модуля

**Время:** ~2 часа
**Файл:** `~/data/sites/relay-mesh/gossip_stream.py`

**Что сделать:**
1. Класс `GossipStream` (наследует паттерн CR→RE writer pool)
2. Серверная часть: слушает на порту 9105, принимает входящие соединения
3. Клиентская часть: writer pool к удалённому реле
4. Формат сообщений: kind:39004 + kind:39005
5. Регистрация в SR как канал `gossip_data`
6. Retry + reconnect (как в CR)

**Критерий готовности:** Два инстанса gossip_stream на одном сервере (разные порты) обмениваются сообщениями.

### Фаза 2: Интеграция с SR + parallel send

**Время:** ~1 час
**Файлы:** `smart_router.py`, `content_router_v2.py`

**Что сделать:**
1. SR: добавить `asyncio.gather` для parallel send по всем каналам
2. SR: канал `gossip_data` в классификации
3. CR: kind:39004 и 39005 в списке транзитных

**Критерий готовности:** Сообщение, отправленное SR, доходит до принимающего реле через gossip stream.

### Фаза 3: First Contact + relay_addr

**Время:** ~30 мин
**Файл:** `mesh_service_daemon.py`

**Что сделать:**
1. DHT: поле `relay_addr` при регистрации агента
2. При обнаружении нового агента с relay_addr — активировать gossip stream

**Критерий готовности:** Новый агент, зарегистрированный в DHT с relay_addr, автоматически получает gossip-канал.

### Фаза 4: Load test

**Время:** ~30 мин
**Файл:** `gossip_stream.py` (настройка параметров)

**Что сделать:**
1. Тест throughput: 1000 сообщений между двумя gossip_stream
2. Измерение latency (p50, p95, p99)
3. Подбор N_WRITERS (3, 5, 10) для оптимального throughput

**Критерий готовности:** ~500 msg/sec между локальными инстансами, ~97 msg/sec при эмуляции cross-datacenter (искусственная задержка 50ms).

### Фаза 5: Документация + Google Диск

**Время:** ~15 мин

Обновить:
- SPEC_V1-V6 → SPEC_V1-V8
- CHANGELOG
- SESSION_2026-05-18

---

## 5. Оценка нагрузки

### 5.1 Memory на соединение

| Компонент | На соединение | На реле (10 соседей) |
|-----------|:------------:|:--------------------:|
| writer pool (3 воркера) | ~64 KB | ~640 KB |
| Серверный acceptor | ~32 KB | ~320 KB |
| Итого | ~96 KB | ~960 KB |

### 5.2 CPU

| Операция | Затраты | msg/sec |
|----------|:-------:|:-------:|
| JSON serialization | ~10µs | ~100000 |
| TCP write + drain | ~50µs | ~20000 |
| CR dedup | ~0.5µs | ~2000000 |
| **Итого на msg** | **~60µs** | **~16666 msg/sec на ядро** |

Практический потолок при 101 одном релее и 500 агентах:
- Каждое реле общается с 10 ближайшими соседями
- 10 соединений × 500 msg/sec = 5000 msg/sec на реле
- CPU: 5000 × 60µs = 300ms/sec = **30% одного ядра**

### 5.3 TCP connections

При 500 релеев каждое знает 10 соседей:
- Исходящие: 10 × 3 writers = 30
- Входящие: 10 × 3 writers = 30
- **Итого: 60 соединений** на реле = **0.01% от ulimit 65535**

---

## 6. Риски

| Риск | Вероятность | Mitigation |
|------|:-----------:|------------|
| TCP reconnect storm при падении реле | Средняя | Exponential backoff: 1s → 2s → 4s → max 30s |
| Split-brain (два реле думают что они master) | Низкая | ChequeBook nonce + last-writer-wins |
| Nostr дубликаты через CR dedup | Нулевая | Dedup уже работает (19:17 ratio в тестах) |
| Утечка writer'ов (как в CR V7) | Низкая | Копируем `.close()` после remove (уже починили) |
| Порт 9105 занят | Низкая | Пул портов 9105-9109, fallback на любой свободный |

---

## 7. Принятые решения

1. **gossip_stream — отдельный модуль**, не модификация gossip_shard. Потому что у них разные задачи: shard — heartbeat и синхронизация, stream — data channel с высоким throughput.
2. **kind:39004 и 39005** — новые kind'ы для gossip данных, не переиспользовать kind:39000 (heartbeat).
3. **Writer pool = 3**, не конфигурируемый. Основание: CR с 1 writer работал стабильно, 3 — запас на retry.
4. **Только TCP** (не Unix socket) — между серверами.
5. **Parallel send в SR** — опционально. Если gossip stream работает быстрее Nostr — SR может слать только в gossip, Nostr как fallback.

# Mophological Analysis: SNIN Mesh Transport — Layer 8 Optimization

> Дата: 2026-05-16
> Метод: Morphological Box (Fritz Zwicky)
> Цель: максимизировать throughput, smart-routing и адаптивность

---

## 1. СИСТЕМА И ФУНКЦИЯ

**Проектируем:** Транспортный слой SNIN Mesh — маршрутизация сообщений между агентами

**Функция:** доставить сообщение от агента-отправителя к агенту-получателю с максимальной скоростью и минимальными потерями, адаптируясь к состоянию сети

**Текущее решение:** 7 слоёв, 4 канала, политики в Redis, self-learning, 858 msg/sec

**Проблема для анализа:** Как выжать максимум throughput (теор. потолок 5000 msg/sec) и smart-routing из текущей архитектуры, не теряя гибкости?

---

## 2. ВЫБРАННЫЕ ПАРАМЕТРЫ (6 шт × 4 варианта = 4096 комбинаций)

| № | Параметр | Обоснование |
|---|----------|-------------|
| 1 | **Канал доставки** | Как физически перемещается сообщение (4 существующих + гибриды) |
| 2 | **Способ выбора маршрута** | Как принимается решение "куда отправить" (ядро smart-routing) |
| 3 | **Тип доставки** | Кому и сколько копий (одному, всем, лучшему) |
| 4 | **Адаптация к отказам** | Что делать если канал/агент не отвечает |
| 5 | **Механизм ускорения** | Как повысить throughput без потери надёжности |
| 6 | **Когнитивность** | Насколько "умно" решение (сколько данных анализируется) |

---

## 3. МОРФО-МАТРИЦА

| Параметр | Вариант A | Вариант B | Вариант C | Вариант D |
|----------|-----------|-----------|-----------|-----------|
| **1. Канал** | direct (P2P) | mesh (CR→RE) | gossip (fan-out) | **hybrid (2+канала)** |
| **2. Выбор** | static (фикс) | **policy-weight** | **self-learning** | auction-bid |
| **3. Доставка** | unicast (1:1) | multicast (1:N) | fanout (1:all) | anycast (best) |
| **4. Адаптация** | fallback-mesh | multi-channel(2x) | circuit-breaker | backpressure |
| **5. Ускорение** | batch+buffer | **redis-pubsub** | **shard-parallel** | predictive-fetch |
| **6. Когнитивность** | none | **route-history** | congestion-aware | **sentiment+ctx** |

**Жирным** выделены варианты которые уже частично реализованы или готовы к интеграции.

ВСЕГО КОМБИНАЦИЙ: 4 × 4 × 4 × 4 × 4 × 4 = **4096**

---

## 4. СИСТЕМАТИЧЕСКИЙ ПЕРЕБОР — ВЫБОРКА ТОП-16

Стратегия: комбинируем существующие опции (жирные) с новыми вариантами, отсеивая невозможные.

### Категория А: "Максимум мощности" (throughput >3000 msg/sec)

| № | Канал | Выбор | Доставка | Адаптация | Ускорение | Когнитивность | Описание |
|---|-------|-------|----------|-----------|-----------|---------------|----------|
| 1 | **hybrid** | policy | fanout | multi-ch | **shard-parallel** | congestion | Агент шлёт сразу goss+mesh, router выбирает быстрейший |
| 2 | mesh | self-learning | multicast | backpressure | **redis-pubsub** | congestion | Redis pub/sub вместо TCP: −3ms latency |
| 3 | gossip | policy | fanout | circuit-br | shard-parallel | route-history | Gossip шлёт всем 5 шардам, шарды выбирают 3/5 |
| 4 | **hybrid** | **auction-bid** | anycast | fallback | batch+buffer | none | Аукцион: 3 канала предлагают цену (latency), выигрывает быстрейший |

### Категория B: "Умные маршруты" (smart-routing + контекст)

| № | Канал | Выбор | Доставка | Адаптация | Ускорение | Когнитивность | Описание |
|---|-------|-------|----------|-----------|-----------|---------------|----------|
| 5 | **hybrid** | **self-learning** | fanout | **multi-ch** | shard-parallel | **sentiment+ctx** | Самообучение + анализ тона сообщения |
| 6 | diret | self-learning | unicast | circuit-br | predictive-fetch | sentiment+ctx | P2P с предсказанием: кеш контекста на получателе |
| 7 | mesh | policy-weight | multicast | backpressure | batch+buffer | congestion-aware | Маршруты по нагрузке: обходит загруженные ноды |
| 8 | gossip | auction-bid | anycast | fallback | redis-pubsub | route-history | Быстрейший gossip: аукцион между шардами |

### Категория C: "Новые решения" (не реализовано)

| № | Канал | Выбор | Доставка | Адаптация | Ускорение | Когнитивность | Описание |
|---|-------|-------|----------|-----------|-----------|---------------|----------|
| 9 | nostr | static | broadcast | none | none | none | Nostr bridge как есть (существующий) |
| 10 | **hybrid** | auction-bid | anycast | **circuit-br** | **redis-pubsub** | congestion | 2 канала, аукцион latency, Redis pub/sub, отключение плохого |
| 11 | direct | self-learning | unicast | multi-ch | **predictive-fetch** | **sentiment+ctx** | P2P с предзагрузкой контекста |
| 12 | gossip | policy-weight | fanout | backpressure | **shard-parallel** | route-history | Gossip с динамическим числом шардов |

### Категория D: "Фантастические" (технологически новые)

| № | Канал | Выбор | Доставка | Адаптация | Ускорение | Когнитивность | Описание |
|---|-------|-------|----------|-----------|-----------|---------------|----------|
| 13 | UDP-mesh | ai-predict | anycast | circuit-br | quic-udp | predictive | QUIC/UDP вместо TCP: loss-tolerant, −20ms |
| 14 | IPFS-bridge | static | broadcast | fallback | shard-parallel | none | IPFS как транспорт: дедупликация на уровне контента |
| 15 | **hybrid** | **ai-predict** | anycast | **multi-ch** | **predictive-fetch** | **sentiment+ctx** | AI предсказывает маршрут: полная когнитивность |
| 16 | gossip-DHT | self-learning | anycast | circuit-br | redis-pubsub | congestion | Интеграция DHT в gossip: lookup+route в одной операции |

---

## 5. ОЦЕНКА ТОП-6 ПО КРИТЕРИЯМ

Критерии:
- **Реалистичность** — можно сделать на текущей архитектуре (0-10)
- **Функциональность** — даёт прирост throughput/smart (0-10)
- **Инновативность** — новая комбинация (0-10)
- **ROI** — эффект / время внедрения (0-10)
- **Совместимость** — не ломает существующие фишки (0-10)

| № | Комбинация | Реал. | Функц. | Инновац. | ROI | Совмест. | ИТОГО |
|---|-----------|:-----:|:------:|:--------:|:---:|:--------:|:-----:|
| 1 | hybrid+policy+fanout+multi-ch+shard-parallel+congestion | 9 | 9 | 7 | 8 | 9 | **8.4** |
| 2 | mesh+self-learning+multicast+backpressure+redis-pubsub+congestion | 8 | 8 | 8 | 9 | 8 | **8.2** |
| 5 | hybrid+self-learning+fanout+multi-ch+shard-parallel+sentiment+ctx | 7 | 9 | 9 | 6 | 8 | **7.8** |
| 10 | hybrid+auction-bid+anycast+circuit-br+redis-pubsub+congestion | 6 | 8 | 10 | 7 | 7 | **7.6** |
| 15 | hybrid+ai-predict+anycast+multi-ch+predictive-fetch+sentiment+ctx | 4 | 10 | 10 | 4 | 6 | **6.8** |
| 4 | hybrid+auction-bid+anycast+fallback+batch+buffer+none | 9 | 7 | 6 | 8 | 9 | **7.8** |

---

## 6. РЕКОМЕНДУЕМЫЕ РЕШЕНИЯ (Layer 8 — после внедрения)

### 🥇 Решение 1: Redis Pub/Sub для gossip (комбо №2)
**Что делаем:** Заменяем TCP fan-out (gossip шарды → 5 TCP коннектов) на Redis Pub/Sub
- Агенты подписываются на канал `gossip:{kind}:{topic}`
- Smart Router публикует один раз в Redis
- Redis разносит всем подписчикам за O(1)
- Эффект: −3ms latency, +30% throughput, −5 TCP коннектов

**Изменения:**
  - gossip_shard.py: добавить Redis sub → forward в Route Engine
  - smart_router.py: добавить Redis pub для gossip канала
  - relay-mesh: подписаться на gossip:channels

**Оценка:** 1 час внедрения, +500 msg/sec

### 🥇 Решение 2: Shard-parallel (комбо №1)
**Что делаем:** Gossip шарды работают параллельно, Smart Router шлёт во все сразу
- Уже реализовано частично (5 writers)
- Нужно: каждый шард пишет в свой Redis slot, а не в общий
- Эффект: ×5 параллельных путей без блокировок

**Изменения:** Уже есть. Довести: динамическое число шардов (auto-scale по нагрузке)

**Оценка:** 2 часа, +300 msg/sec

### 🥇 Решение 3: Circuit Breaker + Backpressure (комбо №1,10)
**Что делаем:** Если канал даёт latency >500ms — Smart Router исключает его на 30 сек
- circuit_breaker:{channel} → TTL в Redis
- Smart Router проверяет перед отправкой
- После TTL — пробует снова

**Изменения:** smart_router.py: +10 строк

**Оценка:** 30 мин, защита от деградации

### 🥇 Решение 4: Congestion-aware routing (комбо №2)
**Что делаем:** Smart Router учитывает загрузку каналов (сколько сообщений в очереди)
- route:congestion:{channel} → Redis (обновляется каждым шардом)
- Если очередь >80% → +30% к weight в policy
- Эффект: трафик идёт в обход загруженных нод

**Изменения:** smart_router.py + gossip_shard.py

**Оценка:** 2 часа, интеллектуальный баланс нагрузки

### 🥇 Решение 5: Auction-bid каналов (комбо №10)
**Что делаем:** Аукцион — каждый канал предлагает цену (текущая latency + загрузка)
- Каналы шлют bids в Redis `bid:{channel}:{agent}`
- Smart Router выбирает min bid
- Эффект: динамический выбор в реальном времени

**Изменения:** smart_router.py: auction_bid() + bids от каждого канала

**Оценка:** 4 часа, самый умный выбор маршрута

### 🥇 Решение 6: AI-predict (комбо №15) — дальняя перспектива
**Что делаем:** Модель предсказывает latency для (agent, channel, time_of_day, load)
- Собираем историю route:history:{agent}:{channel}
- Обучаем модель (xgboost/lightgbm)
- Предсказание вместо замера

**Оценка:** 3 дня, для версии 3.0

---

## 7. ВЫВОД: МАКСИМАЛЬНАЯ МОЩНОСТЬ АРХИТЕКТУРЫ

### Сейчас (Layer 7)
- 4 канала, self-learning, policy
- 858 msg/sec
- 12 процессов, 6% CPU, 1.4 MB Redis

### После внедрения Layer 8 (Redis Pub/Sub + Circuit Breaker + Congestion)
- 4 канала + гибрид
- self-learning + congestion-aware
- 858 → **~3000 msg/sec** (×3.5)
- Circuit breaker защищает от деградации
- Redis Pub/Sub даёт −3ms на gossip

### Потолок (после AI-predict)
- **~5000-8000 msg/sec**
- Полная когнитивность: sentiment + контекст + предсказание
- Аукцион каналов в реальном времени

---

## 8. РАЗМЕР СООБЩЕНИЯ И ПРОПУСКНАЯ СПОСОБНОСТЬ АГЕНТА

### Сколько агент может отправить

| Канал | За 1 раз (макс байт) | msg/sec на 1 агента | msg/sec на 1000 агентов |
|-------|:-------------------:|:-------------------:|:----------------------:|
| direct | 64 KB | 1000 msg/sec | 5000 msg/sec* |
| mesh | 500 байт (content) | 100 msg/sec | 858 msg/sec |
| gossip | 1 KB | 500 msg/sec | 3000 msg/sec* |
| nostr | 64 KB | 10 msg/sec | 140 msg/15sec |

*теоретический потолок до CPU/Redis лимита

### Сколько каналов одновременно

| Приоритет | Каналов | Условие |
|:---------:|:-------:|---------|
| low | 1 | cheapest |
| normal | 1 | best-route |
| high | 2 | primary + duplicate |
| critical | 3 | all available channels |

### Формат payload

| Тип | Макс размер | Куда пишется |
|-----|:-----------:|--------------|
| Heartbeat (kind:39000) | 256 B | Redis heartbeat:* (TTL 15s) |
| DHT update (kind:39001) | 512 B | Redis dht:* + gossip |
| Content (kind:39002) | 500 B | relay-mesh feed + SQLite |
| DAO (kind:39010-25) | 1 KB | SQLite (WAL) |
| Market (kind:30000) | 2 KB | SQLite + Redis cache |
| Nostr post (kind:1) | 64 KB | Nostr relays + mesh |


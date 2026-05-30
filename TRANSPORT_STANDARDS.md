# STANDARD: Транспортный слой SNIN Mesh — Архитектура и Протоколы

> Версия: v2.0 | Дата: 2026-05-16 | Статус: ✅ Production

---

## 1. Архитектура (7 слоёв)

```
┌──────────────────────────────────────────────────────────────────┐
│                        ВНЕШНИЙ МИР                               │
│  Nostr (101 relay)     ESP32 / curl     SNIN agents (cryter...)  │
└───────────────────────────┬──────────────────────────────────────┘
                            │
┌───────────────────────────▼──────────────────────────────────────┐
│  Layer 7: Smart Router   TCP :9932   (выбор канала по политике)  │
│  ┌────────┬────────┬──────────┬──────────┐                      │
│  │ direct │  mesh  │  gossip  │  nostr   │                      │
│  │  ~2ms  │ ~100ms │  ~50ms   │  ~1-5s   │                      │
│  └────┬───┴───┬────┴────┬─────┴────┬─────┘                      │
└───────┼───────┼─────────┼──────────┼────────────────────────────┘
        │       │         │          │
┌───────▼───────┴─────────┴──────────┴────────────────────────────┐
│  Layer 6: External Gateway   TCP :9931                          │
│  Nostr Protocol (WSS) + TCP raw → mesh format                   │
└─────────────────────────────────────────────────────────────────┘
        │
┌───────▼──────────────────────────────────────────────────────────┐
│  Layer 5: Content Router + SQLite WAL    TCP :9920              │
│  Дедубликация (5 sec window), деление change >15%, 5 writerов    │
└───────┬──────────────────────────────────────────────────────────┘
        │
┌───────▼──────────────────────────────────────────────────────────┐
│  Layer 4: Route Engine + WS Data Channel    TCP :9910           │
│  Классификация: kind:39000 bypass, batch → WS :9907             │
│  Flush rate: 59/сек (WS), batch size: 140-170                   │
└───────┬──────────────────────────────────────────────────────────┘
        │ WS :9907
┌───────▼──────────────────────────────────────────────────────────┐
│  Layer 3: Relay Mesh (Flask + HTTP Relay)    TCP :9907          │
│  Redis DHT (dht:* keys) + in-memory feed (1000 buf)             │
│  5 gossip shards fan-out ×3 = 15 копий                          │
└───────┬──────────────────────────────────────────────────────────┘
        │
┌───────▼──────────────────────────────────────────────────────────┐
│  Layer 2: Redis Cache    TCP :6379                              │
│  dht:agent:* (TTL 300s) | heartbeat:* (TTL 15s) | route:*      │
│  policy:routes (kind→channel) | gossip:pub/sub                  │
└───────┬──────────────────────────────────────────────────────────┘
        │
┌───────▼──────────────────────────────────────────────────────────┐
│  Layer 1: SQLite (WAL mode) + in-memory DHT / OrderedDict       │
│  1000 feed buffer, kind classification                          │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. Каналы доставки (сравнение)

| # | Канал | Протокол | Порт | Латентность | Пропускная | Надёжн. | Когда выбирать |
|:-:|-------|----------|:----:|:-----------:|:----------:|:-------:|----------------|
| 1 | **direct** | TCP P2P | динам. | ~2ms | 5000/сек | ⭐⭐ | P2P внутри mesh, быстрый обмен |
| 2 | **gossip** | TCP broadcast | 9100-4 | ~50ms | 3000/сек | ⭐⭐⭐ | Широковещательно, heartbeat, DHT |
| 3 | **mesh** | TCP → WS | 9920→9910 | ~100ms | 858/сек (×85.8) | ⭐⭐⭐ | Дефолт, content, DAO, market |
| 4 | **nostr** | WSS (21 relay) | 9931 | ~1-5s | 140/15s | ⭐⭐ | Публичные посты, внешняя сеть |

### 2.1 direct (P2P)
- **Подключение:** клиент → Smart Router (:9932) → DHT lookup → TCP к целевому агенту
- **Формат:** line-based JSON, строка = 1 событие
- **Ограничения:** агент должен быть зарегистрирован в DHT с ip + port
- **Self-healing:** если direct упал → fallback на mesh

### 2.2 gossip (Broadcast)
- **Подключение:** Smart Router → 5 gossip шардов (:9100-9104)
- **Форвард:** каждый шард fan-out ×3 случайным peers = 15 копий
- **Дубли:** Content Router дедублицирует (5 sec window)
- **Redis DHT:** каждый шард пишет dht:{agent_id} + heartbeat:{agent_id}
- **Anti-entropy:** каждые 60 сек перебалансировка peers

### 2.3 mesh (Основной)
- **Цепочка:** Content Router (:9920) → Route Engine (:9910) → relay-mesh (:9907/WS)
- **Дедубликация:** 5-секундное окно, change detection (>15%)
- **Batch-флаш:** 59 раз/сек через WS, ~140-170 событий/batch
- **Bypass:** kind:39000 (heartbeat) не идёт в SQLite, только в heartbeat.log + Redis

### 2.4 nostr (Внешний)
- **Подключение:** Nostr Gateway (:9931) → 21 релей (WSS)
- **Конверсия:** kind:1 → kind:39002 (через content: { type: "nostr_post" })
- **Входящие:** 140 событий/15 сек (с 9 релеев)
- **Блокировки:** прокси может отклонять (HTTP 500/503) — норма

---

## 3. Маршрутные политики (Redis `policy:routes`)

### 3.1 Таблица правил

| Kind | Описание | Каналы (weight) | Приоритет |
|------|----------|----------------|:---------:|
| 1 | Nostr текст | nostr:1.0 | normal |
| 30000 | Рыночные данные | mesh:0.5, gossip:0.5 | normal |
| 39000 | Heartbeat | gossip:0.9, mesh:0.1 | low |
| 39001 | DHT update | gossip:0.7, direct:0.3 | normal |
| 39002 | Content (текст) | mesh:0.6, nostr:0.4 | normal |
| 39003 | Reaction (лайк) | mesh:0.5, gossip:0.5 | low |
| 39010-39025 | DAO / Governance | mesh:1.0 | high |
| default | Всё остальное | mesh:0.7, gossip:0.3 | normal |

### 3.2 Self-learning (как работает)

```
Каждое сообщение:
  1. Smart Router выбирает канал по политике (с поправкой на best)
  2. Отправляет
  3. Замеряет latency
  4. Пишет в route:history:{agent}:{channel}
  5. Считает скользящее среднее (100 замеров)
  6. Если канал X стабильно быстрее канала Y на >20% → best switching

Формат Redis:
  route:history:{agent}:{channel}  → sorted set  (score=latency_ms)
  route:best:{agent}                → string (лучший канал)
  route:stats:{agent}:{channel}    → hash {sent, failed, avg_latency}
```

### 3.3 Multi-channel (критические сообщения)

При `priority=high`:
- Основной канал (из политики) + 1 дубль (второй по весу)
- Если основной упал → fallback на mesh
- Получатель отбрасывает дубли через Content Router (5 sec window)

---

## 4. Протоколы сообщений

### 4.1 Вход в Smart Router (TCP :9932)

```json
{
  "from": "agent_name",
  "to": "target_agent_or_broadcast",
  "kind": 39002,
  "pubkey": "hex_public_key",
  "payload": {"text": "..."},
  "meta": {
    "priority": "high|normal|low",
    "channel": "auto|direct|mesh|gossip|nostr",
    "ttl": 60,
    "max_hops": 3,
    "ack": true
  }
}
```

### 4.2 Формат события в mesh (Content Router / Route Engine)

```json
{
  "id": "sha256_hash",
  "kind": 39002,
  "pubkey": "hex_key",
  "content": "{\"from\":\"agent_x\",\"seq\":42,\"payload\":{...}}",
  "created_at": 1778886000,
  "sig": "hex_signature"
}
```

### 4.3 WebSocket (relay-mesh :9907/ws)

```json
{
  "events": [{"id":"..","kind":39002,"pubkey":"..","content":"..","created_at":..,"sig":".."}, ...]
}
```

Frame rate: ~59/сек, batch size: 140-170 событий

### 4.4 Nostr Gateway (TCP :9931)

```json
{"kind": 1, "pubkey": "hex", "content": "text", "created_at": 1778886000}
```
Gateway конвертирует kind:1 → kind:39002 с content: { type: "nostr_post" }

---

## 5. Redis ключи (пространство имён)

| Префикс | Назначение | TTL | Формат значения |
|---------|-----------|:---:|-----------------|
| `dht:agent:{id}` | Регистрация агента | 300s | `{"pubkey","ip","port","shard","last_seen"}` |
| `heartbeat:{id}` | Признак жизни | 15s | `{"ts": unix}` |
| `dht:{id}` | DHT запись от gossip | 60s | `{"pubkey","shard","port","data"}` |
| `route:history:{a}:{ch}` | История маршрутов | 86400s | sorted set (score=latency_ms) |
| `route:best:{a}` | Лучший канал | 86400s | string: "mesh" |
| `route:stats:{a}:{ch}` | Статистика канала | 86400s | hash `{sent, failed, avg_latency}` |
| `policy:routes` | Маршрутные политики | ∞ | hash `{kind_range: channel_weights_json}` |

---

## 6. Порты (полная карта)

| Порт | Сервис | Протокол | Назначение |
|:----:|--------|:--------:|-----------|
| 6379 | Redis | TCP | DHT, heartbeat, route_*, policy:* |
| 9100-9104 | Gossip Shards (×5) | TCP | Приём агентов, fan-out, forward в RE |
| 9907 | relay-mesh | HTTP+WS | Flask + sockio (feed, stats, DHT API) |
| 9910 | Route Engine | TCP | Классификация, batch, bypass, WS |
| 9920 | Content Router | TCP | Дедубликация, change detection, 5 writers |
| 9931 | External Gateway | TCP | Nostr kind:1 → mesh, ESP32 input |
| 9932 | Smart Router | TCP | Выбор канала, политики, self-learning |

---

## 7. Self-Improvement (как агенты улучшают маршруты)

### 7.1 Автоматический выбор канала

```
1. Агент шлёт channel=auto
2. Smart Router: get_policy_for_kind(kind) → {mesh:0.6, nostr:0.4}
3. pick_channel_from_policy(): проверяет route:best:{agent}
   - Если best есть в политике → берём его
   - Иначе → случайный выбор по весам
4. После отправки: record_route(agent, channel, latency, ok)
5. Если avg_latency нового канала < 80% от текущего best → переключение
```

### 7.2 Адаптация к отказам

- Канал упал → Smart Router помечает и исключает на 30 сек (через Redis)
- Если все каналы упали → mesh (жёсткий fallback)
- Если mesh упал → повтор через 1 сек, 3 попытки → ошибка клиенту
- Агент может повторить с другим TTL

### 7.3 Ежедневное самообучение

Специальный cron-агент (когда появится):
- Анализирует route:history:* за 24 часа
- Строит матрицу latency (agent × channel)
- Обновляет route:best:* для всех агентов
- Оптимизирует policy:routes (веса каналов)

---

## 8. Соглашения и ограничения

### 8.1 Именование
- `kind:39000-39099` — mesh/system events (зарезервировано)
- `kind:1` — Nostr текст (внешние посты)
- `kind:7` — Nostr reaction (лайки)
- `kind:30000` — рыночные данные (Solana)
- `agent_id` — 16 символов (первые 16 hex pubkey или префикс)
- `pubkey` — hex-строка (64 символа для Nostr/Schnorr, 40 для ETH)

### 8.2 Rate limits (текущие)
- relay-mesh: 1000 msg/сек (входящие)
- Route Engine: 1500 msg/сек (через WS)
- Gossip шард: 500 msg/сек (на шард, ×5 = 2500)
- Smart Router: 1000 msg/сек (входящие, до CPU bound)
- Nostr Gateway: 140 events/15 сек (ограничение релеев)

### 8.3 Безопасность
- Прямого доступа к relay-mesh из интернета нет
- Smart Router доступен только внутри pod (localhost)
- Nostr Gateway — публичные релеи, без аутентификации
- В будущем: подпись kind:39000-39099 через Schnorr/Secp256k1

---

## 9. Диагностика

```bash
# Проверка всех каналов
redis-cli keys 'dht:*' | wc -l          # DHT entries
redis-cli keys 'heartbeat:*' | wc -l   # Live agents
redis-cli keys 'route:*' | wc -l       # Route learning data

# Smart Router статус
curl localhost:9907/api/agents/live      # Live agents (heartbeat <15s)
curl localhost:9907/api/dht/stats        # DHT размер
curl localhost:9907/api/ingest/stats     # Ingest throughput

# Проверка конкретного маршрута
redis-cli zrange 'route:history:agent_x:mesh' 0 -1 withscores
redis-cli get 'route:best:agent_x'
redis-cli hgetall 'route:stats:agent_x:mesh'

# Политики
redis-cli hgetall 'policy:routes'
```

---

## 10. Roadmap (следующие улучшения)

| # | Улучшение | Когда |
|:-:|-----------|:-----:|
| 1 | **gossip через Redis Pub/Sub** (вместо TCP дубляжа) | След. спринт |
| 2 | **Cron-агент обучения** — автоанализ route:history | След. спринт |
| 3 | **Rate limit per agent** (anti-spam) | При нагрузке |
| 4 | **E2E encryption** (kind:39000-39099 с подписью) | V3 |
| 5 | **Дашборд маршрутов** — визуализация графа | V3 |

---

*© 2026 SNIN Mesh — Стандарт транспорта v2.0*

# SNIN V5 — SPEC: Новые слои

**Дата:** 2026-06-03
**Основание:** Google Doc «SNIN V5 — Архитектурный реестр: что добавить»
**Источник:** https://docs.google.com/document/d/1QeoH1UjVBilgINywBUZ7gP7WTWqXIzDxbbrywp9GgV4
**Репозиторий (рабочий):** `/home/agent/data/sites/relay-mesh/`
**Репозиторий (git):** `/home/agent/data/projects/snin-v5-mesh-fabric/`

---

## Фазовая структура

| Фаза | Слой | Название | Зависимости | Приоритет |
|------|------|----------|-------------|-----------|
| 1 | L5T | Temporal Dead-Letter Layer | Smart Router, Nostr kind:9000 | 🔴 КРИТИЧЕСКИЙ |
| 2A | L13 | Health Monitor (доработка) | Health Check Engine v2.1 | 🟡 ВЫСОКИЙ |
| 2B | L14 | Alert Engine | L13, Telegram/Nostr | 🟡 ВЫСОКИЙ |
| 3 | L15 | Auto-Recovery | L13, L14 | 🟡 ВЫСОКИЙ |
| 4 | L2C | Cloudflare Durable Object | Cloudflare аккаунт | 🔵 СРЕДНИЙ |
| 5A | L2A | Azure Blob Lease Heartbeat | Azure аккаунт | 🔵 СРЕДНИЙ |
| 5B | L4E | Ethernet PHY Subliminal | Linux mdio, Ethernet | 🔵 СРЕДНИЙ |

---

# ФАЗА 1 — L5T: Temporal Dead-Letter Layer

## Статус: ❌ НЕ РЕАЛИЗОВАН

### Длительность: 3 дня (код) + 1 день (тесты)

### Назначение
Система асинхронной доставки сообщений, когда получатель офлайн. Если агент пропал на 2 месяца — он получит всё, что пришло в его отсутствие.

### Что меняется в архитектуре
- **До V5:** агент теряет сообщения, если был офлайн (нет очереди)
- **После L5T:** появление в сети → L5T sync → доставка всех пропущенных

### Архитектура

```
[Отправитель] → [Smart Router] → [5+ релеев kind:9000] → ...
                                                         ↓ (агент офлайн)
[Получатель] → [Sync: get_deadletters(since=X)] → [Релеи] → [Доставка]
```

**Протокол:**
- Nostr kind:9000 — dead-letter event
- Replication factor: 3 (минимум 5 копий на разных релеях)
- TTL: 90 дней (обычные), 365 дней (PRIORITY_CRITICAL)
- Сообщения ПОЛНОСТЬЮ зашифрованы — только получатель расшифровывает

### Структура БД

**SQLite: `dead_letter_queue`**
```sql
CREATE TABLE dead_letter_queue (
    hash TEXT PRIMARY KEY,          -- SHA256(content + from + to)
    from_pubkey TEXT NOT NULL,       -- отправитель
    to_pubkey TEXT NOT NULL,         -- получатель
    content_enc TEXT NOT NULL,       -- зашифрованное содержимое
    created_at INTEGER NOT NULL,     -- unix timestamp
    priority TEXT DEFAULT 'NORMAL',  -- NORMAL | HIGH | CRITICAL
    ttl INTEGER NOT NULL,            -- unix timestamp удаления
    delivered INTEGER DEFAULT 0,     -- 0=нет, 1=доставлено
    delivery_at INTEGER,             -- когда доставлено
    relay_count INTEGER DEFAULT 0    -- на скольких релеях опубликовано
);

CREATE INDEX idx_dead_letter_to ON dead_letter_queue(to_pubkey);
CREATE INDEX idx_dead_letter_ttl ON dead_letter_queue(ttl);
CREATE INDEX idx_dead_letter_priority ON dead_letter_queue(priority);
```

### Модули для реализации

#### 1. `dead_letter.py` — ядро (250-350 строк)
**API:**
- `DeadLetterQueue(db_path: str)` — инициализация SQLite
- `push(from_pubkey, to_pubkey, content, priority='NORMAL')` — шифрование + сохранение + публикация на 5+ релеях
- `pull(to_pubkey, since=None)` — получение всех не доставленных сообщений для получателя
- `mark_delivered(hash)` — отметить как доставленное
- `purge_expired()` — TTL-очистка (запускается по cron раз в час)
- `stats()` — статистика (всего, по приоритетам, по статусу доставки)

**Логика публикации:**
1. Выбрать 5+ релеев из TIER 1-2 (релеи с поддержкой delete)
2. Опубликовать kind:9000 на каждом
3. Верифицировать подтверждение от релея
4. Если релей не ответил — заменить другим из пула
5. Записать в БД количество успешных публикаций

**Логика sync:**
1. Получатель появляется в сети
2. Запрос `get_deadletters(since=X)` ко всем релеям
3. Получение kind:9000 от каждого релея
4. Дедупликация по hash (один и тот же event на 5 релеях)
5. Расшифровка через ключ получателя
6. Сортировка по created_at
7. Доставка по одному
8. Отметка delivered в БД (локально) + delete-запрос на релеях

#### 2. Интеграция в Smart Router (100-150 строк)
- Перехват исходящих событий
- Проверка статуса получателя (on/offline через heartbeat)
- Если офлайн → `DeadLetterQueue.push()` вместо прямой отправки
- Добавить middleware-слой в RequestPipeline

**Место вставки в `smart_router.py`:**
```python
# После маршрутизации, перед публикацией
if not recipient_online:
    dlq.push(from_pubkey, to_pubkey, content, priority)
    return {"status": "queued", "ttl": ttl}
```

#### 3. Sync-протокол (100-150 строк)
- Endpoint `/api/v1/deadletter/sync` (health port :9999)
- Query: `since` (int, unix timestamp последнего sync)
- Response: массив kind:9000 событий
- Фоновый task: при старте агента выполнить sync

#### 4. TTL-очистка (50 строк)
- Фоновая задача раз в час
- `DELETE FROM dead_letter_queue WHERE ttl < now() AND delivered = 1`
- Для не доставленных: перепубликация с обновлённым TTL

### Критерии готовности
- [ ] Два агента: А пишет, Б офлайн → А в очереди
- [ ] Б появляется → получает все пропущенные
- [ ] Шифрование end-to-end (только Б расшифровывает)
- [ ] TTL-очистка удаляет просроченные
- [ ] 11+ unit-тестов (push, pull, encrypt/decrypt, ttl, dedup)
- [ ] Интеграционный тест: публикация на 5 релеях

---

# ФАЗА 2A — L13: Health Monitor (доработка)

## Статус: 🟡 ЧАСТИЧНО РЕАЛИЗОВАН

### Длительность: 2 дня

### Что уже есть
- Health Check Engine v2.1 — запущен, проверяет /health каждые 5 сек
- REST API :9999 `/api/health/*`
- Supervisor: 41 сервис
- write JSON в health_status.json

### Чего нет (что реализовать)

#### 1. WebSocket стриминг live-статусов (100 строк)
**Модуль:** `health_ws.py`
**Место:** `/api/v1/health/ws`
**Payload:**
```json
{
  "type": "status_change",
  "service": "nostr_bridge",
  "from": "alive",
  "to": "dead",
  "timestamp": 1717200000
}
```
**Требования:**
- async WebSocket (websockets library)
- Broadcast всем подключённым клиентам
- При коннекте — отправка текущего состояния всех сервисов
- Heartbeat ping/pong каждые 10 сек

#### 2. Telegram/Nostr алерты (150 строк)
**Модуль:** `health_alerts.py`
**Триггеры:**
- Сервис dead > 60 сек → Telegram
- 3+ сервиса dead одновременно → Nostr DM (критический)
- Supervisor restarts > 5 за час → Telegram + Nostr
- RAM > 80% → Telegram (warning)

**Интеграция:**
- Telegram: POST к боту (cryter_bot или прямой API)
- Nostr: публикация kind:9001 (alert event)

#### 3. SNIN Hub Dashboard — health-виджет (1 день, фронтенд)
**Данные для виджета:**
- Общий статус: 🟢 41/41 | 🟡 N dead | 🔴 M dead
- Список сервисов с фильтром по слою
- График uptime за последние 24 часа
- Последние алерты

**API endpoint:** GET `/api/v1/health/dashboard` — возвращает JSON для виджета

#### 4. История uptime/downtime — SQLite логи (80 строк)
**Таблица:**
```sql
CREATE TABLE health_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service_name TEXT NOT NULL,
    status TEXT NOT NULL,         -- alive | dead | degraded
    checked_at INTEGER NOT NULL,
    duration_sec INTEGER DEFAULT 0
);
CREATE INDEX idx_health_service ON health_history(service_name, checked_at);
```

### Критерии готовности
- [ ] WebSocket endpoint возвращает live-статусы
- [ ] Алерт в Telegram при падении сервиса
- [ ] Nostr DM при критическом падении
- [ ] Дашборд показывает 41/41
- [ ] История uptime за 24 часа

---

# ФАЗА 2B — L14: Alert Engine

## Статус: ❌ НЕ РЕАЛИЗОВАН

### Длительность: 3 дня

### Назначение
Система правил алертов по слою, по сервису, по порогу. Множественные каналы доставки. Эскалация.

### Что уже есть
- `alert.log` — пишется
- `alerts.json` — хранит состояние в relay-mesh

### Архитектура

```
[Health Monitor] → [Alert Engine] → [Telegram]
                  ↓                 [Nostr DM]
              [Rule Config]         [Webhook]
              (YAML/JSON)           [Эскалация]
```

### Модули для реализации

#### 1. `alert_engine.py` — ядро (200-300 строк)
**API:**
- `AlertEngine(config_path)` — загрузка правил из YAML
- `evaluate(service_name, status, metrics)` — проверка всех правил
- `trigger(rule, event)` — отправка по каналам
- `escalate(alert_id)` — повышение уровня

**Формат правил (YAML):**
```yaml
rules:
  - name: "nostr_bridge_dead"
    service: "nostr_bridge"
    condition: "status == 'dead'"
    duration: 60  # сек до срабатывания
    channels:
      - telegram
      - nostr_dm
    escalation:
      - after: 300  # 5 мин
        channels: [telegram, nostr_dm, webhook]
      - after: 900  # 15 мин
        channels: [telegram, webhook, call]
    priority: HIGH
    
  - name: "ram_high"
    service: "*"
    condition: "metrics.ram_pct > 80"
    duration: 300
    channels: [telegram]
    priority: WARNING
```

#### 2. Telegram-канал (50 строк)
- Интеграция с Telegram API
- Формат: `🚨 [ALERT] {rule.name} — {service} — {status}`
- Кнопки: `/ack {alert_id}` (подтверждение получения)

#### 3. Nostr-канал (50 строк)
- Публикация kind:9001 (alert event)
- Структура тега: `["alert", alert_id, priority, timestamp]`
- Получатель: pubkey оператора

#### 4. Webhook-канал (30 строк)
- POST на внешний endpoint
- Content-Type: application/json
- Retry: 3 раза с exponential backoff

#### 5. Эскалация (80 строк)
```
Telegram: 0 мин → Nostr DM: 5 мин → Webhook: 15 мин → Звонок: 30 мин
```
- Каждый уровень — отдельный таймер
- `/ack` сбрасывает эскалацию
- Если не ack за N минут → следующий канал

#### 6. Логи алертов — SQLite (50 строк)
```sql
CREATE TABLE alert_log (
    id TEXT PRIMARY KEY,           -- UUID
    rule_name TEXT NOT NULL,
    service_name TEXT NOT NULL,
    status TEXT NOT NULL,
    priority TEXT NOT NULL,
    triggered_at INTEGER NOT NULL,
    acknowledged INTEGER DEFAULT 0,
    ack_at INTEGER,
    escalation_level INTEGER DEFAULT 0
);
```

### Критерии готовности
- [ ] Правила парсятся из YAML
- [ ] Telegram-алерт при падении сервиса
- [ ] Nostr DM при HIGH-приоритете
- [ ] Эскалация работает (Telegram → Nostr → Webhook)
- [ ] `/ack` сбрасывает эскалацию
- [ ] Логи всех алертов в SQLite

---

# ФАЗА 3 — L15: Auto-Recovery

## Статус: ❌ НЕ РЕАЛИЗОВАН

### Длительность: 4 дня

### Зависимости: L13 (Health Monitor) + L14 (Alert Engine)

### Назначение
Автоматическое восстановление сервисов без участия человека. При падении — анализ причины, попытка восстановления, если не помогло — эскалация.

### Архитектура

```
[L13 Health Monitor] → сервис dead
         ↓
[L15 Auto-Recovery] → анализ (статистика падений, логи)
         ↓
[Попытка 1] → restart сервиса (через supervisor)
         ↓ fail
[Попытка 2] → restart + очистка кеша
         ↓ fail
[Попытка 3] → перезагрузка всего слоя
         ↓ fail
[L14 Alert Engine] → эскалация человеку
```

### Модули для реализации

#### 1. `auto_recovery.py` — ядро (250-350 строк)

**API:**
- `AutoRecovery(config_path)` — загрузка стратегий
- `analyze(service_name, context)` — анализ причины падения
- `recover(service_name, level=1)` — попытка восстановления
- `rollback(service_name, snapshot_id)` — откат к предыдущему состоянию

**Стратегии восстановления (конфиг YAML):**
```yaml
recovery_strategies:
  nostr_bridge:
    attempts:
      - action: restart
        description: "Простой рестарт через supervisor"
      - action: restart_clear_cache
        description: "Рестарт с очисткой кеша релеев"
      - action: reload_layer
        description: "Перезагрузка всего Nostr Bridge слоя"
    cooldown: 300  # сек между попытками одного сервиса
    max_daily: 10   # макс рестартов в день

  smart_router:
    attempts:
      - action: restart
      - action: restart_with_dump
        description: "Рестарт с дампом состояния"
      - action: failover_replica
        description: "Переключение на реплику"
    cooldown: 120
    max_daily: 5
```

#### 2. Supervisor Bridge (50 строк)
- API к supervisor.py через HTTP/REST
- `supervisor_restart(service_name)` — рестарт сервиса
- `supervisor_status(service_name)` — текущий статус
- `supervisor_logs(service_name, lines=50)` — последние логи

#### 3. Анализ причины падения (100 строк)
**Что анализировать:**
- Логи сервиса за последние 5 минут
- Метрики: RAM, CPU, open FDs
- Статус зависимостей (релеи, mesh-соседи)
- История падений за последний час

**Формат отчёта:**
```json
{
  "service": "nostr_bridge",
  "status": "dead",
  "probable_cause": "fd_leak",
  "evidence": ["open_fds=2048 (limit 1024)", "crash_log: Too many open files"],
  "recommended_action": "restart_clear_cache"
}
```

#### 4. Статистика и превентивные действия (80 строк)
- Если сервис падает >3 раз за час → временно повысить лимиты
- Если >10 раз за день → отключить сервис, уведомить оператора
- Если падают связанные сервисы → каскадное восстановление

### Интеграция с cryter_v10
- Auto-Recovery публикует статус восстановления в Nostr (kind:9002)
- Cryter получает и логирует
- Telegram-уведомление при каждом уровне попытки

### Критерии готовности
- [ ] Авто-рестарт упавшего сервиса за <30 сек
- [ ] Анализ причины падения по логам
- [ ] 3 уровня попыток с эскалацией
- [ ] Не зацикливается (max_daily лимиты)
- [ ] Отчёт о восстановлении в Telegram
- [ ] 10+ unit-тестов

---

# ФАЗА 4 — L2C: Cloudflare Durable Object

## Статус: ❌ НЕ РЕАЛИЗОВАН

### Длительность: 2 дня

### Назначение
Глобально распределённый shared state через Cloudflare Workers. Два агента на разных континентах подключаются к одному Durable Object через WebSocket и делят переменную. Cloudflare гарантирует: только один экземпляр DO в мире.

### Характеристики
- Скорость: до 800 кбит/с (100 msg/сек × 1 KB)
- Задержка: 50-200 ms глобально (330 городов Cloudflare Edge)
- Скрытность: средняя (шифрование на уровне агента)
- Бюджет: $5/мес (бесплатный лимит — 10 млн запросов)

### Модули для реализации

#### 1. Worker JS — `cloudflare_do_worker.js` (30 строк)
```javascript
export class SharedState {
    constructor(state) {
        this.state = state;
        this.storage = state.storage;
        this.sessions = new Map();
    }

    async fetch(request) {
        const url = new URL(request.url);
        if (url.pathname === '/ws') {
            return this.handleWebSocket(request);
        }
        // REST API для KV-like операций
        if (url.pathname === '/kv') {
            return this.handleKV(request);
        }
        return new Response('Not found', { status: 404 });
    }

    async handleWebSocket(request) {
        const pair = new WebSocketPair();
        const [client, server] = Object.values(pair);
        this.sessions.set(server, { connected: Date.now() });
        // accept WebSocket
        server.accept();
        server.addEventListener('message', async (event) => {
            // Broadcast to all sessions
            for (const [ws] of this.sessions) {
                ws.send(event.data);
            }
        });
        return new Response(null, { status: 101, webSocket: client });
    }

    async handleKV(request) {
        // PUT /kv/{key} — body = value
        // GET /kv/{key} — return value
        // DELETE /kv/{key}
    }
}

export default {
    async fetch(request, env) {
        const id = env.SHARED_STATE.idFromName('global');
        const stub = env.SHARED_STATE.get(id);
        return stub.fetch(request);
    }
}
```

#### 2. `cloudflare_do.py` — WebSocket клиент (150 строк)
**API:**
- `CloudflareDO(api_token, account_id, namespace)` — инициализация
- `connect()` — WebSocket к DO
- `send(payload)` — отправка через WS
- `receive()` — получение через WS
- `kv_put(key, value)` — PUT /kv/{key} (REST API)
- `kv_get(key)` — GET /kv/{key}
- Методы кодирования: Counter Modulo, Counter Delta, Key-Value Map

#### 3. Интеграция в V5 (100 строк)
- Дополнительный канал рядом с Nostr/mesh
- Выбор канала по приоритету: Nostr (надёжность) → DO (скорость) → Mesh (скрытность)
- Pre-shared secret для шифрования поверх WebSocket

#### 4. Cloudflare KV (30 строк)
- Агент A пишет ключ → через 60 сек данные на всех 330 Edge
- 25 MB на namespace
- Использование: конфигурация, белый список, shared state

### Критерии готовности
- [ ] Worker развёрнут в Cloudflare
- [ ] Два агента обмениваются данными через DO
- [ ] Шифрование работает
- [ ] Метрики: latency < 200 ms

---

# ФАЗА 5A — L2A: Azure Blob Lease Heartbeat

## Статус: ❌ НЕ РЕАЛИЗОВАН

### Длительность: 1 день

### Назначение
Глобальный heartbeat-мониторинг через Azure Blob Storage. Независим от нашей инфраструктуры. Если весь хостинг лёг — Azure всё ещё работает.

### Характеристики
- Скорость: 4 бит/мин (один blob), ∞ с мультиплексированием (60 блобов = 15 байт/мин)
- Дальность: глобальная (любой Azure регион)
- Скрытность: ВЫСОКАЯ — Lease-операции штатный механизм Azure
- Цена: $0 (Azure free tier — 5 GB Blob Storage)

### Модули для реализации

#### 1. `azure_lease_heartbeat.py` (120 строк)
**API:**
- `AzureHeartbeat(connection_string, container_name, blob_name)` — инициализация
- `acquire_lease()` — взять lease (если занят → HTTP 409 = другой агент жив)
- `renew_lease()` — продлить lease (раз в 30 сек)
- `release_lease()` — освободить lease (при graceful shutdown)
- `check_peer(blob_name, timeout=90)` — проверить жив ли peer

**Логика:**
```python
# Агент А
heartbeat = AzureHeartbeat(conn_str, "snin-hb", "agent-a")
lease_id = heartbeat.acquire_lease()  # занял blob
while True:
    heartbeat.renew_lease(lease_id)
    await asyncio.sleep(30)

# Агент Б (монитор)
peer_alive = heartbeat.check_peer("agent-a", timeout=90)
if not peer_alive:
    alert_engine.trigger("agent_a_dead", priority=HIGH)
```

#### 2. Cron-задача (20 строк)
- Каждый агент: renew lease раз в 30 сек
- Файл: `cron_heartbeat.py`
- Запуск через supervisor как отдельный сервис

#### 3. Центральный монитор (50 строк)
- Проверка всех blob-ов раз в 30 сек
- Если 3+ проверки подряд неудачны → alert
- Dashboard: список агентов online/offline

---

# ФАЗА 5B — L4E: Ethernet PHY Subliminal

## Статус: ❌ НЕ РЕАЛИЗОВАН

### Длительность: 5 дней (исследование + реализация)

### Назначение
Физический уровень Ethernet как скрытый data channel без разрыва сети. Idle-символы (идущие постоянно, когда нет данных) модулируются полярностью. Второй агент на той же линии читает модуляцию через PHY-регистры. Никакого IP, никаких пакетов — чисто физический уровень.

### Характеристики
- Скорость: до 100 Мбит/с (MLT-3 violations на 125 МГц)
- Дальность: 100 м (витая пара Cat5e), 10 км (оптика)
- Скрытность: ВЫСОКАЯ — ошибки PHY списываются на помехи
- Цена: $0 (любой Ethernet-порт уже есть)

### Модули для реализации

#### 1. `eth_phy_channel.py` (200-300 строк)
**API:**
- `EthernetPHYChannel(interface='eth0')` — инициализация
- `mdio_read(phy_addr, register)` — чтение PHY-регистра через Linux mdio
- `mdio_write(phy_addr, register, value)` — запись PHY-регистра
- `send_bit(bit)` — модуляция одного бита (Polarity Inversion)
- `recv_bit()` — чтение одного бита
- `send_byte(byte)` — отправка байта (8 бит)
- `recv_byte()` — чтение байта

**Методы кодирования:**
1. **Polarity Inversion** (базовый, 125 000 бит/с)
   - Один idle-символ перевёрнут = бит 1
   - Через регистр PHY: переключение полярности

2. **MLT-3 Violation** (1 Мбит/с)
   - Нарушение MLT-3 кодирования
   - Через регистр PHY: принудительное нарушение

3. **Scrambler Seed** (100 кбит/с)
   - Смена seed скремблера = паттерн ошибок

4. **Amplitude Modulation** (10 Мбит/с)
   - Амплитуда сигнала ±2%

#### 2. Протокол кодирования (50 строк)
- Фрейм: preamble (8 байт) + payload (N байт) + checksum (2 байта)
- Preamble: 0xAA 0xAA 0xAA 0xAA 0xAA 0xAA 0xAA 0xAB
- Checksum: CRC-16

#### 3. Интеграция в V5 (50 строк)
- Скрытый канал между агентами на одном Ethernet-сегменте
- Автоопределение: сканирование PHY-регистров соседей
- Выбор метода: Polarity Inversion (по умолчанию) / MLT-3 (скорость)

### Критерии готовности
- [ ] PHY-регистры читаются через mdio
- [ ] Бит передаётся и принимается на той же машине (loopback)
- [ ] Бит передаётся между двумя машинами по Ethernet
- [ ] Не влияет на основной сетевой трафик
- [ ] CRC-16 защита от ошибок

---

# Сводный план

| Фаза | Что | Дней | Строк кода | Зависит от |
|------|-----|------|-----------|------------|
| 1 | L5T Dead-Letter Layer | 4 | 550-700 | Smart Router, kind:9000 |
| 2A | L13 Health Monitor (доработка) | 2 | 330 | Health Check Engine |
| 2B | L14 Alert Engine | 3 | 410-560 | L13 |
| 3 | L15 Auto-Recovery | 4 | 480-580 | L13, L14 |
| 4 | L2C Cloudflare DO | 2 | 310 | Cloudflare аккаунт |
| 5A | L2A Azure Heartbeat | 1 | 190 | Azure аккаунт |
| 5B | L4E Ethernet PHY | 5 | 300-400 | Linux mdio |
| **ИТОГО** | | **21 день** | **~3000** | |

---

# Приоритеты по версиям

## V5.1 (ближайший релиз) — Фазы 1-3
- L5T Dead-Letter Queue — критическая надежность
- L13 Health Monitor (полный) — операционная стабильность
- L14 Alert Engine — реакция на сбои
- L15 Auto-Recovery — автоматизация восстановления

## V5.2 (следующий релиз) — Фазы 4-5
- L2C Cloudflare DO — скоростной канал
- L2A Azure Heartbeat — независимый мониторинг
- L4E Ethernet PHY — скрытый канал (требует отдельного исследования)

---

*Спецификация составлена 2026-06-03 на основе Google Doc пользователя.*
*Working directory: /home/agent/data/sites/relay-mesh/*
*Git: /home/agent/data/projects/snin-v5-mesh-fabric/*

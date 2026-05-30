# Event-Driven Nostr Bridge — Spec

**Дата:** 2026-05-18
**Архитектурный вектор:** V9 (интеграционный)

---

## 1. Проблема

Сейчас NostrBridge публикует mesh-события в Nostr релеи через `publish_loop` — polling раз в 30 секунд.

```python
async def publish_loop(self):
    while self._running:
        await asyncio.sleep(30)          # ⛔ 30 секунд задержки
        queue = self._publish_queue[:]   # забираем накопленное
        self._publish_queue = []
        for event in queue:
            for client in self.clients:  # во все 101 релеи
                await client.publish(event)
```

**Последствия:**
- Сообщение от агента А → Nostr → агенту Б идёт 0-30 секунд
- Всплеск нагрузки на CPU раз в 30 секунд (батч из N событий × 101 WS send)
- При падении процесса — теряется весь непрочитанный батч в `_publish_queue`
- 101 WebSocket соединение открыты, но не используются для немедленной отправки

---

## 2. Решение: event-driven через asyncio.Queue

Каждое событие публикуется **немедленно** через уже открытый WebSocket.

```python
class NostrBridge:
    def __init__(self):
        self._publish_queue = asyncio.Queue()  # вместо list
        self._publisher_task = None
        self.clients = []  # 101 NostrRelayClient, WS уже открыты
    
    async def publish_event(self, event: dict):
        """Положить событие в очередь — публикация произойдёт немедленно."""
        await self._publish_queue.put(event)
    
    async def publish_loop(self):
        """Event-driven: публикуем каждое событие как только оно пришло."""
        while self._running:
            event = await self._publish_queue.get()  # ждёт событие (не спит 30с)
            
            if not self.clients:
                continue
            
            # Публикуем на всех connected релеях сразу
            tasks = [
                client.publish(event) 
                for client in self.clients 
                if client.connected
            ]
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                stats["published"] += len(tasks)
```

---

## 3. Что меняется

### 3.1 NostrBridge.publish_loop (строки 507-530)

**Было:**
```python
async def publish_loop(self):
    while self._running:
        await asyncio.sleep(30)                    # polling
        if not self._publish_queue: continue       # пустая проверка
        queue = self._publish_queue[:]             # копия
        self._publish_queue = []                   # очистка
        for event in queue:                        # батч
            ...publish to all clients...
```

**Стало:**
```python
async def publish_loop(self):
    while self._running:
        event = await self._publish_queue.get()    # event-driven
        ...publish to all clients immediately...
```

### 3.2 _publish_queue: list → asyncio.Queue

**Было:**
```python
self._publish_queue: list = []
...
self._publish_queue.append(event)
```

**Стало:**
```python
self._publish_queue: asyncio.Queue = asyncio.Queue()
...
await self._publish_queue.put(event)
```

### 3.3 External Gateway (строка 661)

**Было:**
```python
self._publish_queue.append(nostr_event)
```

**Стало:**
```python
await self.bridge.publish_event(nostr_event)
```

### 3.4 Добавить: триггер из Smart Router

SR сейчас отправляет kind:39002 в CR → RE → WS (mesh). Для Nostr канала нужно:

**Вариант А (минимальные изменения):**
SR пишет kind:39002 в NostrBridge через TCP на порт 9933 (новый).
NostrBridge слушает :9933 и кладёт в `asyncio.Queue`.

**Вариант Б (без нового порта):**
NostrBridge подписывается на свой же CR через TCP на :9920.
CR форвардит kind:39002 в NostrBridge.
То есть NostrBridge становится ещё одним consumer'ом CR — как RE.

**Вариант В (рекомендуемый):**
SR сам вызывает `bridge.publish_event()` через asyncio (если bridge в том же процессе)
или через Unix socket (если bridge отдельный процесс).

---

## 4. Что это даёт

| Метрика | Polling (30s) | Event-driven | Разница |
|---------|:------------:|:------------:|:-------:|
| Задержка доставки | 0-30000ms | ~50-200ms | **×150 быстрее** |
| Потеря при краше | Весь батч | 1 событие | **×N надёжнее** |
| CPU профиль | Пик раз/30с | Равномерно | **Без просадок** |
| Нагрузка на 101 релей | N событий × 101 | 1 × 101 | **Та же** |

**Побочный эффект:** NostrBridge начинает работать как relay — событие пришло, сразу ушло в 101 релей. Mesh становится ближе к real-time Nostr.

---

## 5. Объём изменений

| Файл | Строк | Что |
|------|:----:|-----|
| `nostr_bridge.py` | ~5 | `publish_loop`: sleep → queue.get |
| `nostr_bridge.py` | ~2 | _publish_queue: list → Queue |
| `nostr_bridge.py` | ~3 | publish_event() — новый метод |
| `external_gateway.py` | ~1 | append → publish_event |
| **Итого** | **~11 строк** | |

**Время:** ~30 минут включая тест.

---

## 6. Риски

| Риск | Mitigation |
|------|------------|
| WebSocket не успевает отправлять | `asyncio.Queue(maxsize=1000)` — превышение = лог + drop oldest |
| Rate limit от Nostr релея | CB на каждый client (уже есть в SR) |
| Перегрузка при батче из SR | Queue + parallel gather — 101 WS send = ~100ms |

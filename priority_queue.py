"""
priority_queue.py — Priority Message Queue для SmartRouter (Фаза 5)

Три уровня:
  CRITICAL — мгновенная доставка (health alerts, system messages)
  HIGH     — приоритетная обработка (важные agent-to-agent)
  NORMAL   — основная очередь

Диспетчер: взвешенный round-robin между очередями (5:3:1).
Aging: NORMAL сообщения старше 30 сек → HIGH; HIGH старше 60 сек → CRITICAL.
"""
import asyncio
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional


class Priority(IntEnum):
    NORMAL = 0
    HIGH = 1
    CRITICAL = 2

    @classmethod
    def from_string(cls, s: str) -> "Priority":
        s = s.upper()
        if s == "CRITICAL":
            return cls.CRITICAL
        if s == "HIGH":
            return cls.HIGH
        return cls.NORMAL


@dataclass
class QueuedMessage:
    msg: dict
    priority: Priority
    enqueued_at: float = field(default_factory=time.monotonic)
    promoted_from: Optional[Priority] = None  # был ли повышен

    @property
    def age(self) -> float:
        return time.monotonic() - self.enqueued_at


# ═══ Aging thresholds ═══
AGING_NORMAL_TO_HIGH = 30.0    # секунд
AGING_HIGH_TO_CRITICAL = 60.0  # секунд
AGING_CHECK_INTERVAL = 5.0     # проверка aging раз в 5 сек

# ═══ Dispatch weights ═══
WEIGHT_CRITICAL = 5
WEIGHT_HIGH = 3
WEIGHT_NORMAL = 1


class PriorityQueue:
    """Приоритетная очередь с aging и взвешенной диспетчеризацией."""

    def __init__(self, maxsize: int = 10000):
        self._critical: asyncio.Queue[QueuedMessage] = asyncio.Queue(maxsize)
        self._high: asyncio.Queue[QueuedMessage] = asyncio.Queue(maxsize)
        self._normal: asyncio.Queue[QueuedMessage] = asyncio.Queue(maxsize)
        self._total_enqueued = 0
        self._total_dequeued = 0
        self._aged_up = 0
        self._max_wait: dict[Priority, float] = {
            Priority.CRITICAL: 0.0,
            Priority.HIGH: 0.0,
            Priority.NORMAL: 0.0,
        }

    async def put(self, msg: dict) -> QueuedMessage:
        """Поместить сообщение в очередь согласно приоритету."""
        priority_str = msg.get("meta", {}).get("priority", "normal")
        priority = Priority.from_string(priority_str)
        qm = QueuedMessage(msg=msg, priority=priority)
        queue = self._queue_for(priority)
        await queue.put(qm)
        self._total_enqueued += 1
        return qm

    async def get(self) -> QueuedMessage:
        """Извлечь сообщение с учётом приоритетов.
        
        Неблокирующая проверка CRITICAL → HIGH → NORMAL.
        Если все пусты — asyncio.wait на все три очереди одновременно.
        """
        # 1. CRITICAL — мгновенно
        qm = await self._try_get(Priority.CRITICAL)
        if qm:
            return qm
        
        # 2. HIGH
        qm = await self._try_get(Priority.HIGH)
        if qm:
            return qm
        
        # 3. NORMAL
        qm = await self._try_get(Priority.NORMAL)
        if qm:
            return qm
        
        # 4. Всё пусто — ждём любую очередь
        done, pending = await asyncio.wait(
            [
                asyncio.ensure_future(self._critical.get()),
                asyncio.ensure_future(self._high.get()),
                asyncio.ensure_future(self._normal.get()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        # Отменяем оставшиеся ожидания
        for task in pending:
            task.cancel()
        # Берём первый завершённый
        for task in done:
            qm = task.result()
            self._record_dequeue(qm)
            return qm
        
        # Недостижимо
        raise RuntimeError("PriorityQueue.get(): no result from any queue")

    async def _try_get(self, priority: Priority) -> Optional[QueuedMessage]:
        """Попытаться взять из очереди без блокировки."""
        queue = self._queue_for(priority)
        try:
            qm = queue.get_nowait()
            self._record_dequeue(qm)
            return qm
        except asyncio.QueueEmpty:
            return None

    async def try_get_critical(self) -> Optional[QueuedMessage]:
        """Неблокирующая попытка взять CRITICAL. Используется worker'ами."""
        return await self._try_get(Priority.CRITICAL)

    def _record_dequeue(self, qm: QueuedMessage):
        self._total_dequeued += 1
        wait = qm.age
        if wait > self._max_wait[qm.priority]:
            self._max_wait[qm.priority] = wait

    def _queue_for(self, priority: Priority) -> asyncio.Queue:
        if priority == Priority.CRITICAL:
            return self._critical
        if priority == Priority.HIGH:
            return self._high
        return self._normal

    async def age_messages(self):
        """Проверить возраст сообщений во всех очередях и повысить приоритет застоявшимся.
        
        Запускается фоновым циклом раз в AGING_CHECK_INTERVAL секунд.
        Логика:
          - NORMAL старше 30 сек → перемещается в HIGH
          - HIGH старше 60 сек → перемещается в CRITICAL
        """
        promoted = 0

        # NORMAL → HIGH
        moved = []
        while True:
            try:
                qm = self._normal.get_nowait()
            except asyncio.QueueEmpty:
                break
            if qm.age > AGING_NORMAL_TO_HIGH:
                qm.promoted_from = qm.priority
                qm.priority = Priority.HIGH
                qm.enqueued_at = time.monotonic()  # сброс таймера
                await self._high.put(qm)
                promoted += 1
            else:
                moved.append(qm)
        for qm in moved:
            await self._normal.put(qm)

        # HIGH → CRITICAL
        moved = []
        while True:
            try:
                qm = self._high.get_nowait()
            except asyncio.QueueEmpty:
                break
            if qm.age > AGING_HIGH_TO_CRITICAL:
                qm.promoted_from = qm.priority
                qm.priority = Priority.CRITICAL
                qm.enqueued_at = time.monotonic()
                await self._critical.put(qm)
                promoted += 1
            else:
                moved.append(qm)
        for qm in moved:
            await self._high.put(qm)

        self._aged_up += promoted
        return promoted

    @property
    def sizes(self) -> dict:
        return {
            "critical": self._critical.qsize(),
            "high": self._high.qsize(),
            "normal": self._normal.qsize(),
            "total": self._critical.qsize() + self._high.qsize() + self._normal.qsize(),
        }

    @property
    def stats(self) -> dict:
        return {
            "enqueued": self._total_enqueued,
            "dequeued": self._total_dequeued,
            "aged_up": self._aged_up,
            "max_wait_critical": round(self._max_wait[Priority.CRITICAL], 3),
            "max_wait_high": round(self._max_wait[Priority.HIGH], 3),
            "max_wait_normal": round(self._max_wait[Priority.NORMAL], 3),
            "sizes": self.sizes,
        }

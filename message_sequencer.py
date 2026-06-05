"""
message_sequencer.py — Message Ordering for SmartRouter (Фаза 3)

Проблема: gossip/nostr каналы доставляют сообщения в разном порядке.
Решение: seq_num + reorder buffer на принимающей стороне.

Архитектура:
  SeqNumTracker  — монотонный счётчик на пару (from → to), Redis-backed
  ReorderBuffer  — буферизация неупорядоченных сообщений, доставка по порядку
"""
import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field


# ═══ Конфигурация ═══
MAX_BUFFER_SIZE = 100       # максимум сообщений в буфере на одну пару
REORDER_TIMEOUT = 5.0       # секунд ожидания следующего seq_num
CLEANUP_INTERVAL = 300.0    # очистка старых пар раз в 5 минут
MAX_PAIRS = 1000            # максимум отслеживаемых пар


@dataclass
class BufferedMsg:
    """Сообщение в reorder buffer."""
    seq: int
    msg: dict
    received_at: float = field(default_factory=time.monotonic)


class SeqNumTracker:
    """Монотонный счётчик seq_num per (from, to) pair.
    
    In-memory cache + периодическая синхронизация с Redis.
    """
    def __init__(self):
        self._counters: dict[str, int] = {}
        self._redis = None  # lazy init
    
    async def _get_redis(self):
        if self._redis is None:
            from router_policy import aredis
            self._redis = await aredis()
        return self._redis
    
    def _pair_key(self, from_agent: str, to_agent: str) -> str:
        return f"seq:{from_agent}:{to_agent}"
    
    async def next_seq(self, from_agent: str, to_agent: str) -> int:
        """Получить следующий seq_num для пары (from → to).
        
        Использует Redis INCR для atomicity между экземплярами.
        In-memory кеш для fallback при недоступности Redis.
        """
        key = self._pair_key(from_agent, to_agent)
        
        r = await self._get_redis()
        if r:
            try:
                seq = await r.incr(key)
                await r.expire(key, 86400)  # TTL 24h
                self._counters[key] = seq  # синхронизируем кеш
                return seq
            except Exception:
                pass  # fallback to in-memory
        
        # In-memory fallback
        seq = self._counters.get(key, 0) + 1
        self._counters[key] = seq
        return seq
    
    async def get_current(self, from_agent: str, to_agent: str) -> int:
        """Получить текущий seq_num без инкремента."""
        key = self._pair_key(from_agent, to_agent)
        r = await self._get_redis()
        if r:
            try:
                val = await r.get(key)
                if val:
                    return int(val)
            except Exception:
                pass
        return self._counters.get(key, 0)


class ReorderBuffer:
    """Буферизация out-of-order сообщений и доставка по порядку.
    
    Алгоритм:
    1. При получении сообщения с seq=N:
       - Если N == expected → доставить сразу + проверить буфер на N+1, N+2, ...
       - Если N > expected → буферизовать, ждать expected
       - Если N < expected → дубликат, скипнуть
    2. Тайм-аут: если expected не приходит за REORDER_TIMEOUT секунд → доставить всё из буфера
    """
    def __init__(self):
        # pair_key → list[BufferedMsg] (сортирован по seq)
        self._buffers: dict[str, list[BufferedMsg]] = {}
        # pair_key → next expected seq_num
        self._next_expected: dict[str, int] = {}
        # pair_key → last activity time
        self._last_activity: dict[str, float] = {}
        self.stats = {
            "delivered_in_order": 0,
            "delivered_from_buffer": 0,
            "buffered": 0,
            "duplicates": 0,
            "timeouts": 0,
            "dropped_buffer_full": 0,
        }
    
    def _pair_key(self, from_agent: str, to_agent: str) -> str:
        return f"{from_agent}→{to_agent}"
    
    async def deliver(self, from_agent: str, to_agent: str, seq: int, msg: dict) -> list[dict]:
        """Обработать входящее сообщение. Возвращает список готовых к доставке.
        
        Вызывающий код должен отправить каждый dict из возвращённого списка получателю.
        """
        key = self._pair_key(from_agent, to_agent)
        expected = self._next_expected.get(key, 1)
        self._last_activity[key] = time.monotonic()
        
        if seq == expected:
            # В точку — доставляем сразу
            ready = [msg]
            self._next_expected[key] = seq + 1
            self.stats["delivered_in_order"] += 1
            
            # Проверяем буфер: есть ли там seq+1, seq+2, ...
            buf = self._buffers.get(key, [])
            while buf and buf[0].seq == self._next_expected[key]:
                ready.append(buf.pop(0).msg)
                self._next_expected[key] += 1
                self.stats["delivered_from_buffer"] += 1
            
            if not buf and key in self._buffers:
                del self._buffers[key]
            
            return ready
        
        elif seq > expected:
            # Будущее сообщение — буферизуем
            if key not in self._buffers:
                self._buffers[key] = []
            
            buf = self._buffers[key]
            if len(buf) >= MAX_BUFFER_SIZE:
                self.stats["dropped_buffer_full"] += 1
                # Буфер полон — доставляем всё что есть + это сообщение
                ready = [m.msg for m in buf] + [msg]
                self._buffers[key] = []
                self._next_expected[key] = seq + 1
                return ready
            
            # Вставляем в сортированную позицию
            bm = BufferedMsg(seq=seq, msg=msg)
            insert_pos = 0
            for i, existing in enumerate(buf):
                if existing.seq > seq:
                    break
                insert_pos = i + 1
            buf.insert(insert_pos, bm)
            self.stats["buffered"] += 1
            return []
        
        else:  # seq < expected — дубликат
            self.stats["duplicates"] += 1
            return []
    
    async def check_timeouts(self) -> list[tuple[str, str, dict]]:
        """Проверить тайм-ауты. Возвращает [(from, to, msg), ...] для доставки."""
        now = time.monotonic()
        ready = []
        
        for key in list(self._buffers.keys()):
            buf = self._buffers.get(key)
            if not buf:
                continue
            
            # Если самое старое сообщение в буфере старше тайм-аута
            oldest = buf[0]
            if now - oldest.received_at > REORDER_TIMEOUT:
                # Доставляем все сообщения из буфера
                from_to = key.split("→", 1)
                from_agent, to_agent = from_to[0], from_to[1] if len(from_to) > 1 else key
                
                for bm in buf:
                    ready.append((from_agent, to_agent, bm.msg))
                
                # Обновляем expected
                last_seq = buf[-1].seq
                self._next_expected[key] = last_seq + 1
                del self._buffers[key]
                self.stats["timeouts"] += 1
        
        return ready
    
    def cleanup_old_pairs(self):
        """Удалить неактивные пары (старше CLEANUP_INTERVAL)."""
        now = time.monotonic()
        stale = []
        for key, last_time in self._last_activity.items():
            if now - last_time > CLEANUP_INTERVAL:
                stale.append(key)
        
        for key in stale:
            self._buffers.pop(key, None)
            self._next_expected.pop(key, None)
            self._last_activity.pop(key, None)
        
        # Если слишком много пар — удаляем самые старые
        if len(self._last_activity) > MAX_PAIRS:
            sorted_pairs = sorted(self._last_activity.items(), key=lambda x: x[1])
            to_remove = sorted_pairs[:len(sorted_pairs) - MAX_PAIRS]
            for key, _ in to_remove:
                self._buffers.pop(key, None)
                self._next_expected.pop(key, None)
                self._last_activity.pop(key, None)
        
        return len(stale)


# ═══ Background task: проверка тайм-аутов ═══
async def reorder_timeout_loop(reorder: ReorderBuffer, push_fn, interval: float = 1.0):
    """Фоновый цикл: проверка тайм-аутов reorder buffer раз в секунду."""
    while True:
        await asyncio.sleep(interval)
        try:
            timed_out = await reorder.check_timeouts()
            if timed_out:
                for from_agent, to_agent, msg in timed_out:
                    await push_fn(msg)
        except Exception as e:
            print(f"[Reorder] timeout check error: {e}")


async def reorder_cleanup_loop(reorder: ReorderBuffer, interval: float = 300.0):
    """Фоновый цикл: очистка старых пар раз в 5 минут."""
    while True:
        await asyncio.sleep(interval)
        try:
            removed = reorder.cleanup_old_pairs()
            if removed:
                print(f"[Reorder] Cleaned {removed} stale pairs")
        except Exception as e:
            print(f"[Reorder] cleanup error: {e}")

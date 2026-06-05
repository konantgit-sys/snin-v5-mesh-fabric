"""
message_deduplicator.py — Message Deduplication for SmartRouter (Фаза 4)

Проблема: в multi-node mesh одно сообщение приходит несколько раз
через разные каналы (gossip → shard 1, gossip → shard 3, nostr).
Решение: dedup по ключу (from_agent, seq) с быстрым in-memory LRU +
Redis TTL для персистентности между перезапусками.

Для broadcast (без seq): ключ = (from_agent, hash(payload)).

Использует:
  - in-memory set с LRU эвикцией (max 50000 ключей)
  - Redis SET с TTL 300 сек для cross-instance dedup
"""
import asyncio
import hashlib
import time
from collections import OrderedDict


# ═══ Конфигурация ═══
DEDUP_TTL = 300           # TTL для ключей в Redis (секунд)
MAX_KEYS = 50000          # максимум ключей в in-memory кеше
CLEANUP_INTERVAL = 120    # очистка старых ключей раз в 2 минуты
BROADCAST_DEDUP = True    # дедуплицировать broadcast сообщения


class LRUDedupSet:
    """In-memory set с LRU-эвикцией.
    
    OrderedDict хранит ключи; при превышении MAX_KEYS
    удаляются самые старые (least recently used).
    """
    def __init__(self, max_size: int = MAX_KEYS):
        self._max = max_size
        self._store: OrderedDict[str, float] = OrderedDict()
        self._hits = 0
        self._misses = 0
    
    def __contains__(self, key: str) -> bool:
        return key in self._store
    
    def add(self, key: str) -> bool:
        """Добавить ключ. Возвращает True если ключ новый (не дубликат)."""
        if key in self._store:
            # Обновляем позицию в LRU
            self._store.move_to_end(key)
            self._store[key] = time.monotonic()
            self._hits += 1
            return False  # дубликат
        
        if len(self._store) >= self._max:
            # Удаляем старейший (первый в OrderedDict)
            self._store.popitem(last=False)
        
        self._store[key] = time.monotonic()
        self._misses += 1
        return True  # новый
    
    def cleanup_old(self, ttl: float = 300.0):
        """Удалить ключи старше ttl секунд."""
        now = time.monotonic()
        stale = [k for k, ts in self._store.items() if now - ts > ttl]
        for k in stale:
            del self._store[k]
        return len(stale)
    
    @property
    def size(self) -> int:
        return len(self._store)
    
    @property
    def hit_rate(self) -> float:
        total = self._hits + self._misses
        return self._hits / total if total > 0 else 0.0


class MessageDeduplicator:
    """Дедупликатор сообщений.
    
    Для agent-to-agent (с seq):  ключ = md5(from:to:seq)
    Для broadcast (без seq):    ключ = md5(from:hash(payload))
    
    Проверка: in-memory LRU → Redis (если доступен).
    """
    def __init__(self):
        self._local = LRUDedupSet()
        self._redis = None  # lazy init
        self.stats = {
            "total": 0,
            "duplicates": 0,
            "unique": 0,
            "local_dup": 0,
            "redis_dup": 0,
            "no_key": 0
        }
    
    async def _get_redis(self):
        if self._redis is None:
            try:
                from router_policy import aredis
                self._redis = await aredis()
            except Exception:
                pass
        return self._redis
    
    def _make_key(self, msg: dict) -> str | None:
        """Создать dedup-ключ из сообщения.
        
        Agent-to-agent:  md5(from|to|payload_hash)
        Broadcast:       md5(from|payload_hash)
        
        Ключ НЕ зависит от seq — seq назначается после dedup-проверки.
        """
        from_agent = msg.get("from", msg.get("pubkey", ""))[:32]
        to_agent = msg.get("to", "")
        
        # Хеш payload (стабильный, не зависит от seq)
        payload = msg.get("payload", "")
        if isinstance(payload, dict):
            payload = str(sorted(payload.items()))
        payload_hash = hashlib.md5(str(payload).encode()).hexdigest()[:12]
        
        if from_agent:
            if to_agent and to_agent != "broadcast":
                raw = f"{from_agent}|{to_agent}|{payload_hash}"
            else:
                raw = f"{from_agent}|broadcast|{payload_hash}"
            return hashlib.md5(raw.encode()).hexdigest()[:16]
        
        return None
    
    async def is_duplicate(self, msg: dict) -> bool:
        """Проверить, не дубликат ли сообщение. True = дубликат, пропустить."""
        self.stats["total"] += 1
        
        key = self._make_key(msg)
        if key is None:
            self.stats["unique"] += 1
            self.stats["no_key"] += 1
            return False
        
        # 1. In-memory check (fast path)
        if not self._local.add(key):
            self.stats["duplicates"] += 1
            self.stats["local_dup"] += 1
            # Периодический cleanup
            if self.stats["total"] % 100 == 0:
                self._local.cleanup_old(ttl=DEDUP_TTL)
            return True
        
        # 2. Redis check (cross-instance)
        r = await self._get_redis()
        if r:
            try:
                redis_key = f"dedup:{key}"
                added = await r.sadd(redis_key, "1")
                await r.expire(redis_key, DEDUP_TTL)
                if added == 0:
                    # Уже есть в Redis → дубликат с другого инстанса
                    self.stats["duplicates"] += 1
                    self.stats["redis_dup"] += 1
                    return True
            except Exception:
                pass  # Redis недоступен — полагаемся на in-memory
        
        self.stats["unique"] += 1
        return False
    
    async def cleanup(self):
        """Очистка старых ключей."""
        removed = self._local.cleanup_old(ttl=DEDUP_TTL)
        if removed:
            print(f"[Dedup] Cleaned {removed} expired keys from LRU (total: {self._local.size})")
        return removed


# ═══ Background task ═══
async def dedup_cleanup_loop(dedup: MessageDeduplicator, interval: float = CLEANUP_INTERVAL):
    """Фоновый цикл очистки dedup-кеша."""
    while True:
        await asyncio.sleep(interval)
        try:
            await dedup.cleanup()
        except Exception as e:
            print(f"[Dedup] cleanup error: {e}")

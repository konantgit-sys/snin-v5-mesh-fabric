"""
Rate Limiter — Token Bucket для SmartRouter.
Предотвращает buffer overflow при лавине сообщений.

Параметры:
  - rate: 100 токенов/сек на агента
  - burst: 200 (размер ведра)
  - cleanup_interval: 60 сек (очистка неактивных агентов)
"""
import time
import threading


class TokenBucket:
    """Token bucket для одного агента/ключа."""
    def __init__(self, rate: float = 100.0, burst: int = 200):
        self.rate = rate          # токенов/сек
        self.burst = burst        # максимум в ведре
        self.tokens = float(burst)  # стартуем с полным ведром
        self.last_refill = time.monotonic()
        self._lock = threading.Lock()  # потокобезопасность (asyncio event loop — один поток, но на всякий)
        self.created_at = time.time()

    def consume(self, n: int = 1) -> bool:
        """Попытаться потребить n токенов. Возвращает True если можно."""
        with self._lock:
            self._refill()
            if self.tokens >= n:
                self.tokens -= n
                return True
            return False

    def _refill(self):
        """Пополнить токены по времени."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.last_refill = now

    @property
    def available(self) -> float:
        with self._lock:
            self._refill()
            return self.tokens


class RateLimiter:
    """Менеджер token bucket'ов для N агентов."""
    def __init__(self, rate: float = 100.0, burst: int = 200, cleanup_after: float = 300.0):
        self.rate = rate
        self.burst = burst
        self.cleanup_after = cleanup_after  # удалить ведро если неактивно 5 минут
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = threading.Lock()
        self._last_cleanup = time.time()
        self.stats = {"allowed": 0, "denied": 0, "buckets_cleaned": 0}

    def allow(self, key: str, n: int = 1) -> bool:
        """Проверить: можно ли отправить n сообщений агенту key."""
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = TokenBucket(self.rate, self.burst)
                self._buckets[key] = bucket

        allowed = bucket.consume(n)
        if allowed:
            self.stats["allowed"] += n
        else:
            self.stats["denied"] += n

        # Периодическая очистка старых вёдер
        now = time.time()
        if now - self._last_cleanup > 60:
            self._cleanup(now)
            self._last_cleanup = now

        return allowed

    def _cleanup(self, now: float):
        """Удалить вёдра неактивных агентов."""
        stale = []
        with self._lock:
            for key, bucket in self._buckets.items():
                if now - bucket.created_at > self.cleanup_after and bucket.tokens >= self.burst * 0.9:
                    stale.append(key)
            for key in stale:
                del self._buckets[key]
        self.stats["buckets_cleaned"] += len(stale)

    def get_stats(self) -> dict:
        with self._lock:
            buckets = len(self._buckets)
        return {
            "buckets_active": buckets,
            **self.stats,
        }

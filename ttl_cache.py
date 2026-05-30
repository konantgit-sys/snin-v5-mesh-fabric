#!/usr/bin/env python3
"""
TTLCache — LRU + TTL кэш для антициклов и dedup.
Используется в NostrBridge (published_set) и GossipStream (seen_nonces).

Потокобезопасный (asyncio). O(1) add/check. O(1) eviction.
"""

import time
from collections import OrderedDict


class TTLCache:
    """
    LRU-кэш с TTL.
    
    - maxsize: макс записей (LRU eviction при превышении)
    - ttl: время жизни записи в секундах
    
    key: любой хешируемый объект (str, tuple, int)
    value: любой (хранится с timestamp)
    """

    def __init__(self, maxsize: int = 2000, ttl: float = 60.0):
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: OrderedDict = OrderedDict()

    def _evict_expired(self):
        """Удалить просроченные записи (амортизированный O(1))."""
        now = time.time()
        while self._cache:
            ts = next(iter(self._cache.values()))
            if now - ts > self.ttl:
                self._cache.popitem(last=False)
            else:
                break

    def add(self, key) -> bool:
        """
        Добавить ключ. Вернуть False если уже был (и не истёк).
        True = новый ключ (добавлен).
        """
        self._evict_expired()

        if key in self._cache:
            # Обновить позицию в LRU
            ts = self._cache.pop(key)
            self._cache[key] = ts
            return False  # already seen

        now = time.time()
        self._cache[key] = now

        # LRU eviction
        while len(self._cache) > self.maxsize:
            self._cache.popitem(last=False)

        return True  # new

    def check(self, key) -> bool:
        """True = ключ есть и не истёк (seen). False = нет (new)."""
        self._evict_expired()
        return key in self._cache

    def __contains__(self, key) -> bool:
        return self.check(key)

    def __len__(self):
        self._evict_expired()
        return len(self._cache)

    def clear(self):
        self._cache.clear()

    @property
    def size(self) -> int:
        return len(self._cache)

#!/usr/bin/env python3
"""
Phase 10: Graph Memory System — семантическая память узлов графа.

Архитектура:
  Каждый узел графа (агент) хранит memory entries.
  Memory entry = {key, value, embedding, timestamp, ttl}
  
  Redis-ключи:
    graph:memory:{node_id}  — Hash: key → JSON(MemoryEntry)
    graph:memory:index      — Sorted Set: term → [node_id,...] (обратный индекс)

Операции:
  set_memory(node_id, key, value)     — сохранить факт
  get_memory(node_id, key)            — прочитать факт
  search_memory(query, top_k)         — семантический поиск
  node_memories(node_id)              — все memory узла
  forget(node_id, key)                — удалить
  decay_memories(threshold_age)       — старение
  export_memory() / import_memory()   — персистентность

Интеграция:
  Phase 9 (snapshots) → snapshot включает memory
  Phase 8 (PubSub) → memory изменения реплицируются
"""

import json
import hashlib
import logging
import math
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from typing import Optional

import redis

logger = logging.getLogger("GraphMemory")

# ─── Constants ─────────────────────────────────────────

EMBEDDING_DIM = 32        # размерность вектора
MEMORY_TTL_DEFAULT = 604800  # 7 дней (сек)
DECAY_THRESHOLD = 86400    # 24 часа — возраст для decay

# ─── Data Classes ──────────────────────────────────────

@dataclass
class MemoryEntry:
    """Одна единица памяти узла."""
    key: str
    value: str
    embedding: list = field(default_factory=lambda: [0.0] * EMBEDDING_DIM)
    timestamp: float = 0.0
    ttl: int = MEMORY_TTL_DEFAULT
    access_count: int = 0
    last_accessed: float = 0.0

    def to_json(self) -> str:
        d = asdict(self)
        return json.dumps(d, ensure_ascii=False)

    @classmethod
    def from_json(cls, raw) -> "MemoryEntry":
        if isinstance(raw, bytes):
            raw = raw.decode()
        d = json.loads(raw) if isinstance(raw, str) else raw
        return cls(**{k: d[k] for k in [
            "key", "value", "embedding", "timestamp", "ttl",
            "access_count", "last_accessed"
        ] if k in d})

    @property
    def is_expired(self) -> bool:
        if self.ttl == 0:
            return False
        return time.time() - self.timestamp > self.ttl

    @property
    def age_sec(self) -> float:
        return time.time() - self.timestamp

    def touch(self):
        """Обновить access_count и last_accessed."""
        self.access_count += 1
        self.last_accessed = time.time()


# ─── Embedding Engine ──────────────────────────────────

def _tokenize(text: str) -> list:
    """Простейшая токенизация: lowercase + split + 3-граммы."""
    text = text.lower()
    tokens = text.split()
    # 3-граммы для частичного совпадения
    trigrams = set()
    for t in tokens:
        t = t.strip(".,!?;:()[]{}\"'")
        if len(t) >= 3:
            for i in range(len(t) - 2):
                trigrams.add(t[i:i+3])
    return tokens + list(trigrams)


def _embed(text: str, dim: int = EMBEDDING_DIM) -> list:
    """Создать embedding из текста через хеш-проекцию.

    Не требует ML-моделей, детерминирован, быстро.
    Качество достаточно для семантического поиска по ключевым словам.
    """
    tokens = _tokenize(text)
    vec = [0.0] * dim

    for token in tokens:
        h = hashlib.sha256(token.encode()).digest()
        # Каждый токен влияет на 4 позиции в векторе
        for i in range(0, len(h), 4):
            idx = (h[i] + (h[i+1] << 8)) % dim
            val = (h[i+2] - 128) / 128.0  # нормализация в [-1, 1]
            # TF-IDF-like: редкие токены (короткий хвост) получают больший вес
            weight = 1.0 / math.log(2 + (h[i+3] & 0x0F))
            vec[idx] += val * weight

    # L2-нормализация
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


def _cosine_similarity(a: list, b: list) -> float:
    """Косинусное сходство двух векторов."""
    dot = sum(x * y for x, y in zip(a, b))
    return max(0.0, min(1.0, dot))  # нормализовано


# ─── Graph Memory ──────────────────────────────────────

class GraphMemory:
    """Семантическая память, привязанная к узлам KnowledgeGraph."""

    KEY_PREFIX = "graph:memory"
    KEY_INDEX = "graph:memory:index"

    def __init__(self, redis_client: redis.Redis):
        self.r = redis_client
        self._entries: dict = {}  # node_id → {key → MemoryEntry}
        self._index: dict = {}    # token → set(node_id)
        self._loaded: set = set()  # множество node_id, для которых loaded из Redis

    # ─── CRUD ──────────────────────────────────────────

    def _key_for(self, node_id: str) -> str:
        return f"{self.KEY_PREFIX}:{node_id}"

    def set_memory(self, node_id: str, key: str, value: str,
                   ttl: int = MEMORY_TTL_DEFAULT) -> MemoryEntry:
        """Сохранить факт в память узла.

        Возвращает созданный MemoryEntry.
        """
        now = time.time()
        embedding = _embed(f"{key} {value}")

        entry = MemoryEntry(
            key=key,
            value=value,
            embedding=embedding,
            timestamp=now,
            ttl=ttl,
        )

        # In-memory
        if node_id not in self._entries:
            self._entries[node_id] = {}
        self._entries[node_id][key] = entry

        # Redis
        self.r.hset(self._key_for(node_id), key, entry.to_json())

        # Индекс
        self._index_entry(node_id, key, value)

        return entry

    def get_memory(self, node_id: str, key: str) -> Optional[MemoryEntry]:
        """Прочитать факт из памяти узла."""
        # In-memory
        if node_id in self._entries and key in self._entries[node_id]:
            entry = self._entries[node_id][key]
            entry.touch()
            return entry

        # Redis
        raw = self.r.hget(self._key_for(node_id), key)
        if raw:
            entry = MemoryEntry.from_json(raw)
            if node_id not in self._entries:
                self._entries[node_id] = {}
            self._entries[node_id][key] = entry
            entry.touch()
            return entry

        return None

    def forget(self, node_id: str, key: str) -> bool:
        """Удалить факт из памяти узла."""
        deleted = self.r.hdel(self._key_for(node_id), key)
        if node_id in self._entries:
            self._entries[node_id].pop(key, None)
        return bool(deleted)

    def node_memories(self, node_id: str) -> dict:
        """Все memory entries узла."""
        self._ensure_loaded(node_id)
        return self._entries.get(node_id, {})

    # ─── Search ────────────────────────────────────────

    def search_memory(self, query: str, top_k: int = 5,
                      node_filter: list = None) -> list:
        """Семантический поиск по памяти всех узлов.

        Возвращает: [(node_id, key, value, similarity_score), ...]
        """
        query_vec = _embed(query)

        results = []
        nodes_to_search = node_filter or list(self._entries.keys())

        # Если entries пустые, загружаем из Redis все доступные
        if not nodes_to_search:
            nodes_to_search = self._discover_nodes_from_redis()

        for node_id in nodes_to_search:
            self._ensure_loaded(node_id)
            for key, entry in self._entries.get(node_id, {}).items():
                if entry.is_expired:
                    continue
                sim = _cosine_similarity(query_vec, entry.embedding)
                if sim > 0.15:  # порог релевантности
                    results.append((node_id, key, entry.value, round(sim, 4)))

        results.sort(key=lambda x: -x[3])
        return results[:top_k]

    def search_by_keywords(self, keywords: list, top_k: int = 5) -> list:
        """Поиск по ключевым словам через обратный индекс.

        Быстрее чем search_memory для точного совпадения.
        """
        if not keywords:
            return []

        # Находим node_id через индекс
        candidates = set()
        for kw in keywords:
            kw_lower = kw.lower()
            for token in self._index:
                if kw_lower in token:
                    candidates.update(self._index[token])

        if not candidates:
            return []

        # Загружаем и ищем
        results = []
        for node_id in candidates:
            self._ensure_loaded(node_id)
            for key, entry in self._entries.get(node_id, {}).items():
                if entry.is_expired:
                    continue
                text = f"{key} {entry.value}".lower()
                score = sum(1 for kw in keywords if kw.lower() in text)
                if score > 0:
                    results.append((node_id, key, entry.value, score / len(keywords)))

        results.sort(key=lambda x: -x[3])
        return results[:top_k]

    # ─── Decay ─────────────────────────────────────────

    def decay_memories(self, threshold_age: float = DECAY_THRESHOLD) -> int:
        """Удалить устаревшие и просроченные memory entries.

        Возвращает количество удалённых entries.
        """
        now = time.time()
        deleted = 0

        for node_id in list(self._entries.keys()):
            self._ensure_loaded(node_id)
            to_delete = []
            for key, entry in self._entries.get(node_id, {}).items():
                if entry.is_expired or entry.age_sec > threshold_age * 3:
                    to_delete.append(key)

            for key in to_delete:
                self.forget(node_id, key)
                deleted += 1

        return deleted

    # ─── Export / Import (для Phase 9 snapshot) ────────

    def export_memory(self) -> dict:
        """Экспортировать всю память для снапшота."""
        data = {}
        for node_id in self._discover_nodes_from_redis():
            self._ensure_loaded(node_id)
            data[node_id] = {
                key: entry.to_json()
                for key, entry in self._entries.get(node_id, {}).items()
            }
        return {
            "version": 10,
            "exported_at": time.time(),
            "nodes": data,
        }

    def import_memory(self, state: dict, clear_first: bool = True) -> int:
        """Импортировать память из export_memory() словаря."""
        if clear_first:
            self._entries.clear()
            self._loaded.clear()

        count = 0
        for node_id, entries in state.get("nodes", {}).items():
            if node_id not in self._entries:
                self._entries[node_id] = {}
            for key, raw in entries.items():
                entry = MemoryEntry.from_json(raw)
                self._entries[node_id][key] = entry
                # Записать в Redis
                self.r.hset(self._key_for(node_id), key, raw if isinstance(raw, str)
                           else json.dumps(raw, ensure_ascii=False))
                count += 1
            self._loaded.add(node_id)

        return count

    def get_stats(self) -> dict:
        """Статистика памяти."""
        total_entries = 0
        total_nodes = 0
        expired = 0

        for node_id in self._discover_nodes_from_redis():
            total_nodes += 1
            self._ensure_loaded(node_id)
            entries = self._entries.get(node_id, {})
            total_entries += len(entries)
            expired += sum(1 for e in entries.values() if e.is_expired)

        return {
            "total_nodes_with_memory": total_nodes,
            "total_entries": total_entries,
            "expired_entries": expired,
            "index_terms": len(self._index),
        }

    # ─── Internal ──────────────────────────────────────

    def _ensure_loaded(self, node_id: str):
        """Гарантировать, что память узла загружена из Redis."""
        if node_id in self._loaded:
            return

        raw = self.r.hgetall(self._key_for(node_id))
        if raw:
            if node_id not in self._entries:
                self._entries[node_id] = {}
            for key, val in raw.items():
                key_str = key.decode() if isinstance(key, bytes) else key
                entry = MemoryEntry.from_json(val)
                self._entries[node_id][key_str] = entry
                # Индексируем
                self._index_entry(node_id, key_str, entry.value)

        self._loaded.add(node_id)

    def _discover_nodes_from_redis(self) -> list:
        """Найти все node_id с памятью в Redis."""
        nodes = set()
        for key_bytes in self.r.scan_iter(f"{self.KEY_PREFIX}:*"):
            key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
            node_id = key_str.split(":", 2)[-1]
            if node_id and node_id not in ("index",):
                nodes.add(node_id)
        return list(nodes)

    def _index_entry(self, node_id: str, key: str, value: str):
        """Добавить токены в обратный индекс."""
        tokens = set(_tokenize(f"{key} {value}"))
        for token in tokens:
            if token not in self._index:
                self._index[token] = set()
            self._index[token].add(node_id)

    def _remove_from_index(self, node_id: str):
        """Удалить все упоминания node_id из индекса."""
        for token in list(self._index.keys()):
            self._index[token].discard(node_id)
            if not self._index[token]:
                del self._index[token]


# ─── Integration with KnowledgeGraph ───────────────────

def attach_memory_to_graph(kg, redis_client=None) -> GraphMemory:
    """Прикрепить GraphMemory к KnowledgeGraph.

    Использование:
        gm = attach_memory_to_graph(kg)
        gm.set_memory("agent_A", "skill", "bitcoin_trading")
        results = gm.search_memory("trading")
    """
    r = redis_client or kg.r
    gm = GraphMemory(r)

    # Monkey-patch: добавляем memory методы в KnowledgeGraph
    kg.set_memory = lambda node_id, key, value, ttl=MEMORY_TTL_DEFAULT: \
        gm.set_memory(node_id, key, value, ttl)
    kg.get_memory = lambda node_id, key: gm.get_memory(node_id, key)
    kg.search_memory = lambda query, top_k=5, nf=None: \
        gm.search_memory(query, top_k, nf)
    kg.node_memories = lambda node_id: gm.node_memories(node_id)
    kg.forget_memory = lambda node_id, key: gm.forget(node_id, key)
    kg.decay_memories = lambda age=DECAY_THRESHOLD: gm.decay_memories(age)
    kg.export_memory = lambda: gm.export_memory()
    kg.import_memory = lambda state, cf=True: gm.import_memory(state, cf)
    kg.get_memory_stats = lambda: gm.get_stats()
    kg._graph_memory = gm

    return gm

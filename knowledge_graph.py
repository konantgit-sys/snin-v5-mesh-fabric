#!/usr/bin/env python3
"""Knowledge Graph — граф mesh-топологии для оптимизации маршрутизации.

Строит взвешенный ориентированный граф агентов и каналов связи.
Обновляется в реальном времени из потока событий RouteEngine.
Используется SmartRouter для выбора оптимального маршрута.

Фаза 1: Ядро графа — GraphNode, GraphEdge, KnowledgeGraph, Redis-схема.

Redis-ключи:
  graph:nodes      — Hash: pubkey → JSON(GraphNode)
  graph:edges      — Hash: "source→target" → JSON(GraphEdge)
  graph:adj:{pk}   — Set: соседи узла
  graph:stats      — Hash: метрики графа
"""

import json
import os
import time
import redis
from dataclasses import dataclass, field, asdict
from typing import Optional
from collections import defaultdict


# ─── Data Classes ─────────────────────────────────────────

@dataclass
class GraphNode:
    """Узел графа — агент или релей."""
    node_id: str
    node_type: str = "agent"  # agent | relay | gateway
    last_seen: float = 0.0
    heartbeat_interval: int = 30
    status: str = "unknown"   # online | offline | degraded | unknown
    capabilities: list = field(default_factory=list)
    position: dict = field(default_factory=dict)  # {"lat": ..., "lon": ...}

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, data: str | dict) -> "GraphNode":
        if isinstance(data, str):
            data = json.loads(data)
        return cls(**{
            k: data.get(k, v.default if v.default is not v.default else None)
            for k, v in cls.__dataclass_fields__.items()
        })


@dataclass
class GraphEdge:
    """Ребро графа — канал связи между двумя узлами."""
    source: str
    target: str
    transport: str = "unknown"  # wifi | lora | esp_now | ble | nostr
    weight: float = 1.0
    latency_ms: float = 0.0
    success_rate: float = 1.0
    bandwidth_kbps: float = 0.0
    hop_count: int = 1
    last_success: float = 0.0
    last_failure: float = 0.0
    failures_24h: int = 0

    @property
    def edge_id(self) -> str:
        return f"{self.source}→{self.target}"

    def to_json(self) -> str:
        d = asdict(self)
        d.pop("edge_id", None)  # computed, not stored
        return json.dumps(d, ensure_ascii=False)

    @classmethod
    def from_json(cls, data: str | dict) -> "GraphEdge":
        if isinstance(data, str):
            data = json.loads(data)
        data.pop("edge_id", None)
        return cls(**{
            k: data.get(k, v.default if v.default is not v.default else None)
            for k, v in cls.__dataclass_fields__.items()
            if k != "edge_id"
        })

    def compute_weight(self, cb_penalty: float = 1.0) -> float:
        """Пересчитать композитный вес ребра.

        Args:
            cb_penalty: множитель CircuitBreaker (1.0 = без штрафа, >1.0 = penalty)
        """
        w = (self.latency_ms / 1000.0)              # нормализованная задержка
        w += (1.0 - self.success_rate) * 10.0        # штраф за ненадёжность
        w += self.hop_count * 0.5                     # штраф за каждый hop

        # Штраф за узкий канал
        if self.bandwidth_kbps == 0:
            pass  # неизвестно — без штрафа
        elif self.bandwidth_kbps < 50:
            w += 5.0   # BLE / LoRa slow
        elif self.bandwidth_kbps < 1000:
            w += 2.0   # LoRa fast
        # WiFi (>1000 kbps) — без штрафа

        # Phase 7: CircuitBreaker penalty multiplier
        w *= cb_penalty

        self.weight = round(w, 4)
        return self.weight


# ─── Knowledge Graph ──────────────────────────────────────

class KnowledgeGraph:
    """Граф mesh-топологии с хранением в Redis."""

    # Redis key prefixes
    KEY_NODES = "graph:nodes"
    KEY_EDGES = "graph:edges"
    KEY_ADJ = "graph:adj"
    KEY_STATS = "graph:stats"
    KEY_SNAPSHOT = "graph:snapshot"

    # Пороги для статусов
    ONLINE_THRESHOLD = 30      # сек: last_seen < 30 → online
    DEGRADED_THRESHOLD = 120   # сек: last_seen < 120 → degraded
    # > 120 → offline

    # TTL для рёбер без активности (24 часа)
    EDGE_TTL = 86400

    # Phase 8: Redis PubSub channel для multi-node синхронизации
    SYNC_CHANNEL = "graph:sync"

    def __init__(self, redis_client: redis.Redis, node_id: str = "node-0"):
        self.r = redis_client
        self.node_id = node_id  # Phase 8: идентификатор ноды для фильтрации своих событий
        # In-memory кеш для быстрых операций (синхронизируется с Redis)
        self._nodes: dict[str, GraphNode] = {}
        self._edges: dict[str, GraphEdge] = {}
        self._adj: dict[str, set[str]] = defaultdict(set)
        self._initialized = False

        # Phase 7: CircuitBreaker карта штрафов
        # Ключ: имя канала CB (direct/mesh/nostr/gossip)
        # Значение: множитель веса (1.0 = без штрафа, OPEN=100, HALF_OPEN=20)
        self._cb_penalties: dict[str, float] = {}

        # Phase 7: маппинг CB-каналов на edge-транспорты
        self._cb_to_transport: dict[str, str | None] = {
            "direct": "wifi",    # TCP Gateway → wifi edges
            "mesh": None,        # SmartRouter — ВСЕ рёбра
            "nostr": "nostr",    # NostrBridge → nostr edges
            "gossip": "lora",    # GossipServer → lora edges
        }

        # Phase 8: PubSub синхронизация
        self._pubsub: redis.client.PubSub | None = None
        self._sync_running = False
        self._sync_stats = {"published": 0, "received": 0, "skipped_own": 0, "errors": 0}

    # ─── Инициализация ──────────────────────────────────

    def load_from_redis(self) -> bool:
        """Загрузить граф из Redis. Возвращает True если данные есть."""
        nodes_raw = self.r.hgetall(self.KEY_NODES)
        if not nodes_raw:
            return False

        for pk_bytes, data_bytes in nodes_raw.items():
            pk = pk_bytes.decode() if isinstance(pk_bytes, bytes) else pk_bytes
            try:
                if isinstance(data_bytes, bytes):
                    data_bytes = data_bytes.decode()
                self._nodes[pk] = GraphNode.from_json(data_bytes)
            except Exception:
                continue

        edges_raw = self.r.hgetall(self.KEY_EDGES)
        for eid_bytes, data_bytes in edges_raw.items():
            eid = eid_bytes.decode() if isinstance(eid_bytes, bytes) else eid_bytes
            try:
                if isinstance(data_bytes, bytes):
                    data_bytes = data_bytes.decode()
                edge = GraphEdge.from_json(data_bytes)
                self._edges[eid] = edge
                self._adj[edge.source].add(edge.target)
            except Exception:
                continue

        self._initialized = bool(self._nodes)
        return self._initialized

    @property
    def is_ready(self) -> bool:
        return self._initialized and len(self._nodes) > 0

    # ─── Узлы ────────────────────────────────────────────

    def upsert_node(self, node: GraphNode) -> None:
        """Добавить или обновить узел."""
        self._nodes[node.node_id] = node
        self.r.hset(self.KEY_NODES, node.node_id, node.to_json())
        self._initialized = True
        self._publish("node:upsert", {"node_id": node.node_id, "node_type": node.node_type, "status": node.status})

    def get_node(self, node_id: str) -> Optional[GraphNode]:
        return self._nodes.get(node_id)

    def update_node_status(self, node_id: str, status: str, last_seen: float = None) -> bool:
        """Обновить статус узла. Возвращает False если узел не найден."""
        node = self._nodes.get(node_id)
        if not node:
            return False
        node.status = status
        if last_seen is not None:
            node.last_seen = last_seen
        self.r.hset(self.KEY_NODES, node_id, node.to_json())
        self._publish("node:upsert", {"node_id": node_id, "status": status})
        return True

    def get_node_status(self, node_id: str) -> str:
        """Определить статус узла по last_seen."""
        node = self._nodes.get(node_id)
        if not node:
            return "unknown"
        now = time.time()
        delta = now - node.last_seen
        if delta < self.ONLINE_THRESHOLD:
            return "online"
        elif delta < self.DEGRADED_THRESHOLD:
            return "degraded"
        else:
            return "offline"

    # ─── Рёбра ────────────────────────────────────────────

    def upsert_edge(self, edge: GraphEdge) -> None:
        """Добавить или обновить ребро. Автоматически пересчитывает вес с CB-штрафом."""
        penalty = self._cb_weight_penalty(edge.transport)
        edge.compute_weight(cb_penalty=penalty)
        eid = edge.edge_id
        self._edges[eid] = edge
        self._adj[edge.source].add(edge.target)
        self.r.hset(self.KEY_EDGES, eid, edge.to_json())
        self.r.sadd(f"{self.KEY_ADJ}:{edge.source}", edge.target)
        self._publish("edge:upsert", {"edge_id": eid, "source": edge.source, "target": edge.target, "transport": edge.transport})

    def get_edge(self, source: str, target: str) -> Optional[GraphEdge]:
        return self._edges.get(f"{source}→{target}")

    def get_neighbors(self, node_id: str) -> set[str]:
        """Получить множество соседей узла (исходящие рёбра)."""
        return self._adj.get(node_id, set())

    def record_delivery(self, source: str, target: str, success: bool,
                        latency_ms: float = 0) -> None:
        """Записать результат доставки: ACK (success=True) или NACK (False)."""
        edge = self.get_edge(source, target)
        if not edge:
            # Создать новое ребро по факту доставки
            edge = GraphEdge(source=source, target=target, transport="inferred")
            self.upsert_edge(edge)

        now = time.time()
        if success:
            edge.last_success = now
            edge.success_rate = min(1.0, edge.success_rate + 0.05)
        else:
            edge.last_failure = now
            edge.success_rate = max(0.0, edge.success_rate - 0.15)
            edge.failures_24h += 1

        if latency_ms > 0:
            # EWMA (сглаживание) с коэффициентом 0.3
            if edge.latency_ms == 0:
                edge.latency_ms = latency_ms
            else:
                edge.latency_ms = edge.latency_ms * 0.7 + latency_ms * 0.3

        edge.compute_weight(cb_penalty=self._cb_weight_penalty(edge.transport))
        self.r.hset(self.KEY_EDGES, edge.edge_id, edge.to_json())
        self._publish("edge:delivery", {"edge_id": edge.edge_id, "success": success})

    def decay_edges(self) -> int:
        """Деградировать рёбра без активности. Возвращает количество затронутых."""
        now = time.time()
        count = 0
        for eid, edge in list(self._edges.items()):
            # Если нет активности > 10 минут — медленная деградация
            last_activity = max(edge.last_success, edge.last_failure)
            if last_activity > 0 and now - last_activity > 600:
                edge.success_rate = max(0.1, edge.success_rate * 0.99)
                edge.compute_weight(cb_penalty=self._cb_weight_penalty(edge.transport))
                self.r.hset(self.KEY_EDGES, eid, edge.to_json())
                self._publish("edge:delivery", {"edge_id": eid, "decayed": True})
                count += 1

            # Если нет активности > 24 часов — удалить
            if last_activity > 0 and now - last_activity > self.EDGE_TTL:
                del self._edges[eid]
                self.r.hdel(self.KEY_EDGES, eid)
                self._adj[edge.source].discard(edge.target)
                self._publish("edge:delete", {"edge_id": eid})
                count += 1

        return count

    # ─── Алгоритмы (Фаза 2) ──────────────────────────────

    def _store_penalty(self, node_id: str) -> float:
        """Вычислить penalty за store-and-forward для узла.

        Если узел online — штрафа нет (0).
        Если degraded — лёгкий штраф (вес * 5).
        Если offline < 5 минут — средний штраф (вес * 30).
        Если offline > 5 минут — тяжёлый штраф (300).

        Используется в Dijkstra чтобы маршрут обходил отвалившиеся узлы.
        """
        status = self.get_node_status(node_id)
        if status == "online":
            return 0.0

        node = self.get_node(node_id)
        if not node:
            return 300.0  # узел неизвестен — считаем недоступным

        now = time.time()
        offline_duration = now - node.last_seen if node.last_seen > 0 else 99999

        if status == "degraded":
            # Лёгкий штраф — узел может вернуться
            return min(offline_duration * 0.5, 30.0)

        # offline
        if offline_duration < 300:  # < 5 минут
            return 30.0 + offline_duration * 0.1
        else:
            return 300.0  # практически недоступен

    def find_path(self, source: str, target: str) -> list[str] | None:
        """Модифицированный Dijkstra с учётом store-and-forward.

        Минимизирует: sum(weight(u,v)) + sum(store_penalty(v))

        Возвращает список node_id от source до target включительно.
        Если путь не найден — возвращает None.
        Если source == target — возвращает [source].
        """
        if source == target:
            return [source]

        if source not in self._nodes or target not in self._nodes:
            return None

        import heapq

        # Расстояния от source до каждого узла
        dist: dict[str, float] = {source: 0.0}
        # Предыдущий узел на оптимальном пути
        prev: dict[str, str | None] = {source: None}
        # Множество посещённых узлов
        visited: set[str] = set()
        # Очередь: (расстояние, узел)
        pq = [(0.0, source)]

        while pq:
            d, u = heapq.heappop(pq)
            if u in visited:
                continue
            visited.add(u)

            # Дошли до цели — восстанавливаем путь
            if u == target:
                path = []
                cur = target
                while cur is not None:
                    path.append(cur)
                    cur = prev.get(cur)
                path.reverse()
                return path

            # Проверяем соседей
            for v in self.get_neighbors(u):
                if v in visited:
                    continue

                edge = self.get_edge(u, v)
                if not edge:
                    continue

                # Если ребро помечено как ненадёжное и есть альтернативы — скипаем
                if edge.success_rate < 0.5:
                    continue

                # Вес ребра + store-пенальти на целевом узле
                penalty = self._store_penalty(v) if v != target else 0.0
                new_dist = d + edge.weight + penalty

                if new_dist < dist.get(v, float("inf")):
                    dist[v] = new_dist
                    prev[v] = u
                    heapq.heappush(pq, (new_dist, v))

        return None  # путь не найден

    def find_path_fallback(self, source: str, target: str) -> list[str] | None:
        """Поиск пути с fallback: сначала без учёта ненадёжных рёбер,
        потом с ними (если основного маршрута нет).
        """
        path = self.find_path(source, target)
        if path:
            return path

        # Fallback: разрешаем ненадёжные рёбра (упрощённый Dijkstra)
        if source not in self._nodes or target not in self._nodes:
            return None

        import heapq
        dist = {source: 0.0}
        prev = {source: None}
        visited = set()
        pq = [(0.0, source)]

        while pq:
            d, u = heapq.heappop(pq)
            if u in visited:
                continue
            visited.add(u)

            if u == target:
                path = []
                cur = target
                while cur is not None:
                    path.append(cur)
                    cur = prev.get(cur)
                path.reverse()
                return path

            for v in self.get_neighbors(u):
                if v in visited:
                    continue
                edge = self.get_edge(u, v)
                if not edge:
                    continue
                # Разрешаем ВСЕ рёбра, даже ненадёжные
                penalty = self._store_penalty(v) if v != target else 0.0
                new_dist = d + edge.weight + penalty
                if new_dist < dist.get(v, float("inf")):
                    dist[v] = new_dist
                    prev[v] = u
                    heapq.heappush(pq, (new_dist, v))

        return None

    def get_path_info(self, path: list[str]) -> dict:
        """Информация о пути: длина, суммарный вес, рёбра."""
        if not path or len(path) < 1:
            return {"hops": 0, "total_weight": 0.0, "edges": [], "valid": False}

        edges = []
        total_weight = 0.0
        for i in range(len(path) - 1):
            edge = self.get_edge(path[i], path[i + 1])
            if edge:
                edges.append({
                    "source": edge.source,
                    "target": edge.target,
                    "transport": edge.transport,
                    "weight": edge.weight,
                    "latency_ms": edge.latency_ms,
                    "success_rate": edge.success_rate,
                })
                total_weight += edge.weight

        return {
            "hops": len(path) - 1,
            "total_weight": round(total_weight, 4),
            "edges": edges,
            "valid": True,
            "path": path,
        }

    # ─── Phase 7: CircuitBreaker интеграция ────────────────

    def _cb_weight_penalty(self, transport: str) -> float:
        """Множитель веса на основе состояния CircuitBreaker.

        mesh (None-транспорт) → штрафуется через специальный ключ 'mesh',
        влияющий на ВСЕ рёбра. Для остальных — матчинг по транспорту.
        """
        penalty = 1.0

        # mesh-канал затрагивает ВСЕ рёбра (value=None в маппинге)
        mesh_penalty = self._cb_penalties.get("mesh", 1.0)
        if mesh_penalty > 1.0:
            penalty = mesh_penalty

        # Матчинг по транспорту (прямой)
        for cb_name, cb_transport in self._cb_to_transport.items():
            if cb_transport is not None and cb_transport == transport:
                if cb_name in self._cb_penalties:
                    penalty = max(penalty, self._cb_penalties[cb_name])

        return penalty

    def update_from_circuit_breaker(self, cb_channels: dict) -> dict:
        """Обновить штрафы на основе состояния CircuitBreaker.

        Args:
            cb_channels: словарь {channel_name: {"state": "closed"|"open"|"half_open"}}

        Returns:
            dict с изменениями: {"applied": [...], "cleared": [...]}
        """
        applied = []
        cleared = []

        for name, info in cb_channels.items():
            state = info.get("state", "unknown")
            if state == "open":
                self._cb_penalties[name] = 100.0
                applied.append(f"{name}:OPEN(x100)")
            elif state == "half_open":
                self._cb_penalties[name] = 20.0
                applied.append(f"{name}:HALF_OPEN(x20)")
            elif state == "closed":
                if name in self._cb_penalties:
                    del self._cb_penalties[name]
                    cleared.append(f"{name}:→no penalty")
                # Также очищаем для здоровых каналов (на случай stale)
            else:
                # unknown state — очищаем
                if name in self._cb_penalties:
                    del self._cb_penalties[name]
                    cleared.append(f"{name}:unknown→cleared")

        # Пересчитываем веса всех рёбер с новыми штрафами
        modified = 0
        for edge in self._edges.values():
            old_weight = edge.weight
            penalty = self._cb_weight_penalty(edge.transport)
            edge.compute_weight(cb_penalty=penalty)
            if edge.weight != old_weight:
                modified += 1

        return {
            "applied": applied,
            "cleared": cleared,
            "edges_modified": modified,
            "active_penalties": dict(self._cb_penalties),
        }

    def get_cb_penalties(self) -> dict:
        """Текущие CB-штрафы для отладки."""
        return dict(self._cb_penalties)

    # ─── Phase 8: Redis PubSub Multi-node Sync ────────────

    def _publish(self, event_type: str, payload: dict) -> bool:
        """Опубликовать событие синхронизации в Redis PubSub."""
        if not self.r:
            return False
        try:
            data = json.dumps({
                "node_id": self.node_id,
                "ts": time.time(),
                "type": event_type,
                "payload": payload,
            })
            self.r.publish(self.SYNC_CHANNEL, data)
            self._sync_stats["published"] += 1
            return True
        except Exception:
            self._sync_stats["errors"] += 1
            return False

    def reload_node(self, node_id: str) -> bool:
        """Перезагрузить один узел из Redis."""
        if not self.r:
            return False
        raw = self.r.hget(self.KEY_NODES, node_id)
        if raw:
            try:
                if isinstance(raw, bytes):
                    raw = raw.decode()
                self._nodes[node_id] = GraphNode.from_json(raw)
                return True
            except Exception:
                return False
        else:
            # Узел удалён из Redis — удалить из кеша
            self._nodes.pop(node_id, None)
            return False

    def reload_edge(self, edge_id: str) -> bool:
        """Перезагрузить одно ребро из Redis."""
        if not self.r:
            return False
        raw = self.r.hget(self.KEY_EDGES, edge_id)
        if raw:
            try:
                if isinstance(raw, bytes):
                    raw = raw.decode()
                edge = GraphEdge.from_json(raw)
                self._edges[edge_id] = edge
                # Обновить adjacency
                self._adj[edge.source].add(edge.target)
                return True
            except Exception:
                return False
        else:
            # Ребро удалено из Redis
            old = self._edges.pop(edge_id, None)
            if old:
                self._adj[old.source].discard(old.target)
            return False

    def full_reload(self) -> int:
        """Полная перезагрузка графа из Redis. Возвращает количество изменений."""
        old_nodes = set(self._nodes.keys())
        old_edges = set(self._edges.keys())
        self._nodes.clear()
        self._edges.clear()
        self._adj.clear()
        self.load_from_redis()
        new_nodes = set(self._nodes.keys())
        new_edges = set(self._edges.keys())
        added_nodes = new_nodes - old_nodes
        removed_nodes = old_nodes - new_nodes
        added_edges = new_edges - old_edges
        removed_edges = old_edges - new_edges
        return len(added_nodes) + len(removed_nodes) + len(added_edges) + len(removed_edges)

    def start_sync(self) -> None:
        """Запустить синхронизацию через PubSub (вызывается в асинхронном контексте)."""
        if not self.r:
            return
        self._pubsub = self.r.pubsub()
        self._pubsub.subscribe(self.SYNC_CHANNEL)
        self._sync_running = True
        # Дать Redis время обработать subscribe (требуется ~0.3s стабильно)
        time.sleep(0.5)

    def process_sync_events(self) -> dict:
        """Обработать накопившиеся события PubSub (неблокирующий вызов).

        Пауза 0.5с для получения pending-сообщений, затем drain до 50 сообщений.

        Возвращает: {"processed": int, "skipped_own": int, "reloaded_nodes": int,
                       "reloaded_edges": int, "errors": int}
        """
        if not self._pubsub or not self._sync_running:
            return {"processed": 0, "skipped_own": 0, "reloaded_nodes": 0, "reloaded_edges": 0, "errors": 0}

        stats = {"processed": 0, "skipped_own": 0, "reloaded_nodes": 0, "reloaded_edges": 0, "errors": 0}

        try:
            # Пауза для получения pending-сообщений (Redis PubSub latency ~0.1-0.5s)
            time.sleep(0.5)

            # Drain до 50 сообщений
            for _ in range(50):
                msg = self._pubsub.get_message(ignore_subscribe_messages=True, timeout=0.02)
                if msg is None:
                    break

                stats["processed"] += 1
                self._sync_stats["received"] += 1

                try:
                    data_raw = msg.get("data", b"{}")
                    if isinstance(data_raw, bytes):
                        data_raw = data_raw.decode()
                    data = json.loads(data_raw)
                    if not isinstance(data, dict):
                        stats["errors"] += 1
                        continue
                except json.JSONDecodeError:
                    stats["errors"] += 1
                    continue

                # Пропускаем свои события
                if data.get("node_id") == self.node_id:
                    self._sync_stats["skipped_own"] += 1
                    stats["skipped_own"] += 1
                    continue

                event_type = data.get("type", "")
                payload = data.get("payload", {})

                try:
                    if event_type == "node:upsert":
                        if self.reload_node(payload.get("node_id", "")):
                            stats["reloaded_nodes"] += 1
                    elif event_type == "edge:upsert":
                        if self.reload_edge(payload.get("edge_id", "")):
                            stats["reloaded_edges"] += 1
                    elif event_type == "edge:delivery":
                        if self.reload_edge(payload.get("edge_id", "")):
                            stats["reloaded_edges"] += 1
                    elif event_type == "edge:delete":
                        edge_id = payload.get("edge_id", "")
                        old = self._edges.pop(edge_id, None)
                        if old:
                            self._adj[old.source].discard(old.target)
                            stats["reloaded_edges"] += 1
                    elif event_type == "full:sync":
                        # Другая нода просит полную перезагрузку
                        changes = self.full_reload()
                        stats["reloaded_nodes"] += changes
                except Exception:
                    stats["errors"] += 1

        except Exception:
            stats["errors"] += 1

        return stats

    def stop_sync(self) -> None:
        """Остановить синхронизацию."""
        self._sync_running = False
        if self._pubsub:
            try:
                self._pubsub.unsubscribe(self.SYNC_CHANNEL)
                self._pubsub.close()
            except Exception:
                pass
            self._pubsub = None

    def get_sync_stats(self) -> dict:
        """Статистика синхронизации."""
        return dict(self._sync_stats)

    # ─── Статистика ───────────────────────────────────────

    def get_stats(self) -> dict:
        """Метрики графа."""
        online = sum(1 for n in self._nodes.values() if self.get_node_status(n.node_id) == "online")
        offline = sum(1 for n in self._nodes.values() if self.get_node_status(n.node_id) == "offline")
        degraded = len(self._nodes) - online - offline

        weights = [e.weight for e in self._edges.values()]
        avg_weight = round(sum(weights) / len(weights), 2) if weights else 0
        success_rates = [e.success_rate for e in self._edges.values()]
        avg_success_rate = round(sum(success_rates) / len(success_rates), 3) if success_rates else 0

        return {
            "total_nodes": len(self._nodes),
            "total_edges": len(self._edges),
            "avg_weight": avg_weight,
            "avg_success_rate": avg_success_rate,
            "nodes_online": online,
            "nodes_offline": offline,
            "nodes_degraded": degraded,
            "ready": self.is_ready,
        }

    def save_stats(self) -> None:
        """Сохранить статистику в Redis."""
        stats = self.get_stats()
        self.r.hset(self.KEY_STATS, mapping={k: str(v) for k, v in stats.items()})

    # ─── Сброс ────────────────────────────────────────────

    def flush(self) -> None:
        """Полностью очистить граф (in-memory и Redis)."""
        self._nodes.clear()
        self._edges.clear()
        self._adj.clear()
        self._initialized = False

        # Очистка Redis
        for pk in self.r.hkeys(self.KEY_NODES):
            self.r.hdel(self.KEY_NODES, pk)
        for eid in self.r.hkeys(self.KEY_EDGES):
            self.r.hdel(self.KEY_EDGES, eid)
        self.r.delete(self.KEY_STATS)

        self._publish("full:sync", {"action": "flush"})

    # ─── Статус-строка (для логов) ───────────────────────

    def status_line(self) -> str:
        """Однострочный статус для вывода в лог ContentRouter."""
        s = self.get_stats()
        return (f"Graph: nodes={s['total_nodes']} edges={s['total_edges']} "
                f"avg_w={s['avg_weight']} "
                f"online={s['nodes_online']} off={s['nodes_offline']} deg={s['nodes_degraded']}")

    # ════════════════════════════════════════════════════════════
    # Phase 9: Snapshots, Integrity, Export/Import
    # ════════════════════════════════════════════════════════════

    def export_state(self) -> dict:
        """Экспортировать полное состояние графа как dict (для snapshot/checkpoint).

        Результат содержит nodes, edges и adjacency, готовые к JSON-сериализации.
        """
        nodes = {}
        for pk, node in self._nodes.items():
            nodes[pk] = {
                "node_id": node.node_id,
                "node_type": node.node_type,
                "last_seen": node.last_seen,
                "status": node.status,
                "pubkey": getattr(node, "pubkey", ""),
                "transport_hint": getattr(node, "transport_hint", ""),
            }

        edges = {}
        for eid, edge in self._edges.items():
            edges[eid] = {
                "source": edge.source,
                "target": edge.target,
                "transport": edge.transport,
                "latency_ms": edge.latency_ms,
                "success_rate": edge.success_rate,
                "last_success": edge.last_success,
                "nack_count": getattr(edge, "nack_count", 0),
                "weight": getattr(edge, "weight", 0.0),
            }

        adj = {}
        for src, targets in self._adj.items():
            adj[src] = sorted(targets)

        return {
            "version": 9,
            "node_id": self.node_id,
            "exported_at": time.time(),
            "nodes": nodes,
            "edges": edges,
            "adj": adj,
            "stats": self.get_stats(),
        }

    def import_state(self, state: dict, clear_first: bool = True) -> int:
        """Импортировать состояние графа из export_state() dict.

        clear_first=True: очистить текущий граф перед импортом.
        Возвращает количество импортированных сущностей.
        """
        if clear_first:
            self._nodes.clear()
            self._edges.clear()
            self._adj.clear()

        count = 0
        for pk, ndata in state.get("nodes", {}).items():
            node = GraphNode(
                node_id=ndata["node_id"],
                node_type=ndata.get("node_type", "agent"),
                last_seen=ndata.get("last_seen", time.time()),
                status=ndata.get("status", "online"),
            )
            node.pubkey = ndata.get("pubkey", "")
            node.transport_hint = ndata.get("transport_hint", "")
            self._nodes[pk] = node
            count += 1

        for eid, edata in state.get("edges", {}).items():
            edge = GraphEdge(
                source=edata["source"],
                target=edata["target"],
                transport=edata.get("transport", "unknown"),
                latency_ms=edata.get("latency_ms", 0),
                success_rate=edata.get("success_rate", 1.0),
                last_success=edata.get("last_success", time.time()),
            )
            edge.nack_count = edata.get("nack_count", 0)
            self._edges[eid] = edge
            self._adj.setdefault(edge.source, set()).add(edge.target)
            count += 1

        return count

    def save_snapshot(self, path: str) -> int:
        """Сохранить снапшот графа в JSON-файл + записать в Redis.

        Возвращает размер файла в байтах.
        """
        state = self.export_state()
        json_bytes = json.dumps(state, indent=2, ensure_ascii=False).encode()

        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "wb") as f:
            f.write(json_bytes)

        # Дублируем в Redis (TTL: 7 дней для аварийного restore)
        self.r.set(self.KEY_SNAPSHOT, json_bytes, ex=604800)

        return len(json_bytes)

    def restore_snapshot(self, path: str = None) -> int:
        """Восстановить граф из снапшота.

        Приоритет: 1) переданный path, 2) Redis KEY_SNAPSHOT, 3) load_from_redis.
        Возвращает количество импортированных сущностей.
        """
        state = None

        # 1) Файл
        if path and os.path.exists(path):
            with open(path, "rb") as f:
                state = json.loads(f.read().decode())

        # 2) Redis snapshot
        if not state:
            raw = self.r.get(self.KEY_SNAPSHOT)
            if raw:
                state = json.loads(raw.decode() if isinstance(raw, bytes) else raw)

        # 3) Fallback: обычная загрузка
        if not state:
            self.load_from_redis()
            return len(self._nodes) + len(self._edges)

        return self.import_state(state, clear_first=True)

    def integrity_check(self) -> dict:
        """Проверить целостность графа.

        Возвращает: {"ok": bool, "issues": [str], "orphan_nodes": [], "dangling_edges": []}
        """
        issues = []
        orphan_nodes = []
        dangling_edges = []

        # Дубликаты рёбер
        seen = set()
        for eid in list(self._edges.keys()):
            if eid in seen:
                issues.append(f"dup edge: {eid}")
            seen.add(eid)

        # Висячие рёбра (source или target отсутствуют)
        for eid, edge in self._edges.items():
            if edge.source not in self._nodes:
                dangling_edges.append(eid)
                issues.append(f"dangling edge {eid}: source {edge.source} missing")
            if edge.target not in self._nodes:
                dangling_edges.append(eid)
                issues.append(f"dangling edge {eid}: target {edge.target} missing")

        # Узлы без рёбер (сироты — не обязательно ошибка, но отмечаем)
        connected = set()
        for edge in self._edges.values():
            connected.add(edge.source)
            connected.add(edge.target)
        orphan_nodes = sorted(set(self._nodes.keys()) - connected)

        # adjacency vs edges consistency
        for eid, edge in self._edges.items():
            if edge.target not in self._adj.get(edge.source, set()):
                issues.append(f"adj mismatch: {eid} — target not in _adj")

        for src, targets in self._adj.items():
            for tgt in targets:
                eid = f"{src}→{tgt}"
                if eid not in self._edges:
                    issues.append(f"adj has {eid} but no edge")

        return {
            "ok": len(issues) == 0,
            "issues": issues,
            "orphan_nodes": orphan_nodes,
            "dangling_edges": dangling_edges,
            "total_nodes": len(self._nodes),
            "total_edges": len(self._edges),
        }

    # ════════════════════════════════════════════════════════════
    # Phase 9: Graph Health (для Supervisor мониторинга)
    # ════════════════════════════════════════════════════════════

    def get_graph_health(self) -> dict:
        """Метрики здоровья графа для supervisor.

        Возвращает: {"ready": bool, "nodes": int, "edges": int,
                      "synced": bool, "integrity_ok": bool, "degraded_pct": float}
        """
        stats = self.get_stats()
        integrity = self.integrity_check()

        total = stats["total_nodes"]
        degraded_pct = (stats["nodes_degraded"] / total * 100) if total > 0 else 0

        return {
            "ready": self.is_ready,
            "nodes": total,
            "edges": stats["total_edges"],
            "online": stats["nodes_online"],
            "offline": stats["nodes_offline"],
            "degraded": stats["nodes_degraded"],
            "degraded_pct": round(degraded_pct, 1),
            "syncing": self._sync_running,
            "integrity_ok": integrity["ok"],
            "integrity_issues": len(integrity["issues"]),
            "avg_weight": round(stats["avg_weight"], 3),
            "avg_success_rate": round(stats["avg_success_rate"], 3),
        }


# ─── Factory ──────────────────────────────────────────────

def create_knowledge_graph(redis_url: str = "redis://localhost:6379/0") -> KnowledgeGraph:
    """Создать KnowledgeGraph с подключением к Redis."""
    r = redis.Redis.from_url(redis_url, decode_responses=False)
    kg = KnowledgeGraph(r)
    kg.load_from_redis()
    return kg

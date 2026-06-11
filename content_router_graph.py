#!/usr/bin/env python3
"""
ContentRouter Knowledge Graph — графо-оптимизированный слой маршрутизации контента.

Спецификация: Google Doc #2 (ContentRouter Knowledge Graph)
Интегрирует KnowledgeGraph P10 с ContentRouter P12.

Архитектура:
  ContentRouter (классификация) → ContentRouterGraph (оптимизация пути)
      ↓
  KnowledgeGraph (Dijkstra + store_penalty + edge_decay)
      ↓
  SemanticRouter (доставка эксперту)

Ключевые компоненты:
  RouteOptimizer     — модифицированный Dijkstra с учётом agent_load и store_penalty
  EdgeDecayManager   — управление свежестью рёбер topic↔agent
  TopicMapper        — маппинг тем на графовые ID для поиска пути
  LoadBalancer       — распределение нагрузки при множественных экспертах

Интеграция:
  content_router_graph = ContentRouterGraph(knowledge_graph, content_router)
  route = content_router_graph.optimize_route(event, classification)
"""

import logging
import time
import heapq
from dataclasses import dataclass, field
from typing import Optional

from knowledge_graph import KnowledgeGraph, GraphNode, GraphEdge

logger = logging.getLogger("ContentRouterGraph")


# ═══════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════

@dataclass
class TopicNode:
    """Узел темы в графе контента."""
    topic_id: str           # нормализованный topic (напр. "bitcoin:defi:lightning")
    label: str              # человекочитаемое имя
    category: str = "general"
    agent_count: int = 0    # сколько агентов разбирается
    total_deliveries: int = 0
    success_rate: float = 0.0


@dataclass
class TopicEdge:
    """Ребро тема→агент с метриками доставки."""
    topic_id: str
    agent_id: str
    weight: float = 1.0
    store_penalty: float = 0.0   # штраф за очередь сообщений агента
    last_delivery: float = 0.0   # timestamp последней успешной доставки
    delivery_count: int = 0
    success_count: int = 0

    @property
    def success_ratio(self) -> float:
        if self.delivery_count == 0:
            return 0.5
        return self.success_count / self.delivery_count

    @property
    def effective_weight(self) -> float:
        """Итоговый вес: базовый + штраф + фактор свежести."""
        # Чем больше store_penalty — тем тяжелее ребро
        base = self.weight + self.store_penalty * 0.5

        # Edge decay: ребро теряет актуальность если >1 часа без доставки
        if self.last_delivery > 0:
            age_hours = (time.time() - self.last_delivery) / 3600
            decay = min(age_hours * 0.15, 0.5)  # max 50% decay
            base *= (1.0 + decay)

        # Фактор успешности: надёжные агенты легче
        sr = self.success_ratio
        base *= (1.5 - sr * 0.5)  # 1.5 при 0%, 1.0 при 100%

        return max(base, 0.1)


@dataclass
class OptimizedRoute:
    """Результат оптимизации маршрута."""
    topic: str
    agent_id: str
    path: list[str]          # путь в графе к агенту
    total_weight: float
    alternatives: list = field(default_factory=list)  # [(agent_id, weight), ...]
    method: str = "dijkstra"


# ═══════════════════════════════════════════════════════════════
# EDGE DECAY MANAGER
# ═══════════════════════════════════════════════════════════════

class EdgeDecayManager:
    """Управление свежестью рёбер topic↔agent."""

    def __init__(self, decay_rate: float = 0.15, max_decay: float = 0.5):
        self.decay_rate = decay_rate   # в час
        self.max_decay = max_decay
        self._edges: dict[str, TopicEdge] = {}

    def register_edge(self, topic_id: str, agent_id: str) -> TopicEdge:
        key = f"{topic_id}::{agent_id}"
        if key not in self._edges:
            self._edges[key] = TopicEdge(topic_id=topic_id, agent_id=agent_id)
        return self._edges[key]

    def record_delivery(self, topic_id: str, agent_id: str, success: bool):
        key = f"{topic_id}::{agent_id}"
        edge = self._edges.get(key)
        if not edge:
            edge = self.register_edge(topic_id, agent_id)
        edge.delivery_count += 1
        if success:
            edge.success_count += 1
            edge.last_delivery = time.time()
            # Уменьшаем store_penalty при успешной доставке
            edge.store_penalty = max(0, edge.store_penalty - 0.1)

    def apply_decay(self):
        """Применить decay ко всем рёбрам."""
        now = time.time()
        for edge in self._edges.values():
            if edge.last_delivery > 0:
                age_hours = (now - edge.last_delivery) / 3600
                decay = min(age_hours * self.decay_rate, self.max_decay)
                edge.weight = 1.0 * (1.0 + decay)

    def get_effective_edges(self, topic_id: str) -> list[TopicEdge]:
        """Получить все рёбра для темы, отсортированные по весу."""
        edges = [
            e for k, e in self._edges.items()
            if k.startswith(f"{topic_id}::")
        ]
        edges.sort(key=lambda e: e.effective_weight)
        return edges


# ═══════════════════════════════════════════════════════════════
# LOAD BALANCER
# ═══════════════════════════════════════════════════════════════

class LoadBalancer:
    """Распределение нагрузки между экспертами одной темы."""

    def __init__(self):
        self._agent_loads: dict[str, int] = {}        # agent_id → pending count
        self._agent_max_load: int = 10

    def increment_load(self, agent_id: str):
        self._agent_loads[agent_id] = self._agent_loads.get(agent_id, 0) + 1

    def decrement_load(self, agent_id: str):
        if self._agent_loads.get(agent_id, 0) > 0:
            self._agent_loads[agent_id] -= 1

    def get_load_penalty(self, agent_id: str) -> float:
        """Возвращает штраф 0..1+ пропорционально загрузке агента."""
        load = self._agent_loads.get(agent_id, 0)
        return load / self._agent_max_load

    def pick_best(self, candidates: list[str]) -> Optional[str]:
        """Выбрать наименее загруженного агента."""
        if not candidates:
            return None
        return min(candidates, key=lambda a: self._agent_loads.get(a, 0))


# ═══════════════════════════════════════════════════════════════
# ROUTE OPTIMIZER
# ═══════════════════════════════════════════════════════════════

class RouteOptimizer:
    """Модифицированный Dijkstra с учётом agent_load и store_penalty."""

    def __init__(self, knowledge_graph: KnowledgeGraph,
                 decay_manager: EdgeDecayManager,
                 load_balancer: LoadBalancer):
        self.kg = knowledge_graph
        self.decay = decay_manager
        self.lb = load_balancer

    def find_best_agent(self, topic_id: str,
                        exclude_agents: list[str] = None) -> Optional[OptimizedRoute]:
        """Найти лучшего агента для темы.

        Алгоритм:
        1. Получить все рёбра topic→agent из EdgeDecayManager
        2. Для каждого — вычислить effective_weight (с decay + load_penalty)
        3. Выбрать минимальный вес
        4. Построить path через KnowledgeGraph.find_path()
        """
        exclude = set(exclude_agents or [])
        candidates = self.decay.get_effective_edges(topic_id)

        results = []
        for edge in candidates:
            if edge.agent_id in exclude:
                continue

            # Итоговый вес = эффективный вес ребра + нагрузка агента
            load_penalty = self.lb.get_load_penalty(edge.agent_id)
            total = edge.effective_weight + load_penalty * 2.0

            results.append((edge.agent_id, total, edge))

        if not results:
            return None

        # Сортировка: лучший (минимальный вес) первый
        results.sort(key=lambda x: x[1])

        best_agent, best_weight, best_edge = results[0]

        # Построить путь через KnowledgeGraph
        path = self.kg.find_path("topic-hub", best_agent) or [best_agent]

        alternatives = [
            (aid, w) for aid, w, _ in results[1:4]  # top-3 альтернативы
        ]

        return OptimizedRoute(
            topic=topic_id,
            agent_id=best_agent,
            path=path,
            total_weight=best_weight,
            alternatives=alternatives,
            method="dijkstra+store_penalty"
        )

    def find_route_with_fallback(self, topic_id: str) -> Optional[OptimizedRoute]:
        """Найти маршрут с fallback-путём при недоступности."""
        route = self.find_best_agent(topic_id)
        if route:
            return route

        # Fallback: через KnowledgeGraph find_path_fallback
        path = self.kg.find_path_fallback("topic-hub", "any-agent")
        if path:
            return OptimizedRoute(
                topic=topic_id,
                agent_id=path[-1],
                path=path,
                total_weight=999.0,
                method="fallback"
            )
        return None


# ═══════════════════════════════════════════════════════════════
# MAIN: ContentRouterGraph
# ═══════════════════════════════════════════════════════════════

class ContentRouterGraph:
    """Графо-оптимизированный слой маршрутизации контента.

    Интеграция: ContentRouter → ContentRouterGraph → KnowledgeGraph → доставка.
    """

    def __init__(self, knowledge_graph: KnowledgeGraph):
        self.kg = knowledge_graph
        self.decay = EdgeDecayManager()
        self.lb = LoadBalancer()
        self.optimizer = RouteOptimizer(knowledge_graph, self.decay, self.lb)

        self._topic_index: dict[str, TopicNode] = {}
        self._stats = {
            "routes_optimized": 0,
            "fallbacks_used": 0,
            "total_deliveries": 0,
        }

    # ─── TOPIC REGISTRATION ───

    def register_topic(self, topic: str, label: str = "",
                       category: str = "general") -> TopicNode:
        """Зарегистрировать тему в графе."""
        topic_id = self._normalize_topic(topic)
        if topic_id not in self._topic_index:
            node = TopicNode(
                topic_id=topic_id,
                label=label or topic,
                category=category,
            )
            self._topic_index[topic_id] = node

            # Регистрируем в KnowledgeGraph
            kg_node = GraphNode(
                node_id=f"topic:{topic_id}",
                node_type="topic",
                status="active",
                last_seen=time.time(),
                capabilities=[f"label:{label}", f"category:{category}"],
            )
            self.kg.upsert_node(kg_node)

        return self._topic_index[topic_id]

    def register_agent_expertise(self, agent_id: str, topic: str,
                                  initial_weight: float = 1.0):
        """Зарегистрировать экспертизу агента в теме."""
        topic_id = self._normalize_topic(topic)

        # Убедимся, что тема существует
        if topic_id not in self._topic_index:
            self.register_topic(topic, category="agent-expertise")

        # Регистрируем ребро topic→agent
        edge = self.decay.register_edge(topic_id, agent_id)
        edge.weight = initial_weight

        # Регистрируем ребро в KnowledgeGraph
        kg_edge = GraphEdge(
            source=f"topic:{topic_id}",
            target=agent_id,
            transport="expertise",
            weight=initial_weight,
        )
        self.kg.upsert_edge(kg_edge)

        # Обновляем TopicNode
        node = self._topic_index[topic_id]
        node.agent_count = len(self.decay.get_effective_edges(topic_id))

    # ─── ROUTING ───

    def optimize_route(self, topic: str,
                       exclude_agents: list[str] = None) -> Optional[OptimizedRoute]:
        """Оптимизировать маршрут для темы."""
        topic_id = self._normalize_topic(topic)

        # Убедимся, что тема зарегистрирована
        if topic_id not in self._topic_index:
            self.register_topic(topic, category="auto-discovered")

        route = self.optimizer.find_route_with_fallback(topic_id)
        if route:
            self._stats["routes_optimized"] += 1
            if route.method == "fallback":
                self._stats["fallbacks_used"] += 1
            # Увеличиваем нагрузку выбранного агента
            self.lb.increment_load(route.agent_id)

        return route

    # ─── FEEDBACK ───

    def record_delivery(self, topic: str, agent_id: str, success: bool):
        """Записать результат доставки."""
        topic_id = self._normalize_topic(topic)

        # Освобождаем нагрузку агента
        if success:
            self.lb.decrement_load(agent_id)

        # Обновляем EdgeDecayManager
        self.decay.record_delivery(topic_id, agent_id, success)

        # Обновляем KnowledgeGraph
        self.kg.record_delivery(
            source=f"topic:{topic_id}",
            target=agent_id,
            success=success,
        )

        self._stats["total_deliveries"] += 1

    # ─── MAINTENANCE ───

    def apply_decay_cycle(self):
        """Применить decay ко всем рёбрам (вызывать периодически)."""
        self.decay.apply_decay()
        # Также применяем decay в KnowledgeGraph
        decayed = self.kg.decay_edges()
        logger.info(f"Decay cycle: {decayed} KG edges decayed, "
                    f"{len(self.decay._edges)} topic edges updated")

    def get_topic_stats(self, topic: str = None) -> dict:
        """Статистика по темам."""
        if topic:
            topic_id = self._normalize_topic(topic)
            edges = self.decay.get_effective_edges(topic_id)
            return {
                "topic": topic_id,
                "agents": len(edges),
                "top_agents": [
                    {"id": e.agent_id,
                     "weight": round(e.effective_weight, 3),
                     "success_rate": round(e.success_ratio, 3),
                     "load": round(self.lb.get_load_penalty(e.agent_id), 3)}
                    for e in edges[:5]
                ],
            }

        return {
            "total_topics": len(self._topic_index),
            "total_edges": len(self.decay._edges),
            "active_agents": len(self.lb._agent_loads),
            "stats": self._stats,
        }

    # ─── UTILS ───

    @staticmethod
    def _normalize_topic(topic: str) -> str:
        """Нормализовать топик в графовый ID."""
        return topic.lower().strip().replace(" ", ":").replace("/", ":")[:128]


# ═══════════════════════════════════════════════════════════════
# INTEGRATION: ContentRouter → ContentRouterGraph bridge
# ═══════════════════════════════════════════════════════════════

def create_content_router_graph(
    knowledge_graph: KnowledgeGraph,
) -> ContentRouterGraph:
    """Factory: создать ContentRouterGraph с готовым KnowledgeGraph."""
    return ContentRouterGraph(knowledge_graph)


def integrate_with_content_router(
    cr_graph: ContentRouterGraph,
    classification,  # ContentClassification
) -> Optional[OptimizedRoute]:
    """Интеграционный мост: ContentRouter.classify → ContentRouterGraph.optimize_route.

    Usage:
        classification = content_router.classify_event(event)
        route = integrate_with_content_router(cr_graph, classification)
        if route:
            deliver_to(route.agent_id, event)
    """
    if not classification or classification.confidence < 0.3:
        return None

    topic = classification.topic
    exclude = classification.recipients if hasattr(classification, 'recipients') else None

    return cr_graph.optimize_route(topic, exclude_agents=exclude)

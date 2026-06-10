#!/usr/bin/env python3
"""
Phase 11: Semantic Router — маршрутизация по смыслу, а не по топологии.

Слой над:
  - GraphMemory (Фаза 10): знает ЧТО узел знает
  - SmartRouter (Фаза 4): знает КАК дойти
  - KnowledgeGraph (Фазы 1-2): граф связей

Принцип:
  Пакет "BTC price feed" 
    → search_memory("BTC") → [(Node_A, 0.91), (Node_X, 0.62)]
    → SmartRouter.find_path(source, Node_A) → лучший маршрут
    → Доставка релевантному агенту

Методы:
  route_by_topic(payload, topic)     — найти эксперта + проложить путь
  register_expertise(node, topic)    — сохранить в память + граф
  find_experts(topic, top_k)         — кто разбирается в теме
  route_to_node(payload, target)     — прямая доставка
  broadcast_with_expertise(payload)  — всем экспертам по теме
  expertise_coverage()               — карта покрытия знаний
"""

import json
import logging
import time
from dataclasses import dataclass, field, asdict
from typing import Optional

import redis

from knowledge_graph import KnowledgeGraph
from graph_memory import GraphMemory, attach_memory_to_graph, MEMORY_TTL_DEFAULT
from smart_router import SmartRouter

logger = logging.getLogger("SemanticRouter")


# ─── Data Classes ──────────────────────────────────────

@dataclass
class TopicExpert:
    """Узел, компетентный в теме."""
    node_id: str
    topic: str
    score: float          # cosine similarity
    description: str = ""
    matched_key: str = ""  # ключ memory, по которому найден

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SemanticRoute:
    """Результат семантической маршрутизации."""
    topic: str
    source: str
    experts: list  # [TopicExpert, ...]
    selected_expert: Optional[str] = None
    path: list = field(default_factory=list)  # [hop1, hop2, ...]
    path_weight: float = 0.0
    path_transports: list = field(default_factory=list)
    ok: bool = False
    error: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["experts"] = [e.to_dict() for e in self.experts]
        return d


# ─── Semantic Router ───────────────────────────────────

class SemanticRouter:
    """Семантический маршрутизатор: тема → эксперт → путь."""

    # Ключи в памяти узла для регистрации экспертизы
    EXPERTISE_PREFIX = "expertise:"

    def __init__(self, kg: KnowledgeGraph, smart_router: SmartRouter,
                 gm: GraphMemory = None):
        """
        kg: KnowledgeGraph (Фазы 1-9)
        smart_router: SmartRouter (Фаза 4)  
        gm: GraphMemory (Фаза 10), если None — attach автоматически
        """
        self.kg = kg
        self.router = smart_router

        if gm is None:
            gm = attach_memory_to_graph(kg)
        self.gm = gm

        self._expertise_index: dict = {}  # token → set(node_id) — локальный кеш индекса

    # ─── Регистрация экспертизы ────────────────────────

    def register_expertise(self, node_id: str, topic: str, description: str,
                           tags: list = None) -> bool:
        """Зарегистрировать экспертизу узла в теме.

        Сохраняет в GraphMemory и KnowledgeGraph.
        
        Пример:
            router.register_expertise("agent_A", "BTC price feeds", 
                                       "Real-time Bitcoin price oracle from 5 exchanges")
        """
        # Узел должен существовать в графе
        node = self.kg.get_node(node_id)
        if not node:
            logger.warning(f"[SemanticRouter] Узел {node_id} не в графе — создаю")
            from knowledge_graph import GraphNode
            self.kg.upsert_node(GraphNode(
                node_id=node_id, node_type="agent",
                last_seen=time.time(), status="online"
            ))

        # Сохраняем в память
        key = f"{self.EXPERTISE_PREFIX}{topic}"
        entry = self.gm.set_memory(node_id, key, description)

        # Индексируем теги для быстрого поиска
        if tags:
            search_text = f"{topic} {' '.join(tags)} {description}"
            self.gm.set_memory(node_id, f"{self.EXPERTISE_PREFIX}_tags:{topic}",
                             search_text, ttl=MEMORY_TTL_DEFAULT)

        return entry is not None

    def unregister_expertise(self, node_id: str, topic: str) -> bool:
        """Удалить экспертизу узла."""
        key = f"{self.EXPERTISE_PREFIX}{topic}"
        return self.gm.forget(node_id, key)

    # ─── Поиск экспертов ───────────────────────────────

    def find_experts(self, topic: str, top_k: int = 5,
                     min_score: float = 0.10) -> list:
        """Найти узлы, компетентные в теме.

        Возвращает список TopicExpert, отсортированный по релевантности.
        """
        results = self.gm.search_memory(topic, top_k=top_k)

        experts = []
        for node_id, key, value, score in results:
            if score < min_score:
                continue
            # Пропускаем служебные _tags записи
            if "_tags:" in key:
                continue
            # Извлекаем чистую тему из ключа
            clean_topic = key.replace(self.EXPERTISE_PREFIX, "").replace(f"{self.EXPERTISE_PREFIX}_tags:", "")
            experts.append(TopicExpert(
                node_id=node_id,
                topic=clean_topic,
                score=score,
                description=value,
                matched_key=key,
            ))

        # Сортируем по score (уже отсортировано, но перестраховка)
        experts.sort(key=lambda e: -e.score)

        # Дополнительно: проверяем статус узла (онлайн?)
        online_experts = []
        for e in experts:
            node = self.kg.get_node(e.node_id)
            if node and node.status != "offline":
                # Штраф за degraded
                if node.status == "degraded":
                    e.score *= 0.5
                online_experts.append(e)

        online_experts.sort(key=lambda e: -e.score)
        return online_experts[:top_k]

    # ─── Маршрутизация ─────────────────────────────────

    def route_by_topic(self, source: str, topic: str, payload: str = "",
                       top_k: int = 3) -> SemanticRoute:
        """Проложить маршрут к лучшему эксперту по теме.

        Алгоритм:
          1. find_experts(topic) → список возможных получателей
          2. Для каждого — find_path(source, expert)
          3. Выбрать с минимальным весом пути × штраф за score
          4. Вернуть SemanticRoute

        Args:
            source: pubkey/node_id отправителя
            topic: тема запроса (напр. "BTC price feed")
            payload: содержимое пакета
            top_k: сколько экспертов рассматривать

        Returns:
            SemanticRoute с выбранным экспертом и путём
        """
        sr = SemanticRoute(
            topic=topic,
            source=source,
            experts=[],
            ok=False,
        )

        # Шаг 1: найти экспертов
        experts = self.find_experts(topic, top_k=top_k)
        if not experts:
            sr.error = f"No experts found for topic: {topic}"
            return sr

        sr.experts = experts

        # Шаг 2-3: для каждого эксперта — путь, выбрать лучший
        best_path = None
        best_expert = None
        best_weight = float("inf")

        for expert in experts:
            try:
                path = self.kg.find_path(source, expert.node_id)
                if not path:
                    # Пробуем fallback без фильтрации
                    path = self.kg.find_path_fallback(source, expert.node_id)
                if not path or len(path) < 2:  # минимум source→target
                    continue

                # Вес: длина пути + штраф за нерелевантность
                path_weight = self.kg.get_path_weight(path) if hasattr(self.kg, 'get_path_weight') else len(path) * 2
                relevance_penalty = (1.0 - expert.score) * 5.0
                composite = path_weight + relevance_penalty

                if composite < best_weight:
                    best_weight = composite
                    best_path = path
                    best_expert = expert

            except Exception as e:
                logger.debug(f"[SemanticRouter] Path to {expert.node_id} failed: {e}")
                continue

        # Шаг 4: результат
        if best_path and best_expert:
            sr.selected_expert = best_expert.node_id
            sr.path = best_path
            sr.path_weight = best_weight
            sr.ok = True
        else:
            sr.error = f"No reachable expert for topic: {topic}"

        return sr

    def route_to_node(self, source: str, target: str, payload: str = "") -> SemanticRoute:
        """Прямая доставка конкретному узлу (без поиска экспертов)."""
        sr = SemanticRoute(
            topic="direct",
            source=source,
            experts=[],
        )

        try:
            path = self.kg.find_path(source, target)
            if not path:
                path = self.kg.find_path_fallback(source, target)
            if path and len(path) >= 2:
                sr.selected_expert = target
                sr.path = path
                sr.path_weight = len(path) * 2
                sr.ok = True
            else:
                sr.error = f"No path from {source} to {target}"
        except Exception as e:
            sr.error = str(e)

        return sr

    def broadcast_with_expertise(self, source: str, topic: str,
                                 payload: str = "",
                                 top_k: int = 5) -> list:
        """Разослать пакет всем экспертам по теме.

        Возвращает список SemanticRoute — по одному на каждого достижимого эксперта.
        """
        experts = self.find_experts(topic, top_k=top_k)
        routes = []

        for expert in experts:
            sr = SemanticRoute(
                topic=topic,
                source=source,
                experts=[expert],
            )
            try:
                path = self.kg.find_path(source, expert.node_id)
                if not path:
                    path = self.kg.find_path_fallback(source, expert.node_id)
                if path and len(path) >= 2:
                    sr.selected_expert = expert.node_id
                    sr.path = path
                    sr.path_weight = len(path) * 2
                    sr.ok = True
            except Exception as e:
                sr.error = str(e)

            routes.append(sr)

        return routes

    # ─── Аналитика ──────────────────────────────────────

    def expertise_coverage(self) -> dict:
        """Карта покрытия: какие темы покрыты какими узлами."""
        coverage = {}
        for node_id in self.gm._discover_nodes_from_redis():
            self.gm._ensure_loaded(node_id)
            for key, entry in self.gm._entries.get(node_id, {}).items():
                if key.startswith(self.EXPERTISE_PREFIX) and not key.endswith("_tags"):
                    topic = key.replace(self.EXPERTISE_PREFIX, "")
                    coverage.setdefault(topic, []).append(node_id)

        return {
            "total_topics": len(coverage),
            "topics": coverage,
            "total_experts": len(set(n for nlist in coverage.values() for n in nlist)),
        }

    def export_state(self) -> dict:
        """Экспорт для снапшотов (расширяет export_state KG)."""
        return {
            "semantic_router": {
                "expertise_coverage": self.expertise_coverage(),
                "version": 11,
            }
        }


# ─── Factory ───────────────────────────────────────────

def create_semantic_router(kg: KnowledgeGraph, smart_router: SmartRouter,
                           redis_client: redis.Redis = None) -> SemanticRouter:
    """Создать SemanticRouter, привязанный к существующим KG и SmartRouter."""
    r = redis_client or kg.r
    gm = GraphMemory(r)
    attach_memory_to_graph(kg, r)

    # Привязать граф к SmartRouter (если ещё не привязан)
    if smart_router.graph is None:
        smart_router.graph = kg

    return SemanticRouter(kg, smart_router, gm)

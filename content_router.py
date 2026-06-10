#!/usr/bin/env python3
"""
Phase 12: Content Router — авто-классификация входящих постов и доставка через SemanticRouter.

Замыкает цикл: пост пришёл → классификация → эксперту.

Стек:
  SemanticRouter (P11) → знает КОМУ отправить
  ContentRouter (P12)  → знает О ЧЁМ пост
  
Методы:
  classify_event(event)        → topic, confidence
  route_event(event, source)   → классифицировать + маршрут
  register_expertise_batch()   → массовая регистрация компетенций
  extract_hashtags(event)      → извлечь хэштеги из тэгов
  match_keywords(text)         → keyword matching по expertise-ключам
  content_coverage()           → статистика покрытия контента
"""

import logging
from dataclasses import dataclass, field, asdict
from typing import Optional

from knowledge_graph import KnowledgeGraph
from graph_memory import GraphMemory, attach_memory_to_graph
from smart_router import SmartRouter
from semantic_router import SemanticRouter, TopicExpert, SemanticRoute, create_semantic_router

logger = logging.getLogger("ContentRouter")


@dataclass
class ContentClassification:
    """Результат классификации контента."""
    topic: str
    confidence: float        # 0..1
    method: str              # "hashtag", "keyword", "semantic", "unknown"
    matched_expertise: str = ""
    hashtags: list = field(default_factory=list)
    extracted_text: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RoutedContent:
    """Результат роутинга контента."""
    event_id: str
    classification: ContentClassification
    route: Optional[SemanticRoute] = None
    routed: bool = False
    error: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["classification"] = self.classification.to_dict()
        if self.route:
            d["route"] = self.route.to_dict()
        return d


class ContentRouter:
    """Контентный маршрутизатор: событие → тема → эксперт."""

    # Хэштег-маппинг: #btc → "BTC price feeds"
    HASHTAG_TOPIC_MAP: dict = {
        "btc": "BTC", "bitcoin": "BTC",
        "eth": "ETH", "ethereum": "ETH",
        "nostr": "Nostr",
        "mesh": "mesh",
        "defi": "DeFi",
        "lightning": "Lightning",
        "ln": "Lightning",
        "privacy": "Privacy",
        "pleb": "Pleb",
        "zap": "Zaps",
        "nwc": "NWC",
        "nip": "NIPs",
        "relay": "relays",
        "damus": "Damus",
        "primal": "Primal",
        "amethyst": "Amethyst",
        "snin": "SNIN",
        "sov": "SoV",
        "pow": "PoW",
        "mining": "Mining",
        "ai": "AI",
        "llm": "LLM",
        "ml": "ML",
        "p2p": "P2P",
        "dex": "DEX",
        "oracle": "oracle",
        "price": "price",
        "feed": "feed",
        "governance": "governance",
        "dao": "DAO",
    }

    def __init__(self, semantic_router: SemanticRouter, gm: GraphMemory = None):
        """
        semantic_router: SemanticRouter (Phase 11)
        gm: GraphMemory (Phase 10). Если None — из semantic_router.
        """
        self.sr = semantic_router
        self.kg = semantic_router.kg
        self.gm = gm or semantic_router.gm

        # Кеш keywords → из expertise
        self._expertise_cache: dict = {}
        self._expertise_cache_loaded = False
        self._load_expertise_cache()

        # Статистика
        self.stats = {
            "events_processed": 0,
            "events_routed": 0,
            "by_hashtag": 0,
            "by_keyword": 0,
            "by_semantic": 0,
            "unknown": 0,
        }

    # ─── Кеш экспертизы ────────────────────────────────

    def _load_expertise_cache(self):
        """Загрузить ключевые слова из зарегистрированной экспертизы.

        Стратегия:
          1. Из coverage (названия тем) → индексируем слова
          2. Из Redis HSET (содержимое expertise записей) → индексируем описание
        """
        if self._expertise_cache_loaded:
            return

        # Шаг 1: темы из coverage
        coverage = self.sr.expertise_coverage()
        for topic, nodes in coverage.get("topics", {}).items():
            topic_lower = topic.lower()
            self._expertise_cache[topic_lower] = {
                "topic": topic,
                "nodes": nodes,
            }
            for word in topic_lower.split():
                if len(word) >= 2 and word not in ("the", "and", "for", "via", "with",
                                                    "of", "in", "to", "on", "is", "at"):
                    self._expertise_cache.setdefault(word, {"topic": topic, "nodes": nodes})

        # Шаг 2: содержимое expertise — сканируем Redis HSET
        try:
            r = self.gm.r
            PREFIX = self.gm.KEY_PREFIX  # "graph:memory"
            for key_bytes in r.scan_iter(match=f"{PREFIX}:*".encode()):
                node_id = key_bytes.decode().split(f"{PREFIX}:")[1]
                # Получаем все поля HSET
                fields = r.hgetall(key_bytes)
                for field_k, field_v in fields.items():
                    k = field_k.decode() if isinstance(field_k, bytes) else field_k
                    if not k.startswith("expertise:") or "_tags" in k:
                        continue
                    # k = "expertise:BTC", извлекаем тему
                    topic = k.replace("expertise:", "")
                    v = field_v.decode() if isinstance(field_v, bytes) else field_v

                    # Парсим JSON чтобы получить value
                    try:
                        import json
                        entry = json.loads(v)
                        desc = entry.get("value", v)
                    except Exception:
                        desc = v

                    # Индексируем слова из описания
                    for word in str(desc).lower().split():
                        word = word.strip(".,;:!?\"'()[]{}")
                        if len(word) >= 3 and word not in ("the", "and", "for", "via",
                                                            "with", "of", "in", "to",
                                                            "on", "is", "at"):
                            self._expertise_cache.setdefault(
                                word, {"topic": topic, "nodes": [node_id]})
        except Exception as e:
            logger.debug(f"[ContentRouter] Redis scan for expertise cache: {e}")

        self._expertise_cache_loaded = True

    def refresh_cache(self):
        """Обновить кеш (после добавления новой экспертизы)."""
        self._expertise_cache = {}
        self._expertise_cache_loaded = False
        self._load_expertise_cache()

    # ─── Извлечение хэштегов ───────────────────────────

    def extract_hashtags(self, event: dict) -> list:
        """Извлечь хэштеги из Nostr события (kind 1)."""
        tags = event.get("tags", [])
        hashtags = []

        for tag in tags:
            if len(tag) >= 2 and tag[0] == "t":
                hashtag = tag[1].lower().strip("#")
                hashtags.append(hashtag)

        # Также ищем хэштеги в тексте
        content = event.get("content", "")
        import re
        content_hashtags = re.findall(r'#(\w+)', content)
        hashtags.extend(h.lower() for h in content_hashtags)

        return list(set(hashtags))  # уникальные

    # ─── Классификация ─────────────────────────────────

    def classify_event(self, event: dict) -> ContentClassification:
        """Классифицировать Nostr событие по теме.

        Стратегия (по убыванию приоритета):
          1. Хэштеги из tags → маппинг HASHTAG_TOPIC_MAP
          2. Ключевые слова в тексте → матчинг по expertise-ключам
          3. Семантический поиск по всему тексту
          4. "unknown" если ничего не найдено

        Returns:
            ContentClassification с topic, confidence, method
        """
        content = event.get("content", "")
        hashtags = self.extract_hashtags(event)

        # Обновляем кеш если нужно
        if not self._expertise_cache_loaded:
            self._load_expertise_cache()

        # Стратегия 1: хэштеги
        if hashtags:
            for h in hashtags:
                if h in self.HASHTAG_TOPIC_MAP:
                    topic = self.HASHTAG_TOPIC_MAP[h]
                    return ContentClassification(
                        topic=topic,
                        confidence=0.95,
                        method="hashtag",
                        hashtags=hashtags,
                        extracted_text=content[:200],
                    )

        # Стратегия 2: ключевые слова
        text_lower = content.lower()
        matched_topics: dict[str, int] = {}  # topic → hits

        for keyword, info in self._expertise_cache.items():
            if keyword in text_lower:
                topic = info["topic"]
                matched_topics[topic] = matched_topics.get(topic, 0) + 1

        if matched_topics:
            # Выбираем тему с наибольшим количеством совпадений
            best_topic = max(matched_topics, key=matched_topics.get)
            confidence = min(0.85, 0.40 + matched_topics[best_topic] * 0.15)
            return ContentClassification(
                topic=best_topic,
                confidence=confidence,
                method="keyword",
                matched_expertise=best_topic,
                hashtags=hashtags,
                extracted_text=content[:200],
            )

        # Стратегия 3: семантический поиск
        if content.strip():
            experts = self.sr.find_experts(content, top_k=1)
            if experts and experts[0].score > 0.20:
                return ContentClassification(
                    topic=experts[0].topic,
                    confidence=experts[0].score,
                    method="semantic",
                    matched_expertise=experts[0].matched_key,
                    hashtags=hashtags,
                    extracted_text=content[:200],
                )

        # Стратегия 4: unknown
        return ContentClassification(
            topic="unknown",
            confidence=0.0,
            method="unknown",
            hashtags=hashtags,
            extracted_text=content[:200],
        )

    # ─── Маршрутизация ─────────────────────────────────

    def route_event(self, event: dict, source: str = None) -> RoutedContent:
        """Классифицировать и маршрутизировать событие.

        Полный цикл:
          1. classify_event(event) → тема
          2. SemanticRouter.route_by_topic(source, topic) → маршрут
          3. RoutedContent с полной информацией

        Args:
            event: Nostr событие (kind, content, tags, id, pubkey)
            source: pubkey отправителя. Если None — event["pubkey"]

        Returns:
            RoutedContent с классификацией и маршрутом
        """
        event_id = event.get("id", event.get("event_id", "unknown"))

        if source is None:
            source = event.get("pubkey", "anonymous")

        self.stats["events_processed"] += 1

        # Шаг 1: классификация
        classification = self.classify_event(event)

        rc = RoutedContent(
            event_id=event_id,
            classification=classification,
        )

        # Шаг 2: маршрутизация (если тема известна)
        if classification.topic != "unknown":
            # Комбинируем тему и контент для лучшего семантического поиска
            rich_query = f"{classification.topic} {event.get('content', '')}"[:500]
            route = self.sr.route_by_topic(
                source=source,
                topic=rich_query,
                payload=event.get("content", ""),
                top_k=3,
            )
            rc.route = route
            rc.routed = route.ok
            if route.ok:
                self.stats["events_routed"] += 1
                self.stats[f"by_{classification.method}"] += 1
            else:
                rc.error = route.error
        else:
            self.stats["unknown"] += 1
            rc.error = "Unknown topic — no routing"

        return rc

    def route_event_broadcast(self, event: dict, source: str = None) -> list:
        """Разослать событие всем экспертам по теме (broadcast)."""
        if source is None:
            source = event.get("pubkey", "anonymous")

        classification = self.classify_event(event)
        if classification.topic == "unknown":
            return []

        routes = self.sr.broadcast_with_expertise(
            source=source,
            topic=classification.topic,
            payload=event.get("content", ""),
        )

        self.stats["events_processed"] += 1
        self.stats["events_routed"] += len([r for r in routes if r.ok])
        return routes

    # ─── Массовая регистрация ──────────────────────────

    def register_expertise_batch(self, expertise_map: dict, refresh: bool = True):
        """Массовая регистрация экспертизы.

        Args:
            expertise_map: {node_id: [(topic, description, [tags]), ...]}
            refresh: обновить кеш после регистрации
        """
        total = 0
        for node_id, items in expertise_map.items():
            for item in items:
                topic, description = item[0], item[1]
                tags = item[2] if len(item) > 2 else None
                self.sr.register_expertise(node_id, topic, description, tags)
                total += 1

        if refresh:
            self.refresh_cache()

        logger.info(f"[ContentRouter] Registered {total} expertise items for {len(expertise_map)} nodes")

    # ─── Статистика ────────────────────────────────────

    def get_stats(self) -> dict:
        """Статистика и покрытие."""
        return {
            **self.stats,
            "expertise_cache_size": len(self._expertise_cache),
            "coverage": self.sr.expertise_coverage(),
            "hashtag_map_size": len(self.HASHTAG_TOPIC_MAP),
        }

    def content_coverage(self) -> dict:
        """Покрытие: какие темы покрыты хэштегами и какие нет."""
        covered_hashtags = set(self.HASHTAG_TOPIC_MAP.keys())
        expertise_topics = set(
            t.lower() for t in self.sr.expertise_coverage().get("topics", {}).keys()
        )

        # Темы с хэштегами, но без экспертов
        uncovered_expertise = covered_hashtags - expertise_topics
        # Экспертиза без хэштегов
        unhashed_expertise = expertise_topics - covered_hashtags

        return {
            "hashtag_covered_topics": len(covered_hashtags),
            "expertise_topics": len(expertise_topics),
            "hashtags_without_experts": list(uncovered_expertise),
            "expertise_without_hashtags": list(unhashed_expertise),
            "full_coverage": covered_hashtags & expertise_topics,
        }

    def export_state(self) -> dict:
        """Экспорт для снапшотов."""
        return {
            "content_router": {
                "stats": self.stats,
                "version": 12,
            }
        }


# ─── Ностр-адаптер ────────────────────────────────────

def nostr_event_from_post(content: str, pubkey: str = None,
                          event_id: str = None, tags: list = None,
                          kind: int = 1) -> dict:
    """Создать событие в формате Nostr из поста (для тестов)."""
    return {
        "id": event_id or f"test:{hash(content) & 0xFFFFFFFF:08x}",
        "pubkey": pubkey or "test_pubkey",
        "kind": kind,
        "content": content,
        "tags": tags or [],
        "created_at": 0,
    }


def create_content_router(semantic_router: SemanticRouter) -> ContentRouter:
    """Factory: создать ContentRouter из SemanticRouter."""
    return ContentRouter(semantic_router)

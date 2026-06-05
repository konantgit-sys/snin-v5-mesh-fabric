"""
marketplace_registry.py — Avito-подобный маркетплейс для агентов (Фаза 6b)

Каждый агент — самостоятельная сущность со своими ключами и маршрутами.
Агент представляет человека и его экономические интересы.

API агента:
  kind='register_marketplace' — зарегистрировать offers/wants
  kind='marketplace_search'   — найти matching агентов
  kind='marketplace_connect'  — запросить связь с агентом

Двусторонний matching:
  - Прямой: query совпадает с offers агента
  - Обратный: query совпадает с wants агента (я ищу то же, что и ты = потенциальный партнёр)
  - Похожие: offers одного агента совпадают с wants другого

Скоринг:
  - keyword match (вес 0.5)
  - category match (вес 0.3)
  - TF-IDF semantic (вес 0.2)
"""

import re
import time
from collections import defaultdict
from typing import Optional

# ═══ Категории маркетплейса ═══
MARKETPLACE_CATEGORIES = {
    "auto": ["машина", "авто", "автомобиль", "car", "toyota", "bmw", "мерседес", "продам авто", "куплю авто",
             "vehicle", "motorcycle", "мотоцикл", "гараж"],
    "real_estate": ["квартира", "дом", "недвижимость", "apartment", "house", "rent", "аренда",
                    "продам квартиру", "куплю квартиру", "дача", "участок", "офис", "коммерческая",
                    "real estate", "property", "жильё"],
    "services": ["услуги", "ремонт", "сантехник", "электрик", "уборка", "доставка", "такси",
                 "service", "plumber", "electrician", "cleaning", "delivery", "разработка", "дизайн",
                 "developer", "development", "software", "консультация", "consulting"],
    "jobs": ["работа", "вакансия", "резюме", "job", "hire", "найм", "сотрудник", "фриланс",
             "зарплата", "специалист", "требуется", "freelance", "career"],
    "tenders": ["тендер", "госзакупки", "контракт", "tender", "government", "поставщик",
                "закупка", "конкурс", "аукцион", "госзаказ"],
    "finance": ["инвестиции", "кредит", "займ", "invest", "loan", "инвестор", "стартап",
                "краудфандинг", "токен", "token", "финансы", "страхование", "startup",
                "funding", "capital", "платёж", "payment", "lightning", "sats"],
    "advertising": ["реклама", "продвижение", "маркетинг", "ad", "promotion", "seo",
                    "таргет", "smm", "трафик", "лиды", "рассылка", "marketing", "growth"],
    "education": ["обучение", "курсы", "репетитор", "tutor", "course", "учитель",
                  "английский", "программирование", "ментор", "наставник", "education",
                  "training", "learning"],
    "ai_agents": ["agent", "агент", "multi-agent", "оркестрация", "orchestration", "llm",
                  "autonomous", "автономный", "memory", "память", "reasoning", "tool-use",
                  "framework", "sdk", "rag", "embedding", "semantic", "семантический",
                  "ai", "ии", "искусственный интеллект", "machine learning", "ml"],
    "content": ["контент", "content", "пост", "post", "аналитика", "analytics", "тренд",
                "trend", "nostr", "telegram", "блог", "blog", "писатель", "writer",
                "текст", "статья", "article", "креатив", "creative"],
    "other": [],  # catch-all
}


class MarketplaceRegistry:
    """Маркетплейс агентов: offers ↔ wants matching."""

    def __init__(self):
        # agent_id → {offers, wants, contact, category, registered_at, tags}
        self._agents: dict[str, dict] = {}
        # keyword → set of agent_ids (для быстрого поиска)
        self._offer_index: dict[str, set[str]] = defaultdict(set)
        self._want_index: dict[str, set[str]] = defaultdict(set)
        # category → set of agent_ids
        self._category_index: dict[str, set[str]] = defaultdict(set)
        # TF-IDF structures
        self._corpus: list[str] = []
        self._corpus_ids: list[str] = []
        self._vectorizer = None
        self._tfidf_matrix = None
        self._dirty = True

    # ═══ Registration ═══

    def register(self, agent_id: str, offers: list[str],
                 wants: list[str], contact: str = "",
                 pubkey: str = "") -> bool:
        """Зарегистрировать агента в маркетплейсе."""
        is_new = agent_id not in self._agents

        # Удаляем старые индексы если агент уже был
        if not is_new:
            self._remove_from_indexes(agent_id)

        # Определяем категорию
        all_text = " ".join(offers + wants).lower()
        category = self._detect_category(all_text)

        self._agents[agent_id] = {
            "offers": [o.strip().lower() for o in offers if o.strip()],
            "wants": [w.strip().lower() for w in wants if w.strip()],
            "contact": contact.strip(),
            "pubkey": pubkey.strip(),
            "category": category,
            "tags": self._extract_tags(offers + wants),
            "registered_at": time.time(),
            "updated_at": time.time(),
        }

        # Индексируем
        self._add_to_indexes(agent_id)
        self._dirty = True

        return is_new

    def unregister(self, agent_id: str):
        """Удалить агента."""
        if agent_id not in self._agents:
            return
        self._remove_from_indexes(agent_id)
        del self._agents[agent_id]
        self._dirty = True

    # ═══ Search ═══

    def search(self, query: str, top_k: int = 10,
               category: str = None) -> list[dict]:
        """Поиск по маркетплейсу.
        
        Returns: [{agent_id, offers, wants, contact, score, match_type, matched_text}]
        """
        if not self._agents:
            return []

        query_lower = query.lower().strip()
        results: dict[str, dict] = {}

        # 1. Keyword match по offers (прямой: кто продаёт то, что я ищу)
        self._match_keywords(query_lower, self._offer_index, "offer_match", results)

        # 2. Keyword match по wants (обратный: кто ищет то же, что и я)
        self._match_keywords(query_lower, self._want_index, "want_match", results)

        # 3. Category boost
        if category:
            for agent_id in self._category_index.get(category, set()):
                if agent_id in self._agents:
                    r = results.setdefault(agent_id, {"agent_id": agent_id, "score": 0, "match_types": [], "matched": []})
                    r["score"] += 0.3
                    r["match_types"].append("category")

        # 4. TF-IDF semantic (опционально)
        tfidf_scores = self._tfidf_search(query_lower)
        for agent_id, score in tfidf_scores.items():
            r = results.setdefault(agent_id, {"agent_id": agent_id, "score": 0, "match_types": [], "matched": []})
            r["score"] += score * 0.2
            r["match_types"].append("semantic")

        # Оборачиваем результаты
        output = []
        for agent_id, r in results.items():
            agent = self._agents.get(agent_id)
            if not agent:
                continue
            # Фильтруем: хотя бы какой-то keyword match
            if r["score"] < 0.15 and "semantic" in r.get("match_types", []):
                continue  # только semantic без keyword — слабый сигнал
            output.append({
                "agent_id": agent_id,
                "offers": agent["offers"],
                "wants": agent["wants"],
                "contact": agent["contact"],
                "category": agent["category"],
                "score": round(r["score"], 3),
                "match_types": list(set(r.get("match_types", []))),
                "matched": list(set(r.get("matched", [])))[:5],
            })

        # Сортировка и top_k
        output.sort(key=lambda x: x["score"], reverse=True)

        # Фильтр по категории
        if category:
            output = [o for o in output if o["category"] == category]

        return output[:top_k]

    # ═══ Keyword matching ═══

    def _match_keywords(self, query: str, index: dict, match_type: str,
                        results: dict):
        """Поиск по keyword-индексу."""
        tokens = self._tokenize(query)
        for token in tokens:
            if token in index:
                for agent_id in index[token]:
                    r = results.setdefault(agent_id, {
                        "agent_id": agent_id, "score": 0, "match_types": [], "matched": []
                    })
                    r["score"] += 0.5 / len(tokens)
                    r["match_types"].append(match_type)
                    r["matched"].append(token)

    # ═══ TF-IDF ═══

    def _tfidf_search(self, query: str) -> dict[str, float]:
        """Semantic search через TF-IDF."""
        try:
            self._rebuild_tfidf()
        except Exception:
            return {}
        if self._vectorizer is None or self._tfidf_matrix is None:
            return {}
        try:
            query_vec = self._vectorizer.transform([query])
            from sklearn.metrics.pairwise import cosine_similarity
            sims = cosine_similarity(query_vec, self._tfidf_matrix)[0]
            scores = {}
            for i, agent_id in enumerate(self._corpus_ids):
                if sims[i] > 0.1:
                    scores[agent_id] = float(sims[i])
            return scores
        except Exception:
            return {}

    def _rebuild_tfidf(self):
        """Перестроить TF-IDF матрицу."""
        if not self._dirty:
            return
        self._corpus = []
        self._corpus_ids = []
        for agent_id, info in self._agents.items():
            text = " ".join(info["offers"] + info["wants"]) + " " + info["category"]
            if text.strip():
                self._corpus.append(text)
                self._corpus_ids.append(agent_id)
        if len(self._corpus) >= 2:
            try:
                from sklearn.feature_extraction.text import TfidfVectorizer
                self._vectorizer = TfidfVectorizer(max_features=500, ngram_range=(1, 2),
                                                   stop_words=None, lowercase=True)
                self._tfidf_matrix = self._vectorizer.fit_transform(self._corpus)
            except Exception:
                self._vectorizer = None
                self._tfidf_matrix = None
        self._dirty = False

    # ═══ Category detection ═══

    def _detect_category(self, text: str) -> str:
        """Определить категорию по тексту."""
        scores = {}
        for cat, keywords in MARKETPLACE_CATEGORIES.items():
            score = 0
            for kw in keywords:
                if kw in text:
                    score += 1
            if score > 0:
                scores[cat] = score
        if scores:
            return max(scores, key=scores.get)
        return "other"

    # ═══ Index management ═══

    def _add_to_indexes(self, agent_id: str):
        agent = self._agents[agent_id]
        for offer in agent["offers"]:
            for token in self._tokenize(offer):
                self._offer_index[token].add(agent_id)
        for want in agent["wants"]:
            for token in self._tokenize(want):
                self._want_index[token].add(agent_id)
        if agent["category"]:
            self._category_index[agent["category"]].add(agent_id)

    def _remove_from_indexes(self, agent_id: str):
        agent = self._agents.get(agent_id)
        if not agent:
            return
        for offer in agent["offers"]:
            for token in self._tokenize(offer):
                self._offer_index[token].discard(agent_id)
        for want in agent["wants"]:
            for token in self._tokenize(want):
                self._want_index[token].discard(agent_id)
        if agent["category"]:
            self._category_index[agent["category"]].discard(agent_id)

    # ═══ Helpers ═══

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        """Токенизация с поддержкой русского и английского."""
        return re.findall(r'[a-zа-яё0-9_]+', text.lower())

    @staticmethod
    def _extract_tags(texts: list[str]) -> list[str]:
        """Извлечь значимые теги из текстов."""
        tags = set()
        for text in texts:
            words = re.findall(r'[a-zа-яё]{3,}', text.lower())
            # Фильтруем стоп-слова
            stop = {"это", "для", "что", "как", "the", "and", "for", "with", "есть",
                    "еще", "уже", "очень", "можно", "надо", "буду", "будет"}
            for w in words:
                if w not in stop:
                    tags.add(w)
        return list(tags)[:20]

    # ═══ Stats ═══

    @property
    def stats(self) -> dict:
        cats = defaultdict(int)
        for a in self._agents.values():
            cats[a["category"]] += 1
        return {
            "total_agents": len(self._agents),
            "categories": dict(cats),
            "total_offers": sum(len(a["offers"]) for a in self._agents.values()),
            "total_wants": sum(len(a["wants"]) for a in self._agents.values()),
            "unique_keywords": len(self._offer_index) + len(self._want_index),
        }

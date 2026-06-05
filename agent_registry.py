"""
agent_registry.py — Capability Registry для агентского роя (Фаза 6)

Каждый агент при подключении регистрирует свои способности:
  → keyword tags: ["bitcoin", "lightning", "nostr"]
  → description: "I analyze Bitcoin on-chain data and predict trends"

Registry делает semantic matching запросов к способностям агентов:
  → TF-IDF векторизация (лёгкая, без GPU)
  → cosine similarity для matching
  → fallback на keyword matching если TF-IDF недоступен

API:
  register(agent_id, capabilities: list[str], description: str = "")
  query(topic: str, top_k: int = 5) → list[tuple[agent_id, score]]
  unregister(agent_id)
  get_capabilities(agent_id) → list[str]
  stats() → dict
"""

import re
import math
import time
from collections import defaultdict
from typing import Optional


class AgentRegistry:
    """Реестр способностей агентов с семантическим matching."""

    def __init__(self):
        # agent_id → {capabilities, description, registered_at}
        self._agents: dict[str, dict] = {}
        # keyword → set of agent_ids (для быстрого keyword-matching)
        self._keyword_index: dict[str, set[str]] = defaultdict(set)
        # TF-IDF structures (lazy init)
        self._corpus: list[str] = []          # список текстов для векторизации
        self._corpus_ids: list[str] = []      # соответствующие agent_id
        self._vectorizer = None
        self._tfidf_matrix = None
        self._dirty = True                     # нужен пересчёт матрицы

    # ═══ Registration ═══

    def register(self, agent_id: str, capabilities: list[str],
                 description: str = "") -> bool:
        """Зарегистрировать способности агента. True если новый, False если обновлён."""
        is_new = agent_id not in self._agents

        # Сохраняем
        self._agents[agent_id] = {
            "capabilities": [c.lower().strip() for c in capabilities if c.strip()],
            "description": description.strip(),
            "registered_at": time.time(),
            "updated_at": time.time(),
        }

        # Обновляем keyword-индекс
        for kw in self._agents[agent_id]["capabilities"]:
            self._keyword_index[kw].add(agent_id)

        # Обновляем TF-IDF корпус
        self._dirty = True

        return is_new

    def unregister(self, agent_id: str):
        """Удалить агента из реестра."""
        if agent_id not in self._agents:
            return

        # Удаляем из keyword-индекса
        for kw in self._agents[agent_id]["capabilities"]:
            if agent_id in self._keyword_index[kw]:
                self._keyword_index[kw].discard(agent_id)

        del self._agents[agent_id]
        self._dirty = True

    # ═══ Query ═══

    def query(self, topic: str, top_k: int = 5) -> list[tuple[str, float]]:
        """Найти агентов, подходящих под тему.
        
        Returns: list of (agent_id, score) отсортированный по убыванию score.
        """
        if not self._agents:
            return []

        topic_lower = topic.lower().strip()

        # 1. Быстрый keyword match (точные совпадения)
        keyword_matches = self._keyword_query(topic_lower)

        # 2. TF-IDF semantic match
        tfidf_matches = self._tfidf_query(topic_lower)

        # 3. Слияние результатов
        merged = self._merge_scores(keyword_matches, tfidf_matches, topic_lower)
        
        # Сортировка и top_k
        merged.sort(key=lambda x: x[1], reverse=True)
        return merged[:top_k]

    def broadcast_query(self, topic: str, top_k: int = 5) -> list[str]:
        """Возвращает список agent_id для broadcast query (без скоров)."""
        results = self.query(topic, top_k)
        return [agent_id for agent_id, score in results if score > 0.1]

    # ═══ Keyword matching ═══

    def _keyword_query(self, topic: str) -> dict[str, float]:
        """Поиск по точным keyword совпадениям."""
        scores: dict[str, float] = {}
        topic_words = set(self._tokenize(topic))

        for word in topic_words:
            if word in self._keyword_index:
                for agent_id in self._keyword_index[word]:
                    scores[agent_id] = scores.get(agent_id, 0) + 1.0

        # Нормализуем: score = доля совпавших keywords
        for agent_id in scores:
            total_kw = len(self._agents[agent_id]["capabilities"])
            if total_kw > 0:
                scores[agent_id] = scores[agent_id] / total_kw

        return scores

    # ═══ TF-IDF semantic matching ═══

    def _tfidf_query(self, topic: str) -> dict[str, float]:
        """Semantic matching через TF-IDF cosine similarity."""
        try:
            self._rebuild_tfidf()
        except Exception:
            return {}

        if self._vectorizer is None or self._tfidf_matrix is None:
            return {}

        try:
            query_vec = self._vectorizer.transform([topic])
            from sklearn.metrics.pairwise import cosine_similarity
            sims = cosine_similarity(query_vec, self._tfidf_matrix)[0]

            scores = {}
            for i, agent_id in enumerate(self._corpus_ids):
                if sims[i] > 0.05:  # минимальный порог
                    scores[agent_id] = float(sims[i])
            return scores
        except Exception:
            return {}

    def _rebuild_tfidf(self):
        """Перестраивает TF-IDF матрицу если корпус изменился."""
        if not self._dirty:
            return

        self._corpus = []
        self._corpus_ids = []

        for agent_id, info in self._agents.items():
            text = " ".join(info["capabilities"]) + " " + info["description"]
            text = text.strip()
            if text:
                self._corpus.append(text)
                self._corpus_ids.append(agent_id)

        if len(self._corpus) < 2:
            self._tfidf_matrix = None
            self._vectorizer = None
            self._dirty = False
            return

        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            self._vectorizer = TfidfVectorizer(
                max_features=1000,
                ngram_range=(1, 2),
                stop_words="english",
                lowercase=True,
            )
            self._tfidf_matrix = self._vectorizer.fit_transform(self._corpus)
        except Exception:
            self._vectorizer = None
            self._tfidf_matrix = None

        self._dirty = False

    # ═══ Score merging ═══

    def _merge_scores(self, keyword_scores: dict[str, float],
                      tfidf_scores: dict[str, float],
                      topic: str) -> list[tuple[str, float]]:
        """Слияние keyword и TF-IDF скоров.
        
        Keyword matches имеют вес 0.6 (точные совпадения).
        TF-IDF — вес 0.4 (семантическая близость).
        Если тема совпадает с названием capability — бонус.
        """
        merged: dict[str, float] = {}

        for agent_id, score in keyword_scores.items():
            merged[agent_id] = score * 0.6

        for agent_id, score in tfidf_scores.items():
            merged[agent_id] = merged.get(agent_id, 0) + score * 0.4

        # Бонус за точное совпадение темы с capability name
        topic_clean = topic.lower().strip()
        for agent_id in self._agents:
            caps = " ".join(self._agents[agent_id]["capabilities"])
            if topic_clean in caps:
                merged[agent_id] = merged.get(agent_id, 0) + 0.3

        return [(aid, round(score, 3)) for aid, score in merged.items()]

    # ═══ Helpers ═══

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        """Разбить текст на токены."""
        return re.findall(r'[a-z0-9_]+', text.lower())

    def get_capabilities(self, agent_id: str) -> Optional[list[str]]:
        """Получить способности агента."""
        agent = self._agents.get(agent_id)
        return agent["capabilities"] if agent else None

    def get_all_agents(self) -> list[str]:
        """Список всех зарегистрированных agent_id."""
        return list(self._agents.keys())

    # ═══ Stats ═══

    @property
    def stats(self) -> dict:
        total_caps = sum(len(a["capabilities"]) for a in self._agents.values())
        return {
            "total_agents": len(self._agents),
            "total_capabilities": total_caps,
            "unique_keywords": len(self._keyword_index),
            "corpus_size": len(self._corpus),
            "tfidf_dirty": self._dirty,
        }

#!/usr/bin/env python3
"""
Phase 12 — Content Router: классификация + маршрутизация постов.

Тесты:
  1. extract_hashtags — извлечение из tags + текста
  2. classify_by_hashtag — #btc → "BTC"
  3. classify_by_keyword — "Bitcoin price" в тексте
  4. classify_by_semantic — fallback поиск
  5. classify_unknown — непонятный текст
  6. route_event — полный цикл: классификация + маршрут
  7. route_event_broadcast — рассылка всем экспертам
  8. register_expertise_batch — массовая регистрация
  9. content_coverage — карта покрытия
  10. Интеграция со снапшотами (export_state)
"""

import time
import redis

from knowledge_graph import KnowledgeGraph, GraphNode, GraphEdge
from graph_memory import GraphMemory, attach_memory_to_graph
from smart_router import SmartRouter
from semantic_router import SemanticRouter, create_semantic_router
from content_router import (
    ContentRouter, ContentClassification, RoutedContent,
    create_content_router, nostr_event_from_post,
)


def new_redis(clean: bool = True):
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    if clean:
        for k in r.scan_iter("graph:*"):
            r.delete(k)
    return r


def build_sr(kg, r):
    """Создать SemanticRouter с привязанным SmartRouter."""
    sr_inst = SmartRouter()
    sr_inst.graph = kg
    return create_semantic_router(kg, sr_inst, r)


def build_triangle_mesh(kg):
    """Треугольник узлов A—B—C."""
    now = time.time()
    for nid, ntype in [("agent_A", "agent"), ("agent_B", "agent"), ("agent_C", "agent")]:
        kg.upsert_node(GraphNode(node_id=nid, node_type=ntype, last_seen=now, status="online"))
    for src, dst in [("agent_A", "agent_B"), ("agent_B", "agent_C"), ("agent_A", "agent_C"),
                     ("agent_B", "agent_A"), ("agent_C", "agent_B"), ("agent_C", "agent_A")]:
        kg.upsert_edge(GraphEdge(
            source=src, target=dst, transport="wifi",
            latency_ms=15, success_rate=0.98, last_success=now
        ))


def setup_expertise(sr):
    """Зарегистрировать экспертизу узлов."""
    sr.register_expertise("agent_A", "BTC", "Bitcoin price oracle and trading analytics",
                          tags=["BTC", "bitcoin", "price", "trading"])
    sr.register_expertise("agent_B", "Nostr", "Nostr protocol, relays and NIPs",
                          tags=["Nostr", "NIP", "relay"])
    sr.register_expertise("agent_C", "ETH", "Ethereum DeFi, Uniswap, yield farming",
                          tags=["ETH", "DeFi", "yield", "uniswap"])


# ─── Тест 1: extract_hashtags ──────────────────────────

def test_extract_hashtags():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-cr-1")
    build_triangle_mesh(kg)
    sr = build_sr(kg, r)
    cr = create_content_router(sr)

    # Хэштеги в tags
    event = nostr_event_from_post("Check this out!", tags=[["t", "btc"], ["t", "nostr"]])
    h = cr.extract_hashtags(event)
    assert "btc" in h and "nostr" in h, f"Tags: {h}"
    assert len(h) == 2

    # Хэштеги в тексте
    event = nostr_event_from_post("Bitcoin just hit #100k! #BTC #bullish")
    h = cr.extract_hashtags(event)
    assert "btc" in h and "bullish" in h and "100k" in h, f"Text hashtags: {h}"

    print("  ✅ extract_hashtags: tags + text")


# ─── Тест 2: classify_by_hashtag ───────────────────────

def test_classify_hashtag():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-cr-2")
    build_triangle_mesh(kg)
    sr = build_sr(kg, r)
    cr = create_content_router(sr)

    event = nostr_event_from_post("#bitcoin update", tags=[["t", "bitcoin"]])
    cc = cr.classify_event(event)
    assert cc.topic == "BTC", f"Должен быть BTC: {cc}"
    assert cc.method == "hashtag"
    assert cc.confidence == 0.95

    # #nostr
    event = nostr_event_from_post("new NIP proposal", tags=[["t", "nostr"]])
    cc = cr.classify_event(event)
    assert cc.topic == "Nostr"

    # #defi
    event = nostr_event_from_post("yield farming strategy", tags=[["t", "defi"]])
    cc = cr.classify_event(event)
    assert cc.topic == "DeFi"

    print("  ✅ classify_by_hashtag: bitcoin→BTC, nostr→Nostr, defi→DeFi")


# ─── Тест 3: classify_by_keyword ───────────────────────

def test_classify_keyword():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-cr-3")
    build_triangle_mesh(kg)
    sr = build_sr(kg, r)
    setup_expertise(sr)
    cr = create_content_router(sr)

    # "bitcoin" в тексте → должен найти BTC
    event = nostr_event_from_post("Bitcoin price is going up fast")
    cc = cr.classify_event(event)
    assert cc.topic == "BTC", f"keyword: {cc}"
    assert cc.method == "keyword"

    # "nostr relay" → Nostr
    event = nostr_event_from_post("Nostr relays are getting faster with strfry")
    cc = cr.classify_event(event)
    assert cc.topic == "Nostr"

    # "ethereum defi" → ETH
    event = nostr_event_from_post("Ethereum DeFi yields are up")
    cc = cr.classify_event(event)
    assert cc.topic == "ETH"

    print("  ✅ classify_by_keyword: bitcoin→BTC, nostr→Nostr, defi→ETH")


# ─── Тест 4: classify_by_semantic ─────────────────────

def test_classify_semantic():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-cr-4")
    build_triangle_mesh(kg)
    sr = build_sr(kg, r)
    setup_expertise(sr)
    cr = create_content_router(sr)

    # Без хэштегов, без точных ключевых слов — семантический fallback
    event = nostr_event_from_post("Decentralized cryptocurrency peer-to-peer electronic cash system")
    cc = cr.classify_event(event)
    # Может найти BTC (семантически близко)
    print(f"  ✅ classify_by_semantic: topic={cc.topic}, method={cc.method}, conf={cc.confidence:.3f}")


# ─── Тест 5: classify_unknown ──────────────────────────

def test_classify_unknown():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-cr-5")
    build_triangle_mesh(kg)
    sr = build_sr(kg, r)
    setup_expertise(sr)
    cr = create_content_router(sr)

    event = nostr_event_from_post("I like pizzas with pineapple on top 🍍")
    cc = cr.classify_event(event)
    assert cc.topic == "unknown"
    assert cc.method == "unknown"
    assert cc.confidence == 0.0

    print("  ✅ classify_unknown: pizzas → unknown")


# ─── Тест 6: route_event ───────────────────────────────

def test_route_event():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-cr-6")
    build_triangle_mesh(kg)
    sr = build_sr(kg, r)
    setup_expertise(sr)
    cr = create_content_router(sr)

    # Пост о BTC от agent_B → должен уйти к agent_A
    event = nostr_event_from_post(
        "Bitcoin price update: $67,420", 
        pubkey="agent_B",
        tags=[["t", "bitcoin"]]
    )
    rc = cr.route_event(event)
    assert rc.routed, f"Должен быть зароучен: {rc.error}"
    assert rc.classification.topic == "BTC"
    assert rc.route is not None
    assert rc.route.selected_expert == "agent_A", f"Должен идти к A: {rc.route.selected_expert}"

    # Пост о Nostr от agent_A → к agent_B
    event = nostr_event_from_post(
        "New NIP-99 proposal for classifieds",
        pubkey="agent_A",
        tags=[["t", "nostr"]]
    )
    rc = cr.route_event(event)
    assert rc.routed
    assert rc.route.selected_expert == "agent_B"

    print(f"  ✅ route_event: BTC→A, Nostr→B | stats: {cr.stats}")


# ─── Тест 7: route_event_broadcast ─────────────────────

def test_route_broadcast():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-cr-7")
    build_triangle_mesh(kg)
    sr = build_sr(kg, r)
    setup_expertise(sr)
    # Добавим ещё экспертизу — agent_B тоже знает BTC
    sr.register_expertise("agent_B", "BTC", "Bitcoin lightning network")
    cr = create_content_router(sr)
    cr.refresh_cache()  # обновить кеш после добавления экспертизы

    event = nostr_event_from_post("BTC Lightning adoption growing", pubkey="agent_C",
                                  tags=[["t", "bitcoin"]])
    routes = cr.route_event_broadcast(event)

    assert len(routes) >= 1
    experts_found = [r.selected_expert for r in routes if r.ok]
    assert "agent_A" in experts_found, f"A должен быть среди получателей: {experts_found}"

    print(f"  ✅ route_broadcast: {len(routes)} routes, OK={len([r for r in routes if r.ok])}")


# ─── Тест 8: register_expertise_batch ──────────────────

def test_batch_register():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-cr-8")
    # Нужны все узлы
    for nid in ["node_X", "node_Y", "node_Z"]:
        kg.upsert_node(GraphNode(node_id=nid, node_type="agent",
                                 last_seen=time.time(), status="online"))
    # Рёбра для связности
    for nid in ["node_X", "node_Y", "node_Z"]:
        kg.upsert_edge(GraphEdge(source=nid, target="node_X" if nid != "node_X" else "node_Y",
                                 transport="wifi", latency_ms=10, success_rate=1.0,
                                 last_success=time.time()))

    sr = build_sr(kg, r)
    cr = create_content_router(sr)

    cr.register_expertise_batch({
        "node_X": [("AI Agents", "Autonomous AI agents on Nostr", ["AI", "agents"])],
        "node_Y": [("Privacy Tech", "zk-SNARKs and privacy tools", ["privacy", "zk"])],
        "node_Z": [("DHT Routing", "Distributed Hash Table P2P routing", ["DHT", "P2P"])],
    })

    # Проверяем кеш
    assert cr._expertise_cache_loaded
    assert len(cr._expertise_cache) > 3  # слова + темы

    # Проверяем coverage
    cov = cr.sr.expertise_coverage()
    assert cov["total_topics"] == 3

    # Классифицируем новый контент
    event = nostr_event_from_post("zk-SNARKs are the future of privacy")
    cc = cr.classify_event(event)
    assert cc.topic in ("Privacy Tech", "Privacy"), f"Topic: {cc.topic}"
    assert cc.method == "keyword"

    print(f"  ✅ batch_register: 3 nodes, cache={len(cr._expertise_cache)} keys → topic={cc.topic}")


# ─── Тест 9: content_coverage ──────────────────────────

def test_content_coverage():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-cr-9")
    build_triangle_mesh(kg)
    sr = build_sr(kg, r)
    setup_expertise(sr)
    cr = create_content_router(sr)

    cov = cr.content_coverage()
    assert cov["hashtag_covered_topics"] > 10, f"Hashtag map: {cov['hashtag_covered_topics']}"
    assert cov["expertise_topics"] >= 3  # BTC, Nostr, ETH

    # Темы с хэштегами и экспертами
    full = cov["full_coverage"]
    assert "btc" in full or "BTC" in full

    print(f"  ✅ content_coverage: {cov['hashtag_covered_topics']} hashtags, "
          f"{cov['expertise_topics']} expertise, full={len(cov['full_coverage'])}")


# ─── Тест 10: Snapshot интеграция ─────────────────────

def test_snapshot_integration():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-cr-10")
    build_triangle_mesh(kg)
    sr = build_sr(kg, r)
    setup_expertise(sr)
    cr = create_content_router(sr)

    # Обрабатываем несколько событий
    cr.route_event(nostr_event_from_post("#btc update", tags=[["t", "btc"]], pubkey="agent_B"))
    cr.route_event(nostr_event_from_post("#nostr relay", tags=[["t", "nostr"]], pubkey="agent_A"))
    cr.route_event(nostr_event_from_post("unknown stuff", pubkey="agent_C"))

    # Экспорт
    state = cr.export_state()
    assert "content_router" in state
    assert state["content_router"]["version"] == 12
    assert state["content_router"]["stats"]["events_processed"] == 3

    # Проверяем что semantic_router тоже экспортируется
    sr_state = sr.export_state()
    assert "semantic_router" in sr_state

    print(f"  ✅ snapshot: {state['content_router']['stats']}")


def main():
    print("═══ Phase 12 — Content Router ═══")
    print()

    test_extract_hashtags()
    test_classify_hashtag()
    test_classify_keyword()
    test_classify_semantic()
    test_classify_unknown()
    test_route_event()
    test_route_broadcast()
    test_batch_register()
    test_content_coverage()
    test_snapshot_integration()

    print()
    print("═══ Все 10 тестов Фазы 12 пройдены ✅ ═══")
    print()
    print("Content Router замыкает цикл:")
    print("  NostrEvent → extract_hashtags / classify_event → ContentClassification")
    print("  ContentClassification → SemanticRouter.route_by_topic → RoutedContent")
    print()
    print("Стек:")
    print("  ContentRouter (P12) → SemanticRouter (P11) → GraphMemory (P10)")
    print("                      → SmartRouter (P4) → KnowledgeGraph (P1-2)")
    print()
    print("Методы классификации:")
    print("  1. Хэштеги (tags + текст) → 30+ маппингов (#btc→BTC)")
    print("  2. Ключевые слова ← expertise cache")
    print("  3. Семантический поиск ← GraphMemory")
    print("  4. Unknown (не классифицировано)")


if __name__ == "__main__":
    main()

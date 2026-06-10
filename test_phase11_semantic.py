#!/usr/bin/env python3
"""
Phase 11 — Semantic Router: маршрутизация по смыслу.

Тесты:
  1. register_expertise / find_experts — регистрация и поиск
  2. route_by_topic — маршрут к лучшему эксперту
  3. route_to_node — прямая доставка
  4. unregister_expertise — удаление экспертизы
  5. broadcast_with_expertise — рассылка всем экспертам
  6. expertise_coverage — карта покрытия
  7. Интеграция с RouteEngine (tick)
  8. Snapshot включает semantic router
"""

import json
import time

import redis

from knowledge_graph import KnowledgeGraph, GraphNode, GraphEdge
from graph_memory import GraphMemory, attach_memory_to_graph
from smart_router import SmartRouter
from semantic_router import SemanticRouter, TopicExpert, SemanticRoute, create_semantic_router


def new_redis(clean: bool = True):
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    if clean:
        for k in r.scan_iter("graph:*"):
            r.delete(k)
    return r


def build_triangle_mesh(kg, r):
    """Построить треугольник: A—B—C с рёбрами."""
    now = time.time()

    # Nodes
    for nid, ntype in [("agent_A", "agent"), ("agent_B", "agent"), ("agent_C", "agent")]:
        kg.upsert_node(GraphNode(node_id=nid, node_type=ntype, last_seen=now, status="online"))

    # Edges: A↔B, B↔C, A↔C (all good)
    for src, dst in [("agent_A", "agent_B"), ("agent_B", "agent_C"), ("agent_A", "agent_C"),
                     ("agent_B", "agent_A"), ("agent_C", "agent_B"), ("agent_C", "agent_A")]:
        kg.upsert_edge(GraphEdge(
            source=src, target=dst, transport="wifi",
            latency_ms=15, success_rate=0.98, last_success=now
        ))

    return kg


def build_sr(kg, r):
    """Создать SemanticRouter с привязанным SmartRouter."""
    sr_inst = SmartRouter()
    sr_inst.graph = kg
    return create_semantic_router(kg, sr_inst, r)


# ─── Тест 1: register + find experts ──────────────────

def test_register_find():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-1")
    build_triangle_mesh(kg, r)

    sr = build_sr(kg, r)
    gm = sr.gm

    # Регистрация экспертизы
    sr.register_expertise("agent_A", "BTC price feeds",
                          "Real-time Bitcoin price oracle from 5 exchanges",
                          tags=["BTC", "price", "oracle"])
    sr.register_expertise("agent_B", "Nostr protocol",
                          "Nostr relay monitoring and protocol analysis",
                          tags=["Nostr", "protocol", "relays"])
    sr.register_expertise("agent_C", "Ethereum DeFi",
                          "Uniswap and Aave yield farming analytics",
                          tags=["ETH", "DeFi", "yield"])

    # Поиск
    experts = sr.find_experts("Bitcoin price oracle", top_k=3)
    assert len(experts) >= 1, f"Должен найти эксперта BTC: {experts}"
    assert experts[0].node_id == "agent_A", f"A должен быть первым: {experts[0]}"
    assert experts[0].score > 0.15

    experts = sr.find_experts("Nostr relays monitoring", top_k=3)
    assert len(experts) >= 1
    assert experts[0].node_id == "agent_B"

    experts = sr.find_experts("DeFi yield farming Ethereum", top_k=3)
    assert len(experts) >= 1
    assert experts[0].node_id == "agent_C"

    # Нерелевантный запрос — повышаем порог чтобы отсеять шум
    experts = sr.find_experts("cooking recipes", top_k=3, min_score=0.25)
    assert len(experts) == 0, f"Не должно быть экспертов по кулинарии: {experts}"

    print("  ✅ register + find: 4/4 queries → correct experts")


# ─── Тест 2: route_by_topic ────────────────────────────

def test_route_by_topic():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-2")
    build_triangle_mesh(kg, r)

    sr = build_sr(kg, r)

    sr.register_expertise("agent_A", "BTC", "Bitcoin oracle")
    sr.register_expertise("agent_C", "ETH", "Ethereum DeFi")

    # Маршрут от B к лучшему эксперту по BTC (должен быть A)
    route = sr.route_by_topic("agent_B", "Bitcoin price update", top_k=3)
    assert route.ok, f"Маршрут должен быть OK: {route.error}"
    assert route.selected_expert == "agent_A", f"Должен выбрать A: {route.selected_expert}"
    assert len(route.path) >= 1, f"Путь не пустой: {route.path}"
    assert route.path_weight > 0

    # Маршрут к ETH эксперту от A (должен быть C)
    route = sr.route_by_topic("agent_A", "Ethereum DeFi yield", top_k=3)
    assert route.ok
    assert route.selected_expert == "agent_C"

    print(f"  ✅ route_by_topic: BTC→A (path={route.path}, w={route.path_weight:.2f})")


# ─── Тест 3: route_to_node ─────────────────────────────

def test_route_to_node():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-3")
    build_triangle_mesh(kg, r)

    sr = build_sr(kg, r)

    # Прямая доставка
    route = sr.route_to_node("agent_A", "agent_C")
    assert route.ok, f"Прямой маршрут должен быть OK: {route.error}"
    assert route.selected_expert == "agent_C"
    # Путь включает source и target: [agent_A, agent_C] или [agent_A, agent_B, agent_C]
    assert len(route.path) >= 2
    assert route.path[0] == "agent_A"
    assert route.path[-1] == "agent_C"

    print(f"  ✅ route_to_node: A→C (path={route.path})")


# ─── Тест 4: unregister_expertise ──────────────────────

def test_unregister():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-4")
    build_triangle_mesh(kg, r)

    sr = build_sr(kg, r)

    sr.register_expertise("agent_B", "test_topic", "test desc")
    experts = sr.find_experts("test_topic")
    assert len(experts) == 1

    sr.unregister_expertise("agent_B", "test_topic")
    experts = sr.find_experts("test_topic")
    assert len(experts) == 0

    print("  ✅ unregister: added → found → removed → not found")


# ─── Тест 5: broadcast_with_expertise ──────────────────

def test_broadcast():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-5")
    build_triangle_mesh(kg, r)

    sr = build_sr(kg, r)

    sr.register_expertise("agent_A", "mesh routing", "P2P mesh routing expert")
    sr.register_expertise("agent_C", "mesh routing", "Decentralized routing specialist")

    routes = sr.broadcast_with_expertise("agent_B", "mesh routing protocol", top_k=5)
    assert len(routes) >= 2, f"Должен найти 2 эксперта: {len(routes)}"
    assert any(r.ok for r in routes), "Хотя бы один маршрут должен быть OK"

    experts_found = [r.selected_expert for r in routes if r.ok]
    assert "agent_A" in experts_found
    assert "agent_C" in experts_found

    print(f"  ✅ broadcast: {len(routes)} routes, {len([r for r in routes if r.ok])} OK")


# ─── Тест 6: expertise_coverage ────────────────────────

def test_coverage():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-6")
    build_triangle_mesh(kg, r)

    sr = build_sr(kg, r)

    sr.register_expertise("agent_A", "BTC", "bitcoin oracle")
    sr.register_expertise("agent_B", "Nostr", "nostr protocol")
    sr.register_expertise("agent_C", "ETH", "ethereum defi")
    sr.register_expertise("agent_A", "Lightning", "bitcoin lightning network")

    cov = sr.expertise_coverage()
    assert cov["total_topics"] == 4
    assert cov["total_experts"] == 3  # A, B, C

    # BTC и Lightning — оба на agent_A
    assert "agent_A" in cov["topics"].get("BTC", [])
    assert "agent_A" in cov["topics"].get("Lightning", [])

    print(f"  ✅ coverage: {cov['total_topics']} topics, {cov['total_experts']} experts")


# ─── Тест 7: Интеграция с RouteEngine ──────────────────

def test_route_engine_integration():
    """Проверяем что SemanticRouter корректно создаётся с теми же Redis/Router что и RouteEngine."""
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-7-re")
    build_triangle_mesh(kg, r)

    # Эмулируем RouteEngine.init с semantic router
    gm = attach_memory_to_graph(kg, r)
    sr_inst = SmartRouter()
    sr_inst.graph = kg
    sr = SemanticRouter(kg, sr_inst, gm)

    sr.register_expertise("agent_B", "health_check", "node health monitoring")

    # Симулируем tick: поиск + маршрут
    route = sr.route_by_topic("agent_A", "health_check status")
    assert route.ok
    assert route.selected_expert == "agent_B"

    # Экспорт для снапшота
    state = sr.export_state()
    assert "semantic_router" in state
    assert state["semantic_router"]["version"] == 11

    print("  ✅ RouteEngine integration: tick → route → snapshot")


# ─── Тест 8: Snapshot full round-trip ──────────────────

def test_snapshot_roundtrip():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-8-snap")
    build_triangle_mesh(kg, r)

    sr = build_sr(kg, r)
    sr.register_expertise("agent_A", "persistent_topic", "should survive snapshot")

    # Снапшот через KnowledgeGraph
    state = kg.export_state()
    assert "memory" in state, "Снапшот должен включать memory"

    # Восстановление
    kg2 = KnowledgeGraph(r, node_id="restore-8")
    gm2 = attach_memory_to_graph(kg2, r)

    # Добавляем те же узлы
    build_triangle_mesh(kg2, r)

    kg2.import_state(state, clear_first=False)

    sr2_inst = SmartRouter()
    sr2_inst.graph = kg2
    sr2 = SemanticRouter(kg2, sr2_inst, gm2)
    experts = sr2.find_experts("persistent_topic")
    assert len(experts) >= 1, f"Экспертиза должна пережить снапшот: {experts}"
    assert experts[0].node_id == "agent_A"

    print("  ✅ Snapshot round-trip: expertise survives export/import")


def main():
    print("═══ Phase 11 — Semantic Router ═══")
    print()

    test_register_find()
    test_route_by_topic()
    test_route_to_node()
    test_unregister()
    test_broadcast()
    test_coverage()
    test_route_engine_integration()
    test_snapshot_roundtrip()

    print()
    print("═══ Все 8 тестов Фазы 11 пройдены ✅ ═══")
    print()
    print("Semantic Router:")
    print("  • register_expertise(node, topic, desc) — регистрация компетенции")
    print("  • find_experts(topic, top_k) — семантический поиск экспертов")
    print("  • route_by_topic(source, topic) — маршрут к лучшему эксперту")
    print("  • route_to_node(source, target) — прямая доставка")
    print("  • broadcast_with_expertise(source, topic) — рассылка всем")
    print("  • unregister_expertise(node, topic) — удаление")
    print("  • expertise_coverage() — карта покрытия знаний")
    print("  • Snapshot: expertise survives export/import")
    print()
    print("  Стек: GraphMemory (P10) + SmartRouter (P4) + KnowledgeGraph (P1-2)")


if __name__ == "__main__":
    main()

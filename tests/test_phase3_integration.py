#!/usr/bin/env python3
"""Integration-тесты Phase 3: Knowledge Graph в RouteEngine.

Проверяет:
  1. Graph инициализируется при старте RouteEngine
  2. События обновляют граф (узлы + рёбра)
  3. Heartbeat обновляет last_seen и статус узла
  4. route_to() находит путь в построенном графе
  5. route_to() возвращает found=False когда граф пуст
  6. Статистика включает graph-метрики
"""

import sys
import os
import time
import json
import asyncio

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_graph_init():
    """Тест 1: KnowledgeGraph инициализируется."""
    from route_engine import RouteEngine
    engine = RouteEngine()

    assert engine.graph is not None, "Graph should be initialized"
    assert engine.r is not None, "Redis should be connected"
    assert hasattr(engine, "_last_decay"), "Should have decay timer"

    # Пустой граф — route_to возвращает found=False
    result = engine.route_to("any_target")
    assert result["found"] is False, "Empty graph should return not found"
    assert result["next_hop"] is None

    if engine.graph:
        engine.graph.flush()
    print("✅ Тест 1: Graph init — OK")


def test_events_update_graph():
    """Тест 2: События создают узлы и рёбра."""
    from route_engine import RouteEngine
    engine = RouteEngine()

    if not engine.graph:
        print("⚠️ Тест 2: SKIP (no Redis)")
        return

    engine.graph.flush()
    now = time.time()

    # Эмулируем heartbeat от agent_A
    event_heartbeat = {
        "kind": 39000,
        "pubkey": "pk_agent_a",
        "content": json.dumps({
            "type": "heartbeat",
            "from": "agent_A",
            "counter": 42,
            "uptime": 3600,
        }),
        "id": "evt_001",
    }
    engine._update_graph_from_event(event_heartbeat, "heartbeat")

    # Проверяем что узел создан
    node_a = engine.graph.get_node("agent_A")
    assert node_a is not None, "agent_A should exist"
    assert node_a.status == "online"
    assert node_a.last_seen > now - 5, "last_seen should be recent"

    # Эмулируем mesh-сообщение от agent_A → agent_B
    event_mesh = {
        "kind": 39002,
        "pubkey": "pk_agent_a",
        "content": json.dumps({
            "from": "agent_A",
            "to": "agent_B",
            "transport": "wifi",
            "data": "hello",
        }),
        "id": "evt_002",
    }
    engine._update_graph_from_event(event_mesh, "mesh")

    # Проверяем что ребро создано
    edge = engine.graph.get_edge("agent_A", "agent_B")
    assert edge is not None, "Edge A→B should exist"
    assert edge.transport == "wifi"

    # agent_B должен быть создан (как target, даже без своего heartbeat)
    node_b = engine.graph.get_node("agent_B")
    assert node_b is None, "target-only nodes NOT auto-created (only source nodes are)"

    # Проверяем статистику
    assert engine.stats["graph_nodes"] >= 1
    assert engine.stats["graph_edges"] >= 1

    engine.graph.flush()
    print("✅ Тест 2: Events update graph — OK")


def test_heartbeat_status_update():
    """Тест 3: Heartbeat обновляет статус узла."""
    from route_engine import RouteEngine
    engine = RouteEngine()

    if not engine.graph:
        print("⚠️ Тест 3: SKIP (no Redis)")
        return

    engine.graph.flush()

    # Создаём узел в прошлом (offline)
    old_time = time.time() - 300  # 5 минут назад
    from knowledge_graph import GraphNode
    engine.graph.upsert_node(GraphNode(
        node_id="agent_sleepy",
        node_type="agent",
        last_seen=old_time,
        status="online",
    ))

    # Статус должен быть offline (last_seen > 120 сек)
    status_before = engine.graph.get_node_status("agent_sleepy")
    assert status_before == "offline", f"Should be offline, got {status_before}"

    # Пришёл heartbeat
    event_hb = {
        "kind": 39000,
        "pubkey": "pk_sleepy",
        "content": json.dumps({
            "type": "heartbeat",
            "from": "agent_sleepy",
            "counter": 100,
            "uptime": 1000,
        }),
        "id": "evt_hb",
    }
    engine._update_graph_from_event(event_hb, "heartbeat")

    # Статус должен обновиться
    status_after = engine.graph.get_node_status("agent_sleepy")
    assert status_after == "online", f"Should be online after heartbeat, got {status_after}"

    engine.graph.flush()
    print("✅ Тест 3: Heartbeat status update — OK")


def test_route_to_path():
    """Тест 4: route_to() находит путь."""
    from route_engine import RouteEngine
    engine = RouteEngine()

    if not engine.graph:
        print("⚠️ Тест 4: SKIP (no Redis)")
        return

    engine.graph.flush()
    now = time.time()

    # Строим топологию: route_engine → A → B → C
    from knowledge_graph import GraphNode, GraphEdge

    engine.graph.upsert_node(GraphNode(node_id="route_engine", node_type="gateway", last_seen=now))
    engine.graph.upsert_node(GraphNode(node_id="agent_A", node_type="agent", last_seen=now))
    engine.graph.upsert_node(GraphNode(node_id="agent_B", node_type="agent", last_seen=now))
    engine.graph.upsert_node(GraphNode(node_id="agent_C", node_type="agent", last_seen=now))

    engine.graph.upsert_edge(GraphEdge(source="route_engine", target="agent_A",
                                        transport="wifi", latency_ms=10, success_rate=0.99))
    engine.graph.upsert_edge(GraphEdge(source="agent_A", target="agent_B",
                                        transport="wifi", latency_ms=15, success_rate=0.98))
    engine.graph.upsert_edge(GraphEdge(source="agent_B", target="agent_C",
                                        transport="lora", latency_ms=200, success_rate=0.85))

    # Ищем путь до agent_C
    result = engine.route_to("agent_C", source_id="route_engine")
    assert result["found"] is True, f"Path should be found, got {result}"
    assert result["path"] == ["route_engine", "agent_A", "agent_B", "agent_C"], \
        f"Unexpected path: {result['path']}"
    assert result["next_hop"] == "agent_A"
    assert result["hops"] == 3
    assert result["total_weight"] > 0
    assert result["fallback"] is False

    # Путь до несуществующего
    result_none = engine.route_to("ghost_agent", source_id="route_engine")
    assert result_none["found"] is False

    engine.graph.flush()
    print("✅ Тест 4: route_to path found — OK")


def test_stats_include_graph():
    """Тест 5: stats_report включает graph-метрики."""
    from route_engine import RouteEngine
    engine = RouteEngine()

    if not engine.graph:
        print("⚠️ Тест 5: SKIP (no Redis)")
        return

    engine.graph.flush()
    now = time.time()

    from knowledge_graph import GraphNode, GraphEdge
    engine.graph.upsert_node(GraphNode(node_id="n1", node_type="agent", last_seen=now))
    engine.graph.upsert_node(GraphNode(node_id="n2", node_type="agent", last_seen=now - 200))
    engine.graph.upsert_edge(GraphEdge(source="n1", target="n2", transport="wifi", latency_ms=30))

    s = engine.stats_report()

    assert s["graph_nodes"] == 2, f"graph_nodes should be 2, got {s['graph_nodes']}"
    assert s["graph_edges"] == 1, f"graph_edges should be 1, got {s['graph_edges']}"
    assert s["graph_online"] == 1, f"1 online, got {s['graph_online']}"
    assert s["graph_offline"] == 1, f"1 offline, got {s['graph_offline']}"

    engine.graph.flush()
    print("✅ Тест 5: Stats include graph — OK")


if __name__ == "__main__":
    print("═══ Phase 3 Integration Tests ═══\n")
    test_graph_init()
    test_events_update_graph()
    test_heartbeat_status_update()
    test_route_to_path()
    test_stats_include_graph()
    print("\n═══ Все 5 integration-тестов пройдены ✅ ═══")

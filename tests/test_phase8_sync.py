#!/usr/bin/env python3
"""Phase 8 — Redis PubSub Multi-node Graph Sync.

Каждая нода использует ОТДЕЛЬНЫЙ Redis-клиент (как в реальной multi-node топологии).
Это гарантирует, что PubSub работает корректно (разные connection pools).

Тесты:
  1. PubSub publish → другая нода получает node:upsert
  2. PubSub publish → другая нода получает edge:upsert
  3. Свои события пропускаются (skip own)
  4. edge:delete → удаление ребра на другой ноде
  5. full:sync → полная перезагрузка
  6. Две ноды параллельно → консистентность графа
  7. process_sync_events → неблокирующий опрос
  8. get_sync_stats → статистика синхронизации
"""

import time
import redis
from knowledge_graph import KnowledgeGraph, GraphNode, GraphEdge


def new_redis():
    """Создать новый Redis-клиент с очисткой от старых данных графа."""
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    for k in r.hkeys("graph:nodes"):
        r.hdel("graph:nodes", k)
    for k in r.hkeys("graph:edges"):
        r.hdel("graph:edges", k)
    r.delete("graph:stats")
    return r


def sync_drain(kg, max_attempts=3) -> dict:
    """Вызвать process_sync_events до получения результата."""
    result = {"processed": 0, "skipped_own": 0, "reloaded_nodes": 0, "reloaded_edges": 0, "errors": 0}
    for _ in range(max_attempts):
        r = kg.process_sync_events()
        for k in result:
            result[k] += r[k]
        if r["processed"] > 0:
            break
        time.sleep(0.1)
    return result


def test_pubsub_node_upsert():
    """Нода A добавляет узел → Нода B видит через PubSub."""
    # Отдельные Redis-клиенты как на разных машинах
    r_a = new_redis()
    r_b = new_redis()

    kg_a = KnowledgeGraph(r_a, node_id="node-a")
    kg_b = KnowledgeGraph(r_b, node_id="node-b")

    kg_b.start_sync()

    now = time.time()
    kg_a.upsert_node(GraphNode(node_id="agent-X", node_type="agent", last_seen=now, status="online"))

    result = sync_drain(kg_b)
    print(f"  B received: +{result['reloaded_nodes']}n +{result['reloaded_edges']}e (skipped={result['skipped_own']})")

    assert result["reloaded_nodes"] >= 1, f"Нода B должна получить узел: {result}"
    assert "agent-X" in kg_b._nodes, f"Узел agent-X должен быть у ноды B: {list(kg_b._nodes.keys())}"
    assert kg_b._nodes["agent-X"].status == "online"
    print(f"  ✅ PubSub node:upsert — нода B получила agent-X")

    kg_b.stop_sync()


def test_pubsub_edge_upsert():
    """Нода A добавляет ребро → Нода B видит."""
    r_a = new_redis()
    r_b = new_redis()

    kg_a = KnowledgeGraph(r_a, node_id="node-a")
    kg_b = KnowledgeGraph(r_b, node_id="node-b")

    now = time.time()
    for nid in ("A", "B"):
        kg_a.upsert_node(GraphNode(node_id=nid, node_type="agent", last_seen=now, status="online"))

    kg_b.start_sync()
    sync_drain(kg_b)  # получить узлы

    kg_a.upsert_edge(GraphEdge(source="A", target="B", transport="wifi", latency_ms=5, success_rate=1.0, last_success=now))

    result = sync_drain(kg_b)
    print(f"  B after edge: +{result['reloaded_nodes']}n +{result['reloaded_edges']}e")

    assert result["reloaded_edges"] >= 1, f"Должен получить ребро: {result}"
    assert kg_b.get_edge("A", "B") is not None
    print(f"  ✅ PubSub edge:upsert — нода B получила A→B")

    kg_b.stop_sync()


def test_skip_own_events():
    """Нода не должна реагировать на свои же события."""
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="node-solo")
    kg.start_sync()

    now = time.time()
    kg.upsert_node(GraphNode(node_id="self-X", node_type="agent", last_seen=now, status="online"))

    result = sync_drain(kg)
    print(f"  Processed={result['processed']}, skipped_own={result['skipped_own']}, reloaded={result['reloaded_nodes']}")

    assert result["skipped_own"] >= 1, f"Свои события должны быть пропущены: {result}"
    assert result["reloaded_nodes"] == 0, "Свои события не должны вызывать reload"
    print(f"  ✅ Свои PubSub события пропускаются")

    kg.stop_sync()


def test_edge_delete():
    """edge:delete → ребро удаляется на другой ноде."""
    r_a = new_redis()
    r_b = new_redis()

    kg_a = KnowledgeGraph(r_a, node_id="node-a")
    kg_b = KnowledgeGraph(r_b, node_id="node-b")

    # ВАЖНО: subscribe ДО publish (Redis PubSub не буферизует)
    kg_b.start_sync()

    now = time.time()
    for nid in ("X", "Y"):
        kg_a.upsert_node(GraphNode(node_id=nid, node_type="agent", last_seen=now, status="online"))
    kg_a.upsert_edge(GraphEdge(source="X", target="Y", transport="wifi", latency_ms=5, success_rate=1.0, last_success=now))

    sync_drain(kg_b)
    assert kg_b.get_edge("X", "Y") is not None, f"Ребро X→Y должно быть у B: {list(kg_b._edges.keys())}"

    kg_a._publish("edge:delete", {"edge_id": "X→Y"})

    result = sync_drain(kg_b)
    print(f"  B after delete: +{result['reloaded_nodes']}n +{result['reloaded_edges']}e")

    assert kg_b.get_edge("X", "Y") is None, f"Ребро X→Y должно быть удалено у ноды B"
    print(f"  ✅ PubSub edge:delete — нода B удалила ребро")

    kg_b.stop_sync()


def test_full_sync():
    """full:sync → другая нода перезагружает весь граф."""
    r_a = new_redis()
    r_b = new_redis()

    kg_a = KnowledgeGraph(r_a, node_id="node-a")
    kg_b = KnowledgeGraph(r_b, node_id="node-b")

    # Subscribe ДО publish
    kg_b.start_sync()

    now = time.time()
    for nid in ("M", "N", "O"):
        kg_a.upsert_node(GraphNode(node_id=nid, node_type="agent", last_seen=now, status="online"))
    kg_a.upsert_edge(GraphEdge(source="M", target="N", transport="lora", latency_ms=50, success_rate=1.0, last_success=now))
    kg_a.upsert_edge(GraphEdge(source="N", target="O", transport="wifi", latency_ms=5, success_rate=1.0, last_success=now))

    kg_a._publish("full:sync", {"reason": "test"})

    result = sync_drain(kg_b)
    print(f"  B full reload: changes={result['reloaded_nodes']}")

    assert "M" in kg_b._nodes, "Узел M должен быть"
    assert "N" in kg_b._nodes, "Узел N должен быть"
    assert "O" in kg_b._nodes, "Узел O должен быть"
    assert kg_b.get_edge("M", "N") is not None
    assert kg_b.get_edge("N", "O") is not None
    print(f"  ✅ PubSub full:sync — нода B получила весь граф")

    kg_b.stop_sync()


def test_two_nodes_consistency():
    """Две ноды работают параллельно, граф консистентен."""
    r_a = new_redis()
    r_b = new_redis()

    kg_a = KnowledgeGraph(r_a, node_id="node-a")
    kg_b = KnowledgeGraph(r_b, node_id="node-b")

    kg_a.start_sync()
    kg_b.start_sync()

    now = time.time()

    for nid in ("P", "Q", "R"):
        kg_a.upsert_node(GraphNode(node_id=nid, node_type="agent", last_seen=now, status="online"))
    kg_a.upsert_edge(GraphEdge(source="P", target="Q", transport="wifi", latency_ms=5, success_rate=1.0, last_success=now))

    kg_b.upsert_node(GraphNode(node_id="S", node_type="relay", last_seen=now, status="online"))
    kg_b.upsert_edge(GraphEdge(source="Q", target="S", transport="nostr", latency_ms=20, success_rate=1.0, last_success=now))

    # Несколько циклов синхронизации
    for _ in range(5):
        sync_drain(kg_a)
        sync_drain(kg_b)

    nodes_a = set(kg_a._nodes.keys())
    nodes_b = set(kg_b._nodes.keys())
    edges_a = set(kg_a._edges.keys())
    edges_b = set(kg_b._edges.keys())

    print(f"  A: nodes={sorted(nodes_a)}, edges={sorted(edges_a)}")
    print(f"  B: nodes={sorted(nodes_b)}, edges={sorted(edges_b)}")

    expected_nodes = {"P", "Q", "R", "S"}
    expected_edges = {"P→Q", "Q→S"}

    assert nodes_a == expected_nodes, f"A nodes mismatch: {nodes_a} != {expected_nodes}"
    assert nodes_b == expected_nodes, f"B nodes mismatch: {nodes_b} != {expected_nodes}"
    assert edges_a == expected_edges, f"A edges mismatch: {edges_a} != {expected_edges}"
    assert edges_b == expected_edges, f"B edges mismatch: {edges_b} != {expected_edges}"
    print(f"  ✅ Две ноды консистентны: {len(expected_nodes)} узлов, {len(expected_edges)} рёбер")

    kg_a.stop_sync()
    kg_b.stop_sync()


def test_process_sync_nonblocking():
    """process_sync_events не блокирует при отсутствии событий."""
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="node-quiet")
    kg.start_sync()

    start = time.time()
    result = kg.process_sync_events()
    elapsed = time.time() - start

    print(f"  No events: {result}, elapsed={elapsed:.4f}s")
    assert elapsed < 1.5, f"process_sync_events слишком долгий: {elapsed:.2f}s"
    assert result["processed"] == 0
    print(f"  ✅ process_sync_events неблокирующий ({elapsed:.3f}s)")

    kg.stop_sync()


def test_sync_stats():
    """get_sync_stats отображает метрики синхронизации."""
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="node-stats")
    kg.start_sync()

    now = time.time()
    kg.upsert_node(GraphNode(node_id="stats-1", node_type="agent", last_seen=now, status="online"))
    kg.upsert_edge(GraphEdge(source="stats-1", target="stats-2", transport="wifi",
                              latency_ms=5, success_rate=1.0, last_success=now))

    sync_drain(kg)

    stats = kg.get_sync_stats()
    print(f"  Stats: {stats}")
    assert stats["published"] >= 2, f"Должно быть ≥2 publish: {stats}"
    print(f"  ✅ get_sync_stats: published={stats['published']}, received={stats['received']}")

    kg.stop_sync()


def test_reload_single_node():
    """reload_node — точечная перезагрузка из Redis."""
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="node-test")

    now = time.time()
    kg.upsert_node(GraphNode(node_id="reload-me", node_type="agent", last_seen=now, status="online"))

    kg._nodes["reload-me"].status = "corrupted"

    ok = kg.reload_node("reload-me")
    assert ok
    assert kg._nodes["reload-me"].status == "online", f"После reload должен быть online: {kg._nodes['reload-me'].status}"
    print(f"  ✅ reload_node восстанавливает из Redis")


def test_reload_nonexistent():
    """reload_node для несуществующего узла — возвращает False."""
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="node-ghost")
    ok = kg.reload_node("ghost-node")
    assert not ok, "Несуществующий узел должен вернуть False"
    print(f"  ✅ reload_node для ghost: False")


def main():
    print("═══ Phase 8 — Redis PubSub Multi-node Graph Sync ═══")
    print()

    test_pubsub_node_upsert()
    test_pubsub_edge_upsert()
    test_skip_own_events()
    test_edge_delete()
    test_full_sync()
    test_two_nodes_consistency()
    test_process_sync_nonblocking()
    test_sync_stats()
    test_reload_single_node()
    test_reload_nonexistent()

    print()
    print("═══ Все 10 тестов Фазы 8 пройдены ✅ ═══")
    print()
    print("Redis PubSub Multi-node Sync:")
    print("  • graph:sync — единый канал PubSub")
    print("  • node:upsert / edge:upsert / edge:delivery / edge:delete / full:sync")
    print("  • node_id — фильтр своих событий")
    print("  • process_sync_events() — неблокирующий опрос каждые 2 сек")
    print("  • full_reload() — полная синхронизация каждые 5 мин")
    print("  • reload_node() / reload_edge() — точечная перезагрузка")


if __name__ == "__main__":
    main()

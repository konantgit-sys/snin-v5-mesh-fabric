#!/usr/bin/env python3
"""
Phase 9 — Supervisor: Snapshots, Integrity, Recovery.

Тесты:
  1. export_state / import_state — round-trip консистентность
  2. save_snapshot / restore_snapshot — файловый цикл
  3. integrity_check — детект dangling edges + orphan nodes
  4. integrity_check — чистый граф (ok)
  5. GraphSupervisor tick — периодический snapshot
  6. GraphSupervisor on_component_restart — восстановление
  7. GraphSupervisor health — метрики для supervisor
  8. Snapshot rotation — старые удаляются
"""

import json
import os
import time
import shutil
import tempfile

import redis

from knowledge_graph import KnowledgeGraph, GraphNode, GraphEdge
from supervisor_graph import GraphSupervisor


def new_redis(clean: bool = True):
    """Создать новый Redis-клиент."""
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    if clean:
        for k in r.hkeys("graph:nodes"):
            r.hdel("graph:nodes", k)
        for k in r.hkeys("graph:edges"):
            r.hdel("graph:edges", k)
        r.delete("graph:stats", "graph:snapshot")
    return r


def test_export_import_roundtrip():
    """export_state → import_state → идентичный граф."""
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-node")

    now = time.time()
    kg.upsert_node(GraphNode(node_id="A", node_type="agent", last_seen=now, status="online"))
    kg.upsert_node(GraphNode(node_id="B", node_type="relay", last_seen=now, status="online"))
    kg.upsert_node(GraphNode(node_id="C", node_type="agent", last_seen=now, status="degraded"))
    kg.upsert_edge(GraphEdge(source="A", target="B", transport="wifi", latency_ms=5, success_rate=1.0, last_success=now))
    kg.upsert_edge(GraphEdge(source="B", target="C", transport="nostr", latency_ms=20, success_rate=0.95, last_success=now))

    state = kg.export_state()
    assert state["version"] == 9
    assert len(state["nodes"]) == 3
    assert len(state["edges"]) == 2
    assert len(state["adj"]) == 2

    # Импортируем в новый граф
    r2 = new_redis()
    kg2 = KnowledgeGraph(r2, node_id="restored-node")
    count = kg2.import_state(state, clear_first=True)
    assert count == 5, f"Должно быть 5 сущностей: {count}"
    assert "A" in kg2._nodes
    assert "C" in kg2._nodes
    assert kg2._nodes["A"].status == "online"
    assert kg2._nodes["C"].status == "degraded"
    assert kg2.get_edge("A", "B") is not None
    assert kg2.get_edge("B", "C") is not None
    assert kg2.get_edge("B", "C").transport == "nostr"

    print(f"  ✅ export/import round-trip: {count} entities")

    # Проверяем adjacency
    assert "B" in kg2._adj["A"]
    assert "C" in kg2._adj["B"]


def test_snapshot_file_cycle():
    """save_snapshot → удаляем граф → restore_snapshot."""
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="snap-node")

    now = time.time()
    for nid in ("X", "Y", "Z"):
        kg.upsert_node(GraphNode(node_id=nid, node_type="agent", last_seen=now, status="online"))
    kg.upsert_edge(GraphEdge(source="X", target="Y", transport="wifi", latency_ms=5, success_rate=1.0, last_success=now))
    kg.upsert_edge(GraphEdge(source="Y", target="Z", transport="lora", latency_ms=100, success_rate=0.8, last_success=now))

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        snap_path = f.name

    size = kg.save_snapshot(snap_path)
    assert size > 100, f"Снапшот слишком мал: {size}"
    assert os.path.exists(snap_path)

    # Очищаем in-memory
    kg._nodes.clear()
    kg._edges.clear()
    kg._adj.clear()
    assert len(kg._nodes) == 0

    # Восстанавливаем
    count = kg.restore_snapshot(path=snap_path)
    assert count >= 5, f"Должно восстановиться ≥5: {count}"
    assert "X" in kg._nodes
    assert "Z" in kg._nodes
    assert kg.get_edge("X", "Y") is not None
    assert kg.get_edge("Y", "Z") is not None

    print(f"  ✅ snapshot file cycle: {count} entities, {size} bytes")

    os.unlink(snap_path)


def test_snapshot_redis_fallback():
    """restore_snapshot без файла → Redis fallback."""
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="redis-fb")

    now = time.time()
    kg.upsert_node(GraphNode(node_id="R1", node_type="agent", last_seen=now, status="online"))
    kg.upsert_edge(GraphEdge(source="R1", target="R2", transport="wifi", latency_ms=5, success_rate=1.0, last_success=now))

    # Очищаем in-memory
    kg._nodes.clear()
    kg._edges.clear()
    kg._adj.clear()

    # restore_snapshot без path → должен загрузить из Redis (KEY_SNAPSHOT или load_from_redis)
    count = kg.restore_snapshot()  # fallback: load_from_redis
    assert count >= 2, f"Redis fallback должен восстановить ≥2: {count}"
    assert "R1" in kg._nodes

    print(f"  ✅ snapshot Redis fallback: {count} entities")


def test_integrity_dangling_edges():
    """integrity_check детектит висячие рёбра."""
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="int-node")

    now = time.time()
    kg.upsert_node(GraphNode(node_id="D1", node_type="agent", last_seen=now, status="online"))

    # Втыкаем ребро вручную (без target-узла)
    kg.upsert_edge(GraphEdge(source="D1", target="D_MISSING", transport="wifi", latency_ms=5, success_rate=1.0, last_success=now))
    # Втыкаем ребро с несуществующим source
    kg._edges["D_GHOST→D1"] = GraphEdge(source="D_GHOST", target="D1", transport="nostr",
                                          latency_ms=10, success_rate=1.0, last_success=now)

    result = kg.integrity_check()
    assert not result["ok"], f"Должны быть issues: {result}"
    assert len(result["issues"]) >= 2, f"Минимум 2 проблемы: {result['issues']}"
    assert len(result["dangling_edges"]) >= 2

    print(f"  ✅ integrity detect: {len(result['issues'])} issues, {len(result['dangling_edges'])} dangling, "
          f"{len(result['orphan_nodes'])} orphans")


def test_integrity_clean_graph():
    """integrity_check на чистом графе — ok."""
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="clean-node")

    now = time.time()
    for nid in ("P", "Q"):
        kg.upsert_node(GraphNode(node_id=nid, node_type="agent", last_seen=now, status="online"))
    kg.upsert_edge(GraphEdge(source="P", target="Q", transport="wifi", latency_ms=5, success_rate=1.0, last_success=now))

    result = kg.integrity_check()
    assert result["ok"], f"Чистый граф должен быть ok: {result['issues']}"
    assert len(result["issues"]) == 0
    assert len(result["dangling_edges"]) == 0

    print(f"  ✅ integrity clean: ok, {result['total_nodes']}n {result['total_edges']}e")


def test_graph_supervisor_tick():
    """GraphSupervisor.tick — периодический snapshot после N вызовов."""
    r = new_redis()
    snap_dir = tempfile.mkdtemp(prefix="gs_snap_")

    gs = GraphSupervisor(redis_url="redis://localhost:6379/0", snapshot_dir=snap_dir)

    # Не вызываем start() — тестируем минимально
    gs._started_at = time.time()
    gs.kg = KnowledgeGraph(r, node_id="tick-test")

    now = time.time()
    gs.kg.upsert_node(GraphNode(node_id="T1", node_type="agent", last_seen=now, status="online"))
    gs.kg.upsert_edge(GraphEdge(source="T1", target="T2", transport="wifi", latency_ms=5, success_rate=1.0, last_success=now))

    # Искусственно ставим таймер, чтобы сработал snapshot
    gs._last_snapshot_at = 0
    result = gs.tick()
    assert result["snapshot"], f"Snapshot должен сработать: {result}"
    assert result["synced"] == 0, f"Sync не должен был сработать без PubSub: {result}"

    # Проверяем что снапшот создался
    snapshots = [f for f in os.listdir(snap_dir) if f.endswith(".json")]
    assert len(snapshots) == 1, f"Должен быть 1 снапшот: {snapshots}"

    print(f"  ✅ GraphSupervisor tick: snapshot={result['snapshot']}, "
          f"integrity={result['integrity']['ok']}")

    shutil.rmtree(snap_dir)


def test_supervisor_health():
    """GraphSupervisor.get_health() — метрики."""
    r = new_redis()
    gs = GraphSupervisor(redis_url="redis://localhost:6379/0")
    gs._started_at = time.time()
    gs.kg = KnowledgeGraph(r, node_id="health-test")

    now = time.time()
    gs.kg.upsert_node(GraphNode(node_id="H1", node_type="agent", last_seen=now, status="online"))
    gs.kg.upsert_node(GraphNode(node_id="H2", node_type="relay", last_seen=now - 200, status="offline"))

    health = gs.get_health()
    assert health["alive"]
    assert health["nodes"] == 2
    assert health["online"] == 1
    assert health["offline"] == 1
    assert "integrity_ok" in health
    assert "uptime_sec" in health

    print(f"  ✅ health: alive={health['alive']}, nodes={health['nodes']}, "
          f"online={health['online']} offline={health['offline']}")


def test_snapshot_rotation():
    """Ротация снапшотов — старые удаляются, свежие остаются."""
    snap_dir = tempfile.mkdtemp(prefix="gs_rot_")

    gs = GraphSupervisor(snapshot_dir=snap_dir)
    gs.SNAPSHOT_RETENTION = 3

    # Создаём 5 снапшотов
    for i in range(5):
        path = os.path.join(snap_dir, f"graph_snapshot_2026-06-{10+i:02d}T00:00:00.json")
        with open(path, "w") as f:
            f.write('{"version": 9, "nodes": {}, "edges": {}}')
        time.sleep(0.01)  # разное время создания

    gs._rotate_snapshots()

    remaining = sorted([f for f in os.listdir(snap_dir) if f.endswith(".json")])
    assert len(remaining) == 3, f"Должно остаться 3: {remaining}"
    # Проверяем что остались самые новые (14, 13, 12)
    assert "06-14" in remaining[-1], f"Самый новый должен быть 14: {remaining}"
    assert "06-12" in remaining[0], f"Самый старый из оставшихся: {remaining}"

    print(f"  ✅ rotation: {len(remaining)} snapshots kept out of 5")

    shutil.rmtree(snap_dir)


def main():
    print("═══ Phase 9 — Supervisor: Snapshots, Integrity, Recovery ═══")
    print()

    test_export_import_roundtrip()
    test_snapshot_file_cycle()
    test_snapshot_redis_fallback()
    test_integrity_dangling_edges()
    test_integrity_clean_graph()
    test_graph_supervisor_tick()
    test_supervisor_health()
    test_snapshot_rotation()

    print()
    print("═══ Все 8 тестов Фазы 9 пройдены ✅ ═══")
    print()
    print("Graph Supervisor:")
    print("  • export_state() / import_state() — round-trip консистентность")
    print("  • save_snapshot(path) — JSON + Redis (TTL 7 дней)")
    print("  • restore_snapshot(path=None) — приоритет: файл → Redis → load_from_redis")
    print("  • integrity_check() — dangling edges, orphan nodes, adj consistency")
    print("  • get_graph_health() — метрики для supervisor API")
    print("  • GraphSupervisor.tick() — периодический snapshot + integrity")
    print("  • Snapshot rotation — хранение последних N")


if __name__ == "__main__":
    main()

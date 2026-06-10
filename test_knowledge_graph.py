#!/usr/bin/env python3
"""Unit-тесты Knowledge Graph — Фаза 1.

Проверяет:
  1. Создание узлов и рёбер
  2. Вычисление веса ребра
  3. Запись доставки (ACK/NACK) и обновление success_rate
  4. Деградация рёбер
  5. Статистика графа
"""

import sys
import os
import time
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from knowledge_graph import (
    GraphNode, GraphEdge, KnowledgeGraph, create_knowledge_graph
)
import redis


def get_test_redis():
    """Подключение к Redis (db 1 — тестовая)."""
    return redis.Redis(host="localhost", port=6379, db=1, decode_responses=False)


def test_node_create():
    """Тест 1: Создание и сериализация узла."""
    node = GraphNode(
        node_id="abc123",
        node_type="agent",
        last_seen=time.time(),
        status="online",
        capabilities=["store_forward", "multi_radio"],
        position={"lat": 55.75, "lon": 37.61},
    )
    js = node.to_json()
    restored = GraphNode.from_json(js)

    assert restored.node_id == "abc123"
    assert restored.node_type == "agent"
    assert restored.status == "online"
    assert "store_forward" in restored.capabilities
    assert restored.position["lat"] == 55.75
    print("✅ Тест 1: Создание узла — OK")


def test_edge_weight():
    """Тест 2: Вычисление композитного веса ребра."""
    # WiFi: latency 45ms, success 0.97, bandwidth 54000 kbps
    edge_wifi = GraphEdge(
        source="A", target="B", transport="wifi",
        latency_ms=45, success_rate=0.97, bandwidth_kbps=54000, hop_count=1
    )
    w_wifi = edge_wifi.compute_weight()
    # weight = 45/1000 + (1-0.97)*10 + 1*0.5 + 0 = 0.045 + 0.3 + 0.5 = 0.845
    assert 0.8 < w_wifi < 0.9, f"WiFi weight expected ~0.845, got {w_wifi}"

    # LoRa slow: latency 500ms, success 0.8, bandwidth 30 kbps, hop 3
    edge_lora = GraphEdge(
        source="C", target="D", transport="lora",
        latency_ms=500, success_rate=0.8, bandwidth_kbps=30, hop_count=3
    )
    w_lora = edge_lora.compute_weight()
    # weight = 500/1000 + (1-0.8)*10 + 3*0.5 + 5.0 = 0.5 + 2.0 + 1.5 + 5.0 = 9.0
    assert 8.5 < w_lora < 9.5, f"LoRa weight expected ~9.0, got {w_lora}"

    # WiFi всегда легче LoRa
    assert w_wifi < w_lora, f"WiFi should be lighter than LoRa ({w_wifi} vs {w_lora})"
    print("✅ Тест 2: Вес ребра — OK (WiFi=%.3f, LoRa=%.3f)" % (w_wifi, w_lora))


def test_delivery_record():
    """Тест 3: Запись доставки и обновление success_rate."""
    r = get_test_redis()
    kg = KnowledgeGraph(r)
    kg.flush()

    # Создаём узел
    kg.upsert_node(GraphNode(node_id="agent_1", node_type="agent"))
    kg.upsert_node(GraphNode(node_id="agent_2", node_type="agent"))

    # Успешная доставка
    kg.record_delivery("agent_1", "agent_2", success=True, latency_ms=50)
    edge = kg.get_edge("agent_1", "agent_2")
    assert edge is not None, "Edge should be created"
    assert edge.success_rate > 0.95, f"Success rate should be >0.95, got {edge.success_rate}"
    assert edge.latency_ms == 50, f"Latency should be 50, got {edge.latency_ms}"

    # Ещё одна успешная
    kg.record_delivery("agent_1", "agent_2", success=True, latency_ms=100)
    # EWMA: 50*0.7 + 100*0.3 = 35 + 30 = 65
    assert 60 < edge.latency_ms < 70, f"EWMA latency should be ~65, got {edge.latency_ms}"

    # Неудачная доставка
    kg.record_delivery("agent_1", "agent_2", success=False)
    assert edge.success_rate < 0.95, f"Success rate should drop after NACK"
    assert edge.failures_24h == 1

    # Соседи
    neighbors = kg.get_neighbors("agent_1")
    assert "agent_2" in neighbors

    kg.flush()
    print("✅ Тест 3: Запись доставки — OK")


def test_edge_decay():
    """Тест 4: Деградация рёбер."""
    r = get_test_redis()
    kg = KnowledgeGraph(r)
    kg.flush()

    kg.upsert_node(GraphNode(node_id="X", node_type="agent"))
    kg.upsert_node(GraphNode(node_id="Y", node_type="agent"))

    # Создаём ребро с активностью в далёком прошлом
    edge = GraphEdge(
        source="X", target="Y", transport="wifi",
        success_rate=0.95, latency_ms=20,
        last_success=time.time() - 1200,  # 20 минут назад
    )
    kg.upsert_edge(edge)

    initial_sr = edge.success_rate
    decayed = kg.decay_edges()
    assert decayed >= 1, f"At least 1 edge should decay, got {decayed}"

    edge_after = kg.get_edge("X", "Y")
    assert edge_after.success_rate < initial_sr, "Success rate should decrease after decay"

    kg.flush()
    print("✅ Тест 4: Деградация рёбер — OK")


def test_graph_stats():
    """Тест 5: Статистика графа."""
    r = get_test_redis()
    kg = KnowledgeGraph(r)
    kg.flush()

    # Пустой граф
    stats = kg.get_stats()
    assert stats["total_nodes"] == 0
    assert not stats["ready"]

    # Добавляем узлы
    now = time.time()
    kg.upsert_node(GraphNode(node_id="online_1", node_type="agent", last_seen=now, status="online"))
    kg.upsert_node(GraphNode(node_id="degraded_1", node_type="agent", last_seen=now - 60, status="online"))
    kg.upsert_node(GraphNode(node_id="offline_1", node_type="agent", last_seen=now - 200, status="online"))

    # Добавляем рёбра
    kg.upsert_edge(GraphEdge(source="online_1", target="degraded_1", transport="wifi", latency_ms=30))
    kg.upsert_edge(GraphEdge(source="degraded_1", target="offline_1", transport="lora", latency_ms=300))

    stats = kg.get_stats()
    assert stats["total_nodes"] == 3
    assert stats["total_edges"] == 2
    assert stats["ready"] is True
    assert stats["nodes_online"] == 1    # online_1: delta < 30
    assert stats["nodes_degraded"] == 1  # degraded_1: 30 < delta < 120
    assert stats["nodes_offline"] == 1   # offline_1: delta > 120
    assert stats["avg_weight"] > 0, f"avg_weight should be >0, got {stats['avg_weight']}"

    # status_line
    line = kg.status_line()
    assert "nodes=3" in line
    assert "edges=2" in line

    kg.flush()
    print("✅ Тест 5: Статистика графа — OK (%s)" % line)


# ═══ Фаза 2: Алгоритмы ═══════════════════════════════════════

def test_store_penalty():
    """Тест 6: Store-and-forward penalty."""
    r = get_test_redis()
    kg = KnowledgeGraph(r)
    kg.flush()

    now = time.time()

    # Online узел — без штрафа
    kg.upsert_node(GraphNode(node_id="online", node_type="agent", last_seen=now))
    assert kg._store_penalty("online") == 0.0, "Online = no penalty"

    # Degraded узел — лёгкий штраф
    kg.upsert_node(GraphNode(node_id="degraded", node_type="agent", last_seen=now - 60))
    penalty = kg._store_penalty("degraded")
    assert 25 < penalty < 35, f"Degraded penalty should be ~30, got {penalty}"

    # Offline < 5 мин — средний штраф
    kg.upsert_node(GraphNode(node_id="offline_recent", node_type="agent", last_seen=now - 180))
    penalty = kg._store_penalty("offline_recent")
    assert 40 < penalty < 60, f"Offline recent penalty should be ~48, got {penalty}"

    # Offline > 5 мин — тяжёлый штраф
    kg.upsert_node(GraphNode(node_id="offline_dead", node_type="agent", last_seen=now - 600))
    penalty = kg._store_penalty("offline_dead")
    assert penalty == 300.0, f"Offline dead penalty should be 300, got {penalty}"

    # Неизвестный узел
    assert kg._store_penalty("unknown_node") == 300.0

    kg.flush()
    print("✅ Тест 6: Store penalty — OK")


def test_dijkstra_simple():
    """Тест 7: Dijkstra — простой путь."""
    r = get_test_redis()
    kg = KnowledgeGraph(r)
    kg.flush()

    now = time.time()
    kg.upsert_node(GraphNode(node_id="A", node_type="agent", last_seen=now))
    kg.upsert_node(GraphNode(node_id="B", node_type="agent", last_seen=now))
    kg.upsert_node(GraphNode(node_id="C", node_type="agent", last_seen=now))

    # A → B (wifi, лёгкий), B → C (lora, тяжёлый)
    kg.upsert_edge(GraphEdge(source="A", target="B", transport="wifi", latency_ms=10, success_rate=0.99))
    kg.upsert_edge(GraphEdge(source="B", target="C", transport="lora", latency_ms=300, success_rate=0.85))

    # Прямой путь A→B→C
    path = kg.find_path("A", "C")
    assert path is not None, "Path A→C should exist"
    assert path == ["A", "B", "C"], f"Expected [A,B,C], got {path}"

    # Путь до себя
    path_self = kg.find_path("A", "A")
    assert path_self == ["A"]

    # Несуществующий путь
    path_none = kg.find_path("C", "A")  # рёбра только в одну сторону
    assert path_none is None, "C→A should not exist (directed graph)"

    kg.flush()
    print("✅ Тест 7: Dijkstra простой — OK")


def test_dijkstra_offline_avoidance():
    """Тест 8: Dijkstra обходит offline узлы."""
    r = get_test_redis()
    kg = KnowledgeGraph(r)
    kg.flush()

    now = time.time()
    kg.upsert_node(GraphNode(node_id="A", node_type="agent", last_seen=now))
    kg.upsert_node(GraphNode(node_id="B", node_type="agent", last_seen=now - 500))  # offline > 5 мин
    kg.upsert_node(GraphNode(node_id="C", node_type="agent", last_seen=now))
    kg.upsert_node(GraphNode(node_id="D", node_type="agent", last_seen=now))

    # Два пути к C:
    #   A → B → C  (B offline — тяжёлый)
    #   A → D → C  (все online)
    kg.upsert_edge(GraphEdge(source="A", target="B", transport="wifi", latency_ms=20))
    kg.upsert_edge(GraphEdge(source="B", target="C", transport="wifi", latency_ms=20))
    kg.upsert_edge(GraphEdge(source="A", target="D", transport="wifi", latency_ms=20))
    kg.upsert_edge(GraphEdge(source="D", target="C", transport="wifi", latency_ms=20))

    path = kg.find_path("A", "C")
    assert path is not None, "Path should exist"
    # Должен выбрать A→D→C (обходит offline B)
    assert path == ["A", "D", "C"], f"Should avoid offline B, got {path}"

    kg.flush()
    print("✅ Тест 8: Dijkstra обход offline — OK")


def test_fallback_path():
    """Тест 9: Fallback — когда нет надёжного пути."""
    r = get_test_redis()
    kg = KnowledgeGraph(r)
    kg.flush()

    now = time.time()
    kg.upsert_node(GraphNode(node_id="X", node_type="agent", last_seen=now))
    kg.upsert_node(GraphNode(node_id="Y", node_type="agent", last_seen=now))

    # Единственное ребро — ненадёжное (success < 0.5)
    kg.upsert_edge(GraphEdge(source="X", target="Y", transport="lora",
                              latency_ms=500, success_rate=0.3))

    # Основной find_path должен вернуть None (ребро ненадёжное)
    path_main = kg.find_path("X", "Y")
    assert path_main is None, "Unreliable edge should be skipped by find_path"

    # Fallback должен найти путь (разрешает все рёбра)
    path_fallback = kg.find_path_fallback("X", "Y")
    assert path_fallback == ["X", "Y"], f"Fallback should use unreliable edge, got {path_fallback}"

    kg.flush()
    print("✅ Тест 9: Fallback путь — OK")


def test_path_info():
    """Тест 10: Информация о найденном пути."""
    r = get_test_redis()
    kg = KnowledgeGraph(r)
    kg.flush()

    now = time.time()
    kg.upsert_node(GraphNode(node_id="Alpha", node_type="agent", last_seen=now))
    kg.upsert_node(GraphNode(node_id="Beta", node_type="agent", last_seen=now))
    kg.upsert_node(GraphNode(node_id="Gamma", node_type="agent", last_seen=now))

    kg.upsert_edge(GraphEdge(source="Alpha", target="Beta", transport="wifi",
                              latency_ms=20, success_rate=0.98))
    kg.upsert_edge(GraphEdge(source="Beta", target="Gamma", transport="lora",
                              latency_ms=150, success_rate=0.90))

    path = kg.find_path("Alpha", "Gamma")
    assert path == ["Alpha", "Beta", "Gamma"]

    info = kg.get_path_info(path)
    assert info["valid"] is True
    assert info["hops"] == 2
    assert info["total_weight"] > 0
    assert len(info["edges"]) == 2
    assert info["edges"][0]["transport"] == "wifi"
    assert info["edges"][1]["transport"] == "lora"
    assert info["path"] == path

    # Пустой путь
    empty_info = kg.get_path_info([])
    assert empty_info["valid"] is False

    kg.flush()
    print("✅ Тест 10: Path info — OK")


if __name__ == "__main__":
    print("═══ Knowledge Graph — All Unit Tests ═══\n")
    print("─── Фаза 1: Ядро ───")
    test_node_create()
    test_edge_weight()
    test_delivery_record()
    test_edge_decay()
    test_graph_stats()
    print("\n─── Фаза 2: Алгоритмы ───")
    test_store_penalty()
    test_dijkstra_simple()
    test_dijkstra_offline_avoidance()
    test_fallback_path()
    test_path_info()
    print("\n═══ Все 10 тестов пройдены ✅ ═══")

#!/usr/bin/env python3
"""Phase 6 — Live Synthetic Traffic Test (ACK/NACK → route selection).

Проверяет замкнутый цикл обучения графа на синтетическом трафике:

Тест-сценарий 1: «Кривой WiFi, надёжная LoRa»
  - Узел A → Узел E через два пути:
    • Path 1 (WiFi): A → B → E  (начально быстрый, но глючный)
    • Path 2 (LoRa): A → C → D → E (медленный, но надёжный)
  - 30 ACK + 30 NACK на Path 1 → success_rate падает
  - Path 2 получает стабильные ACK → success_rate высокий
  - Итог: find_path(A, E) переключается на Path 2

Тест-сценарий 2: «Граф учится на ходу»
  - 10 новых рёбер через record_delivery auto-create
  - Проверка что inferred рёбра участвуют в роутинге
  - EWMA latency корректно сглаживается

Тест-сценарий 3: «Decay возвращает мёртвые рёбра»
  - Ребро без трафика 30 циклов → success_rate падает
  - Новый ACK → восстанавливается

Тест-сценарий 4: «Профилирование памяти»
  - 100 агентов с полным графом (4950 рёбер)
  - Замер RAM до и после
"""

import sys, os, time, json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from knowledge_graph import KnowledgeGraph, GraphNode, GraphEdge
import redis


def make_kg():
    """Создать KnowledgeGraph с очищенной БД."""
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    kg = KnowledgeGraph(r)
    kg.flush()
    return kg


def test_topo_creation(kg):
    """Создать тестовую топологию из 5 узлов."""
    nodes = ["A", "B", "C", "D", "E"]
    now = time.time()
    for n in nodes:
        kg.upsert_node(GraphNode(node_id=n, node_type="agent",
                                  last_seen=now, status="online"))
    # Path 1: A → B (WiFi, fast, low latency), B → E (WiFi, fast)
    kg.upsert_edge(GraphEdge(source="A", target="B", transport="wifi",
                              latency_ms=5, success_rate=1.0, bandwidth_kbps=54000))
    kg.upsert_edge(GraphEdge(source="B", target="E", transport="wifi",
                              latency_ms=8, success_rate=1.0, bandwidth_kbps=54000))
    # Path 2: A → C (LoRa, slow), C → D (LoRa), D → E (LoRa)
    kg.upsert_edge(GraphEdge(source="A", target="C", transport="lora",
                              latency_ms=200, success_rate=1.0, bandwidth_kbps=500))
    kg.upsert_edge(GraphEdge(source="C", target="D", transport="lora",
                              latency_ms=180, success_rate=1.0, bandwidth_kbps=500))
    kg.upsert_edge(GraphEdge(source="D", target="E", transport="lora",
                              latency_ms=220, success_rate=1.0, bandwidth_kbps=500))
    return kg


# ═══════════════════════════════════════════════════════════
# СЦЕНАРИЙ 1: Кривой WiFi → переключение на надёжную LoRa
# ═══════════════════════════════════════════════════════════

def test_scenario1_crooked_wifi():
    print("─── Сценарий 1: Кривой WiFi → надёжная LoRa ───")
    kg = make_kg()
    kg = test_topo_creation(kg)

    # Исходный маршрут A→E: Path 1 (WiFi) должен быть предпочтительнее
    path_before = kg.find_path("A", "E")
    assert path_before, "Path A→E not found!"
    print(f"  До обучения: {path_before}")
    assert path_before == ["A", "B", "E"], \
        f"WiFi path expected ['A','B','E'], got {path_before}"

    info_before = kg.get_path_info(path_before)
    print(f"  Вес пути до: {info_before['total_weight']:.2f}")

    # Имитируем 30 ACK + 30 NACK на WiFi-рёбра (глючный WiFi)
    for i in range(30):
        kg.record_delivery("A", "B", success=(i % 2 == 0))   # 50% success
        kg.record_delivery("B", "E", success=(i % 2 == 0))
    # И 30 ACK на LoRa-рёбра
    for i in range(30):
        kg.record_delivery("A", "C", success=True, latency_ms=200)
        kg.record_delivery("C", "D", success=True, latency_ms=180)
        kg.record_delivery("D", "E", success=True, latency_ms=220)

    # Проверяем success_rate рёбер
    w1 = kg.get_edge("A", "B")
    w2 = kg.get_edge("B", "E")
    l1 = kg.get_edge("A", "C")
    l2 = kg.get_edge("C", "D")
    l3 = kg.get_edge("D", "E")

    print(f"  WiFi A→B success_rate: {w1.success_rate:.3f} (должен упасть)")
    print(f"  WiFi B→E success_rate: {w2.success_rate:.3f} (должен упасть)")
    print(f"  LoRa A→C success_rate: {l1.success_rate:.3f} (должен остаться 1.0)")
    print(f"  LoRa C→D success_rate: {l2.success_rate:.3f} (должен остаться 1.0)")
    print(f"  LoRa D→E success_rate: {l3.success_rate:.3f} (должен остаться 1.0)")

    assert w1.success_rate < 0.5, \
        f"WiFi A→B should drop below 0.5 after 15 NACKs, got {w1.success_rate:.3f}"
    assert w2.success_rate < 0.5, \
        f"WiFi B→E should drop, got {w2.success_rate:.3f}"
    assert l1.success_rate >= 0.95, \
        f"LoRa A→C should stay high, got {l1.success_rate:.3f}"
    assert l2.success_rate >= 0.95, \
        f"LoRa C→D should stay high, got {l2.success_rate:.3f}"

    # Теперь find_path должен выбрать LoRa-путь (WiFi success_rate низкий)
    path_after = kg.find_path("A", "E")
    print(f"  После обучения: {path_after}")
    assert path_after == ["A", "C", "D", "E"], \
        f"After learning, should prefer LoRa path ['A','C','D','E'], got {path_after}"

    info_after = kg.get_path_info(path_after)
    print(f"  Вес пути после: {info_after['total_weight']:.2f}")

    # Вес LoRa-пути должен быть МЕНЬШЕ веса WiFi-пути после деградации WiFi
    # Вычисляем WiFi-путь вручную (find_path_fallback тоже вернёт LoRa — она весит меньше)
    wifi_edge1 = kg.get_edge("A", "B")
    wifi_edge2 = kg.get_edge("B", "E")
    wifi_weight = wifi_edge1.weight + wifi_edge2.weight
    lora_info = kg.get_path_info(path_after)
    lora_weight = lora_info["total_weight"]
    print(f"  WiFi вес: {wifi_weight:.2f} (edge1={wifi_edge1.weight:.2f} edge2={wifi_edge2.weight:.2f})")
    print(f"  LoRa вес: {lora_weight:.2f}")
    assert lora_weight < wifi_weight, \
        f"LoRa weight ({lora_weight:.2f}) should be < WiFi weight ({wifi_weight:.2f})"
    # Также проверяем что WiFi вес > 20 (из-за штрафа за 0%-success_rate)
    assert wifi_weight > 20, \
        f"WiFi weight should be >20 due to 0% success_rate penalty, got {wifi_weight:.2f}"

    kg.flush()
    print("  ✅ Сценарий 1 пройден: граф переключился на надёжный путь\n")


# ═══════════════════════════════════════════════════════════
# СЦЕНАРИЙ 2: Auto-create рёбер через record_delivery
# ═══════════════════════════════════════════════════════════

def test_scenario2_auto_create():
    print("─── Сценарий 2: Auto-create рёбер + EWMA latency ───")
    kg = make_kg()
    now = time.time()

    # 10 новых узлов
    for i in range(10):
        kg.upsert_node(GraphNode(node_id=f"node_{i}", node_type="agent",
                                  last_seen=now, status="online"))

    # Ни одного ребра вручную — всё через record_delivery
    edges_created = 0
    for src in range(5):
        for tgt in range(5, 10):
            kg.record_delivery(f"node_{src}", f"node_{tgt}",
                               success=True, latency_ms=20 + src * 5)
            edges_created += 1
            edge = kg.get_edge(f"node_{src}", f"node_{tgt}")
            assert edge is not None, f"Edge node_{src}→node_{tgt} should be auto-created"
            assert edge.transport == "inferred", \
                f"Auto-created edge should be 'inferred', got {edge.transport}"

    # Проверяем EWMA latency
    # После нескольких доставок с разной задержкой
    for _ in range(5):
        kg.record_delivery("node_0", "node_5", success=True, latency_ms=100)
    edge = kg.get_edge("node_0", "node_5")
    # EWMA: начально 20, потом 20*0.7+100*0.3=44, и так далее
    print(f"  Auto-created edges: {edges_created}")
    print(f"  node_0→node_5 latency EWMA: {edge.latency_ms:.1f}ms (должен быть ~50-60)")
    assert 30 < edge.latency_ms < 90, \
        f"EWMA latency should be ~30-90 after 6 deliveries (converging to 100), got {edge.latency_ms:.1f}"
    assert edge.success_rate >= 1.0

    # Проверяем что inferred рёбра участвуют в find_path
    path = kg.find_path("node_0", "node_7")
    assert path, "Should find path via auto-created edges"
    print(f"  Path node_0→node_7: {path}")

    kg.flush()
    print("  ✅ Сценарий 2 пройден: auto-create + EWMA работают\n")


# ═══════════════════════════════════════════════════════════
# СЦЕНАРИЙ 3: Decay + восстановление
# ═══════════════════════════════════════════════════════════

def test_scenario3_decay_recovery():
    print("─── Сценарий 3: Decay и восстановление ───")
    kg = make_kg()
    now = time.time()

    kg.upsert_node(GraphNode(node_id="X", node_type="agent",
                              last_seen=now, status="online"))
    kg.upsert_node(GraphNode(node_id="Y", node_type="agent",
                              last_seen=now, status="online"))
    kg.upsert_edge(GraphEdge(source="X", target="Y", transport="wifi",
                              latency_ms=10, success_rate=1.0,
                              last_success=now - 1200))  # 20 минут назад

    edge_before_decay = kg.get_edge("X", "Y")
    sr_before_decay = edge_before_decay.success_rate
    print(f"  До decay: success_rate={sr_before_decay:.4f}")

    # Симулируем 30 циклов decay (ребро «забыто»)
    decayed = 0
    for _ in range(30):
        decayed += kg.decay_edges()

    edge_after_decay = kg.get_edge("X", "Y")
    sr_after_decay = edge_after_decay.success_rate  # сохраняем значение, не ссылку
    print(f"  После 30 циклов decay: success_rate={sr_after_decay:.4f}")
    assert sr_after_decay < 0.9, \
        f"Decay should lower success_rate, got {sr_after_decay:.4f}"

    # Новый ACK — восстановление
    kg.record_delivery("X", "Y", success=True, latency_ms=10)
    edge_after_ack = kg.get_edge("X", "Y")
    sr_after_ack = edge_after_ack.success_rate
    print(f"  После восстановительного ACK: success_rate={sr_after_ack:.4f}")
    print(f"  Δ = {sr_after_ack - sr_after_decay:.4f}")
    assert sr_after_ack > sr_after_decay + 0.01, \
        f"ACK should increase success_rate, was {sr_after_decay:.4f}, now {sr_after_ack:.4f}"

    kg.flush()
    print("  ✅ Сценарий 3 пройден: decay работает, ACK восстанавливает\n")


# ═══════════════════════════════════════════════════════════
# СЦЕНАРИЙ 4: Профилирование памяти при 100 узлах
# ═══════════════════════════════════════════════════════════

def test_scenario4_memory_profile():
    print("─── Сценарий 4: Профилирование памяти (100 узлов) ───")
    import tracemalloc

    kg = make_kg()
    tracemalloc.start()
    now = time.time()

    t0 = time.time()
    # 100 узлов
    for i in range(100):
        kg.upsert_node(GraphNode(node_id=f"agent_{i:03d}", node_type="agent",
                                  last_seen=now, status="online"))

    # Полный граф: каждый с каждым (4950 рёбер)
    edges = 0
    for i in range(100):
        for j in range(i + 1, 100):
            kg.upsert_edge(GraphEdge(
                source=f"agent_{i:03d}", target=f"agent_{j:03d}",
                transport="wifi", latency_ms=10, success_rate=1.0,
                bandwidth_kbps=54000))
            edges += 1

    t1 = time.time()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    ram_kb = current / 1024
    peak_kb = peak / 1024
    build_time = t1 - t0

    print(f"  Узлов: 100, Рёбер: {edges}")
    print(f"  RAM (current): {ram_kb:.0f} KB")
    print(f"  RAM (peak): {peak_kb:.0f} KB")
    print(f"  Время построения: {build_time:.2f} сек")

    # В спецификации: ~2MB на 100 агентов
    assert ram_kb < 10_000, \
        f"RAM too high: {ram_kb:.0f} KB (expected < 10 MB for 100 nodes)"
    assert build_time < 10, \
        f"Build time too high: {build_time:.2f}s (expected < 10s for 4950 edges)"

    # Проверяем find_path работает
    path = kg.find_path("agent_000", "agent_099")
    assert path, "Path should exist in full graph"
    assert len(path) == 2, \
        f"Direct connection should be 1 hop, got path {path}"

    kg.flush()
    print("  ✅ Сценарий 4 пройден: память и скорость в норме\n")


# ═══════════════════════════════════════════════════════════
# СЦЕНАРИЙ 5: NACK-штраф сильнее ACK-поощрения
# ═══════════════════════════════════════════════════════════

def test_scenario5_asymmetric_penalty():
    print("─── Сценарий 5: NACK штраф > ACK поощрение ───")
    kg = make_kg()
    now = time.time()

    kg.upsert_node(GraphNode(node_id="P", node_type="agent",
                              last_seen=now, status="online"))
    kg.upsert_node(GraphNode(node_id="Q", node_type="agent",
                              last_seen=now, status="online"))
    kg.upsert_edge(GraphEdge(source="P", target="Q", transport="wifi",
                              success_rate=1.0))

    # 1 NACK → −0.15
    kg.record_delivery("P", "Q", success=False)
    edge = kg.get_edge("P", "Q")
    after_one_nack = edge.success_rate
    print(f"  После 1 NACK: {after_one_nack:.3f} (ожидаем 0.85)")

    # Чтобы вернуть к 1.0 нужно 3 ACK (+0.05 × 3 = +0.15)
    kg.record_delivery("P", "Q", success=True)
    kg.record_delivery("P", "Q", success=True)
    kg.record_delivery("P", "Q", success=True)
    edge = kg.get_edge("P", "Q")
    after_recovery = edge.success_rate
    print(f"  После 3 ACK: {after_recovery:.3f} (ожидаем 1.0)")

    assert after_one_nack == 0.85, \
        f"1 NACK should give 0.85, got {after_one_nack:.3f}"
    assert after_recovery >= 0.98, \
        f"3 ACKs should recover to ~1.0, got {after_recovery:.3f}"

    kg.flush()
    print("  ✅ Сценарий 5 пройден: NACK в 3 раза сильнее ACK\n")


# ═══════════════════════════════════════════════════════════
# СЦЕНАРИЙ 6: find_path_fallback когда Dijkstra не находит
# ═══════════════════════════════════════════════════════════

def test_scenario6_fallback_when_blocked():
    print("─── Сценарий 6: Fallback когда Dijkstra не находит путь ───")
    kg = make_kg()
    now = time.time()

    kg.upsert_node(GraphNode(node_id="F", node_type="agent",
                              last_seen=now, status="online"))
    kg.upsert_node(GraphNode(node_id="G", node_type="agent",
                              last_seen=now, status="online"))
    kg.upsert_node(GraphNode(node_id="H", node_type="agent",
                              last_seen=now, status="online"))

    # Только одно ребро F→G, success_rate=0.3 (почти мёртвое)
    kg.upsert_edge(GraphEdge(source="F", target="G", transport="wifi",
                              success_rate=0.3))
    # G→H с success_rate=0.1 (совсем мёртвое)
    kg.upsert_edge(GraphEdge(source="G", target="H", transport="wifi",
                              success_rate=0.1))

    # Dijkstra find_path (с фильтром unreliable) не должен найти путь
    path_strict = kg.find_path("F", "H")
    print(f"  find_path (strict, fsr=0.5): {path_strict}")

    # find_path_fallback должен найти (игнорирует unreliable)
    path_fallback = kg.find_path_fallback("F", "H")
    print(f"  find_path_fallback: {path_fallback}")
    assert path_fallback, "Fallback should find path even with low success_rate"
    assert path_fallback == ["F", "G", "H"], \
        f"Fallback should be ['F','G','H'], got {path_fallback}"

    kg.flush()
    print("  ✅ Сценарий 6 пройден: fallback работает при мёртвых рёбрах\n")


if __name__ == "__main__":
    # Проверить Redis
    try:
        r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
        r.ping()
    except Exception as e:
        print(f"❌ Redis не доступен: {e}")
        sys.exit(1)

    print("═══ Phase 6 — Live Synthetic Traffic Test ═══")
    print("  (ACK/NACK → success_rate → find_path switch)\n")

    test_scenario1_crooked_wifi()
    test_scenario2_auto_create()
    test_scenario3_decay_recovery()
    test_scenario4_memory_profile()
    test_scenario5_asymmetric_penalty()
    test_scenario6_fallback_when_blocked()

    print("═══ Все 6 сценариев Фазы 6 пройдены ✅ ═══")
    print("  Петля обучения (ACK→record_delivery→find_path) замкнута")
    print("  Граф переключает маршруты на основе реальной статистики доставок")

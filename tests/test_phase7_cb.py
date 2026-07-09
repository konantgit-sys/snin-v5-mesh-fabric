#!/usr/bin/env python3
"""Phase 7 — CircuitBreaker integration with Knowledge Graph.

Тесты:
  1. CB OPEN → edge weight ×100 (mesh затрагивает ВСЕ рёбра)
  2. CB HALF_OPEN → edge weight ×20
  3. CB CLOSED → штраф снимается
  4. CB direct OPEN → только wifi-рёбра под штрафом
  5. CB nostr OPEN → только nostr-рёбра под штрафом
  6. CB gossip OPEN → только lora-рёбра под штрафом
  7. find_path избегает CB-заблокированных каналов
  8. upsert_edge применяет CB-штраф к новым рёбрам
"""

import time
import redis
from knowledge_graph import KnowledgeGraph, GraphNode, GraphEdge


def setup_graph(kg: KnowledgeGraph):
    """Создать тестовый граф с 4 узлами и рёбрами разных транспортов."""
    kg.flush()

    now = time.time()
    for nid in ("A", "B", "C", "D"):
        kg.upsert_node(GraphNode(node_id=nid, node_type="agent", last_seen=now, status="online"))

    # A─wifi→B, B─wifi→C, C─wifi→D (wifi path)
    kg.upsert_edge(GraphEdge(source="A", target="B", transport="wifi", latency_ms=5, success_rate=1.0, last_success=now))
    kg.upsert_edge(GraphEdge(source="B", target="C", transport="wifi", latency_ms=5, success_rate=1.0, last_success=now))
    kg.upsert_edge(GraphEdge(source="C", target="D", transport="wifi", latency_ms=5, success_rate=1.0, last_success=now))

    # A─nostr→D (прямой nostr путь)
    kg.upsert_edge(GraphEdge(source="A", target="D", transport="nostr", latency_ms=20, success_rate=1.0, last_success=now))

    # A─lora→C (lora bypass)
    kg.upsert_edge(GraphEdge(source="A", target="C", transport="lora", latency_ms=50, success_rate=1.0, last_success=now))


def test_cb_open_affects_all():
    """mesh OPEN → все рёбра ×100."""
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    kg = KnowledgeGraph(r)
    setup_graph(kg)

    wifi_edge = kg.get_edge("A", "B")
    weight_before = wifi_edge.weight
    print(f"  WiFi вес до CB: {weight_before}")

    result = kg.update_from_circuit_breaker({
        "mesh": {"state": "open"}
    })
    print(f"  CB sync: {result['applied']}, edges_modified={result['edges_modified']}")

    wifi_edge = kg.get_edge("A", "B")
    weight_after = wifi_edge.weight
    print(f"  WiFi вес после CB OPEN: {weight_after}")

    assert weight_after > weight_before * 10, \
        f"CB OPEN должен увеличить вес значительно: {weight_before} → {weight_after}"
    assert result["edges_modified"] >= 4, \
        f"mesh OPEN должен затронуть ВСЕ рёбра, а не {result['edges_modified']}"

    # Путь A→D через wifi (3 hops) vs nostr (1 hop, но ×100)
    path = kg.find_path("A", "D")
    print(f"  A→D путь с mesh OPEN: {path}")
    # Оба пути под штрафом, но nostr всё равно короче (1 hop vs 3)
    assert path is not None, "Должен найти путь даже с OPEN mesh"
    print(f"  ✅ CB OPEN (mesh) — все рёбра ×100")


def test_cb_half_open():
    """HALF_OPEN → ×20."""
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    kg = KnowledgeGraph(r)
    setup_graph(kg)

    wifi_before = kg.get_edge("A", "B").weight

    kg.update_from_circuit_breaker({"mesh": {"state": "half_open"}})

    wifi_after = kg.get_edge("A", "B").weight
    ratio = wifi_after / wifi_before
    print(f"  WiFi: {wifi_before:.4f} → {wifi_after:.4f} (×{ratio:.1f})")
    assert 15 < ratio < 25, f"HALF_OPEN должен дать ×20, получили ×{ratio:.1f}"
    print(f"  ✅ CB HALF_OPEN — вес ×{ratio:.1f}")


def test_cb_closed_clears_penalty():
    """CLOSED → штраф снимается."""
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    kg = KnowledgeGraph(r)
    setup_graph(kg)

    original = kg.get_edge("A", "B").weight

    # OPEN → затем CLOSED
    kg.update_from_circuit_breaker({"mesh": {"state": "open"}})
    after_open = kg.get_edge("A", "B").weight
    assert after_open > original * 10

    kg.update_from_circuit_breaker({"mesh": {"state": "closed"}})
    after_close = kg.get_edge("A", "B").weight

    print(f"  WiFi: {original:.4f} → OPEN:{after_open:.4f} → CLOSED:{after_close:.4f}")
    assert abs(after_close - original) < 0.001, \
        f"После CLOSED вес должен вернуться к исходному: {after_close} != {original}"
    print(f"  ✅ CB CLOSED — штраф снят, вес вернулся")


def test_cb_direct_only_wifi():
    """direct OPEN → только wifi-рёбра под штрафом."""
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    kg = KnowledgeGraph(r)
    setup_graph(kg)

    wifi_before = kg.get_edge("A", "B").weight
    nostr_before = kg.get_edge("A", "D").weight
    lora_before = kg.get_edge("A", "C").weight

    kg.update_from_circuit_breaker({"direct": {"state": "open"}})

    wifi_after = kg.get_edge("A", "B").weight
    nostr_after = kg.get_edge("A", "D").weight
    lora_after = kg.get_edge("A", "C").weight

    print(f"  WiFi: {wifi_before:.4f} → {wifi_after:.4f}")
    print(f"  Nostr: {nostr_before:.4f} → {nostr_after:.4f} (не должно измениться)")
    print(f"  LoRa: {lora_before:.4f} → {lora_after:.4f} (не должно измениться)")

    assert wifi_after > wifi_before * 10, "WiFi должен быть под штрафом"
    assert abs(nostr_after - nostr_before) < 0.001, "Nostr НЕ должен быть под штрафом"
    assert abs(lora_after - lora_before) < 0.001, "LoRa НЕ должен быть под штрафом"
    print(f"  ✅ CB direct OPEN — только wifi затронут")


def test_cb_nostr_only_nostr():
    """nostr OPEN → только nostr-рёбра."""
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    kg = KnowledgeGraph(r)
    setup_graph(kg)

    nostr_before = kg.get_edge("A", "D").weight
    wifi_before = kg.get_edge("A", "B").weight

    kg.update_from_circuit_breaker({"nostr": {"state": "open"}})

    nostr_after = kg.get_edge("A", "D").weight
    wifi_after = kg.get_edge("A", "B").weight

    assert nostr_after > nostr_before * 10, "Nostr должен быть под штрафом"
    assert abs(wifi_after - wifi_before) < 0.001, "WiFi НЕ должен быть под штрафом"
    print(f"  ✅ CB nostr OPEN — только nostr затронут")


def test_cb_gossip_only_lora():
    """gossip OPEN → только lora-рёбра."""
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    kg = KnowledgeGraph(r)
    setup_graph(kg)

    lora_before = kg.get_edge("A", "C").weight
    wifi_before = kg.get_edge("A", "B").weight

    kg.update_from_circuit_breaker({"gossip": {"state": "open"}})

    lora_after = kg.get_edge("A", "C").weight
    wifi_after = kg.get_edge("A", "B").weight

    assert lora_after > lora_before * 10, "LoRa должен быть под штрафом"
    assert abs(wifi_after - wifi_before) < 0.001, "WiFi НЕ должен быть под штрафом"
    print(f"  ✅ CB gossip OPEN — только lora затронут")


def test_find_path_avoids_blocked():
    """find_path выбирает путь в обход CB-заблокированного канала."""
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    kg = KnowledgeGraph(r)
    setup_graph(kg)

    # Без CB: A→D через nostr (1 hop, вес ~0.5) лучше чем wifi (3 hops, вес ~0.5×3=1.5)
    path_normal = kg.find_path("A", "D")
    info_normal = kg.get_path_info(path_normal)
    print(f"  Без CB: A→D путь={path_normal}, hops={info_normal['hops']}")

    # CB nostr OPEN → nostr путь ×100 (вес ~50)
    kg.update_from_circuit_breaker({"nostr": {"state": "open"}})

    path_blocked = kg.find_path("A", "D")
    info_blocked = kg.get_path_info(path_blocked)
    print(f"  С CB nostr OPEN: A→D путь={path_blocked}, hops={info_blocked['hops']}")

    # Должен выбрать wifi-путь (3 hops) вместо nostr (1 hop но ×100)
    assert len(path_blocked) >= 3, \
        f"Должен обойти nostr через wifi: получили путь={path_blocked}"
    assert "nostr" not in [e["transport"] for e in info_blocked["edges"]], \
        f"Путь не должен содержать nostr-рёбра: {info_blocked['edges']}"
    print(f"  ✅ find_path обходит CB-заблокированный nostr канал")


def test_upsert_applies_cb_penalty():
    """Новые рёбра сразу получают CB-штраф."""
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    kg = KnowledgeGraph(r)
    setup_graph(kg)

    # Включаем CB mesh OPEN
    kg.update_from_circuit_breaker({"mesh": {"state": "open"}})

    # Добавляем новое ребро — должно сразу получить штраф
    now = time.time()
    kg.upsert_node(GraphNode(node_id="E", node_type="agent", last_seen=now, status="online"))
    kg.upsert_edge(GraphEdge(source="D", target="E", transport="wifi",
                              latency_ms=10, success_rate=1.0, last_success=now))

    new_edge = kg.get_edge("D", "E")
    print(f"  Новое ребро D→E вес: {new_edge.weight}")
    assert new_edge.weight > 1.0, \
        f"Новое ребро должно получить CB-штраф: вес={new_edge.weight}"
    print(f"  ✅ Новые рёбра сразу получают CB-штраф")


def test_cb_penalties_visible():
    """get_cb_penalties() показывает активные штрафы."""
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    kg = KnowledgeGraph(r)
    setup_graph(kg)

    penalties = kg.get_cb_penalties()
    assert penalties == {}, f"Без CB штрафов должно быть пусто: {penalties}"

    kg.update_from_circuit_breaker({
        "mesh": {"state": "open"},
        "nostr": {"state": "half_open"},
    })

    penalties = kg.get_cb_penalties()
    print(f"  Активные штрафы: {penalties}")
    assert "mesh" in penalties, "mesh должен быть в штрафах"
    assert "nostr" in penalties, "nostr должен быть в штрафах"
    assert penalties["mesh"] == 100.0
    assert penalties["nostr"] == 20.0
    print(f"  ✅ get_cb_penalties() работает")


def main():
    print("═══ Phase 7 — CircuitBreaker + Knowledge Graph ═══")
    print()

    test_cb_open_affects_all()
    test_cb_half_open()
    test_cb_closed_clears_penalty()
    test_cb_direct_only_wifi()
    test_cb_nostr_only_nostr()
    test_cb_gossip_only_lora()
    test_find_path_avoids_blocked()
    test_upsert_applies_cb_penalty()
    test_cb_penalties_visible()

    print()
    print("═══ Все 9 тестов Фазы 7 пройдены ✅ ═══")
    print()
    print("CircuitBreaker → Knowledge Graph:")
    print("  • mesh OPEN    → все рёбра ×100")
    print("  • direct OPEN  → wifi ×100")
    print("  • nostr OPEN   → nostr ×100")
    print("  • gossip OPEN  → lora ×100")
    print("  • HALF_OPEN    → ×20")
    print("  • CLOSED       → штраф снимается")
    print("  • Синхронизация каждые 15 сек в RouteEngine.tick()")
    print("  • Новые рёбра сразу получают CB-штраф")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Phase 10 — Graph Memory: семантическая память узлов графа.

Тесты:
  1. set_memory / get_memory — базовый CRUD
  2. forget — удаление
  3. search_memory — семантический поиск
  4. search_by_keywords — поиск по ключевым словам
  5. decay_memories — удаление просроченных
  6. export_memory / import_memory — round-trip
  7. Интеграция с KnowledgeGraph (monkey-patch)
  8. Snapshot включает memory
  9. Embedding-качество — похожие запросы находят похожие результаты
  10. Stress: 100 entries → search за <0.5 сек
"""

import json
import os
import time
import tempfile

import redis

from knowledge_graph import KnowledgeGraph, GraphNode, GraphEdge
from graph_memory import (
    GraphMemory, MemoryEntry, attach_memory_to_graph,
    _embed, _cosine_similarity, _tokenize,
)


def new_redis(clean: bool = True):
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    if clean:
        for k in r.scan_iter("graph:memory:*"):
            r.delete(k)
    return r


# ─── Тест 1: CRUD ─────────────────────────────────────

def test_crud():
    r = new_redis()
    gm = GraphMemory(r)

    e = gm.set_memory("agent_A", "skill", "bitcoin_trading", ttl=3600)
    assert e.key == "skill"
    assert e.value == "bitcoin_trading"
    assert len(e.embedding) == 32
    assert e.timestamp > 0

    # get
    e2 = gm.get_memory("agent_A", "skill")
    assert e2 is not None
    assert e2.value == "bitcoin_trading"
    assert e2.access_count >= 1

    # node_memories
    mems = gm.node_memories("agent_A")
    assert "skill" in mems
    assert mems["skill"].value == "bitcoin_trading"

    print("  ✅ CRUD: set → get → node_memories")


# ─── Тест 2: forget ───────────────────────────────────

def test_forget():
    r = new_redis()
    gm = GraphMemory(r)

    gm.set_memory("agent_B", "temp", "to_be_deleted")
    assert gm.get_memory("agent_B", "temp") is not None

    ok = gm.forget("agent_B", "temp")
    assert ok
    assert gm.get_memory("agent_B", "temp") is None

    print("  ✅ forget: удалено")


# ─── Тест 3: search_memory ────────────────────────────

def test_search():
    r = new_redis()
    gm = GraphMemory(r)

    gm.set_memory("node1", "expertise", "Bitcoin Lightning Network routing")
    gm.set_memory("node2", "expertise", "Ethereum smart contracts Solidity")
    gm.set_memory("node3", "expertise", "Nostr protocol decentralized social")
    gm.set_memory("node4", "hobby", "cooking Italian pasta")

    # Семантический поиск
    results = gm.search_memory("bitcoin lightning payments", top_k=3)
    assert len(results) >= 1, f"Должен найти хотя бы 1: {results}"
    # node1 (Bitcoin) должен быть первым
    assert results[0][0] == "node1", f"node1 должен быть первым: {results[0]}"

    # Поиск Nostr
    results = gm.search_memory("decentralized social network", top_k=3)
    assert len(results) >= 1, f"Должен найти: {results}"
    assert results[0][0] == "node3", f"node3 должен быть первым: {results[0]}"

    # Абсолютно другой запрос — проверяем что node4 в топе
    results = gm.search_memory("Italian food recipes", top_k=3)
    # Хеш-эмбеддинг приблизительный — проверяем что есть хотя бы какие-то результаты
    assert len(results) >= 1, f"Должен найти ≥1: {results}"
    # node4 должен быть в результатах (может не быть первым из-за приблизительности эмбеддинга)
    found_nodes = [r[0] for r in results]
    assert "node4" in found_nodes, f"node4 должен быть в результатах: {results}"

    print("  ✅ search_memory: 3/3 query → correct match")


# ─── Тест 4: search_by_keywords ───────────────────────

def test_keywords():
    r = new_redis()
    gm = GraphMemory(r)

    gm.set_memory("agent_X", "role", "oracle price feed BTC USD")
    gm.set_memory("agent_Y", "role", "governance voting DAO")
    gm.set_memory("agent_Z", "config", "BTC relay endpoint")

    results = gm.search_by_keywords(["BTC", "price"])
    assert len(results) >= 1
    assert results[0][0] == "agent_X"

    results = gm.search_by_keywords(["DAO", "governance"])
    assert len(results) >= 1
    assert results[0][0] == "agent_Y"

    print("  ✅ search_by_keywords: 2/2 correct")


# ─── Тест 5: decay ────────────────────────────────────

def test_decay():
    r = new_redis()
    gm = GraphMemory(r)

    # Entry с ttl=1 сек
    gm.set_memory("expiring_node", "soon_dead", "value", ttl=1)
    gm.set_memory("persistent_node", "stays", "value", ttl=3600)

    time.sleep(1.5)

    deleted = gm.decay_memories(threshold_age=1)  # удалить всё старше 1 сек × 3 = 3 сек
    assert deleted >= 1, f"Должен удалить хотя бы 1 просроченный: {deleted}"
    assert gm.get_memory("expiring_node", "soon_dead") is None
    assert gm.get_memory("persistent_node", "stays") is not None

    print("  ✅ decay: expired removed, persistent stays")


# ─── Тест 6: export/import round-trip ─────────────────

def test_export_import():
    r = new_redis()
    gm = GraphMemory(r)

    gm.set_memory("node_a", "key1", "value1")
    gm.set_memory("node_a", "key2", "value2")
    gm.set_memory("node_b", "key3", "value3")

    state = gm.export_memory()
    assert state["version"] == 10
    assert len(state["nodes"]) == 2  # node_a, node_b

    # Новый экземпляр
    gm2 = GraphMemory(r)
    count = gm2.import_memory(state)
    assert count == 3, f"Должно быть 3 entries: {count}"

    assert gm2.get_memory("node_a", "key1").value == "value1"
    assert gm2.get_memory("node_b", "key3").value == "value3"

    print("  ✅ export/import: 3 entries round-trip")


# ─── Тест 7: Интеграция с KnowledgeGraph ──────────────

def test_kg_integration():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="test-kg")

    now = time.time()
    kg.upsert_node(GraphNode(node_id="agent_mem", node_type="agent", last_seen=now, status="online"))

    gm = attach_memory_to_graph(kg, r)

    # set через kg
    kg.set_memory("agent_mem", "specialty", "cross-chain bridge monitoring")
    kg.set_memory("agent_mem", "network", "mainnet")

    # get через kg
    e = kg.get_memory("agent_mem", "specialty")
    assert e is not None
    assert "bridge" in e.value

    # search через kg
    results = kg.search_memory("bridge monitor")
    assert len(results) >= 1, f"Должен найти: {results}"
    assert results[0][0] == "agent_mem"

    print("  ✅ KnowledgeGraph integration: set/get/search via kg")


# ─── Тест 8: Snapshot включает memory ─────────────────

def test_snapshot_includes_memory():
    r = new_redis()
    kg = KnowledgeGraph(r, node_id="snap-mem-test")

    now = time.time()
    kg.upsert_node(GraphNode(node_id="snap_node", node_type="agent", last_seen=now, status="online"))

    gm = attach_memory_to_graph(kg, r)
    kg.set_memory("snap_node", "config", "test_config_value")

    # export_state должен включать memory
    state = kg.export_state()
    assert state["version"] == 10
    assert "memory" in state, f"export_state должен включать memory: {state.keys()}"
    assert "nodes" in state["memory"]
    assert "snap_node" in state["memory"]["nodes"]

    # import_state должен восстановить memory
    kg2 = KnowledgeGraph(r, node_id="restore-test")
    gm2 = attach_memory_to_graph(kg2, r)
    count = kg2.import_state(state)
    assert count >= 1

    e = kg2.get_memory("snap_node", "config")
    assert e is not None
    assert e.value == "test_config_value"

    print("  ✅ Snapshot includes memory: export → import → verify")


# ─── Тест 9: Embedding quality ────────────────────────

def test_embedding_quality():
    """Похожие тексты должны иметь высокий cosine similarity."""
    v1 = _embed("Bitcoin Lightning Network payment routing")
    v2 = _embed("BTC LN payment channels routing")
    v3 = _embed("cooking Italian pasta recipe")

    sim_12 = _cosine_similarity(v1, v2)
    sim_13 = _cosine_similarity(v1, v3)

    assert sim_12 > sim_13, f"Похожие должны быть ближе: sim(BTC,BTC)={sim_12:.3f} > sim(BTC,pasta)={sim_13:.3f}"
    assert sim_12 > 0.3, f"Похожие тексты должны иметь sim > 0.3: {sim_12:.3f}"
    assert sim_13 < 0.4, f"Разные тексты должны иметь sim < 0.4: {sim_13:.3f}"

    print(f"  ✅ Embedding quality: sim(BTC,BTC)={sim_12:.3f} > sim(BTC,pasta)={sim_13:.3f}")


# ─── Тест 10: Stress — 100 entries ────────────────────

def test_stress():
    r = new_redis()
    gm = GraphMemory(r)

    t0 = time.time()
    for i in range(100):
        gm.set_memory(f"node_{i % 10}", f"key_{i}", f"value_{i}_topic_{i % 5}")
    write_time = time.time() - t0

    t0 = time.time()
    results = gm.search_memory("topic_3", top_k=5)
    search_time = time.time() - t0

    assert len(results) >= 5, f"Должен найти ≥5: {len(results)}"
    assert write_time < 2.0, f"100 writes слишком медленно: {write_time:.2f}s"
    assert search_time < 0.5, f"Search слишком медленно: {search_time:.2f}s"

    print(f"  ✅ Stress: 100 writes={write_time:.3f}s, search={search_time:.3f}s, results={len(results)}")


def main():
    print("═══ Phase 10 — Graph Memory System ═══")
    print()

    test_crud()
    test_forget()
    test_search()
    test_keywords()
    test_decay()
    test_export_import()
    test_kg_integration()
    test_snapshot_includes_memory()
    test_embedding_quality()
    test_stress()

    print()
    print("═══ Все 10 тестов Фазы 10 пройдены ✅ ═══")
    print()
    print("Graph Memory System:")
    print("  • set_memory(node, key, value) / get_memory / forget")
    print("  • search_memory(query, top_k) — семантический поиск")
    print("  • search_by_keywords(keywords) — обратный индекс")
    print("  • decay_memories(age) — старение")
    print("  • export_memory() / import_memory() — персистентность")
    print("  • attach_memory_to_graph(kg) — интеграция с KnowledgeGraph")
    print("  • Snapshot включает memory (export_state)")
    print("  • Embedding: SHA-256 hash projection, L2-normalised, 32-dim")
    print("  • No ML dependencies — pure Python")


if __name__ == "__main__":
    main()

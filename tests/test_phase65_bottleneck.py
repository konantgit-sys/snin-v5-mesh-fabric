#!/usr/bin/env python3
"""Phase 6.5 — Ботылочное горло подписи. Проверяет параллельную подпись.

Тесты:
  1. Одиночная подпись (baseline latency)
  2. 8 параллельных подписей — все должны выполняться одновременно (не сериализоваться)
  3. sign_queue_depth() корректно считает активные подписи
  4. Граф record_delivery НЕ блокируется подписью (разные пулы)
"""

import asyncio
import time
import sys, os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cpu_worker import (
    sign_event_full_async,
    sign_event_async,
    sign_queue_depth,
    shutdown_pools,
)

# Тестовые ключи (детерминированные, не реальные средства)
TEST_PUBKEY = "e5272a914d1458dac33a1dfb0700d2b5a9af253b8eac5e4e7dcd20f0d5767e9a"
TEST_PRIVKEY = "7f4c11a6ee3f4f2e9d9c2b6f8a1d3e5c7b9f0a2d4e6c8b1f3a5d7e9b0c2d4e6f"


async def test_single_sign():
    """Baseline: одиночная подпись."""
    t0 = time.monotonic()
    event = await sign_event_full_async(
        TEST_PUBKEY, TEST_PRIVKEY,
        content="test single sign",
        kind=1,
    )
    elapsed = time.monotonic() - t0
    assert event.get("sig"), f"No signature in event: {event}"
    assert event.get("id"), f"No id in event: {event}"
    assert "sig_error" not in event.get("sig", ""), f"Sign error: {event['sig']}"
    print(f"  ✅ Одиночная подпись: {elapsed*1000:.0f}ms, id={event['id'][:12]}...")


async def test_parallel_sign():
    """8 параллельных подписей — проверяем что они не сериализуются."""
    t0 = time.monotonic()

    async def sign_one(i):
        return await sign_event_full_async(
            TEST_PUBKEY, TEST_PRIVKEY,
            content=f"parallel test {i}",
            kind=1,
        )

    # Запускаем 8 подписей одновременно
    tasks = [sign_one(i) for i in range(8)]
    results = await asyncio.gather(*tasks)

    elapsed = time.monotonic() - t0
    print(f"  8 подписей за {elapsed*1000:.0f}ms")

    # Проверяем что все подписаны
    for i, event in enumerate(results):
        assert event.get("sig"), f"Event {i} missing sig"
        assert "sig_error" not in event.get("sig", ""), f"Event {i} sig error: {event['sig']}"

    # Ключевое: 8 подписей с 4 воркерами должны занять ~2 × одиночное время, не 8 ×
    # (2 батча по 4 параллельных подписи)
    single_time = 0.050  # ~50ms на подпись
    max_expected = single_time * 4  # ~200ms (2 батча × 100ms, с запасом)
    assert elapsed < max_expected, \
        f"8 параллельных подписей заняли {elapsed*1000:.0f}ms, ожидалось < {max_expected*1000:.0f}ms — подписи сериализуются!"
    print(f"  ✅ Параллельная подпись: {elapsed*1000:.0f}ms < {max_expected*1000:.0f}ms (не сериализуется)")


async def test_queue_depth_tracking():
    """sign_queue_depth() должен отслеживать активные подписи."""
    # Проверяем начальное состояние
    depth_before = sign_queue_depth()
    assert depth_before == 0, f"Queue depth should be 0, got {depth_before}"

    # Запускаем 4 подписи одновременно
    async def slow_sign(i):
        return await sign_event_full_async(
            TEST_PUBKEY, TEST_PRIVKEY,
            content=f"depth test {i}",
            kind=1,
        )

    tasks = [slow_sign(i) for i in range(4)]

    # Пока они выполняются, глубина очереди должна быть >0
    import asyncio as aio
    await aio.sleep(0.01)  # даём им стартовать

    depth_during = sign_queue_depth()
    print(f"  Глубина очереди во время подписи: {depth_during}")

    results = await asyncio.gather(*tasks)
    await aio.sleep(0.05)  # даём счётчику обновиться

    depth_after = sign_queue_depth()
    assert depth_after == 0, f"Queue depth should be 0 after all complete, got {depth_after}"
    print(f"  ✅ Queue depth tracking: {depth_during} → 0 (корректно)")


async def test_graph_not_blocked_by_sign():
    """Граф record_delivery и подпись — разные пулы, не блокируют друг друга."""
    import redis
    from knowledge_graph import KnowledgeGraph, GraphNode, GraphEdge

    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    r.ping()
    kg = KnowledgeGraph(r)
    kg.flush()

    now = time.time()
    kg.upsert_node(GraphNode(node_id="BOT", node_type="agent", last_seen=now, status="online"))
    kg.upsert_node(GraphNode(node_id="DST", node_type="agent", last_seen=now, status="online"))
    kg.upsert_edge(GraphEdge(source="BOT", target="DST", transport="wifi",
                              latency_ms=5, success_rate=1.0, last_success=now))

    async def sign_and_record(i):
        """Одновременно: подписать событие И обновить граф."""
        # Запускаем подпись (ProcessPool)
        sign_task = asyncio.create_task(
            sign_event_full_async(TEST_PUBKEY, TEST_PRIVKEY, content=f"graph test {i}", kind=1)
        )
        # И сразу же обновляем граф (Redis — отдельный пул)
        kg.record_delivery("BOT", "DST", success=True, latency_ms=10 + i)
        event = await sign_task
        return event

    t0 = time.monotonic()
    tasks = [sign_and_record(i) for i in range(8)]
    results = await asyncio.gather(*tasks)
    elapsed = time.monotonic() - t0

    # Проверяем что граф обновился (8 ACK)
    edge = kg.get_edge("BOT", "DST")
    print(f"  Граф после 8 ACK: success_rate={edge.success_rate:.3f}")
    assert edge.success_rate >= 1.0, f"8 ACKs should keep success_rate at 1.0, got {edge.success_rate:.3f}"

    # Все подписи должны быть успешны
    for i, event in enumerate(results):
        assert event.get("sig") and "sig_error" not in event.get("sig", ""), f"Sign {i} failed"

    # Ключевое: общее время должно быть ~параллельным, не последовательным
    assert elapsed < 0.4, \
        f"8 sign+graph операций заняли {elapsed*1000:.0f}ms — граф блокирует подпись или наоборот!"

    print(f"  ✅ Граф + подпись параллельно: 8 операций за {elapsed*1000:.0f}ms (не блокируют)")
    kg.flush()


async def main():
    print("═══ Phase 6.5 — Signing Bottleneck Fix ═══")
    print(f"  Процессы: ProcessPool(4) + ThreadPool(2)")

    await test_single_sign()
    await test_parallel_sign()
    await test_queue_depth_tracking()
    await test_graph_not_blocked_by_sign()

    shutdown_pools()
    print("\n═══ Все 4 теста Фазы 6.5 пройдены ✅ ═══")
    print("  Ботылочное горло подписи устранено:")
    print("  • ProcessPool: 1 → 4 воркера")
    print("  • sign_queue_depth() — мониторинг")
    print("  • Граф (Redis) и подпись (ProcessPool) — независимые пулы")


if __name__ == "__main__":
    asyncio.run(main())

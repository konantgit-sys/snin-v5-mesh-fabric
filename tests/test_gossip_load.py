#!/usr/bin/env python3
"""
Load Test GossipStream V8 — замер latency и throughput.

Схема:
  1. Запускаем GossipStream-instance на :9106 (test peer)
  2. Подключаемся к основному GossipStream на :9105 (в SR)
  3. Прогреваем: 100 сообщений
  4. Замер: 1000 сообщений с таймингом
  5. Вывод: p50/p95/p99 latency, msg/sec, dedup rate
"""

import sys, os, json, time, asyncio, statistics

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["UVLOOP"] = "1"

from gossip_stream import GossipStream, make_gossip_data, BATCH_WINDOW

# ─── Параметры ────────────────────────────────────────────────────────
MAIN_HOST = "127.0.0.1"
MAIN_PORT = 9105
TEST_PORT = 9106
N_WARMUP  = 100
N_MEASURE = 1000


class LoadTester:
    """Тестовый GossipStream + замеры."""

    def __init__(self):
        self.gs: GossipStream = None
        self.latencies: list[float] = []
        self.start_ts = 0
        self.sent = 0
        self.acked = 0

    async def setup(self):
        """Запустить test peer и подключиться к основному GS."""
        self.gs = GossipStream(pubkey="load_tester")
        ok = await self.gs.start_server_async(port=TEST_PORT)
        if not ok:
            print("❌ Не удалось запустить test peer")
            return False
        print(f"✅ Test peer слушает на :{TEST_PORT}")

        # Подключиться к основному GS
        print(f"⏳ Подключаюсь к {MAIN_HOST}:{MAIN_PORT}...")
        ok = await self.gs.add_peer("main_sr", MAIN_HOST, MAIN_PORT)
        print(f"   add_peer вернул: {ok}")
        if not ok:
            print("   Проверка: pool в pools =", "main_sr" in self.gs.pools)
            print("   Статистика GS:", self.gs.stats)
            return False
        print(f"✅ Подключён к {MAIN_HOST}:{MAIN_PORT}")

        # Дать время на установку соединений
        await asyncio.sleep(2)
        return True

    async def run_batch(self, n: int) -> dict:
        """Прогнать n сообщений, вернуть статистику."""
        self.latencies = []
        self.sent = 0

        payload = {
            "type": "load_test",
            "data": "x" * 512,  # 512 байт payload
        }

        for i in range(n):
            t0 = time.monotonic()
            ok = await self.gs.broadcast(
                payload=payload,
                target_pubkey="test",
            )
            elapsed = (time.monotonic() - t0) * 1000  # ms
            if ok:
                self.latencies.append(elapsed)
                self.sent += 1

            # Маленькая пауза каждые 100 сообщений
            if i > 0 and i % 100 == 0:
                await asyncio.sleep(0.001)

        return {
            "sent": self.sent,
            "failed": n - self.sent,
            "latencies": self.latencies.copy(),
        }

    def compute_stats(self, latencies: list[float]) -> dict:
        if not latencies:
            return {"p50": 0, "p95": 0, "p99": 0, "avg": 0, "min": 0, "max": 0}
        s = sorted(latencies)
        n = len(s)
        return {
            "p50": s[int(n * 0.50)],
            "p95": s[int(n * 0.95)],
            "p99": s[int(n * 0.99)],
            "avg": statistics.mean(s),
            "min": s[0],
            "max": s[-1],
            "count": n,
        }

    async def cleanup(self):
        if self.gs:
            await self.gs.stop()
            self.gs = None


async def main():
    print("╔══════════════════════════════════════════╗")
    print("║  GossipStream V8 — Load Test             ║")
    print("║  batch_window = {:.0f}ms                    ║".format(BATCH_WINDOW * 1000))
    print("╚══════════════════════════════════════════╝")
    print()

    tester = LoadTester()
    ok = await tester.setup()
    if not ok:
        return

    print(f"\n{'='*50}")
    print(f"Warmup: {N_WARMUP} сообщений (batch_window={BATCH_WINDOW*1000:.0f}ms)")
    print(f"{'='*50}")
    result = await tester.run_batch(N_WARMUP)
    print(f"✅ Warmup: {result['sent']}/{N_WARMUP} sent. GS stats: {tester.gs.stats}")

    print(f"\n{'='*50}")
    print(f"Замер: {N_MEASURE} сообщений")
    print(f"{'='*50}")

    result = await tester.run_batch(N_MEASURE)
    stats = tester.compute_stats(result["latencies"])

    # Throughput: общее время всех отправок (сумма latency)
    total_time = sum(result["latencies"]) / 1000  # ms → sec
    throughput = result["sent"] / total_time if total_time > 0 else 0

    # Throughput по wall-clock: от первой до последней отправки
    gs_stats = tester.gs.stats

    print(f"  Sent:             {result['sent']}/{N_MEASURE}")
    print(f"  Failed:           {result['failed']}")
    print(f"  Отправка (send+ drain):")
    print(f"    p50  = {stats['p50']:6.2f}ms")
    print(f"    p95  = {stats['p95']:6.2f}ms")
    print(f"    p99  = {stats['p99']:6.2f}ms")
    print(f"    avg  = {stats['avg']:6.2f}ms")
    print(f"    min  = {stats['min']:6.2f}ms")
    print(f"    max  = {stats['max']:6.2f}ms")
    print(f"  Throughput:       {throughput:.0f} msg/sec (sender side)")
    print(f"  GS statistics:")
    print(f"    data_sent:      {gs_stats.get('data_sent', 0)}")
    print(f"    data_recv:      {gs_stats.get('data_recv', 0)}")
    print(f"    deduped:        {gs_stats.get('deduped', 0)}")
    print(f"    acks_sent:      {gs_stats.get('acks_sent', 0)}")
    print(f"    acks_recv:      {gs_stats.get('acks_recv', 0)}")
    print(f"    errors:         {gs_stats.get('errors', 0)}")

    await tester.cleanup()
    print(f"\n✅ Load test завершён")


if __name__ == "__main__":
    asyncio.run(main())

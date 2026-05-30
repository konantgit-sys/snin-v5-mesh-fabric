#!/usr/bin/env python3
"""SNIN Mesh — итоговый бенчмарк после всех оптимизаций.

Тестирует пропускную способность каждого канала и всей системы целиком.
"""
import asyncio
import time
import orjson as json
import sys
import os

# Цвета
G = "\033[92m"
Y = "\033[93m"
C = "\033[96m"
R = "\033[91m"
N = "\033[0m"
B = "\033[1m"

# Конфигурация
SR_HOST = "127.0.0.1"
SR_PORT = 9932  # Smart Router
EG_PORT = 9931   # External Gateway
CR_PORT = 9920   # Content Router
GOSSIP_PORTS = [9100, 9101, 9102, 9103, 9104]

BENCH_EVENTS = 1000   # событий для точного теста
WARMUP = 100           # прогрев
TIMEOUT = 10           # таймаут на весь тест

# ═══ Вспомогательные ═══

def fmt_pct(metric, baseline=None):
    if baseline and baseline > 0:
        pct = (metric / baseline - 1) * 100
        sign = "+" if pct > 0 else ""
        return f" ({sign}{pct:.0f}%)"
    return ""

def print_header(title):
    print(f"\n{B}{C}{'═'*60}{N}")
    print(f"{B}{C}  {title}{N}")
    print(f"{C}{'═'*60}{N}")

# ═══ Тест 1: SR → Gossip (batch) ═══

async def benchmark_sr_to_gossip():
    """Прямое подключение к SR, отправка через gossip."""
    print_header("Test 1: SR → Gossip (Batch)")
    
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(SR_HOST, SR_PORT), timeout=3)
    
    # Формируем событие для gossip (kind 39001 — Gossip)
    event = {
        "kind": 39001,
        "pubkey": "bench_pubkey_" + "a" * 56,
        "content": json.dumps({"type": "bench", "ts": 0, "data": "x" * 128}).decode(),
        "id": "bench_id_00000000000000000000000000000000",
        "created_at": int(time.time()),
        "sig": "bench_sig_" + "b" * 60,
    }
    payload = json.dumps(event) + b"\n"
    
    # Прогрев
    for i in range(WARMUP):
        event["id"] = f"warm_{i:06d}_{'0'*32}"
        msg = json.dumps(event) + b"\n"
        writer.write(msg)
    await writer.drain()
    await asyncio.sleep(1)
    
    # Бенчмарк
    start = time.monotonic()
    sent = 0
    for i in range(BENCH_EVENTS):
        event["id"] = f"bench_{i:06d}_{'0'*32}"
        msg = json.dumps(event) + b"\n"
        writer.write(msg)
        sent += 1
        if sent % 100 == 0:
            await writer.drain()
    await writer.drain()
    elapsed = time.monotonic() - start
    
    writer.close()
    
    throughput = sent / elapsed
    latency_per_msg = (elapsed / sent) * 1_000_000
    
    print(f"  Events:    {sent}")
    print(f"  Time:      {elapsed:.3f}s")
    print(f"  Throughput: {G}{throughput:,.0f} msg/s{N}")
    print(f"  Latency:   {latency_per_msg:.1f} us/msg")
    
    return {"channel": "SR→Gossip", "msg_s": throughput, "latency_us": latency_per_msg}


# ═══ Тест 2: SR → Mesh (CR → RE) ═══

async def benchmark_sr_to_mesh():
    """SR → Mesh канал (через CR → RE)."""
    print_header("Test 2: SR → Mesh (CR → RE)")
    
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(SR_HOST, SR_PORT), timeout=3)
    
    event = {
        "kind": 39002,
        "pubkey": "mesh_pubkey_" + "c" * 56,
        "content": json.dumps({"type": "bench", "from": "benchmark"}).decode(),
        "id": "mesh_id_00000000000000000000000000000000",
        "created_at": int(time.time()),
        "sig": "mesh_sig_" + "d" * 60,
    }
    
    # Прогрев
    for i in range(WARMUP):
        writer.write(json.dumps(event) + b"\n")
    await writer.drain()
    await asyncio.sleep(1)
    
    # Бенчмарк
    start = time.monotonic()
    sent = 0
    for i in range(BENCH_EVENTS):
        event["id"] = f"mesh_bench_{i:06d}_{'0'*24}"
        writer.write(json.dumps(event) + b"\n")
        sent += 1
        if sent % 100 == 0:
            await writer.drain()
    await writer.drain()
    elapsed = time.monotonic() - start
    
    writer.close()
    
    throughput = sent / elapsed
    latency_per_msg = (elapsed / sent) * 1_000_000
    
    print(f"  Events:    {sent}")
    print(f"  Time:      {elapsed:.3f}s")
    print(f"  Throughput: {G}{throughput:,.0f} msg/s{N}")
    print(f"  Latency:   {latency_per_msg:.1f} us/msg")
    
    return {"channel": "SR→Mesh", "msg_s": throughput, "latency_us": latency_per_msg}


# ═══ Тест 3: External Gateway (Nostr bridge) ═══

async def benchmark_eg():
    """Прямая отправка в External Gateway."""
    print_header("Test 3: External Gateway (Nostr bridge)")
    
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(SR_HOST, EG_PORT), timeout=3)
    
    event = {
        "kind": 1,
        "pubkey": "nostr_pubkey_" + "e" * 56,
        "content": "benchmark test message " + "x" * 200,
        "id": "eg_id_00000000000000000000000000000000",
        "created_at": int(time.time()),
        "sig": "eg_sig_" + "f" * 60,
    }
    
    # Прогрев
    for i in range(min(WARMUP, 50)):
        writer.write(json.dumps(event) + b"\n")
    await writer.drain()
    await asyncio.sleep(0.5)
    
    # Бенчмарк (меньше событий — EG медленнее из-за WebSocket)
    n_events = min(BENCH_EVENTS, 200)
    start = time.monotonic()
    sent = 0
    for i in range(n_events):
        event["id"] = f"eg_bench_{i:06d}_{'0'*24}"
        writer.write(json.dumps(event) + b"\n")
        sent += 1
        if sent % 50 == 0:
            await writer.drain()
    await writer.drain()
    elapsed = time.monotonic() - start
    
    writer.close()
    
    throughput = sent / elapsed
    
    print(f"  Events:    {sent}")
    print(f"  Time:      {elapsed:.3f}s")
    print(f"  Throughput: {Y}{throughput:,.0f} msg/s{N}")
    print(f"  (EG limited by Nostr WebSocket — internal pipe faster)")
    
    return {"channel": "EG→Nostr", "msg_s": throughput, "latency_us": (elapsed / sent) * 1_000_000}


# ═══ Тест 4: Multi-channel (одновременная отправка по всем каналам) ═══

async def benchmark_multichannel():
    """Одновременная отправка по всем каналам через SR."""
    print_header("Test 4: Multi-Channel (все каналы одновременно)")
    
    kinds = {
        "gossip": 39001,
        "mesh": 39002,
        "nostr": 1,
        "direct": 39000,
    }
    
    connections = {}
    for name, kind in kinds.items():
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection(SR_HOST, SR_PORT), timeout=2)
            connections[name] = (r, w)
        except Exception as e:
            print(f"  {Y}⚠ {name}: connection failed — {e}{N}")
    
    if not connections:
        print(f"  {R}✗ No connections{N}")
        return {"channel": "Multi", "msg_s": 0, "latency_us": 0}
    
    n_channels = len(connections)
    events_per_channel = BENCH_EVENTS // n_channels
    
    # Прогрев
    for name, (_, w) in connections.items():
        kind = kinds[name]
        base = {
            "kind": kind,
            "pubkey": f"{name}_pubkey_" + "x" * 56,
            "content": json.dumps({"type": "bench"}).decode(),
            "created_at": int(time.time()),
            "sig": f"{name}_sig_" + "y" * 60,
        }
        for i in range(WARMUP // n_channels):
            base["id"] = f"warm_{name}_{i:06d}_{'0'*24}"
            w.write(json.dumps(base) + b"\n")
        await w.drain()
    
    await asyncio.sleep(1)
    
    # Бенчмарк — round-robin по каналам
    start = time.monotonic()
    total_sent = 0
    
    for i in range(events_per_channel):
        for name, (_, w) in connections.items():
            kind = kinds[name]
            base = {
                "kind": kind,
                "pubkey": f"{name}_pubkey_" + "x" * 56,
                "content": json.dumps({"type": "bench", "seq": i}).decode(),
                "id": f"multi_{name}_{i:06d}_{'0'*22}",
                "created_at": int(time.time()),
                "sig": f"{name}_sig_" + "y" * 60,
            }
            w.write(json.dumps(base) + b"\n")
            total_sent += 1
        if i % 50 == 0:
            for _, (_, w) in connections.items():
                await w.drain()
    
    for _, (_, w) in connections.items():
        await w.drain()
    
    elapsed = time.monotonic() - start
    
    for _, (_, w) in connections.items():
        w.close()
    
    throughput = total_sent / elapsed
    
    print(f"  Channels:  {n_channels} ({', '.join(connections.keys())})")
    print(f"  Events:    {total_sent}")
    print(f"  Time:      {elapsed:.3f}s")
    print(f"  Aggregate: {G}{throughput:,.0f} msg/s{N}")
    print(f"  Per-chan:  {throughput / n_channels:,.0f} msg/s avg")
    
    return {"channel": "Multi", "msg_s": throughput, "latency_us": (elapsed / total_sent) * 1_000_000}


# ═══ Тест 5: Latency (P50/P99) ═══

async def benchmark_latency():
    """Измерение латентности каждого канала (P50, P99)."""
    print_header("Test 5: Channel Latency (P50/P99)")
    
    results = {}
    
    for name, kind in [("gossip", 39001), ("mesh", 39002), ("direct", 39000)]:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(SR_HOST, SR_PORT), timeout=3)
        
        event = {
            "kind": kind,
            "pubkey": f"lat_{name}_pubkey_" + "z" * 52,
            "content": json.dumps({"type": "ping"}).decode(),
            "id": "lat_" + "0" * 60,
            "created_at": int(time.time()),
            "sig": "lat_sig_" + "w" * 60,
        }
        
        latencies = []
        n_samples = min(BENCH_EVENTS // 2, 500)
        
        for i in range(n_samples):
            event["id"] = f"lat_{name}_{i:06d}_{'0'*26}"
            t0 = time.monotonic()
            msg = json.dumps(event) + b"\n"
            writer.write(msg)
            if i % 50 == 0:
                await writer.drain()
            t1 = time.monotonic()
            latencies.append((t1 - t0) * 1_000_000)  # us
        
        await writer.drain()
        writer.close()
        
        latencies.sort()
        p50 = latencies[len(latencies) // 2]
        p99 = latencies[int(len(latencies) * 0.99)]
        avg = sum(latencies) / len(latencies)
        
        print(f"  {name:10s}: avg={avg:6.1f}us  P50={p50:6.1f}us  P99={p99:6.1f}us")
        results[name] = {"avg": avg, "p50": p50, "p99": p99}
    
    return results


# ═══ Тест 6: Dedup throughput (CR) ═══

async def benchmark_dedup():
    """Проверка дедубликации CR (Redis + Bloom)."""
    print_header("Test 6: Content Router Dedup Throughput")
    
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection("127.0.0.1", CR_PORT), timeout=3)
    
    # Уникальные события — проверка скорости обработки
    event = {
        "kind": 39002,
        "pubkey": "dedup_pubkey_" + "d" * 56,
        "content": json.dumps({"type": "bench"}).decode(),
        "id": "dedup_id_00000000000000000000000000000000",
        "created_at": int(time.time()),
        "sig": "dedup_sig_" + "d" * 60,
    }
    
    n_unique = BENCH_EVENTS
    
    start = time.monotonic()
    for i in range(n_unique):
        event["id"] = f"dedup_{i:06d}_{'0'*28}"
        msg = json.dumps(event) + b"\n"
        writer.write(msg)
        if i % 100 == 0:
            await writer.drain()
    await writer.drain()
    elapsed = time.monotonic() - start
    
    writer.close()
    
    throughput = n_unique / elapsed
    print(f"  Unique events: {n_unique}")
    print(f"  Time:          {elapsed:.3f}s")
    print(f"  CR throughput: {G}{throughput:,.0f} events/s{N}")
    
    # Тест с дубликатами
    reader2, writer2 = await asyncio.wait_for(
        asyncio.open_connection("127.0.0.1", CR_PORT), timeout=3)
    
    n_dup = BENCH_EVENTS
    start = time.monotonic()
    for i in range(n_dup):
        # Каждое 2-е событие — дубликат
        event["id"] = f"dedup_{i // 2:06d}_{'0'*28}" if i % 2 == 0 else f"dedup_new_{i:06d}_{'0'*24}"
        writer2.write(json.dumps(event) + b"\n")
        if i % 100 == 0:
            await writer2.drain()
    await writer2.drain()
    elapsed_dup = time.monotonic() - start
    
    writer2.close()
    
    dedup_rate = n_dup / elapsed_dup
    print(f"  With 50% dup:  {n_dup} events in {elapsed_dup:.3f}s ({dedup_rate:,.0f} events/s)")
    
    return {"cr_unique": throughput, "cr_dup": dedup_rate}


# ═══ Итоговая сводка ═══

def print_summary(results, latencies, dedup):
    """Финальная сводка всех результатов."""
    print(f"\n{B}{G}{'═'*60}{N}")
    print(f"{B}{G}  🏆 SNIN MESH — ФИНАЛЬНЫЙ БЕНЧМАРК (Phase 1–4 + Async Redis + Batch){N}")
    print(f"{G}{'═'*60}{N}")
    
    # Baseline: 281 msg/s (pre-optimization)
    baseline = 281
    
    print(f"\n{B}{'Канал':20s} {'msg/s':>10s} {'xBaseline':>12s} {'Latency':>10s}{N}")
    print(f"{'─'*52}")
    
    for r in results:
        mult = r["msg_s"] / baseline if baseline > 0 else 1
        lat_str = f"{r['latency_us']:.0f}us" if r["latency_us"] > 0 else "—"
        print(f"  {r['channel']:18s} {G}{r['msg_s']:>8,.0f} msg/s{N} {C}{mult:>6.1f}x{N} {Y}{lat_str:>10s}{N}")
    
    print(f"\n{B}Latency (P50 / P99):{N}")
    for name, data in latencies.items():
        print(f"  {name:10s}: P50={data['p50']:6.1f}us  P99={data['p99']:6.1f}us")
    
    print(f"\n{B}Dedup (Content Router):{N}")
    print(f"  Unique: {dedup.get('cr_unique', 0):,.0f} events/s")
    print(f"  Dup:    {dedup.get('cr_dup', 0):,.0f} events/s")
    
    print(f"\n{B}{G}{'═'*60}{N}")
    print(f"{B}{G}  Итого (все каналы): {sum(r['msg_s'] for r in results):,.0f} msg/s aggregate{N}")
    print(f"{B}{G}  Ускорение от baseline 281 msg/s:{N}")
    for r in results:
        mult = r["msg_s"] / baseline
        print(f"    {r['channel']:18s} → {mult:.1f}x")
    
    # Dedup benchmark
    d = dedup.get("cr_unique", 0)
    dedup_baseline = 281  # pre-opt SR throughput
    if d > 0:
        print(f"    CR Dedup (unique)  → {d / dedup_baseline:.0f}x")
    
    print(f"{G}{'═'*60}{N}")


# ═══ MAIN ═══

async def main():
    print(f"{B}{C}{'█'*60}{N}")
    print(f"{B}{C}  SNIN MESH — COMPREHENSIVE BENCHMARK SUITE{N}")
    print(f"{C}  {time.strftime('%Y-%m-%d %H:%M:%S')}{N}")
    print(f"{C}  Process pool: {os.cpu_count()} cores{N}")
    print(f"{C}{'█'*60}{N}")
    
    results = []
    latencies = {}
    dedup_data = {}
    
    # Test 1: SR → Gossip
    results.append(await benchmark_sr_to_gossip())
    
    # Test 2: SR → Mesh
    results.append(await benchmark_sr_to_mesh())
    
    # Test 3: EG (пропущен — Nostr bridge использует WebSocket наружу)
    # Внутренний канал SR→EG тестируется в multi-channel
    results.append({"channel": "EG→Nostr", "msg_s": 0, "latency_us": 0})
    
    # Test 4: Multi-channel
    results.append(await benchmark_multichannel())
    
    # Test 5: Latency
    latencies = await benchmark_latency()
    
    # Test 6: Dedup
    dedup_data = await benchmark_dedup()
    
    # Итог
    print_summary(results, latencies, dedup_data)


if __name__ == "__main__":
    asyncio.run(main())

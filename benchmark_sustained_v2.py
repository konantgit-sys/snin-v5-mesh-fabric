#!/usr/bin/env python3
"""SNIN MESH — Sustained 12s High-Speed Benchmark

Быстрый sustained тест с keep-alive: 300 event/burst, 5ms interval.
8 параллельных соединений → RR на 8 workers.
"""

import asyncio
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from benchmark_sustained import ConnectionWorker, make_event

SR_HOST = "127.0.0.1"
SR_PORT = 9932
N_CONNECTIONS = 8
DURATION = 12
FLUSH_EVERY = 64

G = "\033[92m"
Y = "\033[93m"
C = "\033[96m"
N = "\033[0m"
B = "\033[1m"


async def main():
    workers = []
    kinds = [39001, 39002, 39000, 1] * 2
    labels = ["gossip", "mesh", "direct", "nostr"] * 2
    
    print(f"{B}{C}Connecting {N_CONNECTIONS} workers...{N}", flush=True)
    
    for i in range(N_CONNECTIONS):
        w = ConnectionWorker(i, kinds[i], i * 1_000_000, labels[i])
        ok = await w.connect()
        if ok:
            workers.append(w)
        else:
            print(f"  Worker {i} FAILED", flush=True)
    
    n = len(workers)
    print(f"Connected: {n}/{N_CONNECTIONS}", flush=True)
    
    start = time.monotonic()
    total_sent = 0
    stats_interval = 3
    last_total = 0
    
    print(f"\n{B}{C}{'─'*50}{N}")
    
    while True:
        elapsed = time.monotonic() - start
        if elapsed >= DURATION:
            break
        
        # Burst: 300 events per connection
        for w in workers:
            await w.send_burst(300)
        
        # Print stats every 3s
        if int(elapsed) % stats_interval == 0 and elapsed > 0:
            current = sum(w.sent for w in workers)
            rate = (current - last_total) / stats_interval
            errs = sum(w.errors for w in workers)
            print(f"  [{elapsed:3.0f}s]  {current:>10,} events  {rate:>8,.0f} msg/s  errors={errs}", flush=True)
            last_total = current
        
        await asyncio.sleep(0.005)  # 5ms between bursts
    
    elapsed = time.monotonic() - start
    total = sum(w.sent for w in workers)
    errs = sum(w.errors for w in workers)
    cps = total / elapsed
    
    print(f"{B}{C}{'─'*50}{N}")
    print(f"\n{B}{G}{'═'*55}{N}")
    print(f"{B}{G}  🏆 SNIN MESH — SUSTAINED 12s HIGH-SPEED BENCHMARK{N}")
    print(f"{G}{'═'*55}{N}")
    print(f"  Duration:     {elapsed:.1f}s")
    print(f"  Total events: {total:,}")
    print(f"  Throughput:   {B}{G}{cps:>10,.0f} msg/s sustained{N}")
    print(f"  Errors:       {errs}")
    
    mbps = cps * 400 / 1_000_000
    print(f"  Bandwidth:    {mbps:.1f} MB/s ({mbps*8:.0f} Mbps)")
    print(f"  Connections:  {n} × keep-alive")
    print(f"  vs baseline:  {B}{C}{cps/281:,.1f}x{N} (from 281 msg/s)")
    
    # Per-channel
    by_ch = {}
    for w in workers:
        by_ch.setdefault(w.base_label, {"sent": 0, "n": 0})
        by_ch[w.base_label]["sent"] += w.sent
        by_ch[w.base_label]["n"] += 1
    
    print(f"\n  {B}{'Channel':>8s} | {'Total':>10s} | {'msg/s':>10s} | {'per_con':>8s}{N}")
    print(f"  {'─'*8}─┼─{'─'*10}─┼─{'─'*10}─┼─{'─'*8}─")
    for ch, data in sorted(by_ch.items()):
        rate = data["sent"] / elapsed
        per_con = rate / data["n"]
        print(f"  {ch:>8s} | {data['sent']:>10,} | {rate:>8,.0f}/s | {per_con:>6,.0f}/s")
    
    print(f"{G}{'═'*55}{N}")
    
    # Cleanup
    for w in workers:
        await w.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

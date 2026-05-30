#!/usr/bin/env python3
"""SNIN MESH — Sustained 60s Benchmark + Flamegraph

Измеряет реальную пропускную способность при длительной нагрузке
через параллельные соединения ко всем 8 workers.
"""

import asyncio
import time
import sys
import os
import signal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ─── Config ────────────────────────────────────────────────────────
SR_HOST = "127.0.0.1"
SR_PORT = 9932
N_CONNECTIONS = 8  # = N workers
DURATION = 60       # 60s sustained
EVENT_SIZE = 400    # ~400 байт на событие (реальный Nostr event)
FLUSH_EVERY = 32    # per-connection flush interval (как batch gossip)

STATS_INTERVAL = 5  # печатать каждые 5 сек

# ─── Bench Event ───────────────────────────────────────────────────

def make_event(kind: int, seq: int, prefix: str = "bench") -> bytes:
    """Сформировать Nostr-совместимое событие."""
    event = {
        "kind": kind,
        "pubkey": f"{prefix}_pk_{seq:08x}" + "0" * 48,
        "content": '{"type":"bench","data":"' + "x" * 128 + '","seq":' + str(seq) + '}',
        "id": f"{prefix}_{seq:016x}" + "0" * 44,
        "created_at": int(time.time()),
        "sig": f"{prefix}_sig_{seq:08x}" + "0" * 108,
    }
    import orjson as json
    return json.dumps(event) + b"\n"


# ─── Single Connection Worker ──────────────────────────────────────

class ConnectionWorker:
    """Одно TCP соединение → send loop с заданной интенсивностью."""
    
    def __init__(self, conn_id: int, kind: int, seq_start: int, base_label: str):
        self.conn_id = conn_id
        self.kind = kind
        self.seq = seq_start
        self.base_label = base_label
        self.sent = 0
        self.errors = 0
        self.reader = None
        self.writer = None
    
    async def connect(self):
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(SR_HOST, SR_PORT), timeout=5)
            return True
        except Exception as e:
            print(f"  [C{self.conn_id}] Connect failed: {e}", flush=True)
            return False
    
    async def send_burst(self, n: int):
        """Отправить n событий в это соединение."""
        for _ in range(n):
            msg = make_event(self.kind, self.seq, self.base_label)
            self.writer.write(msg)
            self.seq += 1
            self.sent += 1
            if self.sent % FLUSH_EVERY == 0:
                try:
                    await self.writer.drain()
                except Exception:
                    self.errors += 1
                    return False
        try:
            await self.writer.drain()
        except Exception:
            self.errors += 1
            return False
        return True
    
    async def disconnect(self):
        try:
            self.writer.close()
        except:
            pass


# ─── Sustained Benchmark ───────────────────────────────────────────

class SustainedBenchmark:
    """60-секундный sustained тест с параллельными соединениями."""
    
    def __init__(self):
        self.workers: list[ConnectionWorker] = []
        self.start_time = 0.0
        self.running = True
    
    async def run(self):
        print(f"{chr(27)}[1m{chr(27)}[96m{'█'*60}{chr(27)}[0m")
        print(f"{chr(27)}[1m{chr(27)}[96m  SNIN MESH — SUSTAINED 60s BENCHMARK{chr(27)}[0m")
        print(f"{chr(27)}[1m{chr(27)}[96m  Workers: {N_CONNECTIONS} | Duration: {DURATION}s{chr(27)}[0m")
        print(f"{chr(27)}[96m  Event size: ~{EVENT_SIZE}B | Flush: every {FLUSH_EVERY}{chr(27)}[0m")
        print(f"{chr(27)}[1m{chr(27)}[96m{'█'*60}{chr(27)}[0m")
        print()
        
        # Connect all
        print(f"Connecting {N_CONNECTIONS} workers...", flush=True)
        kinds = [39001, 39002, 39000, 1] * (N_CONNECTIONS // 4)
        labels = ["gossip", "mesh", "direct", "nostr"] * (N_CONNECTIONS // 4)
        
        for i in range(N_CONNECTIONS):
            w = ConnectionWorker(i, kinds[i], i * 10000000, labels[i])
            ok = await w.connect()
            if ok:
                self.workers.append(w)
            else:
                print(f"  Worker {i} failed to connect", flush=True)
        
        n = len(self.workers)
        if n == 0:
            print("Zero workers connected. Abort.", flush=True)
            return
        
        print(f"Connected: {n}/{N_CONNECTIONS}", flush=True)
        total_events = 0
        total_errors = 0
        self.start_time = time.monotonic()
        
        # Monitor task
        async def monitor():
            last_total = 0
            while self.running:
                await asyncio.sleep(STATS_INTERVAL)
                elapsed = time.monotonic() - self.start_time
                current = sum(w.sent for w in self.workers)
                rate = (current - last_total) / STATS_INTERVAL
                errors = sum(w.errors for w in self.workers)
                
                # Health check
                import urllib.request
                try:
                    resp = urllib.request.urlopen("http://127.0.0.1:9933/health", timeout=2)
                    import json
                    health = json.loads(resp.read())
                    alive = health["workers"]["alive"]
                except:
                    alive = "?"
                
                print(f"  [{elapsed:3.0f}s] {current:>8,} events | {rate:>8,.0f} msg/s | "
                      f"errors={errors} | workers={alive}/{N_CONNECTIONS}", flush=True)
                last_total = current
        
        monitor_task = asyncio.create_task(monitor())
        
        # Main loop: burst sends
        interval_per_batch = 0.05  # 50ms between batches per connection
        events_per_burst = 50
        
        while time.monotonic() - self.start_time < DURATION:
            for w in self.workers:
                ok = await w.send_burst(events_per_burst)
                if not ok:
                    total_errors += 1
                    # Reconnect
                    print(f"  Reconnecting worker {w.conn_id}...", flush=True)
                    await w.disconnect()
                    await asyncio.sleep(0.5)
                    await w.connect()
            
            # Let workers breathe
            await asyncio.sleep(interval_per_batch)
        
        self.running = False
        await monitor_task
        
        elapsed = time.monotonic() - self.start_time
        total_events = sum(w.sent for w in self.workers)
        total_errors = sum(w.errors for w in self.workers)
        
        print(f"\n{chr(27)}[1m{chr(27)}[92m{'═'*60}{chr(27)}[0m")
        print(f"{chr(27)}[1m{chr(27)}[92m  📊 SUSTAINED 60s — RESULTS{chr(27)}[0m")
        print(f"{chr(27)}[92m{'═'*60}{chr(27)}[0m")
        print(f"  Duration:     {elapsed:.1f}s")
        print(f"  Total events: {total_events:,}")
        print(f"  Throughput:   {chr(27)}[1m{chr(27)}[92m{total_events/elapsed:>8,.0f} msg/s sustained{chr(27)}[0m")
        print(f"  Errors:       {total_errors}")
        
        # CPS
        cps = total_events / elapsed
        mbps = cps * EVENT_SIZE / 1_000_000
        print(f"  Bandwidth:    {mbps:.1f} MB/s ({cps*EVENT_SIZE/1_000_000_000:.2f} Gbps)")
        print(f"  Per-worker:   {cps / n:,.0f} msg/s avg")
        print(f"  Workers:      {n} parallel connections")
        
        # Per-channel stats
        by_channel = {}
        for w in self.workers:
            ch = w.base_label
            by_channel.setdefault(ch, {"sent": 0, "errors": 0, "count": 0})
            by_channel[ch]["sent"] += w.sent
            by_channel[ch]["errors"] += w.errors
            by_channel[ch]["count"] += 1
        
        print(f"\n  {'Channel':>8s} | {'Total':>8s} | {'msg/s':>8s} | {'#con':>4s}")
        print(f"  {'─'*8}─┼─{'─'*8}─┼─{'─'*8}─┼─{'─'*4}─")
        for ch, data in sorted(by_channel.items()):
            rate = data["sent"] / elapsed
            n_conn = data["count"]
            print(f"  {ch:>8s} | {data['sent']:>8,} | {rate:>8,.0f} | {n_conn:>4}")
        
        # Comparison with baseline
        baseline = 281
        speedup = cps / baseline
        print(f"\n  vs baseline 281 msg/s: {chr(27)}[1m{chr(27)}[96m{speedup:,.1f}x{chr(27)}[0m")
        
        # Disconnect all
        for w in self.workers:
            await w.disconnect()
        
        print(f"{chr(27)}[1m{chr(27)}[92m{'═'*60}{chr(27)}[0m")


async def main():
    bench = SustainedBenchmark()
    await bench.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBenchmark interrupted")

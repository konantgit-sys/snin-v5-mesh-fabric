#!/usr/bin/env python3
"""
Phase 3a — NATS Transport Benchmark
════════════════════════════════════

NATS vs ZeroMQ vs TCP throughput comparison.
30K messages over pub/sub + request/reply.
"""

import asyncio, time, struct, json
import sys
sys.path.insert(0, '/home/agent/data/sites/relay-mesh')
from snin_nats import NatsTransport, pack_message

ITERATIONS = 5000  # per test type

async def bench_nats_pubsub():
    """Benchmark NATS pub/sub throughput."""
    t1 = NatsTransport("bench_a")
    t2 = NatsTransport("bench_b")
    await t1.start()
    await t2.start()
    
    received = []
    t2.on_message("mesh", lambda d, r: received.append(1))
    
    msg = pack_message(39000, {"agent_id": "bench", "name": "Test"})
    
    t0 = time.perf_counter()
    for i in range(ITERATIONS):
        await t1.publish("mesh", msg, target="bench_b")
    await asyncio.sleep(0.5)  # wait for delivery
    t1_time = time.perf_counter() - t0
    
    await t1.stop()
    await t2.stop()
    
    return {
        "name": "NATS Pub/Sub",
        "msgs": ITERATIONS,
        "received": len(received),
        "time_s": round(t1_time, 3),
        "msg_per_s": round(ITERATIONS / t1_time) if t1_time > 0 else 0,
    }


async def bench_nats_rpc():
    """Benchmark NATS request/reply throughput."""
    t1 = NatsTransport("rpc_a")
    t2 = NatsTransport("rpc_b")
    await t1.start()
    await t2.start()
    
    # Set up reply handler
    async def handle_reply(data, reply):
        if reply:
            await t2.nc.publish(reply, b"pong")
    t2.on_message("request", handle_reply)
    
    msg = pack_message(39001, {"agent_id": "ping"})
    
    t0 = time.perf_counter()
    successes = 0
    for i in range(ITERATIONS):
        reply = await t1.request("rpc_b", msg, timeout=2.0)
        if reply:
            successes += 1
    t1_time = time.perf_counter() - t0
    
    await t1.stop()
    await t2.stop()
    
    return {
        "name": "NATS RPC",
        "msgs": ITERATIONS,
        "successes": successes,
        "time_s": round(t1_time, 3),
        "msg_per_s": round(successes / t1_time) if t1_time > 0 else 0,
    }


async def main():
    print("═══ Phase 3a — NATS Benchmark ═══\n")
    
    results = []
    
    print("Benchmarking NATS Pub/Sub...", end=" ", flush=True)
    r = await bench_nats_pubsub()
    results.append(r)
    print(f"{r['time_s']}s · {r['msg_per_s']:,} msg/s · {r['received']}/{r['msgs']} delivered")
    
    print("Benchmarking NATS RPC...", end=" ", flush=True)
    r = await bench_nats_rpc()
    results.append(r)
    print(f"{r['time_s']}s · {r['msg_per_s']:,} msg/s · {r['successes']}/{r['msgs']} delivered")
    
    print(f"\n{'Format':<20} {'Time':>8} {'msg/s':>12} {'Delivery':>12}")
    print("-" * 56)
    for r in results:
        delivery = f"{r.get('successes', r.get('received',0))}/{r['msgs']}"
        print(f"{r['name']:<20} {r['time_s']:>7.3f}s {r['msg_per_s']:>11,} {delivery:>12}")
    
    print(f"\n═══ Done ═══")

if __name__ == "__main__":
    asyncio.run(main())

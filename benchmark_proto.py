#!/usr/bin/env python3
"""
Phase 2b — Serialization Benchmark
════════════════════════════════════

JSON vs MessagePack vs Protocol Buffers (throughput + size)
"""
import time, json, struct
import msgpack
import snin_pb2
from snin_proto_adapter import SninProtoAdapter

ITERATIONS = 10000
adapter = SninProtoAdapter()

# Test messages
MESSAGES = {
    39000: {
        "agent_id": "forecaster_ai",
        "name": "SNIN Forecaster V6",
        "version": "6.0.0",
        "capabilities": ["market_prediction", "nlp_analysis", "trend_detection", "reporting"],
        "npub": "npub1forecasterabcdefghijklmnopqrstuvwxyz1234",
        "did": "did:snin:forecaster_ai",
        "created_at": 1750000000,
        "status": "online"
    },
    39001: {
        "agent_id": "forecaster_ai",
        "status": "online",
        "timestamp": 1750000000,
        "uptime_seconds": 86400,
        "messages_processed": 15000,
        "errors_count": 3,
        "cpu_pct": 12.5,
        "memory_mb": 256
    },
    39010: {
        "proposal_id": "prop-001",
        "author": "council_member_1",
        "title": "Increase relay capacity to 100 agents",
        "description": "We need more relays to handle growing agent count. Budget: 500K SNIN.",
        "created_at": 1750000000,
        "voting_ends_at": 1750086400,
        "options": ["Yes", "No", "Abstain"],
        "quorum": 50,
        "status": "active"
    }
}

def bench_format(name, pack_fn, unpack_fn, messages, iterations=ITERATIONS):
    """Benchmark a serialization format."""
    sizes = []
    
    # Pack benchmark
    t0 = time.perf_counter()
    packed = []
    for i in range(iterations):
        for kind, msg in messages.items():
            data = pack_fn(kind, msg, i)
            packed.append(data)
            sizes.append(len(data))
    pack_time = time.perf_counter() - t0
    
    # Unpack benchmark
    t0 = time.perf_counter()
    for data in packed:
        result = unpack_fn(data)
    unpack_time = time.perf_counter() - t0
    
    total_msgs = iterations * len(messages)
    avg_size = sum(sizes) / len(sizes) if sizes else 0
    
    return {
        "name": name,
        "total_msgs": total_msgs,
        "pack_time_s": round(pack_time, 3),
        "unpack_time_s": round(unpack_time, 3),
        "total_time_s": round(pack_time + unpack_time, 3),
        "msg_per_s": round(total_msgs / (pack_time + unpack_time)),
        "avg_size_b": round(avg_size),
        "total_kb": round(sum(sizes) / 1024, 1),
    }


# ─── JSON ───
def pack_json(kind, msg, idx):
    msg_copy = dict(msg)
    msg_copy["_idx"] = idx
    data = json.dumps(msg_copy, ensure_ascii=False).encode()
    return struct.pack(">H", 0x4A53) + data  # "JS" tag

def unpack_json(data):
    return json.loads(data[2:])

# ─── MessagePack ───
def pack_msgpack(kind, msg, idx):
    msg_copy = dict(msg)
    msg_copy["_idx"] = idx
    data = msgpack.packb(msg_copy)
    return struct.pack(">H", 0x4D50) + data

def unpack_msgpack_fn(data):
    return msgpack.unpackb(data[2:])

# ─── Protocol Buffers ───
def pack_proto(kind, msg, idx):
    msg_copy = dict(msg)
    msg_copy["_idx"] = idx
    return adapter.pack(kind, msg_copy, use_envelope=True)

def unpack_proto(data):
    return adapter.unpack(data)


if __name__ == "__main__":
    print("═══ Phase 2b — Serialization Benchmark ═══\n")
    print(f"Iterations: {ITERATIONS} × {len(MESSAGES)} kinds = {ITERATIONS * len(MESSAGES)} messages\n")
    
    results = []
    for name, packer, unpacker in [
        ("JSON", pack_json, unpack_json),
        ("MessagePack", pack_msgpack, unpack_msgpack_fn),
        ("Protocol Buffers", pack_proto, unpack_proto),
    ]:
        print(f"Benchmarking {name}...", end=" ", flush=True)
        r = bench_format(name, packer, unpacker, MESSAGES)
        results.append(r)
        print(f"{r['total_time_s']}s · {r['msg_per_s']} msg/s · {r['avg_size_b']}B avg")
    
    # Comparison
    print(f"\n{'Format':<18} {'Time':>8} {'msg/s':>10} {'Avg Size':>10} {'Total KB':>10}")
    print("-" * 60)
    json_r = results[0]
    for r in results:
        speedup = f"×{json_r['total_time_s']/r['total_time_s']:.1f}" if r != json_r else "baseline"
        print(f"{r['name']:<18} {r['total_time_s']:>7.3f}s {r['msg_per_s']:>9,} {r['avg_size_b']:>9}B {r['total_kb']:>9.1f} {speedup:>8}")
    
    print(f"\n═══ Done ═══")

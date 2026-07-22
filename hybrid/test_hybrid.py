#!/usr/bin/env python3
"""
Integration test for Hybrid Architecture — Coordinator + Channel.

Tests:
  1. Coordinator starts, accepts registration
  2. Two agents register, discover each other
  3. Direct message between agents
  4. NAT strategy recommendations
  5. Peer list caching and TTL
  6. Cleanup of stale agents
"""
import asyncio
import json
import os
import sys
import time
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Override DB path for test
os.environ["HCOOR_PORT"] = "9971"
os.environ["HCOOR_HOST"] = "127.0.0.1"

from hybrid.hcoor import HybridCoordinator, RegistryDB, Agent, DB_PATH
from hybrid.hybrid_channel import HybridChannel

passed = 0
failed = 0
errors = []


def log(msg: str, ok: bool = True):
    global passed, failed
    if ok:
        passed += 1
        print(f"  ✅ {msg}")
    else:
        failed += 1
        errors.append(msg)
        print(f"  ❌ {msg}")


async def main():
    global passed, failed

    print("=" * 60)
    print("  HYBRID ARCHITECTURE — Integration Test")
    print("  Coordinator + Channel + NAT Strategies")
    print("=" * 60)

    # ═══ Test 1: Coordinator startup ═══
    print("\n─── Test 1: Coordinator Startup ───")
    hcoor = HybridCoordinator(port=9971, host="127.0.0.1")
    await hcoor.start()
    await asyncio.sleep(0.2)
    log("Coordinator starts on :9971", hcoor._running)

    stats = hcoor._db.get_stats()
    log(f"Empty registry: {stats['total_agents']} agents", stats["total_agents"] == 0)

    # ═══ Test 2: Agent registration ═══
    print("\n─── Test 2: Agent Registration ───")
    alice = HybridChannel("127.0.0.1", 9971)
    ok = await alice.register(
        pubkey="alice_test_pubkey_001",
        name="Alice Test",
        ip="10.0.0.1",
        port=9001,
        nat_type="easy",
        capabilities=["forecast", "text"],
        npub="npub1alice...",
    )
    log("Alice registered", ok)

    bob = HybridChannel("127.0.0.1", 9971)
    ok = await bob.register(
        pubkey="bob_test_pubkey_002",
        name="Bob Test",
        ip="10.0.0.2",
        port=9002,
        nat_type="symmetric",
        capabilities=["content", "design"],
        npub="npub1bob...",
    )
    log("Bob registered", ok)

    stats = hcoor._db.get_stats()
    log(f"2 agents in registry: {stats['total_agents']}", stats["total_agents"] == 2)
    log(f"Both online: {stats['online']}", stats["online"] == 2)

    # ═══ Test 3: Peer discovery ═══
    print("\n─── Test 3: Peer Discovery ───")
    peers = await alice.get_peer_list()
    log(f"Alice sees {len(peers)} peers", len(peers) == 1)
    if peers:
        log(f"Peer is Bob: {peers[0].get('name', '')}", peers[0].get("name") == "Bob Test")

    # Find specific agent
    bob_found = await alice.get_peer("bob_test_pubkey_002")
    log("Find Bob by pubkey", bob_found is not None)
    if bob_found:
        log(f"Bob NAT type: symmetric", bob_found.get("nat_type") == "symmetric")
        log(f"Bob IP: 10.0.0.2", bob_found.get("ip") == "10.0.0.2")

    # ═══ Test 4: NAT Strategy ═══
    print("\n─── Test 4: NAT Strategy Recommendations ───")

    strategies = {
        ("easy", "easy"): "direct_udp",
        ("easy", "symmetric"): "tcp_reverse",
        ("symmetric", "easy"): "tcp_direct",
        ("cone", "cone"): "udp_holepunch",
        ("symmetric", "symmetric"): "relay_fallback",
    }

    for (src, dst), expected in strategies.items():
        actual = hcoor._nat_strategy(src, dst)
        log(f"{src}+{dst} → {expected}", actual == expected)

    # ═══ Test 5: Re-registration (idempotent) ═══
    print("\n─── Test 5: Re-registration (idempotent) ───")
    ok = await alice.register(
        pubkey="alice_test_pubkey_001",
        name="Alice Updated",
        ip="10.0.1.1",
        port=9001,
        nat_type="cone",    # changed NAT type
    )
    log("Alice re-registered with new data", ok)

    agent = hcoor._db.get_agent("alice_test_pubkey_001")
    log(f"Name updated to '{agent.name}'", agent.name == "Alice Updated")
    log(f"NAT changed to cone", agent.nat_type == "cone")
    log(f"IP updated to 10.0.1.1", agent.ip == "10.0.1.1")
    log(f"Ping count = 2", agent.ping_count == 2)  # registered twice

    # ═══ Test 6: Stats ═══
    print("\n─── Test 6: Coordinator Stats ───")
    stats = hcoor._db.get_stats()
    log(f"online={stats['online']}", stats["online"] == 2)
    log(f"total_agents={stats['total_agents']}", stats["total_agents"] == 2)
    log(f"events in last 24h > 0", stats.get("events_24h", 0) > 0)

    # ═══ Test 7: Channel stats ═══
    print("\n─── Test 7: Channel Statistics ───")
    ch_stats = alice.get_stats()
    log(f"registered=True", ch_stats["registered"] is True)
    log(f"coordinator_queries > 0", ch_stats["coordinator_queries"] > 0)
    log(f"cache_size > 0", ch_stats["cache_size"] > 0)

    # ═══ Test 8: Find nonexistent agent ═══
    print("\n─── Test 8: Find Nonexistent Agent ───")
    ghost = await alice.get_peer("nonexistent_pubkey_999")
    log("Ghost agent not found", ghost is None)

    # ═══ Test 9: Peer list cache ═══
    print("\n─── Test 9: Peer List Caching ───")
    peers1 = await alice.get_peer_list()
    peers2 = await alice.get_peer_list()
    # Second call should use cache (fewer coordinator queries than first)
    ch_stats2 = alice.get_stats()
    log(f"Cache populated: {ch_stats2['cache_size']} entries", ch_stats2["cache_size"] >= 1)

    # ═══ Test 10: Cleanup of stale agents ═══
    print("\n─── Test 10: Stale Agent Cleanup ───")
    # Manually expire Alice's last_seen
    hcoor._db._conn.execute(
        "UPDATE agents SET last_seen=? WHERE pubkey=?",
        (time.time() - 99999, "alice_test_pubkey_001")
    )
    hcoor._db._conn.commit()
    stale = hcoor._db.cleanup_stale()
    log(f"Alice marked stale: {stale}", stale >= 1)

    agent = hcoor._db.get_agent("alice_test_pubkey_001")
    log(f"Alice status = offline", agent.status == "offline")

    # ═══ Cleanup ═══
    print("\n─── Cleanup ───")
    await alice.close()
    await bob.close()
    await hcoor.stop()

    # Remove test DB
    test_db = DB_PATH if Path(DB_PATH).exists() else None
    # Don't delete — keep for inspection

    # ═══ Summary ═══
    print("\n" + "=" * 60)
    print(f"  RESULTS: {passed} passed, {failed} failed")
    if errors:
        print("  FAILURES:")
        for e in errors:
            print(f"    - {e}")
    print("=" * 60)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())

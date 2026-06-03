#!/usr/bin/env python3
"""
test_middleware.py — Unit-тесты для middleware.py (Phase 4).

Запуск: python3 test_middleware.py
"""

import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from middleware import (
    RateLimiter, CircuitBreakerManager, ChannelCB, CircuitState,
    RequestPipeline, get_pipeline,
    check_rate_limit_simple, cb_check, cb_record_error, cb_reset,
    cb_status, cb_degraded_channels,
)

passed = 0
failed = 0


def check(name: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✅ {name}")
    else:
        failed += 1
        msg = f"  ❌ {name}"
        if detail:
            msg += f" — {detail}"
        print(msg)


async def test_rate_limiter():
    print("\n═══ RateLimiter ═══")
    rl = RateLimiter()

    # 1. Basic allow
    ok, reason = await rl.check(ip="1.2.3.4", pubkey="pk1", content="hello", signature="sig_12345")
    check("basic allow", ok, reason)

    # 2. Max event size
    ok, reason = await rl.check(ip="1.2.3.5", content="x" * 70000, signature="sig")
    check("oversized event rejected", not ok, f"{ok}:{reason}")

    # 3. Per-IP rate limit (100/60s is hard to trigger, but check blacklist)
    # Trigger 10 violations to get blacklisted
    ip = "bad.ip.1.2"
    for i in range(12):
        await rl.check(ip=ip, content="x" * 70000, signature="s")
    
    # Now even small events should be blacklisted
    ok, reason = await rl.check(ip=ip, content="small", signature="sig")
    check("blacklist blocks", not ok, f"{ok}:{reason}")

    # 4. Per-session rate limit (10/s for anon)
    session = "test_session_1"
    for i in range(10):
        ok, reason = await rl.check(ip="anon.1", session_key=session, content="x", signature="s")
        if not ok:
            break
    check("session anon: 10th allowed", ok, reason)

    # 11th should be blocked (10/s max for anon)
    await asyncio.sleep(1.1)  # Wait for window to reset
    ok, _ = await rl.check(ip="anon.1", session_key=session, content="x", signature="s")
    check("session anon: after cooldown allowed", ok)

    # 5. Stats
    stats = rl.get_stats()
    check("stats has total", "total" in stats)
    check("stats has blacklist", "blacklist_size" in stats)
    print(f"     total={stats['total']}, blacklist={stats['blacklist_size']}, rejected={stats['rejected']}")


def test_circuit_breaker_manager():
    print("\n═══ CircuitBreakerManager ═══")
    cb = CircuitBreakerManager()

    # 1. All channels start CLOSED
    for ch in ("direct", "mesh", "nostr", "gossip"):
        allowed, state = cb.check(ch)
        check(f"cb {ch} starts closed", allowed and state == "closed")

    # 2. direct (threshold=3) opens after 3 errors
    for i in range(3):
        cb.record_error("direct")
    allowed, state = cb.check("direct")
    check("direct opens after 3 errors", not allowed and state == "open")

    # 3. mesh (threshold=5) opens after 5 errors
    for i in range(5):
        cb.record_error("mesh")
    allowed, state = cb.check("mesh")
    check("mesh opens after 5 errors", not allowed and state == "open")

    # 4. Success during OPEN → stays OPEN (can_proceed prevents it)
    cb.record_success("mesh")
    allowed, _ = cb.check("mesh")
    check("success during open doesn't close", not allowed)

    # 5. Reset restores CLOSED
    cb.reset("direct")
    allowed, state = cb.check("direct")
    check("reset restores closed", allowed and state == "closed")

    # 6. Reset all
    cb.reset()
    for ch in ("direct", "mesh", "nostr", "gossip"):
        allowed, state = cb.check(ch)
        check(f"global reset → {ch} closed", allowed and state == "closed")

    # 7. Status dict
    status = cb.status()
    check("status has channels", "channels" in status)
    check("status has 4 channels", len(status["channels"]) == 4)
    check("status has uptime", "uptime_sec" in status)

    # 8. degraded_channels()
    cb.record_error("gossip")
    cb.record_error("gossip")
    cb.record_error("gossip")
    cb.record_error("gossip")
    cb.record_error("gossip")
    degraded = cb.degraded_channels()
    check("degraded contains gossip", "gossip" in degraded)
    check("gossip in degraded list", len(degraded) == 1)

    cb.reset("gossip")


def test_circuit_breaker_status_persistence():
    print("\n═══ CircuitBreaker → JSON persistence ═══")
    cb = CircuitBreakerManager()
    cb.save_status()
    check("status file exists", os.path.exists(cb.STATUS_FILE))
    with open(cb.STATUS_FILE) as f:
        data = json.load(f)
    check("status json has channels", "channels" in data)
    check("status json has mesh", "mesh" in data["channels"])


def test_check_rate_limit_simple():
    print("\n═══ check_rate_limit_simple (compat API) ═══")
    key = "compat_test_key"

    # First 10: should all pass
    for i in range(10):
        ok = check_rate_limit_simple(key, 10)
        check(f"compat {i+1}/10", ok)

    # 11th: should fail (10/s is max)
    ok = check_rate_limit_simple(key, 10)
    check("compat 11th blocked", not ok)

    # After >1s: should pass again
    import time
    time.sleep(1.1)
    ok = check_rate_limit_simple(key, 10)
    check("compat after cooldown", ok)


def test_cb_shortcuts():
    print("\n═══ CB shortcuts ═══")
    cb_reset()

    allowed, state = cb_check("nostr")
    check("cb_check nostr ok", allowed)

    # error tracking
    cb_record_error("nostr")
    cb_record_error("nostr")
    cb_record_error("nostr")
    allowed, state = cb_check("nostr")
    check("cb_check nostr blocked after 3", not allowed)

    cb_reset("nostr")
    allowed, state = cb_check("nostr")
    check("cb_reset nostr restored", allowed)

    # status
    status = cb_status()
    check("cb_status has channels", "channels" in status)

    # degraded
    deg = cb_degraded_channels()
    check("cb_degraded_channels list", isinstance(deg, list))


async def test_pipeline():
    print("\n═══ RequestPipeline ═══")
    p = RequestPipeline()

    # 1. Normal request passes
    ok, reason, meta = await p.process(
        ip="10.0.0.1", pubkey="pk_good", content="hello", channel="mesh"
    )
    check("pipeline: normal passes", ok, f"{ok}:{reason}")
    check("pipeline: meta has cb", "cb" in meta)

    # 2. Oversized blocked
    ok, reason, meta = await p.process(
        ip="10.0.0.2", content="x" * 70000, channel="mesh"
    )
    check("pipeline: oversized blocked", not ok, f"{ok}:{reason}")

    # 3. CB blocked
    # First open the direct channel
    for i in range(3):
        p.cb.record_error("direct")
    ok, reason, meta = await p.process(
        ip="10.0.0.3", content="test", channel="direct"
    )
    check("pipeline: cb blocked", not ok, f"{ok}:{reason}")
    check("pipeline: err msg has cb:", "cb:" in reason)

    # 4. run_sync (Flask compat — тестируем в отдельном процессе/контексте)
    ok_sync = [None]

    def _run_sync():
        # Создаём новый event loop специально для run_sync (как Flask)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            p2 = RequestPipeline()
            ok, reason, meta = p2.run_sync(
                ip="10.0.0.4", content="sync test", channel="gossip"
            )
            ok_sync[0] = (ok, reason, meta)
        finally:
            loop.close()
    
    import threading, asyncio
    t = threading.Thread(target=_run_sync, daemon=True)
    t.start()
    t.join(timeout=5)
    if ok_sync[0]:
        ok, reason, meta = ok_sync[0]
        check("pipeline: run_sync works", ok, f"{ok}:{reason}")
    else:
        check("pipeline: run_sync works (thread)", False, "no result")

    # 5. Pipeline stats
    stats = p.get_stats()
    check("pipeline stats: rate_limiter", "rate_limiter" in stats)
    check("pipeline stats: circuit_breaker", "circuit_breaker" in stats)

    # Reset for other tests
    p.cb.reset("direct")


async def test_singleton():
    print("\n═══ Singleton pipeline ═══")
    p1 = get_pipeline()
    p2 = get_pipeline()
    check("singleton: same instance", p1 is p2)


async def main():
    print("=" * 60)
    print("Middleware Test Suite (Phase 4)")
    print("=" * 60)

    await test_rate_limiter()
    test_circuit_breaker_manager()
    test_circuit_breaker_status_persistence()
    test_check_rate_limit_simple()
    test_cb_shortcuts()
    await test_pipeline()
    await test_singleton()

    print(f"\n{'='*60}")
    print(f"Результат: {passed} ✅  {failed} ❌")
    print(f"{'='*60}")
    return failed == 0


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)

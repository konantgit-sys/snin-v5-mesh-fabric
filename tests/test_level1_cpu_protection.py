#!/usr/bin/env python3
"""Unit tests for Level 1 CPU Protection."""

import asyncio
import sys
sys.path.insert(0, "/home/agent/data/sites/relay-mesh")

from cpu_worker import hash_sha256_async, dht_distance_async, make_nostr_id_async

async def test_hash_sha256_async():
    """Test SHA256 in thread pool."""
    result = await hash_sha256_async("hello")
    expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    assert result == expected, f"Expected {expected}, got {result}"
    print("✅ test_hash_sha256_async: PASS")

async def test_dht_distance_async():
    """Test XOR distance in thread pool."""
    a = "0000000000000001"
    b = "0000000000000002"
    result = await dht_distance_async(a, b)
    expected = 0x01 ^ 0x02  # = 3
    assert result == expected, f"Expected {expected}, got {result}"
    print("✅ test_dht_distance_async: PASS")

async def test_make_nostr_id_async():
    """Test Nostr event ID generation in thread pool."""
    pubkey = "test_pubkey"
    content = "hello"
    kind = 1
    ts = 1234567890
    result = await make_nostr_id_async(pubkey, content, kind, ts)
    # Must be 64-char hex
    assert len(result) == 64, f"Expected 64 chars, got {len(result)}"
    assert all(c in "0123456789abcdef" for c in result), f"Not valid hex: {result}"
    print("✅ test_make_nostr_id_async: PASS")

async def test_multiple_concurrent():
    """Test concurrent calls (thread pool should handle it)."""
    tasks = [
        hash_sha256_async(f"data_{i}")
        for i in range(10)
    ]
    results = await asyncio.gather(*tasks)
    assert len(results) == 10, f"Expected 10 results, got {len(results)}"
    assert len(set(results)) == 10, "All results should be unique"
    print("✅ test_multiple_concurrent: PASS")

async def main():
    print("━ Level 1 CPU Protection Tests ━")
    try:
        await test_hash_sha256_async()
        await test_dht_distance_async()
        await test_make_nostr_id_async()
        await test_multiple_concurrent()
        print("\n✅ ALL TESTS PASSED")
        return 0
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

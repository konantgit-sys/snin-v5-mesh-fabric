"""
Test suite for serialization.py — MessagePack vs JSON transport layer.
Verifies: roundtrip integrity, format auto-detection, performance delta.
"""
import sys, json, msgpack, time, uuid

sys.path.insert(0, '/home/agent/data/sites/relay-mesh')
from serialization import pack, unpack

def test_json_roundtrip():
    """JSON roundtrip: pack → unpack → verify"""
    msg = {"id": "abc", "kind": 39002, "content": "hello json", "created_at": 1751840000}
    packed = pack(msg, "json")
    assert isinstance(packed, bytes), f"Expected bytes, got {type(packed)}"
    unpacked = unpack(packed)
    assert msg == unpacked, f"JSON roundtrip mismatch: {msg} != {unpacked}"
    print("  ✅ test_json_roundtrip")

def test_msgpack_roundtrip():
    """MessagePack roundtrip: pack → unpack → verify"""
    msg = {"id": "abc", "kind": 39002, "content": "hello msgpack", "created_at": 1751840000}
    packed = pack(msg, "msgpack")
    assert isinstance(packed, bytes), f"Expected bytes, got {type(packed)}"
    unpacked = unpack(packed)
    assert msg == unpacked, f"MsgPack roundtrip mismatch: {msg} != {unpacked}"
    print("  ✅ test_msgpack_roundtrip")

def test_auto_format():
    """Default pack should use msgpack"""
    msg = {"test": "auto"}
    packed = pack(msg)  # default = msgpack
    unpacked = unpack(packed)
    assert msg == unpacked, f"Auto format mismatch"
    print("  ✅ test_auto_format")

def test_auto_detect():
    """unpack() should auto-detect JSON vs msgpack"""
    obj = {"hello": "world", "num": 42, "flag": True, "list": [1,2,3]}
    
    # Pack both ways, unpack without format hint
    js = json.dumps(obj, separators=(",", ":")).encode()
    mp = msgpack.packb(obj)
    
    assert unpack(js) == obj, "JSON auto-detect failed"
    assert unpack(mp) == obj, "msgpack auto-detect failed"
    print("  ✅ test_auto_detect")

def test_string_input():
    """unpack() should handle pre-decoded strings (from old handlers)"""
    obj = {"test": "string path"}
    js_str = json.dumps(obj)
    assert unpack(js_str) == obj, "String input failed"
    print("  ✅ test_string_input")

def test_nested_objects():
    """Roundtrip with deeply nested structures"""
    obj = {
        "tags": [["p", f"pub{i:04d}"] for i in range(10)],
        "meta": {"nested": {"deep": [1, 2, {"more": True}]}},
        "data": {"binary_like": "deadbeef" * 16}
    }
    packed = pack(obj)
    unpacked = unpack(packed)
    assert obj == unpacked, f"Nested mismatch"
    print("  ✅ test_nested_objects")

def test_empty_input():
    """unpack() should raise on empty input"""
    try:
        unpack(b"")
        assert False, "Should have raised"
    except ValueError:
        pass
    print("  ✅ test_empty_input")

def test_unicode_content():
    """MessagePack with unicode content"""
    obj = {"content": "Привет мир! 你好世界! 🚀🔥", "author": "Антон"}
    packed = pack(obj)
    unpacked = unpack(packed)
    assert obj == unpacked, f"Unicode mismatch"
    print("  ✅ test_unicode_content")

def bench_serialization(n=50000):
    """Performance benchmark: JSON vs MessagePack"""
    obj = {
        "event": "kind:39002",
        "pubkey": "abc123def456" * 4,
        "content": "Hello from SNIN agent! " * 5,
        "tags": [["p", f"pub{i:04d}"] for i in range(10)],
        "created_at": 1751840000,
        "sig": "deadbeef" * 16
    }
    
    # JSON
    t0 = time.time()
    for _ in range(n):
        js = json.dumps(obj, separators=(",", ":")).encode()
        json.loads(js)
    t_json = (time.time() - t0) * 1000
    
    # MessagePack
    t0 = time.time()
    for _ in range(n):
        mp = msgpack.packb(obj)
        msgpack.unpackb(mp, raw=False)
    t_mp = (time.time() - t0) * 1000
    
    json_size = len(json.dumps(obj, separators=(",", ":")).encode())
    mp_size = len(msgpack.packb(obj))
    
    print(f"\n  📊 Benchmark ({n} iterations):")
    print(f"  JSON:       {t_json:.0f}ms ({json_size}B/msg) → {n/(t_json/1000):.0f} msg/s")
    print(f"  MessagePack: {t_mp:.0f}ms ({mp_size}B/msg) → {n/(t_mp/1000):.0f} msg/s")
    print(f"  Speedup:    {t_json/t_mp:.1f}×")
    print(f"  Size reduction: {(1-mp_size/json_size)*100:.1f}%")
    
    assert t_mp < t_json * 0.8, f"MsgPack should be at least 20% faster (got {t_json/t_mp:.1f}x)"
    assert mp_size < json_size * 0.95, f"MsgPack should be at least 5% smaller"
    print("  ✅ bench_serialization PASSED")


if __name__ == "__main__":
    print("═══ Serialization Test Suite ═══")
    print("1. Functional tests:")
    test_json_roundtrip()
    test_msgpack_roundtrip()
    test_auto_format()
    test_auto_detect()
    test_string_input()
    test_nested_objects()
    test_empty_input()
    test_unicode_content()
    
    print("\n2. Performance benchmark:")
    bench_serialization()
    
    print("\n✅ ALL TESTS PASSED")

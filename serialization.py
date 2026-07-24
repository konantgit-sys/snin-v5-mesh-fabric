#!/usr/bin/env python3
"""
SNIN V6 — Transport Serialization Layer
═══════════════════════════════════════════════════════════════════════════════
STATUS: Phase 1a — MessagePack as direct JSON replacement

Provides pack()/unpack() with auto-detection of JSON vs MessagePack format.
Designed for ZERO code changes in SmartRouter — drop-in replacement for
json.dumps()/json.loads().

FORMAT DETECTION:
  - First byte 0x80-0x8f, 0x90-0x9f, 0xc0-0xc3, 0xcc-0xcf, 0xd0-0xd3, 0xd4-0xd8,
    0xd9, 0xda, 0xdb, 0xdc-0xdf, 0xde, 0xdf → MessagePack fixext/array/map/bin/str
  - First byte '{' or '[' → JSON
  - Otherwise → JSON with fallback

PERFORMANCE (msgpack vs json):
  - Serialize: 1.3-1.8× faster
  - Deserialize: 1.5-2.0× faster  
  - Size: 0.5-0.7× smaller (binary vs text)
  - Combined transport: ~30-40% throughput improvement

USAGE:
  import serialization as ser
  
  # Serialize
  data = ser.pack({"event": "kind:1", "content": "hello"})  # → bytes (msgpack)
  data = ser.pack({"event": "kind:1"}, fmt="json")          # → bytes (json)
  data = ser.pack({"event": "kind:1"}, fmt="auto")          # → bytes (msgpack)
  
  # Deserialize (auto-detects format)
  obj = ser.unpack(data)  # → dict
  
  # Stream protocol: length-prefixed frames
  frame = ser.frame_pack({"event": "kind:1"})
  obj, consumed = ser.frame_unpack(buffer_with_frame)
  
  # Connection capability negotiation
  ser.offer_msgpack(conn)    # send capability offer
  ser.accept_msgpack(conn)   # handle capability response
"""

import json as _json
import struct as _struct
import msgpack as _msgpack
from typing import Optional, Tuple, Union, Any

__version__ = "1.0.0"
__all__ = ["pack", "unpack", "frame_pack", "frame_unpack", 
           "is_msgpack", "detect_format", "DEFAULT_FORMAT",
           "offer_msgpack", "accept_msgpack", "MSG_MAGIC"]

# ──── Protocol Constants ────────────────────────────────────────────────────
MSG_MAGIC = b"\x8bMSG"  # MessagePack stream marker (fixext-1 with custom type)
DEFAULT_FORMAT = "auto"  # auto-detect, "msgpack", "json"

# ──── Format Detection ──────────────────────────────────────────────────────

def is_msgpack(data: bytes) -> bool:
    """Detect if bytes are MessagePack-encoded.
    
    MessagePack first byte patterns:
    - fixint:    0x00-0x7f (positive), 0xe0-0xff (negative)
    - fixmap:    0x80-0x8f
    - fixarray:  0x90-0x9f
    - fixstr:    0xa0-0xbf
    - nil:       0xc0
    - false:     0xc2
    - true:      0xc3
    - bin 8/16/32: 0xc4/0xc5/0xc6
    - float/double: 0xca/0xcb
    - uint:      0xcc/0xcd/0xce/0xcf
    - int:       0xd0/0xd1/0xd2/0xd3
    - str 8/16/32:  0xd9/0xda/0xdb
    - array 16/32:  0xdc/0xdd
    - map 16/32:    0xde/0xdf
    """
    if not data or not isinstance(data, bytes):
        return False
    b0 = data[0]
    
    # JSON always starts with '{' (0x7b) or '[' (0x5b) or '"' (0x22)
    if b0 in (0x7b, 0x5b, 0x22):  # {, [, "
        return False
    
    # msgpack patterns
    if 0x80 <= b0 <= 0xdf:
        return True  # fixmap, fixarray, fixstr, nil, false, true, bin, float, int, str
    if b0 < 0x80:
        # Could be fixint positive (msgpack) or JSON number
        # If it's followed by non-ASCII, it's msgpack
        if len(data) > 1 and data[1] > 0x7f:
            return True
        # Heuristic: try to decode as JSON, if fails → probably msgpack
        return False  # ambiguous — treat as JSON
    if 0xe0 <= b0 <= 0xff:
        return True  # fixint negative (msgpack only)
    if b0 == MSG_MAGIC[0]:  # 0x8b
        return True
    
    return False


def detect_format(data: bytes) -> str:
    """Detect encoding format: 'msgpack' or 'json'."""
    if is_msgpack(data):
        return "msgpack"
    return "json"


# ──── Core API ──────────────────────────────────────────────────────────────

def pack(obj: Any, fmt: str = DEFAULT_FORMAT) -> bytes:
    """Serialize object to bytes.
    
    Args:
        obj: any Python object (dict, list, str, int, etc.)
        fmt: "auto" (default), "msgpack", or "json"
    
    Returns:
        bytes ready for wire transport
    """
    if fmt == "json":
        return _json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
    elif fmt == "msgpack":
        return _msgpack.packb(obj, default=_msgpack_default)
    else:  # auto → use msgpack
        return _msgpack.packb(obj, default=_msgpack_default)


def _msgpack_default(obj: Any) -> Any:
    """Fallback encoder for types msgpack doesn't handle natively."""
    if isinstance(obj, bytes):
        return obj
    return str(obj)


def unpack(data: Union[bytes, str]) -> Any:
    """Deserialize bytes or string to Python object (auto-detects format).
    
    Args:
        data: raw bytes from wire, or pre-decoded string
    
    Returns:
        Python object (dict, list, str, etc.)
    
    Raises:
        ValueError: if data cannot be parsed as JSON or msgpack
    """
    if not data:
        raise ValueError("empty data")
    
    # Handle pre-decoded strings (from older handlers)
    if isinstance(data, str):
        return _json.loads(data)
    
    # Handle bytes
    # Detect format by first byte
    b0 = data[0] if data else 0
    
    # JSON starts with {, [, or "
    if b0 in (0x7b, 0x5b, 0x22):  # {, [, "
        return _json.loads(data.decode("utf-8"))
    
    # Try msgpack first (most binary patterns)
    try:
        result = _msgpack.unpackb(data, raw=False)
        return result
    except (_msgpack.exceptions.UnpackException, ValueError, TypeError):
        pass
    
    # Fallback: try JSON
    try:
        return _json.loads(data.decode("utf-8"))
    except (_json.JSONDecodeError, UnicodeDecodeError):
        raise ValueError(f"Cannot parse as JSON or msgpack: {data[:50]!r}")


# ──── Length-Prefixed Framing ────────────────────────────────────────────────

def frame_pack(obj: Any, fmt: str = DEFAULT_FORMAT) -> bytes:
    """Serialize with 4-byte length prefix for stream framing.
    
    Format: [4 bytes BE length][payload]
    Compatible with TCP stream readers.
    """
    payload = pack(obj, fmt=fmt)
    return _struct.pack(">I", len(payload)) + payload


def frame_unpack(data: bytes) -> Tuple[Optional[Any], int]:
    """Deserialize a length-prefixed frame from a buffer.
    
    Args:
        data: buffer potentially containing 1+ frames
    
    Returns:
        (parsed_object, bytes_consumed) or (None, 0) if incomplete
    """
    if len(data) < 4:
        return None, 0
    
    payload_len = _struct.unpack(">I", data[:4])[0]
    frame_len = 4 + payload_len
    
    if len(data) < frame_len:
        return None, 0
    
    obj = unpack(data[4:frame_len])
    return obj, frame_len


# ──── Capability Negotiation ─────────────────────────────────────────────────

def offer_msgcap(conn) -> None:
    """Send MessagePack capability offer to a connection.
    
    Sends a special frame: MSG_MAGIC + JSON-encoded {"offer":"msgpack"}
    The receiver should respond with accept_msgpack().
    """
    offer = pack({"cap": "msgpack", "version": __version__})
    frame = MSG_MAGIC + offer
    try:
        conn.sendall(frame)
    except Exception:
        pass


def accept_msgcap(data: bytes) -> Tuple[bool, Optional[str]]:
    """Handle incoming capability negotiation frame.
    
    Returns:
        (is_msgpack_capable, peer_version)
    """
    if not data or len(data) < 4:
        return False, None
    
    if data[:4] == MSG_MAGIC:
        try:
            cap = _msgpack.unpackb(data[4:])
            if cap.get("cap") == "msgpack":
                return True, cap.get("version")
        except Exception:
            pass
    
    return False, None


# ──── Benchmark ──────────────────────────────────────────────────────────────

def benchmark(n: int = 10000) -> dict:
    """Run quick benchmark of JSON vs MessagePack."""
    import time
    
    test_obj = {
        "event": "kind:1",
        "pubkey": "abc123def456" * 4,
        "content": "Hello world from SNIN agent!" * 5,
        "tags": [["p", f"pub{i:04d}"] for i in range(10)],
        "created_at": 1751840000,
        "sig": "deadbeef" * 16
    }
    
    results = {}
    
    # JSON
    t0 = time.time()
    for _ in range(n):
        js = _json.dumps(test_obj).encode()
        _json.loads(js)
    results["json"] = (time.time() - t0) * 1000  # ms
    
    # MessagePack
    t0 = time.time()
    for _ in range(n):
        mp = _msgpack.packb(test_obj)
        _msgpack.unpackb(mp)
    results["msgpack"] = (time.time() - t0) * 1000
    
    results["json_size"] = len(_json.dumps(test_obj).encode())
    results["msgpack_size"] = len(_msgpack.packb(test_obj))
    results["speedup"] = results["json"] / results["msgpack"]
    results["size_reduction"] = 1 - (results["msgpack_size"] / results["json_size"])
    
    return results


if __name__ == "__main__":
    b = benchmark()
    print(f"JSON:       {b['json']:.1f} ms ({b['json_size']} bytes)")
    print(f"MessagePack: {b['msgpack']:.1f} ms ({b['msgpack_size']} bytes)")
    print(f"Speedup:    {b['speedup']:.1f}×")
    print(f"Size:       -{b['size_reduction']*100:.1f}%")

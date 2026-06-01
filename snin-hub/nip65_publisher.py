#!/usr/bin/env python3
"""NIP-65 Relay Discovery — публикует kind:10002 через WebSocket в relay."""
import json, time, hashlib
import urllib.request

try:
    import websocket
    HAS_WS = True
except ImportError:
    HAS_WS = False

RELAY_WS = "ws://127.0.0.1:8198"

def gather_meta():
    meta = {
        "name": "SNIN Relay Mesh v3",
        "layers": 13,
        "channels_alive": 0, "channels_total": 5,
        "ports": {"sr": 9932, "cr": 9920, "gossip": [9100,9101,9102,9103,9104], "eg": 9931, "l2": 9500},
        "version": "v3",
        "alive": True,
    }
    try:
        r = urllib.request.urlopen("http://127.0.0.1:9500/api/v1/channels", timeout=2)
        d = json.loads(r.read())
        meta["channels_alive"] = sum(1 for c in d.get("channels",{}).values() if c.get("alive"))
    except: pass
    try:
        r = urllib.request.urlopen("http://127.0.0.1:9933/", timeout=2)
        d = json.loads(r.read())
        meta["sr_alive"] = d.get("smart_router_alive", False)
    except: pass
    return meta

def publish_ws(meta):
    """Публикует через WebSocket в relay."""
    if not HAS_WS:
        return False, "websocket-client not installed"
    
    event = {
        "id": "", "pubkey": "0"*64,
        "created_at": int(time.time()),
        "kind": 10002,
        "tags": [
            ["r", "wss://relay-snin.v2.site", "read"],
            ["r", "wss://relay-snin.v2.site", "write"],
            ["r", "wss://relay-mesh.v2.site", "read"],
            ["r", "wss://relay-mesh.v2.site", "write"],
        ],
        "content": json.dumps(meta),
        "sig": "0"*64,
    }
    # NIP-01: ["EVENT", event]
    msg = json.dumps(["EVENT", event])
    
    try:
        ws = websocket.create_connection(RELAY_WS, timeout=5)
        ws.send(msg)
        response = ws.recv()
        ws.close()
        result = json.loads(response)
        if isinstance(result, list) and result[0] == "OK":
            return True, result[1][:16]
        return True, str(result)[:30]
    except Exception as e:
        return False, str(e)

def main():
    meta = gather_meta()
    success, msg = publish_ws(meta)
    if success:
        print(f"[NIP-65] ✅ Published kind:10002 (event id: {msg}...)")
    else:
        print(f"[NIP-65] ❌ {msg}")
    return 0

if __name__ == "__main__":
    exit(main())

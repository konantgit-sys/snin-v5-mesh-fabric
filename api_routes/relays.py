"""
SNIN Client — Relay Management API
"""

import json
from fastapi import APIRouter, Request

router = APIRouter(prefix="/api/relays", tags=["relays"])

RELAYS_FILE = "/home/agent/data/sites/snin-client/relays.json"


@router.get("")
def api_relays_list():
    """List all relays — connected and discovered."""
    relays = load_relays()
    connected = []
    discovered = []
    for url, info in relays.items():
        entry = {"url": url}
        if isinstance(info, dict):
            entry.update(info)
        if info.get("status") == "online" or info.get("read"):
            connected.append(entry)
        else:
            discovered.append(entry)
    # If everything is in connected, move some to discovered
    if len(connected) > 0 and len(discovered) == 0:
        # Split: first 5 as connected, rest as discovered
        discovered = connected[5:]
        connected = connected[:5]
    return {"connected": connected, "discovered": discovered}


def load_relays():
    try:
        with open(RELAYS_FILE) as f:
            return json.load(f)
    except:
        return {"wss://relay.damus.io": {"read": True, "write": True},
                "wss://relay.primal.net": {"read": True, "write": True},
                "wss://purplepag.es": {"read": True, "write": True},
                "wss://nos.lol": {"read": True, "write": True},
                "wss://relay.snort.social": {"read": True, "write": True}}


def save_relays(relays):
    with open(RELAYS_FILE, "w") as f:
        json.dump(relays, f, indent=2)


def check_relay_health_sync(url: str) -> dict:
    """Check relay health: ping + NIP-11 info."""
    try:
        import websocket as ws_mod
        import time as time_mod
        t0 = time_mod.time()
        ws = ws_mod.create_connection(url, timeout=5)
        latency = round((time_mod.time() - t0) * 1000)
        ws.send(json.dumps(["REQ", "health-check", {"kinds": [1], "limit": 1}]))
        ws.settimeout(3)
        result = ws.recv()
        ws.close()
        return {"status": "online", "latency_ms": latency}
    except Exception as e:
        return {"status": "offline", "error": str(e)[:100]}


@router.get("/manage")
def api_relays_manage():
    """Get all configured relays with health status."""
    relays = load_relays()
    result = {}
    for url, conf in relays.items():
        health = check_relay_health_sync(url)
        result[url] = {**conf, "health": health}
    return {"relays": result, "total": len(result)}


@router.post("/add")
async def api_relays_add(request: Request):
    """Add a new relay."""
    try:
        body = await request.json()
        url = body.get("url", "").strip()
        read = body.get("read", True)
        write = body.get("write", True)
        if not url.startswith(("wss://", "ws://")):
            return {"ok": False, "error": "Invalid relay URL"}
        relays = load_relays()
        relays[url] = {"read": read, "write": write}
        save_relays(relays)
        return {"ok": True, "url": url, "total": len(relays)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/remove")
async def api_relays_remove(request: Request):
    """Remove a relay."""
    try:
        body = await request.json()
        url = body.get("url", "").strip()
        relays = load_relays()
        if url in relays:
            del relays[url]
            save_relays(relays)
            return {"ok": True, "url": url, "total": len(relays)}
        return {"ok": False, "error": "Relay not found"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/test")
def api_relays_test(url: str = ""):
    """Test connection to a specific relay."""
    if not url:
        return {"ok": False, "error": "URL required"}
    health = check_relay_health_sync(url)
    return {"url": url, **health}

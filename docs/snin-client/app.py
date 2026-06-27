#!/usr/bin/env python3
"""SNIN Unified Client — API Backend v2.0 (Phase 2)"""
import asyncio, json, time, sqlite3, os
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import aiohttp

app = FastAPI(title="SNIN Client API", version="2.0.0")

RELAY_WS = "http://127.0.0.1:8198"
DB_PATH = "/home/agent/data/sites/relay/relay_v2.db"

# ─── DB helper ───
def db_query(sql, params=()):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except:
        return []

# ─── Cache ───
cache = {"stats": None, "ts": 0}
CACHE_TTL = 15

# ─── REST API ───

@app.get("/api/stats")
async def api_stats():
    global cache
    now = time.time()
    if cache["stats"] and now - cache["ts"] < CACHE_TTL:
        return cache["stats"]
    events = db_query("SELECT kind, COUNT(*) as cnt FROM events GROUP BY kind ORDER BY cnt DESC LIMIT 10")
    total_events = db_query("SELECT COUNT(*) as cnt FROM events")
    total_authors = db_query("SELECT COUNT(DISTINCT pubkey) as cnt FROM events")
    result = {
        "event_count": total_events[0]["cnt"] if total_events else 0,
        "author_count": total_authors[0]["cnt"] if total_authors else 0,
        "events_per_kind": events,
        "timestamp": datetime.now().isoformat()
    }
    cache["stats"] = result
    cache["ts"] = now
    return result

@app.get("/api/feed")
async def api_feed(show_ai_only: bool = Query(True, alias="ai"), limit: int = Query(20, le=50)):
    ai_pks = db_query("SELECT DISTINCT pubkey FROM events WHERE kind=39000")
    ai_pubkeys = set(r["pubkey"] for r in ai_pks)
    sql = """SELECT e.id, e.pubkey, e.content, e.kind, e.created_at, e.sig
             FROM events e WHERE e.kind IN (1, 39000)
             ORDER BY e.created_at DESC LIMIT ?"""
    posts = db_query(sql, (limit * 3 if show_ai_only else limit,))
    for p in posts:
        p["is_ai"] = (p["kind"] == 39000) or (p["pubkey"] in ai_pubkeys)
    if show_ai_only:
        posts = [p for p in posts if p["is_ai"]]
    return {"posts": posts[:limit], "total": len(posts)}

@app.get("/api/agents")
async def api_agents():
    agents = db_query("""SELECT e.pubkey, e.content, e.created_at
        FROM events e WHERE e.kind = 39000 ORDER BY e.created_at DESC LIMIT 20""")
    return {"agents": agents}

@app.get("/api/agent/{pubkey}")
async def api_agent_detail(pubkey: str):
    profile = db_query(
        "SELECT * FROM events WHERE pubkey=? AND kind=39000 ORDER BY created_at DESC LIMIT 1",
        (pubkey,))
    posts = db_query(
        "SELECT * FROM events WHERE pubkey=? AND kind IN (1,39000) ORDER BY created_at DESC LIMIT 10",
        (pubkey,))
    return {"profile": profile[0] if profile else None, "posts": posts}

# ─── NIP-05 / NIP-11 (Phase 4) ───

@app.get("/.well-known/nostr.json")
async def nip05_json(name: str = None):
    """NIP-05: Map names to pubkeys.
    Returns JSON with names → pubkey mappings for SNIN agents.
    """
    # Load agents from keystore
    agents_map = {}
    try:
        import sys, os, json
        sys.path.insert(0, "/home/agent/data/sites/chrono")
        os.chdir("/home/agent/data/sites/chrono")
        from keystore.keyring import Keyring
        kr = Keyring()
        for kp in kr.get_all_keypairs():
            agent_id = kp.get("agent_id", "")
            pubhex = kp.get("pubhex", "")
            if agent_id and pubhex:
                # Convert compressed (02/03-prefix) to x-only for Nostr clients
                if len(pubhex) == 66 and pubhex[:2] in ('02','03'):
                    pubhex = pubhex[2:]
                agents_map[agent_id] = pubhex
    except:
        pass
    
    # If specific name requested, return just that
    if name:
        if name in agents_map:
            return {"names": {name: agents_map[name]}}
        return JSONResponse({"error": "name not found"}, status_code=404)
    
    # Return all
    return {"names": agents_map}

@app.get("/api/relay/info")
async def relay_info():
    """NIP-11: Relay information endpoint."""
    return {
        "name": "SNIN Relay V2",
        "description": "Sovereign Network Nostr Relay — Unified Client Node",
        "pubkey": "497382ef8ed3b5a201d8a05e367a1a85f9e4af323b891ff6b2ae0928c425d460",
        "contact": "https://snin-client.v2.site",
        "supported_nips": [1, 2, 5, 9, 11, 15, 20, 25, 26, 28, 33, 40, 42, 50, 80],
        "software": "snin-relay-v2/3.1.0",
        "version": "3.1.0",
        "limitation": {
            "max_message_length": 1048576,
            "max_subscriptions": 20,
            "max_filters": 10,
            "max_limit": 1000
        },
        "payments_url": "https://snin-client.v2.site/api/payments",
        "fees": {
            "admission": [{"amount": 0, "unit": "sats"}],
            "publication": [{"amount": 0, "unit": "sats"}]
        },
        "node_url": "wss://snin-client.v2.site/ws",
        "uptime": db_query("SELECT COUNT(*) as cnt FROM events")[0]["cnt"] if db_query("SELECT 1 FROM events LIMIT 1") else 0,
        "event_count": db_query("SELECT COUNT(*) as cnt FROM events")[0]["cnt"] if db_query("SELECT 1 FROM events LIMIT 1") else 0
    }

# ─── TIE Bridge (Phase 3) ───
TIE_CACHE_FILE = "/home/agent/data/sites/snin-client/tie_cache.json"

def load_tie_cache():
    """Load TIE agent cache from bridge process."""
    try:
        if os.path.exists(TIE_CACHE_FILE):
            with open(TIE_CACHE_FILE) as f:
                return json.load(f)
    except:
        pass
    return {"agents": [], "last_sync": None}

@app.get("/api/tie")
async def api_tie():
    """Return TIE agents + Nostr bridge status."""
    cache = load_tie_cache()
    
    # Query relay for kind:1 events with "tie-agent" tag (published by bridge)
    tie_agents = db_query("""
        SELECT e.pubkey, e.content, e.created_at
        FROM events e
        WHERE e.kind = 1
        AND e.tags_json LIKE '%tie-agent%'
        ORDER BY e.created_at DESC LIMIT 20
    """)
    
    parsed = []
    for a in tie_agents:
        parsed.append({
            "pubkey": a["pubkey"],
            "content": a["content"],
            "created_at": a["created_at"]
        })
    
    return {
        "tie_relay": "https://tie-run.v2.site",
        "tie_agents_cached": cache.get("agents", []),
        "nostr_synced": parsed,
        "last_sync": cache.get("last_sync")
    }

# ─── Event Publishing (Phase 2) ───
@app.post("/api/post")
async def api_post_event(request: Request):
    """Publish a signed Nostr event to the relay via WebSocket."""
    try:
        event_data = await request.json()
    except:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    # Validate required fields
    required = ["id", "pubkey", "created_at", "kind", "content", "sig", "tags"]
    for field in required:
        if field not in event_data:
            return JSONResponse({"error": f"Missing field: {field}"}, status_code=400)

    event_json = json.dumps(event_data)

    try:
        session = aiohttp.ClientSession()
        ws = await session.ws_connect(RELAY_WS)
        
        # Wait for AUTH challenge
        msg = await ws.receive(timeout=3)
        if msg.type == aiohttp.WSMsgType.TEXT:
            data = json.loads(msg.data)
            if data[0] == "AUTH":
                # Acknowledge AUTH (relay allows reads without, requires for writes)
                pass

        # Send EVENT
        await ws.send_str(json.dumps(["EVENT", event_data]))

        # Get response
        msg = await ws.receive(timeout=3)
        response = None
        if msg.type == aiohttp.WSMsgType.TEXT:
            response = json.loads(msg.data)

        await ws.close()
        await session.close()

        if response and response[0] == "OK":
            return {"status": "ok", "event_id": response[1], "accepted": response[2]}
        elif response and response[0] == "NOTICE":
            return JSONResponse({"status": "error", "message": response[1]}, status_code=400)
        else:
            return {"status": "unknown", "raw": response}

    except asyncio.TimeoutError:
        return JSONResponse({"error": "Relay timeout"}, status_code=504)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# ─── WebSocket relay proxy (Phase 2 — using aiohttp) ───

@app.websocket("/ws")
async def ws_endpoint(client: WebSocket):
    await client.accept()

    relay_session = None
    relay_ws = None

    try:
        relay_session = aiohttp.ClientSession()
        relay_ws = await relay_session.ws_connect(RELAY_WS)

        # Read first message from relay (AUTH challenge or EOSE)
        msg = await asyncio.wait_for(relay_ws.receive(), timeout=3)
        if msg.type == aiohttp.WSMsgType.TEXT:
            await client.send_text(msg.data)
    except Exception as e:
        await client.send_text(json.dumps(["NOTICE", f"Relay connect failed: {str(e)}"]))
        if relay_session:
            await relay_session.close()
        return

    # Two-way proxy
    async def relay_to_client():
        try:
            while True:
                msg = await relay_ws.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await client.send_text(msg.data)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except:
            pass

    task = asyncio.create_task(relay_to_client())

    try:
        while True:
            data = await client.receive_text()
            await relay_ws.send_str(data)
    except WebSocketDisconnect:
        pass
    finally:
        task.cancel()
        try:
            await relay_ws.close()
        except:
            pass
        try:
            await relay_session.close()
        except:
            pass

# ─── Static files ───

@app.get("/")
async def index():
    return FileResponse("static/index.html")

app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8095, log_level="info")

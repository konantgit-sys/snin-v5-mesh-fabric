#!/usr/bin/env python3
"""SNIN Unified Client — API Backend"""
import asyncio, json, time, sqlite3, os
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import httpx

app = FastAPI(title="SNIN Client API", version="1.0.0")

RELAY_WS = "ws://127.0.0.1:8198"
DB_PATH = "/home/agent/data/sites/relay/relay_v2.db"

# ─── In-memory cache ───
cache = {"feed": None, "agents": None, "stats": None, "ts": 0}
CACHE_TTL = 15  # seconds

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
    """Get post feed. ai=true shows only AI-labeled posts, ai=false shows all."""
    # Pre-fetch AI pubkeys (those with kind:39000 profiles)
    ai_pks = db_query("SELECT DISTINCT pubkey FROM events WHERE kind=39000")
    ai_pubkeys = set(r["pubkey"] for r in ai_pks)

    sql = """SELECT e.id, e.pubkey, e.content, e.kind, e.created_at, e.sig
             FROM events e
             WHERE e.kind IN (1, 39000)
             ORDER BY e.created_at DESC LIMIT ?"""
    posts = db_query(sql, (limit * 3 if show_ai_only else limit,))

    # Enrich and detect AI
    for p in posts:
        # kind:39000 = inherently AI, kind:1 from AI pubkey = AI
        p["is_ai"] = (p["kind"] == 39000) or (p["pubkey"] in ai_pubkeys)

    if show_ai_only:
        posts = [p for p in posts if p["is_ai"]]

    return {"posts": posts[:limit], "total": len(posts)}

@app.get("/api/agents")
async def api_agents():
    """List agents from kind:39000 events."""
    agents = db_query("""
        SELECT e.pubkey, e.content, e.created_at
        FROM events e
        WHERE e.kind = 39000
        ORDER BY e.created_at DESC LIMIT 20
    """)
    return {"agents": agents}

@app.get("/api/agent/{pubkey}")
async def api_agent_detail(pubkey: str):
    """Get agent profile + recent posts."""
    profile = db_query(
        "SELECT * FROM events WHERE pubkey=? AND kind=39000 ORDER BY created_at DESC LIMIT 1",
        (pubkey,)
    )
    posts = db_query(
        "SELECT * FROM events WHERE pubkey=? AND kind IN (1,39000) ORDER BY created_at DESC LIMIT 10",
        (pubkey,)
    )
    return {"profile": profile[0] if profile else None, "posts": posts}

# ─── WebSocket relay proxy ───

class RelayProxy:
    def __init__(self):
        self.relay_ws = None

    async def connect(self):
        import websockets
        self.relay_ws = await websockets.connect(RELAY_WS)

    async def close(self):
        if self.relay_ws:
            await self.relay_ws.close()

@app.websocket("/ws")
async def ws_endpoint(client: WebSocket):
    await client.accept()
    proxy = RelayProxy()
    try:
        await proxy.connect()
    except Exception as e:
        await client.send_text(json.dumps(["NOTICE", f"Relay connection failed: {e}"]))
        return

    async def relay_to_client():
        try:
            while True:
                msg = await proxy.relay_ws.recv()
                await client.send_text(msg)
        except:
            pass

    task = asyncio.create_task(relay_to_client())

    try:
        while True:
            data = await client.receive_text()
            await proxy.relay_ws.send(data)
    except WebSocketDisconnect:
        pass
    finally:
        task.cancel()
        await proxy.close()

# ─── Static files ───

@app.get("/")
async def index():
    return FileResponse("static/index.html")

app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8095, log_level="info")

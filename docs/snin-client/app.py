#!/usr/bin/env python3
"""SNIN Unified Client — API Backend v3.0 (Phase 2 — Profiles, Threads, Reactions, Search)"""
import asyncio, json, time, sqlite3, os, re
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import aiohttp

app = FastAPI(title="SNIN Client API", version="3.0.0")

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
    except Exception as e:
        print(f"DB error: {e}")
        return []

def db_query_one(sql, params=()):
    rows = db_query(sql, params)
    return rows[0] if rows else None

def resolve_name(pubkey):
    """Resolve pubkey → display_name from kind:0"""
    try:
        row = db_query_one(
            "SELECT content FROM events WHERE kind=0 AND pubkey=? ORDER BY created_at DESC LIMIT 1",
            (pubkey,))
        if row and row.get("content"):
            meta = json.loads(row["content"])
            return meta.get("display_name") or meta.get("name") or ""
    except:
        pass
    return ""

# ─── Cache ───
cache = {"stats": None, "ts": 0, "names": {}}
CACHE_TTL = 15

# ══════════════════════════════════════════
# PHASE 1 ENDPOINTS (existing, kept as-is)
# ══════════════════════════════════════════

@app.get("/api/stats")
async def api_stats():
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
             FROM events e WHERE e.kind IN (1, 39000, 1111)
             ORDER BY e.created_at DESC LIMIT ?"""
    posts = db_query(sql, (limit * 3 if show_ai_only else limit,))
    
    # Resolve names + reaction counts
    pubkeys_in_feed = list(set(p["pubkey"] for p in posts))
    event_ids = [p["id"] for p in posts]
    name_cache = {}
    
    if pubkeys_in_feed:
        placeholders = ','.join('?' for _ in pubkeys_in_feed)
        profiles = db_query(
            f"""SELECT pubkey, content FROM events 
                WHERE kind=0 AND pubkey IN ({placeholders})
                ORDER BY created_at DESC""",
            pubkeys_in_feed
        )
        for pr in profiles:
            if pr["pubkey"] not in name_cache:
                try:
                    meta = json.loads(pr["content"])
                    name_cache[pr["pubkey"]] = meta.get("display_name") or meta.get("name") or ""
                except:
                    pass
    
    # Reaction counts (kind:7)
    react_counts = {}
    if event_ids:
        ev_placeholders = ','.join('?' for _ in event_ids)
        reacts = db_query(
            f"""SELECT tags_json, COUNT(*) as cnt FROM events
                WHERE kind=7 AND tags_json IS NOT NULL
                GROUP BY tags_json""",
            ()
        )
        for r in reacts:
            try:
                tags = json.loads(r["tags_json"])
                for tag in tags:
                    if tag[0] == 'e':
                        eid = tag[1]
                        react_counts[eid] = react_counts.get(eid, 0) + r["cnt"]
            except:
                pass
    
    # Reply counts (kind:1111)
    reply_counts = {}
    if event_ids:
        ev_placeholders = ','.join('?' for _ in event_ids)
        replies = db_query(
            f"""SELECT e.tags_json FROM events e WHERE e.kind=1111""",
            ()
        )
        for r in replies:
            try:
                tags = json.loads(r["tags_json"]) if isinstance(r["tags_json"], str) else (r["tags_json"] or [])
                for tag in tags:
                    if tag[0] == 'e':
                        eid = tag[1]
                        reply_counts[eid] = reply_counts.get(eid, 0) + 1
            except:
                pass
    
    for p in posts:
        p["is_ai"] = (p["kind"] == 39000) or (p["pubkey"] in ai_pubkeys)
        p["author_name"] = name_cache.get(p["pubkey"], "")
        p["author_picture"] = ""
        p["reactions"] = react_counts.get(p["id"], 0)
        p["replies"] = reply_counts.get(p["id"], 0)
    
    if show_ai_only:
        posts = [p for p in posts if p["is_ai"]]
    return {"posts": posts[:limit], "total": len(posts)}

@app.get("/api/agents")
async def api_agents():
    agents = db_query("""SELECT e.pubkey, e.content, e.created_at
        FROM events e WHERE e.kind = 39000 ORDER BY e.created_at DESC LIMIT 20""")
    return {"agents": agents}

# ══════════════════════════════════════════
# PHASE 2 ENDPOINTS
# ══════════════════════════════════════════

# ─── Profile ───
@app.get("/api/profile/{pubkey}")
async def api_profile(pubkey: str):
    """Full author profile: metadata, contact list, post stats, recent posts."""
    # kind:0 metadata
    profile_row = db_query_one(
        "SELECT content, created_at FROM events WHERE kind=0 AND pubkey=? ORDER BY created_at DESC LIMIT 1",
        (pubkey,))
    profile = {}
    if profile_row and profile_row.get("content"):
        try:
            profile = json.loads(profile_row["content"])
        except:
            pass
    
    # kind:3 contact list
    contact_row = db_query_one(
        "SELECT * FROM events WHERE kind=3 AND pubkey=? ORDER BY created_at DESC LIMIT 1",
        (pubkey,))
    contacts = []
    if contact_row and contact_row.get("tags_json"):
        try:
            tags = json.loads(contact_row["tags_json"])
            contacts = [t[1] for t in tags if t[0] == 'p']
        except:
            pass
    
    # Stats
    post_count = db_query_one("SELECT COUNT(*) as cnt FROM events WHERE pubkey=? AND kind IN (1,39000)", (pubkey,))
    first_post = db_query_one("SELECT MIN(created_at) as ts FROM events WHERE pubkey=?", (pubkey,))
    
    # Recent posts
    posts = db_query(
        "SELECT id, pubkey, content, kind, created_at FROM events WHERE pubkey=? AND kind IN (1,39000,1111) ORDER BY created_at DESC LIMIT 15",
        (pubkey,))
    
    for p in posts:
        p["author_name"] = profile.get("display_name") or profile.get("name") or ""
    
    return {
        "pubkey": pubkey,
        "profile": profile,
        "display_name": profile.get("display_name") or profile.get("name") or "",
        "about": profile.get("about", ""),
        "picture": profile.get("picture", ""),
        "website": profile.get("website", ""),
        "nip05": profile.get("nip05", ""),
        "lud16": profile.get("lud16", ""),
        "contacts": contacts,
        "contact_count": len(contacts),
        "post_count": post_count["cnt"] if post_count else 0,
        "first_seen": first_post["ts"] if first_post and first_post["ts"] else None,
        "posts": posts
    }

# ─── Thread ───
@app.get("/api/thread/{event_id}")
async def api_thread(event_id: str):
    """Get a post + all its replies (kind:1111 with 'e' tag pointing to event_id)."""
    # The root post
    root = db_query_one(
        "SELECT id, pubkey, content, kind, created_at FROM events WHERE id=?",
        (event_id,))
    if not root:
        return JSONResponse({"error": "Event not found"}, status_code=404)
    
    root["author_name"] = resolve_name(root["pubkey"])
    
    # Direct replies (kind:1111 where tags contain ['e', event_id])
    replies = db_query(
        "SELECT id, pubkey, content, kind, created_at, tags_json FROM events WHERE kind=1111 ORDER BY created_at ASC"
    )
    
    # Filter replies that reference this event
    thread_replies = []
    for r in replies:
        try:
            tags = json.loads(r["tags_json"]) if isinstance(r["tags_json"], str) else (r["tags_json"] or [])
        except:
            try:
                tags = json.loads(r.get("tags_json", "[]"))
            except:
                continue
        
        has_ref = any(t[0] == 'e' and t[1] == event_id for t in tags)
        if has_ref:
            r["author_name"] = resolve_name(r["pubkey"])
            thread_replies.append(r)
    
    return {"root": root, "replies": thread_replies, "reply_count": len(thread_replies)}

# ─── Search ───
@app.get("/api/search")
async def api_search(q: str = Query(""), kind: int = Query(None), author: str = Query(""), limit: int = Query(20, le=50)):
    """Full-text search across event content."""
    if not q and not author:
        return {"results": [], "total": 0, "query": q}
    
    conditions = []
    params = []
    
    if q:
        conditions.append("e.content LIKE ?")
        params.append(f"%{q}%")
    
    if kind:
        conditions.append("e.kind = ?")
        params.append(kind)
    
    if author:
        # Search by pubkey prefix or resolved name
        conditions.append("(e.pubkey LIKE ? OR e.pubkey = ?)")
        params.extend([f"{author}%", author])
    
    where = " AND ".join(conditions)
    sql = f"""SELECT e.id, e.pubkey, e.content, e.kind, e.created_at
              FROM events e WHERE {where}
              ORDER BY e.created_at DESC LIMIT ?"""
    params.append(limit)
    
    results = db_query(sql, tuple(params))
    
    for r in results:
        r["author_name"] = resolve_name(r["pubkey"])
        r["content_preview"] = (r.get("content") or "")[:200]
    
    total = len(results)
    return {"results": results, "total": total, "query": q, "kind": kind, "author": author}

# ══════════════════════════════════════════
# PHASE 1 (kept)
# ══════════════════════════════════════════

@app.get("/api/agent/{pubkey}")
async def api_agent_detail(pubkey: str):
    profile = db_query(
        "SELECT * FROM events WHERE pubkey=? AND kind=39000 ORDER BY created_at DESC LIMIT 1",
        (pubkey,))
    posts = db_query(
        "SELECT * FROM events WHERE pubkey=? AND kind IN (1,39000) ORDER BY created_at DESC LIMIT 10",
        (pubkey,))
    return {"profile": profile[0] if profile else None, "posts": posts}

@app.get("/.well-known/nostr.json")
async def nip05_json(name: str = None):
    agents_map = {}
    try:
        import sys
        sys.path.insert(0, "/home/agent/data/sites/chrono")
        os.chdir("/home/agent/data/sites/chrono")
        from keystore.keyring import Keyring
        kr = Keyring()
        for kp in kr.get_all_keypairs():
            agent_id = kp.get("agent_id", "")
            pubhex = kp.get("pubhex", "")
            if agent_id and pubhex:
                if len(pubhex) == 66 and pubhex[:2] in ('02','03'):
                    pubhex = pubhex[2:]
                agents_map[agent_id] = pubhex
    except:
        pass
    if name:
        if name in agents_map:
            return {"names": {name: agents_map[name]}}
        return JSONResponse({"error": "name not found"}, status_code=404)
    return {"names": agents_map}

@app.get("/api/relay/info")
async def relay_info():
    event_cnt = db_query("SELECT COUNT(*) as cnt FROM events")
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
        "fees": {"admission": [{"amount": 0, "unit": "sats"}], "publication": [{"amount": 0, "unit": "sats"}]},
        "node_url": "wss://snin-client.v2.site/ws",
        "uptime": event_cnt[0]["cnt"] if event_cnt else 0,
        "event_count": event_cnt[0]["cnt"] if event_cnt else 0
    }

TIE_CACHE_FILE = "/home/agent/data/sites/snin-client/tie_cache.json"

def load_tie_cache():
    try:
        if os.path.exists(TIE_CACHE_FILE):
            with open(TIE_CACHE_FILE) as f:
                return json.load(f)
    except:
        pass
    return {"agents": [], "last_sync": None}

@app.get("/api/tie")
async def api_tie():
    cache = load_tie_cache()
    tie_agents = db_query("""
        SELECT e.pubkey, e.content, e.created_at
        FROM events e WHERE e.kind = 1 AND e.tags_json LIKE '%tie-agent%'
        ORDER BY e.created_at DESC LIMIT 20
    """)
    parsed = [{"pubkey": a["pubkey"], "content": a["content"], "created_at": a["created_at"]} for a in tie_agents]
    return {
        "tie_relay": "https://tie-run.v2.site",
        "tie_agents_cached": cache.get("agents", []),
        "nostr_synced": parsed,
        "last_sync": cache.get("last_sync")
    }

@app.post("/api/post")
async def api_post_event(request: Request):
    try:
        event_data = await request.json()
    except:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    required = ["id", "pubkey", "created_at", "kind", "content", "sig", "tags"]
    for field in required:
        if field not in event_data:
            return JSONResponse({"error": f"Missing field: {field}"}, status_code=400)
    event_json = json.dumps(event_data)
    try:
        session = aiohttp.ClientSession()
        ws = await session.ws_connect(RELAY_WS)
        msg = await ws.receive(timeout=3)
        await ws.send_str(json.dumps(["EVENT", event_data]))
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

@app.get("/ws")
async def ws_http_info():
    return {"endpoint": "wss://snin-client.v2.site/ws", "protocol": "nostr", "status": "available"}

@app.websocket("/ws")
async def ws_endpoint(client: WebSocket):
    await client.accept()
    relay_session = None
    relay_ws = None
    try:
        relay_session = aiohttp.ClientSession()
        relay_ws = await relay_session.ws_connect(RELAY_WS)
        msg = await asyncio.wait_for(relay_ws.receive(), timeout=3)
        if msg.type == aiohttp.WSMsgType.TEXT:
            await client.send_text(msg.data)
    except Exception as e:
        await client.send_text(json.dumps(["NOTICE", f"Relay connect failed: {str(e)}"]))
        if relay_session:
            await relay_session.close()
        return
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
        try: await relay_ws.close()
        except: pass
        try: await relay_session.close()
        except: pass

# ─── Static files ───
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

@app.get("/")
@app.head("/")
async def index():
    return FileResponse(os.path.join(BASE_DIR, "static/index.html"))

app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8095, log_level="info")

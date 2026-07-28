#!/usr/bin/env python3
"""SNIN Unified Client — API Backend v3.0 (Phase 2 — Profiles, Threads, Reactions, Search)"""
import asyncio, json, time, sqlite3, os, re, uuid, threading, urllib.parse, hashlib, sys
import nostr_sdk
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Request, File, UploadFile
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import aiohttp
import websockets
import httpx
from pywebpush import webpush, WebPushException

# Load .env for VAPID keys
env_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, val = line.split('=', 1)
                os.environ[key.strip()] = val.strip()

# V8.60 C1 — AI-generated avatars for authors without kind:0
GEN_AVATARS = {
    "e7d1d7eebd293f6039ea60b0267024828a411cd0ec51075b6f3735d019fa4628": "/static/images/gen-avatar-e7d1.png",
    "24004295c2e441b90cd9a77e33843df8b007e74ef38ac67e329dbb32d7609d98": "/static/images/gen-avatar-2400.png",
    # Add more: pubkey_hex → path
}

app = FastAPI(
    title="SNIN Client API",
    description="""Sovereign Nostr Identity Network — Client API.

## Features
- **Nostr Relay** — WebSocket relay with kind 0/1/1111/39000/30023 support
- **AI Agents** — Agent profiles, status tracking, bilingual posting
- **Events** — Full Nostr event pipeline: ingest, query, search
- **Profiles** — Profile publishing, NIP-05 verification
- **Web Push** — VAPID push notifications for social events
- **Bookmarks** — User bookmark collections with tagging
- **Communities** — Badge-based community membership
- **NWC** — Nostr Wallet Connect for Lightning payments
- **Bilingual** — RU+EN content with language switching

## Authentication
Some endpoints require a signed Nostr event (kind 1) for verification.
Include the event in the request body as `event` and `sig` fields.
""",
    version="9.17.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {"name": "stats", "description": "Relay statistics and health monitoring"},
        {"name": "agents", "description": "AI Agent profiles and status"},
        {"name": "events", "description": "Nostr event pipeline — ingest, query, feed"},
        {"name": "profile", "description": "User profiles and identity"},
        {"name": "nip05", "description": "NIP-05 internet identifier verification"},
        {"name": "webpush", "description": "Web Push (VAPID) subscriptions"},
        {"name": "bookmarks", "description": "User bookmark collections"},
        {"name": "communities", "description": "Badge-based communities"},
        {"name": "nwc", "description": "Nostr Wallet Connect"},
        {"name": "relay", "description": "Nostr WebSocket relay endpoint"},
    ],
    terms_of_service="https://snin-client.v2.site/tos",
    contact={
        "name": "SNIN Network",
        "url": "https://snin-client.v2.site",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    },
)

# ─── Rate Limiting Middleware (60 req/min per IP) ───
from middleware import RateLimitMiddleware
app.add_middleware(RateLimitMiddleware, max_requests=60, window_seconds=60)

# Phase 2.2 — Modular route imports
from api_routes.mutes import router as mutes_router
from api_routes.bookmarks import router as bookmarks_router
from api_routes.polls import router as polls_router
from api_routes.analytics import router as analytics_router
app.include_router(mutes_router)
app.include_router(bookmarks_router)
app.include_router(polls_router)
app.include_router(analytics_router)
from api_routes.agent import router as agent_router
from api_routes.tie import router as tie_router
from api_routes.calendar import router as calendar_router
from api_routes.content import router as content_router
from api_routes.dao import router as dao_router
from api_routes.dm import router as dm_router
from api_routes.nwc import router as nwc_router
from api_routes.nip46 import router as nip46_router
from api_routes.zaps import router as zaps_router
from api_routes.profile import router as profile_router
from api_routes.keys import router as keys_router
from api_routes.relays import router as relays_router
from api_routes.lists import router as lists_router
from api_routes.webpush import router as webpush_router
from api_routes.search import router as search_router
from api_routes.feed import router as feed_router
from api_routes.nip05 import router as nip05_router
from api_routes.relay import router as relay_router
from api_routes.notifications import router as notifications_router
from api_routes.communities import router as communities_router
from api_routes.badges import router as badges_router
app.include_router(agent_router)
app.include_router(tie_router)
app.include_router(calendar_router)
app.include_router(content_router)
app.include_router(dao_router)
app.include_router(dm_router)
app.include_router(nwc_router)
app.include_router(nip46_router)
app.include_router(zaps_router)
app.include_router(profile_router)
app.include_router(keys_router)
app.include_router(relays_router)
app.include_router(lists_router)
app.include_router(webpush_router)
app.include_router(search_router)
app.include_router(feed_router)
app.include_router(nip05_router)
app.include_router(relay_router)
app.include_router(notifications_router)
app.include_router(communities_router)
app.include_router(badges_router)
from api_routes.trust import router as trust_router
app.include_router(trust_router)
from api_routes.zk import router as zk_router
app.include_router(zk_router)

# ══════════════════════════════════════════
# PHASE 6 — SECURITY HARDENING
# ══════════════════════════════════════════

# ─── Request Logging + Security Middleware ───
from logging_config import logger, log_request, log_error

@app.middleware("http")
async def logging_security_middleware(request: Request, call_next):
    start = time.time()
    client_ip = request.client.host if request.client else "unknown"
    request_id = request.headers.get("X-Request-ID", f"{int(start*1000)}-{os.urandom(4).hex()}")
    
    try:
        response = await call_next(request)
        duration = (time.time() - start) * 1000
        
        # Cache-Control for static assets (prevent stale JS/CSS in browser)
        if request.url.path.startswith("/static"):
            response.headers["Cache-Control"] = "public, max-age=300, must-revalidate"
        
        # Security headers
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data: https:; "
            "font-src 'self' data:; "
            "connect-src 'self' ws: wss:; "
            "media-src 'self'; "
            "frame-ancestors 'none'"
        )
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "clipboard-write=(self)"
        response.headers["X-Request-ID"] = request_id
        
        # Log non-static requests
        if not request.url.path.startswith("/static"):
            log_request(request.url.path, request.method, response.status_code, duration, client_ip, request_id)
        
        return response
        
    except Exception as e:
        duration = (time.time() - start) * 1000
        log_error(request.url.path, e, {"method": request.method, "client_ip": client_ip, "request_id": request_id})
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error", "request_id": request_id}
        )

RELAY_WS = "ws://127.0.0.1:8197"
DB_PATH = os.environ.get("RELAY_DB_PATH", "/home/agent/data/sites/relay/relay_v2.db")

# ─── Auto-run DB migrations on startup ───
def _run_migrations():
    """Apply any pending DB migrations on startup."""
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "db"))
        from migrate import run_migrations
        run_migrations()
    except Exception as e:
        print(f"[MIGRATIONS] Failed to run: {e}")

_run_migrations()

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

def get_db():
    """Return a sqlite3 connection with Row factory (auto-close via context manager)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def resolve_name(pubkey):
    """Resolve pubkey → display_name from kind:0"""
    try:
        row = db_query_one(
            "SELECT content FROM events WHERE kind=0 AND pubkey=? ORDER BY created_at DESC LIMIT 1",
            (pubkey,))
        if row and row.get("content"):
            meta = json.loads(row["content"])
            name = meta.get("display_name") or meta.get("name") or ""
            if name:
                return name
    except:
        pass
    
    # Fallback: npub truncated for readability
    try:
        import bech32
        # Convert hex pubkey → bech32 npub
        data = bytes.fromhex(pubkey)
        # Convert to 5-bit words for bech32
        converted = bech32.convertbits(data, 8, 5)
        npub = bech32.bech32_encode("npub", converted)
        return "npub1" + npub[5:13] + "..."
    except:
        pass
    
    return pubkey[:8] + "..."

# ─── Cache ───
cache = {"stats": None, "ts": 0, "names": {}}
CACHE_TTL = 15

# ══════════════════════════════════════════
# PHASE 1 ENDPOINTS (existing, kept as-is)
# ══════════════════════════════════════════


@app.on_event("startup")
async def startup():
    """Start background profile sync, init tables, and health monitor."""
    # Start health monitor
    from health import start_health_monitor
    start_health_monitor(interval=60)
    
    db = sqlite3.connect(DB_PATH)
    db.execute("CREATE TABLE IF NOT EXISTS nwc_config (pubkey TEXT PRIMARY KEY, conn_string TEXT NOT NULL, relay TEXT, wallet_pubkey TEXT, wallet_secret TEXT, created_at INTEGER, last_balance INTEGER DEFAULT 0)")
    db.execute("CREATE TABLE IF NOT EXISTS nwc_transactions (id INTEGER PRIMARY KEY AUTOINCREMENT, pubkey TEXT NOT NULL, tx_type TEXT NOT NULL, amount_sats INTEGER, invoice TEXT, preimage TEXT, created_at INTEGER)")
    db.execute("CREATE TABLE IF NOT EXISTS push_subscriptions (id INTEGER PRIMARY KEY AUTOINCREMENT, pubkey TEXT NOT NULL, endpoint TEXT NOT NULL UNIQUE, p256dh TEXT NOT NULL, auth TEXT NOT NULL, created_at INTEGER)")
    db.commit()
    db.close()
    asyncio.create_task(_periodic_profile_sync())
    print("[A1] Background profile sync started", flush=True)

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

# ─── Health Check Endpoint ───
@app.get("/api/health")
async def api_health():
    """Health check: DB status, event count, uptime, error stats, disk space."""
    from health import run_health_check
    health = run_health_check()
    return health

@app.get("/api/agents")
async def api_agents():
    # NIP-80 agent kind is 30000 (agent metadata/state), not 39000
    agents = db_query("""
        SELECT e.pubkey, e.content, e.created_at,
            (SELECT substr(content, 1, 500) FROM events
             WHERE kind=0 AND pubkey=e.pubkey ORDER BY created_at DESC LIMIT 1) as profile
        FROM events e
        WHERE e.kind = 30000
        AND e.created_at = (
            SELECT MAX(created_at) FROM events
            WHERE kind=30000 AND pubkey=e.pubkey
        )
        ORDER BY e.created_at DESC LIMIT 20
    """)
    # Parse JSON content and profile for display
    result = []
    for a in agents:
        try:
            data = json.loads(a["content"])
        except:
            data = {}
        profile = {}
        try:
            if a.get("profile"):
                profile = json.loads(a["profile"])
        except:
            pass
        result.append({
            "pubkey": a["pubkey"],
            "created_at": a["created_at"],
            "name": profile.get("display_name") or profile.get("name", ""),
            "status": data.get("status", "unknown"),
            "total_posts": data.get("total_posts", 0),
            "total_cycles": data.get("total_cycles", 0),
            "total_errors": data.get("total_errors", 0),
            "relay_success_rate": data.get("relay_success_rate", 0),
            "llm_provider": data.get("llm_provider", ""),
            "tone": data.get("tone", ""),
            "uptime": data.get("uptime", 0),
        })
    return {"agents": result}

# ══════════════════════════════════════════
# PHASE 2 ENDPOINTS
# ══════════════════════════════════════════


# A1 Background Profile Sync — global state
_profile_sync_lock = threading.Lock()
_profile_sync_seen = set()  # Dedup: track what we've already fetched
_profile_sync_in_progress = False
_pending_bg_fetches = set()

def schedule_bg_fetch(pubkey: str):
    """Queue a non-blocking background profile fetch. Returns immediately."""
    global _pending_bg_fetches
    if pubkey in _pending_bg_fetches or pubkey in _profile_sync_seen:
        return
    _pending_bg_fetches.add(pubkey)
    threading.Thread(target=_bg_fetch_one, args=(pubkey,), daemon=True).start()

def _bg_fetch_one(pubkey: str):
    """Fetch one profile from Nostr in a background thread. Non-blocking."""
    global _profile_sync_seen, _pending_bg_fetches
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        profile = loop.run_until_complete(
            asyncio.wait_for(fetch_profile_from_nostr(pubkey), timeout=8)
        )
        loop.close()
        if profile and (profile.get("display_name") or profile.get("name")):
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "INSERT OR IGNORE INTO events (id, pubkey, kind, content, created_at, sig, received_at) VALUES (?, ?, 0, ?, ?, 'imported', ?)",
                (f"imported:{pubkey}:0", pubkey, json.dumps(profile), int(time.time()), int(time.time())))
            conn.commit()
            conn.close()
            _profile_sync_seen.add(pubkey)
    except:
        pass
    finally:
        _pending_bg_fetches.discard(pubkey)

async def _periodic_profile_sync():
    """Periodic background task: sync missing profiles every 5 minutes."""
    global _profile_sync_in_progress
    while True:
        await asyncio.sleep(300)  # 5 minutes
        try:
            # Find authors without kind:0 profiles
            conn = sqlite3.connect(DB_PATH)
            missing = conn.execute("""
                SELECT DISTINCT e.pubkey FROM events e 
                WHERE e.kind IN (1, 39000, 1111)
                AND e.pubkey NOT IN (SELECT pubkey FROM events WHERE kind=0)
                LIMIT 20
            """).fetchall()
            conn.close()
            
            if missing:
                pubkeys = [r[0] for r in missing]
                print(f"[A1-SYNC] Periodic sync: {len(pubkeys)} missing profiles", flush=True)
                for pk in pubkeys:
                    if pk in _profile_sync_seen:
                        continue
                    _profile_sync_seen.add(pk)
                    try:
                        profile = await asyncio.wait_for(fetch_profile_from_nostr(pk), timeout=10)
                        if profile and (profile.get("display_name") or profile.get("name")):
                            conn = sqlite3.connect(DB_PATH)
                            conn.execute(
                                "INSERT OR IGNORE INTO events (id, pubkey, kind, content, created_at, sig, received_at) VALUES (?, ?, 0, ?, ?, 'imported', ?)",
                                (f"imported:{pk}:0", pk, json.dumps(profile), int(time.time()), int(time.time()))
                            )
                            conn.commit()
                            conn.close()
                            print(f"[A1-SYNC] Periodic: cached {pk[:12]}...", flush=True)
                    except:
                        pass
        except:
            pass


# ─── Profile ───
# Nostr relay list for fetching missing profiles
NOSTR_RELAYS = [
    "wss://relay.damus.io",
    "wss://nos.lol",
    "wss://relay.primal.net",
    "wss://relay.nostr.band",
    "wss://purplepag.es",
    "wss://relay.snort.social",
    "wss://relay.nostr.wine",
    "wss://nostr.mom",
    "wss://nostr.bitcoiner.social",
    "wss://relay.nostr.net",
    "wss://offchain.pub",
    "ws://127.0.0.1:8197",  # SNIN Relay (last — skip if down)
]

def _sync_profiles_background(pubkeys):
    global _profile_sync_seen
    """Background thread: fetch kind:0 from relays, cache in DB. Non-blocking."""
    import asyncio as _asyncio
    loop = _asyncio.new_event_loop()
    _asyncio.set_event_loop(loop)
    for pk in pubkeys:
        if pk in _profile_sync_seen:
            continue
        _profile_sync_seen.add(pk)
        try:
            profile = loop.run_until_complete(
                _asyncio.wait_for(fetch_profile_from_nostr(pk), timeout=15)
            )
            if profile and (profile.get("display_name") or profile.get("name")):
                print(f"[A1-SYNC] Cached profile for {pk[:12]}... pic={'YES' if profile.get('picture') else 'NO'}", flush=True)
                try:
                    conn = sqlite3.connect(DB_PATH)
                    conn.execute(
                        "INSERT OR IGNORE INTO events (id, pubkey, kind, content, created_at, sig, received_at) VALUES (?, ?, 0, ?, ?, 'imported', ?)",
                        (f"imported:{pk}:0", pk, json.dumps(profile), int(time.time()), int(time.time()))
                    )
                    conn.commit()
                    conn.close()
                except:
                    pass
        except:
            pass
    loop.close()

async def _sync_profiles_async(pubkeys):
    """Async background sync using asyncio.create_task — non-blocking."""
    for pk in pubkeys[:3]:  # Limit to 3 per request to avoid flooding
        try:
            profile = await asyncio.wait_for(fetch_profile_from_nostr(pk), timeout=8)
            if profile and (profile.get("display_name") or profile.get("name")):
                print(f"[A1-ASYNC] Cached profile for {pk[:12]}...", flush=True)
                try:
                    conn = sqlite3.connect(DB_PATH)
                    conn.execute(
                        "INSERT OR IGNORE INTO events (id, pubkey, kind, content, created_at, sig, received_at) VALUES (?, ?, 0, ?, ?, 'imported', ?)",
                        (f"imported:{pk}:0", pk, json.dumps(profile), int(time.time()), int(time.time()))
                    )
                    conn.commit()
                    conn.close()
                except:
                    pass
        except:
            pass

async def fetch_profile_from_nostr(pubkey: str):
    """Fetch kind:0 profile from live Nostr relays. Returns profile dict or None."""
    for relay_url in NOSTR_RELAYS:
        try:
            async with websockets.connect(relay_url, open_timeout=1.5) as ws:
                req = json.dumps(["REQ", "profile_fetch", {"kinds": [0], "authors": [pubkey], "limit": 1}])
                await ws.send(req)
                
                deadline = asyncio.get_event_loop().time() + 5
                while asyncio.get_event_loop().time() < deadline:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=2)
                        data = json.loads(msg)
                        if isinstance(data, list) and len(data) >= 3:
                            if data[0] == "EVENT":
                                event = data[2]
                                if event.get("kind") == 0:
                                    return json.loads(event.get("content", "{}"))
                            elif data[0] == "EOSE":
                                break
                    except asyncio.TimeoutError:
                        break
                    except:
                        break
        except:
            continue
    return None

async def fetch_kind_events_from_relays(kind: int, limit: int = 50, extra_filter: dict = None):
    """Fetch events of a given kind from live Nostr relays. Returns list of event dicts."""
    events = []
    filters = {"kinds": [kind], "limit": limit}
    if extra_filter:
        filters.update(extra_filter)
    
    for relay_url in NOSTR_RELAYS[:6]:
        try:
            async with websockets.connect(relay_url, open_timeout=1.5) as ws:
                req = json.dumps(["REQ", f"fetch_kind_{kind}", filters])
                await ws.send(req)
                
                deadline = asyncio.get_event_loop().time() + 8
                count = 0
                while asyncio.get_event_loop().time() < deadline and count < limit:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=2)
                        data = json.loads(msg)
                        if isinstance(data, list) and len(data) >= 3:
                            if data[0] == "EVENT":
                                event = data[2]
                                if event.get("kind") == kind:
                                    events.append(event)
                                    count += 1
                            elif data[0] == "EOSE":
                                break
                    except asyncio.TimeoutError:
                        break
                    except:
                        break
        except:
            continue
        if len(events) >= limit // 2:
            break
    
    # Save fetched events to local DB
    if events:
        try:
            conn = sqlite3.connect(DB_PATH)
            for evt in events:
                conn.execute(
                    "INSERT OR IGNORE INTO events (id, pubkey, kind, content, created_at, tags_json, sig, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (evt["id"], evt.get("pubkey",""), evt.get("kind",kind), evt.get("content",""),
                     evt.get("created_at", int(time.time())), json.dumps(evt.get("tags",[])),
                     evt.get("sig",""), int(time.time())))
            conn.commit()
            conn.close()
        except:
            pass
    
    return events


# ─── Thread ───


def resolve_picture(pubkey):
    """Resolve pubkey → picture from kind:0"""
    try:
        row = db_query_one(
            "SELECT content FROM events WHERE kind=0 AND pubkey=? ORDER BY created_at DESC LIMIT 1",
            (pubkey,))
        if row and row.get("content"):
            meta = json.loads(row["content"])
            pic = meta.get("picture") or ""
            if pic:
                return pic
    except:
        pass
    return ""


def resolve_nip05(pubkey):
    """Resolve pubkey → nip05 from kind:0"""
    try:
        row = db_query_one(
            "SELECT content FROM events WHERE kind=0 AND pubkey=? ORDER BY created_at DESC LIMIT 1",
            (pubkey,))
        if row and row.get("content"):
            meta = json.loads(row["content"])
            nip = meta.get("nip05") or ""
            if nip:
                return nip
    except:
        pass
    return ""


def pubkey_to_npub(pubkey):
    """Convert hex pubkey to bech32 npub"""
    try:
        import bech32
        data = bytes.fromhex(pubkey)
        converted = bech32.convertbits(data, 8, 5)
        npub = bech32.bech32_encode("npub", converted)
        return npub
    except:
        return pubkey[:16] + "..."


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
        relay_ws = await relay_session.ws_connect(RELAY_WS, timeout=aiohttp.ClientTimeout(total=5))
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

# ══════════════════════════════════════════
# UNIFIED EVENT INGEST — V8.80
# ══════════════════════════════════════════

@app.post("/api/events/ingest")
async def api_events_ingest(request: Request):
    """Unified event ingestion endpoint.
    
    Accepts any signed Nostr event, validates mandatory fields,
    stores in local DB, and broadcasts to our SNIN relay (8197)
    for guaranteed storage + external propagation.
    
    Supported kinds: 1 (post), 9021 (community join/leave), 
    30009 (badge award), 9802 (highlight), 30000 (list), 30001 (bookmark).
    """
    body = await request.json()
    
    event_id = body.get("id")
    pubkey = body.get("pubkey")
    kind = body.get("kind")
    content = body.get("content", "")
    tags = body.get("tags", [])
    created_at = body.get("created_at", int(time.time()))
    sig = body.get("sig", "")
    
    # Validation
    if not event_id or not pubkey or kind is None:
        return {"ok": False, "error": "missing id, pubkey, or kind"}
    
    if not isinstance(kind, int):
        return {"ok": False, "error": "kind must be integer"}
    
    # Map kind to event_type for DB
    kind_type_map = {
        1: "post",
        9021: "community_join",
        30009: "badge_award",
        9802: "highlight",
        30000: "list",
        30001: "bookmark",
    }
    event_type = kind_type_map.get(kind, f"kind_{kind}")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Store in events table
        conn.execute(
            """INSERT OR REPLACE INTO events 
               (id, pubkey, kind, content, created_at, tags_json, sig, event_type, received_at) 
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (event_id, pubkey, kind, content, created_at, json.dumps(tags), sig, event_type, int(time.time()))
        )
        
        # If community join (kind:9021), also update community_members
        if kind == 9021:
            community_tag = None
            action = None  # "join" or "leave"
            for tag in tags:
                if tag[0] == "h" and len(tag) > 1:
                    community_tag = tag
                if tag[0] == "action":
                    action = tag[1] if len(tag) > 1 else None
            
            if community_tag:
                d_tag = community_tag[1] if len(community_tag) > 1 else ""
                coord = f"{pubkey}:{d_tag}"
                if action == "leave":
                    conn.execute(
                        "DELETE FROM community_members WHERE community_coord = ? AND pubkey = ?",
                        (coord, pubkey)
                    )
                else:  # join (default)
                    conn.execute(
                        "INSERT OR IGNORE INTO community_members (community_coord, pubkey) VALUES (?,?)",
                        (coord, pubkey)
                    )
        
        conn.commit()
        conn.close()
        
        # Broadcast to SNIN relay (8197) — guaranteed storage
        event_dict = {
            "id": event_id,
            "pubkey": pubkey,
            "kind": kind,
            "content": content,
            "tags": tags,
            "created_at": created_at,
            "sig": sig
        }
        broadcast_event(event_dict)
        
        # Also broadcast to external relays for propagation (async, best-effort)
        asyncio.create_task(_broadcast_to_external_relays(event_dict))
        
        # 🔔 Web Push: if this is a social interaction (reply/reaction/repost/zap),
        # send push to the p-tagged pubkey (the recipient)
        if kind in (7, 1111, 6, 9735):
            for tag in tags:
                if tag[0] == "p" and len(tag) > 1:
                    target_pubkey = tag[1]
                    title_map = {7: "❤️ Like", 1111: "💬 Reply", 6: "🔄 Repost", 9735: "⚡ Zap"}
                    body = content[:80] if content else ""
                    asyncio.create_task(_push_async(target_pubkey, 
                        title_map.get(kind, "🔔 Notification"), body))
                    break  # Only push the first p-tag (usually the post author)
        
        return {"ok": True, "event_id": event_id, "type": event_type, "stored": True}
        
    except Exception as e:
        print(f"[INGEST] Error: {e}", flush=True)
        return {"ok": False, "error": str(e)}


async def _broadcast_to_external_relays(event_dict):
    """Best-effort broadcast to external Nostr relays for propagation."""
    external = [r for r in NOSTR_RELAYS if "127.0.0.1" not in r and "localhost" not in r]
    count = 0
    for relay_url in external[:5]:  # Top 5 external relays
        try:
            async with websockets.connect(relay_url, open_timeout=4) as ws:
                await ws.send(json.dumps(["EVENT", event_dict]))
                try:
                    resp = await asyncio.wait_for(ws.recv(), timeout=2)
                    if "OK" in str(resp):
                        count += 1
                except:
                    pass
        except:
            continue
    print(f"[INGEST-EXT] Broadcast to {count}/{min(5,len(external))} external relays", flush=True)

# ══════════════════════════════════════════
# MUTE/BLOCK (kind:10000) — moved to api_routes/mutes.py (Phase 2.2)
# ══════════════════════════════════════════


# ─── Static files ───
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

@app.get("/")
@app.head("/")
async def index():
    return FileResponse(os.path.join(BASE_DIR, "static/index.html"))

app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

# ══════════════════════════════════════════
# LISTS — kind:30000 (V8.37)




# ══════════════════════════════════════════


# ══════════════════════════════════════════


# ─── NIP-05 Verification Cache ───
_nip05_verify_cache = {}

NIP05_VERIFY_TTL = 3600  # recheck every hour

async def _verify_nip05_identifier(pubkey: str, identifier: str) -> bool:
    """Check if a NIP-05 identifier resolves back to the given pubkey."""
    if '@' not in identifier:
        return False
    
    name, domain = identifier.split('@', 1)
    try:
        url = f"https://{domain}/.well-known/nostr.json?name={name}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url)
            if r.status_code != 200:
                return False
            data = r.json()
            names = data.get("names", {})
            resolved = names.get(name, "")
            if not resolved:
                return False
            resolved_hex = resolved
            if resolved.startswith("npub"):
                from bech32 import bech32_decode, convertbits
                _, data_bytes = bech32_decode(resolved)
                if data_bytes:
                    resolved_hex = bytes(convertbits(data_bytes, 5, 8, False)).hex()
            target_hex = pubkey
            if pubkey.startswith("npub"):
                from bech32 import bech32_decode, convertbits
                _, data_bytes = bech32_decode(pubkey)
                if data_bytes:
                    target_hex = bytes(convertbits(data_bytes, 5, 8, False)).hex()
            return resolved_hex.lower() == target_hex.lower()
    except Exception:
        return False


@app.get("/api/profile/nip05/verify")
async def api_nip05_verify(pubkey: str = Query(...)):
    """Verify NIP-05 identifier for a pubkey."""
    now = int(time.time())
    
    cached = _nip05_verify_cache.get(pubkey)
    if cached and (now - cached["checked_at"]) < NIP05_VERIFY_TTL:
        return cached
    
    # Extract nip05 from kind:0 profile
    profile_row = db_query_one(
        "SELECT content FROM events WHERE kind=0 AND pubkey=? ORDER BY created_at DESC LIMIT 1",
        (pubkey,))
    identifier = ""
    if profile_row and profile_row.get("content"):
        try:
            meta = json.loads(profile_row["content"])
            identifier = meta.get("nip05", "")
        except:
            pass
    
    if not identifier:
        result = {"pubkey": pubkey, "verified": False, "identifier": "", "reason": "no_nip05"}
    else:
        verified = await _verify_nip05_identifier(pubkey, identifier)
        result = {
            "pubkey": pubkey,
            "verified": verified,
            "identifier": identifier,
            "reason": "ok" if verified else "resolution_failed"
        }
    
    result["checked_at"] = now
    _nip05_verify_cache[pubkey] = result
    return result


@app.get("/api/profile/nip05/batch")
async def api_nip05_batch(pubkeys: str = Query(...)):
    """Batch verify NIP-05 for multiple pubkeys (comma-separated)."""
    pks = [pk.strip() for pk in pubkeys.split(",") if pk.strip()]
    results = {}
    for pk in pks[:20]:  # max 20 per batch
        results[pk] = await api_nip05_verify(pubkey=pk)


# ══════════════════════════════════════════
# PHASE 9 — MEDIA TAGS PARSER
# ══════════════════════════════════════════

def parse_media_tags(tags_json: str) -> list:
    """Parse imeta/url tags from a Nostr event. Returns list of {url, type, size}."""
    if not tags_json:
        return []
    try:
        tags = json.loads(tags_json) if isinstance(tags_json, str) else tags_json
    except:
        return []

    media = []

    for tag in tags:
        if not tag or not isinstance(tag, list):
            continue
        # NIP-94: imeta tags
        if tag[0] == "imeta" and len(tag) > 1:
            item = {}
            for part in tag[1:]:
                if len(part) >= 2:
                    key, val = part[0], part[1]
                    if key == "url":
                        item["url"] = val
                    elif key == "m":
                        ext = val.split("/")[-1].lower()
                        item["type"] = "video" if ext in ("mp4", "webm", "mov") else "image"
                    elif key == "dim":
                        item["dim"] = val
                    elif key == "size":
                        item["size"] = val
            if item.get("url"):
                item.setdefault("type", "image")
                media.append(item)
        # Plain url tag
        elif tag[0] == "url" and len(tag) > 1:
            url = tag[1]
            ext = url.split("?")[0].split(".")[-1].lower() if "." in url.split("?")[0] else ""
            media.append({
                "url": url,
                "type": "video" if ext in ("mp4", "webm", "mov") else "image"
            })
        # Image tag
        elif tag[0] == "image" and len(tag) > 1:
            media.append({"url": tag[1], "type": "image"})

    return media


# ══════════════════════════════════════════
# DEPLOYMENT GUIDE (Phase 5)
# ══════════════════════════════════════════

DEPLOY_GUIDE = """# SNIN Client — Deployment Guide

## Quick Start (Docker)

```bash
docker-compose up -d
```

Access at http://localhost:8095

## Manual Deployment

### Prerequisites
- Python 3.12+
- SQLite 3
- 50+ MB disk space for relay DB

### Install
```bash
pip install -r requirements.txt
```

### Configure
```bash
export RELAY_DB_PATH=/path/to/relay_v2.db  # optional, default: ./data/relay_v2.db
```

### Run
```bash
uvicorn app:app --host 0.0.0.0 --port 8095 --workers 1
```

## Key Management

### Export keys
```bash
curl http://localhost:8095/api/keys/export?pubkey=YOUR_PUBKEY > backup.json
```

### Import keys
```bash
curl -X POST http://localhost:8095/api/keys/import \\
  -H 'Content-Type: application/json' \\
  -d @backup.json
```

## Production

- Use nginx or Caddy as reverse proxy
- Enable HTTPS with Let's Encrypt
- Mount persistent volume for relay DB
- Health check: GET /api/stats
- Regular backups: curl /api/keys/export > backup-$(date +%Y%m%d).json

## Troubleshooting

| Problem | Solution |
|---------|----------|
| 502 Bad Gateway | Check `ps aux | grep app.py` — restart if dead |
| Empty feed | Check relay DB: `sqlite3 relay_v2.db "SELECT COUNT(*) FROM events"` |
| Missing deps | `pip install -r requirements.txt` |
| Port in use | Change port in uvicorn command |
"""



# ─── Admin Panel ───
@app.get("/admin")
async def admin_panel():
    return FileResponse("admin/index.html")

@app.get("/admin/")
async def admin_panel_slash():
    return FileResponse("admin/index.html")


# ─── Phase 5.1: Services Catalog ───
@app.get("/api/services")
def api_services():
    """Возвращает каталог всех сервисов SNIN."""
    try:
        with open("admin/services.json") as f:
            import json
            data = json.load(f)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e), "services": [], "total": 0}, status_code=500)


# ─── Phase 5.2: P2P Agent Mesh ───
@app.get("/api/mesh/status")
def api_mesh_status():
    """Возвращает статус P2P Agent Mesh и список агентов."""
    try:
        with open("admin/mesh.json") as f:
            import json
            data = json.load(f)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e), "agents": [], "peers_connected": 0}, status_code=500)


# ─── Phase 5.3: Chrono Economy ───
@app.get("/api/chrono/balance")
def api_chrono_balance():
    """Возвращает балансы всех держателей CHRONO."""
    try:
        with open("admin/chrono.json") as f:
            import json
            data = json.load(f)
        return JSONResponse({"balances": data["balances"], "total_holders": len(data["balances"])})
    except Exception as e:
        return JSONResponse({"error": str(e), "balances": []}, status_code=500)

@app.get("/api/chrono/nfts")
def api_chrono_nfts():
    """Возвращает все NFT-ачивки."""
    try:
        with open("admin/chrono.json") as f:
            import json
            data = json.load(f)
        return JSONResponse({"nfts": data["nfts"], "total": len(data["nfts"])})
    except Exception as e:
        return JSONResponse({"error": str(e), "nfts": []}, status_code=500)

@app.get("/api/chrono/status")
def api_chrono_status():
    """Общий статус экономики Chrono."""
    try:
        with open("admin/chrono.json") as f:
            import json
            data = json.load(f)
        return JSONResponse({
            "token": data["token"],
            "treasury": data["treasury"],
            "total_holders": len(data["balances"]),
            "total_nfts": len(data["nfts"])
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ─── Phase 5.4: NIP-80 Hardware/IoT ───
@app.get("/api/hardware/devices")
def api_hardware_devices():
    """Возвращает список устройств NIP-80 (kind:31012)."""
    try:
        with open("admin/hardware.json") as f:
            import json
            data = json.load(f)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e), "devices": []}, status_code=500)

@app.get("/api/hardware/device/{device_id}")
def api_hardware_device(device_id: str):
    """Возвращает детали конкретного устройства."""
    try:
        with open("admin/hardware.json") as f:
            import json
            data = json.load(f)
        for dev in data["devices"]:
            if dev["id"] == device_id:
                return JSONResponse(dev)
        return JSONResponse({"error": "Device not found"}, status_code=404)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

if __name__ == "__main__":
    # Start health monitor
    from health import start_health_monitor
    start_health_monitor(interval=60)
    
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8095, log_level="info")

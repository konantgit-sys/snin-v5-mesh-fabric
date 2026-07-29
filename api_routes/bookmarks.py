"""
SNIN Client — Bookmarks (kind:30001) + Highlights (kind:9802) API
"""

import json
from fastapi import APIRouter, Request
from shared import db_query_one, db_query, db_execute, resolve_name, resolve_picture

router = APIRouter(tags=["bookmarks_highlights"])


# ══════════════════════════════════════════
# BOOKMARKS (kind:30001) — V8.23
# ══════════════════════════════════════════

@router.get("/api/bookmarks")
async def api_bookmarks(pubkey: str = ""):
    """Get bookmarked event IDs from kind:30001."""
    if not pubkey:
        return {"bookmarks": [], "events": []}

    row = db_query_one(
        "SELECT tags_json FROM events WHERE kind=30001 AND pubkey=? ORDER BY created_at DESC LIMIT 1",
        (pubkey,))

    event_ids = []
    if row:
        try:
            tags = json.loads(row.get("tags_json", "[]"))
            for tag in tags:
                if tag[0] == "e" and len(tag) > 1:
                    event_ids.append(tag[1])
        except:
            pass

    events = []
    if event_ids:
        placeholders = ",".join("?" * len(event_ids))
        events = db_query(f"""
            SELECT e.id, e.pubkey, e.content, e.kind, e.created_at, e.tags_json
            FROM events e WHERE e.id IN ({placeholders})
            ORDER BY e.created_at DESC
        """, tuple(event_ids))

        for ev in events:
            ev["author_name"] = resolve_name(ev["pubkey"])
            ev["author_picture"] = resolve_picture(ev["pubkey"])
            ev["content_preview"] = (ev.get("content") or "")[:200]

    return {"bookmarks": event_ids, "events": events}


@router.post("/api/bookmarks/update")
async def api_bookmarks_update(request: Request):
    """Update bookmark list (kind:30001). Client sends signed event."""
    try:
        body = await request.json()
        event = body.get("event", {})

        if not event or event.get("kind") != 30001:
            return {"error": "Invalid event, must be kind:30001", "ok": False}

        tags_json = json.dumps(event.get("tags", []))
        db_execute("""
            INSERT OR REPLACE INTO events (id, pubkey, created_at, kind, tags_json, content, sig, received_at)
            VALUES (?, ?, ?, 30001, ?, ?, ?, ?)
        """, (
            event.get("id"),
            event.get("pubkey"),
            event.get("created_at"),
            tags_json,
            event.get("content", ""),
            event.get("sig"),
            int(__import__('time').time())
        ))

        # Broadcast to relay
        from shared import broadcast_event
        try:
            broadcast_event(event)
        except:
            pass

        return {"ok": True, "id": event.get("id")}

    except Exception as e:
        return {"error": str(e), "ok": False}


# ══════════════════════════════════════════
# HIGHLIGHTS (kind:9802, NIP-84) — V8.81
# ══════════════════════════════════════════

@router.get("/api/highlights")
async def api_highlights(pubkey: str = "", limit: int = 30):
    """Get highlights (kind:9802). Optional pubkey filter."""
    if pubkey:
        rows = db_query(
            "SELECT id, pubkey, content, created_at, tags_json FROM events WHERE kind=9802 AND pubkey=? ORDER BY created_at DESC LIMIT ?",
            (pubkey, limit))
    else:
        rows = db_query(
            "SELECT id, pubkey, content, created_at, tags_json FROM events WHERE kind=9802 ORDER BY created_at DESC LIMIT ?",
            (limit,))

    if not rows:
        from app import fetch_kind_events_from_relays
        import asyncio
        print("[API] No highlights in DB — fetching from Nostr relays...", flush=True)
        await fetch_kind_events_from_relays(9802, limit=50)
        rows = db_query(
            "SELECT id, pubkey, content, created_at, tags_json FROM events WHERE kind=9802 ORDER BY created_at DESC LIMIT ?",
            (limit,))

    result = []
    for r in rows:
        tags = json.loads(r.get("tags_json", "[]"))
        ref_event = ref_url = context = ""
        for tag in tags:
            if tag[0] == "e" and len(tag) > 1: ref_event = tag[1]
            elif tag[0] == "r" and len(tag) > 1: ref_url = tag[1]
            elif tag[0] == "context" and len(tag) > 1: context = tag[1]

        result.append({
            "id": r["id"], "author": r["pubkey"], "content": r.get("content","")[:500],
            "ref_event": ref_event, "ref_url": ref_url, "context": context,
            "author_name": resolve_name(r["pubkey"]), "author_pic": resolve_picture(r["pubkey"]),
            "created_at": r["created_at"]
        })

    return {"highlights": result}


@router.post("/api/highlights/update")
async def api_highlights_update(request: Request):
    """Store a kind:9802 highlight event."""
    body = await request.json()
    event_id = body.get("id")
    pubkey = body.get("pubkey")
    kind = body.get("kind")
    content = body.get("content", "")
    tags = body.get("tags", [])
    created_at = body.get("created_at")
    sig = body.get("sig", "")

    if not event_id or not pubkey or kind != 9802:
        return {"ok": False, "error": "invalid event"}

    import sqlite3
    from shared import DB_PATH
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT OR REPLACE INTO events (id, pubkey, kind, content, created_at, tags_json, sig) VALUES (?,?,?,?,?,?,?)",
        (event_id, pubkey, kind, content, created_at, json.dumps(tags), sig))
    conn.commit()
    conn.close()

    from shared import broadcast_event
    import asyncio
    asyncio.create_task(broadcast_event(event_id, pubkey, kind, content, created_at, tags, sig))

    return {"ok": True, "id": event_id}


@router.get("/api/bookmarks/feed")
async def api_bookmarks_feed(pubkey: str = "", limit: int = 30):
    """Get full bookmark feed with post data."""
    from fastapi.responses import JSONResponse
    
    if not pubkey:
        return JSONResponse({"error": "pubkey required"}, status_code=400)

    bookmarks = db_query("""
        SELECT e.tags_json, e.created_at FROM events e
        WHERE e.kind = 30001 AND e.pubkey = ? AND e.tags_json LIKE '%"e"%'
        ORDER BY e.created_at DESC LIMIT ?
    """, (pubkey, limit))

    event_ids = []
    for bm in bookmarks:
        try:
            tags = json.loads(bm["tags_json"])
            for tag in tags:
                if tag[0] == "e" and len(tag) > 1:
                    event_ids.append(tag[1])
        except:
            pass

    if not event_ids:
        return {"posts": [], "total": 0, "for_pubkey": pubkey}

    placeholders = ",".join("?" for _ in event_ids)
    posts = db_query(f"""
        SELECT e.id, e.pubkey, e.content, e.kind, e.created_at, e.tags_json
        FROM events e WHERE e.id IN ({placeholders})
        ORDER BY e.created_at DESC LIMIT ?
    """, (*event_ids, limit))

    for p in posts:
        p["author_name"] = resolve_name(p["pubkey"])
        p["author_picture"] = resolve_picture(p["pubkey"])
        try:
            from app import parse_media_tags
            p["media"] = parse_media_tags(p.get("tags_json", ""))
        except:
            p["media"] = []

    return {"posts": posts, "total": len(posts), "for_pubkey": pubkey}

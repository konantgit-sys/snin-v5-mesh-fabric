"""
SNIN Client — Communities API (kind:34550)
"""

import json
import sqlite3
from fastapi import APIRouter, Request, Query
from shared import db_query, db_query_one, resolve_name, resolve_picture


router = APIRouter(prefix="/api", tags=["communities"])


async def _fetch_kind_events_from_relays_imported(kind, limit=50):
    """Lazy import of fetch_kind_events_from_relays from app.py"""
    from app import fetch_kind_events_from_relays
    return await fetch_kind_events_from_relays(kind, limit)



def _get_db_path():
    from app import DB_PATH
    return DB_PATH


@router.get("/communities")
async def api_communities(pubkey: str = ""):
    """Get communities (kind:34550). If pubkey given, filter by creator."""
    if pubkey:
        rows = db_query(
            "SELECT id, pubkey, content, created_at, tags_json FROM events WHERE kind=34550 AND pubkey=? ORDER BY created_at DESC LIMIT 30",
            (pubkey,))
    else:
        rows = db_query(
            "SELECT id, pubkey, content, created_at, tags_json FROM events WHERE kind=34550 ORDER BY created_at DESC LIMIT 30")
    
    # If no communities in DB, fetch from relays
    if not rows:
        print("[API] No communities in DB — fetching from Nostr relays...", flush=True)
        await _fetch_kind_events_from_relays_imported(34550, limit=50)
        rows = db_query(
            "SELECT id, pubkey, content, created_at, tags_json FROM events WHERE kind=34550 ORDER BY created_at DESC LIMIT 30")
    
    comms = []
    for r in rows:
        tags = json.loads(r.get("tags_json", "[]"))
        d_tag = name = desc = image = ""
        moderators = []
        for tag in tags:
            if tag[0] == "d" and len(tag) > 1: d_tag = tag[1]
            elif tag[0] == "name" and len(tag) > 1: name = tag[1]
            elif tag[0] == "description" and len(tag) > 1: desc = tag[1]
            elif tag[0] == "image" and len(tag) > 1: image = tag[1]
            elif tag[0] == "p" and len(tag) > 1:
                moderators.append({"pubkey": tag[1], "name": resolve_name(tag[1]), "picture": resolve_picture(tag[1])})
        
        comms.append({
            "id": r["id"], "creator": r["pubkey"], "d_tag": d_tag,
            "name": name or d_tag or "Unnamed", "description": desc or r.get("content","")[:200],
            "image": image, "moderators": moderators, "created_at": r["created_at"]
        })
    
    return {"communities": comms}


@router.post("/communities/update")
async def api_communities_update(request: Request):
    """Store a kind:34550 event."""
    body = await request.json()
    event_id = body.get("id")
    pubkey = body.get("pubkey")
    kind = body.get("kind")
    content = body.get("content", "")
    tags = body.get("tags", [])
    created_at = body.get("created_at")
    sig = body.get("sig", "")
    
    if not event_id or not pubkey or kind != 34550:
        return {"ok": False, "error": "invalid event"}
    
    conn = sqlite3.connect(_get_db_path())
    conn.execute(
        "INSERT OR REPLACE INTO events (id, pubkey, kind, content, created_at, tags_json, sig) VALUES (?,?,?,?,?,?,?)",
        (event_id, pubkey, kind, content, created_at, json.dumps(tags), sig))
    conn.commit()
    conn.close()
    
    import asyncio
    try:
        from shared import broadcast_event
        asyncio.create_task(broadcast_event(event_id, pubkey, kind, content, created_at, tags, sig))
    except (ImportError, AttributeError):
        pass  # broadcast relay unavailable — skip relay sync
    
    return {"ok": True, "id": event_id}


@router.get("/communities/{comm_id}/feed")
async def api_communities_feed(comm_id: str, limit: int = 20):
    """Get posts from a community. Posts are kind:1/1111 with a-tag pointing to the community."""
    row = db_query_one(
        "SELECT id, pubkey, tags_json FROM events WHERE id=? AND kind=34550", (comm_id,))
    
    if not row:
        return {"results": []}
    
    tags = json.loads(row.get("tags_json", "[]"))
    d_tag = ""
    for tag in tags:
        if tag[0] == "d" and len(tag) > 1:
            d_tag = tag[1]
    
    community_pubkey = row["pubkey"]
    address = f"34550:{community_pubkey}:{d_tag}"
    
    # Find posts with a-tag referencing this community
    events = db_query("""
        SELECT e.id, e.pubkey, e.content, e.kind, e.created_at, e.tags_json
        FROM events e
        WHERE e.kind IN (1, 1111, 30023) AND e.tags_json LIKE ?
        ORDER BY e.created_at DESC LIMIT ?
    """, (f"%{address}%", limit))
    
    for ev in events:
        ev["author_name"] = resolve_name(ev["pubkey"])
        ev["author_picture"] = resolve_picture(ev["pubkey"])
    
    return {"results": events, "community_id": comm_id, "d_tag": d_tag}

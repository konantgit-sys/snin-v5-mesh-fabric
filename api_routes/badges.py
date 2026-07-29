"""
SNIN Client — Badges API (kind:30008/30009)
"""

import json
import sqlite3
from fastapi import APIRouter, Request
from shared import db_query, db_query_one


router = APIRouter(prefix="/api", tags=["badges"])


async def _fetch_kind_events_imported(kind, limit=50):
    from app import fetch_kind_events_from_relays
    return await fetch_kind_events_from_relays(kind, limit)



def _get_db_path():
    from app import DB_PATH
    return DB_PATH


@router.get("/badges")
async def api_badges():
    """Get badge definitions (kind:30008) and awards (kind:30009)."""
    badges = db_query("""
        SELECT id, pubkey, content, created_at, tags_json FROM events
        WHERE kind=30008 ORDER BY created_at DESC LIMIT 30
    """)
    
    if not badges:
        print("[API] No badges in DB — fetching from Nostr relays...", flush=True)
        await _fetch_kind_events_imported(30008, limit=50)
        badges = db_query("""
            SELECT id, pubkey, content, created_at, tags_json FROM events
            WHERE kind=30008 ORDER BY created_at DESC LIMIT 30
        """)
    
    result = []
    for r in badges:
        tags = json.loads(r.get("tags_json", "[]"))
        d_tag = name = desc = image = thumb = ""
        zk_challenge = None
        zk_context = ""
        for tag in tags:
            if tag[0] == "d" and len(tag) > 1: d_tag = tag[1]
            elif tag[0] == "name" and len(tag) > 1: name = tag[1]
            elif tag[0] == "description" and len(tag) > 1: desc = tag[1]
            elif tag[0] == "image" and len(tag) > 1: image = tag[1]
            elif tag[0] == "thumb" and len(tag) > 1: thumb = tag[1]
            elif tag[0] == "zk-challenge" and len(tag) > 1:
                zk_challenge = tag[1]
                zk_context = tag[2] if len(tag) > 2 else ""
        
        award_row = db_query_one(
            "SELECT COUNT(*) as cnt FROM events WHERE kind=30009 AND tags_json LIKE ?",
            (f"%\"{r['id']}\"%",))
        award_count = award_row["cnt"] if award_row else 0
        
        # Count ZK-verified awards
        zk_awards = db_query(
            "SELECT id, tags_json FROM events WHERE kind=30009 AND tags_json LIKE ? AND tags_json LIKE '%zk-proof%'",
            (f"%\"{r['id']}\"%",))
        
        result.append({
            "id": r["id"], "creator": r["pubkey"], "d_tag": d_tag,
            "name": name or d_tag or "Badge", "description": desc or r.get("content","")[:200],
            "image": image, "thumb": thumb, "awards_count": award_count,
            "zk_awards_count": len(zk_awards),
            "zk_challenge": zk_challenge,
            "zk_context": zk_context,
            "has_zk": bool(zk_challenge),
            "created_at": r["created_at"],
            "tags": tags
        })
    
    return {"badges": result}


@router.get("/badges/awards")
async def api_badges_awarded(pubkey: str = ""):
    """Get badges awarded to a pubkey."""
    if not pubkey:
        return {"awards": []}
    
    awards = db_query("""
        SELECT id, pubkey, content, created_at, tags_json FROM events
        WHERE kind=30009 AND tags_json LIKE ? ORDER BY created_at DESC LIMIT 30
    """, (f"%\"{pubkey}\"%",))
    
    result = []
    for a in awards:
        tags = json.loads(a.get("tags_json", "[]"))
        badge_refs = []
        for tag in tags:
            if (tag[0] == "a" or tag[0] == "e") and len(tag) > 1:
                badge_refs.append(tag[1])
        
        result.append({
            "id": a["id"], "issuer": a["pubkey"],
            "badge_refs": badge_refs, "content": a.get("content","")[:200],
            "created_at": a["created_at"], "tags_json": a.get("tags_json", "[]")
        })
    
    return {"awards": result}


@router.post("/badges/update")
async def api_badges_update(request: Request):
    """Store kind:30008 or kind:30009 event."""
    body = await request.json()
    event_id = body.get("id")
    pubkey = body.get("pubkey")
    kind = body.get("kind")
    content = body.get("content", "")
    tags = body.get("tags", [])
    created_at = body.get("created_at")
    sig = body.get("sig", "")
    
    if not event_id or not pubkey or kind not in (30008, 30009):
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

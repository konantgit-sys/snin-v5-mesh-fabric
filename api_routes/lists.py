"""
SNIN Client — Lists API (NIP-51)
"""

import json
import time
import re
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse
from shared import db_query, db_query_one, db_execute, resolve_name, resolve_picture, resolve_nip05

router = APIRouter(prefix="/api", tags=["lists"])


def slugify(text: str) -> str:
    """Slugify text for list d-tag."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text[:50]


@router.get("/lists")
async def api_lists(pubkey: str = Query("")):
    """Get all lists (kind:30000) for a pubkey, with item counts."""
    if not pubkey:
        return JSONResponse({"error": "pubkey required"}, status_code=400)

    lists = db_query("""
        SELECT e.id, e.pubkey, e.content, e.created_at, e.tags_json
        FROM events e
        WHERE e.kind = 30000 AND e.pubkey = ?
        ORDER BY e.created_at DESC
        LIMIT 30
    """, (pubkey,))

    result = []
    for lst in lists:
        try:
            meta = json.loads(lst.get("content", "{}"))
            tags = json.loads(lst.get("tags_json", "[]"))
            members = []
            seen_pubkeys = set()
            for tag in tags:
                if tag[0] == "p" and len(tag) > 1 and tag[1] not in seen_pubkeys:
                    seen_pubkeys.add(tag[1])
                    members.append({
                        "pubkey": tag[1],
                        "name": resolve_name(tag[1]),
                        "picture": resolve_picture(tag[1]),
                    })
            result.append({
                "id": lst["id"],
                "pubkey": lst["pubkey"],
                "name": meta.get("name", "Unnamed List"),
                "description": meta.get("description", ""),
                "created_at": lst["created_at"],
                "item_count": len(members),
                "members": members,
                "tags": tags,
            })
        except:
            result.append({
                "id": lst["id"],
                "pubkey": lst["pubkey"],
                "name": lst.get("content", "List")[:80],
                "created_at": lst["created_at"],
                "item_count": 0,
                "members": [],
            })

    return {"lists": result, "total": len(result), "for_pubkey": pubkey}


@router.get("/lists/feed")
async def api_list_feed(list_id: str = Query(""), pubkey: str = Query(""), limit: int = Query(20, le=50)):
    """Get feed from a specific list (events from listed pubkeys/events)."""
    if not list_id:
        return JSONResponse({"error": "list_id required"}, status_code=400)

    lst = db_query_one(
        "SELECT tags_json FROM events WHERE id=? AND kind=30000",
        (list_id,)
    )
    if not lst:
        return JSONResponse({"error": "List not found"}, status_code=404)

    tags = json.loads(lst.get("tags_json", "[]"))
    pubkeys_in_list = []
    event_ids_in_list = []

    for tag in tags:
        if len(tag) >= 2:
            if tag[0] == "p":
                pubkeys_in_list.append(tag[1])
            elif tag[0] == "e":
                event_ids_in_list.append(tag[1])

    posts = []
    if pubkeys_in_list:
        placeholders = ",".join("?" for _ in pubkeys_in_list)
        posts = db_query(f"""
            SELECT e.id, e.pubkey, e.content, e.kind, e.created_at, e.tags_json
            FROM events e
            WHERE e.kind IN (1, 39000, 1111) AND e.pubkey IN ({placeholders})
            ORDER BY e.created_at DESC LIMIT ?
        """, (*pubkeys_in_list, limit))

    if event_ids_in_list:
        placeholders = ",".join("?" for _ in event_ids_in_list)
        pinned = db_query(f"""
            SELECT e.id, e.pubkey, e.content, e.kind, e.created_at, e.tags_json
            FROM events e
            WHERE e.id IN ({placeholders})
            ORDER BY e.created_at DESC
        """, (*event_ids_in_list,))
        existing = {p["id"] for p in posts}
        for p in pinned:
            if p["id"] not in existing:
                posts.insert(0, p)

    # Import parse_media_tags from app (still in app.py)
    from app import parse_media_tags
    for p in posts:
        p["author_name"] = resolve_name(p["pubkey"])
        p["author_picture"] = resolve_picture(p["pubkey"])
        p["author_nip05"] = resolve_nip05(p["pubkey"])
        p["content_preview"] = (p.get("content") or "")[:200]
        try:
            p["media"] = parse_media_tags(p.get("tags_json", ""))
        except:
            p["media"] = []

    return {"posts": posts[:limit], "total": len(posts), "list_id": list_id}


@router.post("/lists")
async def api_create_list(request: Request):
    """Create a new list (kind:30000) with items (kind:30001)."""
    try:
        body = await request.json()
        name = body.get("name", "").strip()
        description = body.get("description", "").strip()
        items = body.get("items", [])
        pubkey = body.get("pubkey", "").strip()
    except:
        return JSONResponse({"success": False, "error": "Invalid JSON"}, status_code=400)

    if not name or not pubkey:
        return JSONResponse({"success": False, "error": "name and pubkey required"}, status_code=400)

    list_id = f"list:{pubkey[:16]}:{int(time.time())}"
    tags = [["d", slugify(name)]]
    for item in items:
        if item.get("pubkey"):
            tags.append(["p", item["pubkey"]])
        if item.get("event_id"):
            tags.append(["e", item["event_id"]])

    db_execute(
        """INSERT OR IGNORE INTO events (id, pubkey, created_at, kind, tags_json, content, sig, received_at)
           VALUES (?, ?, ?, 30000, ?, ?, ?, ?)""",
        (list_id, pubkey, int(time.time()),
         json.dumps(tags), json.dumps({"name": name, "description": description}),
         "local", int(time.time()))
    )

    for i, item in enumerate(items):
        item_id = f"list_item:{pubkey[:16]}:{list_id}:{i}"
        db_execute(
            """INSERT OR IGNORE INTO events (id, pubkey, created_at, kind, tags_json, content, sig, received_at)
               VALUES (?, ?, ?, 30001, ?, ?, ?, ?)""",
            (item_id, pubkey, int(time.time()),
             json.dumps([["a", f"30000:{pubkey}:items"] if item.get("pubkey") else ["e", item.get("event_id", "")]]),
             "", "local", int(time.time()))
        )

    return {"success": True, "list_id": list_id, "name": name, "items_count": len(items)}


@router.post("/lists/update")
async def api_lists_update(request: Request):
    """Store a kind:30000 event (external relay sync)."""
    import sqlite3
    body = await request.json()
    event_id = body.get("id")
    pubkey = body.get("pubkey")
    kind = body.get("kind")
    content = body.get("content", "")
    tags = body.get("tags", [])
    created_at = body.get("created_at")
    sig = body.get("sig", "")
    
    if not event_id or not pubkey or kind != 30000:
        return {"ok": False, "error": "invalid event"}
    
    from app import DB_PATH
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT OR REPLACE INTO events (id, pubkey, kind, content, created_at, tags_json, sig, received_at) VALUES (?,?,?,?,?,?,?,?)",
        (event_id, pubkey, kind, content, created_at, json.dumps(tags), sig, int(time.time())))
    conn.commit()
    conn.close()
    
    import asyncio
    try:
        from shared import broadcast_event
        asyncio.create_task(broadcast_event(event_id, pubkey, kind, content, created_at, tags, sig))
    except (ImportError, AttributeError):
        pass  # broadcast relay unavailable — skip relay sync
    
    return {"ok": True, "id": event_id}

"""
SNIN Client — DM (Direct Messages) API — NIP-04 + NIP-17/44
"""

import json
import time
import os
from fastapi import APIRouter, Request
from shared import db_query, db_query_one, db_execute, resolve_name, resolve_picture, pubkey_to_npub, broadcast_event

router = APIRouter(prefix="/api/dm", tags=["dm"])


@router.get("/list")
async def api_dm_list(pubkey: str = ""):
    """List all DM conversations for a pubkey (kind:4). Uses tags_json column."""
    if not pubkey:
        return {"conversations": []}
    
    events = db_query("""
        SELECT e.id, e.pubkey, e.content, e.created_at, e.tags_json
        FROM events e
        WHERE e.kind = 4 AND (
            e.pubkey = ? OR 
            e.tags_json LIKE ?
        )
        ORDER BY e.created_at DESC
    """, (pubkey, f'%"{pubkey}"%'))
    
    convs = {}
    for ev in events:
        peer = None
        try:
            tags = json.loads(ev.get("tags_json", "[]"))
            for tag in tags:
                if tag[0] == "p" and len(tag) > 1:
                    if tag[1] != pubkey:
                        peer = tag[1]
                        break
        except:
            pass
        
        if peer and peer != pubkey:
            ts = ev["created_at"]
            if peer not in convs or ts > convs[peer]["last_at"]:
                convs[peer] = {
                    "peer": peer,
                    "last_at": ts,
                    "last_content": (ev.get("content") or "")[:60],
                    "last_direction": "out" if ev["pubkey"] == pubkey else "in"
                }
    
    for peer in convs:
        cnt = db_query_one("""
            SELECT COUNT(*) as cnt FROM events
            WHERE kind = 4 AND (
                (pubkey = ? AND tags_json LIKE ?) OR
                (pubkey = ? AND tags_json LIKE ?)
            )
        """, (pubkey, f'%"{peer}"%', peer, f'%"{pubkey}"%'))
        convs[peer]["msg_count"] = cnt["cnt"] if cnt else 0
        convs[peer]["display_name"] = resolve_name(peer)
        convs[peer]["picture"] = resolve_picture(peer)
        convs[peer]["npub"] = pubkey_to_npub(peer)
    
    sorted_convs = sorted(convs.values(), key=lambda x: x["last_at"], reverse=True)
    return {"conversations": sorted_convs}


@router.get("/conversation")
async def api_dm_conversation(pubkey_a: str = "", pubkey_b: str = "", limit: int = 50, since: int = 0):
    """Get DMs between two pubkeys (kind:4). Returns encrypted content — client decrypts."""
    if not pubkey_a or not pubkey_b:
        return {"messages": []}
    
    since_clause = "AND e.created_at > ?" if since else ""
    params = [pubkey_a, pubkey_b, pubkey_b, pubkey_a]
    if since:
        params.append(since)
    
    msgs = db_query(f"""
        SELECT e.id, e.pubkey, e.content, e.created_at, e.kind, e.tags_json
        FROM events e
        WHERE e.kind = 4 AND (
            (e.pubkey = ? AND e.tags_json LIKE ?) OR
            (e.pubkey = ? AND e.tags_json LIKE ?)
        )
        {since_clause}
        ORDER BY e.created_at ASC LIMIT ?
    """, tuple(params + [limit]))
    
    for m in msgs:
        m["direction"] = "out" if m["pubkey"] == pubkey_a else "in"
        m["author_name"] = resolve_name(m["pubkey"])
        m["author_picture"] = resolve_picture(m["pubkey"])
        try:
            tags = json.loads(m.get("tags_json", "[]"))
            for tag in tags:
                if tag[0] == "p" and len(tag) > 1 and tag[1] != pubkey_a:
                    m["recipient"] = tag[1]
                    break
        except:
            m["recipient"] = pubkey_b
    
    return {"messages": list(msgs)}


@router.get("/gallery")
async def api_dm_gallery(pubkey_a: str = "", pubkey_b: str = "", limit: int = 50):
    """Get all media (images) from a DM conversation."""
    if not pubkey_a or not pubkey_b:
        return {"images": []}

    msgs = db_query("""
        SELECT e.id, e.pubkey, e.content, e.created_at, e.tags_json
        FROM events e
        WHERE e.kind = 4 AND (
            (e.pubkey = ? AND e.tags_json LIKE ?) OR
            (e.pubkey = ? AND e.tags_json LIKE ?)
        )
        ORDER BY e.created_at DESC LIMIT ?
    """, (pubkey_a, f'%"{pubkey_b}"%', pubkey_b, f'%"{pubkey_a}"%', limit))

    images = []
    for m in msgs:
        try:
            tags = json.loads(m.get("tags_json", "[]"))
            for tag in tags:
                if tag[0] == "media" and len(tag) > 1:
                    images.append({
                        "url": tag[1],
                        "type": tag[2] if len(tag) > 2 else "image",
                        "from_pubkey": m["pubkey"],
                        "created_at": m["created_at"],
                        "event_id": m["id"],
                    })
        except:
            pass

        content = m.get("content", "")
        if not images and content:
            for url_type in [".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mov"]:
                if url_type in content.lower():
                    images.append({
                        "url": content[:200],
                        "type": "video" if url_type in [".mp4", ".mov"] else "image",
                        "from_pubkey": m["pubkey"],
                        "created_at": m["created_at"],
                        "event_id": m["id"],
                    })
                    break

    return {"images": images[:limit], "total": len(images)}


@router.post("/upload")
async def api_dm_upload(request: Request):
    """Upload a file for DM attachment. Stores in sites/snin-client/uploads/dms/."""
    try:
        body = await request.json()
        file_data = body.get("data", "")
        file_name = body.get("name", "file")
        mime_type = body.get("type", "image/png")

        if not file_data:
            return {"ok": False, "error": "No file data"}

        import base64
        upload_dir = "/home/agent/data/sites/snin-client/uploads/dms"
        os.makedirs(upload_dir, exist_ok=True)

        file_path = f"{upload_dir}/{int(time.time())}_{file_name}"
        raw = base64.b64decode(file_data.split(",")[-1] if "," in file_data else file_data)
        with open(file_path, "wb") as f:
            f.write(raw)

        url = f"https://snin-client.v2.site/uploads/dms/{os.path.basename(file_path)}"
        return {"ok": True, "url": url, "name": file_name, "type": mime_type}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/send")
async def api_dm_send(request: Request):
    """Store and broadcast a kind:4 DM event (pre-encrypted and signed by client)."""
    try:
        body = await request.json()
        event = body.get("event", {})
        
        if not event or event.get("kind") != 4:
            return {"error": "Invalid event", "ok": False}
        
        db_execute("""
            INSERT OR REPLACE INTO events (id, pubkey, created_at, kind, tags_json, content, sig, received_at)
            VALUES (?, ?, ?, 4, ?, ?, ?, ?)
        """, (
            event.get("id"),
            event.get("pubkey"),
            event.get("created_at"),
            json.dumps(event.get("tags", [])),
            event.get("content"),
            event.get("sig"),
            int(time.time())
        ))
        
        try:
            broadcast_event(event)
        except:
            pass
        
        return {"ok": True, "id": event.get("id")}
    except Exception as e:
        return {"error": str(e), "ok": False}

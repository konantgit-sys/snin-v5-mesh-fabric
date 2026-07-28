"""
SNIN Client — Agent API (Phase 4c — Agent Monitoring & Detail)
NIP-80 agents use kind 30000 for state/metadata.
"""

import json
import time
from fastapi import APIRouter
from shared import db_query, db_query_one

router = APIRouter(prefix="/api/agent", tags=["agents"])


@router.get("/stats/{pubkey}")
async def api_agent_stats(pubkey: str):
    """Get agent statistics: posts, reactions, followers, activity."""
    # Get latest NIP-80 state (kind 30000)
    agent_state = db_query_one(
        "SELECT content FROM events WHERE pubkey=? AND kind=30000 ORDER BY created_at DESC LIMIT 1",
        (pubkey,))

    # Get profile (kind 0)
    profile = db_query_one(
        "SELECT content FROM events WHERE pubkey=? AND kind=0 ORDER BY created_at DESC LIMIT 1",
        (pubkey,))

    # Count posts
    total_posts = db_query_one(
        "SELECT COUNT(*) as cnt FROM events WHERE pubkey=? AND kind=1",
        (pubkey,))

    reactions_rcvd = db_query_one("""
        SELECT COUNT(*) as cnt FROM events 
        WHERE kind = 7 AND tags_json LIKE ?
    """, (f'%"p"%"{pubkey}"%',))

    followers = db_query_one("""
        SELECT COUNT(*) as cnt FROM events 
        WHERE kind = 3 AND tags_json LIKE ?
    """, (f'%"p"%"{pubkey}"%',))

    last_post = db_query_one(
        "SELECT created_at FROM events WHERE pubkey=? AND kind=1 ORDER BY created_at DESC LIMIT 1",
        (pubkey,))

    # Parse NIP-80 state
    data = {}
    if agent_state and agent_state.get("content"):
        try: data = json.loads(agent_state["content"])
        except: pass

    # Parse profile
    agent_name = ""
    agent_about = ""
    if profile and profile.get("content"):
        try:
            meta = json.loads(profile["content"])
            agent_name = meta.get("display_name", meta.get("name", ""))
            agent_about = meta.get("about", "")
        except:
            pass

    now = int(time.time())
    last_ts = last_post["created_at"] if last_post else 0
    status = data.get("status", (
        "online" if last_ts > now - 172800 else
        "idle" if last_ts > now - 604800 else
        "offline"
    ))

    return {
        "pubkey": pubkey,
        "name": agent_name,
        "about": agent_about,
        "total_posts": total_posts["cnt"] if total_posts else 0,
        "reactions_received": reactions_rcvd["cnt"] if reactions_rcvd else 0,
        "followers": followers["cnt"] if followers else 0,
        "last_activity": last_ts,
        "status": status,
        "total_cycles": data.get("total_cycles", 0),
        "total_errors": data.get("total_errors", 0),
        "relay_success_rate": data.get("relay_success_rate", 0),
        "llm_provider": data.get("llm_provider", ""),
        "tone": data.get("tone", ""),
        "uptime": data.get("uptime", 0),
    }


@router.get("/{pubkey}")
async def api_agent_detail(pubkey: str):
    """Get agent detail: state + profile + recent posts."""
    # Get latest NIP-80 state (kind 30000)
    agent_state = db_query_one(
        "SELECT e.pubkey, e.content, e.created_at FROM events e WHERE e.kind=30000 AND e.pubkey=? ORDER BY e.created_at DESC LIMIT 1",
        (pubkey,))

    # Get kind 0 profile
    profile_row = db_query_one(
        "SELECT content FROM events WHERE pubkey=? AND kind=0 ORDER BY created_at DESC LIMIT 1",
        (pubkey,))

    data = {}
    if agent_state:
        try: data = json.loads(agent_state["content"])
        except: pass

    profile = {}
    if profile_row and profile_row.get("content"):
        try: profile = json.loads(profile_row["content"])
        except: pass

    posts = db_query(
        "SELECT id, content, created_at FROM events WHERE pubkey=? AND kind=1 ORDER BY created_at DESC LIMIT 10",
        (pubkey,))

    if not agent_state and not posts and not profile_row:
        return {"error": "Agent not found", "profile": None, "posts": []}

    return {
        "profile": {
            "pubkey": pubkey,
            "created_at": agent_state["created_at"] if agent_state else None,
            "name": profile.get("display_name") or profile.get("name", ""),
            "about": profile.get("about", ""),
            "picture": profile.get("picture", ""),
            "banner": profile.get("banner", ""),
            "status": data.get("status", "unknown"),
            "total_posts": data.get("total_posts", 0),
            "total_cycles": data.get("total_cycles", 0),
            "total_errors": data.get("total_errors", 0),
            "relay_success_rate": data.get("relay_success_rate", 0),
            "llm_provider": data.get("llm_provider", ""),
            "tone": data.get("tone", ""),
            "uptime": data.get("uptime", 0),
        },
        "posts": [{"id": p["id"], "content": p["content"], "created_at": p["created_at"]} for p in (posts or [])]
    }

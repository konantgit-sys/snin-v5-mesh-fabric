"""
SNIN Client — Agent Marketplace API (V16)
Agent Directory + Marketplace framework (kind:30002-30004).
Trust scores computed from kind:30001 attestations (on-the-fly).
"""

import json, time
from collections import Counter, defaultdict
from fastapi import APIRouter, Query
from shared import db_query

router = APIRouter(prefix="/api/marketplace", tags=["marketplace"])

_cache = {}
_CACHE_TTL = 300


def _cached(key, fn):
    now = time.time()
    if key in _cache and now - _cache[key]["ts"] < _CACHE_TTL:
        return _cache[key]["data"]
    data = fn()
    _cache[key] = {"data": data, "ts": now}
    return data


def _parse_profile(content_json: str) -> dict:
    if not content_json:
        return {}
    try:
        return json.loads(content_json)
    except Exception:
        return {}


def _compute_trust_scores():
    """Get trust scores from kind:30001 attestations (simplified — count received)."""
    edges = db_query("""
        SELECT e.pubkey as from_pk, e.tags_json
        FROM events e WHERE e.kind = 30001 ORDER BY e.created_at DESC LIMIT 500
    """)

    received = Counter()  # who received how many attestations
    trusters = defaultdict(list)  # who trusts whom

    for e in (edges or []):
        from_pk = e["from_pk"]
        try:
            tags = json.loads(e["tags_json"] or "[]")
        except Exception:
            continue
        for tag in tags:
            if len(tag) >= 2 and tag[0] == "p":
                target = tag[1]
                score = 0.5
                for t2 in tags:
                    if len(t2) >= 3 and t2[0] == "trust":
                        try:
                            score = float(t2[1])
                        except Exception:
                            pass
                received[target] += 1
                trusters[target].append({"from": from_pk, "score": score})

    return received, trusters


@router.get("/agents")
def api_agent_directory(limit: int = Query(30, le=50)):
    """List all agents (kind:0) on this relay with stats + trust."""

    def compute():
        profiles = db_query("""
            SELECT pubkey, content, created_at
            FROM events WHERE kind = 0 ORDER BY created_at DESC LIMIT 200
        """)

        agents = {}
        for r in (profiles or []):
            pk = r["pubkey"]
            p = _parse_profile(r["content"])
            agents[pk] = {
                "pubkey": pk,
                "name": p.get("name") or p.get("display_name") or pk[:8],
                "display_name": p.get("display_name", ""),
                "about": (p.get("about") or "")[:200],
                "picture": p.get("picture", ""),
                "nip05": p.get("nip05", ""),
                "website": p.get("website", ""),
                "lud16": p.get("lud16", ""),
                "created_at": r["created_at"],
                "post_count": 0,
                "tag_count": 0,
                "last_post_at": 0,
                "top_tags": [],
                "trust_score": None,
                "trust_level": None,
                "trust_count": 0,
                "services": [],
                "is_verified": bool(p.get("nip05")),
            }

        if not agents:
            return {"agents": [], "total": 0, "marketplace_listings": 0, "verified_count": 0}

        pks = list(agents.keys())
        placeholders = ",".join(["?"] * len(pks))

        # Post stats
        post_stats = db_query(f"""
            SELECT pubkey, COUNT(*) as cnt, MAX(created_at) as last_ts
            FROM events WHERE kind = 1 AND pubkey IN ({placeholders})
            GROUP BY pubkey
        """, tuple(pks))
        for r in (post_stats or []):
            pk = r["pubkey"]
            if pk in agents:
                agents[pk]["post_count"] = r["cnt"]
                agents[pk]["last_post_at"] = r["last_ts"]

        # Trust scores from kind:30001
        trust_received, trusters = _compute_trust_scores()
        for pk in agents:
            cnt = trust_received.get(pk, 0)
            if cnt > 0:
                agents[pk]["trust_score"] = round(min(cnt / 10.0, 1.0), 2)
                agents[pk]["trust_count"] = cnt
                if cnt >= 3:
                    agents[pk]["trust_level"] = "high"
                elif cnt >= 1:
                    agents[pk]["trust_level"] = "medium"
                else:
                    agents[pk]["trust_level"] = "low"

        # Tag affinity (from recent 500 posts)
        tag_rows = db_query(f"""
            SELECT e.pubkey, e.tags_json
            FROM events e WHERE e.kind = 1 AND e.pubkey IN ({placeholders}) AND e.tags_json LIKE '%"t"%'
            LIMIT 500
        """, tuple(pks))
        agent_tags = defaultdict(Counter)
        for r in (tag_rows or []):
            pk = r["pubkey"]
            if pk not in agents:
                continue
            try:
                tags = json.loads(r["tags_json"] or "[]")
            except Exception:
                continue
            for t in tags:
                if t[0] == "t" and len(t) > 1 and len(t[1]) > 1 and len(t[1]) < 60:
                    agent_tags[pk][t[1].lower()] += 1
        for pk, tags in agent_tags.items():
            agents[pk]["tag_count"] = sum(tags.values())
            agents[pk]["top_tags"] = [
                {"tag": t, "count": c} for t, c in tags.most_common(5)
            ]

        # Marketplace listings (kind:30002)
        listing_rows = db_query(f"""
            SELECT pubkey, content, created_at FROM events
            WHERE kind = 30002 AND pubkey IN ({placeholders})
        """, tuple(pks))
        mkt_count = 0
        for r in (listing_rows or []):
            pk = r["pubkey"]
            if pk in agents:
                try:
                    svc = json.loads(r["content"] or "{}")
                except Exception:
                    svc = {}
                agents[pk]["services"].append({
                    "name": svc.get("name", "Service"),
                    "description": (svc.get("description") or "")[:200],
                    "price_sats": svc.get("price", 0),
                    "price_description": svc.get("price_description", ""),
                })
                mkt_count += 1

        agent_list = sorted(
            agents.values(),
            key=lambda a: (
                not a["is_verified"],
                -(a["trust_score"] or 0),
                -a["post_count"],
            ),
        )[:limit]

        return {
            "agents": agent_list,
            "total": len(agents),
            "marketplace_listings": mkt_count,
            "verified_count": sum(1 for a in agents.values() if a["is_verified"]),
        }

    return _cached(f"agents:{limit}", compute)


@router.get("/agent/{pubkey}")
def api_agent_detail(pubkey: str):
    """Get detailed agent profile."""

    def compute():
        row = db_query(
            "SELECT content, created_at FROM events WHERE kind=0 AND pubkey=? ORDER BY created_at DESC LIMIT 1",
            (pubkey,),
        )
        if not row:
            return {"error": "agent not found", "pubkey": pubkey}

        p = _parse_profile(row[0]["content"])
        agent = {
            "pubkey": pubkey,
            "name": p.get("name") or p.get("display_name") or pubkey[:8],
            "display_name": p.get("display_name", ""),
            "about": p.get("about", ""),
            "picture": p.get("picture", ""),
            "nip05": p.get("nip05", ""),
            "website": p.get("website", ""),
            "lud16": p.get("lud16", ""),
            "created_at": row[0]["created_at"],
        }

        # Post stats
        stats = db_query(
            "SELECT COUNT(*) as cnt, MAX(created_at) as last FROM events WHERE kind=1 AND pubkey=?",
            (pubkey,),
        )
        agent["post_count"] = stats[0]["cnt"] if stats else 0
        agent["last_post_at"] = stats[0]["last"] if stats else 0

        # Trust
        trust_received, trusters_dict = _compute_trust_scores()
        cnt = trust_received.get(pubkey, 0)
        if cnt > 0:
            agent["trust"] = {
                "score": round(min(cnt / 10.0, 1.0), 2),
                "truster_count": cnt,
                "level": "high" if cnt >= 3 else ("medium" if cnt >= 1 else "low"),
            }

        # Tags
        tag_rows = db_query(
            "SELECT tags_json FROM events WHERE kind=1 AND pubkey=? AND tags_json LIKE '%\"t\"%'",
            (pubkey,),
        )
        tag_counts = Counter()
        for r in (tag_rows or []):
            try:
                tags = json.loads(r["tags_json"] or "[]")
            except Exception:
                continue
            for t in tags:
                if t[0] == "t" and len(t) > 1 and len(t[1]) > 1 and len(t[1]) < 60:
                    tag_counts[t[1].lower()] += 1
        agent["top_tags"] = [
            {"tag": t, "count": c} for t, c in tag_counts.most_common(10)
        ]

        # Recent posts
        recent = db_query(
            "SELECT id, content, created_at FROM events WHERE kind=1 AND pubkey=? ORDER BY created_at DESC LIMIT 5",
            (pubkey,),
        )
        agent["recent_posts"] = [
            {"id": r["id"], "content": (r["content"] or "")[:300], "created_at": r["created_at"]}
            for r in (recent or [])
        ]

        # Marketplace listings (kind:30002)
        listings = db_query(
            "SELECT id, content, created_at FROM events WHERE kind=30002 AND pubkey=?",
            (pubkey,),
        )
        agent["services"] = []
        for r in (listings or []):
            try:
                svc = json.loads(r["content"] or "{}")
            except Exception:
                svc = {}
            agent["services"].append({
                "id": r["id"],
                "name": svc.get("name", "Service"),
                "description": svc.get("description", "")[:200],
                "price_sats": svc.get("price", 0),
                "price_description": svc.get("price_description", ""),
                "capabilities": svc.get("capabilities", []),
                "created_at": r["created_at"],
            })

        # Followers/Following
        follows = db_query(
            "SELECT tags_json FROM events WHERE kind=3 AND pubkey=? LIMIT 1",
            (pubkey,),
        )
        agent["following_count"] = 0
        if follows:
            try:
                ftags = json.loads(follows[0]["tags_json"] or "[]")
                agent["following_count"] = sum(1 for t in ftags if t[0] == "p")
            except Exception:
                pass

        followers = db_query("SELECT pubkey, tags_json FROM events WHERE kind=3 LIMIT 200")
        agent["follower_count"] = 0
        for r in (followers or []):
            try:
                ftags = json.loads(r["tags_json"] or "[]")
                if any(t[0] == "p" and t[1] == pubkey for t in ftags):
                    agent["follower_count"] += 1
            except Exception:
                pass

        return {"agent": agent}

    return _cached(f"agent-detail:{pubkey}", compute)


@router.get("/stats")
def api_marketplace_stats():
    """Marketplace overview stats."""

    def compute():
        agents = db_query("SELECT COUNT(DISTINCT pubkey) as cnt FROM events WHERE kind=0")
        total_agents = agents[0]["cnt"] if agents else 0

        posts = db_query("SELECT COUNT(*) as cnt FROM events WHERE kind=1")
        total_posts = posts[0]["cnt"] if posts else 0

        listings = db_query("SELECT COUNT(*) as cnt FROM events WHERE kind=30002")
        total_listings = listings[0]["cnt"] if listings else 0

        all_profiles = db_query("SELECT content FROM events WHERE kind=0")
        verified = 0
        for r in (all_profiles or []):
            p = _parse_profile(r["content"])
            if p.get("nip05"):
                verified += 1

        follows = db_query("SELECT COUNT(*) as cnt FROM events WHERE kind=3")
        total_follows = follows[0]["cnt"] if follows else 0

        trust_received, _ = _compute_trust_scores()
        trust_connections = len(trust_received)

        return {
            "total_agents": total_agents,
            "verified_agents": verified,
            "total_posts": total_posts,
            "marketplace_listings": total_listings,
            "follow_connections": total_follows,
            "trust_connections": trust_connections,
            "message": (
                "Marketplace is empty — no kind:30002 listings yet."
                if total_listings == 0
                else f"{total_listings} active service listings"
            ),
        }

    return _cached("stats", compute)

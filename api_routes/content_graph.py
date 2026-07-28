"""
SNIN Client — Content Graph API (V15)
Topic clustering, trends, author-topic affinity.
Uses 5-min in-memory cache to avoid expensive DB + JSON parsing.
"""

import json, time
from collections import Counter, defaultdict
from fastapi import APIRouter, Query
from shared import db_query

router = APIRouter(prefix="/api/content", tags=["content"])

# ─── Simple 5-min cache ───
_cache = {}
_CACHE_TTL = 300  # 5 minutes


def _cached(key, fn):
    now = time.time()
    if key in _cache and now - _cache[key]["ts"] < _CACHE_TTL:
        return _cache[key]["data"]
    data = fn()
    _cache[key] = {"data": data, "ts": now}
    return data


@router.get("/topics")
def api_content_topics(
    limit: int = Query(50, le=100),
    min_count: int = Query(5, le=100)
):
    """Get topic clusters: hashtags grouped by co-occurrence."""

    cache_key = f"topics:{limit}:{min_count}"

    def compute():
        rows = db_query("""
            SELECT tags_json FROM events 
            WHERE kind = 1 AND tags_json LIKE '%"t"%'
            LIMIT 300
        """)

        tag_counts = Counter()
        co_occur = Counter()

        for r in (rows or []):
            try:
                tags = json.loads(r["tags_json"] or "[]")
            except Exception:
                continue

            t_tags = sorted(set(
                t[1].lower() for t in tags
                if t[0] == 't' and len(t) > 1 and len(t[1]) > 1 and len(t[1]) < 60
            ))
            # Cap at 12 tags per post to prevent O(n²) explosion
            t_tags = t_tags[:12]
            if not t_tags:
                continue

            for tag in t_tags:
                tag_counts[tag] += 1

            if len(t_tags) < 2:
                continue

            for i in range(len(t_tags)):
                for j in range(i + 1, len(t_tags)):
                    co_occur[(t_tags[i], t_tags[j])] += 1

        active_tags = {t for t, c in tag_counts.items() if c >= min_count}

        nodes = []
        for tag in sorted(active_tags, key=lambda t: -tag_counts[t])[:limit]:
            nodes.append({
                "id": tag, "label": f"#{tag}",
                "count": tag_counts[tag], "rank": tag_counts[tag]
            })

        edges = []
        for (a, b), cnt in co_occur.items():
            if a in active_tags and b in active_tags and cnt >= 3:
                edges.append({"source": a, "target": b, "weight": cnt, "type": "co-occurrence"})

        node_ids = {n["id"] for n in nodes}
        degrees = defaultdict(int)
        adj = defaultdict(set)
        for e in edges:
            degrees[e["source"]] += 1
            degrees[e["target"]] += 1
            adj[e["source"]].add(e["target"])
            adj[e["target"]].add(e["source"])

        visited = set()
        components = []
        for nid in node_ids:
            if nid not in visited:
                comp, queue = [], [nid]
                visited.add(nid)
                while queue:
                    cur = queue.pop(0)
                    comp.append(cur)
                    for nb in adj.get(cur, set()):
                        if nb not in visited:
                            visited.add(nb)
                            queue.append(nb)
                components.append(comp)

        for n in nodes:
            n["degree"] = degrees.get(n["id"], 0)

        return {
            "nodes": nodes, "edges": edges,
            "metrics": {
                "node_count": len(nodes), "edge_count": len(edges),
                "components": len(components),
                "component_sizes": sorted([len(c) for c in components], reverse=True)[:10],
                "total_tags": len(tag_counts), "total_pairs": len(co_occur)
            }
        }

    return _cached(cache_key, compute)


@router.get("/trends")
def api_content_trends(limit: int = Query(20, le=50)):
    """Get trending topics: velocity + recent activity."""

    cache_key = f"trends:{limit}"

    def compute():
        rows = db_query("""
            SELECT tags_json, created_at
            FROM events WHERE kind=1 AND tags_json LIKE '%"t"%'
            ORDER BY created_at DESC
            LIMIT 500
        """)

        now_max = 0
        tag_windows = defaultdict(lambda: {"total": 0, "recent": 0})

        for r in (rows or []):
            try:
                tags = json.loads(r["tags_json"] or "[]")
            except Exception:
                continue
            ts = r["created_at"] or 0
            if ts > now_max:
                now_max = ts

            for t in tags:
                if t[0] == 't' and len(t) > 1 and len(t[1]) > 1 and len(t[1]) < 60:
                    tag = t[1].lower()
                    tag_windows[tag]["total"] += 1
                    if ts > now_max - 86400:
                        tag_windows[tag]["recent"] += 1

        trends = []
        for tag, counts in tag_windows.items():
            total = counts["total"]
            recent = counts["recent"]
            if total >= 2:
                velocity = recent / max(total, 1)
                trends.append({
                    "tag": tag, "total": total, "recent_24h": recent,
                    "velocity": round(velocity, 3)
                })

        trends.sort(key=lambda x: -x["velocity"] * x["total"])
        top = trends[:limit]

        rising = [t for t in top if t["velocity"] > 0.3]
        stable = [t for t in top if 0.1 <= t["velocity"] <= 0.3]
        declining = [t for t in top if t["velocity"] < 0.1]

        return {
            "trending": top,
            "categories": {"rising": rising, "stable": stable, "declining": declining},
            "summary": f"{len(rising)} rising · {len(stable)} stable · {len(declining)} slow"
        }

    return _cached(cache_key, compute)


@router.get("/author-topics")
def api_author_topics(limit: int = Query(20, le=50)):
    """Get author-topic affinity: who writes about what."""
    cache_key = f"author-topics:{limit}"

    def compute():
        rows = db_query("""
            SELECT e.pubkey, e.tags_json, p.content as profile_json
            FROM events e
            LEFT JOIN events p ON p.pubkey = e.pubkey AND p.kind = 0
            WHERE e.kind = 1 AND e.tags_json LIKE '%"t"%'
            LIMIT 400
        """)

        author_tags = defaultdict(Counter)
        author_names = {}

        for r in (rows or []):
            pubkey = r["pubkey"]
            try:
                tags = json.loads(r["tags_json"] or "[]")
            except Exception:
                continue
            try:
                profile = json.loads(r["profile_json"] or "{}")
            except Exception:
                profile = {}
            name = profile.get("name") or profile.get("display_name") or pubkey[:8]
            author_names[pubkey] = name

            for t in tags:
                if t[0] == 't' and len(t) > 1 and len(t[1]) > 1 and len(t[1]) < 60:
                    author_tags[pubkey][t[1].lower()] += 1

        authors = []
        for pubkey, tags in sorted(author_tags.items(), key=lambda x: -sum(x[1].values()))[:limit]:
            top_topics = [{"tag": t, "count": c} for t, c in tags.most_common(5)]
            authors.append({
                "pubkey": pubkey, "name": author_names.get(pubkey, pubkey[:8]),
                "total_posts": sum(tags.values()), "unique_topics": len(tags),
                "top_topics": top_topics
            })

        return {"authors": authors, "total_authors": len(author_tags)}

    return _cached(cache_key, compute)


@router.get("/topic/{tag}")
def api_topic_detail(tag: str):
    """Get details for a specific topic: recent posts, top authors, related tags."""
    cache_key = f"topic-detail:{tag.lower()}"

    def compute():
        rows = db_query("""
            SELECT e.id, e.pubkey, e.content, e.tags_json, e.created_at, p.content as profile_json
            FROM events e
            LEFT JOIN events p ON p.pubkey = e.pubkey AND p.kind = 0
            WHERE e.kind = 1 AND e.tags_json LIKE '%"t"%'
            ORDER BY e.created_at DESC
            LIMIT 500
        """)

        tag_lower = tag.lower()
        related_tags = Counter()
        author_counts = Counter()
        recent_posts = []

        for r in (rows or []):
            try:
                tags = json.loads(r["tags_json"] or "[]")
            except Exception:
                continue
            post_tags = [t[1].lower() for t in tags
                         if t[0] == 't' and len(t) > 1 and len(t[1]) < 60]

            if tag_lower in post_tags:
                try:
                    profile = json.loads(r["profile_json"] or "{}")
                except Exception:
                    profile = {}
                name = profile.get("name") or r["pubkey"][:8]
                author_counts[name] += 1

                for pt in post_tags:
                    if pt != tag_lower:
                        related_tags[pt] += 1

                if len(recent_posts) < 5:
                    recent_posts.append({
                        "id": r["id"], "author": name,
                        "content": (r["content"] or "")[:200],
                        "tags": post_tags[:8], "created_at": r["created_at"]
                    })

        return {
            "tag": tag,
            "total_posts": sum(author_counts.values()),
            "top_authors": [{"name": n, "posts": c} for n, c in author_counts.most_common(8)],
            "related_topics": [{"tag": t, "count": c} for t, c in related_tags.most_common(15)],
            "recent_posts": recent_posts
        }

    return _cached(cache_key, compute)

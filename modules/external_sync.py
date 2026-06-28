#!/usr/bin/env python3
"""External Sync Daemon — pulls reactions/comments from external Nostr relays.

Phase 3: Relay mesh synchronization.
Queries external relays for kind:7 (reactions) and kind:1111 (comments) 
referencing Cryter's posts. Stores in local relay_v2.db.

Run: python3 external_sync.py [--interval 300] [--once]
"""

import asyncio
import json
import os
import ssl
import sys
import time
import sqlite3
from datetime import datetime, timezone
from collections import defaultdict

import websockets

# ─── Config ──────────────────────────────────────────────────────────────
CRYTER_PK = "8ae7965af1b61347bb9900b91cfa9487e4da2400bdb063521ad0850706ff5f96"
RELAY_DB = "/home/agent/data/sites/relay/relay_v2.db"
SYNC_LOG = "/tmp/external_sync.log"

EXTERNAL_RELAYS = [
    "wss://relay.damus.io",
    "wss://relay.primal.net",
    "wss://nos.lol",
    "wss://relay.nostr.band",
    "wss://purplepag.es",
]

# How many recent Cryter events to check
MAX_EVENT_IDS = 50
# How long to wait between sync cycles (seconds)
DEFAULT_INTERVAL = 300

# ─── SSL context ─────────────────────────────────────────────────────────
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

# ─── Stats ───────────────────────────────────────────────────────────────
stats = defaultdict(int)


def log(msg: str):
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(SYNC_LOG, 'a') as f:
        f.write(line + '\n')


def get_recent_event_ids(db_path: str, limit: int = 50) -> list[str]:
    """Get recent Cryter event IDs from local relay."""
    db = sqlite3.connect(db_path)
    rows = db.execute(
        "SELECT id FROM events WHERE pubkey=? AND kind=1 ORDER BY created_at DESC LIMIT ?",
        (CRYTER_PK, limit)
    ).fetchall()
    db.close()
    return [r[0] for r in rows]


def event_already_stored(db_path: str, event_id: str) -> bool:
    """Check if event already in DB."""
    db = sqlite3.connect(db_path)
    exists = db.execute(
        "SELECT 1 FROM events WHERE id=? LIMIT 1", (event_id,)
    ).fetchone()
    db.close()
    return exists is not None


def store_event(db_path: str, event: dict) -> bool:
    """Store a Nostr event in relay_v2.db. Returns True if new."""
    
    event_id = event.get('id', '')
    if not event_id:
        return False
    
    if event_already_stored(db_path, event_id):
        return False  # Already stored
    
    pubkey = event.get('pubkey', '')
    created_at = event.get('created_at', 0)
    kind = event.get('kind', 0)
    content = event.get('content', '')
    sig = event.get('sig', '')
    tags = event.get('tags', [])
    
    db = sqlite3.connect(db_path)
    db.execute("""
        INSERT INTO events (id, pubkey, created_at, kind, tags_json, content, sig, received_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        event_id,
        pubkey,
        created_at,
        kind,
        json.dumps(tags),
        content,
        sig,
        int(time.time())
    ))
    
    # Store tags
    for tag in tags:
        if len(tag) >= 2:
            db.execute(
                "INSERT INTO tags (event_id, tag_type, tag_value) VALUES (?, ?, ?)",
                (event_id, tag[0], tag[1])
            )
    
    db.commit()
    db.close()
    return True


async def query_relay_reactions(relay_url: str, event_ids: list[str]) -> list[dict]:
    """Query a relay for reactions/comments on Cryter's events."""
    try:
        ws = await asyncio.wait_for(
            websockets.connect(relay_url, ssl=SSL_CTX, max_size=5_000_000),
            timeout=10
        )
    except Exception as e:
        log(f"  {relay_url}: connection failed — {str(e)[:60]}")
        stats['conn_fails'] += 1
        return []
    
    # Query for kind:7 (reactions) + kind:1111 (comments) referencing Cryter events
    # Use #e (event tag) filter to find events that reference our event IDs
    sub_id = f"sync_{int(time.time())}"
    sub = json.dumps(["REQ", sub_id, {
        "kinds": [7, 1111, 1],
        "#e": event_ids[:20],  # limit to 20 IDs per subscription to avoid rejection
        "limit": 50,
    }])
    
    await ws.send(sub)
    
    events = []
    try:
        while True:
            msg = await asyncio.wait_for(ws.recv(), timeout=5)
            try:
                data = json.loads(msg)
            except json.JSONDecodeError:
                continue
            
            if not isinstance(data, list) or len(data) < 2:
                continue
            
            msg_type = data[0]
            if msg_type == "EVENT" and len(data) >= 3:
                event = data[2]
                # Skip our own events
                if event.get('pubkey') != CRYTER_PK:
                    events.append(event)
            elif msg_type == "EOSE":
                break
            elif msg_type == "NOTICE":
                log(f"  {relay_url} NOTICE: {data[1][:80]}")
    except asyncio.TimeoutError:
        pass  # No more events
    except Exception as e:
        log(f"  {relay_url}: read error — {str(e)[:60]}")
    
    await ws.close()
    return events


async def query_followed_posts(relay_url: str, followed_pks: list[str]) -> list[dict]:
    """Query a relay for recent posts from followed authors."""
    try:
        ws = await asyncio.wait_for(
            websockets.connect(relay_url, ssl=SSL_CTX, max_size=5_000_000),
            timeout=10
        )
    except Exception:
        stats['conn_fails'] += 1
        return []
    
    sub = json.dumps(["REQ", f"followed_{int(time.time())}", {
        "kinds": [1],
        "authors": followed_pks[:10],
        "limit": 5,
    }])
    
    await ws.send(sub)
    
    events = []
    try:
        while True:
            msg = await asyncio.wait_for(ws.recv(), timeout=5)
            try:
                data = json.loads(msg)
            except json.JSONDecodeError:
                continue
            
            if not isinstance(data, list) or len(data) < 2:
                continue
            
            if data[0] == "EVENT" and len(data) >= 3:
                events.append(data[2])
            elif data[0] == "EOSE":
                break
    except asyncio.TimeoutError:
        pass
    
    await ws.close()
    return events


def get_followed_pubkeys(db_path: str) -> list[str]:
    """Get pubkeys from Cryter's kind:3 contact list."""
    db = sqlite3.connect(db_path)
    rows = db.execute(
        "SELECT tags_json FROM events WHERE pubkey=? AND kind=3 ORDER BY created_at DESC LIMIT 1",
        (CRYTER_PK,)
    ).fetchone()
    db.close()
    
    if not rows:
        return []
    
    tags = json.loads(rows[0])
    pks = [tag[1] for tag in tags if tag[0] == 'p' and len(tag) > 1]
    return pks


async def sync_cycle():
    """One full sync cycle: query all external relays for reactions + comments."""
    log("=== Sync cycle start ===")
    
    event_ids = get_recent_event_ids(RELAY_DB, MAX_EVENT_IDS)
    log(f"  Event IDs to check: {len(event_ids)}")
    
    # Also get followed pubkeys for pulling their posts
    followed_pks = get_followed_pubkeys(RELAY_DB)
    log(f"  Followed pubkeys: {len(followed_pks)}")
    
    total_new = 0
    
    for relay_url in EXTERNAL_RELAYS:
        # Query reactions/comments on Cryter's events
        events = await query_relay_reactions(relay_url, event_ids)
        
        stored = 0
        for ev in events:
            if store_event(RELAY_DB, ev):
                stored += 1
        
        if stored > 0:
            log(f"  {relay_url}: {len(events)} found, {stored} new stored")
        total_new += stored
        
        # Also query posts from followed authors
        if followed_pks:
            posts = await query_followed_posts(relay_url, followed_pks)
            pstored = 0
            for ev in posts:
                if store_event(RELAY_DB, ev):
                    pstored += 1
            if pstored > 0:
                log(f"  {relay_url}: {len(posts)} followed posts, {pstored} new")
            total_new += pstored
    
    log(f"  Total new events: {total_new}")
    stats['total_synced'] += total_new
    stats['cycles'] += 1
    
    # Count by kind
    db = sqlite3.connect(RELAY_DB)
    k7 = db.execute("SELECT COUNT(*) FROM events WHERE kind=7").fetchone()[0]
    k1111 = db.execute("SELECT COUNT(*) FROM events WHERE kind=1111").fetchone()[0]
    db.close()
    log(f"  DB state: kind:7={k7}, kind:1111={k1111}, total_non_cryter_kind1=...")
    
    return total_new


async def run_daemon(interval: int):
    """Run sync cycles continuously."""
    log(f"Starting External Sync Daemon — interval={interval}s, {len(EXTERNAL_RELAYS)} relays")
    
    while True:
        try:
            await sync_cycle()
        except Exception as e:
            log(f"Cycle error: {e}")
        
        log(f"Sleeping {interval}s...")
        await asyncio.sleep(interval)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL, help="Seconds between cycles")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    args = parser.parse_args()
    
    if args.once:
        asyncio.run(sync_cycle())
    else:
        asyncio.run(run_daemon(args.interval))

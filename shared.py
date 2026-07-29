"""
SNIN Client — Shared utilities (DB + name resolution)
Extracted from app.py for modularization Phase 2.2
"""

import sqlite3
import json
import time
import os

# ─── Constants ───
DB_PATH = "/home/agent/data/sites/relay/relay_v2.db"

# ─── DB helpers ───
def db_query(sql, params=()):
    """Execute a read query and return list of dicts."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()

def db_query_one(sql, params=()):
    """Execute a read query and return single dict or None."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(sql, params).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

def db_execute(sql, params=()):
    """Execute a write query."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(sql, params)
        conn.commit()
    finally:
        conn.close()

def get_db():
    """Get a raw database connection (caller must close)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ─── Name resolution ───
def resolve_name(pubkey):
    """Resolve pubkey to display name from kind:0 profile."""
    row = db_query_one(
        "SELECT content FROM events WHERE kind=0 AND pubkey=? ORDER BY created_at DESC LIMIT 1",
        (pubkey,))
    if not row:
        return ""
    try:
        content = json.loads(row["content"])
        name = content.get("display_name") or content.get("name") or ""
        return str(name)[:100]
    except:
        return ""

def resolve_picture(pubkey):
    """Resolve pubkey to profile picture URL from kind:0 profile."""
    row = db_query_one(
        "SELECT content FROM events WHERE kind=0 AND pubkey=? ORDER BY created_at DESC LIMIT 1",
        (pubkey,))
    if not row:
        return ""
    try:
        content = json.loads(row["content"])
        return str(content.get("picture") or "")[:500]
    except:
        return ""

def resolve_nip05(pubkey):
    """Resolve pubkey to NIP-05 identifier."""
    row = db_query_one(
        "SELECT content FROM events WHERE kind=0 AND pubkey=? ORDER BY created_at DESC LIMIT 1",
        (pubkey,))
    if not row:
        return ""
    try:
        content = json.loads(row["content"])
        return str(content.get("nip05") or "")[:200]
    except:
        return ""

def pubkey_to_npub(pubkey):
    """Convert hex pubkey to bech32 npub format (basic)."""
    # Simplified: just return hex truncated for display
    if not pubkey:
        return ""
    return pubkey[:8] + "..." + pubkey[-4:]


def broadcast_event(*args):
    """Push event to our SNIN relay (ws://127.0.0.1:8197) for guaranteed storage.
    
    Accepts either a single dict (Nostr event) or positional args:
    (event_id, pubkey, kind, content, created_at, tags, sig)
    """
    if len(args) == 1 and isinstance(args[0], dict):
        event = args[0]
    elif len(args) >= 6:
        event = {
            "id": args[0],
            "pubkey": args[1],
            "kind": args[2],
            "content": args[3] if len(args) > 3 else "",
            "created_at": args[4] if len(args) > 4 else int(time.time()),
            "tags": args[5] if len(args) > 5 else [],
            "sig": args[6] if len(args) > 6 else ""
        }
    else:
        print(f"[BROADCAST] Invalid args: {len(args)}", flush=True)
        return
    
    try:
        import websocket
        ws = websocket.create_connection("ws://127.0.0.1:8197", timeout=5)
        ws.send(json.dumps(["EVENT", event]))
        ws.close()
        eid = event.get('id', '?')
        ek = event.get('kind', '?')
        print(f"[BROADCAST] Event {str(eid)[:12]} kind:{ek} -> SNIN Relay OK", flush=True)
    except Exception as e:
        print(f"[BROADCAST] Error: {e}", flush=True)


def verify_nostr_event(event_dict: dict) -> tuple[bool, str]:
    """Verify a Nostr event signature using nostr-sdk.
    
    Returns (is_valid, error_message).
    Verifies both EventId (hash) and Signature.
    """
    try:
        import nostr_sdk
        event_json = json.dumps(event_dict)
        event = nostr_sdk.Event.from_json(event_json)
        if event.verify():
            return True, ""
        else:
            return False, "signature verification failed"
    except Exception as e:
        return False, f"verification error: {str(e)}"

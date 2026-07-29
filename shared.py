"""
SNIN Client — Shared utilities (DB + name resolution + crypto)
Extracted from app.py for modularization Phase 2.2
"""

import sqlite3
import json
import time
import os
import base64
import hashlib

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


# ─── Wallet Secret Encryption (SEC-004) ───

_MASTER_KEY_PATH = "/home/agent/data/.nwc_master_key"
_fernet = None


def _get_fernet():
    """Lazy-load Fernet instance with master key."""
    global _fernet
    if _fernet is None:
        from cryptography.fernet import Fernet
        try:
            with open(_MASTER_KEY_PATH, 'rb') as f:
                key = f.read().strip()
        except FileNotFoundError:
            # Generate new key on first use
            key = Fernet.generate_key()
            with open(_MASTER_KEY_PATH, 'wb') as f:
                f.write(key)
            os.chmod(_MASTER_KEY_PATH, 0o600)
        _fernet = Fernet(key)
    return _fernet


def encrypt_wallet_secret(plaintext: str) -> str:
    """Encrypt a wallet secret using Fernet (AES-128-CBC). 
    Returns base64-encoded ciphertext."""
    if not plaintext:
        return ""
    try:
        f = _get_fernet()
        token = f.encrypt(plaintext.encode('utf-8'))
        return base64.b64encode(token).decode('ascii')
    except Exception as e:
        print(f"[ENCRYPT] Error: {e}", flush=True)
        return plaintext  # Fallback: store as plaintext (should not happen)


def decrypt_wallet_secret(ciphertext: str) -> str:
    """Decrypt a Fernet-encrypted wallet secret. 
    Accepts base64-encoded ciphertext, returns plaintext."""
    if not ciphertext:
        return ""
    try:
        f = _get_fernet()
        token = base64.b64decode(ciphertext)
        return f.decrypt(token).decode('utf-8')
    except Exception:
        # If decryption fails, the value might be legacy plaintext
        return ciphertext

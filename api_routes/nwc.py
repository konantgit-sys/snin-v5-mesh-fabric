"""
SNIN Client — NWC Wallet API (NIP-47)
"""

import json
import time
import asyncio
import sqlite3
from fastapi import APIRouter, Request, Query
from fastapi.responses import JSONResponse
from shared import db_query, db_query_one, encrypt_wallet_secret, decrypt_wallet_secret

router = APIRouter(prefix="/api/nwc", tags=["nwc"])

DB_PATH = "/home/agent/data/sites/relay/relay_v2.db"

# In-memory NWC instances: cache by user_pubkey
_nwc_instances = {}


def _get_nwc(pubkey: str):
    """Get or create NWC instance from stored config."""
    if pubkey in _nwc_instances:
        return _nwc_instances[pubkey]
    
    row = db_query_one("SELECT conn_string FROM nwc_config WHERE pubkey = ?", (pubkey,))
    if not row:
        return None
    
    try:
        import nostr_sdk
        uri = nostr_sdk.NostrWalletConnectUri.parse(row["conn_string"])
        nwc = nostr_sdk.Nwc(uri)
        _nwc_instances[pubkey] = nwc
        return nwc
    except Exception as e:
        print(f"[NWC] Parse error for {pubkey}: {e}", flush=True)
        return None


@router.post("/connect")
async def api_nwc_connect(request: Request):
    """Connect NWC wallet. Body: {pubkey, conn_string}"""
    try:
        body = await request.json()
        pubkey = body.get("pubkey", "").strip()
        conn_string = body.get("conn_string", "").strip()
    except:
        return JSONResponse({"success": False, "error": "Missing pubkey or conn_string"}, status_code=400)
    
    if not pubkey or not conn_string:
        return JSONResponse({"success": False, "error": "pubkey and conn_string required"}, status_code=400)
    
    if not conn_string.startswith("nostr+walletconnect://"):
        return JSONResponse({"success": False, "error": "Invalid NWC URI (must start with nostr+walletconnect://)"}, status_code=400)
    
    try:
        import nostr_sdk
        uri = nostr_sdk.NostrWalletConnectUri.parse(conn_string)
        wallet_pubkey = str(uri.public_key())
        relays = uri.relays()
        relay_url = str(relays[0]) if relays else ""
        wallet_secret = str(uri.secret())
        encrypted_secret = encrypt_wallet_secret(wallet_secret)
        
        nwc = nostr_sdk.Nwc(uri)
        try:
            status = await asyncio.wait_for(nwc.status(), timeout=10.0)
        except asyncio.TimeoutError:
            return JSONResponse({"success": False, "error": "NWC connection timeout"}, status_code=400)
        except Exception as e:
            return JSONResponse({"success": False, "error": f"NWC connection failed: {e}"}, status_code=400)
        
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT OR REPLACE INTO nwc_config (pubkey, conn_string, relay, wallet_pubkey, wallet_secret, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (pubkey, conn_string, relay_url, wallet_pubkey, encrypted_secret, int(time.time()))
        )
        conn.commit()
        conn.close()
        
        _nwc_instances[pubkey] = nwc
        
        return {"success": True, "wallet_pubkey": wallet_pubkey, "relay": relay_url}
    
    except Exception as e:
        return JSONResponse({"success": False, "error": f"Parse error: {e}"}, status_code=400)


@router.post("/pay")
async def api_nwc_pay(request: Request):
    """Pay a BOLT11 invoice via NWC. Body: {pubkey, invoice, amount_sats?}"""
    try:
        body = await request.json()
        pubkey = body.get("pubkey", "").strip()
        invoice = body.get("invoice", "").strip()
        amount_sats = body.get("amount_sats", 0)
    except:
        return JSONResponse({"success": False, "error": "Missing pubkey or invoice"}, status_code=400)
    
    if not pubkey or not invoice:
        return JSONResponse({"success": False, "error": "pubkey and invoice required"}, status_code=400)
    
    if not invoice.startswith("lnbc"):
        return JSONResponse({"success": False, "error": "Invalid BOLT11 invoice"}, status_code=400)
    
    nwc = _get_nwc(pubkey)
    if not nwc:
        return JSONResponse({"success": False, "error": "NWC not connected. Use /api/nwc/connect first"}, status_code=400)
    
    try:
        result = await asyncio.wait_for(nwc.pay_invoice(invoice), timeout=30.0)
        preimage = str(result) if result else ""
        
        conn2 = sqlite3.connect(DB_PATH)
        conn2.execute(
            "INSERT INTO nwc_transactions (pubkey, tx_type, amount_sats, invoice, preimage, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (pubkey, "send", amount_sats or 0, invoice, preimage, int(time.time()))
        )
        conn2.commit()
        conn2.close()
        
        try:
            bal = await asyncio.wait_for(nwc.get_balance(), timeout=5.0)
            balance_sats = int(bal) if bal else 0
            conn = sqlite3.connect(DB_PATH)
            conn.execute("UPDATE nwc_config SET last_balance = ? WHERE pubkey = ?", (balance_sats, pubkey))
            conn.commit()
            conn.close()
        except:
            pass
        
        return {"success": True, "preimage": preimage, "message": "Payment sent!"}
    
    except asyncio.TimeoutError:
        return JSONResponse({"success": False, "error": "Payment timeout (30s)"}, status_code=408)
    except Exception as e:
        err = str(e)
        if "insufficient" in err.lower():
            return JSONResponse({"success": False, "error": "Insufficient balance"}, status_code=402)
        return JSONResponse({"success": False, "error": f"Payment failed: {err}"}, status_code=400)


@router.get("/status")
async def api_nwc_status(pubkey: str = Query("")):
    """Get NWC connection status and balance."""
    if not pubkey:
        return {"connected": False, "error": "pubkey required", "balance_sats": 0}
    
    row = db_query_one("SELECT * FROM nwc_config WHERE pubkey = ?", (pubkey,))
    if not row:
        return {"connected": False, "balance_sats": 0}
    
    nwc = _get_nwc(pubkey)
    balance = row["last_balance"] or 0
    if nwc:
        try:
            bal = await asyncio.wait_for(nwc.get_balance(), timeout=5.0)
            balance = int(bal) if bal else 0
            conn = sqlite3.connect(DB_PATH)
            conn.execute("UPDATE nwc_config SET last_balance = ? WHERE pubkey = ?", (balance, pubkey))
            conn.commit()
            conn.close()
        except:
            pass
    
    return {
        "connected": True,
        "wallet_pubkey": row["wallet_pubkey"],
        "relay": row["relay"],
        "balance_sats": balance,
        "created_at": row["created_at"]
    }


@router.delete("/disconnect")
async def api_nwc_disconnect(pubkey: str = Query("")):
    """Disconnect NWC wallet."""
    if not pubkey:
        return {"success": False, "error": "pubkey required"}
    
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM nwc_config WHERE pubkey = ?", (pubkey,))
    conn.commit()
    conn.close()
    
    if pubkey in _nwc_instances:
        del _nwc_instances[pubkey]
    
    return {"success": True, "message": "NWC disconnected"}


@router.get("/history")
async def api_nwc_history(pubkey: str = Query(""), limit: int = 20):
    """Get NWC transaction history."""
    if not pubkey:
        return {"transactions": []}
    rows = db_query(
        "SELECT * FROM nwc_transactions WHERE pubkey = ? ORDER BY created_at DESC LIMIT ?",
        (pubkey, limit)
    )
    return {"transactions": rows}


@router.post("/receive")
async def api_nwc_receive(request: Request):
    """Generate a BOLT11 invoice to receive funds. Body: {pubkey, amount_sats, description?}"""
    try:
        body = await request.json()
    except:
        return JSONResponse({"success": False, "error": "Invalid JSON"}, status_code=400)
    pubkey = body.get("pubkey", "").strip()
    amount_sats = body.get("amount_sats", 0)
    description = body.get("description", "SNIN Client payment")

    if not pubkey:
        return JSONResponse({"success": False, "error": "pubkey required"}, status_code=400)
    if amount_sats <= 0:
        return JSONResponse({"success": False, "error": "amount_sats must be positive"}, status_code=400)

    nwc = _get_nwc(pubkey)
    if not nwc:
        return JSONResponse({"success": False, "error": "NWC not connected. Use /api/nwc/connect first"}, status_code=400)

    try:
        result = await asyncio.wait_for(nwc.make_invoice(amount_sats * 1000, description), timeout=15.0)
        invoice_str = str(result) if result else ""
        
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO nwc_transactions (pubkey, tx_type, amount_sats, invoice, created_at) VALUES (?, ?, ?, ?, ?)",
            (pubkey, "receive", amount_sats, invoice_str, int(time.time()))
        )
        conn.commit()
        conn.close()

        return {"success": True, "invoice": invoice_str}
    except asyncio.TimeoutError:
        return JSONResponse({"success": False, "error": "NWC request timed out"}, status_code=504)
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

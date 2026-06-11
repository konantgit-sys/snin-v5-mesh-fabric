#!/usr/bin/env python3
"""
Relay Signing — L5 Identity верификация релеев.

Релей подписывает свой статус Ed25519 ключом.
Smart Router предпочитает подписанные релеи (Tier 1).

Nostr kind:39003 — relay_signed event.

Эндпоинты:
  POST /verify_relay  — подписать релей {relay_url, pubkey, timestamp}
  GET  /signed_relays — список подписанных релеев
  GET  /health        — статус сервиса

Запуск:
  python3 relay_signing.py --port 9125
"""

import json
import os
import sys
import time
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import blinded_sigs as sigs

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

# ─── Конфиг ───
PORT = 9125
SIGNED_RELAYS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data", "signed_relays.json"
)
os.makedirs(os.path.dirname(SIGNED_RELAYS_FILE), exist_ok=True)

app = FastAPI(title="SNIN Relay Signing (L5)")

# ─── In-memory хранилище ───
_signed: dict[str, dict] = {}  # relay_url → {pubkey, signature, timestamp, mesh_id}
_start_time = time.time()


def _load_signed():
    """Загрузить подписанные релеи из файла."""
    global _signed
    try:
        with open(SIGNED_RELAYS_FILE) as f:
            _signed = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        _signed = {}


def _save_signed():
    """Сохранить подписанные релеи в файл."""
    with open(SIGNED_RELAYS_FILE, "w") as f:
        json.dump(_signed, f, indent=2)


# ─── Эндпоинты ───

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "Relay Signing (L5)",
        "signed_relays": len(_signed),
        "uptime": time.time() - _start_time,
    }


@app.get("/signed_relays")
async def get_signed_relays():
    """Вернуть список всех подписанных релеев."""
    return {
        "count": len(_signed),
        "relays": {url: info for url, info in _signed.items()}
    }


@app.get("/verify")
async def verify_relay_get(relay_url: str = "", signature: str = "",
                            timestamp: int = 0, pubkey: str = "",
                            mesh_id: str = "snin-main-1"):
    """
    Верифицировать подпись релея (GET версия).
    Использует собственный verifying_key relay_signing сервиса.
    """
    if not relay_url or not signature:
        return JSONResponse(status_code=400, content={"error": "relay_url and signature required"})
    
    vk = sigs.get_verifying_key_hex()
    valid = sigs.verify_cheque_sig(
        verifying_key_hex=vk,
        book_id=relay_url,
        index=timestamp,
        amount=0,
        recipient=f"{pubkey}:{mesh_id}",
        sig_hex=signature
    )
    
    return {
        "verified": valid,
        "tier": 1 if valid else 2,
        "relay_url": relay_url,
        "verifying_key": vk,
    }


@app.post("/verify_relay")
async def verify_relay(data: dict):
    """
    Подписать релей.
    
    Тело запроса:
      {
        "relay_url": "wss://relay.primal.net",
        "pubkey": "hex...",
        "mesh_id": "snin-main-1"
      }
    
    Релей подписывает: relay_url + pubkey + mesh_id + timestamp
    Возвращает подпись для Smart Router.
    """
    relay_url = data.get("relay_url", "").strip()
    pubkey = data.get("pubkey", "").strip()
    mesh_id = data.get("mesh_id", "snin-main-1")

    if not relay_url or not pubkey:
        raise HTTPException(status_code=400, detail="relay_url and pubkey required")
    if not relay_url.startswith("ws://") and not relay_url.startswith("wss://"):
        raise HTTPException(status_code=400, detail="relay_url must start with ws:// or wss://")
    if len(pubkey) != 64 or not all(c in "0123456789abcdef" for c in pubkey.lower()):
        raise HTTPException(status_code=400, detail="pubkey must be 64 hex chars")

    timestamp = int(time.time())

    # Подписываем: relay_url + pubkey + mesh_id + timestamp
    message = f"{relay_url}:{pubkey}:{mesh_id}:{timestamp}"
    signature = sigs.get_verifying_key_hex()  # ключ relay как идентификатор
    # Используем sign_cheque как generic Ed25519 signer
    sig_hex = sigs.sign_cheque(
        book_id=relay_url,
        index=timestamp,
        amount=0,
        recipient=f"{pubkey}:{mesh_id}"
    )

    entry = {
        "relay_url": relay_url,
        "pubkey": pubkey,
        "mesh_id": mesh_id,
        "signature": sig_hex,
        "timestamp": timestamp,
        "verified_at": time.time(),
    }

    _signed[relay_url] = entry
    _save_signed()

    return {
        "status": "signed",
        "relay_url": relay_url,
        "signature": sig_hex,
        "timestamp": timestamp,
        "verifying_key": sigs.get_verifying_key_hex(),
    }


# ─── Загрузка при старте ───
_load_signed()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=PORT)
    args = parser.parse_args()
    
    # Инициализация ключей подписи
    sigs.init_signing()
    print(f"🔏 Relay Signing (L5) on :{args.port}")
    print(f"   Verifying key: {sigs.get_verifying_key_hex()[:16]}...")
    print(f"   Signed relays loaded: {len(_signed)}")
    
    uvicorn.run(app, host="0.0.0.0", port=args.port, log_level="info")

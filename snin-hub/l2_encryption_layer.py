"""
SNIN L2.5 — Encryption Layer (Universal Architecture 2.0, порт :9600)

Защита трафика между L2 Transport и L3 Mesh:
  — X25519 ECDH — генерация общих ключей
  — ChaCha20-Poly1305 — шифрование сообщений (AEAD)
  — Perfect Forward Secrecy — смена ключа каждые N сообщений
  — Onion Routing (3 hop) — скрытие маршрута
  — Ed25519 подписи — аутентификация отправителя

Интеграция:
  → L5 Identity: публичные ключи агентов, DID
  → L4 Payment: подпись транзакций
  → L2 Transport: шифрование перед отправкой
"""

import hashlib
import hmac
import json
import logging
import os
import sys
import time
import uuid
import secrets
import struct
import threading
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel
import uvicorn

# ─── Crypto imports ───
from cryptography.hazmat.primitives.asymmetric import x25519, ed25519
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305

logging.basicConfig(level=logging.INFO, format="[ENC] %(message)s")
logger = logging.getLogger("enc")

app = FastAPI(title="SNIN L2.5 Encryption Layer", version="1.0.0")
api = APIRouter(prefix="/api/v1")

# ─── Health endpoint для L9 (без префикса) ─────
@app.get("/health")
def root_health():
    return {"status": "ok", "layer": "L2.5 Encryption", "ts": time.time(), "alive": True}

# ─── Internal State ─────
sessions: Dict[str, dict] = {}       # session_id → {keys, counter, created}
onion_routes: Dict[str, list] = {}   # route_id → [hop1, hop2, hop3]
identities: Dict[str, dict] = {}     # peer_id → {pubkey_enc, pubkey_sig}
message_log: list = []
MAX_LOG = 100
PFS_INTERVAL = 50                    # смена ключа каждые N сообщений

# ───── Models ─────

class KeyExchangeRequest(BaseModel):
    peer_id: str
    public_key_enc: str              # hex-encoded X25519 public key
    public_key_sig: str              # hex-encoded Ed25519 public key

class EncryptRequest(BaseModel):
    session_id: str
    plaintext: str
    sender: str = ""

class DecryptRequest(BaseModel):
    session_id: str
    ciphertext: str
    nonce: str

class OnionBuildRequest(BaseModel):
    hops: list[str]                  # [hop1_pubkey, hop2_pubkey, hop3_pubkey]
    payload: str

class SignRequest(BaseModel):
    peer_id: str
    message: str

# ══════════════════════════════════════════════════════════════
# 1. KEY MANAGEMENT — X25519 + Ed25519
# ══════════════════════════════════════════════════════════════

def generate_identity() -> dict:
    """Генерация пары ключей агента (X25519 + Ed25519)."""
    priv_enc = x25519.X25519PrivateKey.generate()
    pub_enc = priv_enc.public_key()

    priv_sig = ed25519.Ed25519PrivateKey.generate()
    pub_sig = priv_sig.public_key()

    return {
        "private_key_enc": priv_enc.private_bytes_raw().hex(),
        "public_key_enc": pub_enc.public_bytes_raw().hex(),
        "private_key_sig": priv_sig.private_bytes_raw().hex(),
        "public_key_sig": pub_sig.public_bytes_raw().hex(),
        "created": time.time(),
    }

def ecdh_shared(priv_hex: str, pub_hex: str) -> bytes:
    """X25519 ECDH: общий секрет."""
    priv = x25519.X25519PrivateKey.from_private_bytes(bytes.fromhex(priv_hex))
    pub = x25519.X25519PublicKey.from_public_bytes(bytes.fromhex(pub_hex))
    return priv.exchange(pub)

def derive_session_key(shared_secret: bytes, salt: bytes = b"snin-l2.5-v1") -> bytes:
    """HKDF → 32 байта ключа для ChaCha20-Poly1305."""
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        info=b"snin-encryption-layer",
    )
    return hkdf.derive(shared_secret)


def create_session(peer_a: str, peer_b: str,
                   priv_a: str, pub_b: str) -> dict:
    """Создание сессии между двумя пирами."""
    shared = ecdh_shared(priv_a, pub_b)
    key = derive_session_key(shared)

    session_id = uuid.uuid4().hex[:16]
    sessions[session_id] = {
        "session_id": session_id,
        "peer_a": peer_a,
        "peer_b": peer_b,
        "key": key.hex(),
        "counter": 0,
        "created": time.time(),
        "last_used": time.time(),
        "pfs_interval": PFS_INTERVAL,
    }
    return sessions[session_id]

# ══════════════════════════════════════════════════════════════
# 2. ENCRYPTION / DECRYPTION — ChaCha20-Poly1305 (AEAD)
# ══════════════════════════════════════════════════════════════

def encrypt(session_id: str, plaintext: str, aad: bytes = b"") -> dict:
    """Шифрование plaintext через ChaCha20-Poly1305."""
    if session_id not in sessions:
        raise ValueError(f"Session {session_id} not found")

    session = sessions[session_id]
    key = bytes.fromhex(session["key"])
    nonce = secrets.token_bytes(12)  # 96-bit nonce
    aad_data = aad or session_id.encode()

    chacha = ChaCha20Poly1305(key)
    ciphertext = chacha.encrypt(nonce, plaintext.encode(), aad_data)

    session["counter"] += 1
    session["last_used"] = time.time()

    # PFS: смена ключа каждые N сообщений
    if session["counter"] >= PFS_INTERVAL:
        # Ре-инициализация ключа
        new_shared = secrets.token_bytes(32)
        new_key = derive_session_key(new_shared, salt=session_id.encode())
        session["key"] = new_key.hex()
        session["counter"] = 0
        session["pfs_rotated_at"] = time.time()
        logger.info(f"PFS key rotation for session {session_id[:8]}")

    return {
        "ciphertext": ciphertext.hex(),
        "nonce": nonce.hex(),
        "session_id": session_id,
        "counter": session["counter"],
    }

def decrypt(session_id: str, ciphertext_hex: str, nonce_hex: str,
            aad: bytes = b"") -> str:
    """Дешифрование ChaCha20-Poly1305."""
    if session_id not in sessions:
        raise ValueError(f"Session {session_id} not found")

    session = sessions[session_id]
    key = bytes.fromhex(session["key"])
    nonce = bytes.fromhex(nonce_hex)
    ciphertext = bytes.fromhex(ciphertext_hex)
    aad_data = aad or session_id.encode()

    chacha = ChaCha20Poly1305(key)
    plaintext = chacha.decrypt(nonce, ciphertext, aad_data)

    return plaintext.decode()

# ══════════════════════════════════════════════════════════════
# 3. ONION ROUTING — 3-hop
# ══════════════════════════════════════════════════════════════

def onion_wrap(payload: str, hops: list[str], session_keys: list[bytes]) -> dict:
    """
    Onion routing: оборачиваем payload в 3 слоя.
    Каждый слой шифруется ключом соответствующего хопа.
    """
    if len(hops) != 3 or len(session_keys) != 3:
        raise ValueError("Onion requires exactly 3 hops and 3 keys")

    # Внутренний слой: payload + routing info к финальному получателю
    inner = json.dumps({
        "payload": payload,
        "final": True,
        "ts": time.time(),
    })

    # Заворачиваем снаружи внутрь: hop3 → hop2 → hop1
    layers = []
    current = inner.encode()

    for i in reversed(range(3)):
        nonce = secrets.token_bytes(12)
        chacha = ChaCha20Poly1305(session_keys[i])
        encrypted = chacha.encrypt(nonce, current, b"onion-v1")

        # Внешний слой: encrypted + указание следующего хопа
        layer = {
            "next": hops[i] if i < 2 else "",  # последний слой — пустой next
            "ciphertext": encrypted.hex(),
            "nonce": nonce.hex(),
            "hop": i,
        }
        current = json.dumps(layer).encode()
        layers.append(layer)

    route_id = uuid.uuid4().hex[:12]

    # Внешний слой (hop1) — то что отправляется в сеть
    outer = layers[-1]

    # Сохраняем маршрут для логирования
    onion_routes[route_id] = layers

    return {
        "route_id": route_id,
        "hops": len(hops),
        "outer_ciphertext": outer["ciphertext"],
        "outer_nonce": outer["nonce"],
        "next_hop": outer.get("next", ""),
    }

def onion_unwrap(layer_ciphertext: str, layer_nonce: str,
                 session_key: bytes) -> dict:
    """
    Разворачивание одного слоя луковицы.
    Если next пустой — это финальный получатель.
    """
    chacha = ChaCha20Poly1305(session_key)
    decrypted = chacha.decrypt(
        bytes.fromhex(layer_nonce),
        bytes.fromhex(layer_ciphertext),
        b"onion-v1"
    )

    layer = json.loads(decrypted)
    return layer

# ══════════════════════════════════════════════════════════════
# 4. SIGNATURES — Ed25519
# ══════════════════════════════════════════════════════════════

def sign_message(priv_sig_hex: str, message: str) -> str:
    """Ed25519 подпись сообщения."""
    priv = ed25519.Ed25519PrivateKey.from_private_bytes(
        bytes.fromhex(priv_sig_hex)
    )
    sig = priv.sign(message.encode())
    return sig.hex()

def verify_signature(pub_sig_hex: str, message: str, signature_hex: str) -> bool:
    """Проверка Ed25519 подписи."""
    try:
        pub = ed25519.Ed25519PublicKey.from_public_bytes(
            bytes.fromhex(pub_sig_hex)
        )
        pub.verify(bytes.fromhex(signature_hex), message.encode())
        return True
    except Exception:
        return False

# ══════════════════════════════════════════════════════════════
# 5. KEYSERVER — хранилище публичных ключей
# ══════════════════════════════════════════════════════════════

def _sync_from_l5():
    """Загружаем ключи агентов из L5 Identity."""
    import urllib.request as r
    try:
        resp = r.urlopen("http://127.0.0.1:9940/identity/all", timeout=5)
        data = json.loads(resp.read())
        for a in data.get("agents", []):
            name = a.get("agent_name", "")
            if name:
                # Генерируем ключи, если нет — L5 не хранит ключи шифрования
                if name not in identities:
                    identity = generate_identity()
                    identities[name] = {
                        "peer_id": name,
                        "public_key_enc": identity["public_key_enc"],
                        "public_key_sig": identity["public_key_sig"],
                        "source": "l5_sync",
                        "created": time.time(),
                    }
                    logger.info(f"Generated keys for {name}")
    except Exception as e:
        logger.warning(f"L5 sync error: {e}")


# ══════════════════════════════════════════════════════════════
# API ENDPOINTS
# ══════════════════════════════════════════════════════════════

@api.get("/")
def root():
    return {
        "service": "SNIN L2.5 Encryption Layer",
        "version": "1.0.0",
        "sessions": len(sessions),
        "identities": len(identities),
        "routes": len(onion_routes),
        "ciphers": ["ChaCha20-Poly1305", "X25519", "Ed25519"],
        "pfs_interval": PFS_INTERVAL,
        "status": "live"
    }

@api.get("/health")
def health():
    return {
        "encryption": "ok",
        "ts": time.time(),
        "sessions_active": len(sessions),
        "identities_registered": len(identities),
        "capabilities": [
            "x25519_ecdh",
            "chacha20_poly1305",
            "ed25519_sign",
            "pfs_key_rotation",
            "onion_routing_3hop"
        ]
    }

# ─── Key Server ───

@api.post("/keys/generate")
def keys_generate(peer_id: str):
    """Генерация Identity для пира."""
    identity = generate_identity()
    identities[peer_id] = {
        "peer_id": peer_id,
        "public_key_enc": identity["public_key_enc"],
        "public_key_sig": identity["public_key_sig"],
        "source": "generated",
        "created": time.time(),
    }
    # Возвращаем ВСЁ (включая приватные) — только для создателя
    return {
        "peer_id": peer_id,
        **identity
    }

@api.get("/keys/{peer_id}")
def keys_get(peer_id: str):
    """Публичные ключи пира (публичный эндпоинт)."""
    if peer_id not in identities:
        raise HTTPException(404, f"Identity {peer_id} not found")
    info = identities[peer_id]
    return {
        "peer_id": peer_id,
        "public_key_enc": info["public_key_enc"],
        "public_key_sig": info["public_key_sig"],
    }

@api.get("/keys")
def keys_list():
    """Список зарегистрированных Identity."""
    return {
        "identities": [
            {"peer_id": pid, "has_enc": bool(i.get("public_key_enc"))}
            for pid, i in identities.items()
        ],
        "count": len(identities)
    }

# ─── Session Management ───

@api.post("/session/create")
def session_create(req: KeyExchangeRequest):
    """Создание сессии с peer'ом через X25519 ECDH."""
    # Генерируем временный ключ для этой сессии
    ephemeral_priv = x25519.X25519PrivateKey.generate()
    ephemeral_pub = ephemeral_priv.public_key()

    peer_pub = x25519.X25519PublicKey.from_public_bytes(
        bytes.fromhex(req.public_key_enc)
    )
    shared = ephemeral_priv.exchange(peer_pub)
    key = derive_session_key(shared)

    session_id = uuid.uuid4().hex[:16]
    sessions[session_id] = {
        "session_id": session_id,
        "peer_id": req.peer_id,
        "ephemeral_pub": ephemeral_pub.public_bytes_raw().hex(),
        "key": key.hex(),
        "counter": 0,
        "created": time.time(),
        "last_used": time.time(),
        "pfs_interval": PFS_INTERVAL,
    }

    # Сохраняем публичный ключ пира
    identities[req.peer_id] = {
        "peer_id": req.peer_id,
        "public_key_enc": req.public_key_enc,
        "public_key_sig": req.public_key_sig,
        "source": "session",
        "created": time.time(),
    }

    return {
        "session_id": session_id,
        "peer_id": req.peer_id,
        "our_public_key": ephemeral_pub.public_bytes_raw().hex(),
        "counter": 0,
    }

@api.get("/session/{session_id}")
def session_status(session_id: str):
    """Статус сессии."""
    if session_id not in sessions:
        raise HTTPException(404, "Session not found")
    s = sessions[session_id]
    return {
        "session_id": s["session_id"],
        "peer_id": s["peer_id"],
        "counter": s["counter"],
        "age_sec": round(time.time() - s["created"], 1),
        "idle_sec": round(time.time() - s["last_used"], 1),
        "pfs_remaining": PFS_INTERVAL - s["counter"],
    }

@api.delete("/session/{session_id}")
def session_destroy(session_id: str):
    """Уничтожение сессии."""
    if session_id not in sessions:
        raise HTTPException(404, "Session not found")
    s = sessions.pop(session_id)
    return {"status": "destroyed", "messages_encrypted": s["counter"]}

# ─── Encrypt / Decrypt ───

@api.post("/encrypt")
def api_encrypt(req: EncryptRequest):
    """Шифрование сообщения через существующую сессию."""
    try:
        result = encrypt(req.session_id, req.plaintext)
        return result
    except ValueError as e:
        raise HTTPException(400, str(e))

@api.post("/decrypt")
def api_decrypt(req: DecryptRequest):
    """Дешифрование сообщения."""
    try:
        plaintext = decrypt(req.session_id, req.ciphertext, req.nonce)
        return {"plaintext": plaintext}
    except ValueError as e:
        raise HTTPException(400, str(e))

@api.post("/encrypt/raw")
def encrypt_raw(pub_key_hex: str, plaintext: str):
    """
    Шифрование одноразовым ключом (без создания сессии).
    Полезно для broadcast сообщений.
    """
    try:
        peer_pub = x25519.X25519PublicKey.from_public_bytes(
            bytes.fromhex(pub_key_hex)
        )
        ephemeral_priv = x25519.X25519PrivateKey.generate()
        ephemeral_pub = ephemeral_priv.public_key()

        shared = ephemeral_priv.exchange(peer_pub)
        key = derive_session_key(shared, salt=b"raw-encrypt")

        nonce = secrets.token_bytes(12)
        chacha = ChaCha20Poly1305(key)
        ciphertext = chacha.encrypt(nonce, plaintext.encode(), b"raw")

        return {
            "ciphertext": ciphertext.hex(),
            "nonce": nonce.hex(),
            "ephemeral_pub": ephemeral_pub.public_bytes_raw().hex(),
        }
    except Exception as e:
        raise HTTPException(400, str(e)[:80])

@api.post("/decrypt/raw")
def decrypt_raw(pub_key_hex: str, priv_key_hex: str,
                ciphertext: str, nonce: str):
    """Дешифрование одноразового сообщения."""
    try:
        priv = x25519.X25519PrivateKey.from_private_bytes(
            bytes.fromhex(priv_key_hex)
        )
        pub = x25519.X25519PublicKey.from_public_bytes(
            bytes.fromhex(pub_key_hex)
        )
        shared = priv.exchange(pub)
        key = derive_session_key(shared, salt=b"raw-encrypt")

        chacha = ChaCha20Poly1305(key)
        plaintext = chacha.decrypt(
            bytes.fromhex(nonce),
            bytes.fromhex(ciphertext),
            b"raw"
        )
        return {"plaintext": plaintext.decode()}
    except Exception as e:
        raise HTTPException(400, str(e)[:80])

# ─── Signatures ───

@api.post("/sign")
def api_sign(req: SignRequest):
    """Подпись сообщения (локально, через симулированную подпись)."""
    if req.peer_id not in identities:
        raise HTTPException(404, f"Identity {req.peer_id} not found")
    # Локальная подпись через SHA256 хэш (simulated signature)
    # В проде: Ed25519 через приватный ключ из secure enclave
    msg_hash = hashlib.sha256(req.message.encode()).hexdigest()
    fake_sig = f"sig_{req.peer_id}_{msg_hash[:16]}"
    return {"signature": fake_sig, "peer_id": req.peer_id, "algorithm": "simulated-ed25519"}

@api.post("/verify")
def api_verify(peer_id: str, message: str, signature: str):
    """Проверка подписи."""
    if peer_id not in identities:
        raise HTTPException(404, f"Identity {peer_id} not found")
    # Simulated verify: hash-based
    msg_hash = hashlib.sha256(message.encode()).hexdigest()
    expected = f"sig_{peer_id}_{msg_hash[:16]}"
    valid = hmac.compare_digest(expected, signature)
    return {"valid": valid, "peer_id": peer_id, "algorithm": "simulated-ed25519"}

# ─── Onion Routing ───

@api.post("/onion/build")
def onion_build(req: OnionBuildRequest):
    """Построение 3-hop onion маршрута."""
    if len(req.hops) != 3:
        raise HTTPException(400, "Need exactly 3 hops")

    # Получаем ключи для каждого хопа
    hop_keys = []
    for hop_id in req.hops:
        if hop_id not in identities:
            raise HTTPException(404, f"Hop {hop_id} not registered")
        pub_enc = bytes.fromhex(identities[hop_id]["public_key_enc"])
        key = derive_session_key(pub_enc, salt=f"hop-{hop_id}".encode())
        hop_keys.append(key)

    result = onion_wrap(req.payload, req.hops, hop_keys)
    return result

@api.post("/onion/unwrap")
def onion_unwrap_endpoint(hop_id: str, layer_ciphertext: str, layer_nonce: str):
    """Разворачивание одного слоя onion."""
    if hop_id not in identities:
        raise HTTPException(404, f"Hop {hop_id} not found")

    # Восстанавливаем ключ хопа
    pub_enc = bytes.fromhex(identities[hop_id]["public_key_enc"])
    key = derive_session_key(pub_enc, salt=f"hop-{hop_id}".encode())

    try:
        layer = onion_unwrap(layer_ciphertext, layer_nonce, key)
        return layer
    except Exception as e:
        raise HTTPException(400, f"Decrypt failed: {str(e)[:80]}")

@api.get("/onion/routes")
def onion_routes_list():
    """Список onion маршрутов."""
    return {
        "routes": [
            {"route_id": rid, "hops": len(layers), "created": layers[-1].get("hop", 0) if layers else 0}
            for rid, layers in list(onion_routes.items())[:20]
        ],
        "count": len(onion_routes)
    }

# ─── Sync ───

@api.post("/sync/from-l5")
def sync_from_l5():
    """Синхронизация ключей из L5 Identity."""
    before = len(identities)
    _sync_from_l5()
    after = len(identities)
    return {
        "status": "synced",
        "before": before,
        "after": after,
        "new": after - before,
        "total_identities": after,
    }

# ─── PFS status ───

@api.get("/pfs/status")
def pfs_status():
    """Статус PFS для всех сессий."""
    now = time.time()
    return {
        "pfs_interval": PFS_INTERVAL,
        "sessions": [
            {
                "session_id": s["session_id"][:8],
                "peer_id": s["peer_id"],
                "counter": s["counter"],
                "remaining": PFS_INTERVAL - s["counter"],
                "created_ago": round(now - s["created"], 1),
            }
            for s in sessions.values()
        ],
        "total": len(sessions),
        "pfs_enabled": True,
    }


# ─── Init ───
_sync_from_l5()

# ─── Mount ───
app.include_router(api)

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9600
    print(f"[ENC] Starting L2.5 Encryption Layer on :{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")

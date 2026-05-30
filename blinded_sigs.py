"""
SNIN Blinded Signatures — Ed25519 Cheque Signing (Phase 21)

Для MVP: plain Ed25519 (без RSA blinding).
Каждый cheque = relay подписывает (book_id + index) приватным ключом.
Верификация: Ed25519 verify — 0.05ms.

Реальная blinded схема (RSA) — опция для production,
чтобы relay не знал, какой именно cheque тратится.
"""

import json
import logging
import time
import os
from typing import Optional, Tuple

logger = logging.getLogger('blinded_sigs')

# ── Ed25519 ключ relay ──
# Для MVP: генерируем при старте, храним в памяти
# В production: читать из env / secure storage
RELAY_SIGNING_KEY = None
RELAY_VERIFYING_KEY = None


def init_signing():
    """Инициализировать ключи подписи relay."""
    global RELAY_SIGNING_KEY, RELAY_VERIFYING_KEY
    from cryptography.hazmat.primitives.asymmetric import ed25519
    from cryptography.hazmat.primitives import serialization
    
    KEY_FILE = "/home/agent/data/sites/relay-mesh/.cheque_key"
    
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, "rb") as f:
            RELAY_SIGNING_KEY = ed25519.Ed25519PrivateKey.from_private_bytes(f.read(32))
    else:
        RELAY_SIGNING_KEY = ed25519.Ed25519PrivateKey.generate()
        with open(KEY_FILE, "wb") as f:
            f.write(RELAY_SIGNING_KEY.private_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PrivateFormat.Raw,
                encryption_algorithm=serialization.NoEncryption()
            ))
    
    RELAY_VERIFYING_KEY = RELAY_SIGNING_KEY.public_key()
    logger.info(f"Signing key loaded. Verifying key: {get_verifying_key_hex()[:16]}...")
    return True


def get_verifying_key_hex() -> str:
    """Получить публичный ключ relay (hex)."""
    global RELAY_VERIFYING_KEY
    if not RELAY_VERIFYING_KEY:
        return ""
    from cryptography.hazmat.primitives import serialization
    return RELAY_VERIFYING_KEY.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw
    ).hex()


def sign_cheque(book_id: str, index: int, amount: float, recipient: str) -> str:
    """
    Подписать cheque.
    
    Подписывается: f"{book_id}:{index}:{amount}:{recipient}"
    Возвращает: hex-подпись (64 байта)
    """
    global RELAY_SIGNING_KEY
    if not RELAY_SIGNING_KEY:
        raise RuntimeError("Signing key not initialized")
    
    message = f"{book_id}:{index}:{amount}:{recipient}".encode()
    sig = RELAY_SIGNING_KEY.sign(message)
    return sig.hex()


def verify_cheque_sig(
    verifying_key_hex: str,
    book_id: str,
    index: int,
    amount: float,
    recipient: str,
    sig_hex: str
) -> bool:
    """
    Верифицировать подпись чека.
    
    Returns: True если подпись валидна
    """
    try:
        from cryptography.hazmat.primitives.asymmetric import ed25519
        from cryptography.hazmat.primitives import serialization
        
        vk_bytes = bytes.fromhex(verifying_key_hex)
        vk = ed25519.Ed25519PublicKey.from_public_bytes(vk_bytes)
        
        message = f"{book_id}:{index}:{amount}:{recipient}".encode()
        sig = bytes.fromhex(sig_hex)
        
        vk.verify(sig, message)
        return True
    except Exception as e:
        logger.warning(f"Cheque sig verify failed: {e}")
        return False


def sign_kind30000_cheque(
    book_id: str,
    index: int,
    amount: float,
    recipient: str,
    sender_pubkey: str
) -> dict:
    """
    Создать cheque для kind:30000.
    
    Returns:
        {"cheque_sig": "hex", "book_id": "...", "index": N}
    """
    sig = sign_cheque(book_id, index, amount, recipient)
    return {
        "cheque_sig": sig,
        "book_id": book_id,
        "index": index,
    }

"""
SNIN Payment Handler — адаптирован под relay-mesh
Phase 20 (S1): Optimistic Verify-Later

Обрабатывает:
- kind:30000 (payment) — платёж SNIN между pubkey
- kind:30001 (balance_request) — запрос баланса
- kind:30002 (balance_response) — ответ с балансом

Архитектура:
  Mesh НЕ ждёт верификацию (optimistic).
  verifier.py верифицирует асинхронно.
  Все средства на Solana blockchain.
"""

import json
import logging
import time
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')
logger = logging.getLogger('payment_handler')

# ── Config ──
DEFAULT_FEE_SNIN = 0.01
MAX_SEEN_TX = 100_000
SNIN_DECIMALS = 9
RELAY_FEE_ADDRESS = None

# In-memory double-spend prevention
seen_tx: set = set()

# Очередь для verifier.py (файловая, verifier читает через polling)
QUEUE_FILE = "/dev/shm/payment_queue.jsonl"


def init_payments(fee_address: str = None):
    """Инициализировать платёжный модуль."""
    global RELAY_FEE_ADDRESS
    RELAY_FEE_ADDRESS = fee_address
    logger.info(f"Payment handler initialized. Fee: {DEFAULT_FEE_SNIN} SNIN/event")
    return True


def validate_payment_event(event: dict) -> dict:
    """
    Валидация kind:30000 на лету (без RPC).
    Проверяет только структуру — реальную верификацию делает verifier.
    """
    tags = event.get("tags", [])
    pubkey = event.get("pubkey", "")
    content_raw = event.get("content", "{}")
    
    # Парсим content
    try:
        content = json.loads(content_raw) if isinstance(content_raw, str) else content_raw
    except json.JSONDecodeError:
        return {"accepted": False, "reason": "invalid JSON in content"}
    
    amount = content.get("amount", 0)
    token = content.get("token", "SNIN")
    
    # Извлекаем теги
    p_tag = None
    solana_tx = None
    solana_addr = None
    expiration = None
    
    for tag in tags:
        t = tag[0] if isinstance(tag, list) else ""
        if t == "p" and len(tag) > 1:
            p_tag = tag[1]
        elif t == "solana_tx" and len(tag) > 1:
            solana_tx = tag[1]
        elif t == "solana_addr" and len(tag) > 1:
            solana_addr = tag[1]
        elif t == "expiration" and len(tag) > 1:
            try:
                expiration = int(tag[1])
            except ValueError:
                pass
    
    # Проверка обязательных полей
    if not p_tag:
        return {"accepted": False, "reason": "missing required tag: p"}
    if not solana_tx:
        return {"accepted": False, "reason": "missing required tag: solana_tx"}
    if not amount or amount <= 0:
        return {"accepted": False, "reason": "amount must be positive"}
    
    # Проверка expiration
    now = int(time.time())
    if expiration and now > expiration:
        return {"accepted": False, "reason": "event expired"}
    
    # Оптимистичное принятие — добавляем в очередь для verifier
    queue_item = {
        "event_id": event.get("id", solana_tx[:32]),
        "pubkey": pubkey,
        "p_tag": p_tag,
        "amount": amount,
        "token": token,
        "solana_tx": solana_tx,
        "solana_addr": solana_addr,
        "received_at": time.time(),
    }
    
    with open(QUEUE_FILE, "a") as f:
        f.write(json.dumps(queue_item) + "\n")
    
    return {
        "accepted": True,
        "reason": "optimistic — verification in progress",
        "queue_item": queue_item,
    }


def get_stats() -> dict:
    """Статистика платёжного модуля."""
    return {
        "seen_tx_count": len(seen_tx),
        "fee_snin": DEFAULT_FEE_SNIN,
        "relay_fee_address": RELAY_FEE_ADDRESS,
        "queue_file": QUEUE_FILE,
    }


def mark_verified(solana_tx: str):
    """Отметить транзакцию как верифицированную."""
    seen_tx.add(solana_tx)
    if len(seen_tx) > MAX_SEEN_TX:
        seen_tx.pop()

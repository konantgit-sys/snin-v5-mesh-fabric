"""
SNIN L4.5 — Privacy Layer (Universal Architecture 2.0, порт :9700)

Анонимизация трафика между L4 Payment и L5 Identity:
  — Mixnet — пул перемешивания + случайная задержка (10-60s)
  — Dandelion++ — Stem (цепочка) → Flock (broadcast)
  — CoinJoin — смешивание L4 платежей в общие транзакции
  — Cover Traffic — фиктивные сообщения для защиты от анализа
  — Noise Injection — подмешивание случайных MAC-адресов

Интеграция:
  → L4 Payment: анонимные транзакции через CoinJoin
  → L2 Transport: скрытие источника через Dandelion
  → L5 Identity: эфемерные DID (одноразовые ключи)
"""

import hashlib
import json
import logging
import os
import random
import sys
import time
import uuid
import secrets
import threading
from collections import deque
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel
import uvicorn

logging.basicConfig(level=logging.INFO, format="[PRIV] %(message)s")
logger = logging.getLogger("privacy")

app = FastAPI(title="SNIN L4.5 Privacy Layer", version="1.0.0")
api = APIRouter(prefix="/api/v1")

# ─── Health endpoint для L9 (без префикса) ─────
@app.get("/health")
def root_health():
    return {"status": "ok", "layer": "L4 Privacy", "ts": time.time(), "alive": True}

# ───── Internal State ─────
mix_pool: List[dict] = []                # неотправленные сообщения в миксе
mix_history: deque = deque(maxlen=500)    # история отправленных
dandelion_routes: Dict[str, list] = {}    # stem_id → [node1, node2, ...]
coinjoin_pools: Dict[str, list] = {}      # pool_id → [transactions]
cover_jobs: List[dict] = []               # активные cover traffic задачи
noise_keys: set = set()                   # использованные эфемерные ключи
stats: dict = {
    "mixed": 0, "dandelion_sent": 0,
    "coinjoin_merged": 0, "cover_sent": 0,
    "failed": 0,
}

# ───── Configuration ─────
MIX_INTERVAL = 8               # сек — интервал отправки из микса
MIX_DELAY_MIN = 10             # мин задержка в миксе (сек)
MIX_DELAY_MAX = 60             # макс задержка
DANDELION_STEM_LEN = (2, 5)    # длина цепочки stem
COINJOIN_MIN_TXS = 3           # мин транзакций для пула
COVER_INTERVAL = 15            # сек — интервал cover traffic
NOISE_FACTOR = 0.3             # доля фиктивного трафика

# ───── Models ─────

class MixMessage(BaseModel):
    payload: str
    source: str = ""
    target: str = ""
    delay: int = 0               # 0 = auto (10-60s)
    ttl: int = 120

class DandelionMessage(BaseModel):
    payload: str
    source: str = ""
    stem_len: int = 0            # 0 = auto

class CoinJoinRequest(BaseModel):
    transactions: list[dict]
    pool_id: str = ""            # пустой = создать новый пул
    metadata: dict = {}

class AnonymizeMessageRequest(BaseModel):
    content: str
    method: str = "dandelion"

class AnonymizePaymentRequest(BaseModel):
    sender: str
    amount: float
    target: str = ""

class CoverConfig(BaseModel):
    enabled: bool = True
    interval: int = 15
    noise_factor: float = 0.3
    enabled: bool = True
    interval: int = 15
    noise_factor: float = 0.3


# ══════════════════════════════════════════════════════════════
# 1. MIXNET — пул перемешивания + задержка
# ══════════════════════════════════════════════════════════════

def mix_add(msg: MixMessage) -> str:
    """Добавить сообщение в mixnet."""
    msg_id = uuid.uuid4().hex[:12]
    delay = msg.delay if msg.delay > 0 else random.randint(MIX_DELAY_MIN, MIX_DELAY_MAX)

    entry = {
        "id": msg_id,
        "payload": msg.payload,
        "source": msg.source,
        "target": msg.target,
        "added": time.time(),
        "release_at": time.time() + delay,
        "delay": delay,
        "ttl": msg.ttl,
        "shuffle_rounds": random.randint(2, 5),
    }
    mix_pool.append(entry)
    stats["mixed"] += 1
    logger.debug(f"Mix add {msg_id}: delay={delay}s")
    return msg_id


def _mix_process():
    """Процессор микса — каждые MIX_INTERVAL секунд."""
    while True:
        time.sleep(MIX_INTERVAL)
        now = time.time()

        # Собираем созревшие сообщения
        ready = [m for m in mix_pool if now >= m["release_at"]]
        if not ready:
            continue

        # Перемешиваем (shuffle rounds)
        for _ in range(ready[0].get("shuffle_rounds", 3)):
            random.shuffle(ready)

        # Перемешиваем с cover traffic (подмешиваем фиктивные)
        cover_count = max(1, int(len(ready) * NOISE_FACTOR))
        for _ in range(cover_count):
            ready.append({
                "id": f"cover_{uuid.uuid4().hex[:8]}",
                "payload": secrets.token_hex(16),
                "source": "cover",
                "target": "",
                "is_cover": True,
            })

        # Отправляем
        for msg in ready:
            mix_pool.remove(msg)
            msg["sent_at"] = now
            msg["delay_actual"] = round(now - msg["added"], 1)
            mix_history.append(msg)

        logger.info(f"Mix flushed: {len(ready)} msgs ({(cover_count if ready else 0)} cover)")


# ══════════════════════════════════════════════════════════════
# 2. DANDELION++ — Stem → Flock
# ══════════════════════════════════════════════════════════════

def _dandelion_send(msg: DandelionMessage) -> dict:
    """
    Dandelion++:
    — Stem: сообщение идёт по цепочке из N узлов
    — Flock: последний узел broadcast'ит
    """
    msg_id = uuid.uuid4().hex[:12]
    stem_len = msg.stem_len if msg.stem_len > 0 else random.randint(*DANDELION_STEM_LEN)

    # Строим случайный маршрут
    nodes = [f"node_{random.randint(1000, 9999)}" for _ in range(stem_len)]
    dandelion_routes[msg_id] = {
        "route": nodes,
        "current_hop": 0,
        "payload": msg.payload,
        "source": msg.source,
        "created": time.time(),
        "phase": "stem",
    }

    # Симуляция stem: каждый хоп задерживает на ~1-3 сек
    for i, node in enumerate(nodes):
        hop_delay = random.uniform(0.5, 3.0)
        time.sleep(hop_delay / 10)  # ускорено для тестов
        # На последнем хопе — flock (broadcast)
        if i == len(nodes) - 1:
            dandelion_routes[msg_id]["phase"] = "flock"
            dandelion_routes[msg_id]["flock_at"] = time.time()

    stats["dandelion_sent"] += 1

    return {
        "msg_id": msg_id,
        "stem_len": stem_len,
        "route": nodes,
        "phase": "flock",
        "flock_time_ms": round((time.time() - dandelion_routes[msg_id]["created"]) * 1000),
    }


# ══════════════════════════════════════════════════════════════
# 3. COINJOIN — смешивание L4 платежей
# ══════════════════════════════════════════════════════════════

def coinjoin_add(txns: list[dict], pool_id: str = "") -> dict:
    """
    CoinJoin: смешивание платежей в один пул.
    Каждый участник вносит одинаковую сумму, получает анонимные выходы.
    """
    pid = pool_id or f"cj_{uuid.uuid4().hex[:10]}"

    if pid not in coinjoin_pools:
        coinjoin_pools[pid] = []

    for tx in txns:
        tx["added_at"] = time.time()
        tx["anon_id"] = secrets.token_hex(8)
        coinjoin_pools[pid].append(tx)

    # Если набрался минимум — "мержим" (в реальности: aggregated transaction)
    merged = None
    if len(coinjoin_pools[pid]) >= COINJOIN_MIN_TXS:
        total_amount = sum(t.get("amount", 0) for t in coinjoin_pools[pid])
        n_txs = len(coinjoin_pools[pid])

        merged = {
            "pool_id": pid,
            "total_amount": total_amount,
            "n_txs": n_txs,
            "outputs": f"anon_{secrets.token_hex(16)}",
            "merged_at": time.time(),
            "anonymity_set": n_txs,
            "participants": [t.get("sender", "?") for t in coinjoin_pools[pid]],
        }
        stats["coinjoin_merged"] += 1
        # Очищаем пул
        coinjoin_pools[pid] = []
        logger.info(f"CoinJoin merged: {n_txs} txns → {total_amount:.2f} anon")

    return {
        "pool_id": pid,
        "size": len(coinjoin_pools[pid]),
        "needed": max(0, COINJOIN_MIN_TXS - len(coinjoin_pools[pid])),
        "merged": merged,
    }


# ══════════════════════════════════════════════════════════════
# 4. COVER TRAFFIC — фиктивные сообщения
# ══════════════════════════════════════════════════════════════

def _cover_worker(interval: int = COVER_INTERVAL):
    """Генератор cover traffic — каждые N секунд."""
    while True:
        time.sleep(interval)

        # Простая обфускация: фиктивное сообщение
        cover = {
            "id": f"cover_{uuid.uuid4().hex[:12]}",
            "payload": secrets.token_hex(32),
            "fake_source": f"anon_{secrets.token_hex(6)}",
            "fake_target": f"anon_{secrets.token_hex(6)}",
            "created": time.time(),
            "size_bytes": random.randint(64, 1024),
        }
        cover_jobs.append(cover)
        stats["cover_sent"] += 1

        # Ограничиваем историю
        while len(cover_jobs) > 500:
            cover_jobs.pop(0)


# ══════════════════════════════════════════════════════════════
# 5. NOISE INJECTION — подмешивание эфемерных ключей
# ══════════════════════════════════════════════════════════════

def generate_noise_key() -> str:
    """Генерация одноразового эфемерного ключа."""
    key = f"noise_{secrets.token_hex(16)}"
    noise_keys.add(key)
    return key

def consume_noise_key(key: str) -> bool:
    """Использовать (сжечь) эфемерный ключ."""
    if key in noise_keys:
        noise_keys.remove(key)
        return True
    return False


# ══════════════════════════════════════════════════════════════
# 6. PRIVACY SCORE — оценка анонимности
# ══════════════════════════════════════════════════════════════

def privacy_assessment(pool_size: int = 1, cover_ratio: float = 0.3,
                       dandelion_stem: int = 3) -> dict:
    """
    Оценка уровня анонимности по шкале 0-100.
    """
    score = 0
    details = []

    # Anonymity set (mix pool)
    if pool_size >= 10:
        score += 30
        details.append(f"pool_size={pool_size}: +30")
    elif pool_size >= 5:
        score += 20
        details.append(f"pool_size={pool_size}: +20")
    elif pool_size >= 2:
        score += 10
        details.append(f"pool_size={pool_size}: +10")

    # Cover traffic
    if cover_ratio >= 0.5:
        score += 25
        details.append(f"cover={cover_ratio}: +25")
    elif cover_ratio >= 0.3:
        score += 15
        details.append(f"cover={cover_ratio}: +15")
    elif cover_ratio >= 0.1:
        score += 5
        details.append(f"cover={cover_ratio}: +5")

    # Dandelion stem
    if dandelion_stem >= 5:
        score += 25
        details.append(f"stem={dandelion_stem}: +25")
    elif dandelion_stem >= 3:
        score += 15
        details.append(f"stem={dandelion_stem}: +15")
    elif dandelion_stem >= 1:
        score += 5
        details.append(f"stem={dandelion_stem}: +5")

    # CoinJoin
    if coinjoin_pools:
        total_waiting = sum(len(v) for v in coinjoin_pools.values())
        if total_waiting >= COINJOIN_MIN_TXS:
            score += 20
            details.append(f"coinjoin_pending={total_waiting}: +20")
        elif total_waiting > 0:
            score += 10
            details.append(f"coinjoin_pending={total_waiting}: +10")

    level = "A" if score >= 80 else "B" if score >= 60 else "C" if score >= 40 else "D"

    return {
        "score": score,
        "level": level,
        "details": details,
        "indicators": {
            "anonymity_set": pool_size,
            "cover_traffic": cover_ratio,
            "dandelion_hops": dandelion_stem,
        },
        "recommendation": "low latency" if score < 40 else "medium" if score < 70 else "high privacy",
    }


# ══════════════════════════════════════════════════════════════
# API ENDPOINTS
# ══════════════════════════════════════════════════════════════

@api.get("/")
def root():
    return {
        "service": "SNIN L4.5 Privacy Layer",
        "version": "1.0.0",
        "mix_pool": len(mix_pool),
        "mix_history": len(mix_history),
        "dandelion_routes": len(dandelion_routes),
        "coinjoin_pools": {k: len(v) for k, v in coinjoin_pools.items()},
        "cover_jobs": len(cover_jobs),
        "noise_keys": len(noise_keys),
        "stats": stats,
        "status": "live",
    }

@api.get("/health")
def health():
    return {
        "privacy": "ok",
        "ts": time.time(),
        "mix_pool_size": len(mix_pool),
        "capabilities": [
            "mixnet",
            "dandelion_plus_plus",
            "coinjoin",
            "cover_traffic",
            "noise_injection",
            "privacy_score",
        ],
    }

# ─── 1. Mixnet ───

@api.post("/mix/add")
def mix_add_endpoint(msg: MixMessage):
    """Добавить сообщение в mixnet."""
    msg_id = mix_add(msg)
    return {"msg_id": msg_id, "delay": "auto (10-60s)", "pool_size": len(mix_pool)}

@api.get("/mix/pool")
def mix_pool_status():
    """Статус пула микса."""
    return {
        "pool_size": len(mix_pool),
        "waiting": max(0, len([m for m in mix_pool if m["release_at"] > time.time()])),
        "ready": len([m for m in mix_pool if m["release_at"] <= time.time()]),
    }

@api.get("/mix/history")
def mix_history_list(limit: int = 20):
    """История отправленных."""
    return {"history": list(mix_history)[-limit:], "total": len(mix_history)}

# ─── 2. Dandelion++ ───

@api.post("/dandelion/send")
def dandelion_send(msg: DandelionMessage):
    """Отправка через Dandelion++ (Stem → Flock)."""
    result = _dandelion_send(msg)
    return result

@api.get("/dandelion/routes")
def dandelion_routes_list():
    """Активные Dandelion маршруты."""
    return {
        "routes": [
            {"id": rid, "phase": r.get("phase"), "hops": len(r.get("route", [])),
             "age_sec": round(time.time() - r["created"], 1)}
            for rid, r in list(dandelion_routes.items())[:50]
        ],
        "total": len(dandelion_routes),
    }

# ─── 3. CoinJoin ───

@api.post("/coinjoin/add")
def coinjoin_add_endpoint(req: CoinJoinRequest):
    """Добавить транзакции в CoinJoin пул."""
    result = coinjoin_add(req.transactions, req.pool_id)
    return result

@api.get("/coinjoin/pools")
def coinjoin_pools_list():
    """Список CoinJoin пулов."""
    return {
        "pools": [
            {"pool_id": pid, "size": len(txs),
             "oldest_sec": round(time.time() - min(t["added_at"] for t in txs), 1) if txs else 0}
            for pid, txs in coinjoin_pools.items()
        ],
        "min_for_merge": COINJOIN_MIN_TXS,
    }

# ─── 4. Cover Traffic ───

@api.post("/cover/configure")
def cover_configure(cfg: CoverConfig):
    """Настройка cover traffic."""
    global COVER_INTERVAL, NOISE_FACTOR
    COVER_INTERVAL = cfg.interval
    NOISE_FACTOR = cfg.noise_factor
    return {
        "status": "updated",
        "interval": COVER_INTERVAL,
        "noise_factor": NOISE_FACTOR,
    }

@api.get("/cover/status")
def cover_status():
    """Статус cover traffic."""
    return {
        "enabled": True,
        "interval": COVER_INTERVAL,
        "noise_factor": NOISE_FACTOR,
        "total_sent": stats["cover_sent"],
        "active_covers": len(cover_jobs),
    }

# ─── 5. Noise Keys ───

@api.post("/noise/generate")
def noise_generate(count: int = 1):
    """Генерация эфемерных ключей."""
    keys = [generate_noise_key() for _ in range(count)]
    return {"keys": keys, "total": len(noise_keys)}

@api.post("/noise/consume")
def noise_consume(key: str):
    """Использовать эфемерный ключ (одноразовый)."""
    ok = consume_noise_key(key)
    return {"consumed": ok}

@api.get("/noise/status")
def noise_status():
    """Статус noise keys."""
    return {"total": len(noise_keys), "max_capacity": 10000}

# ─── 6. Privacy Score ───

@api.get("/privacy-score")
def privacy_score():
    """Оценка анонимности сети."""
    pool_size = len(mix_pool) + len(mix_history)
    cover_ratio = NOISE_FACTOR
    dandelion_stem = sum(len(r.get("route", [])) for r in dandelion_routes.values()) // max(len(dandelion_routes), 1)
    return privacy_assessment(pool_size, cover_ratio, min(dandelion_stem, 10))

# ─── 7. Integration: L4 Payment anonymizer ───

@api.post("/integration/anonymize-payment")
def anonymize_payment(req: AnonymizePaymentRequest):
    """
    Анонимизация L4 платежа:
    1. Добавляем в CoinJoin пул
    2. Обёртка через эфемерный ключ
    3. Часть покрывается cover traffic
    """
    # Смешиваем с другими
    anon_key = generate_noise_key()
    tx = {"sender": req.sender, "amount": req.amount, "target": req.target, "anon_key": anon_key}

    result = coinjoin_add([tx])
    return {
        "original_tx": {"sender": req.sender, "amount": req.amount, "target": req.target},
        "anonymized_as": anon_key,
        "coinjoin_status": result,
        "privacy_level": privacy_assessment(
            len(mix_pool) + 1, NOISE_FACTOR,
            random.randint(2, 5)
        ),
    }

# ─── 8. Integration: L2 Transport anonymizer ───

@api.post("/integration/anonymize-message")
def anonymize_message(req: AnonymizeMessageRequest):
    """
    Анонимизация L2 сообщения:
    method: mixnet | dandelion | both
    """
    results = {}
    msg_id = ""

    if req.method in ("mixnet", "both"):
        msg = MixMessage(payload=req.content, source="_anon")
        msg_id = mix_add(msg)
        results["mixnet"] = {"msg_id": msg_id, "pool_size": len(mix_pool)}

    if req.method in ("dandelion", "both"):
        dandelion = DandelionMessage(payload=req.content, source="_anon")
        dandelion_result = _dandelion_send(dandelion)
        results["dandelion"] = dandelion_result

    return {
        "method": req.method,
        "results": results,
        "anonymized": True,
        "trace_removed": True,
        "privacy_score": privacy_assessment(
            len(mix_pool) + 1, NOISE_FACTOR, 3
        ),
    }


# ─── Init ───
threading.Thread(target=_mix_process, daemon=True).start()
threading.Thread(target=_cover_worker, daemon=True).start()

logger.info(f"Privacy Layer initialized: mix={MIX_INTERVAL}s, cover={COVER_INTERVAL}s")

app.include_router(api)

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9700
    print(f"[PRIV] Starting L4.5 Privacy Layer on :{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")

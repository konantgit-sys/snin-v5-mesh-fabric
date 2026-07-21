#!/usr/bin/env python3
"""
Reputation Gate — интеграция reputation.py в relay_server_v2.

Заменяет статический WHITELIST на динамическую репутационную модель:
- WHITELIST остаётся как "bootstrap trust" (всегда trusted)
- Остальные агенты оцениваются по reputation score
- Score ≥ REP_THRESHOLD → полный доступ (whitelist-equivalent)
- Score ≥ REP_MIN_READ → чтение + PUBLIC_WRITE_KINDS
- Score < REP_MIN_READ → read-only (или denied для writes)

Reputation считает reputation.py (226 lines) из:
1. Надёжность доставки (delivery success rate из accounting.db)
2. Вклад (количество постов, комментариев, NIPs)
3. Возраст (дней с первой активности)
4. Аттестации (количество VC attestations)
"""

import json
import time
import sqlite3
import logging
from pathlib import Path

logger = logging.getLogger("snin.reputation_gate")

# ═══ Thresholds ═══
REP_THRESHOLD = 0.5        # выше — полный whitelist-доступ
REP_MIN_READ = 0.2         # выше — чтение + базовые kinds
REP_MIN_WRITE = 0.3        # выше — чтение + публичные kinds

# ═══ Cache ═══
_cache: dict[str, dict] = {}
_cache_ttl = 300  # 5 минут
_cache_file = Path.home() / "data" / "sites" / "relay" / "data" / "reputation_cache.json"

# ═══ Reputation sources ═══
RELAY_DB = Path.home() / "data" / "sites" / "relay" / "relay_v2.db"
ACCOUNTING_DB = Path.home() / "data" / "sites" / "relay-mesh" / "accounting.db"
ACK_STATE = Path.home() / "data" / "sites" / "relay-mesh" / "data" / "ack_state.json"


def load_reputation_scores() -> dict[str, float]:
    """Загрузить reputation из reputation.py (если доступен) + собственные данные."""
    scores: dict[str, float] = {}

    # 1. Попробовать reputation.py
    try:
        from reputation import calculate_reputation
        # reputation.py использует accounting.db и identities/
        # Пробуем получить для всех известных pubkey
        from reputation import _get_agent_pubkeys as get_keys
        pubkeys = get_keys()
        for name, pk in pubkeys.items():
            rep = calculate_reputation(name)
            if rep and rep.get("score"):
                scores[pk] = rep["score"]
    except ImportError:
        logger.info("reputation.py not available — using fallback metrics")
    except Exception as e:
        logger.warning(f"reputation.py error: {e}")

    # 2. Accounting DB — delivery reliability
    try:
        conn = sqlite3.connect(f"file:{ACCOUNTING_DB}?mode=ro", uri=True)
        cur = conn.execute("""
            SELECT pubkey, 
                   SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END) as success,
                   COUNT(*) as total
            FROM deliveries 
            GROUP BY pubkey
        """)
        for row in cur:
            pk = row[0]
            success_rate = row[1] / max(row[2], 1)
            if pk not in scores:
                scores[pk] = 0.0
            scores[pk] += 0.4 * success_rate  # reliability weight
        conn.close()
    except Exception as e:
        logger.debug(f"Accounting DB not available: {e}")

    # 3. ACK Tracker — delivery stats
    try:
        if ACK_STATE.exists():
            data = json.loads(ACK_STATE.read_text())
            by_channel = data.get("stats", {}).get("by_channel", {})
            # Aggregate delivery rates across channels
            total_ok = sum(c.get("acked", 0) for c in by_channel.values())
            total_all = sum(c.get("sent", 0) for c in by_channel.values())
            if total_all > 0:
                ack_rate = total_ok / total_all
                # Distribute to all known agents
                for pk in scores:
                    scores[pk] = min(scores.get(pk, 0) + 0.1 * ack_rate, 1.0)
    except Exception as e:
        logger.debug(f"ACK stats not available: {e}")

    # 4. Relay DB — agent age + post count
    try:
        conn = sqlite3.connect(f"file:{RELAY_DB}?mode=ro", uri=True)
        now = time.time()
        cur = conn.execute("SELECT pubkey, registered_at FROM agents")
        for row in cur:
            pk = row[0]
            age_days = max(1, (now - (row[1] or 0)) / 86400)
            age_score = min(age_days / 180, 1.0)  # 0 → 1.0 over 6 months
            if pk not in scores:
                scores[pk] = 0.0
            scores[pk] += 0.15 * age_score  # age weight (partial)
        
        # Post count as contribution proxy
        cur = conn.execute("""
            SELECT pubkey, COUNT(*) as cnt 
            FROM events 
            WHERE kind IN (1, 30023, 30024) 
            GROUP BY pubkey
        """)
        for row in cur:
            pk = row[0]
            contrib_score = min(row[1] / 100, 1.0)  # 100 posts = max
            if pk not in scores:
                scores[pk] = 0.0
            scores[pk] += 0.2 * contrib_score
        conn.close()
    except Exception as e:
        logger.debug(f"Relay DB not available: {e}")

    return scores


def get_reputation(pubkey: str, force_refresh: bool = False) -> dict:
    """Получить reputation для pubkey."""
    now = time.time()

    # Check cache
    if not force_refresh and pubkey in _cache:
        entry = _cache[pubkey]
        if now - entry["ts"] < _cache_ttl:
            return entry

    # Load all scores
    try:
        scores = load_reputation_scores()
    except Exception as e:
        logger.error(f"Failed to load reputation scores: {e}")
        scores = {}

    score = scores.get(pubkey, 0.0)

    # Determine access level
    if score >= REP_THRESHOLD:
        access = "full"       # whitelist-equivalent
    elif score >= REP_MIN_WRITE:
        access = "write"      # can write public kinds
    elif score >= REP_MIN_READ:
        access = "read"       # read-only, no writes
    else:
        access = "none"       # denied

    entry = {
        "pubkey": pubkey,
        "score": round(score, 4),
        "access": access,
        "threshold_full": REP_THRESHOLD,
        "threshold_write": REP_MIN_WRITE,
        "threshold_read": REP_MIN_READ,
        "ts": now,
    }

    _cache[pubkey] = entry
    return entry


def is_reputation_trusted(pubkey: str, min_access: str = "full") -> bool:
    """Проверить, имеет ли pubkey reputation ≥ min_access."""
    rep = get_reputation(pubkey)
    access_levels = {"none": 0, "read": 1, "write": 2, "full": 3}
    return access_levels.get(rep["access"], 0) >= access_levels.get(min_access, 0)


def save_cache():
    """Сохранить кеш репутации."""
    try:
        _cache_file.parent.mkdir(parents=True, exist_ok=True)
        _cache_file.write_text(json.dumps(_cache, indent=2))
    except Exception:
        pass


def load_cache():
    """Загрузить кеш репутации."""
    global _cache
    try:
        if _cache_file.exists():
            data = json.loads(_cache_file.read_text())
            now = time.time()
            _cache = {
                k: v for k, v in data.items()
                if now - v.get("ts", 0) < _cache_ttl
            }
    except Exception:
        _cache = {}


# Load cache on import
load_cache()


def get_reputation_stats() -> dict:
    """Статистика репутационной системы."""
    scores = load_reputation_scores()
    if not scores:
        return {"agents": 0, "avg_score": 0, "by_access": {}}

    by_access = {"full": 0, "write": 0, "read": 0, "none": 0}
    for pk, score in scores.items():
        if score >= REP_THRESHOLD:
            by_access["full"] += 1
        elif score >= REP_MIN_WRITE:
            by_access["write"] += 1
        elif score >= REP_MIN_READ:
            by_access["read"] += 1
        else:
            by_access["none"] += 1

    return {
        "agents": len(scores),
        "avg_score": round(sum(scores.values()) / max(len(scores), 1), 4),
        "by_access": by_access,
        "thresholds": {
            "full": REP_THRESHOLD,
            "write": REP_MIN_WRITE,
            "read": REP_MIN_READ,
        },
    }

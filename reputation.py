#!/usr/bin/env python3
"""Reputation Score — децентрализованная репутация агентов SNIN.

Каждый агент имеет reputation score, который влияет на:
- Приоритет доставки gossip
- Вес в голосовании DAO
- Доверие к маршрутизации

Источники данных:
1. accounting.db — платежи, чеки, балансы
2. bridge log — количество доставленных сообщений
3. gossip log — участие в gossip, ошибки
4. Аттестации от SmartRouter

Репутация считается как weighted комбинация:
  R = 0.4 × reliability + 0.3 × contribution + 0.2 × age + 0.1 × attestations
"""

import json
import os
import sqlite3
import time
from pathlib import Path

BASE_DIR = Path.home() / "data" / "sites" / "relay-mesh"
ACCOUNTING_DB = BASE_DIR / "accounting.db"
IDENTITIES_DIR = BASE_DIR / "identities"


def _get_agent_pubkeys() -> dict[str, str]:
    """Получить mapping: agent_name → mesh_pubkey из identity файлов."""
    agents = {}
    for fpath in sorted(IDENTITIES_DIR.glob("*.json")):
        if "attestations" in str(fpath):
            continue
        try:
            with open(fpath) as f:
                data = json.load(f)
            agents[data.get("agent_name", fpath.stem)] = data.get("mesh_pubkey", "")
        except (json.JSONDecodeError, FileNotFoundError):
            pass
    return agents


def _count_bridge_deliveries(agent_name: str) -> int:
    """Посчитать доставленные сообщения для агента через bridge."""
    log_file = BASE_DIR / "logs" / "nostr_bridge.log"
    if not log_file.exists():
        return 0
    try:
        with open(log_file) as f:
            content = f.read()
        # Считаем упоминания агента в логе
        return content.count(f"[{agent_name}]") + content.count(agent_name[:12])
    except (FileNotFoundError, OSError):
        return 0


def _count_gossip_participation(agent_name: str) -> tuple[int, int]:
    """Посчитать участие в gossip: (успешно, ошибок)."""
    log_dir = BASE_DIR / "logs"
    ok, err = 0, 0
    for pattern in ["gossip_0", "gossip_1", "gossip_2", "gossip_3", "gossip_4"]:
        log_file = log_dir / f"{pattern}.log"
        if log_file.exists():
            try:
                with open(log_file) as f:
                    content = f.read()
                ok += content.count(f"✅") + content.count(f"sent") + content.count(agent_name)
                err += content.count(f"❌") + content.count(f"error") + content.count(f"timeout")
            except (FileNotFoundError, OSError):
                pass
    return ok, err


def _count_attestations(agent_name: str) -> int:
    """Посчитать количество аттестаций агента."""
    from mesh_identity import load_or_create_identity, pubkey_to_did
    from mesh_identity import IDENTITIES_DIR
    try:
        identity = load_or_create_identity(agent_name)
        did = pubkey_to_did(identity["mesh_pubkey"])
        attest_file = IDENTITIES_DIR / "attestations" / f"{did.replace(':', '_')}.json"
        if attest_file.exists():
            import json
            with open(attest_file) as f:
                attests = json.load(f)
            return len(attests)
        # fallback — старый формат
        return len(identity.get("attestations", []))
    except Exception:
        return 0


def _agent_age_days(agent_name: str) -> float:
    """Возраст агента в днях."""
    from mesh_identity import load_or_create_identity
    try:
        identity = load_or_create_identity(agent_name)
        created = identity.get("created_at", time.time())
        return (time.time() - created) / 86400
    except Exception:
        return 0


def _payment_volume(agent_name: str) -> int:
    """Количество платежных транзакций агента."""
    if not ACCOUNTING_DB.exists():
        return 0
    try:
        db = sqlite3.connect(str(ACCOUNTING_DB))
        cursor = db.execute(
            "SELECT COUNT(*) FROM payments WHERE sender = ? OR receiver = ?",
            (agent_name, agent_name)
        )
        count = cursor.fetchone()[0]
        db.close()
        return count
    except Exception:
        return 0


def calculate_reputation(agent_name: str) -> dict:
    """Рассчитать репутацию агента.
    
    Returns:
        dict с score (0.0-1.0) и детальными метриками
    """
    # 1. Надёжность (reliability) = ratio успешных операций
    ok, err = _count_gossip_participation(agent_name)
    total = ok + err
    reliability = ok / total if total > 0 else 0.5
    
    # 2. Вклад (contribution) = количество доставленных сообщений
    deliveries = _count_bridge_deliveries(agent_name)
    # Нормализуем: max 1000 сообщений = 1.0
    contribution = min(deliveries / 1000, 1.0)
    
    # 3. Возраст (age) — чем дольше живёт, тем больше доверия
    age_days = _agent_age_days(agent_name)
    age_score = min(age_days / 30, 1.0)  # 30 дней = полная зрелость
    
    # 4. Аттестации (attestations) — количество подписей от верификаторов
    attest_count = _count_attestations(agent_name)
    attest_score = min(attest_count / 3, 1.0)  # 3 аттестации = максимум
    
    # 5. Платежи (payment) — финансовая активность
    payments = _payment_volume(agent_name)
    payment_score = min(payments / 10, 1.0)  # 10 транзакций = максимум
    
    # Weighted score
    score = (
        0.30 * reliability +
        0.25 * contribution +
        0.20 * age_score +
        0.15 * attest_score +
        0.10 * payment_score
    )
    
    return {
        "agent_name": agent_name,
        "score": round(score, 4),
        "reliability": round(reliability, 4),
        "contribution": round(contribution, 4),
        "age_score": round(age_score, 4),
        "attest_score": round(attest_score, 4),
        "payment_score": round(payment_score, 4),
        "details": {
            "deliveries": deliveries,
            "gossip_ok": ok,
            "gossip_err": err,
            "attestations": attest_count,
            "payments": payments,
            "age_days": round(age_days, 1),
        }
    }


def get_all_reputations() -> dict[str, dict]:
    """Получить репутацию всех известных агентов."""
    reputations = {}
    for agent_name in _get_agent_pubkeys():
        reputations[agent_name] = calculate_reputation(agent_name)
    return reputations


def get_reputation_for_pubkey(pubkey: str) -> dict:
    """Получить репутацию по pubkey."""
    agents = _get_agent_pubkeys()
    for name, pk in agents.items():
        if pk == pubkey:
            return calculate_reputation(name)
    return {
        "agent_name": "unknown",
        "score": 0.3,  # Новый агент — базовое доверие
        "reliability": 0.3,
        "contribution": 0.0,
        "age_score": 0.0,
        "attest_score": 0.0,
        "payment_score": 0.0,
        "details": {"deliveries": 0, "gossip_ok": 0, "gossip_err": 0, "attestations": 0, "payments": 0},
    }


# ─── CLI ───
if __name__ == "__main__":
    import sys
    
    print("╔════════════════════════════════════════════╗")
    print("║     SNIN Reputation Scores                ║")
    print("╚════════════════════════════════════════════╝")
    
    if len(sys.argv) > 1:
        rep = calculate_reputation(sys.argv[1])
        print(f"\n  {rep['agent_name']}: score={rep['score']}")
        for k, v in rep.items():
            if k not in ("agent_name", "details"):
                print(f"    {k}: {v}")
        print(f"    details: {rep['details']}")
    else:
        reps = get_all_reputations()
        for name, rep in sorted(reps.items(), key=lambda x: -x[1]["score"]):
            print(f"\n  {name:<20} score={rep['score']:.4f}")
            print(f"    reliability={rep['reliability']:.3f}  contribution={rep['contribution']:.3f}")
            print(f"    age={rep['age_score']:.3f}  attestations={rep['attest_score']:.3f}  payments={rep['payment_score']:.3f}")
            print(f"    details: {rep['details']}")

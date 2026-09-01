"""
SNIN PROOF MESH — Фаза 3: тесты экономики роя (econ.py).

Unit: парсинг 9735/9734/kind 0, декодер bolt11, агрегаты.
Integration (live): если релеи отвечают — sync_econ пишет реальные
zap-события в payment_events (done-when Ф3: не заглушки). Если релеи
недоступны — тест пропускается (не падает).

Спека: SNIN_PROOF_MESH_SPEC.md, Фаза 3.
"""

import sqlite3
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from proof_mesh import db, econ  # noqa: E402


@pytest.fixture
def audit_db(tmp_path):
    path = str(tmp_path / "econ_audit.db")
    db.init_db(path)
    return path


# ── декодер bolt11 ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("bolt11,expected_msat", [
    ("lnbc10m", 10 * 10 ** 8),          # 10 milliBTC = 1e9 msat
    ("lnbc100u", 100 * 10 ** 5),        # 100 microBTC = 1e7 msat
    ("lnbc2500n", 2500 * 10 ** 2),      # 2500 nanoBTC = 2.5e5 msat
    ("lnbc123p", 12),                    # 123 picoBTC ≈ 12.3 → 12 msat (int)
    ("lnbc21", 21 * 10 ** 11),          # 21 BTC = 2.1e12 msat
    ("", 0),
    ("lntb10m", 0),                     # testnet — не поддерживаем
])
def test_decode_bolt11(bolt11, expected_msat):
    assert econ.decode_bolt11_msat(bolt11) == expected_msat


# ── парсинг событий ─────────────────────────────────────────────────────────

def _zap_evt(**over):
    ev = {
        "id": "zap1", "pubkey": "zapper_pub", "created_at": 1700000000,
        "kind": 9735,
        "tags": [
            ["p", "cryter_pub"],
            ["zap", "sender_pub"],
            ["bolt11", "lnbc10m"],
        ],
    }
    ev.update(over)
    return ev


def test_parse_zap_receipt():
    p = econ.parse_zap_receipt(_zap_evt())
    assert p is not None
    assert p["sender_pub"] == "sender_pub"
    assert p["receiver_pub"] == "cryter_pub"
    assert p["amount_msat"] == 10 * 10 ** 8
    assert p["event_id"] == "zap1"


def test_parse_zap_receipt_no_amount():
    assert econ.parse_zap_receipt(_zap_evt(tags=[["p", "x"]])) is None


def test_parse_zap_request():
    ev = {"id": "req1", "pubkey": "biller", "created_at": 1700000001,
          "kind": 9734, "tags": [["p", "debtor"], ["amount", "5000000"]]}
    p = econ.parse_zap_request(ev)
    assert p is not None
    assert p["sender_pub"] == "biller"
    assert p["receiver_pub"] == "debtor"
    assert p["amount_msat"] == 5_000_000


def test_parse_zap_request_no_amount():
    ev = {"id": "req2", "pubkey": "b", "created_at": 1, "kind": 9734,
          "tags": [["p", "d"]]}
    assert econ.parse_zap_request(ev) is None


def test_parse_metadata():
    ev = {"pubkey": "pk1", "created_at": 1, "kind": 0,
          "content": '{"name":"x","lud16":"x@getalby.com","lud06":"lnurl1..."}'}
    m = econ.parse_metadata(ev)
    assert m["pubkey"] == "pk1"
    assert m["lud16"] == "x@getalby.com"
    assert m["lud06"] == "lnurl1..."


def test_parse_metadata_no_wallet():
    ev = {"pubkey": "pk2", "created_at": 1, "kind": 0, "content": '{"name":"x"}'}
    assert econ.parse_metadata(ev) is None


# ── store + aggregate ───────────────────────────────────────────────────────

def test_store_and_aggregate(audit_db):
    now = int(time.time())
    zap = econ.parse_zap_receipt(_zap_evt(created_at=now))
    zap2 = econ.parse_zap_receipt(_zap_evt(
        id="zap2", tags=[["p", "archivist_pub"], ["zap", "sender2"],
                         ["bolt11", "lnbc20m"]], created_at=now))
    assert econ.store_payments(audit_db, [zap, zap2, zap], 9735) == 2  # дедуп
    req = econ.parse_zap_request(
        {"id": "req1", "pubkey": "biller", "created_at": now, "kind": 9734,
         "tags": [["p", "debtor"], ["amount", "3000000"]]})
    assert econ.store_payments(audit_db, [req], 9734) == 1

    agg = econ.aggregate(audit_db, days=7)
    assert agg["total_zaps"] == 2
    assert agg["total_msat"] == 10 * 10 ** 8 + 20 * 10 ** 8
    assert agg["invoices_issued"] == 1
    top = agg["top_receivers"][0]
    assert top["pubkey"] == "archivist_pub"
    assert top["amount_msat"] == 20 * 10 ** 8


def test_upsert_wallets(audit_db):
    pr = {"pubkey": "pk1", "lud16": "a@b.com", "lud06": ""}
    assert econ.upsert_wallets(audit_db, [pr]) == 1
    assert econ.upsert_wallets(audit_db, [pr]) == 1  # upsert, не дубль
    with sqlite3.connect(audit_db) as c:
        n = c.execute("SELECT COUNT(*) FROM wallet_profiles").fetchone()[0]
    assert n == 1


# ── live-интеграция (done-when Ф3) ──────────────────────────────────────────

@pytest.mark.integration
def test_live_zaps_from_relays(audit_db, tmp_path):
    """Реальные 9735 с primal/damus → payment_events. Skip, если релеи мертвы."""
    since = int(time.time()) - 7 * 86400
    events = econ.fetch_kinds(econ.RELAYS[:2], [9735], since, limit=50, timeout=8)
    if not events:
        pytest.skip("релеи не ответили — live-тест пропущен (юнит-покрытие выше)")
    pays = [p for p in (econ.parse_zap_receipt(e) for e in events) if p]
    assert pays, "есть 9735, но ни один не распарсился (нет bolt11?)"
    n = econ.store_payments(audit_db, pays, 9735)
    assert n >= 1, "реальные zaps должны лечь в payment_events"
    agg = econ.aggregate(audit_db, days=7)
    assert agg["total_zaps"] >= 1
    assert agg["total_msat"] > 0

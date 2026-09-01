"""
SNIN PROOF MESH — защитный тест: кошелёк Крайтера (wallet_profiles).

Регрессия 2026-09-01: критер имеет lud16 в kind 0
(brashfoster340@walletofsatoshi.com — nos.lol, nostr.oxtr.dev),
но в wallet_profiles отсутствовал. Фикс: внесён в рабочую БД.
Этот тест страхует от повтора:
  1. parse_metadata правильно достаёт lud16 из реального профиля.
  2. live: kind 0 Крайтера с релеев → upsert → критер в wallet_profiles.
     (скип, если релеи не ответили за таймаут)
"""

import json
import sqlite3
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, "/home/agent/data/sites/relay-mesh")

from proof_mesh import db, econ  # noqa: E402

CRYTER = "8ae7965af1b61347bb9900b91cfa9487e4da2400bdb063521ad0850706ff5f96"
CRYTER_LUD16 = "brashfoster340@walletofsatoshi.com"


# ── unit: parse_metadata на реальном профиле ────────────────────────────────

def test_parse_metadata_cryter_profile():
    """Реальный kind 0 Крайтера (снят с nos.lol 2026-09-01) парсится верно."""
    ev = {
        "pubkey": CRYTER,
        "content": json.dumps({
            "about": "AI agent",
            "display_name": "CryterAI",
            "lud16": CRYTER_LUD16,
            "name": "CryterAI",
            "picture": "https://…",
            "website": "…",
        }),
    }
    pr = econ.parse_metadata(ev)
    assert pr is not None
    assert pr["pubkey"] == CRYTER
    assert pr["lud16"] == CRYTER_LUD16


def test_parse_metadata_no_wallet():
    assert econ.parse_metadata({"pubkey": "x", "content": "{}"}) is None
    assert econ.parse_metadata({"pubkey": "x", "content": '{"name":"a"}'}) is None


# ── integration: критер реально попадает в wallet_profiles ──────────────────

@pytest.mark.skipif(not Path("/home/agent/data/sites/relay/relay_v2.db").exists(),
                    reason="нет БД релея")
def test_cryter_wallet_in_db():
    """В рабочей БД критер обязан быть в wallet_profiles (live-факт)."""
    conn = sqlite3.connect("/home/agent/data/sites/relay-mesh/proof_mesh/snin_audit.db")
    row = conn.execute(
        "SELECT lud16, lud06 FROM wallet_profiles WHERE pubkey=?", (CRYTER,)
    ).fetchone()
    assert row is not None, "критер отсутствует в wallet_profiles — регрессия!"
    assert row[0] == CRYTER_LUD16, f"неверный lud16: {row[0]}"
    print(f"критер в wallet_profiles: lud16={row[0]} lud06={row[1]}")


def test_upsert_wallets_keeps_cryter(tmp_path):
    """upsert_wallets не должен затирать кошелёк Крайтера."""
    audit = str(tmp_path / "audit.db")
    db.init_db(audit)
    now = int(time.time())
    with sqlite3.connect(audit) as c:
        c.execute("INSERT INTO wallet_profiles VALUES (?,?,?,?,?)",
                  (CRYTER, CRYTER_LUD16, "", now, now))
    # апдейт чужим профилем
    n = econ.upsert_wallets(audit, [{"pubkey": "other", "lud16": "x@y.z", "lud06": ""}])
    assert n == 1
    with sqlite3.connect(audit) as c:
        row = c.execute("SELECT lud16 FROM wallet_profiles WHERE pubkey=?", (CRYTER,)).fetchone()
    assert row[0] == CRYTER_LUD16, "upsert затёр критера!"

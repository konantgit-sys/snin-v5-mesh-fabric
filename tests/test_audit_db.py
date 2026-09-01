"""
SNIN PROOF MESH — Фаза 0: тесты БД аудита.

Покрытие: init_db, append_event (хэш-цепочка), verify_chain (OK),
tamper-детекция (payload/ts/signature), verify_from (контрольная точка),
chain_state (height/last_hash/root), CRUD agent_instances / payment_events /
wallet_profiles. Спека: SNIN_PROOF_MESH_SPEC.md.
"""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # корень проекта

from proof_mesh import db  # noqa: E402

N = 100  # размер тестовой цепочки


@pytest.fixture
def audit_db(tmp_path):
    path = str(tmp_path / "snin_audit.db")
    db.init_db(path)
    return path


def _fill_chain(path, n=N):
    """Заполнить цепочку n событиями, вернуть последний block_hash."""
    last = None
    for i in range(n):
        last = db.append_event(
            path,
            agent_id=f"agent_{i % 5}",
            instance_id=f"inst_{i % 5}",
            action="test_action",
            payload=f"payload #{i}",
            attribution="confirmed" if i % 2 == 0 else "inferred",
            evidence_code="REG_MATCH" if i % 2 == 0 else "SIG_MATCH",
            signature="sig" if i % 2 == 0 else "",
            ts=1_700_000_000 + i,
        )
    return last


# ── init ────────────────────────────────────────────────────────────────────

def test_init_db_creates_all_tables(audit_db):
    with sqlite3.connect(audit_db) as c:
        tables = {r[0] for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
    assert {"audit_events", "agent_instances", "payment_events",
            "wallet_profiles", "chain_state"} <= tables
    with sqlite3.connect(audit_db) as c:
        assert c.execute("PRAGMA user_version").fetchone()[0] == 1


def test_init_db_idempotent(audit_db):
    db.init_db(audit_db)  # повторный вызов не должен падать
    with sqlite3.connect(audit_db) as c:
        assert c.execute("PRAGMA user_version").fetchone()[0] == 1


# ── hash-chain ──────────────────────────────────────────────────────────────

def test_append_event_returns_hash_and_updates_state(audit_db):
    h = db.append_event(audit_db, agent_id="a1", action="post", payload="x")
    assert len(h) == 64
    assert all(ch in "0123456789abcdef" for ch in h)
    state = db.get_chain_state(audit_db)
    assert state["height"] == 1
    assert state["last_hash"] == h


def test_chain_hashes_are_linked(audit_db):
    _fill_chain(audit_db, n=5)
    with sqlite3.connect(audit_db) as c:
        rows = c.execute(
            "SELECT prev_hash, block_hash FROM audit_events ORDER BY id"
        ).fetchall()
    # prev_hash[i+1] == block_hash[i]
    for i in range(1, len(rows)):
        assert rows[i][0] == rows[i - 1][1], f"разрыв на блоке {i}"
    # первый блок привязан к genesis
    assert rows[0][0] == "0" * 64


def test_verify_chain_ok(audit_db):
    _fill_chain(audit_db, n=N)
    ok, count, broken = db.verify_chain(audit_db)
    assert ok is True
    assert count == N
    assert broken is None


def test_verify_empty_chain_ok(audit_db):
    ok, count, broken = db.verify_chain(audit_db)
    assert ok is True and count == 0 and broken is None


# ── tamper-детекция ─────────────────────────────────────────────────────────

def test_tamper_payload_detected(audit_db):
    _fill_chain(audit_db, n=N)
    with sqlite3.connect(audit_db) as c:
        row = c.execute("SELECT id FROM audit_events WHERE id=50").fetchone()
        c.execute("UPDATE audit_events SET payload='ПОДМЕНЕНО' WHERE id=50")
    ok, count, broken = db.verify_chain(audit_db)
    assert ok is False
    assert broken == 50, f"ожидали обрыв на 50, получили {broken}"


def test_tamper_timestamp_detected(audit_db):
    _fill_chain(audit_db, n=N)
    with sqlite3.connect(audit_db) as c:
        c.execute("UPDATE audit_events SET ts=999 WHERE id=30")
    ok, _, broken = db.verify_chain(audit_db)
    assert ok is False and broken == 30


def test_tamper_signature_detected(audit_db):
    _fill_chain(audit_db, n=N)
    with sqlite3.connect(audit_db) as c:
        c.execute("UPDATE audit_events SET signature='FORGED' WHERE id=70")
    ok, _, broken = db.verify_chain(audit_db)
    assert ok is False and broken == 70


def test_verify_from_checkpoint(audit_db):
    _fill_chain(audit_db, n=N)
    # ломаем блок 90, проверка от 95 должна пройти (не затрагивает битый)
    ok, _, broken = db.verify_from(audit_db, start_id=95)
    assert ok is True and broken is None
    # ломаем блок 10, проверка от 95 по-прежнему OK (битый вне диапазона)
    with sqlite3.connect(audit_db) as c:
        c.execute("UPDATE audit_events SET payload='X' WHERE id=10")
    ok, _, _ = db.verify_from(audit_db, start_id=95)
    assert ok is True


# ── chain_state / root ──────────────────────────────────────────────────────

def test_set_root(audit_db):
    _fill_chain(audit_db, n=10)
    db.set_root(audit_db, "deadbeef" * 8)
    state = db.get_chain_state(audit_db)
    assert state["root"] == "deadbeef" * 8
    assert state["root_ts"] > 0
    # root не ломает высоту
    assert state["height"] == 10


# ── instance identity ───────────────────────────────────────────────────────

def test_instances_upsert(audit_db):
    db.upsert_instance(
        audit_db, instance_id="inst_1", agent_id="cryter",
        pid=1234, start_time=1700000000.0, cgroup="/docker/abc", cmdline="python3 -u main.py",
        ts=1_700_000_100,
    )
    db.upsert_instance(
        audit_db, instance_id="inst_1", agent_id="cryter",
        pid=1234, start_time=1700000000.0, cgroup="/docker/abc", cmdline="python3 -u main.py",
        ts=1_700_000_200,
    )
    inst = db.get_instances(audit_db)
    assert len(inst) == 1, "upsert не должен плодить дубли"
    assert inst[0]["last_seen"] == 1_700_000_200


def test_instances_filter_by_agent(audit_db):
    db.upsert_instance(audit_db, instance_id="i1", agent_id="a", pid=1,
                       start_time=1.0, ts=100)
    db.upsert_instance(audit_db, instance_id="i2", agent_id="b", pid=2,
                       start_time=2.0, ts=200)
    only_a = db.get_instances(audit_db, agent_id="a")
    assert len(only_a) == 1 and only_a[0]["instance_id"] == "i1"


# ── экономика роя ──────────────────────────────────────────────────────────

def test_payments_crud(audit_db):
    pid = db.add_payment(
        audit_db, kind=9735, sender_pub="abc", receiver_pub="def",
        amount_msat=21000, ln_address="a@ln.btc", event_id="evt1", relay="primal",
        ts=1_700_000_000,
    )
    assert pid > 0
    rows = db.get_payments(audit_db, since=0)
    assert len(rows) == 1
    assert rows[0]["amount_msat"] == 21000
    assert rows[0]["ln_address"] == "a@ln.btc"
    # фильтр since
    assert db.get_payments(audit_db, since=1_700_000_001) == []


def test_wallets_upsert(audit_db):
    db.upsert_wallet(audit_db, pubkey="pk1", lud16="a@ln.btc")
    db.upsert_wallet(audit_db, pubkey="pk1", lud16="b@ln.btc", lud06="lnurl1")
    w = db.get_wallets(audit_db)
    assert len(w) == 1
    assert w[0]["lud16"] == "b@ln.btc"
    assert w[0]["lud06"] == "lnurl1"


# ── валидация ──────────────────────────────────────────────────────────────

def test_append_rejects_bad_attribution(audit_db):
    with pytest.raises(ValueError):
        db.append_event(audit_db, action="x", attribution="maybe")


def test_append_rejects_bad_evidence(audit_db):
    with pytest.raises(ValueError):
        db.append_event(audit_db, action="x", evidence_code="GUESS")

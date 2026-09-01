"""
SNIN PROOF MESH — Фаза 2: тесты hash-chain + подписи (chain.py).

Покрытие done-when Ф2: 1000 событий verify OK; тампер любой записи
(payload/ts) → падает с указанием места; подделка подписи → падает;
плюс: контроль pubkey через реестр, root каждые 100 событий/10 мин,
интеграция с proof_registry.db (chain_roots), миграция v1→v2,
реальная подпись ключом cryter (если keystore доступен).

Спека: SNIN_PROOF_MESH_SPEC.md, Фаза 2.
"""

import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # корень проекта

from proof_mesh import chain, db  # noqa: E402

N = 1000  # размер тестовой цепочки (done-when)


@pytest.fixture(scope="session")
def nsec():
    from nostr_sdk import Keys
    return Keys.generate().secret_key().to_bech32()


@pytest.fixture(scope="session")
def pubkey(nsec):
    from nostr_sdk import Keys
    return Keys.parse(nsec).public_key().to_hex()


@pytest.fixture
def audit_db(tmp_path):
    path = str(tmp_path / "snin_audit.db")
    db.init_db(path)
    return path


@pytest.fixture
def proof_db(tmp_path):
    return str(tmp_path / "proof_registry.db")


def _fill(audit_db, nsec, pubkey, n=N, proof_db=None):
    """Заполнить цепочку n подписанными событиями."""
    for i in range(n):
        chain.append_signed(
            audit_db, nsec=nsec, agent_id="cryter", action="test",
            payload=f"payload #{i}", ts=1_700_000_000 + i,
            proof_registry=proof_db or "/tmp/nonexistent_proof.db",
        )
    return pubkey


# ── 1000 событий ────────────────────────────────────────────────────────────

def test_1000_events_verify_ok(audit_db, nsec, pubkey):
    _fill(audit_db, nsec, pubkey, n=N)
    ok, count, reason, broken = chain.verify_chain_signed(
        audit_db, registry={"cryter": pubkey}
    )
    assert ok is True
    assert count == N
    assert reason is None and broken is None


# ── тампер ──────────────────────────────────────────────────────────────────

def test_tamper_payload_detected(audit_db, nsec, pubkey):
    _fill(audit_db, nsec, pubkey, n=300)
    with sqlite3.connect(audit_db) as c:
        c.execute("UPDATE audit_events SET payload='ПОДМЕНЕНО' WHERE id=150")
    ok, _, reason, broken = chain.verify_chain_signed(audit_db)
    assert ok is False
    assert reason == "hash"
    assert broken == 150, f"ожидали обрыв на 150, получили {broken}"


def test_tamper_timestamp_detected(audit_db, nsec, pubkey):
    _fill(audit_db, nsec, pubkey, n=300)
    with sqlite3.connect(audit_db) as c:
        c.execute("UPDATE audit_events SET ts=1234567890 WHERE id=77")
    ok, _, reason, broken = chain.verify_chain_signed(audit_db)
    assert ok is False and reason == "hash" and broken == 77


# ── подделка подписи ────────────────────────────────────────────────────────

def test_forged_signature_detected(audit_db, nsec, pubkey):
    _fill(audit_db, nsec, pubkey, n=300)
    with sqlite3.connect(audit_db) as c:
        # подделываем подпись 200-го блока И пересчитываем его block_hash
        row = c.execute(
            "SELECT id, ts, payload, prev_hash FROM audit_events WHERE id=200"
        ).fetchone()
        import hashlib
        content = hashlib.sha256(row[2].encode()).hexdigest()
        unsigned = hashlib.sha256(f"{row[3]}|{content}|{row[1]}".encode()).hexdigest()
        forged_sig = "ab" * 64
        forged_hash = hashlib.sha256(f"{unsigned}|{forged_sig}".encode()).hexdigest()
        c.execute(
            "UPDATE audit_events SET signature=?, block_hash=? WHERE id=200",
            (forged_sig, forged_hash),
        )
    ok, _, reason, broken = chain.verify_chain_signed(audit_db)
    assert ok is False
    assert reason == "sig", f"ожидали sig, получили {reason}"
    assert broken == 200


def test_wrong_signer_pubkey_detected(audit_db, nsec, pubkey):
    """Валидная подпись новым ключом + agent_id='cryter' → реестр ловит."""
    import hashlib
    from nostr_sdk import Keys
    other_nsec = Keys.generate().secret_key().to_bech32()
    other_pub = Keys.parse(other_nsec).public_key().to_hex()
    chain.append_signed(
        audit_db, nsec=nsec, agent_id="cryter", action="t",
        payload="x", ts=1_700_000_000,
    )
    # полная подделка блока 1: новая подпись другим ключом + пересчёт
    with sqlite3.connect(audit_db) as c:
        row = c.execute(
            "SELECT ts, payload, prev_hash FROM audit_events WHERE id=1"
        ).fetchone()
    content = hashlib.sha256(row[1].encode()).hexdigest()
    unsigned = hashlib.sha256(f"{row[2]}|{content}|{row[0]}".encode()).hexdigest()
    forged_sig = chain.sign_block(other_nsec, unsigned)
    forged_hash = hashlib.sha256(f"{unsigned}|{forged_sig}".encode()).hexdigest()
    with sqlite3.connect(audit_db) as c:
        c.execute(
            "UPDATE audit_events SET agent_id='cryter', signer_pub=?, "
            "signature=?, block_hash=? WHERE id=1",
            (other_pub, forged_sig, forged_hash),
        )
    ok, _, reason, broken = chain.verify_chain_signed(
        audit_db, registry={"cryter": pubkey}
    )
    # подпись валидна для чужого pubkey, но cryter в реестре — другой ключ
    assert ok is False and reason == "pubkey" and broken == 1


# ── root ────────────────────────────────────────────────────────────────────

def test_root_every_100_events(audit_db, nsec, pubkey, proof_db):
    _fill(audit_db, nsec, pubkey, n=100, proof_db=proof_db)
    state = db.get_chain_state(audit_db)
    assert state["height"] == 100
    assert state["root"] == state["last_hash"], "root должен быть на 100-м блоке"
    roots = chain.get_roots(proof_db)
    assert len(roots) >= 1
    last = roots[0]
    assert last["height"] == 100
    assert last["root"] == state["root"]
    assert last["pubkey"] == pubkey
    assert len(last["sig"]) == 128


def test_root_not_before_100(audit_db, nsec, pubkey, proof_db):
    _fill(audit_db, nsec, pubkey, n=50, proof_db=proof_db)
    state = db.get_chain_state(audit_db)
    assert state["root"] == "", "до 100 событий root не ставим"
    assert chain.get_roots(proof_db) == []


def test_root_time_based(audit_db, nsec, pubkey, proof_db):
    # первый root — по 100 событиям
    _fill(audit_db, nsec, pubkey, n=100, proof_db=proof_db)
    state = db.get_chain_state(audit_db)
    assert state["root"] != ""
    # «состарим» root_ts → следующий append даёт root по времени
    import time as _t
    with sqlite3.connect(audit_db) as c:
        c.execute("UPDATE chain_state SET root_ts=? WHERE chain_id='main'",
                  (_t.time() - 1000,))
    chain.append_signed(
        audit_db, nsec=nsec, agent_id="cryter", action="a",
        ts=1_700_000_100, proof_registry=proof_db,
    )
    state2 = db.get_chain_state(audit_db)
    assert state2["height"] == 101
    roots = chain.get_roots(proof_db)
    assert len(roots) == 2
    assert roots[0]["height"] == 101  # новый корень по времени


# ── миграция v1 → v2 ────────────────────────────────────────────────────────

def test_migration_v1_to_v2(tmp_path):
    """БД, созданная схемой Ф0 (v1), после init_db получает signer_pub."""
    path = str(tmp_path / "old_audit.db")
    # имитируем v1: без signer_pub
    with sqlite3.connect(path) as c:
        c.executescript(
            """CREATE TABLE audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL, agent_id TEXT NOT NULL DEFAULT '',
                instance_id TEXT NOT NULL DEFAULT '', action TEXT NOT NULL,
                payload TEXT NOT NULL DEFAULT '', payload_hash TEXT NOT NULL,
                prev_hash TEXT NOT NULL, block_hash TEXT NOT NULL,
                signature TEXT NOT NULL DEFAULT '',
                attribution TEXT NOT NULL DEFAULT 'unattributed',
                evidence_code TEXT NOT NULL DEFAULT 'UNKNOWN',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX idx_audit_agent ON audit_events(agent_id);
            CREATE INDEX idx_audit_ts ON audit_events(ts);
            PRAGMA user_version = 1;"""
        )
    db.init_db(path)  # миграция
    with sqlite3.connect(path) as c:
        ver = c.execute("PRAGMA user_version").fetchone()[0]
        cols = [r[1] for r in c.execute("PRAGMA table_info(audit_events)")]
    assert ver == 2
    assert "signer_pub" in cols


# ── реальный ключ cryter ────────────────────────────────────────────────────

def test_real_cryter_key(audit_db):
    """Полный контур: реальный nsec cryter (keystore) → verify по реестру."""
    ks = Path("/home/agent/data/sites/chrono/keystore/agent_keys.json.old")
    if not ks.exists():
        pytest.skip("keystore недоступен")
    data = json.loads(ks.read_text())
    nsec_cryter = data.get("cryter", {}).get("nsec")
    if not nsec_cryter:
        pytest.skip("nsec cryter не найден в keystore")
    pubkeys = chain.load_pubkeys()
    real_pub = pubkeys.get("cryter")
    assert real_pub, "cryter должен быть в chrono.agent_registry"

    chain.append_signed(
        audit_db, nsec=nsec_cryter, agent_id="cryter", action="post",
        payload="подпись реальным ключом cryter", ts=1_700_000_000,
    )
    ok, count, reason, broken = chain.verify_chain_signed(
        audit_db, registry={"cryter": real_pub}
    )
    assert ok is True, f"реальная подпись не верифицировалась: {reason}@{broken}"
    assert count == 1

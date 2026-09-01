"""
SNIN PROOF MESH — Фаза 6: тесты демона аудита (test_audit_daemon.py).

Юнит: should_publish (порог 50 блоков / 600 с), state persist,
полный цикл с фейковым relay_db (события → цепочка), идемпотентность
второго прохода, устойчивость при пустых источниках.

Спека: SNIN_PROOF_MESH_SPEC.md, Фаза 6.
"""

import json
import sqlite3
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, "/home/agent/data/sites/relay-mesh")

from proof_mesh import audit_daemon, chain, db, mesh_int  # noqa: E402


@pytest.fixture(scope="session")
def nsec():
    from nostr_sdk import Keys
    return Keys.generate().secret_key().to_bech32()


@pytest.fixture
def audit_db(tmp_path):
    path = str(tmp_path / "audit.db")
    db.init_db(path)
    return path


def _make_relay_db(tmp_path, n_events=2):
    """Фейковый relay_v2.db с событиями kinds 1/7."""
    path = str(tmp_path / "relay_v2.db")
    with sqlite3.connect(path) as c:
        c.execute(
            """CREATE TABLE events (id TEXT PRIMARY KEY, pubkey TEXT,
               created_at INTEGER, kind INTEGER, content TEXT,
               received_at INTEGER)""")
        base = int(time.time()) - 100
        for i in range(n_events):
            c.execute(
                "INSERT INTO events VALUES (?,?,?,?,?,?)",
                (f"relay_{i}", "ab" * 32, base + i, 1 if i % 2 == 0 else 7,
                 f"событие {i}", base + i + 10),
            )
    return path


def test_should_publish_block_threshold():
    now = 1_000_000
    # +50 блоков → публиковать, даже если прошло мало времени
    assert audit_daemon.should_publish(150, 100, now - 10, now) is True
    # 600 с прошло и цепочка выросла → публиковать
    assert audit_daemon.should_publish(101, 100, now - 700, now) is True
    # ничего не изменилось → НЕ публиковать
    assert audit_daemon.should_publish(100, 100, now - 700, now) is False
    # мало блоков и мало времени → НЕ публиковать
    assert audit_daemon.should_publish(110, 100, now - 10, now) is False


def test_state_persist(audit_db):
    mesh_int._save_state(audit_db, "daemon_cert_ts", 12345)
    mesh_int._save_state(audit_db, "daemon_cert_height", 42)
    assert mesh_int._sync_state(audit_db, "daemon_cert_ts") == 12345
    assert mesh_int._sync_state(audit_db, "daemon_cert_height") == 42
    # обновление существующего ключа
    mesh_int._save_state(audit_db, "daemon_cert_ts", 67890)
    assert mesh_int._sync_state(audit_db, "daemon_cert_ts") == 67890


def test_run_cycle_empty_sources(audit_db, nsec, tmp_path, monkeypatch):
    """Пустой relay_db → цикл не падает, 0 событий, цепочка пуста."""
    monkeypatch.setattr(audit_daemon.publisher, "publish_cert",
                        lambda *a, **k: {"cert": {}})
    monkeypatch.setattr(audit_daemon.publisher, "build_snapshot",
                        lambda *a, **k: {})
    empty = _make_relay_db(tmp_path, n_events=0)
    r = audit_daemon.run_cycle(audit_db, nsec, relay_db=empty,
                               snapshot_out="")
    assert r["stored"] == 0
    assert r["height"] == 0
    assert r["cert"] is False  # пустая цепочка → сертификат не публикуем


def test_run_cycle_writes_events(audit_db, nsec, tmp_path, monkeypatch):
    """2 события из relay_v2.db → 2 блока в цепочке."""
    monkeypatch.setattr(audit_daemon.publisher, "publish_cert",
                        lambda *a, **k: {"cert": {}})
    monkeypatch.setattr(audit_daemon.publisher, "build_snapshot",
                        lambda *a, **k: {})
    relay_db = _make_relay_db(tmp_path, n_events=2)
    r = audit_daemon.run_cycle(audit_db, nsec, relay_db=relay_db,
                               snapshot_out="")
    assert r["stored"] == 2
    assert r["height"] == 2
    ok, count, reason, broken = chain.verify_chain_signed(audit_db)
    assert count == 2, f"подписано блоков: {count} (reason={reason}, broken={broken})"
    assert ok is True


def test_run_cycle_idempotent(audit_db, nsec, tmp_path, monkeypatch):
    """Второй проход → 0 новых (state last_ts сохранился)."""
    monkeypatch.setattr(audit_daemon.publisher, "publish_cert",
                        lambda *a, **k: {"cert": {}})
    monkeypatch.setattr(audit_daemon.publisher, "build_snapshot",
                        lambda *a, **k: {})
    relay_db = _make_relay_db(tmp_path, n_events=3)
    r1 = audit_daemon.run_cycle(audit_db, nsec, relay_db=relay_db,
                                snapshot_out="")
    assert r1["stored"] == 3
    r2 = audit_daemon.run_cycle(audit_db, nsec, relay_db=relay_db,
                                snapshot_out="")
    assert r2["stored"] == 0, "повторный проход не должен дублировать"
    assert r2["height"] == 3


def test_run_cycle_snapshot_written(audit_db, nsec, tmp_path, monkeypatch):
    """snapshot пишется при публикации сертификата."""
    monkeypatch.setattr(audit_daemon.publisher, "publish_cert",
                        lambda *a, **k: {"cert": {}})
    written = {}
    def _snap(a, out):
        import json as _j
        written["snap"] = out
        return {"chain": {"height": 3}, "last_cert": {"height": 3}}
    monkeypatch.setattr(audit_daemon.publisher, "build_snapshot", _snap)
    relay_db = _make_relay_db(tmp_path, n_events=3)
    snap = str(tmp_path / "snapshot.json")
    # форсируем публикацию: last_height=0, порог по блокам достигнут
    mesh_int._save_state(audit_db, "daemon_cert_ts", 0)
    mesh_int._save_state(audit_db, "daemon_cert_height", 0)
    r = audit_daemon.run_cycle(audit_db, nsec, relay_db=relay_db,
                               snapshot_out=snap)
    # 3 события < 50 блоков, но 600с прошло (last_ts=0) и цепочка выросла
    assert r["cert"] is True
    assert written["snap"] == snap

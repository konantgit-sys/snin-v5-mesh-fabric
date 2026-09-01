"""
SNIN PROOF MESH — Фаза 5: тесты publisher (сертификаты + snapshot).

Unit: build_cert (root=last_hash, prev_cert_id), publish_cert с мок-адаптером
(cert пишется в cert_state, event_id сохраняется), build_snapshot (реальные
цифры из БД). Live-проверка fetch_cert/verify — в smoke вне pytest (реальная
публикация на релеи — действие, не тест).

Done-when Ф5: kind 30000+8010 на 3+ релеях; verify по root OK;
дашборд с реальными цифрами.

Спека: SNIN_PROOF_MESH_SPEC.md, Фаза 5.
"""

import json
import sqlite3
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, "/home/agent/data/sites/relay-mesh")
sys.path.insert(0, "/home/agent/data/agents/core/cryter/src")

from proof_mesh import db, publisher  # noqa: E402


@pytest.fixture(scope="session")
def nsec():
    from nostr_sdk import Keys
    return Keys.generate().secret_key().to_bech32()


@pytest.fixture
def audit_db(tmp_path):
    path = str(tmp_path / "audit.db")
    db.init_db(path)
    return path


def _seed_chain(audit_db, nsec, n=3):
    from proof_mesh import chain
    for i in range(n):
        chain.append_signed(audit_db, nsec=nsec, agent_id="sensor",
                            action=f"seed:{i}", payload=f"тест {i}",
                            ts=int(time.time()) + i,
                            attribution="confirmed")


def test_build_cert_fields(audit_db, nsec):
    _seed_chain(audit_db, nsec)
    cert = publisher.build_cert(audit_db)
    assert cert["node_pubkey"] == publisher.CRYTER_PUB
    st = db.get_chain_state(audit_db)
    assert cert["root"] == st["last_hash"], "root = последний хэш цепочки"
    assert cert["height"] == st["height"] >= 3
    assert cert["prev_cert_id"] == ""  # первый сертификат


def test_build_cert_empty_chain(tmp_path):
    path = str(tmp_path / "empty.db")
    db.init_db(path)
    with pytest.raises(ValueError):
        publisher.build_cert(path)


def test_publish_cert_persists(audit_db, nsec, monkeypatch):
    _seed_chain(audit_db, nsec)

    class FakeAdapter:
        def __init__(self):
            self.calls = []

        def publish_event(self, content, tags=None, kind=1):
            self.calls.append({"content": content, "tags": tags, "kind": kind})
            return f"ev_{len(self.calls)}"

    fake = FakeAdapter()
    monkeypatch.setattr(publisher, "_make_adapter", lambda nsec: fake)

    r = publisher.publish_cert(audit_db, nsec)
    assert r["cert_event_id"] == "ev_1"
    assert r["root_event_id"] == "ev_2"
    # kind 8010 + kind 30000 с d-tag
    kinds = [c["kind"] for c in fake.calls]
    assert kinds == [8010, 30000]
    tags8010 = fake.calls[0]["tags"]
    assert any(t[0] == "root" for t in tags8010)
    tags30000 = fake.calls[1]["tags"]
    assert any(t[0] == "d" and t[1] == publisher.D_TAG for t in tags30000)
    # cert в БД с prev_cert_id следующего = event_id первого
    with sqlite3.connect(audit_db) as c:
        rows = c.execute("SELECT kind, root, height, event_id FROM cert_state").fetchall()
    assert len(rows) == 1
    assert rows[0][1] == r["cert"]["root"]


def test_prev_cert_id_links(audit_db, nsec, monkeypatch):
    _seed_chain(audit_db, nsec)

    class FakeAdapter:
        def __init__(self):
            self.n = 0

        def publish_event(self, content, tags=None, kind=1):
            self.n += 1
            return f"ev_{self.n}"

    monkeypatch.setattr(publisher, "_make_adapter", lambda nsec: FakeAdapter())
    publisher.publish_cert(audit_db, nsec)
    cert2 = publisher.build_cert(audit_db)
    assert cert2["prev_cert_id"] == "ev_1", "второй сертификат ссылается на первый"


def test_build_snapshot_real_data(audit_db, nsec, monkeypatch):
    _seed_chain(audit_db, nsec, n=3)

    class FakeAdapter:
        def __init__(self):
            self.n = 0

        def publish_event(self, content, tags=None, kind=1):
            self.n += 1
            return f"ev_{self.n}"

    monkeypatch.setattr(publisher, "_make_adapter", lambda nsec: FakeAdapter())
    publisher.publish_cert(audit_db, nsec)
    # пара экономических данных
    db.add_payment(audit_db, ts=int(time.time()), kind=9735,
                   sender_pub="a" * 64, receiver_pub="b" * 64,
                   amount_msat=21000, event_id="zap1")
    db.upsert_wallet(audit_db, pubkey="c" * 64, lud16="x@y.z")

    out = str(Path(audit_db).parent / "snapshot.json")
    snap = publisher.build_snapshot(audit_db, out)
    assert snap["chain"]["height"] >= 3
    assert snap["chain"]["last_root"]  # не пустой
    assert snap["economy"]["wallets"] == 1
    assert snap["economy"]["payments"][0]["total_msat"] == 21000
    assert snap["last_cert"]["height"] >= 3
    # файл реально записан и валидный JSON
    with open(out) as f:
        assert json.load(f)["chain"]["events_total"] >= 3

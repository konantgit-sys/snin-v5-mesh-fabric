"""
SNIN PROOF MESH — Фаза 4: тесты интеграции с mesh/relay (mesh_int.py).

Unit: sync_relay_db (идемпотентность, degraded для kind 9000),
_parse_line (JSON + msgpack).
Integration (live): события из relay_v2.db → chain → verify OK;
сквозной контур: запись в nostr.sock (как SmartRouter) → слушатель →
chain → verify OK. Если mesh-сокеты недоступны — тест пропускается.

Done-when Ф4: событие маршрутизации в audit_events с attribution;
пост → relay → mesh → chain → verify OK.

Спека: SNIN_PROOF_MESH_SPEC.md, Фаза 4.
"""

import json
import os
import socket
import sqlite3
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, "/home/agent/data/sites/relay-mesh")

from proof_mesh import chain, db, mesh_int  # noqa: E402

RELAY_DB = "/home/agent/data/sites/relay/relay_v2.db"


@pytest.fixture(scope="session")
def nsec():
    from nostr_sdk import Keys
    return Keys.generate().secret_key().to_bech32()


@pytest.fixture
def audit_db(tmp_path):
    path = str(tmp_path / "mesh_audit.db")
    db.init_db(path)
    return path


@pytest.fixture
def relay_db(tmp_path):
    path = str(tmp_path / "relay_v2.db")
    with sqlite3.connect(path) as c:
        c.execute("""CREATE TABLE events (
            id TEXT PRIMARY KEY, pubkey TEXT NOT NULL,
            created_at INTEGER NOT NULL, kind INTEGER NOT NULL,
            tags_json TEXT, content TEXT, sig TEXT NOT NULL,
            received_at INTEGER NOT NULL)""")
        now = int(time.time())
        for i in range(25):
            c.execute(
                """INSERT INTO events VALUES (?,?,?,?,?,?,?,?)""",
                (f"ev{i}", f"pub{i}", now - i, 1, "[]",
                 f"реальное событие #{i}", "sig", now - i),
            )
        c.execute(
            """INSERT INTO events VALUES (?,?,?,?,?,?,?,?)""",
            ("dl1", "dlpub", now, 9000, "[]", "dead letter", "sig", now),
        )
    return path


# ── unit: sync_relay_db ─────────────────────────────────────────────────────

def test_sync_relay_db_and_idempotent(audit_db, nsec, relay_db):
    r1 = mesh_int.sync_relay_db(audit_db, nsec, relay_db=relay_db, limit=300)
    assert r1["seen"] == 26  # 25 kind:1 + 1 dead-letter
    assert r1["stored"] == 26
    ok, cnt, reason, broken = chain.verify_chain_signed(audit_db)
    assert ok is True and cnt == 26

    # идемпотентность: повторный запуск не дублирует
    r2 = mesh_int.sync_relay_db(audit_db, nsec, relay_db=relay_db, limit=300)
    assert r2["stored"] == 0, "повторный sync не должен дублировать события"


def test_deadletter_marked_degraded(audit_db, nsec, relay_db):
    mesh_int.sync_relay_db(audit_db, nsec, relay_db=relay_db, limit=300)
    with sqlite3.connect(audit_db) as c:
        rows = c.execute(
            "SELECT action, attribution FROM audit_events WHERE action='deadletter'"
        ).fetchall()
    assert len(rows) == 1
    assert rows[0][1] == "inferred"


# ── unit: _parse_line ───────────────────────────────────────────────────────

def test_parse_json_line():
    ev = mesh_int._parse_line(b'{"kind":39002,"pubkey":"x","content":"hi"}\n', "nostr.sock")
    assert ev["kind"] == 39002 and ev["content"] == "hi"


def test_parse_msgpack_line():
    sys.path.insert(0, "/home/agent/data/sites/relay-mesh")
    import serialization as ser
    packed = ser.pack({"kind": 9902, "pubkey": "router", "payload": {"text": "hi"}}) + b"\n"
    ev = mesh_int._parse_line(packed, "cr.sock")
    assert ev["kind"] == 9902
    assert ev["payload"]["text"] == "hi"


def test_parse_garbage():
    assert mesh_int._parse_line(b"\x00\xff\xfe not valid", "cr.sock") is None


# ── integration: сокеты mesh (live) ─────────────────────────────────────────

@pytest.mark.skipif(not os.path.exists("/tmp/snin/cr.sock"),
                    reason="mesh-сокеты не подняты")
def test_live_socket_route_event(audit_db, nsec):
    """Сквозной контур: событие в cr.sock → CRV2 (маршрутизация mesh) →
    audit.sock → chain → verify OK (done-when Ф4: mesh → chain)."""
    import asyncio
    import threading

    # 1. слушатель audit.sock в фоне
    res_holder = {}

    def runner():
        res_holder.update(
            mesh_int.listen_audit_socket(audit_db, nsec, duration=12)
        )

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    time.sleep(1)

    # 2. шлём событие в cr.sock так же, как это делает SmartRouter
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect("/tmp/snin/cr.sock")
        sys.path.insert(0, "/home/agent/data/sites/relay-mesh")
        import serialization as ser
        ev = {"id": f"e2e_{int(time.time() * 1000)}", "kind": 1,
              "pubkey": "test_router",
              "content": json.dumps({"from": "e2e_test", "text": "route e2e"}),
              "payload": {"text": "route e2e"}, "tags": [],
              "created_at": int(time.time())}
        s.sendall(ser.pack(ev) + b"\n")
        s.close()
    except Exception as e:
        pytest.skip(f"не удалось записать в cr.sock: {e}")

    t.join(timeout=15)
    res = res_holder.get("seen", 0)
    assert res >= 1, "слушатель должен поймать событие, прошедшее через CRV2"
    ok, cnt, reason, broken = chain.verify_chain_signed(audit_db)
    assert ok is True, f"цепочка не верифицируется: {reason}@{broken}"
    with sqlite3.connect(audit_db) as c:
        attrs = c.execute("SELECT DISTINCT attribution FROM audit_events").fetchall()
    assert ("inferred",) in attrs or ("confirmed",) in attrs


@pytest.mark.skipif(not os.path.exists(RELAY_DB),
                    reason="нет живой БД релея")
def test_live_relay_db_into_chain(audit_db, nsec):
    """Реальные события релея (relay_v2.db) → цепочка → verify OK."""
    res = mesh_int.sync_relay_db(audit_db, nsec, limit=100)
    assert res["stored"] >= 1, "живые события релея должны лечь в цепочку"
    ok, cnt, _, _ = chain.verify_chain_signed(audit_db)
    assert ok is True and cnt == res["stored"]

"""
SNIN PROOF MESH — Фаза 4: интеграция с mesh/relay (mesh_int.py).

Три источника событий → подписанная audit-chain (append_signed):
  1. UNIX-сокеты mesh: /tmp/snin/cr.sock (msgpack, от SmartRouter)
     и /tmp/snin/nostr.sock (JSON) — события маршрутизации
  2. relay_v2.db (sites/relay): события, принятые релеем
     (kinds 1/7/0/9000…) — перенос в цепочку
  3. dead-letter (kind 9000) → события degraded

Атрибуция: mesh/relay — компоненты АРХИТЕКТУРЫ (не из chrono-реестра),
поэтому attribution='inferred', evidence_code='PARENT_CHAIN'. Подпись —
ключом оператора (cryter), как владельца реестра.

Done-when Ф4: событие маршрутизации из SmartRouter → audit_events с
attribution; сквозной тест пост → relay → mesh → chain → verify OK.

Спека: SNIN_PROOF_MESH_SPEC.md, Фаза 4.
"""

import json
import os
import sqlite3
import sys
import time
from pathlib import Path

from proof_mesh import chain, db

RELAY_DB = "/home/agent/data/sites/relay/relay_v2.db"
SOCK_DIR = "/tmp/snin"
SYNC_KINDS = (1, 7, 0, 9000)   # посты, реакции, метаданные, dead-letter
RELAY_SYNC_LIMIT = 300


def _audit_conn(audit_db: str) -> sqlite3.Connection:
    c = sqlite3.connect(audit_db)
    c.row_factory = sqlite3.Row
    return c


def _sync_state(audit_db: str, key: str) -> int:
    with _audit_conn(audit_db) as c:
        c.execute(
            """CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY, last_id TEXT NOT NULL DEFAULT '',
                last_ts INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL)"""
        )
        row = c.execute("SELECT last_ts FROM sync_state WHERE key=?", (key,)).fetchone()
        return row[0] if row else 0


def _save_state(audit_db: str, key: str, last_ts: int) -> None:
    with _audit_conn(audit_db) as c:
        c.execute(
            """INSERT INTO sync_state (key, last_id, last_ts, updated_at)
               VALUES (?, '', ?, ?)
               ON CONFLICT(key) DO UPDATE SET last_ts=excluded.last_ts,
                 updated_at=excluded.updated_at""",
            (key, last_ts, int(time.time())),
        )


# ── 1. relay_v2.db → chain ──────────────────────────────────────────────────

def sync_relay_db(audit_db: str, nsec: str, relay_db: str = RELAY_DB,
                  limit: int = RELAY_SYNC_LIMIT,
                  kinds: tuple = SYNC_KINDS) -> dict:
    """Перенести события релея в audit-chain (идемпотентно по received_at)."""
    if not Path(relay_db).exists():
        return {"relay_db": relay_db, "stored": 0, "error": "нет БД релея"}
    last_ts = _sync_state(audit_db, "relay_v2")
    with sqlite3.connect(relay_db) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute(
            """SELECT id, pubkey, created_at, kind, content, received_at
               FROM events
               WHERE received_at > ? AND kind IN (%s)
               ORDER BY received_at ASC LIMIT ?""" % ",".join("?" * len(kinds)),
            (last_ts, *kinds, limit),
        ).fetchall()
    stored = 0
    max_ts = last_ts
    for r in rows:
        if r["received_at"] > max_ts:
            max_ts = r["received_at"]
        kind = r["kind"]
        action = "deadletter" if kind == 9000 else f"relay:{kind}"
        payload = (r["content"] or "")[:300]
        if not payload:
            payload = f"pubkey={r['pubkey'][:16]}… kind={kind}"
        try:
            chain.append_signed(
                audit_db, nsec=nsec, agent_id="relay_8197",
                action=action, payload=payload, ts=int(r["received_at"]),
                attribution="inferred", evidence_code="PARENT_CHAIN",
            )
            stored += 1
        except Exception as e:
            print(f"[mesh_int] событие {r['id'][:12]}… не записано: {e}", flush=True)
    _save_state(audit_db, "relay_v2", max_ts)
    return {"relay_db": relay_db, "seen": len(rows), "stored": stored,
            "last_ts": max_ts}


# ── 2. UNIX-сокеты mesh → chain ─────────────────────────────────────────────

def _parse_line(line: bytes, sock: str) -> dict | None:
    """Разобрать строку из сокета: JSON или msgpack (ser.unpack)."""
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except (ValueError, UnicodeDecodeError):
        pass
    try:
        sys.path.insert(0, "/home/agent/data/sites/relay-mesh")
        import serialization as ser  # noqa: PLC0415
        return ser.unpack(line)
    except Exception:
        return None


def listen_sockets(audit_db: str, nsec: str, duration: int = 25,
                   agent_id: str = "smart_router") -> dict:
    """Послушать cr.sock/nostr.sock N секунд → события в цепочку.
    Возвращает {seen, stored, samples:[...]}."""
    import socket  # noqa: PLC0415
    seen: list[dict] = []
    for name in ("cr.sock", "nostr.sock"):
        path = os.path.join(SOCK_DIR, name)
        if not os.path.exists(path):
            print(f"[mesh_int] нет сокета {path}", flush=True)
            continue
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(duration)
            s.connect(path)
            s.sendall(b'{"cmd":"ping"}\n')  # wake-up
        except Exception as e:
            print(f"[mesh_int] {name}: {type(e).__name__}", flush=True)
            s.close()
            continue
        deadline = time.time() + duration
        try:
            while time.time() < deadline:
                try:
                    line = s.recv(65536)
                    if not line:
                        break
                except socket.timeout:
                    break
                for raw in line.split(b"\n"):
                    ev = _parse_line(raw, name)
                    if ev and isinstance(ev, dict):
                        seen.append({"sock": name, "ev": ev})
        finally:
            s.close()
    stored = 0
    for item in seen:
        ev = item["ev"]
        kind = ev.get("kind", ev.get("type", "?"))
        pub = ev.get("pubkey", "router")
        content = ev.get("content") or json.dumps(ev.get("payload", ""), default=str)[:200]
        ts = int(ev.get("created_at") or ev.get("ts") or time.time())
        try:
            chain.append_signed(
                audit_db, nsec=nsec, agent_id=agent_id,
                action=f"route:{kind}", payload=f"{pub[:16]}… {content[:180]}",
                ts=ts, attribution="inferred", evidence_code="PARENT_CHAIN",
            )
            stored += 1
        except Exception as e:
            print(f"[mesh_int] socket-событие не записано: {e}", flush=True)
    samples = [{"sock": s["sock"], "kind": s["ev"].get("kind", s["ev"].get("type")),
                "pubkey": s["ev"].get("pubkey", "")[:16]} for s in seen[:5]]
    return {"seen": len(seen), "stored": stored, "samples": samples}


def listen_audit_socket(audit_db: str, nsec: str, duration: int = 15,
                        agent_id: str = "smart_router") -> dict:
    """Серверный сокет /tmp/snin/audit.sock: принимает события, которые
    CRV2 дублирует после маршрутизации → подписанная audit-chain.
    Возвращает {seen, stored, samples}. Сокет создаётся на время работы."""
    import socket as _socket  # noqa: PLC0415
    path = os.path.join(SOCK_DIR, "audit.sock")
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    srv = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
    srv.bind(path)
    srv.listen(8)
    srv.settimeout(1.0)
    seen: list[dict] = []
    deadline = time.time() + duration
    while time.time() < deadline:
        try:
            conn, _ = srv.accept()
            conn.settimeout(2.0)
            while True:
                try:
                    line = conn.recv(65536)
                except _socket.timeout:
                    break
                if not line:
                    break
                for raw in line.split(b"\n"):
                    ev = _parse_line(raw, "audit.sock")
                    if ev and isinstance(ev, dict):
                        seen.append(ev)
            conn.close()
        except _socket.timeout:
            continue
        except OSError:
            break
    srv.close()
    try:
        os.unlink(path)
    except OSError:
        pass

    stored = 0
    for ev in seen:
        kind = ev.get("kind", ev.get("type", "?"))
        pub = ev.get("pubkey", ev.get("from", "router"))
        content = ev.get("content") or json.dumps(ev.get("payload", ""), default=str)[:200]
        ts = int(ev.get("created_at") or ev.get("ts") or time.time())
        try:
            chain.append_signed(
                audit_db, nsec=nsec, agent_id=agent_id,
                action=f"route:{kind}", payload=f"{pub[:16]}… {content[:180]}",
                ts=ts, attribution="inferred", evidence_code="PARENT_CHAIN",
            )
            stored += 1
        except Exception as e:
            print(f"[mesh_int] audit-событие не записано: {e}", flush=True)
    samples = [{"kind": ev.get("kind", ev.get("type")),
                "pubkey": ev.get("pubkey", ev.get("from", ""))[:16]} for ev in seen[:5]]
    return {"seen": len(seen), "stored": stored, "samples": samples}


# ── 3. dead-letter (kind 9000) ──────────────────────────────────────────────

def check_dead_letters(audit_db: str, nsec: str,
                       relay_db: str = RELAY_DB) -> dict:
    """Свежие kind 9000 из БД релея → события degraded (по action)."""
    res = sync_relay_db(audit_db, nsec, relay_db=relay_db,
                        limit=50, kinds=(9000,))
    return {"deadletters_seen": res.get("seen", 0),
            "deadletters_stored": res.get("stored", 0)}

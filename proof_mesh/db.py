"""
SNIN PROOF MESH — Фаза 0: модуль БД аудита (snin_audit.db).

Tamper-evident хэш-цепочка событий + реестр instance-identity агентов +
экономика роя (zap-ы/кошельки). Схема цепочки — по образцу archivist hash_chain:
каждый блок = sha256(prev_hash | content_hash | ts | signature).

Фаза 0 предоставляет: init_db, append_event, verify_chain, verify_from,
get_chain_state, set_root + CRUD для agent_instances / payment_events /
wallet_profiles. Реальная подпись секцией nsec — Фаза 2 (здесь signature
принимается как строка и включается в block_hash).

Спека: SNIN_PROOF_MESH_SPEC.md (projects/snin-v5-mesh-fabric/).
"""

import hashlib
import sqlite3
import time
from pathlib import Path

SCHEMA_VERSION = 2

# Миграции: {версия: [ALTER-операции]}
_MIGRATIONS = {
    2: [
        "ALTER TABLE audit_events ADD COLUMN signer_pub TEXT NOT NULL DEFAULT ''",
    ],
}

# Атрибуция (идея из AEGIS: не выдумывать владельца)
ATTRIBUTION = ("confirmed", "inferred", "unattributed")
# Коды доказательств
EVIDENCE_CODES = ("REG_MATCH", "SIG_MATCH", "PARENT_CHAIN", "UNKNOWN")

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS audit_events (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        ts            INTEGER NOT NULL,
        agent_id      TEXT NOT NULL DEFAULT '',
        instance_id   TEXT NOT NULL DEFAULT '',
        action        TEXT NOT NULL,
        payload       TEXT NOT NULL DEFAULT '',
        payload_hash  TEXT NOT NULL,
        prev_hash     TEXT NOT NULL,
        block_hash    TEXT NOT NULL,
        signature     TEXT NOT NULL DEFAULT '',
        signer_pub    TEXT NOT NULL DEFAULT '',
        attribution   TEXT NOT NULL DEFAULT 'unattributed',
        evidence_code TEXT NOT NULL DEFAULT 'UNKNOWN',
        created_at    TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_audit_agent ON audit_events(agent_id);
    CREATE INDEX IF NOT EXISTS idx_audit_ts    ON audit_events(ts);
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_instances (
        instance_id TEXT PRIMARY KEY,
        agent_id    TEXT NOT NULL DEFAULT '',
        pid         INTEGER NOT NULL,
        start_time  REAL NOT NULL,
        cgroup      TEXT NOT NULL DEFAULT '',
        cmdline     TEXT NOT NULL DEFAULT '',
        first_seen  INTEGER NOT NULL,
        last_seen   INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_inst_agent ON agent_instances(agent_id);
    """,
    """
    CREATE TABLE IF NOT EXISTS payment_events (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        ts           INTEGER NOT NULL,
        kind         INTEGER NOT NULL,
        sender_pub   TEXT NOT NULL DEFAULT '',
        receiver_pub TEXT NOT NULL DEFAULT '',
        amount_msat  INTEGER NOT NULL DEFAULT 0,
        ln_address   TEXT NOT NULL DEFAULT '',
        event_id     TEXT NOT NULL DEFAULT '',
        relay        TEXT NOT NULL DEFAULT '',
        created_at   TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_pay_ts      ON payment_events(ts);
    CREATE INDEX IF NOT EXISTS idx_pay_sender  ON payment_events(sender_pub);
    """,
    """
    CREATE TABLE IF NOT EXISTS wallet_profiles (
        pubkey     TEXT PRIMARY KEY,
        lud16      TEXT NOT NULL DEFAULT '',
        lud06      TEXT NOT NULL DEFAULT '',
        first_seen INTEGER NOT NULL,
        last_seen  INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS chain_state (
        chain_id   TEXT PRIMARY KEY,
        last_hash  TEXT NOT NULL DEFAULT '',
        height     INTEGER NOT NULL DEFAULT 0,
        root       TEXT NOT NULL DEFAULT '',
        root_ts    INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    """,
]


def _sha256(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _conn(db_path: str) -> sqlite3.Connection:
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    return c


def init_db(db_path: str) -> None:
    """Создать snin_audit.db со всеми таблицами, schema_version=2, миграции."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with _conn(db_path) as c:
        for ddl in _DDL:
            c.executescript(ddl)
        ver = c.execute("PRAGMA user_version").fetchone()[0]
        existing = {r[1] for r in c.execute("PRAGMA table_info(audit_events)")}
        for target in sorted(_MIGRATIONS):
            if ver < target:
                for stmt in _MIGRATIONS[target]:
                    # защита от дубликата: ALTER только если колонки ещё нет
                    if "ADD COLUMN" in stmt:
                        words = stmt.split()
                        col = words[words.index("COLUMN") + 1]
                        if col in existing:
                            continue
                    c.execute(stmt)
                c.execute(f"PRAGMA user_version = {target}")
        c.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


def _last_hash(c: sqlite3.Connection) -> str:
    row = c.execute(
        "SELECT block_hash FROM audit_events ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return row["block_hash"] if row else "0" * 64


def append_event(
    db_path: str,
    *,
    agent_id: str = "",
    instance_id: str = "",
    action: str,
    payload: str = "",
    attribution: str = "unattributed",
    evidence_code: str = "UNKNOWN",
    signature: str = "",
    signer_pub: str = "",
    ts: int | None = None,
) -> str:
    """
    Добавить событие в хэш-цепочку. Возвращает block_hash нового блока.

    block_hash = sha256(prev_hash | content_hash(payload) | ts | signature)
    signer_pub — pubkey подписанта (Ф2, chain.py). Плюс обновление
    chain_state: height+1, last_hash.
    """
    if attribution not in ATTRIBUTION:
        raise ValueError(f"attribution must be one of {ATTRIBUTION}, got {attribution!r}")
    if evidence_code not in EVIDENCE_CODES:
        raise ValueError(f"evidence_code must be one of {EVIDENCE_CODES}, got {evidence_code!r}")
    ts = int(ts if ts is not None else time.time())
    content_hash = _sha256(payload)
    with _conn(db_path) as c:
        prev = _last_hash(c)
        block_hash = _sha256(f"{prev}|{content_hash}|{ts}|{signature}")
        c.execute(
            """INSERT INTO audit_events
               (ts, agent_id, instance_id, action, payload, payload_hash,
                prev_hash, block_hash, signature, signer_pub, attribution, evidence_code)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (ts, agent_id, instance_id, action, payload, content_hash,
             prev, block_hash, signature, signer_pub, attribution, evidence_code),
        )
        c.execute(
            """INSERT INTO chain_state (chain_id, last_hash, height, updated_at)
               VALUES ('main', ?, 1, datetime('now'))
               ON CONFLICT(chain_id) DO UPDATE SET
                 last_hash=excluded.last_hash,
                 height=height+1,
                 updated_at=datetime('now')""",
            (block_hash,),
        )
    return block_hash


def verify_chain(db_path: str) -> tuple[bool, int, int | None]:
    """
    Полная проверка цепочки. Возвращает (ok, count, first_broken_id).
    broken_at=None при ok=True. При обрыве — id первого битого блока.
    """
    return verify_from(db_path, start_id=1)


def verify_from(db_path: str, start_id: int) -> tuple[bool, int, int | None]:
    """Проверка цепочки от start_id (контрольная точка)."""
    with _conn(db_path) as c:
        rows = c.execute(
            "SELECT id, ts, payload, payload_hash, prev_hash, block_hash, signature "
            "FROM audit_events WHERE id >= ? ORDER BY id",
            (start_id,),
        ).fetchall()
        if not rows:
            return True, 0, None
        if start_id > 1:
            # prev для первого проверяемого блока = block_hash блока перед ним
            prev_row = c.execute(
                "SELECT block_hash FROM audit_events WHERE id < ? ORDER BY id DESC LIMIT 1",
                (start_id,),
            ).fetchone()
            prev = prev_row["block_hash"] if prev_row else "0" * 64
        else:
            prev = rows[0]["prev_hash"]
        for r in rows:
            content_hash = _sha256(r["payload"])
            expect = _sha256(f"{prev}|{content_hash}|{r['ts']}|{r['signature']}")
            if expect != r["block_hash"]:
                return False, len(rows), r["id"]
            prev = r["block_hash"]
    return True, len(rows), None


def get_chain_state(db_path: str) -> dict | None:
    with _conn(db_path) as c:
        row = c.execute("SELECT * FROM chain_state WHERE chain_id='main'").fetchone()
        return dict(row) if row else None


def set_root(db_path: str, root: str) -> None:
    """Зафиксировать публичный корень (Фаза 5: kind 30000 / 8010)."""
    with _conn(db_path) as c:
        c.execute(
            """INSERT INTO chain_state (chain_id, last_hash, height, root, root_ts, updated_at)
               VALUES ('main', '0'*64, 0, ?, ?, datetime('now'))
               ON CONFLICT(chain_id) DO UPDATE SET
                 root=excluded.root, root_ts=excluded.root_ts, updated_at=datetime('now')""",
            (root, int(time.time())),
        )


# ── instance identity (Фаза 1 API, заготовка) ──────────────────────────────

def upsert_instance(
    db_path: str,
    *,
    instance_id: str,
    agent_id: str = "",
    pid: int,
    start_time: float,
    cgroup: str = "",
    cmdline: str = "",
    ts: int | None = None,
) -> None:
    ts = int(ts if ts is not None else time.time())
    with _conn(db_path) as c:
        c.execute(
            """INSERT INTO agent_instances
               (instance_id, agent_id, pid, start_time, cgroup, cmdline, first_seen, last_seen)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(instance_id) DO UPDATE SET
                 last_seen=excluded.last_seen""",
            (instance_id, agent_id, pid, start_time, cgroup, cmdline, ts, ts),
        )


def get_instances(db_path: str, agent_id: str = "") -> list[dict]:
    with _conn(db_path) as c:
        if agent_id:
            rows = c.execute(
                "SELECT * FROM agent_instances WHERE agent_id=?", (agent_id,)
            ).fetchall()
        else:
            rows = c.execute("SELECT * FROM agent_instances ORDER BY last_seen DESC").fetchall()
        return [dict(r) for r in rows]


# ── экономика роя (Фаза 3 API, заготовка) ─────────────────────────────────

def add_payment(
    db_path: str,
    *,
    kind: int,
    sender_pub: str = "",
    receiver_pub: str = "",
    amount_msat: int = 0,
    ln_address: str = "",
    event_id: str = "",
    relay: str = "",
    ts: int | None = None,
) -> int:
    ts = int(ts if ts is not None else time.time())
    with _conn(db_path) as c:
        cur = c.execute(
            """INSERT INTO payment_events
               (ts, kind, sender_pub, receiver_pub, amount_msat, ln_address, event_id, relay)
               VALUES (?,?,?,?,?,?,?,?)""",
            (ts, kind, sender_pub, receiver_pub, amount_msat, ln_address, event_id, relay),
        )
        return cur.lastrowid


def get_payments(
    db_path: str, *, since: int = 0, limit: int = 100
) -> list[dict]:
    with _conn(db_path) as c:
        rows = c.execute(
            "SELECT * FROM payment_events WHERE ts>=? ORDER BY ts DESC LIMIT ?",
            (since, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def upsert_wallet(db_path: str, *, pubkey: str, lud16: str = "", lud06: str = "") -> None:
    ts = int(time.time())
    with _conn(db_path) as c:
        c.execute(
            """INSERT INTO wallet_profiles (pubkey, lud16, lud06, first_seen, last_seen)
               VALUES (?,?,?,?,?)
               ON CONFLICT(pubkey) DO UPDATE SET
                 lud16=excluded.lud16, lud06=excluded.lud06, last_seen=excluded.last_seen""",
            (pubkey, lud16, lud06, ts, ts),
        )


def get_wallets(db_path: str) -> list[dict]:
    with _conn(db_path) as c:
        rows = c.execute("SELECT * FROM wallet_profiles ORDER BY last_seen DESC").fetchall()
        return [dict(r) for r in rows]

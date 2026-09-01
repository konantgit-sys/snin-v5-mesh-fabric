"""
SNIN PROOF MESH — Фаза 2: hash-chain журнал с подписями (chain.py).

Цепочка (поверх db.py, schema v2):
    unsigned   = sha256(prev_hash | content_hash(payload) | ts)   # тело блока
    signature  = BIP340-schnorr подпись unsigned секцией nsec агента
    block_hash = sha256(unsigned | signature)                     # блок

Такой порядок разрешает цикл «подпись внутри хэша»: подписывается тело,
хэш блока включает подпись. Подделка ЛЮБОЙ записи (payload/ts/sig/pubkey)
ломает block_hash и/или верификацию подписи.

verify_chain_signed(db, registry):
  - целостность: пересчёт всех block_hash (как db.verify_chain)
  - подписи: каждая подпись проверяется по signer_pub из события
  - реестр: если агент есть в chrono.agent_registry — signer_pub обязан
    совпасть с hex_pub агента (защита от подмены pubkey в блоке)

Root: каждые 100 событий ИЛИ 10 минут → chain_state.root + запись в
snin-hub/proof_registry.db (таблица chain_roots) — интеграция с реестром
доказательств. Публикация корня в Nostr — Фаза 5.

Done-when Ф2: 1000 событий verify OK; тампер любой записи → падает с
указанием места; подделка подписи → падает.

Спека: SNIN_PROOF_MESH_SPEC.md.
"""

import sqlite3
import time
from pathlib import Path

from proof_mesh import db

ROOT_INTERVAL_EVENTS = 100      # root каждые N событий
ROOT_INTERVAL_SEC = 600         # ...или каждые 10 минут
PROOF_REGISTRY_DB = "/home/agent/data/sites/snin-hub/proof_registry.db"

_SIG = None  # ленивый импорт nostr-sdk (дорогой)


def _sdk():
    global _SIG
    if _SIG is None:
        from nostr_sdk import Keys  # noqa: PLC0415
        from secp256k1 import PublicKey  # noqa: PLC0415
        _SIG = (Keys, PublicKey)
    return _SIG


# ── подпись / верификация (BIP340, совместимо с Nostr) ─────────────────────

def sign_block(nsec: str, unsigned_hash: str) -> str:
    """Подписать тело блока (unsigned_hash) секцией nsec. Возвращает sig hex."""
    Keys, _ = _sdk()
    keys = Keys.parse(nsec)
    return keys.sign_schnorr(bytes.fromhex(unsigned_hash))


def verify_block(pubkey_hex: str, unsigned_hash: str, sig_hex: str) -> bool:
    """Проверить schnorr-подпись по pubkey (x-only, BIP340)."""
    _, PublicKey = _sdk()
    try:
        pub = PublicKey(b"\x02" + bytes.fromhex(pubkey_hex), raw=True)
        return pub.schnorr_verify(
            bytes.fromhex(unsigned_hash), bytes.fromhex(sig_hex), None, raw=True
        )
    except Exception:
        return False


# ── запись в цепочку ────────────────────────────────────────────────────────

def append_signed(
    db_path: str,
    *,
    nsec: str,
    agent_id: str,
    action: str,
    payload: str = "",
    instance_id: str = "",
    attribution: str = "confirmed",
    evidence_code: str = "SIG_MATCH",
    ts: int | None = None,
    proof_registry: str = PROOF_REGISTRY_DB,
) -> tuple[str, str, str]:
    """
    Добавить ПОДПИСАННОЕ событие. Возвращает (block_hash, unsigned, sig).

    Формат блока (отличается от Ф0-append_event: подпись подписывает ТЕЛО):
        unsigned   = sha256(prev_hash | content_hash | ts)  # тело блока
        signature  = BIP340-schnorr подпись unsigned nsec-ключом
        block_hash = sha256(unsigned | signature)
    Верифицируется chain.verify_chain_signed. Обновляет root при
    height % 100 == 0 или (для уже инициализированных корней) >= 10 минут.
    """
    ts = int(ts if ts is not None else time.time())
    Keys, _ = _sdk()
    keys = Keys.parse(nsec)
    signer_pub = keys.public_key().to_hex()
    with sqlite3.connect(db_path) as c:
        row = c.execute(
            "SELECT block_hash FROM audit_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        prev = row[0] if row else "0" * 64
        content_hash = db._sha256(payload)
        unsigned = db._sha256(f"{prev}|{content_hash}|{ts}")
    sig = sign_block(nsec, unsigned)
    block_hash = db._sha256(f"{unsigned}|{sig}")

    with sqlite3.connect(db_path) as c:
        c.execute(
            """INSERT INTO audit_events
               (ts, agent_id, instance_id, action, payload, payload_hash,
                prev_hash, block_hash, signature, signer_pub, attribution, evidence_code)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (ts, agent_id, instance_id, action, payload, content_hash,
             prev, block_hash, sig, signer_pub, attribution, evidence_code),
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
    _maybe_root(db_path, signer_pub, sig, proof_registry)
    return block_hash, unsigned, sig


def _last_hash(db_path: str) -> str:
    with sqlite3.connect(db_path) as c:
        row = c.execute(
            "SELECT block_hash FROM audit_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else "0" * 64


# ── корень цепочки ──────────────────────────────────────────────────────────

def _maybe_root(db_path: str, signer_pub: str, sig: str, proof_registry: str) -> None:
    state = db.get_chain_state(db_path) or {}
    height = state.get("height", 0)
    root_ts = state.get("root_ts", 0)
    now = int(time.time())
    # root_ts==0 → корень ещё ни разу не ставился: ждём 100 событий.
    # Иначе — 100 событий ИЛИ 10 минут с последнего корня.
    by_events = height > 0 and height % ROOT_INTERVAL_EVENTS == 0
    by_time = root_ts > 0 and (now - root_ts) >= ROOT_INTERVAL_SEC
    if not (by_events or by_time):
        return
    last_hash = _last_hash(db_path)
    db.set_root(db_path, last_hash)
    _integrate_root(proof_registry, last_hash, height, now, signer_pub, sig)


def _integrate_root(
    proof_db: str, root: str, height: int, root_ts: int,
    pubkey: str, sig: str,
) -> None:
    """Дублировать корень в proof_registry.db (snin-hub) — таблица chain_roots."""
    Path(proof_db).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(proof_db) as c:
        c.execute(
            """CREATE TABLE IF NOT EXISTS chain_roots (
                chain_id   TEXT NOT NULL DEFAULT 'main',
                height     INTEGER NOT NULL,
                root       TEXT NOT NULL,
                root_ts    INTEGER NOT NULL,
                pubkey     TEXT NOT NULL DEFAULT '',
                sig        TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (chain_id, height)
            )"""
        )
        c.execute(
            """INSERT OR REPLACE INTO chain_roots
               (chain_id, height, root, root_ts, pubkey, sig)
               VALUES ('main', ?, ?, ?, ?, ?)""",
            (height, root, root_ts, pubkey, sig),
        )


def get_roots(proof_db: str = PROOF_REGISTRY_DB, limit: int = 10) -> list[dict]:
    """Последние корни из proof_registry.db."""
    with sqlite3.connect(proof_db) as c:
        c.row_factory = sqlite3.Row
        try:
            rows = c.execute(
                "SELECT * FROM chain_roots ORDER BY height DESC LIMIT ?", (limit,)
            ).fetchall()
        except sqlite3.Error:
            return []
        return [dict(r) for r in rows]


# ── верификация ─────────────────────────────────────────────────────────────

def verify_chain_signed(
    db_path: str,
    registry: dict[str, str] | None = None,
) -> tuple[bool, int, str | None, int | None]:
    """
    Полная проверка: (ok, count, reason, first_broken_id).
    reason: None | 'hash' (цепочка) | 'sig' (подпись) | 'pubkey' (реестр).
    registry: agent_id → hex_pub (для контроля подмены pubkey).
    """
    registry = registry or {}
    with sqlite3.connect(db_path) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute(
            """SELECT id, ts, payload, prev_hash, block_hash, signature,
                      signer_pub, agent_id
               FROM audit_events ORDER BY id"""
        ).fetchall()
    if not rows:
        return True, 0, None, None

    prev = rows[0]["prev_hash"]
    for r in rows:
        content_hash = db._sha256(r["payload"])
        unsigned = db._sha256(f"{prev}|{content_hash}|{r['ts']}")
        expect = db._sha256(f"{unsigned}|{r['signature']}")
        if expect != r["block_hash"]:
            return False, len(rows), "hash", r["id"]
        # подпись должна валидироваться по signer_pub блока
        if r["signature"]:
            if not verify_block(r["signer_pub"], unsigned, r["signature"]):
                return False, len(rows), "sig", r["id"]
            # реестровый контроль: pubkey агента не подменили
            known_pub = registry.get(r["agent_id"])
            if known_pub and r["signer_pub"] != known_pub:
                return False, len(rows), "pubkey", r["id"]
        prev = r["block_hash"]
    return True, len(rows), None, None


# ── загрузка pubkey агентов из chrono ───────────────────────────────────────

def load_pubkeys(registry_db: str = "/home/agent/data/sites/chrono/chrono.db") -> dict[str, str]:
    """agent_id → hex_pub из chrono.agent_registry."""
    result: dict[str, str] = {}
    if not Path(registry_db).exists():
        return result
    try:
        with sqlite3.connect(registry_db) as c:
            rows = c.execute(
                "SELECT agent_id, hex_pub FROM agent_registry WHERE hex_pub != ''"
            ).fetchall()
        result = {aid: pub for aid, pub in rows}
    except sqlite3.Error:
        pass
    return result

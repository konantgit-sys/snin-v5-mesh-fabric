#!/usr/bin/env python3
"""anchor.py — якорь артефактов в цепочке доказательств (Proof Mesh / Sentinel).

ФАЗА 1 спеки PROOF_CHAIN_EXPANSION_SPEC_2026-09-17.md.

Что делает: берёт файл, считает sha256 и пишет в цепочку ПОДПИСАННЫЙ блок
`anchor.artifact` с хешем, размером, типом и именем. После этого можно доказать
третьей стороне: файл существовал в таком виде в такой-то момент — и что его
не подменили после якоря.

Команды:
    anchor.py add <файл> [--note "текст"] [--agent ID]
        → блок в цепочке, печатает id/хеш/корень

    anchor.py verify <файл> [--block N]
        → проверяет: (1) блок самосогласован, (2) связь с предыдущим,
          (3) включение в опубликованный корень, (4) хеш файла = хеш в блоке.
          Код возврата 0 = совпадает, 1 = расхождение (годится для скриптов)

    anchor.py list [N]
        → последние N якорей

Ключ: тот же, что у audit_daemon (cryter из keystore chrono). Значения ключей
не печатаются — только пути.
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import sqlite3
import sys
import time
from hashlib import sha256

BASE = "/home/agent/data"
MESH = f"{BASE}/sites/relay-mesh"
PROOF = f"{MESH}/proof_mesh"
AUDIT_DB = f"{PROOF}/snin_audit.db"
NSEC_PATH = f"{BASE}/sites/chrono/keystore/agent_keys.json.old"
KEY_NAME = "cryter"
DEFAULT_AGENT = "audit_daemon"
ACTION = "anchor.artifact"

sys.path.insert(0, MESH)
sys.path.insert(0, PROOF)

from proof_mesh import chain, db  # noqa: E402


def load_nsec() -> str:
    return json.load(open(NSEC_PATH))[KEY_NAME]["nsec"]


def file_sha256(path: str) -> str:
    h = sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def anchor_add(path: str, note: str = "", agent: str = DEFAULT_AGENT) -> dict:
    if not os.path.isfile(path):
        raise SystemExit(f"нет такого файла: {path}")
    digest = file_sha256(path)
    size = os.path.getsize(path)
    mime = mimetypes.guess_type(path)[0] or "application/octet-stream"
    ts = int(time.time())
    payload = json.dumps(
        {
            "kind": "artifact",
            "sha256": digest,
            "size": size,
            "mime": mime,
            "name": os.path.basename(path),
            "agent": agent,
            "note": note,
            "ts": ts,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    block_hash, unsigned, sig = chain.append_signed(
        AUDIT_DB,
        nsec=load_nsec(),
        agent_id=agent,
        action=ACTION,
        payload=payload,
        attribution="confirmed",
        evidence_code="SIG_MATCH",
        ts=ts,
    )
    con = sqlite3.connect(AUDIT_DB)
    bid = con.execute(
        "SELECT id FROM audit_events WHERE block_hash=?", (block_hash,)
    ).fetchone()[0]
    con.close()
    return {
        "id": bid,
        "sha256": digest,
        "size": size,
        "mime": mime,
        "block_hash": block_hash,
        "ts": ts,
    }


def _covering_root(block_id: int) -> tuple[int, str] | None:
    """Ближайший опубликованный корень с высотой >= block_id."""
    with sqlite3.connect(chain.PROOF_REGISTRY_DB) as c:
        try:
            row = c.execute(
                "SELECT height, root FROM chain_roots WHERE chain_id='main'"
                " AND height >= ? ORDER BY height LIMIT 1",
                (block_id,),
            ).fetchone()
        except sqlite3.Error:
            return None
    return (row[0], row[1]) if row else None


def _self_consistent(row: sqlite3.Row) -> bool:
    content_hash = db._sha256(row["payload"])
    unsigned = db._sha256(f"{row['prev_hash']}|{content_hash}|{row['ts']}")
    return db._sha256(f"{unsigned}|{row['signature']}") == row["block_hash"]


def _inclusion(block_id: int, target_height: int, target_hash: str) -> tuple[bool, int]:
    """Идём от блока вперёд до target_height, сверяя каждую связку prev→hash."""
    with sqlite3.connect(AUDIT_DB) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute(
            "SELECT id, prev_hash, block_hash FROM audit_events"
            " WHERE id BETWEEN ? AND ? ORDER BY id",
            (block_id, target_height),
        ).fetchall()
    if not rows:
        return False, 0
    prev = rows[0]["prev_hash"]
    checked = 0
    for r in rows:
        if r["prev_hash"] != prev:
            return False, checked
        prev = r["block_hash"]
        checked += 1
    return prev == target_hash, checked


def anchor_verify(path: str, block_id: int | None = None) -> dict:
    if not os.path.isfile(path):
        raise SystemExit(f"нет такого файла: {path}")
    cur = file_sha256(path)

    with sqlite3.connect(AUDIT_DB) as c:
        c.row_factory = sqlite3.Row
        if block_id is None:
            row = c.execute(
                "SELECT * FROM audit_events WHERE action=?"
                " AND payload LIKE ? ORDER BY id DESC LIMIT 1",
                (ACTION, f'%"name": "{os.path.basename(path)}"%'),
            ).fetchone()
        else:
            row = c.execute(
                "SELECT * FROM audit_events WHERE id=? AND action=?",
                (block_id, ACTION),
            ).fetchone()
    if row is None:
        return {"found": False, "file_ok": False, "reason": "якорь не найден"}

    stored = json.loads(row["payload"])
    out: dict = {
        "found": True,
        "id": row["id"],
        "block_hash": row["block_hash"],
        "ts": row["ts"],
        "anchored_sha256": stored.get("sha256", ""),
        "current_sha256": cur,
        "self_consistent": _self_consistent(row),
    }

    with sqlite3.connect(AUDIT_DB) as c:
        prev_row = c.execute(
            "SELECT block_hash FROM audit_events WHERE id=?", (row["id"] - 1,)
        ).fetchone()
    out["linked"] = bool(prev_row) and prev_row[0] == row["prev_hash"]

    root = _covering_root(row["id"])
    if root:
        out["root_height"], out["root_hash"] = root
        ok, checked = _inclusion(row["id"], root[0], root[1])
        out["inclusion_ok"], out["chain_links_checked"] = ok, checked
        out["root_kind"] = "published"
    else:
        # Корень пока не покрыл блок: проверяем связность до ЖИВОГО ХВОСТА
        # (max(id) в audit_events, а не chain_state — тот обновляется только
        # при постановке корня и может отставать).
        with sqlite3.connect(AUDIT_DB) as c:
            r2 = c.execute(
                "SELECT id, block_hash FROM audit_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
        head, head_hash = (r2[0], r2[1]) if r2 else (row["id"], row["block_hash"])
        ok, checked = _inclusion(row["id"], head, head_hash)
        out["inclusion_ok"], out["chain_links_checked"] = ok, checked
        out["root_kind"] = "pending-external"
        out["head_height"] = head

    out["file_ok"] = cur == stored.get("sha256", "")
    return out


def anchor_list(limit: int = 10) -> list[dict]:
    with sqlite3.connect(AUDIT_DB) as c:
        rows = c.execute(
            "SELECT id, ts, payload, block_hash FROM audit_events"
            " WHERE action=? ORDER BY id DESC LIMIT ?",
            (ACTION, limit),
        ).fetchall()
    out = []
    for i, ts, payload, bh in rows:
        d = json.loads(payload)
        out.append(
            {
                "id": i,
                "ts": ts,
                "name": d.get("name"),
                "size": d.get("size"),
                "sha256": d.get("sha256", "")[:16],
                "block_hash": bh[:16],
            }
        )
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Якорь артефактов в цепочке Sentinel")
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("add", help="заякорить файл")
    a.add_argument("path")
    a.add_argument("--note", default="")
    a.add_argument("--agent", default=DEFAULT_AGENT)
    v = sub.add_parser("verify", help="проверить файл против якоря")
    v.add_argument("path")
    v.add_argument("--block", type=int, default=None)
    l = sub.add_parser("list", help="последние якори")
    l.add_argument("n", nargs="?", type=int, default=10)

    args = p.parse_args()

    if args.cmd == "add":
        r = anchor_add(args.path, args.note, args.agent)
        print(f"заякорено: {args.path}")
        print(f"  блок    : id={r['id']} hash={r['block_hash'][:24]}…")
        print(f"  sha256  : {r['sha256']}")
        print(f"  размер  : {r['size']} байт, тип {r['mime']}")
        root = _covering_root(r["id"])
        if root:
            print(f"  корень  : высота {root[0]} (опубликован) {root[1][:24]}…")
        else:
            print("  корень  : пока не покрыт — уйдёт в следующий корень")
        return 0

    if args.cmd == "verify":
        r = anchor_verify(args.path, args.block)
        if not r.get("found"):
            print(f"❌ {r['reason']}: {args.path}")
            return 1
        print(f"файл    : {args.path}")
        print(f"  якорь : блок id={r['id']}, {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r['ts']))}")
        print(f"  блок самосогласован : {'да' if r['self_consistent'] else 'НЕТ'}")
        print(f"  связь с предыдущим  : {'да' if r['linked'] else 'НЕТ'}")
        where = (
            f"корень высоты {r['root_height']} (опубликован)"
            if r["root_kind"] == "published"
            else f"до хвоста высоты {r.get('head_height', '-')} — корень ещё не опубликован"
        )
        print(
            f"  включение в цепочку : {'да' if r['inclusion_ok'] else 'НЕТ'}"
            f" ({r['chain_links_checked']} звеньев проверено, {where})"
        )
        print(f"  хеш в блоке         : {r['anchored_sha256']}")
        print(f"  хеш файла сейчас    : {r['current_sha256']}")
        ok = r["file_ok"] and r["self_consistent"] and r["linked"] and r["inclusion_ok"]
        print(f"  ВЫВОД               : {'✅ файл совпадает, доказательство цело' if ok else '❌ файл изменён или доказательство нарушено'}")
        return 0 if ok else 1

    rows = anchor_list(args.n)
    for r in rows:
        print(
            f"  id={r['id']:<7} {time.strftime('%Y-%m-%d %H:%M', time.localtime(r['ts']))}"
            f"  {r['name']}  {r['size']} б  sha={r['sha256']}…  blk={r['block_hash']}…"
        )
    if not rows:
        print("  якорей пока нет")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

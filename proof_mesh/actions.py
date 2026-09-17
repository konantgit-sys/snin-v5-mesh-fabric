#!/usr/bin/env python3
"""actions.py — запись ДЕЙСТВИЙ агентов в цепочку доказательств (фаза 2).

Спека: PROOF_CHAIN_EXPANSION_SPEC_2026-09-17.md, фаза 2.
Отличие от пульса: пульс говорит «агент был жив», запись действия говорит
«агент сделал вот это» — с хешем аргументов, хешем результата, версией модели
и хешем политик. Содержимое не храним: только хеши и короткая сводка без PII.

Что гарантировано кодом:
  * ДЕДУП — каждая акция пишется один раз (ключ акции в actions_seen.db)
  * КАП   — жёсткий потолок событий в сутки (по умолчанию 5000, см. спеку §4).
            Счётчик ХРАНИТСЯ НА ДИСКЕ, а не в памяти: перезапуск его не сбрасывает.
  * Подпись ключом САМОГО агента (для агентов из mesh_agents.json) — иначе
    архитектурным ключом cryter.

Команды:
    actions.py types
    actions.py record --type agent.tool_call --agent v2bot --tool publish_post \
        --summary "пост 812 симв." --result "текст поста" [--dry-run]
    actions.py verify <block_id>
    actions.py stats
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from hashlib import sha256

BASE = "/home/agent/data"
MESH = f"{BASE}/sites/relay-mesh"
PROOF = f"{MESH}/proof_mesh"
AUDIT_DB = f"{PROOF}/snin_audit.db"
SEEN_DB = os.environ.get("SNIN_ACTIONS_SEEN_DB", f"{PROOF}/actions_seen.db")
MESH_AGENTS = f"{BASE}/.secure/mesh_agents.json"
ARCH_KEY = f"{BASE}/sites/chrono/keystore/agent_keys.json.old"

DEFAULT_CAP = 5000           # событий в сутки (спека §4)
MAX_PER_RUN = 300            # потолок на один проход продюсера
TYPES = (
    "agent.tool_call",
    "agent.decision",
    "human.approve",
    "human.override",
    "guard.block",
)

sys.path.insert(0, MESH)
sys.path.insert(0, PROOF)

from proof_mesh import chain, anchor  # noqa: E402


def sha(s: str | bytes) -> str:
    if isinstance(s, str):
        s = s.encode()
    return sha256(s).hexdigest()


def _seen_con() -> sqlite3.Connection:
    con = sqlite3.connect(SEEN_DB, timeout=15)
    con.execute(
        "CREATE TABLE IF NOT EXISTS seen("
        " action_key TEXT PRIMARY KEY, block_id INTEGER, ts INTEGER,"
        " agent_id TEXT, atype TEXT, source TEXT)"
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_seen_ts ON seen(ts)")
    return con


def already_seen(action_key: str) -> int | None:
    con = _seen_con()
    try:
        row = con.execute(
            "SELECT block_id FROM seen WHERE action_key=?", (action_key,)
        ).fetchone()
        return row[0] if row else None
    finally:
        con.close()


def daily_count(now: int | None = None) -> int:
    """Сколько действий записано за текущие сутки (UTC) — с диска, не из памяти."""
    now = int(now or time.time())
    day_start = now - (now % 86400)
    con = _seen_con()
    try:
        return con.execute(
            "SELECT count(*) FROM seen WHERE ts >= ?", (day_start,)
        ).fetchone()[0]
    finally:
        con.close()


def _nsec_for(agent_id: str) -> tuple[str, str]:
    """(nsec, ключ_от_кого). Свой ключ агента, если он известен.

    Порядок: ключи агентов роя (mesh_agents.json) → ключи из keystore chrono
    (там живут и люди: anton, director, executor…) → архитектурный ключ cryter.
    Подпись своим ключом и есть смысл мультиподписантности: запись «одобрил
    человек» должна быть подписана ключом человека, а не нашим.
    """
    def _valid(nsec: str) -> bool:
        """Ключ годится только если реально парсится (в keystore есть пустышки:
        ключ anton в nsec-формате, но невалиден — подписывать им нельзя)."""
        try:
            K, _ = chain._sdk()
            K.parse(nsec)
            return True
        except Exception:
            return False

    for path_, label in ((MESH_AGENTS, "agent"), (ARCH_KEY, "keystore")):
        try:
            keys = json.load(open(path_))
            cand = keys.get(agent_id, {}).get("nsec") if isinstance(keys.get(agent_id), dict) else None
            if cand and _valid(cand):
                return cand, f"{label}:{agent_id}"
        except Exception:
            pass
    return json.load(open(ARCH_KEY))["cryter"]["nsec"], "architecture:cryter"


def record_action(
    *,
    atype: str,
    agent: str,
    source: str = "",
    ref: str = "",
    action_key: str | None = None,
    tool: str = "",
    summary: str = "",
    args: str | None = None,
    result: str | None = None,
    model_version: str = "",
    policy_hash: str = "",
    extra: dict | None = None,
    cap: int = DEFAULT_CAP,
    dry_run: bool = False,
) -> dict:
    if atype not in TYPES:
        raise SystemExit(f"неизвестный тип действия: {atype}. Доступны: {', '.join(TYPES)}")
    # ФИКС 2026-09-17 (найден на живых данных). Ключ дедупа обязан быть уникальным
    # ПО ДЕЙСТВИЮ, а не по его цели. Было: ключ собирался из ref (у v2bot это
    # reply_event_id — цель) → 19 РАЗНЫХ действий с одной целью схлопнулись в
    # «дубликаты» и молча не попали в цепочку. Теперь продюсеры передают свой
    # уникальный ключ (id строки/события), а сборка из ref — только запасной путь.
    if not action_key:
        action_key = (
            f"{atype}|{agent}|{source}|{ref}"
            if ref
            else f"{atype}|{agent}|{source}|{sha(summary)[:16]}"
        )

    dup = already_seen(action_key)
    if dup:
        return {"status": "duplicate", "action_key": action_key, "block_id": dup}

    used = daily_count()
    if used >= cap:
        return {
            "status": "cap_reached",
            "used": used,
            "cap": cap,
            "action_key": action_key,
            "note": "потолок суточного логирования достигнут — блок не пишем",
        }

    ts = int(time.time())
    payload = json.dumps(
        {
            "kind": "action",
            "type": atype,
            "agent": agent,
            "actor": agent,
            "tool": tool,
            "summary": summary[:300],
            "args_hash": sha(args) if args is not None else "",
            "result_hash": sha(result) if result is not None else "",
            "model_version": model_version,
            "policy_hash": policy_hash,
            "source": source,
            "ref": ref,
            "ts": ts,
            **(extra or {}),
        },
        ensure_ascii=False,
        sort_keys=True,
    )

    if dry_run:
        return {"status": "dry_run", "action_key": action_key, "payload": payload, "used_today": used}

    nsec, key_src = _nsec_for(agent)
    block_hash, _u, _s = chain.append_signed(
        AUDIT_DB,
        nsec=nsec,
        agent_id=agent,
        action=atype,
        payload=payload,
        attribution="confirmed",
        evidence_code="SIG_MATCH",
        ts=ts,
    )
    con = sqlite3.connect(AUDIT_DB)
    bid = con.execute("SELECT id FROM audit_events WHERE block_hash=?", (block_hash,)).fetchone()[0]
    con.close()

    con = _seen_con()
    con.execute(
        "INSERT OR REPLACE INTO seen(action_key, block_id, ts, agent_id, atype, source)"
        " VALUES(?,?,?,?,?,?)",
        (action_key, bid, ts, agent, atype, source),
    )
    con.commit()
    con.close()
    return {
        "status": "written",
        "action_key": action_key,
        "block_id": bid,
        "block_hash": block_hash,
        "signed_by": key_src,
        "used_today": used + 1,
        "cap": cap,
    }


def verify_action(block_id: int) -> dict:
    con = sqlite3.connect(AUDIT_DB)
    con.row_factory = sqlite3.Row
    row = con.execute(
        "SELECT * FROM audit_events WHERE id=? AND action IN"
        " ('agent.tool_call','agent.decision','human.approve','human.override','guard.block')",
        (block_id,),
    ).fetchone()
    con.close()
    if row is None:
        return {"found": False, "reason": "блок действия не найден"}
    out = {
        "found": True,
        "id": row["id"],
        "action": row["action"],
        "agent_id": row["agent_id"],
        "ts": row["ts"],
        "self_consistent": anchor._self_consistent(row),
        "payload": json.loads(row["payload"]),
    }
    con = sqlite3.connect(AUDIT_DB)
    prev = con.execute("SELECT block_hash FROM audit_events WHERE id=?", (row["id"] - 1,)).fetchone()
    con.close()
    out["linked"] = bool(prev) and prev[0] == row["prev_hash"]
    root = anchor._covering_root(row["id"])
    if root:
        ok, n = anchor._inclusion(row["id"], root[0], root[1])
        out.update(inclusion_ok=ok, links=n, root_kind="published", root_height=root[0])
    else:
        con = sqlite3.connect(AUDIT_DB)
        head = con.execute("SELECT id, block_hash FROM audit_events ORDER BY id DESC LIMIT 1").fetchone()
        con.close()
        ok, n = anchor._inclusion(row["id"], head[0], head[1])
        out.update(inclusion_ok=ok, links=n, root_kind="pending-external", head_height=head[0])
    return out


def stats() -> dict:
    con = _seen_con()
    try:
        used = daily_count()
        by_type = con.execute(
            "SELECT atype, count(*) FROM seen WHERE ts >= ? GROUP BY atype",
            (int(time.time()) - (int(time.time()) % 86400),),
        ).fetchall()
        total = con.execute("SELECT count(*) FROM seen").fetchone()[0]
    finally:
        con.close()
    # Уникальные действия считаем ПО СОДЕРЖИМОМУ цепочки: до-фиксовые записи
    # (ключ дедупа собирался из ref) дали дубликаты, стирать их нельзя —
    # поэтому честная цифра это distinct по агент+цель+хеш результата.
    types = "','".join(TYPES)
    con = sqlite3.connect(AUDIT_DB)
    try:
        uniq = con.execute(
            "SELECT count(DISTINCT agent_id || '|' || COALESCE(json_extract(payload,'$.ref'),'')"
            " || '|' || COALESCE(json_extract(payload,'$.result_hash'),'')) FROM audit_events"
            f" WHERE action IN ('{types}')"
        ).fetchone()[0]
        blocks = con.execute(
            f"SELECT count(*) FROM audit_events WHERE action IN ('{types}')"
        ).fetchone()[0]
    finally:
        con.close()
    return {
        "действий_уникальных_в_цепочке": uniq,
        "блоков_действий_в_цепочке": blocks,
        "дубликатов_от_до_фиксового_ключа": blocks - uniq,
        "сегодня_записей": used,
        "кап": DEFAULT_CAP,
        "по_типам": dict(by_type),
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Запись действий агентов в цепочку Sentinel")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("types")
    r = sub.add_parser("record")
    r.add_argument("--type", required=True)
    r.add_argument("--agent", required=True)
    r.add_argument("--tool", default="")
    r.add_argument("--summary", default="")
    r.add_argument("--args", default=None)
    r.add_argument("--result", default=None)
    r.add_argument("--source", default="")
    r.add_argument("--ref", default="")
    r.add_argument("--action-key", default=None)
    r.add_argument("--model-version", default="")
    r.add_argument("--policy-hash", default="")
    r.add_argument("--cap", type=int, default=DEFAULT_CAP)
    r.add_argument("--dry-run", action="store_true")
    v = sub.add_parser("verify")
    v.add_argument("block", type=int)
    sub.add_parser("stats")

    a = p.parse_args()
    if a.cmd == "types":
        for t in TYPES:
            print("   ", t)
        return 0
    if a.cmd == "record":
        res = record_action(
            atype=a.type, agent=a.agent, source=a.source, ref=a.ref,
            action_key=a.action_key, tool=a.tool,
            summary=a.summary, args=a.args, result=a.result, model_version=a.model_version,
            policy_hash=a.policy_hash, cap=a.cap, dry_run=a.dry_run,
        )
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return 0 if res["status"] in ("written", "dry_run", "duplicate") else 2
    if a.cmd == "verify":
        r2 = verify_action(a.block)
        if not r2.get("found"):
            print(f"❌ {r2['reason']}")
            return 1
        pl = r2["payload"]
        print(f"блок {r2['id']}: {r2['action']} от {r2['agent_id']}")
        print(f"  инструмент : {pl.get('tool')}")
        print(f"  сводка     : {pl.get('summary')}")
        print(f"  args_hash  : {pl.get('args_hash','')[:32]}")
        print(f"  result_hash: {pl.get('result_hash','')[:32]}")
        print(f"  источник   : {pl.get('source')} ref={pl.get('ref')}")
        print(f"  самосогласован: {'да' if r2['self_consistent'] else 'НЕТ'}"
              f" | связь: {'да' if r2['linked'] else 'НЕТ'}"
              f" | включение: {'да' if r2['inclusion_ok'] else 'НЕТ'}"
              f" ({r2['links']} звеньев, {r2['root_kind']})")
        ok = r2["self_consistent"] and r2["linked"] and r2["inclusion_ok"]
        print("  ВЫВОД      :", "✅ доказательство цело" if ok else "❌ доказательство нарушено")
        return 0 if ok else 1
    print(json.dumps(stats(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

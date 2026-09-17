#!/usr/bin/env python3
"""action_producers.py — продюсеры действий агентов (фаза 2).

Читает ДЕЙСТВИЯ из живых источников агентов и отдаёт их в actions.py для
подписи и записи в цепочку.

ЖЁСТКОЕ ПРАВИЛО (спека §2): читаем ТОЛЬКО НА ЧТЕНИЕ (mode=ro). В контур
управления агентами не пишем ничего, в их базы не вставляем.

Что гарантировано кодом:
  * ДЕДУП — ключ акции (агент+источник+id события), повторно не пишется
  * ОКНО  — просматриваем последние 2 суток (поздние записи не теряются)
  * ПОТОЛКИ — не более --limit за проход и не более --cap за сутки (диск!)
  * --dry-run — печатает, что бы записал, и НИЧЕГО не пишет

Команды:
    action_producers.py sources            # список источников и объём за 2 суток
    action_producers.py scan --dry-run     # примерка
    action_producers.py scan --limit 100   # реальная запись
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

BASE = "/home/agent/data"
MESH = f"{BASE}/sites/relay-mesh"
PROOF = f"{MESH}/proof_mesh"
CRYTER_DB = f"{BASE}/agents/core/cryter/data/cryter_v7.db"
V2BOT_DB = f"{BASE}/sites/v2bot-daemon/data/v2bot.db"

sys.path.insert(0, MESH)
sys.path.insert(0, PROOF)

from proof_mesh import actions  # noqa: E402


def _q(db: str, sql: str) -> list[sqlite3.Row]:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=15)
    con.row_factory = sqlite3.Row
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


SOURCES = [
    {
        "name": "cryter.posts",
        "db": CRYTER_DB,
        "agent": "cryter",
        "tool": "publish_post",
        "atype": "agent.tool_call",
        "sql": "SELECT event_id, created_at, content, status FROM nostr_posts"
               " WHERE created_at > datetime('now','-2 day') ORDER BY id",
        "key": lambda r: f"cryter:nostr_posts:{r['event_id']}",
        "ref": lambda r: str(r["event_id"] or ""),
        "result": lambda r: r["content"] or "",
        "summary": lambda r: f"публикация в Nostr, {len(r['content'] or '')} симв., статус {r['status']}",
    },
    {
        "name": "cryter.comments",
        "db": CRYTER_DB,
        "agent": "cryter",
        "tool": "reply_to_comment",
        "atype": "agent.tool_call",
        "sql": "SELECT comment_event_id, reply_event_id, timestamp FROM processed_comments"
               " WHERE timestamp > strftime('%s','now')-172800 ORDER BY rowid",
        "key": lambda r: f"cryter:comment:{r['comment_event_id']}",
        "ref": lambda r: str(r["comment_event_id"] or ""),
        "result": lambda r: str(r["reply_event_id"] or ""),
        "summary": lambda r: "ответ на комментарий",
    },
    {
        "name": "v2bot.posts",
        "db": V2BOT_DB,
        "agent": "v2bot",
        "tool": "publish_post",
        "atype": "agent.tool_call",
        "sql": "SELECT event_id, created_at, lang, topic_id, char_count, content FROM posts"
               " WHERE created_at > datetime('now','-2 day') ORDER BY id",
        "key": lambda r: f"v2bot:post:{r['event_id']}",
        "ref": lambda r: str(r["event_id"] or ""),
        "result": lambda r: r["content"] or "",
        "summary": lambda r: f"публикация ({r['lang']}, тема {r['topic_id']}, {r['char_count']} симв.)",
    },
    {
        "name": "v2bot.interactions",
        "db": V2BOT_DB,
        "agent": "v2bot",
        "tool": "interact",
        "atype": "agent.tool_call",
        "sql": "SELECT id, reply_event_id, our_event_id, action, kind, content, created_at"
               " FROM interactions WHERE created_at > datetime('now','-2 day') ORDER BY id",
        "key": lambda r: f"v2bot:interaction:{r['id']}",
        "ref": lambda r: str(r["reply_event_id"] or ""),
        "result": lambda r: str(r["our_event_id"] or ""),
        "summary": lambda r: f"взаимодействие {r['action']} (kind {r['kind']})",
    },
]


def _pick(src: dict, limit: int | None = None) -> list[dict]:
    rows = _q(src["db"], src["sql"])
    out = []
    for r in rows[: limit or len(rows)]:
        out.append(
            {
                "source": src["name"],
                "agent": src["agent"],
                "tool": r["action"] if src["name"] == "v2bot.interactions" and "action" in r.keys()
                        else src["tool"],
                "atype": src["atype"],
                "key": src["key"](r),
                "ref": src["ref"](r),
                "result": src["result"](r),
                "summary": src["summary"](r),
            }
        )
    return out


def cmd_sources() -> int:
    for src in SOURCES:
        try:
            rows = _q(src["db"], src["sql"])
            print(f"    {src['name']:<20} агент={src['agent']:<8} за 2 суток: {len(rows)}")
        except Exception as e:
            print(f"    {src['name']:<20} ОШИБКА чтения: {e}")
    print(f"    дедуп-база: {actions.SEEN_DB}")
    print(f"    потолок: {actions.DEFAULT_CAP}/сутки, на проход {actions.MAX_PER_RUN}")
    return 0


def cmd_scan(only: str, limit: int, cap: int, dry_run: bool) -> int:
    picks: list[dict] = []
    for src in SOURCES:
        if only and src["name"] != only:
            continue
        try:
            picks += _pick(src)
        except Exception as e:
            print(f"    ⚠️ {src['name']}: {e}")
    picks = picks[:limit]

    written = dup = capped = 0
    for p in picks:
        res = actions.record_action(
            atype=p["atype"], agent=p["agent"], source=p["source"], ref=p["ref"],
            action_key=p["key"], tool=p["tool"], summary=p["summary"], result=p["result"],
            cap=cap, dry_run=dry_run,
        )
        if res["status"] == "written":
            written += 1
            print(f"    ✅ {res['block_id']} {p['source']} {p['tool']} {res['signed_by']} — {p['summary'][:60]}")
        elif res["status"] == "dry_run":
            written += 1
        elif res["status"] == "duplicate":
            dup += 1
        elif res["status"] == "cap_reached":
            capped += 1
            if capped == 1:
                print(f"    ⛔ потолок: {res['used']}/{res['cap']} за сутки — дальше не пишем")
    print(
        json.dumps(
            {
                "режим": "примерка (ничего не записано)" if dry_run else "запись",
                "кандидатов": len(picks),
                "записано": None if dry_run else written,
                "в_примерке_прошло_бы": written if dry_run else None,
                "дубликатов": dup,
                "упёрлось_в_потолок": capped,
                "сегодня_всего": actions.daily_count(),
            },
            ensure_ascii=False,
        )
    )
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Продюсеры действий агентов → цепочка")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("sources")
    s = sub.add_parser("scan")
    s.add_argument("--source", default="")
    s.add_argument("--limit", type=int, default=actions.MAX_PER_RUN)
    s.add_argument("--cap", type=int, default=actions.DEFAULT_CAP)
    s.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    if a.cmd == "sources":
        return cmd_sources()
    return cmd_scan(a.source, a.limit, a.cap, a.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())

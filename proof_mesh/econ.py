"""
SNIN PROOF MESH — Фаза 3: экономика роя (econ.py).

Наблюдение за платежами агентов через Nostr:
  - kind 9735 (zap receipt): отправитель (тег zap), получатель (тег p),
    сумма (декодируется из bolt11), event_id, релей
  - kind 9734 (zap request = «счёт»): кому выставлен счёт
  - kind 0 → lud16/lud06 → wallet_profiles (LN-адреса)

Агрегаты за период: кто кому платил, суммы, топ LN-адресов,
счета выставлены/получено.

NWC (nwc_config в snin_client.db) — НЕ заполняется без NWC-токена
(нужно согласование с пользователем; здесь только задел под Фазу 5).

Done-when Ф3: реальные zap-события с релеев → payment_events (не
заглушки); выборка «кто кому платил за 7 дней»; pytest test_econ.py
(live или дамп).

Спека: SNIN_PROOF_MESH_SPEC.md, Фаза 3.
"""

import json
import re
import sqlite3
import time
from pathlib import Path

# Проверенные релеи (спека Ф3)
RELAYS = [
    "wss://relay.primal.net",
    "wss://relay.damus.io",
    "wss://nos.lol",
    # наши релеи (если живы — читаем и с них)
    "ws://127.0.0.1:8197",
]

FETCH_LIMIT = 800        # событий на релей
DEFAULT_SINCE_DAYS = 14  # глубина первого скана

_BOLT11_RE = re.compile(r"^lnbc(?:l)?(\d+)([pnum])?")


def decode_bolt11_msat(bolt11: str) -> int:
    """Минимальный декодер суммы из bolt11 (только сумма, без подписи).
    p=1e-12 BTC, n=1e-9, u=1e-6, m=1e-3; 1 BTC = 1e11 msat."""
    if not bolt11:
        return 0
    m = _BOLT11_RE.match(bolt11)
    if not m:
        return 0
    digits = int(m.group(1))
    mult = m.group(2) or ""
    factor = {"p": 1e-1, "n": 1e2, "u": 1e5, "m": 1e8}.get(mult, 1e11)
    return int(round(digits * factor))


def parse_zap_receipt(ev: dict) -> dict | None:
    """9735 → {sender_pub, receiver_pub, amount_msat, bolt11, event_id, ts}."""
    tags = {t[0]: (t[1] if len(t) > 1 else "") for t in ev.get("tags", []) if t}
    bolt11 = tags.get("bolt11", "")
    amount = decode_bolt11_msat(bolt11)
    if not amount:
        return None
    sender = tags.get("zap", "") or ev.get("pubkey", "")
    receiver = tags.get("p", "")
    return {
        "sender_pub": sender,
        "receiver_pub": receiver,
        "amount_msat": amount,
        "bolt11": bolt11[:80],
        "event_id": ev.get("id", ""),
        "ts": int(ev.get("created_at", 0)),
    }


def parse_zap_request(ev: dict) -> dict | None:
    """9734 (счёт) → {receiver_pub (кому счёт), amount_msat, event_id, ts}."""
    tags = {t[0]: (t[1] if len(t) > 1 else "") for t in ev.get("tags", []) if t}
    amount = int(tags.get("amount", 0) or 0)
    if amount <= 0:
        return None
    receiver = tags.get("p", "")
    return {
        "sender_pub": ev.get("pubkey", ""),   # кто выставил счёт
        "receiver_pub": receiver,             # кому
        "amount_msat": amount,
        "event_id": ev.get("id", ""),
        "ts": int(ev.get("created_at", 0)),
    }


def parse_metadata(ev: dict) -> dict | None:
    """kind 0 → {pubkey, lud16, lud06}."""
    try:
        meta = json.loads(ev.get("content", "{}"))
    except (json.JSONDecodeError, TypeError):
        return None
    lud16 = meta.get("lud16", "") or ""
    lud06 = meta.get("lud06", "") or ""
    if not (lud16 or lud06):
        return None
    return {"pubkey": ev.get("pubkey", ""), "lud16": lud16, "lud06": lud06}


def fetch_kinds(relays: list[str], kinds: list[int], since: int,
                limit: int = FETCH_LIMIT, timeout: int = 10) -> list[dict]:
    """Прочитать события kinds с релеев (REQ/EOSE, по образцу scam_radar)."""
    import websocket
    out: list[dict] = []
    req = json.dumps(["REQ", "econscan",
                      {"kinds": kinds, "since": since, "limit": limit}])
    for url in relays:
        try:
            ws = websocket.create_connection(url, timeout=timeout)
        except Exception as e:
            print(f"[econ] {url}: недоступен ({type(e).__name__})", flush=True)
            continue
        try:
            ws.send(req)
            while True:
                try:
                    msg = json.loads(ws.recv())
                except Exception:
                    break
                if not isinstance(msg, list) or len(msg) < 2:
                    continue
                if msg[0] == "EVENT":
                    ev = msg[2]
                    ev["_relay"] = url
                    out.append(ev)
                elif msg[0] == "EOSE":
                    break
        finally:
            try:
                ws.close()
            except Exception:
                pass
    # дедуп по id
    seen: set[str] = set()
    deduped = []
    for ev in out:
        eid = ev.get("id", "")
        if eid in seen:
            continue
        seen.add(eid)
        deduped.append(ev)
    return deduped


# ── БД ──────────────────────────────────────────────────────────────────────

def store_payments(db_path: str, payments: list[dict], kind: int) -> int:
    """Вставить 9735/9734 в payment_events (дедуп по event_id)."""
    n = 0
    with sqlite3.connect(db_path) as c:
        for p in payments:
            if not p.get("event_id"):
                continue
            cur = c.execute(
                "SELECT 1 FROM payment_events WHERE event_id=? AND kind=?",
                (p["event_id"], kind),
            )
            if cur.fetchone():
                continue
            c.execute(
                """INSERT INTO payment_events
                   (ts, kind, sender_pub, receiver_pub, amount_msat, event_id, relay)
                   VALUES (?,?,?,?,?,?,?)""",
                (p["ts"], kind, p.get("sender_pub", ""), p.get("receiver_pub", ""),
                 p.get("amount_msat", 0), p["event_id"], ""),
            )
            n += 1
    return n


def upsert_wallets(db_path: str, profiles: list[dict]) -> int:
    n = 0
    now = int(time.time())
    with sqlite3.connect(db_path) as c:
        for pr in profiles:
            if not pr.get("pubkey"):
                continue
            c.execute(
                """INSERT INTO wallet_profiles (pubkey, lud16, lud06, first_seen, last_seen)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(pubkey) DO UPDATE SET
                     lud16=excluded.lud16, lud06=excluded.lud06, last_seen=excluded.last_seen""",
                (pr["pubkey"], pr.get("lud16", ""), pr.get("lud06", ""), now, now),
            )
            n += 1
    return n


def fetch_metadata(relays: list[str], pubkeys: list[str],
                   timeout: int = 10) -> list[dict]:
    """Прочитать kind 0 для конкретных авторов (authors-фильтр — надёжнее
    массового since-скана, релеи отдают метаданные точечно)."""
    import websocket
    if not pubkeys:
        return []
    out: list[dict] = []
    chunk = list(pubkeys)[:100]  # лимит фильтра на запрос
    req = json.dumps(["REQ", "econmeta", {"kinds": [0], "authors": chunk, "limit": 200}])
    for url in relays:
        try:
            ws = websocket.create_connection(url, timeout=timeout)
        except Exception:
            continue
        try:
            ws.send(req)
            while True:
                try:
                    msg = json.loads(ws.recv())
                except Exception:
                    break
                if not isinstance(msg, list) or len(msg) < 2:
                    continue
                if msg[0] == "EVENT":
                    out.append(msg[2])
                elif msg[0] == "EOSE":
                    break
        finally:
            try:
                ws.close()
            except Exception:
                pass
    seen: set[str] = set()
    deduped = []
    for ev in out:
        if ev.get("id") in seen:
            continue
        seen.add(ev.get("id"))
        deduped.append(ev)
    return deduped


def sync_econ(db_path: str, relays: list[str] | None = None,
              since: int | None = None) -> dict:
    """Полный цикл: 9735+9734 → payment_events; kind 0 → wallet_profiles."""
    relays = relays or RELAYS
    since = since or int(time.time()) - DEFAULT_SINCE_DAYS * 86400
    zaps = fetch_kinds(relays, [9735], since)
    reqs = fetch_kinds(relays, [9734], since)
    pay_zaps = [p for p in (parse_zap_receipt(e) for e in zaps) if p]
    pay_reqs = [p for p in (parse_zap_request(e) for e in reqs) if p]
    n_zaps = store_payments(db_path, pay_zaps, 9735)
    n_reqs = store_payments(db_path, pay_reqs, 9734)
    # kind 0 для получателей, у которых нет кошелька (точечный authors-фильтр)
    pubkeys = {p["receiver_pub"] for p in pay_zaps} | {p["receiver_pub"] for p in pay_reqs}
    with sqlite3.connect(db_path) as c:
        known = {r[0] for r in c.execute(
            "SELECT pubkey FROM wallet_profiles WHERE lud16 != '' OR lud06 != ''")}
    missing = [pk for pk in pubkeys if pk and pk not in known]
    n_wal = 0
    if missing:
        metas = fetch_metadata(relays, missing)
        profiles = [p for p in (parse_metadata(e) for e in metas)
                    if p and p["pubkey"] in set(missing)]
        n_wal = upsert_wallets(db_path, profiles)
    return {"zaps_seen": len(zaps), "zaps_stored": n_zaps,
            "reqs_seen": len(reqs), "reqs_stored": n_reqs,
            "wallets_updated": n_wal}


# ── агрегаты ────────────────────────────────────────────────────────────────

def aggregate(db_path: str, days: int = 7) -> dict:
    """«Кто кому платил за N дней»: суммы, топ LN-адресов, счета."""
    since = int(time.time()) - days * 86400
    with sqlite3.connect(db_path) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute(
            """SELECT * FROM payment_events WHERE ts >= ? AND kind=9735""",
            (since,),
        ).fetchall()
        inv = c.execute(
            """SELECT * FROM payment_events WHERE ts >= ? AND kind=9734""",
            (since,),
        ).fetchall()
    by_pair: dict[tuple[str, str], int] = {}
    for r in rows:
        key = (r["sender_pub"] or "?", r["receiver_pub"] or "?")
        by_pair[key] = by_pair.get(key, 0) + r["amount_msat"]
    by_receiver: dict[str, int] = {}
    for r in rows:
        by_receiver[r["receiver_pub"] or "?"] = \
            by_receiver.get(r["receiver_pub"] or "?", 0) + r["amount_msat"]
    pairs = sorted(by_pair.items(), key=lambda kv: -kv[1])[:20]
    receivers = sorted(by_receiver.items(), key=lambda kv: -kv[1])[:10]
    # LN-адреса для топ-получателей
    with sqlite3.connect(db_path) as c:
        ln = {r[0]: r[1] for r in c.execute(
            "SELECT pubkey, lud16 FROM wallet_profiles WHERE lud16 != ''")}
    return {
        "period_days": days,
        "total_zaps": len(rows),
        "total_msat": sum(r["amount_msat"] for r in rows),
        "invoices_issued": len(inv),
        "top_pairs": [{"sender": s, "receiver": r, "amount_msat": a}
                      for (s, r), a in pairs],
        "top_receivers": [{"pubkey": r, "amount_msat": a,
                           "ln_address": ln.get(r, "")} for r, a in receivers],
    }


def fmt_msat(msat: int) -> str:
    """msat → sat (1 sat = 1000 msat)."""
    return f"{msat / 1000:,.0f} sat"

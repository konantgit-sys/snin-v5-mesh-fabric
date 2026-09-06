#!/usr/bin/env python3
"""Sentinel dash history v2 — генератор history.json для графиков цепочки.

Каждые 5 минут (cron). Вытаскивает из snin_audit.db:
  - сертификаты: 24ч (все), 7д (прореж.), 30д (прореж.) — рост высоты
  - события: по часам 48ч, по дням 30д
  - непрерывность: разрывы >3ч за 30д, текущий аптайм
  - агенты: всего / за 24ч / доля % / последняя активность / топ действий
  - последние звенья цепочки (prev_hash → block_hash) для визуализации блоков
  - экономика: топ получателей zap-ов (по сумме)
  - сводка: первое событие, дней в работе, всего событий
Лог: /home/agent/data/backups/monitor/sentinel_history.log
"""
import json
import os
import sqlite3
import time
import urllib.request
from collections import Counter, defaultdict

AUDIT_DB = "/home/agent/data/sites/relay-mesh/proof_mesh/snin_audit.db"
RELAY_DB = "/home/agent/data/sites/relay-mesh/data/relay_health.db"
OUT = "/home/agent/data/sites/sentinel-dash/history.json"
LOG = "/home/agent/data/backups/monitor/sentinel_history.log"

NOW = int(time.time())
H = 3600
D = 86400


def log(msg):
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    with open(LOG, "a") as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}\n")


def thin(points, target=280):
    """Прореживание списка dict до ~target точек (сохраняя первую и последнюю)."""
    if len(points) <= target:
        return points
    step = len(points) / target
    out = []
    for i in range(target - 1):
        out.append(points[int(i * step)])
    out.append(points[-1])
    return out


def main():
    db = sqlite3.connect(AUDIT_DB, timeout=15)
    db.row_factory = sqlite3.Row

    # ── Релеи: здоровье (relay_health.db) ──
    relays = []
    try:
        rdb = sqlite3.connect(RELAY_DB, timeout=10)
        rdb.row_factory = sqlite3.Row
        for r in rdb.execute("SELECT * FROM relays ORDER BY score DESC, latency_ms ASC"):
            host = (r["url"] or "").replace("wss://", "").replace("ws://", "").split("/")[0]
            alive_ts = r["last_alive"] or 0
            age = NOW - alive_ts if alive_ts else None
            if alive_ts == 0 or (age is not None and age > 6 * H):
                status = "dead" if alive_ts == 0 else "stale"
            else:
                status = "ok"
            relays.append({
                "u": host or r["url"], "full": r["url"],
                "lat": round(r["latency_ms"] or 0), "score": round(r["score"] or 0),
                "ok": r["success_count"] or 0, "fail": r["fail_count"] or 0,
                "cf": r["consecutive_fails"] or 0, "tier": r["tier"] or "unknown",
                "status": status, "checked": int(r["last_check"] or 0), "alive": int(alive_ts or 0),
            })
        rdb.close()
    except Exception as e:
        log(f"relay_health db err: {e}")

    # ── Supervisor :9909 ──
    sup = None
    try:
        with urllib.request.urlopen("http://localhost:9909/health", timeout=4) as resp:
            sup = json.loads(resp.read().decode())
    except Exception as e:
        log(f"supervisor err: {e}")
    sup_status = None
    if sup and isinstance(sup, dict) and "alive" in sup:
        sup_status = {"alive": sup.get("alive", 0), "total": sup.get("total", 0),
                      "dead": sup.get("dead", 0), "restarts": sup.get("total_restarts", 0)}

    # ── Сертификаты ──
    certs_24 = db.execute(
        "SELECT ts, height, root FROM cert_state WHERE kind=8010 AND ts > ? ORDER BY ts",
        (NOW - D,),
    ).fetchall()
    certs_7 = db.execute(
        "SELECT ts, height, root FROM cert_state WHERE kind=8010 AND ts > ? ORDER BY ts",
        (NOW - 7 * D,),
    ).fetchall()
    certs_30 = db.execute(
        "SELECT ts, height, root FROM cert_state WHERE kind=8010 AND ts > ? ORDER BY ts",
        (NOW - 30 * D,),
    ).fetchall()
    mk = lambda rows: [{"ts": c["ts"], "h": c["height"], "r": c["root"]} for c in rows]
    certs_full_7 = mk(certs_7)
    # 24ч — все точки; 7д/30д — прореженные
    cert_series = {
        "d1": mk(certs_24),
        "d7": thin(certs_full_7),
        "d30": thin(mk(certs_30)),
    }

    # ── События: часы 48ч, дни 30д ──
    evs_48 = db.execute(
        "SELECT ts FROM audit_events WHERE ts > ?", (NOW - 48 * H,)
    ).fetchall()
    hourly = Counter((e["ts"] // H) * H for e in evs_48)
    t0h = ((NOW - 48 * H) // H) * H
    hourly_series = [{"t": t, "n": hourly.get(t, 0)} for t in range(t0h, t0h + 48 * H, H)]

    evs_30 = db.execute(
        "SELECT ts FROM audit_events WHERE ts > ?", (NOW - 30 * D,)
    ).fetchall()
    daily = Counter((e["ts"] // D) * D for e in evs_30)
    t0d = ((NOW - 30 * D) // D) * D
    daily_series = [{"t": t, "n": daily.get(t, 0)} for t in range(t0d, t0d + 30 * D, D)]

    # ── Разрывы >3ч за 30д ──
    all_ts = sorted(e["ts"] for e in evs_30)
    gaps = []
    for i in range(1, len(all_ts)):
        dt = all_ts[i] - all_ts[i - 1]
        if dt > 3 * H:
            gaps.append({"from": all_ts[i - 1], "to": all_ts[i], "hours": round(dt / H, 1)})
    last_ev = all_ts[-1] if all_ts else 0
    uptime_since = gaps[-1]["to"] if gaps else (t0d if days else NOW)
    uptime_hours = round((NOW - uptime_since) / H, 1)

    # ── Агенты ──
    ag = db.execute(
        "SELECT agent_id, count(*) n, max(ts) last_ts FROM audit_events GROUP BY agent_id"
    ).fetchall()
    ag24 = db.execute(
        "SELECT agent_id, count(*) n FROM audit_events WHERE ts > ? GROUP BY agent_id",
        (NOW - 24 * H,),
    ).fetchall()
    ag24map = {r["agent_id"]: r["n"] for r in ag24}
    total_ev = sum(r["n"] for r in ag)
    agents_full = [
        {
            "agent_id": a["agent_id"], "events": a["n"], "pct": round(100.0 * a["n"] / total_ev, 1),
            "e24": ag24map.get(a["agent_id"], 0), "last_ts": a["last_ts"],
        }
        for a in sorted(ag, key=lambda r: -r["n"])
    ]

    # ── Топ действий по агентам за 24ч ──
    act = db.execute(
        "SELECT agent_id, action, count(*) n FROM audit_events WHERE ts > ? GROUP BY agent_id, action",
        (NOW - 24 * H,),
    ).fetchall()
    act_map = defaultdict(list)
    for r in act:
        act_map[r["agent_id"]].append({"action": r["action"], "n": r["n"]})
    actions_24h = {
        a: sorted(v, key=lambda x: -x["n"])[:3] for a, v in act_map.items()
    }

    # ── Последние звенья (блоки) — полные хеши для модалки ──
    blocks = db.execute(
        "SELECT id, ts, agent_id, instance_id, action, payload_hash, prev_hash, block_hash, "
        "signature, evidence_code FROM audit_events ORDER BY id DESC LIMIT 5"
    ).fetchall()
    last_blocks = [
        {
            "id": b["id"], "ts": b["ts"], "agent": b["agent_id"], "action": b["action"],
            "inst": (b["instance_id"] or "")[:20],
            "prev": b["prev_hash"] or "", "hash": b["block_hash"] or "",
            "ph": b["payload_hash"] or "", "sig": (b["signature"] or "")[:20],
            "evc": (b["evidence_code"] or "")[:20],
        }
        for b in reversed(blocks)
    ]

    # ── Журнал (10) ──
    journal_rows = db.execute(
        "SELECT id, ts, agent_id, instance_id, action, payload_hash FROM audit_events ORDER BY id DESC LIMIT 12"
    ).fetchall()
    journal = [
        {"id": r["id"], "ts": r["ts"], "agent": r["agent_id"], "inst": (r["instance_id"] or "")[:14],
         "action": r["action"], "hash": (r["payload_hash"] or "")[:14]}
        for r in reversed(journal_rows)
    ]

    # ── Последние сертификаты (8) ──
    last_certs = db.execute(
        "SELECT ts, height, root, event_id FROM cert_state WHERE kind=8010 ORDER BY ts DESC LIMIT 8"
    ).fetchall()
    cert_list = [
        {"ts": c["ts"], "h": c["height"], "r": c["root"][:18], "eid": (c["event_id"] or "")[:12]}
        for c in reversed(last_certs)
    ]

    # ── Экономика: топ получателей (по сумме, kind 9735) ──
    top_pay = db.execute(
        """SELECT receiver_pub, ln_address, sum(amount_msat) s, count(*) n
           FROM payment_events WHERE ts > ? GROUP BY receiver_pub, ln_address
           ORDER BY s DESC LIMIT 6""",
        (NOW - 30 * D,),
    ).fetchall()
    pay_top = [
        {"who": (r["ln_address"] or r["receiver_pub"][:14]), "sats": round(r["s"] / 1000),
         "n": r["n"]}
        for r in top_pay
    ]

    # ── Zap-ы по дням (30д) + счета ──
    zaps_30 = db.execute(
        "SELECT ts, amount_msat FROM payment_events WHERE kind=9735 AND ts > ?",
        (NOW - 30 * D,),
    ).fetchall()
    zday = defaultdict(lambda: [0, 0])  # ts -> [count, msat]
    for z in zaps_30:
        zday[(z["ts"] // D) * D][0] += 1
        zday[(z["ts"] // D) * D][1] += z["amount_msat"]
    z_t0 = ((NOW - 30 * D) // D) * D
    zaps_daily = [
        {"t": t, "n": zday.get(t, [0, 0])[0], "sats": round(zday.get(t, [0, 0])[1] / 1000)}
        for t in range(z_t0, z_t0 + 30 * D, D)
    ]
    invoices_30 = db.execute(
        "SELECT count(*), coalesce(sum(amount_msat),0) FROM payment_events WHERE kind=9734 AND ts > ?",
        (NOW - 30 * D,),
    ).fetchone()
    econ_last = db.execute(
        "SELECT max(ts) FROM payment_events WHERE kind=9735"
    ).fetchone()[0]

    # ── Сводка ──
    first_row = db.execute("SELECT min(ts) t, count(*) n FROM audit_events").fetchone()
    first_ts = first_row["t"]
    ev24 = sum(1 for e in evs_48 if e["ts"] > NOW - 24 * H)
    n_certs24 = db.execute(
        "SELECT count(*) FROM cert_state WHERE kind=8010 AND ts > ?", (NOW - 24 * H,)
    ).fetchone()[0]
    db.close()

    data = {
        "generated_at": NOW,
        "first_ts": first_ts,
        "total_events": first_row["n"],
        "chain": {
            "certs": cert_series,
            "hourly_48h": hourly_series,
            "daily_30d": daily_series,
            "gaps_30d": gaps,
            "last_event_ts": last_ev,
            "uptime_since_ts": uptime_since,
            "uptime_hours": uptime_hours,
            "events_24h": ev24,
            "certs_24h": n_certs24,
            "avg_events_per_hour_24h": round(ev24 / 24, 1),
            "avg_cert_interval_min": round(24 * 60 / max(n_certs24, 1), 1),
        },
        "agents": agents_full,
        "actions_24h": actions_24h,
        "blocks": last_blocks,
        "journal": journal,
        "certs_last": cert_list,
        "pay_top": pay_top,
        "relays": relays,
        "zaps_daily": zaps_daily,
        "econ_last_ts": econ_last or 0,
        "invoices_30d": {"count": invoices_30[0], "sats": round((invoices_30[1] or 0) / 1000)},
        "supervisor": sup_status,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    tmp = OUT + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, OUT)
    log(f"OK: h24={len(cert_series['d1'])} h7={len(cert_series['d7'])} h30={len(cert_series['d30'])} "
        f"gaps30={len(gaps)} ev24={ev24} agents={len(agents_full)} relays={len(relays)} "
        f"zaps30={sum(1 for _ in zaps_30)} sup={'ok' if sup_status else 'n/a'} uptime={uptime_hours}h")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""sentinel_live_gen.py — живой поток событий для дашборда.

Демон: каждые 15 секунд читает snin_audit.db и пишет live.json
(последние 25 событий + высота + последний сертификат).
Запускается из init.sh при старте пода (идемпотентно), авто-рестарт
при падении — блоком в init.sh каждые 60с через supervisor-style проверку.

Не конфликтует с sentinel_history_gen.py (cron */5) — тот пишет полную
историю для графиков, этот — короткий живой срез.
"""
import json
import os
import sqlite3
import sys
import time

AUDIT_DB = "/home/agent/data/sites/relay-mesh/proof_mesh/snin_audit.db"
OUT = "/home/agent/data/sites/sentinel-dash/live.json"
PIDF = "/home/agent/data/backups/monitor/sentinel_live.pid"
INTERVAL = 15  # сек


def already_running(pid):
    """Жив ли процесс с данным pid (по /proc)."""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            cmd = f.read().decode(errors="ignore")
        return "sentinel_live_gen.py" in cmd
    except Exception:
        return False


def main():
    pid = os.getpid()
    # идемпотентность через pidfile
    try:
        if os.path.exists(PIDF):
            with open(PIDF) as f:
                old = int(f.read().strip())
            if old != pid and already_running(old):
                sys.exit(0)
    except Exception:
        pass
    os.makedirs(os.path.dirname(PIDF), exist_ok=True)
    with open(PIDF, "w") as f:
        f.write(str(pid))

    while True:
        t0 = time.time()
        try:
            db = sqlite3.connect(AUDIT_DB, timeout=10)
            db.row_factory = sqlite3.Row
            rows = db.execute(
                "SELECT id, ts, agent_id, action, payload_hash, block_hash "
                "FROM audit_events ORDER BY id DESC LIMIT 25"
            ).fetchall()
            cert = db.execute(
                "SELECT ts, height, root FROM cert_state WHERE kind=8010 "
                "ORDER BY ts DESC LIMIT 1"
            ).fetchone()
            ch = db.execute(
                "SELECT height, last_hash FROM chain_state LIMIT 1"
            ).fetchone()
            db.close()

            events = [
                {
                    "id": r["id"], "ts": r["ts"], "agent": r["agent_id"],
                    "action": r["action"], "ph": (r["payload_hash"] or "")[:14],
                    "bh": (r["block_hash"] or "")[:14],
                }
                for r in reversed(rows)
            ]
            data = {
                "generated_at": int(time.time()),
                "height": ch["height"] if ch else (rows[0]["id"] if rows else 0),
                "root": (ch["last_hash"] if ch else ""),
                "last_event": {"ts": rows[0]["ts"], "id": rows[0]["id"]} if rows else None,
                "cert": {"ts": cert["ts"], "height": cert["height"], "root": (cert["root"] or "")[:20]} if cert else None,
                "events": events,
            }
            os.makedirs(os.path.dirname(OUT), exist_ok=True)
            tmp = OUT + ".tmp"
            with open(tmp, "w") as f:
                json.dump(data, f)
            os.replace(tmp, OUT)
        except Exception as e:
            try:
                with open("/home/agent/data/backups/monitor/sentinel_live.log", "a") as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} ERR {e}\n")
            except Exception:
                pass

        # интервал от конца обработки (не дрейфует)
        elapsed = time.time() - t0
        time.sleep(max(1.0, INTERVAL - elapsed))


if __name__ == "__main__":
    main()

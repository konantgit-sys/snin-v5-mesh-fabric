#!/usr/bin/env python3
"""Sentinel Chain Watch — контроль непрерывности цепочки Proof Mesh.

Каждые 5 минут (cron, пересоздаётся init.sh) проверяет три вещи:
  1. audit_daemon жив (pgrep proof_mesh/audit_daemon)
  2. Цепочка РАСТЁТ: max(ts) в audit_events не старше 12 минут
  3. Сертификат kind 8010 (публичный корень в Nostr) не старше 15 минут

Логика: supervisor держит ПРОЦЕСС, но не заметит ЗАВИСАНИЕ (процесс жив,
а цепочка стоит). Этот монитор ловит именно зависания.

Алерт: только Octopus-бот (octopus_bot_token.txt), группа -1003797670859.
Дедуп: алерт при ПЕРЕХОДЕ в bad, повтор раз в 60 мин (sticky), «восстановлено»
при возврате в ok. При остановке цепочки — авторестарт audit_daemon.

Лог: /home/agent/data/backups/monitor/chain_watch.log
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import time
import urllib.request

BASE = "/home/agent/data"
AUDIT_DB = "/home/agent/data/sites/relay-mesh/proof_mesh/snin_audit.db"
MESH_DIR = "/home/agent/data/sites/relay-mesh"
TOKEN_FILE = os.path.join(BASE, "octopus_bot_token.txt")
CHAT_ID = "-1003797670859"
STATE_FILE = os.path.join(BASE, "backups", "monitor", "chain_state.json")
LOG_FILE = os.path.join(BASE, "backups", "monitor", "chain_watch.log")
REALERT_SEC = 3600

# Пороги свежести (минут). Обычный темп: событие ~1-2 мин, сертификат ~10 мин.
EVENT_MAX_AGE = 12
CERT_MAX_AGE = 16


def log(msg: str):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")
    print(line)


def load_state() -> dict:
    try:
        return json.load(open(STATE_FILE))
    except Exception:
        return {"bad_since": None, "last_alert": 0}


def save_state(st: dict):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(st, f)


def proc_alive(sig: str) -> bool:
    try:
        out = subprocess.run(["pgrep", "-f", sig], capture_output=True, text=True, timeout=10)
        return out.returncode == 0 and out.stdout.strip() != ""
    except Exception:
        return False


def restart_audit() -> bool:
    """Рестарт audit_daemon (после падения или зависания)."""
    try:
        subprocess.run(["pkill", "-f", "proof_mesh/audit_daemon"], timeout=10)
        time.sleep(2)
        cmd = f"cd {MESH_DIR} && nohup python3 proof_mesh/audit_daemon.py >> {MESH_DIR}/logs/audit_daemon.log 2>&1 &"
        subprocess.Popen(["bash", "-c", cmd])
        time.sleep(3)
        ok = proc_alive("proof_mesh/audit_daemon")
        log(f"🔄 Рестарт audit_daemon: {'OK' if ok else 'НЕ ПОДНЯЛСЯ'}")
        return ok
    except Exception as e:
        log(f"Рестарт audit_daemon ошибка: {e}")
        return False


def chain_freshness() -> dict:
    """Возвращает: last_event_age_min, last_cert_age_min, ok."""
    now = time.time()
    try:
        db = sqlite3.connect(AUDIT_DB, timeout=10)
        row = db.execute("SELECT max(ts) FROM audit_events").fetchone()
        last_event_ts = row[0] if row and row[0] else 0
        row = db.execute(
            "SELECT max(ts) FROM cert_state WHERE kind IN (8010, 30000)"
        ).fetchone()
        last_cert_ts = row[0] if row and row[0] else 0
        db.close()
        ev_age = (now - last_event_ts) / 60.0 if last_event_ts else 9999
        cert_age = (now - last_cert_ts) / 60.0 if last_cert_ts else 9999
        return {
            "event_age_min": round(ev_age, 1),
            "cert_age_min": round(cert_age, 1),
            "ok": ev_age <= EVENT_MAX_AGE and cert_age <= CERT_MAX_AGE,
        }
    except Exception as e:
        return {"event_age_min": None, "cert_age_min": None, "ok": False, "error": str(e)}


def send_tg(text: str) -> bool:
    try:
        token = open(TOKEN_FILE).read().strip()
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = json.dumps({
            "chat_id": CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        }).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.load(r).get("ok", False)
    except Exception as e:
        log(f"TG send fail: {e}")
        return False


def main():
    st = load_state()
    alive = proc_alive("proof_mesh/audit_daemon")
    fr = chain_freshness()
    fresh_ok = fr.get("ok", False)

    if alive and fresh_ok:
        # Всё хорошо: если были проблемы — сообщить о восстановлении
        if st.get("bad_since"):
            log("✅ Цепочка восстановлена")
            send_tg("✅ Sentinel: цепочка Proof Mesh восстановлена, контроль снова полный.")
            st = {"bad_since": None, "last_alert": 0}
            save_state(st)
        else:
            log(f"OK: audit жив, события {fr.get('event_age_min')} мин, сертификат {fr.get('cert_age_min')} мин")
        return

    # Проблема
    reasons = []
    if not alive:
        reasons.append("процесс audit_daemon МЁРТВ")
    if not fresh_ok:
        reasons.append(f"цепочка СТОИТ (события {fr.get('event_age_min')} мин, сертификат {fr.get('cert_age_min')} мин назад)")
    reason = "; ".join(reasons)

    if not st.get("bad_since"):
        # Переход в bad: алерт + рестарт
        st["bad_since"] = time.time()
        st["last_alert"] = time.time()
        log(f"🚨 Цепочка под угрозой: {reason}")
        send_tg(f"🚨 Sentinel Chain Watch: {reason}. Пробую авторестарт audit_daemon…")
        restart_audit()
        save_state(st)
    else:
        # Уже в bad: повторный алерт раз в час
        if time.time() - st.get("last_alert", 0) > REALERT_SEC:
            st["last_alert"] = time.time()
            log(f"🔁 Повторный алерт: {reason}")
            send_tg(f"🔁 Sentinel Chain Watch (уже {int((time.time()-st['bad_since'])/60)} мин): {reason}. Рестартую снова…")
            restart_audit()
            save_state(st)
        else:
            log(f"BAD (дедуп): {reason}")


if __name__ == "__main__":
    main()

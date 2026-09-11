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
import sys
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

# Публикация кода: (имя, каталог, ветка). Коммиты, не ушедшие на GitHub
# дольше UNPUSHED_MAX_HOURS, — сигнал: работа есть, а снаружи её нет.
GIT_REPOS = (
    ("relay-mesh", "/home/agent/data/sites/relay-mesh", "master"),
    ("snin-v5-mesh-fabric", "/home/agent/data/projects/snin-v5-mesh-fabric", "main"),
    ("snin-mail-nostr", "/home/agent/data/projects/cryter-mail-release", "main"),
)
GIT_FETCH_INTERVAL = 21600   # обновлять remote-ссылки раз в 6 часов
UNPUSHED_MAX_HOURS = 24      # дольше суток без публикации — сигнал


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


def external_publish_check() -> dict:
    """Внешняя проверка: публичный сертификат (kind 8010) виден на релеях и сходится с цепью.

    Возвращает {"ok": True|False|None}. None — проверку выполнить не удалось
    (нет сети/библиотек): это не тревога о цепочке, а недоступность проверки.

    Критерий внутри — сверка root сертификата с block_hash цепи на ЕГО высоте
    плюс непрерывность до текущего хвоста (см. proof_mesh.publisher).
    Раньше сравнивался живой хвост, поэтому проверка не могла сойтись никогда.
    """
    try:
        if MESH_DIR not in sys.path:
            sys.path.insert(0, MESH_DIR)
        from proof_mesh import publisher
        r = publisher.verify_cert_on_relays(AUDIT_DB)
        best = (r.get("matching_certs") or [{}])[0]
        return {
            "ok": bool(r.get("verified")),
            "local_height": r.get("local_height"),
            "found": r.get("found_on_relays"),
            "relays_checked": r.get("relays_checked"),
            "best": best,
        }
    except Exception as e:
        return {"ok": None, "error": f"{type(e).__name__}: {e}"[:150]}


def _git(path: str, *args: str, timeout: int = 60) -> subprocess.CompletedProcess:
    """git в каталоге репозитория без риска dubious-ownership."""
    return subprocess.run(
        ["git", "-c", f"safe.directory={path}", "-C", path, *args],
        capture_output=True, text=True, timeout=timeout,
    )


def unpushed_check(st: dict) -> dict:
    """Коммиты, которые лежат локально и не ушли на GitHub дольше UNPUSHED_MAX_HOURS.

    Раз в GIT_FETCH_INTERVAL обновляет remote-ссылки (git fetch), затем сравнивает
    ветку с origin/<branch> и считает возраст САМОГО СТАРОГО неотправленного
    коммита. До суток расхождение — норма (правим и публикуем), дольше — сигнал.
    Ровно этот случай 10.09 стоил времени: фикс был готов и закоммичен, но наружу
    не ушёл, и заметили это только руками.

    Возвращает {"ok": bool, "items": [...], "problems": [...], "errors": [...]}.
    """
    now = time.time()
    do_fetch = now - float(st.get("git_fetch_at") or 0) > GIT_FETCH_INTERVAL
    items, problems, errors = [], [], []
    for name, path, branch in GIT_REPOS:
        if not os.path.isdir(os.path.join(path, ".git")):
            continue
        try:
            if do_fetch:
                _git(path, "fetch", "--quiet", "origin", timeout=90)
            ref = f"origin/{branch}"
            cnt = _git(path, "rev-list", "--count", f"{ref}..{branch}")
            if cnt.returncode != 0:
                errors.append(f"{name}: нет {ref}")
                continue
            n = int((cnt.stdout or "0").strip() or 0)
            if n == 0:
                items.append({"repo": name, "unpushed": 0})
                continue
            ages = _git(path, "log", "--format=%ct", f"{ref}..{branch}")
            ts = [int(x) for x in (ages.stdout or "").split() if x.isdigit()]
            hours = round((now - min(ts)) / 3600.0, 1) if ts else None
            item = {"repo": name, "unpushed": n, "oldest_hours": hours}
            items.append(item)
            if hours is not None and hours > UNPUSHED_MAX_HOURS:
                problems.append(item)
        except Exception as e:
            errors.append(f"{name}: {type(e).__name__}")
    if do_fetch:
        st["git_fetch_at"] = now
    return {"ok": not problems, "items": items, "problems": problems, "errors": errors}


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
            st = {"bad_since": None, "last_alert": 0,
                  "warn_since": st.get("warn_since"), "warn_alert": st.get("warn_alert", 0)}
            save_state(st)
        else:
            log(f"OK: audit жив, события {fr.get('event_age_min')} мин, сертификат {fr.get('cert_age_min')} мин")

        ext = external_publish_check()
        if ext.get("ok") is True:
            b = ext.get("best") or {}
            log(f"OK: внешняя публикация подтверждена (сертификат h={b.get('height')}, "
                f"релеев {b.get('relays')}, возраст {b.get('age_min')} мин, цепь до хвоста сходится)")
            if st.get("warn_since"):
                st["warn_since"] = None
                st["warn_alert"] = 0
                save_state(st)
                log("✅ Внешняя проверка восстановлена")
        elif ext.get("ok") is False:
            detail = (f"найдено сертификатов {ext.get('found')} из {ext.get('relays_checked')} релеев, "
                      f"ни один не сходится с цепью на своей высоте")
            if not st.get("warn_since"):
                st["warn_since"] = time.time()
                st["warn_alert"] = time.time()
                save_state(st)
                log(f"⚠️ WARN: внешняя публикация не подтверждена ({detail})")
                send_tg(f"⚠️ Sentinel: цепочка растёт, но публичный сертификат не подтверждается с релеев "
                        f"({detail}). Локальный контроль полный, внешний — нет.")
            elif time.time() - st.get("warn_alert", 0) > REALERT_SEC:
                st["warn_alert"] = time.time()
                save_state(st)
                mins = int((time.time() - st["warn_since"]) / 60)
                log(f"🔁 WARN повтор ({mins} мин): внешняя публикация не подтверждена")
                send_tg(f"🔁 Sentinel: публичный сертификат не подтверждается с релеев уже {mins} мин "
                        f"({detail}).")
            else:
                log(f"WARN (дедуп): внешняя публикация не подтверждена ({detail})")
        else:
            log(f"WARN: внешняя проверка недоступна: {ext.get('error')}")

        up = unpushed_check(st)
        save_state(st)
        if up.get("problems"):
            desc = "; ".join(
                f"{p['repo']}: {p['unpushed']} коммитов, старший {p['oldest_hours']} ч"
                for p in up["problems"])
            if not st.get("push_since"):
                st["push_since"] = time.time()
                st["push_alert"] = time.time()
                save_state(st)
                log(f"⚠️ WARN: работа не опубликована — {desc}")
                send_tg(f"⚠️ Sentinel: коммиты старше {UNPUSHED_MAX_HOURS} ч не уходят "
                        f"на GitHub — {desc}. Локально всё цело, снаружи этой работы нет.")
            elif time.time() - st.get("push_alert", 0) > REALERT_SEC:
                st["push_alert"] = time.time()
                save_state(st)
                mins = int((time.time() - st["push_since"]) / 60)
                log(f"🔁 WARN повтор ({mins} мин): работа не опубликована — {desc}")
                send_tg(f"🔁 Sentinel: работа не опубликована уже {mins} мин — {desc}")
            else:
                log(f"WARN (дедуп): работа не опубликована — {desc}")
        else:
            if st.get("push_since"):
                st["push_since"] = None
                st["push_alert"] = 0
                save_state(st)
                log("✅ Публикация кода восстановлена")
            pending = ", ".join(f"{i['repo']} {i['unpushed']}" for i in up["items"] if i.get("unpushed"))
            log("OK: публикация кода в порядке" + (f" (в работе: {pending})" if pending else ""))
            if up.get("errors"):
                log(f"   не проверено: {', '.join(up['errors'])}")
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

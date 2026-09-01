"""
SNIN PROOF MESH — Фаза 6: демон аудита (audit_daemon.py).

Постоянный сборщик событий роя → подписанная hash-chain:
  1. Цикл каждые DAEMON_INTERVAL (60 с):
     - relay_v2.db: дельта по received_at (sync_relay_db, идемпотентно)
     - UNIX-сокеты mesh: cr.sock / nostr.sock (короткое прослушивание)
     - dead-letter (kind 9000) → события degraded
  2. Публикация сертификата (kind 30000 + 8010) + обновление snapshot
     дашборда, когда: +50 блоков с последней публикации ИЛИ прошло
     600 с и цепочка выросла. Пустые сертификаты не публикуются.
  3. Состояние — в таблице sync_state (переживает рестарт):
     daemon_cert_ts / daemon_cert_height.

Ресурсы (замерено): append 335 событий/с на 1 ядро, ~0.3 с работы на
цикл → <1% CPU; ~50-80 MB RAM; 909 B/событие на диске.

Restart: start.sh relay-mesh (заменяет DashUpdater из Ф5).

Спека: SNIN_PROOF_MESH_SPEC.md, Фаза 6.
"""

import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from proof_mesh import chain, db, mesh_int, publisher  # noqa: E402

AUDIT_DB = "/home/agent/data/sites/relay-mesh/proof_mesh/snin_audit.db"
SNAPSHOT_OUT = "/home/agent/data/sites/sentinel-dash/snapshot.json"
DAEMON_INTERVAL = 60          # с
SOCKET_DURATION = 4           # с прослушивания сокетов за цикл
PULSE_INTERVAL = 300          # с — пульс демона: heartbeat+cgroup+relay-health
CERT_BLOCKS = 50              # порог по блокам
CERT_MIN_INTERVAL = 600       # с — минимальный интервал публикации
NSEC_PATH = "/home/agent/data/sites/chrono/keystore/agent_keys.json.old"


def load_nsec() -> str:
    ks = json.load(open(NSEC_PATH))
    return ks["cryter"]["nsec"]


def get_height(audit_db: str) -> int:
    st = db.get_chain_state(audit_db)
    return st["height"] if st else 0


def should_publish(height: int, last_height: int, last_ts: int,
                   now: int | None = None) -> bool:
    """+50 блоков ИЛИ (600 с прошло И цепочка выросла)."""
    now = now or int(time.time())
    if height - last_height >= CERT_BLOCKS:
        return True
    return now - last_ts >= CERT_MIN_INTERVAL and height > last_height


def run_cycle(audit_db: str, nsec: str, relay_db: str = mesh_int.RELAY_DB,
              snapshot_out: str = SNAPSHOT_OUT,
              pulse_interval: int = PULSE_INTERVAL,
              log=print) -> dict:
    """Один цикл демона: собрать → подписать → (при пороге) опубликовать."""
    t0 = time.time()

    # 1. relay_v2.db → цепочка
    r = mesh_int.sync_relay_db(audit_db, nsec, relay_db=relay_db)
    # 2. сокеты mesh (короткое прослушивание)
    s = mesh_int.listen_sockets(audit_db, nsec, duration=SOCKET_DURATION)
    # 3. dead-letter
    d = mesh_int.check_dead_letters(audit_db, nsec, relay_db=relay_db)
    # 4. собственный пульс (heartbeat + cgroup + relay-health) — раз в 5 мин
    p = mesh_int.pulse(audit_db, nsec, interval=pulse_interval, log=log)

    stored = r.get("stored", 0) + s.get("stored", 0) + d.get("deadletters_stored", 0) + p.get("stored", 0)
    height = get_height(audit_db)

    # 4. сертификат + snapshot по порогу
    cert = None
    last_ts = mesh_int._sync_state(audit_db, "daemon_cert_ts")
    last_height = mesh_int._sync_state(audit_db, "daemon_cert_height")
    if should_publish(height, last_height, last_ts):
        try:
            cert = publisher.publish_cert(audit_db, nsec)
            mesh_int._save_state(audit_db, "daemon_cert_ts", int(time.time()))
            mesh_int._save_state(audit_db, "daemon_cert_height", height)
            if snapshot_out:
                publisher.build_snapshot(audit_db, snapshot_out)
        except Exception as e:
            log(f"[daemon] сертификат НЕ опубликован: {e}")

    log(f"[daemon] цикл {time.time()-t0:.1f}с | новых {stored} "
        f"(relay {r.get('stored',0)}, sock {s.get('stored',0)}, "
        f"dl {d.get('deadletters_stored',0)}, pulse {p.get('stored',0)}) | height {height} | "
        f"cert {'OK' if cert else '-'}")
    return {"stored": stored, "height": height, "cert": bool(cert)}


def main() -> None:
    nsec = load_nsec()
    log_path = Path("/home/agent/data/sites/relay-mesh/logs/audit_daemon.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"\n=== SPM audit_daemon старт {time.strftime('%F %T')} "
          f"pid={os.getpid()} ===", flush=True)

    def log(msg: str) -> None:
        # stdout перенаправлен в лог через nohup — дублировать не нужно
        print(f"{time.strftime('%F %T')} {msg}", flush=True)

    cycle = 0
    while True:
        try:
            run_cycle(AUDIT_DB, nsec, log=log)
        except Exception as e:
            log(f"[daemon] ОШИБКА цикла: {e}")
        cycle += 1
        time.sleep(DAEMON_INTERVAL)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""econ_sync.py — инкрементальный zap-монитор (cron */5).

Читает свежие NIP-57 (9735 zap-ы, 9734 счета) и kind 0 (кошельки)
с проверенных релеев → payment_events / wallet_profiles в snin_audit.db.
Только ЧТЕНИЕ публичных событий — ничего не публикует, денег не шлёт.

since = max(ts в payment_events) − 900с (перекрытие на задержку релеев),
дедуп по event_id в store_payments — повторные запуски безопасны.

Запуск: cron */5 * * * * (см. init.sh, блок sentinel).
"""
import sys
import time
import sqlite3
import logging

sys.path.insert(0, "/home/agent/data/sites/relay-mesh")

from proof_mesh import econ  # noqa: E402

DB = "/home/agent/data/sites/relay-mesh/proof_mesh/snin_audit.db"
LOG = "/home/agent/data/sites/relay-mesh/logs/econ_sync.log"

logging.basicConfig(
    filename=LOG, level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def main() -> None:
    econ._ensure_zaps_incoming(DB)  # таблица our_zaps до чтения
    last = sqlite3.connect(DB).execute(
        "SELECT MAX(ts) FROM payment_events").fetchone()[0]
    since = int(last) - 900 if last else int(time.time()) - 14 * 86400
    t0 = time.time()
    r = econ.sync_econ(DB, since=since)
    # #p-мониторинг: запы в адрес НАШИХ ключей (Cryter и др.)
    last_our = sqlite3.connect(DB).execute(
        "SELECT MAX(ts) FROM zaps_incoming").fetchone()[0]
    since_our = int(last_our) - 900 if last_our else \
        int(time.time()) - econ.BACKFILL_DAYS * 86400
    t1 = time.time()
    ours = econ.sync_ours(DB, since=since_our)
    secs = round(time.time() - t0, 1)
    logging.info(
        f"sync: zaps={r['zaps_stored']} reqs={r['reqs_stored']} "
        f"wallets={r['wallets_updated']} ours={ours['stored']} "
        f"(seen {r['zaps_seen']}/{r['reqs_seen']}, since={since}, "
        f"ours_since={since_our}, {secs}s)")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"sync FAILED: {e}")

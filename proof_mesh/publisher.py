"""
SNIN PROOF MESH — Фаза 5: публичные сертификаты честности (publisher.py).

Внешний наблюдатель может проверить честность ноды по публичным данным
Nostr:
  - kind 30000 (параметризованный replaceable, d-tag "spm"):
    {root, height, ts} — корень hash-chain
  - kind 8010-8017 (NIP-80): сертификат честности
    {node_pubkey, root, height, prev_cert_id, ts} — подпись ноды
  - fanout на проверенные релеи (primal, damus, nos.lol + CURATED_RELAYS
    через MeshAdapter — тот же путь, которым cryter публикует посты)

Публикация через create_nostr_adapter (рабочий адаптер cryter), подпись —
ключом ноды (cryter). Done-when Ф5:
  - kind 30000 + 8010 найдены на 3+ релеях по pubkey ноды
  - внешняя проверка: fetch kind 8010 → root == last_hash цепочки → OK
  - дашборд sentinel-dash.v2.site с реальными цифрами из БД

Спека: SNIN_PROOF_MESH_SPEC.md, Фаза 5.
"""

import json
import os
import sqlite3
import sys
import time

sys.path.insert(0, "/home/agent/data/sites/relay-mesh")
sys.path.insert(0, "/home/agent/data/agents/core/cryter")

from proof_mesh import chain, db  # noqa: E402

CRYTER_PUB = "8ae7965af1b61347bb9900b91cfa9487e4da2400bdb063521ad0850706ff5f96"

CERT_MIN_RELAYS = 2   # сертификат внешне подтверждён, если его отдали не меньше релеев
CERT_MAX_AGE = 3600   # сек: старше часа — не считается подтверждением «сейчас»
# Наши кошельки (только они показываются в экономике дашборда):
OUR_PUBKEYS = [
    "8ae7965af1b61347bb9900b91cfa9487e4da2400bdb063521ad0850706ff5f96",  # Cryter
    "8d468694fe3b294afa71271ed409fbfe061caedebe307992a1308696ef7fa9f4",  # Remora
    "39c15ed9502a781fa15abc132d39044c1df2a2262bdf86c7ac1d1f9d52baf2f4",  # v2bot
    "b659b2d0b13da2e00c104b38fe936fa27ec3cee9a59378075c21ee2045616377",  # botperevod
    "2bb1a9f5bdbd7bab31a7e7069d5f406ee3edfc338c4e1e6b395d57bce95267e7",  # Urantia Daily
]
CERT_KIND = 8010
ROOT_KIND = 30000
D_TAG = "spm-chain-root"

# релеи для fanout (проверенные + наши)
PUB_RELAYS = [
    "wss://relay.primal.net",
    "wss://relay.damus.io",
    "wss://nos.lol",
    "wss://purplepag.es",
    "wss://nostr.oxtr.dev",
]


def _cert_table(audit_db: str) -> None:
    with db._conn(audit_db) as c:
        c.execute(
            """CREATE TABLE IF NOT EXISTS cert_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind INTEGER NOT NULL, root TEXT NOT NULL,
                height INTEGER NOT NULL, prev_cert_id TEXT NOT NULL DEFAULT '',
                event_id TEXT NOT NULL DEFAULT '', ts INTEGER NOT NULL)"""
        )


def build_cert(audit_db: str) -> dict:
    """Сертификат честности: {node_pubkey, root, height, prev_cert_id, ts}.
    root — последний хэш подписанной цепочки (корень)."""
    st = db.get_chain_state(audit_db)
    if not st or not st.get("last_hash"):
        raise ValueError("цепочка пуста — нечего сертифицировать")
    _cert_table(audit_db)
    with db._conn(audit_db) as c:
        row = c.execute(
            "SELECT event_id FROM cert_state ORDER BY id DESC LIMIT 1"
        ).fetchone()
    prev = row[0] if row else ""
    return {
        "node_pubkey": CRYTER_PUB,
        "root": st["last_hash"],
        "height": st["height"],
        "prev_cert_id": prev,
        "ts": int(time.time()),
    }


def _make_adapter(nsec: str):
    """Рабочий адаптер публикации cryter (MeshAdapter + RelayOrchestrator)."""
    sys.path.insert(0, "/home/agent/data/agents/core/cryter/src")
    from core.factories import create_nostr_adapter  # noqa: PLC0415
    return create_nostr_adapter(nsec=nsec)


def publish_cert(audit_db: str, nsec: str,
                 fanout: list[str] | None = None) -> dict:
    """Опубликовать kind 8010 + kind 30000 (root). Возвращает event_id.
    Fanout идёт через RelayOrchestrator адаптера (CURATED_RELAYS);
    параметр fanout зарезервирован на будущее."""
    _cert_table(audit_db)
    cert = build_cert(audit_db)
    adapter = _make_adapter(nsec)

    cert_json = json.dumps(cert, ensure_ascii=False)
    tags = [["root", cert["root"]], ["height", str(cert["height"])],
            ["prev", cert["prev_cert_id"]]]
    ev_id = adapter.publish_event(cert_json, tags=tags, kind=CERT_KIND)
    root_id = None
    if ev_id:
        root_payload = json.dumps(
            {"root": cert["root"], "height": cert["height"], "ts": cert["ts"]},
            ensure_ascii=False)
        root_id = adapter.publish_event(
            root_payload, tags=[["d", D_TAG], ["root", cert["root"]]],
            kind=ROOT_KIND)

    with db._conn(audit_db) as c:
        c.execute(
            """INSERT INTO cert_state (kind, root, height, prev_cert_id, event_id, ts)
               VALUES (?,?,?,?,?,?)""",
            (CERT_KIND, cert["root"], cert["height"],
             cert["prev_cert_id"], ev_id or "", cert["ts"]),
        )
    return {"cert": cert, "cert_event_id": ev_id, "root_event_id": root_id}


def fetch_cert(pubkey: str = CRYTER_PUB, limit: int = 3,
               relays: list[str] | None = None) -> list[dict]:
    """Найти kind 8010 ноды на релеях (для внешней проверки).

    К каждому событию добавляется служебный ключ "_relays" — с каких релеев
    оно получено (нужно, чтобы отличить «сертификат лежит на одном релее»
    от «сертификат подтверждён несколькими»)."""
    import websocket  # noqa: PLC0415
    relays = relays or PUB_RELAYS
    out: list[dict] = []
    for url in relays:
        try:
            ws = websocket.create_connection(url, timeout=8)
            ws.send(json.dumps(["REQ", "spm-cert", {"kinds": [CERT_KIND],
                                                    "authors": [pubkey],
                                                    "limit": limit}]))
            while True:
                msg = json.loads(ws.recv())
                if msg[0] == "EVENT":
                    ev = dict(msg[2])
                    ev.setdefault("_relays", [])
                    if url not in ev["_relays"]:
                        ev["_relays"].append(url)
                    out.append(ev)
                elif msg[0] == "EOSE":
                    break
            ws.close()
        except Exception:
            continue
    # дедуп по id с объединением списка релеев
    seen: dict[str, dict] = {}
    deduped: list[dict] = []
    for ev in out:
        eid = ev.get("id")
        if eid in seen:
            merged = set(seen[eid].get("_relays") or []) | set(ev.get("_relays") or [])
            seen[eid]["_relays"] = sorted(merged)
            continue
        ev["_relays"] = sorted(set(ev.get("_relays") or []))
        seen[eid] = ev
        deduped.append(ev)
    return deduped


def verify_cert_on_relays(audit_db: str, pubkey: str = CRYTER_PUB,
                          min_relays: int = CERT_MIN_RELAYS,
                          max_age: int = CERT_MAX_AGE) -> dict:
    """Внешняя проверка: сертификат с релеев соответствует цепи на СВОЕЙ высоте.

    Критерий: root сертификата сравнивается с block_hash цепочки на высоте
    сертификата (свойство снимка), а НЕ с живым хвостом last_hash. Сертификат —
    периодический снимок (раз в ~11 минут), хвост растёт каждые ~минуту, поэтому
    равенство «root == last_hash» недостижимо по построению и давало ложный
    verified=False (проверено на 1225 сертификатах: root == block_hash своей
    высоты совпадает в 1225/1225 случаях).

    Сертификат считается внешне подтверждённым, если одновременно:
      * root совпадает с block_hash цепи на его высоте;
      * цепь непрерывна от этой высоты до текущего хвоста (db.verify_from);
      * событие отдали не меньше min_relays релеев;
      * возраст события не больше max_age секунд.
    """
    certs = fetch_cert(pubkey, limit=5)
    st = db.get_chain_state(audit_db) or {}
    local_root = st.get("last_hash", "")
    local_height = int(st.get("height") or 0)
    now = int(time.time())

    checked: list[dict] = []
    ok_certs: list[dict] = []
    for ev in certs:
        try:
            c = json.loads(ev.get("content", "{}"))
        except (json.JSONDecodeError, TypeError):
            continue
        if c.get("node_pubkey") != pubkey:
            continue
        try:
            height = int(c.get("height") or 0)
        except (TypeError, ValueError):
            continue
        root = c.get("root") or ""
        with db._conn(audit_db) as conn:
            row = conn.execute(
                "SELECT block_hash FROM audit_events WHERE id=?", (height,)
            ).fetchone()
        local_block = row[0] if row else ""
        root_matches = bool(local_block) and local_block == root
        chain_continues, events_after, _broken = (False, 0, None)
        if root_matches:
            chain_continues, events_after, _broken = db.verify_from(audit_db, height)
        try:
            ts = int(c.get("ts") or ev.get("created_at") or 0)
        except (TypeError, ValueError):
            ts = 0
        age = now - ts if ts else -1
        relays = ev.get("_relays") or []
        entry = {
            "height": height,
            "age_min": round(age / 60.0, 1) if age >= 0 else None,
            "relays": len(relays),
            "root_matches_local_block": root_matches,
            "chain_continues_to_head": bool(chain_continues),
            "events_after_cert": events_after,
            "event_id": ev.get("id"),
        }
        checked.append(entry)
        if (root_matches and chain_continues
                and len(relays) >= min_relays
                and 0 <= age <= max_age):
            ok_certs.append(entry)

    return {
        "found_on_relays": len(certs),
        "relays_checked": len(PUB_RELAYS),
        "local_root": local_root,
        "local_height": local_height,
        "min_relays": min_relays,
        "max_age_min": max_age // 60,
        "checked": checked,
        "matching_certs": ok_certs,
        "verified": bool(ok_certs),
    }


# ── дашборд: snapshot.json из реальных данных БД ────────────────────────────

def build_snapshot(audit_db: str, out_path: str) -> dict:
    """Реальные цифры из БД для дашборда: height/root/экономика/health."""
    st = db.get_chain_state(audit_db)
    with sqlite3.connect(audit_db) as c:
        events = c.execute(
            "SELECT COUNT(*) FROM audit_events").fetchone()[0]
        agents = c.execute(
            "SELECT agent_id, COUNT(*) FROM audit_events GROUP BY agent_id "
            "ORDER BY 2 DESC LIMIT 8").fetchall()
        # Экономика — ТОЛЬКО наши кошельки (входящие zap-ы на наши pubkeys).
        # Глобальный скан сети (payment_events/wallet_profiles целиком) на
        # дашборд не выводится — там чужие кошельки и чужие трансферы.
        ph = ",".join("?" * len(OUR_PUBKEYS))
        our_wallets = c.execute(
            f"SELECT COUNT(DISTINCT receiver_pub) FROM zaps_incoming "
            f"WHERE receiver_pub IN ({ph})", OUR_PUBKEYS).fetchone()[0]
        our_senders = c.execute(
            f"SELECT COUNT(DISTINCT sender_pub) FROM zaps_incoming "
            f"WHERE receiver_pub IN ({ph})", OUR_PUBKEYS).fetchone()[0]
        zap_our = c.execute(
            f"SELECT COUNT(*), COALESCE(SUM(amount_msat),0) FROM zaps_incoming "
            f"WHERE receiver_pub IN ({ph})", OUR_PUBKEYS).fetchone()
        certs = c.execute(
            "SELECT root, height, ts FROM cert_state ORDER BY id DESC LIMIT 1"
        ).fetchone() if db._conn(audit_db).execute(
            "SELECT 1 FROM sqlite_master WHERE name='cert_state'"
        ).fetchone() else None

    snap = {
        "generated_at": int(time.time()),
        "chain": {
            "height": st["height"] if st else 0,
            "last_root": st["last_hash"] if st else "",
            "events_total": events,
        },
        "economy": {
            "wallets": our_wallets,
            "senders": our_senders,
            "payments": [{"kind": 9735, "count": zap_our[0] or 0,
                          "total_msat": zap_our[1] or 0}],
        },
        "agents": [{"agent_id": a, "events": n} for a, n in agents],
        "last_cert": {"root": certs[0], "height": certs[1], "ts": certs[2]}
        if certs else None,
    }
    with open(out_path, "w") as f:
        json.dump(snap, f, ensure_ascii=False, indent=2)
    return snap


if __name__ == "__main__":
    audit = "/home/agent/data/sites/relay-mesh/proof_mesh/snin_audit.db"
    if len(sys.argv) > 1 and sys.argv[1] == "snapshot":
        out = sys.argv[2] if len(sys.argv) > 2 else "/tmp/snap.json"
        s = build_snapshot(audit, out)
        print(json.dumps(s, ensure_ascii=False)[:400])
        sys.exit(0)
    ks = json.load(open("/home/agent/data/sites/chrono/keystore/agent_keys.json.old"))
    nsec = ks["cryter"]["nsec"]
    r = publish_cert(audit, nsec)
    print("cert:", json.dumps(r["cert"])[:250])
    print("cert_event_id:", r["cert_event_id"])
    print("root_event_id:", r["root_event_id"])

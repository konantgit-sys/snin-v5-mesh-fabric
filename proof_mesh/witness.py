"""
SNIN PROOF MESH — Фаза 4: свидетели (witness.py).

ЗАЧЕМ. Корень цепочки сегодня подписывает один наш ключ. Это самоаттестация:
если мы соврём о том, что было записано, проверить это снаружи нечем.
Свидетель — независимая сторона, которая сама забирает чекпоинт, сама
проверяет его сходимость с цепочкой и подписывает СВОИМ ключом.

ЧЕКПОИНТ — запись в proof_registry.chain_roots (height, root, root_ts, pubkey, sig).

ЦИКЛ СВИДЕТЕЛЯ:
  1) берёт чекпоинт из внешнего источника (релеи, kind 8010 — то, что реально
     опубликовано наружу) либо из реестра;
  2) проверяет, что root сходится с block_hash цепочки НА ЕГО ВЫСОТЕ;
  3) подписывает аттестацию своим ключом (BIP340, тот же формат, что у блоков);
  4) пишет аттестацию в proof_registry.witness_attestations.

ЧТО ТРЕБУЕТ ПРОВЕРКА (verify_checkpoint):
  * не меньше WITNESS_REQUIRED РАЗНЫХ ключей свидетелей (дубли по ключу не считаются);
  * аттестация не от того же ключа, что подписал сам чекпоинт (самоаттестация не в счёт);
  * не меньше WITNESS_MIN_RELAY_SOURCED аттестаций с независимым путём чтения
    (source = relay:...). Два свидетеля, читавших одну нашу базу, не ловят нашу
    же ложь о корне — поэтому «два локальных» проверку не проходят.

Чего это НЕ даёт: пока ключи свидетелей наши, независимость техническая
(разные ключи, разный путь чтения), а не организационная. Механизм принимает
любой внешний pubkey: партнёрский узел можно добавить одной записью в roster.

CLI:
  python3 -m proof_mesh.witness keygen --name w1
  python3 -m proof_mesh.witness keys
  python3 -m proof_mesh.witness attest --witness w1 --source relay
  python3 -m proof_mesh.witness status
  python3 -m proof_mesh.witness verify
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path

from proof_mesh import chain, publisher

CHAIN_ID = "main"
AUDIT_DB = "/home/agent/data/sites/relay-mesh/proof_mesh/snin_audit.db"
DEFAULT_KEYS = "/home/agent/data/.secure/witness_keys.json"

ATTEST_KIND = "witness.checkpoint"
WITNESS_REQUIRED = 2            # минимум разных подписей свидетелей
WITNESS_MIN_RELAY_SOURCED = 1   # минимум одна — с независимым путём чтения
HEALTH_MAX_AGE_SEC = 1800       # чекпоинт без свежих свидетелей — сигнал тревоги


# ── пути (в тестах переопределяются, прод не трогаем) ───────────────────────

def registry_db() -> str:
    return os.environ.get("SPM_WITNESS_REGISTRY") or chain.PROOF_REGISTRY_DB


def audit_db() -> str:
    return os.environ.get("SPM_WITNESS_AUDIT") or AUDIT_DB


def keys_path() -> str:
    return os.environ.get("SPM_WITNESS_KEYS") or DEFAULT_KEYS


def _sdk():
    return chain._sdk()


def canonical(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def payload_hash(payload: dict) -> str:
    return hashlib.sha256(canonical(payload).encode("utf-8")).hexdigest()


# ── ключи свидетелей ────────────────────────────────────────────────────────

def load_keys() -> dict:
    try:
        return json.loads(Path(keys_path()).read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_keys(keys: dict) -> None:
    p = Path(keys_path())
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(keys, ensure_ascii=False, indent=2), encoding="utf-8")
    os.chmod(p, 0o600)


def keygen(name: str, force: bool = False) -> dict:
    """Завести ключ свидетеля. Секрет лежит в файле с правами 600."""
    Keys, _ = _sdk()
    keys = load_keys()
    if name in keys and not force:
        raise SystemExit(f"свидетель «{name}» уже есть (--force — перевыпуск)")
    k = Keys.generate()
    sk, pk = k.secret_key(), k.public_key()
    keys[name] = {
        "nsec": sk.to_bech32() if hasattr(sk, "to_bech32") else str(sk),
        "pubhex": pk.to_hex(),
        "npub": pk.to_bech32() if hasattr(pk, "to_bech32") else "",
        "created_at": int(time.time()),
    }
    save_keys(keys)
    return {"name": name, "pubhex": keys[name]["pubhex"], "npub": keys[name]["npub"]}


def roster() -> set[str]:
    """Публичные ключи свидетелей, которые считаются признанными."""
    return {v.get("pubhex", "") for v in load_keys().values() if v.get("pubhex")}


# ── хранилище аттестаций ────────────────────────────────────────────────────

def _att_table() -> None:
    Path(registry_db()).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(registry_db(), timeout=30.0) as c:
        c.execute(
            """CREATE TABLE IF NOT EXISTS witness_attestations (
                chain_id    TEXT NOT NULL DEFAULT 'main',
                height      INTEGER NOT NULL,
                root        TEXT NOT NULL,
                witness_id  TEXT NOT NULL,
                witness_pub TEXT NOT NULL,
                payload     TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                sig         TEXT NOT NULL,
                source      TEXT NOT NULL DEFAULT '',
                created_at  INTEGER NOT NULL,
                PRIMARY KEY (chain_id, height, witness_pub)
            )"""
        )


def store_attestation(att: dict) -> None:
    _att_table()
    with sqlite3.connect(registry_db(), timeout=30.0) as c:
        c.execute(
            """INSERT OR REPLACE INTO witness_attestations
               (chain_id, height, root, witness_id, witness_pub, payload,
                payload_hash, sig, source, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (att["chain_id"], att["height"], att["root"], att["witness_id"],
             att["witness_pub"], canonical(att["payload"]), att["payload_hash"],
             att["sig"], att["source"], att["created_at"]),
        )


def attestations(height: int, chain_id: str = CHAIN_ID) -> list[dict]:
    _att_table()
    with sqlite3.connect(registry_db(), timeout=30.0) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute(
            """SELECT * FROM witness_attestations
               WHERE chain_id=? AND height=? ORDER BY created_at""",
            (chain_id, height),
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["payload"] = json.loads(d["payload"])
        except Exception:
            d["payload"] = {}
        out.append(d)
    return out


# ── чекпоинт: два пути чтения ───────────────────────────────────────────────

# Сертификат честности: 10110 — свой кинд (см. publisher.py), 8010 — зеркало
# переходного периода. Свидетель читает оба, иначе на время переключения
# локальный и внешний свидетель подпишут разные чекпоинты.
CERT_KINDS = (10110, 8010)
CERT_KIND = CERT_KINDS[0]


def _signer_at(height: int) -> str:
    """Ключ, которым подписан блок на этой высоте (для контроля самоаттестации)."""
    try:
        with sqlite3.connect(audit_db(), timeout=30.0) as c:
            row = c.execute(
                "SELECT signer_pub FROM audit_events WHERE id=?", (int(height),)
            ).fetchone()
        return (row[0] if row and row[0] else "") or ""
    except sqlite3.Error:
        return ""


def checkpoint_local(height: int | None = None) -> dict | None:
    """Чекпоинт с локальной стороны: ОПУБЛИКОВАННЫЙ сертификат (kind 8010).

    Почему не просто последний корень реестра: свидетель с релеев видит именно
    сертификат, а он уходит наружу реже, чем растёт корень. Если локальный
    свидетель берёт свежий корень, а внешний — сертификат, они подписывают
    РАЗНЫЕ чекпоинты, и требование «2 подписи на один чекпоинт» не выполняется.
    Реальный случай 17.09: 20282 (реестр) против 20281 (сертификат на релеях).
    """
    with sqlite3.connect(audit_db(), timeout=30.0) as c:
        c.row_factory = sqlite3.Row
        try:
            if height is None:
                r = c.execute(
                    "SELECT root, height, ts FROM cert_state WHERE kind IN (10110, 8010) "
                    "ORDER BY id DESC LIMIT 1",
                ).fetchone()
            else:
                r = c.execute(
                    "SELECT root, height, ts FROM cert_state WHERE kind IN (10110, 8010) AND height=? LIMIT 1",
                    (height,),
                ).fetchone()
        except sqlite3.Error:
            r = None
    if not r:
        # сертификатов ещё нет — падаем на корень реестра
        with sqlite3.connect(registry_db(), timeout=30.0) as c:
            c.row_factory = sqlite3.Row
            try:
                if height is None:
                    r2 = c.execute(
                        "SELECT root, height, root_ts, pubkey, sig FROM chain_roots WHERE chain_id=? ORDER BY height DESC LIMIT 1",
                        (CHAIN_ID,),
                    ).fetchone()
                else:
                    r2 = c.execute(
                        "SELECT root, height, root_ts, pubkey, sig FROM chain_roots WHERE chain_id=? AND height=?",
                        (CHAIN_ID, height),
                    ).fetchone()
            except sqlite3.Error:
                return None
        if not r2:
            return None
        return {"chain_id": CHAIN_ID, "height": int(r2["height"]), "root": r2["root"],
                "root_ts": int(r2["root_ts"]), "pubkey": r2["pubkey"] or _signer_at(r2["height"]),
                "sig": r2["sig"] or "", "relays": [], "origin": "registry"}
    return {"chain_id": CHAIN_ID, "height": int(r["height"]), "root": r["root"],
            "root_ts": int(r["ts"]), "pubkey": _signer_at(r["height"]),
            "sig": "", "relays": [], "origin": "cert"}


def newest_settled(lag_sec: int = 90) -> dict | None:
    """Свежайший сертификат, которому уже не меньше lag_sec.

    Нужен, чтобы оба свидетеля подписывали ОДИН чекпоинт: релеям нужно время
    разнести свежий сертификат (проверено: сразу после публикации его на
    релеях ещё нет, свидетель с релеев подписал бы предыдущую высоту).
    """
    cutoff = int(time.time()) - int(lag_sec)
    with sqlite3.connect(audit_db(), timeout=30.0) as c:
        c.row_factory = sqlite3.Row
        try:
            r = c.execute(
                "SELECT root, height, ts FROM cert_state WHERE kind IN (10110, 8010) AND ts <= ? ORDER BY id DESC LIMIT 1",
                (cutoff,),
            ).fetchone()
        except sqlite3.Error:
            return None
    if not r:
        return None
    return {"chain_id": CHAIN_ID, "height": int(r["height"]), "root": r["root"],
            "root_ts": int(r["ts"]), "pubkey": _signer_at(r["height"]),
            "sig": "", "relays": [], "origin": "cert-settled"}


def checkpoint_relay(height: int | None = None) -> dict | None:
    """Чекпоинт из внешнего источника: сертификат kind 8010, прочитанный с релеев.

    Здесь наша база не участвует — свидетель видит то, что реально опубликовано
    наружу, и в аттестацию попадает список релеев, которые это отдали.
    """
    try:
        events = publisher.fetch_cert(limit=5)
    except Exception:
        return None
    best: dict | None = None
    for ev in events:
        try:
            data = json.loads(ev.get("content") or "{}")
        except Exception:
            continue
        h = int(data.get("height") or 0)
        if height is not None and h != int(height):
            continue
        cand = {
            "chain_id": CHAIN_ID, "height": h, "root": str(data.get("root") or ""),
            "root_ts": int(data.get("ts") or 0), "pubkey": str(data.get("node_pubkey") or ""),
            "sig": "", "event_id": ev.get("id", ""), "relays": ev.get("_relays") or [],
        }
        if not cand["root"] or not cand["height"]:
            continue
        if height is not None:
            return cand
        if best is None or cand["height"] > best["height"]:
            best = cand
    return best


def root_matches_chain(cp: dict) -> tuple[bool, str]:
    """Корень чекпоинта обязан совпасть с block_hash цепочки на его высоте."""
    with sqlite3.connect(audit_db(), timeout=30.0) as c:
        row = c.execute(
            "SELECT block_hash FROM audit_events WHERE id=?", (int(cp["height"]),)
        ).fetchone()
    if not row:
        return False, f"в цепочке нет блока с высотой {cp['height']}"
    if row[0] != cp["root"]:
        return False, "корень чекпоинта не совпадает с block_hash на этой высоте"
    return True, "ok"


# ── аттестация ──────────────────────────────────────────────────────────────

def make_attestation(name: str, cp: dict, source: str) -> dict:
    keys = load_keys()
    if name not in keys:
        raise SystemExit(f"нет ключа свидетеля «{name}» — сделай: witness keygen --name {name}")
    nsec = keys[name]["nsec"]
    Keys, _ = _sdk()
    pub = Keys.parse(nsec).public_key().to_hex()
    payload = {
        "attest": ATTEST_KIND,
        "chain_id": cp.get("chain_id") or CHAIN_ID,
        "height": int(cp["height"]),
        "root": cp["root"],
        "root_ts": int(cp.get("root_ts") or 0),
        "source": source,
        "witness_pub": pub,
        "created_at": int(time.time()),
    }
    ph = payload_hash(payload)
    sig = chain.sign_block(nsec, ph)
    if not chain.verify_block(pub, ph, sig):
        raise SystemExit("подпись свидетеля не проверяется — не сохраняю")
    return {"chain_id": payload["chain_id"], "height": payload["height"], "root": payload["root"],
            "witness_id": name, "witness_pub": pub, "payload": payload, "payload_hash": ph,
            "sig": sig, "source": source, "created_at": payload["created_at"]}


def attest(name: str, source: str = "local", height: int | None = None,
           verify_chain: bool = True) -> dict:
    """Полный цикл свидетеля: получить чекпоинт, проверить, подписать, записать."""
    if source not in ("local", "relay"):
        raise SystemExit("source: local | relay")
    cp = checkpoint_local(height) if source == "local" else checkpoint_relay(height)
    if not cp:
        raise SystemExit(f"чекпоинт не найден ({source}) — подписывать нечего")
    if verify_chain:
        ok, why = root_matches_chain(cp)
        if not ok:
            raise SystemExit(f"свидетель отказывается подписывать: {why}")
    src = ("local-" + str(cp.get("origin") or "registry")) if source == "local" \
        else "relay:" + ",".join(cp.get("relays") or ["?"])
    att = make_attestation(name, cp, src)
    store_attestation(att)
    return att


def verify_attestation(att: dict, known: set[str] | None = None) -> tuple[bool, str]:
    payload = att.get("payload") or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return False, "тело аттестации не читается"
    if payload_hash(payload) != att.get("payload_hash"):
        return False, "payload_hash не совпал с телом"
    if payload.get("witness_pub") != att.get("witness_pub"):
        return False, "pubkey не совпал с телом"
    if payload.get("root") != att.get("root") or int(payload.get("height") or -1) != int(att.get("height") or -2):
        return False, "поля чекпоинта не совпали с телом"
    if not chain.verify_block(att["witness_pub"], att["payload_hash"], att.get("sig") or ""):
        return False, "подпись BIP340 недействительна"
    if known is not None and att["witness_pub"] not in known:
        return False, "свидетель не в списке признанных"
    return True, "ok"


# ── статус и проверка чекпоинта ─────────────────────────────────────────────

def count_witnesses(cp: dict, atts: list[dict], required: int = WITNESS_REQUIRED,
                    require_relay: int = WITNESS_MIN_RELAY_SOURCED) -> dict:
    """Подсчёт зачтённых свидетелей по чекпоинту и списку аттестаций.

    Отдельная функция (а не внутри checkpoint_status) затем, что её можно
    проверить на синтетических данных: в базе дубль по ключу гасится первичным
    ключом (chain_id, height, witness_pub), поэтому проверка «один ключ — один
    голос» должна быть видна и на уровне логики.
    """
    known = roster()
    good: list[dict] = []
    rejected: list[dict] = []
    seen: set[str] = set()
    for a in atts:
        ok, why = verify_attestation(a, known)
        if ok and a.get("witness_pub") == cp.get("pubkey"):
            rejected.append({"witness": a.get("witness_id"), "why": "самоаттестация: ключ совпал с ключом чекпоинта"})
            continue
        if not ok:
            rejected.append({"witness": a.get("witness_id"), "why": why})
            continue
        if a["witness_pub"] in seen:
            rejected.append({"witness": a.get("witness_id"), "why": "дубль ключа (тот же свидетель)"})
            continue
        seen.add(a["witness_pub"])
        good.append(a)
    relay_n = len([a for a in good if str(a.get("source", "")).startswith("relay")])
    chain_ok, chain_why = root_matches_chain(cp)
    shortage = len(good) < required
    no_independent = relay_n < require_relay
    reason = "ok"
    if not chain_ok:
        reason = chain_why
    elif shortage:
        reason = f"свидетелей {len(good)}, нужно {required}"
    elif no_independent:
        reason = f"независимых путей чтения {relay_n}, нужно {require_relay}"
    return {
        "chain_id": cp.get("chain_id"), "height": cp.get("height"), "root": cp.get("root"),
        "witnesses": len(good), "required": required, "signers": [a["witness_id"] for a in good],
        "relay_sourced": relay_n, "required_relay": require_relay,
        "rejected": rejected, "chain_ok": chain_ok, "ok": chain_ok and not shortage and not no_independent,
        "reason": reason,
    }


def checkpoint_status(height: int | None = None, required: int = WITNESS_REQUIRED,
                      require_relay: int = WITNESS_MIN_RELAY_SOURCED) -> dict:
    cp = checkpoint_local(height)
    if not cp:
        return {"ok": False, "reason": "чекпоинта нет", "witnesses": 0,
                "required": required, "signers": [], "rejected": []}
    return count_witnesses(cp, attestations(cp["height"], cp["chain_id"]), required, require_relay)


def verify_checkpoint(height: int | None = None, required: int = WITNESS_REQUIRED,
                      require_relay: int = WITNESS_MIN_RELAY_SOURCED) -> tuple[bool, str, dict]:
    st = checkpoint_status(height, required, require_relay)
    return bool(st.get("ok")), st.get("reason", ""), st


def witness_health(max_age_sec: int = HEALTH_MAX_AGE_SEC,
                   required: int = WITNESS_REQUIRED,
                   settling_sec: int = 300) -> dict:
    """Для сторожа: свежий ли чекпоинт и подтверждён ли он свидетелями.

    settling_sec — окно оседания. Сертификат уходит наружу раньше, чем свидетели
    успевают его прочитать и подписать, поэтому свежий чекпоинт без подписей —
    штатная задержка, а не сбой. Строгость живёт в verify_checkpoint: она
    окна не знает и без двух подписей проверку не пропускает.
    """
    cp = checkpoint_local()
    if not cp:
        return {"ok": False, "reason": "чекпоинта нет", "witnesses": 0}
    settle_age = int(time.time()) - int(cp.get("root_ts") or 0)
    if settle_age < settling_sec:
        return {"ok": True, "settling": True, "height": cp["height"], "witnesses": 0,
                "required": required, "age_min": settle_age // 60,
                "reason": f"свежий чекпоинт ({settle_age} с) — свидетели ещё не подписали, штатная задержка"}
    st = checkpoint_status(cp["height"], required)
    age = int(time.time()) - int(st.get("root_ts") or cp.get("root_ts") or 0)
    fresh = age <= max_age_sec
    ok = bool(st.get("ok")) and fresh
    reason = "ok"
    if not st.get("ok"):
        reason = st.get("reason", "")
    elif not fresh:
        reason = f"чекпоинт высоты {cp['height']} старше {max_age_sec // 60} мин ({age // 60} мин)"
    return {"ok": ok, "reason": reason, "height": cp["height"], "witnesses": st.get("witnesses", 0),
            "required": required, "signers": st.get("signers", []), "age_min": age // 60,
            "relay_sourced": st.get("relay_sourced", 0)}


# ── CLI ─────────────────────────────────────────────────────────────────────

def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="proof_mesh.witness", description="Свидетели чекпоинтов (фаза 4)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("keygen", help="завести ключ свидетеля")
    p.add_argument("--name", required=True)
    p.add_argument("--force", action="store_true")

    sub.add_parser("keys", help="показать свидетелей (без секретов)")
    sub.add_parser("status", help="статус последнего чекпоинта")

    p = sub.add_parser("attest", help="подписать чекпоинт")
    p.add_argument("--witness", required=True)
    p.add_argument("--source", default="local", choices=["local", "relay"])
    p.add_argument("--height", type=int, default=None)

    p = sub.add_parser("verify", help="проверить чекпоинт")
    p.add_argument("--height", type=int, default=None)
    p.add_argument("--require", type=int, default=WITNESS_REQUIRED)
    p.add_argument("--require-relay", type=int, default=WITNESS_MIN_RELAY_SOURCED)

    p = sub.add_parser("health", help="свежесть свидетельств (для сторожа)")
    p.add_argument("--max-age", type=int, default=HEALTH_MAX_AGE_SEC)
    p.add_argument("--settling", type=int, default=300)

    a = ap.parse_args(argv)

    if a.cmd == "keygen":
        _print(keygen(a.name, a.force))
    elif a.cmd == "keys":
        _print([{"name": n, "pubhex": v.get("pubhex"), "npub": v.get("npub")}
                for n, v in load_keys().items()])
    elif a.cmd == "attest":
        att = attest(a.witness, a.source, a.height)
        _print({"saved": True, "witness": att["witness_id"], "height": att["height"],
                "root": att["root"][:16] + "…", "source": att["source"], "pub": att["witness_pub"][:12] + "…"})
    elif a.cmd == "status":
        st = checkpoint_status()
        _print(st)
        return 0 if st.get("ok") else 1
    elif a.cmd == "verify":
        ok, why, st = verify_checkpoint(a.height, a.require, a.require_relay)
        _print({"ok": ok, "reason": why, **{k: st[k] for k in
                ("height", "witnesses", "required", "signers", "relay_sourced", "rejected") if k in st}})
        return 0 if ok else 1
    elif a.cmd == "health":
        h = witness_health(a.max_age, settling_sec=a.settling)
        _print(h)
        return 0 if h.get("ok") else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""audit_pack.py — аудит-пак по цепочке доказательств (фаза 3).

Что делает: собирает САМОДОСТАТОЧНЫЙ пакет, который посторонний человек может
проверить сам, в браузере, без доступа к нашим серверам и без доверия к нам.

Состав пакета (в каталоге):
    manifest.json   — что внутри, какие алгоритмы, хеши файлов, честные границы
    events.jsonl    — блоки: payload + все хеши + подпись + pubkey подписанта
    roots.jsonl     — опубликованные корни (реестр snin-hub), с подписью корня
    certs.jsonl     — внешние сертификаты Nostr (kind 8010), с id события
    README.md       — как проверить своими руками
    tampered/       — копия с ОДНОЙ подменённой записью (для негативного теста)

Формулы (совпадают с chain.py байт-в-байт):
    content_hash = sha256(payload)
    unsigned     = sha256(f"{prev_hash}|{content_hash}|{ts}")
    block_hash   = sha256(f"{unsigned}|{signature}")
    signature    = BIP340-schnorr(unsigned) ключом подписанта
    root         = block_hash верхнего блока; pubkey/sig корня — от того же блока

Команды:
    audit_pack.py build  --out <dir> [--events N]
    audit_pack.py verify <dir>        # независимая проверка пака (питон)
    audit_pack.py info   <dir>
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import sys
import time
from hashlib import sha256

BASE = "/home/agent/data"
MESH = f"{BASE}/sites/relay-mesh"
PROOF = f"{MESH}/proof_mesh"
AUDIT_DB = f"{PROOF}/snin_audit.db"
REGISTRY_DB = f"{BASE}/sites/snin-hub/proof_registry.db"

sys.path.insert(0, MESH)
sys.path.insert(0, PROOF)

from proof_mesh import chain, witness  # noqa: E402

PACK_VERSION = "audit-pack/1"
ROOTS_AND_CERTS_KINDS = (8010, 8011, 8012, 8013)


def sha(s: str | bytes) -> str:
    return sha256(s.encode() if isinstance(s, str) else s).hexdigest()


def file_sha(path: str) -> str:
    h = sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _rows(db: str, sql: str, args: tuple = ()) -> list[dict]:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=20)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql, args)]
    finally:
        con.close()


def collect(events_limit: int) -> dict:
    """Выборка: непрерывный хвост цепочки + все действия за сутки + корни."""
    tail = _rows(AUDIT_DB, "SELECT * FROM audit_events ORDER BY id DESC LIMIT ?", (events_limit,))
    if not tail:
        raise SystemExit("в цепочке нет событий")
    max_id = tail[0]["id"]
    min_id = tail[-1]["id"]

    # расширяем назад, чтобы попали все события действий за последние сутки
    day_ago = int(time.time()) - 86400
    for r in _rows(AUDIT_DB, "SELECT MIN(id) AS m FROM audit_events WHERE ts >= ?", (day_ago,)):
        if r["m"]:
            min_id = min(min_id, r["m"])

    events = _rows(AUDIT_DB, "SELECT * FROM audit_events WHERE id >= ? ORDER BY id", (min_id,))
    roots = _rows(
        REGISTRY_DB,
        "SELECT * FROM chain_roots WHERE chain_id='main' AND height >= ? ORDER BY height",
        (min_id,),
    )
    certs = _rows(
        AUDIT_DB,
        "SELECT * FROM cert_state WHERE height >= ? ORDER BY height",
        (max(1, min_id - 200),),
    )
    # аттестации свидетелей (фаза 4). Таблицы может не быть на старом реестре —
    # тогда пак собирается без свидетелей, и проверка честно скажет, что их нет.
    try:
        atts = _rows(
            REGISTRY_DB,
            """SELECT * FROM witness_attestations
               WHERE chain_id='main' AND height >= ? ORDER BY height, created_at""",
            (min_id,),
        )
    except sqlite3.Error:
        atts = []
    return {"events": events, "roots": roots, "certs": certs, "atts": atts,
            "min_id": min_id, "max_id": max_id}


def att_row(a: dict) -> dict:
    """Строка аттестации для пака: тело разворачиваем в объект, чтобы проверяющий
    сам пересчитал payload_hash и проверил подпись — без доверия к нам."""
    payload = a.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    return {
        "chain_id": a.get("chain_id", "main"), "height": a.get("height"),
        "root": a.get("root"), "witness_id": a.get("witness_id", ""),
        "witness_pub": a.get("witness_pub", ""), "payload": payload,
        "payload_hash": a.get("payload_hash", ""), "sig": a.get("sig", ""),
        "source": a.get("source", ""), "created_at": a.get("created_at", 0),
    }


def compute_stats(events: list[dict], roots: list[dict]) -> dict:
    heights = {e["id"] for e in events}
    covered = [r for r in roots if r["height"] in heights]
    signers: dict[str, int] = {}
    for e in events:
        signers[e["signer_pub"]] = signers.get(e["signer_pub"], 0) + 1
    return {
        "событий": len(events),
        "диапазон_высот": [events[0]["id"], events[-1]["id"]],
        "пропусков_в_нумерации": (events[-1]["id"] - events[0]["id"] + 1) - len(events),
        "корней_в_пакете": len(roots),
        "корней_внутри_диапазона": len(covered),
        "подписантов": len(signers),
        "блоков_подписанных_каждым": {k[:12]: v for k, v in sorted(signers.items(), key=lambda x: -x[1])},
    }


def build(out_dir: str, events_limit: int = 400) -> dict:
    data = collect(events_limit)
    events, roots, certs = data["events"], data["roots"], data["certs"]
    os.makedirs(out_dir, exist_ok=True)

    def write_jsonl(name: str, items: list[dict]) -> str:
        p = os.path.join(out_dir, name)
        with open(p, "w", encoding="utf-8") as f:
            for it in items:
                f.write(json.dumps(it, ensure_ascii=False, sort_keys=True) + "\n")
        return p

    ev_path = write_jsonl("events.jsonl", events)
    ro_path = write_jsonl("roots.jsonl", roots)
    ce_path = write_jsonl("certs.jsonl", certs)

    # ── свидетели чекпоинта (фаза 4) ─────────────────────────────────────
    atts = data.get("atts") or []
    wit_path = write_jsonl("witnesses.jsonl", [att_row(a) for a in atts])
    in_range = {e["id"] for e in events}
    att_heights = sorted({int(a["height"]) for a in atts if a.get("height") in in_range})
    cp_height = att_heights[-1] if att_heights else None
    cp = witness.checkpoint_local(cp_height) if cp_height is not None else None
    cp_root = (cp or {}).get("root") or ""
    if cp_height is not None:
        st = witness.count_witnesses(cp, [a for a in atts if int(a["height"]) == cp_height])
        wcheck = {"свидетелей": st["witnesses"], "нужно": st["required"],
                  "независимых_путей": st["relay_sourced"], "подписи": st["signers"],
                  "отклонено": st["rejected"], "итог": "ok" if st["ok"] else st["reason"]}
    else:
        wcheck = {"свидетелей": 0, "нужно": witness.WITNESS_REQUIRED,
                  "независимых_путей": 0, "подписи": [], "отклонено": [],
                  "итог": "в пакете нет подтверждённых чекпоинтов"}

    # негативный тест по свидетелям: тот же пакет, но подписей свидетелей нет
    nw_dir = os.path.join(out_dir, "no-witness")
    os.makedirs(nw_dir, exist_ok=True)
    nw_path = os.path.join(nw_dir, "witnesses.jsonl")
    open(nw_path, "w", encoding="utf-8").close()

    readme = f"""# Аудит-пак цепочки доказательств Sentinel

Пакет собран {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC.

## Что здесь

- `events.jsonl` — {len(events)} блоков (высоты {events[0]['id']}..{events[-1]['id']}), каждый с
  payload, хешами, подписью и публичным ключом подписанта;
- `roots.jsonl` — {len(roots)} опубликованных корней (реестр snin-hub) с подписью;
- `certs.jsonl` — внешние сертификаты Nostr (kind 8010) — то, что ушло наружу;
- `witnesses.jsonl` — подписи свидетелей под чекпоинтом (фаза 4): без них проверка
  не проходит;
- `manifest.json` — хеши этих файлов и границы пакета; `no-witness/` — негативный
  тест: тот же пакет без подписей свидетелей.

## Как проверить самому

Формулы (проверяются на любом языке, здесь — словами):

    content_hash = sha256(payload)
    unsigned     = sha256(prev_hash + "|" + content_hash + "|" + ts)
    block_hash   = sha256(unsigned + "|" + signature)

1. Возьми любую строку `events.jsonl`, посчитай `content_hash` от `payload` —
   он должен совпасть с полем `payload_hash`.
2. Посчитай `unsigned` и `block_hash` — должны совпасть с полями `block_hash`.
3. Проверь подпись `signature` (BIP340-schnorr) по `unsigned` и `signer_pub`.
4. Проверь `prev_hash` каждого блока = `block_hash` предыдущего (в пакете
   диапазон непрерывный).
5. Возьми корень из `roots.jsonl`: его `height` — это высота блока, а `root`
   должен быть равен `block_hash` этого блока. Подпись корня — подпись того же блока.
6. Возьми строку из `witnesses.jsonl` и посчитай `payload_hash` = sha256 от её тела
   `payload` в канонической форме: ключи по алфавиту, без пробелов, тот же порядок
   символов. Подпись `sig` (BIP340-schnorr) должна проверяться по `payload_hash` и
   `witness_pub`. Чекпоинт `height`/`root` должен совпасть с блоком этой высоты, а
   подписей РАЗНЫХ ключей должно быть не меньше двух, причём хотя бы одна — с
   источником `source`, начинающимся на `relay:` (свидетель прочитал сертификат
   с внешних релеев, а не из нашего же файла).

Если хоть один шаг не сходится — пакет недействителен, и это видно БЕЗ нашего участия.

## Честные границы

- Пакет не содержит всю цепочку: включён непрерывный хвост и все события
  действий за последние сутки. Полный лог доступен отдельно.
- Payload включён целиком: без него проверка хеша невозможна. Маскирование
  персональных данных до записи — отдельная задача.
- Проверка ссылок доказывает, что записи не переписаны ПОСЛЕ публикации корня.
  Она не доказывает, что в цепочку записано всё, что агент делал.
"""
    rm_path = os.path.join(out_dir, "README.md")
    open(rm_path, "w", encoding="utf-8").write(readme)

    # ── негативный тест: копия с одной подменённой записью ────────────────
    tam_dir = os.path.join(out_dir, "tampered")
    os.makedirs(tam_dir, exist_ok=True)
    victim = len(events) // 2
    t_events = [dict(e) for e in events]
    orig = t_events[victim]["payload"]
    t_events[victim]["payload"] = orig + " [ПОДМЕНА]"
    with open(os.path.join(tam_dir, "events.jsonl"), "w", encoding="utf-8") as f:
        for e in t_events:
            f.write(json.dumps(e, ensure_ascii=False, sort_keys=True) + "\n")
    shutil.copy(ro_path, os.path.join(tam_dir, "roots.jsonl"))

    files = {}
    for name in ("events.jsonl", "roots.jsonl", "certs.jsonl", "witnesses.jsonl", "README.md"):
        p = os.path.join(out_dir, name)
        files[name] = {
            "sha256": file_sha(p),
            "bytes": os.path.getsize(p),
            "rows": sum(1 for _ in open(p, encoding="utf-8")) if name.endswith(".jsonl") else None,
        }
    # копируем остальные файлы как есть, чтобы негативный тест ловил РОВНО
    # подменённую запись, а не отсутствующие файлы
    shutil.copy(ce_path, os.path.join(tam_dir, "certs.jsonl"))
    shutil.copy(rm_path, os.path.join(tam_dir, "README.md"))
    shutil.copy(wit_path, os.path.join(tam_dir, "witnesses.jsonl"))
    tam_files = {}
    for name in ("events.jsonl", "roots.jsonl", "certs.jsonl", "witnesses.jsonl", "README.md"):
        p = os.path.join(tam_dir, name)
        tam_files[name] = {"sha256": file_sha(p), "bytes": os.path.getsize(p), "rows": None}

    manifest = {
        "pack_version": PACK_VERSION,
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "algorithms": {
            "hash": "sha256",
            "hash_concat": 'sha256(f"{prev_hash}|{content_hash}|{ts}")',
            "block_hash_concat": 'sha256(f"{unsigned}|{signature}")',
            "signature": "BIP340-schnorr (x-only pubkey, 32-byte message hash)",
            "root_rule": "root = block_hash верхнего блока; pubkey/sig корня — от того же блока",
        },
        "chain": {"id": "main", "высоты": [events[0]["id"], events[-1]["id"]], "tip": events[-1]["block_hash"]},
        "stats": compute_stats(events, roots),
        "files": files,
        "tampered_copy": {
            "dir": "tampered",
            "что_испорчено": f"payload блока id={t_events[victim]['id']} (дописано ' [ПОДМЕНА]')",
            "зачем": "негативный тест: проверка обязана сказать «НЕ СОВПАЛО», а не «ошибка»",
            "files": tam_files,
        },
        "witnesses": {
            "зачем": "чекпоинт подтверждён независимыми свидетелями; без двух подписей проверка не проходит",
            "required": witness.WITNESS_REQUIRED,
            "required_relay": witness.WITNESS_MIN_RELAY_SOURCED,
            "чекпоинт": {"height": cp_height, "root": cp_root},
            "проверено_при_сборке": wcheck,
            "файлы": {"witnesses.jsonl": files["witnesses.jsonl"]},
            "как_проверить": ("payload_hash = sha256(канонический JSON тела payload: ключи по алфавиту, без пробелов); "
                              "подпись BIP340-schnorr проверяется по payload_hash и witness_pub; "
                              "нужно >=2 разных ключа, из них >=1 с источником source=relay:..."),
            "негативный_тест": {
                "dir": "no-witness",
                "файл": "witnesses.jsonl",
                "что_испорчено": "подписей свидетелей нет (пустой файл)",
                "зачем": "проверка обязана сказать «НЕ СОВПАЛО: чекпоинт не подтверждён свидетелями», а не «ПРОВЕРЕНО»",
                "sha256": file_sha(nw_path), "bytes": 0, "rows": 0,
            },
        },
        "limits": {
            "событий_в_пакете": len(events),
            "полная_цепочка_не_включена": True,
            "payload_включён_целиком": True,
        },
    }
    man_path = os.path.join(out_dir, "manifest.json")
    open(man_path, "w", encoding="utf-8").write(json.dumps(manifest, ensure_ascii=False, indent=2))

    # Манифест подменённой копии: хеши ФАЙЛОВ пересчитаны (целостность файлов
    # сходится), а математика блоков — нет. Так негативный тест проверяет именно
    # проверку хешей и подписей, а не «поймали несовпадение файла».
    tam_manifest = json.loads(json.dumps(manifest))
    tam_manifest["files"] = {
        **{k: v for k, v in manifest["files"].items() if k not in ("events.jsonl", "roots.jsonl")},
        **tam_files,
    }
    tam_manifest["pack_version"] = PACK_VERSION + "+tampered"
    tam_manifest["tampered_copy"]["это_и_есть_подмена"] = True
    open(os.path.join(tam_dir, "manifest.json"), "w", encoding="utf-8").write(
        json.dumps(tam_manifest, ensure_ascii=False, indent=2)
    )
    return {"out": out_dir, "manifest": manifest}


# ── независимая проверка пака (то же, что делает браузер) ────────────────────

def count_pack_witnesses(atts: list[dict], cp_block: dict | None) -> dict:
    """Считаем свидетелей ровно так же, как обязан считать любой проверяющий:
    подпись + разные ключи + независимый путь чтения. Самоаттестация и дубль
    ключа не в счёт. Возвращает {зачтено, всего, независимых_путей, отклонено}."""
    cp_signer = cp_block["signer_pub"] if cp_block else ""
    seen: set[str] = set()
    rejected: list[str] = []
    relay_n = 0
    for a in atts:
        ok, why = witness.verify_attestation(a)
        tag = a.get("witness_id") or (a.get("witness_pub") or "?")[:12]
        if not ok:
            rejected.append(f"{tag}: {why}")
            continue
        if cp_signer and a.get("witness_pub") == cp_signer:
            rejected.append(f"{tag}: самоаттестация (тот же ключ, что подписал чекпоинт)")
            continue
        if a.get("witness_pub") in seen:
            rejected.append(f"{tag}: дубль ключа")
            continue
        if cp_block and a.get("root") != cp_block["block_hash"]:
            rejected.append(f"{tag}: корень аттестации не совпал с блоком высоты {a.get('height')}")
            continue
        seen.add(a["witness_pub"])
        if str(a.get("source", "")).startswith("relay"):
            relay_n += 1
    return {"зачтено": len(seen), "всего": len(atts), "независимых_путей": relay_n, "отклонено": rejected}


def verify_witness_negative(pack_dir: str) -> dict:
    """Приёмка фазы 4: пакет без подписей свидетелей НЕ проходит проверку.

    Берём объявленный в манифесте негативный фикстур (пустой witnesses.jsonl),
    проверяем его по хешу и убеждаемся, что требование «2 подписи, одна с релеев»
    соблюсти нельзя. Ожидаемый итог — «НЕ СОВПАЛО» с причиной про свидетелей,
    а не «ПРОВЕРЕНО» и не техническая ошибка чтения.
    """
    man = json.load(open(os.path.join(pack_dir, "manifest.json"), encoding="utf-8"))
    block = (man.get("witnesses") or {}).get("негативный_тест") or {}
    wit = man.get("witnesses") or {}
    need = int(wit.get("required") or witness.WITNESS_REQUIRED)
    need_relay = int(wit.get("required_relay") or witness.WITNESS_MIN_RELAY_SOURCED)
    fpath = os.path.join(pack_dir, block.get("dir", "no-witness"), block.get("файл", "witnesses.jsonl"))
    file_ok = os.path.exists(fpath) and file_sha(fpath) == block.get("sha256")
    atts = []
    if os.path.exists(fpath):
        atts = [json.loads(l) for l in open(fpath, encoding="utf-8") if l.strip()]
    events = [json.loads(l) for l in open(os.path.join(pack_dir, "events.jsonl"), encoding="utf-8")]
    by_height = {e["id"]: e for e in events}
    cp_height = (wit.get("чекпоинт") or {}).get("height")
    wc = count_pack_witnesses(atts, by_height.get(cp_height) if cp_height is not None else None)
    pass_negative = wc["зачтено"] < need
    return {
        "фикстур": {"файл": fpath, "по_хешу_сходится": bool(file_ok), "строк": len(atts)},
        "свидетелей": wc["зачтено"], "нужно": need, "независимых_путей": wc["независимых_путей"],
        "нужно_независимых": need_relay,
        "итог": "НЕ СОВПАЛО" if pass_negative else "ПРОВЕРЕНО",
        "причина": (f"чекпоинт не подтверждён свидетелями: {wc['зачтено']} из {need}" if pass_negative
                    else "пакет без свидетелей прошёл проверку — это дефект проверки"),
        "тест_пройден": bool(file_ok and pass_negative),
    }


def verify_pack(pack_dir: str, expect_fail: bool = False) -> dict:
    man = json.load(open(os.path.join(pack_dir, "manifest.json"), encoding="utf-8"))
    checks = {"файлы": 0, "файлов_всего": 0, "события": 0, "событий_всего": 0,
              "подписи": 0, "связи": 0, "корни": 0,
              "свидетели": 0, "свидетелей_всего": 0, "независимых_путей": 0, "ошибки": []}
    for name, meta in man["files"].items():
        p = os.path.join(pack_dir, name)
        checks["файлов_всего"] += 1
        if os.path.exists(p) and file_sha(p) == meta["sha256"]:
            checks["файлы"] += 1
        else:
            checks["ошибки"].append(f"файл {name}: sha256 не совпал")

    events = [json.loads(l) for l in open(os.path.join(pack_dir, "events.jsonl"), encoding="utf-8")]
    prev_expected = None
    for e in events:
        checks["событий_всего"] += 1
        ch = sha(e["payload"])
        un = sha(f"{e['prev_hash']}|{ch}|{e['ts']}")
        bh = sha(f"{un}|{e['signature']}")
        ok = ch == e["payload_hash"] and bh == e["block_hash"]
        if ok:
            checks["события"] += 1
        else:
            checks["ошибки"].append(
                f"блок {e['id']}: хеш не совпал (payload_hash={'совпал' if ch == e['payload_hash'] else 'НЕТ'},"
                f" block_hash={'совпал' if bh == e['block_hash'] else 'НЕТ'})"
            )
        if chain.verify_block(e["signer_pub"], un, e["signature"]):
            checks["подписи"] += 1
        else:
            checks["ошибки"].append(f"блок {e['id']}: подпись BIP340 недействительна")
        if prev_expected is None or e["prev_hash"] == prev_expected:
            checks["связи"] += 1
        else:
            checks["ошибки"].append(f"блок {e['id']}: разрыв связи с предыдущим")
        prev_expected = e["block_hash"]

    roots = [json.loads(l) for l in open(os.path.join(pack_dir, "roots.jsonl"), encoding="utf-8")]
    by_height = {e["id"]: e for e in events}
    for r in roots:
        e = by_height.get(r["height"])
        if not e:
            continue
        ch = sha(e["payload"])
        un = sha(f"{e['prev_hash']}|{ch}|{e['ts']}")
        if r["root"] == e["block_hash"] and chain.verify_block(r["pubkey"], un, r["sig"]):
            checks["корни"] += 1
        else:
            checks["ошибки"].append(f"корень высоты {r['height']}: не соответствует блоку или подпись плохая")

    # ── свидетели чекпоинта ──────────────────────────────────────────────
    wit_file = os.path.join(pack_dir, "witnesses.jsonl")
    man_wit = man.get("witnesses") or {}
    cp_height = (man_wit.get("чекпоинт") or {}).get("height")
    if not os.path.exists(wit_file):
        checks["ошибки"].append("файл witnesses.jsonl отсутствует — чекпоинт не подтверждён свидетелями")
        atts = []
    else:
        atts = [json.loads(l) for l in open(wit_file, encoding="utf-8") if l.strip()]
    cp_block = by_height.get(cp_height) if cp_height is not None else None
    # Считаем ТОЛЬКО аттестации чекпоинта пака. В файле лежит история подписей
    # по разным высотам; если смешать их, голоса одного свидетеля по разным
    # чекпоинтам погасят друг друга как «дубли» (поймано проверкой в браузере).
    atts_all = atts
    atts = [a for a in atts_all if cp_height is not None and a.get("height") == cp_height]
    checks["аттестаций_в_файле"] = len(atts_all)
    checks["аттестаций_чекпоинта"] = len(atts)
    wc = count_pack_witnesses(atts, cp_block)
    checks["свидетелей_всего"] = wc["всего"]
    checks["свидетели"] = wc["зачтено"]
    checks["независимых_путей"] = wc["независимых_путей"]
    checks["отклонённые_аттестации"] = wc["отклонено"]
    need = int(man_wit.get("required") or witness.WITNESS_REQUIRED)
    need_relay = int(man_wit.get("required_relay") or witness.WITNESS_MIN_RELAY_SOURCED)
    if wc["зачтено"] < need:
        checks["ошибки"].append(
            f"чекпоинт не подтверждён свидетелями: {wc['зачтено']} из {need}"
            + (f" (отклонено: {'; '.join(wc['отклонено'])})" if wc["отклонено"] else ""))
    elif wc["независимых_путей"] < need_relay:
        checks["ошибки"].append(
            f"нет независимого пути чтения у свидетелей: {wc['независимых_путей']} из {need_relay}")

    checks["итог"] = "ПРОВЕРЕНО" if not checks["ошибки"] else "НЕ СОВПАЛО"
    if expect_fail:
        checks["ожидали_провал"] = checks["итог"] == "НЕ СОВПАЛО"
    return checks


def main() -> int:
    p = argparse.ArgumentParser(description="Аудит-пак цепочки доказательств")
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--out", required=True)
    b.add_argument("--events", type=int, default=400)
    v = sub.add_parser("verify")
    v.add_argument("dir")
    v.add_argument("--expect-fail", action="store_true")
    n = sub.add_parser("verify-no-witness", help="негативный тест: пакет без свидетелей")
    n.add_argument("dir")
    i = sub.add_parser("info")
    i.add_argument("dir")
    a = p.parse_args()

    if a.cmd == "build":
        res = build(a.out, a.events)
        print(json.dumps(res["manifest"]["stats"], ensure_ascii=False, indent=2))
        print(f"    пакет: {a.out}")
        return 0
    if a.cmd == "verify":
        r = verify_pack(a.dir, a.expect_fail)
        print(json.dumps(r, ensure_ascii=False, indent=2))
        if a.expect_fail:
            return 0 if r["итог"] == "НЕ СОВПАЛО" else 1
        return 0 if r["итог"] == "ПРОВЕРЕНО" else 1
    if a.cmd == "verify-no-witness":
        r = verify_witness_negative(a.dir)
        print(json.dumps(r, ensure_ascii=False, indent=2))
        return 0 if r["тест_пройден"] else 1
    m = json.load(open(os.path.join(a.dir, "manifest.json"), encoding="utf-8"))
    print(json.dumps({k: m[k] for k in ("pack_version", "generated_at_utc", "chain", "stats", "limits")},
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

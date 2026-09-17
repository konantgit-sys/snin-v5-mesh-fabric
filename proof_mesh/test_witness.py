"""
Тесты фазы 4 (свидетели). Гоняются на ВРЕМЕННОМ реестре и ВРЕМЕННЫХ ключах:
продовый proof_registry.db только читается, в него ничего не пишется.

Критерии приёмки спеки, которые здесь проверяются:
  * чекпоинт без свидетелей проверку НЕ проходит;
  * с 2 свидетелями и независимым путём чтения — проходит;
  * самоаттестация (ключ совпал с ключом чекпоинта) не считается;
  * дубль ключа не считается дважды;
  * подмена в аттестации ломает проверку;
  * свидетель отказывается подписывать чекпоинт, не сходящийся с цепочкой;
  * чекпоинт без свежих свидетелей даёт сигнал (health).

Запуск: python3 proof_mesh/test_witness.py
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, "/home/agent/data/sites/relay-mesh")

TMP = Path(tempfile.mkdtemp(prefix="witness_test_"))
os.environ["SPM_WITNESS_REGISTRY"] = str(TMP / "registry.db")
os.environ["SPM_WITNESS_KEYS"] = str(TMP / "witness_keys.json")
os.environ["SPM_WITNESS_AUDIT"] = "/home/agent/data/sites/relay-mesh/proof_mesh/snin_audit.db"

from proof_mesh import chain, witness  # noqa: E402

PASS, FAIL = [], []


def check(name: str, cond: bool, detail: str = "") -> None:
    (PASS if cond else FAIL).append(name)
    print(f"  {'✅' if cond else '❌'} {name}{(' — ' + detail) if detail else ''}")


def seed_registry(rows: list[dict]) -> None:
    """Завести временный реестр с заданными чекпоинтами."""
    with sqlite3.connect(str(TMP / "registry.db")) as c:
        c.execute("""CREATE TABLE IF NOT EXISTS chain_roots (
            chain_id TEXT NOT NULL DEFAULT 'main', height INTEGER NOT NULL,
            root TEXT NOT NULL, root_ts INTEGER NOT NULL,
            pubkey TEXT NOT NULL DEFAULT '', sig TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (chain_id, height))""")
        for r in rows:
            c.execute("INSERT OR REPLACE INTO chain_roots (chain_id,height,root,root_ts,pubkey,sig) VALUES (?,?,?,?,?,?)",
                      ("main", r["height"], r["root"], r["root_ts"], r.get("pubkey", ""), r.get("sig", "")))


def prod_roots(limit: int = 6) -> list[dict]:
    uri = f"file:{chain.PROOF_REGISTRY_DB}?mode=ro"
    with sqlite3.connect(uri, uri=True) as c:
        c.row_factory = sqlite3.Row
        return [dict(r) for r in c.execute(
            "SELECT * FROM chain_roots WHERE chain_id='main' ORDER BY height DESC LIMIT ?", (limit,))]


print(f"временный каталог: {TMP}")
roots = prod_roots(6)
if len(roots) < 3:
    print("нет чекпоинтов в проде — тест невозможен")
    raise SystemExit(2)
seed_registry([{k: r[k] for k in ("height", "root", "root_ts", "pubkey", "sig")} for r in roots])
H1, H2, H3 = roots[0]["height"], roots[1]["height"], roots[2]["height"]
print(f"высоты для тестов: {H1} {H2} {H3}\n")

print("── 1. чекпоинт без свидетелей ──")
ok, why, st = witness.verify_checkpoint(H1)
check("без свидетелей — отказ", ok is False, why)
check("причина называет нехватку", "нужно 2" in why or "свидетелей" in why, why)

print("\n── 2. ключи свидетелей ──")
w1 = witness.keygen("w1")
w2 = witness.keygen("w2")
w3 = witness.keygen("w3")
keys_file = TMP / "witness_keys.json"
mode = oct(os.stat(keys_file).st_mode & 0o777)
check("ключи созданы", all(k["pubhex"] for k in (w1, w2, w3)))
check("файл ключей 600", mode == "0o600", f"права {mode}")
check("ключи разные", len({w1["pubhex"], w2["pubhex"], w3["pubhex"]}) == 3)

print("\n── 3. один свидетель — всё ещё отказ ──")
witness.attest("w1", "local", H1)
ok, why, st = witness.verify_checkpoint(H1)
check("1 свидетель — отказ", ok is False, why)
check("посчитан ровно один", st["witnesses"] == 1, str(st["witnesses"]))

print("\n── 4. два ЛОКАЛЬНЫХ свидетеля — отказ (нет независимого пути чтения) ──")
witness.attest("w2", "local", H1)
ok, why, st = witness.verify_checkpoint(H1)
check("2 локальных — отказ", ok is False, why)
check("причина про независимый путь", "независимых" in why, why)

print("\n── 5. независимый путь чтения: реальный сертификат с релеев ──")
cp_relay = witness.checkpoint_relay()
relay_detail = "релеи не ответили"
if cp_relay:
    relay_detail = f"высота {cp_relay['height']}, релеев {(cp_relay.get('relays') or [])!r}"
    m_ok, m_why = witness.root_matches_chain(cp_relay)
    check("сертификат с релеев сходится с цепочкой", m_ok, m_why or relay_detail)
    if m_ok:
        seed_registry([{"height": cp_relay["height"], "root": cp_relay["root"],
                        "root_ts": cp_relay["root_ts"], "pubkey": cp_relay["pubkey"], "sig": ""}])
        H4 = cp_relay["height"]
        witness.attest("w3", "relay", H4)
        witness.attest("w1", "local", H4)
        witness.attest("w2", "local", H4)
        ok, why, st = witness.verify_checkpoint(H4)
        check("3 свидетеля, 1 из них с релеев — ПРОХОДИТ", ok is True, why)
        check("независимых путей >= 1", st["relay_sourced"] >= 1, str(st["relay_sourced"]))
        check("в источниках видно релей", any(a["source"].startswith("relay") for a in witness.attestations(H4)))
else:
    check("сертификат с релеев прочитан", False, relay_detail)

print("\n── 6. подмена внутри аттестации ──")
att = [a for a in witness.attestations(H1) if a["witness_id"] == "w1"][0]
bad_root = json.loads(json.dumps(att))
bad_root["root"] = "f" * 64
check("подменённый root — отказ", witness.verify_attestation(bad_root)[0] is False,
      witness.verify_attestation(bad_root)[1])
bad_sig = json.loads(json.dumps(att))
bad_sig["sig"] = "00" * 64
check("подменённая подпись — отказ", witness.verify_attestation(bad_sig)[0] is False,
      witness.verify_attestation(bad_sig)[1])
bad_body = json.loads(json.dumps(att))
bad_body["payload"]["height"] = att["height"] + 1
check("правка тела — отказ", witness.verify_attestation(bad_body)[0] is False,
      witness.verify_attestation(bad_body)[1])
check("целая аттестация проходит", witness.verify_attestation(att)[0] is True)

print("\n── 7. самоаттестация своим же ключом ──")
seed_registry([{"height": H2, "root": roots[1]["root"], "root_ts": roots[1]["root_ts"],
                "pubkey": w1["pubhex"], "sig": ""}])
witness.attest("w1", "local", H2)
ok, why, st = witness.verify_checkpoint(H2)
check("самоаттестация не в счёт", st["witnesses"] == 0 and ok is False, why)
check("названа причина", any("самоаттестация" in r["why"] for r in st["rejected"]), json.dumps(st["rejected"], ensure_ascii=False))

print("\n── 8. дубль ключа под другим именем ──")
keys = witness.load_keys()
keys["w2-copy"] = dict(keys["w2"])
witness.save_keys(keys)
seed_registry([{"height": H3, "root": roots[2]["root"], "root_ts": roots[2]["root_ts"],
                "pubkey": roots[2]["pubkey"], "sig": ""}])
witness.attest("w2", "local", H3)
witness.attest("w2-copy", "local", H3)
stored = witness.attestations(H3)
check("на ключ — одна аттестация (первичный ключ таблицы)", len(stored) == 1, f"строк {len(stored)}")
ok, why, st = witness.verify_checkpoint(H3)
check("дубль не удваивает счёт", st["witnesses"] == 1, str(st["witnesses"]))
# логическая страховка: два синтетических голоса одним ключом не должны дать два
synth = witness.count_witnesses(witness.checkpoint_local(H3),
                                [dict(stored[0]), dict(stored[0], witness_id="w2-copy")])
check("логика отбивает дубль", synth["witnesses"] == 1 and any("дубль" in r["why"] for r in synth["rejected"]),
      json.dumps(synth["rejected"], ensure_ascii=False))
del keys["w2-copy"]
witness.save_keys(keys)

print("\n── 9. свидетель отказывается подписывать чужой корень ──")
seed_registry([{"height": H1, "root": "a" * 64, "root_ts": int(time.time()),
                "pubkey": roots[0]["pubkey"], "sig": ""}])
refused = False
try:
    witness.attest("w2", "local", H1)
except SystemExit as e:
    refused = "отказывается подписывать" in str(e)
check("подпись не поставлена", refused)

print("\n── 10. свежесть свидетельств (сигнал для сторожа) ──")
# Свой audit-DB: в нём и сертификат с нужным временем, и блок, с которым он должен
# сходиться. Живая цепочка не участвует — иначе возраст чекпоинта не предсказуем,
# а свежий сертификат попадает в окно оседания.
ALT_DB = str(TMP / "alt_audit.db")
H_ALT, ROOT_ALT, OLD_TS = 999001, "b" * 64, int(time.time()) - 3600
with sqlite3.connect(ALT_DB) as c:
    c.execute("""CREATE TABLE cert_state (id INTEGER PRIMARY KEY AUTOINCREMENT, kind INTEGER,
                 root TEXT, height INTEGER, prev_cert_id TEXT, event_id TEXT, ts INTEGER)""")
    c.execute("INSERT INTO cert_state (kind, root, height, prev_cert_id, event_id, ts) VALUES (8010,?,?,?,?,?)",
              (ROOT_ALT, H_ALT, "", "", OLD_TS))
    c.execute("""CREATE TABLE audit_events (id INTEGER PRIMARY KEY, ts INTEGER, payload TEXT,
                 prev_hash TEXT, block_hash TEXT, signature TEXT, signer_pub TEXT,
                 agent_id TEXT, payload_hash TEXT, action TEXT)""")
    c.execute("""INSERT INTO audit_events (id, ts, payload, prev_hash, block_hash, signature,
                 signer_pub, agent_id, payload_hash, action) VALUES (?,?,?,?,?,?,?,?,?,?)""",
              (H_ALT, OLD_TS, "{}", "", ROOT_ALT, "", "a" * 64, "test", "", "test"))
seed_registry([{"height": H_ALT, "root": ROOT_ALT, "root_ts": OLD_TS, "pubkey": "a" * 64, "sig": ""}])
os.environ["SPM_WITNESS_AUDIT"] = ALT_DB
try:
    # сертификата с этой высоты на релеях нет, поэтому путь чтения подменён на
    # локальный чекпоинт; РЕАЛЬНОЕ чтение с релеев проверено в п.5
    _real_relay = witness.checkpoint_relay
    witness.checkpoint_relay = lambda height=None: witness.checkpoint_local(height)
    try:
        witness.attest("w1", "local", H_ALT)
        witness.attest("w2", "relay", H_ALT)
    finally:
        witness.checkpoint_relay = _real_relay
    h_ok = witness.witness_health(max_age_sec=7200, settling_sec=0)
    check("подтверждённый чекпоинт — ok", h_ok["ok"] is True, json.dumps(h_ok, ensure_ascii=False))
    h_age = witness.witness_health(max_age_sec=60, settling_sec=0)
    check("старый чекпоинт — сигнал по возрасту",
          h_age["ok"] is False and "старше" in h_age.get("reason", ""), h_age.get("reason", ""))
    h_set = witness.witness_health(settling_sec=10 ** 9)
    check("окно оседания гасит ложный шум",
          h_set["ok"] is True and "штатная задержка" in h_set.get("reason", ""), h_set.get("reason", ""))
finally:
    os.environ["SPM_WITNESS_AUDIT"] = "/home/agent/data/sites/relay-mesh/proof_mesh/snin_audit.db"

print("\n── 11. ключ свидетеля не из списка признанных ──")
Keys, _ = chain._sdk()
alien = Keys.generate()
alien_pub = alien.public_key().to_hex()
alien_att = {"chain_id": "main", "height": H1, "root": roots[0]["root"],
             "witness_id": "alien", "witness_pub": alien_pub,
             "payload": {"attest": witness.ATTEST_KIND, "chain_id": "main", "height": H1,
                         "root": roots[0]["root"], "root_ts": roots[0]["root_ts"],
                         "source": "relay:fake", "witness_pub": alien_pub, "created_at": int(time.time())},
             "source": "relay:fake", "created_at": int(time.time())}
alien_att["payload_hash"] = witness.payload_hash(alien_att["payload"])
alien_att["sig"] = chain.sign_block(alien.secret_key().to_bech32(), alien_att["payload_hash"])
check("чужой ключ отбит ростером", witness.verify_attestation(alien_att, witness.roster())[0] is False,
      witness.verify_attestation(alien_att, witness.roster())[1])

print(f"\n═══ ИТОГ: ✅ {len(PASS)} / ❌ {len(FAIL)} ═══")
if FAIL:
    print("провалено: " + ", ".join(FAIL))
raise SystemExit(1 if FAIL else 0)

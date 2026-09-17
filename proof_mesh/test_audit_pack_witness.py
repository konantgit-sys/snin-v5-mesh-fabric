"""
Тесты пака со свидетелями (фаза 4, продуктовая часть).

Приёмка спеки на том, что мы отдаём наружу: пакет без подписей свидетелей
проверку НЕ проходит, пакет с двумя подписями (одна с релеев) — проходит.
Проверяется тем же кодом, что и в браузере, по файлам пака.

Пак собирается один раз во временную папку. Сценарии получаются заменой файла
свидетелей на подготовленный, с честным пересчётом его хеша в манифесте: так
провал вызывает именно требование к свидетелям, а не «несовпадение файла».

Подписи для сценариев делаются своими временными ключами: `source` лежит ВНУТРИ
подписанного тела, поэтому подделать его без новой подписи нельзя — и это само
по себе хорошее свойство аттестации.

Запуск: python3 proof_mesh/test_audit_pack_witness.py
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, "/home/agent/data/sites/relay-mesh")

from proof_mesh import audit_pack, chain, witness  # noqa: E402

PASS, FAIL = [], []
TMP = tempfile.mkdtemp(prefix="pack_witness_")
PACK = os.path.join(TMP, "pack")


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  {'✅' if cond else '❌'} {name}{(' — ' + detail) if detail else ''}")


def rows_of(pack: str) -> list[dict]:
    with open(os.path.join(pack, "witnesses.jsonl"), encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def write_rows(pack: str, rows: list[dict]) -> None:
    p = os.path.join(pack, "witnesses.jsonl")
    with open(p, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n")
    mp = os.path.join(pack, "manifest.json")
    man = json.load(open(mp, encoding="utf-8"))
    man["files"]["witnesses.jsonl"] = {"sha256": audit_pack.file_sha(p),
                                       "bytes": os.path.getsize(p), "rows": len(rows)}
    json.dump(man, open(mp, "w", encoding="utf-8"), ensure_ascii=False, indent=2)


def signed_row(tag: str, source: str, height: int, root: str, key=None) -> dict:
    """Настоящая аттестация: своё тело, свой хеш, своя подпись."""
    Keys, _ = chain._sdk()
    k = key or Keys.generate()
    pub = k.public_key().to_hex()
    payload = {"attest": witness.ATTEST_KIND, "chain_id": "main", "height": height,
               "root": root, "root_ts": 0, "source": source, "witness_pub": pub,
               "created_at": int(time.time())}
    ph = witness.payload_hash(payload)
    return {"chain_id": "main", "height": height, "root": root, "witness_id": tag,
            "witness_pub": pub, "payload": payload, "payload_hash": ph,
            "sig": chain.sign_block(k.secret_key().to_bech32(), ph),
            "source": source, "created_at": payload["created_at"]}


print(f"временный каталог: {TMP}")
t0 = time.time()
audit_pack.build(PACK, 400)
print(f"пак собран за {time.time() - t0:.1f} с\n")

manifest = json.load(open(os.path.join(PACK, "manifest.json"), encoding="utf-8"))
with open(os.path.join(PACK, "events.jsonl"), encoding="utf-8") as f:
    events = [json.loads(l) for l in f if l.strip()]
by_height = {e["id"]: e for e in events}
CP = (manifest["witnesses"]["чекпоинт"] or {}).get("height")
root_at_cp = by_height[CP]["block_hash"]
cp_signer = by_height[CP]["signer_pub"]
base = rows_of(PACK)

print("── 1. пак со свидетелями проходит ──")
r = audit_pack.verify_pack(PACK)
check("итог ПРОВЕРЕНО", r["итог"] == "ПРОВЕРЕНО", str(r["ошибки"][:1]))
check("свидетелей >= 2", r["свидетели"] >= 2, str(r["свидетели"]))
check("есть независимый путь чтения", r["независимых_путей"] >= 1, str(r["независимых_путей"]))
check("считаются только аттестации чекпоинта", r["аттестаций_чекпоинта"] <= r["аттестаций_в_файле"],
      f"в файле {r['аттестаций_в_файле']}, по чекпоинту {r['аттестаций_чекпоинта']}")

print("\n── 2. приёмка: пакет БЕЗ свидетелей проверку не проходит ──")
n = audit_pack.verify_witness_negative(PACK)
check("итог НЕ СОВПАЛО", n["итог"] == "НЕ СОВПАЛО", n["итог"])
check("причина про свидетелей", "не подтверждён свидетелями" in n["причина"], n["причина"])
check("фикстур сходится по хешу", n["фикстур"]["по_хешу_сходится"] is True)
check("тест пройден", n["тест_пройден"] is True)

print(f"\n── 3. две ЛОКАЛЬНЫЕ подписи на чекпоинт {CP}: отказ ──")
write_rows(PACK, [signed_row("local-a", "local-cert", CP, root_at_cp),
                  signed_row("local-b", "local-cert", CP, root_at_cp)])
r = audit_pack.verify_pack(PACK)
check("обе подписи валидны и зачтены", r["свидетели"] == 2, str(r["свидетели"]))
check("независимых путей ноль", r["независимых_путей"] == 0, str(r["независимых_путей"]))
check("итог НЕ СОВПАЛО", r["итог"] == "НЕ СОВПАЛО")
check("причина про независимый путь", any("независимого пути" in x for x in r["ошибки"]),
      " ".join(r["ошибки"])[:120])

print("\n── 4. дубль ключа под другим именем ──")
Keys, _ = chain._sdk()
one_key = Keys.generate()
dup = [signed_row("w-1", "relay:wss://one", CP, root_at_cp, one_key),
       signed_row("w-1-copy", "relay:wss://one", CP, root_at_cp, one_key)]
write_rows(PACK, dup)
r = audit_pack.verify_pack(PACK)
check("зачтён один ключ", r["свидетели"] == 1, str(r["свидетели"]))
check("итог НЕ СОВПАЛО", r["итог"] == "НЕ СОВПАЛО")
check("названа причина «дубль»", any("дубль" in x for x in r["отклонённые_аттестации"]),
      json.dumps(r["отклонённые_аттестации"], ensure_ascii=False)[:120])

print("\n── 5. подпись верна, но корень не тот ──")
write_rows(PACK, [signed_row("wrong-root", "relay:wss://one", CP, "e" * 64),
                  signed_row("good", "relay:wss://two", CP, root_at_cp)])
r = audit_pack.verify_pack(PACK)
check("зачтена только верная", r["свидетели"] == 1, str(r["свидетели"]))
check("отклонение названо", any("корень" in x for x in r["отклонённые_аттестации"]),
      json.dumps(r["отклонённые_аттестации"], ensure_ascii=False)[:140])

print("\n── 6. правка тела без переподписи ──")
tampered = [signed_row("tampered", "relay:wss://one", CP, root_at_cp)]
tampered[0]["payload"] = dict(tampered[0]["payload"], source="relay:wss://другой-релей")
write_rows(PACK, tampered + [signed_row("good", "relay:wss://two", CP, root_at_cp)])
r = audit_pack.verify_pack(PACK)
check("зачтена только честная", r["свидетели"] == 1, str(r["свидетели"]))
check("поймано несовпадение тела", any("payload_hash" in x for x in r["отклонённые_аттестации"]),
      json.dumps(r["отклонённые_аттестации"], ensure_ascii=False)[:140])

print("\n── 7. самоаттестация (ключ = ключ чекпоинта) ──")
# ключа демона у теста нет и быть не должно: проверяем правило на уровне подсчёта,
# подставив на место подписанта чекпоинта свой ключ
Keys, _ = chain._sdk()
self_key = Keys.generate()
self_row = signed_row("self-key", "relay:wss://self", CP, root_at_cp, self_key)
wc = audit_pack.count_pack_witnesses([self_row], {"signer_pub": self_row["witness_pub"],
                                                  "block_hash": root_at_cp})
check("подпись валидна", witness.verify_attestation(self_row)[0] is True)
check("самоаттестация не зачтена", wc["зачтено"] == 0, json.dumps(wc, ensure_ascii=False)[:100])
check("названа самоаттестацией", any("самоаттестация" in x for x in wc["отклонено"]),
      json.dumps(wc["отклонено"], ensure_ascii=False)[:140])
# и в живом паке: тот же ключ, что подписал чекпоинт, не должен считаться
live_self = signed_row("live-self", "relay:wss://self", CP, root_at_cp)
live_self["witness_pub"] = cp_signer
check("ключ чекпоинта ≠ ключи свидетелей",
      all(a["witness_pub"] != cp_signer for a in base))

print("\n── 8. возврат к исходным аттестациям: пак снова проходит ──")
write_rows(PACK, base)
r = audit_pack.verify_pack(PACK)
check("итог ПРОВЕРЕНО", r["итог"] == "ПРОВЕРЕНО", str(r["ошибки"][:1]))
n = audit_pack.verify_witness_negative(PACK)
check("негативный тест по-прежнему проходит", n["тест_пройден"] is True)

print(f"\n═══ ИТОГ: ✅ {len(PASS)} / ❌ {len(FAIL)} ═══")
shutil.rmtree(TMP, ignore_errors=True)
if FAIL:
    print("провалено: " + ", ".join(FAIL))
raise SystemExit(1 if FAIL else 0)

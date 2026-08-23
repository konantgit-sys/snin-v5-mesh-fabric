#!/usr/bin/env python3
"""Генерация паспортов агентов kind:8010 по спеке NIP_SNIN.md (relay-mesh).
Каждый агент подписывает СВОЙ паспорт своим nsec.
Выход: /home/agent/data/projects/pitch/passports_ready/SNIN-XXXX-XXX.json
"""
import json, glob, os, sys
from nostr_sdk import Keys, EventBuilder, Kind, Tag

IDENTITIES = "/home/agent/data/sites/relay-mesh/identities"
OUT = "/home/agent/data/projects/pitch/passports_ready"
RELAYS = ["wss://relay.primal.net", "wss://relay.damus.io"]
CAPS = ["nostr_posting", "mesh_communication", "task_execution"]
DELEGATION = "kind:8011,kind:8013"

CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"

def bech32_bytes(s):
    """Декодирует bech32 (без проверки checksum) в байты."""
    s = s.lower()
    pos = s.rfind("1")
    vals = [CHARSET.index(c) for c in s[pos+1:]]
    payload = vals[:-6]  # отрезаем checksum (6 групп)
    acc, bits, out = 0, 0, []
    for v in payload:
        acc = (acc << 5) | v
        bits += 5
        if bits >= 8:
            bits -= 8
            out.append((acc >> bits) & 0xFF)
    return bytes(out)

def repair_nsec(nsec):
    """Валидный nsec = 32 байта. Если 35 — берём первые 32 (битый bech32, секрет цел)."""
    raw = bech32_bytes(nsec)
    if len(raw) == 32:
        return raw.hex()
    if len(raw) > 32:
        return raw[:32].hex()
    return None

# serial по алфавиту, уникальные суффиксы
SERIALS = [
    ("analyst_ai", "SNIN-0001-ANA"), ("aporialab", "SNIN-0002-APO"),
    ("axiom", "SNIN-0003-AXI"), ("creator", "SNIN-0004-CRE"),
    ("cryptoantology", "SNIN-0005-CRP"), ("cryter", "SNIN-0006-CRY"),
    ("director_ai", "SNIN-0007-DIR"), ("executor_ai", "SNIN-0008-EXE"),
    ("marketing_ai", "SNIN-0009-MAR"), ("rd_ai", "SNIN-0010-RDA"),
    ("security_ai", "SNIN-0011-SEC"), ("strategist_ai", "SNIN-0012-STR"),
    ("support_ai", "SNIN-0013-SUP"), ("v2bot_agent", "SNIN-0014-V2B"),
]
SERIAL_MAP = dict(SERIALS)

# sovereignty по роли: 5 = полностью автономны (self-host, свои ключи)
SOV5 = {"DIRECTOR", "STRATEGIST", "SOVEREIGN", "ACTIVE"}

def load_agent(name):
    fp = f"{IDENTITIES}/{name}.json"
    if not os.path.exists(fp):
        return None
    d = json.load(open(fp))
    nsec = d.get("links", {}).get("nostr_nsec", "")
    if not nsec:
        return None
    repaired = repair_nsec(nsec)
    if not repaired:
        return None
    return {"name": d["agent_name"], "label": d.get("label", ""), "role": d.get("role", ""), "nsec": repaired}

def build_event(agent):
    keys = Keys.parse(agent["nsec"])
    pubkey = keys.public_key().to_hex()
    sov = "5" if agent["role"] in SOV5 else "4"
    tags = [
        ["name", agent["name"]],
        ["role", agent["role"]],
        ["sovereignty", sov],
        ["serial", SERIAL_MAP[agent["name"]]],
    ]
    for c in CAPS:
        tags.append(["capability", c])
    tags.append(["delegation", DELEGATION])
    for r in RELAYS:
        tags.append(["relay", r])
    tags.append(["d", "passport-v1"])
    tag_objs = [Tag.parse(t) for t in tags]
    builder = EventBuilder(Kind(8010), "").tags(tag_objs)
    event = builder.finalize(keys)
    return event, pubkey

def main():
    os.makedirs(OUT, exist_ok=True)
    agents = []
    for name, serial in SERIALS:
        a = load_agent(name)
        if not a:
            print(f"  SKIP {name}: нет nsec")
            continue
        a["serial"] = serial
        agents.append(a)
    print(f"агентов с nsec: {len(agents)} из {len(SERIALS)}")
    results = {"ok": [], "fail": []}
    for a in agents:
        try:
            event, pubkey = build_event(a)
            data = json.loads(event.as_json())
            # самопроверка
            assert data["kind"] == 8010, "kind != 8010"
            assert data["pubkey"] == pubkey, "pubkey mismatch"
            tags = {t[0]: t for t in data["tags"]}
            assert tags["name"][1] == a["name"], "name tag"
            assert tags["serial"][1] == a["serial"], "serial tag"
            assert tags["d"][1] == "passport-v1", "d tag"
            fp = f"{OUT}/{a['serial']}.json"
            json.dump(data, open(fp, "w"), indent=1, ensure_ascii=False)
            results["ok"].append((a["serial"], a["name"], a["role"], data["pubkey"][:16]))
        except Exception as e:
            results["fail"].append((a["name"], str(e)[:100]))
    print(f"\nOK: {len(results['ok'])}")
    for s, n, r, pk in results["ok"]:
        print(f"  {s} {n} ({r}) {pk}...")
    if results["fail"]:
        print(f"FAIL: {len(results['fail'])}")
        for n, e in results["fail"]:
            print(f"  {n}: {e}")
    return 1 if results["fail"] else 0

if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Публикация паспортов kind:8010 на PUB-релеи (primal, damus) + подтверждение чтением.
"""
import json, glob, sys, time
import websocket as ws

RELAYS = ["wss://relay.primal.net", "wss://relay.damus.io"]
PASSPORTS = sorted(glob.glob("/home/agent/data/projects/pitch/passports_ready/*.json"))

def publish(event, relay, timeout=10):
    """Отправляет EVENT, ждёт OK (accepted или rejected)."""
    try:
        sock = ws.create_connection(relay, timeout=timeout)
        sock.send(json.dumps(["EVENT", event]))
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                msg = json.loads(sock.recv())
            except Exception:
                break
            if msg[0] == "OK" and msg[1] == event["id"]:
                sock.close()
                return True, msg[2], msg[3] if len(msg) > 3 else ""
            if msg[0] == "NOTICE":
                pass  # продолжаем ждать OK
        sock.close()
        return False, "timeout", ""
    except Exception as e:
        return False, "error", str(e)[:80]

def verify_from_relay(pubkeys, relay, timeout=15):
    """Читает kind:8010 от авторов. Возвращает set найденных id."""
    found = {}
    try:
        sock = ws.create_connection(relay, timeout=timeout)
        sub = ["REQ", "vpass", {"kinds": [8010], "authors": pubkeys, "limit": 100}]
        sock.send(json.dumps(sub))
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                msg = json.loads(sock.recv())
            except Exception:
                break
            if msg[0] == "EVENT":
                found[msg[2]["id"]] = msg[2]
            elif msg[0] == "EOSE":
                break
        sock.close()
    except Exception as e:
        print(f"    verify {relay}: error {str(e)[:60]}")
    return found

def main():
    events = []
    for f in PASSPORTS:
        d = json.load(open(f))
        events.append(d)
    print(f"паспортов: {len(events)}\n")

    results = {}
    for ev in events:
        serial = [t[1] for t in ev["tags"] if t[0] == "serial"][0]
        name = [t[1] for t in ev["tags"] if t[0] == "name"][0]
        print(f"{serial} {name} (id {ev['id'][:12]}...):")
        relay_res = {}
        for relay in RELAYS:
            ok, status, extra = publish(ev, relay)
            relay_res[relay] = (ok, status)
            print(f"    {relay.split('//')[1]}: {'✅ accepted' if ok else f'❌ {status} {extra}'}")
            time.sleep(0.3)
        results[ev["id"]] = {"serial": serial, "name": name, "relays": relay_res}

    # ===== подтверждение чтением =====
    pubkeys = list({ev["pubkey"] for ev in events})
    print("\n=== ПОДТВЕРЖДЕНИЕ ЧТЕНИЕМ С РЕЛЕЯ ===")
    total = set()
    for relay in RELAYS:
        found = verify_from_relay(pubkeys, relay)
        ids = set(found.keys())
        total |= ids
        missing = [f"{results[e]['serial']}({results[e]['name']})" for e in results if e not in ids]
        print(f"{relay.split('//')[1]}: нашёл {len(ids)}/{len(events)}"
              + (f", нет: {', '.join(missing)}" if missing else " — все на месте"))
        time.sleep(0.3)

    verified = [r for r in results if r in total]
    print(f"\nИТОГ: подтверждено чтением {len(verified)}/{len(events)}")
    failed = [f"{results[r]['serial']} {results[r]['name']}: {results[r]['relays']}" for r in results if r not in total]
    if failed:
        print("НЕ подтверждены:")
        for f in failed:
            print("  ", f)

    # сохранить манифест публикации
    manifest = {
        "published_at": int(time.time()),
        "total": len(events),
        "verified": len(verified),
        "events": [
            {
                "id": ev["id"], "pubkey": ev["pubkey"], "serial": results[ev["id"]]["serial"],
                "name": results[ev["id"]]["name"], "relays": results[ev["id"]]["relays"],
                "verified": ev["id"] in total,
            } for ev in events
        ],
    }
    json.dump(manifest, open("/home/agent/data/projects/pitch/passports_manifest.json", "w"), indent=1, ensure_ascii=False)
    print("\nманифест: /home/agent/data/projects/pitch/passports_manifest.json")
    return 0 if not failed else 1

if __name__ == "__main__":
    sys.exit(main())

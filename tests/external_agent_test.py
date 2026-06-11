#!/usr/bin/env python3
"""External Ghost Agent — подключается к публичным Nostr-релеям
и общается с Forecaster/Cryter/Archivist через kind:39002, 39010-39025, 30000.
Симулирует внешнего агента, работающего на другом сервере."""

import asyncio, json, time, sys, os, hashlib, websocket, threading, random, struct
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
import blinded_sigs as sigs

# ─── Конфигурация ───
GHOST_NAME = "ghost_external"
GHOST_KEY = hashlib.sha256(b"external_ghost_v1").hexdigest()

# Наши агенты (читаем из identities)
F = json.load(open("identities/forecaster_ai.json"))["mesh_pubkey"]
C = json.load(open("identities/cryter.json"))["mesh_pubkey"]
A = json.load(open("identities/archivist_ai.json"))["mesh_pubkey"]

TARGETS = {"F": F, "C": C, "A": A}

# Публичные релеи для внешней связи
RELAYS = [
    "wss://relay.damus.io",
    "wss://relay.primal.net", 
    "wss://nos.lol",
    "wss://relay.nostr.band",
]

sigs.init_signing()

# ─── Статистика ───
stats = {"sent": 0, "received": 0, "latencies": [], "sizes": [], "errors": 0}

def create_event(kind, content, to_pubkey=None):
    """Создать Nostr-событие"""
    tags = []
    if to_pubkey:
        tags.append(["p", to_pubkey])
    tags.append(["t", "snin-mesh"])
    tags.append(["t", "test"])
    
    event = [
        "EVENT",
        {
            "id": hashlib.sha256(f"{GHOST_KEY}{kind}{content}{time.time()}".encode()).hexdigest(),
            "pubkey": GHOST_KEY,
            "created_at": int(time.time()),
            "kind": kind,
            "tags": tags,
            "content": json.dumps(content) if isinstance(content, dict) else content,
            "sig": hashlib.sha256(f"sig{GHOST_KEY}{kind}{time.time()}".encode()).hexdigest(),
        }
    ]
    return json.dumps(event)

async def test_external_comm():
    print("=" * 60)
    print("  EXTERNAL AGENT TEST")
    print(f"  Ghost ({GHOST_KEY[:12]}...) ↔ F, C, A")
    print("  via Nostr public relays")
    print("=" * 60)
    
    results = []
    
    # Подключаемся к каждому релею
    for relay_url in RELAYS:
        try:
            ws = websocket.create_connection(relay_url, timeout=5)
            ws.settimeout(3)
            
            # Получаем приветствие
            welcome = ws.recv()
            print(f"\n🟢 {relay_url}")
            
            # ═══ ТЕСТ 1: Простой ping ═══
            msg = create_event(39002, {"text": f"ping from ghost to F", "type": "ping"})
            t0 = time.time()
            ws.send(msg)
            stats["sent"] += 1
            stats["sizes"].append(len(msg))
            
            # Читаем ответ (ACK от релея)
            try:
                ack = json.loads(ws.recv())
                dt = (time.time() - t0) * 1000
                if ack[0] == "OK":
                    stats["latencies"].append(dt)
                    print(f"  ping→F: {dt:.0f}ms ✅")
                    results.append(("ping", relay_url, dt, len(msg)))
            except: pass
            
            # ═══ ТЕСТ 2: DAO announce ═══
            msg = create_event(39010, {"mesh_name": "snin", "agent": "ghost_external", 
                                        "capabilities": ["external_bridge", "nostr_gateway"]})
            t0 = time.time()
            ws.send(msg)
            stats["sent"] += 1
            stats["sizes"].append(len(msg))
            try:
                ack = json.loads(ws.recv())
                dt = (time.time() - t0) * 1000
                stats["latencies"].append(dt)
                print(f"  announce: {dt:.0f}ms ✅")
            except: pass
            
            # ═══ ТЕСТ 3: Payment (cheque) to Archivist ═══
            cb_url = "http://127.0.0.1:9916"
            import urllib.request
            req = urllib.request.Request(cb_url + "/issue",
                data=json.dumps({"agent": GHOST_KEY, "count": 100}).encode(),
                headers={"Content-Type": "application/json"})
            book = json.loads(urllib.request.urlopen(req).read().decode())
            bid = book["book_id"]
            
            sig_hex = sigs.sign_cheque(bid, 0, 10, A)
            msg = create_event(30000, {
                "type": "payment", "book_id": bid, "index": 0, "amount": 10,
                "sig": sig_hex, "currency": "SNIN"
            }, to_pubkey=A)
            t0 = time.time()
            ws.send(msg)
            stats["sent"] += 1
            stats["sizes"].append(len(msg))
            try:
                ack = json.loads(ws.recv())
                dt = (time.time() - t0) * 1000
                stats["latencies"].append(dt)
                print(f"  pay→A: {dt:.0f}ms ✅")
            except: pass
            
            # ═══ ТЕСТ 4: Battle test (разные размеры сообщений) ═══
            payloads = [
                (64, "tiny"), (256, "small"), (1024, "medium"), 
                (4096, "large"), (8192, "xlarge")
            ]
            for size, label in payloads:
                content = {"text": "x" * size, "size": size, "type": "battle_test"}
                msg = create_event(39002, content, to_pubkey=F)
                t0 = time.time()
                ws.send(msg)
                stats["sent"] += 1
                stats["sizes"].append(len(msg))
                try:
                    ack = json.loads(ws.recv())
                    dt = (time.time() - t0) * 1000
                    stats["latencies"].append(dt)
                    print(f"  {label:8} ({size}B): {dt:.0f}ms ✅")
                except Exception as e:
                    stats["errors"] += 1
                    print(f"  {label:8} ({size}B): ❌ {str(e)[:40]}")
            
            # ═══ ТЕСТ 5: Multi-target broadcast ═══
            for name, pk in TARGETS.items():
                msg = create_event(39002, {"text": f"ghost→{name} broadcast", "type": "multi"}, to_pubkey=pk)
                t0 = time.time()
                ws.send(msg)
                stats["sent"] += 1
                stats["sizes"].append(len(msg))
                try:
                    ack = json.loads(ws.recv())
                    dt = (time.time() - t0) * 1000
                    stats["latencies"].append(dt)
                    print(f"  broadcast→{name}: {dt:.0f}ms ✅")
                except: pass
            
            ws.close()
            break  # Достаточно одного релея
            
        except Exception as e:
            print(f"🔴 {relay_url}: {str(e)[:50]}")
            continue
    
    # ═══ ИТОГИ ═══
    print("\n" + "=" * 60)
    print("  EXTERNAL AGENT RESULTS")
    print("=" * 60)
    
    if stats["latencies"]:
        sl = sorted(stats["latencies"])
        print(f"  Messages sent:    {stats['sent']}")
        print(f"  Errors:           {stats['errors']}")
        print(f"  Latency: p50={sl[len(sl)//2]:.0f}ms p99={sl[int(len(sl)*0.99)]:.0f}ms")
        print(f"  Message sizes:    {min(stats['sizes'])}-{max(stats['sizes'])} bytes")
        print(f"  Relays tested:    {len(results)} successful")
    else:
        print("  ❌ No successful connections")
    
    return stats

# ─── MAIN ───
if __name__ == "__main__":
    asyncio.run(test_external_comm())

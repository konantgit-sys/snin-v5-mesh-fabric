#!/usr/bin/env python3
"""Тестовый приёмник — принимает сообщения от SmartRouter для Cryter"""

import asyncio, json, time, sys, os
sys.path.insert(0, os.path.dirname(__file__))

async def main():
    name = sys.argv[1] if len(sys.argv) > 1 else "cryter"
    
    # Загружаем ключи
    if name == "cryter":
        ident = json.load(open("identities/cryter.json"))
        mesh_pubkey = ident["mesh_pubkey"]
    else:
        ident = json.load(open(f"identities/{name}.json"))
        mesh_pubkey = ident.get("mesh_pubkey", ident.get("pubkey"))
    
    print(f"[{name}] Connecting to SmartRouter as {mesh_pubkey[:20]}...")
    
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", 9932), timeout=5
        )
    except Exception as e:
        print(f"[{name}] ❌ Cannot connect: {e}")
        return 1
    
    # Heartbeat
    hb = json.dumps({"from": mesh_pubkey, "kind": 39000, 
                     "meta": {"agent": name}, "payload": {"status": "alive"}}) + "\n"
    writer.write(hb.encode()); await writer.drain()
    
    # Subscribe
    sub = json.dumps({"from": mesh_pubkey, "kind": 39000, 
                      "meta": {"agent": name, "action": "subscribe"},
                      "payload": {"type": "subscribe"}}) + "\n"
    writer.write(sub.encode()); await writer.drain()
    
    # Read subscribe response
    resp = await asyncio.wait_for(reader.readline(), timeout=5)
    resp_data = json.loads(resp.decode())
    print(f"[{name}] Subscribed: {resp_data.get('ok', resp_data.get('subscription_id','?'))}")
    
    received = []
    start = time.time()
    timeout = 60
    
    print(f"[{name}] Listening... (max {timeout}s)")
    
    while time.time() - start < timeout:
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=3)
            data = json.loads(line.decode())
            ts = time.time()
            if data.get("kind") in [39002, 39004] and data.get("payload"):
                channel = data.get("meta", {}).get("channel", "?")
                payload = data.get("payload", {})
                text = str(payload.get("text", ""))[:50]
                received.append({"channel": channel, "text": text, "ts": ts})
                print(f"  📩 [{channel:10}] {text}")
        except asyncio.TimeoutError:
            if time.time() - start > timeout - 15:
                break
            continue
        except Exception as e:
            if "empty" not in str(e).lower():
                print(f"  ⚠️ {e}")
            break
    
    print(f"\n[{name}] Received: {len(received)} messages")
    for ch in ["mesh", "gossip", "nostr", "deadletter"]:
        count = sum(1 for r in received if r["channel"] == ch)
        print(f"  {ch}: {count}")
    
    writer.close()
    return len(received)

if __name__ == "__main__":
    count = asyncio.run(main())
    sys.exit(0 if count > 0 else 1)

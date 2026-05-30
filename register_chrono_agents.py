#!/usr/bin/env python3
"""register_chrono_agents.py — Регистрирует всех агентов Хроноса в relay-mesh при старте."""
import sys, json, sqlite3, requests, os
sys.path.insert(0, '/home/agent/data/sites/chrono')

MESH = "http://localhost:9907"
DB = "/home/agent/data/sites/chrono/chrono.db"

from nostr.key import PrivateKey

db = sqlite3.connect(DB)
rows = db.execute(
    "SELECT agent_id, name, nsec, emoji FROM chrono WHERE nsec IS NOT NULL AND nsec != '' AND status='live'"
).fetchall()
db.close()

ok = 0
for aid, name, nsec, emoji in rows:
    try:
        key = PrivateKey.from_nsec(nsec)
        pubkey = key.public_key.hex()
        r = requests.post(f"{MESH}/api/register", json={
            "pubkey": pubkey,
            "name": name,
            "capabilities": ["chrono", "nostr", "p2p"],
            "meta": {"agent_id": aid, "emoji": emoji or '', "source": "chrono"}
        }, timeout=5)
        if r.ok:
            ok += 1
    except:
        pass

print(f"[register] {ok} Chrono agents registered in relay-mesh")

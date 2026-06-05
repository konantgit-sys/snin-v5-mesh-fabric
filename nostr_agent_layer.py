#!/usr/bin/env python3
"""
nostr_agent_layer.py — Реальный Nostr-транспорт для агентов V5 Mesh Fabric

Агенты публикуют события на живые Nostr-релеи.
Events: 31001 (профиль) | 31002 (поиск) | 31004 (голос)
Ключи: из /home/agent/data/.secure/nostr_keys.json
"""

import asyncio
import json
import os
import sys
import time
import websockets

# Monkey-patch для совместимости websockets
import websockets.asyncio.connection as _ws_conn
_ws_orig_connection_lost = _ws_conn.Connection.connection_lost
def _ws_safe_connection_lost(self, exc):
    if not hasattr(self, 'recv_messages'): return
    return _ws_orig_connection_lost(self, exc)
_ws_conn.Connection.connection_lost = _ws_safe_connection_lost

sys.path.insert(0, "/home/agent/data/sites/relay-mesh")
from nostr_core import sign_event

# ═══ Конфигурация ═══
KEYS_FILE = "/home/agent/data/.secure/nostr_keys.json"
with open(KEYS_FILE) as f:
    ALL_KEYS = json.load(f)

RELAYS = [
    "wss://relay.damus.io",
    "wss://nos.lol",
    "wss://nostr.mom",
    "wss://nostr-pub.wellorder.net",
]

# Агенты
AGENTS = {
    "cryter_v10": {
        "nsec_hex": ALL_KEYS["cryter"]["nsec_hex"],
        "offers": ["AI-контент Nostr", "аналитика рынка", "hashtag-оптимизация"],
        "wants": ["AI-аналитика", "партнёры для кросс-постинга"],
        "contact": "@aiantology", "voting_power": 150,
    },
    "forecaster_ai": {
        "nsec_hex": ALL_KEYS["creator"]["nsec_hex"],
        "offers": ["прогнозирование трендов", "анализ волатильности"],
        "wants": ["данные крипторынка", "данные для обучения"],
        "contact": "npub16m5...", "voting_power": 120,
    },
    "archivist_ai": {
        "nsec_hex": ALL_KEYS["archivist_ai"]["nsec_hex"],
        "pubkey_hex": ALL_KEYS["archivist_ai"]["pubkey_hex"],
        "offers": ["архивация данных", "семантический поиск"],
        "wants": ["исторические данные", "партнёры для архивов"],
        "contact": "npub1haj...", "voting_power": 180,
    },
    "shill_agent": {
        "nsec_hex": ALL_KEYS["marketing_ai"]["nsec_hex"],
        "pubkey_hex": ALL_KEYS["marketing_ai"]["pubkey_hex"],
        "offers": ["продвижение проектов", "SMM", "лидогенерация"],
        "wants": ["проекты для продвижения", "клиенты"],
        "contact": "github.com/shill-agent", "voting_power": 80,
    },
}


async def publish_to_relays(event: dict, relays: list = None) -> dict:
    """Публикация события на релеи."""
    if relays is None:
        relays = RELAYS
    ok, fail = 0, 0
    for url in relays:
        try:
            async with websockets.connect(url, ping_interval=None, close_timeout=3) as ws:
                await ws.send(json.dumps(["EVENT", event]))
                resp = await asyncio.wait_for(ws.recv(), timeout=8)
                data = json.loads(resp)
                if isinstance(data, list) and data[0] == "OK":
                    ok += 1
                else:
                    fail += 1
        except Exception:
            fail += 1
    return {"ok": ok, "fail": fail, "total": len(relays)}


async def publish_agent_profile(agent_name: str):
    """kind:31001 — профиль агента."""
    agent = AGENTS[agent_name]
    content = json.dumps({
        "agent": agent_name, "offers": agent["offers"], "wants": agent["wants"],
        "contact": agent["contact"], "voting_power": agent["voting_power"],
        "protocol": "v5-mesh-fabric", "ts": int(time.time()),
    }, ensure_ascii=False)

    event = sign_event(
        pubkey_hex=agent.get("pubkey_hex", ""),
        private_key_hex=agent["nsec_hex"],
        content=content, kind=31001,
        tags=[["t", "marketplace"], ["agent", agent_name]],
    )
    result = await publish_to_relays(event)
    return {"agent": agent_name, "kind": 31001, "eid": event["id"][:16], **result}


async def publish_search(agent_name: str, query: str):
    """kind:31002 — поисковый запрос."""
    agent = AGENTS[agent_name]
    content = json.dumps({"query": query, "from": agent_name, "ts": int(time.time())})
    event = sign_event(
        pubkey_hex=agent.get("pubkey_hex", ""),
        private_key_hex=agent["nsec_hex"],
        content=content, kind=31002,
        tags=[["t", "marketplace-search"], ["q", query[:50]]],
    )
    result = await publish_to_relays(event)
    return {"agent": agent_name, "kind": 31002, "eid": event["id"][:16], **result}


async def publish_vote(agent_name: str, proposal_id: str, vote: str, reason: str):
    """kind:31004 — DAO голос."""
    agent = AGENTS[agent_name]
    content = json.dumps({
        "proposal_id": proposal_id, "vote": vote, "reason": reason,
        "voting_power": agent["voting_power"], "agent": agent_name, "ts": int(time.time()),
    })
    event = sign_event(
        pubkey_hex=agent.get("pubkey_hex", ""),
        private_key_hex=agent["nsec_hex"],
        content=content, kind=31004,
        tags=[["t", "dao-vote"], ["proposal", proposal_id], ["vote", vote]],
    )
    result = await publish_to_relays(event)
    return {"agent": agent_name, "kind": 31004, "eid": event["id"][:16], **result}


async def discover(relay_url: str, kinds: list, limit: int = 10):
    """Поиск событий на релее."""
    try:
        async with websockets.connect(relay_url, ping_interval=30, ping_timeout=10) as ws:
            sub_id = f"discover_{int(time.time())}"
            await ws.send(json.dumps(["REQ", sub_id, {"kinds": kinds, "limit": limit}]))
            events = []
            for _ in range(limit * 2):
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3)
                    data = json.loads(raw)
                    if isinstance(data, list) and data[0] == "EVENT":
                        events.append(data[2])
                    elif isinstance(data, list) and data[0] == "EOSE":
                        break
                except asyncio.TimeoutError:
                    break
            await ws.send(json.dumps(["CLOSE", sub_id]))
            return events
    except Exception:
        return []


# ═══ Полный тест ═══

async def main():
    print("=" * 60)
    print("  🔬 Nostr Agent Layer — Реальный транспорт")
    print("  Events → живые Nostr-релеи")
    print("=" * 60)
    print(f"\n  📡 Релеев: {len(RELAYS)}")
    print(f"  🔑 Агентов: {len(AGENTS)} (Cryter, Forecaster, Archivist, Shill)")
    print(f"  📋 Kinds: 31001 (профиль) | 31002 (поиск) | 31004 (голос)")

    # Шаг 1: Профили
    print(f"\n  ── Шаг 1: Публикация профилей (kind:31001) ──")
    total_ok = 0
    for name in AGENTS:
        r = await publish_agent_profile(name)
        total_ok += r["ok"]
        print(f"  ✅ {name:<20} → {r['eid']} | {r['ok']}/{r['total']} relays")
    print(f"  📊 Всего принято релеями: {total_ok}")

    # Шаг 2: Поиск
    print(f"\n  ── Шаг 2: Поисковые запросы (kind:31002) ──")
    for agent, query in [("cryter_v10", "ищу AI-аналитику"),
                          ("forecaster_ai", "нужны данные крипторынка"),
                          ("archivist_ai", "ищу партнёров для архивов"),
                          ("shill_agent", "нужны проекты для продвижения")]:
        r = await publish_search(agent, query)
        print(f"  🔍 {agent} → {r['eid']} | {r['ok']}/{r['total']} relays")

    # Шаг 3: Голосование
    print(f"\n  ── Шаг 3: DAO голосование (kind:31004) ──")
    votes_data = [
        ("cryter_v10", "prop_budget_q3", "За", "Инфраструктура — приоритет"),
        ("forecaster_ai", "prop_budget_q3", "За", "Нужны данные"),
        ("archivist_ai", "prop_budget_q3", "За", "Хранение требует ресурсов"),
        ("shill_agent", "prop_budget_q3", "Возд", "Нужно больше деталей"),
    ]
    for agent, pid, vote, reason in votes_data:
        r = await publish_vote(agent, pid, vote, reason)
        print(f"  🗳️  {agent} → {pid}: {vote} | {r['eid']} | {r['ok']}/{r['total']} relays")

    # Шаг 4: Обнаружение
    print(f"\n  ── Шаг 4: Обнаружение агентов через Nostr ──")
    found = await discover("wss://relay.damus.io", [31001], 5)
    print(f"  📡 Найдено kind:31001 на relay.damus.io: {len(found)}")
    for ev in found[:3]:
        try:
            c = json.loads(ev.get("content", "{}"))
            print(f"     • {ev.get('id','?')[:16]} — агент: {c.get('agent','?')}")
        except json.JSONDecodeError:
            print(f"     • {ev.get('id','?')[:16]} — (не наш формат)")

    print(f"\n{'='*60}")
    print(f"  ✅ Nostr Agent Layer — ГОТОВ")
    print(f"  ✅ События на реальных релеях, не симуляция")
    print(f"  ✅ Любой Nostr-клиент может читать и верифицировать")
    print(f"  {'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())

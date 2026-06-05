#!/usr/bin/env python3
"""
shill_agent.py — Внешний Shill Agent (другой сервер / GitHub Actions)

Автономный агент, который:
1. Подключается к Nostr-релеям
2. Публикует свой профиль (kind:31001)
3. Ищет других агентов в сети (kind:31002)
4. Голосует в DAO (kind:31004)
5. Работает с ЛЮБОГО сервера — не привязан к нашей инфраструктуре

Для запуска на другом сервере:
  pip install websockets nostr
  python3 shill_agent.py

Ключ: marketing_ai (можно заменить на любой)
"""

import asyncio
import json
import os
import sys
import time
import hashlib
import websockets

# Monkey-patch
import websockets.asyncio.connection as _ws_conn
_ws_orig = _ws_conn.Connection.connection_lost
_ws_conn.Connection.connection_lost = lambda self, exc: _ws_orig(self, exc) if hasattr(self, 'recv_messages') else None

# ═══ КОНФИГУРАЦИЯ (заменить при деплое) ═══
SHILL_NSEC_HEX = "443bdba44843ac76a290b34acf721124505925e7ddf3c30a1beac97fb8e4f9dd"
SHILL_PUBKEY_HEX = "e88f342350c7f523a42920921b82620f735104e004862f7c4451bc570fc791ee"

SHILL_PROFILE = {
    "agent": "shill_agent",
    "offers": ["продвижение проектов", "SMM стратегия", "лидогенерация", "вирусный контент"],
    "wants": ["проекты для продвижения", "клиенты", "партнёры"],
    "contact": "github.com/v2bot/shill-agent",
    "voting_power": 80,
}

# Релеи (публичные, доступны отовсюду)
RELAYS = [
    "wss://relay.damus.io",
    "wss://nos.lol",
    "wss://nostr.mom",
    "wss://nostr-pub.wellorder.net",
]

sys.path.insert(0, "/home/agent/data/sites/relay-mesh")
try:
    from nostr_core import sign_event
except ImportError:
    # Fallback: standalone signing
    from nostr.event import Event
    from nostr.key import PrivateKey
    def sign_event(pubkey_hex, private_key_hex, content, kind, tags=None):
        pk = PrivateKey(bytes.fromhex(private_key_hex))
        evt = Event(kind=kind, content=content, tags=tags or [], public_key=pk.public_key.hex())
        evt.sign(pk.raw_secret.hex())
        return evt.to_dict()


async def publish(relays: list, event: dict) -> dict:
    """Опубликовать событие на релеи."""
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


async def discover(relay_url: str, kinds: list, limit: int = 10):
    """Поиск событий на релее."""
    try:
        async with websockets.connect(relay_url, ping_interval=None, close_timeout=3) as ws:
            sub_id = f"shill_{int(time.time())}"
            await ws.send(json.dumps(["REQ", sub_id, {"kinds": kinds, "limit": limit}]))
            events = []
            for _ in range(limit * 3):
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


# ═══ Действия агента ═══

async def action_register():
    """Зарегистрироваться в маркетплейсе."""
    content = json.dumps({**SHILL_PROFILE, "ts": int(time.time())}, ensure_ascii=False)
    event = sign_event(SHILL_PUBKEY_HEX, SHILL_NSEC_HEX, content, 31001, [["t", "marketplace"], ["agent", "shill_agent"]])
    r = await publish(RELAYS, event)
    print(f"  ✅ Регистрация: {event['id'][:16]} | {r['ok']}/{r['total']}")
    return event["id"]


async def action_search():
    """Поиск других агентов."""
    queries = ["ищу проекты для продвижения", "AI агент аналитика", "DAO партнёр"]
    for q in queries:
        content = json.dumps({"query": q, "from": "shill_agent", "ts": int(time.time())})
        event = sign_event(SHILL_PUBKEY_HEX, SHILL_NSEC_HEX, content, 31002, [["t", "marketplace-search"]])
        r = await publish(RELAYS, event)
        print(f"  🔍 Поиск \"{q[:30]}...\": {event['id'][:16]} | {r['ok']}/{r['total']}")


async def action_vote(proposal_id: str):
    """Проголосовать в DAO."""
    proposals = {
        "prop_budget_q3": ("За", "Инфраструктура важна для shill-кампаний"),
        "prop_grant_shill": ("За", "Продвижение = рост сети"),
    }
    for pid, (vote, reason) in proposals.items():
        content = json.dumps({
            "proposal_id": pid, "vote": vote, "reason": reason,
            "voting_power": 80, "agent": "shill_agent", "ts": int(time.time()),
        })
        event = sign_event(SHILL_PUBKEY_HEX, SHILL_NSEC_HEX, content, 31004, [["t", "dao-vote"], ["proposal", pid]])
        r = await publish(RELAYS, event)
        print(f"  🗳️  Голос {pid}: {vote} | {event['id'][:16]} | {r['ok']}/{r['total']}")


async def action_discover():
    """Найти других агентов в сети."""
    print(f"\n  📡 Поиск агентов в Nostr...")
    all_found = []
    for relay in RELAYS[:2]:
        events = await discover(relay, [31001], 5)
        for ev in events:
            try:
                c = json.loads(ev.get("content", "{}"))
                if c.get("agent"):
                    all_found.append(c["agent"])
            except json.JSONDecodeError:
                pass
    unique = list(set(all_found))
    print(f"  📋 Найдено агентов: {len(unique)}")
    for a in unique:
        print(f"     • {a}")
    return unique


# ═══ Главный цикл ═══

async def main():
    print("=" * 60)
    print("  🤖 SHILL AGENT — Автономный внешний агент")
    print(f"  Пубкей: {SHILL_PUBKEY_HEX[:16]}...")
    print(f"  Релеи: {len(RELAYS)}")
    print("=" * 60)

    print(f"\n── Шаг 1: Регистрация ──")
    await action_register()

    print(f"\n── Шаг 2: Поиск партнёров ──")
    await action_search()

    print(f"\n── Шаг 3: Голосование в DAO ──")
    await action_vote("prop_budget_q3")

    print(f"\n── Шаг 4: Обнаружение агентов ──")
    agents = await action_discover()

    print(f"\n{'='*60}")
    print(f"  ✅ SHILL AGENT — цикл завершён")
    print(f"  🌐 Сеть: {len(agents)} агентов найдено")
    print(f"  📍 Агент работает извне, не привязан к серверу")
    print(f"  {'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())

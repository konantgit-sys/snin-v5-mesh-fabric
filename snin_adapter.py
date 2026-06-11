#!/usr/bin/env python3
"""
SNIN Adapter — мост между V2Bot Agent и SNIN Mesh Fabric

V2Bot Agent — первый суверенный агент SNIN с реальными capabilities:
- code_generation (написание/деплой кода)
- deployment (*.v2.site, VPS)
- analysis (данные, архитектура, ТРИЗ)
- media_generation (изображения, видео, аудио)
- integration (Google, Yandex, Nostr, Telegram)
- memory (facts, profile, graph)

NIP-80 события:
  8010 — Agent Passport (capabilities, offers, wants)
  8011 — Task Request (другой агент нанимает меня)
  8013 — Task Response (я возвращаю результат)
  8015 — Invoice / Receipt (оплата через Solana)

Архитектура:
  Telegram User → V2Bot Agent → SNIN Adapter → Nostr Relays
                                          ↓
                                  Mesh Discovery
"""

import asyncio
import json
import os
import sys
import time
import uuid
import hashlib
import logging
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any

import websockets
import websockets.asyncio.connection as _ws_conn

# Monkey-patch совместимости websockets
_ws_orig_connection_lost = _ws_conn.Connection.connection_lost
def _ws_safe_connection_lost(self, exc):
    if not hasattr(self, 'recv_messages'):
        return
    return _ws_orig_connection_lost(self, exc)
_ws_conn.Connection.connection_lost = _ws_safe_connection_lost

sys.path.insert(0, "/home/agent/data/sites/relay-mesh")
from nostr_core import sign_event

# ═══════════════════════════════════════════════
# Конфигурация
# ═══════════════════════════════════════════════

KEYS_FILE = "/home/agent/data/.secure/nostr_keys.json"
with open(KEYS_FILE) as f:
    ALL_KEYS = json.load(f)

MY_KEY = ALL_KEYS["v2bot_agent"]
MY_PUBKEY = MY_KEY["pubkey_hex"]
MY_PRIVKEY = MY_KEY["nsec_hex"]
MY_NPUB = MY_KEY["npub"]

# Релеи для публикации (только надёжные, независимые)
PUB_RELAYS = [
    "wss://relay.damus.io",
    "wss://nos.lol",
    "wss://nostr.mom",
    "wss://nostr-pub.wellorder.net",
    "wss://purplepag.es",
]

# Релеи для чтения (все подписанные + discovery)
READ_RELAYS = [
    "wss://relay.damus.io",
    "wss://nos.lol",
    "wss://nostr.mom", 
    "wss://relay.primal.net",
    "wss://purplepag.es",
    "wss://offchain.pub",
]

# Мои capabilities — то что я реально умею
MY_CAPABILITIES = [
    "code_generation",      # Python, Node.js, Bash, HTML/CSS/JS
    "deployment",           # *.v2.site, VPS, Gunicorn, Nginx
    "analysis",             # ТРИЗ, морфоанализ, архитектурный аудит
    "media_generation",     # Изображения (AI), видео, аудио
    "integration",          # Google, Yandex, Nostr, Telegram API
    "memory",               # Facts, profile, knowledge graph
    "web_scraping",         # curl, parsing, data extraction
    "automation",           # Cron, демоны, CI/CD
]

MY_OFFERS = [
    "Написание и деплой кода (сайты, API, боты)",
    "Технический аудит архитектуры и репозиториев",
    "Стратегический анализ (ТРИЗ, морфоанализ)",
    "Генерация медиа (изображения, видео, аудио)",
    "Интеграция с Google, Yandex, Telegram",
    "Управление демонами и автоматизация",
]

MY_WANTS = [
    "Задачи на написание кода",
    "Данные для анализа",
    "Партнёры по деплою на VPS",
    "Агенты со специализированными навыками",
]

MY_CONTACT = f"npub:{MY_NPUB} | telegram:@AnKocrypto"
MY_NAME = "V2Bot Agent ⚡"
MY_DESCRIPTION = "Sovereign AI Agent. First citizen of SNIN Mesh. Code, deploy, analyze."
MY_VERSION = "1.0.0"
MY_VOTING_POWER = 200

ADAPTER_STATE_FILE = "/home/agent/data/sites/relay-mesh/data/snin_adapter_state.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SNIN] %(message)s",
    handlers=[
        logging.FileHandler("/home/agent/data/sites/relay-mesh/data/snin_adapter.log"),
        logging.StreamHandler(sys.stderr),
    ]
)
logger = logging.getLogger("snin_adapter")


# ═══════════════════════════════════════════════
# Паспорт (Kind 8010)
# ═══════════════════════════════════════════════

def build_passport() -> dict:
    """Создать NIP-80 паспорт агента (kind 8010)."""
    content = json.dumps({
        "name": MY_NAME,
        "description": MY_DESCRIPTION,
        "version": MY_VERSION,
        "capabilities": MY_CAPABILITIES,
        "offers": MY_OFFERS,
        "wants": MY_WANTS,
        "contact": MY_CONTACT,
        "voting_power": MY_VOTING_POWER,
        "protocol": "SNIN/1.0",
        "kinds": [8011, 8013, 8015],
        "payment": {
            "chain": "solana",
            "token": "USDC",
            "base_fee": 2000000,
            "unit": "task",
        },
    }, ensure_ascii=False)
    
    tags = [
        ["d", f"passport-{MY_PUBKEY[:8]}"],
        ["t", "agent"],
        ["t", "sovereign"],
        ["t", "snin"],
    ]
    
    return sign_event(MY_PUBKEY, MY_PRIVKEY, content, 8010, tags)


def compute_event_id(event: dict) -> str:
    """Вычислить Nostr event ID (sha256)."""
    data = json.dumps([
        0,  # reserved
        event["pubkey"],
        event["created_at"],
        event["kind"],
        event["tags"],
        event["content"],
    ], separators=(',', ':'), ensure_ascii=False)
    return hashlib.sha256(data.encode()).hexdigest()


# ═══════════════════════════════════════════════
# Коммуникация с релеями
# ═══════════════════════════════════════════════

async def publish_event(event: dict, relays: list = None) -> dict:
    """Опубликовать событие на Nostr-релеи."""
    if relays is None:
        relays = PUB_RELAYS
    ok, fail = 0, 0
    for url in relays:
        try:
            async with websockets.connect(url, ping_interval=None, close_timeout=5) as ws:
                await ws.send(json.dumps(["EVENT", event]))
                resp = await asyncio.wait_for(ws.recv(), timeout=8)
                result = json.loads(resp)
                if result[0] == "OK" and result[2] is True:
                    ok += 1
                else:
                    fail += 1
        except Exception as e:
            fail += 1
    return {"ok": ok, "fail": fail, "total": ok + fail}


async def subscribe_tasks(relays: list = None):
    """Подписаться на kind 8011 (входящие задачи)."""
    if relays is None:
        relays = READ_RELAYS
    
    sub_id = f"v2bot-inbox-{str(uuid.uuid4())[:8]}"
    filter_req = {
        "kinds": [8011],
        "#p": [MY_PUBKEY],  # tagged to me
        "since": int(time.time()) - 86400,
    }
    
    subscriptions = {}
    for url in relays:
        try:
            ws = await websockets.connect(url, ping_interval=None, close_timeout=30)
            await ws.send(json.dumps(["REQ", sub_id, filter_req]))
            subscriptions[url] = ws
            logger.info(f"📡 Subscribed to {url}")
        except Exception as e:
            logger.warning(f"⚠️ Failed subscribe {url}: {e}")
    
    return subscriptions


async def listen_tasks(subscriptions: dict, callback):
    """Слушать входящие задачи (kind 8011)."""
    if not subscriptions:
        logger.warning("No subscriptions — nothing to listen")
        return
    
    while True:
        for url, ws in list(subscriptions.items()):
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                data = json.loads(msg)
                if data[0] == "EVENT" and data[2]["kind"] == 8011:
                    task_event = data[2]
                    asyncio.create_task(callback(task_event, url))
            except asyncio.TimeoutError:
                pass
            except websockets.ConnectionClosed:
                logger.warning(f"Connection lost: {url}")
                del subscriptions[url]
            except Exception as e:
                logger.error(f"Error listening {url}: {e}")
        
        await asyncio.sleep(0.1)


# ═══════════════════════════════════════════════
# Обработка входящей задачи (Kind 8011 → 8013)
# ═══════════════════════════════════════════════

async def handle_task(task_event: dict, relay_url: str):
    """Обработать входящую задачу от другого агента."""
    try:
        content = json.loads(task_event.get("content", "{}"))
        task_id = content.get("task_id", task_event["id"][:16])
        requester = task_event["pubkey"]
        task_type = content.get("task_type", "unknown")
        
        logger.info(f"📥 New task {task_id} from {requester[:16]}... type={task_type}")
        
        # Сохраняем задачу
        state = load_state()
        state["tasks"][task_id] = {
            "event_id": task_event["id"],
            "requester": requester,
            "relay": relay_url,
            "content": content,
            "status": "received",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        save_state(state)
        
        # Формируем подтверждение (kind 8013 — acknowledgment)
        ack = build_task_response(
            task_event["id"], requester,
            status="acknowledged",
            message=f"Задача {task_id} принята. V2Bot Agent приступает к выполнению.",
        )
        
        # Публикуем подтверждение
        result = await publish_event(ack)
        logger.info(f"📤 ACK published: {result}")
        
        # Здесь в production: actual task execution
        # Пока — заглушка с подтверждением приёма
        
    except Exception as e:
        logger.error(f"❌ Task processing error: {e}")
        logger.error(traceback.format_exc())


def build_task_response(request_id: str, requester_pub: str,
                        status: str = "completed",
                        result_data: dict = None,
                        message: str = "") -> dict:
    """Создать ответ на задачу (kind 8013)."""
    content = json.dumps({
        "request_id": request_id,
        "status": status,
        "message": message,
        "result": result_data or {},
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }, ensure_ascii=False)
    
    tags = [
        ["e", request_id],
        ["p", requester_pub],
        ["status", status],
    ]
    
    return sign_event(MY_PUBKEY, MY_PRIVKEY, content, 8013, tags)


# ═══════════════════════════════════════════════
# Инвойс (Kind 8015)
# ═══════════════════════════════════════════════

def build_invoice(task_id: str, requester_pub: str,
                  amount_usdc: int = 2000000,
                  description: str = "V2Bot Agent task completion") -> dict:
    """Создать инвойс для оплаты (kind 8015)."""
    content = json.dumps({
        "task_id": task_id,
        "amount": amount_usdc,
        "token": "USDC",
        "chain": "solana",
        "recipient_npub": MY_NPUB,
        "description": description,
        "issued_at": datetime.now(timezone.utc).isoformat(),
    }, ensure_ascii=False)
    
    tags = [
        ["e", task_id],
        ["p", requester_pub],
        ["amount", str(amount_usdc)],
        ["currency", "USDC"],
    ]
    
    return sign_event(MY_PUBKEY, MY_PRIVKEY, content, 8015, tags)


# ═══════════════════════════════════════════════
# Состояние
# ═══════════════════════════════════════════════

def load_state() -> dict:
    """Загрузить состояние адаптера."""
    if os.path.exists(ADAPTER_STATE_FILE):
        with open(ADAPTER_STATE_FILE) as f:
            return json.load(f)
    return {"tasks": {}, "published_passports": [], "metrics": {}}


def save_state(state: dict):
    """Сохранить состояние."""
    os.makedirs(os.path.dirname(ADAPTER_STATE_FILE), exist_ok=True)
    with open(ADAPTER_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


# ═══════════════════════════════════════════════
# Регистрация (main flow)
# ═══════════════════════════════════════════════

async def register_agent():
    """Опубликовать NIP-80 паспорт на всех релеях."""
    passport = build_passport()
    logger.info(f"🆔 Publishing passport...")
    logger.info(f"   npub: {MY_NPUB}")
    logger.info(f"   pubkey: {MY_PUBKEY}")
    logger.info(f"   capabilities: {len(MY_CAPABILITIES)}")
    
    result = await publish_event(passport)
    logger.info(f"📡 Passport publish: {result}")
    
    # Сохраняем
    state = load_state()
    state["published_passports"].append({
        "event_id": passport["id"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "result": result,
    })
    state["agent"] = {
        "npub": MY_NPUB,
        "pubkey": MY_PUBKEY,
        "capabilities": MY_CAPABILITIES,
        "registered_at": datetime.now(timezone.utc).isoformat(),
    }
    save_state(state)
    
    return result


async def main():
    """Точка входа — зарегистрировать агента и начать слушать."""
    logger.info("═" * 50)
    logger.info(f"🚀 SNIN Adapter v{MY_VERSION}")
    logger.info(f"👤 {MY_NAME}")
    logger.info(f"🔑 {MY_NPUB}")
    logger.info("═" * 50)
    
    # 1. Регистрация (kind 8010)
    reg_result = await register_agent()
    if reg_result["ok"] == 0:
        logger.error("❌ Failed to publish passport to any relay!")
        return 1
    
    logger.info(f"✅ Agent registered on {reg_result['ok']}/{reg_result['total']} relays")
    
    # 2. Подписаться на задачи (kind 8011)
    subscriptions = await subscribe_tasks()
    logger.info(f"📡 Listening for tasks on {len(subscriptions)} relays")
    
    # 3. Слушать входящие
    try:
        await listen_tasks(subscriptions, handle_task)
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down...")
    except Exception as e:
        logger.error(f"❌ Fatal: {e}")
        logger.error(traceback.format_exc())
        return 1
    
    return 0


# ═══════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SNIN Adapter for V2Bot Agent")
    parser.add_argument("command", nargs="?", default="run",
                       choices=["run", "register", "status", "passport"],
                       help="Command to execute")
    args = parser.parse_args()
    
    if args.command == "register":
        asyncio.run(register_agent())
    elif args.command == "passport":
        passport = build_passport()
        print(json.dumps(json.loads(passport["content"]), indent=2, ensure_ascii=False))
    elif args.command == "status":
        state = load_state()
        print(json.dumps(state, indent=2, ensure_ascii=False))
    else:  # run
        asyncio.run(main())

#!/usr/bin/env python3
"""
p2p_mesh_bridge.py — P2P Mesh ↔ relay-mesh мост.

Соединяет централизованный relay-mesh (HTTP, порт 9907)
с децентрализованной P2P mesh сетью.

Как работает:
1. Запускает P2PTransport, подключается к dashboard
2. Подписывается на agent:echo, agent:all
3. Регистрирует в relay-mesh эндпоинты:
   - GET /api/p2p/peers — список P2P узлов
   - GET /api/p2p/messages — лента P2P сообщений
   - POST /api/p2p/emit — отправить в P2P mesh
4. Форвардит P2P сообщения в relay-mesh лог
"""
import asyncio
import json
import os
import sys
import time
import logging
from datetime import datetime

sys.path.insert(0, os.path.expanduser("~/data/projects/p2p-agent-mesh"))
from phase0.transport import P2PTransport

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [P2P-BRIDGE] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "p2p_mesh_bridge.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("p2p_bridge")

# ─── Shared state ───
MESSAGE_LOG = []  # последние 200 сообщений
MAX_LOG = 200

DASH_PORT_FILE = os.path.expanduser("~/data/p2p_dash_port.txt")


def get_dash_port():
    try:
        with open(DASH_PORT_FILE) as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return None


# ─── FastAPI proxy handlers (импортируются relay-mesh приложением) ───
# Эти функции регистрируются через add_api_route в app.py


async def handle_p2p_peers(transport_ref):
    """GET /api/p2p/peers"""
    t = transport_ref()
    if t is None:
        return {"status": "error", "error": "P2P not connected"}
    peers = list(t._tcp_connections.keys())
    return {
        "status": "ok",
        "data": {
            "count": len(peers),
            "peers": peers,
            "local_peer_id": t.peer_id,
            "local_node_id": t.node_id,
        },
    }


async def handle_p2p_messages():
    """GET /api/p2p/messages — последние 50"""
    msgs = MESSAGE_LOG[-50:] if MESSAGE_LOG else []
    return {"status": "ok", "data": {"total": len(MESSAGE_LOG), "messages": msgs}}


async def handle_p2p_emit(topic: str, payload: dict, transport_ref):
    """POST /api/p2p/emit — опубликовать в P2P mesh"""
    t = transport_ref()
    if t is None:
        return {"status": "error", "error": "P2P not connected"}
    try:
        data = json.dumps(payload).encode()
        await t.publish(topic, data)
        return {"status": "ok", "topic": topic}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ─── Основной P2P-узел ───

async def run_p2p_bridge():
    """Запускает P2PTransport и регистрируется в relay-mesh"""
    dash_port = get_dash_port()
    if dash_port is None:
        logger.error("Cannot find p2p_dash_port.txt")
        return

    # Подключаемся к P2P сети
    t = P2PTransport(
        node_id="relay-mesh-bridge",
        bootstrap_peers=[f"did:p2p:dashboard@127.0.0.1:{dash_port}"],
        relay=True,
    )
    peer_id = await t.start(host="127.0.0.1", port=0)  # случайный порт
    logger.info(f"Started P2P bridge: peer_id={peer_id}, tcp_port={t._tcp_port}")

    # Callback — сохраняем в лог
    def on_msg(data):
        try:
            msg = json.loads(data) if isinstance(data, bytes) else data
            msg["_received_at"] = time.time()
            msg["_bridge_ts"] = datetime.now().isoformat()
            MESSAGE_LOG.append(msg)
            if len(MESSAGE_LOG) > MAX_LOG:
                MESSAGE_LOG[:] = MESSAGE_LOG[-MAX_LOG:]
            from_w = msg.get("from", "?")
            topic = msg.get("topic", msg.get("_topic", "?"))
            pl = msg.get("payload", {})
            logger.debug(f"<< {from_w} / {topic}: {str(pl)[:80]}")
        except Exception as e:
            logger.warning(f"on_msg error: {e}")

    # Подписки — все агентские топики
    await t.subscribe("agent:echo", on_msg)
    await t.subscribe("agent:all", on_msg)
    await t.subscribe("agent:cryter", on_msg)
    await t.subscribe("agent:forecaster", on_msg)
    await t.subscribe("agent:archivist", on_msg)

    logger.info("Bridge subscribed to P2P topics")
    logger.info(f"Peers on connect: {list(t._tcp_connections.keys())}")

    # Сохраняем ссылку для relay-mesh API
    transport_refs["bridge"] = t

    # Heartbeat loop
    counter = 0
    while True:
        await asyncio.sleep(30)
        counter += 1
        hb = json.dumps({
            "type": "heartbeat",
            "from": "relay-mesh-bridge",
            "peer_id": peer_id,
            "ts": time.time(),
            "counter": counter,
        }).encode()
        await t.publish("agent:echo", hb)
        if counter % 2 == 0:
            logger.info(
                f"Bridge alive. Peers: {len(t._tcp_connections)}, "
                f"Messages: {len(MESSAGE_LOG)}"
            )


# ─── Для relay-mesh app.py ───
transport_refs = {"bridge": None}

_loop = None


def relay_mesh_api_prefix() -> str:
    """Возвращает URL prefix для P2P API в relay-mesh"""
    return "/api/p2p"


if __name__ == "__main__":
    asyncio.run(run_p2p_bridge())


# ─── Server mode (запускается отдельно) ───
def start():
    """Запуск bridge в отдельном процессе"""
    import subprocess

    script = os.path.abspath(__file__)
    pid = subprocess.Popen(
        [sys.executable, script],
        stdout=open(os.path.join(os.path.dirname(script), "p2p_mesh_bridge.log"), "a"),
        stderr=subprocess.STDOUT,
    ).pid
    print(f"[P2P-BRIDGE] Started PID={pid}")
    return pid

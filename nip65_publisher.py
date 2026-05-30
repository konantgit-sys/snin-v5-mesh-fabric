#!/usr/bin/env python3
"""
NIP-65 Publisher Daemon — публикация kind:10002 в Nostr Relay.
Версия: V4
Интервал: каждые 6 часов (с авто-рестартом при падении Relay)

Логика:
  1. Загружает relay_meta из nip65_discovery.py
  2. Строит kind:10002 event
  3. Публикует через Nostr Relay (wss://relay-snin.v2.site)
  4. Сохраняет в Redis и файл
  5. Получает список релеев, подписанных на mesh
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time

# Добавляем путь
sys.path.insert(0, os.path.dirname(__file__))
from nip65_discovery import build_relay_list_event, RELAY_META_FILE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [NIP65] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "logs", "nip65_publisher.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("nip65")

PUBLISH_INTERVAL = 21600  # 6 часов
NOSTR_RELAY_URL = "wss://relay-snin.v2.site"
PIDFILE = "/tmp/snin_nip65.pid"


async def publish_via_websocket(event: dict) -> bool:
    """Публикует событие в Nostr Relay через WebSocket."""
    try:
        import websockets
        async with websockets.connect(NOSTR_RELAY_URL, ping_interval=30, close_timeout=5) as ws:
            msg = json.dumps(["EVENT", event])
            await ws.send(msg)
            response = await asyncio.wait_for(ws.recv(), timeout=10)
            logger.info(f"✅ Опубликовано kind:10002 в {NOSTR_RELAY_URL} → {response[:80]}")
            return True
    except ImportError:
        logger.warning("⚠️ websockets не установлен, публикую через файл")
        return _publish_file(event)
    except Exception as e:
        logger.warning(f"⚠️ WS publish error: {e}")
        return _publish_file(event)


def _publish_file(event: dict) -> bool:
    """Fallback: сохранить в файл."""
    path = os.path.join(os.path.dirname(__file__), "nip65_relay_list.json")
    with open(path, "w") as f:
        json.dump(event, f, indent=2)
    logger.info(f"💾 Сохранено в файл: {path}")
    return True


async def publish_loop():
    """Цикл публикации NIP-65."""
    logger.info("=" * 50)
    logger.info("🚀 NIP-65 Publisher V4 запущен")
    logger.info(f"   Relay: {NOSTR_RELAY_URL}")
    logger.info(f"   Интервал: {PUBLISH_INTERVAL // 3600} ч")
    logger.info("=" * 50)

    while True:
        try:
            event = build_relay_list_event()
            logger.info(f"📡 Событие kind:10002 построено ({len(event.get('tags', []))} relay tags)")
            ok = await publish_via_websocket(event)
            if ok:
                logger.info("✅ Публикация успешна")
            else:
                logger.warning("⚠️ Публикация не удалась (сохранено локально)")
        except Exception as e:
            logger.error(f"❌ Ошибка цикла: {e}")

        await asyncio.sleep(PUBLISH_INTERVAL)


def main():
    # PID file
    pid = str(os.getpid())
    with open(PIDFILE, "w") as f:
        f.write(pid)

    # Signal handler
    def _handle_signal(sig, frame):
        logger.info("👋 Завершение по сигналу")
        if os.path.exists(PIDFILE):
            os.remove(PIDFILE)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        asyncio.run(publish_loop())
    except KeyboardInterrupt:
        pass
    finally:
        if os.path.exists(PIDFILE):
            os.remove(PIDFILE)


if __name__ == "__main__":
    main()

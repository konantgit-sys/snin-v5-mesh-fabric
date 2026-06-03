#!/usr/bin/env python3
"""
L13: WebSocket Broadcaster for Health Monitor
Транслирует live-статусы сервисов всем подключённым клиентам.
На коннект — отправляет полное текущее состояние.
"""

import asyncio
import json
import logging
import time
from typing import Callable, Dict, Set

from aiohttp import web, WSMsgType

logger = logging.getLogger("HealthWS")


class HealthWSBroadcaster:
    """
    WebSocket broadcast hub для live health-статусов.
    Не управляет WS-сервером — endpoint регистрируется в aiohttp.
    """

    def __init__(self):
        self.clients: Set[asyncio.Task] = set()
        self._current_state: Dict = {}
        self._state_getter: Callable = lambda: {}

    def set_state_getter(self, fn: Callable):
        """Устанавливает функцию, возвращающую полное состояние всех сервисов."""
        self._state_getter = fn

    async def ws_handler(self, request):
        """aiohttp WebSocket handler — регистрируется в app.router."""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.clients.add(ws)
        logger.info(f"WS client connected ({len(self.clients)} total)")

        try:
            # Отправляем текущее полное состояние при коннекте
            full_state = self._state_getter()
            await ws.send_json({"type": "snapshot", "data": full_state})

            # Цикл heartbeat + приёма сообщений
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    if msg.data == "ping":
                        await ws.send_json({"type": "pong"})
                elif msg.type == WSMsgType.ERROR:
                    logger.warning(f"WS error: {ws.exception()}")

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"WS client error: {e}")
        finally:
            self.clients.discard(ws)
            logger.info(f"WS client disconnected ({len(self.clients)} left)")

        return ws

    async def broadcast(self, message: dict):
        """Отправляет сообщение всем подключённым клиентам."""
        if not self.clients:
            return
        dead = set()
        for ws in self.clients.copy():
            try:
                await ws.send_json(message)
            except (ConnectionResetError, ConnectionError, Exception):
                dead.add(ws)
        for ws in dead:
            self.clients.discard(ws)

    async def broadcast_status_change(self, service: str, old: Dict, new: Dict):
        """Транслирует изменение статуса сервиса."""
        await self.broadcast({
            "type": "status_change",
            "service": service,
            "from": old,
            "to": new,
            "timestamp": int(time.time())
        })


# Глобальный экземпляр
_broadcaster = None


def get_broadcaster() -> HealthWSBroadcaster:
    global _broadcaster
    if _broadcaster is None:
        _broadcaster = HealthWSBroadcaster()
    return _broadcaster

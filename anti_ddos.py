#!/usr/bin/env python3
"""
anti_ddos.py — Thin wrapper вокруг middleware.RateLimiter (Phase 4).

Предоставляет обратную совместимость с существующими импортами.
Вся логика перенесена в middleware.py.
"""

import asyncio
import json
import logging
import os
import time

from middleware import get_pipeline

logger = logging.getLogger("anti_ddos")

# ─── Экспортируем для обратной совместимости ───
MAX_EVENT_SIZE = 65536
RATE_LIMIT_WINDOW = 60
MAX_REQUESTS_PER_IP = 100
MAX_REQUESTS_PER_PUBKEY = 50
BLACKLIST_TTL = 300
BLACKLIST_THRESHOLD = 10

STATUS_FILE = "/home/agent/data/sites/snin-hub/antiddos_status.json"


class AntiDDoS:
    """Thin wrapper — делегирует всё в middleware.RateLimiter."""

    def __init__(self):
        self._ratelimit = get_pipeline().ratelimit
        self._cb_manager = get_pipeline().cb
        self._started_at = time.time()

    async def check_event(self, ip: str, pubkey: str, content: str,
                          signature: str = None) -> tuple[bool, str]:
        ok, reason = await self._ratelimit.check(
            ip=ip, pubkey=pubkey, content=content,
            signature=signature or "",
            is_authenticated=bool(pubkey),
        )
        return ok, reason

    def get_stats(self) -> dict:
        rl_stats = self._ratelimit.get_stats()
        cb_stats = self._cb_manager.status()
        rl_stats["circuit_breaker"] = {
            "channels": cb_stats["channels"],
            "degraded": self._cb_manager.degraded_channels(),
        }
        return rl_stats

    def save_status(self):
        try:
            os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)
            with open(STATUS_FILE, "w") as f:
                json.dump(self.get_stats(), f, indent=2, default=str)
        except Exception:
            pass

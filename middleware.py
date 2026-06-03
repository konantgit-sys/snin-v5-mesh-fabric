"""
middleware.py — Unified request pipeline для SNIN Mesh Fabric (Phase 4).

Централизует cross-cutting:
  - RateLimiter     (per-IP, per-pubkey, per-session, blacklist)
  - CircuitBreaker  (4 канала: direct, mesh, nostr, gossip)
  - RequestPipeline (композиция middleware)

Usage:
    pipeline = RequestPipeline()
    ok, reason, meta = await pipeline.process(ip, pubkey, content)
    if not ok:
        reject(reason)
"""

import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from enum import Enum
from typing import Dict, Optional, Tuple

logger = logging.getLogger("middleware")

# ═══════════════════════════════════════════════════════════════
#  RateLimiter — вдохновлено anti_ddos.py + nip42_auth.py
# ═══════════════════════════════════════════════════════════════

class RateLimiter:
    """
    Многоуровневый rate limiter:
      - per-IP:     100 req / 60s window
      - per-pubkey:  50 req / 60s window
      - per-session:  10 req/s (anon) / 100 req/s (auth)
      - Blacklist: после 10 нарушений → 300s блокировка
      - Max event size: 64KB
      - Signature gate: reject без подписи
    """

    MAX_EVENT_SIZE = 65536
    RATE_WINDOW = 60
    MAX_PER_IP = 100
    MAX_PER_PUBKEY = 50
    SESSION_MAX_ANON = 10
    SESSION_MAX_AUTH = 100
    BLACKLIST_TTL = 300
    BLACKLIST_THRESHOLD = 10

    def __init__(self):
        self._ip_counter: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.MAX_PER_IP * 2))
        self._pubkey_counter: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.MAX_PER_PUBKEY * 2))
        self._session_counter: Dict[str, list] = defaultdict(list)
        self._blacklist: Dict[str, float] = {}
        self._violations: Dict[str, int] = defaultdict(int)
        self._stats = {
            "rejected_size": 0, "rejected_rate_ip": 0, "rejected_rate_pubkey": 0,
            "rejected_rate_session": 0, "rejected_blacklist": 0, "rejected_no_sig": 0,
            "accepted": 0, "total": 0, "blacklist_size": 0,
        }
        self._started_at = time.time()

    async def check(self, ip: str, pubkey: str = "", content: str = "",
                    signature: str = "", is_authenticated: bool = False,
                    session_key: str = "") -> Tuple[bool, str]:
        """
        Проверить запрос. Returns (ok: bool, reason: str).
        """
        self._stats["total"] += 1

        # 1. Blacklist
        for key in (ip, pubkey):
            if key and key in self._blacklist:
                if time.time() < self._blacklist[key]:
                    self._stats["rejected_blacklist"] += 1
                    return False, "blacklisted"
                del self._blacklist[key]

        # 2. Max size
        if content:
            size = len(content.encode("utf-8"))
            if size > self.MAX_EVENT_SIZE:
                self._stats["rejected_size"] += 1
                self._ban(ip, pubkey)
                return False, f"event too large: {size} > {self.MAX_EVENT_SIZE}"

        # 4. Rate per-IP
        now = time.time()
        if ip:
            self._ip_counter[ip].append(now)
            while self._ip_counter[ip] and self._ip_counter[ip][0] < now - self.RATE_WINDOW:
                self._ip_counter[ip].popleft()
            if len(self._ip_counter[ip]) > self.MAX_PER_IP:
                self._stats["rejected_rate_ip"] += 1
                self._ban(ip, pubkey)
                return False, f"rate limit (IP): {len(self._ip_counter[ip])}/{self.MAX_PER_IP}"

        # 5. Rate per-pubkey
        if pubkey:
            self._pubkey_counter[pubkey].append(now)
            while self._pubkey_counter[pubkey] and self._pubkey_counter[pubkey][0] < now - self.RATE_WINDOW:
                self._pubkey_counter[pubkey].popleft()
            if len(self._pubkey_counter[pubkey]) > self.MAX_PER_PUBKEY:
                self._stats["rejected_rate_pubkey"] += 1
                self._ban(pubkey)
                return False, f"rate limit (pubkey): {len(self._pubkey_counter[pubkey])}/{self.MAX_PER_PUBKEY}"

        # 6. Rate per-session (1s sliding window, из nip42_auth)
        if session_key:
            ts_list = self._session_counter[session_key]
            ts_list[:] = [t for t in ts_list if now - t < 1.0]
            max_s = self.SESSION_MAX_AUTH if is_authenticated else self.SESSION_MAX_ANON
            if len(ts_list) >= max_s:
                self._stats["rejected_rate_session"] += 1
                return False, f"rate limit (session): {len(ts_list)}/{max_s} per sec"
            ts_list.append(now)

        self._stats["accepted"] += 1
        return True, "ok"

    def _ban(self, *keys: str):
        for key in keys:
            if key:
                self._violations[key] += 1
                if self._violations[key] >= self.BLACKLIST_THRESHOLD:
                    self._blacklist[key] = time.time() + self.BLACKLIST_TTL
        self._stats["blacklist_size"] = len(self._blacklist)

    def get_stats(self) -> dict:
        return {
            "uptime_sec": int(time.time() - self._started_at),
            "total": self._stats["total"],
            "accepted": self._stats["accepted"],
            "rejected": {
                "size": self._stats["rejected_size"],
                "rate_ip": self._stats["rejected_rate_ip"],
                "rate_pubkey": self._stats["rejected_rate_pubkey"],
                "rate_session": self._stats["rejected_rate_session"],
                "blacklist": self._stats["rejected_blacklist"],
                "no_signature": self._stats["rejected_no_sig"],
            },
            "blacklist_size": self._stats["blacklist_size"],
        }

# ═══════════════════════════════════════════════════════════════
#  CircuitBreakerCheck — unified CB (4 канала)
#  Заменяет: circuit_breaker.py + first_contact.py CB functions
# ═══════════════════════════════════════════════════════════════

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class ChannelCB:
    """Circuit breaker для одного канала: direct, mesh, nostr, gossip."""

    def __init__(self, name: str, threshold: int = 5, cooldown: float = 30.0):
        self.name = name
        self.state = CircuitState.CLOSED
        self.errors: list[float] = []
        self.threshold = threshold
        self.cooldown = cooldown
        self.last_open = 0.0
        self.total_opens = 0
        self.total_closes = 0

    def record_success(self):
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.CLOSED
            self.total_closes += 1
            logger.info(f"[CB:{self.name}] → CLOSED (restored)")
        elif self.state == CircuitState.OPEN:
            logger.info(f"[CB:{self.name}] still OPEN — success ignored (use can_proceed first)")
        self.errors = []

    def record_error(self, now: float = None):
        now = now or time.time()
        self.errors.append(now)
        # Clean old (>cooldown*2)
        cutoff = now - self.cooldown * 2
        self.errors = [t for t in self.errors if t > cutoff]

        if len(self.errors) >= self.threshold:
            self.state = CircuitState.OPEN
            self.last_open = now
            self.total_opens += 1
            logger.warning(f"[CB:{self.name}] → OPEN ({len(self.errors)} errors)")

    def can_proceed(self, now: float = None) -> bool:
        now = now or time.time()
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if now - self.last_open >= self.cooldown:
                self.state = CircuitState.HALF_OPEN
                logger.info(f"[CB:{self.name}] → HALF_OPEN (test)")
                return True
            return False
        if self.state == CircuitState.HALF_OPEN:
            return True
        return True

    def reset(self):
        self.state = CircuitState.CLOSED
        self.errors = []
        self.last_open = 0

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "state": self.state.value,
            "errors_in_window": len(self.errors),
            "threshold": self.threshold,
            "cooldown_remaining": max(0.0, self.cooldown - (time.time() - self.last_open)) if self.state == CircuitState.OPEN else 0.0,
            "total_opens": self.total_opens,
            "total_closes": self.total_closes,
        }


class CircuitBreakerManager:
    """
    Управляет всеми каналами: direct, mesh, nostr, gossip.
    Читается из mesh_config.yaml (если есть), иначе defaults.
    """

    DEFAULT_CHANNELS = {
        "direct": {"threshold": 3, "cooldown": 30},
        "mesh": {"threshold": 5, "cooldown": 30},
        "nostr": {"threshold": 3, "cooldown": 60},
        "gossip": {"threshold": 5, "cooldown": 30},
    }

    STATUS_FILE = "/home/agent/data/logs/circuit_breaker_status.json"

    def __init__(self, channels: dict = None):
        ch_map = channels or self.DEFAULT_CHANNELS
        self.channels: dict[str, ChannelCB] = {
            name: ChannelCB(name, **cfg) for name, cfg in ch_map.items()
        }
        self._stats = {"total_checks": 0, "total_blocks": 0, "total_allows": 0}
        self._started_at = time.time()

    def check(self, channel: str) -> Tuple[bool, str]:
        """
        Проверить канал на readiness.
        Returns (allowed: bool, state_name: str).
        """
        cb = self.channels.get(channel)
        if not cb:
            return True, "unknown_channel"
        self._stats["total_checks"] += 1
        if cb.can_proceed():
            self._stats["total_allows"] += 1
            return True, cb.state.value
        self._stats["total_blocks"] += 1
        return False, cb.state.value

    def record_error(self, channel: str):
        cb = self.channels.get(channel)
        if cb:
            cb.record_error()

    def record_success(self, channel: str):
        cb = self.channels.get(channel)
        if cb:
            cb.record_success()

    def reset(self, channel: str = None):
        if channel:
            cb = self.channels.get(channel)
            if cb:
                cb.reset()
        else:
            for cb in self.channels.values():
                cb.reset()

    def status(self) -> dict:
        now = time.time()
        return {
            "uptime_sec": int(now - self._started_at),
            "channels": {name: cb.to_dict() for name, cb in self.channels.items()},
            "total_checks": self._stats["total_checks"],
            "total_blocks": self._stats["total_blocks"],
            "total_allows": self._stats["total_allows"],
        }

    def save_status(self):
        try:
            os.makedirs(os.path.dirname(self.STATUS_FILE), exist_ok=True)
            with open(self.STATUS_FILE, "w") as f:
                json.dump(self.status(), f, indent=2)
        except Exception:
            pass

    def degraded_channels(self) -> list[str]:
        """Вернуть список каналов в OPEN состоянии."""
        return [
            name for name, cb in self.channels.items()
            if cb.state == CircuitState.OPEN
        ]

# ═══════════════════════════════════════════════════════════════
#  RequestPipeline — композиция всех middleware
# ═══════════════════════════════════════════════════════════════

class RequestPipeline:
    """
    Последовательный pipeline: RateLimiter → CircuitBreaker → Auth.
    
    Usage:
        pipeline = RequestPipeline()
        ok, reason, meta = await pipeline.process(
            ip="1.2.3.4", pubkey="abc...", content="...",
            channel="mesh", is_authenticated=False
        )
        if not ok:
            reject(reason)
    """

    def __init__(self, rate_limiter: RateLimiter = None, circuit_breaker: CircuitBreakerManager = None):
        self.ratelimit = rate_limiter or RateLimiter()
        self.cb = circuit_breaker or CircuitBreakerManager()

    async def process(self, *, ip: str = "", pubkey: str = "", content: str = "",
                      signature: str = "", channel: str = "mesh",
                      is_authenticated: bool = False,
                      session_key: str = "") -> Tuple[bool, str, dict]:
        """
        Прогнать запрос через весь pipeline.
        
        Returns:
            (True, "ok", {"channel_state": ...}) — всё хорошо
            (False, "reason", {}) — блокировано
        """
        meta = {}

        # 1. Rate Limiter
        ok, reason = await self.ratelimit.check(
            ip=ip, pubkey=pubkey, content=content,
            signature=signature, is_authenticated=is_authenticated,
            session_key=session_key or ip or pubkey,
        )
        if not ok:
            return False, f"ratelimit:{reason}", meta

        # 2. Circuit Breaker
        allowed, state = self.cb.check(channel)
        meta["channel_state"] = state
        if not allowed:
            return False, f"cb:{channel}:{state}", meta

        meta["cb"] = state
        return True, "ok", meta

    def get_stats(self) -> dict:
        return {
            "rate_limiter": self.ratelimit.get_stats(),
            "circuit_breaker": self.cb.status(),
        }

    def run_sync(self, *, ip: str = "", pubkey: str = "", content: str = "",
                  signature: str = "", channel: str = "mesh",
                  is_authenticated: bool = False,
                  session_key: str = "") -> Tuple[bool, str, dict]:
        """Sync обёртка для Flask-роутов (только из синхронного контекста!)."""
        return asyncio.run(self.process(
            ip=ip, pubkey=pubkey, content=content,
            signature=signature, channel=channel,
            is_authenticated=is_authenticated,
            session_key=session_key,
        ))


# ─── Singleton для быстрого импорта ───
_default_pipeline = None

def get_pipeline() -> RequestPipeline:
    """Получить/создать pipeline singleton."""
    global _default_pipeline
    if _default_pipeline is None:
        _default_pipeline = RequestPipeline()
    return _default_pipeline

def check_rate_limit(ip: str, pubkey: str = "", content: str = "",
                     signature: str = "", session_key: str = "") -> Tuple[bool, str]:
    """Rate limit check (sync, для app.py). Возвращает (ok, reason)."""
    import asyncio
    pipeline = get_pipeline()
    return asyncio.run(pipeline.ratelimit.check(
        ip=ip, pubkey=pubkey, content=content,
        signature=signature, session_key=session_key,
    ))


def check_rate_limit_simple(key: str, max_per_sec: int = 10) -> bool:
    """Rate limit per-key per-second (совместимость с nip42_auth API).
    
    Args:
        key: unique key (IP, session, ws_id)
        max_per_sec: max requests per second
    Returns:
        True если можно пропустить, False если превышен лимит
    """
    import asyncio
    now = time.time()
    ts_list = get_pipeline().ratelimit._session_counter[key]
    ts_list[:] = [t for t in ts_list if now - t < 1.0]
    if len(ts_list) >= max_per_sec:
        return False
    ts_list.append(now)
    return True

def cb_check(channel: str) -> Tuple[bool, str]:
    """Quick shortcut — только circuit breaker (для app.py)."""
    return get_pipeline().cb.check(channel)

def cb_record_error(channel: str):
    get_pipeline().cb.record_error(channel)

def cb_record_success(channel: str):
    get_pipeline().cb.record_success(channel)

def cb_reset(channel: str = None):
    get_pipeline().cb.reset(channel)

def cb_status() -> dict:
    return get_pipeline().cb.status()

def cb_degraded_channels() -> list[str]:
    return get_pipeline().cb.degraded_channels()

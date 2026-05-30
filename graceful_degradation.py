#!/usr/bin/env python3
"""
SNIN V4 Graceful Degradation Module
Версия: 4.0
Дата: 2026-05-23

Обеспечивает выживание системы при падении любого компонента:
- Redis fallback → in-memory DHT кэш
- Nostr Bridge health monitor → исключение мёртвых из ротации
- External Gateway probe → fallback Nostr канала
- Supervisor sync → единый отчёт о degradation

Интеграция: Smart Router (self._deg = GracefulDegradation)
"""

import asyncio
import json
import logging
import os
import socket
import time
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger("graceful_degradation")

# ─── Конфигурация ───
CHECK_INTERVAL = 15  # сек — проверка всех компонентов
BRIDGE_TIMEOUT = 3   # сек — таймаут проверки bridge
GATEWAY_TIMEOUT = 3  # сек — таймаут проверки gateway
REDIS_TIMEOUT = 2    # сек — таймаут ping Redis

# Список Nostr Bridges (порты)
NOSTR_BRIDGE_PORTS = [9941, 9942, 9943, 9944, 9945]

# Порт External Gateway
EXTERNAL_GATEWAY_PORT = 9931

# Порт Redis
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379

# Статус файл (читается supervisor)
DEGRADATION_STATUS_FILE = "/home/agent/data/sites/snin-hub/degradation_status.json"


def _port_open(host: str = "127.0.0.1", port: int = 0, timeout: float = 2) -> bool:
    """Проверка открыт ли порт (TCP connect)."""
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


class GracefulDegradation:
    """
    Монитор и восстановление graceful degradation.
    Используется Smart Router'ом для fallback при падениях.
    """

    def __init__(self):
        # ─── In-memory DHT кэш (дублёр Redis) ───
        self._dht_cache: Dict[str, dict] = {}      # pubkey → agent_data
        self._dht_last_sync: float = 0
        self._dht_redis_available: bool = False
        
        # ─── Nostr Bridges health ───
        self._bridge_health: Dict[int, dict] = {}
        for port in NOSTR_BRIDGE_PORTS:
            self._bridge_health[port] = {
                "alive": False,
                "last_check": 0,
                "fails": 0,
                "state": "unknown",  # unknown / alive / dead
            }
        
        # ─── External Gateway ───
        self._gateway_alive: bool = False
        self._gateway_fails: int = 0
        
        # ─── Redis ───
        self._redis_alive: bool = False
        self._redis_fails: int = 0
        
        # ─── Global state ───
        self._state: str = "normal"  # normal / degraded / critical
        self._last_state_change: float = time.time()
        self._degraded_since: float = 0
        self._check_count: int = 0
        self._running: bool = False
        
        # ─── Locks ───
        self._lock = asyncio.Lock()
    
    # ═══════════════════════════════════════════
    # Public API — вызывается Smart Router'ом
    # ═══════════════════════════════════════════
    
    async def check_all(self, redis_client=None) -> dict:
        """Проверить все компоненты. Вызывается раз в 15 сек."""
        async with self._lock:
            self._check_count += 1
            
            # 1. Redis
            self._redis_alive = await self._check_redis(redis_client)
            
            # 2. Nostr Bridges
            for port in NOSTR_BRIDGE_PORTS:
                alive = _port_open(port=port, timeout=BRIDGE_TIMEOUT)
                bh = self._bridge_health[port]
                if alive:
                    bh["alive"] = True
                    bh["fails"] = 0
                    bh["state"] = "alive"
                else:
                    bh["fails"] += 1
                    if bh["fails"] >= 2:
                        bh["state"] = "dead"
                    bh["alive"] = False
                bh["last_check"] = time.time()
            
            # 3. External Gateway
            self._gateway_alive = _port_open(port=EXTERNAL_GATEWAY_PORT, timeout=GATEWAY_TIMEOUT)
            if not self._gateway_alive:
                self._gateway_fails += 1
            else:
                self._gateway_fails = 0
            
            # 4. Global state
            old_state = self._state
            self._update_global_state()
            
            if old_state != self._state:
                self._last_state_change = time.time()
                logger.info(
                    f"[Degradation] State: {old_state} → {self._state}"
                )
            
            # 5. Save status
            status = self.get_status()
            self._save_status(status)
            
            return status
    
    async def sync_dht_from_redis(self, redis_client) -> int:
        """
        Синхронизировать in-memory DHT кэш из Redis.
        Вызывается после каждого check, если Redis доступен.
        Возвращает кол-во агентов в кэше.
        """
        if not redis_client:
            self._dht_redis_available = False
            return len(self._dht_cache)
        
        try:
            all_agents = await redis_client.hgetall("dht:agents")
            for pk_hex, raw in all_agents.items():
                try:
                    data = json.loads(raw) if isinstance(raw, str) else raw
                    self._dht_cache[pk_hex] = data
                except (json.JSONDecodeError, TypeError):
                    self._dht_cache[pk_hex] = {"raw": str(raw)[:64]}
            
            self._dht_redis_available = True
            self._dht_last_sync = time.time()
            logger.debug(f"[DHT Cache] synced {len(self._dht_cache)} agents from Redis")
        except Exception as e:
            logger.warning(f"[DHT Cache] sync error: {e}")
            self._dht_redis_available = False
        
        return len(self._dht_cache)
    
    def get_dht_agent(self, pubkey: str) -> Optional[dict]:
        """Получить данные агента из кэша (Redis или in-memory)."""
        return self._dht_cache.get(pubkey) or self._dht_cache.get(pubkey[:16])
    
    def get_live_bridges(self) -> List[int]:
        """Список живых Nostr Bridges (для ротации)."""
        return [
            port for port, bh in self._bridge_health.items()
            if bh["state"] == "alive"
        ]
    
    def get_alive_bridge_count(self) -> int:
        """Количество живых Nostr Bridges."""
        return len(self.get_live_bridges())
    
    def is_gateway_alive(self) -> bool:
        """External Gateway жив?"""
        return self._gateway_alive
    
    def is_redis_alive(self) -> bool:
        """Redis жив?"""
        return self._redis_alive
    
    def get_state(self) -> str:
        """Текущий глобальный статус (normal/degraded/critical)."""
        return self._state
    
    def get_status(self) -> dict:
        """Полный отчёт о degradation."""
        return {
            "state": self._state,
            "redis": {
                "alive": self._redis_alive,
                "fails": self._redis_fails,
                "dht_cache": len(self._dht_cache),
                "dht_redis_available": self._dht_redis_available,
            },
            "nostr_bridges": {
                "total": len(NOSTR_BRIDGE_PORTS),
                "alive": self.get_alive_bridge_count(),
                "dead": len(NOSTR_BRIDGE_PORTS) - self.get_alive_bridge_count(),
                "health": {
                    str(p): bh["state"]
                    for p, bh in self._bridge_health.items()
                },
            },
            "gateway": {
                "alive": self._gateway_alive,
                "fails": self._gateway_fails,
            },
            "degraded_since": self._degraded_since,
            "checks": self._check_count,
            "timestamp": time.time(),
        }
    
    # ═══════════════════════════════════════════
    # Internal
    # ═══════════════════════════════════════════
    
    async def _check_redis(self, redis_client=None) -> bool:
        """Ping Redis."""
        if redis_client is None:
            return False
        try:
            await asyncio.wait_for(redis_client.ping(), timeout=REDIS_TIMEOUT)
            self._redis_fails = 0
            return True
        except Exception:
            self._redis_fails += 1
            return False
    
    def _update_global_state(self):
        """Обновить глобальный статус на основе состояния компонентов."""
        bridge_dead = NOSTR_BRIDGE_PORTS - self.get_alive_bridge_count()  # type: ignore
        
        if (not self._redis_alive and not self._gateway_alive 
            and self.get_alive_bridge_count() == 0):
            # Redis + Gateway + все Bridge → critical
            new_state = "critical"
        elif (not self._redis_alive or not self._gateway_alive 
              or self.get_alive_bridge_count() < 3):
            # Хотя бы один компонент упал → degraded
            new_state = "degraded"
        else:
            new_state = "normal"
        
        if new_state != "normal" and self._degraded_since == 0:
            self._degraded_since = time.time()
        
        self._state = new_state
    
    def _save_status(self, status: dict):
        """Сохранить статус в файл (читается supervisor)."""
        try:
            os.makedirs(os.path.dirname(DEGRADATION_STATUS_FILE), exist_ok=True)
            with open(DEGRADATION_STATUS_FILE, "w") as f:
                json.dump(status, f, indent=2, default=str)
        except Exception:
            pass

#!/usr/bin/env python3
"""
SNIN Circuit Breaker Module v2.3

Фаза 2.3: Изоляция cascading failures между каналами.

Логика:
  - 4 канала: direct (TCP), mesh, nostr, gossip
  - Каждый канал имеет свой circuit breaker (3 состояния: CLOSED, OPEN, HALF_OPEN)
  - CLOSED: нормальное состояние, запросы проходят
  - OPEN: канал упал 3+ раза за 30 сек, запросы отклоняются с fallback
  - HALF_OPEN: через 30 сек пытаемся восстановить (test request)

Все 4 канала работают параллельно → если 1 упал, остальные 3 работают.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple

LOG_DIR = "/home/agent/data/logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [CIRCUIT] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "circuit_breaker.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("CircuitBreaker")

CB_STATUS_FILE = "/home/agent/data/sites/relay-mesh/circuit_breaker_status.json"

class CircuitState(Enum):
    CLOSED = "closed"      # Нормально, запросы проходят
    OPEN = "open"          # Упал, запросы отклоняются
    HALF_OPEN = "half_open"  # Пытаемся восстановить

class Channel:
    """Один канал (TCP, mesh, nostr, gossip)"""
    def __init__(self, name: str, port: Optional[int] = None):
        self.name = name
        self.port = port
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.failure_timestamp = None
        self.half_open_attempt_time = None
        self.request_count = 0
        self.success_count = 0
        self.last_error = ""

        # Пороги и таймауты
        self.failure_threshold = 3  # откровать после 3 ошибок
        self.failure_window = 30  # за 30 сек
        self.half_open_timeout = 30  # 30 сек перед попыткой восстановления

    def record_success(self):
        """Успешный запрос"""
        self.request_count += 1
        self.success_count += 1

        if self.state == CircuitState.HALF_OPEN:
            # Успешно восстановился
            logger.info(f"✅ {self.name} recovered (HALF_OPEN → CLOSED)")
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.failure_timestamp = None
            self.last_error = ""

    def record_failure(self, error: str = ""):
        """Неудачный запрос"""
        self.request_count += 1
        self.last_error = error
        self.failure_count += 1

        now = time.time()

        # Сбросить счётчик если окно прошло
        if self.failure_timestamp and (now - self.failure_timestamp) > self.failure_window:
            self.failure_count = 1
            self.failure_timestamp = now
        elif not self.failure_timestamp:
            self.failure_timestamp = now

        logger.warning(f"⚠️ {self.name} failure #{self.failure_count} ({error})")

        if self.failure_count >= self.failure_threshold and self.state == CircuitState.CLOSED:
            # Открываем circuit
            logger.error(f"🔴 {self.name} CIRCUIT OPEN ({self.failure_count} failures in {self.failure_window}s)")
            self.state = CircuitState.OPEN
            self.half_open_attempt_time = now

    def can_process_request(self) -> Tuple[bool, Optional[str]]:
        """Можно ли отправить запрос через этот канал?"""
        if self.state == CircuitState.CLOSED:
            return True, None

        if self.state == CircuitState.OPEN:
            now = time.time()
            if (now - self.half_open_attempt_time) >= self.half_open_timeout:
                # Пробуем восстановиться
                logger.info(f"🟡 {self.name} attempting recovery (OPEN → HALF_OPEN)")
                self.state = CircuitState.HALF_OPEN
                self.failure_count = 0  # Сбросить счётчик для HALF_OPEN
                return True, None  # Пускаем один тестовый запрос
            else:
                # Канал ещё открыт, используем fallback
                remaining = self.half_open_timeout - (now - self.half_open_attempt_time)
                return False, f"{self.name} is OPEN (recover in {remaining:.0f}s)"

        if self.state == CircuitState.HALF_OPEN:
            # В HALF_OPEN пускаем запросы, но с осторожностью
            return True, None

        return False, f"Unknown state: {self.state}"

    def to_dict(self):
        return {
            "name": self.name,
            "state": self.state.value,
            "failures": self.failure_count,
            "requests": self.request_count,
            "success_rate": round(self.success_count / max(1, self.request_count) * 100, 1),
            "last_error": self.last_error
        }

class CircuitBreakerManager:
    """Управляет всеми 4 каналами"""
    def __init__(self):
        self.channels: Dict[str, Channel] = {}
        self.setup_channels()
        self.fallback_routes = {
            "direct": ["mesh", "nostr", "gossip"],
            "mesh": ["nostr", "gossip", "direct"],
            "nostr": ["gossip", "mesh", "direct"],
            "gossip": ["direct", "mesh", "nostr"],
        }

    def setup_channels(self):
        """Инициализирует 4 канала"""
        channels_config = [
            ("direct", 8080),     # TCP Gateway
            ("mesh", 9932),       # Smart Router (mesh network)
            ("nostr", 9941),      # Nostr Bridge (primary)
            ("gossip", 9100),     # Gossip Server
        ]
        for name, port in channels_config:
            self.channels[name] = Channel(name, port)

    def request(self, channel_name: str, callback) -> Tuple[bool, Optional[str]]:
        """
        Пытается отправить запрос через канал с circuit breaker.

        Args:
            channel_name: "direct", "mesh", "nostr", "gossip"
            callback: async function для выполнения запроса

        Returns:
            (success, error_msg)
        """
        if channel_name not in self.channels:
            return False, f"Unknown channel: {channel_name}"

        channel = self.channels[channel_name]
        can_process, error = channel.can_process_request()

        if not can_process:
            logger.info(f"Falling back from {channel_name}: {error}")
            return False, error

        try:
            result = callback()  # Пытаемся выполнить запрос
            channel.record_success()
            return True, None
        except Exception as e:
            error_msg = str(e)
            channel.record_failure(error_msg)

            # Если канал открыт, пытаемся fallback
            if channel.state == CircuitState.OPEN:
                return False, f"Circuit open for {channel_name}: {error_msg}"

            return False, error_msg

    def get_healthy_channels(self) -> List[str]:
        """Возвращает список здоровых каналов"""
        return [name for name, ch in self.channels.items() if ch.state == CircuitState.CLOSED]

    def get_status(self):
        """Возвращает статус всех circuit breaker'ов"""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "channels": {name: ch.to_dict() for name, ch in self.channels.items()},
            "healthy": self.get_healthy_channels(),
            "fallback_routes": self.fallback_routes
        }

    def save_status(self):
        """Сохраняет статус в JSON"""
        with open(CB_STATUS_FILE, 'w') as f:
            json.dump(self.get_status(), f, indent=2)

    def monitor_loop(self):
        """Периодическое сохранение статуса"""
        logger.info("🚀 Circuit Breaker Manager started")

        while True:
            try:
                self.save_status()
                time.sleep(10)
            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
                time.sleep(10)

# Глобальный инстанс
_manager = None

def get_circuit_breaker() -> CircuitBreakerManager:
    global _manager
    if _manager is None:
        _manager = CircuitBreakerManager()
    return _manager

if __name__ == "__main__":
    manager = get_circuit_breaker()
    manager.monitor_loop()

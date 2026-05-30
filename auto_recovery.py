#!/usr/bin/env python3
"""
SNIN Auto-Recovery Module v2.2

Фаза 2.2: Автоматический рестарт упавших сервисов с graceful degradation.

Логика:
  1. Health Engine детектирует 3+ consecutive fails
  2. Auto-Recovery изолирует сервис (включает graceful degradation)
  3. Запускает graceful_shutdown и restart через start.sh
  4. Логирует recovery attempt
"""

import json
import logging
import os
import subprocess
import time
from datetime import datetime
from typing import Dict, Optional

LOG_DIR = "/home/agent/data/logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [RECOVERY] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "auto_recovery.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("AutoRecovery")

HEALTH_STATUS_FILE = "/home/agent/data/sites/relay-mesh/health_status.json"
RECOVERY_LOG_FILE = os.path.join(LOG_DIR, "recovery_log.json")

# Конфиг сервисов с путями для рестарта
SERVICE_CONFIG = {
    "smart_router": {"port": 9932, "start_script": "/home/agent/data/sites/relay-mesh/start.sh"},
    "route_engine": {"port": 9910, "start_script": "/home/agent/data/sites/relay-mesh/start.sh"},
    "content_router": {"port": 9920, "start_script": "/home/agent/data/sites/relay-mesh/start.sh"},
    "nostr_bridge_0": {"port": 9941, "start_script": "/home/agent/data/sites/relay-mesh/start.sh"},
    "nostr_bridge_1": {"port": 9942, "start_script": "/home/agent/data/sites/relay-mesh/start.sh"},
    "nostr_bridge_2": {"port": 9943, "start_script": "/home/agent/data/sites/relay-mesh/start.sh"},
    "nostr_bridge_3": {"port": 9944, "start_script": "/home/agent/data/sites/relay-mesh/start.sh"},
    "external_gateway": {"port": 9931, "start_script": "/home/agent/data/sites/relay-mesh/start.sh"},
    "identity_api": {"port": 9940, "start_script": "/home/agent/data/sites/identity-api/start.sh"},
    "cheque_book": {"port": 9916, "start_script": "/home/agent/data/sites/relay-mesh/start.sh"},
    "verifier": {"port": 9915, "start_script": "/home/agent/data/sites/relay-mesh/start.sh"},
}

class RecoveryManager:
    def __init__(self):
        self.recovery_history = []
        self.load_recovery_log()

    def load_recovery_log(self):
        """Загружает историю recovery attempts"""
        if os.path.exists(RECOVERY_LOG_FILE):
            try:
                with open(RECOVERY_LOG_FILE, 'r') as f:
                    self.recovery_history = json.load(f)
            except:
                self.recovery_history = []

    def save_recovery_log(self):
        """Сохраняет историю recovery"""
        with open(RECOVERY_LOG_FILE, 'w') as f:
            json.dump(self.recovery_history[-100:], f, indent=2)  # Последние 100 попыток

    def trigger_graceful_degradation(self, service_name: str):
        """Изолирует упавший сервис"""
        logger.warning(f"Triggering graceful degradation for {service_name}")
        # TODO: отправить сигнал другим компонентам об изоляции
        # Например, через signal к smart_router чтобы он перераспределил нагрузку

    def restart_service(self, service_name: str) -> bool:
        """Перезапускает сервис через start.sh"""
        if service_name not in SERVICE_CONFIG:
            logger.error(f"Unknown service: {service_name}")
            return False

        config = SERVICE_CONFIG[service_name]
        start_script = config["start_script"]
        port = config["port"]

        if not os.path.exists(start_script):
            logger.error(f"Start script not found: {start_script}")
            return False

        logger.info(f"🔄 Restarting {service_name}:{port}")

        try:
            # Graceful shutdown (если есть)
            self._graceful_shutdown(service_name, port)

            # Убиваем старый процесс
            os.system(f"lsof -ti :{port} | xargs kill -9 2>/dev/null || true")
            time.sleep(0.5)

            # Запускаем через start.sh
            result = subprocess.run(
                [start_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=10
            )

            if result.returncode == 0:
                logger.info(f"✅ {service_name} restarted successfully")
                self._log_recovery("success", service_name, port)
                return True
            else:
                logger.error(f"❌ {service_name} restart failed: {result.stderr.decode()}")
                self._log_recovery("failed", service_name, port, result.stderr.decode())
                return False
        except subprocess.TimeoutExpired:
            logger.error(f"❌ {service_name} restart timeout")
            self._log_recovery("timeout", service_name, port)
            return False
        except Exception as e:
            logger.error(f"❌ {service_name} restart error: {e}")
            self._log_recovery("error", service_name, port, str(e))
            return False

    def _graceful_shutdown(self, service_name: str, port: int):
        """Пытается graceful shutdown перед kill -9"""
        try:
            os.system(f"curl -s http://127.0.0.1:{port}/shutdown 2>/dev/null || true")
            time.sleep(1)
        except:
            pass

    def _log_recovery(self, status: str, service: str, port: int, error: str = ""):
        """Логирует попытку recovery"""
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "service": service,
            "port": port,
            "status": status,
            "error": error
        }
        self.recovery_history.append(record)
        self.save_recovery_log()
        logger.info(f"Logged recovery: {service} → {status}")

    def check_and_recover(self):
        """Проверяет health_status.json и восстанавливает упавшие сервисы"""
        try:
            with open(HEALTH_STATUS_FILE, 'r') as f:
                health_data = json.load(f)
        except:
            logger.warning("Could not read health status")
            return

        services = health_data.get("services", {})
        for service_name, status in services.items():
            consecutive_fails = status.get("consecutive_fails", 0)

            if consecutive_fails >= 3:
                logger.warning(f"Service {service_name} has {consecutive_fails} consecutive fails")

                # Изолируем сервис
                self._trigger_graceful_degradation(service_name)

                # Перезапускаем
                self.restart_service(service_name)

    def get_recovery_status(self):
        """Возвращает статус recovery (для REST API)"""
        return {
            "total_attempts": len(self.recovery_history),
            "successful": len([r for r in self.recovery_history if r["status"] == "success"]),
            "failed": len([r for r in self.recovery_history if r["status"] == "failed"]),
            "recent": self.recovery_history[-10:]
        }

def run_recovery_monitor():
    """Главный loop: каждые 10 сек проверяет и восстанавливает"""
    manager = RecoveryManager()
    logger.info("🚀 Auto-Recovery Monitor started")

    while True:
        try:
            manager.check_and_recover()
            time.sleep(10)  # Проверяем каждые 10 сек
        except Exception as e:
            logger.error(f"Recovery monitor error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    run_recovery_monitor()

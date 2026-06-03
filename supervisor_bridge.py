#!/usr/bin/env python3
"""
L15: Supervisor Bridge — HTTP API к supervisor.py для restart/status/logs.

Архитектура:
  supervisor_pid.json — файл с PID и портом supervisor'а
  supervisor.py — имеет HTTP API на {host}:{port}
  supervisor_bridge — клиент к этому API

Если supervisor не отвечает → fallback: kill + systemd restart
"""

import json
import logging
import os
import subprocess
import time
from typing import Dict, List, Optional

import aiohttp

logger = logging.getLogger("SupervisorBridge")

# ─── Пути ───
SUPERVISOR_PID_FILE = "/home/agent/data/sites/relay-mesh/supervisor_pid.json"
SUPERVISOR_DIR = "/home/agent/data/sites/relay-mesh/"
SUPERVISOR_SCRIPT = os.path.join(SUPERVISOR_DIR, "supervisor.py")

# ─── Зависимости (что нужно рестартить при рестарте сервиса) ───
SERVICE_DEPENDENCIES = {
    "nostr_bridge_0": [],
    "nostr_bridge_1": [],
    "nostr_bridge_2": [],
    "smart_router": ["nostr_bridge_0", "nostr_bridge_1", "nostr_bridge_2"],
    "route_engine": ["smart_router"],
    "supervisor": [],
}

# ─── Кеш релеев (очищать при restart_clear_cache) ───
CACHE_PATHS = {
    "nostr_bridge": "/home/agent/data/sites/relay-mesh/logs/relay_cache/",
}


class SupervisorBridge:
    """API клиент к supervisor.py."""

    def __init__(self):
        self._host = "127.0.0.1"
        self._port = 0
        self._loaded = False

    def _load(self):
        """Загружает адрес supervisor из PID файла."""
        if self._loaded:
            return
        self._loaded = True
        try:
            if os.path.exists(SUPERVISOR_PID_FILE):
                with open(SUPERVISOR_PID_FILE) as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self._port = data.get("supervisor_port", self._discover_port())
                    self._host = data.get("supervisor_host", "127.0.0.1")
                elif isinstance(data, int):
                    self._port = data
                logger.info(f"Supervisor bridge: {self._host}:{self._port}")
        except Exception as e:
            logger.warning(f"Supervisor PID load failed: {e}")
            self._port = self._discover_port()

    def _discover_port(self) -> int:
        """Fallback: найти supervisor по портам."""
        import socket
        for port in [8001, 8002, 8003]:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect(("127.0.0.1", port))
                s.close()
                return port
            except:
                s.close()
        return 0

    async def _api(self, endpoint: str, method: str = "GET", data: dict = None,
                   timeout: float = 5) -> Optional[dict]:
        """HTTP-запрос к supervisor API."""
        self._load()
        if not self._port:
            logger.warning("Supervisor port not found")
            return None

        url = f"http://{self._host}:{self._port}{endpoint}"
        try:
            async with aiohttp.ClientSession() as session:
                kwargs = {"timeout": aiohttp.ClientTimeout(total=timeout)}
                if data:
                    kwargs["json"] = data
                async with (session.post(url, **kwargs) if method == "POST"
                            else session.get(url, **kwargs)) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    logger.warning(f"Supervisor API error: {resp.status} {endpoint}")
                    return None
        except Exception as e:
            logger.warning(f"Supervisor API failed: {e}")
            return None

    async def restart(self, service_name: str) -> bool:
        """Рестарт сервиса через supervisor."""
        result = await self._api(f"/restart/{service_name}", method="POST")
        if result and result.get("ok"):
            logger.info(f"✅ supervisor restart {service_name}")
            return True
        # Fallback: kill + nohup
        return await self._fallback_restart(service_name)

    async def status(self, service_name: str) -> Optional[dict]:
        """Статус сервиса от supervisor."""
        return await self._api(f"/status/{service_name}")

    async def logs(self, service_name: str, lines: int = 50) -> Optional[List[str]]:
        """Последние N строк лога сервиса."""
        result = await self._api(f"/logs/{service_name}?lines={lines}")
        if result and isinstance(result, dict):
            return result.get("logs", [])
        return None

    async def _fallback_restart(self, service_name: str) -> bool:
        """Fallback: kill + nohup рестарт."""
        logger.info(f"⚠️ supervisor_api failed, fallback restart {service_name}")

        # Найти PID
        try:
            result = subprocess.run(
                ["pgrep", "-f", f"python3.*{service_name}"],
                capture_output=True, text=True, timeout=5
            )
            if result.stdout.strip():
                pids = result.stdout.strip().split()
                for pid in pids:
                    subprocess.run(["kill", pid], timeout=3)
                    logger.info(f"  Killed PID {pid} ({service_name})")

            time.sleep(2)

            nohup_cmd = f"cd {SUPERVISOR_DIR} && nohup python3 -u supervisor.py --service {service_name} > logs/{service_name}.log 2>&1 &"
            subprocess.run(nohup_cmd, shell=True, timeout=5)
            logger.info(f"  Fallback restart {service_name} done")
            return True
        except Exception as e:
            logger.error(f"  Fallback restart failed: {e}")
            return False

    @staticmethod
    def clear_cache(service_type: str) -> bool:
        """Очистка кеша для типа сервиса."""
        cache_path = CACHE_PATHS.get(service_type, "")
        if not cache_path or not os.path.exists(cache_path):
            logger.warning(f"Cache path not found for {service_type}: {cache_path}")
            return False
        try:
            for f in os.listdir(cache_path):
                fp = os.path.join(cache_path, f)
                if os.path.isfile(fp):
                    os.remove(fp)
            logger.info(f"✅ Cache cleared: {cache_path}")
            return True
        except Exception as e:
            logger.error(f"Cache clear failed: {e}")
            return False

    @staticmethod
    def get_dependencies(service_name: str) -> List[str]:
        """Возвращает зависимости сервиса."""
        return SERVICE_DEPENDENCIES.get(service_name, [])


# ─── Singleton ───
_bridge: Optional[SupervisorBridge] = None


def get_supervisor_bridge() -> SupervisorBridge:
    global _bridge
    if _bridge is None:
        _bridge = SupervisorBridge()
    return _bridge

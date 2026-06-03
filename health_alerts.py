#!/usr/bin/env python3
"""
L13: Health Alerts — Telegram + Nostr уведомления о падении сервисов.

Триггеры:
  - Сервис dead > CONSECUTIVE_THRESHOLD раз подряд → Telegram
  - 3+ сервиса dead одновременно → Nostr DM (критический)
  - RAM > 80% → Telegram (warning)

Конфигурация через переменные окружения:
  ALERT_TG_BOT_TOKEN — токен Telegram бота
  ALERT_TG_CHAT_ID — chat_id для алертов
  NOSTR_PRIVATE_KEY — ключ для публикации kind:9001
"""

import asyncio
import json
import logging
import os
import time
from typing import Dict, List, Optional

import aiohttp

logger = logging.getLogger("HealthAlerts")

# ─── Конфиг из окружения ───
TG_BOT_TOKEN = os.environ.get("ALERT_TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("ALERT_TG_CHAT_ID", "")
NOSTR_PRIVATE_KEY = os.environ.get("NOSTR_PRIVATE_KEY", "")

CONSECUTIVE_THRESHOLD = int(os.environ.get("ALERT_CONSECUTIVE_FAILS", 6))
CRITICAL_DEAD_COUNT = int(os.environ.get("ALERT_CRITICAL_DEAD", 3))
ALERT_COOLDOWN = int(os.environ.get("ALERT_COOLDOWN", 300))  # 5 мин между повторными

# Путь к конфигу релеев для Nostr-алертов
NOSTR_RELAYS = [
    "wss://relay.damus.io",
    "wss://nos.lol",
    "wss://relay.primal.net",
]


class HealthAlertEngine:
    """
    Отслеживает статусы сервисов и отправляет алерты.
    Триггер — вызов check() на каждом цикле health engine.
    """

    def __init__(self):
        self._previous_status: Dict[str, bool] = {}
        self._consecutive_dead: Dict[str, int] = {}
        self._last_alert_time: Dict[str, float] = {}
        self._last_critical_alert = 0.0
        self.enabled_tg = bool(TG_BOT_TOKEN and TG_CHAT_ID)
        self.enabled_nostr = bool(NOSTR_PRIVATE_KEY)
        if self.enabled_tg:
            logger.info(f"📱 Telegram alerts enabled (chat={TG_CHAT_ID})")
        else:
            logger.info("📱 Telegram alerts: not configured (set ALERT_TG_BOT_TOKEN)")
        if self.enabled_nostr:
            logger.info("🌐 Nostr alerts enabled (kind:9001)")
        else:
            logger.info("🌐 Nostr alerts: not configured (set NOSTR_PRIVATE_KEY)")

    async def check(self, statuses: Dict[str, Dict]):
        """
        Проверяет все сервисы на изменения статуса.
        Вызывается после каждого цикла health check.
        statuses: {name: ServiceStatus.to_dict()}
        """
        now = time.time()
        dead_services = []
        degraded_services = []

        for name, st in statuses.items():
            was_alive = self._previous_status.get(name, True)
            is_alive = st.get("is_alive", False)

            # Счётчик последовательных падений
            if not is_alive:
                self._consecutive_dead[name] = self._consecutive_dead.get(name, 0) + 1
            else:
                self._consecutive_dead[name] = 0

            # Изменение статуса alive→dead
            if was_alive and not is_alive:
                consec = self._consecutive_dead[name]
                await self._notify_dead(name, st, consec, now)

            # Изменение статуса dead→alive
            if not was_alive and is_alive:
                consec = self._consecutive_dead.get(name, 0)
                logger.info(f"🔁 {name} recovered (was dead for {consec} checks)")
                await self._notify_recovered(name, st, now)

            # RAM warning (если сервис отдаёт metrics.ram_pct)
            if is_alive and st.get("degraded", False):
                degraded_services.append(name)

            if not is_alive:
                dead_services.append(name)

            self._previous_status[name] = is_alive

        # Критический алерт: 3+ dead одновременно
        if len(dead_services) >= CRITICAL_DEAD_COUNT:
            if now - self._last_critical_alert > ALERT_COOLDOWN:
                await self._notify_critical(dead_services, now)
                self._last_critical_alert = now

    async def _notify_dead(self, name: str, status: Dict, consec: int, now: float):
        """Сервис упал — Telegram (если превышен порог)."""
        if consec < CONSECUTIVE_THRESHOLD:
            return

        cooldown_key = f"dead:{name}"
        if now - self._last_alert_time.get(cooldown_key, 0) < ALERT_COOLDOWN:
            return
        self._last_alert_time[cooldown_key] = now

        msg = (
            f"🚨 *Сервис упал*\n"
            f"• {name}\n"
            f"• Ошибка: {status.get('error', 'N/A')}\n"
            f"• Подряд падений: {consec}\n"
            f"• Время: {time.strftime('%H:%M:%S', time.localtime(now))}"
        )

        logger.warning(f"🔴 ALERT: {name} dead ({consec}x) — {status.get('error', 'N/A')}")

        tasks = []
        if self.enabled_tg:
            tasks.append(self._send_tg(msg))
        if self.enabled_nostr:
            tasks.append(self._send_nostr_alert(name, "dead", status))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _notify_recovered(self, name: str, status: Dict, now: float):
        """Сервис восстановился — Telegram."""
        msg = (
            f"✅ *Сервис восстановился*\n"
            f"• {name}\n"
            f"• Время: {time.strftime('%H:%M:%S', time.localtime(now))}"
        )
        if self.enabled_tg:
            await self._send_tg(msg)

    async def _notify_critical(self, dead_services: List[str], now: float):
        """3+ сервиса dead — критический алерт во все каналы."""
        names = "\n".join(f"• {s}" for s in dead_services)
        msg = (
            f"🔴🔴 *КРИТИЧЕСКИЙ АЛЕРТ* 🔴🔴\n"
            f"{len(dead_services)} сервисов упало:\n{names}\n"
            f"Время: {time.strftime('%H:%M:%S', time.localtime(now))}"
        )
        logger.critical(f"🔴🔴 CRITICAL: {len(dead_services)} services dead: {dead_services}")

        tasks = []
        if self.enabled_tg:
            tasks.append(self._send_tg(msg))
        if self.enabled_nostr:
            tasks.append(self._send_nostr_alert("critical", "dead", {
                "services": dead_services,
                "count": len(dead_services)
            }))
        # Даже если не включено — пытаемся локально залогировать
        logger.info(f"CRITICAL ALERT would send: {msg}")
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_tg(self, text: str):
        """Отправка сообщения в Telegram."""
        if not self.enabled_tg:
            return
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TG_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error(f"TG alert failed ({resp.status}): {body[:200]}")
        except Exception as e:
            logger.error(f"TG alert error: {e}")

    async def _send_nostr_alert(self, name: str, status: str, details: Dict):
        """Публикация kind:9001 (alert event) в Nostr."""
        if not self.enabled_nostr:
            return

        event = {
            "kind": 9001,
            "created_at": int(time.time()),
            "tags": [
                ["alert", name, status],
                ["t", "health"],
                ["priority", "high" if status == "dead" else "info"],
            ],
            "content": json.dumps(details, ensure_ascii=False),
        }

        # Публикация на всех релеях
        results = []
        for relay_url in NOSTR_RELAYS:
            try:
                result = await self._publish_to_relay(relay_url, event)
                results.append(result)
            except Exception as e:
                logger.warning(f"Nostr alert publish to {relay_url}: {e}")

        ok = sum(1 for r in results if r)
        logger.info(f"Nostr alert kind:9001 published to {ok}/{len(NOSTR_RELAYS)} relays")

    async def _publish_to_relay(self, relay_url: str, event: dict) -> bool:
        """Публикует event на один relay."""
        try:
            import websockets
            async with websockets.connect(relay_url, timeout=5) as ws:
                msg = json.dumps(["EVENT", event])
                await ws.send(msg)
                response = await asyncio.wait_for(ws.recv(), timeout=5)
                resp = json.loads(response)
                if isinstance(resp, list) and len(resp) >= 2 and resp[0] == "OK":
                    return True
                return False
        except Exception:
            return False


# Глобальный экземпляр
_alert_engine: Optional[HealthAlertEngine] = None


def get_alert_engine() -> HealthAlertEngine:
    global _alert_engine
    if _alert_engine is None:
        _alert_engine = HealthAlertEngine()
    return _alert_engine

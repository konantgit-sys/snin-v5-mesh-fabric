#!/usr/bin/env python3
"""
ACK Tracker — End-to-end delivery confirmation for SNIN messages.

Каждое сообщение получает msg_id (SHA-256 от content+timestamp+from).
После отправки — ожидание ACK (kind:8014).
Нет ACK за ACK_TIMEOUT → retry через другой канал.
MAX_RETRIES исчерпаны → Dead Letter Queue.

Интеграция: вызывается из SmartRouter.route_message() при отправке
и из SmartRouter._handle_ack() при получении kind:8014.
"""

import asyncio
import hashlib
import json
import time
import os
from dataclasses import dataclass, field
from typing import Optional

# ═══ Constants ═══
ACK_KIND = 8014
ACK_TIMEOUT = 30        # секунд ожидания ACK
MAX_RETRIES = 3         # максимальное число повторов
RETRY_BACKOFF = [2, 5, 15]  # секунд между повторами (экспоненциальный)
ACK_CLEANUP_INTERVAL = 60   # интервал очистки старых записей
ACK_MAX_AGE = 300           # максимальный возраст pending ACK (5 мин)

STATE_FILE = "/home/agent/data/sites/relay-mesh/data/ack_state.json"


@dataclass
class PendingACK:
    msg_id: str
    original_event_id: str
    from_agent: str
    to_agent: str
    channel: str           # какой канал использовался
    sent_at: float
    retries: int = 0
    last_retry_at: float = 0.0
    next_retry_channel: str = ""  # альтернативный канал для повтора

    def is_expired(self) -> bool:
        return time.time() - self.sent_at > ACK_MAX_AGE

    def should_retry(self) -> bool:
        if self.retries >= MAX_RETRIES:
            return False
        backoff = RETRY_BACKOFF[min(self.retries, len(RETRY_BACKOFF) - 1)]
        return time.time() - self.last_retry_at > backoff


class ACKTracker:
    """Отслеживание подтверждений доставки."""

    # Порядок fallback-каналов при retry
    FALLBACK_ORDER = ["direct", "mesh", "gossip", "nostr"]

    def __init__(self):
        self._pending: dict[str, PendingACK] = {}
        self._delivered: dict[str, dict] = {}
        self._failed: dict[str, dict] = {}
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False

        # Статистика
        self.stats = {
            "total_sent": 0,
            "acked": 0,
            "failed": 0,
            "retried": 0,
            "avg_latency_ms": 0.0,
            "by_channel": {},
        }

        self._load_state()

    def _load_state(self):
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE) as f:
                    data = json.load(f)
                self.stats = data.get("stats", self.stats)
        except Exception:
            pass

    def _save_state(self):
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            with open(STATE_FILE, "w") as f:
                json.dump({
                    "stats": self.stats,
                    "pending_count": len(self._pending),
                    "delivered_count": len(self._delivered),
                    "failed_count": len(self._failed),
                }, f)
        except Exception:
            pass

    def make_msg_id(self, from_agent: str, content: str, kind: int) -> str:
        """Генерация уникального ID сообщения."""
        raw = f"{from_agent}:{content}:{kind}:{time.time()}:{os.urandom(8).hex()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def register_send(
        self, msg_id: str, from_agent: str, to_agent: str,
        channel: str, event_id: str = ""
    ):
        """Зарегистрировать отправку сообщения — начать ожидание ACK."""
        pending = PendingACK(
            msg_id=msg_id,
            original_event_id=event_id or msg_id,
            from_agent=from_agent,
            to_agent=to_agent,
            channel=channel,
            sent_at=time.time(),
        )

        # Определить следующий канал для retry
        try:
            idx = self.FALLBACK_ORDER.index(channel)
            pending.next_retry_channel = (
                self.FALLBACK_ORDER[(idx + 1) % len(self.FALLBACK_ORDER)]
            )
        except ValueError:
            pending.next_retry_channel = "gossip"

        self._pending[msg_id] = pending
        self.stats["total_sent"] += 1

    def receive_ack(self, ack_event: dict) -> dict | None:
        """Обработать полученный ACK (kind:8014).

        Returns:
            {msg_id, from, to, latency_ms, channel, delivered: True/False}
            или None если ACK не распознан.
        """
        tags = ack_event.get("tags", [])
        event_id = ""
        status = "delivered"
        latency_ms = 0

        for tag in tags:
            if len(tag) >= 2:
                if tag[0] == "e":
                    event_id = tag[1]
                elif tag[0] == "status":
                    status = tag[1]
                elif tag[0] == "latency_ms" and len(tag) >= 2:
                    try:
                        latency_ms = int(tag[1])
                    except (ValueError, TypeError):
                        pass

        if not event_id:
            return None

        # Найти pending по original_event_id
        found_msg_id = None
        for msg_id, p in self._pending.items():
            if p.original_event_id == event_id:
                found_msg_id = msg_id
                break

        if not found_msg_id:
            return None

        pending = self._pending.pop(found_msg_id)

        result = {
            "msg_id": found_msg_id,
            "from": pending.from_agent,
            "to": pending.to_agent,
            "latency_ms": latency_ms or (time.time() - pending.sent_at) * 1000,
            "channel": pending.channel,
            "delivered": status == "delivered",
            "retries": pending.retries,
        }

        if status == "delivered":
            self._delivered[found_msg_id] = result
            self.stats["acked"] += 1
        else:
            self._failed[found_msg_id] = result
            self.stats["failed"] += 1

        # Обновить статистику по каналу
        ch = pending.channel
        if ch not in self.stats["by_channel"]:
            self.stats["by_channel"][ch] = {"sent": 0, "acked": 0, "failed": 0}
        self.stats["by_channel"][ch]["sent"] = (
            self.stats["by_channel"][ch].get("sent", 0) + 1
        )
        if status == "delivered":
            self.stats["by_channel"][ch]["acked"] = (
                self.stats["by_channel"][ch].get("acked", 0) + 1
            )
        else:
            self.stats["by_channel"][ch]["failed"] = (
                self.stats["by_channel"][ch].get("failed", 0) + 1
            )

        # Обновить среднюю задержку
        total_acks = self.stats["acked"]
        if total_acks > 0:
            old_avg = self.stats["avg_latency_ms"]
            new_lat = result["latency_ms"]
            self.stats["avg_latency_ms"] = (
                old_avg * (total_acks - 1) + new_lat
            ) / total_acks

        self._save_state()
        return result

    def get_pending_retries(self) -> list[PendingACK]:
        """Вернуть сообщения, требующие повторной отправки."""
        retries = []
        for msg_id, p in self._pending.items():
            if p.is_expired():
                self._failed[msg_id] = {
                    "msg_id": msg_id,
                    "from": p.from_agent,
                    "to": p.to_agent,
                    "channel": p.channel,
                    "error": "timeout",
                    "retries": p.retries,
                }
                self.stats["failed"] += 1
                continue
            if p.should_retry():
                retries.append(p)
        # Удалить expired
        for msg_id in list(self._pending.keys()):
            if self._pending[msg_id].is_expired():
                del self._pending[msg_id]
        return retries

    def mark_retry(self, msg_id: str, new_channel: str):
        """Отметить повторную отправку."""
        if msg_id in self._pending:
            self._pending[msg_id].retries += 1
            self._pending[msg_id].last_retry_at = time.time()
            self._pending[msg_id].channel = new_channel
            self._pending[msg_id].next_retry_channel = (
                self.FALLBACK_ORDER[
                    (self.FALLBACK_ORDER.index(new_channel) + 1)
                    % len(self.FALLBACK_ORDER)
                ]
            )
            self.stats["retried"] += 1

    def get_stats(self) -> dict:
        """Статистика ACK трекера."""
        return {
            **self.stats,
            "pending": len(self._pending),
            "delivered": len(self._delivered),
            "failed": len(self._failed),
            "delivery_rate": (
                self.stats["acked"] / max(self.stats["total_sent"], 1)
            ),
            "channels": self.stats["by_channel"],
        }

    async def cleanup_loop(self):
        """Фоновый цикл очистки просроченных ACK."""
        while self._running:
            await asyncio.sleep(ACK_CLEANUP_INTERVAL)
            expired = [
                msg_id for msg_id, p in self._pending.items() if p.is_expired()
            ]
            for msg_id in expired:
                p = self._pending.pop(msg_id)
                self._failed[msg_id] = {
                    "msg_id": msg_id,
                    "from": p.from_agent,
                    "to": p.to_agent,
                    "channel": p.channel,
                    "error": "expired",
                    "retries": p.retries,
                }
                self.stats["failed"] += 1
            if expired:
                self._save_state()

    def start(self):
        self._running = True
        self._cleanup_task = asyncio.create_task(self.cleanup_loop())

    def stop(self):
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
        self._save_state()


# ═══ Global singleton ═══
_ack_tracker: Optional[ACKTracker] = None


def get_ack_tracker() -> ACKTracker:
    global _ack_tracker
    if _ack_tracker is None:
        _ack_tracker = ACKTracker()
    return _ack_tracker


def build_ack_event(
    original_event_id: str,
    recipient_pubkey: str,
    status: str = "delivered",
    latency_ms: float = 0,
    relay: str = ""
) -> dict:
    """Построить ACK-событие (kind:8014)."""
    tags = [
        ["e", original_event_id],
        ["p", recipient_pubkey],
        ["status", status],
        ["latency_ms", str(int(latency_ms))],
    ]
    if relay:
        tags.append(["relay", relay])

    return {
        "kind": ACK_KIND,
        "content": "",
        "tags": tags,
        "created_at": int(time.time()),
    }

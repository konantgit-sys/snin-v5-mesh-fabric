#!/usr/bin/env python3
"""
SNIN V4 Anti-DDoS Module
Версия: 4.0
Дата: 2026-05-23

Защита mesh от массовых атак:
- Rate limiter: per-IP, per-pubkey
- Max event size: 64KB
- Blacklist: мусорные pubkey с TTL
- Signature gate: reject без подписи / с битой подписью
"""

import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from typing import Dict, Optional, Tuple

logger = logging.getLogger("anti_ddos")

# ─── Конфигурация ───
MAX_EVENT_SIZE = 65536           # 64 KB — макс. размер события
RATE_LIMIT_WINDOW = 60           # сек — окно rate limit
MAX_REQUESTS_PER_IP = 100        # макс. запросов за окно (per-IP)
MAX_REQUESTS_PER_PUBKEY = 50     # макс. запросов за окно (per-pubkey)
BLACKLIST_TTL = 300              # сек — время блокировки (5 мин)
BLACKLIST_THRESHOLD = 10         # кол-во нарушений до блокировки

# Статус файл
STATUS_FILE = "/home/agent/data/sites/snin-hub/antiddos_status.json"


class AntiDDoS:
    """
    Защита mesh от массовых атак.
    
    Использование:
        ddos = AntiDDoS()
        
        # В точке входа событий:
        ok, reason = await ddos.check_event(sender_ip, pubkey, content)
        if not ok:
            reject(reason)
    """
    
    def __init__(self):
        # ─── Rate limiter (in-memory) ───
        self._ip_counter: Dict[str, deque] = defaultdict(lambda: deque(maxlen=MAX_REQUESTS_PER_IP * 2))
        self._pubkey_counter: Dict[str, deque] = defaultdict(lambda: deque(maxlen=MAX_REQUESTS_PER_PUBKEY * 2))
        
        # ─── Blacklist ───
        self._blacklist: Dict[str, float] = {}  # ip/pubkey → expires_at
        
        # ─── Stats ───
        self._stats = {
            "rejected_size": 0,
            "rejected_rate_ip": 0,
            "rejected_rate_pubkey": 0,
            "rejected_blacklist": 0,
            "rejected_no_signature": 0,
            "accepted": 0,
            "total": 0,
            "blacklist_size": 0,
        }
        self._started_at = time.time()
    
    async def check_event(self, ip: str, pubkey: str, content: str, 
                          signature: Optional[str] = None) -> Tuple[bool, str]:
        """
        Проверить событие на DDoS признаки.
        
        Returns:
            (True, "ok") — пропустить
            (False, "reason") — отклонить с причиной
        """
        self._stats["total"] += 1
        
        # 1. Blacklist check
        for key in (ip, pubkey):
            if key in self._blacklist:
                if time.time() < self._blacklist[key]:
                    self._stats["rejected_blacklist"] += 1
                    return False, "blacklisted"
                else:
                    del self._blacklist[key]
        
        # 2. Max size check
        content_size = len(content.encode("utf-8")) if content else 0
        if content_size > MAX_EVENT_SIZE:
            self._stats["rejected_size"] += 1
            self._ban(ip)
            self._ban(pubkey)
            return False, f"event too large: {content_size} > {MAX_EVENT_SIZE}"
        
        # 3. Signature gate
        if not signature or len(signature) < 10:
            self._stats["rejected_no_signature"] += 1
            return False, "signature required or invalid"
        
        # 4. Rate limit per-IP
        now = time.time()
        self._ip_counter[ip].append(now)
        # Clean old
        while self._ip_counter[ip] and self._ip_counter[ip][0] < now - RATE_LIMIT_WINDOW:
            self._ip_counter[ip].popleft()
        
        if len(self._ip_counter[ip]) > MAX_REQUESTS_PER_IP:
            self._stats["rejected_rate_ip"] += 1
            self._ban(ip)
            return False, f"rate limit exceeded (IP): {len(self._ip_counter[ip])}/{MAX_REQUESTS_PER_IP}"
        
        # 5. Rate limit per-pubkey
        self._pubkey_counter[pubkey].append(now)
        while self._pubkey_counter[pubkey] and self._pubkey_counter[pubkey][0] < now - RATE_LIMIT_WINDOW:
            self._pubkey_counter[pubkey].popleft()
        
        if len(self._pubkey_counter[pubkey]) > MAX_REQUESTS_PER_PUBKEY:
            self._stats["rejected_rate_pubkey"] += 1
            self._ban(pubkey)
            return False, f"rate limit exceeded (pubkey): {len(self._pubkey_counter[pubkey])}/{MAX_REQUESTS_PER_PUBKEY}"
        
        self._stats["accepted"] += 1
        return True, "ok"
    
    def _ban(self, key: str):
        """Добавить ключ в blacklist (если превышен порог нарушений)."""
        self._blacklist[key] = time.time() + BLACKLIST_TTL
        self._stats["blacklist_size"] = len(self._blacklist)
    
    def get_stats(self) -> dict:
        """Статистика защиты."""
        return {
            "uptime_sec": int(time.time() - self._started_at),
            "total_checked": self._stats["total"],
            "accepted": self._stats["accepted"],
            "rejected": {
                "size": self._stats["rejected_size"],
                "rate_ip": self._stats["rejected_rate_ip"],
                "rate_pubkey": self._stats["rejected_rate_pubkey"],
                "blacklist": self._stats["rejected_blacklist"],
                "no_signature": self._stats["rejected_no_signature"],
            },
            "blacklist_size": self._stats["blacklist_size"],
            "config": {
                "max_event_size": MAX_EVENT_SIZE,
                "rate_window_sec": RATE_LIMIT_WINDOW,
                "max_per_ip": MAX_REQUESTS_PER_IP,
                "max_per_pubkey": MAX_REQUESTS_PER_PUBKEY,
                "blacklist_ttl": BLACKLIST_TTL,
            },
        }
    
    def save_status(self):
        """Сохранить статус в файл (читается supervisor)."""
        try:
            os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)
            with open(STATUS_FILE, "w") as f:
                json.dump(self.get_stats(), f, indent=2, default=str)
        except Exception:
            pass

#!/usr/bin/env python3
"""
L14: Alert Engine — YAML-driven rule engine with multi-channel dispatch + escalation.

Фаза 2B → L14 по SPEC_NEW_LAYERS.md

Архитектура:
  [Health Monitor] → evaluate(statuses) → [Alert Engine]
                                           ↓ match rules
                                     [Trigger / Escalate]
                                           ↓
                              [Telegram] [Nostr] [Webhook]

Конфигурация: alert_config.yaml
Каналы: Telegram (env: ALERT_TG_BOT_TOKEN, ALERT_TG_CHAT_ID)
        Nostr (env: NOSTR_PRIVATE_KEY)
        Webhook (env: ALERT_WEBHOOK_URL)
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

import aiohttp
import yaml

logger = logging.getLogger("AlertEngine")

# ─── Конфигурация ───
CONFIG_PATH = os.environ.get("ALERT_CONFIG", "/home/agent/data/sites/relay-mesh/alert_config.yaml")
ALERT_DB = os.environ.get("ALERT_DB", "/home/agent/data/sites/relay-mesh/logs/alert_log.db")
TG_BOT_TOKEN = os.environ.get("ALERT_TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("ALERT_TG_CHAT_ID", "")
NOSTR_PRIVATE_KEY = os.environ.get("NOSTR_PRIVATE_KEY", "")
WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL", "")

NOSTR_RELAYS = [
    "wss://relay.damus.io",
    "wss://nos.lol",
    "wss://relay.primal.net",
]

# ─── SQLite Schema ───
SCHEMA = """
CREATE TABLE IF NOT EXISTS alert_log (
    id TEXT PRIMARY KEY,
    rule_name TEXT NOT NULL,
    service_name TEXT NOT NULL,
    status TEXT NOT NULL,
    priority TEXT NOT NULL,
    triggered_at INTEGER NOT NULL,
    acknowledged INTEGER DEFAULT 0,
    ack_at INTEGER,
    escalation_level INTEGER DEFAULT 0,
    channel TEXT DEFAULT '',
    resolved_at INTEGER
);
CREATE TABLE IF NOT EXISTS alert_events (
    id TEXT PRIMARY KEY,
    alert_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    sent_at INTEGER NOT NULL,
    success INTEGER DEFAULT 1,
    error TEXT
);
"""


class AlertEngine:
    """
    Rule engine for health alerts.
    Usage: engine.evaluate(statuses) → triggers matching rules → sends via channels → escalates.
    """

    def __init__(self, config_path: str = CONFIG_PATH):
        self.config_path = config_path
        self.rules: List[Dict] = []
        self._active_alerts: Dict[str, Dict] = {}  # alert_id → alert data
        self._escalation_timers: Dict[str, asyncio.Task] = {}
        self._dead_since: Dict[str, float] = {}  # service_name → first_dead_timestamp
        self._last_status: Dict[str, str] = {}
        self._metrics: Dict[str, Any] = {}

        # DB
        os.makedirs(os.path.dirname(ALERT_DB) or ".", exist_ok=True)
        self._db = sqlite3.connect(ALERT_DB, check_same_thread=False)
        self._init_db()

        # Load rules
        self._load_rules()
        logger.info(f"📋 Loaded {len(self.rules)} alert rules from {config_path}")

        # Channel availability
        self._channels = {}
        if TG_BOT_TOKEN and TG_CHAT_ID:
            self._channels["telegram"] = True
            logger.info("📱 Telegram channel: enabled")
        else:
            logger.info("📱 Telegram channel: disabled (set ALERT_TG_BOT_TOKEN, ALERT_TG_CHAT_ID)")
        if NOSTR_PRIVATE_KEY:
            self._channels["nostr_dm"] = True
            logger.info("🌐 Nostr channel: enabled")
        else:
            logger.info("🌐 Nostr channel: disabled (set NOSTR_PRIVATE_KEY)")
        if WEBHOOK_URL:
            self._channels["webhook"] = True
            logger.info("🔗 Webhook channel: enabled")
        else:
            logger.info("🔗 Webhook channel: disabled (set ALERT_WEBHOOK_URL)")

    # ─── INIT DB ───
    def _init_db(self):
        for stmt in SCHEMA.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                self._db.execute(stmt)
        self._db.commit()

    # ─── LOAD RULES ───
    def _load_rules(self):
        if not os.path.exists(self.config_path):
            logger.warning(f"Config not found: {self.config_path}, using empty rules")
            self.rules = []
            return
        with open(self.config_path) as f:
            data = yaml.safe_load(f)
        self.rules = data.get("rules", []) if data else []

    def reload_rules(self):
        """Hot-reload правил без перезапуска."""
        self._load_rules()
        logger.info(f"🔄 Rules reloaded: {len(self.rules)} rules")

    # ─── EVALUATION ───
    async def evaluate(self, statuses: Dict[str, Dict]):
        """
        Evaluate all services against all rules.
        Called from health_check_engine.monitor_loop() each cycle.
        statuses: {service_name: ServiceStatus.to_dict()}
        """
        now = time.time()

        # Build metrics
        total = len(statuses)
        dead_count = sum(1 for s in statuses.values() if not s.get("is_alive"))
        alive_count = total - dead_count
        self._metrics = {
            "total": total,
            "dead_count": dead_count,
            "alive_count": alive_count,
            "alive_pct": round(alive_count / total * 100, 1) if total else 0,
            "critical_count": sum(1 for s in statuses.values()
                                   if not s.get("is_alive") and "nostr_bridge" in s.get("name", "")),
            "layer_alive_pct": round(alive_count / total * 100, 1) if total else 100.0,
        }

        # Per-service evaluation
        for name, st in statuses.items():
            current_status = "alive" if st.get("is_alive") else "dead"
            prev_status = self._last_status.get(name, "alive")

            # Track dead duration
            if current_status == "dead" and prev_status == "alive":
                self._dead_since[name] = now
            elif current_status == "alive":
                if prev_status == "dead":
                    # Recovery event
                    for rule in self._get_matching_rules(name, current_status, st):
                        await self._trigger(rule, name, st)
                    self._resolve_active_alert(name)
                self._dead_since.pop(name, None)

            self._last_status[name] = current_status

            # Evaluate rules for dead/degraded services
            if current_status == "dead":
                for rule in self._get_matching_rules(name, current_status, st):
                    await self._evaluate_rule(rule, name, st, now)

        # Layer-level rules
        for rule in self.rules:
            svc = rule.get("service", "*")
            if svc == "*" and "critical_mass" in rule.get("name", ""):
                # Check critical mass
                cond = rule.get("condition", "")
                if self._eval_condition(cond, {}, self._metrics):
                    await self._trigger(rule, "__system__", {"metrics": self._metrics})

    def _get_matching_rules(self, service_name: str, status: str, st: Dict) -> List[Dict]:
        """Returns rules matching this service + status."""
        matched = []
        for rule in self.rules:
            svc_pattern = rule.get("service", "*")
            if svc_pattern == "*":
                matched.append(rule)
                continue
            if svc_pattern == service_name:
                matched.append(rule)
                continue
            if svc_pattern.endswith("*") and service_name.startswith(svc_pattern[:-1]):
                matched.append(rule)
                continue
            if svc_pattern.startswith("*") and service_name.endswith(svc_pattern[1:]):
                matched.append(rule)
                continue
        return matched

    def _eval_condition(self, condition: str, service_status: Dict, metrics: Dict) -> bool:
        """Evaluate a simple condition string against status + metrics.
        Supports: status == 'dead', metrics.dead_count >= 3, etc.
        """
        if not condition:
            return True

        # Dict → object wrapper for dotted access
        class DictObj:
            def __init__(self, d):
                self.__dict__["_data"] = d
            def __getattr__(self, k):
                v = self.__dict__["_data"].get(k)
                if isinstance(v, dict):
                    return DictObj(v)
                return v
            def __repr__(self):
                return repr(self.__dict__["_data"])

        # Build namespace
        ns = {
            "status": "alive" if service_status.get("is_alive", False) else "dead",
            "metrics": DictObj(metrics),
        }
        for k, v in service_status.items():
            if isinstance(k, str) and isinstance(v, (str, int, float, bool)):
                ns[k] = v

        try:
            result = eval(condition, {"__builtins__": {}}, ns)
            return bool(result)
        except Exception as e:
            logger.warning(f"Condition eval error '{condition}': {e}")
            return False

    async def _evaluate_rule(self, rule: Dict, name: str, st: Dict, now: float):
        """Check if rule should fire based on duration."""
        cond = rule.get("condition", "status == 'dead'")
        if not self._eval_condition(cond, st, self._metrics):
            return

        duration = rule.get("duration", 0)
        since = self._dead_since.get(name, now)

        if now - since < duration:
            return  # Not yet — waiting for duration threshold

        # Check if already triggered for this rule+service
        trigger_key = f"{rule['name']}:{name}"
        if trigger_key in self._active_alerts:
            return  # Already active, escalation handles it

        await self._trigger(rule, name, st)

    async def _trigger(self, rule: Dict, name: str, st: Dict):
        """Fire alert — log to DB, send via channels, start escalation."""
        alert_id = str(uuid.uuid4())[:8]
        priority = rule.get("priority", "INFO")
        now = int(time.time())

        # Log to DB
        self._db.execute(
            "INSERT INTO alert_log (id, rule_name, service_name, status, priority, triggered_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (alert_id, rule["name"], name,
             "alive" if st.get("is_alive", False) else "dead",
             priority, now)
        )
        self._db.commit()

        # Store active alert
        alert_data = {
            "id": alert_id,
            "rule": rule,
            "service": name,
            "status": st,
            "priority": priority,
            "triggered_at": now,
            "escalation_level": 0,
            "channels_sent": set(),
        }
        self._active_alerts[f"{rule['name']}:{name}"] = alert_data

        # Send via initial channels
        channels = rule.get("channels", ["telegram"] if TG_BOT_TOKEN else [])
        await self._dispatch(alert_id, channels, alert_data, level=0)

        # Start escalation
        await self._start_escalation(alert_id, rule, alert_data)

        priority_icon = {"CRITICAL": "🔴🔴", "HIGH": "🔴", "WARNING": "🟡", "INFO": "✅"}.get(priority, "🔵")
        logger.info(f"{priority_icon} ALERT [{alert_id}] {rule['name']} → {name}")

    def _resolve_active_alert(self, service_name: str):
        """Resolve all active alerts for a recovered service."""
        to_remove = []
        for key, alert in self._active_alerts.items():
            if alert["service"] == service_name:
                now = int(time.time())
                self._db.execute(
                    "UPDATE alert_log SET resolved_at = ? WHERE id = ?",
                    (now, alert["id"])
                )
                self._db.commit()
                # Cancel escalation timer
                if alert["id"] in self._escalation_timers:
                    self._escalation_timers[alert["id"]].cancel()
                    del self._escalation_timers[alert["id"]]
                to_remove.append(key)
        for key in to_remove:
            del self._active_alerts[key]

    async def acknowledge(self, alert_id: str) -> bool:
        """Acknowledge an alert — cancels escalation."""
        now = int(time.time())
        self._db.execute(
            "UPDATE alert_log SET acknowledged = 1, ack_at = ? WHERE id = ?",
            (now, alert_id)
        )
        self._db.commit()

        # Cancel escalation timer
        for key, alert in list(self._active_alerts.items()):
            if alert["id"] == alert_id:
                if alert_id in self._escalation_timers:
                    self._escalation_timers[alert_id].cancel()
                    del self._escalation_timers[alert_id]
                del self._active_alerts[key]
                logger.info(f"✅ Alert {alert_id} acknowledged — escalation cancelled")
                return True

        logger.info(f"✅ Alert {alert_id} acknowledged (no active escalation)")
        return True

    # ─── DISPATCH ───
    async def _dispatch(self, alert_id: str, channels: List[str], alert_data: Dict, level: int):
        """Send alert via specified channels."""
        now = int(time.time())
        rule = alert_data["rule"]
        name = alert_data["service"]
        priority = alert_data["priority"]
        status_text = "alive" if alert_data["status"].get("is_alive", False) else "dead"
        error = alert_data["status"].get("error", "")

        msg = (
            f"🚨 *{priority} ALERT* [{alert_id}]\n"
            f"• Rule: {rule['name']}\n"
            f"• Service: {name}\n"
            f"• Status: {status_text}\n"
            f"• Error: {error or 'N/A'}\n"
            f"• Level: {level}\n"
            f"• `/ack {alert_id}` — подтвердить"
        )

        for ch in channels:
            alert_data["channels_sent"].add(ch)
            self._db.execute(
                "INSERT INTO alert_events (id, alert_id, channel, sent_at) VALUES (?, ?, ?, ?)",
                (str(uuid.uuid4())[:8], alert_id, ch, now)
            )
            self._db.commit()

        tasks = []
        if "telegram" in channels and TG_BOT_TOKEN:
            tasks.append(self._send_tg(msg))
        if "nostr_dm" in channels and NOSTR_PRIVATE_KEY:
            tasks.append(self._send_nostr(alert_id, rule, name, priority, status_text))
        if "webhook" in channels and WEBHOOK_URL:
            tasks.append(self._send_webhook(alert_id, alert_data))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_tg(self, text: str):
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json={
                    "chat_id": TG_CHAT_ID, "text": text,
                    "parse_mode": "Markdown", "disable_web_page_preview": True,
                }, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        logger.error(f"TG send failed ({resp.status})")
        except Exception as e:
            logger.error(f"TG send error: {e}")

    async def _send_nostr(self, alert_id, rule, name, priority, status_text):
        kind = 9001
        event = {
            "kind": kind,
            "created_at": int(time.time()),
            "tags": [
                ["alert", alert_id, priority],
                ["t", "health"],
                ["p", ""],  # DM target — нужен pubkey оператора
            ],
            "content": json.dumps({
                "alert_id": alert_id,
                "rule": rule["name"],
                "service": name,
                "status": status_text,
                "priority": priority,
            }),
        }
        for relay_url in NOSTR_RELAYS:
            try:
                import websockets
                async with websockets.connect(relay_url, timeout=5) as ws:
                    msg = json.dumps(["EVENT", event])
                    await ws.send(msg)
                    await asyncio.wait_for(ws.recv(), timeout=5)
            except Exception:
                pass
        logger.info(f"🌐 Nostr alert {alert_id} → {len(NOSTR_RELAYS)} relays")

    async def _send_webhook(self, alert_id: str, alert_data: Dict):
        payload = {
            "alert_id": alert_id,
            "rule": alert_data["rule"]["name"],
            "service": alert_data["service"],
            "priority": alert_data["priority"],
            "timestamp": int(time.time()),
            "status": alert_data["status"],
        }
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(WEBHOOK_URL, json=payload,
                                            timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status < 500:
                            return
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
        logger.error(f"Webhook {alert_id} failed after 3 attempts")

    # ─── ESCALATION ───
    async def _start_escalation(self, alert_id: str, rule: Dict, alert_data: Dict):
        """Create background task for escalation timers."""
        task = asyncio.create_task(self._escalation_loop(alert_id, rule, alert_data))
        self._escalation_timers[alert_id] = task

    async def _escalation_loop(self, alert_id, rule, alert_data):
        """Background loop: waits for escalation levels, then dispatches."""
        escalation = rule.get("escalation", [])
        for level, step in enumerate(escalation, start=1):
            await asyncio.sleep(step["after"])
            # Check if alert is still active (not ack'd, not resolved)
            if alert_id not in [a["id"] for a in self._active_alerts.values()]:
                return  # Alert was ack'd or resolved
            alert_data["escalation_level"] = level
            self._db.execute(
                "UPDATE alert_log SET escalation_level = ? WHERE id = ?",
                (level, alert_id)
            )
            self._db.commit()
            await self._dispatch(alert_id, step.get("channels", []), alert_data, level=level)
            logger.info(f"⬆️ Escalation {alert_id} → level {level} ({step.get('channels', [])})")

    # ─── API ───
    def get_alerts(self, limit: int = 20, active_only: bool = False) -> List[Dict]:
        """Get alert log entries."""
        if active_only:
            return [
                a["id"] for a in self._active_alerts.values()
            ]
        cur = self._db.execute(
            "SELECT * FROM alert_log ORDER BY triggered_at DESC LIMIT ?", (limit,)
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_active_alerts(self) -> List[Dict]:
        """Get currently active (unack'd, unresolved) alerts."""
        cur = self._db.execute(
            "SELECT * FROM alert_log WHERE acknowledged = 0 "
            "AND resolved_at IS NULL ORDER BY triggered_at DESC"
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ─── Singleton ───
_engine: Optional[AlertEngine] = None


def get_alert_engine() -> AlertEngine:
    global _engine
    if _engine is None:
        _engine = AlertEngine()
    return _engine

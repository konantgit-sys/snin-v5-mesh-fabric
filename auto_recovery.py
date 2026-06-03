#!/usr/bin/env python3
"""
L15: Auto-Recovery Engine — автоматическое восстановление сервисов.

Архитектура:
  [Health Monitor] → сервис dead (3+ consecutive fails)
           ↓
  [Auto-Recovery] → analyze (логи, метрики, история)
           ↓
  [Attempt 1] → restart (supervisor_bridge)
           ↓ fail
  [Attempt 2] → restart + clear_cache
           ↓ fail
  [Attempt 3] → reload_layer / failover
           ↓ fail
  [L14 Alert Engine] → эскалация человеку

Конфигурация: recovery_config.yaml (стратегии восстановления)
Интеграция: health_check_engine.py вызывает auto_recovery.on_service_dead()

Безопасность:
  - Max daily лимиты (не зацикливаться)
  - Cooldown между попытками
  - Slot health threshold (если >3 сервисов dead → эскалация)
  - Rate limit глобальный
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiohttp
import yaml

from supervisor_bridge import get_supervisor_bridge

logger = logging.getLogger("AutoRecovery")

# ─── Конфигурация ───
CONFIG_PATH = os.environ.get("RECOVERY_CONFIG",
    "/home/agent/data/sites/relay-mesh/recovery_config.yaml")
RECOVERY_DB = os.environ.get("RECOVERY_DB",
    "/home/agent/data/sites/relay-mesh/logs/recovery_log.db")

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS recovery_events (
    id TEXT PRIMARY KEY,
    service_name TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    duration_ms INTEGER,
    error TEXT,
    triggered_at INTEGER NOT NULL,
    resolved_at INTEGER
);
CREATE TABLE IF NOT EXISTS recovery_daily (
    date TEXT NOT NULL,
    service_name TEXT NOT NULL,
    count INTEGER DEFAULT 0,
    PRIMARY KEY (date, service_name)
);
CREATE TABLE IF NOT EXISTS recovery_analysis (
    id TEXT PRIMARY KEY,
    service_name TEXT NOT NULL,
    probable_cause TEXT,
    evidence TEXT,
    recommended_action TEXT,
    analyzed_at INTEGER NOT NULL
);
"""

class AutoRecovery:
    """
    L15: Auto-Recovery Engine.
    Основной цикл: on_service_dead() → analyze → recover() → escalate() если всё сломалось.
    """

    def __init__(self, config_path: str = CONFIG_PATH):
        self.config_path = config_path
        self.config: Dict = {}
        self.strategies: Dict = {}

        # Состояние
        self._dead_since: Dict[str, float] = {}          # service → first_dead_timestamp
        self._consecutive_fails: Dict[str, int] = {}     # service → fail count
        self._attempt_in_progress: Dict[str, bool] = {}  # service → is_recovering
        self._global_cooldown_until: float = 0
        self._recovery_locks: Dict[str, float] = {}      # service → locked_until
        self._daily_count: int = 0

        # DB
        os.makedirs(os.path.dirname(RECOVERY_DB) or ".", exist_ok=True)
        self._db = sqlite3.connect(RECOVERY_DB, check_same_thread=False)
        self._init_db()

        # Load config
        self._load_config()
        self._load_daily_count()

        # Supervisor bridge
        self._sv = get_supervisor_bridge()

        self._stats: Dict = {
            "total_attempts": 0,
            "successful": 0,
            "failed": 0,
            "escalated": 0,
            "active_recoveries": 0,
        }

        logger.info(f"🔧 AutoRecovery: {len(self.strategies)} strategies, "
                     f"today={self._daily_count}")

    def _init_db(self):
        for stmt in DB_SCHEMA.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                self._db.execute(stmt)
        self._db.commit()

    def _load_config(self):
        if not os.path.exists(self.config_path):
            logger.warning(f"Config not found: {self.config_path}")
            self.config = {"strategies": {}, "max_concurrent": 3,
                           "global_cooldown": 60, "max_daily_total": 30}
            self.strategies = {}
            return
        with open(self.config_path) as f:
            self.config = yaml.safe_load(f) or {}
        self.strategies = self.config.get("strategies", {})

    def _load_daily_count(self):
        today = datetime.utcnow().strftime("%Y-%m-%d")
        cur = self._db.execute(
            "SELECT COALESCE(SUM(count), 0) FROM recovery_daily WHERE date = ?",
            (today,)
        )
        row = cur.fetchone()
        self._daily_count = row[0] if row else 0

    # ─── PUBLIC API ───

    async def on_service_dead(self, service_name: str, status: Dict) -> bool:
        """
        Вызывается из health_check_engine.monitor_loop().
        Возвращает True, если recovery запущен, False если blocked/завершён.
        """
        now = time.time()

        # ─── Guards ───

        # 1. Slot health threshold
        slot_threshold = self.config.get("slot_health_threshold", 3)
        statuses = status.get("_all_statuses", {})
        if len([s for s in statuses.values() if not s.get("is_alive")]) >= slot_threshold:
            logger.warning(f"🔴 Slot threshold exceeded ({slot_threshold}+ dead) — skipping auto-recovery")
            await self._escalate(service_name, f"mass_death>={slot_threshold}")
            return False

        # 2. Already recovering
        if self._attempt_in_progress.get(service_name):
            logger.debug(f"  Already recovering {service_name}")
            return False

        # 3. Global cooldown
        if now < self._global_cooldown_until:
            logger.debug(f"  Global cooldown, wait {self._global_cooldown_until - now:.0f}s")
            return False

        # 4. Service cooldown
        if now < self._recovery_locks.get(service_name, 0):
            logger.debug(f"  Service cooldown for {service_name}")
            return False

        # 5. Daily limit
        max_daily = self.config.get("max_daily_total", 30)
        if self._daily_count >= max_daily:
            logger.warning(f" Daily limit reached ({max_daily}) — skipping recovery")
            return False

        # ─── Consecutive fails check ───
        fail_count = self._consecutive_fails.get(service_name, 0)
        if fail_count < 3:
            self._consecutive_fails[service_name] = fail_count + 1
            logger.info(f"  {service_name}: consec_fails={fail_count+1}/3 (waiting threshold)")
            return False

        # ─── Start recovery ───
        if service_name not in self._dead_since:
            self._dead_since[service_name] = now

        self._attempt_in_progress[service_name] = True
        self._stats["active_recoveries"] += 1

        try:
            # Step 1: Analyze
            cause, evidence = self._analyze(service_name, status)
            logger.warning(f"🔍 {service_name} analysis: {cause}")

            # Step 2: Get strategy
            strategy_name = self._get_strategy_name(service_name)
            strategy = self.strategies.get(strategy_name, self.strategies.get("default", {}))
            attempts = strategy.get("attempts", [])

            if not attempts:
                logger.warning(f"  No recovery strategy for {service_name}, escalating")
                await self._escalate(service_name, "no_strategy")
                return False

            # Step 3: Execute attempts
            success = await self._execute_attempts(service_name, attempts, cause)

            # Step 4: Result
            if success:
                self._stats["successful"] += 1
                logger.info(f"✅ {service_name} recovered successfully")
            else:
                self._stats["failed"] += 1
                self._stats["escalated"] += 1
                logger.warning(f"❌ {service_name} recovery failed — escalated")

            return success

        finally:
            self._attempt_in_progress[service_name] = False
            self._stats["active_recoveries"] -= 1

    # ─── ANALYZE ───

    def _analyze(self, service_name: str, status: Dict) -> tuple:
        """Анализ причины падения по логам и метрикам.

        Returns: (probable_cause, evidence_dict)
        """
        evidence = {}
        error = status.get("error", "")
        latency = status.get("latency_ms", 0)
        uptime = status.get("uptime_seconds", 0)
        consecutive = self._consecutive_fails.get(service_name, 0)

        # Анализ ошибок
        if "refused" in error.lower() or "connection refused" in error.lower():
            cause = "connection_refused"
            evidence = {"error": error, "consecutive": consecutive}
        elif "timeout" in error.lower():
            cause = "timeout"
            evidence = {"error": error, "latency_ms": latency}
        elif "memory" in error.lower() or "oom" in error.lower():
            cause = "oom"
            evidence = {"error": error, "uptime_seconds": uptime}
        elif "fd" in error.lower() or "file descriptor" in error.lower():
            cause = "fd_leak"
            evidence = {"error": error, "uptime_seconds": uptime,
                        "symptom": "too_many_open_files"}
        elif "key" in error.lower() or "auth" in error.lower():
            cause = "auth_failure"
            evidence = {"error": error}
        else:
            cause = "unknown"
            evidence = {"error": error, "latency_ms": latency,
                        "consecutive": consecutive}

        # Сохраняем анализ в DB
        analysis_id = str(uuid.uuid4())[:8]
        self._db.execute(
            "INSERT INTO recovery_analysis (id, service_name, probable_cause, "
            "evidence, recommended_action, analyzed_at) VALUES (?, ?, ?, ?, ?, ?)",
            (analysis_id, service_name, cause, json.dumps(evidence),
             self._recommend_action(cause), int(time.time()))
        )
        self._db.commit()

        return cause, evidence

    @staticmethod
    def _recommend_action(cause: str) -> str:
        mapping = {
            "connection_refused": "restart",
            "timeout": "restart_clear_cache",
            "oom": "restart_with_limits",
            "fd_leak": "restart_clear_cache",
            "auth_failure": "restart",
        }
        return mapping.get(cause, "restart")

    def _get_strategy_name(self, service_name: str) -> str:
        """Определяет имя стратегии по имени сервиса."""
        for key in self.strategies:
            if key != "default" and (service_name.startswith(key) or service_name == key):
                return key
        return "default"

    # ─── EXECUTION ───

    async def _execute_attempts(self, service_name: str,
                                 attempts: List[Dict], cause: str) -> bool:
        """Выполняет попытки восстановления по стратегии."""
        health_check_after = self._get_health_check_delay(service_name)

        for attempt_idx, attempt in enumerate(attempts, start=1):
            action = attempt.get("action", "restart")
            args = attempt.get("args", {})
            desc = attempt.get("description", action)

            if action == "escalate":
                await self._escalate(service_name, f"after_{attempt_idx}_attempts: {desc}")
                return False

            start = time.time()
            success = await self._execute_action(service_name, action, args)
            duration_ms = int((time.time() - start) * 1000)

            # Log to DB
            attempt_id = str(uuid.uuid4())[:8]
            self._db.execute(
                "INSERT INTO recovery_events (id, service_name, attempt, action, "
                "status, duration_ms, triggered_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (attempt_id, service_name, attempt_idx, action,
                 "success" if success else "failed", duration_ms, int(time.time()))
            )
            self._db.commit()
            self._stats["total_attempts"] += 1
            self._daily_count += 1
            self._update_daily_count(service_name)

            logger.info(f"  Attempt {attempt_idx}/{len(attempts)}: {action} "
                        f"→ {'✅' if success else '❌'} ({duration_ms}ms)")

            if success:
                # Wait for health check
                await asyncio.sleep(health_check_after)
                return True

            # Cooldown between attempts
            if attempt_idx < len(attempts):
                await asyncio.sleep(5)

        return False

    async def _execute_action(self, service_name: str, action: str,
                               args: Dict) -> bool:
        """Выполняет конкретное действие восстановления."""
        try:
            if action == "restart":
                return await self._sv.restart(service_name)

            elif action == "restart_clear_cache":
                svc_type = self._get_strategy_name(service_name)
                self._sv.clear_cache(svc_type)
                await asyncio.sleep(1)
                return await self._sv.restart(service_name)

            elif action == "reload_layer":
                # Рестарт всех сервисов этого типа
                svc_type = self._get_strategy_name(service_name)
                deps = self._sv.get_dependencies(service_name)
                all_svc = [service_name] + list(
                    self._sv.get_dependencies(f"{svc_type}_{i}")
                    for i in range(3) if f"{svc_type}_{i}" != service_name
                ) if svc_type != "default" else [service_name]

                if deps:
                    for dep in deps:
                        self._sv.restart(dep)
                        await asyncio.sleep(2)

                for s in all_svc:
                    if isinstance(s, str):
                        self._sv.restart(s)
                        await asyncio.sleep(1)
                return True

            elif action == "clear_cache":
                svc_type = self._get_strategy_name(service_name)
                return self._sv.clear_cache(svc_type)

            elif action == "restart_with_dump":
                dump_path = args.get("dump_path",
                    "/home/agent/data/sites/relay-mesh/logs/dumps/")
                os.makedirs(dump_path, exist_ok=True)
                dump_file = os.path.join(dump_path,
                    f"{service_name}_{int(time.time())}.json")
                # Сохраняем текущее состояние
                with open(dump_file, 'w') as f:
                    json.dump({
                        "service": service_name,
                        "timestamp": time.time(),
                        "action": "pre_restart_dump"
                    }, f)
                return await self._sv.restart(service_name)

            elif action == "failover_replica":
                # Пытаемся найти и запустить реплику
                replica_name = f"{service_name}_replica"
                return await self._sv.restart(replica_name)

            elif action == "recreate":
                logger.warning(f"Recreate not implemented for {service_name}")
                return False

            else:
                logger.warning(f"Unknown action: {action}")
                return False

        except Exception as e:
            logger.error(f"Action {action} failed: {e}")
            return False

    # ─── ESCALATION ───

    async def _escalate(self, service_name: str, reason: str):
        """Эскалация в L14 Alert Engine (если доступен) или лог."""
        logger.warning(f"🔴 ESCALATION: {service_name} — {reason}")
        # Попробуем импортировать alert_engine
        try:
            from alert_engine import get_alert_engine
            engine = get_alert_engine()
            await engine.on_status_change(service_name, "dead", {
                "error": f"recovery_failed: {reason}",
                "latency_ms": 0,
                "uptime": 0,
            })
        except Exception as e:
            logger.error(f"Alert engine escalation failed: {e}")

    # ─── CLEANUP & RESET ───

    def reset_service(self, service_name: str):
        """Сброс состояния сервиса (после ручного восстановления)."""
        self._dead_since.pop(service_name, None)
        self._consecutive_fails.pop(service_name, None)
        self._attempt_in_progress[service_name] = False
        self._recovery_locks.pop(service_name, None)
        logger.info(f"🔄 {service_name} state reset")

    def _get_health_check_delay(self, service_name: str) -> int:
        strategy_name = self._get_strategy_name(service_name)
        strategy = self.strategies.get(strategy_name, {})
        return strategy.get("health_check_after", 10)

    def _update_daily_count(self, service_name: str):
        today = datetime.utcnow().strftime("%Y-%m-%d")
        self._db.execute(
            "INSERT INTO recovery_daily (date, service_name, count) VALUES (?, ?, 1) "
            "ON CONFLICT(date, service_name) DO UPDATE SET count = count + 1",
            (today, service_name)
        )
        self._db.commit()

    # ─── API ───

    def get_stats(self) -> Dict:
        return dict(self._stats)

    def get_recovery_events(self, service_name: str = "",
                            limit: int = 50) -> List[Dict]:
        if service_name:
            cur = self._db.execute(
                "SELECT * FROM recovery_events WHERE service_name = ? "
                "ORDER BY triggered_at DESC LIMIT ?",
                (service_name, limit)
            )
        else:
            cur = self._db.execute(
                "SELECT * FROM recovery_events ORDER BY triggered_at DESC LIMIT ?",
                (limit,)
            )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_analysis(self, limit: int = 20) -> List[Dict]:
        cur = self._db.execute(
            "SELECT * FROM recovery_analysis ORDER BY analyzed_at DESC LIMIT ?",
            (limit,)
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def reload_config(self):
        self._load_config()
        logger.info(f"🔄 Recovery config reloaded: {len(self.strategies)} strategies")


# ─── Singleton ───
_recovery: Optional[AutoRecovery] = None


def get_auto_recovery() -> AutoRecovery:
    global _recovery
    if _recovery is None:
        _recovery = AutoRecovery()
    return _recovery

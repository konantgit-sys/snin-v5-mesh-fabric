"""
P16: Agent Cron System — scheduled task execution for mesh agents.

Agents register periodic tasks (crons). The CronScheduler ticks
every second, checks which crons are due, and executes them via
the mesh routing layers.

Integration:
    CronScheduler → first_contact (heartbeat, status update)
                   → ContentRouter (capability sync)
                   → ChequeBook (periodic accounting)
"""

import time
import json
import logging
from typing import Callable, Optional
from dataclasses import dataclass, field

logger = logging.getLogger("AgentCron")


# ─── Data Structures ───────────────────────────────────────

@dataclass
class CronJob:
    agent_id: str
    task_name: str
    interval_seconds: float
    handler: Callable
    last_run: float = 0.0
    enabled: bool = True
    fail_count: int = 0
    max_fails: int = 5

    @property
    def is_due(self) -> bool:
        if not self.enabled:
            return False
        if self.fail_count >= self.max_fails:
            return False
        return (time.time() - self.last_run) >= self.interval_seconds

    def mark_run(self):
        self.last_run = time.time()

    def mark_fail(self):
        self.fail_count += 1
        if self.fail_count >= self.max_fails:
            self.enabled = False
            logger.warning(f"[Cron] {self.agent_id}/{self.task_name}: disabled after {self.max_fails} failures")


# ─── Cron Scheduler ───────────────────────────────────────

class CronScheduler:
    """Manages scheduled tasks for all agents in the mesh."""

    def __init__(self):
        self.jobs: dict[str, list[CronJob]] = {}           # agent_id → [CronJob, ...]
        self.stats = {
            "total_jobs": 0,
            "total_runs": 0,
            "total_fails": 0,
            "active_agents": 0,
        }

    # ── Registration ──────────────────────────────────

    def register(self, agent_id: str, task_name: str, interval_seconds: float,
                 handler: Callable) -> CronJob:
        """Register a cron job for an agent. Cron starts after first interval (not immediately)."""
        job = CronJob(
            agent_id=agent_id,
            task_name=task_name,
            interval_seconds=interval_seconds,
            handler=handler,
            last_run=time.time(),  # don't run immediately
        )
        self.jobs.setdefault(agent_id, []).append(job)
        self.stats["total_jobs"] += 1
        self.stats["active_agents"] = len(self.jobs)
        logger.info(f"[Cron] {agent_id} registered '{task_name}' every {interval_seconds}s")
        return job

    def register_defaults(self, agent_id: str, handlers: dict):
        """Register standard agent crons:
        - heartbeat: 60s (keep-alive)
        - capability_sync: 300s (update marketplace)
        - health_check: 120s (self-diagnostic)
        - accounting: 600s (cheque/settlement)
        """
        defaults = [
            ("heartbeat", 60.0, handlers.get("heartbeat")),
            ("capability_sync", 300.0, handlers.get("capability_sync")),
            ("health_check", 120.0, handlers.get("health_check")),
            ("accounting", 600.0, handlers.get("accounting")),
        ]
        for name, interval, handler in defaults:
            if handler is not None:
                self.register(agent_id, name, interval, handler)

    def unregister(self, agent_id: str, task_name: Optional[str] = None):
        """Remove cron jobs for an agent. If task_name given, remove only that task."""
        if agent_id not in self.jobs:
            return
        if task_name is None:
            removed = len(self.jobs.pop(agent_id))
            self.stats["total_jobs"] -= removed
            self.stats["active_agents"] = len(self.jobs)
            logger.info(f"[Cron] {agent_id}: all {removed} jobs removed")
        else:
            before = len(self.jobs[agent_id])
            self.jobs[agent_id] = [j for j in self.jobs[agent_id]
                                   if j.task_name != task_name]
            after = len(self.jobs[agent_id])
            self.stats["total_jobs"] -= (before - after)
            if not self.jobs[agent_id]:
                del self.jobs[agent_id]
                self.stats["active_agents"] = len(self.jobs)
            logger.info(f"[Cron] {agent_id}/{task_name}: removed")

    # ── Execution ─────────────────────────────────────

    def tick(self) -> dict:
        """Check and execute due cron jobs. Call from main loop (every ~1s).

        Returns summary: {agent_id: {task_name: "ok"/"fail", ...}, ...}
        """
        results: dict[str, dict[str, str]] = {}
        now = time.time()

        for agent_id in list(self.jobs.keys()):
            agent_results = {}
            for job in self.jobs.get(agent_id, []):
                if not job.is_due:
                    continue
                try:
                    job.handler()
                    job.mark_run()
                    self.stats["total_runs"] += 1
                    agent_results[job.task_name] = "ok"
                except Exception as e:
                    job.mark_fail()
                    self.stats["total_fails"] += 1
                    agent_results[job.task_name] = f"fail:{e}"
                    logger.error(f"[Cron] {agent_id}/{job.task_name} FAIL: {e}")

            if agent_results:
                results[agent_id] = agent_results

        return results

    def tick_sync(self, max_iterations: int = 100) -> dict:
        """Synchronous tick: run until no more due jobs or max iterations."""
        total = {}
        for _ in range(max_iterations):
            batch = self.tick()
            if not batch:
                break
            total.update(batch)
        return total

    # ── Query ─────────────────────────────────────────

    def get_agent_crons(self, agent_id: str) -> list[dict]:
        """Return all cron jobs for an agent."""
        return [
            {
                "task_name": j.task_name,
                "interval": j.interval_seconds,
                "last_run": j.last_run,
                "enabled": j.enabled,
                "fail_count": j.fail_count,
                "is_due": j.is_due,
            }
            for j in self.jobs.get(agent_id, [])
        ]

    def get_all_crons(self) -> dict[str, list[dict]]:
        """Return all cron jobs across all agents."""
        return {aid: self.get_agent_crons(aid) for aid in self.jobs}

    def get_due_crons(self) -> dict[str, list[dict]]:
        """Return only due cron jobs."""
        return {
            aid: [cr for cr in crons if cr["is_due"]]
            for aid, crons in self.get_all_crons().items()
            if any(cr["is_due"] for cr in crons)
        }

    def get_stats(self) -> dict:
        """Return scheduler statistics with active agent count."""
        return {**self.stats, "active_agents": len(self.jobs)}

    def reset_stats(self):
        """Reset runtime statistics."""
        self.stats = {
            "total_jobs": sum(len(jobs) for jobs in self.jobs.values()),
            "total_runs": 0,
            "total_fails": 0,
            "active_agents": len(self.jobs),
        }


# ─── Built-in Cron Handlers ────────────────────────────────

def make_heartbeat_handler(agent_id: str, process_func=None):
    """Factory: handler that calls process_heartbeat from first_contact."""
    def _handler():
        from first_contact import process_heartbeat
        func = process_func if process_func else process_heartbeat
        result = func(agent_id, {})
        logger.debug(f"[Cron] {agent_id} heartbeat: {result.get('status', '?')}")
    return _handler


def make_capability_sync_handler(agent_id: str, caps: list, process_func=None):
    """Factory: handler that re-registers capabilities in marketplace."""
    def _handler():
        from first_contact import register_capabilities, build_kind39004_event
        result = register_capabilities(agent_id, caps)
        event = build_kind39004_event(agent_id, caps)
        logger.debug(f"[Cron] {agent_id} capability_sync: {len(caps)} caps, event kind={event.get('kind')}")
    return _handler


def make_health_check_handler(agent_id: str, check_func=None):
    """Factory: handler that performs a self-health check."""
    def _handler():
        ok = check_func() if check_func else True
        from first_contact import build_kind39005_event
        status = "alive" if ok else "degraded"
        event = build_kind39005_event(agent_id, status)
        logger.debug(f"[Cron] {agent_id} health_check: {status}")
    return _handler


def make_accounting_handler(agent_id: str, cheque_book=None):
    """Factory: handler that does periodic accounting/cheque settlement."""
    def _handler():
        if cheque_book:
            from cheque_mesh import reconcile_cheque_book
            result = reconcile_cheque_book(agent_id, cheque_book)
            logger.debug(f"[Cron] {agent_id} accounting: settled={result.get('settled', 0)}")
    return _handler


# ─── Singleton ────────────────────────────────────────────

_scheduler: Optional[CronScheduler] = None


def get_cron_scheduler() -> CronScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = CronScheduler()
    return _scheduler

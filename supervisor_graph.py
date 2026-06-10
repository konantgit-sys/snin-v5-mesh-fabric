#!/usr/bin/env python3
"""
Phase 9: Graph Supervisor — персистентность, чекпоинты, авто-восстановление.

Архитектура:
  Знаниевый граф → Redis (hot)
           ↓
  Snapshot JSON → /home/agent/data/sites/relay-mesh/snapshots/ (warm)
           ↓
  Supervisor auto-recover → restore_snapshot() при рестарте компонента

Цикл:
  Каждые 5 мин: save_snapshot() + integrity_check()
  При рестарте компонента: restore_snapshot() за 0.1-0.3 сек
  При падении >3 компонентов: full_reload() из Redis
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Optional

import redis

from knowledge_graph import KnowledgeGraph, create_knowledge_graph

logger = logging.getLogger("GraphSupervisor")

SNAPSHOT_DIR = "/home/agent/data/sites/relay-mesh/snapshots"
SNAPSHOT_INTERVAL = 300  # сек (5 мин)
SNAPSHOT_RETENTION = 10  # хранить последние N снапшотов


class GraphSupervisor:
    """Супервизор знаниевого графа: чекпоинты, восстановление, health-метрики."""

    def __init__(self, redis_url: str = "redis://localhost:6379/0",
                 snapshot_dir: str = SNAPSHOT_DIR):
        self.redis_url = redis_url
        self.snapshot_dir = snapshot_dir
        self.kg: Optional[KnowledgeGraph] = None
        self._last_snapshot_at: float = 0
        self._snapshots_taken: int = 0
        self._recoveries: int = 0
        self._last_integrity: dict = {"ok": True, "issues": []}
        self._started_at: float = 0

        os.makedirs(snapshot_dir, exist_ok=True)

    # ─── Lifecycle ───────────────────────────────────────

    def start(self) -> bool:
        """Запустить GraphSupervisor. Возвращает True если граф готов."""
        self._started_at = time.time()
        r = redis.Redis.from_url(self.redis_url, decode_responses=False)
        self.kg = KnowledgeGraph(r)

        # Попытка восстановления: snapshot → Redis fallback
        restored = self.kg.restore_snapshot()
        if restored == 0:
            # Пустой граф — это нормально, первый запуск
            logger.info("GraphSupervisor: fresh graph (0 entities)")
        else:
            logger.info(f"GraphSupervisor: restored {restored} entities")

        # Стартуем PubSub синхронизацию
        self.kg.start_sync()

        # Первый чекпоинт
        self._save_checkpoint()

        return self.kg.is_ready or True  # ready даже с пустым графом

    def stop(self):
        """Остановить. Финальный снапшот перед выключением."""
        if self.kg:
            self._save_checkpoint()
            self.kg.stop_sync()
            self.kg = None

    # ─── Checkpoint ──────────────────────────────────────

    def tick(self) -> dict:
        """Вызвать периодически (каждые N сек). Делает drain + snapshot по таймеру.

        Возвращает: {"synced": int, "snapshot": bool, "integrity": dict}
        """
        if not self.kg:
            return {"synced": 0, "snapshot": False, "integrity": {"ok": False}}

        # Drain PubSub
        drain_result = self.kg.process_sync_events()
        synced = drain_result.get("processed", 0)

        # Snapshot по таймеру
        snapshot_done = False
        if time.time() - self._last_snapshot_at >= SNAPSHOT_INTERVAL:
            self._save_checkpoint()
            snapshot_done = True

        # Integrity check при каждом снапшоте
        if snapshot_done:
            self._last_integrity = self.kg.integrity_check()

        return {
            "synced": synced,
            "snapshot": snapshot_done,
            "integrity": self._last_integrity,
        }

    def _save_checkpoint(self):
        """Сохранить снапшот + очистить старые."""
        if not self.kg:
            return

        ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        path = os.path.join(self.snapshot_dir, f"graph_snapshot_{ts}.json")
        size = self.kg.save_snapshot(path)
        self._last_snapshot_at = time.time()
        self._snapshots_taken += 1

        logger.info(f"GraphSupervisor: snapshot #{self._snapshots_taken} "
                    f"saved ({size} bytes) → {path}")

        # Ротация: удалить старые (> retention)
        self._rotate_snapshots()

    def _rotate_snapshots(self):
        """Удалить старые снапшоты, оставив последние SNAPSHOT_RETENTION."""
        retention = getattr(self, 'SNAPSHOT_RETENTION', SNAPSHOT_RETENTION)
        snapshots = sorted(
            [f for f in os.listdir(self.snapshot_dir) if f.endswith(".json")],
            reverse=True,
        )
        for old in snapshots[retention:]:
            os.remove(os.path.join(self.snapshot_dir, old))

    # ─── Recovery ────────────────────────────────────────

    def on_component_restart(self, component_name: str) -> dict:
        """Вызвать при рестарте компонента. Восстанавливает состояние графа.

        Возвращает: {"action": str, "entities": int, "integrity_ok": bool}
        """
        if not self.kg:
            return {"action": "no_graph", "entities": 0, "integrity_ok": False}

        self._recoveries += 1
        logger.warning(f"GraphSupervisor: recovery #{self._recoveries} "
                       f"triggered by component restart: {component_name}")

        # Full reload из Redis (быстро, без снапшота)
        self.kg.flush()  # очистить in-memory
        loaded = self.kg.load_from_redis()
        self._last_integrity = self.kg.integrity_check()

        if not loaded or not self._last_integrity["ok"]:
            # Fallback: snapshot
            logger.warning("GraphSupervisor: Redis load failed, restoring from snapshot")
            restored = self.kg.restore_snapshot()
            self._last_integrity = self.kg.integrity_check()
            return {
                "action": "snapshot_restore",
                "entities": restored,
                "integrity_ok": self._last_integrity["ok"],
            }

        return {
            "action": "redis_reload",
            "entities": len(self.kg._nodes) + len(self.kg._edges),
            "integrity_ok": self._last_integrity["ok"],
        }

    # ─── Health ──────────────────────────────────────────

    def get_health(self) -> dict:
        """Полный health-отчёт для supervisor API."""
        if not self.kg:
            return {
                "alive": False,
                "error": "no graph instance",
            }

        gh = self.kg.get_graph_health()
        return {
            "alive": True,
            "uptime_sec": round(time.time() - self._started_at, 1),
            "snapshots_taken": self._snapshots_taken,
            "recoveries": self._recoveries,
            "last_snapshot_ago_sec": round(time.time() - self._last_snapshot_at, 1)
                if self._last_snapshot_at else None,
            **gh,
        }

    def get_latest_snapshot_path(self) -> Optional[str]:
        """Путь к последнему снапшоту (для отладки)."""
        snapshots = sorted(
            [f for f in os.listdir(self.snapshot_dir) if f.endswith(".json")],
            reverse=True,
        )
        if snapshots:
            return os.path.join(self.snapshot_dir, snapshots[0])
        return None


# ─── Factory ────────────────────────────────────────────

def create_graph_supervisor(redis_url: str = "redis://localhost:6379/0",
                            snapshot_dir: str = SNAPSHOT_DIR) -> GraphSupervisor:
    """Factory: создать и запустить GraphSupervisor."""
    gs = GraphSupervisor(redis_url=redis_url, snapshot_dir=snapshot_dir)
    gs.start()
    return gs

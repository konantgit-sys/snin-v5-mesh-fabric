"""
Agent SLA — Service Level Agreements for SNIN agents (#2)

Each agent has SLA commitments tracked by the mesh:
  1. Response deadline (max time to respond to queries)
  2. Uptime commitment (minimum online percentage)
  3. Throughput commitment (events processed per block)

Violations trigger:
  → Warning (1-2 violations): agent notified
  → Downgrade (3-5 violations): trust score reduced, restricted access
  → Eviction (6+ violations): removed from mesh, probation period

Recovery path:
  → Probation (7 days): re-apply with verified SLA
  → Re-entry after probation: start at base trust score
"""

import json
import time
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger("agent_sla")

# ─── SLA Configuration ───
DEFAULT_RESPONSE_DEADLINE = 10.0    # seconds
DEFAULT_UPTIME_COMMITMENT = 0.95    # 95%
DEFAULT_THROUGHPUT_MIN = 1.0        # events/sec minimum

# Violation thresholds
WARNING_THRESHOLD = 2       # violations before warning
DOWNGRADE_THRESHOLD = 5     # violations before trust downgrade
EVICTION_THRESHOLD = 8      # violations before eviction
PROBATION_DAYS = 7          # days of probation after eviction

# Trust impact
DOWNGRADE_FACTOR = 0.5      # multiply trust score by this on downgrade
BASE_TRUST_ON_REENTRY = 0.3 # trust score after probation

# Tracking window
VIOLATION_WINDOW_HOURS = 24  # violations reset after this many hours

DB_PATH = Path.home() / "data" / "sites" / "relay-mesh" / "agent_sla.db"


class AgentSLADB:
    """SQLite store for SLA commitments and violation tracking."""

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sla_commitments (
                    agent_id TEXT PRIMARY KEY,
                    response_deadline REAL NOT NULL DEFAULT 10.0,
                    uptime_commitment REAL NOT NULL DEFAULT 0.95,
                    throughput_min REAL NOT NULL DEFAULT 1.0,
                    registered_at TEXT NOT NULL,
                    updated_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sla_violations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    agent_id TEXT NOT NULL,
                    violation_type TEXT NOT NULL,
                    detail TEXT,
                    timestamp REAL NOT NULL,
                    block_interval REAL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sla_state (
                    agent_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'active',
                    violations_count INTEGER DEFAULT 0,
                    trust_score REAL DEFAULT 1.0,
                    last_violation_at REAL,
                    probation_until REAL,
                    downgraded_at REAL,
                    evicted_at REAL
                )
            """)
            conn.commit()

    # ═══ SLA Registration ═══

    def register_agent(self, agent_id: str, response_deadline=None,
                       uptime_commitment=None, throughput_min=None):
        now = datetime.utcnow().isoformat()
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO sla_commitments
                (agent_id, response_deadline, uptime_commitment, throughput_min,
                 registered_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                agent_id,
                response_deadline or DEFAULT_RESPONSE_DEADLINE,
                uptime_commitment or DEFAULT_UPTIME_COMMITMENT,
                throughput_min or DEFAULT_THROUGHPUT_MIN,
                now, now
            ))
            conn.execute("""
                INSERT OR IGNORE INTO sla_state(agent_id, status)
                VALUES (?, 'active')
            """, (agent_id,))
            conn.commit()

    def get_commitment(self, agent_id: str) -> Optional[dict]:
        with sqlite3.connect(str(self.db_path)) as conn:
            row = conn.execute(
                "SELECT * FROM sla_commitments WHERE agent_id=?",
                (agent_id,)
            ).fetchone()
            if not row:
                return None
            return {
                "agent_id": row[0],
                "response_deadline": row[1],
                "uptime_commitment": row[2],
                "throughput_min": row[3],
                "registered_at": row[4],
                "updated_at": row[5],
            }

    # ═══ Violation tracking ═══

    def record_violation(self, agent_id: str, violation_type: str,
                         detail: str = "", block_interval: float = 0):
        """Record a violation and update agent state."""
        now = time.time()
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute(
                "INSERT INTO sla_violations(agent_id, violation_type, detail, timestamp, block_interval) "
                "VALUES (?, ?, ?, ?, ?)",
                (agent_id, violation_type, detail, now, block_interval)
            )

            # Count recent violations (within window)
            cutoff = now - (VIOLATION_WINDOW_HOURS * 3600)
            row = conn.execute(
                "SELECT COUNT(*) FROM sla_violations WHERE agent_id=? AND timestamp > ?",
                (agent_id, cutoff)
            ).fetchone()
            recent_count = row[0] if row else 0

            conn.execute(
                "UPDATE sla_state SET violations_count=?, last_violation_at=? WHERE agent_id=?",
                (recent_count, now, agent_id)
            )
            conn.commit()

        return recent_count

    def get_violations(self, agent_id: str, hours=VIOLATION_WINDOW_HOURS) -> list:
        cutoff = time.time() - (hours * 3600)
        with sqlite3.connect(str(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT * FROM sla_violations WHERE agent_id=? AND timestamp > ? ORDER BY timestamp DESC",
                (agent_id, cutoff)
            ).fetchall()
            return [
                {
                    "id": r[0], "agent_id": r[1],
                    "violation_type": r[2], "detail": r[3],
                    "timestamp": r[4], "block_interval": r[5],
                }
                for r in rows
            ]

    # ═══ State management ═══

    def get_state(self, agent_id: str) -> Optional[dict]:
        with sqlite3.connect(str(self.db_path)) as conn:
            row = conn.execute(
                "SELECT * FROM sla_state WHERE agent_id=?", (agent_id,)
            ).fetchone()
            if not row:
                return None
            return {
                "agent_id": row[0],
                "status": row[1],
                "violations_count": row[2],
                "trust_score": row[3],
                "last_violation_at": row[4],
                "probation_until": row[5],
                "downgraded_at": row[6],
                "evicted_at": row[7],
            }

    def set_downgraded(self, agent_id: str):
        now = time.time()
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute(
                "UPDATE sla_state SET status='downgraded', "
                "trust_score=trust_score*?, downgraded_at=?, last_violation_at=? "
                "WHERE agent_id=?",
                (DOWNGRADE_FACTOR, now, now, agent_id)
            )
            conn.commit()

    def set_evicted(self, agent_id: str):
        now = time.time()
        probation_until = now + (PROBATION_DAYS * 86400)
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute(
                "UPDATE sla_state SET status='evicted', trust_score=0.0, "
                "evicted_at=?, probation_until=?, last_violation_at=? "
                "WHERE agent_id=?",
                (now, probation_until, now, agent_id)
            )
            conn.commit()
        logger.warning(f"Agent {agent_id[:16]}... EVICTED — probation until "
                      f"{datetime.fromtimestamp(probation_until).isoformat()}")

    def set_warned(self, agent_id: str):
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute(
                "UPDATE sla_state SET status='warned', last_violation_at=? WHERE agent_id=?",
                (time.time(), agent_id)
            )
            conn.commit()

    def check_probation(self, agent_id: str) -> bool:
        """Check if agent's probation is over. Returns True if they can re-enter."""
        state = self.get_state(agent_id)
        if not state or state["status"] != "evicted":
            return False
        if state.get("probation_until") and time.time() > state["probation_until"]:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute(
                    "UPDATE sla_state SET status='probation_complete', "
                    "trust_score=? WHERE agent_id=?",
                    (BASE_TRUST_ON_REENTRY, agent_id)
                )
                conn.commit()
            logger.info(f"Agent {agent_id[:16]}... probation complete — can re-enter")
            return True
        return False

    def get_all_agents(self) -> list:
        with sqlite3.connect(str(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT s.agent_id, s.status, s.violations_count, s.trust_score, "
                "c.response_deadline, c.uptime_commitment, c.throughput_min "
                "FROM sla_state s LEFT JOIN sla_commitments c ON s.agent_id=c.agent_id"
            ).fetchall()
            return [
                {
                    "agent_id": r[0], "status": r[1],
                    "violations": r[2], "trust": r[3],
                    "deadline": r[4], "uptime": r[5], "throughput": r[6],
                }
                for r in rows
            ]


class AgentSLAMonitor:
    """Monitors agent performance and enforces SLA terms."""

    def __init__(self, db=None):
        self.db = db or AgentSLADB()

    def check_response_time(self, agent_id: str, response_time: float) -> Optional[str]:
        """
        Check if agent's response time violates SLA.
        Returns: None (ok), or violation type string.
        """
        commitment = self.db.get_commitment(agent_id)
        if not commitment:
            self.db.register_agent(agent_id)
            return None

        state = self.db.get_state(agent_id)
        if state and state["status"] in ("evicted",):
            return None  # Already evicted

        deadline = commitment["response_deadline"]
        if response_time > deadline:
            count = self.db.record_violation(
                agent_id, "response_timeout",
                f"Response took {response_time:.1f}s, deadline={deadline:.1f}s",
                block_interval=response_time
            )
            self._enforce(agent_id, count)
            return "response_timeout"

        return None

    def check_throughput(self, agent_id: str, events_per_sec: float) -> Optional[str]:
        commitment = self.db.get_commitment(agent_id)
        if not commitment:
            return None

        state = self.db.get_state(agent_id)
        if state and state["status"] in ("evicted",):
            return None

        if events_per_sec < commitment["throughput_min"]:
            count = self.db.record_violation(
                agent_id, "low_throughput",
                f"Throughput {events_per_sec:.1f} ev/s, min={commitment['throughput_min']:.1f}",
                block_interval=0
            )
            self._enforce(agent_id, count)
            return "low_throughput"

        return None

    def check_uptime(self, agent_id: str, uptime_ratio: float) -> Optional[str]:
        commitment = self.db.get_commitment(agent_id)
        if not commitment:
            return None

        state = self.db.get_state(agent_id)
        if state and state["status"] in ("evicted",):
            return None

        if uptime_ratio < commitment["uptime_commitment"]:
            count = self.db.record_violation(
                agent_id, "low_uptime",
                f"Uptime {uptime_ratio:.1%}, committed={commitment['uptime_commitment']:.1%}",
                block_interval=0
            )
            self._enforce(agent_id, count)
            return "low_uptime"

        return None

    def _enforce(self, agent_id: str, violation_count: int):
        """Apply enforcement based on violation count."""
        if violation_count >= EVICTION_THRESHOLD:
            self.db.set_evicted(agent_id)
            logger.warning(f"⚡ {agent_id[:16]}... evicted — {violation_count} violations")
        elif violation_count >= DOWNGRADE_THRESHOLD:
            self.db.set_downgraded(agent_id)
            logger.info(f"📉 {agent_id[:16]}... downgraded — {violation_count} violations")
        elif violation_count >= WARNING_THRESHOLD:
            self.db.set_warned(agent_id)
            logger.info(f"⚠️ {agent_id[:16]}... warned — {violation_count} violations")

    def get_sla_report(self) -> dict:
        agents = self.db.get_all_agents()
        active = sum(1 for a in agents if a["status"] == "active")
        warned = sum(1 for a in agents if a["status"] == "warned")
        downgraded = sum(1 for a in agents if a["status"] == "downgraded")
        evicted = sum(1 for a in agents if a["status"] == "evicted")
        probation = sum(1 for a in agents if a["status"] == "probation_complete")

        return {
            "total": len(agents),
            "active": active,
            "warned": warned,
            "downgraded": downgraded,
            "evicted": evicted,
            "probation_complete": probation,
            "agents": agents,
        }

    def process_recovery(self) -> int:
        """Check all evicted agents for probation expiry. Returns count of recovered."""
        recovered = 0
        agents = self.db.get_all_agents()
        for agent in agents:
            if agent["status"] == "evicted":
                if self.db.check_probation(agent["agent_id"]):
                    recovered += 1
        return recovered

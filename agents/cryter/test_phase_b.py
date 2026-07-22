#!/usr/bin/env python3
"""
Tests for Phase B modules:
  - adaptive_scheduler.py (#6 Dynamic Block Intervals)
  - agent_sla.py (#2 Agent SLA)
"""

import os
import sys
import json
import time
import math
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


# ═══════════════════════════════════════════════════════════
# AdaptiveScheduler Tests (#6)
# ═══════════════════════════════════════════════════════════

class TestAdaptiveScheduler(unittest.TestCase):
    def setUp(self):
        from core.adaptive_scheduler import AdaptiveBlockScheduler
        self.tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        self.tmp.close()
        self.scheduler = AdaptiveBlockScheduler(state_path=Path(self.tmp.name))

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_initial_interval_is_base(self):
        self.assertEqual(self.scheduler.current_interval, 60.0)

    def test_calculate_interval_idle(self):
        self.scheduler.ema_rate = 0.0
        interval = self.scheduler._calculate_interval()
        self.assertAlmostEqual(interval, 300.0, delta=5.0)

    def test_calculate_interval_peak(self):
        self.scheduler.ema_rate = 100.0
        interval = self.scheduler._calculate_interval()
        self.assertAlmostEqual(interval, 10.0, delta=1.0)

    def test_calculate_interval_mid(self):
        self.scheduler.ema_rate = 25.0
        interval = self.scheduler._calculate_interval()
        self.assertGreater(interval, 10.0)
        self.assertLess(interval, 300.0)

    def test_calculate_interval_monotonic(self):
        """Higher rate → lower or equal interval."""
        self.scheduler.ema_rate = 5.0
        low = self.scheduler._calculate_interval()
        self.scheduler.ema_rate = 40.0
        high = self.scheduler._calculate_interval()
        self.assertLess(high, low)

    def test_force_interval(self):
        self.scheduler.force_interval(120.0)
        self.assertEqual(self.scheduler.current_interval, 120.0)

    def test_force_interval_clamped(self):
        self.scheduler.force_interval(1000.0)
        self.assertLessEqual(self.scheduler.current_interval, 300.0)
        self.scheduler.force_interval(1.0)
        self.assertGreaterEqual(self.scheduler.current_interval, 10.0)

    def test_get_stats(self):
        stats = self.scheduler.get_stats()
        self.assertIn("ema_rate", stats)
        self.assertIn("current_interval", stats)
        self.assertIn("direction", stats)
        self.assertEqual(stats["direction"], "steady")

    def test_state_persistence(self):
        self.scheduler.ema_rate = 42.0
        self.scheduler.current_interval = 133.0
        self.scheduler.direction = "speeding_up"
        self.scheduler._save_state()

        # Reload
        from core.adaptive_scheduler import AdaptiveBlockScheduler
        s2 = AdaptiveBlockScheduler(state_path=Path(self.tmp.name))
        self.assertEqual(s2.ema_rate, 42.0)
        self.assertEqual(s2.current_interval, 133.0)
        self.assertEqual(s2.direction, "speeding_up")

    def test_hysteresis_prevents_bounce(self):
        # Simulate small rate change — interval shouldn't jump
        self.scheduler.ema_rate = 25.0
        self.scheduler.current_interval = 50.0
        self.scheduler.direction = "steady"
        self.scheduler._save_state()

        # Simulate update with nearly same rate
        instant_rate = 24.5
        # Override _get_event_rate to return instant_rate
        old_get = self.scheduler._get_event_rate
        self.scheduler._get_event_rate = lambda: instant_rate
        try:
            new_interval = self.scheduler.update()
            # Should stay close to 50 (within hysteresis margin)
            margin = self.scheduler.current_interval * 0.15
            self.assertAlmostEqual(new_interval, 50.0, delta=margin + 5)
        finally:
            self.scheduler._get_event_rate = old_get


# ═══════════════════════════════════════════════════════════
# Agent SLA Tests (#2)
# ═══════════════════════════════════════════════════════════

class TestAgentSLADB(unittest.TestCase):
    def setUp(self):
        from core.agent_sla import AgentSLADB
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.db = AgentSLADB(Path(self.tmp.name))

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_register_agent(self):
        self.db.register_agent("agent_1", response_deadline=5.0)
        commitment = self.db.get_commitment("agent_1")
        self.assertIsNotNone(commitment)
        self.assertEqual(commitment["response_deadline"], 5.0)
        self.assertEqual(commitment["uptime_commitment"], 0.95)
        self.assertEqual(commitment["throughput_min"], 1.0)

    def test_register_defaults(self):
        self.db.register_agent("agent_2")
        commitment = self.db.get_commitment("agent_2")
        self.assertEqual(commitment["response_deadline"], 10.0)
        self.assertEqual(commitment["uptime_commitment"], 0.95)

    def test_get_nonexistent(self):
        self.assertIsNone(self.db.get_commitment("ghost"))

    def test_record_violation(self):
        self.db.register_agent("agent_3")
        count = self.db.record_violation("agent_3", "response_timeout",
                                         "Took 15s, deadline=10s")
        self.assertEqual(count, 1)

    def test_multiple_violations(self):
        self.db.register_agent("agent_4")
        for i in range(5):
            self.db.record_violation("agent_4", "low_throughput",
                                    f"violation #{i}")
        state = self.db.get_state("agent_4")
        self.assertEqual(state["violations_count"], 5)

    def test_downgrade(self):
        self.db.register_agent("agent_5")
        self.db.set_downgraded("agent_5")
        state = self.db.get_state("agent_5")
        self.assertEqual(state["status"], "downgraded")
        self.assertLess(state["trust_score"], 1.0)

    def test_eviction_and_probation(self):
        self.db.register_agent("agent_6")
        self.db.set_evicted("agent_6")
        state = self.db.get_state("agent_6")
        self.assertEqual(state["status"], "evicted")
        self.assertEqual(state["trust_score"], 0.0)
        self.assertIsNotNone(state["probation_until"])

    def test_probation_not_expired(self):
        self.db.register_agent("agent_7")
        self.db.set_evicted("agent_7")
        recovered = self.db.check_probation("agent_7")
        self.assertFalse(recovered)  # Just evicted, probation not over

    def test_get_all_agents(self):
        self.db.register_agent("a1")
        self.db.register_agent("a2")
        agents = self.db.get_all_agents()
        self.assertEqual(len(agents), 2)

    def test_get_violations_empty(self):
        self.db.register_agent("agent_clean")
        violations = self.db.get_violations("agent_clean")
        self.assertEqual(len(violations), 0)


class TestAgentSLAMonitor(unittest.TestCase):
    def setUp(self):
        from core.agent_sla import AgentSLADB, AgentSLAMonitor
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.db = AgentSLADB(Path(self.tmp.name))
        self.monitor = AgentSLAMonitor(self.db)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_response_time_ok(self):
        self.db.register_agent("fast_agent", response_deadline=10.0)
        result = self.monitor.check_response_time("fast_agent", 5.0)
        self.assertIsNone(result)

    def test_response_time_violation(self):
        self.db.register_agent("slow_agent", response_deadline=2.0)
        result = self.monitor.check_response_time("slow_agent", 8.0)
        self.assertEqual(result, "response_timeout")

    def test_throughput_ok(self):
        self.db.register_agent("busy_agent", throughput_min=0.5)
        result = self.monitor.check_throughput("busy_agent", 2.0)
        self.assertIsNone(result)

    def test_throughput_violation(self):
        self.db.register_agent("idle_agent", throughput_min=5.0)
        result = self.monitor.check_throughput("idle_agent", 0.1)
        self.assertEqual(result, "low_throughput")

    def test_uptime_violation(self):
        self.db.register_agent("down_agent", uptime_commitment=0.99)
        result = self.monitor.check_uptime("down_agent", 0.50)
        self.assertEqual(result, "low_uptime")

    def test_evicted_agent_not_checked(self):
        self.db.register_agent("evicted_1")
        self.db.set_evicted("evicted_1")
        result = self.monitor.check_response_time("evicted_1", 99.0)
        self.assertIsNone(result)  # Already evicted, don't re-check

    def test_auto_register_on_check(self):
        result = self.monitor.check_response_time("new_agent", 5.0)
        self.assertIsNone(result)
        commitment = self.db.get_commitment("new_agent")
        self.assertIsNotNone(commitment)

    def test_enforce_warning(self):
        self.db.register_agent("warn_agent", response_deadline=1.0)
        for i in range(3):
            self.monitor.check_response_time("warn_agent", 10.0)
        state = self.db.get_state("warn_agent")
        self.assertEqual(state["status"], "warned")

    def test_enforce_downgrade(self):
        self.db.register_agent("dg_agent", response_deadline=1.0)
        for i in range(6):
            self.monitor.check_response_time("dg_agent", 10.0)
        state = self.db.get_state("dg_agent")
        self.assertEqual(state["status"], "downgraded")

    def test_enforce_eviction(self):
        self.db.register_agent("ev_agent", response_deadline=0.1)
        for i in range(9):
            self.monitor.check_response_time("ev_agent", 10.0)
        state = self.db.get_state("ev_agent")
        self.assertEqual(state["status"], "evicted")

    def test_sla_report(self):
        self.db.register_agent("r1")
        self.db.register_agent("r2")
        report = self.monitor.get_sla_report()
        self.assertEqual(report["total"], 2)
        self.assertEqual(report["active"], 2)

    def test_recovery_after_probation(self):
        self.db.register_agent("recover_agent")
        self.db.set_evicted("recover_agent")
        # Manually expire probation
        with __import__('sqlite3').connect(self.tmp.name) as conn:
            conn.execute(
                "UPDATE sla_state SET probation_until=? WHERE agent_id=?",
                (time.time() - 1, "recover_agent")
            )
            conn.commit()
        recovered = self.monitor.process_recovery()
        self.assertEqual(recovered, 1)
        state = self.db.get_state("recover_agent")
        self.assertEqual(state["status"], "probation_complete")
        self.assertAlmostEqual(state["trust_score"], 0.3)


if __name__ == "__main__":
    unittest.main(verbosity=2)

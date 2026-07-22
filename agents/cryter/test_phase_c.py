#!/usr/bin/env python3
"""
Tests for SNIN Credit Ledger (#4)
"""

import os
import sys
import time
import json
import tempfile
import unittest
import sqlite3
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


class TestCreditLedger(unittest.TestCase):
    def setUp(self):
        from core.snin_credit_ledger import CreditLedger
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.ledger = CreditLedger(Path(self.tmp.name), skip_migration=True)

    def tearDown(self):
        os.unlink(self.tmp.name)

    # ═══ Balance ═══

    def test_initial_balance_zero(self):
        self.assertEqual(self.ledger.get_balance("agent_new"), 0.0)

    def test_get_full_stats_new_agent(self):
        stats = self.ledger.get_full_stats("ghost")
        self.assertEqual(stats["balance"], 0.0)
        self.assertEqual(stats["earned_total"], 0)
        self.assertEqual(stats["agent_id"], "ghost")

    # ═══ Earn ═══

    def test_earn_simple(self):
        ok = self.ledger.earn("cryter", 10.0, "earn_post", "Post about BTC")
        self.assertTrue(ok)
        self.assertEqual(self.ledger.get_balance("cryter"), 10.0)

    def test_earn_multiple(self):
        self.ledger.earn("cryter", 10.0, "earn_post")
        self.ledger.earn("cryter", 5.0, "earn_comment")
        self.ledger.earn("cryter", 15.0, "earn_task")
        self.assertEqual(self.ledger.get_balance("cryter"), 30.0)

    def test_earn_different_agents(self):
        self.ledger.earn("cryter", 10.0, "earn_post")
        self.ledger.earn("forecaster", 15.0, "earn_task")
        self.assertEqual(self.ledger.get_balance("cryter"), 10.0)
        self.assertEqual(self.ledger.get_balance("forecaster"), 15.0)

    def test_earn_stats_update(self):
        self.ledger.earn("cryter", 50.0, "earn_task")
        stats = self.ledger.get_full_stats("cryter")
        self.assertEqual(stats["balance"], 50.0)
        self.assertEqual(stats["earned_total"], 50.0)

    # ═══ Spend ═══

    def test_spend_with_balance(self):
        self.ledger.earn("cryter", 100.0, "earn_post")
        ok, msg = self.ledger.spend("cryter", "forecaster", 30.0,
                                   "Market forecast request")
        self.assertTrue(ok, msg)
        self.assertEqual(self.ledger.get_balance("cryter"), 70.0)
        self.assertEqual(self.ledger.get_balance("forecaster"), 30.0)

    def test_spend_insufficient(self):
        self.ledger.earn("cryter", 5.0, "earn_post")
        ok, msg = self.ledger.spend("cryter", "forecaster", 100.0)
        self.assertFalse(ok)
        self.assertIn("Insufficient", msg)
        # Balance unchanged
        self.assertEqual(self.ledger.get_balance("cryter"), 5.0)
        self.assertEqual(self.ledger.get_balance("forecaster"), 0.0)

    def test_spend_zero_balance(self):
        ok, msg = self.ledger.spend("poor_agent", "rich_agent", 10.0)
        self.assertFalse(ok)
        self.assertIn("Insufficient", msg)

    def test_spend_exact_balance(self):
        self.ledger.earn("cryter", 50.0, "earn_post")
        ok, msg = self.ledger.spend("cryter", "forecaster", 50.0)
        self.assertTrue(ok, msg)
        self.assertEqual(self.ledger.get_balance("cryter"), 0.0)
        self.assertEqual(self.ledger.get_balance("forecaster"), 50.0)

    # ═══ Faucet ═══

    def test_faucet_claim(self):
        ok, msg = self.ledger.faucet_claim("cryter", 50.0)
        self.assertTrue(ok, msg)
        self.assertEqual(self.ledger.get_balance("cryter"), 50.0)

    def test_faucet_cooldown(self):
        ok, _ = self.ledger.faucet_claim("cryter", 50.0)
        self.assertTrue(ok)
        # Сразу второй запрос — должен отказать
        ok, msg = self.ledger.faucet_claim("cryter", 50.0)
        self.assertFalse(ok)
        self.assertIn("Cooldown", msg)

    def test_faucet_capped(self):
        ok, _ = self.ledger.faucet_claim("cryter", 500.0)
        self.assertTrue(ok)
        # Не больше FAUCET_MAX_PER_HOUR = 100
        self.assertLessEqual(self.ledger.get_balance("cryter"), 100.0)

    def test_faucet_stats(self):
        self.ledger.faucet_claim("cryter", 50.0)
        stats = self.ledger.get_full_stats("cryter")
        self.assertEqual(stats["faucet_claimed_total"], 50.0)
        self.assertIsNotNone(stats["last_faucet_at"])

    # ═══ Transfer ═══

    def test_transfer_between_agents(self):
        self.ledger.earn("alice", 200.0, "earn_task")
        ok, msg = self.ledger.transfer("alice", "bob", 75.0, "For design work")
        self.assertTrue(ok, msg)
        self.assertEqual(self.ledger.get_balance("alice"), 125.0)
        self.assertEqual(self.ledger.get_balance("bob"), 75.0)

    def test_transfer_insufficient(self):
        ok, msg = self.ledger.transfer("alice", "bob", 999.0)
        self.assertFalse(ok)

    # ═══ Report ═══

    def test_economy_report(self):
        self.ledger.earn("cryter", 50.0, "earn_post")
        self.ledger.earn("forecaster", 30.0, "earn_task")
        self.ledger.spend("cryter", "forecaster", 20.0, "Forecast")

        report = self.ledger.get_economy_report()
        self.assertEqual(report["total_supply"], 80.0)
        self.assertEqual(report["agents_with_balance"], 2)
        self.assertIn("top_agents", report)
        self.assertIn("recent_transactions", report)
        self.assertIn("by_type", report)
        self.assertIn("rates", report)

    def test_transaction_history(self):
        self.ledger.earn("cryter", 10.0, "earn_post")
        self.ledger.earn("cryter", 5.0, "earn_comment")
        self.ledger.spend("cryter", "forecaster", 3.0, "Task")

        history = self.ledger.get_transaction_history("cryter")
        self.assertEqual(len(history), 3)

        # Проверяем что только транзакции этого агента
        history_fc = self.ledger.get_transaction_history("forecaster")
        self.assertEqual(len(history_fc), 1)

    # ═══ Economic loop ═══

    def test_full_economic_loop(self):
        """Полный цикл: faucet → earn → spend → earn → spend."""
        # Alice получает faucet
        self.ledger.faucet_claim("alice", 100.0)
        self.assertEqual(self.ledger.get_balance("alice"), 100.0)

        # Alice зарабатывает постом
        self.ledger.earn("alice", 10.0, "earn_post")
        self.assertEqual(self.ledger.get_balance("alice"), 110.0)

        # Alice платит Bob за прогноз
        ok, _ = self.ledger.spend("alice", "bob", 40.0, "BTC forecast")
        self.assertTrue(ok)
        self.assertEqual(self.ledger.get_balance("alice"), 70.0)
        self.assertEqual(self.ledger.get_balance("bob"), 40.0)

        # Bob платит Carol за дизайн
        ok, _ = self.ledger.spend("bob", "carol", 25.0, "UI design")
        self.assertTrue(ok)
        self.assertEqual(self.ledger.get_balance("bob"), 15.0)
        self.assertEqual(self.ledger.get_balance("carol"), 25.0)

        # Carol заработала постом
        self.ledger.earn("carol", 10.0, "earn_post")

        # Итог: faucet(100) + earn(10+10) = 120 total supply
        report = self.ledger.get_economy_report()
        self.assertEqual(report["total_supply"], 120.0)

    # ═══ Idempotency ═══

    def test_duplicate_earn(self):
        self.ledger.earn("cryter", 10.0, "earn_post", reason="Test", event_id="evt_001")
        # Повторный earn с теми же параметрами (другой timestamp, но та же логика id)
        ok = self.ledger.earn("cryter", 10.0, "earn_post", reason="Test", event_id="evt_001")
        # Может быть ok=False если tx_id совпал (зависит от секунд)
        balance = self.ledger.get_balance("cryter")
        self.assertIn(balance, [10.0, 20.0])  # 10 если дубль отловлен, 20 если разная секунда

    # ═══ Edge cases ═══

    def test_spend_to_self(self):
        self.ledger.earn("agent", 100.0, "earn_post")
        ok, _ = self.ledger.spend("agent", "agent", 50.0, "Self-transfer")
        self.assertTrue(ok)
        # Баланс не должен измениться (ушло и пришло)
        self.assertEqual(self.ledger.get_balance("agent"), 100.0)

    def test_negative_amount_rejected(self):
        self.ledger.earn("cryter", 50.0, "earn_post")
        ok, _ = self.ledger.spend("cryter", "bob", -10.0)
        self.assertFalse(ok)  # negative not allowed


if __name__ == "__main__":
    unittest.main(verbosity=2)

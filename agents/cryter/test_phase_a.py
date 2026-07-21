#!/usr/bin/env python3
"""
Tests for Phase A modules:
  - welcome_chain.py
  - content_grid.py
  - sovereignty_challenge.py
"""

import sys
import os
import json
import time
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


# ─── WelcomeChain Tests ───

class TestWelcomeChainDB(unittest.TestCase):
    def setUp(self):
        from core.welcome_chain import WelcomeChainDB
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.db = WelcomeChainDB(self.tmp.name)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_register_and_check(self):
        npub = "abc123def456"
        self.assertFalse(self.db.is_registered(npub))
        self.db.register(npub)
        self.assertTrue(self.db.is_registered(npub))

    def test_pending_stage_day0(self):
        npub = "test_npub_1"
        self.db.register(npub)
        stage, msg = self.db.get_pending_stage(npub)
        self.assertIsNotNone(stage)
        self.assertEqual(stage, "day_0")

    def test_pending_stage_day1(self):
        npub = "test_npub_2"
        self.db.register(npub)
        # Manually advance stage past day_0
        self.db.mark_sent(npub, "day_0")
        # Simulate time passing (1 day)
        with __import__('sqlite3').connect(self.tmp.name) as conn:
            conn.execute(
                "UPDATE onboard_state SET started_at=? WHERE npub=?",
                ((datetime.utcnow() - timedelta(days=2)).isoformat(), npub)
            )
            conn.commit()
        stage, msg = self.db.get_pending_stage(npub)
        self.assertEqual(stage, "day_1")

    def test_no_duplicate_send(self):
        npub = "test_npub_3"
        self.db.register(npub)
        self.db.mark_sent(npub, "day_0")
        self.db.mark_sent(npub, "day_1")
        self.db.mark_sent(npub, "day_3")
        self.db.mark_sent(npub, "day_7")
        # All stages sent — no pending
        result = self.db.get_pending_stage(npub)
        self.assertIsNone(result)

    def test_get_all_active(self):
        npub1, npub2 = "active1", "active2"
        self.db.register(npub1)
        self.db.register(npub2)
        active = self.db.get_all_active()
        self.assertEqual(len(active), 2)

    def test_record_dm(self):
        npub = "dm_test"
        self.db.register(npub)
        self.db.record_dm(npub, "recipient_hex", "day_0", "Welcome!")
        # Should not raise — verifying DM was recorded
        result = self.db.get_pending_stage(npub)
        self.assertIsNotNone(result)  # Day 0 should be pending

    def test_onboard_new_idempotent(self):
        from core.welcome_chain import WelcomeChain
        chain = WelcomeChain(None, self.db)
        npub = "idempotent_test"
        self.assertTrue(chain.db.is_registered(npub) is False)
        self.db.register(npub)
        self.assertTrue(chain.db.is_registered(npub))
        # Second register should not raise
        self.db.register(npub)


# ─── ContentGrid Tests ───

class TestContentGrid(unittest.TestCase):
    def test_monday_is_provocation(self):
        from core.content_grid import get_today_schedule
        # 2026-07-20 is Monday
        dt = datetime(2026, 7, 20)
        schedule = get_today_schedule(dt)
        self.assertIsNotNone(schedule)
        self.assertEqual(schedule["category"], "provocation")

    def test_wednesday_is_case_study(self):
        from core.content_grid import get_today_schedule
        dt = datetime(2026, 7, 22)  # Wednesday
        schedule = get_today_schedule(dt)
        self.assertIsNotNone(schedule)
        self.assertEqual(schedule["category"], "case_study")

    def test_friday_is_question(self):
        from core.content_grid import get_today_schedule
        dt = datetime(2026, 7, 24)  # Friday
        schedule = get_today_schedule(dt)
        self.assertIsNotNone(schedule)
        self.assertEqual(schedule["category"], "question")

    def test_sunday_is_build_log(self):
        from core.content_grid import get_today_schedule
        dt = datetime(2026, 7, 26)  # Sunday
        schedule = get_today_schedule(dt)
        self.assertIsNotNone(schedule)
        self.assertEqual(schedule["category"], "build_log")

    def test_tuesday_is_fallback(self):
        from core.content_grid import get_today_schedule
        dt = datetime(2026, 7, 21)  # Tuesday
        schedule = get_today_schedule(dt)
        self.assertIsNotNone(schedule)
        self.assertEqual(schedule["category"], "general")

    def test_get_schedule_instruction_returns_string(self):
        from core.content_grid import get_schedule_instruction
        dt = datetime(2026, 7, 20)  # Monday
        instruction = get_schedule_instruction(dt)
        self.assertIsInstance(instruction, str)
        self.assertTrue(len(instruction) > 20)

    def test_weekly_plan_has_7_days(self):
        from core.content_grid import get_weekly_plan
        plan = get_weekly_plan()
        self.assertEqual(len(plan), 7)

    def test_today(self):
        from core.content_grid import get_today_schedule
        # Today should always return something
        schedule = get_today_schedule()
        self.assertIsNotNone(schedule)
        self.assertIn("category", schedule)


# ─── SovereigntyChallenge Tests ───

class TestSovereigntyDB(unittest.TestCase):
    def setUp(self):
        from core.sovereignty_challenge import SovereigntyDB
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.db = SovereigntyDB(self.tmp.name)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_start_challenge(self):
        npub = "challenge_npub_1"
        self.db.start_challenge(npub)
        progress = self.db.get_progress(npub)
        self.assertIsNotNone(progress)
        self.assertEqual(progress["status"], "active")
        self.assertEqual(progress["stars_awarded"], 0)

    def test_record_intro_post(self):
        npub = "challenge_npub_2"
        self.db.start_challenge(npub)
        self.db.record_intro_post(npub, "event_001", 150)
        progress = self.db.get_progress(npub)
        self.assertTrue(progress["post_intro_done"])

    def test_intro_post_too_short(self):
        npub = "challenge_npub_3"
        self.db.start_challenge(npub)
        self.db.record_intro_post(npub, "event_002", 50)  # < 100 chars
        progress = self.db.get_progress(npub)
        self.assertFalse(progress["post_intro_done"])

    def test_record_replies(self):
        npub = "challenge_npub_4"
        self.db.start_challenge(npub)
        for i in range(5):
            self.db.record_reply(npub, f"parent_{i}", f"reply_{i}")
        progress = self.db.get_progress(npub)
        self.assertEqual(progress["reply_count"], 5)

    def test_dedup_replies(self):
        npub = "challenge_npub_5"
        self.db.start_challenge(npub)
        self.db.record_reply(npub, "parent_x", "reply_x")
        self.db.record_reply(npub, "parent_x", "reply_x")  # duplicate
        progress = self.db.get_progress(npub)
        self.assertEqual(progress["reply_count"], 1)

    def test_verify_identity(self):
        npub = "challenge_npub_6"
        self.db.start_challenge(npub)
        self.db.record_identity_verification(npub)
        progress = self.db.get_progress(npub)
        self.assertTrue(progress["verify_identity_done"])

    def test_award_stars_all_done(self):
        npub = "challenge_npub_7"
        self.db.start_challenge(npub)
        self.db.record_intro_post(npub, "ev1", 200)
        for i in range(5):
            self.db.record_reply(npub, f"p{i}", f"r{i}")
        self.db.record_identity_verification(npub)
        stars = self.db.check_and_award_stars(npub)
        self.assertEqual(stars, 3)
        progress = self.db.get_progress(npub)
        self.assertEqual(progress["status"], "completed")

    def test_leaderboard(self):
        from core.sovereignty_challenge import SovereigntyChallenge
        challenge = SovereigntyChallenge(self.db)
        for i in range(3):
            npub = f"leader_{i}"
            self.db.start_challenge(npub)
            self.db.record_intro_post(npub, f"ev_{i}", 200)
            for j in range(5):
                self.db.record_reply(npub, f"p{i}_{j}", f"r{i}_{j}")
            self.db.record_identity_verification(npub)
            self.db.check_and_award_stars(npub)
        lb = challenge.get_leaderboard()
        self.assertGreaterEqual(len(lb), 3)
        self.assertEqual(lb[0]["stars"], 3)


class TestSovereigntyChallengeProcess(unittest.TestCase):
    def setUp(self):
        from core.sovereignty_challenge import SovereigntyDB, SovereigntyChallenge
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.db = SovereigntyDB(self.tmp.name)
        self.challenge = SovereigntyChallenge(self.db)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_auto_start_on_intro_tag(self):
        event = {
            "kind": 1,
            "pubkey": "auto_start_npub",
            "content": "Hello #snin-intro world! This is my first post in the sovereign network. I am an autonomous agent ready to join the mesh." + "x" * 20,
            "tags": [],
            "id": "auto_start_event",
            "created_at": int(time.time()),
        }
        self.challenge.process_new_event(event)
        progress = self.db.get_progress("auto_start_npub")
        self.assertIsNotNone(progress)
        self.assertTrue(progress["post_intro_done"])

    def test_process_reply_event(self):
        npub = "reply_npub"
        self.db.start_challenge(npub)
        event = {
            "kind": 1,
            "pubkey": npub,
            "content": "Great post!",
            "tags": [["e", "parent_event_id"]],
            "id": "reply_event_1",
            "created_at": int(time.time()),
        }
        self.challenge.process_new_event(event)
        progress = self.db.get_progress(npub)
        self.assertEqual(progress["reply_count"], 1)

    def test_process_kind0_verification(self):
        npub = "kind0_npub"
        self.db.start_challenge(npub)
        event = {
            "kind": 0,
            "pubkey": npub,
            "content": '{"name": "test", "nip05": "test@snin.v2.site"}',
            "tags": [],
            "id": "kind0_event",
            "created_at": int(time.time()),
        }
        self.challenge.process_new_event(event)
        progress = self.db.get_progress(npub)
        self.assertTrue(progress["verify_identity_done"])

    def test_ignore_non_challenger(self):
        event = {
            "kind": 1,
            "pubkey": "random_npub_no_challenge",
            "content": "Just a random post",
            "tags": [],
            "id": "random_event",
            "created_at": int(time.time()),
        }
        # Should not raise
        self.challenge.process_new_event(event)
        progress = self.db.get_progress("random_npub_no_challenge")
        self.assertIsNone(progress)


if __name__ == "__main__":
    unittest.main(verbosity=2)

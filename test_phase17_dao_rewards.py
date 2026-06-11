"""
Phase 17: DAO Treasury Mesh Rewards
Tests for:
  - RewardLedger: work recording, balance tracking
  - Auto-payout proposals (kind:30001)
  - Integration: routing → rewards, expertise → rewards
  - Treasury snapshot and stats
  - Proposal execution via ChequeMesh
"""

import sys
import time
import json
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

passed = 0
failed = 0


def test(name: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✅ {name}" + (f" ({detail})" if detail else ""))
    else:
        failed += 1
        print(f"  ❌ {name}" + (f" — {detail}" if detail else ""))


def section(name: str):
    print(f"\n─ {name} ─")


# ═══════════════════════════════════════════════════════════
# P17.1: Reward Ledger — Basic Operations
# ═══════════════════════════════════════════════════════════

def test_ledger_basics():
    section("P17.1: Reward Ledger — Basics")

    from dao_rewards import RewardLedger, REWARD_RATES

    rl = RewardLedger()

    # 1.1 Record single work
    entry = rl.record_work("agent_A", "route_hop", {"hops": 3})
    test("Record work: not None", entry is not None)
    test("Record work: correct type", entry.work_type == "route_hop")
    test("Record work: 3 hops × 0.5 = 1.5", entry.amount == 1.5)
    test("Balance: 1.5 SNIN", rl.balances["agent_A"] == 1.5)

    # 1.2 Record another work
    rl.record_work("agent_A", "cheque_settled")
    test("Balance: 1.5 + 2.0 = 3.5", rl.balances["agent_A"] == 3.5)

    # 1.3 Record for different agent
    rl.record_work("agent_B", "expertise_match")
    test("Agent B balance: 1.0", rl.balances["agent_B"] == 1.0)
    test("Agent A unchanged", rl.balances["agent_A"] == 3.5)

    # 1.4 Stats
    test("Stats: total_rewards = 4.5", rl.stats["total_rewards_issued"] == 4.5)
    test("Stats: work_events = 3", rl.stats["work_events"] == 3)

    # 1.5 Unknown work type
    entry = rl.record_work("agent_C", "nonexistent_work")
    test("Unknown work type → None", entry is None)
    test("Unknown work → not in balances", "agent_C" not in rl.balances)

    # 1.6 Record batch
    batch = [
        ("agent_A", "route_hop", {"hops": 2}),       # 1.0
        ("agent_B", "cheque_settled", {}),            # 2.0
        ("agent_C", "expertise_match", {}),           # 1.0
    ]
    results = rl.record_batch(batch)
    test("Batch: 3 recorded", len(results) == 3)
    test("Agent A balance: 3.5 + 1.0 = 4.5", rl.balances["agent_A"] == 4.5)
    test("Agent B balance: 1.0 + 2.0 = 3.0", rl.balances["agent_B"] == 3.0)
    test("Agent C balance: 1.0", rl.balances["agent_C"] == 1.0)

    # 1.7 RewardEntry serialization
    entry = rl.ledger["agent_A"][0]
    tags = entry.to_event_tags()
    test("Event tags: has p-tag", any(t[0] == "p" for t in tags))
    test("Event tags: has work_type", any(t[0] == "work_type" for t in tags))
    test("Event tags: has amount", any(t[0] == "amount" for t in tags))

    return rl


# ═══════════════════════════════════════════════════════════
# P17.2: Auto-Payout Proposals
# ═══════════════════════════════════════════════════════════

def test_auto_payout():
    section("P17.2: Auto-Payout Proposals")

    from dao_rewards import RewardLedger, AUTO_PAYOUT_THRESHOLD

    # Mock DAO
    class MockDAO:
        def __init__(self):
            self.proposals = []
            self.ranks = {}
            self.treasury_data = {"balance": 10000.0, "currency": "SNIN"}

        def update_rank(self, agent_id, delta_score=0):
            if agent_id not in self.ranks:
                self.ranks[agent_id] = {"score": 0}
            self.ranks[agent_id]["score"] += delta_score

        def create_proposal(self, title, description, proposal_type, author, details=None):
            pid = f"prop_{len(self.proposals):04d}"
            prop = {
                "id": pid,
                "title": title,
                "description": description,
                "type": proposal_type,
                "author": author,
                "details": details or {},
                "status": "voting",
                "votes_for": 0,
                "votes_against": 0,
            }
            self.proposals.append(prop)
            return prop

        def get_proposal(self, pid):
            for p in self.proposals:
                if p["id"] == pid:
                    return p
            return None

        def get_treasury(self):
            return dict(self.treasury_data)

        def _execute_proposal(self, prop):
            prop["status"] = "executed"

    dao = MockDAO()
    rl = RewardLedger(dao_db=dao)

    # 2.1 Below threshold → no payout
    rl.record_work("agent_X", "route_hop", {"hops": 1})  # 0.5
    proposals = rl.tick()
    test("Below threshold: no proposals", len(proposals) == 0)
    test("Balance retained", rl.balances.get("agent_X", 0) == 0.5)

    # 2.2 Above threshold → auto payout
    rl.record_work("agent_X", "route_hop", {"hops": 100})  # 0.5 × 100 = 50
    # Now balance = 50.5 ≥ 50 (threshold)
    proposals = rl.tick()
    test("Above threshold: proposal created", len(proposals) == 1)
    test("Proposal type = reward", proposals[0].get("type") == "reward")

    # 2.3 Balance zeroed after payout
    test("Balance zeroed after payout", rl.balances.get("agent_X", -1) == 0.0)
    test("Stats: 1 payout", rl.stats["total_payouts"] == 1)
    test("Stats: 1 proposal created", rl.stats["proposals_created"] == 1)

    # 2.4 force_payout
    rl.record_work("agent_Y", "expertise_match")  # 1.0 — below threshold
    prop = rl.force_payout("agent_Y")
    test("Force payout: proposal created", prop is not None)
    test("Force payout: balance zeroed", rl.balances.get("agent_Y", -1) == 0.0)

    # 2.5 force_payout on zero balance
    prop2 = rl.force_payout("agent_Y")
    test("Force payout on zero: none", prop2 is None)

    return rl, dao


# ═══════════════════════════════════════════════════════════
# P17.3: Treasury Snapshot & Stats
# ═══════════════════════════════════════════════════════════

def test_treasury_snapshot():
    section("P17.3: Treasury Snapshot & Stats")

    from dao_rewards import RewardLedger

    class MockDAO:
        def __init__(self):
            self.ranks = {}
            self.treasury_data = {"balance": 10000.0, "currency": "SNIN", "allocated": 500.0}

        def update_rank(self, agent_id, delta_score=0):
            if agent_id not in self.ranks:
                self.ranks[agent_id] = {"score": 0}
            self.ranks[agent_id]["score"] += delta_score

        def get_treasury(self):
            return dict(self.treasury_data)

    dao = MockDAO()
    rl = RewardLedger(dao_db=dao)

    # Record some work
    rl.record_work("node_1", "route_hop", {"hops": 10})    # 5.0
    rl.record_work("node_1", "cheque_settled")              # 2.0
    rl.record_work("node_2", "expertise_match")             # 1.0

    # 3.1 Snapshot structure
    snap = rl.get_treasury_snapshot()
    test("Snapshot: has treasury", "treasury" in snap)
    test("Snapshot: has pending_balances", "pending_balances" in snap)
    test("Snapshot: has total_pending", "total_pending" in snap)
    test("Snapshot: has agent_count", "agent_count" in snap)
    test("Snapshot: has stats", "stats" in snap)
    test("Snapshot: has rates", "rates" in snap)

    # 3.2 Snapshot values
    test("Snapshot: 2 agents", snap["agent_count"] == 2)
    test("Snapshot: total_pending = 8.0", snap["total_pending"] == 8.0)
    test("Snapshot: treasury balance = 10000.0", snap["treasury"].get("balance") == 10000.0)
    test("Snapshot: rates has all types", len(snap["rates"]) >= 4)

    # 3.3 No DAO → works without crash
    rl2 = RewardLedger()
    rl2.record_work("solo_agent", "route_hop")
    snap2 = rl2.get_treasury_snapshot()
    test("No DAO: snapshot works", snap2["agent_count"] == 1)
    test("No DAO: treasury empty", snap2["treasury"] == {})


# ═══════════════════════════════════════════════════════════
# P17.4: Integration — Routing → Rewards
# ═══════════════════════════════════════════════════════════

def test_routing_rewards_integration():
    section("P17.4: Routing → Rewards Integration")

    from dao_rewards import (
        RewardLedger,
        process_routing_reward,
        process_expertise_reward,
        process_uptime_reward,
    )

    rl = RewardLedger()

    # 4.1 Forwarded hop
    process_routing_reward("relay_X",
                          {"status": "forwarded", "next": "relay_Y"},
                          rl)
    test("Forwarded: balance = 0.5", rl.balances["relay_X"] == 0.5)
    test("Forwarded: work_events = 1", rl.stats["work_events"] == 1)

    # 4.2 Settled with hops
    process_routing_reward("relay_X",
                          {"status": "settled", "total_hops": 4},
                          rl)
    # 4 hops × 0.5 = 2.0 + 2.0 (cheque_settled) = 4.0
    test("Settled: balance = 0.5 + 4.0 = 4.5", rl.balances["relay_X"] == 4.5)
    test("Settled: work_events = 3", rl.stats["work_events"] == 3)

    # 4.3 Expertise match
    process_expertise_reward("agent_expert",
                             {"topic": "AI", "confidence": 0.95},
                             rl)
    test("Expertise: balance = 1.0", rl.balances["agent_expert"] == 1.0)

    # 4.4 Expertise no-match (unknown)
    process_expertise_reward("agent_expert",
                             {"topic": "unknown", "confidence": 0.1},
                             rl)
    test("Unknown topic: no reward", rl.balances["agent_expert"] == 1.0)

    # 4.5 Uptime reward
    process_uptime_reward("relay_X", 10.0, rl)
    # 10 hours × 0.1 = 1.0
    test("Uptime: balance = 4.5 + 1.0 = 5.5", rl.balances["relay_X"] == 5.5)


# ═══════════════════════════════════════════════════════════
# P17.5: Proposal Execution via ChequeMesh
# ═══════════════════════════════════════════════════════════

def test_proposal_execution():
    section("P17.5: Proposal Execution via ChequeMesh")

    from dao_rewards import RewardLedger
    from cheque_mesh import ChequeMeshRouter, MeshCheque

    # Mock DAO with proposals
    class MockDAOFull:
        def __init__(self):
            self.proposals = {}
            self.treasury = {"balance": 10000.0, "currency": "SNIN"}

        def update_rank(self, agent_id, delta_score=0):
            pass

        def create_proposal(self, title, description, proposal_type, author, details=None):
            pid = "prop_test_001"
            self.proposals[pid] = {
                "id": pid,
                "title": title,
                "description": description,
                "type": proposal_type,
                "author": author,
                "details": details,
                "status": "voting",
                "votes_for": 0,
                "votes_against": 0,
            }
            return self.proposals[pid]

        def get_proposal(self, pid):
            return self.proposals.get(pid)

        def get_treasury(self):
            return dict(self.treasury)

        def _execute_proposal(self, prop):
            prop["status"] = "executed"

    dao = MockDAOFull()
    router = ChequeMeshRouter()
    rl = RewardLedger(dao_db=dao, cheque_router=router)

    # 5.1 Create and pass a proposal
    prop = dao.create_proposal(
        title="Reward: agent_Z — 100 SNIN",
        description="Test reward",
        proposal_type="reward",
        author="treasury",
        details={"agent_id": "agent_Z", "amount": 100.0},
    )
    prop["status"] = "passed"  # Simulate voting passed
    prop["votes_for"] = 15
    prop["votes_against"] = 0

    # 5.2 Execute payout
    result = rl.execute_proposal_payout("prop_test_001")
    test("Payout executed", result is not None)
    test("Payout status = payout_sent", result["status"] == "payout_sent")
    test("Payout: cheque created", "cheque" in result)
    test("Payout: cheque kind=30000", result["cheque"]["kind"] == 30000)
    test("Payout: payee = agent_Z",
         any(t[0] == "p" and t[1] == "agent_Z" for t in result["cheque"]["tags"]))
    test("Payout: amount = 100",
         any(t[0] == "amount" and t[1] == "100.0" for t in result["cheque"]["tags"]))

    # 5.3 Proposal marked executed
    test("Proposal status = executed", prop["status"] == "executed")

    # 5.4 Non-passed proposal
    prop2 = dao.create_proposal(
        title="Reward: agent_B — 50 SNIN",
        description="Test",
        proposal_type="reward",
        author="treasury",
        details={"agent_id": "agent_B", "amount": 50.0},
    )
    # Status is "voting" (not passed)
    result2 = rl.execute_proposal_payout("prop_test_001")
    # Already executed
    test("Non-passed: not executed", result2 is None or result2.get("status") != "payout_sent")

    # 5.5 Invalid details
    prop3 = dao.create_proposal(
        title="Bad proposal",
        description="Missing agent",
        proposal_type="reward",
        author="treasury",
        details={},  # No agent_id, no amount
    )
    prop3["status"] = "passed"
    result3 = rl.execute_proposal_payout("prop_test_001")
    test("Invalid details: no payout", result3 is None or result3.get("status") != "payout_sent")


# ═══════════════════════════════════════════════════════════
# P17.6: Cron Handler Integration
# ═══════════════════════════════════════════════════════════

def test_cron_handlers():
    section("P17.6: Cron Handler Integration")

    from dao_rewards import (
        RewardLedger,
        make_reward_tick_handler,
        make_uptime_reward_handler,
        AUTO_PAYOUT_THRESHOLD,
    )

    class MockDAO:
        def __init__(self):
            self.proposals = []

        def update_rank(self, agent_id, delta_score=0):
            pass

        def create_proposal(self, title, description, proposal_type, author, details=None):
            pid = f"cron_prop_{len(self.proposals)}"
            prop = {"id": pid, "title": title, "status": "voting", "details": details}
            self.proposals.append(prop)
            return prop

        def get_treasury(self):
            return {"balance": 10000.0}

    dao = MockDAO()
    rl = RewardLedger(dao_db=dao)

    # 6.1 Uptime handler via cron
    uptime_handler = make_uptime_reward_handler("relay_R", rl, hours_per_tick=2.0)
    uptime_handler()
    test("Uptime cron: balance = 0.2", rl.balances.get("relay_R", 0) == 0.2)

    # 6.2 Multiple uptime ticks
    for _ in range(5):
        uptime_handler()
    # 6 ticks × 2h × 0.1 = 1.2
    test("6 uptime ticks: balance = 1.2", abs(rl.balances["relay_R"] - 1.2) < 0.01)

    # 6.3 Reward tick handler
    rl.record_work("agent_big", "route_hop", {"hops": 200})  # 100 SNIN ≥ threshold
    tick_handler = make_reward_tick_handler(rl)
    tick_handler()
    test("Tick cron: proposal created", len(dao.proposals) > 0)
    test("Tick cron: balance zeroed", rl.balances.get("agent_big", -1) == 0.0)


# ═══════════════════════════════════════════════════════════
# P17.7: Regression — P10-P16 unchanged
# ═══════════════════════════════════════════════════════════

def test_regression():
    section("P17.7: Regression Checks")

    import redis
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    from knowledge_graph import KnowledgeGraph
    from smart_router import SmartRouter
    from semantic_router import create_semantic_router
    from content_router import create_content_router
    from agent_cron import CronScheduler
    from cheque_mesh import MeshCheque, ChequeMeshRouter

    kg = KnowledgeGraph(r)
    sr = SmartRouter()
    sem = create_semantic_router(kg, sr, r)
    cr = create_content_router(sem)

    # P12: Classification
    cc = cr.classify_event({"content": "Bitcoin on-chain analysis report", "kind": 1})
    test("Regression: P12 classification", cc.topic in ("Crypto", "BTC"))

    # P13: Word boundary
    cc2 = cr.classify_event({"content": "airline fuel costs rising", "kind": 1})
    test("Regression: P13 airline ≠ AI", cc2.topic != "AI")

    # P14: Non-latin
    cc3 = cr.classify_event({"content": "ビットコインの価格が上昇中", "kind": 1})
    test("Regression: P14 Japanese → unknown", cc3.topic == "unknown")

    # P16: Cron
    cs = CronScheduler()
    calls = []
    cs.register("reg_test", "tick", 0.01, lambda: calls.append(1))
    time.sleep(0.02)
    cs.tick()
    test("Regression: P16 cron executes", len(calls) >= 1)

    # P16: Cheque
    chq = MeshCheque(
        cheque_id="reg_chq",
        payer_pubkey="reg_A",
        payee_pubkey="reg_B",
        amount=10.0,
    )
    event = chq.to_kind30000_event()
    test("Regression: P16 kind=30000", event["kind"] == 30000)


# ═══════════════════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("═══ Phase 17 — DAO Treasury Mesh Rewards ═══")

    test_ledger_basics()
    test_auto_payout()
    test_treasury_snapshot()
    test_routing_rewards_integration()
    test_proposal_execution()
    test_cron_handlers()
    test_regression()

    print(f"\n═══ Phase 17: {passed} passed, {failed} failed ═══")
    sys.exit(0 if failed == 0 else 1)

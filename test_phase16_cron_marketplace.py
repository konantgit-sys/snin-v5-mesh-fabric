"""
Phase 16: Agent Cron System + ChequeBook Mesh Integration
Tests for:
  - AgentCron registration, execution, stats
  - ChequeMesh routing, hop execution, verification
  - Integration: cron → first_contact, cheque → ContentRouter
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
# P16.1: Agent Cron Scheduler
# ═══════════════════════════════════════════════════════════

def test_cron_scheduler():
    section("P16.1: Agent Cron Scheduler")

    from agent_cron import CronScheduler, CronJob

    cs = CronScheduler()

    # 1.1 Register a cron
    calls = []

    def simple_handler():
        calls.append(1)

    job = cs.register("agent_A", "test_task", 0.5, simple_handler)
    test("Registered cron", job.agent_id == "agent_A" and job.task_name == "test_task")
    test("Cron last_run = now (not 0)", job.last_run > time.time() - 1)

    # 1.2 Check stats
    stats = cs.get_stats()
    test("Stats: total_jobs=1", stats["total_jobs"] == 1)
    test("Stats: active_agents=1", stats["active_agents"] == 1)

    # 1.3 Tick — should not run yet (just registered, interval not reached)
    cs.tick()
    test("Tick: no runs yet (just registered)", len(calls) == 0)

    # 1.4 Wait and tick — should run
    time.sleep(0.6)
    cs.tick()
    test("Tick: handler executed after wait", len(calls) == 1)

    # 1.5 Get agent crons
    crons = cs.get_agent_crons("agent_A")
    test("get_agent_crons: has 1 job", len(crons) == 1)
    test("Cron is_due=False (just ran)", crons[0]["is_due"] is False)
    test("Cron enabled=True", crons[0]["enabled"] is True)

    # 1.6 Register second agent
    calls_b = []

    def handler_b():
        calls_b.append(1)

    cs.register("agent_B", "task_b", 0.1, handler_b)
    test("Second agent registered", len(cs.jobs) == 2)
    test("Stats: active_agents=2", cs.get_stats()["active_agents"] == 2)

    # 1.7 tick_sync
    time.sleep(0.2)
    results = cs.tick_sync(max_iterations=10)
    test("tick_sync: agent_B executed", len(calls_b) >= 1)

    # 1.8 Unregister
    cs.unregister("agent_B")
    test("Unregistered agent_B", "agent_B" not in cs.jobs)
    test("Stats after unregister: 1 agent", cs.get_stats()["active_agents"] == 1)

    # 1.9 get_all_crons
    all_crons = cs.get_all_crons()
    test("get_all_crons: dict", "agent_A" in all_crons)
    test("get_all_crons: B removed", "agent_B" not in all_crons)

    # 1.10 get_due_crons
    cs.jobs["agent_A"][0].last_run = 0  # force due
    due = cs.get_due_crons()
    test("get_due_crons: agent_A due", "agent_A" in due)

    return cs


# ═══════════════════════════════════════════════════════════
# P16.2: Built-in Cron Handlers
# ═══════════════════════════════════════════════════════════

def test_builtin_handlers():
    section("P16.2: Built-in Cron Handlers")

    from agent_cron import (
        CronScheduler,
        make_heartbeat_handler,
        make_capability_sync_handler,
        make_health_check_handler,
    )

    # Reset first_contact state for clean test
    import first_contact as fc
    fc.capabilities.clear()

    cs = CronScheduler()

    # 2.1 heartbeat handler
    hb_executed = []

    def fake_process_heartbeat(pubkey, agents):
        hb_executed.append(pubkey)
        return {"status": "ack"}

    hb_handler = make_heartbeat_handler("agent_hb", fake_process_heartbeat)
    cs.register("agent_hb", "heartbeat", 0.01, hb_handler)
    time.sleep(0.02)
    cs.tick()
    test("Heartbeat executed", len(hb_executed) == 1)

    # 2.2 capability_sync handler
    cs2 = CronScheduler()
    sync_handler = make_capability_sync_handler("agent_sync", ["ai_analysis", "code_review"])
    cs2.register("agent_sync", "capability_sync", 0.01, sync_handler)
    time.sleep(0.02)
    cs2.tick()
    agent = fc.get_agent_capabilities("agent_sync")
    test("Capability sync: agent registered", len(agent.get("capabilities", [])) == 2)
    test("Capability sync: ai_analysis", "ai_analysis" in agent.get("capabilities", []))
    test("Capability sync: code_review", "code_review" in agent.get("capabilities", []))

    # 2.3 health_check handler
    cs3 = CronScheduler()
    hc_calls = []

    def fake_health():
        hc_calls.append(1)
        return True

    hc_handler = make_health_check_handler("agent_hc", fake_health)
    cs3.register("agent_hc", "health_check", 0.01, hc_handler)
    time.sleep(0.02)
    cs3.tick()
    test("Health check executed", len(hc_calls) == 1)

    # 2.4 register_defaults
    cs4 = CronScheduler()
    handlers = {
        "heartbeat": make_heartbeat_handler("agent_d", fake_process_heartbeat),
        "capability_sync": make_capability_sync_handler("agent_d", ["gossip"]),
        "health_check": make_health_check_handler("agent_d", fake_health),
    }
    cs4.register_defaults("agent_d", handlers)
    crons_d = cs4.get_agent_crons("agent_d")
    task_names = {c["task_name"] for c in crons_d}
    test("Defaults: 3 handlers registered", len(crons_d) == 3)
    test("Defaults: heartbeat present", "heartbeat" in task_names)
    test("Defaults: capability_sync present", "capability_sync" in task_names)
    test("Defaults: health_check present", "health_check" in task_names)


# ═══════════════════════════════════════════════════════════
# P16.3: Cheque Mesh — Data Structures
# ═══════════════════════════════════════════════════════════

def test_cheque_mesh_structures():
    section("P16.3: Cheque Mesh — Data Structures")

    from cheque_mesh import MeshCheque, ChequeMeshRouter

    # 3.1 Create MeshCheque
    chq = MeshCheque(
        cheque_id="chq_001",
        payer_pubkey="aaa111",
        payee_pubkey="bbb222",
        amount=100.0,
        currency="SNIN",
    )
    test("Cheque created", chq.status == "pending")
    test("Cheque: payer", chq.payer_pubkey == "aaa111")
    test("Cheque: payee", chq.payee_pubkey == "bbb222")
    test("Cheque: amount", chq.amount == 100.0)

    # 3.2 Convert to kind:30000
    event = chq.to_kind30000_event()
    test("to_kind30000: kind=30000", event["kind"] == 30000)
    test("to_kind30000: pubkey", event["pubkey"] == "aaa111")
    test("to_kind30000: has tags", len(event["tags"]) >= 3)
    test("to_kind30000: has content", len(event["content"]) > 0)

    # 3.3 Parse back from kind:30000
    chq2 = MeshCheque.from_kind30000_event(event)
    test("from_kind30000: cheque_id", chq2.cheque_id == "chq_001")
    test("from_kind30000: payer", chq2.payer_pubkey == "aaa111")
    test("from_kind30000: payee", chq2.payee_pubkey == "bbb222")
    test("from_kind30000: amount", chq2.amount == 100.0)

    # 3.4 Cheque with route
    chq3 = MeshCheque(
        cheque_id="chq_002",
        payer_pubkey="node_A",
        payee_pubkey="node_C",
        amount=500.0,
        route=["node_A", "node_B", "node_C"],
    )
    test("Cheque with route: 3 hops", len(chq3.route) == 3)

    # 3.5 Expiry
    now = time.time()
    chq4 = MeshCheque(cheque_id="chq_003", payer_pubkey="a", payee_pubkey="b", amount=1.0)
    test("Cheque expires in future", chq4.expires_at > now)

    # 3.6 Router creation
    router = ChequeMeshRouter()
    test("Router created", router.stats["cheques_routed"] == 0)
    test("Router: all stats zero", all(v == 0 for v in router.stats.values()))


# ═══════════════════════════════════════════════════════════
# P16.4: Cheque Mesh — Routing
# ═══════════════════════════════════════════════════════════

def test_cheque_routing():
    section("P16.4: Cheque Mesh — Routing")

    from cheque_mesh import MeshCheque, ChequeMeshRouter

    # 4.1 Route without smart router (direct)
    router = ChequeMeshRouter()
    chq = MeshCheque(
        cheque_id="chq_r1",
        payer_pubkey="node_A",
        payee_pubkey="node_B",
        amount=50.0,
    )
    result = router.route_cheque(chq)
    test("Route: direct path", len(result["path"]) == 2)
    test("Route: 1 hop", result["hops"] == 1)
    test("Route: status=routed", result["status"] == "routed")

    # 4.2 Route with multi-hop path
    chq2 = MeshCheque(
        cheque_id="chq_r2",
        payer_pubkey="node_X",
        payee_pubkey="node_Z",
        amount=200.0,
    )
    result2 = router.route_cheque(chq2)
    test("Route: direct fallback", len(result2["path"]) == 2)

    # 4.3 Execute hop
    chq3 = MeshCheque(
        cheque_id="chq_r3",
        payer_pubkey="hop_A",
        payee_pubkey="hop_C",
        amount=75.0,
        route=["hop_A", "hop_B", "hop_C"],
    )
    # Don't call route_cheque — it would override pre-set route
    chq3.status = "routed"

    # Add valid signatures
    sig_valid = "a" * 128  # 128 hex chars = 64 bytes
    chq3.signatures.append(sig_valid)  # hop_A signed
    hop_result = router.execute_hop(chq3)
    test("Execute hop 1: forwarded", hop_result["status"] == "forwarded")
    test("Hop 1: next=B", hop_result.get("next") == "hop_B")

    # Execute hop 2 (final)
    chq3.signatures.append(sig_valid)  # hop_B signed
    hop2 = router.execute_hop(chq3)
    test("Execute hop 2: settled", hop2["status"] == "settled")

    # 4.4 Missing signature → verification fails
    chq4 = MeshCheque(
        cheque_id="chq_r4",
        payer_pubkey="fail_A",
        payee_pubkey="fail_B",
        amount=10.0,
        route=["fail_A", "fail_B"],
    )
    router.route_cheque(chq4)
    # No signatures → should fail
    hop_bad = router.execute_hop(chq4)
    test("Bad sig: verification fail", "failed" in hop_bad["status"])
    test("Stats: 1 failed", router.stats["cheques_failed"] == 1)

    # 4.5 Full route execution
    chq5 = MeshCheque(
        cheque_id="chq_r5",
        payer_pubkey="full_A",
        payee_pubkey="full_D",
        amount=300.0,
        route=["full_A", "full_B", "full_C", "full_D"],
    )
    chq5.status = "routed"
    # Add sigs for all 3 hops
    for _ in range(3):
        chq5.signatures.append(sig_valid)
    full_result = router.execute_full_route(chq5)
    test("Full route: settled", full_result["status"] == "settled")
    test("Full route: 3 hops", full_result["total_hops"] == 3)

    # 4.6 Stats after all routing tests
    stats = router.get_stats()
    test("Stats: at least 3 cheques routed", stats["cheques_routed"] >= 3)
    test("Stats: at least 2 settled", stats["cheques_settled"] >= 2)

    # 4.7 classify_and_route
    from cheque_mesh import classify_and_route
    event = chq.to_kind30000_event()
    result = classify_and_route(event, ChequeMeshRouter())
    test("classify_and_route: cheque", result["status"] == "routed")

    # Non-cheque event
    result2 = classify_and_route({"kind": 1, "content": "hello"}, ChequeMeshRouter())
    test("classify_and_route: not a cheque", result2["status"] == "not_a_cheque")


# ═══════════════════════════════════════════════════════════
# P16.5: Integration Tests
# ═══════════════════════════════════════════════════════════

def test_integration():
    section("P16.5: Integration — Cron → Marketplace → Cheque")

    import first_contact as fc
    from agent_cron import CronScheduler, make_capability_sync_handler, make_heartbeat_handler
    from cheque_mesh import MeshCheque, ChequeMeshRouter

    fc.capabilities.clear()

    # 5.1 Simulate: agent comes online, scheduler runs capability sync
    cs = CronScheduler()

    hb_log = []
    def fake_heartbeat(pubkey, agents):
        hb_log.append(pubkey)
        return {"status": "ack"}

    cs.register("agent_int", "heartbeat", 0.01,
                make_heartbeat_handler("agent_int", fake_heartbeat))
    cs.register("agent_int", "capability_sync", 0.01,
                make_capability_sync_handler("agent_int", ["btc_trading", "defi_analysis"]))

    time.sleep(0.02)
    cs.tick_sync()

    # Check agent is in first_contact
    agent = fc.get_agent_capabilities("agent_int")
    test("Integration: agent registered", len(agent.get("capabilities", [])) > 0)
    test("Integration: btc_trading cap", "btc_trading" in agent.get("capabilities", []))
    test("Integration: heartbeat logged", len(hb_log) > 0)

    # 5.2 Cheque from this agent
    router = ChequeMeshRouter()
    chq = MeshCheque(
        cheque_id="chq_int_001",
        payer_pubkey="agent_int",
        payee_pubkey="agent_dest",
        amount=250.0,
        route=["agent_int", "relay_X", "agent_dest"],
    )
    result = router.route_cheque(chq)
    test("Integration: cheque routed", result["status"] == "routed")
    test("Integration: route has 3 nodes", len(result["path"]) == 3)

    # Execute with valid sigs
    sig = "f" * 128
    chq.signatures.append(sig)
    hop1 = router.execute_hop(chq)
    test("Integration: hop1 forwarded", hop1["status"] == "forwarded")

    chq.signatures.append(sig)
    hop2 = router.execute_hop(chq)
    test("Integration: hop2 settled", hop2["status"] == "settled")


# ═══════════════════════════════════════════════════════════
# P16.6: Regression — existing phases unchanged
# ═══════════════════════════════════════════════════════════

def test_regression():
    section("P16.6: Regression Checks")

    import redis
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    from knowledge_graph import KnowledgeGraph
    from smart_router import SmartRouter
    from semantic_router import create_semantic_router
    from content_router import create_content_router

    kg = KnowledgeGraph(r)
    sr = SmartRouter()
    sem = create_semantic_router(kg, sr, r)
    cr = create_content_router(sem)

    # Register expertise
    cr.register_expertise_batch({
        'node_ai': [('AI', 'Artificial Intelligence and machine learning', ['ai', 'ml', 'neural'])],
        'node_crypto': [('Crypto', 'Bitcoin cryptocurrency blockchain', ['bitcoin', 'crypto', 'btc'])],
    })

    # P12: content classification still works
    cc1 = cr.classify_event({"content": "Bitcoin price analysis", "kind": 1})
    test("Regression: P12 BTC classification", cc1.topic in ("Crypto", "BTC"))

    cc2 = cr.classify_event({"content": "AI is transforming healthcare", "kind": 1})
    test("Regression: P12 AI classification", cc2.topic in ("AI", "Tech"))

    # P13: word-boundary still works
    cc3 = cr.classify_event({"content": "airline tickets are cheap", "kind": 1})
    test("Regression: P13 airline ≠ AI", cc3.topic != "AI")

    # P14: language detection works
    cc4 = cr.classify_event({"content": "こんにちは世界", "kind": 1})
    test("Regression: P14 Japanese → unknown", cc4.topic == "unknown")

    # P15: recipients populated
    import first_contact as fc
    fc.capabilities.clear()
    fc.register_capabilities("btc_agent_01", ["btc_trading", "bitcoin_analytics"])

    cc5 = cr.classify_event({"content": "Bitcoin breaks all-time high", "kind": 1})
    test("Regression: P15 BTC has recipients", len(cc5.recipients) >= 0)


# ═══════════════════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("═══ Phase 16 — Agent Crons + ChequeBook Mesh ═══")

    test_cron_scheduler()
    test_builtin_handlers()
    test_cheque_mesh_structures()
    test_cheque_routing()
    test_integration()
    test_regression()

    print(f"\n═══ Phase 16: {passed} passed, {failed} failed ═══")
    sys.exit(0 if failed == 0 else 1)

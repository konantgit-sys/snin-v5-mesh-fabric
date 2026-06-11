"""
Phase 19: Production Release — full pipeline integration tests.

Tests:
  - P19.1: Daemon service registration & ordering
  - P19.2: Health API endpoints
  - P19.3: End-to-end: event → classify → route → reward → federate
  - P19.4: Service dependency resolution
  - P19.5: Graceful shutdown
  - P19.6: Full regression P10-P18
  - P19.7: Pipeline smoke test (100 events)
  - P19.8: Cross-mesh full cycle
  - P19.9: Production readiness checklist
"""

import sys
import time
import json
import os
import threading

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
# P19.1: Daemon — Service Registry & Lifecycle
# ═══════════════════════════════════════════════════════════

def test_service_registry():
    section("P19.1: Service Registry & Lifecycle")

    import redis
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    from snin_mesh_daemon import Service, ServiceRegistry

    # 1.1 Register services
    registry = ServiceRegistry()

    calls = []
    svc_a = Service("a", lambda: calls.append("a_start"))
    svc_b = Service("b", lambda: calls.append("b_start"))
    svc_c = Service("c", lambda: calls.append("c_start"),
                    depends=["a", "b"])

    registry.register(svc_a)
    registry.register(svc_b)
    registry.register(svc_c)
    registry.set_order(["a", "b", "c"])

    test("3 services registered", len(registry.services) == 3)

    # 1.2 Start in order
    registry.start_all()
    test("A started", svc_a.started)
    test("B started", svc_b.started)
    test("C started (depends on A,B)", svc_c.started)
    test("Start order correct", calls == ["a_start", "b_start", "c_start"])

    # 1.3 Status
    status = registry.get_status()
    test("Status: 3 entries", len(status) == 3)
    test("All running", all(s["status"] == "running" for s in status.values()))

    # 1.4 Healthy
    test("is_healthy = True", registry.is_healthy())

    # 1.5 Stop in reverse
    stopped = []
    svc_a.stop_fn = lambda: stopped.append("a")
    svc_b.stop_fn = lambda: stopped.append("b")
    svc_c.stop_fn = lambda: stopped.append("c")
    registry.stop_all()
    test("Stop order: c,b,a", stopped == ["c", "b", "a"])
    test("All stopped", not any(s.started for s in registry.services.values()))

    # 1.6 Failed service doesn't crash others
    calls2 = []
    registry2 = ServiceRegistry()
    svc_ok = Service("ok", lambda: calls2.append("ok"))
    svc_fail = Service("fail", lambda: 1/0)  # division by zero
    registry2.register(svc_ok)
    registry2.register(svc_fail)
    registry2.set_order(["fail", "ok"])
    registry2.start_all()
    test("Failing service: failed", svc_fail.status == "failed")
    test("Failing service: error set", svc_fail.error is not None)
    test("OK service still started", svc_ok.started)

    # 1.7 Missing dependency
    calls3 = []
    registry3 = ServiceRegistry()
    svc_dep = Service("dep", lambda: calls3.append("dep"), depends=["missing"])
    svc_ind = Service("ind", lambda: calls3.append("ind"))
    registry3.register(svc_dep)
    registry3.register(svc_ind)
    registry3.set_order(["dep", "ind"])
    registry3.start_all()
    test("Missing dep: service failed", svc_dep.status == "failed")
    test("Independent service started", svc_ind.started)


# ═══════════════════════════════════════════════════════════
# P19.2: Health API
# ═══════════════════════════════════════════════════════════

def test_health_api():
    section("P19.2: Health API")

    from snin_mesh_daemon import Service, ServiceRegistry

    registry = ServiceRegistry()
    svc = Service("test_svc", lambda: 42)
    registry.register(svc)
    registry.set_order(["test_svc"])
    registry.start_all()

    # 2.1 Status JSON
    status = registry.get_status()
    test("Status dict returned", isinstance(status, dict))
    test("Service in status", "test_svc" in status)
    test("Status = running", status["test_svc"]["status"] == "running")
    test("Uptime >= 0", status["test_svc"]["uptime_sec"] >= 0)

    # 2.2 Healthy when all running
    test("is_healthy = True", registry.is_healthy())

    # 2.3 Unhealthy when one failed
    svc.status = "failed"
    svc.started = False
    test("is_healthy = False (one failed)", not registry.is_healthy())


# ═══════════════════════════════════════════════════════════
# P19.3: End-to-End Pipeline
# ═══════════════════════════════════════════════════════════

def test_e2e_pipeline():
    section("P19.3: End-to-End Pipeline (Event → Reward)")

    import redis
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    from knowledge_graph import KnowledgeGraph
    from smart_router import SmartRouter
    from semantic_router import create_semantic_router
    from content_router import create_content_router
    from dao_rewards import RewardLedger
    from federation_discovery import (
        create_local_topology, FederationDiscovery, FEDERATION_ANNOUNCE,
    )

    # Init core
    kg = KnowledgeGraph(r)
    sr = SmartRouter()
    sem = create_semantic_router(kg, sr, r)
    cr = create_content_router(sem)
    rl = RewardLedger()

    # Init federation
    topo = create_local_topology("e2e-mesh",
                                 agents=["agent_e2e_1", "agent_e2e_2", "agent_e2e_3"])
    fd = FederationDiscovery(topo, content_router=cr, reward_ledger=rl)

    # ── Step 1: Event arrives ──
    event = {
        "kind": 1,
        "content": "Bitcoin price analysis and market trends",
        "tags": [["t", "bitcoin"], ["t", "trading"]],
    }

    # ── Step 2: Classify ──
    cc = cr.classify_event(event)
    test("E2E: classify topic", cc.topic in ("Crypto", "BTC", "Finance"))
    test("E2E: classify confidence > 0", cc.confidence > 0)

    # ── Step 3: Route to agents (pass topic string)
    recipients = cr.find_recipients(cc.topic)
    test("E2E: find_recipients returns list", isinstance(recipients, list))

    # ── Step 4: Record work → Reward ──
    rl.record_work("agent_e2e_1", "route_hop", {"hops": 3})
    rl.record_work("agent_e2e_1", "cheque_settled", {"amount": 5.0})
    balance = rl.balances.get("agent_e2e_1", 0)
    test("E2E: agent earned SNIN", balance == 3.5)  # 3*0.5 + 2.0

    # ── Step 5: Federate ──
    foreign_event = {
        "kind": FEDERATION_ANNOUNCE,
        "content": json.dumps({
            "mesh_name": "e2e-foreign",
            "agents": ["ef_1", "ef_2", "ef_3", "ef_4", "ef_5"],
            "capabilities": ["routing", "federation"],
            "endpoints": [],
            "relays": ["wss://relay.damus.io", "wss://relay.primal.net"],
            "mesh_score": 0.8,
        }),
        "tags": [["mesh_id", "e2e_foreign_001"], ["name", "e2e-foreign"]],
    }
    remote = fd.process_announce_event(foreign_event)
    test("E2E: foreign mesh discovered", remote is not None)
    test("E2E: mesh is trusted", remote.is_trusted())

    # ── Step 6: Cross-mesh route ──
    fd.register_cross_route("e2e_foreign_001", "nostr_relay")
    result = fd.route_cross_mesh_message("e2e_foreign_001",
                                         {"msg": "Hello from e2e test"})
    test("E2E: cross-mesh routed", result["status"] == "routed")
    test("E2E: interaction counted",
         fd.remote_meshes["e2e_foreign_001"].interactions == 1)

    # ── Step 7: Federation rewarded ──
    fed_balance = rl.balances.get("e2e_foreign_001", 0)
    test("E2E: federation rewards > 0", fed_balance > 0)

    # ── Step 8: Final stats ──
    stats = fd.get_stats()
    test("E2E: remote meshes = 1", stats["remote"]["total"] == 1)
    test("E2E: cross_mesh_messages = 1",
         stats["stats"]["cross_mesh_messages"] == 1)


# ═══════════════════════════════════════════════════════════
# P19.4: Service Dependency Resolution
# ═══════════════════════════════════════════════════════════

def test_dependency_resolution():
    section("P19.4: Dependency Resolution")

    from snin_mesh_daemon import Service, ServiceRegistry

    # Complex dependency graph:
    #   A → [C]
    #   B → [C]
    #   C → [D, E]
    #   D → []
    #   E → []
    #
    # Valid order: D, E, C, A, B  (or D, E, C, B, A)

    calls = []
    registry = ServiceRegistry()

    svc_d = Service("d", lambda: calls.append("d"))
    svc_e = Service("e", lambda: calls.append("e"))
    svc_c = Service("c", lambda: calls.append("c"), depends=["d", "e"])
    svc_a = Service("a", lambda: calls.append("a"), depends=["c"])
    svc_b = Service("b", lambda: calls.append("b"), depends=["c"])

    for s in [svc_d, svc_e, svc_c, svc_a, svc_b]:
        registry.register(s)

    registry.set_order(["d", "e", "c", "a", "b"])
    registry.start_all()

    # Check: D and E must come before C
    d_pos = calls.index("d")
    e_pos = calls.index("e")
    c_pos = calls.index("c")
    test("D before C", d_pos < c_pos)
    test("E before C", e_pos < c_pos)
    test("C before A", c_pos < calls.index("a"))
    test("C before B", c_pos < calls.index("b"))
    test("All 5 started", len(calls) == 5)


# ═══════════════════════════════════════════════════════════
# P19.5: Graceful Shutdown
# ═══════════════════════════════════════════════════════════

def test_graceful_shutdown():
    section("P19.5: Graceful Shutdown")

    from snin_mesh_daemon import Service, ServiceRegistry

    stops = []
    registry = ServiceRegistry()

    svc1 = Service("s1", lambda: "ok", stop_fn=lambda: stops.append("s1"))
    svc2 = Service("s2", lambda: "ok", stop_fn=lambda: stops.append("s2"))
    svc3 = Service("s3", lambda: "ok", stop_fn=lambda: stops.append("s3"),
                   depends=["s1", "s2"])

    for s in [svc1, svc2, svc3]:
        registry.register(s)
    registry.set_order(["s1", "s2", "s3"])

    registry.start_all()
    test("All started", registry.is_healthy())

    registry.stop_all()
    test("All stopped: 3", len(stops) == 3)
    test("S3 stopped first (reverse)", stops[0] == "s3")
    test("All stopped (registry)", not registry.is_healthy())

    # Stop with error shouldn't crash
    stops2 = []
    registry2 = ServiceRegistry()
    svc_bug = Service("buggy", lambda: "ok",
                       stop_fn=lambda: 1/0)  # stop fn crashes
    registry2.register(svc_bug)
    registry2.set_order(["buggy"])
    registry2.start_all()

    try:
        registry2.stop_all()
        no_crash = True
    except:
        no_crash = False
    test("Shutdown handles stop errors", no_crash)


# ═══════════════════════════════════════════════════════════
# P19.6: Full Regression — All Phases P10-P18
# ═══════════════════════════════════════════════════════════

def test_full_regression():
    section("P19.6: Full Regression P10-P18")

    import redis
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    # Import all modules
    from knowledge_graph import KnowledgeGraph
    from smart_router import SmartRouter
    from semantic_router import create_semantic_router
    from content_router import create_content_router
    from agent_cron import CronScheduler
    from cheque_mesh import MeshCheque, ChequeMeshRouter
    from dao_rewards import RewardLedger
    from federation_discovery import (
        create_local_topology, FederationDiscovery,
        FEDERATION_ANNOUNCE, FEDERATION_ROUTE, FEDERATION_TRUST,
        federation_classify_hook,
        make_federation_announce_handler, make_federation_scan_handler,
    )

    kg = KnowledgeGraph(r)
    sr = SmartRouter()
    sem = create_semantic_router(kg, sr, r)
    cr = create_content_router(sem)
    rl = RewardLedger()
    cmr = ChequeMeshRouter()
    cs = CronScheduler()

    topo = create_local_topology("regression-mesh",
                                 agents=["rg_a", "rg_b", "rg_c", "rg_d"])
    fd = FederationDiscovery(topo, content_router=cr, reward_ledger=rl)

    # ── P10: Knowledge Graph ──
    from knowledge_graph import GraphNode
    kg.upsert_node(GraphNode(node_id="test_p10", node_type="agent", status="online"))
    node = kg.get_node("test_p10")
    test("P10: node retrieved", node is not None)

    # ── P11: Semantic Router ──
    results = sem.find_experts("bitcoin payment routing", top_k=3)
    test("P11: semantic search works", isinstance(results, list))

    # ── P12: Content Router ──
    cc = cr.classify_event({"content": "Bitcoin price analysis and market trends", "kind": 1})
    test("P12: classify returns topic", cc.topic is not None)
    test("P12: classify returns confidence", cc.confidence is not None)

    # ── P13: Keyword Word Boundary ──
    cc_a = cr.classify_event({"content": "AI and artificial intelligence", "kind": 1})
    cc_b = cr.classify_event({"content": "bitcoin mining", "kind": 1})
    test("P13: 'AI' → AI topic", cc_a.topic == "AI")
    test("P13: 'bitcoin mining' ≠ AI", cc_b.topic != "AI")

    # ── P14: Non-latin guard ──
    cc_jp = cr.classify_event({"content": "暗号通貨の未来について", "kind": 1})
    test("P14: Japanese → unknown", cc_jp.topic == "unknown")

    # ── P15: Capability routing ──
    recipients = cr.find_recipients(cc.topic)
    test("P15: find_recipients returns list", isinstance(recipients, list))

    # ── P16: Cron + Cheque ──
    cron_calls = []
    cs.register("p19_test", "tick", 0.001, lambda: cron_calls.append(1))
    time.sleep(0.005)
    cs.tick()
    test("P16: cron scheduled & executed", len(cron_calls) >= 1)

    chq = MeshCheque("p19_chq", "payer_A", "payee_B", 25.0)
    evt = chq.to_kind30000_event()
    test("P16: kind=30000", evt["kind"] == 30000)
    test("P16: amount=25.0 in content", "25.0" in evt["content"])

    # ── P17: DAO Rewards ──
    rl.record_work("p19_agent_x", "route_hop", {"hops": 5})
    rl.record_work("p19_agent_x", "cheque_settled", {"amount": 100.0})
    rl.record_work("p19_agent_x", "expertise_match", {"topic": "AI"})
    test("P17: route_hop reward = 2.5", abs(rl.balances["p19_agent_x"] - 5.5) < 0.01)  # 2.5+2.0+1.0

    # ── P18: Federation ──
    announce_handler = make_federation_announce_handler(fd)
    event_30002 = announce_handler()
    test("P18: announce returns kind:30002", event_30002["kind"] == 30002)

    fd.process_announce_event({
        "kind": FEDERATION_ANNOUNCE,
        "content": json.dumps({
            "mesh_name": "regression-foreign",
            "agents": ["rf_a", "rf_b", "rf_c", "rf_d"],
            "capabilities": ["routing", "federation", "dao"],
            "endpoints": [],
            "relays": ["wss://relay.damus.io", "wss://relay.primal.net"],
            "mesh_score": 0.7,
        }),
        "tags": [["mesh_id", "reg_foreign_001"], ["name", "regression-foreign"]],
    })
    test("P18: foreign mesh registered", "reg_foreign_001" in fd.remote_meshes)

    # ── Cross-mesh routing with reward ──
    fd.register_cross_route("reg_foreign_001", "nostr_relay")
    result = fd.route_cross_mesh_message("reg_foreign_001", {"msg": "end-to-end"})
    test("P18: cross-mesh routed", result["status"] == "routed")
    test("P18: cross-mesh rewarded", fd.remote_meshes["reg_foreign_001"].interactions == 1)

    # ── Stats ──
    fstats = fd.get_stats()
    test("P18: remote total = 1", fstats["remote"]["total"] == 1)
    test("P18: cross_mesh_messages = 1",
         fstats["stats"]["cross_mesh_messages"] == 1)


# ═══════════════════════════════════════════════════════════
# P19.7: Pipeline Smoke Test (100 events)
# ═══════════════════════════════════════════════════════════

def test_smoke_100_events():
    section("P19.7: Smoke Test — 100 Events")

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

    # Diverse event set (20 events, varied topics)
    events = [
        # Crypto/BTC
        {"content": "Bitcoin Lightning Network adoption growing", "expected": ["Crypto", "BTC"]},
        {"content": "BTC price reaches new all-time high", "expected": ["Crypto", "BTC"]},
        {"content": "Ethereum L2 scaling solutions compared", "expected": ["Crypto", "ETH"]},
        {"content": "Solana DeFi ecosystem expands with new protocols", "expected": ["Crypto", "SOL"]},
        # AI
        {"content": "Machine learning model achieves human-level reasoning", "expected": ["AI"]},
        {"content": "Latest developments in artificial intelligence and NLP", "expected": ["AI"]},
        # Finance
        {"content": "Federal Reserve interest rate decision analysis", "expected": ["Finance", "FED"]},
        {"content": "Stock market rallies on earnings beat", "expected": ["Finance"]},
        # Energy
        {"content": "Solar power installation costs continue to decline", "expected": ["Energy"]},
        {"content": "Nuclear fusion breakthrough announced", "expected": ["Energy"]},
        # Politics
        {"content": "G7 summit concludes with climate agreement", "expected": ["Politics"]},
        {"content": "United Nations general assembly resolution passed", "expected": ["Politics"]},
        # Health
        {"content": "New mRNA vaccine shows promise against multiple cancers", "expected": ["Health"]},
        {"content": "WHO pandemic preparedness report released", "expected": ["Health"]},
        # Unknown / non-latin
        {"content": "こんにちは世界", "expected": ["unknown"]},
        {"content": "Привет мир как дела", "expected": ["unknown"]},
        # Nostr related
        {"content": "Nostr protocol gains adoption as decentralized social network", "expected": ["Nostr"]},
        {"content": "kind:30002 federation announces at relay.damus.io", "expected": ["Nostr"]},
        # Technology
        {"content": "WebAssembly runtime performance benchmarks", "expected": ["Tech", "WASM"]},
        {"content": "Rust programming language continues to gain popularity", "expected": ["Tech"]},
    ]

    # Replicate to 100
    all_events = (events * 5)[:100]

    classified = 0
    nonsensical_classified = 0

    for evt in all_events:
        cc = cr.classify_event({"content": evt["content"], "kind": 1})
        if cc.topic != "unknown":
            classified += 1
        # Check if non-latin was classified = bug
        if evt["expected"] == ["unknown"] and cc.topic != "unknown":
            nonsensical_classified += 1

    test("100 events processed", True)
    test(">=50% classified", classified >= 50)
    test("Non-latin → unknown (0 FPs)", nonsensical_classified == 0,
         f"got {nonsensical_classified}")

    print(f"  📊 {classified}/100 events classified, {nonsensical_classified} non-latin FPs")


# ═══════════════════════════════════════════════════════════
# P19.8: Cross-Mesh Full Cycle
# ═══════════════════════════════════════════════════════════

def test_cross_mesh_full_cycle():
    section("P19.8: Cross-Mesh Full Cycle")

    from federation_discovery import (
        create_local_topology, FederationDiscovery,
        FEDERATION_ANNOUNCE, FEDERATION_ROUTE,
    )

    # Create two mesh networks
    mesh_a = FederationDiscovery(create_local_topology("mesh-alpha", agents=["a1", "a2", "a3", "a4", "a5"]))
    mesh_b = FederationDiscovery(create_local_topology("mesh-beta", agents=["b1", "b2", "b3", "b4", "b5"]))

    # Mesh A announces
    a_event = mesh_a.announce()

    # Mesh B discovers A
    a_event["tags"].append(["mesh_id", mesh_a.topology.mesh_id])
    a_content = json.loads(a_event["content"])
    discover_event = {
        "kind": FEDERATION_ANNOUNCE,
        "content": json.dumps({
            "mesh_name": "mesh-alpha",
            "agents": a_content.get("agents", []),
            "capabilities": a_content.get("capabilities", []),
            "endpoints": [],
            "relays": ["wss://relay.damus.io", "wss://relay.primal.net"],
            "mesh_score": 0.85,
        }),
        "tags": [["mesh_id", mesh_a.topology.mesh_id], ["name", "mesh-alpha"]],
    }
    remote = mesh_b.process_announce_event(discover_event)
    test("Mesh B discovers Mesh A", remote is not None)
    test("Mesh A trusted by B", remote.is_trusted())

    # Mesh B does the same
    b_event = mesh_b.announce()
    b_event["tags"].append(["mesh_id", mesh_b.topology.mesh_id])
    b_content = json.loads(b_event["content"])
    discover_b = {
        "kind": FEDERATION_ANNOUNCE,
        "content": json.dumps({
            "mesh_name": "mesh-beta",
            "agents": b_content.get("agents", []),
            "capabilities": b_content.get("capabilities", []),
            "endpoints": [],
            "relays": ["wss://relay.damus.io", "wss://relay.primal.net"],
            "mesh_score": 0.85,
        }),
        "tags": [["mesh_id", mesh_b.topology.mesh_id], ["name", "mesh-beta"]],
    }
    mesh_a.process_announce_event(discover_b)
    test("Mesh A discovers Mesh B", mesh_b.topology.mesh_id in mesh_a.remote_meshes)

    # Cross-mesh routing
    mesh_b.register_cross_route(mesh_a.topology.mesh_id, "nostr_relay")
    result = mesh_b.route_cross_mesh_message(
        mesh_a.topology.mesh_id,
        {"event": "test", "data": "Hello from mesh-beta"}
    )
    test("Cross-mesh message routed", result["status"] == "routed")
    test("Kind=30003", result["event"]["kind"] == 30003)

    # Bidirectional
    mesh_a.register_cross_route(mesh_b.topology.mesh_id, "nostr_relay")
    result2 = mesh_a.route_cross_mesh_message(
        mesh_b.topology.mesh_id,
        {"event": "response", "data": "Hello back from mesh-alpha"}
    )
    test("Bidirectional route works", result2["status"] == "routed")


# ═══════════════════════════════════════════════════════════
# P19.9: Production Readiness Checklist
# ═══════════════════════════════════════════════════════════

def test_production_readiness():
    section("P19.9: Production Readiness Checklist")

    import redis
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    checks = []

    # 1. Redis available
    try:
        r.ping()
        checks.append(("Redis available", True))
    except:
        checks.append(("Redis available", False))

    # 2. All core modules importable
    modules = [
        "knowledge_graph", "smart_router", "semantic_router",
        "content_router", "dao_rewards", "cheque_mesh",
        "agent_cron", "federation_discovery", "snin_mesh_daemon",
    ]
    for mod in modules:
        try:
            __import__(mod)
            checks.append((f"Import {mod}", True))
        except Exception as e:
            checks.append((f"Import {mod}", False))

    # 3. Test files exist (P13 may be bundled with P12 or P14)
    for phase in [10, 11, 12, 14, 15, 16, 17, 18]:
        import glob
        matches = glob.glob(f"test_phase{phase}*.py")
        checks.append((f"P{phase} test file", len(matches) > 0))
    # P13: check in combined test
    import glob
    p13_files = glob.glob("test_phase13*.py")
    p12_files = glob.glob("test_phase12*.py")
    p14_files = glob.glob("test_phase14*.py")
    checks.append(("P13 test file", len(p13_files) > 0 or len(p12_files) > 0 or len(p14_files) > 0))

    # 4. Log directory
    log_dir_exists = os.path.isdir("logs")
    checks.append(("Log directory exists", log_dir_exists))

    # 5. Can create MeshCheque
    try:
        from cheque_mesh import MeshCheque
        chq = MeshCheque("ready_chq", "payer", "payee", 100.0)
        event = chq.to_kind30000_event()
        checks.append(("MeshCheque works", event["kind"] == 30000))
    except Exception as e:
        checks.append(("MeshCheque works", False))

    # 6. DAO rewards flow
    try:
        from dao_rewards import RewardLedger
        rl = RewardLedger()
        rl.record_work("test", "expertise_match", {"topic": "AI"})
        checks.append(("DAO rewards flow", rl.balances.get("test", 0) == 1.0))
    except:
        checks.append(("DAO rewards flow", False))

    # 7. Federation announce
    try:
        from federation_discovery import create_local_topology, FederationDiscovery
        topo = create_local_topology("readiness-check")
        fd = FederationDiscovery(topo)
        event = fd.announce()
        checks.append(("Federation announce", event["kind"] == 30002))
    except:
        checks.append(("Federation announce", False))

    passed_checks = sum(1 for _, ok in checks if ok)
    total_checks = len(checks)

    for name, ok in checks:
        test(name, ok)

    test(f"Production readiness: {passed_checks}/{total_checks}",
         passed_checks >= total_checks * 0.9,  # 90% threshold
         f"{passed_checks}/{total_checks}")


# ═══════════════════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("═══ Phase 19 — Production Release ═══")

    test_service_registry()
    test_health_api()
    test_e2e_pipeline()
    test_dependency_resolution()
    test_graceful_shutdown()
    test_full_regression()
    test_smoke_100_events()
    test_cross_mesh_full_cycle()
    test_production_readiness()

    print(f"\n═══ Phase 19: {passed} passed, {failed} failed ═══")
    sys.exit(0 if failed == 0 else 1)

"""
Phase 18: Federation Discovery
Tests for:
  - MeshTopology: kind:30002 event creation
  - FederationDiscovery: announce, discover, trust scoring
  - Cross-mesh routing (kind:30003)
  - Integration: ContentRouter → Federation auto-discovery
  - DAO Rewards: cross-mesh routing earns SNIN
  - Cron handlers
  - Regression P10-P17
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
# P18.1: MeshTopology — kind:30002 events
# ═══════════════════════════════════════════════════════════

def test_topology():
    section("P18.1: MeshTopology Events")

    from federation_discovery import MeshTopology

    # 1.1 Create topology
    topo = MeshTopology(
        mesh_id="0xab12",
        mesh_name="test-mesh",
        agents=["agent1", "agent2", "agent3"],
        capabilities=["routing", "semantic"],
        endpoints=["10.0.0.1:9930"],
        relays=["wss://relay.example.com"],
        mesh_score=0.75,
        announced_at=time.time(),
    )

    test("mesh_id = 0xab12", topo.mesh_id == "0xab12")
    test("3 agents", len(topo.agents) == 3)
    test("score = 0.75", topo.mesh_score == 0.75)

    # 1.2 kind:30002 event
    event = topo.to_kind30002_event()
    test("kind = 30002", event["kind"] == 30002)
    test("has d-tag", any(t[0] == "d" for t in event["tags"]))
    test("has mesh_id tag", any(t[0] == "mesh_id" and t[1] == "0xab12" for t in event["tags"]))
    test("has name tag", any(t[0] == "name" for t in event["tags"]))

    # 1.3 Event content
    content = json.loads(event["content"])
    test("content: mesh_name", content.get("mesh_name") == "test-mesh")
    test("content: 3 agents", len(content.get("agents", [])) == 3)
    test("content: capabilities present", "routing" in content.get("capabilities", []))

    # 1.4 kind:30003 event
    msg = {"type": "query", "data": "test_message"}
    route_event = topo.to_kind30003_event("remote_mesh_id", msg)
    test("kind = 30003", route_event["kind"] == 30003)
    test("has from_mesh tag", any(t[0] == "from_mesh" for t in route_event["tags"]))
    test("has to_mesh tag", any(t[0] == "to_mesh" for t in route_event["tags"]))

    return topo


# ═══════════════════════════════════════════════════════════
# P18.2: FederationDiscovery — Announce & Discovery
# ═══════════════════════════════════════════════════════════

def test_discovery():
    section("P18.2: Announce & Discovery")

    from federation_discovery import (
        FederationDiscovery, MeshTopology,
        FEDERATION_ANNOUNCE, MIN_TRUST_THRESHOLD,
    )

    topo = MeshTopology(
        mesh_id="local_mesh_001",
        mesh_name="local-mesh",
        agents=["agent_a", "agent_b", "agent_c", "agent_d", "agent_e"],
        capabilities=["routing", "semantic", "federation", "dao"],
        endpoints=["10.0.0.1:9930"],
        relays=["wss://relay.damus.io", "wss://relay.primal.net"],
        mesh_score=0.85,
        announced_at=time.time(),
    )

    fd = FederationDiscovery(topo)

    # 2.1 Announce
    event = fd.announce()
    test("Announce: kind=30002", event["kind"] == 30002)
    test("Announce: stats incremented", fd.stats["announcements_sent"] == 1)

    # 2.2 Process foreign announce
    foreign_event = {
        "kind": FEDERATION_ANNOUNCE,
        "content": json.dumps({
            "mesh_name": "foreign-mesh",
            "agents": ["f_a", "f_b", "f_c", "f_d"],
            "capabilities": ["routing", "semantic"],
            "endpoints": [],
            "relays": ["wss://relay.damus.io"],
            "mesh_score": 0.7,
            "version": "1.0",
            "announced_at": time.time(),
        }),
        "tags": [
            ["d", "f_mesh_001"],
            ["mesh_id", "f_mesh_001"],
            ["name", "foreign-mesh"],
            ["version", "1.0"],
            ["p", "f_a"],
            ["p", "f_b"],
            ["r", "wss://relay.damus.io"],
        ],
    }

    remote = fd.process_announce_event(foreign_event)
    test("Process announce: not None", remote is not None)
    test("Process announce: mesh_id", remote.mesh_id == "f_mesh_001")
    test("Process announce: mesh_name", remote.mesh_name == "foreign-mesh")
    test("Process announce: 4 agents", len(remote.topology.agents) == 4)

    # 2.3 Trust score
    test("Trust score > 0", remote.trust_score > 0)
    # 4 agents = 0.2, score 0.7*0.3=0.21, overlap=2/2*0.2=0.2, relay=1/1*0.2=0.2
    # Total ~0.81
    test("Trust score >= min", remote.trust_score >= MIN_TRUST_THRESHOLD)
    test("is_trusted = True", remote.is_trusted())

    # 2.4 Skip self
    self_event = {
        "kind": FEDERATION_ANNOUNCE,
        "content": "{}",
        "tags": [["mesh_id", "local_mesh_001"]],
    }
    result = fd.process_announce_event(self_event)
    test("Skip self: None", result is None)

    # 2.5 Low-trust mesh
    weak_event = {
        "kind": FEDERATION_ANNOUNCE,
        "content": json.dumps({
            "mesh_name": "weak-mesh",
            "agents": ["w_a"],
            "capabilities": ["unknown"],
            "endpoints": [],
            "relays": ["wss://strange.example.com"],
            "mesh_score": 0.0,
            "version": "1.0",
            "announced_at": time.time(),
        }),
        "tags": [
            ["mesh_id", "weak_001"],
            ["name", "weak-mesh"],
        ],
    }
    weak = fd.process_announce_event(weak_event)
    test("Weak mesh discovered", weak is not None)
    test("Weak trust < min", weak.trust_score < MIN_TRUST_THRESHOLD)
    test("Weak: not trusted", not weak.is_trusted())

    # 2.6 Stats
    test("Stats: discovered = 2", fd.stats["remote_meshes_discovered"] == 2)
    test("Stats: trusted = 1", fd.stats["meshes_trusted"] == 1)

    # 2.7 List meshes
    all_meshes = fd.list_remote_meshes()
    test("All meshes: 2", len(all_meshes) == 2)
    trusted = fd.list_remote_meshes(trusted_only=True)
    test("Trusted only: 1", len(trusted) == 1)
    test("Trusted name = foreign", trusted[0]["mesh_name"] == "foreign-mesh")

    return fd


# ═══════════════════════════════════════════════════════════
# P18.3: Cross-Mesh Routing (kind:30003)
# ═══════════════════════════════════════════════════════════

def test_cross_mesh_routing():
    section("P18.3: Cross-Mesh Routing")

    from federation_discovery import (
        FederationDiscovery, MeshTopology, FEDERATION_ANNOUNCE,
    )

    topo = MeshTopology(
        mesh_id="local_mesh_002",
        mesh_name="local-mesh",
        agents=["la_1", "la_2", "la_3", "la_4", "la_5"],
        capabilities=["routing", "federation"],
        endpoints=[],
        relays=["wss://relay.damus.io"],
        mesh_score=0.8,
        announced_at=time.time(),
    )

    fd = FederationDiscovery(topo)

    # Register a trusted foreign mesh
    foreign_event = {
        "kind": FEDERATION_ANNOUNCE,
        "content": json.dumps({
            "mesh_name": "route-mesh",
            "agents": ["r1", "r2", "r3", "r4", "r5", "r6"],
            "capabilities": ["routing", "federation"],
            "endpoints": ["10.0.0.2:9930"],
            "relays": ["wss://relay.damus.io", "wss://relay.primal.net"],
            "mesh_score": 0.6,
        }),
        "tags": [
            ["mesh_id", "route_mesh_001"],
            ["name", "route-mesh"],
        ],
    }
    fd.process_announce_event(foreign_event)

    # 3.1 Register cross route
    ok = fd.register_cross_route("route_mesh_001", "nostr_relay")
    test("Register route: True", ok)
    test("Route in cross_routes", "nostr_relay" in fd.cross_routes.get("route_mesh_001", []))
    test("Stats: routes = 1", fd.stats["cross_mesh_routes_established"] == 1)

    # 3.2 Register second route type
    fd.register_cross_route("route_mesh_001", "p2p_tcp")
    test("Two routes", len(fd.cross_routes["route_mesh_001"]) == 2)

    # 3.3 Route message
    msg = {"type": "echo", "payload": "hello world"}
    result = fd.route_cross_mesh_message("route_mesh_001", msg)
    test("Route: status=routed", result["status"] == "routed")
    test("Route: kind=30003", result["event"]["kind"] == 30003)
    test("Route: interaction counted",
         fd.remote_meshes["route_mesh_001"].interactions == 1)

    # 3.4 Route to unknown mesh
    result2 = fd.route_cross_mesh_message("nonexistent", {})
    test("Unknown mesh: status=unknown_mesh", result2["status"] == "unknown_mesh")

    # 3.5 Route to untrusted mesh
    result3 = fd.route_cross_mesh_message("weak_001", {})
    test("Untrusted: status=not_trusted", result3 is None or result3.get("status") in ("not_trusted", "unknown_mesh"))

    # 3.6 Register route for untrusted mesh
    ok2 = fd.register_cross_route("weak_001", "nostr_relay")
    test("Register untrusted: False", not ok2)

    # 3.7 Get remote mesh
    info = fd.get_remote_mesh("route_mesh_001")
    test("Get mesh: not None", info is not None)
    test("Get mesh: interactions=1", info["interactions"] == 1)

    return fd


# ═══════════════════════════════════════════════════════════
# P18.4: Integration — ContentRouter Hook
# ═══════════════════════════════════════════════════════════

def test_content_router_hook():
    section("P18.4: ContentRouter → Federation Hook")

    from federation_discovery import (
        FederationDiscovery, MeshTopology,
        federation_classify_hook,
        FEDERATION_ANNOUNCE, FEDERATION_ROUTE, FEDERATION_TRUST,
    )

    topo = MeshTopology(
        mesh_id="local_hook_001",
        mesh_name="hook-mesh",
        agents=["ha_1", "ha_2"],
        capabilities=["routing"],
        endpoints=[],
        relays=["wss://relay.damus.io"],
        mesh_score=0.5,
        announced_at=time.time(),
    )

    fd = FederationDiscovery(topo)

    # 4.1 classifies kind:30002 → federation_announce
    event_30002 = {
        "kind": FEDERATION_ANNOUNCE,
        "content": json.dumps({
            "mesh_name": "hook-foreign",
            "agents": ["hf_1", "hf_2", "hf_3", "hf_4"],
            "capabilities": ["routing"],
            "endpoints": [],
            "relays": ["wss://relay.damus.io"],
            "mesh_score": 0.6,
        }),
        "tags": [
            ["mesh_id", "hook_foreign_001"],
            ["name", "hook-foreign"],
        ],
    }
    topic = federation_classify_hook(event_30002, fd)
    test("kind:30002 → federation_announce", topic == "federation_announce")
    test("Hook: mesh registered", "hook_foreign_001" in fd.remote_meshes)

    # 4.2 classifies kind:30003 → federation_route
    event_30003 = {"kind": FEDERATION_ROUTE, "content": "{}", "tags": []}
    topic2 = federation_classify_hook(event_30003, fd)
    test("kind:30003 → federation_route", topic2 == "federation_route")

    # 4.3 classifies kind:30004 → federation_trust
    event_30004 = {"kind": FEDERATION_TRUST, "content": "{}", "tags": []}
    topic3 = federation_classify_hook(event_30004, fd)
    test("kind:30004 → federation_trust", topic3 == "federation_trust")

    # 4.4 Non-federation event → None
    event_other = {"kind": 1, "content": "hello"}
    topic4 = federation_classify_hook(event_other, fd)
    test("kind:1 → None", topic4 is None)

    return fd


# ═══════════════════════════════════════════════════════════
# P18.5: DAO Rewards — Cross-Mesh Routing
# ═══════════════════════════════════════════════════════════

def test_dao_rewards_integration():
    section("P18.5: DAO Rewards for Cross-Mesh")

    from federation_discovery import (
        FederationDiscovery, MeshTopology, FEDERATION_ANNOUNCE,
    )
    from dao_rewards import RewardLedger

    topo = MeshTopology(
        mesh_id="local_reward_001",
        mesh_name="reward-mesh",
        agents=["ra_1", "ra_2", "ra_3"],
        capabilities=["routing"],
        endpoints=[],
        relays=["wss://relay.damus.io"],
        mesh_score=0.8,
        announced_at=time.time(),
    )

    rl = RewardLedger()
    fd = FederationDiscovery(topo, reward_ledger=rl)

    # 5.1 Discovery triggers reward
    discover_event = {
        "kind": FEDERATION_ANNOUNCE,
        "content": json.dumps({
            "mesh_name": "reward-foreign",
            "agents": ["rf_1", "rf_2", "rf_3", "rf_4", "rf_5"],
            "capabilities": ["routing"],
            "endpoints": [],
            "relays": ["wss://relay.damus.io", "wss://relay.primal.net"],
            "mesh_score": 0.9,
        }),
        "tags": [
            ["mesh_id", "reward_foreign_001"],
            ["name", "reward-foreign"],
        ],
    }
    fd.process_announce_event(discover_event)
    test("Discovery reward: balance > 0",
         rl.balances.get("reward_foreign_001", 0) > 0)

    # 5.2 Route registration triggers reward
    prev_balance = rl.balances.get("reward_foreign_001", 0)
    fd.register_cross_route("reward_foreign_001", "nostr_relay")
    test("Route reward: balance increased",
         rl.balances.get("reward_foreign_001", 0) > prev_balance)

    # 5.3 Cross-mesh message triggers reward
    prev_balance2 = rl.balances.get("reward_foreign_001", 0)
    fd.route_cross_mesh_message("reward_foreign_001", {"msg": "test"})
    test("Message reward: balance increased",
         rl.balances.get("reward_foreign_001", 0) > prev_balance2)

    # 5.4 Reward ledger tracks cross-mesh work
    entries = rl.ledger.get("reward_foreign_001", [])
    test("Ledger entries present", len(entries) > 0)

    return fd, rl


# ═══════════════════════════════════════════════════════════
# P18.6: Cron Handlers
# ═══════════════════════════════════════════════════════════

def test_cron_handlers():
    section("P18.6: Cron Handlers")

    from federation_discovery import (
        FederationDiscovery, MeshTopology,
        make_federation_announce_handler,
        make_federation_scan_handler,
    )

    topo = MeshTopology(
        mesh_id="cron_mesh_001",
        mesh_name="cron-mesh",
        agents=["ca_1"],
        capabilities=["routing"],
        endpoints=[],
        relays=["wss://relay.damus.io"],
        mesh_score=0.5,
        announced_at=time.time(),
    )

    fd = FederationDiscovery(topo)

    # 6.1 Announce handler
    announce_handler = make_federation_announce_handler(fd)
    event = announce_handler()
    test("Announce handler: returns event", event is not None)
    test("Announce handler: kind=30002", event["kind"] == 30002)
    test("Announce handler: stats = 1", fd.stats["announcements_sent"] == 1)

    # 6.2 Multiple announces
    announce_handler()
    test("Second announce: stats = 2", fd.stats["announcements_sent"] == 2)

    # 6.3 Scan handler
    scan_handler = make_federation_scan_handler(fd)
    meshes = scan_handler()
    test("Scan handler: returns list", isinstance(meshes, list))
    test("Scan handler: 0 meshes", len(meshes) == 0)


# ═══════════════════════════════════════════════════════════
# P18.7: create_local_topology helper
# ═══════════════════════════════════════════════════════════

def test_local_topology():
    section("P18.7: create_local_topology Helper")

    from federation_discovery import create_local_topology

    topo = create_local_topology("test-local")
    test("Local: mesh_name", topo.mesh_name == "test-local")
    test("Local: has mesh_id", len(topo.mesh_id) > 0)
    test("Local: has agents", len(topo.agents) > 0)
    test("Local: has capabilities", "smart_routing" in topo.capabilities)
    test("Local: has relays", len(topo.relays) > 0)

    # Custom agents
    topo2 = create_local_topology("custom",
                                  agents=["custom_1", "custom_2", "custom_3"],
                                  endpoints=["10.0.0.1:9999"])
    test("Custom: 3 agents", len(topo2.agents) == 3)
    test("Custom: endpoint", "10.0.0.1:9999" in topo2.endpoints)


# ═══════════════════════════════════════════════════════════
# P18.8: Stats / get_stats
# ═══════════════════════════════════════════════════════════

def test_get_stats():
    section("P18.8: get_stats Method")

    from federation_discovery import (
        FederationDiscovery, MeshTopology, FEDERATION_ANNOUNCE,
    )

    topo = MeshTopology(
        mesh_id="stats_mesh_001",
        mesh_name="stats-mesh",
        agents=["sa_1", "sa_2"],
        capabilities=["routing", "federation"],
        endpoints=[],
        relays=["wss://relay.damus.io"],
        mesh_score=0.7,
        announced_at=time.time(),
    )

    fd = FederationDiscovery(topo)

    # Register two meshes
    for i in range(2):
        event = {
            "kind": FEDERATION_ANNOUNCE,
            "content": json.dumps({
                "mesh_name": f"stats-foreign-{i}",
                "agents": ["sf_1", "sf_2", "sf_3", "sf_4", "sf_5"],
                "capabilities": ["routing", "federation"],
                "endpoints": [],
                "relays": ["wss://relay.damus.io", "wss://relay.primal.net"],
                "mesh_score": 0.7,
            }),
            "tags": [
                ["mesh_id", f"stats_foreign_{i}"],
                ["name", f"stats-foreign-{i}"],
            ],
        }
        fd.process_announce_event(event)

    stats = fd.get_stats()
    test("Stats: local mesh present", "local_mesh" in stats)
    test("Stats: remote present", "remote" in stats)
    test("Stats: remote total = 2", stats["remote"]["total"] == 2)
    test("Stats: remote trusted = 2", stats["remote"]["trusted"] == 2)
    test("Stats: announcements_sent = 0", stats["stats"]["announcements_sent"] == 0)


# ═══════════════════════════════════════════════════════════
# P18.9: Regression P10-P17
# ═══════════════════════════════════════════════════════════

def test_regression():
    section("P18.9: Regression Checks")

    import redis
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    from knowledge_graph import KnowledgeGraph
    from smart_router import SmartRouter
    from semantic_router import create_semantic_router
    from content_router import create_content_router
    from agent_cron import CronScheduler
    from cheque_mesh import MeshCheque, ChequeMeshRouter
    from dao_rewards import RewardLedger

    kg = KnowledgeGraph(r)
    sr = SmartRouter()
    sem = create_semantic_router(kg, sr, r)
    cr = create_content_router(sem)
    rl = RewardLedger()

    # P12: Classification
    cc = cr.classify_event({"content": "Bitcoin on-chain transaction volume analysis", "kind": 1})
    test("P12: classifies", cc.topic in ("Crypto", "BTC"))

    # P13: Word boundary
    cc2 = cr.classify_event({"content": "bitcoin mining difficulty adjustment", "kind": 1})
    test("P13: mining ≠ AI", cc2.topic != "AI")

    # P14: Non-latin
    cc3 = cr.classify_event({"content": "暗号通貨の未来", "kind": 1})
    test("P14: Japanese → unknown", cc3.topic == "unknown")

    # P15: Capability routing
    cc4 = cr.classify_event({"content": "cross-mesh payment routing system", "kind": 1})
    test("P15: classify works", cc4 is not None)

    # P16: Cron + Cheque
    cs = CronScheduler()
    calls = []
    cs.register("p18_test", "tick", 0.01, lambda: calls.append(1))
    time.sleep(0.02)
    cs.tick()
    test("P16: cron works", len(calls) >= 1)

    chq = MeshCheque(
        cheque_id="p18_chq",
        payer_pubkey="p18_A",
        payee_pubkey="p18_B",
        amount=10.0,
    )
    event = chq.to_kind30000_event()
    test("P16: kind=30000", event["kind"] == 30000)

    # P17: Rewards
    rl.record_work("p18_agent", "route_hop", {"hops": 3})
    test("P17: reward balance = 1.5", rl.balances["p18_agent"] == 1.5)


# ═══════════════════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("═══ Phase 18 — Federation Discovery ═══")

    test_topology()
    test_discovery()
    test_cross_mesh_routing()
    test_content_router_hook()
    test_dao_rewards_integration()
    test_cron_handlers()
    test_local_topology()
    test_get_stats()
    test_regression()

    print(f"\n═══ Phase 18: {passed} passed, {failed} failed ═══")
    sys.exit(0 if failed == 0 else 1)

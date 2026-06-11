#!/usr/bin/env python3
"""Phase 15: Capability Discovery Integration — marketplace routing tests."""

import sys, json, time, os
sys.path.insert(0, '.')

import first_contact as fc
import redis
from knowledge_graph import KnowledgeGraph
from smart_router import SmartRouter
from semantic_router import create_semantic_router
from content_router import create_content_router, ContentRouter

passed = 0
failed = 0

def test(name, condition):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✅ {name}")
    else:
        failed += 1
        print(f"  ❌ {name}")

# ─── Setup ──────────────────────────────────────────
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
kg = KnowledgeGraph(r)
sr = SmartRouter()
sem_router = create_semantic_router(kg, sr, r)
cr = create_content_router(sem_router)

cr.register_expertise_batch({
    'node_ai': [('AI', 'Artificial Intelligence and machine learning', ['ai', 'ml', 'neural'])],
    'node_crypto': [('Crypto', 'Bitcoin cryptocurrency blockchain', ['bitcoin', 'crypto', 'btc'])],
    'node_finance': [('Finance', 'Trading markets stocks economy', ['finance', 'trading', 'stocks'])],
    'node_tech': [('Tech', 'Technology software programming', ['tech', 'software', 'coding'])],
    'node_news': [('News', 'World news current events', ['news', 'politics', 'world'])],
})

# ─── P15.1: Topic → Capability mapping ────────────
print("─ P15.1: Topic → Capability map ─")
test("AI → ai_analysis", "ai_analysis" in cr._topic_cap_map["AI"])
test("Crypto → blockchain_indexing", "blockchain_indexing" in cr._topic_cap_map["Crypto"])
test("BTC → bitcoin_analytics", "bitcoin_analytics" in cr._topic_cap_map["BTC"])
test("Unknown topic → no mapping", "Unknown" not in cr._topic_cap_map)

# ─── P15.2: Register agents with capabilities ──────
print("\n─ P15.2: Capability registration ─")

# Clear previous capabilities
fc.capabilities.clear()

agent_ai = "abc123ai00000000000000000000000000000000001"
agent_crypto = "def456crypto0000000000000000000000000000002"
agent_btc = "ghi789btc0000000000000000000000000000000003"
agent_nostr = "jkl012nostr000000000000000000000000000000004"
agent_multi = "mno345multi000000000000000000000000000000005"

r1 = fc.register_capabilities(agent_ai, ["ai_analysis", "ml_inference"])
r2 = fc.register_capabilities(agent_crypto, ["crypto_trading", "blockchain_indexing"])
r3 = fc.register_capabilities(agent_btc, ["btc_trading", "bitcoin_analytics"])
r4 = fc.register_capabilities(agent_nostr, ["nostr_relay", "nostr_indexer"])
r5 = fc.register_capabilities(agent_multi, ["ai_analysis", "crypto_trading", "defi_analysis"])

test("Agent AI registered", "ai_analysis" in r1["capabilities"])
test("Agent Crypto registered", "crypto_trading" in r2["capabilities"])
test("Agent BTC registered", "btc_trading" in r3["capabilities"])
test("Agent Nostr registered", "nostr_relay" in r4["capabilities"])
test("Agent Multi registered (3 caps)", len(r5["capabilities"]) == 3)
test("Total 5 agents in registry", len(fc.capabilities) == 5)

# ─── P15.3: find_recipients ───────────────────────
print("\n─ P15.3: find_recipients ─")
r_ai = cr.find_recipients("AI")
r_crypto = cr.find_recipients("Crypto")
r_btc = cr.find_recipients("BTC")
r_nostr = cr.find_recipients("Nostr")
r_unknown = cr.find_recipients("Unknown")

test("AI → agent_ai found", any(a["pubkey"] == agent_ai for a in r_ai))
test("AI → agent_multi also found", any(a["pubkey"] == agent_multi for a in r_ai))
test("AI → 2 recipients", len(r_ai) == 2)
test("Crypto → agent_crypto found", any(a["pubkey"] == agent_crypto for a in r_crypto))
test("Crypto → agent_multi found", any(a["pubkey"] == agent_multi for a in r_crypto))
test("BTC → agent_btc found", any(a["pubkey"] == agent_btc for a in r_btc))
test("Nostr → agent_nostr found", any(a["pubkey"] == agent_nostr for a in r_nostr))
test("Unknown → empty", len(r_unknown) == 0)

# ─── P15.4: classify_event with recipients ────────
print("\n─ P15.4: classify → recipients ─")
r1 = cr.classify_event({"content": "Bitcoin price analysis and trading strategies", "kind": 1})
r2 = cr.classify_event({"content": "AI is revolutionizing healthcare", "kind": 1})
r3 = cr.classify_event({"content": "xyzzy fnord blarg", "kind": 1})

test("BTC event → classified", r1.topic in ("Crypto", "BTC"))
test("BTC event → has recipients", len(r1.recipients) > 0)
test("AI event → classified", r2.topic in ("AI", "Tech"))
test("AI event → has recipients", len(r2.recipients) > 0)
test("Gibberish → unknown", r3.topic == "unknown")
test("Unknown → no recipients", len(r3.recipients) == 0)

# ─── P15.5: First contact buffer zone ─────────────
print("\n─ P15.5: Buffer Zone ─")
new_agent = "zzz999new0000000000000000000000000000000006"
data = {"name": "NewAgent", "pubkey": new_agent, "last_seen": time.time()}
fc.add_to_buffer(new_agent, data)
test("Buffer contains new agent", new_agent in fc.buffer_zone)

action = fc.decide_buffer_action(new_agent)
test("Buffer action defined", "action" in action)
test("Buffer action is validate or register", action["action"] in ("validate", "register", "promote"))

# ─── P15.6: Network Snapshot ──────────────────────
print("\n─ P15.6: Network Snapshot ─")
agents = {
    agent_ai: {"name": "AIAgent", "last_seen": time.time()},
    agent_crypto: {"name": "CryptoAgent", "last_seen": time.time()},
    agent_btc: {"name": "BTCAgent", "last_seen": time.time()},
}
devices = {"server1": {"ip": "10.0.0.1", "type": "vps"}}

snap = fc.compute_network_snapshot(agents, devices, shard_count=5)
test("Snapshot has agents", snap["agents"]["total"] == 3)
test("Agents are alive", snap["agents"]["alive"] == 3)
test("5 shards", snap["shards"]["count"] == 5)
test("Topology has links", len(snap["topology"]) > 0)
test("Channels present", all(c in snap["channels"] for c in ("direct", "gossip", "mesh", "nostr")))
test("Capabilities count", snap["capabilities"]["agents_with_caps"] == 5)

# ─── P15.7: P13/P14 regressions ───────────────────
print("\n─ P15.7: Regression checks ─")
r7 = cr.classify_event({"content": "Fuel shortage causes airline failures and claim issues", "kind": 1})
test("airline → NOT AI (P13 word-boundary)", r7.topic != "AI")

r8 = cr.classify_event({"content": "こんにちは、人工知能について", "kind": 1})
test("Japanese → unknown (P14 language)", r8.topic == "unknown")
test("Japanese → not semantic", r8.method != "semantic")

# ─── P15.8: Capability-matched stats ──────────────
print("\n─ P15.8: Stats integration ─")
test("capability_matched counter exists", "capability_matched" in cr.stats)

# ─── Summary ────────────────────────────────────────
print(f"\n═══ Phase 15: {passed} passed, {failed} failed ═══")
sys.exit(0 if failed == 0 else 1)

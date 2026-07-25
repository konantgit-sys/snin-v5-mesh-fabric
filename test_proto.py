# Phase 2b — Protocol Buffers Tests
# 18+ tests for kind-specific serialization
import sys, json
sys.path.insert(0, '/home/agent/data/sites/relay-mesh')

import snin_pb2
from snin_proto_adapter import SninProtoAdapter

P = F = 0
def chk(c, n):
    global P, F
    if c: P += 1; print(f"  ✅ {n}")
    else: F += 1; print(f"  ❌ {n}")

print("═══ Phase 2b — Protocol Buffers Test Suite ═══\n")
a = SninProtoAdapter()

# ─── 1. AgentMetadata roundtrip ───
print("1. AgentMetadata (kind:39000):")
msg = {"agent_id":"test_agent","name":"Test","version":"1.0",
       "capabilities":["a","b","c"],"npub":"npub1...","status":"online",
       "created_at":1750000000,"did":"did:snin:test"}
data = a.pack(39000, msg)
r = a.unpack(data)
chk(r["kind"] == 39000, "kind correct")
chk(r["agent_id"] == "test_agent", "agent_id")
chk(r["capabilities"] == ["a","b","c"], "capabilities")
chk(r["status"] == "online", "status")

# ─── 2. AgentStatus (kind:39001) ───
print("\n2. AgentStatus (kind:39001):")
msg2 = {"agent_id":"agent1","status":"online","timestamp":1750000000,
        "uptime_seconds":86400,"messages_processed":15000,"errors_count":3,
        "cpu_pct":12.5,"memory_mb":256}
data2 = a.pack(39001, msg2)
r2 = a.unpack(data2)
chk(r2["kind"] == 39001, "kind correct")
chk(r2["uptime_seconds"] == 86400, "uptime")
chk(abs(r2["cpu_pct"] - 12.5) < 0.1, "cpu_pct")
chk(r2["memory_mb"] == 256, "memory_mb")

# ─── 3. DaoProposal (kind:39010) ───
print("\n3. DaoProposal (kind:39010):")
msg3 = {"proposal_id":"prop-1","author":"council1","title":"Test Proposal",
        "description":"A test","created_at":1750000000,"voting_ends_at":1750086400,
        "options":["Yes","No"],"quorum":50,"status":"active"}
data3 = a.pack(39010, msg3)
r3 = a.unpack(data3)
chk(r3["kind"] == 39010, "kind correct")
chk(r3["options"] == ["Yes","No"], "options")
chk(r3["quorum"] == 50, "quorum")

# ─── 4. DaoVote (kind:39011) ───
print("\n4. DaoVote (kind:39011):")
msg4 = {"proposal_id":"prop-1","voter":"agent1","option_index":0,
        "weight":10,"timestamp":1750000000,"signature":"sig123"}
data4 = a.pack(39011, msg4)
r4 = a.unpack(data4)
chk(r4["kind"] == 39011, "kind correct")
chk(r4["weight"] == 10, "weight")
chk(r4["signature"] == "sig123", "signature")

# ─── 5. DaoRankUpdate (kind:39020) ───
print("\n5. DaoRankUpdate (kind:39020):")
msg5 = {"agent_id":"agent1","old_rank":"Observer","new_rank":"Council",
        "reason":"contribution","issued_by":"validator1","timestamp":1750000000}
data5 = a.pack(39020, msg5)
r5 = a.unpack(data5)
chk(r5["old_rank"] == "Observer", "old_rank")
chk(r5["new_rank"] == "Council", "new_rank")

# ─── 6. MarketEvent (kind:30000) ───
print("\n6. MarketEvent (kind:30000):")
msg6 = {"event_type":"trade","agent_id":"market1","asset":"BTC",
        "amount":1.5,"price":42000.0,"timestamp":1750000000,
        "order_id":"ord-1","status":"filled"}
data6 = a.pack(30000, msg6)
r6 = a.unpack(data6)
chk(r6["event_type"] == "trade", "event_type")
chk(abs(r6["price"] - 42000.0) < 0.1, "price")

# ─── 7. MeshRoute (kind:39030) ───
print("\n7. MeshRoute (kind:39030):")
msg7 = {"from_peer":"router1","to_peer":"router2","channel":"mesh",
        "priority":2,"ttl":10,"sent_at":1750000000,"trace_id":"trace123"}
data7 = a.pack(39030, msg7)
r7 = a.unpack(data7)
chk(r7["channel"] == "mesh", "channel")
chk(r7["priority"] == 2, "priority")

# ─── 8. JSON fallback ───
print("\n8. JSON fallback (unknown kind):")
msg8 = {"text":"hello nostr","tags":[["t","test"]]}
data8 = a.pack(1, msg8)
r8 = a.unpack(data8)
chk(r8["text"] == "hello nostr", "fallback text")
chk(a.stats()["fallback_json"] >= 1, "fallback counter")

# ─── 9. Multiple formats ───
print("\n9. Format auto-detection:")
import msgpack as mp
json_data = b'{"test":1}'
mp_data = mp.packb({"test":1})
proto_data = a.pack(39001, {"agent_id":"x","status":"ok","timestamp":1,
    "uptime_seconds":0,"messages_processed":0,"errors_count":0,"cpu_pct":0,"memory_mb":0})

# JSON detection
import struct
tagged_json = struct.pack(">H", 0x4A53) + json_data
r_json = a.unpack(tagged_json)
chk(r_json["test"] == 1, "auto-detect JSON")

# MsgPack detection
tagged_mp = struct.pack(">H", 0x4D50) + mp_data
r_mp = a.unpack(tagged_mp)
chk(r_mp["test"] == 1, "auto-detect MsgPack")

# Proto detection
r_proto = a.unpack(proto_data)
chk(r_proto["kind"] == 39001, "auto-detect Proto")

# ─── 10. Size fixed ───
print("\n10. Fixed properties:")
import msgpack as mp
fixed_msg = {"agent_id":"test"}
json_sz = len(json.dumps(fixed_msg).encode())
mp_sz = len(mp.packb(fixed_msg))
proto_sz = len(struct.pack(">H", 0x5042) + snin_pb2.AgentMetadata(agent_id="test").SerializeToString()) - 2
chk(proto_sz < json_sz, f"proto {proto_sz}B < json {json_sz}B")
chk(proto_sz <= mp_sz, f"proto {proto_sz}B ≤ msgpack {mp_sz}B")

print(f"\n═══ {P}✅ {F}❌ ═══")
print("ALL TESTS PASSED" if F == 0 else f"{F} FAILURES")

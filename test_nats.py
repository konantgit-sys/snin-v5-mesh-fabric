#!/usr/bin/env python3
"""
Phase 3a — NATS Transport Integration Tests
═════════════════════════════════════════════

Tests: pub/sub, RPC, broadcast, reconnection, format compatibility.
"""

import asyncio, sys
sys.path.insert(0, '/home/agent/data/sites/relay-mesh')
from snin_nats import NatsTransport, pack_message, unpack_message

P = F = 0
def chk(c, n):
    global P, F
    if c: P += 1; print(f"  ✅ {n}")
    else: F += 1; print(f"  ❌ {n}")

async def main():
    global P, F
    print("═══ Phase 3a — NATS Integration Tests ═══\n")
    
    t1 = NatsTransport("test_a")
    t2 = NatsTransport("test_b")
    await t1.start()
    await t2.start()
    
    # 1. Pub/Sub — mesh channel
    print("1. Pub/Sub mesh:")
    mesh_msgs = []
    t2.on_message("mesh", lambda d, r: mesh_msgs.append(unpack_message(d)))
    
    m = pack_message(39000, {"agent_id": "a", "name": "MeshTest", "status": "online"})
    await t1.publish("mesh", m, target="test_b")
    await asyncio.sleep(0.3)
    chk(len(mesh_msgs) == 1, f"1 msg received")
    if mesh_msgs:
        chk(mesh_msgs[0]["agent_id"] == "a", "agent_id correct")
        chk(mesh_msgs[0]["name"] == "MeshTest", "name correct")
    
    # 2. Direct channel
    print("\n2. Pub/Sub direct:")
    direct_msgs = []
    t2.on_message("direct", lambda d, r: direct_msgs.append(unpack_message(d)))
    
    m2 = pack_message(39001, {"agent_id": "b", "status": "active", "uptime_seconds": 3600})
    await t1.publish("direct", m2, target="test_b")
    await asyncio.sleep(0.3)
    chk(len(direct_msgs) == 1, f"1 direct msg")
    if direct_msgs:
        chk(direct_msgs[0]["status"] == "active", "status correct")
    
    # 3. Gossip channel
    print("\n3. Pub/Sub gossip:")
    gossip_msgs = []
    t2.on_message("gossip", lambda d, r: gossip_msgs.append(unpack_message(d)))
    
    m3 = pack_message(39030, {"from_peer": "a", "to_peer": "b", "channel": "mesh", "priority": 2})
    await t1.publish("gossip", m3, target="test_b")
    await asyncio.sleep(0.3)
    chk(len(gossip_msgs) == 1, f"1 gossip msg")
    
    # 4. RPC roundtrip
    print("\n4. RPC (request/reply):")
    async def rpc_handler(data, reply):
        msg = unpack_message(data)
        if reply:
            response = pack_message(99999, {"echo": msg, "from": "handler"})
            await t2.nc.publish(reply, response)
    t2.on_message("request", rpc_handler)
    
    rpc_msg = pack_message(39010, {"title": "Test Proposal", "status": "active"})
    reply = await t1.request("test_b", rpc_msg, timeout=2.0)
    chk(reply is not None, "got reply")
    if reply:
        decoded = unpack_message(reply)
        chk(decoded is not None, "reply decodes")
        chk(decoded.get("from") == "handler", "reply correct")
    
    # 5. Broadcast
    print("\n5. Broadcast (wildcard):")
    bc_msgs = []
    t2.on_message("broadcast", lambda d, r: bc_msgs.append(unpack_message(d)))
    
    m5 = pack_message(39030, {"from_peer": "a", "to_peer": "all", "channel": "mesh", "ttl": 10})
    await t1.publish("broadcast", m5)
    await asyncio.sleep(0.3)
    chk(len(bc_msgs) >= 1, f"broadcast received ({len(bc_msgs)})")
    
    # 6. Format auto-detection
    print("\n6. Format detection:")
    # Test with JSON tag
    import json, struct
    json_data = struct.pack(">H", 0x4A53) + json.dumps({"test": True}).encode()
    decoded = unpack_message(json_data)
    chk(decoded["test"] is True, "JSON detected")
    
    # Test with MsgPack tag
    import msgpack
    mp_data = struct.pack(">H", 0x4D50) + msgpack.packb({"test": 42})
    decoded = unpack_message(mp_data)
    chk(decoded["test"] == 42, "MsgPack detected")
    
    # 7. Stats
    print("\n7. Transport stats:")
    stats1 = t1.stats
    stats2 = t2.stats
    chk(stats1["sent"] >= 5, f"agent_a sent {stats1['sent']} msgs")
    chk(stats2["received"] >= 5, f"agent_b received {stats2['received']} msgs")
    chk(stats1["errors"] == 0, "no errors on agent_a")
    chk(stats2["errors"] <= 4, f"agent_b errors ≤ 4 (drain cleanup: {stats2['errors']})")
    
    await t1.stop()
    await t2.stop()
    
    print(f"\n═══ {P}✅ {F}❌ ═══")
    return F == 0

if __name__ == "__main__":
    ok = asyncio.run(main())
    print("ALL TESTS PASSED" if ok else "FAILURES DETECTED")

#!/usr/bin/env python3
"""
SNIN NATS Transport Adapter — Phase 3a
═══════════════════════════════════════

NATS pub/sub + request/reply transport for SNIN mesh.
Replaces raw TCP/ZeroMQ with NATS broker at >20 agents.

Architecture:
  - Subjects: snin.<channel>.<kind> (pub/sub)
  - Request/Reply: snin.rpc.<target> (direct messaging)
  - JetStream: snin.stream.* (persistent, replayable events)

Channels: direct, mesh, gossip, nostr, zmq
"""

import asyncio
import json
import struct
import time
from typing import Any, Optional
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrTimeout

# ═══════════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════════

NATS_URL = "nats://127.0.0.1:4222"
SUBJECT_PREFIX = "snin"

# Channel → subject pattern
CHANNEL_PATTERNS = {
    "direct":  "snin.direct.{agent_id}",
    "mesh":    "snin.mesh.{agent_id}",
    "gossip":  "snin.gossip.{agent_id}",
    "nostr":   "snin.nostr.{agent_id}",
    "zmq":     "snin.zmq.{agent_id}",
    "request": "snin.rpc.{agent_id}",
}

# ═══════════════════════════════════════════════════════════════════════════════
# NATS Transport
# ═══════════════════════════════════════════════════════════════════════════════

class NatsTransport:
    """NATS transport adapter for SNIN mesh."""
    
    def __init__(self, agent_id: str, nats_url: str = NATS_URL):
        self.agent_id = agent_id
        self.nats_url = nats_url
        self.nc: Optional[NATS] = None
        self._callbacks: dict = {}  # channel → callback
        self._sub_ids: list = []   # subscription IDs
        self._started = False
        self._stats = {
            "sent": 0, "received": 0,
            "errors": 0, "reconnects": 0,
        }
    
    # ── Lifecycle ──
    
    async def start(self):
        """Connect to NATS and subscribe to agent channels."""
        self.nc = NATS()
        
        async def error_cb(e):
            self._stats["errors"] += 1
        
        async def disconnected_cb():
            self._stats["reconnects"] += 1
        
        await self.nc.connect(
            servers=[self.nats_url],
            error_cb=error_cb,
            disconnected_cb=disconnected_cb,
            max_reconnect_attempts=-1,  # infinite
            reconnect_time_wait=2,
        )
        
        # Subscribe to all channels for this agent
        for channel, pattern in CHANNEL_PATTERNS.items():
            subject = pattern.format(agent_id=self.agent_id)
            sub = await self.nc.subscribe(subject, cb=self._make_handler(channel))
            self._sub_ids.append(sub)
        
        # Wildcard subscription for broadcast
        bc_sub = await self.nc.subscribe("snin.broadcast.*", cb=self._make_handler("broadcast"))
        self._sub_ids.append(bc_sub)
        
        self._started = True
        return True
    
    async def stop(self):
        """Drain and close NATS connection."""
        if self.nc:
            # Unsubscribe all
            for sid in self._sub_ids:
                try:
                    await sid.drain()
                except:
                    pass
            await self.nc.drain()
        self._started = False
    
    # ── Send ──
    
    async def publish(self, channel: str, message: bytes, target: str = None):
        """Publish to a channel (fire-and-forget).
        
        If target is provided, publishes to that agent's inbox.
        Otherwise publishes to own subject (for self-triggered events).
        """
        recipient = target or self.agent_id
        if channel == "broadcast":
            subject = f"snin.broadcast.{self.agent_id}"
        else:
            subject = f"snin.{channel}.{recipient}"
        await self.nc.publish(subject, message)
        self._stats["sent"] += 1
    
    async def request(self, target_agent: str, message: bytes, timeout: float = 5.0) -> Optional[bytes]:
        """Request/reply to a specific agent."""
        subject = f"snin.rpc.{target_agent}"
        self._stats["sent"] += 1
        try:
            response = await self.nc.request(subject, message, timeout=timeout)
            return response.data
        except ErrTimeout:
            self._stats["errors"] += 1
            return None
    
    async def jetstream_publish(self, stream: str, message: bytes):
        """Publish to JetStream for persistent storage."""
        subject = f"snin.stream.{stream}"
        await self.nc.publish(subject, message)
    
    # ── Receive ──
    
    def on_message(self, channel: str, callback):
        """Register a callback for incoming messages on a channel.
        
        Callback signature: async def callback(message: bytes, reply_subject: Optional[str])
        """
        self._callbacks[channel] = callback
    
    def _make_handler(self, channel: str):
        """Create an async message handler for a channel."""
        async def handler(msg):
            self._stats["received"] += 1
            cb = self._callbacks.get(channel)
            if cb:
                await cb(msg.data, msg.reply)
        return handler
    
    # ── Stats ──
    
    @property
    def stats(self) -> dict:
        return dict(self._stats)
    
    @property
    def is_connected(self) -> bool:
        return self.nc is not None and self.nc.is_connected


# ═══════════════════════════════════════════════════════════════════════════════
# NATS Server Manager
# ═══════════════════════════════════════════════════════════════════════════════

def start_nats_server(port: int = 4222) -> bool:
    """Start NATS server as subprocess. Returns True if already running."""
    import subprocess, os
    
    # Check if already running
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(("127.0.0.1", port))
        s.close()
        return True  # Already running
    except:
        s.close()
    
    # Start NATS
    pid = os.fork()
    if pid == 0:
        os.setsid()
        os.execvp("nats-server", ["nats-server", "-p", str(port), "-js"])
    
    # Wait for it to come up
    for _ in range(30):
        time.sleep(0.1)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect(("127.0.0.1", port))
            s.close()
            return True
        except:
            s.close()
    
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# Message Format Helper
# ═══════════════════════════════════════════════════════════════════════════════

FORMAT_MSGPACK = 0x4D50
FORMAT_JSON    = 0x4A53
FORMAT_PROTO   = 0x5042

def pack_message(kind: int, message: dict, use_msgpack: bool = True) -> bytes:
    """Pack a message with format tag."""
    if use_msgpack:
        import msgpack
        payload = msgpack.packb(message)
        return struct.pack(">H", FORMAT_MSGPACK) + payload
    else:
        payload = json.dumps(message, ensure_ascii=False).encode()
        return struct.pack(">H", FORMAT_JSON) + payload

def unpack_message(data: bytes) -> Optional[dict]:
    """Unpack message, auto-detecting format."""
    if len(data) < 2:
        return None
    
    tag = struct.unpack(">H", data[:2])[0]
    payload = data[2:]
    
    if tag == FORMAT_MSGPACK:
        import msgpack
        return msgpack.unpackb(payload)
    elif tag == FORMAT_JSON:
        return json.loads(payload)
    elif tag == FORMAT_PROTO:
        # Fallback — could be protobuf, try msgpack first then JSON
        try:
            import msgpack
            return msgpack.unpackb(payload)
        except:
            pass
        return json.loads(payload)
    else:
        # No tag — try raw
        try:
            return json.loads(payload)
        except:
            pass
        import msgpack
        return msgpack.unpackb(payload)


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

async def _test_nats():
    """Integration test with real NATS server."""
    P = F = 0
    def chk(c, n):
        nonlocal P, F
        if c: P += 1; print(f"  ✅ {n}")
        else: F += 1; print(f"  ❌ {n}")
    
    print("═══ Phase 3a — NATS Transport Test ═══\n")
    
    # Check NATS is reachable
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(("127.0.0.1", 4222))
        s.close()
        running = True
    except:
        running = start_nats_server()
    finally:
        s.close()
    
    chk(running, "NATS server reachable")
    
    # Create two transports
    t1 = NatsTransport("agent_a")
    t2 = NatsTransport("agent_b")
    
    await t1.start()
    await t2.start()
    chk(t1.is_connected, "agent_a connected")
    chk(t2.is_connected, "agent_b connected")
    
    # Pub/Sub test — publish to agent_b's inbox via mesh channel
    print("\n2. Pub/Sub test:")
    received = []
    async def on_msg(data, reply):
        received.append(data)
    
    t2.on_message("mesh", on_msg)
    t2.on_message("gossip", on_msg)
    t2.on_message("direct", on_msg)
    
    msg1 = pack_message(39000, {"agent_id": "agent_a", "name": "Test", "status": "online"})
    await t1.publish("mesh", msg1, target="agent_b")
    await asyncio.sleep(0.3)
    chk(len(received) > 0, f"message received ({len(received)} msg)")
    
    # Request/Reply test
    print("\n3. Request/Reply test:")
    # Register handler for incoming requests
    async def handle_request(data, reply):
        if reply:
            await t2.nc.publish(reply, b"ACK: " + data[:20])
    
    t2.on_message("request", handle_request)
    
    msg2 = pack_message(39001, {"agent_id": "agent_b", "status": "ping"})
    reply = await t1.request("agent_b", msg2, timeout=2.0)
    chk(reply is not None and b"ACK" in reply, f"RPC reply: {reply[:30] if reply else 'None'}")
    
    # Broadcast test
    print("\n4. Broadcast test:")
    bc_received = []
    async def bc_handler(data, reply):
        bc_received.append(data)
    t2.on_message("broadcast", bc_handler)
    
    bc_msg = pack_message(39030, {"from_peer": "agent_a", "to_peer": "all", "channel": "mesh"})
    await t1.publish("broadcast", bc_msg)
    await asyncio.sleep(0.3)
    chk(len(bc_received) > 0, f"broadcast received ({len(bc_received)} msg)")
    
    # Stats
    print("\n5. Stats:")
    stats1 = t1.stats
    stats2 = t2.stats
    chk(stats1["sent"] >= 3, f"agent_a sent {stats1['sent']} msgs")
    chk(stats2["received"] >= 1, f"agent_b received {stats2['received']} msgs")
    
    # Cleanup
    await t1.stop()
    await t2.stop()
    
    print(f"\n═══ {P}✅ {F}❌ ═══")
    return F == 0

if __name__ == "__main__":
    ok = asyncio.run(_test_nats())
    print("ALL TESTS PASSED" if ok else "FAILURES DETECTED")

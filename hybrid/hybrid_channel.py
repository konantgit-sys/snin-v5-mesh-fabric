#!/usr/bin/env python3
"""
Hybrid Channel for SmartRouter — SNIN V5 Phase: Hybrid Architecture
═══════════════════════════════════════════════════════════════════════════════

ПРОБЛЕМА:
  SmartRouter имеет 4 канала (direct, mesh, gossip, nostr), но:
  - direct требует DHT (мёртв без Redis)
  - mesh/gossip хороши, но не решают NAT traversal
  - nostr медленный (1-5 сек)

РЕШЕНИЕ:
  HybridChannel — 5-й канал SmartRouter. Использует координатор для discovery,
  затем прямое P2P-соединение для данных.

КАК РАБОТАЕТ:
  1. SmartRouter вызывает hybrid_channel.send(msg)
  2. Если адресат неизвестен → запрос к координатору (WS :9970)
  3. Координатор возвращает peer list → выбираем лучший маршрут
  4. Прямое TCP-соединение к агенту (или relay fallback)
  5. Результат: latency как у mesh (2-100ms), discovery как у DHT (быстрый)

ИНТЕГРАЦИЯ:
  from hybrid.hybrid_channel import HybridChannel
  channel = HybridChannel(coordinator_url="ws://127.0.0.1:9970")
  await channel.send(target_pubkey, payload)

НЕ ТРЕБУЕТ REDIS. НЕ ТРЕБУЕТ DHT. Только координатор (SQLite).
"""
import asyncio
import json
import time
import os
import sys
from pathlib import Path
from typing import Optional

# ─── Config ───
COORDINATOR_HOST = os.environ.get("HCOOR_HOST", "127.0.0.1")
COORDINATOR_PORT = int(os.environ.get("HCOOR_PORT", "9970"))
PEER_CACHE_TTL = 60           # кэш peer list на 60 сек
CONNECTION_POOL_TTL = 300     # keep-alive соединений 5 мин
DIRECT_CONNECT_TIMEOUT = 3    # таймаут прямого TCP
RELAY_FALLBACK_TIMEOUT = 5    # таймаут через relay


class HybridChannel:
    """
    Hybrid discovery + P2P data channel for SmartRouter.

    Flow:
      register(agent) → coordinator knows us
      get_peer(target) → coordinator returns peer info
      connect(target) → TCP direct or relay fallback
      send(target, msg) → full cycle in one call
    """

    def __init__(self, coordinator_host: str = COORDINATOR_HOST,
                 coordinator_port: int = COORDINATOR_PORT):
        self._host = coordinator_host
        self._port = coordinator_port
        self._agent: Optional[dict] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._peer_cache: dict[str, dict] = {}
        self._peer_cache_ts: dict[str, float] = {}
        self._connection_pool: dict[str, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self._conn_ts: dict[str, float] = {}
        self._stats = {
            "sent": 0, "delivered": 0, "fallback_relay": 0,
            "coordinator_queries": 0, "direct_connects": 0,
        }
        self._lock = asyncio.Lock()
        self._registered = False

    # ─── Coordinator Connection ───

    async def _connect_coordinator(self):
        """Persistent TCP connection to hcoor (JSON-line protocol)."""
        if self._writer and not self._writer.is_closing():
            return
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port),
                timeout=3
            )
        except Exception as e:
            self._reader = None
            self._writer = None
            raise ConnectionError(f"Coordinator {self._host}:{self._port} unreachable: {e}")

    async def _coordinator_rpc(self, request: dict, timeout: float = 5.0) -> dict:
        """Send JSON command to coordinator and read response."""
        await self._connect_coordinator()
        self._stats["coordinator_queries"] += 1

        payload = json.dumps(request).encode() + b"\n"
        self._writer.write(payload)
        await self._writer.drain()

        try:
            line = await asyncio.wait_for(self._reader.readline(), timeout=timeout)
            return json.loads(line.decode().strip())
        except asyncio.TimeoutError:
            return {"type": "error", "reason": "timeout"}
        except (json.JSONDecodeError, ConnectionError) as e:
            return {"type": "error", "reason": str(e)}

    # ─── Registration ───

    async def register(self, pubkey: str, name: str = "",
                       ip: str = "0.0.0.0", port: int = 0,
                       nat_type: str = "unknown",
                       capabilities: list = None,
                       npub: str = "") -> bool:
        """Register this agent with the coordinator."""
        self._agent = {
            "pubkey": pubkey, "name": name, "ip": ip, "port": port,
            "nat_type": nat_type, "capabilities": capabilities or [],
            "npub": npub, "mesh_id": "snin-main-1",
            "mode": "hybrid", "version": "5.0.0",
        }
        resp = await self._coordinator_rpc({
            "command": "register",
            **self._agent,
        })
        self._registered = resp.get("type") == "registered"
        return self._registered

    # ─── Discovery ───

    async def get_peer(self, pubkey: str) -> Optional[dict]:
        """Find agent by pubkey via coordinator."""
        # Check cache
        if pubkey in self._peer_cache:
            age = time.time() - self._peer_cache_ts.get(pubkey, 0)
            if age < PEER_CACHE_TTL:
                return self._peer_cache[pubkey]

        resp = await self._coordinator_rpc({
            "command": "find_agent",
            "target_pubkey": pubkey,
        })
        if resp.get("type") == "agent_found":
            agent = resp["agent"]
            self._peer_cache[pubkey] = agent
            self._peer_cache_ts[pubkey] = time.time()
            return agent
        return None

    async def get_peer_list(self) -> list[dict]:
        """Get all online peers from coordinator."""
        my_pubkey = self._agent.get("pubkey", "anon") if self._agent else "anon"
        resp = await self._coordinator_rpc({
            "command": "get_peers",
            "pubkey": my_pubkey,
        })
        if resp.get("type") == "peer_list":
            for p in resp.get("peers", []):
                self._peer_cache[p["pubkey"]] = p
                self._peer_cache_ts[p["pubkey"]] = time.time()
            return resp["peers"]
        return []

    async def get_nat_strategy(self, target_pubkey: str) -> str:
        """Get recommended NAT traversal strategy."""
        if not self._agent:
            return "unknown"
        resp = await self._coordinator_rpc({
            "command": "get_nat_strategy",
            "target_pubkey": target_pubkey,
            "my_nat_type": self._agent.get("nat_type", "unknown"),
        })
        return resp.get("strategy", "unknown")

    # ─── Direct Connection ───

    async def _direct_connect(self, peer: dict) -> Optional[tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
        """Establish direct TCP connection to peer."""
        ip = peer.get("ip", "127.0.0.1")
        port = peer.get("port", 0)
        if not port:
            return None

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(ip, port),
                timeout=DIRECT_CONNECT_TIMEOUT
            )
            self._stats["direct_connects"] += 1
            return (reader, writer)
        except Exception:
            return None

    async def _relay_connect(self, target_pubkey: str) -> Optional[tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
        """Connect via coordinator broker (symmetric NAT fallback)."""
        resp = await self._coordinator_rpc({
            "command": "broker_connect",
            "target_pubkey": target_pubkey,
            "ip": self._agent.get("ip", "0.0.0.0") if self._agent else "0.0.0.0",
            "port": self._agent.get("port", 0) if self._agent else 0,
        }, timeout=RELAY_FALLBACK_TIMEOUT)
        if resp.get("type") == "broker_initiated":
            # Wait for broker_accepted response
            try:
                line = await asyncio.wait_for(self._reader.readline(), timeout=RELAY_FALLBACK_TIMEOUT)
                data = json.loads(line.decode().strip())
                if data.get("type") == "broker_accepted":
                    self._stats["fallback_relay"] += 1
                    # Return coordinator connection as relay
                    return (self._reader, self._writer)
            except Exception:
                pass
        return None

    async def get_connection(self, target_pubkey: str) -> Optional[tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
        """Get or establish connection to target agent. Cached for 5 min."""
        # Pool check
        if target_pubkey in self._connection_pool:
            reader, writer = self._connection_pool[target_pubkey]
            if not writer.is_closing():
                age = time.time() - self._conn_ts.get(target_pubkey, 0)
                if age < CONNECTION_POOL_TTL:
                    return (reader, writer)
            # Stale — close and remove
            writer.close()
            del self._connection_pool[target_pubkey]

        # Find peer
        peer = await self.get_peer(target_pubkey)
        if not peer:
            return None

        # Try direct
        conn = await self._direct_connect(peer)
        if conn:
            self._connection_pool[target_pubkey] = conn
            self._conn_ts[target_pubkey] = time.time()
            return conn

        # Try relay
        if peer.get("nat_type") == "symmetric":
            conn = await self._relay_connect(target_pubkey)
            if conn:
                self._connection_pool[target_pubkey] = conn
                self._conn_ts[target_pubkey] = time.time()
                return conn

        return None

    # ─── Send ───

    async def send(self, target_pubkey: str, payload: str,
                   kind: int = 39002, meta: dict = None) -> dict:
        """
        Send message to target agent via hybrid channel.

        Returns: {"ok": bool, "channel": "hybrid", "latency_ms": float, "method": str}
        """
        t0 = time.time()
        self._stats["sent"] += 1

        conn = await self.get_connection(target_pubkey)
        if not conn:
            return {"ok": False, "channel": "hybrid", "latency_ms": (time.time() - t0) * 1000,
                    "error": "no_route", "method": "none"}

        reader, writer = conn
        msg = {
            "kind": kind,
            "from": self._agent.get("pubkey", "unknown") if self._agent else "unknown",
            "to": target_pubkey,
            "payload": payload,
            "meta": meta or {},
            "channel": "hybrid",
            "ts": time.time(),
        }

        try:
            data = json.dumps(msg).encode() + b"\n"
            writer.write(data)
            await writer.drain()

            # Read ack
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            ack = json.loads(line.decode().strip())
            latency = (time.time() - t0) * 1000

            if ack.get("type") == "ack":
                self._stats["delivered"] += 1
                return {"ok": True, "channel": "hybrid", "latency_ms": latency,
                        "method": "direct", "ack": ack}
            return {"ok": False, "channel": "hybrid", "latency_ms": latency,
                    "error": "no_ack", "method": "direct"}
        except Exception as e:
            # Connection died — remove from pool
            self._connection_pool.pop(target_pubkey, None)
            return {"ok": False, "channel": "hybrid", "latency_ms": (time.time() - t0) * 1000,
                    "error": str(e)[:80], "method": "failed"}

    # ─── Stats ───

    def get_stats(self) -> dict:
        s = dict(self._stats)
        s["pool_size"] = len(self._connection_pool)
        s["cache_size"] = len(self._peer_cache)
        s["registered"] = self._registered
        return s

    # ─── Cleanup ───

    async def close(self):
        for reader, writer in self._connection_pool.values():
            writer.close()
        self._connection_pool.clear()
        if self._writer and not self._writer.is_closing():
            self._writer.close()
        self._registered = False


# ─── SmartRouter Integration ───

class HybridRouterAdapter:
    """
    Adapter to use HybridChannel as a 5th channel in SmartRouter.
    Implements the same interface as mesh/gossip/nostr channels.
    """

    def __init__(self, coordinator_host: str = COORDINATOR_HOST,
                 coordinator_port: int = COORDINATOR_PORT):
        self._channel = HybridChannel(coordinator_host, coordinator_port)
        self._name = "hybrid"

    @property
    def name(self) -> str:
        return self._name

    async def start(self, agent_pubkey: str, agent_name: str = "",
                    agent_ip: str = "0.0.0.0", agent_port: int = 0,
                    nat_type: str = "unknown"):
        """Register agent and start channel."""
        return await self._channel.register(
            pubkey=agent_pubkey, name=agent_name,
            ip=agent_ip, port=agent_port, nat_type=nat_type,
        )

    async def send(self, target: str, payload: str, kind: int = 39002,
                   meta: dict = None) -> dict:
        """Send via hybrid channel. Compatible with SmartRouter channel API."""
        return await self._channel.send(target, payload, kind, meta)

    async def get_peer(self, pubkey: str):
        return await self._channel.get_peer(pubkey)

    async def get_peers(self) -> list[dict]:
        return await self._channel.get_peer_list()

    def get_stats(self) -> dict:
        return self._channel.get_stats()

    async def stop(self):
        await self._channel.close()


# ─── Test ───

async def _test():
    """Self-test: connect to local coordinator, register, get peers."""
    print("═" * 50)
    print(" HybridChannel Self-Test")
    print("═" * 50)

    channel = HybridChannel("127.0.0.1", 9970)
    try:
        await channel.register(
            pubkey="test_hybrid_agent",
            name="TestAgent",
            ip="127.0.0.1",
            port=9122,
            nat_type="easy",
            capabilities=["test", "hybrid"],
        )
        print("✅ Registered with coordinator")

        peers = await channel.get_peer_list()
        print(f"✅ Peer list: {len(peers)} online")

        stats = channel.get_stats()
        print(f"✅ Stats: {stats}")
    except ConnectionError as e:
        print(f"⚠️ Coordinator not running: {e}")
        print("   Start with: python3 hybrid/hcoor.py --port 9970")
    finally:
        await channel.close()


if __name__ == "__main__":
    asyncio.run(_test())

#!/usr/bin/env python3
"""
Hybrid Discovery Coordinator (hcoor) — SNIN V5 Phase: Hybrid Architecture
═══════════════════════════════════════════════════════════════════════════════

ПРОБЛЕМА (почему built):
  holepunch.py + dht_node.py + knowledge_graph.py — все зависят от Redis.
  Redis на VPS мёртв → 6 модулей (~3000 строк) лежат без дела.
  Pure mesh страдает от: NAT traversal, discovery latency, no fallback registry.

РЕШЕНИЕ:
  Лёгкий координатор на SQLite + WebSocket. Не заменяет mesh — дополняет его.
  Mesh остаётся для данных (суверенность). Координатор — только для discovery.

АРХИТЕКТУРА:
  ┌──────────┐    WS register    ┌──────────────┐    WS query    ┌──────────┐
  │ Agent A  │ ───────────────→  │  HCOOR (:9970)│ ←───────────── │ Agent B  │
  │ (за NAT) │ ←── peer list ──  │  SQLite reg   │ ── peer list → │ (за NAT) │
  └──────────┘                   └──────────────┘                └──────────┘
       │                               │                              │
       └─────────── P2P data (mesh/gossip/direct) ───────────────────┘

ЧТО ДЕЛАЕТ КООРДИНАТОР:
  1. Агенты регистрируются через WebSocket (pubkey, ip, port, nat_type, caps)
  2. Координатор хранит реестр в SQLite (persistent, no Redis dependency)
  3. Агенты запрашивают peer list — координатор возвращает активных
  4. NAT type exchange: координатор знает кто за каким NAT → подсказывает стратегию
  5. Connection brokering: для symmetric NAT — координатор организует TCP relay
  6. Health check: если агент 60с не пингует → marked offline

ОТЛИЧИЕ ОТ DHT:
  - Не децентрализован (это минус)
  - Работает без Redis (это плюс — прямо сейчас)
  - Не требует Kademlia routing table (проще)
  - Можно заменить на DHT позже, API совместим

ПОРТ: 9970 (WebSocket)
SQLITE: ~/data/sites/relay-mesh/hybrid/registry.db
"""
import asyncio
import json
import os
import sqlite3
import time
import sys
import signal
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional

# ─── Config ───
WS_PORT = 9970
WS_HOST = "0.0.0.0"
DB_PATH = Path(__file__).parent / "registry.db"
AGENT_TTL = 120          # секунд без пинга → offline
PING_INTERVAL = 30       # сервер шлёт ping каждые 30с
CLEANUP_INTERVAL = 15    # чистка мёртвых каждые 15с


@dataclass
class Agent:
    pubkey: str
    npub: str = ""
    name: str = ""
    ip: str = "0.0.0.0"
    port: int = 0
    nat_type: str = "unknown"       # easy, symmetric, cone, unknown
    capabilities: list = field(default_factory=list)
    mesh_id: str = "snin-main-1"
    mode: str = "direct"            # direct, relay, hybrid
    version: str = "5.0.0"
    first_seen: float = 0.0
    last_seen: float = 0.0
    ping_count: int = 0
    status: str = "online"          # online, offline, evicted

    def to_dict(self) -> dict:
        d = asdict(self)
        d["first_seen"] = self.first_seen
        d["last_seen"] = self.last_seen
        return d


class RegistryDB:
    """SQLite-based agent registry — zero Redis dependency."""

    def __init__(self, db_path: Path = DB_PATH):
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    def _init_db(self):
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=2000")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS agents (
                pubkey TEXT PRIMARY KEY,
                npub TEXT DEFAULT '',
                name TEXT DEFAULT '',
                ip TEXT DEFAULT '0.0.0.0',
                port INTEGER DEFAULT 0,
                nat_type TEXT DEFAULT 'unknown',
                capabilities TEXT DEFAULT '[]',
                mesh_id TEXT DEFAULT 'snin-main-1',
                mode TEXT DEFAULT 'direct',
                version TEXT DEFAULT '5.0.0',
                first_seen REAL DEFAULT 0.0,
                last_seen REAL DEFAULT 0.0,
                ping_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'online'
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL DEFAULT (strftime('%s','now')),
                event_type TEXT NOT NULL,
                pubkey TEXT,
                data TEXT DEFAULT '{}'
            )
        """)
        self._conn.commit()

    def register(self, agent: Agent) -> bool:
        """Register or update agent."""
        now = time.time()
        cur = self._conn.execute(
            "SELECT pubkey, first_seen, ping_count FROM agents WHERE pubkey=?",
            (agent.pubkey,)
        )
        row = cur.fetchone()

        caps_json = json.dumps(agent.capabilities)
        if row:
            new_ping = (row[2] or 0) + 1
            self._conn.execute("""
                UPDATE agents SET npub=?, name=?, ip=?, port=?, nat_type=?,
                    capabilities=?, mesh_id=?, mode=?, version=?,
                    last_seen=?, ping_count=?, status='online'
                WHERE pubkey=?
            """, (agent.npub, agent.name, agent.ip, agent.port, agent.nat_type,
                  caps_json, agent.mesh_id, agent.mode, agent.version,
                  now, new_ping, agent.pubkey))
        else:
            self._conn.execute("""
                INSERT INTO agents (pubkey, npub, name, ip, port, nat_type,
                    capabilities, mesh_id, mode, version, first_seen, last_seen,
                    ping_count, status)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,'online')
            """, (agent.pubkey, agent.npub, agent.name, agent.ip, agent.port,
                  agent.nat_type, caps_json, agent.mesh_id, agent.mode,
                  agent.version, now, now))
        self._conn.commit()
        self._log_event("register" if not row else "ping", agent.pubkey, "")
        return True

    def get_agent(self, pubkey: str) -> Optional[Agent]:
        cur = self._conn.execute("SELECT * FROM agents WHERE pubkey=?", (pubkey,))
        row = cur.fetchone()
        if row:
            return self._row_to_agent(row)
        return None

    def get_all_online(self) -> list[Agent]:
        cutoff = time.time() - AGENT_TTL
        cur = self._conn.execute(
            "SELECT * FROM agents WHERE status='online' AND last_seen > ? ORDER BY last_seen DESC",
            (cutoff,)
        )
        return [self._row_to_agent(r) for r in cur.fetchall()]

    def get_peer_list(self, requesting_pubkey: str) -> list[dict]:
        """Return peers suitable for direct connection — excluding requester."""
        agents = self.get_all_online()
        peers = []
        for a in agents:
            if a.pubkey == requesting_pubkey:
                continue
            peers.append({
                "pubkey": a.pubkey,
                "npub": a.npub,
                "name": a.name,
                "ip": a.ip,
                "port": a.port,
                "nat_type": a.nat_type,
                "capabilities": a.capabilities,
                "mode": a.mode,
                "last_seen": a.last_seen,
            })
        return peers

    def mark_offline(self, pubkey: str):
        self._conn.execute(
            "UPDATE agents SET status='offline' WHERE pubkey=? AND status='online'",
            (pubkey,)
        )
        self._conn.commit()

    def cleanup_stale(self):
        """Mark agents offline if not seen within TTL."""
        cutoff = time.time() - AGENT_TTL
        cur = self._conn.execute(
            "SELECT pubkey FROM agents WHERE status='online' AND last_seen < ?",
            (cutoff,)
        )
        stale = [r[0] for r in cur.fetchall()]
        for pubkey in stale:
            self.mark_offline(pubkey)
        return len(stale)

    def get_stats(self) -> dict:
        total = self._conn.execute("SELECT COUNT(*) FROM agents").fetchone()[0]
        online = self._conn.execute(
            "SELECT COUNT(*) FROM agents WHERE status='online'"
        ).fetchone()[0]
        events_24h = self._conn.execute(
            "SELECT COUNT(*) FROM events WHERE ts > ?",
            (time.time() - 86400,)
        ).fetchone()[0]
        return {"total_agents": total, "online": online, "events_24h": events_24h}

    def _row_to_agent(self, row: tuple) -> Agent:
        cols = [d[1] for d in self._conn.execute("PRAGMA table_info(agents)").fetchall()]
        d = dict(zip(cols, row))
        try:
            d["capabilities"] = json.loads(d.get("capabilities", "[]"))
        except (json.JSONDecodeError, TypeError):
            d["capabilities"] = []
        return Agent(
            pubkey=d.get("pubkey", ""), npub=d.get("npub", ""),
            name=d.get("name", ""), ip=d.get("ip", "0.0.0.0"),
            port=d.get("port", 0), nat_type=d.get("nat_type", "unknown"),
            capabilities=d.get("capabilities", []),
            mesh_id=d.get("mesh_id", "snin-main-1"),
            mode=d.get("mode", "direct"), version=d.get("version", "5.0.0"),
            first_seen=d.get("first_seen", 0.0), last_seen=d.get("last_seen", 0.0),
            ping_count=d.get("ping_count", 0), status=d.get("status", "online"),
        )

    def _log_event(self, event_type: str, pubkey: str, data: str = ""):
        try:
            self._conn.execute(
                "INSERT INTO events (event_type, pubkey, data) VALUES (?,?,?)",
                (event_type, pubkey, data)
            )
            self._conn.commit()
        except Exception:
            pass

    def close(self):
        if self._conn:
            self._conn.close()


class HybridCoordinator:
    """WebSocket-based discovery coordinator for SNIN mesh."""

    def __init__(self, port: int = WS_PORT, host: str = WS_HOST):
        self._port = port
        self._host = host
        self._db = RegistryDB()
        self._server: Optional[asyncio.AbstractServer] = None
        self._running = False
        self._connections: dict[str, asyncio.StreamWriter] = {}
        self._start_time = time.time()

    # ─── WebSocket-like handler over raw TCP (JSON-line protocol) ───

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")
        agent_pubkey = "anon"
        try:
            buf = b""
            while self._running:
                # Read line (JSON message)
                try:
                    line = await asyncio.wait_for(reader.readline(), timeout=AGENT_TTL + 30)
                except asyncio.TimeoutError:
                    break
                if not line:
                    break

                try:
                    msg = json.loads(line.decode().strip())
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue

                cmd = msg.get("command", msg.get("type", ""))

                if cmd == "register":
                    agent_pubkey = msg.get("pubkey", "anon")
                    agent = Agent(
                        pubkey=agent_pubkey,
                        npub=msg.get("npub", ""),
                        name=msg.get("name", ""),
                        ip=msg.get("ip", addr[0] if addr else "0.0.0.0"),
                        port=msg.get("port", 0),
                        nat_type=msg.get("nat_type", "unknown"),
                        capabilities=msg.get("capabilities", []),
                        mesh_id=msg.get("mesh_id", "snin-main-1"),
                        mode=msg.get("mode", "direct"),
                        version=msg.get("version", "5.0.0"),
                    )
                    self._db.register(agent)
                    self._connections[agent_pubkey] = writer
                    await self._send_json(writer, {
                        "type": "registered",
                        "pubkey": agent_pubkey,
                        "peers_online": self._db.get_stats()["online"],
                    })

                elif cmd == "ping":
                    agent_pubkey = msg.get("pubkey", agent_pubkey)
                    # Only update last_seen + ping_count — preserve IP/port/name
                    self._db._conn.execute(
                        "UPDATE agents SET last_seen=?, ping_count=ping_count+1, status='online' WHERE pubkey=?",
                        (time.time(), agent_pubkey)
                    )
                    self._db._conn.commit()
                    self._db._log_event("ping", agent_pubkey, "")
                    await self._send_json(writer, {"type": "pong", "ts": time.time()})

                elif cmd == "get_peers":
                    req_pubkey = msg.get("pubkey", agent_pubkey)
                    peers = self._db.get_peer_list(req_pubkey)
                    await self._send_json(writer, {
                        "type": "peer_list",
                        "count": len(peers),
                        "peers": peers,
                    })

                elif cmd == "find_agent":
                    target = msg.get("target_pubkey", "")
                    agent = self._db.get_agent(target)
                    if agent and agent.status == "online":
                        await self._send_json(writer, {
                            "type": "agent_found",
                            "agent": agent.to_dict(),
                        })
                    else:
                        await self._send_json(writer, {
                            "type": "agent_not_found",
                            "target": target,
                        })

                elif cmd == "broker_connect":
                    # Connection brokering for symmetric NAT
                    target = msg.get("target_pubkey", "")
                    target_agent = self._db.get_agent(target)
                    if target_agent and target_agent.status == "online":
                        # Send broker request to target
                        target_writer = self._connections.get(target)
                        if target_writer:
                            await self._send_json(target_writer, {
                                "type": "broker_request",
                                "from_pubkey": agent_pubkey,
                                "from_ip": msg.get("ip", addr[0] if addr else "0.0.0.0"),
                                "from_port": msg.get("port", 0),
                            })
                            await self._send_json(writer, {
                                "type": "broker_initiated",
                                "target": target,
                            })
                        else:
                            await self._send_json(writer, {
                                "type": "broker_failed",
                                "reason": "target_not_connected_to_coordinator",
                            })
                    else:
                        await self._send_json(writer, {
                            "type": "broker_failed",
                            "reason": "target_offline",
                        })

                elif cmd == "broker_accept":
                    # Accept broker request — relay to original requester
                    requester = msg.get("requester_pubkey", "")
                    req_writer = self._connections.get(requester)
                    if req_writer:
                        await self._send_json(req_writer, {
                            "type": "broker_accepted",
                            "from": agent_pubkey,
                            "ip": msg.get("ip", ""),
                            "port": msg.get("port", 0),
                        })

                elif cmd == "stats":
                    stats = self._db.get_stats()
                    stats["uptime_sec"] = round(time.time() - self._start_time, 1)
                    stats["connections"] = len(self._connections)
                    await self._send_json(writer, {"type": "stats", "stats": stats})

                elif cmd == "get_nat_strategy":
                    target = msg.get("target_pubkey", "")
                    src_type = msg.get("my_nat_type", "unknown")
                    target_agent = self._db.get_agent(target)
                    if target_agent:
                        strategy = self._nat_strategy(src_type, target_agent.nat_type)
                        await self._send_json(writer, {
                            "type": "nat_strategy",
                            "strategy": strategy,
                            "my_nat": src_type,
                            "target_nat": target_agent.nat_type,
                        })
                    else:
                        await self._send_json(writer, {
                            "type": "nat_strategy",
                            "strategy": "unknown_target",
                        })

        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            pass
        finally:
            if agent_pubkey in self._connections:
                del self._connections[agent_pubkey]
            writer.close()

    async def _send_json(self, writer: asyncio.StreamWriter, data: dict):
        try:
            payload = json.dumps(data).encode() + b"\n"
            writer.write(payload)
            await writer.drain()
        except Exception:
            pass

    def _nat_strategy(self, src_type: str, dst_type: str) -> str:
        """Recommend NAT traversal strategy based on both parties' NAT types."""
        if src_type == "easy" and dst_type == "easy":
            return "direct_udp"         # Both can hole-punch
        elif src_type == "easy" and dst_type in ("symmetric", "cone"):
            return "tcp_reverse"         # Easy side listens, symmetric connects
        elif src_type in ("symmetric", "cone") and dst_type == "easy":
            return "tcp_direct"          # Symmetric connects to easy listener
        elif src_type == "cone" and dst_type == "cone":
            return "udp_holepunch"       # Cone NAT can hole-punch
        else:
            return "relay_fallback"      # Both symmetric → coordinator relays

    # ─── Background tasks ───

    async def _ping_loop(self):
        while self._running:
            await asyncio.sleep(PING_INTERVAL)
            dead = []
            for pubkey, writer in list(self._connections.items()):
                try:
                    await self._send_json(writer, {"type": "ping", "ts": time.time()})
                except Exception:
                    dead.append(pubkey)
            for pubkey in dead:
                self._connections.pop(pubkey, None)
                self._db.mark_offline(pubkey)

    async def _cleanup_loop(self):
        while self._running:
            await asyncio.sleep(CLEANUP_INTERVAL)
            stale = self._db.cleanup_stale()
            if stale:
                print(f"[HCOOR] 🧹 {stale} agents marked offline")

    # ─── Lifecycle ───

    async def start(self):
        if self._running:
            return
        self._running = True
        self._server = await asyncio.start_server(
            self._handle_client, self._host, self._port
        )
        asyncio.ensure_future(self._ping_loop())
        asyncio.ensure_future(self._cleanup_loop())
        print(f"[HCOOR] 🟢 Hybrid Coordinator WS on {self._host}:{self._port}")
        print(f"[HCOOR] 💾 SQLite: {DB_PATH}")

    async def stop(self):
        self._running = False
        if self._server:
            self._server.close()
            self._server = None
        for writer in self._connections.values():
            writer.close()
        self._connections.clear()
        self._db.close()
        print("[HCOOR] 🔴 Stopped")


# ─── CLI ───

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="SNIN Hybrid Discovery Coordinator")
    parser.add_argument("--port", type=int, default=WS_PORT, help="WebSocket port (default: 9970)")
    parser.add_argument("--host", type=str, default=WS_HOST, help="Bind host (default: 0.0.0.0)")
    args = parser.parse_args()

    hcoor = HybridCoordinator(port=args.port, host=args.host)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _sig_handler():
        print("\n[HCOOR] Shutting down...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig_handler)
        except NotImplementedError:
            pass

    await hcoor.start()
    print(f"[HCOOR] Stats: {hcoor._db.get_stats()}")

    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await hcoor.stop()


if __name__ == "__main__":
    asyncio.run(main())

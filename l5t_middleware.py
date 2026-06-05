#!/usr/bin/env python3
"""
L5T Middleware — интеграция Dead-Letter Queue в Smart Router.

Перехватывает agent-to-agent сообщения в route_message(),
проверяет online/offline получателя через heartbeat,
при офлайне — отправляет в DeadLetterQueue.
"""

import asyncio
import time
import json
import os
from dataclasses import dataclass, field
from typing import Optional


# ═══════════════════════════════════════════════════════════════
#  Agent Heartbeat Tracker
# ═══════════════════════════════════════════════════════════════

HEARTBEAT_TIMEOUT = 120  # секунд — если heartbeat не было >120 сек → offline
HEARTBEAT_KIND = 39000
STATE_FILE = "/home/agent/data/sites/relay-mesh/data/agent_heartbeats.json"


@dataclass
class AgentStatus:
    pubkey: str
    name: str = ""
    last_heartbeat: float = 0.0
    online: bool = False
    first_seen: float = 0.0
    total_heartbeats: int = 0

    def is_online(self) -> bool:
        return self.online and (time.time() - self.last_heartbeat) < HEARTBEAT_TIMEOUT


class HeartbeatTracker:
    """Отслеживает heartbeat агентов (kind:39000)."""

    def __init__(self):
        self._agents: dict[str, AgentStatus] = {}
        self._lock = asyncio.Lock()
        self._load_state()

    def _load_state(self):
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE) as f:
                    data = json.load(f)
                    for pk, info in data.items():
                        ag = AgentStatus(
                            pubkey=pk,
                            name=info.get("name", ""),
                            last_heartbeat=info.get("last_heartbeat", 0),
                            online=False,  # при загрузке — offline, пока не придёт heartbeat
                            first_seen=info.get("first_seen", 0),
                            total_heartbeats=info.get("total_heartbeats", 0),
                        )
                        self._agents[pk] = ag
        except Exception:
            pass

    def _save_state(self):
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        data = {
            pk: {
                "name": ag.name,
                "last_heartbeat": ag.last_heartbeat,
                "first_seen": ag.first_seen,
                "total_heartbeats": ag.total_heartbeats,
            }
            for pk, ag in self._agents.items()
        }
        with open(STATE_FILE, "w") as f:
            json.dump(data, f)

    async def process_heartbeat(self, pubkey: str, meta: dict = None) -> AgentStatus:
        """Обработать heartbeat от агента."""
        async with self._lock:
            now = time.time()
            if pubkey not in self._agents:
                self._agents[pubkey] = AgentStatus(
                    pubkey=pubkey,
                    name=meta.get("agent", "") if meta else "",
                    first_seen=now,
                    last_heartbeat=now,
                    online=True,
                    total_heartbeats=1,
                )
            else:
                ag = self._agents[pubkey]
                ag.last_heartbeat = now
                ag.online = True
                ag.total_heartbeats += 1
                if meta and meta.get("agent") and not ag.name:
                    ag.name = meta["agent"]
            self._save_state()
            return self._agents[pubkey]

    async def is_online(self, pubkey: str) -> bool:
        """Проверить, онлайн ли агент."""
        async with self._lock:
            ag = self._agents.get(pubkey)
            if ag is None:
                return False
            return ag.is_online()

    async def get_agent(self, pubkey: str) -> Optional[AgentStatus]:
        async with self._lock:
            return self._agents.get(pubkey)

    async def list_agents(self) -> list:
        async with self._lock:
            return [
                {"pubkey": ag.pubkey[:16] + "...", "name": ag.name,
                 "online": ag.is_online(), "last_hb": int(ag.last_heartbeat),
                 "hb_count": ag.total_heartbeats}
                for ag in self._agents.values()
            ]

    async def check_timeouts(self):
        """Пометить офлайн агентов, у которых истек heartbeat timeout."""
        async with self._lock:
            now = time.time()
            changed = False
            for pk, ag in self._agents.items():
                if ag.online and (now - ag.last_heartbeat) > HEARTBEAT_TIMEOUT:
                    ag.online = False
                    changed = True
            if changed:
                self._save_state()


# ═══════════════════════════════════════════════════════════════
#  L5T Middleware
# ═══════════════════════════════════════════════════════════════

class L5TMiddleware:
    """
    Прослойка между Smart Router и DeadLetterQueue.
    
    Использование в smart_router.py:
        self.l5t = L5TMiddleware(bridge_privkey="...", bridge_pubkey="...")
        ...
        if msg.get("to") and not await self.l5t.heartbeat.is_online(msg["to"]):
            return await self.l5t.queue_to_dlq(msg)
    """

    def __init__(self, bridge_privkey: str = "", bridge_pubkey: str = "",
                 db_path: str = ""):
        self.heartbeat = HeartbeatTracker()
        self._dlq = None
        self._bridge_privkey = bridge_privkey
        self._bridge_pubkey = bridge_pubkey
        self._db_path = db_path
        self._init_dlq()

    def _init_dlq(self):
        """Ленивая инициализация DeadLetterQueue."""
        try:
            from dead_letter import DeadLetterQueue
            self._dlq = DeadLetterQueue(
                db_path=self._db_path,
                pubkey_hex=self._bridge_pubkey,
                privkey_hex=self._bridge_privkey,
            )
        except Exception as e:
            print(f"[L5T] ⚠️ DeadLetterQueue init failed: {e}")

    async def queue_to_dlq(self, msg: dict) -> dict:
        """
        Отправить сообщение в Dead-Letter Queue.
        
        Ожидает:
            msg["from"] — pubkey отправителя
            msg["to"] — pubkey получателя
            msg["payload"] или msg["content"] — тело сообщения
        """
        if self._dlq is None:
            return {"ok": False, "error": "dlq_not_initialized"}

        from_pubkey = msg.get("from", msg.get("pubkey", ""))
        to_pubkey = msg.get("to", "")
        content = json.dumps(msg.get("payload", msg.get("content", "")),
                            ensure_ascii=False)
        priority = msg.get("meta", {}).get("priority", "NORMAL")
        kind = msg.get("kind", 39002)

        result = await self._dlq.push(
            from_pubkey=from_pubkey,
            to_pubkey=to_pubkey,
            content=content,
            kind=kind,
            priority=priority.upper() if priority.upper() in ("NORMAL", "HIGH", "CRITICAL") else "NORMAL",
        )
        if result.get("ok"):
            print(f"[L5T] 📮 Queued msg {result.get('hash','?')} → {to_pubkey[:16]}... (relays={result.get('relay_count',0)})")
            return {
                "ok": True,
                "channel": "deadletter",
                "dlq_hash": result["hash"],
                "relay_count": result["relay_count"],
                "ttl": result["ttl"],
                "status": "queued_for_offline_agent",
            }
        return {"ok": False, "error": result.get("error", "dlq_push_failed")}

    async def sync_for_agent(self, to_pubkey: str, since: int = 0) -> list:
        """Синхронизировать пропущенные сообщения для агента (pull + mark delivered)."""
        if self._dlq is None:
            return []
        messages = await self._dlq.pull(to_pubkey=to_pubkey, since=since, mark_delivered=False)
        # Отмечаем как доставленные после успешной отправки
        for msg in messages:
            self._dlq.mark_delivered(msg.hash)
        return messages

    async def process_incoming_heartbeat(self, pubkey: str, meta: dict = None) -> dict:
        """Обработать heartbeat и вернуть статус + pending messages."""
        ag = await self.heartbeat.process_heartbeat(pubkey, meta)
        # Если агент только что появился — считаем (не помечаем доставленными!)
        pending_count = 0
        if ag.online:
            pending = await self._dlq.pull(to_pubkey=pubkey, since=0, mark_delivered=False)
            pending_count = len(pending)
            if pending:
                print(f"[L5T] 📬 Found {pending_count} pending for {ag.name or pubkey[:16]}...")
        return {
            "ok": True,
            "online": ag.is_online(),
            "name": ag.name,
            "pending_messages": pending_count,
        }


# ═══════════════════════════════════════════════════════════════
#  Инициализация
# ═══════════════════════════════════════════════════════════════

_BRIDGE_PUBKEY = os.environ.get(
    "BRIDGE_PUBKEY",
    "e241197ccc1477b039652916a2e6e679f0df66b1b2ca37cf7596591e46a2088d"
)
_BRIDGE_PRIVKEY = os.environ.get(
    "BRIDGE_PRIVKEY",
    "69e7b1239518e6b8c1e3f7d4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4"
)


def create_l5t_middleware() -> L5TMiddleware:
    """Фабрика: создать L5T Middleware с ключами из identities."""
    import json as _json
    identity_path = "/home/agent/data/sites/relay-mesh/identities/archivist_ai.json"
    pubkey = _BRIDGE_PUBKEY
    privkey = _BRIDGE_PRIVKEY
    # Use cipher keys (X25519) for DLQ encryption
    cipher_pubkey = pubkey
    cipher_privkey = privkey
    try:
        if os.path.exists(identity_path):
            with open(identity_path) as f:
                ident = _json.load(f)
                mesh_pubkey = ident.get("mesh_pubkey", pubkey)
                mesh_privkey = ident.get("mesh_privkey", privkey)
                # Use cipher keys for DLQ (X25519, works with all agents)
                cipher_pubkey = ident.get("cipher_pubkey", mesh_pubkey)
                cipher_privkey = ident.get("cipher_privkey", mesh_privkey)
    except Exception:
        pass
    return L5TMiddleware(
        bridge_privkey=cipher_privkey,
        bridge_pubkey=cipher_pubkey,
        db_path="/home/agent/data/sites/relay-mesh/data/dead_letter.db",
    )

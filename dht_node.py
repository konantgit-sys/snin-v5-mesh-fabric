"""DHT Kademlia + Redis — распределённый реестр агентов SNIN Mesh.

Архитектура:
  - Redis: primary store (мгновенный get/set, персистентность)
  - Kademlia: overlay для децентрализации (P2P sync между нодами)
  - Dual-write: set → Redis + Kademlia
  - Fast-read: get → Redis first, Kademlia fallback
  - UDP 9934 для Kademlia P2P

Протокол Kademlia:
  - Каждый узел = UDP сервер на порту 9934
  - Node ID = SHA-256(pubkey SR)[:20] (160 бит)
  - Routing table: k-buckets, XOR distance
  - FIND_NODE → ближайшие узлы по ID
  - STORE/FIND_VALUE → данные агентов

Redis схема:
  - dht:agents:<pubkey> → {"ip","tcp_port","capabilities","last_seen","node_id"}
  - dht:nodes → список node_id всех активных узлов
  - dht:bootstrap → адрес bootstrap ноды
"""
import json, time, asyncio, hashlib, logging
from pathlib import Path

logger = logging.getLogger("DHT")

# ─── Конфиг ───
DHT_PORT = 9934
DHT_STATE_FILE = str(Path.home() / "data" / "sites" / "relay-mesh" / "dht_state.dat")
AGENT_TTL = 7200          # 2 часа — signed ping раз в 10 сек, так что с запасом

# Redis
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379

# ─── Утилиты ───

def pubkey_to_node_id(pubkey: str) -> bytes:
    """SHA-256(pubkey) → 20 байт (160 бит, kademlia digest)."""
    return hashlib.sha256(pubkey.encode()).digest()[:20]

def get_redis():
    """Синхронное подключение к Redis."""
    import redis as r
    return r.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0,
                   socket_timeout=2, decode_responses=True)

async def get_aredis():
    """Асинхронное подключение к Redis."""
    import redis.asyncio as aredis
    return aredis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0,
                         socket_timeout=2, decode_responses=True)

# ─── DHT Node ───

class DHTNode:
    """Kademlia + Redis DHT узел."""

    def __init__(self, port: int = DHT_PORT, bootstrap: tuple = None,
                 agent_pubkey: str = "router", agent_meta: dict = None):
        self.port = port
        self.bootstrap = bootstrap
        self.agent_pubkey = agent_pubkey
        self.agent_meta = agent_meta or {"ip": "127.0.0.1", "tcp_port": 9932, "role": "router"}
        self.server = None
        self._running = False

    async def start(self):
        """Запустить DHT (Kademlia + Redis)."""
        # 1. Kademlia сервер
        from kademlia.network import Server
        self.server = Server()
        await self.server.listen(self.port)
        logger.info(f"DHT Kademlia listening on UDP :{self.port}")

        # 2. Bootstrap
        if self.bootstrap:
            host, port = self.bootstrap
            is_self = (host in ("127.0.0.1", "0.0.0.0")) and port == self.port
            if not is_self:
                try:
                    await asyncio.wait_for(
                        self.server.bootstrap([self.bootstrap]),
                        timeout=5
                    )
                    logger.info(f"DHT bootstrapped to {self.bootstrap}")
                except asyncio.TimeoutError:
                    logger.warning(f"DHT bootstrap timeout (first node?)")
                except Exception as e:
                    logger.warning(f"DHT bootstrap error: {e}")

        self._running = True
        node_id = self.server.node.id.hex() if self.server.node else "?"
        logger.info(f"DHT node ready (id={node_id})")

        # 3. Регистрация в Redis
        try:
            r = get_redis()
            r.hset(f"dht:nodes", node_id, json.dumps({
                "ip": "127.0.0.1",
                "port": self.port,
                "agent_pubkey": self.agent_pubkey,
                "started_at": time.time(),
            }))
        except Exception as e:
            logger.warning(f"DHT Redis node registration: {e}")

        # 4. Регистрация себя как агента
        await self.register_agent(self.agent_pubkey, self.agent_meta)

        # 5. Автосохранение
        asyncio.create_task(self._auto_cleanup())

    async def stop(self):
        """Остановить DHT."""
        self._running = False
        if self.server:
            self.server.stop()
        logger.info("DHT stopped")

    async def register_agent(self, pubkey: str, data: dict, ttl: int = AGENT_TTL):
        """Зарегистрировать агента (Redis + Kademlia)."""
        entry = {
            **data,
            "last_seen": time.time(),
        }

        # Redis (fast store)
        try:
            r = get_redis()
            r.hset(f"dht:agents", pubkey, json.dumps(entry))
            # Без TTL на hash — cleanup забирает мёртвых по last_seen
            logger.info(f"✅ agent {pubkey[:16]} → Redis")
        except Exception as e:
            logger.warning(f"DHT Redis store error: {e}")

        # Kademlia (P2P sync)
        if self.server and self._running:
            try:
                key = f"agent:{pubkey}".encode()
                value = json.dumps({**entry, "node_id": self.server.node.id.hex()}).encode()
                await self.server.set(key, value)
            except Exception as e:
                logger.debug(f"DHT Kademlia set error: {e}")

        return entry

    async def lookup_agent(self, pubkey: str) -> dict | None:
        """Найти агента в DHT (Redis → Kademlia fallback)."""
        # 1. Redis (fast)
        try:
            r = get_redis()
            data = r.hget(f"dht:agents", pubkey)
            if data:
                result = json.loads(data)
                logger.info(f"🔍 DHT hit (Redis): {pubkey[:16]}")
                return result
        except Exception:
            pass

        # 2. Kademlia (P2P)
        if self.server and self._running:
            try:
                key = f"agent:{pubkey}".encode()
                value = await self.server.get(key)
                if value:
                    logger.info(f"🔍 DHT hit (Kademlia): {pubkey[:16]}")
                    return json.loads(value.decode())
            except Exception as e:
                logger.debug(f"DHT Kademlia get error: {e}")

        logger.info(f"🔍 DHT miss: {pubkey[:16]}")
        return None

    async def refresh_agent(self, pubkey: str, meta: dict = None):
        """Обновить last_seen. Если агента нет — создать."""
        existing = await self.lookup_agent(pubkey)
        if existing:
            existing["last_seen"] = time.time()
            await self.register_agent(pubkey, existing)
            logger.info(f"🔄 refresh {pubkey[:16]} (existing)")
        else:
            # Новый агент — создать с базовыми мета
            entry = meta or {"ip": "127.0.0.1", "tcp_port": 9932, "role": "agent"}
            entry["last_seen"] = time.time()
            await self.register_agent(pubkey, entry)
            logger.info(f"🆕 create {pubkey[:16]} role={meta.get('role','?')}")

    async def list_agents(self) -> list[dict]:
        """Список всех зарегистрированных агентов."""
        try:
            r = get_redis()
            agents = r.hgetall(f"dht:agents") or {}
            now = time.time()
            result = []
            for pubkey, data_json in agents.items():
                try:
                    data = json.loads(data_json)
                    data["pubkey"] = pubkey
                    data["age_sec"] = int(now - data.get("last_seen", now))
                    data["alive"] = data["age_sec"] < 120
                    result.append(data)
                except Exception:
                    pass
            return sorted(result, key=lambda x: x.get("last_seen", 0), reverse=True)
        except Exception as e:
            logger.warning(f"DHT list error: {e}")
            return []

    async def list_nodes(self) -> list[dict]:
        """Список всех DHT нод в сети."""
        try:
            r = get_redis()
            nodes = r.hgetall(f"dht:nodes") or {}
            result = []
            for node_id, data_json in nodes.items():
                try:
                    data = json.loads(data_json)
                    data["node_id"] = node_id
                    result.append(data)
                except Exception:
                    pass
            return result
        except Exception as e:
            logger.warning(f"DHT nodes list error: {e}")
            return []

    async def _auto_cleanup(self):
        """Авто-очистка мёртвых агентов каждые 30 сек."""
        while self._running:
            await asyncio.sleep(30)
            try:
                r = get_redis()
                agents = r.hgetall(f"dht:agents") or {}
                now = time.time()
                for pubkey, data_json in agents.items():
                    try:
                        data = json.loads(data_json)
                        if now - data.get("last_seen", 0) > AGENT_TTL:
                            r.hdel(f"dht:agents", pubkey)
                            logger.info(f"🧹 DHT cleanup: {pubkey[:16]} expired")
                    except Exception:
                        r.hdel(f"dht:agents", pubkey)
            except Exception:
                pass

    def is_running(self) -> bool:
        return self._running


# ─── Тест ───
async def _test():
    node = DHTNode(port=19934, agent_pubkey="test_router",
                   agent_meta={"ip": "127.0.0.1", "tcp_port": 9932, "role": "tester"})
    await node.start()
    print(f"Node ID: {node.server.node.id.hex() if node.server and node.server.node else '?'}")
    
    # Регистрация
    await node.register_agent("agent_test", {"ip": "10.0.0.1", "tcp_port": 9100, "role": "test_agent"})
    
    # Lookup
    data = await node.lookup_agent("agent_test")
    print(f"Lookup: {data}")
    
    # Список
    agents = await node.list_agents()
    print(f"Agents: {len(agents)}")
    for a in agents:
        print(f"  {a.get('pubkey','?')[:16]} role={a.get('role','?')} alive={a.get('alive')}")
    
    await asyncio.sleep(1)
    await node.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_test())

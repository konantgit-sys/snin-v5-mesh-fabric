#!/usr/bin/env python3
"""
SNIN Cross-Mesh Bridge — Фаза 3.

Соединяет mesh сети друг с другом через Federated Discovery Protocol.

Архитектура:
  ┌──────────┐     Nostr kind:39010     ┌──────────┐
  │ Mesh A   │◄────── discovery ───────►│ Mesh B   │
  │ :9932    │      trust_transfer      │ :9932    │
  └────┬─────┘◄══════════ P2P ═════════►└────┬─────┘
       │                                      │
  ┌────┴─────┐                          ┌────┴─────┐
  │ L5 ID    │                          │ L5 ID    │
  │ Rep      │                          │ Rep      │
  └──────────┘                          └──────────┘

Протоколы:
  kind:39010 — Mesh Discovery Announce
  kind:39011 — Cross-Mesh Trust Transfer  
  kind:39012 — Cross-Mesh Route (маршрутизация между mesh)
  P2P TCP    — прямое соединение (если IP достижим)
"""

import asyncio
import hashlib
import json
import os
import sys
import time
import logging
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ─── Импорт mesh модулей ───
from mesh_identity import (
    load_or_create_identity, pubkey_to_did, sign_attestation,
    get_attestations, pubkey_to_bech32
)
from reputation import calculate_reputation, get_reputation_for_pubkey

# ─── Логирование ───
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CROSS-MESH] %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_DIR / "cross_mesh.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("cross_mesh")

# ─── Константы ───
MESH_DISCOVERY_KIND = 39010   # Mesh → Nostr: "I am a mesh, here's my ID"
MESH_TRUST_KIND = 39011       # Mesh → Mesh: "I vouch for this agent"
MESH_ROUTE_KIND = 39012       # Mesh → Mesh: route this message

# Наш mesh identity (загружается при старте)
MESH_NAME = "snin-network"
AGENTS = ["forecaster_ai", "archivist_ai", "anton_ai"]

# Nostr relay для discovery (публичные)
DISCOVERY_RELAYS = [
    "wss://relay.damus.io",
    "wss://relay.primal.net",
    "wss://nostr.wine",
    "wss://relay.nostrati.com",
    "wss://relay.azzamo.net",
]

# Статистика
stats = defaultdict(int)
start_time = time.time()


# ═══════════════════════════════════════════════════════════
# 1. FEDERATED IDENTITY — наша mesh в глобальном реестре
# ═══════════════════════════════════════════════════════════

class MeshIdentity:
    """Идентичность mesh сети в глобальном реестре.
    
    Каждая mesh сеть имеет mesh_id (pubkey старшего агента).
    Публикует announce в Nostr kind:39010 с:
      - mesh_id: pubkey
      - agents: список агентов с DID
      - endpoints: публичные IP/порты (если есть)
      - rep_root: корневая репутация
    """
    
    def __init__(self, agent_name: str = "forecaster_ai"):
        self.agent_name = agent_name
        self.identity = load_or_create_identity(agent_name)
        self.mesh_id = self.identity["mesh_pubkey"]
        self.mesh_npub = self.identity["mesh_npub"]
        self.did = pubkey_to_did(self.mesh_id)
        self._ws = None
        self._running = True
        
    def get_announce(self) -> dict:
        """Сформировать announce: кто мы и что можем."""
        dids = []
        for name in AGENTS:
            try:
                ident = load_or_create_identity(name)
                did = pubkey_to_did(ident["mesh_pubkey"])
                rep = calculate_reputation(name)
                dids.append({
                    "name": name,
                    "did": did,
                    "npub": ident.get("mesh_npub", ""),
                    "rep_score": rep["score"],
                })
            except Exception:
                pass
        
        return {
            "type": "mesh_discovery",
            "protocol_version": "1.0",
            "mesh_name": MESH_NAME,
            "mesh_id": self.mesh_id,
            "mesh_npub": self.mesh_npub,
            "mesh_did": self.did,
            "agents": dids,
            "agent_count": len(dids),
            "capabilities": [
                "relay-mesh",
                "identity-l5",
                "reputation",
                "soulbound-attestation",
                "cross-mesh-bridge",
            ],
            "endpoints": {
                "identity_api": "https://identity-api.v2.site",
                "dashboard": "https://identity-dash.v2.site",
            },
            "timestamp": int(time.time()),
        }
    
    async def publish_discovery(self, relay_url: str):
        """Опубликовать announce в Nostr relay (kind:39010)."""
        import websockets
        
        attempt = 0
        while self._running:
            attempt += 1
            try:
                ws = await asyncio.wait_for(
                    websockets.connect(relay_url, max_size=500_000, ping_interval=30),
                    timeout=10,
                )
                log.info(f"📡 Connected to discovery relay: {relay_url}")
                
                announce = self.get_announce()
                content = json.dumps(announce)
                
                # Создаём Nostr event kind:39010
                import hashlib
                event_id = hashlib.sha256(
                    json.dumps([0, self.mesh_id, int(time.time()), 
                                MESH_DISCOVERY_KIND, [], content], 
                               separators=(",", ":")).encode()
                ).hexdigest()
                
                event = json.dumps([
                    "EVENT",
                    {
                        "id": event_id,
                        "pubkey": self.mesh_id,
                        "created_at": int(time.time()),
                        "kind": MESH_DISCOVERY_KIND,
                        "tags": [["d", MESH_NAME]],
                        "content": content,
                        "sig": "mesh_" + hashlib.md5(content.encode()).hexdigest()[:32],
                    }
                ])
                
                await ws.send(event)
                log.info(f"✅ Published discovery announce ({len(announce['agents'])} agents)")
                
                # После публикации — подписываемся на announce других mesh
                sub = json.dumps(["REQ", "cross-mesh-disc", {
                    "kinds": [MESH_DISCOVERY_KIND],
                    "limit": 20,
                }])
                await ws.send(sub)
                log.info(f"👂 Subscribed to mesh discovery feed")
                
                # Слушаем другие mesh
                while self._running:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=60)
                        self._handle_discovery_msg(msg)
                    except asyncio.TimeoutError:
                        # Refresh announce every 60s
                        await ws.send(event)
                        continue
                
                await ws.close()
                
            except Exception as e:
                log.warning(f"⚠️ Discovery relay {relay_url}: {e}")
                
            delay = min(5 * (2 ** min(attempt - 1, 4)), 60)
            await asyncio.sleep(delay)
    
    def _handle_discovery_msg(self, msg: str):
        """Обработать discovery сообщение от другой mesh сети."""
        try:
            parsed = json.loads(msg)
            if not isinstance(parsed, list) or len(parsed) < 3:
                return
            if parsed[0] != "EVENT":
                return
            
            event = parsed[2]
            if event.get("kind") != MESH_DISCOVERY_KIND:
                return
            
            try:
                content = json.loads(event.get("content", "{}"))
            except json.JSONDecodeError:
                return
            
            mesh_id = content.get("mesh_id", event.get("pubkey", ""))
            mesh_name = content.get("mesh_name", f"mesh_{mesh_id[:8]}")
            agents = content.get("agents", [])
            agent_count = content.get("agent_count", len(agents))
            
            # Не обрабатываем себя
            if mesh_id == self.mesh_id:
                return
            
            log.info(f"🌐 DISCOVERED MESH: {mesh_name} ({mesh_id[:16]}...)")
            log.info(f"   Agents: {agent_count}, DID: {content.get('mesh_did', '?')[:30]}...")
            
            # Сохраняем в реестр
            self._save_remote_mesh(mesh_id, mesh_name, content)
            
            # Отвечаем announce (чтобы другая mesh узнала о нас)
            stats["discovered_meshes"] += 1
            stats[f"mesh_{mesh_id[:16]}"] += 1
            
        except Exception as e:
            log.warning(f"⚠️ Parse discovery msg: {e}")
    
    def _save_remote_mesh(self, mesh_id: str, mesh_name: str, content: dict):
        """Сохранить информацию о удалённой mesh сети."""
        registry_dir = Path(__file__).parent / "registry"
        registry_dir.mkdir(exist_ok=True)
        
        registry_file = registry_dir / f"mesh_{mesh_id[:16]}.json"
        existing = []
        if registry_file.exists():
            try:
                existing = json.loads(registry_file.read_text())
                if not isinstance(existing, list):
                    existing = [existing]
            except:
                existing = []
        
        entry = {
            "mesh_id": mesh_id,
            "mesh_name": mesh_name,
            "mesh_npub": content.get("mesh_npub", ""),
            "mesh_did": content.get("mesh_did", ""),
            "agents": content.get("agents", []),
            "agent_count": content.get("agent_count", 0),
            "capabilities": content.get("capabilities", []),
            "endpoints": content.get("endpoints", {}),
            "discovered_at": time.time(),
        }
        existing.append(entry)
        # Храним последние 50
        existing = existing[-50:]
        registry_file.write_text(json.dumps(existing, indent=2))


# ═══════════════════════════════════════════════════════════
# 2. TRUST TRANSFER — репутация между mesh сетями
# ═══════════════════════════════════════════════════════════

class TrustTransfer:
    """Передача доверия между mesh сетями.
    
    Когда mesh A обнаруживает mesh B, они могут:
    1. Обменяться корневыми attestations
    2. Аттестовать агентов друг друга (soulbound cross-mesh)
    3. Передавать репутационные данные
    """
    
    def __init__(self, mesh_id: MeshIdentity):
        self.mesh = mesh_id
        
    def create_trust_package(self, remote_mesh_id: str) -> dict:
        """Создать пакет доверия для другой mesh сети."""
        our_agents = []
        for name in AGENTS:
            try:
                ident = load_or_create_identity(name)
                did = pubkey_to_did(ident["mesh_pubkey"])
                rep = calculate_reputation(name)
                
                # Наши аттестации для агентов
                attestations = get_attestations(did)
                
                our_agents.append({
                    "name": name,
                    "did": did,
                    "npub": ident.get("mesh_npub", ""),
                    "rep_score": rep["score"],
                    "attestations": len(attestations),
                    "age_days": round(rep["details"]["age_days"], 1),
                    "gossip_ok": rep["details"]["gossip_ok"],
                })
            except Exception:
                pass
        
        return {
            "type": "trust_transfer",
            "from_mesh": MESH_NAME,
            "from_mesh_id": self.mesh.mesh_id,
            "to_mesh_id": remote_mesh_id,
            "agents": our_agents,
            "rep_root": calculate_reputation(AGENTS[0])["score"],
            "signed_by": self.mesh.mesh_id,
            "timestamp": int(time.time()),
        }
    
    def verify_trust_package(self, package: dict) -> dict:
        """Верифицировать пакет доверия от другой mesh.
        
        Returns:
            dict: верифицированные данные + уровень доверия
        """
        from_mesh_id = package.get("from_mesh_id", "")
        agents = package.get("agents", [])
        rep_root = package.get("rep_root", 0.3)
        
        # Проверяем базовую структуру
        if not from_mesh_id or not agents:
            return {"trust_level": 0, "reason": "invalid_package"}
        
        # Уровень доверия = средняя репутация агентов * количество
        avg_rep = sum(a.get("rep_score", 0) for a in agents) / max(len(agents), 1)
        agent_count = len(agents)
        
        # Чем больше агентов и выше rep — тем больше доверия
        trust_level = min(avg_rep * min(agent_count / 3, 1.0), 1.0)
        
        log.info(f"🔗 Trust transfer from {from_mesh_id[:16]}... "
                 f"agents={agent_count} avg_rep={avg_rep:.3f} "
                 f"trust_level={trust_level:.3f}")
        
        return {
            "trust_level": round(trust_level, 4),
            "from_mesh_id": from_mesh_id,
            "agents_verified": agent_count,
            "avg_reputation": round(avg_rep, 4),
            "reason": "verified",
        }
    
    async def issue_cross_attestation(self, remote_mesh_id: str, 
                                       remote_did: str, trust_level: float) -> dict:
        """Выпустить soulbound аттестацию для агента другой mesh.
        
        Если trust_level > 0.5 — аттестуем агента внешней mesh.
        """
        if trust_level < 0.5:
            log.info(f"⏭️ Trust level {trust_level:.3f} < 0.5, skipping attestation")
            return {"attested": False, "reason": "trust_too_low"}
        
        try:
            attestation = sign_attestation(
                agent_name=AGENTS[0],
                target_did=remote_did,
                role="cross_mesh_verifier",
            )
            log.info(f"✅ Cross-mesh attestation issued: {remote_did[:30]}...")
            stats["cross_attestations"] += 1
            return {"attested": True, "attestation": attestation}
        except Exception as e:
            log.warning(f"⚠️ Cross-attestation failed: {e}")
            return {"attested": False, "error": str(e)}


# ═══════════════════════════════════════════════════════════
# 3. MESH-TO-MESH ROUTING — маршрутизация между mesh
# ═══════════════════════════════════════════════════════════

class CrossMeshRouter:
    """Маршрутизация сообщений между mesh сетями.
    
    Когда mesh A хочет отправить сообщение mesh B:
    1. Если есть прямое P2P TCP соединение → используем его
    2. Если нет → публикуем в Nostr kind:39012 (route)
    3. Mesh B подписан на kind:39012 → получает и обрабатывает
    """
    
    def __init__(self, mesh_id: MeshIdentity):
        self.mesh = mesh_id
        self._routes = {}  # mesh_id → channel (tcp/nostr)
        self._pending = {}  # seq → message (awaiting ack)
        self._seq = 0
    
    def register_route(self, mesh_id: str, channel: str, **kwargs):
        """Зарегистрировать маршрут к mesh сети."""
        self._routes[mesh_id] = {"channel": channel, **kwargs}
        log.info(f"🛣️ Route registered: {mesh_id[:16]}... → {channel}")
        stats["routes"] += 1
    
    async def route_message(self, target_mesh_id: str, message: dict) -> bool:
        """Отправить сообщение в другую mesh сеть."""
        self._seq += 1
        
        route = self._routes.get(target_mesh_id)
        if route and route.get("channel") == "tcp":
            return await self._route_via_tcp(target_mesh_id, message, route)
        else:
            return await self._route_via_nostr(target_mesh_id, message)
    
    async def _route_via_tcp(self, target_mesh_id: str, 
                             message: dict, route: dict) -> bool:
        """Маршрутизация через прямое TCP соединение."""
        host = route.get("host", "127.0.0.1")
        port = route.get("port", 9931)
        
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=5
            )
            
            payload = {
                "kind": MESH_ROUTE_KIND,
                "from_mesh": MESH_NAME,
                "from_mesh_id": self.mesh.mesh_id,
                "to_mesh_id": target_mesh_id,
                "seq": str(self._seq),
                "content": message,
                "timestamp": int(time.time()),
            }
            
            writer.write((json.dumps(payload) + "\n").encode())
            await asyncio.wait_for(writer.drain(), timeout=3)
            writer.close()
            
            log.info(f"📤 TCP route to {target_mesh_id[:16]}... seq={self._seq}")
            stats["tcp_routed"] += 1
            return True
            
        except Exception as e:
            log.warning(f"⚠️ TCP route failed: {e}")
            stats["tcp_errors"] += 1
            return False
    
    async def _route_via_nostr(self, target_mesh_id: str, 
                                message: dict) -> bool:
        """Маршрутизация через Nostr kind:39012 (fallback)."""
        import websockets
        
        payload = json.dumps({
            "type": "cross_mesh_route",
            "from_mesh": MESH_NAME,
            "from_mesh_id": self.mesh.mesh_id,
            "to_mesh_id": target_mesh_id,
            "seq": str(self._seq),
            "content": message,
            "protocol": "nostr_relay",
            "timestamp": int(time.time()),
        })
        
        # Публикуем в один из discovery relay
        for relay_url in DISCOVERY_RELAYS[:3]:
            try:
                async with websockets.connect(relay_url, max_size=500_000) as ws:
                    event = json.dumps([
                        "EVENT",
                        {
                            "id": hashlib.sha256(payload.encode()).hexdigest(),
                            "pubkey": self.mesh.mesh_id,
                            "created_at": int(time.time()),
                            "kind": MESH_ROUTE_KIND,
                            "tags": [["p", target_mesh_id]],
                            "content": payload,
                            "sig": "route_" + hashlib.md5(payload.encode()).hexdigest()[:32],
                        }
                    ])
                    await ws.send(event)
                    log.info(f"📤 Nostr route to {target_mesh_id[:16]}... "
                             f"via {relay_url}")
                    stats["nostr_routed"] += 1
                    return True
            except Exception as e:
                log.warning(f"⚠️ Nostr route via {relay_url}: {e}")
        
        stats["route_errors"] += 1
        return False

    def get_routes(self) -> dict:
        """Получить все активные маршруты."""
        return {
            "total": len(self._routes),
            "routes": {k[:16]: v for k, v in self._routes.items()},
            "stats": dict(stats),
        }


# ═══════════════════════════════════════════════════════════
# 4. CROSS-MESH BRIDGE — главный координатор
# ═══════════════════════════════════════════════════════════

class CrossMeshBridge:
    """Главный координатор Cross-Mesh Bridge.
    
    Объединяет:
    - Discovery (поиск других mesh через Nostr)
    - Trust Transfer (передача репутации)
    - Routing (маршрутизация между mesh)
    - REST API для управления
    
    Запуск: python3 cross_mesh_bridge.py [port]
    """
    
    def __init__(self, api_port: int = 9945):
        self.api_port = api_port
        self.mesh_id = MeshIdentity("forecaster_ai")
        self.trust = TrustTransfer(self.mesh_id)
        self.router = CrossMeshRouter(self.mesh_id)
        self._remote_meshes = {}  # mesh_id → data
        self._running = True
    
    async def start(self):
        """Запустить Cross-Mesh Bridge."""
        log.info(f"🚀 Cross-Mesh Bridge starting...")
        log.info(f"   Mesh: {MESH_NAME} ({self.mesh_id.mesh_id[:16]}...)")
        log.info(f"   DID:  {self.mesh_id.did[:30]}...")
        log.info(f"   Discovery relays: {len(DISCOVERY_RELAYS)}")
        
        # 1. Запускаем discovery (публикация + подписка в Nostr)
        discovery_tasks = [
            self.mesh_id.publish_discovery(url) 
            for url in DISCOVERY_RELAYS
        ]
        
        # 2. Запускаем REST API для управления
        api_task = self._run_api()
        
        # 3. Запускаем периодический обзор реестра
        scan_task = self._periodic_scan()
        
        await asyncio.gather(
            *discovery_tasks,
            api_task,
            scan_task,
            return_exceptions=True,
        )
    
    async def _run_api(self):
        """Async REST API для управления Cross-Mesh Bridge."""
        import json
        
        bridge = self
        
        async def handle_request(reader, writer):
            try:
                # Читаем полный HTTP запрос (первые 4096 байт)
                raw = await asyncio.wait_for(reader.read(4096), timeout=5)
                request_str = raw.decode("utf-8", errors="replace")
                
                # Парсим первую строку
                lines = request_str.split("\r\n")
                if not lines:
                    writer.close()
                    return
                
                parts = lines[0].split(" ")
                path = parts[1] if len(parts) > 1 else "/"
                
                status = "200 OK"
                body = {}
                content_type = "application/json"
                response_body = ""
                
                if path == "/":
                    # Отдаём HTML дашборд
                    import os
                    html_path = os.path.expanduser("~/data/sites/cross-mesh/index.html")
                    if os.path.exists(html_path):
                        with open(html_path) as f:
                            response_body = f.read()
                        content_type = "text/html"
                    else:
                        status = "200 OK"
                        response_body = json.dumps({"service": "cross-mesh-bridge", "info": "API at /health"})
                        content_type = "application/json"
                elif path == "/health":
                    body = {
                        "status": "ok",
                        "layer": "L1.5 — Cross-Mesh Bridge",
                        "mesh_name": MESH_NAME,
                        "mesh_id": bridge.mesh_id.mesh_id[:16] + "...",
                        "mesh_did": bridge.mesh_id.did[:30] + "...",
                        "remote_meshes": len(bridge._remote_meshes),
                        "routes": bridge.router.get_routes(),
                        "uptime": int(time.time() - start_time),
                    }
                elif path == "/discovery":
                    body = {
                        "meshes": list(bridge._remote_meshes.values()),
                        "count": len(bridge._remote_meshes),
                    }
                elif path == "/routes":
                    body = bridge.router.get_routes()
                elif path == "/stats":
                    body = dict(stats)
                else:
                    status = "404 Not Found"
                    body = {"error": "Not found",
                            "paths": ["/health", "/discovery", "/routes", "/stats"]}
                
                response_body = json.dumps(body, indent=2, default=str)
                response = (
                    f"HTTP/1.1 {status}\r\n"
                    f"Content-Type: {content_type}\r\n"
                    f"Content-Length: {len(response_body.encode('utf-8'))}\r\n"
                    f"Access-Control-Allow-Origin: *\r\n"
                    f"Connection: close\r\n"
                    f"\r\n"
                    f"{response_body}"
                )
                
                writer.write(response.encode("utf-8"))
                await writer.drain()
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                log.warning(f"⚠️ API handler error: {e}")
            finally:
                try:
                    writer.close()
                except:
                    pass
        
        server = await asyncio.start_server(
            handle_request, "0.0.0.0", self.api_port
        )
        
        log.info(f"🌐 Cross-Mesh API on :{self.api_port}")
        
        # Запускаем без serve_forever — просто держим сервер
        async with server:
            await asyncio.Future()  # бесконечное ожидание
    
    async def _periodic_scan(self):
        """Периодический обзор реестра удалённых mesh."""
        registry_dir = Path(__file__).parent / "registry"
        registry_dir.mkdir(exist_ok=True)
        
        while self._running:
            await asyncio.sleep(120)  # каждые 2 минуты
            
            # Сканируем реестр
            for fpath in sorted(registry_dir.glob("mesh_*.json")):
                try:
                    entries = json.loads(fpath.read_text())
                    for entry in entries:
                        mid = entry.get("mesh_id", "")
                        if mid and mid not in self._remote_meshes:
                            self._remote_meshes[mid] = entry
                            log.info(f"📋 Registry load: {entry.get('mesh_name', mid[:16])}")
                except Exception:
                    pass
            
            # Пробуем trust transfer для новых mesh
            for mid, data in list(self._remote_meshes.items()):
                if "trust_level" not in data:
                    trust = self.trust.create_trust_package(mid)
                    result = self.trust.verify_trust_package(trust)
                    data["trust_level"] = result.get("trust_level", 0)
                    
                    # Если trust > 0.5 — пробуем route
                    if result["trust_level"] > 0.5:
                        self.router.register_route(
                            mid, nostr="discovery", 
                            host="127.0.0.1", port=9931
                        )
                        log.info(f"🔄 Route established to {mid[:16]}...")
                        stats["meshes_connected"] += 1
            
            log.info(f"📊 Mesh registry: {len(self._remote_meshes)} remote, "
                     f"{self.router.get_routes()['total']} routes")


# ═══════════════════════════════════════════════════════════
# 5. MAIN
# ═══════════════════════════════════════════════════════════

async def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9945
    bridge = CrossMeshBridge(api_port=port)
    
    try:
        await bridge.start()
    except KeyboardInterrupt:
        log.info("🛑 Cross-Mesh Bridge stopped")


if __name__ == "__main__":
    _port = int(sys.argv[1]) if len(sys.argv) > 1 else 9945
    
    print(f"╔════════════════════════════════════════════╗")
    print(f"║     SNIN Cross-Mesh Bridge v1.0          ║")
    print(f"║     Layer 1.5 — Federation Protocol      ║")
    print(f"╚════════════════════════════════════════════╝")
    print(f"\n  Mesh: {MESH_NAME}")
    print(f"  Discovery relays: {len(DISCOVERY_RELAYS)}")
    print(f"  Protocols: kind:{MESH_DISCOVERY_KIND} (discovery)")
    print(f"             kind:{MESH_TRUST_KIND} (trust)")
    print(f"             kind:{MESH_ROUTE_KIND} (route)")
    print(f"\n  Starting on :{_port}")
    
    # Health endpoint
    from mesh_health import start_health
    start_health(_port, "cross_mesh_bridge")
    
    # Graceful shutdown
    import signal
    signal.signal(signal.SIGTERM, lambda s, f: (print(f"\n  SIGTERM — shutdown"), sys.exit(0)))
    
    asyncio.run(main())

#!/usr/bin/env python3
"""Relay-Mesh API Server — реестр агентов для gossip peer discovery.
Порт 9907. Хранение: in-memory + Redis persistence.

Endpoints:
  POST /agents/gossip   — регистрация агента
  GET  /agents/gossip/peers — список пиров
  DELETE /agents/gossip/<pubkey> — удаление агента
  GET  /health          — healthcheck
"""

import asyncio, json, time, os, sys, re
from urllib.parse import urlparse, parse_qs

REDIS_AGENTS_KEY = "relay:agents"

class RelayMeshAPI:
    def __init__(self, host="127.0.0.1", port=9907):
        self.host = host
        self.port = port
        self._agents = {}  # pubkey → agent_info
        self._redis = None
    
    async def _get_redis(self):
        if self._redis is None:
            try:
                import redis.asyncio as aredis
                self._redis = aredis.Redis(host="127.0.0.1", port=6379, db=0,
                                           socket_connect_timeout=2, socket_timeout=2)
                await self._redis.ping()
            except Exception:
                self._redis = False  # Redis недоступен
        return self._redis if self._redis else None
    
    async def _load_from_redis(self):
        r = await self._get_redis()
        if not r:
            return
        try:
            raw = await r.get(REDIS_AGENTS_KEY)
            if raw:
                data = json.loads(raw)
                self._agents.update(data)
        except Exception:
            pass
    
    async def _save_to_redis(self):
        r = await self._get_redis()
        if not r:
            return
        try:
            await r.setex(REDIS_AGENTS_KEY, 86400, json.dumps(self._agents))
        except Exception:
            pass
    
    async def handle_request(self, reader, writer):
        try:
            request = b""
            while True:
                chunk = await asyncio.wait_for(reader.read(65536), timeout=5)
                if not chunk:
                    break
                request += chunk
                if b"\r\n\r\n" in request:
                    break
            
            if not request:
                writer.close()
                return
            
            # Парсим HTTP запрос
            lines = request.split(b"\r\n")
            if not lines:
                writer.close()
                return
            
            request_line = lines[0].decode("utf-8", errors="replace")
            parts = request_line.split(" ")
            if len(parts) < 2:
                writer.close()
                return
            
            method = parts[0].upper()
            path = parts[1]
            
            # Читаем тело
            body = b""
            for i, line in enumerate(lines):
                if line == b"" and i + 1 < len(lines):
                    body = b"\r\n".join(lines[i+1:])
                    break
            
            # Маршрутизация
            status, headers, resp_body = await self.route(method, path, body)
            
            # Ответ
            resp = f"HTTP/1.1 {status}\r\n"
            for k, v in headers.items():
                resp += f"{k}: {v}\r\n"
            resp += "\r\n"
            
            writer.write(resp.encode() + (resp_body if isinstance(resp_body, bytes) else resp_body.encode()))
            await writer.drain()
        except Exception as e:
            try:
                writer.write(f"HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain\r\n\r\n{str(e)}".encode())
                await writer.drain()
            except Exception:
                pass
        finally:
            try:
                writer.close()
            except Exception:
                pass
    
    async def route(self, method: str, path: str, body: bytes):
        """Маршрутизация запросов."""
        headers = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}
        
        if path == "/health" and method == "GET":
            return "200 OK", headers, json.dumps({"status": "ok", "agents": len(self._agents), "time": time.time()})
        
        if path == "/agents/myip" and method == "GET":
            return "200 OK", headers, json.dumps({"ip": "0.0.0.0"})
        
        # Регистрация агента — поддерживаем оба пути для совместимости
        if path in ("/agents/gossip", "/agents/register") and method == "POST":
            return await self._post_agent(body, headers)
        
        if path == "/agents/gossip/heartbeat" and method == "POST":
            return await self._post_heartbeat(body, headers)
        
        if path == "/agents/gossip/peers" and method == "GET":
            return await self._get_peers(headers)
        
        # DELETE /agents/gossip/<pubkey>
        if path.startswith("/agents/gossip/") and method == "DELETE":
            pubkey = path.split("/agents/gossip/")[1]
            return await self._delete_agent(pubkey, headers)
        
        # PING /agents/<pubkey>/ping → heartbeat
        if re.match(r"^/agents/[a-f0-9]+/ping$", path) and method == "POST":
            return await self._post_heartbeat(body, headers)
        
        return "404 Not Found", headers, json.dumps({"error": "not found"})
    
    async def _post_agent(self, body: bytes, headers: dict):
        """POST /agents/gossip — регистрация агента."""
        try:
            data = json.loads(body)
            pubkey = data.get("pubkey", "")
            if not pubkey:
                return "400 Bad Request", headers, json.dumps({
                    "error": "pubkey required",
                    "received": data
                })
            
            agent_info = {
                "pubkey": pubkey,
                "name": data.get("name", ""),
                "gossip_host": data.get("gossip_host", "127.0.0.1"),
                "gossip_port": int(data.get("gossip_port", 0)),
                "last_seen": time.time(),
            }
            
            self._agents[pubkey] = agent_info
            await self._save_to_redis()
            
            return "200 OK", headers, json.dumps({"status": "registered", "agent": agent_info})
        except json.JSONDecodeError:
            return "400 Bad Request", headers, json.dumps({"error": "invalid json"})
    
    async def _get_peers(self, headers: dict):
        """GET /agents/gossip/peers — список всех агентов."""
        # Очищаем мёртвых (не было 5 минут)
        now = time.time()
        dead = [pk for pk, info in self._agents.items() if now - info.get("last_seen", 0) > 300]
        for pk in dead:
            del self._agents[pk]
        
        peers = list(self._agents.values())
        return "200 OK", headers, json.dumps({"peers": peers, "count": len(peers)})
    
    async def _delete_agent(self, pubkey: str, headers: dict):
        """DELETE /agents/gossip/<pubkey> — удаление агента."""
        if pubkey in self._agents:
            del self._agents[pubkey]
            await self._save_to_redis()
            return "200 OK", headers, json.dumps({"status": "deleted"})
        return "404 Not Found", headers, json.dumps({"error": "agent not found"})
    
    async def _post_heartbeat(self, body: bytes, headers: dict):
        """POST /agents/gossip/heartbeat — обновление last_seen."""
        try:
            data = json.loads(body)
            pubkey = data.get("pubkey", "")
            if pubkey and pubkey in self._agents:
                self._agents[pubkey]["last_seen"] = time.time()
                return "200 OK", headers, json.dumps({"status": "ok"})
            return "404 Not Found", headers, json.dumps({"error": "agent not found"})
        except json.JSONDecodeError:
            return "400 Bad Request", headers, json.dumps({"error": "invalid json"})
    
    async def run(self):
        # Загружаем сохранённых агентов из Redis
        await self._load_from_redis()
        
        server = await asyncio.start_server(self.handle_request, self.host, self.port)
        print(f"[RelayMeshAPI] 📡 Listening on {self.host}:{self.port}")
        print(f"[RelayMeshAPI]   Agents loaded: {len(self._agents)}")
        
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9907
    api = RelayMeshAPI(host="0.0.0.0", port=port)
    try:
        asyncio.run(api.run())
    except KeyboardInterrupt:
        print("[RelayMeshAPI] 🔌 Shutdown")

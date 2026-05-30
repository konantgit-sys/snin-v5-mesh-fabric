"""SNIN Mesh Agent Client — библиотека для подключения AI-агентов к SmartRouter сети.

Использование:
    from mesh_client import MeshAgent
    
    agent = MeshAgent(
        pubkey="npub1forecaster...",
        name="forecaster_ai",
        mesh_host="127.0.0.1",
        mesh_port=9932,
        api_url="http://127.0.0.1:9907"
    )
    
    # Отправить сообщение
    result = await agent.send(
        to="npub1archivist...",
        payload={"text": "hello"},
        channel="mesh"
    )
    
    # Получить статус сети
    status = await agent.ping()
"""

import asyncio
import json
import time
import uuid
from typing import Optional

class MeshAgent:
    """Клиент для подключения AI-агента к SNIN Mesh."""
    
    def __init__(self, pubkey: str, name: str = "", 
                 mesh_host: str = "127.0.0.1", mesh_port: int = 9932,
                 api_url: str = "http://127.0.0.1:9907"):
        self.pubkey = pubkey
        self.name = name or pubkey[:16]
        self.mesh_host = mesh_host
        self.mesh_port = mesh_port
        self.api_url = api_url.rstrip("/")
        self._reader = None
        self._writer = None
        self._connected = False
        self._msg_id = 0
        # ═══ Phase 8: Subscribe/Push ═══
        self._sub_reader = None
        self._sub_writer = None
        self._sub_connected = False
        self._listener_task = None
        self._on_push_event = None
        # ═══ Phase 9: Gossip ═══
        self.gossip = None  # GossipPeer instance
    
    async def connect(self) -> bool:
        """Подключиться к SmartRouter."""
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.mesh_host, self.mesh_port), 
                timeout=5
            )
            self._connected = True
            return True
        except Exception as e:
            print(f"[{self.name}] ❌ Connect failed: {e}")
            return False
    
    async def disconnect(self):
        """Закрыть соединение."""
        await self._stop_listener()
        for conn_name, reader, writer in [("main", self._reader, self._writer), ("sub", self._sub_reader, self._sub_writer)]:
            if writer:
                try:
                    writer.close()
                except:
                    pass
        self._connected = False
        self._sub_connected = False
    
    async def subscribe(self, on_push_event=None) -> bool:
        """Подписаться на push-события от SR (отдельное TCP-соединение).
        
        on_push_event — async callback, вызывается для каждого события.
        """
        self._on_push_event = on_push_event
        try:
            self._sub_reader, self._sub_writer = await asyncio.wait_for(
                asyncio.open_connection(self.mesh_host, self.mesh_port),
                timeout=5
            )
            # Send subscribe
            msg = json.dumps({"kind": "subscribe", "from": self.pubkey, "name": self.name}).encode() + b"\n"
            self._sub_writer.write(msg)
            await asyncio.wait_for(self._sub_writer.drain(), timeout=3)
            # Read response
            line = await asyncio.wait_for(self._sub_reader.readline(), timeout=5)
            resp = json.loads(line)
            if resp.get("subscribed"):
                self._sub_connected = True
                self._sub_id = resp.get("sub_id", -1)
                print(f"[{self.name}] ✅ Subscribed to mesh events (sub_id={self._sub_id})")
                # Start listener
                self._listener_task = asyncio.create_task(self._push_listener())
                return True
            else:
                print(f"[{self.name}] ❌ Subscribe rejected: {resp}")
                return False
        except Exception as e:
            print(f"[{self.name}] ❌ Subscribe failed: {e}")
            return False
    
    async def unsubscribe(self):
        """Отписаться от push-событий."""
        if self._sub_writer and self._sub_connected:
            try:
                msg = json.dumps({"kind": "unsubscribe", "from": self.pubkey, "sub_id": getattr(self, '_sub_id', -1)})
                self._sub_writer.write(msg.encode() + b"\n")
                await asyncio.wait_for(self._sub_writer.drain(), timeout=3)
            except:
                pass
        await self._stop_listener()
        self._sub_connected = False
        print(f"[{self.name}] ❌ Unsubscribed from mesh events")
    
    async def _push_listener(self):
        """Фоновый слушатель push-событий от SR (читает из _sub_reader).
        
        Phase 3: Auto-reconnect при обрыве связи с SR.
        """
        reader = self._sub_reader
        max_retries = 10
        retry_delay = 5  # начальная задержка, сек
        
        while self._sub_connected and max_retries > 0:
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=60)
                if not line:
                    break
                line = line.decode().strip()
                if not line:
                    continue
                data = json.loads(line)
                if data.get("type") == "push":
                    kind = data.get("kind", "")
                    if kind == "pipeline_feed":
                        payload = data.get("payload", {})
                        events_list = payload.get("events", []) if isinstance(payload, dict) else []
                        for ev in events_list:
                            if self._on_push_event:
                                await self._on_push_event(ev)
                    else:
                        if self._on_push_event:
                            await self._on_push_event(data)
                        else:
                            frm = data.get("from", "?")[:24]
                            print(f"[{self.name}] 📨 Push event kind={kind} from={frm}")
                elif data.get("type") == "ping":
                    pass
                # После успешного чтения сбрасываем retry
                retry_delay = 5
                max_retries = 10
            except asyncio.TimeoutError:
                continue
            except (ConnectionResetError, BrokenPipeError, OSError):
                # Phase 3: Попытка переподключения
                max_retries -= 1
                if max_retries <= 0:
                    break
                print(f"[{self.name}] ⚠️ SR connection lost, retry in {retry_delay}s ({max_retries} left)")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)  # exponential backoff
                try:
                    self._sub_reader, self._sub_writer = await asyncio.wait_for(
                        asyncio.open_connection(self.mesh_host, self.mesh_port),
                        timeout=5
                    )
                    # Re-subscribe
                    msg = json.dumps({"kind": "subscribe", "from": self.pubkey, "name": self.name}).encode() + b"\n"
                    self._sub_writer.write(msg)
                    await asyncio.wait_for(self._sub_writer.drain(), timeout=3)
                    resp_line = await asyncio.wait_for(self._sub_reader.readline(), timeout=5)
                    resp = json.loads(resp_line)
                    if resp.get("subscribed"):
                        reader = self._sub_reader
                        print(f"[{self.name}] ✅ Reconnected & re-subscribed")
                        continue
                except Exception as reconnect_err:
                    print(f"[{self.name}] ⚠️ Reconnect failed: {reconnect_err}")
                    continue
            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"[{self.name}] ⚠️ Push listener error: {e}")
                break
        
        self._sub_connected = False
        print(f"[{self.name}] 🔌 Push listener stopped (mode: gossip-only if peers available)")
    
    async def _stop_listener(self):
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            self._listener_task = None
    
    async def send(self, to: str = "broadcast", payload: dict = None,
                   kind: int = 39002, channel: str = "auto", 
                   priority: str = "normal") -> dict:
        """Отправить сообщение через SmartRouter."""
        if not self._connected:
            if not await self.connect():
                return {"ok": False, "error": "not connected"}
        
        msg = {
            "kind": kind,
            "pubkey": self.pubkey,
            "from": self.pubkey,
            "to": to,
            "id": f"{self.pubkey[:16]}_{self._msg_id}_{int(time.time()*1000)}",
            "meta": {
                "channel": channel,
                "priority": priority,
                "timestamp": time.time(),
                "agent": self.name
            },
            "payload": payload or {}
        }
        self._msg_id += 1
        
        try:
            self._writer.write(json.dumps(msg).encode() + b"\n")
            await asyncio.wait_for(self._writer.drain(), timeout=5)
            return {"ok": True, "id": msg["id"]}
        except Exception as e:
            self._connected = False
            return {"ok": False, "error": str(e)}
    
    async def broadcast(self, payload: dict, kind: int = 39002) -> dict:
        """Отправить всем агентам в сети."""
        return await self.send(to="broadcast", payload=payload, kind=kind)
    
    async def register(self, gossip_port: int = 0, gossip_host: str = "") -> dict:
        """Зарегистрировать агента в Mesh API.
        
        Args:
            gossip_port: UDP/TCP порт gossip-канала агента
            gossip_host: IP для gossip (по умолчанию автоматически)
        """
        import urllib.request
        # Определяем порт по соглашению: forecaster=9911, archivist=9912, anton=9913
        if not gossip_port and "_ai" in self.name:
            port_map = {"forecaster": 9911, "archivist": 9912, "anton": 9913}
            prefix = self.name.split("_ai")[0].split("_")[0]
            gossip_port = port_map.get(prefix, 0)
        if not gossip_host:
            import socket
            try:
                gossip_host = socket.gethostbyname(socket.gethostname())
            except:
                gossip_host = "127.0.0.1"
        data = json.dumps({
            "pubkey": self.pubkey,
            "name": self.name,
            "gossip_host": gossip_host,
            "gossip_port": gossip_port,
            "meta": {
                "type": "ai-agent",
                "role": self.name.split("_")[0] if "_" in self.name else "agent",
                "version": "1.0"
            }
        }).encode()
        req = urllib.request.Request(
            f"{self.api_url}/agents/register",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        try:
            resp = urllib.request.urlopen(req, timeout=5)
            return json.loads(resp.read())
        except Exception as e:
            return {"ok": False, "error": str(e)}
    
    async def ping(self) -> dict:
        """Проверить статус агента в сети."""
        import urllib.request
        try:
            req = urllib.request.Request(f"{self.api_url}/agents/{self.pubkey}/ping", method="POST")
            resp = urllib.request.urlopen(req, timeout=3)
            return json.loads(resp.read())
        except Exception as e:
            return {"ok": False, "error": str(e)}
    
    async def listen_events(self, ws_host: str = "127.0.0.1", ws_port: int = 9909,
                            on_event=None):
        """Подключиться к WS :9909 и слушать события из Relay Mesh.
        
        on_event — callback, вызывается для каждого полученного события.
        Если None — выводит в лог.
        Блокирует выполнение пока соединение живо.
        """
        import websockets
        uri = f"ws://{ws_host}:{ws_port}"
        print(f"[{self.name}] 🔗 Connecting to mesh events: {uri}")
        try:
            async with websockets.connect(uri, ping_interval=15) as ws:
                print(f"[{self.name}] ✅ Connected to mesh events")
                async for raw in ws:
                    if isinstance(raw, bytes):
                        raw = raw.decode()
                    if not raw:
                        continue
                    try:
                        data = json.loads(raw)
                        event = data.get("event", data)
                        if on_event:
                            await on_event(event)
                        else:
                            kind = event.get("kind", "?")
                            frm = event.get("from", "?")[:24]
                            print(f"[{self.name}] 📨 Event kind={kind} from={frm}")
                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            print(f"[{self.name}] ❌ Mesh events connection error: {e}")
    
    @staticmethod
    async def get_network_status(api_url: str = "http://127.0.0.1:9907") -> dict:
        """Получить статус всей сети."""
        import urllib.request
        try:
            resp = urllib.request.urlopen(f"{api_url}/health", timeout=3)
            return json.loads(resp.read())
        except Exception as e:
            return {"ok": False, "error": str(e)}


# ─── Console test ───
class GossipPeer:
    """P2P gossip между агентами — напрямую, без SmartRouter.
    
    Каждый агент:
      - слушает на своём gossip_port (TCP сервер)
      - подключается к gossip_port других агентов
      - шлёт/получает сообщения напрямую
    """
    
    def __init__(self, agent_name: str, listen_port: int, on_gossip=None):
        self.agent_name = agent_name
        self.listen_port = listen_port
        self._on_gossip = on_gossip  # async callback(gossip_data)
        self._server = None
        self._peers: dict[str, tuple] = {}  # name → (reader, writer)
        self._running = False
        self._stats = {"sent": 0, "recv": 0, "errors": 0}
    
    async def start(self):
        """Запустить gossip TCP сервер."""
        try:
            self._server = await asyncio.start_server(
                self._handle_peer,
                "127.0.0.1",
                self.listen_port,
            )
            self._running = True
            print(f"[Gossip-{self.agent_name}] Server on :{self.listen_port}")
            return True
        except Exception as e:
            print(f"[Gossip-{self.agent_name}] ❌ Server failed: {e}")
            return False
    
    async def connect_to(self, peer_name: str, host: str, port: int) -> bool:
        """Подключиться к gossip серверу другого агента."""
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=3
            )
            # Приветствие — кто я
            hello = json.dumps({
                "kind": "gossip_hello",
                "from": self.agent_name,
                "ts": time.time()
            }) + "\n"
            w.write(hello.encode())
            await asyncio.wait_for(w.drain(), timeout=2)
            
            self._peers[peer_name] = (r, w)
            print(f"[Gossip-{self.agent_name}] ✅ Connected to {peer_name}:{port}")
            
            # Запускаем чтение из этого пира в фоне
            asyncio.create_task(self._peer_reader(peer_name, r))
            return True
        except Exception as e:
            print(f"[Gossip-{self.agent_name}] ⚠️ Can't connect to {peer_name}:{port}: {e}")
            return False
    
    async def send(self, data: dict):
        """Разослать gossip-сообщение всем подключённым пирам."""
        if not self._peers:
            return
        dead = []
        msg = json.dumps(data) + "\n"
        for name, (r, w) in self._peers.items():
            try:
                w.write(msg.encode())
                await asyncio.wait_for(w.drain(), timeout=2)
                self._stats["sent"] += 1
            except Exception:
                dead.append(name)
                self._stats["errors"] += 1
        for name in dead:
            del self._peers[name]
            print(f"[Gossip-{self.agent_name}] Peer {name} disconnected")
    
    async def _handle_peer(self, reader, writer):
        """Обработка входящего gossip-соединения от другого агента."""
        peer_name = f"peer_{id(reader):x}"
        try:
            # Читаем приветствие
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            if not line:
                writer.close()
                return
            data = json.loads(line.decode().strip())
            if data.get("kind") == "gossip_hello":
                peer_name = data.get("from", peer_name)
                print(f"[Gossip-{self.agent_name}] ← {peer_name} connected")
            
            # Читаем сообщения от этого пира
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=120)
                if not line:
                    break
                msg = json.loads(line.decode().strip())
                self._stats["recv"] += 1
                if self._on_gossip:
                    await self._on_gossip(msg, peer_name)
        except asyncio.TimeoutError:
            pass
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            self._stats["errors"] += 1
        finally:
            writer.close()
            print(f"[Gossip-{self.agent_name}] Peer {peer_name} disconnected")
    
    async def _peer_reader(self, peer_name: str, reader):
        """Фоновое чтение сообщений от конкретного пира."""
        try:
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=120)
                if not line:
                    break
                msg = json.loads(line.decode().strip())
                self._stats["recv"] += 1
                if self._on_gossip:
                    await self._on_gossip(msg, peer_name)
        except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            self._stats["errors"] += 1
    
    async def stop(self):
        """Остановить gossip сервер."""
        if self._server:
            self._server.close()
        for name, (r, w) in self._peers.items():
            try:
                w.close()
            except:
                pass
        self._peers.clear()
        self._running = False
        print(f"[Gossip-{self.agent_name}] Stopped, stats: {self._stats}")


if __name__ == "__main__":
    async def test():
        agent = MeshAgent(
            pubkey="npub1testagent...",
            name="test_agent",
        )
        # Register
        r = await agent.register()
        print(f"Register: {r}")
        # Connect
        c = await agent.connect()
        print(f"Connect: {c}")
        # Send
        s = await agent.send(to="broadcast", payload={"text": "hello mesh!"})
        print(f"Send: {s}")
        # Status
        status = await MeshAgent.get_network_status()
        print(f"Network: {status.get('pools',{}).get('workers_alive',0)} workers, {status.get('agents',0)} agents")
        await agent.disconnect()
    
    asyncio.run(test())

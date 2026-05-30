#!/usr/bin/env python3
"""SNIN Agent Gossip Channel — Peer-to-peer между AI-агентами.

Каждый агент запускает gossip-сервер на своём порту.
Агенты находят друг друга через регистрацию в SR (DHT / API).
После обнаружения — общаются напрямую, без посредников.

Порты (конвенция для localhost-тестирования):
  forecaster_ai → 9911
  archivist_ai  → 9912
  anton_ai      → 9913
"""

import asyncio, json, time, uuid, os
from typing import Optional

import sys
sys.path.insert(0, "/home/agent/data/sites/relay-mesh")
from gossip_stream import GossipStream
import mesh_crypto

# Маппинг имён агентов на gossip-порты (локальный тест)
AGENT_GOSSIP_PORTS = {
    "forecaster_ai": 9911,
    "archivist_ai": 9912,
    "anton_ai": 9913,
}


class AgentGossipChannel:
    """Gossip-канал агента: p2p-коммуникация с другими агентами.
    
    Использует GossipStream внутри, добавляет:
    - Регистрацию gossip-адреса в SR через MeshAgent
    - Discovery пиров через push-события от SR
    - Авто-подключение к новым пирам
    - ═══ Phase 3: Health-check пиров + dead peer detection ═══
    """
    
    HEARTBEAT_INTERVAL = 30  # сек — ping
    MISSED_MAX = 3           # пропущенных пингов = мёртвый пир
    RECONNECT_INTERVAL = 60  # сек — попытка переподключения к мёртвому
    
    def __init__(self, agent_name: str, pubkey: str, 
                 api_url: str = "",
                 mesh_agent=None):
        self.agent_name = agent_name
        self.pubkey = pubkey
        self.api_url = (api_url or "https://snin-gossip.v2.site").rstrip("/")
        self.mesh_agent = mesh_agent  # MeshAgent для регистрации в SR
        self.gossip = None  # GossipStream instance
        self.gossip_port = AGENT_GOSSIP_PORTS.get(agent_name, 0)
        self._peers = {}  # pubkey → {"host": str, "port": int}
        self._running = False
        # ═══ Phase 3: Health ═══
        self._peer_health: dict[str, dict] = {}  # pubkey → {"missed": 0, "last_ok": 0, "dead": False}
        self._health_task = None
        self._dead_peers: dict[str, dict] = {}   # pubkey → host/port (for reconnect)
        # ═══ Phase 3b: Channel quality (self-learning) ═══
        self._channel_quality: dict[str, dict] = {}
        self.best_channel: str = ""
        self._quality_task = None
        self._on_peer_change = None  # callback при изменении состояния пира
        self._on_gossip_message = None  # callback при получении gossip-сообщения (кроме ping/pong)
        # ═══ Phase 2: Cipher keys для шифрования gossip ═══
        self._peer_cipher: dict[str, str] = {}  # pubkey → cipher_pubkey hex
        self._peer_names: dict[str, str] = {}   # pubkey → agent_name
        # ═══ Phase 2: Ack/Retry гарантия доставки ═══
        self._pending_ack: dict[str, dict] = {}  # msg_id → {payload, peer_pubkey, attempt, ts}
        self._ack_timeout = 5.0                   # секунд ждать ack
        self._ack_max_attempts = 3                # макс ретраев
        self._ack_task = None                     # фоновый task
    
    @property
    def listen_host(self) -> str:
        return "0.0.0.0"  # слушать на всех интерфейсах
    
    @property  
    def gossip_host(self) -> str:
        return "155.212.133.195"  # внешний IP для регистрации в API
        return AGENT_GOSSIP_PORTS.get(self.agent_name, 9911)
    
    async def start(self) -> bool:
        """Запустить gossip-сервер и зарегистрироваться в SR."""
        if self.gossip_port == 0:
            print(f"[{self.agent_name}] ❌ No gossip port configured")
            return False
        
        # Создаём GossipStream
        self.gossip = GossipStream(
            pubkey=self.pubkey,
            listen_host=self.listen_host,
        )
        
        # Запускаем сервер
        try:
            await self.gossip.start_server_async(port=self.gossip_port)
            print(f"[{self.agent_name}] ✅ Gossip server on :{self.gossip_port}")
        except Exception as e:
            print(f"[{self.agent_name}] ❌ Gossip server failed: {e}")
            return False
        
        # Регистрируем gossip-адрес в SR (через /agents/ API)
        await self._register_gossip_addr()
        
        # ═══ Phase 2: Загружаем cipher_pubkey всех известных агентов ═══
        await self._load_known_agent_keys()
        
        # Устанавливаем callback для входящих сообщений
        self.gossip.on_data = self._on_gossip_data
        
        # Запрашиваем список пиров
        await self._discover_peers()
        
        self._running = True
        
        # ═══ Phase 3: Запускаем health-check ═══
        self._health_task = asyncio.create_task(self._health_loop())
        print(f"[{self.agent_name}] ❤️ Health-check started (every {self.HEARTBEAT_INTERVAL}s)")
        
        # ═══ Phase 3b: Запускаем quality tracker ═══
        self._quality_task = asyncio.create_task(self._quality_loop())
        print(f"[{self.agent_name}] 📊 Quality tracking started")
        
        # ═══ Phase 2: Запускаем ack-мониторинг ═══
        self._ack_task = asyncio.create_task(self._ack_monitor_loop())
        print(f"[{self.agent_name}] 📬 Ack/Retry monitor started")
        
        return True
    
    async def _ack_monitor_loop(self):
        """Проверять _pending_ack: retry если timeout."""
        while self._running:
            await asyncio.sleep(1.0)  # проверка раз в секунду
            now = time.time()
            timed_out = []
            for msg_id, entry in list(self._pending_ack.items()):
                # ═══ Пропускаем сообщения, у которых send_to сам ждёт ack (wait_ack=True) ═══
                if "event" in entry:
                    continue
                age = now - entry["ts"]
                if age >= self._ack_timeout:
                    timed_out.append(msg_id)
            
            for msg_id in timed_out:
                entry = self._pending_ack.get(msg_id)
                if not entry:
                    continue
                entry["attempt"] += 1
                if entry["attempt"] > self._ack_max_attempts:
                    print(f"[{self.agent_name}] ❌ Ack failed after {self._ack_max_attempts} attempts: {msg_id[:12]}")
                    self._pending_ack.pop(msg_id, None)
                    if self._on_peer_change:
                        try:
                            await self._on_peer_change(entry["peer_pubkey"], "ack_timeout")
                        except Exception:
                            pass
                    continue
                
                # Retry
                peer_pk = entry["peer_pubkey"]
                payload = entry["payload"]
                print(f"[{self.agent_name}] 🔄 Retry #{entry['attempt']} for {msg_id[:12]} → {peer_pk[:16]}")
                entry["ts"] = time.time()
                if self.gossip:
                    try:
                        await self.gossip.send_to(peer_pk, payload)
                    except Exception as e:
                        print(f"[{self.agent_name}] ⚠️ Retry send: {e}")
    
    async def _send_ack(self, to_pubkey: str, msg_id: str):
        """Отправить ack обратно отправителю."""
        if not self.gossip:
            return
        try:
            ack_payload = {
                "kind": 39004,
                "type": "ack",
                "msg_id": msg_id,
                "timestamp": time.time(),
            }
            await self.gossip.send_to(to_pubkey, ack_payload)
            print(f"[{self.agent_name}] ↩️ Ack sent for {msg_id[:12]} → {to_pubkey[:16]}")
        except Exception as e:
            print(f"[{self.agent_name}] ⚠️ Ack send error: {e}")
    
    async def _health_loop(self):
        """Фоновый цикл: ping пирам через прямой TCP (обходит GossipStream.WriterPool)."""
        while self._running:
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            if not self._peers:
                if not self._running:
                    break
                await self._discover_peers()
                continue
            
            now = time.time()
            for pk, info in list(self._peers.items()):
                if not self._running:
                    break
                try:
                    # Используем ПРЯМОЙ TCP ping — WriterPool ненадёжен
                    ok, latency = await self._tcp_ping(info["host"], info["port"])
                    self._record_quality(pk, ok, latency)
                    if ok:
                        h = self._peer_health.setdefault(pk, {"missed": 0, "last_ok": 0, "dead": False})
                        h["missed"] = 0
                        h["last_ok"] = now
                        if h.get("dead"):
                            h["dead"] = False
                            print(f"[{self.agent_name}] 🔄 Peer {pk[:16]} revived (TCP)")
                            if self._on_peer_change:
                                await self._on_peer_change(pk, "alive")
                    else:
                        self._mark_missed(pk)
                except Exception:
                    self._mark_missed(pk)
            
            # Попытка reconnect к мёртвым пирам
            for pk, info in list(self._dead_peers.items()):
                if now - info.get("last_try", 0) < self.RECONNECT_INTERVAL:
                    continue
                self._dead_peers[pk]["last_try"] = now
                try:
                    await self._connect_peer(pk, info["host"], info["port"])
                    if pk in self._dead_peers:
                        del self._dead_peers[pk]
                except Exception:
                    pass
    
    async def _tcp_ping(self, host: str, port: int) -> tuple[bool, float]:
        """Прямой TCP ping: возвращает (ok, latency_seconds)."""
        start = time.time()
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=5
            )
            # Отправляем ping в формате gossip_stream
            msg = json.dumps({
                "kind": 39004,
                "pubkey": self.pubkey,
                "created_at": int(time.time() * 1000),
                "content": {
                    "target_pubkey": "self",
                    "payload": {"type": "ping", "ts": time.time()},
                    "nonce": "ping:" + str(int(time.time()))
                }
            }).encode() + b"\n"
            w.write(msg)
            await asyncio.wait_for(w.drain(), timeout=3)
            
            # Ждём ACK
            try:
                resp = await asyncio.wait_for(r.readline(), timeout=3)
                latency = time.time() - start
                w.close()
                ok = b"ack" in resp.lower() or b"ok" in resp.lower() or len(resp) > 0
                return ok, latency
            except asyncio.TimeoutError:
                latency = time.time() - start
                w.close()
                return True, latency  # TCP открыт — пир жив, даже без ACK
        except (OSError, asyncio.TimeoutError, ConnectionRefusedError, ConnectionResetError):
            return False, 0.0
    
    def _record_quality(self, pk: str, ok: bool, latency: float = 0):
        """Записать метрику качества канала к пиру."""
        q = self._channel_quality.setdefault(pk, {"success": 0, "fail": 0, "latency_sum": 0.0, "count": 0, "avg_latency": 0.0})
        q["count"] += 1
        if ok:
            q["success"] += 1
            q["latency_sum"] += latency
            q["avg_latency"] = q["latency_sum"] / max(q["success"], 1)
        else:
            q["fail"] += 1
    
    async def _quality_loop(self):
        """Фоновый цикл: обновление best_channel на основе качества."""
        while self._running:
            await asyncio.sleep(self.HEARTBEAT_INTERVAL * 2)  # раз в 60с
            self._update_best_channel()
    
    def _update_best_channel(self):
        """Выбрать пира с лучшим качеством (max success/rate, min latency)."""
        best_pk = ""
        best_score = -1.0
        
        for pk, q in self._channel_quality.items():
            if q["count"] < 3:
                continue  # недостаточно данных
            total = q["success"] + q["fail"]
            if total == 0:
                continue
            success_rate = q["success"] / total
            latency_penalty = min(q["avg_latency"] / 2.0, 1.0)  # 0-1 penalty
            score = success_rate * (1.0 - latency_penalty * 0.3)  # latency 30% веса
            
            if pk in self._peer_health and self._peer_health[pk].get("dead", False):
                score *= -1  # мёртвые не могут быть лучшими
            
            if score > best_score:
                best_score = score
                best_pk = pk
        
        old = self.best_channel
        self.best_channel = best_pk
        if old != best_pk and best_pk:
            print(f"[{self.agent_name}] 🏆 Best channel switched to {best_pk[:16]} (score={best_score:.2f})")
    
    def _mark_missed(self, pk: str):
        """Увеличить счётчик пропущенных пингов для пира."""
        h = self._peer_health.setdefault(pk, {"missed": 0, "last_ok": 0, "dead": False})
        h["missed"] += 1
        if h["missed"] >= self.MISSED_MAX and not h["dead"]:
            h["dead"] = True
            info = self._peers.pop(pk, None)
            if info:
                self._dead_peers[pk] = {**info, "last_try": time.time()}
            print(f"[{self.agent_name}] 💀 Peer {pk[:16]} declared dead ({self.MISSED_MAX} missed)")
            if self._on_peer_change:
                # Этот вызов требует существования event loop
                asyncio.ensure_future(self._on_peer_change(pk, "dead"))
    
    async def _on_gossip_data(self, from_pubkey: str, payload: dict, nonce: str):
        """Callback для входящих gossip-сообщений (включая ping)."""
        kind = payload.get("kind", payload.get("type", ""))
        
        # ═══ Ping/pong для health-check ═══
        if kind == 39001 or payload.get("type") == "ping":
            # Отвечаем pong
            await self.gossip.send_to(from_pubkey, {
                "type": "pong",
                "ts": time.time(),
                "echo": payload.get("ts", 0)
            })
            return
        
        if payload.get("type") == "pong":
            # Ping получил ответ — обновляем health
            pk = from_pubkey
            if pk in self._peer_health:
                h = self._peer_health[pk]
                h["missed"] = 0
                h["last_ok"] = time.time()
                if h.get("dead"):
                    h["dead"] = False
                    print(f"[{self.agent_name}] 🔄 Peer {pk[:16]} revived (pong)")
                    if self._on_peer_change:
                        await self._on_peer_change(pk, "alive")
            return
        
        # ═══ Phase 2: Ack — получатель подтвердил доставку ═══
        if payload.get("type") == "ack" and payload.get("msg_id"):
            ack_msg_id = payload["msg_id"]
            if ack_msg_id in self._pending_ack:
                entry = self._pending_ack[ack_msg_id]
                if "event" in entry:
                    entry["event"].set()
                self._pending_ack.pop(ack_msg_id, None)
                print(f"[{self.agent_name}] ✅ Ack received for {ack_msg_id[:12]} from {from_pubkey[:16]}")
            return
        
        # ═══ Phase 2: Для всех обычных сообщений — отправляем ack обратно ═══
        msg_id = payload.get("msg_id", "")
        if msg_id and from_pubkey:
            asyncio.ensure_future(self._send_ack(from_pubkey, msg_id))
        
        # ═══ Обычное сообщение ═══
        print(f"[{self.agent_name}] 📩 Gossip kind={payload.get('kind', '?')} from={from_pubkey[:16]}")
        
        # ═══ Phase 2: Расшифровка если encrypted ═══
        if payload.get("encrypted") or payload.get("kind") == "encrypted":
            try:
                # Ищем cipher_pubkey отправителя
                sender_cipher = self._peer_cipher.get(from_pubkey, "")
                if not sender_cipher:
                    print(f"[{self.agent_name}] ⚠️ No cipher key for {from_pubkey[:16]}, skipping decrypt")
                else:
                    # Загружаем свой cipher_privkey
                    ident = mesh_crypto.load_identity(self.agent_name)
                    my_cipher_priv = ident.get("cipher_privkey", "")
                    cipher_content = payload.get("content", "")
                    if isinstance(cipher_content, str) and my_cipher_priv and sender_cipher:
                        decrypted = mesh_crypto.decrypt_from_agent(
                            cipher_content, my_cipher_priv, sender_cipher
                        )
                        payload["content"] = json.loads(decrypted)
                        print(f"[{self.agent_name}] 🔓 Decrypted gossip content")
            except Exception as e:
                print(f"[{self.agent_name}] ⚠️ Decrypt error: {e}")
        
        # ═══ Проброс в MeshMessageHandler через callback ═══
        if self._on_gossip_message:
            try:
                if asyncio.iscoroutinefunction(self._on_gossip_message):
                    await self._on_gossip_message(from_pubkey, payload)
                else:
                    self._on_gossip_message(from_pubkey, payload)
            except Exception as e:
                print(f"[{self.agent_name}] ⚠️ gossip callback error: {e}")
        
        # ═══ Форвард ретрансляция: пир → всем остальным ═══
        if hasattr(self, 'gossip') and self.gossip and hasattr(self.gossip, '_peers'):
            for pk in list(self.gossip._peers.keys())[:5]:
                if pk != from_pubkey and not self._peer_health.get(pk, {}).get("dead", False):
                    try:
                        await self.gossip.send_to(pk, payload)
                    except:
                        pass
    
    async def _load_known_agent_keys(self):
        """Загрузить cipher_pubkey для всех известных агентов."""
        known = ["forecaster_ai", "archivist_ai", "anton_ai"]
        for name in known:
            if name == self.agent_name:
                continue
            try:
                ident = mesh_crypto.load_identity(name)
                pk = ident.get("mesh_pubkey", "")
                cp = ident.get("cipher_pubkey", "")
                if pk and cp:
                    self._peer_cipher[pk] = cp
                    self._peer_names[pk] = name
                    print(f"[{self.agent_name}] 📇 Cipher key loaded: {name} ({pk[:16]}...)")
            except Exception as e:
                print(f"[{self.agent_name}] ⚠️ Load key {name}: {e}")
    
    async def _register_gossip_addr(self):
        """Зарегистрировать gossip-адрес в relay-mesh API."""
        if not self.mesh_agent:
            return
        try:
            import urllib.request
            data = json.dumps({
                "pubkey": self.pubkey,
                "name": self.agent_name,
                "gossip_host": self.gossip_host,
                "gossip_port": self.gossip_port,
            }).encode()
            req = urllib.request.Request(
                f"{self.api_url}/agents/gossip",
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            resp = urllib.request.urlopen(req, timeout=5)
            result = json.loads(resp.read())
            print(f"[{self.agent_name}] 📡 Gossip registered: :{self.gossip_port}")
            return result
        except Exception as e:
            print(f"[{self.agent_name}] ⚠️ Gossip register: {e}")
            return None
    
    async def _discover_peers(self):
        """Получить список известных пиров из relay-mesh."""
        if not self.mesh_agent:
            return
        try:
            import urllib.request
            req = urllib.request.Request(f"{self.api_url}/agents/gossip/peers")
            resp = urllib.request.urlopen(req, timeout=5)
            peers = json.loads(resp.read())
            if isinstance(peers, dict):
                peers_list = peers.get("peers", [])
            elif isinstance(peers, list):
                peers_list = peers
            else:
                peers_list = []
            
            for p in peers_list:
                pk = p.get("pubkey", "")
                if pk and pk != self.pubkey and pk not in self._peers:
                    await self._connect_peer(pk, p.get("gossip_host", "127.0.0.1"), 
                                            int(p.get("gossip_port", 0)))
            if peers_list:
                print(f"[{self.agent_name}] 👥 Peers discovered: {len(peers_list)}")
        except Exception as e:
            print(f"[{self.agent_name}] ⚠️ Peer discovery: {e}")
    
    async def _connect_peer(self, peer_pubkey: str, host: str, port: int):
        """Подключиться к gossip-серверу пира."""
        if not self.gossip or port <= 0:
            return
        if peer_pubkey in self._peers:
            return  # уже подключены
        
        try:
            await self.gossip.add_peer(peer_pubkey, host, port)
            self._peers[peer_pubkey] = {"host": host, "port": port}
            print(f"[{self.agent_name}] 🔗 Gossip peer {peer_pubkey[:16]} → {host}:{port}")
        except Exception as e:
            print(f"[{self.agent_name}] ⚠️ Peer connect {peer_pubkey[:16]}: {e}")
    
    async def on_peer_event(self, event: dict):
        """Обработчик push-события от SR: новый пир в сети.
        
        Вызывается из MeshAgent push listener, когда приходит
        событие с type=gossip_peer_update.
        """
        if event.get("type") == "gossip_peer_update":
            peer_pk = event.get("pubkey", "")
            if peer_pk and peer_pk != self.pubkey:
                await self._connect_peer(
                    peer_pk,
                    event.get("gossip_host", "127.0.0.1"),
                    int(event.get("gossip_port", 0))
                )
    
    def on_peer_change(self, callback):
        """Установить callback при изменении статуса пира: callback(pubkey, "alive"|"dead")."""
        self._on_peer_change = callback
    
    def on_gossip_message(self, callback):
        """Установить callback для входящих gossip-сообщений (кроме ping/pong).
        
        callback(from_pubkey: str, payload: dict) — вызывается для каждого 
        обычного сообщения от другого агента.
        """
        self._on_gossip_message = callback
    
    async def send_to(self, peer_pubkey: str, payload: dict,
                      kind: int = 39004, encrypt: bool = False,
                      require_ack: bool = True, wait_ack: bool = True,
                      timeout: float = 5.0, max_retries: int = 3) -> bool:
        """Отправить gossip-сообщение с гарантией доставки (ack + retry).
        
        Args:
            peer_pubkey: mesh_pubkey получателя
            payload: тело сообщения
            kind: kind (39004 = gossip)
            encrypt: если True — шифровать content через cipher_pubkey получателя
            require_ack: ждать ли подтверждение доставки
            timeout: таймаут ожидания ack (сек)
        """
        if not self.gossip:
            return False
        
        # ═══ Генерируем msg_id ═══
        msg_id = str(uuid.uuid4())
        
        # ═══ Шифрование content ═══
        if encrypt and "content" in payload:
            try:
                recipient_cipher = self._peer_cipher.get(peer_pubkey, "")
                if not recipient_cipher:
                    peer_name = self._peer_names.get(peer_pubkey, "")
                    if peer_name:
                        ident = mesh_crypto.load_identity(peer_name)
                        recipient_cipher = ident.get("cipher_pubkey", "")
                
                if recipient_cipher:
                    my_ident = mesh_crypto.load_identity(self.agent_name)
                    my_cipher_priv = my_ident.get("cipher_privkey", "")
                    
                    content_str = payload["content"]
                    if isinstance(content_str, dict):
                        content_str = json.dumps(content_str)
                    
                    encrypted = mesh_crypto.encrypt_for_agent(
                        content_str, recipient_cipher, my_cipher_priv
                    )
                    payload = dict(payload)
                    payload["content"] = encrypted
                    payload["encrypted"] = True
                    print(f"[{self.agent_name}] 🔒 Encrypted for {peer_pubkey[:16]}")
            except Exception as e:
                print(f"[{self.agent_name}] ⚠️ Encrypt error: {e}")
        
        # ═══ Добавляем msg_id в payload ═══
        payload = dict(payload)
        payload["msg_id"] = msg_id
        payload["from_pubkey"] = self.pubkey
        
        # ═══ Отправка ═══
        try:
            ok = await self.gossip.send_to(peer_pubkey, payload)
            if not ok:
                return False
        except Exception as e:
            print(f"[{self.agent_name}] ⚠️ Gossip send: {e}")
            return False
        
        # ═══ Регистрация в _pending_ack ═══
        if require_ack:
            event = asyncio.Event()
            self._pending_ack[msg_id] = {
                "payload": payload,
                "peer_pubkey": peer_pubkey,
                "attempt": 0,
                "ts": time.time(),
                "event": event,
                "max_retries": max_retries,
            }
            print(f"[{self.agent_name}] 📤 Sent {msg_id[:12]} → {peer_pubkey[:16]} (waiting ack)")
            
            # ═══ Ждём ack с таймаутом ═══
            if wait_ack:
                last_event = self._pending_ack.get(msg_id, {}).get("event")
                if last_event:
                    for attempt in range(max_retries):
                        try:
                            await asyncio.wait_for(
                                asyncio.shield(last_event.wait()),
                                timeout=timeout
                            )
                            print(f"[{self.agent_name}] ✅ Ack {msg_id[:12]} received")
                            self._pending_ack.pop(msg_id, None)
                            return True
                        except asyncio.TimeoutError:
                            # Retry
                            if attempt < max_retries - 1 and msg_id in self._pending_ack:
                                print(f"[{self.agent_name}] 🔄 Retry {attempt + 1}/{max_retries} for {msg_id[:12]}")
                                self._pending_ack[msg_id]["attempt"] = attempt + 1
                                self._pending_ack[msg_id]["ts"] = time.time()
                                last_event.clear()
                                try:
                                    await self.gossip.send_to(peer_pubkey, payload)
                                except:
                                    pass
                            else:
                                print(f"[{self.agent_name}] ❌ Ack timeout {msg_id[:12]} after {max_retries} retries")
                                self._pending_ack.pop(msg_id, None)
                                return False
        
        return True
    
    async def broadcast(self, payload: dict, kind: int = 39004) -> bool:
        """Разослать gossip-сообщение всем известным пирам (p2p)."""
        if not self.gossip:
            return False
        try:
            return await self.gossip.broadcast(payload, kind=kind)
        except Exception as e:
            print(f"[{self.agent_name}] ⚠️ Gossip broadcast: {e}")
            return False
    
    async def stop(self):
        """Остановить gossip-канал."""
        self._running = False
        if self._health_task:
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass
            self._health_task = None
        if self._quality_task:
            self._quality_task.cancel()
            try:
                await self._quality_task
            except asyncio.CancelledError:
                pass
            self._quality_task = None
        if self.gossip:
            await self.gossip.stop()
        print(f"[{self.agent_name}] 🔌 Gossip stopped")
    
    async def peer_count(self) -> int:
        """Количество подключённых пиров."""
        return len(self._peers) if self.gossip else 0

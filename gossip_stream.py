#!/usr/bin/env python3
"""
Gossip Stream V8 — Data channel между реле через TCP writer pool.
Паттерн: CR→RE writer pool, но двунаправленный между серверами.

Порты: 9105-9109 (fallback, если 9105 занят)
kind: 39004 (gossip_data), 39005 (gossip_ack)
Writer pool: 3 воркера на соединение (основной + batch + retry)
"""

import asyncio
# import uvloop (disabled)
# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
import json, time, os, sys, socket, random
from collections import defaultdict

from ttl_cache import TTLCache

# ─── Константы ────────────────────────────────────────────────────────────────

GOSSIP_PORT_START = 9105
GOSSIP_PORT_END   = 9109
N_WRITERS         = 3          # размер writer pool к одному пиру
MAX_RETRY         = 5          # попыток reconnect перед backoff
BACKOFF_INIT      = 1          # начальная задержка (сек)
BACKOFF_MAX       = 30         # максимальная задержка
DRAIN_TIMEOUT     = 0.5        # таймаут drain (сек)
BATCH_WINDOW      = 0.02       # 20ms batch window для writer 2
RECONNECT_DELAY   = 3          # задержка при потере соединения

# ══════════════════════════════════════════════════════════════════════════════
#  GossipMessage — формат данных
# ══════════════════════════════════════════════════════════════════════════════

def make_gossip_data(pubkey: str, target_pubkey: str, payload: dict,
                     ttl: int = 5000, nonce: str = "") -> dict:
    """kind:39004 — gossip data message."""
    if not nonce:
        nonce = f"{pubkey}:{int(time.time()*1000)}:{random.randint(0, 999999)}"
    return {
        "kind": 39004,
        "pubkey": pubkey,
        "created_at": int(time.time() * 1000),  # ms
        "content": {
            "target_pubkey": target_pubkey,
            "payload": payload,
            "ttl": ttl,
            "nonce": nonce,
        },
        "tags": [],
    }

def make_gossip_ack(pubkey: str, nonce: str, status: str = "ok") -> dict:
    """kind:39005 — gossip ack."""
    return {
        "kind": 39005,
        "pubkey": pubkey,
        "content": {"ack_for": nonce, "status": status},
    }


# ══════════════════════════════════════════════════════════════════════════════
#  WriterPool — N параллельных TCP писателей к одному пиру
# ══════════════════════════════════════════════════════════════════════════════

class WriterPool:
    """
    N писателей к одному пиру (host:port).
    
    Writer 0: основной поток данных (write + drain немедленно)
    Writer 1: batch (20ms window, накапливает + bulk drain)
    Writer 2: retry / fallback (используется если writer 0 занят или упал)
    
    Round-robin выбор writer_idx.
    Экспоненциальный backoff при reconnect.
    """

    def __init__(self, peer_id: str, host: str, port: int,
                 on_message=None, loop=None):
        """
        peer_id: уникальный ID пира (npub или relay_addr)
        host: IP пира
        port: TCP порт пира (9105-9109)
        on_message: callback(message: dict) для входящих данных
        """
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.on_message = on_message
        self.loop = loop or asyncio.get_event_loop()

        # Writer pool
        self.writers: list[asyncio.StreamWriter | None] = [None] * N_WRITERS
        self.writer_idx = 0
        self.connected = False

        # Batch drain (writer 1)
        self._batch_buf = bytearray()
        self._last_drain = time.time()

        # Backoff
        self._retry_count = 0
        self._backoff = BACKOFF_INIT
        self._reconnecting = False

        # Stats
        self.stats = {
            "sent": 0, "acks": 0, "errors": 0, "reconnects": 0,
            "bytes_sent": 0,
        }

    async def connect(self):
        """Создать N TCP соединений к пиру."""
        created = 0
        for i in range(N_WRITERS):
            try:
                r, w = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=3
                )
                sock = w.get_extra_info('socket')
                if sock:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
                self.writers[i] = w
                created += 1
            except (OSError, asyncio.TimeoutError, ConnectionRefusedError) as e:
                print(f"[GossipStream:{self.peer_id}] ❌ writer {i}: {e}")
                self.writers[i] = None

        self.connected = created > 0
        if self.connected:
            self._retry_count = 0
            self._backoff = BACKOFF_INIT
            self.stats["reconnects"] += 1
            print(f"[GossipStream:{self.peer_id}] ✅ {created}/{N_WRITERS} writers connected")
        return self.connected

    async def reconnect(self):
        """Переподключить упавших писателей с экспоненциальным backoff."""
        if self._reconnecting:
            return False
        self._reconnecting = True
        try:
            self._retry_count += 1
            delay = min(self._backoff, BACKOFF_MAX)
            print(f"[GossipStream:{self.peer_id}] 🔄 reconnect (attempt {self._retry_count}) in {delay}s...")
            await asyncio.sleep(delay)
            self._backoff = min(self._backoff * 2, BACKOFF_MAX)

            # Закрыть старые (мёртвые)
            for i in range(N_WRITERS):
                if self.writers[i] is not None:
                    try:
                        self.writers[i].close()
                    except:
                        pass
                    self.writers[i] = None

            return await self.connect()
        finally:
            self._reconnecting = False

    async def send(self, msg: dict, use_batch: bool = False) -> bool:
        """
        Отправить сообщение через writer pool (round-robin).
        use_batch=True → writer 1 (batch drain с 20ms окном).
        """
        if not self.connected:
            if not await self.reconnect():
                return False

        payload = json.dumps(msg, ensure_ascii=False).encode() + b"\n"

        if use_batch and self.writers[1] is not None:
            # Batch writer: накапливаем
            self._batch_buf.extend(payload)
            now = time.time()
            if now - self._last_drain >= BATCH_WINDOW:
                w = self.writers[1]
                try:
                    w.write(bytes(self._batch_buf))
                    await asyncio.wait_for(w.drain(), timeout=DRAIN_TIMEOUT)
                    self._batch_buf.clear()
                    self._last_drain = now
                    self.stats["sent"] += 1
                    self.stats["bytes_sent"] += len(payload)
                    return True
                except (BrokenPipeError, ConnectionResetError, OSError, asyncio.TimeoutError) as e:
                    self.stats["errors"] += 1
                    self.writers[1] = None
                    await self.reconnect()
                    return False
            else:
                # Ещё не прошёл batch window — считаем отправленным (буфер)
                self.stats["sent"] += 1
                return True

        # Round-robin: выбираем живого писателя
        for _ in range(N_WRITERS * 2):  # 2x — запас на мёртвых
            idx = self.writer_idx % N_WRITERS
            self.writer_idx += 1
            w = self.writers[idx]
            if w is not None:
                try:
                    w.write(payload)
                    await asyncio.wait_for(w.drain(), timeout=DRAIN_TIMEOUT)
                    self.stats["sent"] += 1
                    self.stats["bytes_sent"] += len(payload)
                    return True
                except (BrokenPipeError, ConnectionResetError, OSError, asyncio.TimeoutError) as e:
                    self.stats["errors"] += 1
                    self.writers[idx] = None
                    # Continue to next writer

        # Все писатели мертвы — reconnect
        self.connected = False
        await self.reconnect()
        return False

    async def close(self):
        """Закрыть все писатели."""
        for i in range(N_WRITERS):
            if self.writers[i] is not None:
                try:
                    self.writers[i].close()
                except:
                    pass
                self.writers[i] = None
        self.connected = False

    def is_alive(self) -> bool:
        """Хотя бы один writer жив. Если нет — пытаемся reconnect (sync триггер)."""
        alive = any(w is not None for w in self.writers)
        if not alive and self._retry_count < MAX_RETRY:
            # Триггерим reconnect в фоне
            asyncio.ensure_future(self.reconnect())
        return alive


# ══════════════════════════════════════════════════════════════════════════════
#  GossipStream — TCP сервер + менеджер writer pool'ов
# ══════════════════════════════════════════════════════════════════════════════

class GossipStream:
    """
    Двунаправленный data channel между реле.
    
    - TCP сервер на порту 9105-9109 (auto-fallback)
    - WriterPool к каждому известному пиру
    - kind:39004 (data) → kind:39005 (ack)
    - Dedup по nonce
    """

    def __init__(self, pubkey: str, listen_host: str = "0.0.0.0"):
        self.pubkey = pubkey
        self.listen_host = listen_host
        self.listen_port = GOSSIP_PORT_START

        # Пул писателей к удалённым пирам: peer_id → WriterPool (устаревшее)
        self.pools: dict[str, WriterPool] = {}

        # Реестр пиров для коротких TCP: peer_id → (host, port)
        self.p2p_registry: dict[str, tuple[str, int]] = {}

        # Dedup по nonce (LRU + TTL)
        self._seen_nonces = TTLCache(maxsize=5000, ttl=60)

        # Callback для входящих kind:39004 (data)
        self.on_data = None  # async def callback(from_pubkey, payload, nonce)

        # Callback для kind:39004 который поедет в CR для dedup
        self.on_event = None  # async def callback(event_dict)

        # Состояние
        self._server = None
        self._running = False
        self._cleanup_task = None

        # Stats
        self.stats = {
            "data_sent": 0, "data_recv": 0,
            "acks_sent": 0, "acks_recv": 0,
            "deduped": 0, "errors": 0,
            "peers": 0,
        }

    # ── Сервер ──

    async def start_server(self):
        """Запустить TCP сервер на 9105-9109."""
        for port in range(GOSSIP_PORT_START, GOSSIP_PORT_END + 1):
            try:
                self._server = await asyncio.start_server(
                    self._handle_connection,
                    host=self.listen_host,
                    port=port,
                )
                self.listen_port = port
                print(f"[GossipStream] 📡 Server listening on :{port}")
                break
            except OSError:
                if port == GOSSIP_PORT_END:
                    raise
                continue

        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

        async with self._server:
            await self._server.serve_forever()

    async def start_server_async(self, port: int = 0):
        """Запустить сервер не блокируя (для встраивания).
        Если port > 0 — пробуем только его. Иначе диапазон GOSSIP_PORT_START..GOSSIP_PORT_END.
        """
        ports = [port] if port > 0 else range(GOSSIP_PORT_START, GOSSIP_PORT_END + 1)
        for p in ports:
            try:
                self._server = await asyncio.start_server(
                    self._handle_connection,
                    host=self.listen_host,
                    port=p,
                )
                self.listen_port = p
                print(f"[GossipStream] 📡 Server listening on :{p}")
                break
            except OSError:
                if p == ports[-1]:
                    print(f"[GossipStream] ❌ All ports {ports} busy")
                    raise
                continue

        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        return True

    async def _handle_connection(self, reader, writer):
        """Обработать входящее TCP соединение от другого GossipStream."""
        peer = writer.get_extra_info('peername')
        peer_id = f"{peer[0]}:{peer[1]}"
        print(f"[GossipStream] ⚡ Incoming from {peer_id}")

        try:
            while self._running:
                line = await asyncio.wait_for(reader.readline(), timeout=60)
                if not line:
                    break

                msg = json.loads(line.decode().strip())
                kind = msg.get("kind", 0)

                if kind == 39004:
                    # Входящие данные
                    content = msg.get("content", {})
                    nonce = content.get("nonce", "")
                    target_pubkey = content.get("target_pubkey", "")
                    payload = content.get("payload", {})

                    # Dedup по nonce (TTLCache: add = True если новый, False если дубликат)
                    if nonce and not self._seen_nonces.add(nonce):
                        self.stats["deduped"] += 1
                        continue

                    self.stats["data_recv"] += 1
                    print(f"[GossipStream] 📩 kind:39004 from {msg.get('pubkey','?')[:12]} nonce={nonce[:16]}...")

                    # Отправляем ACK
                    ack = make_gossip_ack(self.pubkey, nonce, "ok")
                    writer.write(json.dumps(ack, ensure_ascii=False).encode() + b"\n")
                    await writer.drain()
                    self.stats["acks_sent"] += 1

                    # Callback: on_data (высокоуровневый)
                    if self.on_data:
                        asyncio.ensure_future(
                            self.on_data(msg.get("pubkey", ""), payload, nonce)
                        )

                    # Callback: on_event (для CR dedup pipeline)
                    if self.on_event:
                        asyncio.ensure_future(self.on_event(msg))

                elif kind == 39005:
                    # ACK
                    content = msg.get("content", {})
                    ack_for = content.get("ack_for", "")
                    self.stats["acks_recv"] += 1
                    if ack_for:
                        print(f"[GossipStream] ✅ ACK for {ack_for[:16]}...")

                else:
                    print(f"[GossipStream] ⚠️ unknown kind: {kind}")

        except asyncio.TimeoutError:
            pass  # idle timeout — нормально
        except (ConnectionResetError, BrokenPipeError, json.JSONDecodeError) as e:
            print(f"[GossipStream] ⚠️ Connection from {peer_id}: {e}")
        finally:
            writer.close()
            print(f"[GossipStream] 🔒 Closed {peer_id}")

    # ── Writer pool management ──

    async def add_peer(self, peer_id: str, host: str, port: int = GOSSIP_PORT_START):
        """Добавить пира в реестр коротких TCP."""
        self.p2p_registry[peer_id] = (host, port)
        print(f"[GossipStream] ✅ Peer {peer_id} ({host}:{port}) registered")
        return True

    async def remove_peer(self, peer_id: str):
        """Удалить пира из реестра."""
        self.p2p_registry.pop(peer_id, None)
        pool = self.pools.pop(peer_id, None)
        if pool:
            await pool.close()
        self.stats["peers"] = len(self.p2p_registry)
        print(f"[GossipStream] 🗑️ Removed peer {peer_id}")

    async def send_to(self, peer_id: str, payload: dict,
                      target_pubkey: str = "", ttl: int = 5000,
                      nonce: str = "") -> bool:
        """Отправить kind:39004 через короткое TCP-соединение (без WriterPool)."""
        # Берём данные пира из self.peer_info (должны быть добавлены через add_peer)
        host, port = self.p2p_registry.get(peer_id, (None, None))
        if not host or not port:
            self.stats["errors"] += 1
            return False
        
        msg = make_gossip_data(
            pubkey=self.pubkey,
            target_pubkey=target_pubkey or peer_id,
            payload=payload,
            ttl=ttl,
            nonce=nonce,
        )
        
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=3
            )
            data = json.dumps(msg, ensure_ascii=False).encode() + b"\n"
            w.write(data)
            await asyncio.wait_for(w.drain(), timeout=3)
            
            # Ждём ACK
            try:
                resp = await asyncio.wait_for(r.readline(), timeout=3)
                ok = b"ack" in resp.lower() or b"ok" in resp.lower()
            except asyncio.TimeoutError:
                ok = False
            finally:
                w.close()
            
            if ok:
                self.stats["data_sent"] += 1
            return ok
        except (OSError, asyncio.TimeoutError, ConnectionRefusedError, ConnectionResetError) as e:
            self.stats["errors"] += 1
            return False

    async def broadcast(self, payload: dict, target_pubkey: str = "",
                        ttl: int = 5000, nonce: str = "") -> dict:
        """
        Разослать всем пирам (parallel send).
        Возвращает {peer_id: True/False, ...}
        """
        if not nonce:
            nonce = f"{self.pubkey}:{int(time.time()*1000)}:{random.randint(0, 999999)}"

        msg = make_gossip_data(
            pubkey=self.pubkey,
            target_pubkey=target_pubkey or "broadcast",
            payload=payload,
            ttl=ttl,
            nonce=nonce,
        )

        tasks = []
        peer_ids = []
        for pid, pool in self.pools.items():
            if pool.is_alive():
                tasks.append(pool.send(msg))
                peer_ids.append(pid)
            else:
                tasks.append(asyncio.sleep(0, result=False))

        if not tasks:
            return {}

        results = await asyncio.gather(*tasks, return_exceptions=True)
        sent = sum(1 for r in results if r is True)
        self.stats["data_sent"] += sent
        return dict(zip(peer_ids, results))

    # ── Maintenance ──

    async def _cleanup_loop(self):
        """Раз в 60 сек проверяем мёртвые pool'ы (nonces — TTLCache сам чистит)."""
        while self._running:
            await asyncio.sleep(60)

            # Проверка мёртвых pool'ов
            for pid, pool in list(self.pools.items()):
                if not pool.is_alive():
                    print(f"[GossipStream] 🔄 Reconnecting stale peer {pid}...")
                    asyncio.ensure_future(pool.reconnect())

    async def stop(self):
        """Остановить сервер + закрыть все pool'ы."""
        self._running = False
        for pool in self.pools.values():
            await pool.close()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        print(f"[GossipStream] 🛑 Stopped")

    def get_stats(self) -> str:
        return (
            f"[GossipStream] Port:{self.listen_port} "
            f"Peers:{self.stats['peers']} "
            f"TX:{self.stats['data_sent']} RX:{self.stats['data_recv']} "
            f"ACK TX:{self.stats['acks_sent']} RX:{self.stats['acks_recv']} "
            f"Dedup:{self.stats['deduped']} Err:{self.stats['errors']}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  Entry point (standalone daemon)
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    pubkey = os.environ.get("GOSSIP_PUBKEY", "gossip_stream_v8")
    
    gs = GossipStream(pubkey=pubkey)
    
    # Пример callback для входящих данных
    async def on_data(from_pubkey, payload, nonce):
        print(f"[GossipStream] 📨 Data from {from_pubkey[:12]}: "
              f"{str(payload)[:60]}")

    gs.on_data = on_data
    await gs.start_server_async()

    print(f"\n[GossipStream] 🚀 Running on :{gs.listen_port}")
    print(f"[GossipStream] 📊 {gs.get_stats()}")
    
    # Keep alive
    try:
        while True:
            await asyncio.sleep(60)
            print(f"[GossipStream] ⏱️ {gs.get_stats()}")
    except KeyboardInterrupt:
        await gs.stop()

if __name__ == "__main__":
    print(f"\n{'='*50}")
    print(f"  GossipStream V8 — Data Channel")
    print(f"{'='*50}\n")
    asyncio.run(main())

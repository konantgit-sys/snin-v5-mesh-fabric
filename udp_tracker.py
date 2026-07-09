#!/usr/bin/env python3
"""
UDP Tracker BEP15 — быстрый discovery пиров через UDP.

Протокол BEP15:
  1. Client → connect (magic_cookie + action=0 + transaction_id)
  2. Server → connection_id (8 байт)
  3. Client → announce с info_hash
  4. Server → список живых пиров (6 байт на пира)

Порт: :9020
Интеграция: Smart Router получает пиров из трекера (Tier 2)

Запуск: python3 udp_tracker.py
"""

import asyncio
import json
import os
import socket
import struct
import sys
import time
import random

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import blinded_sigs as sigs

# ─── Конфиг ───
TRACKER_PORT = 9020
ANNOUNCE_INTERVAL = 1800     # сек между announce (30 мин)
PEER_TIMEOUT = 3600          # сек без announce → удаление
CLEANUP_INTERVAL = 60        # сек между чисткой
MAX_PEERS_PER_ANNOUNCE = 50  # макс пиров в ответе

# BEP15 константы
MAGIC_COOKIE = 0x41727101980
ACTION_CONNECT = 0
ACTION_ANNOUNCE = 1
ACTION_ERROR = 3
EVENT_NONE = 0
EVENT_COMPLETED = 1
EVENT_STARTED = 2
EVENT_STOPPED = 3

# SNIN
MESH_ID = "snin-main-1"
PEER_ID_PREFIX = b"SN5"

# Redis DHT
REDIS_DHT_KEY = "dht:agents"
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379


class UDPTracker:
    """UDP Tracker BEP15 сервер."""

    def __init__(self, port: int = TRACKER_PORT):
        self._port = port
        self._running = False
        self._sock: socket.socket | None = None
        self._connections: dict[bytes, float] = {}  # connection_id → expires_at
        self._peers: dict[str, dict] = {}  # info_hash → {ip, port, last_seen}
        self._stats = {
            "connects": 0,
            "announces": 0,
            "peers_active": 0,
            "errors": 0,
        }
        self._pubkey = sigs.get_verifying_key_hex()
        self._connection_ttl = 120  # сек

    # ─── Сокет ───

    def _create_socket(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", self._port))
        sock.settimeout(1.0)
        return sock

    # ─── BEP15: Connect ───

    def _handle_connect(self, data: bytes, addr: tuple) -> bytes:
        """Обработать connect запрос. Вернуть response."""
        if len(data) < 16:
            self._stats["errors"] += 1
            return b""

        magic, action, transaction_id = struct.unpack_from("!QII", data, 0)

        if magic != MAGIC_COOKIE or action != ACTION_CONNECT:
            self._stats["errors"] += 1
            return b""

        # Генерируем connection_id
        connection_id = random.randint(1, 0xFFFFFFFFFFFFFFFF)
        self._connections[connection_id.to_bytes(8, "big")] = time.time() + self._connection_ttl

        self._stats["connects"] += 1

        # Response: action + transaction_id + connection_id
        resp = struct.pack("!II", ACTION_CONNECT, transaction_id)
        resp += struct.pack("!Q", connection_id)
        return resp

    # ─── BEP15: Announce ───

    def _handle_announce(self, data: bytes, addr: tuple) -> bytes:
        """Обработать announce запрос. Вернуть список пиров."""
        if len(data) < 98:
            self._stats["errors"] += 1
            return b""

        connection_id = struct.unpack_from("!Q", data, 0)[0]
        action = struct.unpack_from("!I", data, 8)[0]
        transaction_id = struct.unpack_from("!I", data, 12)[0]
        info_hash = data[16:36]
        peer_id = data[36:56]
        downloaded = struct.unpack_from("!Q", data, 56)[0]
        left = struct.unpack_from("!Q", data, 64)[0]
        uploaded = struct.unpack_from("!Q", data, 72)[0]
        event = struct.unpack_from("!I", data, 80)[0]
        ip = struct.unpack_from("!I", data, 84)[0]
        key = struct.unpack_from("!I", data, 88)[0]
        num_want = struct.unpack_from("!I", data, 92)[0]
        port = struct.unpack_from("!H", data, 96)[0]

        # Проверка connection_id
        conn_id_bytes = connection_id.to_bytes(8, "big")
        if conn_id_bytes not in self._connections:
            self._stats["errors"] += 1
            # Возвращаем error
            return struct.pack("!II", ACTION_ERROR, transaction_id) + b"invalid connection_id\0"

        if action != ACTION_ANNOUNCE:
            self._stats["errors"] += 1
            return b""

        # Обновить/добавить пира
        info_hash_hex = info_hash.hex()
        peer_ip = addr[0]

        if event == EVENT_STOPPED:
            # Пир отключается
            self._peers.pop(info_hash_hex, None)
            self._stats["peers_active"] = len(self._peers)
            # Ответ без пиров
            resp = struct.pack("!II", ACTION_ANNOUNCE, transaction_id)
            resp += struct.pack("!I", ANNOUNCE_INTERVAL)  # interval
            resp += struct.pack("!I", 0)  # leechers
            resp += struct.pack("!I", 0)  # seeders
            return resp

        self._peers[info_hash_hex] = {
            "info_hash": info_hash_hex,
            "ip": peer_ip,
            "port": port or self._port,
            "peer_id": peer_id.decode("latin-1", errors="replace"),
            "event": event,
            "last_seen": time.time(),
        }
        self._stats["announces"] += 1
        self._stats["peers_active"] = len(self._peers)

        # Сохранить в Redis DHT
        asyncio.ensure_future(self._save_to_dht(info_hash_hex, self._peers[info_hash_hex]))

        # Собрать список пиров (кроме себя)
        peers_list = []
        for pkh, peer in self._peers.items():
            if pkh == info_hash_hex:
                continue  # себя не включаем
            # IP в 4 байта
            try:
                ip_bytes = socket.inet_aton(peer["ip"])
            except OSError:
                continue
            port_bytes = struct.pack("!H", peer.get("port", self._port))
            peers_list.append(ip_bytes + port_bytes)

        # Ограничение
        if num_want > 0 and num_want < len(peers_list):
            peers_list = random.sample(peers_list, min(num_want, MAX_PEERS_PER_ANNOUNCE))
        else:
            peers_list = peers_list[:MAX_PEERS_PER_ANNOUNCE]

        # Response
        resp = struct.pack("!II", ACTION_ANNOUNCE, transaction_id)
        resp += struct.pack("!I", ANNOUNCE_INTERVAL)  # interval
        resp += struct.pack("!I", 0)  # leechers
        resp += struct.pack("!I", len(peers_list))  # seeders = peers_count
        for p in peers_list:
            resp += p

        return resp

    # ─── Обработка пакетов ───

    def _process_packet(self, data: bytes, addr: tuple) -> bytes | None:
        """Определить тип запроса и обработать."""
        if len(data) < 16:
            return None

        try:
            magic_check = struct.unpack_from("!Q", data, 0)[0]
        except struct.error:
            return None

        if len(data) >= 16 and magic_check == MAGIC_COOKIE:
            return self._handle_connect(data, addr)
        elif len(data) >= 98:
            return self._handle_announce(data, addr)
        return None

    # ─── Приём ───

    async def _receive_loop(self):
        """Цикл приёма UDP пакетов."""
        loop = asyncio.get_running_loop()

        while self._running:
            try:
                data, addr = await loop.run_in_executor(None, self._sock.recvfrom, 4096)
                response = self._process_packet(data, addr)
                if response:
                    self._sock.sendto(response, addr)
            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    print(f"[TRK] ⚠️ recv error: {e}")

    # ─── Cleanup ───

    async def _cleanup_loop(self):
        """Чистка старых connection_id и мёртвых пиров."""
        while self._running:
            await asyncio.sleep(30)
            now = time.time()

            # Старые connection_id
            dead_conns = [k for k, v in self._connections.items()
                         if v < now]
            for k in dead_conns:
                self._connections.pop(k, None)

            # Мёртвые пиры
            dead_peers = [k for k, v in self._peers.items()
                         if now - v["last_seen"] > PEER_TIMEOUT]
            for k in dead_peers:
                await self._remove_from_dht(k)
                self._peers.pop(k, None)

            self._stats["peers_active"] = len(self._peers)

    # ─── DHT ───

    async def _save_to_dht(self, info_hash: str, peer: dict):
        """Сохранить пира в Redis DHT."""
        try:
            import aioredis
            r = await aioredis.from_url(
                f"redis://{REDIS_HOST}:{REDIS_PORT}",
                decode_responses=True
            )
            dht_entry = {
                "pubkey": info_hash[:16],
                "ip": peer["ip"],
                "tcp_port": peer.get("port", self._port),
                "source": "udp_tracker",
                "last_seen": time.time(),
            }
            await r.hset(REDIS_DHT_KEY, info_hash[:16], json.dumps(dht_entry))
            await r.expire(f"{REDIS_DHT_KEY}:ttl:{info_hash[:16]}", PEER_TIMEOUT)
            await r.aclose()
        except Exception:
            pass

    async def _remove_from_dht(self, info_hash: str):
        """Удалить пира из DHT."""
        try:
            import aioredis
            r = await aioredis.from_url(
                f"redis://{REDIS_HOST}:{REDIS_PORT}",
                decode_responses=True
            )
            await r.hdel(REDIS_DHT_KEY, info_hash[:16])
            await r.aclose()
        except Exception:
            pass

    # ─── API ───

    def get_peers(self) -> list[dict]:
        return list(self._peers.values())

    def get_stats(self) -> dict:
        return dict(self._stats)

    # ─── Lifecycle ───

    async def start(self):
        if self._running:
            return
        self._running = True
        self._sock = self._create_socket()

        print(f"[TRK] 🟢 UDP Tracker BEP15 on :{self._port}")
        asyncio.ensure_future(self._receive_loop())
        asyncio.ensure_future(self._cleanup_loop())

    async def stop(self):
        self._running = False
        if self._sock:
            self._sock.close()
            self._sock = None


# ─── BEP15 Client ───

class UDPTrackerClient:
    """UDP Tracker BEP15 клиент."""

    def __init__(self, tracker_host: str = "127.0.0.1", tracker_port: int = TRACKER_PORT):
        self._host = tracker_host
        self._port = tracker_port
        self._sock: socket.socket | None = None

    def _create_socket(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(5.0)
        return sock

    def connect(self) -> bytes:
        """BEP15 connect: получить connection_id."""
        sock = self._create_socket()
        transaction_id = random.randint(1, 0xFFFFFFFF)

        packet = struct.pack("!QII", MAGIC_COOKIE, ACTION_CONNECT, transaction_id)
        sock.sendto(packet, (self._host, self._port))

        resp, addr = sock.recvfrom(16)
        action, tid = struct.unpack_from("!II", resp, 0)
        connection_id = struct.unpack_from("!Q", resp, 8)[0]

        if action != ACTION_CONNECT or tid != transaction_id:
            sock.close()
            raise ValueError("Invalid connect response")

        self._sock = sock
        return connection_id.to_bytes(8, "big")

    def announce(self, connection_id: bytes, info_hash: bytes,
                 peer_id: bytes = b"SN500000000000001",
                 port: int = 9908) -> list[dict]:
        """BEP15 announce: получить список пиров."""
        if self._sock is None:
            raise RuntimeError("Not connected")

        transaction_id = random.randint(1, 0xFFFFFFFF)

        packet = connection_id
        packet += struct.pack("!I", ACTION_ANNOUNCE)
        packet += struct.pack("!I", transaction_id)
        packet += info_hash
        packet += peer_id.ljust(20, b"\0")[:20]
        packet += struct.pack("!Q", 0)  # downloaded
        packet += struct.pack("!Q", 0)  # left
        packet += struct.pack("!Q", 0)  # uploaded
        packet += struct.pack("!I", EVENT_STARTED)  # event
        packet += struct.pack("!I", 0)  # ip
        packet += struct.pack("!I", 0)  # key
        packet += struct.pack("!i", -1)  # num_want (-1 = все)
        packet += struct.pack("!H", port)

        self._sock.sendto(packet, (self._host, self._port))

        # Читаем response
        resp, addr = self._sock.recvfrom(4096)
        action = struct.unpack_from("!I", resp, 0)[0]

        if action == ACTION_ERROR:
            error_msg = resp[8:].decode("utf-8", errors="replace").split("\0")[0]
            self._sock.close()
            self._sock = None
            raise ValueError(f"Tracker error: {error_msg}")

        if action != ACTION_ANNOUNCE:
            self._sock.close()
            self._sock = None
            raise ValueError(f"Unexpected action: {action}")

        tid = struct.unpack_from("!I", resp, 4)[0]
        interval = struct.unpack_from("!I", resp, 8)[0]
        leechers = struct.unpack_from("!I", resp, 12)[0]
        seeders = struct.unpack_from("!I", resp, 16)[0]

        peers = []
        offset = 20
        for i in range(seeders):
            if offset + 6 > len(resp):
                break
            ip_bytes = resp[offset:offset+4]
            port_bytes = resp[offset+4:offset+6]
            ip = f"{ip_bytes[0]}.{ip_bytes[1]}.{ip_bytes[2]}.{ip_bytes[3]}"
            port = struct.unpack("!H", port_bytes)[0]
            peers.append({"ip": ip, "port": port})
            offset += 6

        self._sock.close()
        self._sock = None

        return peers

    def close(self):
        if self._sock:
            self._sock.close()
            self._sock = None


# ─── Main ───
async def main():
    import argparse
    parser = argparse.ArgumentParser(description="SNIN UDP Tracker BEP15")
    parser.add_argument("--port", type=int, default=TRACKER_PORT,
                       help="UDP port (default: 9020)")
    args = parser.parse_args()

    sigs.init_signing()
    tracker = UDPTracker(port=args.port)
    await tracker.start()

    print("[TRK] Running. Press Ctrl+C to stop.")
    try:
        while True:
            await asyncio.sleep(60)
            stats = tracker.get_stats()
            peers = tracker.get_peers()
            print(f"[TRK] 📊 conn:{stats['connects']} ann:{stats['announces']} "
                  f"peers:{len(peers)}")
    except (KeyboardInterrupt, asyncio.CancelledError):
        await tracker.stop()
        print("[TRK] 🔴 Stopped")


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
LAN Discovery v1 — UDP multicast для обнаружения агентов в локальной сети.

Beacon: подписанный публичным ключом, каждые 30 сек на 239.255.77.77:7777.
Passive listen: приём чужих beacon, верификация через L5 Identity.
Найденные пиры регистрируются в DHT Smart Router (Redis).
TTL 120 сек без beacon → пир удаляется.

Запуск: python3 lan_discovery.py --port 9901
"""

import asyncio
import json
import os
import socket
import struct
import sys
import time
from collections import OrderedDict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import blinded_sigs as sigs

# ─── Конфиг ───
MULTICAST_GROUP = "239.255.77.77"
MULTICAST_PORT = 7777
BEACON_INTERVAL = 30        # сек между beacon
PEER_TTL = 120              # сек без beacon → удаление
CLEANUP_INTERVAL = 15       # сек между чисткой
BEACON_TTL = 86400          # 1 день — время жизни подписи beacon

# Redis DHT
REDIS_DHT_KEY = "dht:agents"
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379

# Mesh
MESH_ID = "snin-main-1"
VERSION = "5.0.0.dev1"

# ─── In-memory хранилище пиров ───
# peer_key (pubkey:ip) → {pubkey, ip, port, nat_type, last_seen, agent_name}
_peers: dict[str, dict] = {}
_relay_signing_url = "http://127.0.0.1:9125"


class LANDicovery:
    """UDP multicast discovery сервис."""

    def __init__(self, listen_port: int = 9901, mesh_id: str = MESH_ID):
        self._listen_port = listen_port
        self._mesh_id = mesh_id
        self._running = False
        self._sock: socket.socket | None = None
        self._pubkey = sigs.get_verifying_key_hex()
        self._peers = {}
        self._stats = {
            "beacons_sent": 0,
            "beacons_received": 0,
            "beacons_verified": 0,
            "beacons_rejected": 0,
            "peers_active": 0,
        }

    # ─── Сокет ───

    def _create_socket(self) -> socket.socket:
        """Создать UDP сокет с multicast membership."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Разрешить loopback (важно для локальных тестов)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

        # TTL multicast
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

        # Bind на listen_port
        sock.bind(("0.0.0.0", self._listen_port))

        # Подписаться на multicast группу
        mreq = struct.pack(
            "4sl",
            socket.inet_aton(MULTICAST_GROUP),
            socket.INADDR_ANY
        )
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        # Таймаут (для неблокирующего receive)
        sock.settimeout(1.0)

        return sock

    # ─── Beacon ───

    def _create_beacon(self) -> bytes:
        """Создать подписанный beacon."""
        pubkey = self._pubkey
        ip = self._get_local_ip()
        nat_type = "easy"  # default для локальной сети
        timestamp = int(time.time())

        # Подпись через blinded_sigs
        # book_id = relay_url (lan://ip:port) для совместимости с relay_signing verify
        relay_url = f"lan://{ip}:{self._listen_port}"
        signature = sigs.sign_cheque(
            book_id=relay_url,
            index=timestamp,
            amount=0,
            recipient=f"{pubkey}:{self._mesh_id}"
        )

        beacon = {
            "pubkey": pubkey,
            "ip": ip,
            "port": self._listen_port,
            "nat_type": nat_type,
            "version": VERSION,
            "mesh_id": self._mesh_id,
            "timestamp": timestamp,
            "signature": signature,
        }

        return json.dumps(beacon).encode()

    def _get_local_ip(self) -> str:
        """Узнать локальный IP через UDP socket (не отправляет данные)."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(0.1)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    # ─── Отправка beacon ───

    async def _send_beacon_loop(self):
        """Периодическая отправка beacon."""
        while self._running:
            try:
                beacon_bytes = self._create_beacon()
                self._sock.sendto(beacon_bytes, (MULTICAST_GROUP, MULTICAST_PORT))
                self._stats["beacons_sent"] += 1
                await asyncio.sleep(BEACON_INTERVAL)
            except Exception as e:
                print(f"[LAN] ⚠️ send error: {e}")
                await asyncio.sleep(5)

    # ─── Приём beacon ───

    async def _receive_loop(self):
        """Цикл приёма beacon в отдельном потоке."""
        loop = asyncio.get_running_loop()

        while self._running:
            try:
                data, addr = await loop.run_in_executor(None, self._sock.recvfrom, 4096)
                await self._process_beacon(data, addr)
            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    print(f"[LAN] ⚠️ recv error: {e}")

    async def _process_beacon(self, data: bytes, addr: tuple):
        """Обработать входящий beacon."""
        try:
            beacon = json.loads(data.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._stats["beacons_rejected"] += 1
            return

        # Проверка полей
        required = ("pubkey", "ip", "port", "timestamp", "signature", "mesh_id")
        if not all(k in beacon for k in required):
            self._stats["beacons_rejected"] += 1
            return

        # Пропустить свой beacon
        if beacon.get("pubkey") == self._pubkey:
            self._stats["beacons_received"] += 1
            return

        # Проверка mesh_id
        if beacon.get("mesh_id") != self._mesh_id:
            self._stats["beacons_rejected"] += 1
            return

        # Проверка TTL подписи
        if time.time() - beacon["timestamp"] > BEACON_TTL:
            self._stats["beacons_rejected"] += 1
            return

        self._stats["beacons_received"] += 1

        # Верификация через локальный relay_signing
        verified = await self._verify_beacon(beacon)

        if verified:
            self._stats["beacons_verified"] += 1
            peer_key = f"{beacon['pubkey']}:{beacon['ip']}"
            self._peers[peer_key] = {
                "pubkey": beacon["pubkey"],
                "agent_name": beacon.get("agent_name", f"lan_{beacon['pubkey'][:8]}"),
                "ip": beacon["ip"],
                "port": beacon.get("port", self._listen_port),
                "nat_type": beacon.get("nat_type", "unknown"),
                "version": beacon.get("version", VERSION),
                "last_seen": time.time(),
                "source": "lan_discovery",
                "tcp_port": beacon.get("port", 9908),
            }
            self._stats["peers_active"] = len(self._peers)

            # Сохранить в Redis DHT
            await self._save_to_dht(self._peers[peer_key])
        else:
            self._stats["beacons_rejected"] += 1

    async def _verify_beacon(self, beacon: dict) -> bool:
        """Верифицировать подпись beacon через relay_signing API."""
        try:
            import urllib.request
            import urllib.parse

            # Собираем сообщение для верификации (то же что в _create_beacon)
            params = urllib.parse.urlencode({
                "relay_url": f"lan://{beacon['ip']}:{beacon['port']}",
                "signature": beacon["signature"],
                "timestamp": beacon["timestamp"],
                "pubkey": beacon["pubkey"],
                "mesh_id": beacon["mesh_id"],
            })
            url = f"{_relay_signing_url}/verify?{params}"

            resp = json.loads(urllib.request.urlopen(url, timeout=3).read())
            return resp.get("verified", False)
        except Exception:
            return False

    # ─── DHT интеграция ───

    async def _save_to_dht(self, peer: dict):
        """Сохранить найденного пира в Redis DHT."""
        try:
            import aioredis
            r = await aioredis.from_url(
                f"redis://{REDIS_HOST}:{REDIS_PORT}",
                decode_responses=True
            )

            dht_entry = {
                "pubkey": peer["pubkey"],
                "ip": peer["ip"],
                "tcp_port": peer.get("tcp_port", peer["port"]),
                "nat_type": peer["nat_type"],
                "source": "lan_discovery",
                "version": peer["version"],
                "last_seen": time.time(),
                "agent_name": peer["agent_name"],
            }

            # Сохраняем в dht:agents hash
            await r.hset(REDIS_DHT_KEY, peer["pubkey"], json.dumps(dht_entry))

            # TTL на ключ (автоочистка Redis если сервис упал)
            await r.expire(f"{REDIS_DHT_KEY}:ttl:{peer['pubkey']}", PEER_TTL)

            await r.aclose()
        except ImportError:
            print("[LAN] ⚠️ aioredis not installed, skipping DHT save")
        except Exception as e:
            print(f"[LAN] ⚠️ DHT save error: {e}")

    async def _cleanup_dead_peers(self):
        """Удалить пиров с истёкшим TTL."""
        now = time.time()
        dead = [k for k, v in self._peers.items()
                if now - v["last_seen"] > PEER_TTL]

        for key in dead:
            peer = self._peers.pop(key, None)
            if peer:
                # Удалить из DHT
                try:
                    import aioredis
                    r = await aioredis.from_url(
                        f"redis://{REDIS_HOST}:{REDIS_PORT}",
                        decode_responses=True
                    )
                    await r.hdel(REDIS_DHT_KEY, peer["pubkey"])
                    await r.aclose()
                except Exception:
                    pass
                print(f"[LAN] 🟤 Peer dead (TTL): {peer.get('agent_name', peer['pubkey'][:16])}")

        self._stats["peers_active"] = len(self._peers)

    async def _cleanup_loop(self):
        """Периодическая чистка мёртвых пиров."""
        while self._running:
            await asyncio.sleep(CLEANUP_INTERVAL)
            await self._cleanup_dead_peers()

    # ─── API ───

    def get_peers(self) -> dict:
        """Вернуть список живых пиров."""
        return dict(self._peers)

    def get_stats(self) -> dict:
        """Вернуть статистику."""
        return dict(self._stats)

    # ─── Lifecycle ───

    async def start(self):
        """Запустить сервис."""
        if self._running:
            return

        self._running = True
        self._sock = self._create_socket()

        print(f"[LAN] 🟢 Discovery on {MULTICAST_GROUP}:{MULTICAST_PORT} (listen :{self._listen_port})")
        print(f"[LAN] 🔑 Pubkey: {self._pubkey[:16]}...")
        print(f"[LAN] 📡 Beacon every {BEACON_INTERVAL}s, TTL {PEER_TTL}s")

        asyncio.ensure_future(self._send_beacon_loop())
        asyncio.ensure_future(self._receive_loop())
        asyncio.ensure_future(self._cleanup_loop())

    async def stop(self):
        """Остановить сервис."""
        self._running = False
        if self._sock:
            self._sock.close()
            self._sock = None


# ─── Main ───
async def main():
    import argparse

    parser = argparse.ArgumentParser(description="SNIN LAN Discovery")
    parser.add_argument("--port", type=int, default=9901, help="Listen port")
    parser.add_argument("--mesh-id", default=MESH_ID, help="Mesh ID")
    parser.add_argument("--ttl", type=int, default=PEER_TTL, help="Peer TTL (s)")

    args = parser.parse_args()

    # Инициализация ключей
    sigs.init_signing()

    discovery = LANDicovery(
        listen_port=args.port,
        mesh_id=args.mesh_id,
    )

    await discovery.start()
    print("[LAN] Running. Press Ctrl+C to stop.")

    try:
        while True:
            await asyncio.sleep(60)
            stats = discovery.get_stats()
            peers = discovery.get_peers()
            print(f"[LAN] 📊 beacon: {stats['beacons_sent']}s/{stats['beacons_received']}r "
                  f"({stats['beacons_verified']}v/{stats['beacons_rejected']}! )"
                  f" peers: {len(peers)}")
    except (KeyboardInterrupt, asyncio.CancelledError):
        await discovery.stop()
        print("[LAN] 🔴 Stopped")


if __name__ == "__main__":
    asyncio.run(main())

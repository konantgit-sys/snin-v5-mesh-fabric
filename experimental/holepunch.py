#!/usr/bin/env python3
"""
NAT Hole Punch v1 — P2P прямое соединение между агентами за NAT.

Режимы:
  1. Prediction (быстрый) — отправка UDP на известный адрес
  2. Signal exchange — через Nostr kind:39010
  3. TCP relay fallback — при symmetric NAT

Порты:
  :9120 — UDP hole punch + signal API
  :9121 — TCP relay fallback

Запуск: python3 holepunch.py
"""

import asyncio
import json
import os
import socket
import struct
import sys
import time
import random
from collections import OrderedDict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import blinded_sigs as sigs

# ─── Конфиг ───
UDP_PORT = 9120
TCP_RELAY_PORT = 9121
BEACON_INTERVAL = 15        # UDP punch interval (сек)
PEER_TTL = 120              # сек без пинга → удаление
PUNCH_RETRIES = 5           # попыток hole punch
PUNCH_TIMEOUT = 3           # сек ожидания ответа
SYMMETRIC_FALLBACK = True   # TCP relay при symmetric NAT

# Signal exchange через Nostr
NOSTR_KIND_SIGNAL = 39010   # kind для обмена адресами
MESH_ID = "snin-main-1"

# Redis DHT
REDIS_DHT_KEY = "dht:agents"
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379


class HolePunch:
    """NAT Hole Punch сервис."""

    def __init__(self, udp_port: int = UDP_PORT, tcp_port: int = TCP_RELAY_PORT):
        self._udp_port = udp_port
        self._tcp_port = tcp_port
        self._running = False
        self._udp_sock: socket.socket | None = None
        self._tcp_server: asyncio.AbstractServer | None = None
        self._pubkey = sigs.get_verifying_key_hex()
        self._peers: dict[str, dict] = {}  # pubkey → {ip, port, nat_type, last_seen, mode}
        self._stats = {
            "punches_sent": 0,
            "punches_received": 0,
            "punches_ok": 0,
            "relay_used": 0,
            "peers_active": 0,
            "nat_type": "unknown",
        }
        self._local_ip = self._get_local_ip()

    # ─── Сокеты ───

    def _create_udp_socket(self) -> socket.socket:
        """UDP сокет для hole punch."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", self._udp_port))
        sock.settimeout(1.0)
        return sock

    async def _start_tcp_relay(self):
        """TCP relay для symmetric NAT fallback."""
        self._tcp_server = await asyncio.start_server(
            self._handle_tcp_relay,
            "0.0.0.0", self._tcp_port
        )
        print(f"[HP] 🔌 TCP Relay on :{self._tcp_port}")

    async def _handle_tcp_relay(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Прокси TCP трафика между двумя пирами."""
        peer_addr = writer.get_extra_info("peername")
        try:
            # Читаем заголовок: {target_pubkey, command}
            header = await asyncio.wait_for(reader.readline(), timeout=5)
            data = json.loads(header.decode().strip())

            if data.get("command") == "relay_connect":
                target_pubkey = data.get("target_pubkey", "")
                if target_pubkey in self._peers:
                    target = self._peers[target_pubkey]
                    target_ip = target["ip"]
                    target_port = target.get("tcp_port", target.get("port", TCP_RELAY_PORT))

                    # Прокси: читаем от A, пишем в B
                    r2, w2 = await asyncio.wait_for(
                        asyncio.open_connection(target_ip, target_port), timeout=5
                    )
                    w2.write(json.dumps({"command": "relay_accept", "from": data.get("pubkey", "")}).encode() + b"\n")
                    await w2.drain()

                    self._stats["relay_used"] += 1

                    # Двунаправленный прокси
                    async def forward(r, w):
                        try:
                            while True:
                                chunk = await r.read(4096)
                                if not chunk:
                                    break
                                w.write(chunk)
                                await w.drain()
                        except Exception:
                            pass

                    await asyncio.gather(
                        forward(reader, w2),
                        forward(r2, writer),
                    )
                    w2.close()
                else:
                    writer.write(json.dumps({"error": "target not found"}).encode() + b"\n")
            elif data.get("command") == "relay_accept":
                pass  # ждём данные от relay
        except Exception as e:
            print(f"[HP] ⚠️ TCP relay error: {e}")
        finally:
            writer.close()

    def _get_local_ip(self) -> str:
        """Узнать локальный IP."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(0.1)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    # ─── NAT Detection ───

    def detect_nat_type(self) -> str:
        """Определить тип NAT (упрощённо). Порт UDP известен → easy, иначе symmetric."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("0.0.0.0", 0))
            s.settimeout(2)

            # STUN-like запрос к внешнему серверу (если есть)
            # Для локальной сети — всегда easy
            s.sendto(b"ping", ("127.0.0.1", self._udp_port))
            s.close()
            return "easy"
        except Exception:
            return "symmetric"

    # ─── Hole Punch ───

    def _create_punch_packet(self, target_pubkey: str, mesh_id: str = MESH_ID) -> bytes:
        """Создать UDP hole punch пакет."""
        packet = {
            "type": "holepunch",
            "pubkey": self._pubkey,
            "target": target_pubkey,
            "ip": self._local_ip,
            "port": self._udp_port,
            "mesh_id": mesh_id,
            "timestamp": int(time.time()),
            "signature": "",
        }
        # Подпись
        sig_msg = f"{packet['pubkey']}:{packet['target']}:{packet['ip']}:{packet['port']}:{packet['timestamp']}"
        packet["signature"] = sigs.sign_cheque(
            book_id=f"punch:{packet['pubkey']}",
            index=packet["timestamp"],
            amount=0,
            recipient=f"{packet['target']}:{packet['ip']}:{packet['port']}"
        )
        return json.dumps(packet).encode()

    def _verify_punch(self, packet: dict) -> bool:
        """Верифицировать подпись punch пакета."""
        try:
            sig_msg = f"{packet['pubkey']}:{packet['target']}:{packet['ip']}:{packet['port']}:{packet['timestamp']}"
            vk = packet["pubkey"]
            return sigs.verify_cheque_sig(
                verifying_key_hex=vk,
                book_id=f"punch:{packet['pubkey']}",
                index=packet["timestamp"],
                amount=0,
                recipient=f"{packet['target']}:{packet['ip']}:{packet['port']}",
                sig_hex=packet["signature"]
            )
        except Exception:
            return False

    # ─── Отправка punch ───

    async def _send_punch_loop(self):
        """Периодическая отправка hole punch пакетов активным пирам."""
        while self._running:
            try:
                now = time.time()
                for pubkey, peer in list(self._peers.items()):
                    if now - peer["last_seen"] > PEER_TTL:
                        continue
                    if peer.get("mode") == "relay":
                        continue  # relay не требует punch

                    packet = self._create_punch_packet(pubkey)
                    target = (peer["ip"], peer.get("port", self._udp_port))
                    self._udp_sock.sendto(packet, target)
                    self._stats["punches_sent"] += 1

                await asyncio.sleep(BEACON_INTERVAL)
            except Exception as e:
                print(f"[HP] ⚠️ send punch error: {e}")
                await asyncio.sleep(5)

    # ─── Приём punch ───

    async def _receive_loop(self):
        """Цикл приёма UDP hole punch пакетов."""
        loop = asyncio.get_running_loop()
        while self._running:
            try:
                data, addr = await loop.run_in_executor(None, self._udp_sock.recvfrom, 4096)
                await self._process_punch(data, addr)
            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    print(f"[HP] ⚠️ recv error: {e}")

    async def _process_punch(self, data: bytes, addr: tuple):
        """Обработать входящий hole punch пакет."""
        try:
            packet = json.loads(data.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            return

        if packet.get("type") != "holepunch":
            return
        if packet.get("target") != self._pubkey:
            return  # не нам
        if packet.get("pubkey") == self._pubkey:
            return  # свой пакет

        self._stats["punches_received"] += 1

        # Верификация подписи
        if not self._verify_punch(packet):
            return

        # Сохраняем пира
        pubkey = packet["pubkey"]
        self._stats["punches_ok"] += 1

        peer = {
            "pubkey": pubkey,
            "ip": packet.get("ip", addr[0]),
            "port": packet.get("port", self._udp_port),
            "nat_type": packet.get("nat_type", "easy"),
            "mode": "direct",
            "last_seen": time.time(),
            "source": "holepunch",
        }
        self._peers[pubkey] = peer
        self._stats["peers_active"] = len(self._peers)

        # Обновить DHT
        asyncio.ensure_future(self._save_to_dht(pubkey, peer))

        # Отправить ответ (punch back)
        response = self._create_punch_packet(pubkey)
        self._udp_sock.sendto(response, (peer["ip"], peer["port"]))
        self._stats["punches_sent"] += 1

    # ─── Signal exchange через Nostr ───

    async def publish_signal(self, nostr_bridge_port: int = 9941):
        """
        Опубликовать kind:39010 сигнал через Nostr bridge.
        Сигнал: {pubkey, ip, port, nat_type, mesh_id}
        """
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", nostr_bridge_port), timeout=2
            )

            nat_type = self.detect_nat_type()
            signal = {
                "kind": NOSTR_KIND_SIGNAL,
                "pubkey": self._pubkey,
                "content": json.dumps({
                    "pubkey": self._pubkey,
                    "ip": self._local_ip,
                    "port": self._udp_port,
                    "tcp_port": self._tcp_port,
                    "nat_type": nat_type,
                    "mesh_id": MESH_ID,
                    "version": "5.0.0.dev1",
                }),
                "created_at": int(time.time()),
            }
            w.write(json.dumps(signal).encode() + b"\n")
            await w.drain()
            w.close()
            self._stats["nat_type"] = nat_type
        except Exception as e:
            print(f"[HP] ⚠️ signal publish error: {e}")

    # ─── DHT интеграция ───

    async def _save_to_dht(self, pubkey: str, peer: dict):
        """Сохранить пира в Redis DHT."""
        try:
            import aioredis
            r = await aioredis.from_url(
                f"redis://{REDIS_HOST}:{REDIS_PORT}",
                decode_responses=True
            )

            dht_entry = {
                "pubkey": pubkey,
                "ip": peer["ip"],
                "tcp_port": peer.get("port", self._udp_port),
                "udp_port": self._udp_port,
                "nat_type": peer["nat_type"],
                "mode": peer.get("mode", "direct"),
                "source": "holepunch",
                "last_seen": time.time(),
            }

            await r.hset(REDIS_DHT_KEY, pubkey, json.dumps(dht_entry))
            await r.expire(f"{REDIS_DHT_KEY}:ttl:{pubkey}", PEER_TTL)
            await r.aclose()
        except ImportError:
            pass
        except Exception as e:
            print(f"[HP] ⚠️ DHT save: {e}")

    # ─── Cleanup ───

    async def _cleanup_loop(self):
        """Чистка мёртвых пиров."""
        while self._running:
            await asyncio.sleep(15)
            now = time.time()
            dead = [k for k, v in self._peers.items()
                    if now - v["last_seen"] > PEER_TTL]
            for key in dead:
                self._peers.pop(key, None)
            self._stats["peers_active"] = len(self._peers)

    # ─── API ───

    def get_peers(self) -> dict:
        return dict(self._peers)

    def get_stats(self) -> dict:
        return dict(self._stats)

    # ─── Lifecycle ───

    async def start(self):
        if self._running:
            return
        self._running = True
        self._udp_sock = self._create_udp_socket()
        await self._start_tcp_relay()

        print(f"[HP] 🟢 Hole Punch UDP :{self._udp_port}, TCP Relay :{self._tcp_port}")
        print(f"[HP] 🔑 Pubkey: {self._pubkey[:16]}...")

        asyncio.ensure_future(self._send_punch_loop())
        asyncio.ensure_future(self._receive_loop())
        asyncio.ensure_future(self._cleanup_loop())

    async def stop(self):
        self._running = False
        if self._udp_sock:
            self._udp_sock.close()
            self._udp_sock = None
        if self._tcp_server:
            self._tcp_server.close()
            self._tcp_server = None


# ─── Main ───
async def main():
    import argparse
    parser = argparse.ArgumentParser(description="SNIN NAT Hole Punch")
    parser.add_argument("--udp-port", type=int, default=UDP_PORT)
    parser.add_argument("--tcp-port", type=int, default=TCP_RELAY_PORT)
    args = parser.parse_args()

    sigs.init_signing()
    hp = HolePunch(udp_port=args.udp_port, tcp_port=args.tcp_port)
    await hp.start()

    # Опубликовать сигнал при старте
    await hp.publish_signal()

    print("[HP] Running. Press Ctrl+C to stop.")
    try:
        while True:
            await asyncio.sleep(30)
            stats = hp.get_stats()
            peers = hp.get_peers()
            print(f"[HP] 📊 punches: {stats['punches_sent']}s/{stats['punches_ok']}ok "
                  f"relay: {stats['relay_used']} peers: {len(peers)}")
    except (KeyboardInterrupt, asyncio.CancelledError):
        await hp.stop()
        print("[HP] 🔴 Stopped")


if __name__ == "__main__":
    asyncio.run(main())

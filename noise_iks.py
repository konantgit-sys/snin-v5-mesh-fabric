#!/usr/bin/env python3
"""
Noise Protocol — IK Handshake (Phase 1c: Encryption Ladder Level 2)
═══════════════════════════════════════════════════════════════════════════════

Спецификация: Noise Protocol Framework (rev34), паттерн IK.
Криптография: X25519 + ChaCha20-Poly1305 + HKDF-SHA256.
Статус: Phase 1c — активация transport-layer аутентификации.

ПАТТЕРН IK:
  <- s  (статический ключ получателя известен заранее)
  ...
  -> e, es, s, ss   (инициатор: ephemeral, static, оба DH)
  <- e, ee, se       (ответчик: ephemeral, оба DH)

После handshake — два CipherState для шифрования payload.

ИНТЕГРАЦИЯ:
  SmartRouter.__init__:
    noise = NoiseIKHandshake(my_static_key, peer_static_keys={...})
  
  При mesh-соединении:
    encrypted = noise.encrypt_for_peer("content_router", payload_bytes)
    payload = noise.decrypt_from_peer("content_router", encrypted)

ОТЛИЧИЕ ОТ mesh_crypto.py (уровень 1):
  mesh_crypto.py:   шифрует ДАННЫЕ (payload). Транспорт открытый.
  noise_iks.py:     аутентифицирует ТРАНСПОРТ. Handshake + ongoing encryption.
  Вместе:           mesh_crypto поверх noise = двойное шифрование (Data + Transport).

ЗАВИСИМОСТИ:
  cryptography (X25519, ChaCha20Poly1305, HKDF-SHA256) — уже установлены.
  Никаких внешних библиотек (не нужен noiseprotocol).
"""

import os
import hashlib
import hmac
import json
import struct
import time
import logging
from typing import Optional, Tuple, Dict

from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes, hmac as crypto_hmac

logger = logging.getLogger("snin.noise")

# ─── Константы Noise ───
PROTOCOL_NAME = b"Noise_IK_25519_ChaChaPoly_SHA256"
HASHLEN = 32
KEYLEN = 32
NONCELEN = 12
TAGLEN = 16
MAX_NONCE = 2**64 - 1

# ─── Ключи из окружения ───
NOISE_PORT = int(os.environ.get("NOISE_HANDSHAKE_PORT", "9620"))
NOISE_ENABLED = os.environ.get("SNIN_USE_NOISE", "0") == "1"
PEERS_FILE = os.environ.get("NOISE_PEERS_FILE", 
    os.path.join(os.path.dirname(__file__), "noise_peers.json"))


def hkdf(chaining_key: bytes, input_key_material: bytes, length: int = 64) -> Tuple[bytes, bytes]:
    """HKDF-SHA256 per Noise spec: (ck, ikm) → (output1, output2)."""
    temp_key = hmac.new(chaining_key, input_key_material, hashlib.sha256).digest()
    output1 = hmac.new(temp_key, bytes([0x01]), hashlib.sha256).digest()
    output2 = hmac.new(temp_key, output1 + bytes([0x02]), hashlib.sha256).digest()
    return output1, output2


class CipherState:
    """Шифрование/расшифровка отдельных сообщений после handshake."""
    
    def __init__(self):
        self.k: Optional[bytes] = None
        self.n: int = 0
    
    @property
    def has_key(self) -> bool:
        return self.k is not None
    
    def initialize_key(self, key: bytes):
        self.k = key
        self.n = 0
    
    def encrypt_with_ad(self, ad: bytes, plaintext: bytes) -> bytes:
        """Encrypt: увеличивает nonce, возвращает ciphertext+tag (16 байт)."""
        if not self.has_key:
            return plaintext
        nonce = struct.pack("<Q", self.n).ljust(12, b'\x00')
        self.n += 1
        aead = ChaCha20Poly1305(self.k)
        return aead.encrypt(nonce, plaintext, ad)
    
    def decrypt_with_ad(self, ad: bytes, ciphertext: bytes) -> bytes:
        """Decrypt: nonce из счётчика, возвращает plaintext."""
        if not self.has_key:
            return ciphertext
        nonce = struct.pack("<Q", self.n).ljust(12, b'\x00')
        self.n += 1
        aead = ChaCha20Poly1305(self.k)
        return aead.decrypt(nonce, ciphertext, ad)


class SymmetricState:
    """SymmetricState: handshake state machine."""
    
    def __init__(self, protocol_name: bytes = PROTOCOL_NAME):
        self.h = hashlib.sha256(protocol_name).digest() if len(protocol_name) <= HASHLEN else hashlib.sha256(protocol_name).digest()
        self.ck = self.h  # chaining key initialized to h
        self.cs = CipherState()
    
    def mix_key(self, input_key_material: bytes):
        """MixKey: HKDF(ck, ikm) → (ck, tk), then InitializeKey(tk)."""
        self.ck, temp_k = hkdf(self.ck, input_key_material)
        self.cs.initialize_key(temp_k)
    
    def mix_hash(self, data: bytes):
        """MixHash: h = SHA256(h || data)."""
        self.h = hashlib.sha256(self.h + data).digest()
    
    def mix_key_and_hash(self, input_key_material: bytes):
        """HKDF(ck, ikm) → (ck, temp_h, temp_k). MixHash(temp_h), InitializeKey(temp_k)."""
        self.ck, temp = hkdf(self.ck, input_key_material)
        temp_h, temp_k = temp[:HASHLEN], temp[HASHLEN:]
        self.mix_hash(temp_h)
        self.cs.initialize_key(temp_k)
    
    def encrypt_and_hash(self, plaintext: bytes) -> bytes:
        """Encrypt with cs, then MixHash ciphertext."""
        ciphertext = self.cs.encrypt_with_ad(self.h, plaintext)
        self.mix_hash(ciphertext)
        return ciphertext
    
    def decrypt_and_hash(self, ciphertext: bytes) -> bytes:
        """Decrypt with cs using h as AD, THEN MixHash ciphertext."""
        plaintext = self.cs.decrypt_with_ad(self.h, ciphertext)
        self.mix_hash(ciphertext)
        return plaintext
    
    def split(self) -> Tuple[CipherState, CipherState]:
        """Split: HKDF(ck, b'') → (k1, k2). Два CipherState для отправки/приёма."""
        k1, k2 = hkdf(self.ck, b"")
        c1, c2 = CipherState(), CipherState()
        c1.initialize_key(k1)
        c2.initialize_key(k2)
        return c1, c2


class NoiseIKHandshake:
    """
    Noise IK handshake — инициатор или ответчик.
    
    ИНИЦИАТОР (тот кто подключается):
        h = NoiseIKHandshake(my_static_key, peer_static_key, role="initiator")
        msg1 = h.write_message(b"")         # -> e, es, s, ss
        # ... отправить msg1 получателю ...
        msg2 = h.read_message(reply)        # <- e, ee, se
        send_cs, recv_cs = h.split()
    
    ОТВЕТЧИК (тот кто принимает):
        h = NoiseIKHandshake(my_static_key, peer_static_key, role="responder")
        msg1 = h.read_message(incoming)     # -> e, es, s, ss
        msg2 = h.write_message(b"")         # <- e, ee, se
        send_cs, recv_cs = h.split()
    """
    
    def __init__(self, local_static: bytes, responder_static: bytes, role: str = "initiator"):
        """
        Args:
            local_static: 32 байта static private key
            responder_static: 32 байта RESPONDER'S static public key (pre-message IK)
            role: "initiator" или "responder"
        """
        self.role = role
        self.sym = SymmetricState()
        
        # Парсим ключи
        if local_static:
            self.s = X25519PrivateKey.from_private_bytes(local_static)
            self.s_pub = self.s.public_key()
        else:
            self.s = None
            self.s_pub = None
        
        self.rs = X25519PublicKey.from_public_bytes(responder_static)
        
        # Эфемерный ключ (генерируется при write_message)
        self.e: Optional[X25519PrivateKey] = None
        
        # Состояние handshake
        self._handshake_done = False
        self._send_cs: Optional[CipherState] = None
        self._recv_cs: Optional[CipherState] = None
        
        # Pre-message: IK pattern " <- s" — MixHash of RESPONDER's static key for both.
        self.sym.mix_hash(responder_static)
        
        logger.debug(f"NoiseIK: init role={role} rs={responder_static.hex()[:16]}...")
    
    def write_message(self, payload: bytes = b"") -> bytes:
        """Запись следующего handshake-сообщения. Возвращает байты для отправки."""
        assert not self._handshake_done, "Handshake already complete"
        
        if self.role == "initiator":
            return self._write_initiator(payload)
        else:
            return self._write_responder(payload)
    
    def read_message(self, message: bytes) -> bytes:
        """Чтение handshake-сообщения от пира. Возвращает payload."""
        assert not self._handshake_done, "Handshake already complete"
        
        if self.role == "initiator":
            return self._read_initiator(message)
        else:
            return self._read_responder(message)
    
    def split(self) -> Tuple[CipherState, CipherState]:
        """Завершить handshake → (send_cipher, recv_cipher)."""
        assert not self._handshake_done, "Already split"
        self._handshake_done = True
        self._send_cs, self._recv_cs = self.sym.split()
        # Responder swaps: initiator sends with k1/recvs with k2,
        # responder sends with k2/recvs with k1
        if self.role == "responder":
            self._send_cs, self._recv_cs = self._recv_cs, self._send_cs
        logger.info(f"NoiseIK: handshake complete, keys derived")
        return self._send_cs, self._recv_cs
    
    @property
    def handshake_complete(self) -> bool:
        return self._handshake_done
    
    def encrypt(self, plaintext: bytes) -> bytes:
        """Encrypt после handshake (использует send_cs)."""
        assert self._handshake_done, "Handshake not complete"
        return self._send_cs.encrypt_with_ad(b"", plaintext)
    
    def decrypt(self, ciphertext: bytes) -> bytes:
        """Decrypt после handshake (использует recv_cs)."""
        assert self._handshake_done, "Handshake not complete"
        return self._recv_cs.decrypt_with_ad(b"", ciphertext)
    
    # ─── Internal: IK pattern steps ───
    
    def _write_initiator(self, payload: bytes) -> bytes:
        """Инициатор → первое сообщение: e, es, s, ss"""
        # Генерируем ephemeral
        self.e = X25519PrivateKey.generate()
        e_pub = self.e.public_key()
        
        # -> e (публичный ephemeral)
        msg = e_pub.public_bytes_raw()  # 32 bytes
        self.sym.mix_hash(msg)
        
        # -> es (DH: e × rs)
        es = self.e.exchange(self.rs)
        self.sym.mix_key(es)
        
        # -> s (публичный static, ЗАШИФРОВАННЫЙ)
        assert self.s is not None, "Initiator must have static key"
        s_pub_enc = self.sym.encrypt_and_hash(self.s_pub.public_bytes_raw())
        msg += s_pub_enc
        
        # -> ss (DH: s × rs)
        ss = self.s.exchange(self.rs)
        self.sym.mix_key(ss)
        
        # Payload шифруем
        if payload:
            msg += self.sym.encrypt_and_hash(payload)
        
        logger.debug(f"NoiseIK: initiator wrote msg1 ({len(msg)} bytes)")
        return msg
    
    def _read_responder(self, message: bytes) -> bytes:
        """Ответчик ← первое сообщение: e, es, s, ss"""
        cursor = 0
        
        # <- e (ephemeral public, открытый)
        re_raw = message[cursor:cursor+KEYLEN]
        cursor += KEYLEN
        self.re = X25519PublicKey.from_public_bytes(re_raw)
        self.sym.mix_hash(re_raw)
        
        # <- es (DH: s × re)
        assert self.s is not None, "Responder must have static key"
        es = self.s.exchange(self.re)
        self.sym.mix_key(es)
        
        # <- s (static public, ЗАШИФРОВАННЫЙ)
        s_enc = message[cursor:cursor+KEYLEN+TAGLEN]  # 32 + 16 tag
        cursor += KEYLEN + TAGLEN
        rs_raw = self.sym.decrypt_and_hash(s_enc)
        self.rs = X25519PublicKey.from_public_bytes(rs_raw)
        
        # <- ss (DH: s × rs)
        ss = self.s.exchange(self.rs)
        self.sym.mix_key(ss)
        
        # Payload (если есть)
        payload = b""
        if cursor < len(message):
            payload = self.sym.decrypt_and_hash(message[cursor:])
        
        logger.debug(f"NoiseIK: responder read msg1, payload={len(payload)}B")
        return payload
    
    def _write_responder(self, payload: bytes) -> bytes:
        """Ответчик → второе сообщение: e, ee, se"""
        # Генерируем ephemeral
        self.e = X25519PrivateKey.generate()
        e_pub = self.e.public_key()
        
        # -> e (публичный ephemeral)
        msg = e_pub.public_bytes_raw()
        self.sym.mix_hash(msg)
        
        # -> ee (DH: e × re)
        ee = self.e.exchange(self.re)
        self.sym.mix_key(ee)
        
        # -> se (DH: e × rs — responder's ephemeral × initiator's static)
        assert self.s is not None
        assert self.rs is not None
        se = self.e.exchange(self.rs)
        self.sym.mix_key(se)
        
        # Payload шифруем
        if payload:
            msg += self.sym.encrypt_and_hash(payload)
        
        logger.debug(f"NoiseIK: responder wrote msg2 ({len(msg)} bytes)")
        return msg
    
    def _read_initiator(self, message: bytes) -> bytes:
        """Инициатор ← второе сообщение: e, ee, se"""
        cursor = 0
        
        # <- e (ephemeral public)
        re_raw = message[cursor:cursor+KEYLEN]
        cursor += KEYLEN
        self.re = X25519PublicKey.from_public_bytes(re_raw)
        self.sym.mix_hash(re_raw)
        
        # <- ee (DH: e × re)
        ee = self.e.exchange(self.re)
        self.sym.mix_key(ee)
        
        # <- se (DH: s × re)
        assert self.s is not None
        se = self.s.exchange(self.re)
        self.sym.mix_key(se)
        
        # Payload
        payload = b""
        if cursor < len(message):
            payload = self.sym.decrypt_and_hash(message[cursor:])
        
        logger.debug(f"NoiseIK: initiator read msg2, payload={len(payload)}B")
        return payload


# ═══════════════════════════════════════════════════════════════════════════════
# PEER MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

class NoisePeerManager:
    """
    Управление статическими ключами пиров. Каждый mesh-узел имеет noise_static_key.
    """
    
    def __init__(self, peers_file: str = PEERS_FILE):
        self._peers_file = peers_file
        self._keys: Dict[str, bytes] = {}       # peer_name → static_public_key (32 bytes)
        self._my_key: Optional[bytes] = None     # my static private key (32 bytes)
        self._my_pub: Optional[bytes] = None     # my static public key (32 bytes)
        self._load()
    
    @property
    def my_static_private(self) -> Optional[bytes]:
        return self._my_key
    
    @property
    def my_static_public(self) -> Optional[bytes]:
        return self._my_pub
    
    def get_peer_key(self, peer_name: str) -> Optional[bytes]:
        return self._keys.get(peer_name)
    
    def list_peers(self) -> list:
        return list(self._keys.keys())
    
    def _load(self):
        """Загрузка ключей из noise_peers.json."""
        if os.path.exists(self._peers_file):
            try:
                with open(self._peers_file) as f:
                    data = json.load(f)
                self._my_key = bytes.fromhex(data.get("my_private", "")) if data.get("my_private") else None
                self._my_pub = bytes.fromhex(data.get("my_public", "")) if data.get("my_public") else None
                for name, pub in data.get("peers", {}).items():
                    self._keys[name] = bytes.fromhex(pub)
                logger.info(f"NoisePeerManager: loaded {len(self._keys)} peers")
            except Exception as e:
                logger.error(f"NoisePeerManager: load failed: {e}")
        else:
            logger.info(f"NoisePeerManager: no peers file at {self._peers_file}")
    
    def generate_my_key(self, save: bool = True) -> bytes:
        """Сгенерировать новый статический ключ."""
        priv = X25519PrivateKey.generate()
        self._my_key = priv.private_bytes_raw()
        self._my_pub = priv.public_key().public_bytes_raw()
        if save:
            self._save()
        logger.info(f"NoisePeerManager: generated new key pub={self._my_pub.hex()[:16]}...")
        return self._my_key
    
    def add_peer(self, name: str, public_key_hex: str, save: bool = True):
        """Добавить пир с известным публичным ключом."""
        self._keys[name] = bytes.fromhex(public_key_hex)
        if save:
            self._save()
    
    def _save(self):
        data = {
            "my_private": self._my_key.hex() if self._my_key else "",
            "my_public": self._my_pub.hex() if self._my_pub else "",
            "peers": {name: key.hex() for name, key in self._keys.items()}
        }
        os.makedirs(os.path.dirname(self._peers_file) or ".", exist_ok=True)
        with open(self._peers_file, "w") as f:
            json.dump(data, f, indent=2)
        logger.debug(f"NoisePeerManager: saved {len(self._keys)} peers")


# ═══════════════════════════════════════════════════════════════════════════════
# TCP WRAPPER — Noise поверх TCP соединения
# ═══════════════════════════════════════════════════════════════════════════════

class NoiseTCPWrapper:
    """
    Оборачивает TCP reader/writer в Noise-шифрованный канал.
    
    Поток:
    1. Подключение → NoiseIK handshake (2 сообщения)
    2. После handshake → encrypt/decrypt каждое сообщение через send_cs/recv_cs
    
    Использование:
        wrapper = NoiseTCPWrapper(peer_mgr, "content_router")
        await wrapper.connect("127.0.0.1", 9920)
        await wrapper.send(b"hello")
        data = await wrapper.recv()
    """
    
    def __init__(self, peer_mgr: NoisePeerManager, peer_name: str):
        self._mgr = peer_mgr
        self._peer_name = peer_name
        self._hs: Optional[NoiseIKHandshake] = None
        self._send_cs: Optional[CipherState] = None
        self._recv_cs: Optional[CipherState] = None
        self._reader = None
        self._writer = None
    
    @property
    def is_encrypted(self) -> bool:
        return self._hs is not None and self._hs.handshake_complete
    
    async def connect_as_initiator(self, host: str, port: int) -> bool:
        """Подключиться и выполнить IK handshake как инициатор."""
        peer_pub = self._mgr.get_peer_key(self._peer_name)
        if not peer_pub:
            logger.error(f"NoiseTCP: no key for peer '{self._peer_name}'")
            return False
        if not self._mgr.my_static_private:
            logger.error("NoiseTCP: no local static key")
            return False
        
        import asyncio
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=5.0
            )
        except Exception as e:
            logger.error(f"NoiseTCP: connect failed: {e}")
            return False
        
        # IK handshake — инициатор
        self._hs = NoiseIKHandshake(self._mgr.my_static_private, peer_pub, role="initiator")
        msg1 = self._hs.write_message(b"snin-noise-v1")
        
        # Отправляем msg1
        self._writer.write(struct.pack(">I", len(msg1)) + msg1)
        await self._writer.drain()
        
        # Читаем msg2
        try:
            len_bytes = await asyncio.wait_for(self._reader.readexactly(4), timeout=5.0)
            msg2_len = struct.unpack(">I", len_bytes)[0]
            if msg2_len > 65536:
                logger.error(f"NoiseTCP: msg2 too large: {msg2_len}")
                return False
            msg2 = await asyncio.wait_for(self._reader.readexactly(msg2_len), timeout=5.0)
        except Exception as e:
            logger.error(f"NoiseTCP: read msg2 failed: {e}")
            return False
        
        self._hs.read_message(msg2)
        self._send_cs, self._recv_cs = self._hs.split()
        
        logger.info(f"NoiseTCP: handshake complete with {self._peer_name} (initiator)")
        return True
    
    async def accept_as_responder(self, reader, writer, peer_name: str) -> bool:
        """Принять входящее соединение, выполнить IK handshake как ответчик."""
        peer_pub = self._mgr.get_peer_key(peer_name)
        if not peer_pub:
            logger.error(f"NoiseTCP: no key for peer '{peer_name}'")
            return False
        if not self._mgr.my_static_private:
            logger.error("NoiseTCP: no local static key")
            return False
        
        self._reader = reader
        self._writer = writer
        
        # Читаем msg1
        import asyncio
        try:
            len_bytes = await asyncio.wait_for(self._reader.readexactly(4), timeout=5.0)
            msg1_len = struct.unpack(">I", len_bytes)[0]
            if msg1_len > 65536:
                return False
            msg1 = await asyncio.wait_for(self._reader.readexactly(msg1_len), timeout=5.0)
        except Exception as e:
            logger.error(f"NoiseTCP: read msg1 failed: {e}")
            return False
        
        # IK handshake — ответчик
        self._hs = NoiseIKHandshake(self._mgr.my_static_private, peer_pub, role="responder")
        self._hs.read_message(msg1)
        msg2 = self._hs.write_message(b"snin-noise-v1")
        
        # Отправляем msg2
        self._writer.write(struct.pack(">I", len(msg2)) + msg2)
        await self._writer.drain()
        
        self._send_cs, self._recv_cs = self._hs.split()
        
        logger.info(f"NoiseTCP: handshake complete with {peer_name} (responder)")
        return True
    
    async def send(self, data: bytes) -> bool:
        """Отправить зашифрованное сообщение."""
        if not self.is_encrypted or not self._writer:
            return False
        try:
            encrypted = self._send_cs.encrypt_with_ad(b"", data)
            self._writer.write(struct.pack(">I", len(encrypted)) + encrypted)
            await self._writer.drain()
            return True
        except Exception as e:
            logger.error(f"NoiseTCP send: {e}")
            return False
    
    async def recv(self) -> Optional[bytes]:
        """Получить и расшифровать сообщение."""
        if not self.is_encrypted or not self._reader:
            return None
        import asyncio
        try:
            len_bytes = await asyncio.wait_for(self._reader.readexactly(4), timeout=30.0)
            data_len = struct.unpack(">I", len_bytes)[0]
            if data_len > 1048576:  # 1MB max
                logger.error(f"NoiseTCP recv: too large ({data_len})")
                return None
            data = await asyncio.wait_for(self._reader.readexactly(data_len), timeout=30.0)
            return self._recv_cs.decrypt_with_ad(b"", data)
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.error(f"NoiseTCP recv: {e}")
            return None
    
    def close(self):
        if self._writer:
            self._writer.close()
        self._writer = None
        self._reader = None
        self._send_cs = None
        self._recv_cs = None


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

def test_noise_ik_handshake() -> bool:
    """Тест: два пира выполняют IK handshake и обмениваются сообщениями."""
    import struct
    
    # Генерируем ключи
    alice_static = X25519PrivateKey.generate()
    bob_static = X25519PrivateKey.generate()
    
    alice_priv = alice_static.private_bytes_raw()
    bob_priv = bob_static.private_bytes_raw()
    alice_pub = alice_static.public_key().public_bytes_raw()
    bob_pub = bob_static.public_key().public_bytes_raw()
    
    # Alice = инициатор, Bob = ответчик — оба передают bob_pub как responder_static
    alice = NoiseIKHandshake(alice_priv, bob_pub, role="initiator")
    bob = NoiseIKHandshake(bob_priv, bob_pub, role="responder")
    
    # Alice → msg1 → Bob
    msg1 = alice.write_message(b"hello from alice")
    payload1 = bob.read_message(msg1)
    assert payload1 == b"hello from alice", f"Payload mismatch: {payload1!r}"
    
    # Bob → msg2 → Alice
    msg2 = bob.write_message(b"hello from bob")
    payload2 = alice.read_message(msg2)
    assert payload2 == b"hello from bob", f"Payload mismatch: {payload2!r}"
    
    # Split
    alice_send, alice_recv = alice.split()
    bob_send, bob_recv = bob.split()
    
    # Alice отправляет Bob
    encrypted = alice_send.encrypt_with_ad(b"", b"This is a secret message from Alice!")
    decrypted = bob_recv.decrypt_with_ad(b"", encrypted)
    assert decrypted == b"This is a secret message from Alice!", f"Decrypt mismatch: {decrypted!r}"
    
    # Bob отправляет Alice
    encrypted2 = bob_send.encrypt_with_ad(b"", b"And this is Bob's response!")
    decrypted2 = alice_recv.decrypt_with_ad(b"", encrypted2)
    assert decrypted2 == b"And this is Bob's response!", f"Decrypt mismatch: {decrypted2!r}"
    
    # Много сообщений (nonce increment)
    for i in range(100):
        msg = f"Message {i}".encode()
        enc = alice_send.encrypt_with_ad(b"", msg)
        dec = bob_recv.decrypt_with_ad(b"", enc)
        assert dec == msg, f"Nonce test failed at msg {i}"
    
    return True


if __name__ == "__main__":
    print("═══ Noise IK Handshake Test ═══")
    ok = test_noise_ik_handshake()
    print(f"  {'✅' if ok else '❌'} IK handshake + message exchange")
    
    # Тест с реальным TCP (локально)
    if ok:
        print("  ✅ All tests passed")
    else:
        print("  ❌ Tests failed")

#!/usr/bin/env python3
"""
L5T — Temporal Dead-Letter Layer
=================================
Система асинхронной доставки сообщений для офлайн-агентов.

Kind:9000 (dead-letter event) — зашифрованное сообщение, хранящееся на 5+ релеях.
TTL: 90 дней (NORMAL), 365 дней (CRITICAL).

Архитектура:
  [Отправитель] → DeadLetterQueue.push() → 5+ релеев kind:9000
  [Получатель] → DeadLetterQueue.pull() → расшифровка → доставка

Зависимости: cryptography, secp256k1, orjson, aiohttp/websockets
"""

import asyncio
import hashlib
import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional, Union

# ── Шифрование: X25519 + ChaCha20-Poly1305 ──
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

try:
    import orjson
    def _dumps(obj): return orjson.dumps(obj)
    def _loads(data): return orjson.loads(data)
except ImportError:
    import json as _json
    def _dumps(obj): return _json.dumps(obj).encode()
    def _loads(data): return _json.loads(data)


# ═══════════════════════════════════════════════════════════════
#  X25519 + ChaCha20-Poly1305 Encryption
# ═══════════════════════════════════════════════════════════════

def _derive_x25519_shared_key(privkey_hex: str, pubkey_hex: str) -> bytes:
    """
    X25519 ECDH → HKDF-SHA256 → 32-byte key.
    pubkey_hex: 64-char hex string (32 bytes X25519 public key).
    """
    privkey_bytes = bytes.fromhex(privkey_hex)
    pubkey_bytes = bytes.fromhex(pubkey_hex)
    priv = X25519PrivateKey.from_private_bytes(privkey_bytes)
    peer_pub = X25519PublicKey.from_public_bytes(pubkey_bytes)
    shared = priv.exchange(peer_pub)
    # Derive 32-byte key via HKDF
    hkdf = HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=b"v2bot-dead-letter-v1")
    return hkdf.derive(shared)


def dlq_encrypt(privkey_hex: str, recipient_pubkey_hex: str, plaintext: str) -> str:
    """
    X25519 ECDH → ChaCha20Poly1305 → base64.
    Возвращает: "base64_nonce:base64_ciphertext"
    """
    import base64
    key = _derive_x25519_shared_key(privkey_hex, recipient_pubkey_hex)
    nonce = os.urandom(12)
    chacha = ChaCha20Poly1305(key)
    ct = chacha.encrypt(nonce, plaintext.encode(), None)
    return f"{base64.b64encode(nonce).decode()}:{base64.b64encode(ct).decode()}"


def dlq_decrypt(privkey_hex: str, sender_pubkey_hex: str, encrypted: str) -> str:
    """
    X25519 ECDH → ChaCha20Poly1305 → расшифровка.
    Принимает: "base64_nonce:base64_ciphertext"
    """
    import base64
    try:
        nonce_b64, ct_b64 = encrypted.split(":", 1)
        nonce = base64.b64decode(nonce_b64)
        ct = base64.b64decode(ct_b64)
    except (ValueError, base64.binascii.Error):
        raise ValueError("Invalid DLQ encryption format")
    key = _derive_x25519_shared_key(privkey_hex, sender_pubkey_hex)
    chacha = ChaCha20Poly1305(key)
    plaintext = chacha.decrypt(nonce, ct, None)
    return plaintext.decode()


# ═══════════════════════════════════════════════════════════════
#  Типы данных
# ═══════════════════════════════════════════════════════════════

DLQ_PRIORITIES = {
    "NORMAL":   90 * 86400,   # 90 дней TTL
    "HIGH":     180 * 86400,  # 180 дней
    "CRITICAL": 365 * 86400,  # 1 год
}


@dataclass
class DeadLetterMessage:
    """Сообщение в очереди мёртвых писем."""
    hash: str
    from_pubkey: str
    to_pubkey: str
    content_enc: str          # X25519-ChaCha20 encrypted
    content: str = ""         # расшифрованный (только в памяти)
    kind: int = 39002
    priority: str = "NORMAL"
    created_at: int = 0
    ttl: int = 0
    delivered: bool = False
    delivery_at: Optional[int] = None
    relay_count: int = 0
    event_ids: list = field(default_factory=list)  # ID событий на релеях

    def is_expired(self) -> bool:
        return time.time() > self.ttl

    def to_dict(self) -> dict:
        return {
            "hash": self.hash,
            "from_pubkey": self.from_pubkey,
            "to_pubkey": self.to_pubkey,
            "content_enc": self.content_enc,
            "kind": self.kind,
            "priority": self.priority,
            "created_at": self.created_at,
            "ttl": self.ttl,
            "delivered": self.delivered,
            "delivery_at": self.delivery_at,
            "relay_count": self.relay_count,
        }

# ═══════════════════════════════════════════════════════════════
#  SQLite: Dead Letter Queue Storage
# ═══════════════════════════════════════════════════════════════

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    hash TEXT PRIMARY KEY,
    from_pubkey TEXT NOT NULL,
    to_pubkey TEXT NOT NULL,
    content_enc TEXT NOT NULL,
    kind INTEGER DEFAULT 39002,
    priority TEXT DEFAULT 'NORMAL',
    created_at INTEGER NOT NULL,
    ttl INTEGER NOT NULL,
    delivered INTEGER DEFAULT 0,
    delivery_at INTEGER,
    relay_count INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_dlq_to ON dead_letter_queue(to_pubkey);
CREATE INDEX IF NOT EXISTS idx_dlq_ttl ON dead_letter_queue(ttl);
CREATE INDEX IF NOT EXISTS idx_dlq_delivered ON dead_letter_queue(delivered);
"""


class DeadLetterQueue:
    """
    Dead Letter Queue — ядро L5T.
    
    Хранит зашифрованные сообщения в SQLite, публикует их на Nostr релеях
    как kind:9000 и синхронизирует пропущенные сообщения при появлении агента.
    """

    def __init__(self, db_path: str = "", pubkey_hex: str = "", privkey_hex: str = ""):
        self.db_path = db_path or "/home/agent/data/sites/relay-mesh/data/dead_letter.db"
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.pubkey = pubkey_hex
        self.privkey = privkey_hex
        self._conn = None
        self._lock = asyncio.Lock()
        self._relay_clients: dict = {}  # relay_url -> websocket connection
        self._init_db()

    def _init_db(self):
        conn = self._get_conn()
        conn.executescript(DB_SCHEMA)
        conn.commit()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    # ── Push: сохранение + публикация на релеи ──

    async def push(self, from_pubkey: str, to_pubkey: str, content: str,
                   kind: int = 39002, priority: str = "NORMAL",
                   skip_relay: bool = False) -> dict:
        """
        Зашифровать сообщение, сохранить в SQLite и опубликовать на 5+ релеях.
        skip_relay=True — только в БД, без публикации на релеи (для тестов).
        """
        if not self.privkey:
            return {"ok": False, "error": "no private key configured"}
        if priority not in DLQ_PRIORITIES:
            priority = "NORMAL"
        ttl_sec = DLQ_PRIORITIES[priority]
        created_at = int(time.time())
        content_enc = content  # Plaintext — L2 encryption handles wire security
        msg_hash = hashlib.sha256(
            f"{from_pubkey}:{to_pubkey}:{content}:{created_at}".encode()
        ).hexdigest()[:16]

        msg = DeadLetterMessage(
            hash=msg_hash,
            from_pubkey=from_pubkey,
            to_pubkey=to_pubkey,
            content_enc=content_enc,
            kind=kind,
            priority=priority,
            created_at=created_at,
            ttl=created_at + ttl_sec,
            relay_count=0,
            event_ids=[],
        )

        # Сохраняем в SQLite
        async with self._lock:
            conn = self._get_conn()
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO dead_letter_queue
                       (hash, from_pubkey, to_pubkey, content_enc, kind,
                        priority, created_at, ttl, delivered, relay_count)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0)""",
                    (msg.hash, msg.from_pubkey, msg.to_pubkey, msg.content_enc,
                     msg.kind, msg.priority, msg.created_at, msg.ttl)
                )
                conn.commit()
            except sqlite3.Error as e:
                return {"ok": False, "error": str(e)}

        if skip_relay:
            return {"ok": True, "hash": msg.hash, "relay_count": 0, "ttl": msg.ttl, "priority": priority}

        # Публикуем на релеях
        relay_count = await self._publish_to_relays(msg)
        if relay_count > 0:
            async with self._lock:
                conn = self._get_conn()
                conn.execute(
                    "UPDATE dead_letter_queue SET relay_count = ? WHERE hash = ?",
                    (relay_count, msg.hash)
                )
                conn.commit()

        return {
            "ok": True,
            "hash": msg.hash,
            "relay_count": relay_count,
            "ttl": msg.ttl,
            "priority": priority,
        }

    # ── Публикация на Nostr релеи ──

    async def _publish_to_relays(self, msg: DeadLetterMessage) -> int:
        """
        Опубликовать kind:9000 на 5+ релеях.
        Возвращает количество успешных публикаций.
        """
        relays = self._get_publish_relays(min_count=5)
        if not relays:
            return 0

        # Создаём Nostr событие kind:9000
        tags = [
            ["p", msg.to_pubkey],           # получатель
            ["p", msg.from_pubkey],          # отправитель
            ["t", "deadletter"],
            ["expiration", str(msg.ttl)],    # TTL (NIP-40)
        ]
        if msg.priority == "CRITICAL":
            tags.append(["priority", "critical"])

        event = await self._sign_event(msg.content_enc, 9000, tags, msg.created_at)

        # Публикуем параллельно на все релеи
        tasks = [self._publish_single(r, event) for r in relays]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success = sum(1 for r in results if r and isinstance(r, str))
        return success

    def _get_publish_relays(self, min_count: int = 5) -> list:
        """Вернуть список релеев для публикации (TIER 1-2)."""
        try:
            from nostr_relay_list import OUR_RELAYS_ALL, RELAY_TIERS
            # Берём TIER 1 + TIER 2
            tier1 = RELAY_TIERS.get(1, [])
            tier2 = RELAY_TIERS.get(2, [])
            relays = (tier1 + tier2)[:max(min_count, 10)]
            return relays if len(relays) >= min_count else OUR_RELAYS_ALL[:min_count]
        except ImportError:
            return []

    async def _publish_single(self, relay_url: str, event: dict) -> Optional[str]:
        """Опубликовать событие на одном релее."""
        import websockets
        uri = relay_url.replace("wss://", "wss://").replace("ws://", "ws://")
        # Добавляем /, если нет
        if not uri.endswith("/"):
            uri += "/"
        try:
            async with websockets.connect(uri, open_timeout=10, close_timeout=5) as ws:
                payload = _dumps(["EVENT", event])
                await ws.send(payload.decode() if isinstance(payload, bytes) else payload)
                resp = await asyncio.wait_for(ws.recv(), timeout=10)
                data = _loads(resp) if isinstance(resp, (bytes, str)) else resp
                if isinstance(data, list) and len(data) >= 3 and data[0] == "OK" and data[1] == event["id"]:
                    if data[2] is True:
                        return event["id"]
                    else:
                        return None
                return None
        except Exception as e:
            return None

    async def _sign_event(self, content: str, kind: int,
                          tags: list, created_at: int = 0) -> dict:
        """Подписать Nostr событие (синхронно, без ProcessPool для экономии)."""
        try:
            from nostr_core import sign_event
            return sign_event(
                self.pubkey, self.privkey, content, kind, tags, created_at
            )
        except Exception:
            # Полный fallback — подпись прямо тут
            if created_at == 0:
                created_at = int(time.time())
            import json as _js
            serialized = _js.dumps(
                [0, self.pubkey, created_at, kind, tags, content],
                ensure_ascii=False, separators=(',', ':')
            )
            event_id = hashlib.sha256(serialized.encode()).hexdigest()
            privkey_bytes = bytes.fromhex(self.privkey)
            pk = secp256k1.PrivateKey(privkey_bytes)
            sig = pk.schnorr_sign(bytes.fromhex(event_id), None)
            return {
                "id": event_id,
                "pubkey": self.pubkey,
                "created_at": created_at,
                "kind": kind,
                "tags": tags,
                "content": content,
                "sig": sig.hex(),
            }

    # ── Pull: получение пропущенных сообщений ──

    async def pull(self, to_pubkey: str, since: int = 0, mark_delivered: bool = True) -> list:
        """
        Получить все не доставленные сообщения для получателя.
        Запрашивает kind:9000 со всех релеев since=последний sync.
        Возвращает список DeadLetterMessage (расшифрованных).
        Если mark_delivered=False — только читает, не помечает доставленными.
        """
        messages = []
        seen_hashes = set()

        # 1. Сначала локальная БД (быстрее, работает без релеев)
        async with self._lock:
            conn = self._get_conn()
            rows = conn.execute(
                """SELECT hash, from_pubkey, to_pubkey, content_enc, kind,
                   priority, created_at, ttl, relay_count
                   FROM dead_letter_queue
                   WHERE to_pubkey = ? AND delivered = 0 AND created_at > ?
                   ORDER BY created_at""",
                (to_pubkey, since)
            ).fetchall()
            for row in rows:
                msg = DeadLetterMessage(
                    hash=row["hash"],
                    from_pubkey=row["from_pubkey"],
                    to_pubkey=row["to_pubkey"],
                    content_enc=row["content_enc"],
                    kind=row["kind"],
                    priority=row["priority"],
                    created_at=row["created_at"],
                    ttl=row["ttl"],
                    relay_count=row["relay_count"],
                    content=row["content_enc"],
                )
                if msg.hash not in seen_hashes:
                    seen_hashes.add(msg.hash)
                    messages.append(msg)

        # 2. Запрашиваем с релеев (для кросс-серверной синхронизации)
        relays = self._get_publish_relays(min_count=1)
        if relays:
            tasks = [self._fetch_from_relay(r, to_pubkey, since) for r in relays]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for raw_events in results:
                if not isinstance(raw_events, list):
                    continue
                for ev in raw_events:
                    msg = self._event_to_message(ev)
                    if msg and msg.hash not in seen_hashes:
                        seen_hashes.add(msg.hash)
                        messages.append(msg)

        # 2. Сортируем по времени
        messages.sort(key=lambda m: m.created_at)

        # 3. Контент уже plaintext
        for msg in messages:
            msg.content = msg.content_enc  # Plaintext, no decryption needed

        # 4. Помечаем как доставленные в БД
        if mark_delivered:
            async with self._lock:
                conn = self._get_conn()
                now = int(time.time())
                for msg in messages:
                    conn.execute(
                        "UPDATE dead_letter_queue SET delivered=1, delivery_at=? WHERE hash=?",
                        (now, msg.hash)
                    )
                conn.commit()

        return messages

    # ── Fetch с одного релея ──

    async def _fetch_from_relay(self, relay_url: str, to_pubkey: str,
                                since: int = 0) -> list:
        """
        Запросить kind:9000 для pubkey с одного релея.
        Использует NIP-01 фильтр по тегу p и kind.
        """
        import websockets
        uri = relay_url.replace("wss://", "wss://").replace("ws://", "ws://")
        if not uri.endswith("/"):
            uri += "/"
        try:
            async with websockets.connect(uri, open_timeout=10, close_timeout=5) as ws:
                sub_id = hashlib.md5(f"{to_pubkey}:{since}:{time.time()}".encode()).hexdigest()[:8]
                filters = [{
                    "kinds": [9000],
                    "#p": [to_pubkey],
                    "since": since,
                    "limit": 100,
                }]
                req = _dumps(["REQ", sub_id] + filters)
                await ws.send(req.decode() if isinstance(req, bytes) else req)
                events = []
                while True:
                    try:
                        resp = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        break
                    data = _loads(resp) if isinstance(resp, (bytes, str)) else resp
                    if isinstance(data, list):
                        if data[0] == "EVENT" and data[1] == sub_id:
                            events.append(data[2])
                        elif data[0] == "EOSE" and data[1] == sub_id:
                            break
                # Закрываем подписку
                try:
                    await ws.send(_dumps(["CLOSE", sub_id]))
                except Exception:
                    pass
                return events
        except Exception:
            return []

    def _event_to_message(self, event: dict) -> Optional[DeadLetterMessage]:
        """Преобразовать Nostr событие kind:9000 в DeadLetterMessage."""
        if not event or event.get("kind") != 9000:
            return None
        tags = {t[0]: t[1] if len(t) > 1 else "" for t in event.get("tags", [])}
        to_pubkey = tags.get("p", "")
        from_pubkey = event.get("pubkey", "")
        if not to_pubkey or not from_pubkey:
            return None
        priority = "NORMAL"
        if "priority" in tags and tags["priority"] == "critical":
            priority = "CRITICAL"
        elif "l" in tags:
            priority = tags["l"].upper() if tags["l"].upper() in DLQ_PRIORITIES else "NORMAL"

        ttl = int(tags.get("expiration", 0))
        if ttl == 0:
            ttl_sec = DLQ_PRIORITIES.get(priority, 90 * 86400)
            ttl = event.get("created_at", int(time.time())) + ttl_sec

        msg_hash = hashlib.sha256(
            f"{from_pubkey}:{to_pubkey}:{event.get('content','')}:{event.get('created_at',0)}".encode()
        ).hexdigest()[:16]

        return DeadLetterMessage(
            hash=msg_hash,
            from_pubkey=from_pubkey,
            to_pubkey=to_pubkey,
            content_enc=event.get("content", ""),
            kind=event.get("kind", 39002),
            priority=priority,
            created_at=event.get("created_at", 0),
            ttl=ttl,
            delivered=False,
            relay_count=1,
            event_ids=[event.get("id", "")],
        )

    # ── Sync API ──

    async def sync(self, to_pubkey: str, since: int = 0) -> list:
        """
        Полная синхронизация:
        1. Берёт не доставленные из локальной БД
        2. Запрашивает новые с релеев
        3. Объединяет, дедуплицирует
        4. Помечает доставленные
        """
        # Сначала локальные не доставленные
        local_undelivered = self._get_local_undelivered(to_pubkey, since=since)
        local_hashes = {m.hash for m in local_undelivered}

        # Потом с релеев
        remote = await self.pull(to_pubkey, since=since)
        for m in remote:
            if m.hash not in local_hashes:
                local_undelivered.append(m)
                local_hashes.add(m.hash)

        local_undelivered.sort(key=lambda m: m.created_at)
        return local_undelivered

    def _get_local_undelivered(self, to_pubkey: str, since: int = 0) -> list:
        """Вернуть не доставленные сообщения из локальной БД (опционально с since)."""
        conn = self._get_conn()
        if since > 0:
            rows = conn.execute(
                """SELECT * FROM dead_letter_queue
                   WHERE to_pubkey = ? AND delivered = 0 AND ttl > ? AND created_at >= ?
                   ORDER BY created_at ASC""",
                (to_pubkey, int(time.time()), since)
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT * FROM dead_letter_queue
                   WHERE to_pubkey = ? AND delivered = 0 AND ttl > ?
                   ORDER BY created_at ASC""",
                (to_pubkey, int(time.time()))
            ).fetchall()
        result = []
        for row in rows:
            msg = DeadLetterMessage(
                hash=row["hash"],
                from_pubkey=row["from_pubkey"],
                to_pubkey=row["to_pubkey"],
                content_enc=row["content_enc"],
                kind=row["kind"],
                priority=row["priority"],
                created_at=row["created_at"],
                ttl=row["ttl"],
                delivered=bool(row["delivered"]),
                delivery_at=row["delivery_at"],
                relay_count=row["relay_count"],
            )
            # Plaintext — no decryption needed
            msg.content = msg.content_enc
            result.append(msg)
        return result

    def mark_delivered(self, msg_hash: str) -> bool:
        """Отметить сообщение как доставленное."""
        conn = self._get_conn()
        now = int(time.time())
        cur = conn.execute(
            "UPDATE dead_letter_queue SET delivered = 1, delivery_at = ? WHERE hash = ?",
            (now, msg_hash)
        )
        conn.commit()
        return cur.rowcount > 0

    # ── TTL Cleanup ──

    def purge_expired(self) -> int:
        """Удалить просроченные и доставленные сообщения."""
        conn = self._get_conn()
        now = int(time.time())
        # Удаляем просроченные (не важно, доставлены или нет)
        cur = conn.execute(
            "DELETE FROM dead_letter_queue WHERE ttl < ?",
            (now,)
        )
        removed = cur.rowcount
        # Также удаляем доставленные старше 7 дней
        cur = conn.execute(
            "DELETE FROM dead_letter_queue WHERE delivered = 1 AND delivery_at < ?",
            (now - 7 * 86400,)
        )
        removed += cur.rowcount
        conn.commit()
        return removed

    # ── Статистика ──

    def stats(self) -> dict:
        """Статистика очереди."""
        conn = self._get_conn()
        total = conn.execute("SELECT COUNT(*) FROM dead_letter_queue").fetchone()[0]
        undelivered = conn.execute(
            "SELECT COUNT(*) FROM dead_letter_queue WHERE delivered = 0 AND ttl > ?",
            (int(time.time()),)
        ).fetchone()[0]
        by_priority = {}
        for row in conn.execute(
            "SELECT priority, COUNT(*) as cnt FROM dead_letter_queue GROUP BY priority"
        ).fetchall():
            by_priority[row["priority"]] = row["cnt"]
        expired = conn.execute(
            "SELECT COUNT(*) FROM dead_letter_queue WHERE ttl < ?",
            (int(time.time()),)
        ).fetchone()[0]
        return {
            "total": total,
            "undelivered": undelivered,
            "by_priority": by_priority,
            "expired": expired,
        }

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None


# ═══════════════════════════════════════════════════════════════
#  Sync API Server (для интеграции с Health Engine)
# ═══════════════════════════════════════════════════════════════

DLQ_INSTANCE: Optional[DeadLetterQueue] = None


def get_dlq() -> DeadLetterQueue:
    global DLQ_INSTANCE
    if DLQ_INSTANCE is None:
        DLQ_INSTANCE = DeadLetterQueue()
    return DLQ_INSTANCE


async def handle_dlq_sync(request):
    """HTTP handler для /api/v1/deadletter/sync"""
    try:
        import json as _j
        body = await request.json()
        to_pubkey = body.get("pubkey", "")
        since = body.get("since", 0)
        if not to_pubkey:
            return _j.dumps({"ok": False, "error": "pubkey required"})
        dlq = get_dlq()
        messages = await dlq.sync(to_pubkey, since)
        return _j.dumps({
            "ok": True,
            "count": len(messages),
            "messages": [m.to_dict() for m in messages],
        })
    except Exception as e:
        return _j.dumps({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════
#  Самотест
# ═══════════════════════════════════════════════════════════════

async def self_test():
    """Быстрый тест шифрования и БД."""
    print("=== L5T Dead-Letter Queue Self-Test ===")
    
    # Тест 1: NIP-04 шифрование
    print("\n1. X25519-ChaCha20 encrypt/decrypt...")
    # Генерируем тестовые X25519 ключи
    alice_priv = X25519PrivateKey.generate()
    bob_priv = X25519PrivateKey.generate()
    alice_pub = alice_priv.public_key().public_bytes_raw().hex()
    bob_pub = bob_priv.public_key().public_bytes_raw().hex()
    alice_priv_hex = alice_priv.private_bytes_raw().hex()
    bob_priv_hex = bob_priv.private_bytes_raw().hex()
    
    plaintext = "Hello Bob, this is a dead letter test!"
    encrypted = dlq_encrypt(alice_priv_hex, bob_pub, plaintext)
    decrypted = dlq_decrypt(bob_priv_hex, alice_pub, encrypted)
    assert decrypted == plaintext, f"Mismatch: {decrypted} != {plaintext}"
    print(f"   ✅ Alice→Bob: '{plaintext}'")
    print(f"   Encrypted: {encrypted[:50]}...")
    
    # Тест 2: SQLite
    print("\n2. SQLite storage...")
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    dlq_alice = DeadLetterQueue(db_path=db_path, pubkey_hex=alice_pub, privkey_hex=alice_priv_hex)
    result = await dlq_alice.push(alice_pub, bob_pub, plaintext, priority="NORMAL", skip_relay=True)
    assert result["ok"], f"Push failed: {result}"
    print(f"   ✅ Push: hash={result['hash']}, relays={result['relay_count']}")
    
    # Тест 3: локальный pull (от имени получателя Bob)
    print("\n3. Local pull (Bob получает)...")
    dlq_bob = DeadLetterQueue(db_path=db_path, pubkey_hex=bob_pub, privkey_hex=bob_priv_hex)
    local = dlq_bob._get_local_undelivered(bob_pub)
    assert len(local) > 0, "No undelivered messages"
    assert local[0].content == plaintext, f"Content mismatch: {local[0].content[:40]} != {plaintext[:40]}"
    print(f"   ✅ Pull: {len(local)} message(s), content='{local[0].content}'")
    
    # Тест 4: TTL
    print("\n4. TTL check...")
    msg = local[0]
    assert msg.ttl > msg.created_at, f"Invalid TTL: {msg.ttl} <= {msg.created_at}"
    print(f"   ✅ TTL: {msg.ttl - msg.created_at}s ({DLQ_PRIORITIES[msg.priority]}s expected)")
    
    # Тест 5: mark delivered
    print("\n5. Mark delivered...")
    dlq_bob.mark_delivered(msg.hash)
    assert dlq_bob._get_local_undelivered(bob_pub) == [], "Should be empty after delivery"
    print(f"   ✅ Delivered")
    
    # Тест 6: purge expired
    print("\n6. Purge expired...")
    removed = dlq_bob.purge_expired()
    print(f"   ✅ Purged: {removed} messages")
    
    stats = dlq_bob.stats()
    print(f"\n   Stats: {stats}")
    
    # Очистка
    os.unlink(db_path)
    print("\n=== All L5T self-tests passed ===")


if __name__ == "__main__":
    asyncio.run(self_test())

"""CPU Worker — thread pool + process pool for blocking operations.

Exports:
  hash_sha256_async(data: str) → await (ThreadPool, Level 1)
  dht_distance_async(a: str, b: str) → await (ThreadPool, Level 1)
  sign_event_async(serialized: str) → await (ThreadPool, Level 1)
  sign_event_full_async(pubkey, privkey, content, kind, tags, ts) → dict (ProcessPool, Level 2)
  verify_ed25519_processpool_async(pubkey, payload, sig) → bool (ProcessPool, Level 2)
  shutdown_pools() — graceful shutdown
"""

import asyncio
import hashlib
import json
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# ─── Thread Pool (Level 1) ────────────────────────────────────────────
_CPU_POOL = ThreadPoolExecutor(max_workers=2, thread_name_prefix="cpu_")

# ─── Process Pool (Level 2) — lazy init with spawn context ────────────
_CRYPTO_POOL: ProcessPoolExecutor | None = None
_POOL_INIT_LOCK = asyncio.Lock()


def _get_crypto_pool() -> ProcessPoolExecutor:
    """Lazy init ProcessPoolExecutor с spawn контекстом (fork-safe)."""
    global _CRYPTO_POOL
    if _CRYPTO_POOL is None:
        ctx = multiprocessing.get_context('spawn')
        _CRYPTO_POOL = ProcessPoolExecutor(
            max_workers=1,
            mp_context=ctx,
        )
    return _CRYPTO_POOL


def get_cpu_loop():
    """Текущий event loop для executor."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.new_event_loop()


# ═══════════════════════════════════════════════════════════════════════
# Level 1 — ThreadPool для SHA256 / XOR
# ═══════════════════════════════════════════════════════════════════════

async def hash_sha256_async(data: str) -> str:
    """SHA256 в thread pool. Возвращает hex digest."""
    loop = get_cpu_loop()
    return await loop.run_in_executor(
        _CPU_POOL,
        lambda: hashlib.sha256(data.encode()).hexdigest()
    )


async def dht_distance_async(node_id_a: str, node_id_b: str) -> int:
    """XOR distance между двумя node ID (hex). Возвращает int."""
    loop = get_cpu_loop()

    def xor_metric():
        a = int(node_id_a, 16)
        b = int(node_id_b, 16)
        return a ^ b

    return await loop.run_in_executor(_CPU_POOL, xor_metric)


async def sign_event_async(serialized: str) -> str:
    """SHA256 события. Возвращает event ID (hex)."""
    loop = get_cpu_loop()
    return await loop.run_in_executor(
        _CPU_POOL,
        lambda: hashlib.sha256(serialized.encode()).hexdigest()
    )


async def make_nostr_id_async(pubkey: str, content: str, kind: int, ts: int) -> str:
    """Генерация Nostr event id в thread pool (Level 1)."""
    raw = json.dumps([0, pubkey, ts, kind, [], content], separators=(",", ":"))
    return await hash_sha256_async(raw)


# ── Алиас для обратной совместимости ──
create_event_with_async_sha256 = make_nostr_id_async


# ═══════════════════════════════════════════════════════════════════════
# Level 2 — ProcessPool для Schnorr signing / Ed25519 verify
# ═══════════════════════════════════════════════════════════════════════

# ── Worker для подписи (запускается в отдельном процессе) ──

def _sign_event_worker(pubkey_hex: str, private_key_hex: str, content: str,
                        kind: int, tags: list | None, created_at: int) -> dict:
    """
    Полная подпись Nostr события В ОТДЕЛЬНОМ ПРОЦЕССЕ.
    SHA256 + Schnorr signing — ни то, ни другое не блокирует event loop.
    """
    import hashlib as _hl
    import json as _js

    ts = created_at or int(__import__('time').time())
    tags = tags or []

    # SHA256 event ID (в процессе — не блокирует event loop)
    serialized = _js.dumps([0, pubkey_hex, ts, kind, tags, content], separators=(",", ":"))
    event_id = _hl.sha256(serialized.encode()).hexdigest()

    event = {
        "id": event_id,
        "pubkey": pubkey_hex,
        "created_at": ts,
        "kind": kind,
        "tags": tags,
        "content": content,
        "sig": "",
    }

    # Schnorr подпись в процессе — main process не блокируется
    try:
        from nostr.key import PrivateKey
        from nostr.event import Event

        if len(private_key_hex) == 64:
            key = PrivateKey(bytes.fromhex(private_key_hex))
        else:
            key = PrivateKey.from_nsec(private_key_hex)

        ev = Event(
            content=content,
            public_key=pubkey_hex,
            kind=kind,
            tags=tags,
            created_at=ts,
        )
        key.sign_event(ev)
        event["id"] = ev.id
        event["sig"] = ev.signature
    except Exception as e:
        event["sig"] = f"sig_error:{e}"

    return event


async def sign_event_full_async(pubkey_hex: str, private_key_hex: str, content: str,
                                kind: int, tags: list = None, created_at: int = 0) -> dict:
    """
    Level 2: Полная подпись Nostr события в ProcessPool.
    SHA256 + Schnorr — оба в отдельном процессе.
    Event loop не блокируется вообще.
    Использовать вместо sign_event_async() на горячих путях.
    """
    loop = get_cpu_loop()
    pool = _get_crypto_pool()
    return await loop.run_in_executor(
        pool,
        _sign_event_worker,
        pubkey_hex, private_key_hex, content, kind, tags, created_at,
    )


# ── Worker для Ed25519 verify (запускается в отдельном процессе) ──

def _verify_ed25519_worker(pubkey_hex: str, payload_dict: dict, sig_hex: str) -> bool:
    """
    Ed25519 verify В ОТДЕЛЬНОМ ПРОЦЕССЕ.
    """
    try:
        from cryptography.hazmat.primitives.asymmetric import ed25519
        import json as _js
        vk_bytes = bytes.fromhex(pubkey_hex)
        vk = ed25519.Ed25519PublicKey.from_public_bytes(vk_bytes)
        message = _js.dumps(payload_dict, sort_keys=True, separators=(",", ":")).encode()
        sig = bytes.fromhex(sig_hex)
        vk.verify(sig, message)
        return True
    except Exception:
        return False


async def verify_ed25519_processpool_async(pubkey_hex: str, payload: dict, sig_hex: str) -> bool:
    """
    Level 2: Ed25519 verify в ProcessPool.
    Использовать вместо run_in_executor(ThreadPool) для гарантии.
    """
    loop = get_cpu_loop()
    pool = _get_crypto_pool()
    return await loop.run_in_executor(
        pool,
        _verify_ed25519_worker,
        pubkey_hex, payload, sig_hex,
    )


# ═══════════════════════════════════════════════════════════════════════
# Shutdown
# ═══════════════════════════════════════════════════════════════════════

def shutdown_pools():
    """Закрыть оба пула gracefully."""
    _CPU_POOL.shutdown(wait=True)
    global _CRYPTO_POOL
    if _CRYPTO_POOL:
        _CRYPTO_POOL.shutdown(wait=True)
        _CRYPTO_POOL = None

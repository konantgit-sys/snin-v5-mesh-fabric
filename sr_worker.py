#!/usr/bin/env python3
"""SR Worker — процесс SmartRouter, слушает на AF_UNIX.

Запуск: python3 sr_worker.py <worker_id> <unix_path>

Архитектура:
  - Полный SmartRouter (политики, CB, self-learning, каналы)
  - Принимает запросы через Unix socket (не TCP)
  - Каждый worker держит свои соединения к каналам
  - internal health на порту BASE_PORT + worker_id
"""

import sys
import os
import time as time_mod
import asyncio
import json

# ─── Config ────────────────────────────────────────────────────────
UNIX_DIR = "/tmp/snin"

# Добавляем путь к модулям mesh
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def run_worker(worker_id: int):
    """Запуск worker-а в отдельном процессе."""
    import smart_router as sr
    
    unix_path = os.path.join(UNIX_DIR, f"sr_w{worker_id}.sock")
    
    # Remove old socket
    try:
        os.unlink(unix_path)
    except FileNotFoundError:
        pass
    
    os.makedirs(UNIX_DIR, exist_ok=True)
    
    print(f"[Worker {worker_id}] 🧠 SR Worker starting...", flush=True)
    
    router = sr.SmartRouter()
    
    async def worker_run():
        # Connect all channels via ensure_channels
        await router.ensure_channels()
        
        # Apply policies
        await sr.apply_policies()
        
        # Фаза 6.2: in-memory policy cache
        await router._load_policy_cache()
        await router._sync_best_channels()
        
        n_gossip = len(router._gossip_writers)
        print(f"[Worker {worker_id}] Channels: mesh ✓ nostr ✓ gossip({n_gossip}/5) direct ✓", flush=True)
        print(f"[Worker {worker_id}] Policy cache: {len(router._policy_cache)} rules in memory", flush=True)
        r = await sr.aredis()
        n_policies = len(await r.hkeys(sr.POLICY_KEY)) if r else 0
        print(f"[Worker {worker_id}] Policies: {n_policies} rules in Redis", flush=True)
        print(f"[Worker {worker_id}] Unix socket: {unix_path}", flush=True)
        
        # Unix socket server
        unix_server = await asyncio.start_unix_server(
            router.handle_client, unix_path)
        
        # Internal health ping — without TCP port
        # Master checks worker health by connecting to Unix socket
        
        async def health_ping(reader, writer):
            await reader.readline()
            writer.close()
        
        async with unix_server:
            await asyncio.gather(
                unix_server.serve_forever(),
                router.self_learning_loop(),
                sr.print_status(router),
            )
    
    try:
        # import uvloop (disabled)
        # uvloop.install() (disabled)
        asyncio.run(worker_run())
    except Exception as e:
        print(f"[Worker {worker_id}] ❌ Fatal: {e}", flush=True)
        os._exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: sr_worker.py <worker_id>")
        sys.exit(1)
    worker_id = int(sys.argv[1])
    run_worker(worker_id)

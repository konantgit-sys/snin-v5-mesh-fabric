#!/usr/bin/env python3
"""SR Master v2 — Persistent Unix Connection Pool.

Вместо создания Unix соединения на каждое событие → Pool по 3 conn/worker.
Событие берёт готовое соединение из очереди, отправляет, возвращает обратно.
"""

import asyncio
import time
import os
import subprocess
import sys
import json

# ─── Config ────────────────────────────────────────────────────────
LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 9932
HEALTH_PORT = 9933
N_WORKERS = 8
POOL_SIZE = 3          # persistent connections per worker (3×8=24 total)
RECONNECT_DELAY = 2.0
UNIX_DIR = "/tmp/snin"
SITES_DIR = os.path.dirname(os.path.abspath(__file__))


# ─── Connection Pool ───────────────────────────────────────────────

class WorkerPool:
    """Пул постоянных Unix-соединений к одному worker."""
    
    def __init__(self, worker_id: int, unix_path: str, pool_size: int):
        self.worker_id = worker_id
        self.unix_path = unix_path
        self.pool_size = pool_size
        self.queue: asyncio.Queue = asyncio.Queue()
        self.alive = True
        self._fill_task = None
    
    async def fill(self):
        """Заполнить пул соединениями."""
        for i in range(self.pool_size):
            ok = await self._create_one(i)
            if not ok:
                print(f"  [Pool W{self.worker_id}] conn {i} FAILED", flush=True)
    
    async def _create_one(self, idx: int) -> bool:
        """Создать одно соединение к worker-у."""
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_unix_connection(self.unix_path), timeout=5)
            self.queue.put_nowait((r, w))
            return True
        except Exception as e:
            print(f"  [Pool W{self.worker_id}] create error: {e}", flush=True)
            return False
    
    async def get(self) -> tuple:
        """Взять соединение из пула (блокирующая операция)."""
        try:
            r, w = await asyncio.wait_for(self.queue.get(), timeout=5)
        except asyncio.TimeoutError:
            raise RuntimeError(f"Pool W{self.worker_id} empty")
        return r, w
    
    def put(self, reader, writer):
        """Вернуть соединение в пул."""
        self.queue.put_nowait((reader, writer))
    
    async def reconnect(self):
        """Пересоздать все соединения в пуле."""
        # Drain queue
        while not self.queue.empty():
            try:
                r, w = self.queue.get_nowait()
                w.close()
            except:
                pass
        
        # Re-fill
        for i in range(self.pool_size):
            await self._create_one(i)
        
        # Check health
        alive_count = self.queue.qsize()
        self.alive = alive_count > 0
        return alive_count


# ─── Master ────────────────────────────────────────────────────────

class SRMasterV2:
    """Master с Persistent Connection Pool."""
    
    def __init__(self):
        self.pools: list[WorkerPool] = []
        self.rr_idx = 0
        self.stats = {
            "forwarded": 0, "errors": 0, "connections": 0,
            "pool_empty": 0, "reconnects": 0,
        }
        self.start_time = time.time()
        self.worker_processes: list[subprocess.Popen] = []
    
    def start_workers(self):
        print(f"[Master] Starting {N_WORKERS} workers...", flush=True)
        
        for i in range(N_WORKERS):
            unix_path = os.path.join(UNIX_DIR, f"sr_w{i}.sock")
            try:
                os.unlink(unix_path)
            except FileNotFoundError:
                pass
            
            worker_script = os.path.join(SITES_DIR, "sr_worker.py")
            log_path = os.path.join(SITES_DIR, "logs", f"sr_worker_{i}.log")
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            
            with open(log_path, "w") as log_f:
                p = subprocess.Popen(
                    [sys.executable, "-u", worker_script, str(i)],
                    stdout=log_f, stderr=subprocess.STDOUT, close_fds=True,
                )
            
            self.worker_processes.append(p)
            print(f"[Master] Worker {i} start (PID {p.pid})", flush=True)
        
        print(f"[Master] Waiting for workers...", flush=True)
        time.sleep(6)
        
        alive = sum(1 for p in self.worker_processes if p.poll() is None)
        print(f"[Master] Workers alive: {alive}/{N_WORKERS}", flush=True)
        
        for i, p in enumerate(self.worker_processes):
            if p.poll() is not None:
                log_path = os.path.join(SITES_DIR, "logs", f"sr_worker_{i}.log")
                with open(log_path) as f:
                    for line in f.readlines()[-3:]:
                        print(f"  W{i} died: {line.rstrip()}", flush=True)
    
    async def build_pools(self):
        """Создать connection pools ко всем живым workers."""
        print(f"[Master] Building connection pools (×{POOL_SIZE} per worker)...", flush=True)
        
        for i in range(N_WORKERS):
            unix_path = os.path.join(UNIX_DIR, f"sr_w{i}.sock")
            pool = WorkerPool(i, unix_path, POOL_SIZE)
            await pool.fill()
            self.pools.append(pool)
            
            alive = pool.queue.qsize()
            status = "✅" if alive > 0 else "❌"
            print(f"  {status} W{i}: {alive}/{POOL_SIZE} connections", flush=True)
        
        total = sum(p.queue.qsize() for p in self.pools)
        print(f"[Master] Pool: {total}/{N_WORKERS*POOL_SIZE} connections alive", flush=True)
    
    async def fanout_event(self, data: bytes) -> bool:
        """Fire-and-forget: отправить событие в worker, не ждать ответа.
        
        Для sustained throughput — не блокируем loop на ожидание ответа.
        """
        forwarded = False
        for _ in range(N_WORKERS):
            wi = self.rr_idx % N_WORKERS
            self.rr_idx += 1
            
            if not self.pools[wi].alive:
                continue
            
            try:
                w_reader, w_writer = await self.pools[wi].get()
                w_writer.write(data)
                await w_writer.drain()
                self.pools[wi].put(w_reader, w_writer)
                self.stats["forwarded"] += 1
                forwarded = True
                break
            except Exception:
                self.pools[wi].alive = False
                self.stats["errors"] += 1
        
        return forwarded

    async def handle_client(self, reader, writer):
        """Keep-alive TCP → fire-and-forget → worker pool.
        
        Для большинства kind-ов (39000-39099) не ждём ответа.
        Для kind:1 (Nostr external) — ждём.
        """
        self.stats["connections"] += 1
        
        try:
            while True:
                data = await asyncio.wait_for(reader.readline(), timeout=30)
                if not data:
                    break
                await self.fanout_event(data)
        
        except asyncio.TimeoutError:
            pass
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            self.stats["errors"] += 1
            print(f"[Master] Error: {str(e)[:50]}", flush=True)
        finally:
            try:
                writer.close()
            except:
                pass
    
    async def health_check(self, reader, writer):
        await reader.readline()
        
        pool_status = {}
        for p in self.pools:
            pool_status[f"w{p.worker_id}"] = {
                "alive": p.alive,
                "pooled": p.queue.qsize(),
            }
        
        alive_pools = sum(1 for p in self.pools if p.alive)
        status = {
            "status": "ok" if alive_pools > 0 else "dead",
            "version": "5.1.0",
            "pools": {
                "total": N_WORKERS * POOL_SIZE,
                "alive": sum(p.queue.qsize() for p in self.pools),
                "workers_alive": alive_pools,
            },
            "pool_detail": pool_status,
            "stats": self.stats,
            "uptime_sec": int(time.time() - self.start_time),
        }
        body = json.dumps(status).encode()
        resp = (
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Content-Length: " + str(len(body)).encode() + b"\r\n"
            b"Access-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n" + body
        )
        writer.write(resp)
        await writer.drain()
        writer.close()
    
    async def monitor_loop(self):
        """Периодически проверять здоровье пулов и пересоздавать упавшие."""
        while True:
            await asyncio.sleep(10)
            for pool in self.pools:
                if not pool.alive:
                    print(f"[Master] Reconnecting pool W{pool.worker_id}...", flush=True)
                    n = await pool.reconnect()
                    self.stats["reconnects"] += 1
                    if n:
                        print(f"[Master] W{pool.worker_id} reconnected ({n}/{POOL_SIZE})", flush=True)
                    else:
                        print(f"[Master] W{pool.worker_id} STILL DEAD", flush=True)
    
    async def run(self):
        os.makedirs(UNIX_DIR, exist_ok=True)
        
        print(f"[Master v2] 🚀 SR Cluster — {N_WORKERS} workers × {POOL_SIZE} pool", flush=True)
        print(f"[Master v2]    TCP :{LISTEN_PORT} → Pool → Workers (RR)", flush=True)
        print(f"[Master v2]    Health :{HEALTH_PORT}", flush=True)
        
        # Start workers
        self.start_workers()
        
        # Build connection pools
        await self.build_pools()
        total_alive = sum(p.queue.qsize() for p in self.pools)
        if total_alive == 0:
            print("[Master v2] ❌ Zero connections. Abort.", flush=True)
            return
        
        # TCP server
        server = await asyncio.start_server(
            self.handle_client, LISTEN_HOST, LISTEN_PORT)
        
        # Health server
        health = await asyncio.start_server(
            self.health_check, "127.0.0.1", HEALTH_PORT)
        
        print(f"[Master v2] ✅ TCP :{LISTEN_PORT} ({total_alive} pooled connections)", flush=True)
        
        async with server, health:
            await asyncio.gather(
                server.serve_forever(),
                health.serve_forever(),
                self.monitor_loop(),
            )


if __name__ == "__main__":
    try:
        asyncio.run(SRMasterV2().run())
    except KeyboardInterrupt:
        print("[Master] Shutdown")

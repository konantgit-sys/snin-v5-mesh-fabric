#!/usr/bin/env python3
"""SR Master v3 — Fire-and-Forget + Persistent Pool + Background Drain.

Master: читает событие из TCP → шлёт в pool (fire-and-forget) → сразу next.
Pool: 3 persistent Unix conn/worker, каждый с фоновой drain-задачей.
Drain: читает ответы workers в фоне, не блокируя главный loop.
"""

import asyncio
import time
import os
import subprocess
import sys
import json

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 9932
HEALTH_PORT = 9933
N_WORKERS = 8
POOL_SIZE = 3
UNIX_DIR = "/tmp/snin"
SITES_DIR = os.path.dirname(os.path.abspath(__file__))


# ─── Pool Connection with Background Drain ─────────────────────────

class PoolConnection:
    """Одно постоянное Unix-соединение с фоновым drain."""

    def __init__(self, worker_id: int, conn_id: int, reader, writer):
        self.worker_id = worker_id
        self.conn_id = conn_id
        self.reader = reader
        self.writer = writer
        self.alive = True
        self._drain_task: asyncio.Task | None = None
    
    async def write(self, data: bytes):
        """Fire: отправить данные, не ждать полной отправки.
        
        Не вызываем drain() — это блокирует loop когда pipe buffer полон.
        Background drain task освобождает буфер читая ответы.
        Если write_buffer_size превышает лимит → ждём немного.
        """
        try:
            self.writer.write(data)
            # Only drain if buffer is small — avoids blocking
            buf_size = len(self.writer._buffer) if hasattr(self.writer, '_buffer') else 0
            if buf_size > 65536:  # 64KB threshold
                await self.writer.drain()
            return True
        except Exception:
            self.alive = False
            return False
    
    def start_drain(self):
        """Фон: читать ответы worker-а, отбрасывать их."""
        async def _drain():
            while self.alive:
                try:
                    line = await asyncio.wait_for(
                        self.reader.readline(), timeout=60)
                    if not line:
                        self.alive = False
                        break
                except asyncio.TimeoutError:
                    continue
                except Exception:
                    self.alive = False
                    break
        self._drain_task = asyncio.create_task(_drain())
    
    async def close(self):
        self.alive = False
        if self._drain_task:
            self._drain_task.cancel()
        try:
            self.writer.close()
        except:
            pass


class WorkerPool:
    """Пул PoolConnection-ов к одному worker."""

    def __init__(self, worker_id: int, unix_path: str, pool_size: int):
        self.worker_id = worker_id
        self.unix_path = unix_path
        self.pool_size = pool_size
        self.queue: asyncio.Queue[PoolConnection] = asyncio.Queue()
        self.alive = True
        self._conns: list[PoolConnection] = []
    
    async def fill(self):
        for i in range(self.pool_size):
            conn = await self._create_one(i)
            if conn:
                self._conns.append(conn)
                self.queue.put_nowait(conn)
    
    async def _create_one(self, idx: int) -> PoolConnection | None:
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_unix_connection(self.unix_path), timeout=5)
            conn = PoolConnection(self.worker_id, idx, r, w)
            conn.start_drain()
            return conn
        except Exception as e:
            print(f"  [Pool W{self.worker_id}] conn {idx}: {e}", flush=True)
            return None
    
    async def get(self) -> PoolConnection:
        conn = await asyncio.wait_for(self.queue.get(), timeout=5)
        return conn
    
    def put(self, conn: PoolConnection):
        self.queue.put_nowait(conn)
    
    async def reconnect(self):
        """Пересоздать pool после падения."""
        for conn in self._conns:
            await conn.close()
        self._conns.clear()
        # Drain queue
        while not self.queue.empty():
            try:
                c = self.queue.get_nowait()
                await c.close()
            except Exception:
                pass
        
        await self.fill()
        self.alive = self.queue.qsize() > 0
        return self.queue.qsize()

    @property
    def pooled_count(self) -> int:
        return self.queue.qsize()


# ─── Master v3 ─────────────────────────────────────────────────────

class SRMasterV3:
    """Master: fire-and-forget + pool + drain."""

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
            
            log_path = os.path.join(SITES_DIR, "logs", f"sr_worker_{i}.log")
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            
            with open(log_path, "w") as log_f:
                p = subprocess.Popen(
                    [sys.executable, "-u", os.path.join(SITES_DIR, "sr_worker.py"), str(i)],
                    stdout=log_f, stderr=subprocess.STDOUT, close_fds=True,
                )
            self.worker_processes.append(p)
            print(f"  W{i} start (PID {p.pid})", flush=True)
        
        print(f"  Waiting...", flush=True)
        time.sleep(7)
        alive = sum(1 for p in self.worker_processes if p.poll() is None)
        print(f"  Workers alive: {alive}/{N_WORKERS}", flush=True)
        
        for i, p in enumerate(self.worker_processes):
            if p.poll() is not None:
                with open(os.path.join(SITES_DIR, "logs", f"sr_worker_{i}.log")) as f:
                    for line in f.readlines()[-3:]:
                        print(f"  W{i} died: {line.rstrip()}", flush=True)
    
    async def build_pools(self):
        print(f"[Master] Building pools (×{POOL_SIZE} per worker)...", flush=True)
        for i in range(N_WORKERS):
            unix_path = os.path.join(UNIX_DIR, f"sr_w{i}.sock")
            pool = WorkerPool(i, unix_path, POOL_SIZE)
            await pool.fill()
            self.pools.append(pool)
            alive = pool.pooled_count
            print(f"  {'✅' if alive else '❌'} W{i}: {alive}/{POOL_SIZE}", flush=True)
        
        total = sum(p.pooled_count for p in self.pools)
        print(f"  Total: {total}/{N_WORKERS*POOL_SIZE}", flush=True)
    
    async def handle_client(self, reader, writer):
        """Keep-alive → fire-and-forget → pool."""
        self.stats["connections"] += 1
        
        try:
            while True:
                data = await asyncio.wait_for(reader.readline(), timeout=30)
                if not data:
                    break
                
                # Pick worker RR
                forwarded = False
                for _ in range(N_WORKERS):
                    wi = self.rr_idx % N_WORKERS
                    self.rr_idx += 1
                    
                    pool = self.pools[wi]
                    if not pool.alive:
                        continue
                    
                    try:
                        conn = await pool.get()
                        ok = await conn.write(data)
                        pool.put(conn)
                        if ok:
                            self.stats["forwarded"] += 1
                            forwarded = True
                        else:
                            pool.alive = False
                        break
                    except asyncio.TimeoutError:
                        pool.alive = False
                        self.stats["pool_empty"] += 1
                    except Exception:
                        pool.alive = False
                
                if not forwarded:
                    self.stats["errors"] += 1
        
        except asyncio.TimeoutError:
            pass
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            self.stats["errors"] += 1
            print(f"[Master] {str(e)[:50]}", flush=True)
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
                "pooled": p.pooled_count,
            }
        alive_pools = sum(1 for p in self.pools if p.alive)
        status = json.dumps({
            "status": "ok" if alive_pools > 0 else "dead",
            "version": "5.2.0",
            "mode": "fire-and-forget + pool + drain",
            "pools": {
                "total": N_WORKERS * POOL_SIZE,
                "alive": sum(p.pooled_count for p in self.pools),
                "workers_alive": alive_pools,
            },
            "pool_detail": pool_status,
            "stats": self.stats,
            "uptime_sec": int(time.time() - self.start_time),
        }).encode()
        resp = (
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Content-Length: " + str(len(status)).encode() + b"\r\n"
            b"Access-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n" + status
        )
        writer.write(resp)
        await writer.drain()
        writer.close()
    
    async def monitor_loop(self):
        while True:
            await asyncio.sleep(10)
            for pool in self.pools:
                if not pool.alive:
                    print(f"[Master] Reconnecting W{pool.worker_id}...", flush=True)
                    n = await pool.reconnect()
                    self.stats["reconnects"] += 1
                    print(f"  {'✅' if n else '❌'} {n}/{POOL_SIZE}", flush=True)
    
    async def run(self):
        os.makedirs(UNIX_DIR, exist_ok=True)
        print(f"[Master v3] 🚀 SR Cluster — {N_WORKERS}w × {POOL_SIZE}pool FAF", flush=True)
        print(f"    TCP :{LISTEN_PORT} → Fire-and-Forget → RR Pool → Workers", flush=True)
        
        self.start_workers()
        await self.build_pools()
        
        total = sum(p.pooled_count for p in self.pools)
        if total == 0:
            print("[Master] ❌ Zero connections", flush=True)
            return
        
        server = await asyncio.start_server(self.handle_client, LISTEN_HOST, LISTEN_PORT)
        health = await asyncio.start_server(self.health_check, "127.0.0.1", HEALTH_PORT)
        
        print(f"    ✅ TCP :{LISTEN_PORT} — {total} pooled conns, fire-and-forget", flush=True)
        
        async with server, health:
            await asyncio.gather(
                server.serve_forever(),
                health.serve_forever(),
                self.monitor_loop(),
            )


if __name__ == "__main__":
    try:
        asyncio.run(SRMasterV3().run())
    except KeyboardInterrupt:
        print("[Master] Shutdown")

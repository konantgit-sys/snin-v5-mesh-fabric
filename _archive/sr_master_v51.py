#!/usr/bin/env python3
"""SR Master v5.1 — TCP proxy → Persistent Unix Worker Pool.

Ключевое отличие от v5.0: Master держит 2-4 постоянных Unix соединения
к каждому worker'у. Никакого open/close в hot path.

Архитектура:
  TCP :9932 ──► Master (RR × pool) ──► 16 persistent Unix connections to 8 workers
  Health :9933
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
POOL_PER_WORKER = 2
UNIX_DIR = "/tmp/snin"
SITES_DIR = os.path.dirname(os.path.abspath(__file__))


class PooledWorker:
    """Постоянный пул Unix соединений к одному worker'у.
    
    Каждое соединение защищено asyncio.Lock — защита от race condition
    при concurrent доступе из разных TCP-клиентов Master.
    """

    def __init__(self, worker_id: int, unix_path: str, pool_size: int = POOL_PER_WORKER):
        self.worker_id = worker_id
        self.unix_path = unix_path
        self.pool_size = pool_size
        self._conns: list[tuple] = []  # [(reader, writer, lock)]
        self._idx = 0
        self.alive = True

    async def connect(self) -> bool:
        """Открыть pool_size постоянных соединений."""
        for i in range(self.pool_size):
            try:
                r, w = await asyncio.wait_for(
                    asyncio.open_unix_connection(self.unix_path), timeout=5)
                lock = asyncio.Lock()
                self._conns.append((r, w, lock))
            except Exception as e:
                print(f"  [Pool {self.worker_id}] Conn {i} fail: {e}", flush=True)
        self.alive = len(self._conns) > 0
        return self.alive

    async def send_recv(self, data: bytes) -> bytes | None:
        """Отправить через постоянное соединение, получить ответ.
        
        Lock гарантирует что только одна coroutine читает из соединения.
        """
        if not self._conns:
            self.alive = False
            return None

        r, w, lock = self._conns[self._idx % len(self._conns)]
        self._idx += 1

        async with lock:
            try:
                w.write(data)
                await w.drain()
                resp = await asyncio.wait_for(r.readline(), timeout=15)
                return resp
            except (BrokenPipeError, ConnectionResetError, ConnectionError, asyncio.TimeoutError) as e:
                self._conns.remove((r, w, lock))
                try:
                    w.close()
                except:
                    pass
                # Reconnect if all dead
                if not self._conns:
                    try:
                        new_r, new_w = await asyncio.wait_for(
                            asyncio.open_unix_connection(self.unix_path), timeout=3)
                        new_lock = asyncio.Lock()
                        self._conns.append((new_r, new_w, new_lock))
                        self.alive = True
                        async with new_lock:
                            new_w.write(data)
                            await new_w.drain()
                            return await asyncio.wait_for(new_r.readline(), timeout=15)
                    except:
                        self.alive = False
                        return None
                # Retry with remaining conn
                return await self.send_recv(data)
            except Exception as e:
                print(f"  [Pool {self.worker_id}] Fatal: {e}", flush=True)
                self.alive = False
                return None

    async def close(self):
        for r, w, lock in self._conns:
            try:
                w.close()
            except:
                pass
        self._conns = []


class SRMaster:
    """Master with persistent connection pool to workers."""

    def __init__(self):
        self.workers: list[PooledWorker] = []
        self.rr_idx = 0
        self.stats = {"forwarded": 0, "errors": 0, "connections": 0}
        self.start_time = time.time()
        self.worker_processes: list[subprocess.Popen] = []

    def start_workers_blocking(self):
        """Синхронный запуск workers (до создания event loop)."""
        print(f"[Master] Starting {N_WORKERS} workers...", flush=True)
        for i in range(N_WORKERS):
            sock = os.path.join(UNIX_DIR, f"sr_w{i}.sock")
            try:
                os.unlink(sock)
            except FileNotFoundError:
                pass
            script = os.path.join(SITES_DIR, "sr_worker.py")
            log_p = os.path.join(SITES_DIR, "logs", f"sr_worker_{i}.log")
            os.makedirs(os.path.dirname(log_p), exist_ok=True)
            with open(log_p, "w") as f:
                p = subprocess.Popen(
                    [sys.executable, "-u", script, str(i)],
                    stdout=f, stderr=subprocess.STDOUT, close_fds=True)
            self.worker_processes.append(p)
            print(f"[Master] Worker {i} → PID {p.pid}", flush=True)
        print(f"[Master] Waiting 6s for worker init...", flush=True)
        time.sleep(6)
        alive = sum(1 for p in self.worker_processes if p.poll() is None)
        print(f"[Master] Workers alive: {alive}/{N_WORKERS}", flush=True)

    async def connect_pools_async(self):
        """Асинхронное подключение пулов ко всем живым workers."""
        print(f"[Master] Connecting pools ({POOL_PER_WORKER}×{N_WORKERS})...", flush=True)
        tasks = []
        for i in range(N_WORKERS):
            sock = os.path.join(UNIX_DIR, f"sr_w{i}.sock")
            if not os.path.exists(sock):
                continue
            pw = PooledWorker(i, sock)
            tasks.append((i, pw))
        results = await asyncio.gather(*[
            pw.connect() for _, pw in tasks
        ])
        for (i, pw), ok in zip(tasks, results):
            if ok:
                self.workers.append(pw)
                print(f"  [Master] Pool {i}: {len(pw._conns)} conn ✓", flush=True)
            else:
                print(f"  [Master] Pool {i}: DEAD ✗", flush=True)
        print(f"[Master] Pools: {len(self.workers)}/{N_WORKERS}, "
              f"{sum(len(pw._conns) for pw in self.workers)} total conns", flush=True)

    async def handle_client(self, reader, writer):
        """Keep-alive TCP → forward via persistent pool (zero open/close)."""
        self.stats["connections"] += 1
        try:
            while True:
                data = await asyncio.wait_for(reader.readline(), timeout=30)
                if not data:
                    break
                forwarded = False
                for _ in range(len(self.workers)):
                    wi = self.rr_idx % len(self.workers)
                    self.rr_idx += 1
                    pw = self.workers[wi]
                    if not pw.alive:
                        continue
                    resp = await pw.send_recv(data)
                    if resp is not None:
                        self.stats["forwarded"] += 1
                        writer.write(resp)
                        await writer.drain()
                        forwarded = True
                        break
                if not forwarded:
                    self.stats["errors"] += 1
                    writer.write(b'{"ok":false,"error":"no workers"}\n')
                    await writer.drain()
        except (TimeoutError, asyncio.TimeoutError):
            pass
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            self.stats["errors"] += 1
            print(f"[Master] Error: {e}", flush=True)
        finally:
            try:
                writer.close()
            except:
                pass

    async def health_check(self, reader, writer):
        await reader.readline()
        alive = sum(1 for pw in self.workers if pw.alive)
        conns = sum(len(pw._conns) for pw in self.workers)
        body = json.dumps({
            "status": "ok" if alive > 0 else "dead",
            "version": "5.1.0",
            "workers": {"total": len(self.workers), "alive": alive},
            "pool": {"connections": conns, "per_worker": POOL_PER_WORKER},
            "stats": self.stats,
            "uptime_sec": int(time.time() - self.start_time),
        }).encode()
        resp = (
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Content-Length: " + str(len(body)).encode() + b"\r\n"
            b"Access-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n" + body
        )
        writer.write(resp)
        await writer.drain()
        writer.close()

    async def run(self):
        os.makedirs(UNIX_DIR, exist_ok=True)
        print(f"[Master] 🚀 SR v5.1 — Persistent Pool ({POOL_PER_WORKER}×{N_WORKERS})", flush=True)

        # Workers start (sync, pre-event-loop)
        self.start_workers_blocking()

        # Connect pools (async)
        await self.connect_pools_async()

        if not self.workers:
            print("[Master] ❌ No pools. Abort.", flush=True)
            return

        server = await asyncio.start_server(self.handle_client, LISTEN_HOST, LISTEN_PORT)
        health = await asyncio.start_server(self.health_check, "127.0.0.1", HEALTH_PORT)

        print(f"[Master] ✅ TCP :{LISTEN_PORT} | Pool: "
              f"{sum(len(pw._conns) for pw in self.workers)} conns | Health :{HEALTH_PORT}", flush=True)

        async def monitor():
            while True:
                await asyncio.sleep(15)
                alive = sum(1 for pw in self.workers if pw.alive)
                if alive < len(self.workers):
                    print(f"[Master] Workers: {alive}/{len(self.workers)}", flush=True)

        async with server, health:
            await asyncio.gather(server.serve_forever(), health.serve_forever(), monitor())


if __name__ == "__main__":
    try:
        asyncio.run(SRMaster().run())
    except KeyboardInterrupt:
        print("[Master] Shutdown")

#!/usr/bin/env python3
"""SR Master — TCP proxy → N Unix Worker processes.

Архитектура:
  TCP :9932 ──► Master (round-robin) ──► AF_UNIX Workers
  Health :9933 ◄── Master (aggregated)

Каждый Worker — отдельный Python процесс с полным SmartRouter.
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
UNIX_DIR = "/tmp/snin"
SITES_DIR = os.path.dirname(os.path.abspath(__file__))


class SRMaster:
    """Lightweight TCP proxy → Unix workers."""
    
    def __init__(self):
        self.workers: list[dict] = []
        self.rr_idx = 0
        self.stats = {"forwarded": 0, "errors": 0, "connections": 0}
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
                    stdout=log_f,
                    stderr=subprocess.STDOUT,
                    close_fds=True,
                )
            
            self.worker_processes.append(p)
            self.workers.append({
                "pid": p.pid, "path": unix_path,
                "alive": True, "worker_id": i,
            })
            print(f"[Master] Worker {i} started (PID {p.pid})", flush=True)
        
        print(f"[Master] Waiting for workers...", flush=True)
        time.sleep(6)
        
        alive = sum(1 for p in self.worker_processes if p.poll() is None)
        print(f"[Master] Workers alive: {alive}/{N_WORKERS}", flush=True)
        
        for i, p in enumerate(self.worker_processes):
            if p.poll() is not None:
                log_path = os.path.join(SITES_DIR, "logs", f"sr_worker_{i}.log")
                print(f"[Master] Worker {i} DIED:", flush=True)
                try:
                    with open(log_path) as f:
                        for line in f.readlines()[-5:]:
                            print(f"  {line.rstrip()}", flush=True)
                except FileNotFoundError:
                    pass
                self.workers[i]["alive"] = False
    
    def check_workers(self):
        for i, p in enumerate(self.worker_processes):
            self.workers[i]["alive"] = (p.poll() is None)
    
    async def handle_client(self, reader, writer):
        self.stats["connections"] += 1
        try:
            while True:
                data = await asyncio.wait_for(reader.readline(), timeout=30)
                if not data:
                    break
                self.check_workers()
                
                forwarded = False
                for _ in range(N_WORKERS):
                    wi = self.rr_idx % N_WORKERS
                    self.rr_idx += 1
                    if not self.workers[wi]["alive"]:
                        continue
                    try:
                        w_reader, w_writer = await asyncio.wait_for(
                            asyncio.open_unix_connection(self.workers[wi]["path"]), timeout=1)
                        w_writer.write(data)
                        await w_writer.drain()
                        self.stats["forwarded"] += 1
                        resp = await asyncio.wait_for(w_reader.readline(), timeout=10)
                        if resp:
                            writer.write(resp)
                            await writer.drain()
                        w_writer.close()
                        forwarded = True
                        break
                    except Exception:
                        self.workers[wi]["alive"] = False
                
                if not forwarded:
                    self.stats["errors"] += 1
                    writer.write(b'{"ok":false,"error":"no workers"}\n')
                    await writer.drain()
        
        except asyncio.TimeoutError:
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
        self.check_workers()
        alive = sum(1 for w in self.workers if w["alive"])
        status = json.dumps({
            "status": "ok" if alive > 0 else "dead",
            "version": "5.0.0",
            "workers": {"total": N_WORKERS, "alive": alive, "dead": N_WORKERS - alive},
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
    
    async def run(self):
        os.makedirs(UNIX_DIR, exist_ok=True)
        print(f"[Master] 🚀 SR Cluster — {N_WORKERS} workers", flush=True)
        print(f"[Master]    TCP :{LISTEN_PORT} → Unix workers (RR)", flush=True)
        print(f"[Master]    Health :{HEALTH_PORT}", flush=True)
        
        self.start_workers()
        if sum(1 for w in self.workers if w["alive"]) == 0:
            print("[Master] ❌ All workers dead", flush=True)
            return
        
        server = await asyncio.start_server(self.handle_client, LISTEN_HOST, LISTEN_PORT)
        health = await asyncio.start_server(self.health_check, "127.0.0.1", HEALTH_PORT)
        
        print(f"[Master] ✅ Listening TCP :{LISTEN_PORT}", flush=True)
        
        async def monitor():
            while True:
                await asyncio.sleep(15)
                self.check_workers()
                a = sum(1 for w in self.workers if w["alive"])
                if a < N_WORKERS:
                    print(f"[Master] Workers: {a}/{N_WORKERS} alive", flush=True)
        
        async with server, health:
            await asyncio.gather(server.serve_forever(), health.serve_forever(), monitor())


if __name__ == "__main__":
    try:
        asyncio.run(SRMaster().run())
    except KeyboardInterrupt:
        print("[Master] Shutdown")

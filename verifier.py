#!/usr/bin/env python3
"""
SNIN Verifier Daemon — Phase 20 (S1 Optimistic Verify-Later)

Асинхронно читает kind:30000 из очереди, верифицирует через Solana RPC.
Запуск: python3 verifier.py [--port 9915]
"""

import json
import logging
import time
import os
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [VERIFIER] %(message)s')
logger = logging.getLogger('verifier')

# ── Config ──
QUEUE_FILE = "/dev/shm/payment_queue.jsonl"
POLL_INTERVAL = 1.0        # проверять очередь раз в секунду
STATUS_FILE = "/dev/shm/verifier_status.json"

# Solana RPC
try:
    from solana_rpc import verify_transaction
    SOLANA_AVAILABLE = True
    logger.info("Solana RPC module loaded")
except ImportError as e:
    SOLANA_AVAILABLE = False
    logger.warning(f"Solana RPC NOT available: {e}")

# In-memory stats
stats = {
    "total_processed": 0,
    "valid": 0,
    "invalid": 0,
    "errors": 0,
    "last_checked": 0,
}


def poll_queue():
    """Проверять очередь новых kind:30000."""
    global stats
    last_pos = 0
    
    while True:
        try:
            if not os.path.exists(QUEUE_FILE):
                with open(QUEUE_FILE, "w") as f:
                    f.write("")
                last_pos = 0
                time.sleep(POLL_INTERVAL)
                continue
            
            with open(QUEUE_FILE, "r") as f:
                f.seek(last_pos)
                lines = f.readlines()
                if lines:
                    last_pos = f.tell()
                
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json.loads(line)
                        verify_item(item)
                    except json.JSONDecodeError:
                        stats["errors"] += 1
                        continue
            
            stats["last_checked"] = time.time()
            _save_status()
            
        except Exception as e:
            logger.error(f"Poll error: {e}")
        
        time.sleep(POLL_INTERVAL)


def verify_item(item: dict):
    """Верифицировать один kind:30000."""
    global stats
    stats["total_processed"] += 1
    
    solana_tx = item.get("solana_tx", "")
    if not solana_tx:
        stats["invalid"] += 1
        logger.warning(f"Empty solana_tx from {item.get('pubkey','')[:12]}")
        return
    
    if not SOLANA_AVAILABLE:
        # Режим без Solana: все tx считаются валидными (тестовый)
        stats["valid"] += 1
        logger.info(f"[TEST MODE] ✅ tx {solana_tx[:16]}... accepted (no Solana RPC)")
        return
    
    try:
        import asyncio
        
        async def _verify():
            result = await verify_transaction(solana_tx)
            return result
        
        result = asyncio.run(_verify())
        
        if result.get("valid"):
            stats["valid"] += 1
            logger.info(f"✅ tx {solana_tx[:16]}... verified on Solana "
                        f"(amount: {item.get('amount',0)} {item.get('token','SNIN')})")
        else:
            stats["invalid"] += 1
            logger.warning(f"❌ tx {solana_tx[:16]}... INVALID: {result.get('reason','?')}")
    
    except Exception as e:
        stats["errors"] += 1
        logger.error(f"⚠️ tx {solana_tx[:16]}... verify error: {e}")


def _save_status():
    """Сохранить статус для чтения из app.py."""
    status = {
        "stats": stats,
        "uptime": time.time() - start_time,
        "solana_available": SOLANA_AVAILABLE,
        "queue_file": QUEUE_FILE,
        "poll_interval": POLL_INTERVAL,
    }
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f)


class StatusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        status = {
            **stats,
            "uptime": round(time.time() - start_time, 1),
            "solana_available": SOLANA_AVAILABLE,
        }
        self.wfile.write(json.dumps(status).encode())
    
    def log_message(self, *args):
        pass


def run_http(port: int):
    """HTTP-сервер для статуса."""
    server = HTTPServer(("127.0.0.1", port), StatusHandler)
    logger.info(f"Verifier HTTP status on :{port}")
    server.serve_forever()


if __name__ == "__main__":
    port = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[1] == "--port" else 9915
    start_time = time.time()
    
    logger.info(f"=== Verifier Daemon starting on :{port} ===")
    logger.info(f"Queue: {QUEUE_FILE}")
    logger.info(f"Solana RPC: {'AVAILABLE' if SOLANA_AVAILABLE else 'NOT AVAILABLE (test mode)'}")
    
    # HTTP статус в отдельном потоке
    http_thread = threading.Thread(target=run_http, args=(port,), daemon=True)
    http_thread.start()
    
    # Основной цикл — polling очереди
    poll_queue()

#!/usr/bin/env python3
"""
SNIN Cheque Book Daemon — Phase 21 (S4)
:9916 — управление чековыми книжками агентов

Каждый агент может купить чековую книжку:
  1 Solana tx = 10,000 подписанных чеков
  
kind:30000 с cheque верифицируется локально (Ed25519, 0.05ms)
→ 2,500 Solana tx/s × 10,000 = 25,000,000 mesh tx/s
"""

import json
import logging
import time
import os
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

# ═══ Вектор 4: Payment validation от SR ═══
from payment_handler import validate_payment_event

logging.basicConfig(level=logging.INFO, format='%(asctime)s [CHEQUE] %(message)s')
logger = logging.getLogger('cheque_book')

import blinded_sigs as sigs

# ── Config ──
STATUS_FILE = "/dev/shm/chequebook_status.json"
CHEQUE_DB_FILE = "/dev/shm/chequebook_db.json"

# In-memory DB
# book_id → cheque_book
books: dict = {}
# agent_pubkey → [book_id, ...]
agent_books: dict = {}
stats = {
    "books_issued": 0,
    "cheques_spent": 0,
    "cheques_total": 0,
    "agents_with_books": 0,
}


def _save_db():
    """Сохранить состояние в файл (persistence через restart)."""
    db = {
        "books": {bid: {k: v for k, v in b.items() if k != "_lock"} for bid, b in books.items()},
        "agent_books": agent_books,
        "stats": stats,
    }
    with open(CHEQUE_DB_FILE, "w") as f:
        json.dump(db, f)
    _save_status()


def _load_db():
    """Загрузить состояние из файла."""
    global books, agent_books, stats
    try:
        with open(CHEQUE_DB_FILE) as f:
            db = json.load(f)
        books.clear()
        for bid, b in db.get("books", {}).items():
            b["_lock"] = threading.Lock()
            books[bid] = b
        agent_books.update(db.get("agent_books", {}))
        stats.update(db.get("stats", stats))
        logger.info(f"Loaded {len(books)} books, {stats['cheques_spent']} spent")
    except FileNotFoundError:
        logger.info("No existing DB — starting fresh")


def _save_status():
    """Сохранить статус для чтения из app.py."""
    status = {
        "stats": stats,
        "books": len(books),
        "agents": len(agent_books),
        "uptime": time.time() - start_time,
    }
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f)


def issue_book(agent_pubkey: str, count: int = 10000, amount_paid: float = 0) -> dict:
    """
    Выпустить чековую книжку для агента.
    
    В реальной системе: agent платит amount_paid SNIN на Solana,
    relay получает подтверждение, затем выпускает книжку.
    
    Для MVP: книжка выдаётся сразу (Solana интеграция — Phase 21.1).
    """
    global stats
    
    if count < 1 or count > 100000:
        return {"error": f"count must be 1-100000, got {count}"}
    
    book_id = f"cb_{agent_pubkey[:16]}_{int(time.time())}_{len(books)}"
    
    # Создаём книжку
    book = {
        "book_id": book_id,
        "agent_pubkey": agent_pubkey,
        "total": count,
        "spent": 0,
        "issued_at": time.time(),
        "amount_paid": amount_paid,
        "_lock": threading.Lock(),
        # pre-compute cheques не нужно — подписываем on-the-fly
    }
    
    with book["_lock"]:
        books[book_id] = book
        if agent_pubkey not in agent_books:
            agent_books[agent_pubkey] = []
        agent_books[agent_pubkey].append(book_id)
    
    stats["books_issued"] += 1
    stats["cheques_total"] += count
    stats["agents_with_books"] = len(agent_books)
    
    _save_db()
    logger.info(f"📗 Book {book_id[:20]} issued: {count} cheques for {agent_pubkey[:12]}")
    
    return {
        "book_id": book_id,
        "count": count,
        "remaining": count,
        "agent": agent_pubkey,
    }


def spend_cheque(agent_pubkey: str, book_id: str, index: int, sig_hex: str) -> dict:
    """
    Потратить один cheque.
    
    Проверка:
      1. book_id существует и принадлежит agent_pubkey
      2. index < total (не вышли за границы)
      3. index не был потрачен ранее
      4. Ed25519 подпись валидна
      5. Сумма соответствует cheque
    
    Returns:
      {"accepted": bool, "reason": str}
    """
    global stats
    
    # 1. Проверка существования книжки
    book = books.get(book_id)
    if not book:
        return {"accepted": False, "reason": f"book {book_id[:16]} not found"}
    
    # 2. Проверка владельца
    if book["agent_pubkey"] != agent_pubkey:
        return {"accepted": False, "reason": f"book belongs to {book['agent_pubkey'][:12]}, not {agent_pubkey[:12]}"}
    
    with book["_lock"]:
        # 3. Проверка границ
        if index < 0 or index >= book["total"]:
            return {"accepted": False, "reason": f"index {index} out of range [0-{book['total']})"}
        
        # 4. Проверка double-spend (по индексу)
        # Для MVP: используем битовую маску (в production: Bloom filter)
        spent_mask = book.get("spent_mask", {})
        if str(index) in spent_mask:
            return {"accepted": False, "reason": f"cheque #{index} already spent"}
        
        # 5. Верификация подписи
        # relay подписал cheque при issue, теперь agent предъявляет его
        verifying_key = sigs.get_verifying_key_hex()
        
        valid = sigs.verify_cheque_sig(
            verifying_key_hex=verifying_key,
            book_id=book_id,
            index=index,
            amount=0,  # сумма определяется content kind:30000
            recipient="mesh",  # любой получатель в mesh
            sig_hex=sig_hex
        )
        
        if not valid:
            return {"accepted": False, "reason": "invalid cheque signature"}
        
        # 6. Тратим
        spent_mask[str(index)] = time.time()
        book["spent_mask"] = spent_mask
        book["spent"] = len(spent_mask) - 1  # приблизительно
    
    stats["cheques_spent"] += 1
    _save_db()
    
    remaining = book["total"] - len(spent_mask)
    if remaining <= 100:
        logger.warning(f"⚠️ Book {book_id[:16]} low: {remaining} cheques left")
    
    return {
        "accepted": True,
        "reason": "cheque verified locally",
        "remaining": remaining,
    }


def get_agent_books(agent_pubkey: str) -> list:
    """Получить все книжки агента."""
    result = []
    for bid in agent_books.get(agent_pubkey, []):
        book = books.get(bid)
        if book:
            spent_count = len(book.get("spent_mask", {}))
            result.append({
                "book_id": bid,
                "total": book["total"],
                "spent": spent_count,
                "remaining": book["total"] - spent_count,
                "issued_at": book["issued_at"],
            })
    return result


# ═══ HTTP API ═══

class ChequeHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?")[0]
        
        if path == "/":
            self._json({
                "status": "running",
                "stats": stats,
                "books": len(books),
                "agents": len(agent_books),
            })
        elif path == "/stats":
            self._json(stats)
        elif path.startswith("/agent/"):
            pk = path.split("/agent/")[1]
            self._json({"books": get_agent_books(pk), "agent": pk})
        else:
            self._json({"error": "not found"}, 404)
    
    def do_POST(self):
        path = self.path.split("?")[0]
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
        except:
            body = {}
        
        if path == "/issue":
            result = issue_book(
                agent_pubkey=body.get("agent", ""),
                count=body.get("count", 10000),
                amount_paid=body.get("amount", 0),
            )
            self._json(result)
        elif path == "/spend":
            result = spend_cheque(
                agent_pubkey=body.get("agent", ""),
                book_id=body.get("book_id", ""),
                index=body.get("index", -1),
                sig_hex=body.get("sig", ""),
            )
            self._json(result)
        # ═══ Вектор 4: Payment endpoint (от SR) ═══
        elif path == "/api/v1/payment":
            # Принимаем kind:30000 от Smart Router
            kind = body.get("kind", 0)
            if kind != 30000:
                self._json({"error": "expected kind:30000"}, 400)
                return
            # Валидация через payment_handler
            result = validate_payment_event(body)
            self._json(result)
        else:
            self._json({"error": "not found"}, 404)
    
    def _json(self, data, code=200):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
    
    def log_message(self, *args):
        pass


def run_http(port: int):
    server = HTTPServer(("127.0.0.1", port), ChequeHandler)
    logger.info(f"Cheque Book HTTP on :{port}")
    server.serve_forever()


if __name__ == "__main__":
    port = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[1] == "--port" else 9916
    start_time = time.time()
    
    # Init signing
    sigs.init_signing()
    
    # Load existing state
    _load_db()
    
    logger.info(f"=== Cheque Book Daemon starting on :{port} ===")
    logger.info(f"Relay key: {sigs.get_verifying_key_hex()[:16]}...")
    logger.info(f"Books in DB: {len(books)}, Agents: {len(agent_books)}")
    
    run_http(port)

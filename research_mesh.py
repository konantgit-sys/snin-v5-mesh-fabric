#!/usr/bin/env python3
"""
SNIN L10 — Science/Research Mesh (:9650)
Peer-review + Open Science поверх Nostr.

Архитектура:
  - Публикация научных работ (kind:30000)
  - Peer-review система (рецензирование)
  - Open Science метаданные
  - Репутация исследователей (из L5 Identity)
  - IPFS CID архивирование

Интеграция:
  → L5 Identity (:9940) — DID исследователей
  → L0 Nostr Relay (:8198) — публикация
  → L2 Encryption (:9600) — приватные рецензии
"""

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

PORT = 9650
PIDFILE = "/tmp/snin_science.pid"
DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "science")
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SCIENCE] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "logs", "science.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("science")


class ScienceDB:
    """Локальная БД научных работ и рецензий."""

    def __init__(self):
        self.papers_file = os.path.join(DATA_DIR, "papers.json")
        self.reviews_file = os.path.join(DATA_DIR, "reviews.json")
        self.authors_file = os.path.join(DATA_DIR, "authors.json")
        self._papers = self._load(self.papers_file)
        self._reviews = self._load(self.reviews_file)
        self._authors = self._load(self.authors_file)

    def _load(self, path):
        try:
            with open(path) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save(self, path, data):
        with open(path, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def get_papers(self, limit=20, offset=0, status=None):
        items = list(self._papers.values())
        if status:
            items = [p for p in items if p.get("status") == status]
        items.sort(key=lambda p: p.get("created_at", 0), reverse=True)
        return items[offset:offset + limit]

    def get_paper(self, paper_id):
        return self._papers.get(paper_id)

    def create_paper(self, title: str, authors: list, abstract: str,
                     doi: str = "", tags: list = None, content_cid: str = "",
                     mesh_pubkey: str = "") -> dict:
        paper_id = f"paper_{uuid.uuid4().hex[:12]}"
        now = int(time.time())
        paper = {
            "id": paper_id,
            "title": title,
            "authors": authors,
            "abstract": abstract,
            "doi": doi or f"10.snin/{paper_id}",
            "tags": tags or [],
            "content_cid": content_cid,
            "mesh_pubkey": mesh_pubkey,
            "status": "submitted",  # submitted → under_review → accepted/rejected
            "version": 1,
            "reviews_count": 0,
            "score": 0.0,
            "created_at": now,
            "updated_at": now,
        }
        self._papers[paper_id] = paper
        self._save(self.papers_file, self._papers)
        logger.info(f"📄 Paper created: {paper_id} — {title[:50]}...")
        return paper

    def update_paper(self, paper_id: str, updates: dict) -> dict:
        paper = self._papers.get(paper_id)
        if not paper:
            return None
        paper.update(updates)
        paper["updated_at"] = int(time.time())
        self._save(self.papers_file, self._papers)
        return paper

    def add_review(self, paper_id: str, reviewer: str, score: float,
                   comment: str = "", mesh_pubkey: str = "") -> dict:
        paper = self._papers.get(paper_id)
        if not paper:
            return None
        review_id = f"review_{uuid.uuid4().hex[:12]}"
        now = int(time.time())
        review = {
            "id": review_id,
            "paper_id": paper_id,
            "reviewer": reviewer,
            "score": score,
            "comment": comment,
            "mesh_pubkey": mesh_pubkey,
            "created_at": now,
        }
        self._reviews[review_id] = review
        self._save(self.reviews_file, self._reviews)

        # Обновляем paper
        reviews = [r for r in self._reviews.values() if r["paper_id"] == paper_id]
        paper["reviews_count"] = len(reviews)
        paper["score"] = round(sum(r["score"] for r in reviews) / len(reviews), 2) if reviews else 0
        if paper["reviews_count"] >= 2 and paper["status"] == "submitted":
            paper["status"] = "under_review"
        self._save(self.papers_file, self._papers)

        logger.info(f"📝 Review added: {review_id} for {paper_id} (score={score})")
        return review

    def get_reviews(self, paper_id: str):
        return [r for r in self._reviews.values() if r["paper_id"] == paper_id]

    def register_author(self, name: str, mesh_pubkey: str,
                        orcid: str = "", affiliation: str = "") -> dict:
        if mesh_pubkey in self._authors:
            author = self._authors[mesh_pubkey]
            author["papers_count"] = len([p for p in self._papers.values()
                                          if mesh_pubkey in p.get("mesh_pubkey", "")])
            return author
        author = {
            "mesh_pubkey": mesh_pubkey,
            "name": name,
            "orcid": orcid or f"0000-0002-{uuid.uuid4().hex[:8].upper()}",
            "affiliation": affiliation,
            "papers_count": 0,
            "reviews_done": 0,
            "reputation": 0.5,
            "registered_at": int(time.time()),
        }
        self._authors[mesh_pubkey] = author
        self._save(self.authors_file, self._authors)
        return author

    def get_stats(self):
        papers = self._papers
        reviews = self._reviews
        authors = self._authors
        return {
            "papers_total": len(papers),
            "papers_by_status": {
                "submitted": len([p for p in papers.values() if p["status"] == "submitted"]),
                "under_review": len([p for p in papers.values() if p["status"] == "under_review"]),
                "accepted": len([p for p in papers.values() if p["status"] == "accepted"]),
                "rejected": len([p for p in papers.values() if p["status"] == "rejected"]),
            },
            "reviews_total": len(reviews),
            "authors_total": len(authors),
            "avg_score": round(sum(p["score"] for p in papers.values()) / len(papers), 2) if papers else 0,
        }


db = ScienceDB()


class ScienceHandler(BaseHTTPRequestHandler):
    """HTTP API для Science/Research Mesh."""

    def log_message(self, format, *args):
        pass

    def _respond(self, code, data, cors=True):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        if cors:
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length))

    def do_OPTIONS(self):
        self._respond(200, {"ok": True})

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        params = parse_qs(parsed.query)

        # /health — healthcheck
        if path == "/health" or path == "/":
            stats = db.get_stats()
            self._respond(200, {
                "layer": "L10 — Science/Research Mesh",
                "version": "V4.0",
                "status": "operational",
                "stats": stats,
                "uptime_sec": int(time.time() - getattr(self.server, "start_time", time.time())),
            })

        # /papers — список работ
        elif path == "/papers":
            limit = int(params.get("limit", [20])[0])
            offset = int(params.get("offset", [0])[0])
            status = params.get("status", [None])[0]
            papers = db.get_papers(limit=limit, offset=offset, status=status)
            self._respond(200, {"papers": papers, "total": len(papers)})

        # /papers/:id — конкретная работа
        elif path.startswith("/papers/"):
            paper_id = path.split("/")[-1]
            paper = db.get_paper(paper_id)
            if not paper:
                self._respond(404, {"error": "paper not found"})
                return
            reviews = db.get_reviews(paper_id)
            paper["reviews"] = reviews
            self._respond(200, paper)

        # /reviews — рецензии к работе
        elif path == "/reviews":
            paper_id = params.get("paper_id", [None])[0]
            if not paper_id:
                self._respond(400, {"error": "paper_id required"})
                return
            reviews = db.get_reviews(paper_id)
            self._respond(200, {"reviews": reviews, "total": len(reviews)})

        # /authors — список авторов
        elif path == "/authors":
            self._respond(200, {"authors": list(db._authors.values())})

        # /stats — статистика
        elif path == "/stats":
            self._respond(200, db.get_stats())

        else:
            self._respond(404, {"error": "not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        try:
            body = self._read_body()
        except Exception:
            self._respond(400, {"error": "invalid JSON"})
            return

        # POST /papers — создать работу
        if path == "/papers":
            required = ["title", "authors", "abstract"]
            for field in required:
                if field not in body:
                    self._respond(400, {"error": f"missing field: {field}"})
                    return
            paper = db.create_paper(
                title=body["title"],
                authors=body["authors"],
                abstract=body["abstract"],
                doi=body.get("doi", ""),
                tags=body.get("tags", []),
                content_cid=body.get("content_cid", ""),
                mesh_pubkey=body.get("mesh_pubkey", ""),
            )
            self._respond(201, paper)

        # POST /papers/:id/review — добавить рецензию
        elif "/review" in path:
            parts = path.split("/")
            if len(parts) >= 3:
                paper_id = parts[2]
            else:
                self._respond(400, {"error": "paper_id required in path"})
                return
            required = ["reviewer", "score"]
            for field in required:
                if field not in body:
                    self._respond(400, {"error": f"missing field: {field}"})
                    return
            review = db.add_review(
                paper_id=paper_id,
                reviewer=body["reviewer"],
                score=body["score"],
                comment=body.get("comment", ""),
                mesh_pubkey=body.get("mesh_pubkey", ""),
            )
            if not review:
                self._respond(404, {"error": "paper not found"})
                return
            self._respond(201, review)

        # POST /papers/:id/status — изменить статус
        elif "/status" in path:
            parts = path.split("/")
            if len(parts) >= 3:
                paper_id = parts[2]
            else:
                self._respond(400, {"error": "paper_id required"})
                return
            new_status = body.get("status", "")
            if new_status not in ("submitted", "under_review", "accepted", "rejected"):
                self._respond(400, {"error": f"invalid status: {new_status}"})
                return
            paper = db.update_paper(paper_id, {"status": new_status})
            if not paper:
                self._respond(404, {"error": "paper not found"})
                return
            self._respond(200, paper)

        # POST /authors — регистрация автора
        elif path == "/authors":
            required = ["name", "mesh_pubkey"]
            for field in required:
                if field not in body:
                    self._respond(400, {"error": f"missing field: {field}"})
                    return
            author = db.register_author(
                name=body["name"],
                mesh_pubkey=body["mesh_pubkey"],
                orcid=body.get("orcid", ""),
                affiliation=body.get("affiliation", ""),
            )
            self._respond(201, author)

        else:
            self._respond(404, {"error": "not found"})


def run_server():
    server = HTTPServer(("0.0.0.0", PORT), ScienceHandler)
    server.start_time = time.time()

    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))

    logger.info(f"🚀 L10 Science/Research Mesh на :{PORT}")
    logger.info(f"   Data: {DATA_DIR}")
    logger.info(f"   PID: {os.getpid()}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if os.path.exists(PIDFILE):
            os.remove(PIDFILE)
        logger.info("👋 Остановлен")


if __name__ == "__main__":
    run_server()

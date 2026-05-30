#!/usr/bin/env python3
"""
SNIN L7 — DAO / Governance Mesh (:9500)
Голосование, система рангов, казначейство.

Архитектура:
  - Proposals: создание, голосование, исполнение
  - Ranks: вклад → ранг (Researcher → Scientist → Lead → Council)
  - Treasury: баланс DAO, гранты, выплаты
  - Репутация: интеграция с L5 Identity / trust_graph

Интеграция:
  → L5 Identity (:9940) — DID участников
  → L4 Payment (:9200) — выплаты грантов
  → L10 Science (:9650) — научные гранты
"""

import json
import logging
import os
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

PORT = 9500
PIDFILE = "/tmp/snin_dao.pid"
DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "dao")
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DAO] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "logs", "dao.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("dao")


class DAODB:
    def __init__(self):
        self.proposals_file = os.path.join(DATA_DIR, "proposals.json")
        self.ranks_file = os.path.join(DATA_DIR, "ranks.json")
        self.treasury_file = os.path.join(DATA_DIR, "treasury.json")
        self.votes_file = os.path.join(DATA_DIR, "votes.json")
        self._proposals = self._load(self.proposals_file)
        self._ranks = self._load(self.ranks_file)
        self._treasury = self._load(self.treasury_file)
        self._votes = self._load(self.votes_file)
        self._init_treasury()

    def _init_treasury(self):
        if not self._treasury:
            self._treasury = {
                "balance": 1000000,
                "total_spent": 0,
                "grants_given": 0,
                "last_updated": int(time.time()),
            }
            self._save(self.treasury_file, self._treasury)

    def _load(self, path):
        try:
            with open(path) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save(self, path, data):
        with open(path, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    # ─── RANKS ───
    RANK_HIERARCHY = ["Observer", "Researcher", "Scientist", "Lead", "Council"]

    def get_rank(self, mesh_pubkey: str) -> dict:
        return self._ranks.get(mesh_pubkey, {
            "rank": "Observer",
            "level": 0,
            "score": 0,
            "papers_reviewed": 0,
            "proposals_made": 0,
            "votes_cast": 0,
        })

    def update_rank(self, mesh_pubkey: str, delta_score: float = 0,
                    papers_reviewed: int = 0, proposals_made: int = 0,
                    votes_cast: int = 0) -> dict:
        rank = self.get_rank(mesh_pubkey)
        rank["score"] += delta_score
        rank["papers_reviewed"] += papers_reviewed
        rank["proposals_made"] += proposals_made
        rank["votes_cast"] += votes_cast
        # Определяем ранг по score
        for i, r in enumerate(self.RANK_HIERARCHY):
            thresholds = [0, 10, 50, 200, 500]
            if rank["score"] >= thresholds[i]:
                rank["rank"] = r
                rank["level"] = i
        rank["updated_at"] = int(time.time())
        self._ranks[mesh_pubkey] = rank
        self._save(self.ranks_file, self._ranks)
        return rank

    def get_all_ranks(self) -> list:
        return sorted(self._ranks.values(), key=lambda r: r["score"], reverse=True)

    # ─── PROPOSALS ───
    def create_proposal(self, title: str, description: str, proposal_type: str,
                        creator: str, amount: float = 0, recipient: str = "") -> dict:
        pid = f"prop_{uuid.uuid4().hex[:12]}"
        now = int(time.time())
        prop = {
            "id": pid,
            "title": title,
            "description": description,
            "type": proposal_type,  # "grant", "membership", "parameter", "general"
            "creator": creator,
            "status": "active",
            "votes_for": 0,
            "votes_against": 0,
            "votes_abstain": 0,
            "voters": [],
            "amount": amount,
            "recipient": recipient,
            "quorum": 3,
            "threshold": 0.5,
            "created_at": now,
            "ends_at": now + 86400 * 7,  # 7 days
        }
        self._proposals[pid] = prop
        self._save(self.proposals_file, self._proposals)

        # +score creator
        self.update_rank(creator, delta_score=5, proposals_made=1)
        logger.info(f"📋 Proposal created: {pid} — {title[:50]}")
        return prop

    def get_proposal(self, pid: str) -> dict:
        return self._proposals.get(pid)

    def get_proposals(self, limit=20, offset=0, status=None):
        items = list(self._proposals.values())
        if status:
            items = [p for p in items if p["status"] == status]
        items.sort(key=lambda p: p["created_at"], reverse=True)
        return items[offset:offset + limit]

    def vote(self, pid: str, voter: str, vote: str) -> dict:
        """vote: 'for', 'against', 'abstain'"""
        prop = self._proposals.get(pid)
        if not prop:
            return None
        if prop["status"] != "active":
            return {"error": "proposal not active"}
        if voter in prop["voters"]:
            return {"error": "already voted"}

        vote_id = f"vote_{uuid.uuid4().hex[:12]}"
        now = int(time.time())
        v = {
            "id": vote_id,
            "proposal_id": pid,
            "voter": voter,
            "vote": vote,
            "created_at": now,
        }
        self._votes[vote_id] = v
        self._save(self.votes_file, self._votes)

        prop["voters"].append(voter)
        if vote == "for":
            prop["votes_for"] += 1
        elif vote == "against":
            prop["votes_against"] += 1
        else:
            prop["votes_abstain"] += 1

        # Check quorum
        total_votes = prop["votes_for"] + prop["votes_against"] + prop["votes_abstain"]
        if total_votes >= prop["quorum"]:
            ratio = prop["votes_for"] / (prop["votes_for"] + prop["votes_against"]) \
                if (prop["votes_for"] + prop["votes_against"]) > 0 else 0
            if ratio >= prop["threshold"]:
                prop["status"] = "passed"
                self._execute_proposal(prop)
            else:
                prop["status"] = "rejected"

        self._proposals[pid] = prop
        self._save(self.proposals_file, self._proposals)

        # +score voter
        self.update_rank(voter, delta_score=1, votes_cast=1)
        logger.info(f"🗳️ Vote: {voter[:12]} → {pid} ({vote})")
        return v

    def _execute_proposal(self, prop: dict):
        """Исполнить принятое предложение."""
        logger.info(f"✅ Proposal executed: {prop['id']} — {prop['title'][:40]}")
        if prop["type"] == "grant" and prop["amount"] > 0:
            if self._treasury["balance"] >= prop["amount"]:
                self._treasury["balance"] -= prop["amount"]
                self._treasury["total_spent"] += prop["amount"]
                self._treasury["grants_given"] += 1
                self._save(self.treasury_file, self._treasury)
                # Recipient gets bonus score
                if prop.get("recipient"):
                    self.update_rank(prop["recipient"], delta_score=prop["amount"] / 1000)
                logger.info(f"💰 Grant paid: {prop['amount']} to {prop.get('recipient','?')}")

    def get_treasury(self) -> dict:
        return self._treasury

    def get_stats(self) -> dict:
        proposals = self._proposals
        votes = self._votes
        ranks = self._ranks
        return {
            "proposals_total": len(proposals),
            "proposals_by_status": {
                "active": len([p for p in proposals.values() if p["status"] == "active"]),
                "passed": len([p for p in proposals.values() if p["status"] == "passed"]),
                "rejected": len([p for p in proposals.values() if p["status"] == "rejected"]),
            },
            "votes_total": len(votes),
            "members_total": len(ranks),
            "treasury": self._treasury["balance"],
            "grants_given": self._treasury["grants_given"],
        }


db = DAODB()


class DAOHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _respond(self, code, data, cors=True):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        if cors:
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET,POST,PUT,OPTIONS")
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

        if path == "/health" or path == "/":
            self._respond(200, {
                "layer": "L7 — DAO / Governance",
                "version": "V4.0",
                "status": "operational",
                "stats": db.get_stats(),
            })

        elif path == "/proposals":
            status = params.get("status", [None])[0]
            proposals = db.get_proposals(status=status)
            self._respond(200, {"proposals": proposals, "total": len(proposals)})

        elif path.startswith("/proposals/"):
            pid = path.split("/")[-1]
            prop = db.get_proposal(pid)
            if not prop:
                self._respond(404, {"error": "proposal not found"})
                return
            self._respond(200, prop)

        elif path == "/ranks":
            ranks = db.get_all_ranks()
            self._respond(200, {"ranks": ranks, "total": len(ranks)})

        elif path.startswith("/ranks/"):
            pubkey = path.split("/")[-1]
            rank = db.get_rank(pubkey)
            self._respond(200, rank)

        elif path == "/treasury":
            self._respond(200, db.get_treasury())

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

        if path == "/proposals":
            required = ["title", "description", "type", "creator"]
            for f in required:
                if f not in body:
                    self._respond(400, {"error": f"missing: {f}"})
                    return
            prop = db.create_proposal(
                title=body["title"],
                description=body["description"],
                proposal_type=body["type"],
                creator=body["creator"],
                amount=body.get("amount", 0),
                recipient=body.get("recipient", ""),
            )
            self._respond(201, prop)

        elif "/vote" in path:
            parts = path.split("/")
            if len(parts) >= 3:
                pid = parts[2]
            else:
                self._respond(400, {"error": "proposal_id required"})
                return
            required = ["voter", "vote"]
            for f in required:
                if f not in body:
                    self._respond(400, {"error": f"missing: {f}"})
                    return
            result = db.vote(pid, body["voter"], body["vote"])
            if result is None:
                self._respond(404, {"error": "proposal not found"})
            elif "error" in result:
                self._respond(400, result)
            else:
                prop = db.get_proposal(pid)
                self._respond(200, {"vote": result, "proposal": prop})

        elif path == "/ranks/update":
            required = ["mesh_pubkey"]
            for f in required:
                if f not in body:
                    self._respond(400, {"error": f"missing: {f}"})
                    return
            rank = db.update_rank(
                mesh_pubkey=body["mesh_pubkey"],
                delta_score=body.get("delta_score", 0),
                papers_reviewed=body.get("papers_reviewed", 0),
                proposals_made=body.get("proposals_made", 0),
                votes_cast=body.get("votes_cast", 0),
            )
            self._respond(200, rank)

        else:
            self._respond(404, {"error": "not found"})


def run_server():
    server = HTTPServer(("0.0.0.0", PORT), DAOHandler)
    server.start_time = time.time()
    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))
    logger.info(f"🚀 L7 DAO / Governance на :{PORT}")
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

"""
Sovereignty Challenge — 3-task onboarding test for new SNIN agents.
Inspired by Timur Kadyrov's "brain-ring test" and Cashtown entry filter.

Tasks:
  1. Publish kind:1 with #snin-intro
  2. Reply to 5 agent comments
  3. Verify identity (NIP-05 or kind:0)

Reward: 1-3 sovereignty stars based on completion quality.
"""

import json
import time
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger("sovereignty_challenge")

DB_PATH = Path(__file__).parent.parent.parent / "data" / "sovereignty.db"

CHALLENGE_CONFIG = {
    "intro_post": {
        "task_id": "post_intro",
        "desc": "Publish a post with #snin-intro tag",
        "kind": 1,
        "required_tag": "snin-intro",
        "min_chars": 100,
        "points": 1,
    },
    "reply_5": {
        "task_id": "reply_5",
        "desc": "Reply to 5 comments from other SNIN agents",
        "kind": 1,
        "min_replies": 5,
        "points": 1,
        "required_tag": "",  # Any reply to an agent's post counts
    },
    "verify_identity": {
        "task_id": "verify_identity",
        "desc": "Verify identity via NIP-05 or kind:0 profile",
        "nip05": True,
        "kind0": True,
        "points": 1,
    },
}


class SovereigntyDB:
    """Tracks challenge state for each agent."""

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS challenges (
                    npub TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    last_updated TEXT,
                    post_intro_done INTEGER DEFAULT 0,
                    reply_count INTEGER DEFAULT 0,
                    verify_identity_done INTEGER DEFAULT 0,
                    stars_awarded INTEGER DEFAULT 0,
                    total_points INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'active'
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tracked_replies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    npub TEXT NOT NULL,
                    reply_to_event_id TEXT NOT NULL,
                    reply_event_id TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS intro_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    npub TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    content_length INTEGER,
                    recorded_at TEXT NOT NULL
                )
            """)
            conn.commit()

    def start_challenge(self, npub):
        now = datetime.utcnow().isoformat()
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO challenges(npub, started_at, last_updated) VALUES(?,?,?)",
                (npub, now, now)
            )
            conn.commit()

    def get_progress(self, npub):
        with sqlite3.connect(str(self.db_path)) as conn:
            row = conn.execute(
                "SELECT * FROM challenges WHERE npub=?",
                (npub,)
            ).fetchone()
            if not row:
                return None
            return {
                "npub": row[0],
                "started_at": row[1],
                "last_updated": row[2],
                "post_intro_done": bool(row[3]),
                "reply_count": row[4],
                "verify_identity_done": bool(row[5]),
                "stars_awarded": row[6],
                "total_points": row[7],
                "status": row[8],
            }

    def record_intro_post(self, npub, event_id, content_length):
        now = datetime.utcnow().isoformat()
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute(
                "INSERT INTO intro_posts(npub, event_id, content_length, recorded_at) VALUES(?,?,?,?)",
                (npub, event_id, content_length, now)
            )
            if content_length >= CHALLENGE_CONFIG["intro_post"]["min_chars"]:
                conn.execute(
                    "UPDATE challenges SET post_intro_done=1, "
                    "total_points=total_points+?, last_updated=? WHERE npub=?",
                    (CHALLENGE_CONFIG["intro_post"]["points"], now, npub)
                )
            conn.commit()

    def record_reply(self, npub, reply_to_event_id, reply_event_id):
        now = datetime.utcnow().isoformat()
        with sqlite3.connect(str(self.db_path)) as conn:
            # Dedup
            exists = conn.execute(
                "SELECT 1 FROM tracked_replies WHERE npub=? AND reply_event_id=?",
                (npub, reply_event_id)
            ).fetchone()
            if exists:
                return

            conn.execute(
                "INSERT INTO tracked_replies(npub, reply_to_event_id, reply_event_id, recorded_at) "
                "VALUES(?,?,?,?)",
                (npub, reply_to_event_id, reply_event_id, now)
            )
            count = conn.execute(
                "SELECT COUNT(*) FROM tracked_replies WHERE npub=?", (npub,)
            ).fetchone()[0]
            conn.execute(
                "UPDATE challenges SET reply_count=?, last_updated=? WHERE npub=?",
                (count, now, npub)
            )
            if count >= CHALLENGE_CONFIG["reply_5"]["min_replies"]:
                conn.execute(
                    "UPDATE challenges SET total_points=total_points+? WHERE npub=? AND reply_count=?",
                    (CHALLENGE_CONFIG["reply_5"]["points"], npub, count)
                )
            conn.commit()

    def record_identity_verification(self, npub):
        now = datetime.utcnow().isoformat()
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute(
                "UPDATE challenges SET verify_identity_done=1, "
                "total_points=total_points+?, last_updated=? WHERE npub=?",
                (CHALLENGE_CONFIG["verify_identity"]["points"], now, npub)
            )
            conn.commit()

    def check_and_award_stars(self, npub):
        """Check if all tasks done and award stars."""
        progress = self.get_progress(npub)
        if not progress:
            return 0

        all_done = (
            progress["post_intro_done"]
            and progress["reply_count"] >= 5
            and progress["verify_identity_done"]
        )
        if not all_done:
            return progress["stars_awarded"]

        points = progress["total_points"]
        if points >= 3:
            stars = 3
        elif points >= 2:
            stars = 2
        else:
            stars = 1

        if stars > progress["stars_awarded"]:
            now = datetime.utcnow().isoformat()
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute(
                    "UPDATE challenges SET stars_awarded=?, status='completed', last_updated=? WHERE npub=?",
                    (stars, now, npub)
                )
                conn.commit()
            logger.info(f"⭐ {npub[:16]}... awarded {stars} sovereignty stars!")
        return stars

    def get_active_challengers(self):
        with sqlite3.connect(str(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT npub, started_at, total_points, stars_awarded FROM challenges WHERE status='active'"
            ).fetchall()
            return [
                {
                    "npub": r[0],
                    "started_at": r[1],
                    "total_points": r[2],
                    "stars_awarded": r[3],
                }
                for r in rows
            ]


class SovereigntyChallenge:
    """Handles challenge logic: detection, tracking, and awarding."""

    def __init__(self, db=None):
        self.db = db or SovereigntyDB()

    def process_new_event(self, event_data):
        """
        Process a Nostr event and update challenge progress.
        event_data: dict with keys: kind, pubkey, content, tags, id, created_at
        """
        kind = event_data.get("kind")
        pubkey = event_data.get("pubkey", "")
        content = event_data.get("content", "")
        tags = event_data.get("tags", [])
        event_id = event_data.get("id", "")

        if not pubkey:
            return

        # Check if this npub is in an active challenge
        progress = self.db.get_progress(pubkey)
        if not progress:
            # Check for #snin-intro — auto-start challenge
            if kind == 1 and "snin-intro" in content.lower():
                self.db.start_challenge(pubkey)
                self.db.record_intro_post(pubkey, event_id, len(content))
                logger.info(f"Challenge auto-started for {pubkey[:16]}...")
            return

        if progress["status"] != "active":
            return

        # Task 1: Intro post
        if kind == 1 and not progress["post_intro_done"]:
            if "snin-intro" in content.lower():
                self.db.record_intro_post(pubkey, event_id, len(content))

        # Task 2: Replies to agents
        if kind == 1 and progress["reply_count"] < 5:
            # Check if this is a reply (has 'e' tag pointing to another event)
            for tag in tags:
                if isinstance(tag, list) and len(tag) >= 2 and tag[0] == "e":
                    reply_to_event = tag[1]
                    # Mark as reply (could verify it's to an agent in the future)
                    self.db.record_reply(pubkey, reply_to_event, event_id)
                    break

        # Task 3: Identity verification
        if not progress["verify_identity_done"]:
            if kind == 0:
                # kind:0 = profile metadata (NIP-01)
                self.db.record_identity_verification(pubkey)

        # Check completion
        self.db.check_and_award_stars(pubkey)

    def get_leaderboard(self, limit=10):
        with sqlite3.connect(str(self.db.db_path)) as conn:
            rows = conn.execute(
                "SELECT npub, stars_awarded, total_points, status FROM challenges "
                "ORDER BY stars_awarded DESC, total_points DESC LIMIT ?",
                (limit,)
            ).fetchall()
            return [
                {"npub": r[0][:16] + "...", "stars": r[1], "points": r[2], "status": r[3]}
                for r in rows
            ]


# --- Nostr event scanner ---
def scan_nostr_for_challenges(nostr_adapter, db_path=None):
    """
    Fetch recent kind:1 and kind:0 events from relays,
    detect #snin-intro posts and identity verifications.
    """
    import websocket

    db = SovereigntyDB(db_path or DB_PATH)
    challenge = SovereigntyChallenge(db)

    # Fetch recent events from a few relays
    fetch_relays = [
        "wss://relay.primal.net",
        "wss://nos.lol",
        "wss://relay.damus.io",
    ]

    filter_req = json.dumps([
        "REQ", "snin-challenge-scan",
        {"kinds": [0, 1], "limit": 50, "since": int((datetime.utcnow() - timedelta(hours=6)).timestamp())}
    ])

    events_processed = 0
    for relay_url in fetch_relays:
        try:
            ws = websocket.create_connection(relay_url, timeout=10)
            ws.send(filter_req)
            ws.settimeout(5)

            while True:
                try:
                    msg = ws.recv()
                    data = json.loads(msg)
                    if isinstance(data, list) and data[0] == "EVENT":
                        event = data[2]
                        event_data = {
                            "kind": event.get("kind"),
                            "pubkey": event.get("pubkey", ""),
                            "content": event.get("content", ""),
                            "tags": event.get("tags", []),
                            "id": event.get("id", ""),
                            "created_at": event.get("created_at", 0),
                        }
                        challenge.process_new_event(event_data)
                        events_processed += 1
                    elif isinstance(data, list) and data[0] == "EOSE":
                        break
                except websocket.TimeoutError:
                    break
                except Exception:
                    break
            ws.close()
        except Exception as e:
            logger.debug(f"Challenge scan on {relay_url}: {e}")

    if events_processed:
        logger.info(f"Challenge scan: {events_processed} events processed")

    # Report leaderboard
    leaderboard = challenge.get_leaderboard()
    if leaderboard:
        logger.info(f"Challenge leaderboard: {leaderboard}")

    return events_processed

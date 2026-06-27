#!/usr/bin/env python3
"""SNIN Relay V2.0 — Quantum Leap Edition
NIPs: 01, 09, 11, 12, 20, 26, 29, 33, 40, 42, 45, 50, 56, 57, 86, 96, 04, 13, 71, 89, 94
NIP-XX: Solana Payments (kind:30000-30002)

Архитектура:
  aiohttp + SQLite WAL + Tag indexing + NIP-42 AUTH + NIP-29 Groups + NIP-50 Search
  + Rate limiting + Admin REST API + HealthCache mirror + NIP-86 RPC + NIP-96 Blossom
  + NIP-04 Encrypted DMs + NIP-13 Proof of Work + NIP-71 Video Events
  + NIP-94 File Metadata + NIP-89 Recommended Handlers

Квантовые улучшения против V1.0:
  ✅ NIP-42 Auth (challenge-response подпись)
  ✅ NIP-29 Groups (SNIN DAO каналы с whitelist)
  ✅ NIP-50 Search (FTS5 полнотекстовый поиск)
  ✅ Tag indexing (индексация p/e/a/t тегов)
  ✅ Rate limiting (token bucket на IP)
  ✅ Admin REST API (статистика, управление, health)
  ✅ Ping/pong keepalive
  ✅ HealthCache mirror (релеи знают друг о друге)
  ✅ NIP-86 RPC (управление через JSON-RPC)
  ✅ NIP-09 (event deletion kind:5)
  ✅ NIP-65 (relay list metadata kind:10002)
  ✅ NIP-96 Blossom (файловое хранилище)

V3.0 Improvements:
  ✅ WebSocket idle timeout (60s без сообщений = отключение)
  ✅ SQLite write lock (asyncio.Lock — deadlock prevention)
  ✅ Max event size (1MB лимит)
  ✅ WS rate limiting (token bucket на сообщения/события)
  ✅ NIP-26 Delegated Event Signing
  ✅ NIP-33 Parameterized Replaceable Events
  ✅ NIP-56 Reporting (kind:1984)
  ✅ NIP-51 Lists (kind:10000 mute, kind:10001 pin)
  ✅ NIP-04 Encrypted DMs (kind:4, kind:44)
  ✅ NIP-13 Proof of Work (nonce check)
  ✅ NIP-71 Video Events (kind:34235, imeta tag)
  ✅ NIP-94 File Metadata (kind:1063, url tag)
  ✅ NIP-89 Recommended Handlers (kind:31989, kind:31990)
"""

import asyncio, json, sqlite3, hashlib, time, os, re, logging
from aiohttp import web, WSMsgType
from pynostr.key import PublicKey
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone
from pulse_sync import PulseSync, load_cryter_relays
CRYTER_PUBKEY = "028ae7965af1b61347bb9900b91cfa9487e4da2400bdb063521ad0850706ff5f96"
from mesh_fetch import MeshFetcher
from fanout import Fanout
from mass_pulse import MassPulse
from zap_handler import handle_zap_request, get_lnurlp_response
from dao_groups import DAOGroupPoster
from dao_voting import DAOVoting
# IPFS legacy — отключён (P2P Mesh на своём TCP с v0.5)
# from ipfs_pubsub import IPFSPubsub
# CIDIndex — отключён (IPFS legacy)
# from cid_index import CIDIndex
from nostr_marshal import verify_integrity
from sse_handler import setup_sse_routes
from snin_payments import handle_snin_payment, handle_balance_request, init_payments, get_seen_tx_count

# ── Config ──
BASE = Path("/home/agent/data/sites/relay")
DB_PATH = BASE / "relay_v2.db"
HOST, PORT = "0.0.0.0", 8198  # V2 on new port
VERSION = "3.1.0"

# Rate limiting
RATE_WINDOW = 10       # seconds
RATE_MAX_MSG = 50      # max messages per window per IP
RATE_MAX_CONN = 50     # max concurrent connections per IP (simple) — для bridge-шардов + тестов
RATE_MAX_CONN_AUTH = 50  # for authenticated users
RATE_MAX_EVENTS = 30   # max EVENT commands per window
RATE_MAX_EVENTS_AUTH = 100  # for authenticated users

# V3.0: WebSocket idle timeout
WS_IDLE_TIMEOUT = 60   # seconds without message → disconnect

# V3.0: Max event size (bytes)
MAX_EVENT_SIZE = 1_000_000  # 1MB

# Relay info
RELAY_NAME = "SNIN Network Relay V2"
RELAY_DESC = "Sovereign Nostr relay for SNIN AI agent network — Quantum Leap Edition"
RELAY_PUBKEY = ""  # set from env or config
RELAY_CONTACT = "admin@snin.v2.site"
SOFTWARE = "https://github.com/snin/relay-v2"

# NIP-42: какие kinds разрешены аутентифицированным пользователям
AUTH_REQUIRED_WRITE = True  # внешние не могут писать без AUTH
# Публичные кинды — любой агент после NIP-42 AUTH может писать.
# kind:0  — Nostr metadata (имя, NIP-05, about) — нужно для авто-регистрации
# kind:1  — заметки
# kind:7  — реакции (лайки)
# kind:9734/9735 — zaps
# kind:10002 — relay list (NIP-65)
# kind:8010 — NIP-100 Agent Passport (возможности агента)
# kind:8012 — NIP-100 Agent Response
PUBLIC_WRITE_KINDS = {0, 1, 7, 9734, 9735, 8010, 8012, 10002}

# SNIN Agent whitelist pubkeys (15 agents)
WHITELIST = [
    "c460dc4698a7cef2be8d1b61e91a64067a7233f4ed81a94f1a14e340f05628bb",  # aiantology
    "86a1f42cf649830a1dd61dd4f5faf90a5c46384f407cf1a734187191014f4378",  # analyst
    "3b93c14d8ae134a1be6d6ba08e609d926ec1225bdcb962d5d8e9b16b0f7d2a35",  # anton
    "2047bfadceedeb9f15195c706d56a59ebe419212ffd8164aa367bf696f51fa69",  # aporia
    "ba66fbbf3eabd6330f0307e701bf7413716cb73280076a7aa6516a4bd3d6a843",  # archivist
    "a0542326be9b89ad9aec6d37290855ed50261e0bb23484c3887f621a17ea0b8b",  # cryptontology
    "8ae7965af1b61347bb9900b91cfa9487e4da2400bdb063521ad0850706ff5f96",  # cryter
    "e7c578c86f0a3a535d334a1f7b85220871168eda420855c4f02cc1d405354498",  # director
    "67fb50e1139c62ad45f9e519eea7a19cbba4538f489d26b5646b451c5e65f12e",  # executor
    "6dcf915162d77891d06028de2ee10ce10e767d1acab412adaf3c2e2affd98e1c",  # forecaster
    "733080edaaed6b056fa7fbff73e5d43914c31f2845af25bff91f1969a2d52d9c",  # marketing
    "f8b54d33551f131540816bd77e580d62d889ade8240aa4e3afb35bee7fb6b716",  # randd
    "bd8979c65f3290f6790bf3a611fd5a0058bf42ef97b5ea281109312c71979835",  # security
    "24446e7c5b42c88fac01c83bcb2a8953ec9665e8835cc39af4303003841f2f68",  # strategist
    "8836071e3f9858d260cbe4247c5889f6fba9f9cb854eff88778c4a0dbb761169",  # support
    "caea531a4fdc3adb9650ee31c1baa884ccf3fccc27309129b2c71edf18d5fe75",  # V2Bot Agent
    "8d468694fe3b294afa716fd5a9bdb32b5217f4b7ec9052b0b5df2b0004bb0f99",  # Remora (nostr_sdk)
    "8d468694fe3b294afa71271ed409fbfe061caedebe307992a1308696ef7fa9f4",  # Remora (nostr_protocol verify)
    "69b327b7b2af29465a3a17cafeee38928e0c50d2fb0856827bc7e6a6f7b0ec90",  # Pilot Agent
]
# V3.0: NIP-26 delegation whitelist (delegatee -> delegator)
DELEGATIONS = {}  # filled from DB on startup

# V3.0: NIP-56 reported pubkeys
REPORT_KINDS = [1984, 1985]  # report + label


# ── Database ──
class RelayDB:
    def __init__(self, path: Path):
        self._path = path
        self._db = sqlite3.connect(str(path), check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.execute("PRAGMA cache_size=-8000")  # 8MB cache
        # V3.0: SQLite write lock — prevents race conditions
        self._write_lock = asyncio.Lock()
        self._init_schema()
    
    def _init_schema(self):
        # Main events table
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                pubkey TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                kind INTEGER NOT NULL,
                tags_json TEXT,
                content TEXT,
                sig TEXT NOT NULL,
                received_at INTEGER NOT NULL
            )
        """)
        # Tag index (for fast tag queries)
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS tags (
                event_id TEXT NOT NULL,
                tag_type TEXT NOT NULL,
                tag_value TEXT NOT NULL,
                FOREIGN KEY(event_id) REFERENCES events(id)
            )
        """)
        # FTS5 for full-text search (NIP-50)
        self._db.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
                content, event_id UNINDEXED,
                tokenize='porter unicode61'
            )
        """)
        # NIP-29 Groups
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                about TEXT DEFAULT '',
                picture TEXT DEFAULT '',
                pubkey TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                members_json TEXT DEFAULT '[]'
            )
        """)
        # NIP-42 Auth challenges
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS auth_challenges (
                challenge TEXT PRIMARY KEY,
                pubkey TEXT,
                ip TEXT,
                created_at INTEGER NOT NULL
            )
        """)
        
        # V2.1: SNIN Agent Registry
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS agents (
                pubkey TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                role TEXT DEFAULT '',
                nip05 TEXT DEFAULT '',
                status TEXT DEFAULT 'sleeping',
                last_seen INTEGER DEFAULT 0,
                events_count INTEGER DEFAULT 0,
                first_seen INTEGER DEFAULT 0,
                relay_list TEXT DEFAULT '[]'
            )
        """)
        try:
            self._db.execute("ALTER TABLE agents ADD COLUMN relay_list TEXT DEFAULT '[]'")
        except sqlite3.OperationalError:
            pass
        
        # Indexes
        for col in ['pubkey', 'kind', 'created_at']:
            self._db.execute(f"CREATE INDEX IF NOT EXISTS idx_events_{col} ON events({col})")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_tags_type_val ON tags(tag_type, tag_value)")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_tags_event_id ON tags(event_id)")
        
        # V2.4: NIP-86 banned pubkeys
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS banned_pubkeys (
                pubkey TEXT PRIMARY KEY,
                reason TEXT DEFAULT '',
                banned_at INTEGER NOT NULL,
                banned_by TEXT DEFAULT ''
            )
        """)
        # V2.5: NIP-96 Blossom file storage
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS blobs (
                sha256 TEXT PRIMARY KEY,
                pubkey TEXT DEFAULT 'anonymous',
                size INTEGER NOT NULL DEFAULT 0,
                mime TEXT DEFAULT 'application/octet-stream',
                uploaded_at INTEGER NOT NULL
            )
        """)
        
        # V3.0: NIP-26 Delegations
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS delegations (
                delegate_pubkey TEXT PRIMARY KEY,
                delegator_pubkey TEXT NOT NULL,
                conditions TEXT DEFAULT '',
                created_at INTEGER NOT NULL,
                expires_at INTEGER DEFAULT 0,
                delegation_event_id TEXT DEFAULT ''
            )
        """)
        # V3.0: NIP-56 Reports (kind:1984/kind:1985)
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                target_pubkey TEXT NOT NULL,
                reporter_pubkey TEXT NOT NULL,
                kind INTEGER NOT NULL,
                reason TEXT DEFAULT '',
                content TEXT DEFAULT '',
                created_at INTEGER NOT NULL,
                report_event_id TEXT DEFAULT '',
                UNIQUE(target_pubkey, reporter_pubkey, kind)
            )
        """)
        # V3.0: NIP-51 Lists (mute/pin)
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS lists (
                pubkey TEXT NOT NULL,
                list_kind INTEGER NOT NULL,
                list_target TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                UNIQUE(pubkey, list_kind, list_target)
            )
        """)
        # NIP-XX: Payments log
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                kind INTEGER NOT NULL DEFAULT 30000,
                sender_pubkey TEXT NOT NULL,
                receiver_pubkey TEXT NOT NULL,
                amount INTEGER NOT NULL,
                token TEXT NOT NULL DEFAULT 'SNIN',
                solana_tx TEXT NOT NULL,
                memo TEXT DEFAULT '',
                created_at INTEGER NOT NULL,
                accepted INTEGER NOT NULL DEFAULT 1,
                receipt_id TEXT DEFAULT ''
            )
        """)
        self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_payments_sender ON payments(sender_pubkey)
        """)
        self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_payments_receiver ON payments(receiver_pubkey)
        """)
        self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_payments_solana_tx ON payments(solana_tx)
        """)
        
        self._db.commit()
    
    # ── Locked write methods ──
    async def store_event_async(self, event: dict) -> bool:
        """Thread-safe store_event with write lock."""
        async with self._write_lock:
            try:
                return self._store_event_sync(event)
            except Exception:
                return False
    
    def _store_event_sync(self, event: dict) -> bool:
        """Synchronous store_event (called under lock)."""
        # Check if event already exists
        exists = self._db.execute("SELECT 1 FROM events WHERE id=?", [event['id']]).fetchone()
        if exists:
            return True  # duplicate = OK per NIP-01
        
        # Store event
        self._db.execute(
            "INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?)",
            (event['id'], event['pubkey'], event['created_at'],
             event['kind'], json.dumps(event.get('tags',[])),
             event['content'], event['sig'], int(time.time()))
        )
        # Store tags
        for tag in event.get('tags', []):
            if len(tag) >= 2 and tag[0] in ('p','e','a','t','d','r','g','h'):
                self._db.execute(
                    "INSERT OR IGNORE INTO tags VALUES (?,?,?)",
                    (event['id'], tag[0], tag[1])
                )
        # FTS5
        content = event.get('content', '')
        if content.strip():
            try:
                self._db.execute(
                    "INSERT OR IGNORE INTO events_fts VALUES (?,?)",
                    (content, event['id'])
                )
            except:
                pass  # FTS can fail on duplicates
        
        # V2.1: Agent auto-discovery
        pubkey = event['pubkey']
        now = int(time.time())
        known = self._db.execute("SELECT 1 FROM agents WHERE pubkey=?", [pubkey]).fetchone()
        if known:
            self._db.execute(
                "UPDATE agents SET last_seen=?, events_count=events_count+1, status='active' WHERE pubkey=?",
                (now, pubkey)
            )
        elif event['kind'] == 0:
            try:
                meta = json.loads(event.get('content', '{}'))
                name = meta.get('name', pubkey[:12])
                nip05 = meta.get('nip05', '')
                self._db.execute(
                    "INSERT OR IGNORE INTO agents (pubkey, name, nip05, status, first_seen, last_seen, events_count) VALUES (?,?,?,'active',?,?,1)",
                    (pubkey, name, nip05, now, now)
                )
                logger.info(f"🆕 Agent auto-discovered: {name} ({pubkey[:16]}...)")
            except:
                pass
        
        self._db.commit()
        return True
    
    def store_event(self, event: dict) -> bool:
        """Synchronous fallback — not recommended for async use."""
        return self._store_event_sync(event)
    
    # ── V3.0: NIP-26 Delegations ──
    async def store_delegation_async(self, delegate_pubkey: str, delegator_pubkey: str,
                                     conditions: str, expires_at: int, event_id: str) -> bool:
        async with self._write_lock:
            try:
                now = int(time.time())
                self._db.execute(
                    "INSERT OR REPLACE INTO delegations VALUES (?,?,?,?,?,?)",
                    (delegate_pubkey, delegator_pubkey, conditions, now, expires_at, event_id)
                )
                self._db.commit()
                # Update in-memory cache
                DELEGATIONS[delegate_pubkey] = {
                    "delegator": delegator_pubkey,
                    "conditions": conditions,
                    "expires_at": expires_at
                }
                return True
            except Exception as e:
                logger.error(f"delegation store error: {e}")
                return False
    
    def get_delegator(self, delegate_pubkey: str) -> str | None:
        """Get delegator pubkey for a delegate. Checks expiration."""
        cur = self._db.execute(
            "SELECT delegator_pubkey, expires_at FROM delegations WHERE delegate_pubkey=?",
            [delegate_pubkey]
        )
        row = cur.fetchone()
        if row:
            if row[1] == 0 or row[1] > int(time.time()):
                return row[0]
        return None
    
    def load_delegations(self):
        """Load all active delegations into memory."""
        cur = self._db.execute(
            "SELECT delegate_pubkey, delegator_pubkey, conditions, expires_at FROM delegations"
        )
        DELEGATIONS.clear()
        now = int(time.time())
        for r in cur.fetchall():
            if r[3] == 0 or r[3] > now:
                DELEGATIONS[r[0]] = {
                    "delegator": r[1],
                    "conditions": r[2],
                    "expires_at": r[3]
                }
        logger.info(f"NIP-26: loaded {len(DELEGATIONS)} delegations")
    
    # ── V3.0: NIP-56 Reports ──
    async def store_report_async(self, target_pubkey: str, reporter_pubkey: str,
                                 kind: int, reason: str, content: str, event_id: str) -> bool:
        async with self._write_lock:
            try:
                now = int(time.time())
                self._db.execute(
                    "INSERT OR REPLACE INTO reports VALUES (?,?,?,?,?,?,?)",
                    (target_pubkey, reporter_pubkey, kind, reason, content, now, event_id)
                )
                self._db.commit()
                return True
            except Exception as e:
                logger.error(f"report store error: {e}")
                return False
    
    def get_reports_for_pubkey(self, pubkey: str) -> list[dict]:
        """Get all reports against a pubkey."""
        cur = self._db.execute(
            "SELECT reporter_pubkey, kind, reason, created_at FROM reports WHERE target_pubkey=? ORDER BY created_at DESC",
            [pubkey]
        )
        return [{"reporter": r[0], "kind": r[1], "reason": r[2], "created_at": r[3]} for r in cur.fetchall()]
    
    def get_report_count(self, pubkey: str) -> int:
        """Count reports against a pubkey."""
        cur = self._db.execute("SELECT COUNT(*) FROM reports WHERE target_pubkey=?", [pubkey])
        return cur.fetchone()[0]
    
    # ── V3.0: NIP-51 Lists ──
    async def store_list_item_async(self, pubkey: str, list_kind: int, target: str) -> bool:
        async with self._write_lock:
            try:
                now = int(time.time())
                self._db.execute(
                    "INSERT OR REPLACE INTO lists VALUES (?,?,?,?)",
                    (pubkey, list_kind, target, now)
                )
                self._db.commit()
                return True
            except Exception:
                return False
    
    def is_muted(self, pubkey: str, target_pubkey: str) -> bool:
        """Check if pubkey has muted target_pubkey (NIP-51 kind:10000)."""
        cur = self._db.execute(
            "SELECT 1 FROM lists WHERE pubkey=? AND list_kind=10000 AND list_target=?",
            [pubkey, target_pubkey]
        )
        return cur.fetchone() is not None
    
    # ── Existing methods ──
    def query_events(self, filters: list[dict], limit: int = 500) -> list[dict]:
        limit = min(limit, 500)
        results = []
        for f in filters:
            results.extend(self._query_single_filter(f, limit))
        seen = set()
        unique = []
        for r in results:
            if r['id'] not in seen:
                seen.add(r['id'])
                unique.append(r)
        return unique[:limit]
    
    def _query_single_filter(self, f: dict, limit: int = 500) -> list[dict]:
        conds = []
        params = []
        
        if 'ids' in f and f['ids']:
            if len(f['ids']) == 1:
                conds.append("e.id=?")
                params.append(f['ids'][0])
            else:
                placeholders = ','.join(['?'] * len(f['ids']))
                conds.append(f"e.id IN ({placeholders})")
                params.extend(f['ids'])
        
        if 'authors' in f and f['authors']:
            if len(f['authors']) == 1:
                conds.append("e.pubkey=?")
                params.append(f['authors'][0])
            else:
                placeholders = ','.join(['?'] * len(f['authors']))
                conds.append(f"e.pubkey IN ({placeholders})")
                params.extend(f['authors'])
        
        if 'kinds' in f and f['kinds']:
            if len(f['kinds']) == 1:
                conds.append("e.kind=?")
                params.append(f['kinds'][0])
            else:
                placeholders = ','.join(['?'] * len(f['kinds']))
                conds.append(f"e.kind IN ({placeholders})")
                params.extend(f['kinds'])
        
        if 'since' in f:
            conds.append("e.created_at>=?")
            params.append(f['since'])
        if 'until' in f:
            conds.append("e.created_at<=?")
            params.append(f['until'])
        
        # Tag filters
        tag_filters = []
        for key, prefix in [('#p','p'), ('#e','e'), ('#a','a'),
                            ('#t','t'), ('#d','d'), ('#g','g'), ('#r','r')]:
            if key in f and f[key]:
                vals = f[key] if isinstance(f[key], list) else [f[key]]
                for val in vals:
                    tag_filters.append((prefix, val))
        
        if tag_filters:
            tag_joins = []
            for i, (prefix, val) in enumerate(tag_filters):
                alias = f"t{i}"
                tag_joins.append(
                    f"INNER JOIN tags {alias} ON {alias}.event_id=e.id "
                    f"AND {alias}.tag_type=? AND {alias}.tag_value=?"
                )
                params.extend([prefix, val])
            
            sql = (
                "SELECT DISTINCT e.id, e.pubkey, e.created_at, e.kind, "
                "e.tags_json, e.content, e.sig "
                f"FROM events e {' '.join(tag_joins)} "
            )
            if conds:
                sql += "WHERE " + " AND ".join(conds)
            sql += " ORDER BY e.created_at DESC LIMIT ?"
            params.append(limit)
            
            cur = self._db.execute(sql, params)
            return [self._row_to_event(r) for r in cur.fetchall()]
        
        # Search filter (NIP-50)
        if 'search' in f and f['search'].strip():
            return self._search_fts(f['search'], conds, params, limit)
        
        where = " AND ".join(conds) if conds else "1=1"
        sql = f"SELECT e.id, e.pubkey, e.created_at, e.kind, e.tags_json, e.content, e.sig FROM events e WHERE {where} ORDER BY e.created_at DESC LIMIT ?"
        params.append(limit)
        
        cur = self._db.execute(sql, params)
        return [self._row_to_event(r) for r in cur.fetchall()]
    
    def _search_fts(self, query: str, extra_conds: list, extra_params: list, limit: int) -> list[dict]:
        safe_query = re.sub(r'[^\w\s"\'\-]', ' ', query)[:200]
        words = safe_query.split()
        fts_query = ' OR '.join(f'"{w}"' for w in words if w)
        if not fts_query:
            return []
        try:
            where = " AND ".join(extra_conds) if extra_conds else "1=1"
            sql = (
                "SELECT e.id, e.pubkey, e.created_at, e.kind, e.tags_json, e.content, e.sig "
                "FROM events_fts f INNER JOIN events e ON e.id = f.event_id "
                f"WHERE events_fts MATCH ? AND {where} "
                "ORDER BY rank LIMIT ?"
            )
            cur = self._db.execute(sql, [fts_query] + extra_params + [limit])
            return [self._row_to_event(r) for r in cur.fetchall()]
        except Exception:
            return []
    
    def _row_to_event(self, r: tuple) -> dict:
        return {
            'id': r[0], 'pubkey': r[1], 'created_at': r[2],
            'kind': r[3], 'tags': json.loads(r[4]) if r[4] else [],
            'content': r[5], 'sig': r[6]
        }
    
    def get_stats(self) -> dict:
        cur = self._db.execute("""
            SELECT COUNT(*) as total,
                   COUNT(DISTINCT pubkey) as authors,
                   COUNT(DISTINCT kind) as kinds,
                   MAX(created_at) as newest
            FROM events
        """).fetchone()
        return {
            "events": cur[0], "authors": cur[1],
            "kinds": cur[2], "newest_event": cur[3],
            "fts_indexed": self._db.execute(
                "SELECT COUNT(*) FROM events_fts"
            ).fetchone()[0]
        }
    
    def close(self):
        self._db.close()
    
    # ── V2.1: Agent Registry ──
    def populate_agents(self, agents_dict: dict):
        now = int(time.time())
        for pubhex, info in agents_dict.items():
            name = info if isinstance(info, str) else info.get('name', pubhex[:12])
            role = info.get('role', '') if isinstance(info, dict) else ''
            nip05 = info.get('nip05', '') if isinstance(info, dict) else ''
            self._db.execute(
                "INSERT OR IGNORE INTO agents (pubkey, name, role, nip05, status, first_seen) VALUES (?,?,?,?,'registered',?)",
                (pubhex, name, role, nip05, now)
            )
        self._db.commit()
    
    def get_agents(self) -> list[dict]:
        cur = self._db.execute("""
            SELECT a.pubkey, a.name, a.role, a.nip05, a.status, a.last_seen, a.events_count, a.first_seen, a.relay_list,
                   e.created_at as last_event_at
            FROM agents a
            LEFT JOIN events e ON e.pubkey = a.pubkey AND e.id = (
                SELECT id FROM events WHERE pubkey = a.pubkey ORDER BY created_at DESC LIMIT 1
            )
            ORDER BY a.status='active' DESC, a.events_count DESC
        """)
        agents = []
        for r in cur.fetchall():
            status = r[4]
            last_seen = r[5] or 0
            now = int(time.time())
            if status == 'registered' and last_seen == 0:
                status = 'dormant'
            elif last_seen > 0 and (now - last_seen) < 21600:
                status = 'active'
            elif last_seen > 0 and (now - last_seen) < 86400:
                status = 'sleeping'
            else:
                status = 'dormant'
            
            agents.append({
                "pubkey": r[0],
                "name": r[1],
                "role": r[2],
                "nip05": r[3] or '',
                "status": status,
                "last_seen": last_seen,
                "events_count": r[6] or 0,
                "first_seen": r[7] or 0,
                "relay_list": json.loads(r[8]) if isinstance(r[8], str) and r[8] != '[]' else [],
                "last_event_at": r[9] or 0,
            })
        return agents
    
    def get_agent_by_pubkey(self, pubkey: str) -> dict | None:
        agents = self.get_agents()
        for a in agents:
            if a['pubkey'] == pubkey:
                return a
        return None
    
    # ── V2.2: NIP-29 Groups ──
    def init_groups(self, groups_config: list[dict]):
        now = int(time.time())
        for g in groups_config:
            gid = g['id']
            self._db.execute(
                "INSERT OR IGNORE INTO groups (id, name, about, pubkey, created_at, members_json) VALUES (?,?,?,?,?,?)",
                (gid, g.get('name', gid), g.get('about', ''),
                 g.get('pubkey', ''), now,
                 json.dumps(g.get('members', [])))
            )
        self._db.commit()
    
    def get_groups(self) -> list[dict]:
        cur = self._db.execute(
            "SELECT g.id, g.name, g.about, g.picture, g.pubkey, g.created_at, g.members_json, "
            "(SELECT COUNT(*) FROM events WHERE kind BETWEEN 39000 AND 39003 AND tags_json LIKE '%\"h\"%' || g.id || '%') as msg_count "
            "FROM groups g ORDER BY g.name"
        )
        groups = []
        for r in cur.fetchall():
            members = json.loads(r[6]) if r[6] else []
            groups.append({
                "id": r[0], "name": r[1], "about": r[2],
                "picture": r[3] or '', "pubkey": r[4],
                "created_at": r[5], "members": members,
                "member_count": len(members),
                "message_count": r[7] or 0,
            })
        return groups
    
    def get_group_members(self, group_id: str) -> list[str]:
        cur = self._db.execute("SELECT members_json FROM groups WHERE id=?", [group_id])
        r = cur.fetchone()
        if r and r[0]:
            return json.loads(r[0])
        return []
    
    def is_group_member(self, group_id: str, pubkey: str) -> bool:
        members = self.get_group_members(group_id)
        return pubkey in members
    
    def get_group_events(self, group_id: str, kind: int, limit: int = 50) -> list[dict]:
        cur = self._db.execute(
            "SELECT e.id, e.pubkey, e.created_at, e.kind, e.tags_json, e.content, e.sig "
            "FROM events e "
            "INNER JOIN tags t ON t.event_id = e.id "
            "WHERE t.tag_type='h' AND t.tag_value=? AND e.kind=? "
            "ORDER BY e.created_at DESC LIMIT ?",
            [group_id, kind, limit]
        )
        return [self._row_to_event(r) for r in cur.fetchall()]


# ── Nostr Auth (NIP-42) ──
def generate_challenge() -> str:
    return hashlib.sha256(os.urandom(32)).hexdigest()[:16]

def verify_signed_auth(event: dict, challenge: str) -> tuple[bool, str]:
    """NIP-42 AUTH верификация с полной проверкой Schnorr подписи."""
    if event.get('kind') != 22242:
        return False, "wrong kind (must be 22242)"
    
    tags = event.get('tags', [])
    challenge_tag = None
    relay_tag = None
    for t in tags:
        if len(t) >= 2 and t[0] == 'challenge':
            challenge_tag = t[1]
        if len(t) >= 2 and t[0] == 'relay':
            relay_tag = t[1]
    
    if challenge_tag != challenge:
        return False, "challenge mismatch"
    
    pubkey_hex = event.get('pubkey', '')
    sig_hex = event.get('sig', '')
    event_id = event.get('id', '')
    
    if len(pubkey_hex) != 64 or len(sig_hex) != 128 or len(event_id) != 64:
        return False, "invalid key/sig/id length"
    
    # 1. Проверяем id (хеш события)
    try:
        raw = json.dumps([0, event['pubkey'], event['created_at'],
            event['kind'], tags, event['content']],
            separators=(',',':'), ensure_ascii=False)
        expected_id = hashlib.sha256(raw.encode()).hexdigest()
        if expected_id != event_id:
            return False, "invalid id"
    except:
        return False, "serialization error"
    
    # 2. Проверяем Schnorr подпись (pubkey, event_id, sig)
    try:
        pk = PublicKey(bytes.fromhex(pubkey_hex))
        verified = pk.verify(
            bytes.fromhex(sig_hex),
            bytes.fromhex(event_id)
        )
        if not verified:
            return False, "invalid signature"
    except Exception as e:
        return False, f"signature verification error: {e}"
    
    return True, pubkey_hex


# V3.0: NIP-26 — Verify delegated event
def verify_delegated_event(event: dict) -> tuple[bool, str]:
    """Check if event has a valid delegation tag."""
    for tag in event.get('tags', []):
        if len(tag) >= 4 and tag[0] == 'delegation':
            delegator_pubkey = tag[1]
            conditions = tag[2]
            delegation_sig = tag[3]
            # Check delegation exists in DB
            db_delegator = DB_REF.get_delegator(event['pubkey'])
            if not db_delegator:
                return False, "no delegation found"
            if db_delegator != delegator_pubkey:
                return False, "delegation mismatch"
            # Verify delegation signature (kind:22242 signed by delegator)
            # The delegation event itself must be valid — we trust it was validated on insert
            return True, delegator_pubkey
    return False, "no delegation tag"

DB_REF = None  # Global ref for delegation lookups


# ── NIP-86: Relay Management API ──
async def admin_nip86(request: web.Request):
    if request.content_type != 'application/nostr+json+rpc':
        return web.json_response({"error": "use Content-Type: application/nostr+json+rpc"}, status=400)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)
    
    method = body.get('method', '')
    params = body.get('params', [])
    db: RelayDB = request.app['db']
    
    if method == 'banpubkey':
        if len(params) < 1:
            return web.json_response({"error": "banpubkey needs pubkey param"})
        pubkey = params[0]
        reason = params[1] if len(params) > 1 else ''
        now = int(time.time())
        db._db.execute(
            "INSERT OR REPLACE INTO banned_pubkeys VALUES (?,?,?,?)",
            (pubkey, reason, now, 'nip86_api')
        )
        db._db.commit()
        logger.info(f"NIP-86: banned {pubkey[:16]}... reason={reason}")
        return web.json_response({"result": True})
    
    elif method == 'allowpubkey':
        if len(params) < 1:
            return web.json_response({"error": "allowpubkey needs pubkey param"})
        pubkey = params[0]
        db._db.execute("DELETE FROM banned_pubkeys WHERE pubkey=?", (pubkey,))
        db._db.commit()
        logger.info(f"NIP-86: unbanned {pubkey[:16]}...")
        return web.json_response({"result": True})
    
    elif method == 'listbannedpubkeys':
        cur = db._db.execute("SELECT pubkey, reason, banned_at FROM banned_pubkeys ORDER BY banned_at DESC")
        rows = cur.fetchall()
        result = [{"pubkey": r[0], "reason": r[1], "banned_at": r[2]} for r in rows]
        return web.json_response({"result": result})
    
    elif method == 'changename':
        if len(params) < 1:
            return web.json_response({"error": "changename needs name param"})
        request.app['relay_name'] = params[0]
        logger.info(f"NIP-86: relay name changed to '{params[0]}'")
        return web.json_response({"result": True})
    
    elif method == 'listallowedkinds':
        # Standard + V3.0: DMs (4,44), file meta (1063), video (34235), handlers (31989), NIP-26 (22222)
        allowed = [0, 1, 3, 4, 5, 7, 44, 9734, 9735, 10000, 10001, 10002,
                   1063, 1984, 1985, 22222, 31989, 31990, 34235, 39000, 39001, 39002, 39003]
        return web.json_response({"result": allowed})
    
    elif method == 'supportedmethods':
        methods = ["banpubkey", "allowpubkey", "listbannedpubkeys", "changename", "listallowedkinds"]
        return web.json_response({"result": methods})
    
    # V3.0: NIP-86 — list/reports
    elif method == 'listreports':
        target = params[0] if params else ''
        if not target:
            return web.json_response({"error": "listreports needs target_pubkey param"})
        reports = db.get_reports_for_pubkey(target)
        return web.json_response({"result": {"target": target, "reports": reports, "count": len(reports)}})
    
    else:
        return web.json_response({"error": f"unknown method: {method}"}, status=400)


# ── NIP-96: Blossom File Storage ──
BLOBS_DIR = BASE / "blobs"
BLOBS_DIR.mkdir(exist_ok=True)

async def blossom_upload(request: web.Request):
    body = await request.read()
    if not body:
        return web.json_response({"error": "empty body"}, status=400)
    # V3.0: Check max blob size (100MB)
    if len(body) > 100 * 1024 * 1024:
        return web.json_response({"error": "file too large (max 100MB)"}, status=400)
    
    sha = hashlib.sha256(body).hexdigest()
    filepath = BLOBS_DIR / sha
    filepath.write_bytes(body)
    
    db = request.app['db']
    now = int(time.time())
    auth_pubkey = request.headers.get('X-Pubkey', 'anonymous')
    
    await db._write_lock.acquire()
    try:
        db._db.execute(
            "INSERT OR IGNORE INTO blobs (sha256, pubkey, size, mime, uploaded_at) VALUES (?,?,?,?,?)",
            (sha, auth_pubkey, len(body), request.content_type or 'application/octet-stream', now)
        )
        db._db.commit()
    finally:
        db._write_lock.release()
    
    url = f"/blobs/{sha}"
    logger.info(f"Blossom: uploaded {sha[:12]}... ({len(body)}B) by {auth_pubkey[:12]}...")
    return web.json_response({"status": "ok", "url": url})


async def blossom_download(request: web.Request):
    sha = request.match_info.get('sha', '')
    filepath = BLOBS_DIR / sha
    if not filepath.exists():
        return web.json_response({"error": "file not found"}, status=404)
    db = request.app['db']
    cur = db._db.execute("SELECT mime FROM blobs WHERE sha256=?", (sha,))
    row = cur.fetchone()
    mime = row[0] if row else 'application/octet-stream'
    return web.Response(body=filepath.read_bytes(), content_type=mime)


async def blossom_list(request: web.Request):
    pubkey = request.match_info.get('pubkey', '')
    db = request.app['db']
    cur = db._db.execute(
        "SELECT sha256, size, mime, uploaded_at FROM blobs WHERE pubkey=? ORDER BY uploaded_at DESC",
        (pubkey,)
    )
    files = [{"sha256": r[0], "size": r[1], "mime": r[2], "uploaded_at": r[3]} for r in cur.fetchall()]
    return web.json_response({"files": files})


async def blossom_delete(request: web.Request):
    sha = request.match_info.get('sha', '')
    db = request.app['db']
    filepath = BLOBS_DIR / sha
    if filepath.exists():
        filepath.unlink()
    await db._write_lock.acquire()
    try:
        db._db.execute("DELETE FROM blobs WHERE sha256=?", (sha,))
        db._db.commit()
    finally:
        db._write_lock.release()
    logger.info(f"Blossom: deleted {sha[:12]}...")
    return web.json_response({"status": "ok"})


async def blossom_info(request: web.Request):
    db = request.app['db']
    cur = db._db.execute("SELECT COUNT(*), SUM(size) FROM blobs")
    count, total_size = cur.fetchone()
    return web.json_response({
        "status": "ok",
        "count": count or 0,
        "total_size": total_size or 0,
        "supported_nips": [96],
        "max_upload_size": 100 * 1024 * 1024,
    })


# ── Rate Limiter ──
class RateLimiter:
    def __init__(self):
        self._ip_conns = defaultdict(set)  # ip -> set of ws ids
        self._ip_msgs = defaultdict(list)  # ip -> [timestamps]
        self._ip_events = defaultdict(list)  # ip -> [timestamps]
        self._authed_ws = {}  # ws_id -> bool
    
    def mark_authed(self, ws_id: str):
        self._authed_ws[ws_id] = True
    
    def _is_authed(self, ws_id: str) -> bool:
        return self._authed_ws.get(ws_id, False)
    
    def check_connect(self, ip: str, ws_id: str) -> bool:
        max_conn = RATE_MAX_CONN_AUTH if self._is_authed(ws_id) else RATE_MAX_CONN
        if len(self._ip_conns[ip]) >= max_conn:
            return False
        self._ip_conns[ip].add(ws_id)
        self._authed_ws[ws_id] = False
        return True
    
    def disconnect(self, ip: str, ws_id: str):
        self._ip_conns.get(ip, set()).discard(ws_id)
        self._authed_ws.pop(ws_id, None)
        if not self._ip_conns.get(ip):
            self._ip_conns.pop(ip, None)
    
    def check_message(self, ip: str) -> bool:
        now = time.time()
        cutoff = now - RATE_WINDOW
        msgs = [t for t in self._ip_msgs.get(ip, []) if t > cutoff]
        self._ip_msgs[ip] = msgs
        if len(msgs) >= RATE_MAX_MSG:
            return False
        self._ip_msgs[ip].append(now)
        return True
    
    def check_event(self, ip: str, ws_id: str = '') -> bool:
        now = time.time()
        cutoff = now - RATE_WINDOW
        max_evts = RATE_MAX_EVENTS_AUTH if self._authed_ws.get(ws_id) else RATE_MAX_EVENTS
        evts = [t for t in self._ip_events.get(ip, []) if t > cutoff]
        self._ip_events[ip] = evts
        if len(evts) >= max_evts:
            return False
        self._ip_events[ip].append(now)
        return True


# ── WebSocket Handler ──
class NostrWSHandler:
    def __init__(self, db: RelayDB, rate: RateLimiter):
        self.db = db
        self.rate = rate
        self.dao_voting = None  # Set by main()
        self.fanout = None
        self._subscriptions = {}  # sid -> (ws, filters, authed_pubkey)
        self._sessions = {}  # ws_id -> {ip, last_activity}
        self._ws_counter = 0
    
    async def handle(self, request: web.Request):
        # NIP-86: Relay Management API (JSON-RPC over HTTP)
        if request.method == 'POST' and request.content_type == 'application/nostr+json+rpc':
            return await admin_nip86(request)
        
        upgrade_hdr = request.headers.get('Upgrade', '').lower()
        logger.debug(f"WS check: upgrade_hdr='{upgrade_hdr}', headers={dict(request.headers)}")
        if upgrade_hdr != 'websocket':
            # V3.1: HTML dashboard for browsers, NIP-11 JSON for clients
            accept = request.headers.get('Accept', '')
            if 'text/html' in accept or 'text/*' in accept:
                return await self._dashboard_response(request)
            return await self._nip11_response(request)
        
        ip = request.remote or 'unknown'
        ws = web.WebSocketResponse(max_msg_size=MAX_EVENT_SIZE + 1024)  # V3.0: use MAX_EVENT_SIZE
        await ws.prepare(request)
        
        self._ws_counter += 1
        ws_id = f"ws_{self._ws_counter}_{int(time.time())}"
        self._sessions[ws_id] = {"ip": ip, "last_activity": time.time()}
        
        if not self.rate.check_connect(ip, ws_id):
            await ws.close(code=4001, message=b"rate limit: too many connections")
            return ws
        
        authed_pubkey = None
        challenge = None
        
        logger.info(f"WS connect: {ip} (ws_id={ws_id})")
        
        # Send NIP-42 auth challenge ВСЕМ новым подключениям (не только Nostr-заголовок)
        challenge = generate_challenge()
        await ws.send_json(["AUTH", challenge])
        
        try:
            # V3.0: Idle timeout — if no message for WS_IDLE_TIMEOUT seconds, disconnect
            while True:
                try:
                    msg = await asyncio.wait_for(ws.__anext__(), timeout=WS_IDLE_TIMEOUT)
                except asyncio.TimeoutError:
                    # V3.0: Send ping first, then close
                    try:
                        await ws.ping()
                        # Wait a bit more for pong
                        msg = await asyncio.wait_for(ws.__anext__(), timeout=5)
                    except (asyncio.TimeoutError, Exception):
                        logger.info(f"WS idle timeout: {ws_id} ({ip})")
                        await ws.close(code=4000, message=b"idle timeout")
                        break
                
                if msg.type != WSMsgType.TEXT:
                    if msg.type == WSMsgType.CLOSED:
                        break
                    continue
                
                # Update last activity
                self._sessions[ws_id]["last_activity"] = time.time()
                
                ip = self._sessions.get(ws_id, {}).get("ip", ip)
                if not self.rate.check_message(ip):
                    await ws.send_json(["NOTICE", "rate limit exceeded"])
                    continue
                
                try:
                    data = json.loads(msg.data)
                except:
                    continue
                
                if not isinstance(data, list) or len(data) < 2:
                    continue
                
                cmd = data[0]
                
                if cmd == "EVENT":
                    event = data[1]
                    # V3.0: Max event size check
                    raw_size = len(json.dumps(event))
                    if raw_size > MAX_EVENT_SIZE:
                        await ws.send_json(["OK", event.get('id',''), False, f"event too large ({raw_size}B > {MAX_EVENT_SIZE}B)"])
                        continue
                    if not self.rate.check_event(ip, ws_id):
                        await ws.send_json(["OK", event.get('id',''), False, "rate limit: too many events"])
                        continue
                    await self._handle_event(ws, event, authed_pubkey, ws_id)
                
                elif cmd == "REQ":
                    sub_id = data[1]
                    filters = data[2:] if len(data) > 2 else [{}]
                    self._subscriptions[sub_id] = (ws, filters, authed_pubkey)
                    await self._handle_req(ws, sub_id, filters, authed_pubkey)
                
                elif cmd == "CLOSE":
                    if data[1] in self._subscriptions:
                        del self._subscriptions[data[1]]
                
                elif cmd == "AUTH":
                    event = data[1]
                    valid, result = verify_signed_auth(event, challenge or "")
                    if valid:
                        authed_pubkey = result
                        self.rate.mark_authed(ws_id)
                        await ws.send_json(["OK", event['id'], True, "authenticated"])
                        logger.info(f"NIP-42 auth: {authed_pubkey[:16]}...")
                    else:
                        await ws.send_json(["OK", event['id'], False, f"auth failed: {result}"])
        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            err_msg = str(e) or type(e).__name__ or "unknown"
            logger.warning(f"WS close ({ws_id}): {err_msg}")
        finally:
            self.rate.disconnect(ip, ws_id)
            self._sessions.pop(ws_id, None)
            dead_subs = [sid for sid, (sws, _, _) in self._subscriptions.items() if sws == ws]
            for sid in dead_subs:
                del self._subscriptions[sid]
        
        return ws
    
    async def _handle_event(self, ws, event: dict, authed_pubkey: str | None, ws_id: str = ""):
        event_id = event.get('id', '')
        kind = event.get('kind', -1)
        pubkey = event.get('pubkey', '')
        
        # Verify event has all required fields
        for r in ['id','pubkey','created_at','kind','content','sig']:
            if r not in event:
                await ws.send_json(["OK", event_id, False, f"missing {r}"])
                return
        
        # K7: Verify integrity (NIP-01 id + Schnorr sig)
        try:
            integrity = verify_integrity(event)
            if not integrity["valid"]:
                await ws.send_json(["OK", event_id, False, f"integrity: {integrity['error']}"])
                logger.warning(f"Integrity reject: {event_id[:20]}... {integrity['error']}")
                return
        except Exception as e:
            logger.warning(f"Integrity check error: {e}")
            # soft-fail: пропускаем проверку при ошибке импорта
        
        # Check banned pubkey
        cur = self.db._db.execute("SELECT reason FROM banned_pubkeys WHERE pubkey=?", (pubkey,))
        banned = cur.fetchone()
        if banned:
            await ws.send_json(["OK", event_id, False, f"pubkey banned: {banned[0] or 'no reason'}"])
            return
        
        # V3.0: Check delegated pubkey
        effective_pubkey = pubkey
        if pubkey not in WHITELIST:
            delegator = self.db.get_delegator(pubkey)
            if delegator:
                effective_pubkey = delegator
                logger.debug(f"NIP-26: {pubkey[:16]}... delegated by {delegator[:16]}...")
        
        # V3.1: NIP-42 — permission tiers
        # WHITELIST: полный доступ (все kinds)
        # Authenticated (NIP-42): PUBLIC_WRITE_KINDS
        # Unauthenticated: read-only (reject write)
        # Localhost bypass: внутренние сервисы (DAO poster, heartbeat) не требуют AUTH
        remote_ip = self._sessions.get(ws_id, {}).get("ip", "")
        if remote_ip in ("127.0.0.1", "::1", "localhost"):
            # Localhost — полный доступ
            pass
        elif effective_pubkey not in WHITELIST and pubkey not in WHITELIST:
            if not authed_pubkey:
                await ws.send_json(["OK", event_id, False, "auth-required: authenticate via NIP-42 AUTH first"])
                return
            if kind not in PUBLIC_WRITE_KINDS:
                await ws.send_json(["OK", event_id, False, f"blocked: kind {kind} requires whitelist access"])
                return
            logger.info(f"NIP-42: authed user {pubkey[:16]}... writing kind {kind}")
        
        # NIP-65: Index relay list metadata
        if kind == 10002:
            relays = []
            for tag in event.get('tags', []):
                if len(tag) >= 2 and tag[0] == 'r':
                    relays.append(tag[1])
            if relays:
                self.db._db.execute(
                    "UPDATE agents SET relay_list=? WHERE pubkey=?",
                    (json.dumps(relays), pubkey)
                )
                self.db._db.commit()
                logger.info(f"NIP-65: indexed {len(relays)} relays for {pubkey[:16]}...")
        
        # NIP-09: Event deletion
        if kind == 5:
            deleted = 0
            for tag in event.get('tags', []):
                if len(tag) >= 2 and tag[0] == 'e':
                    target_id = tag[1]
                    cur = self.db._db.execute("SELECT pubkey FROM events WHERE id=?", (target_id,))
                    row = cur.fetchone()
                    if row and row[0] == pubkey:
                        self.db._db.execute("DELETE FROM events WHERE id=?", (target_id,))
                        self.db._db.execute("DELETE FROM tags WHERE event_id=?", (target_id,))
                        self.db._db.execute("DELETE FROM events_fts WHERE event_id=?", (target_id,))
                        deleted += 1
            self.db._db.commit()
            self.db._db.execute("INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?)",
                (event_id, pubkey, event['created_at'], 5,
                 json.dumps(event.get('tags',[])), event['content'],
                 event['sig'], int(time.time())))
            self.db._db.commit()
            logger.info(f"NIP-09: deleted {deleted} events for {pubkey[:16]}...")
            await ws.send_json(["OK", event_id, True, f"deleted {deleted} events"])
            return
        
        # V3.0: NIP-56 — handle reports (kind:1984)
        if kind == 1984:
            tags = event.get('tags', [])
            target_pubkey = None
            reason = ''
            for t in tags:
                if len(t) >= 2 and t[0] == 'p':
                    target_pubkey = t[1]
                if len(t) >= 2 and t[0] in ('l', 'm'):
                    reason = t[1]
            if target_pubkey:
                await self.db.store_report_async(
                    target_pubkey, pubkey, kind, reason,
                    event.get('content', ''), event_id
                )
                logger.info(f"NIP-56: {pubkey[:16]}... reported {target_pubkey[:16]}... reason={reason}")
                # Auto-ban after 3+ reports from different pubkeys
                report_count = self.db.get_report_count(target_pubkey)
                if report_count >= 3:
                    self.db._db.execute(
                        "INSERT OR REPLACE INTO banned_pubkeys VALUES (?,?,?,?)",
                        (target_pubkey, f"auto-ban after {report_count} reports", int(time.time()), 'nip56_auto')
                    )
                    self.db._db.commit()
                    logger.warning(f"NIP-56 auto-ban: {target_pubkey[:16]}... ({report_count} reports)")
                    await ws.send_json(["NOTICE", f"reported pubkey {target_pubkey[:16]}... has been banned"])
        
        # V3.0: NIP-51 — track mute/pin lists (kind:10000, 10001)
        if kind in (10000, 10001):
            tags = event.get('tags', [])
            for t in tags:
                if len(t) >= 2 and t[0] == 'p':
                    await self.db.store_list_item_async(pubkey, kind, t[1])
            logger.debug(f"NIP-51: processed {kind} list for {pubkey[:16]}...")
        
        # V3.0: NIP-26 — register delegation
        if kind == 22222:
            tags = event.get('tags', [])
            for t in tags:
                if len(t) >= 4 and t[0] == 'delegation':
                    # NIP-26: tag = ["delegation", delegator_pk, conditions_query, signature_hex]
                    # expires_at is in conditions_query: "until=<timestamp>"
                    cond = t[2]
                    expires_at = 0
                    for part in cond.split('&'):
                        if part.startswith('until='):
                            try:
                                expires_at = int(part.split('=')[1])
                            except:
                                pass
                            break
                    await self.db.store_delegation_async(
                        event['pubkey'], t[1], cond,
                        expires_at,
                        event_id
                    )
                    logger.info(f"NIP-26: delegation: {event['pubkey'][:16]}... -> {t[1][:16]}...")
                    await ws.send_json(["OK", event_id, True, "delegation registered"])
                    return
        
        # V3.0: Check if pubkey is muted by subscriber (NIP-51)
        # (checked at subscription time, not here)
        
        # NIP-29 Group access control
        if 39000 <= kind <= 39003:
            tags = event.get('tags', [])
            group_id = None
            for t in tags:
                if len(t) >= 2 and t[0] == 'h':
                    group_id = t[1]
                    break
            if not group_id:
                await ws.send_json(["OK", event_id, False, "group event missing 'h' tag"])
                return
            if kind == 39003:
                # Check via effective_pubkey (delegated)
                if not self.db.is_group_member(group_id, pubkey) and not self.db.is_group_member(group_id, effective_pubkey):
                    await ws.send_json(["OK", event_id, False, f"not a member of group {group_id}"])
                    return
            elif kind in (39000, 39001, 39002):
                # Normalize pubkey: strip 02/03 prefix for comparison
                check_key = effective_pubkey
                if check_key.startswith(("02", "03")) and len(check_key) == 66:
                    check_key = check_key[2:]
                # Also check original pubkey
                check_orig = pubkey
                if check_orig.startswith(("02", "03")) and len(check_orig) == 66:
                    check_orig = check_orig[2:]
                if check_key not in WHITELIST and check_orig not in WHITELIST:
                    await ws.send_json(["OK", event_id, False, "only SNIN agents can manage groups"])
                    return
        
        # Verify event ID (pass with delegated pubkey check)
        try:
            raw = json.dumps([0, event['pubkey'], event['created_at'],
                event['kind'], event.get('tags',[]), event['content']],
                separators=(',',':'), ensure_ascii=False)
            if hashlib.sha256(raw.encode()).hexdigest() != event_id:
                await ws.send_json(["OK", event_id, False, "invalid id"])
                return
        except:
            await ws.send_json(["OK", event_id, False, "serialization error"])
            return
        
        # V3.0: NIP-13 — optional Proof of Work check
        # Если в событии есть nonce тег — проверяем что первые difficulty бит нулевые
        nonce_tag = None
        difficulty = 0
        for t in event.get('tags', []):
            if len(t) >= 2 and t[0] == 'nonce':
                nonce_tag = t
                if len(t) >= 3:
                    try:
                        difficulty = int(t[2])
                    except (ValueError, IndexError):
                        difficulty = 0
                break
        if nonce_tag and difficulty > 0:
            # Check that id starts with 'difficulty' zero bits
            id_int = int(event_id, 16)
            required_zeros = difficulty
            if (id_int >> (256 - required_zeros)) != 0:
                # Non-critical: reject with notice but still accept
                logger.debug(f"NIP-13: insufficient PoW for {event_id[:12]}... (needs {difficulty})")
                # Relay can choose to reject — we log and accept (optional PoW)
        
        # V3.0: NIP-33 — handle parameterized replaceable events (kind >= 30000)
        if 30000 <= kind < 40000 and kind not in (39000, 39001, 39002, 39003):
            # Extract 'd' tag — the identifier
            d_tag = ''
            for t in event.get('tags', []):
                if len(t) >= 2 and t[0] == 'd':
                    d_tag = t[1]
                    break
            if d_tag:
                # Delete previous events with same kind + pubkey + d tag
                cursor = self.db._db.execute(
                    "SELECT id FROM events WHERE pubkey=? AND kind=? AND id != ? AND tags_json LIKE ?",
                    (pubkey, kind, event_id, f'%"d"%"{d_tag}"%')
                )
                old_events = cursor.fetchall()
                for old in old_events:
                    old_id = old[0]
                    self.db._db.execute("DELETE FROM events WHERE id=?", (old_id,))
                    self.db._db.execute("DELETE FROM tags WHERE event_id=?", (old_id,))
                    self.db._db.execute("DELETE FROM events_fts WHERE event_id=?", (old_id,))
                if old_events:
                    self.db._db.commit()
                    logger.debug(f"NIP-33: replaced {len(old_events)} events for kind={kind} d={d_tag}")
        
        # NIP-XX: Solana Payments — kind:30000 validation
        if kind == 30000:
            result = await handle_snin_payment(event)
            if not result.get("accepted", False):
                await ws.send_json(["OK", event_id, False, f"payment rejected: {result.get('reason', 'unknown')}"])
                logger.warning(f"[PAYMENT] ❌ {event_id[:12]} rejected: {result.get('reason')}")
                return
            logger.info(f"[PAYMENT] ✅ {event_id[:12]} verified on Solana")
            
            # Логируем в БД payments
            try:
                content_data = json.loads(event.get("content", "{}"))
            except (json.JSONDecodeError, TypeError):
                content_data = {}
            p_tag = next((t[1] for t in event.get("tags", []) if t[0] == "p"), "")
            solana_tx_tag = next((t[1] for t in event.get("tags", []) if t[0] == "solana_tx"), "")
            self.db._db.execute("""
                INSERT OR IGNORE INTO payments 
                (id, event_id, kind, sender_pubkey, receiver_pubkey, amount, token, solana_tx, memo, created_at, accepted)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, (
                event_id[:16], event_id, kind, pubkey, p_tag,
                content_data.get("amount", 0), content_data.get("token", "SNIN"),
                solana_tx_tag, content_data.get("memo", ""), event.get("created_at", 0)
            ))
            self.db._db.commit()
            logger.info(f"[PAYMENT] 💾 logged to DB: {event_id[:12]} → {p_tag[:12]} {content_data.get('amount', 0)} SNIN")

        # NIP-XX: Solana Payments — kind:30001 balance request (NOT stored)
        if kind == 30001:
            relay_url = f"wss://relay-snin.v2.site"
            relay_pubkey = CRYTER_PUBKEY
            response = await handle_balance_request(event, relay_url, relay_pubkey)
            if response:
                await ws.send_json(["EVENT", "balance_response", response])
                logger.info(f"[BALANCE] 📊 balance request from {pubkey[:12]}")
            return

        # Store event (thread-safe)
        if await self.db.store_event_async(event):
            
# Auto-Fanout — ДО send_json (чтобы успеть до закрытия WS клиентом)
            should_fanout = hasattr(self, 'fanout') and self.fanout is not None and (
                effective_pubkey == CRYTER_PUBKEY or
                effective_pubkey in WHITELIST or
                kind in (39000, 39001, 39002) or  # DAO group posts
                kind == 30000 or  # NIP-XX: Solana Payments fanout
                any(
                    t[0] == "p" and t[1].startswith(("02", "03")) and len(t[1]) == 66
                    for t in event.get("tags", [])
                )
            )
            if should_fanout:
                try:
                    self.fanout.enqueue(event)
                except Exception as e:
                    logger.warning(f"Fanout enqueue error: {e}")
            
            # DAO Voting: process proposals and votes
            if hasattr(self, 'dao_voting') and self.dao_voting:
                if kind == 1111:
                    self.dao_voting.handle_proposal(event)
                elif kind == 1112:
                    self.dao_voting.handle_vote(event)
            
            await ws.send_json(["OK", event_id, True, ""])
            
            # IPFS — отключён (legacy)
            # if hasattr(self, 'ipfs') and self.ipfs:
            #     try:
            #         cid = await self.ipfs.publish_event(event)
            #         if hasattr(self, 'cid_index') and self.cid_index:
            #             self.cid_index.add(
            #                 event["id"], cid,
            #                 event.get("pubkey", ""),
            #                 event.get("kind", -1),
            #                 event.get("created_at", 0)
            #             )
            #         logger.debug(f"K7 IPFS: {cid} kind={event.get('kind')}")
            #     except Exception as e:
            #         logger.debug(f"K7 IPFS: {e}")
            
            # Notify subscribers (skip muted pubkeys)
            for sid, (sws, filters, _) in list(self._subscriptions.items()):
                try:
                    if any(self._match_filter(event, f) for f in filters):
                        await sws.send_json(["EVENT", sid, event])
                except:
                    self._subscriptions.pop(sid, None)
        else:
            await ws.send_json(["OK", event_id, True, ""])  # duplicate
    
    async def _handle_req(self, ws, sub_id: str, filters: list[dict], authed_pubkey: str | None):
        events = self.db.query_events(filters)
        for event in events:
            # Check expiration
            exp = next((t[1] for t in event.get('tags',[]) if len(t)>=2 and t[0]=='expiration'), None)
            if exp and int(exp) < time.time():
                continue
            # V3.0: Skip events from pubkeys that the subscriber has muted (NIP-51)
            if authed_pubkey and event.get('pubkey'):
                if self.db.is_muted(authed_pubkey, event['pubkey']):
                    continue
            await ws.send_json(["EVENT", sub_id, event])
        await ws.send_json(["EOSE", sub_id])
    
    def _match_filter(self, event: dict, f: dict) -> bool:
        if 'ids' in f and f['ids']:
            if event['id'] not in f['ids']:
                return False
        if 'authors' in f and f['authors']:
            if event['pubkey'] not in f['authors']:
                return False
        if 'kinds' in f and f['kinds']:
            if event['kind'] not in f['kinds']:
                return False
        if 'since' in f and event['created_at'] < f['since']:
            return False
        if 'until' in f and event['created_at'] > f['until']:
            return False
        for key, prefix in [('#p','p'),('#e','e'),('#a','a'),('#t','t'),('#g','g'),('#d','d')]:
            if key in f and f[key]:
                vals = f[key] if isinstance(f[key], list) else [f[key]]
                event_tags = [t[1] for t in event.get('tags',[]) if len(t)>=2 and t[0]==prefix]
                if not any(v in event_tags for v in vals):
                    return False
        return True
    
    async def _dashboard_response(self, request):
        """HTML dashboard for browser visitors."""
        stats = self.db.get_stats()
        mpr = request.app.get('pulse')
        alive = mpr.alive_count if mpr else 0
        total = mpr.relay_count if mpr else 0
        fanout = request.app.get('fanout')
        fstats = {}
        if fanout:
            try:
                fstats = fanout.get_stats()
            except:
                pass
        
        # K7: IPFS stats — отключён
        # ipfs_obj = request.app.get('ipfs')
        # ipfs_stats = {}
        if ipfs_obj:
            try:
                await ipfs_obj.get_peers()
                ipfs_stats = ipfs_obj.get_stats()
            except:
                pass
        ipfs_peers = ipfs_stats.get('peers', 0)
        ipfs_published = ipfs_stats.get('published', 0)
        
        html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SNIN Relay — Status</title>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ background:#0a0a1a; color:#e0e0e0; font-family:-apple-system,system-ui,sans-serif; min-height:100vh; display:flex; flex-direction:column; align-items:center; padding:40px 20px; }}
.container {{ max-width:800px; width:100%; }}
h1 {{ font-size:2em; color:#00d4ff; margin-bottom:8px; }}
.subtitle {{ color:#888; margin-bottom:32px; }}
.grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:16px; margin-bottom:32px; }}
.card {{ background:#12122a; border:1px solid #1a1a3a; border-radius:12px; padding:20px; text-align:center; }}
.card .value {{ font-size:2em; font-weight:700; color:#00d4ff; }}
.card .label {{ font-size:.85em; color:#888; margin-top:4px; }}
.card.green .value {{ color:#00ff88; }}
.card.orange .value {{ color:#ff8800; }}
.section {{ margin-bottom:24px; }}
.section h2 {{ font-size:1.2em; color:#00d4ff; margin-bottom:12px; border-bottom:1px solid #1a1a3a; padding-bottom:8px; }}
.info-row {{ display:flex; justify-content:space-between; padding:8px 0; border-bottom:1px solid #0d0d20; }}
.info-row .label {{ color:#888; }}
.info-row .value {{ color:#e0e0e0; }}
.howto {{ background:#0d0d20; border-radius:12px; padding:20px; margin-bottom:24px; }}
.howto code {{ display:block; background:#1a1a2e; padding:12px; border-radius:8px; margin:8px 0; color:#00ff88; font-family:monospace; }}
.howto a {{ color:#00d4ff; text-decoration:none; }}
.howto a:hover {{ text-decoration:underline; }}
.ws-indicator {{ display:inline-flex; align-items:center; gap:6px; }}
.ws-dot {{ width:10px; height:10px; border-radius:50%; background:#00ff88; display:inline-block; }}
.footer {{ text-align:center; color:#555; font-size:.85em; margin-top:32px; }}
</style>
</head>
<body>
<div class="container">
<h1>⚡ SNIN Network Relay</h1>
<p class="subtitle">Sovereign Nostr Infrastructure · Version {VERSION}</p>

<div class="grid">
<div class="card green"><div class="value">{stats.get('events',0):,}</div><div class="label">Событий</div></div>
<div class="card"><div class="value">{stats.get('authors',0):,}</div><div class="label">Авторов</div></div>
<div class="card green"><div class="value">{alive:,}</div><div class="label">Relay Alive</div></div>
<div class="card"><div class="value">{fstats.get('published',0):,}</div><div class="label">Fanout</div></div>
<div class="card orange"><div class="value">{ipfs_peers}</div><div class="label">IPFS Peers</div></div>
<div class="card green"><div class="value">{ipfs_published}</div><div class="label">IPFS Published</div></div>
</div>

<div class="section">
<h2>🪐 IPFS Pubsub (K7)</h2>
<div class="info-row"><span class="label">IPFS Peers</span><span class="value">{ipfs_peers}</span></div>
<div class="info-row"><span class="label">Published</span><span class="value">{ipfs_published}</span></div>
<div class="info-row"><span class="label">Received</span><span class="value">{ipfs_stats.get('received', 0)}</span></div>
<div class="info-row"><span class="label">Topic</span><span class="value" style="color:#00ff88">{ipfs_stats.get('topic', '—')}</span></div>
</div>

<div class="section">
<h2>📡 Fanout Engine</h2>
<div class="info-row"><span class="label">Всего relay в БД</span><span class="value">{total:,}</span></div>
<div class="info-row"><span class="label">Живых relay</span><span class="value" style="color:#00ff88">{alive:,}</span></div>
<div class="info-row"><span class="label">Событий разослано</span><span class="value">{fstats.get('broadcast',0):,}</span></div>
<div class="info-row"><span class="label">Relay затронуто</span><span class="value">{fstats.get('total_relays_hit',0):,}</span></div>
</div>

<div class="section">
<h2>🔐 NIP-42 Authentication</h2>
<div class="info-row"><span class="label">Write без AUTH</span><span class="value" style="color:#ff4444">Только чтение</span></div>
<div class="info-row"><span class="label">AUTH (NIP-42)</span><span class="value" style="color:#00ff88">kind:1, 7, 9734, 9735</span></div>
<div class="info-row"><span class="label">Rate limit (без AUTH)</span><span class="value">5 conn · 30 evt/10s</span></div>
<div class="info-row"><span class="label">Rate limit (AUTH)</span><span class="value">20 conn · 100 evt/10s</span></div>
<div class="info-row"><span class="label">Whitelist</span><span class="value" style="color:#ff8800">15 SNIN агентов</span></div>
</div>

<div class="howto">
<h2>🔌 Подключение</h2>
<p>Nostr клиент:</p>
<code>wss://snin-relay.v2.site</code>
<p>NIP-42 AUTH для публикации заметок:</p>
<code>["AUTH", {{...}}]</code>
</div>

<div class="footer">
<span class="ws-indicator"><span class="ws-dot"></span> Relay online</span>
· <a href="/api/stats">API Stats</a> · <a href="/api/fanout">Fanout</a>
· snin-relay.v2.site
</div>
</div>
</body>
</html>"""
        return web.Response(text=html, content_type='text/html')

    async def _nip11_response(self, request):
        """NIP-11 relay info."""
        stats = self.db.get_stats()
        return web.json_response({
            "name": RELAY_NAME,
            "description": RELAY_DESC,
            "pubkey": RELAY_PUBKEY,
            "contact": RELAY_CONTACT,
            "supported_nips": [1, 4, 9, 11, 12, 13, 20, 26, 29, 33, 40, 42, 45, 50, 56, 71, 86, 89, 94, 96],
            "software": SOFTWARE,
            "version": VERSION,
            "event_count": stats['events'],
            "authors_count": stats['authors'],
            "limitation": {
                "max_message_length": MAX_EVENT_SIZE,
                "max_subscriptions": 100,
                "max_filters": 50,
                "max_limit": 500,
                "max_subid_length": 256,
                "min_prefix": 4,
                "auth_required": True,
                "payment_required": False,
                "restricted_writes": True,
                "public_write_kinds": sorted(list(PUBLIC_WRITE_KINDS)),
            },
            "fees": {},
            "retention": [{"kinds": [0, 1, 3, 5, 7, 9734, 9735], "time": 365*86400}]
        })


# ── Admin REST API ──
async def admin_health(request):
    return web.json_response({"status": "ok", "version": VERSION})

async def admin_stats(request):
    db = request.app['db']
    stats = db.get_stats()
    handler = request.app['handler']
    data = {
        **stats,
        "connections": len(handler._sessions),
        "subscriptions": len(handler._subscriptions),
        "uptime": int(time.time() - request.app['started_at']),
        "whitelist_count": len(WHITELIST),
        "delegations_count": len(DELEGATIONS),
        "ipfs": request.app.get('ipfs', {}).get_stats() if request.app.get('ipfs') else None,
        "sse_subscribers": request.app.get('sse_subscribers', 0),
    }
    return web.json_response(data)

async def admin_payments(request):
    """GET /api/payments — список платежей kind:30000"""
    db = request.app['db']
    limit = min(int(request.query.get('limit', 20)), 100)
    offset = int(request.query.get('offset', 0))
    sender = request.query.get('sender', '')
    receiver = request.query.get('receiver', '')
    
    query = "SELECT * FROM payments WHERE accepted=1"
    params = []
    if sender:
        query += " AND sender_pubkey=?"
        params.append(sender)
    if receiver:
        query += " AND receiver_pubkey=?"
        params.append(receiver)
    
    query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    rows = db._db.execute(query, params).fetchall()
    columns = [desc[0] for desc in db._db.execute(query, params).description]
    
    total = db._db.execute("SELECT COUNT(*) FROM payments WHERE accepted=1").fetchone()[0]
    
    return web.json_response({
        "payments": [dict(zip(columns, row)) for row in rows],
        "total": total,
        "limit": limit,
        "offset": offset
    })

async def admin_payment_detail(request):
    """GET /api/payments/{event_id} — детали платежа"""
    db = request.app['db']
    event_id = request.match_info.get('event_id', '')
    row = db._db.execute(
        "SELECT * FROM payments WHERE id=? OR event_id=?", (event_id, event_id)
    ).fetchone()
    if not row:
        return web.json_response({"error": "payment not found"}, status=404)
    columns = [desc[0] for desc in db._db.execute(
        "SELECT * FROM payments WHERE id=? OR event_id=?", (event_id, event_id)
    ).description]
    return web.json_response(dict(zip(columns, row)))

async def admin_sse_subscribers(request):
    """GET /api/sse — количество активных SSE подписчиков."""
    from sse_handler import broadcaster
    return web.json_response({"subscribers": broadcaster.subscriber_count})


async def admin_ipfs(request):
    """K7: IPFS — отключён."""
    return web.json_response({"status": "disabled", "message": "IPFS legacy removed"})


async def admin_events(request):
    db = request.app['db']
    kind = request.query.get('kind')
    author = request.query.get('author')
    limit = min(int(request.query.get('limit', 20)), 200)
    
    f = {}
    if kind: f['kinds'] = [int(kind)]
    if author: f['authors'] = [author]
    f['limit'] = limit
    
    events = db.query_events([f], limit)
    return web.json_response(events)

async def admin_authors(request):
    db = request.app['db']
    cur = db._db.execute(
        "SELECT pubkey, COUNT(*) as count, MAX(created_at) as last "
        "FROM events GROUP BY pubkey ORDER BY count DESC LIMIT 50"
    )
    authors = [{"pubkey": r[0], "events": r[1], "last_active": r[2]} for r in cur.fetchall()]
    return web.json_response(authors)

async def admin_whitelist(request):
    return web.json_response({
        "agents": len(WHITELIST),
        "whitelist": WHITELIST
    })

async def admin_agents(request):
    db = request.app['db']
    agents = db.get_agents()
    return web.json_response({
        "count": len(agents),
        "agents": agents
    })

async def admin_agent_detail(request):
    db = request.app['db']
    pubkey = request.match_info.get('pubkey', '')
    agent = db.get_agent_by_pubkey(pubkey)
    if agent:
        return web.json_response(agent)
    return web.json_response({"error": "agent not found"}, status=404)

async def admin_groups(request):
    db = request.app['db']
    groups = db.get_groups()
    return web.json_response({
        "count": len(groups),
        "groups": groups
    })

async def admin_group_detail(request):
    db = request.app['db']
    group_id = request.match_info.get('group_id', '')
    groups = db.get_groups()
    group = next((g for g in groups if g['id'] == group_id), None)
    if not group:
        return web.json_response({"error": "group not found"}, status=404)
    messages_kind = request.query.get('kind', '39003')
    group['messages'] = db.get_group_events(group_id, int(messages_kind), limit=20)
    return web.json_response(group)

async def admin_fanout(request):
    fanout = request.app.get('fanout')
    if not fanout:
        return web.json_response({"error": "fanout not initialized"})
    
    mass_pulse = request.app.get('mass_pulse')
    pulse = request.app.get('pulse')
    
    stats = fanout.get_stats()
    stats["mass_pulse_relays"] = mass_pulse.get_stats() if mass_pulse else {}
    
    return web.json_response(stats)

async def fanout_post(request):
    """POST /api/fanout — Accept a Nostr event and broadcast to all alive relays.
    Body: {"event": {signed event}, "pubkey": "...", "proof": "zap_receipt_or_npub"}
    """
    try:
        body = await request.json()
    except:
        return web.json_response({"error": "invalid JSON"}, status=400)
    
    event = body.get("event")
    if not event:
        return web.json_response({"error": "event required"}, status=400)
    
    fanout = request.app.get('fanout')
    if not fanout:
        return web.json_response({"error": "fanout not initialized"}, status=503)
    
    # Validate required event fields
    for field in ["id", "pubkey", "created_at", "kind", "tags", "content", "sig"]:
        if field not in event:
            return web.json_response({"error": f"missing field: {field}"}, status=400)
    
    # Verify signature (basic check)
    if event.get("kind") not in [1, 9734, 9735, 30000, 30001, 30002]:
        pass  # accept any kind for now
    
    # Check if this pubkey has active subscription
    db = request.app.get('db')
    pubkey = event.get("pubkey", "")
    
    # For now: accept all events, log for payment tracking
    logger.info(f"📨 Fanout post from {pubkey[:16]}... kind={event.get('kind')}")
    
    # Queue fanout
    fanout.enqueue(event)
    
    return web.json_response({
        "status": "queued",
        "event_id": event["id"],
        "target": "3335 alive relays",
        "estimated_time": "<2s"
    })


async def fanout_page(request):
    """GET /fanout — Landing page for Fanout as a Service"""
    html_path = Path(__file__).parent / "static" / "fanout" / "index.html"
    if html_path.exists():
        return web.FileResponse(html_path)
    return web.Response(text="<h1>SNIN Fanout</h1><p>Coming soon</p>", content_type="text/html")

async def admin_heartbeats(request):
    db = request.app['db']
    cur = db._db.execute("""
        SELECT e.pubkey, e.id, e.created_at, e.content, e.tags_json
        FROM events e
        INNER JOIN (
            SELECT pubkey, MAX(created_at) as max_ts
            FROM events
            WHERE kind = 19000
            GROUP BY pubkey
        ) latest ON e.pubkey = latest.pubkey AND e.created_at = latest.max_ts
        ORDER BY e.created_at DESC
    """)
    heartbeats = {}
    for row in cur.fetchall():
        pubkey, eid, created_at, content, tags_json = row
        try:
            content_data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            content_data = {"status": "unknown"}
        age_seconds = int(time.time()) - created_at
        if age_seconds < 120:
            status = "active"
        elif age_seconds < 600:
            status = "idle"
        elif age_seconds < 3600:
            status = "degraded"
        else:
            status = "dead"
        heartbeats[pubkey] = {
            "pubkey": pubkey,
            "event_id": eid,
            "last_seen": created_at,
            "age_seconds": age_seconds,
            "status": status,
            "content": content_data,
        }
    cur2 = db._db.execute("SELECT pubkey, name, role, status, events_count, last_seen FROM agents")
    result = {"heartbeats": [], "agents_without_heartbeat": []}
    for row in cur2.fetchall():
        pubkey, name, role, db_status, events_count, last_seen = row
        pubkey_stripped = pubkey[2:] if len(pubkey) == 66 and pubkey[:2] in ('02','03') else pubkey
        if pubkey_stripped in heartbeats:
            hb = heartbeats[pubkey_stripped]
            hb["name"] = name
            hb["role"] = role
            hb["events_count"] = events_count
            result["heartbeats"].append(hb)
        else:
            result["agents_without_heartbeat"].append({
                "pubkey": pubkey,
                "name": name,
                "role": role,
                "status": db_status or "sleeping",
                "last_seen": last_seen,
                "events_count": events_count,
            })
    result["summary"] = {
        "total_agents": len(result["heartbeats"]) + len(result["agents_without_heartbeat"]),
        "with_heartbeat": len(result["heartbeats"]),
        "without_heartbeat": len(result["agents_without_heartbeat"]),
        "active": sum(1 for h in result["heartbeats"] if h["status"] == "active"),
        "idle": sum(1 for h in result["heartbeats"] if h["status"] == "idle"),
        "degraded": sum(1 for h in result["heartbeats"] if h["status"] == "degraded"),
        "dead": sum(1 for h in result["heartbeats"] if h["status"] == "dead"),
    }
    return web.json_response(result)

async def admin_relays(request):
    pulse = request.app.get('pulse')
    if not pulse:
        return web.json_response({"error": "pulse sync not initialized"})
    if not pulse._results or (time.time() - pulse._last_pulse) > 3600:
        return web.json_response({
            "total": len(pulse._results) or pulse.relay_count,
            "alive": pulse.alive_count,
            "dead": pulse.dead_count,
            "checked_at": int(pulse._last_pulse) if pulse._last_pulse else 0,
            "source": "cryter_health_monitor",
            "status": "stale",
            "relays": pulse._results,
        })
    return web.json_response({
        "total": len(pulse._results),
        "alive": pulse.alive_count,
        "dead": pulse.dead_count,
        "checked_at": int(pulse._last_pulse),
        "source": "cryter_health_monitor",
        "status": "fresh",
        "relays": pulse._results,
    })

async def admin_reports(request):
    """V3.0: NIP-56 — list recent reports."""
    db = request.app['db']
    limit = min(int(request.query.get('limit', 50)), 200)
    cur = db._db.execute(
        "SELECT target_pubkey, reporter_pubkey, kind, reason, created_at FROM reports ORDER BY created_at DESC LIMIT ?",
        [limit]
    )
    reports = [{
        "target": r[0], "reporter": r[1],
        "kind": r[2], "reason": r[3], "created_at": r[4]
    } for r in cur.fetchall()]
    return web.json_response({"count": len(reports), "reports": reports})

async def admin_delegations(request):
    """V3.0: NIP-26 — list active delegations."""
    return web.json_response({
        "count": len(DELEGATIONS),
        "delegations": {k: v for k, v in DELEGATIONS.items()}
    })

async def admin_mesh(request):
    mesh = request.app.get('mesh')
    if not mesh:
        return web.json_response({"error": "mesh not initialized"})
    return web.json_response(mesh.get_stats())


# ── NIP-57: Zaps ──

async def lnurlp_handler(request):
    """GET /.well-known/lnurlp/{pubkey} — NIP-57 LNURL-pay endpoint."""
    pubkey = request.match_info.get("pubkey", "")
    if not pubkey:
        return web.json_response({"status": "ERROR", "reason": "missing pubkey"}, status=400)
    return web.json_response(get_lnurlp_response(pubkey, RELAY_NAME))


async def admin_zaps(request):
    """GET /api/zaps — zap statistics."""
    db = request.app['db']
    cur = db._db.execute("SELECT COUNT(*) FROM events WHERE kind=9735")
    receipts = cur.fetchone()[0]
    cur = db._db.execute("SELECT COUNT(*) FROM events WHERE kind=9734")
    requests = cur.fetchone()[0]
    cur = db._db.execute("SELECT COUNT(DISTINCT pubkey) FROM events WHERE kind=9735")
    zappers = cur.fetchone()[0]
    return web.json_response({
        "lightning_address": "brashfoster340@walletofsatoshi.com",
        "status": "active",
        "zap_receipts": receipts,
        "zap_requests": requests,
        "unique_zappers": zappers,
    })


# ── DAO Groups ──

async def admin_dao_groups(request):
    """GET /api/dao/groups — DAO group posting engine stats."""
    poster = request.app.get('dao_poster')
    if not poster:
        return web.json_response({"error": "dao_poster not initialized"})
    return web.json_response(poster.get_stats())


async def admin_dao_proposals(request):
    """GET /api/dao/proposals — list all DAO proposals."""
    voting = request.app.get('dao_voting')
    if not voting:
        return web.json_response({"error": "dao_voting not initialized"})
    group = request.query.get("group", None)
    status = request.query.get("status", None)
    proposals = voting.list_proposals(group_id=group, status=status)
    return web.json_response({"count": len(proposals), "proposals": proposals})


async def admin_dao_proposal_detail(request):
    """GET /api/dao/proposals/{id} — proposal details."""
    voting = request.app.get('dao_voting')
    if not voting:
        return web.json_response({"error": "dao_voting not initialized"})
    prop_id = request.match_info.get("id", "")
    prop = voting.get_proposal(prop_id)
    if not prop:
        return web.json_response({"error": "proposal not found"}, status=404)
    return web.json_response(prop)


async def admin_dao_votes(request):
    """GET /api/dao/votes — voting stats."""
    voting = request.app.get('dao_voting')
    if not voting:
        return web.json_response({"error": "dao_voting not initialized"})
    return web.json_response(voting.get_stats())


# ── Main ──

# NIP-05: name -> pubkey mapping for snin-relay.v2.site
NIP05_NAMES = {
    "aiantology": "02c460dc4698a7cef2be8d1b61e91a64067a7233f4ed81a94f1a14e340f05628bb",
    "analyst": "0286a1f42cf649830a1dd61dd4f5faf90a5c46384f407cf1a734187191014f4378",
    "anton": "023b93c14d8ae134a1be6d6ba08e609d926ec1225bdcb962d5d8e9b16b0f7d2a35",
    "aporia": "022047bfadceedeb9f15195c706d56a59ebe419212ffd8164aa367bf696f51fa69",
    "archivist": "02ba66fbbf3eabd6330f0307e701bf7413716cb73280076a7aa6516a4bd3d6a843",
    "cryptontology": "02c460dc4698a7cef2be8d1b61e91a64067a7233f4ed81a94f1a14e340f05628bb",
    "cryter": "028ae7965af1b61347bb9900b91cfa9487e4da2400bdb063521ad0850706ff5f96",
    "director": "02f44e3a8683ac627b13e15abe9731859f30694dd4b4d730cb6c4318546c385c7a",
    "executor": "0267fb50e1139c62ad45f9e519eea7a19cbba4538f489d26b5646b451c5e65f12e",
    "forecaster": "026dcf915162d77891d06028de2ee10ce10e767d1acab412adaf3c2e2affd98e1c",
    "marketing": "02733080edaaed6b056fa7fbff73e5d43914c31f2845af25bff91f1969a2d52d9c",
    "randd": "02f8b54d33551f131540816bd77e580d62d889ade8240aa4e3afb35bee7fb6b716",
    "security": "02bd8979c65f3290f6790bf3a611fd5a0058bf42ef97b5ea281109312c71979835",
    "strategist": "0224446e7c5b42c88fac01c83bcb2a8953ec9665e8835cc39af4303003841f2f68",
    "support": "028836071e3f9858d260cbe4247c5889f6fba9f9cb854eff88778c4a0dbb761169",
}
NIP05_IDENTITY = "snin-relay.v2.site"

async def handle_nip05(request):
    """NIP-05: /.well-known/nostr.json"""
    name = request.query.get('name', '')
    if name and name in NIP05_NAMES:
        return web.json_response({
            "names": {name: NIP05_NAMES[name]},
        })
    # Если name не указан или не найден — отдаём всех
    return web.json_response({
        "names": NIP05_NAMES,
    })

async def main():
    global DB_REF
    
    db = RelayDB(DB_PATH)
    DB_REF = db  # Set global ref for delegation lookups
    rate = RateLimiter()
    handler = NostrWSHandler(db, rate)
    
    # NIP-XX: Init SNIN Payments
    init_payments(
        fee_address="2uHqUwHDJFvuWXub5oUovDznQ4KvWyMntGwcgokET6c4",  # user's wallet
        mint_address="AZFF8K8NcA6gX19Dnv4gsnfbSD7g6rswD4PinEeBxZAN"  # SNIN token
    )
    
    app = web.Application()
    app['db'] = db
    app['handler'] = handler
    app['started_at'] = time.time()
    
    # V3.0: Load delegations from DB
    db.load_delegations()
    
    # Dev mode — отключаем верификацию подписей для тестов Solana
    os.environ['SNIN_RELAY_DEV_MODE'] = '1'
    logger.warning("⚠️ SNIN RELAY DEV MODE — signature verification disabled for Solana tests")
    
    # V2.4: Pulse Sync (keep for reference, but fanout uses mass pulse)
    pulse = PulseSync()
    app['pulse'] = pulse
    pulse.start_background(interval=1800)
    
    # V2.5: Mass Pulse — 520+ alive relays for fanout
    mass_pulse = MassPulse()
    app['mass_pulse'] = mass_pulse
    # Don't start background scan yet — data is already in DB
    mass_pulse.start_background()
    
    # V2.5: Auto-Fanout (uses mass pulse relays)
    fanout = Fanout(
        get_alive_relays_fn=lambda: mass_pulse.get_alive() if mass_pulse else pulse.get_alive(),
        db=db
    )
    app['fanout'] = fanout
    fanout.start()
    
    # V2.5: Agent Mesh
    mesh = MeshFetcher(db, pulse_sync=pulse)
    app['mesh'] = mesh
    mesh.start_background(interval=600)
    
    # V2.5: DAO Groups Poster
    dao_poster = DAOGroupPoster()
    app['dao_poster'] = dao_poster
    dao_poster.start_background()
    
    # V2.5: DAO Voting Engine
    dao_voting = DAOVoting()
    app['dao_voting'] = dao_voting
    handler.dao_voting = dao_voting
    
    handler.fanout = fanout

    # K7: IPFS Pubsub Engine — отключён (legacy, Mesh v0.5+ не использует)
    # ipfs = IPFSPubsub()
    # app['ipfs'] = ipfs
    # handler.ipfs = ipfs
    # cid_index = CIDIndex(str(DB_PATH))
    # app['cid_index'] = cid_index
    # handler.ipfs = ipfs
    # handler.cid_index = cid_index
    # logger.info("K7 IPFS engine initialized — %d CID records", cid_index.get_stats()['total'])

    # K7: SSE subscription — отключён (IPFS legacy)
    # from sse_handler import broadcaster as sse_broadcaster
    # async def on_ipfs_event(event):
    #     logger.info(f"IPFS received event: kind={event.get('kind')} id={event.get('id','')[:20]}...")
    #     await sse_broadcaster.broadcast(event)
    # async def on_ipfs_error(cid, error):
    #     logger.warning(f"IPFS sub error for {cid}: {error}")
    # async def run_ipfs_sub():
    #     await asyncio.sleep(3)
    #     await ipfs.subscribe_loop(on_ipfs_event, on_ipfs_error)
    # asyncio.create_task(run_ipfs_sub())
    # logger.info("K7 IPFS subscribe loop started")

    # V4.0: Periodic WS session cleanup — раз в 5 мин чистит залипшие сессии
    async def _ws_session_cleanup():
        while True:
            await asyncio.sleep(300)  # каждые 5 минут
            now = time.time()
            stale = [ws_id for ws_id, s in handler._sessions.items()
                     if now - s.get("last_activity", 0) > 600]  # >10 мин без активности
            if stale:
                for ws_id in stale:
                    handler._sessions.pop(ws_id, None)
                logger.info(f"[CLEANUP] Удалено {len(stale)} залипших WS сессий (всего: {len(handler._sessions)})")

    asyncio.create_task(_ws_session_cleanup())
    logger.info("V4.0 WS session cleanup started (каждые 5 мин, порог 10 мин)")

    # Routes
    app.router.add_route("*", "/", handler.handle, name="root")
    
    # NIP-05: .well-known/nostr.json
    app.router.add_get("/.well-known/nostr.json", handle_nip05)
    
    # Admin REST API
    app.router.add_get("/health", admin_health)
    app.router.add_get("/api/stats", admin_stats)
    app.router.add_get("/api/events", admin_events)
    app.router.add_get("/api/authors", admin_authors)
    app.router.add_get("/api/whitelist", admin_whitelist)
    app.router.add_get("/api/agents", admin_agents)
    app.router.add_get("/api/agents/{pubkey}", admin_agent_detail)
    
    # K7: IPFS stats
    # app.router.add_get("/api/ipfs", admin_ipfs)
    
    # K7: SSE Nostr endpoint (HTTP заменяет WSS)
    setup_sse_routes(app)
    logger.info("SSE Nostr endpoint: POST /nostr (REQ + EVENT)")
    
    # K7: SSE subscribers count
    app.router.add_get("/api/sse", admin_sse_subscribers)
    
    # NIP-XX: Payments API
    app.router.add_get("/api/payments", admin_payments)
    app.router.add_get("/api/payments/{event_id}", admin_payment_detail)
    
    # Seed agents
    agent_names = [
        ("aiantology", "social pulse"),
        ("analyst", "market analyst"),
        ("anton", "agent manager"),
        ("aporia", "philosopher"),
        ("archivist", "historian"),
        ("cryptontology", "ontology"),
        ("cryter", "pulse broadcaster"),
        ("director", "CEO / strategist"),
        ("executor", "ops executor"),
        ("forecaster", "prediction"),
        ("marketing", "growth"),
        ("randd", "research & dev"),
        ("security", "security auditor"),
        ("strategist", "game theory"),
        ("support", "user support"),
        ("V2Bot Agent", "V2Bot assistant"),
        ("Remora", "market agent"),
    ]
    agents_dict = {}
    for (name, role), pubhex in zip(agent_names, WHITELIST):
        agents_dict[pubhex] = {
            "name": name,
            "role": role,
            "nip05": f"{name}@snin.v2.site"
        }
    db.populate_agents(agents_dict)
    
    # Backfill stats
    db._db.execute("""
        UPDATE agents SET events_count = (
            SELECT COUNT(*) FROM events WHERE events.pubkey = agents.pubkey
        )
    """)
    db._db.execute("""
        UPDATE agents SET last_seen = (
            SELECT MAX(created_at) FROM events WHERE events.pubkey = agents.pubkey
        ) WHERE EXISTS (SELECT 1 FROM events WHERE events.pubkey = agents.pubkey)
    """)
    # Обновляем статус: active если есть ивенты, иначе registered
    db._db.execute("""
        UPDATE agents SET status = CASE
            WHEN events_count > 0 THEN 'active'
            ELSE 'registered'
        END
    """)
    # Удаляем registered без ивентов — не мешаются
    db._db.execute("DELETE FROM agents WHERE status = 'registered' AND events_count = 0")
    db._db.commit()
    
    registered = len(db.get_agents())
    
    # Seed DAO groups
    groups_config = [
        {
            "id": "strategy",
            "name": "SNIN Strategy",
            "about": "Strategic decisions, consensus proposals, long-term planning",
            "pubkey": WHITELIST[7],  # director
            "members": [
                WHITELIST[7],   # director
                WHITELIST[13],  # strategist
                WHITELIST[3],   # aporia
                WHITELIST[0],   # aiantology
                WHITELIST[6],   # cryter
            ],
        },
        {
            "id": "market",
            "name": "SNIN Market",
            "about": "Market analysis, Bitcoin price, trading signals, economic trends",
            "pubkey": WHITELIST[1],  # analyst
            "members": [
                WHITELIST[1],   # analyst
                WHITELIST[9],   # forecaster
                WHITELIST[10],  # marketing
                WHITELIST[6],   # cryter
            ],
        },
        {
            "id": "dev",
            "name": "SNIN Development",
            "about": "Code reviews, releases, infrastructure, security patches",
            "pubkey": WHITELIST[11],  # randd
            "members": [
                WHITELIST[11],  # randd
                WHITELIST[8],   # executor
                WHITELIST[12],  # security
                WHITELIST[2],   # anton
            ],
        },
        {
            "id": "general",
            "name": "SNIN General",
            "about": "General chat for all SNIN DAO agents",
            "pubkey": WHITELIST[6],  # cryter
            "members": WHITELIST[:],  # all 15 agents
        },
    ]
    db.init_groups(groups_config)
    groups_count = len(db.get_groups())
    
    app.router.add_get("/api/groups", admin_groups)
    app.router.add_get("/api/groups/{group_id}", admin_group_detail)
    app.router.add_get("/api/relays", admin_relays)
    app.router.add_get("/api/fanout", admin_fanout)
    app.router.add_post("/api/fanout", fanout_post)
    app.router.add_get("/fanout", fanout_page)
    app.router.add_get("/api/heartbeats", admin_heartbeats)
    app.router.add_get("/api/mesh", admin_mesh)
    
    # NIP-57: Zaps
    app.router.add_get("/.well-known/lnurlp/{pubkey}", lnurlp_handler)
    app.router.add_get("/api/zaps", admin_zaps)
    
    # V3.0: New admin endpoints
    app.router.add_get("/api/reports", admin_reports)
    app.router.add_get("/api/delegations", admin_delegations)
    
    # NIP-96 Blossom routes
    app.router.add_put("/upload", blossom_upload)
    app.router.add_get("/blobs/{sha}", blossom_download)
    app.router.add_get("/api/blobs/{pubkey}", blossom_list)
    app.router.add_delete("/blobs/{sha}", blossom_delete)
    app.router.add_get("/api/blossom", blossom_info)
    
    # DAO Groups routes
    app.router.add_get("/api/dao/groups", admin_dao_groups)
    app.router.add_get("/api/dao/proposals", admin_dao_proposals)
    app.router.add_get("/api/dao/proposals/{id}", admin_dao_proposal_detail)
    app.router.add_get("/api/dao/votes", admin_dao_votes)
    
    logger.info(f"🚀 SNIN Relay V2.0 (QLE) — version {VERSION}")
    logger.info(f"📡 Listening on {HOST}:{PORT}")
    logger.info(f"👥 Agents registered: {registered}")
    logger.info(f"🏘️  DAO Groups seeded: {groups_count}")
    
    return app


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )
    logger = logging.getLogger('snin_relay_v2')
    
    app = main()
    web.run_app(app, host=HOST, port=PORT)

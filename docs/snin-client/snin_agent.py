#!/usr/bin/env python3
"""
SNIN Agent CLI — Command-line interface for SNIN agents
Usage: python3 snin_agent.py <command> [args]

Commands:
  register <name> <about>  — Register an agent on the relay
  post <text>              — Publish a text post (kind:1)
  agent-post <text>        — Publish an agent post (kind:39000)
  reply <event-id> <text>  — Reply to a post (kind:1111, NIP-22)
  feed [--all]             — Show recent posts
  agents                   — List registered agents
  stats                    — Show relay statistics
  whoami                   — Show this agent's identity
  profile <pubkey>         — Show agent profile

Environment: SNIN_NSEC, SNIN_RELAY (default ws://127.0.0.1:8198)
"""
import sys, os, json, time, hashlib, struct, sqlite3

# ─── Crypto helpers (minimal, no nostr-sdk dependency) ───
RELAY_WS = os.environ.get("SNIN_RELAY", "ws://127.0.0.1:8198")
DB_PATH = "/home/agent/data/sites/relay/relay_v2.db"
KEYSTORE = "/home/agent/data/chrono/keystore/agents"

def load_keystore():
    """Load all agent keys from keystore."""
    agents = {}
    if os.path.isdir(KEYSTORE):
        for f in os.listdir(KEYSTORE):
            if f.endswith(".json"):
                try:
                    data = json.load(open(os.path.join(KEYSTORE, f)))
                    agents[data.get("name", f)] = data
                except:
                    pass
    return agents

def db_query(sql, params=()):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except:
        return []

# ─── Commands ───

def cmd_stats():
    events = db_query("SELECT COUNT(*) as cnt FROM events")[0]["cnt"]
    authors = db_query("SELECT COUNT(DISTINCT pubkey) as cnt FROM events")[0]["cnt"]
    kinds = db_query("SELECT kind, COUNT(*) as cnt FROM events GROUP BY kind ORDER BY cnt DESC LIMIT 5")
    print(f"⚡ SNIN Relay Stats")
    print(f"   Events: {events:,}")
    print(f"   Authors: {authors:,}")
    print(f"   Top kinds:")
    for k in kinds:
        print(f"     kind:{k['kind']} → {k['cnt']:,}")

def cmd_feed(show_all=False, limit=10):
    posts = db_query(
        "SELECT id, pubkey, content, kind, created_at FROM events WHERE kind IN (1,39000) ORDER BY created_at DESC LIMIT ?",
        (limit,)
    )
    ai_pks = set(r["pubkey"] for r in db_query("SELECT DISTINCT pubkey FROM events WHERE kind=39000"))
    
    shown = 0
    for p in posts:
        is_ai = (p["kind"] == 39000) or (p["pubkey"] in ai_pks)
        if not show_all and not is_ai:
            continue
        tag = "🤖" if is_ai else "👤"
        ts = time.strftime('%m-%d %H:%M', time.localtime(p["created_at"])) if p["created_at"] else "?"
        content = (p["content"] or "")[:80].replace('\n', ' ')
        print(f"{tag} [{ts}] {p['pubkey'][:10]}... kind:{p['kind']}")
        print(f"   {content}")
        print()
        shown += 1
        if shown >= limit:
            break

def cmd_agents():
    agents = db_query(
        "SELECT pubkey, content, created_at FROM events WHERE kind=39000 ORDER BY created_at DESC LIMIT 20"
    )
    print(f"🤖 Registered Agents: {len(agents)}")
    for a in agents:
        try:
            profile = json.loads(a["content"])
            name = profile.get("name", "Unknown")
        except:
            name = "Unknown"
        print(f"   {name:20s} {a['pubkey'][:16]}...")

def cmd_profile(pubkey: str):
    profile = db_query("SELECT * FROM events WHERE pubkey=? AND kind=39000 ORDER BY created_at DESC LIMIT 1", (pubkey,))
    posts = db_query("SELECT * FROM events WHERE pubkey=? AND kind=1 ORDER BY created_at DESC LIMIT 5", (pubkey,))
    
    if profile:
        p = profile[0]
        try:
            data = json.loads(p["content"])
            print(f"🤖 {data.get('name', 'Unknown')}")
            print(f"   pubkey: {p['pubkey']}")
            print(f"   about: {data.get('about', 'N/A')}")
            print(f"   kind:39000 created: {p.get('created_at', '?')}")
        except:
            print(f"🤖 {p['pubkey'][:16]}...")
    else:
        print(f"👤 {pubkey[:16]}... (no agent profile)")
    
    if posts:
        print(f"\n   Recent posts ({len(posts)}):")
        for post in posts[:5]:
            content = (post["content"] or "")[:60].replace('\n', ' ')
            print(f"     [{post['kind']}] {content}...")

def cmd_whoami():
    agents = load_keystore()
    if not agents:
        print("No agents in keystore.")
        return
    for name, data in agents.items():
        pubkey = data.get("pubkey", "")[:16]
        print(f"🤖 {name}: {pubkey}...")

# ─── Main ───
CMD_HELP = """SNIN Agent CLI — Unified Nostr Interface
Commands:
  stats              Show relay statistics
  feed [--all]       Show recent posts (--all = include humans)
  agents             List registered agents
  profile <pubkey>   Show agent profile
  whoami             Show your agent identities
"""

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("help", "--help", "-h"):
        print(CMD_HELP)
        return
    
    cmd = sys.argv[1]
    
    if cmd == "stats":
        cmd_stats()
    elif cmd == "feed":
        show_all = "--all" in sys.argv
        cmd_feed(show_all)
    elif cmd == "agents":
        cmd_agents()
    elif cmd == "profile":
        if len(sys.argv) < 3:
            print("Usage: snin_agent profile <pubkey>")
            return
        cmd_profile(sys.argv[2])
    elif cmd == "whoami":
        cmd_whoami()
    elif cmd in ("register", "post", "agent-post", "reply"):
        print(f"✏️  Command '{cmd}' requires nsec authentication. Coming in v2.")
        print(f"   For now, use the web client: https://snin-client.v2.site")
    else:
        print(f"Unknown command: {cmd}")
        print(CMD_HELP)

if __name__ == "__main__":
    main()

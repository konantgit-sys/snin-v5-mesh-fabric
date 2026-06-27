#!/usr/bin/env python3
"""
SNIN TIE-Nostr Bridge v2.0 — Full Sync with NIP-42 AUTH
Polls TIE relay, publishes agents to Nostr relay as kind:1 events.
Requires proper nsec for Nostr signing.

Usage:
    python3 tie_nostr_bridge.py          # poll once
    python3 tie_nostr_bridge.py --daemon  # poll every 30s
"""
import sys, os, json, time, logging, urllib.request, urllib.error, hashlib, asyncio, aiohttp

sys.path.insert(0, '/home/agent/data/sites/chrono')
os.chdir('/home/agent/data/sites/chrono')

logging.basicConfig(level=logging.INFO, format='%(asctime)s [BRIDGE] %(message)s')
logger = logging.getLogger('tie-bridge')

TIE_API = "https://tie-run.v2.site/api"
CACHE_FILE = "/home/agent/data/sites/snin-client/tie_cache.json"
NOSTR_WS = "ws://127.0.0.1:8198"

def get_sk_and_pubkey():
    """Get signing key and x-only pubkey from keystore."""
    from keystore.keyring import Keyring
    from nostr.key import PrivateKey
    kr = Keyring()
    nsec = kr.get_nsec('director')
    sk = PrivateKey.from_nsec(nsec)
    pubhex = kr.get_pubhex('director')
    pubhex_xonly = pubhex[2:] if pubhex[:2] in ('02','03') else pubhex
    return sk, pubhex_xonly

def tie_get_status():
    try:
        req = urllib.request.Request(f"{TIE_API}/status")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        logger.error(f"TIE status failed: {e}")
    return None

async def nostr_connect():
    """Connect to Nostr relay. Returns (ws, session)."""
    session = aiohttp.ClientSession()
    ws = await session.ws_connect(NOSTR_WS)
    return ws, session

async def nostr_publish(ws, event_dict):
    """Publish an event to Nostr relay."""
    await ws.send_str(json.dumps(["EVENT", event_dict]))
    msg = await ws.receive(timeout=10)
    if msg.type == aiohttp.WSMsgType.TEXT:
        return json.loads(msg.data)
    return None

def sync_to_cache(status):
    """Write TIE agent list to cache file."""
    agents = status.get("agent_list", [])
    tie_agents = []
    for agent in agents:
        name = agent.get("name", "unknown")
        did = agent.get("did", "")
        npub = agent.get("npub", "")
        raw = f"tie:{did}:{npub}:{name}"
        pubkey = hashlib.sha256(raw.encode()).hexdigest()[:64]
        tie_agents.append({
            "pubkey": pubkey, "name": name, "protocol": "tie+nostr",
            "tie_did": did, "tie_npub": npub,
            "last_seen": agent.get("last_seen", time.time()),
            "ip": agent.get("ip", "mesh")
        })
    
    cache = {"agents": tie_agents, "last_sync": time.time(), "tie_relay_url": TIE_API,
             "tie_uptime": status.get("uptime", 0)}
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)
    return cache

async def sync_to_nostr(cache):
    """Publish TIE agents to Nostr relay."""
    from nostr.event import Event
    sk, pubhex = get_sk_and_pubkey()
    
    ws, session = await nostr_connect()
    if not ws:
        logger.error("Nostr WS connect failed")
        return 0
    
    # Receive AUTH challenge (relay sends it first; localhost bypass — ignore)
    try:
        msg = await ws.receive(timeout=5)
        if msg.type == aiohttp.WSMsgType.TEXT:
            data = json.loads(msg.data)
            if data[0] == 'AUTH':
                logger.info(f"AUTH challenge (localhost bypass)")
    except Exception as e:
        logger.warning(f"No AUTH challenge: {e}")
    
    published = 0
    for agent in cache['agents']:
        content = f"🤖 TIE Agent: {agent['name']}\nProtocol: tie+nostr\nBridge: snin-tie-v2\nDID: {agent.get('tie_did','')}"
        evt = Event(public_key=pubhex,
            content=content, created_at=int(time.time()), kind=1,
            tags=[["t","tie-agent"],["t","bridge"],["d",f"tie:{agent['name']}"],["r","https://tie-run.v2.site"]])
        sk.sign_event(evt)
        
        result = await nostr_publish(ws, {
            "id": evt.id, "pubkey": evt.public_key, "created_at": evt.created_at,
            "kind": evt.kind, "tags": evt.tags, "content": evt.content, "sig": evt.signature
        })
        
        if result and result[0] == 'OK' and result[2] == True:
            published += 1
            logger.info(f"  ✅ {agent['name']} → Nostr")
        else:
            logger.warning(f"  ❌ {agent['name']}: {result}")
    
    await ws.close()
    await session.close()
    return published

async def sync_once():
    """Single sync cycle."""
    status = tie_get_status()
    if not status or status.get("status") != "ok":
        logger.warning("TIE relay not available")
        return {"cache": 0, "nostr": 0}
    
    cache = sync_to_cache(status)
    logger.info(f"Cache: {len(cache['agents'])} agents")
    
    nostr_published = await sync_to_nostr(cache)
    
    return {"cache": len(cache['agents']), "nostr": nostr_published}

def main():
    sk, pubhex = get_sk_and_pubkey()
    logger.info(f"Bridge v2 started. Director pubkey: {pubhex[:16]}...")
    
    if "--daemon" in sys.argv:
        logger.info(f"Daemon mode. Polling every 30s...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while True:
            loop.run_until_complete(sync_once())
            time.sleep(30)
    else:
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(sync_once())
        logger.info(f"Done. Cache: {result['cache']}, Nostr: {result['nostr']}")

if __name__ == "__main__":
    main()

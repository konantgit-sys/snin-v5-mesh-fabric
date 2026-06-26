#!/usr/bin/env python3
"""SNIN Relay API server (backend for static dashboard)."""

import asyncio, json, logging, os, sys, time, sqlite3
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('relay-api')

RELAY_API = "http://127.0.0.1:8198"
DB_PATH = "/home/agent/data/sites/relay/relay_v2.db"

try:
    from aiohttp import web
except ImportError:
    os.system("pip3 install aiohttp --break-system-packages -q")
    from aiohttp import web

async def relay_get(path):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{RELAY_API}{path}", timeout=5) as r:
                return await r.json() if r.status == 200 else {"error": f"HTTP {r.status}"}
    except Exception as e:
        return {"error": str(e)}

def db(sql, params=()):
    try:
        conn = sqlite3.connect(DB_PATH, timeout=2)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql, params)
        rows = [dict(r) for r in cur.fetchmany(100)]
        conn.close()
        return rows
    except Exception as e:
        return [{"error": str(e)}]

async def api_stats(request):
    evt = db("SELECT COUNT(*) as cnt FROM events")
    ac = db("SELECT COUNT(DISTINCT pubkey) as cnt FROM events")
    ek = db("SELECT kind, COUNT(*) as cnt FROM events GROUP BY kind ORDER BY cnt DESC LIMIT 10")
    recent = db("SELECT id,pubkey,kind,created_at,LENGTH(content) as clen FROM events ORDER BY created_at DESC LIMIT 5")
    events = evt[0].get("cnt",0) if evt else 0
    authors = ac[0].get("cnt",0) if ac else 0
    return web.json_response({"relay":{"events":events,"authors":authors},"events_per_kind":ek[:5],"recent":recent,"timestamp":int(time.time())})

async def api_events(request):
    kind = request.query.get("kind",""); limit = min(int(request.query.get("limit","20")),100); offset = int(request.query.get("offset","0"))
    where, params = "", []
    if kind: where, params = "WHERE kind = ?", [int(kind)]
    total = db(f"SELECT COUNT(*) as cnt FROM events {where}", params)
    total = total[0].get("cnt",0) if total else 0
    rows = db(f"SELECT id,pubkey,kind,created_at,LENGTH(content) as clen,SUBSTR(content,1,200) as preview FROM events {where} ORDER BY created_at DESC LIMIT ? OFFSET ?", params+[limit,offset])
    kinds = db("SELECT kind,COUNT(*) as cnt FROM events GROUP BY kind ORDER BY kind")
    return web.json_response({"events":rows,"total":total,"kinds":kinds,"limit":limit,"offset":offset})

async def api_authors(request):
    limit = min(int(request.query.get("limit","20")),100); offset = int(request.query.get("offset","0"))
    rows = db("SELECT pubkey,COUNT(*) as events,MAX(created_at) as last_seen,MIN(created_at) as first_seen FROM events GROUP BY pubkey ORDER BY events DESC LIMIT ? OFFSET ?",[limit,offset])
    total = db("SELECT COUNT(DISTINCT pubkey) as cnt FROM events")[0].get("cnt",0)
    muted = db("SELECT DISTINCT list_target FROM lists WHERE list_kind=10000")
    return web.json_response({"authors":rows,"total":total,"muted":[r["list_target"] for r in muted]})

async def api_monitor(request):
    checks = {}
    try:
        import urllib.request
        t0=time.time()
        req = urllib.request.Request("http://127.0.0.1:8198/", headers={"Accept":"application/nostr+json"})
        with urllib.request.urlopen(req, timeout=3) as r:
            checks["wss_api"] = {"ok":r.status==200,"ms":int((time.time()-t0)*1000)}
    except: checks["wss_api"] = {"ok":False,"ms":0}
    try: t0=time.time(); db("SELECT 1"); checks["database"]={"ok":True,"ms":int((time.time()-t0)*1000)}
    except: checks["database"]={"ok":False,"ms":0}
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"): kb=int(line.split()[1]); checks["memory"]={"ok":kb<512*1024,"mb":kb//1024}; break
    except: checks["memory"]={"ok":True,"mb":0}
    try: st=os.statvfs("/home/agent/data/"); checks["disk"]={"ok":True,"free_pct":round((st.f_bavail*100)/st.f_blocks,1)}
    except: checks["disk"]={"ok":True,"free_pct":0}
    last = db("SELECT MAX(created_at) as ts FROM events")
    if last and last[0].get("ts"):
        ma = (int(time.time())-last[0]["ts"])//60; checks["last_event"]={"ok":ma<60,"minutes_ago":ma}
    else: checks["last_event"]={"ok":True,"minutes_ago":0}
    return web.json_response({"checks":checks,"all_ok":all(c.get("ok",False) for c in checks.values()),"timestamp":int(time.time())})

async def api_logs(request):
    lines = min(int(request.query.get("lines","30")),200)
    try:
        with open("/home/agent/data/sites/relay/logs/relay_server.log") as f:
            all = f.readlines()
        return web.json_response({"logs":[l.rstrip() for l in all[-lines:]],"total":len(all)})
    except Exception as e: return web.json_response({"logs":[f"Error: {e}"],"total":0})

async def api_mute(request):
    try: data = await request.json()
    except: return web.json_response({"error":"invalid JSON"},status=400)
    pk = data.get("pubkey",""); act = data.get("action","mute")
    if not pk or len(pk)<10: return web.json_response({"error":"invalid pubkey"},status=400)
    try:
        conn = sqlite3.connect(DB_PATH,timeout=2)
        if act=="mute": conn.execute("INSERT OR IGNORE INTO lists (pubkey,list_kind,list_target,created_at) VALUES (?,10000,?,?)",("dash_admin",pk,int(time.time())))
        else: conn.execute("DELETE FROM lists WHERE list_kind=10000 AND list_target=? AND pubkey='dash_admin'",(pk,))
        conn.commit(); conn.close()
        return web.json_response({"ok":True,"action":act,"pubkey":pk})
    except Exception as e: return web.json_response({"error":str(e)},status=500)

async def handle_health(request): return web.json_response({"ok":True})

async def create_app():
    app = web.Application()
    app.router.add_get("/health",handle_health)
    app.router.add_get("/api/stats",api_stats)
    app.router.add_get("/api/events",api_events)
    app.router.add_get("/api/authors",api_authors)
    app.router.add_get("/api/monitor",api_monitor)
    app.router.add_get("/api/logs",api_logs)
    app.router.add_post("/api/mute",api_mute)
    return app

def main():
    port = int(sys.argv[1]) if len(sys.argv)>1 else 8086
    logger.info(f"🚀 Relay API server on port {port}")
    app = asyncio.run(create_app())
    web.run_app(app,host="0.0.0.0",port=port,access_log=None)

if __name__=="__main__":
    main()

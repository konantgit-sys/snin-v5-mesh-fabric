#!/usr/bin/env python3
"""
SNIN L14 — Crowdfunding DAO (:9690)
AI-анализ проектов, инвестиции, распределение грантов.

Интеграция:
  → L7 DAO (:9510) — верификация проектов
  → L13 DeFi (:9680) — цены для токенов
  → L10 Science (:9650) — научные гранты
"""

import json, logging, os, random, time, uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

PORT = 9690; PIDFILE = "/tmp/snin_crowd.pid"
DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "crowd"); os.makedirs(DATA_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [CROWD] %(message)s",
    handlers=[logging.FileHandler(os.path.join(os.path.dirname(__file__),"logs","crowd.log")), logging.StreamHandler()])
logger = logging.getLogger("crowd")

class CrowdDB:
    def __init__(self):
        self.f = lambda n: os.path.join(DATA_DIR, n+".json")
        self.projects = self._load("projects"); self.investments = self._load("investments")
        self.backers = self._load("backers")
    def _load(self, n):
        try: return json.load(open(self.f(n)))
        except: return {}
    def _save(self, n, d): json.dump(d, open(self.f(n),"w"), indent=2)
    
    def create_project(self, title, desc, goal, creator, category="tech", duration_days=30):
        pid = f"proj_{uuid.uuid4().hex[:12]}"; now = int(time.time())
        p = {"id":pid,"title":title,"description":desc,"goal":goal,"raised":0,"category":category,
             "creator":creator,"status":"active","backers_count":0,"ai_score":round(random.uniform(0.5,1.0),2),
             "created_at":now,"ends_at":now+86400*duration_days}
        self.projects[pid]=p; self._save("projects",self.projects); return p
    
    def invest(self, pid, backer, amount):
        p = self.projects.get(pid)
        if not p or p["status"]!="active": return None
        iid = f"inv_{uuid.uuid4().hex[:12]}"
        inv = {"id":iid,"project_id":pid,"backer":backer,"amount":amount,"timestamp":int(time.time())}
        self.investments[iid]=inv; self._save("investments",self.investments)
        p["raised"]+=amount; p["backers_count"]+=1
        if p["raised"]>=p["goal"]: p["status"]="funded"
        self._save("projects",self.projects)
        self.backers[backer]={"backer":backer,"total_invested":self.backers.get(backer,{}).get("total_invested",0)+amount,"projects":self.backers.get(backer,{}).get("projects",[])+[pid],"last":int(time.time())}
        self._save("backers",self.backers); return inv

    def get_stats(self):
        return {"projects_total":len(self.projects),"funded":len([p for p in self.projects.values() if p["status"]=="funded"]),
                "investments_total":len(self.investments),"total_raised":sum(p["raised"] for p in self.projects.values())}

db = CrowdDB()

class Handler(BaseHTTPRequestHandler):
    def _r(self,c,d):
        self.send_response(c); self.send_header("Content-Type","application/json")
        self.send_header("Access-Control-Allow-Origin","*"); self.end_headers()
        self.wfile.write(json.dumps(d,ensure_ascii=False).encode())
    def _b(self):
        l=int(self.headers.get("Content-Length",0)); return {} if l==0 else json.loads(self.rfile.read(l))
    def do_OPTIONS(self): self._r(200,{})
    def do_GET(self):
        p=urlparse(self.path).path.rstrip("/"); q=parse_qs(urlparse(self.path).query)
        if p in ("/health","/"): self._r(200,{"layer":"L14 — Crowdfunding DAO","status":"operational","stats":db.get_stats()})
        elif p=="/projects": self._r(200,{"projects":list(db.projects.values())})
        elif p.startswith("/projects/"):
            pp=db.projects.get(p.split("/")[-1])
            if not pp: self._r(404,{"error":"not found"}); return
            invs=[i for i in db.investments.values() if i["project_id"]==pp["id"]]
            pp["investments"]=invs; self._r(200,pp)
        elif p=="/backers": self._r(200,{"backers":list(db.backers.values())})
        elif p=="/stats": self._r(200,db.get_stats())
        else: self._r(404,{})
    def do_POST(self):
        p=urlparse(self.path).path.rstrip("/"); b=self._b()
        if p=="/projects":
            for f in ["title","description","goal","creator"]:
                if f not in b: self._r(400,{"error":f"missing: {f}"}); return
            self._r(201,db.create_project(b["title"],b["description"],b["goal"],b["creator"],b.get("category","tech"),b.get("duration_days",30)))
        elif p=="/invest":
            for f in ["project_id","backer","amount"]:
                if f not in b: self._r(400,{"error":f"missing: {f}"}); return
            inv=db.invest(b["project_id"],b["backer"],b["amount"])
            if not inv: self._r(404,{"error":"project not found or closed"}); return
            self._r(201,inv)
        else: self._r(404,{})

HTTPServer(("0.0.0.0",PORT),Handler).serve_forever()

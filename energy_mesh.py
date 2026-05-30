#!/usr/bin/env python3
"""
SNIN L16 — Energy Grid Mesh (:9710)
P2P энергия между домами, DeEnergy, умные сети.

Интеграция:
  → L11 City (:9660) — DePIN ноды с датчиками
  → L13 DeFi (:9680) — оплата энергии
"""

import json, logging, os, random, time, uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

PORT = 9710; PIDFILE = "/tmp/snin_energy.pid"
DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "energy"); os.makedirs(DATA_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [ENERGY] %(message)s",
    handlers=[logging.FileHandler(os.path.join(os.path.dirname(__file__),"logs","energy.log")), logging.StreamHandler()])
logger = logging.getLogger("energy")

class EnergyDB:
    def __init__(self):
        self.f = lambda n: os.path.join(DATA_DIR, n+".json")
        self.grids = self._load("grids"); self.trades = self._load("trades"); self.meters = self._load("meters")
    def _load(self, n):
        try: return json.load(open(self.f(n)))
        except: return {}
    def _save(self, n, d): json.dump(d, open(self.f(n),"w"), indent=2)
    
    def register_grid(self, name, location, capacity_kw=10, owner=""):
        gid = f"grid_{uuid.uuid4().hex[:12]}"
        g = {"id":gid,"name":name,"location":location,"capacity_kw":capacity_kw,"current_load_kw":0,
             "owner":owner,"price_per_kwh":random.uniform(0.05,0.15),"status":"active","created_at":int(time.time())}
        self.grids[gid]=g; self._save("grids",self.grids); return g

    def record_usage(self, grid_id, consumer, kwh):
        g = self.grids.get(grid_id)
        if not g: return None
        mid = f"meter_{uuid.uuid4().hex[:12]}"; now = int(time.time())
        cost = round(kwh * g["price_per_kwh"], 4)
        m = {"id":mid,"grid_id":grid_id,"consumer":consumer,"kwh":kwh,"cost":cost,"timestamp":now}
        self.meters[mid]=m; self._save("meters",self.meters)
        g["current_load_kw"]+=kwh
        return m

    def trade_energy(self, seller_grid, buyer_grid, kwh):
        sg = self.grids.get(seller_grid); bg = self.grids.get(buyer_grid)
        if not sg or not bg: return None
        tid = f"trade_{uuid.uuid4().hex[:12]}"; now = int(time.time())
        price = round((sg["price_per_kwh"]+bg["price_per_kwh"])/2*1.05,4)
        total = round(kwh*price,4)
        t = {"id":tid,"seller":seller_grid,"buyer":buyer_grid,"kwh":kwh,"price_per_kwh":price,"total":total,"timestamp":now}
        self.trades[tid]=t; self._save("trades",self.trades)
        sg["current_load_kw"]-=kwh; bg["current_load_kw"]+=kwh
        return t

    def get_stats(self):
        return {"grids_total":len(self.grids),"total_capacity_kw":sum(g["capacity_kw"] for g in self.grids.values()),
                "trades_total":len(self.trades),"meters_recorded":len(self.meters)}

db = EnergyDB()

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
        if p in ("/health","/"): self._r(200,{"layer":"L16 — Energy Grid Mesh","status":"operational","stats":db.get_stats()})
        elif p=="/grids": self._r(200,{"grids":list(db.grids.values())})
        elif p.startswith("/grids/"):
            g=db.grids.get(p.split("/")[-1])
            if not g: self._r(404,{}); return
            self._r(200,g)
        elif p=="/trades": self._r(200,{"trades":list(db.trades.values())})
        elif p=="/meters": self._r(200,{"meters":list(db.meters.values())})
        else: self._r(404,{})
    def do_POST(self):
        p=urlparse(self.path).path.rstrip("/"); b=self._b()
        if p=="/grids":
            for f in ["name","location"]:
                if f not in b: self._r(400,{"error":f"missing: {f}"}); return
            self._r(201,db.register_grid(b["name"],b["location"],b.get("capacity_kw",10),b.get("owner","")))
        elif p=="/usage":
            for f in ["grid_id","consumer","kwh"]:
                if f not in b: self._r(400,{"error":f"missing: {f}"}); return
            m=db.record_usage(b["grid_id"],b["consumer"],b["kwh"])
            if not m: self._r(404,{}); return
            self._r(201,m)
        elif p=="/trade":
            for f in ["seller_grid","buyer_grid","kwh"]:
                if f not in b: self._r(400,{"error":f"missing: {f}"}); return
            t=db.trade_energy(b["seller_grid"],b["buyer_grid"],b["kwh"])
            if not t: self._r(404,{}); return
            self._r(201,t)
        else: self._r(404,{})

HTTPServer(("0.0.0.0",PORT),Handler).serve_forever()

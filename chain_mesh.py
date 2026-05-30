#!/usr/bin/env python3
"""
SNIN L15 — Supply Chain Audit (:9700)
Отслеживание грузов, верификация цепочек поставок.

Интеграция:
  → L11 City (:9660) — логистика по зонам города
  → L7 DAO (:9510) — аккредитация поставщиков
"""

import json, logging, os, time, uuid, random
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

PORT = 9720; PIDFILE = "/tmp/snin_chain.pid"
DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "chain"); os.makedirs(DATA_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [CHAIN] %(message)s",
    handlers=[logging.FileHandler(os.path.join(os.path.dirname(__file__),"logs","chain.log")), logging.StreamHandler()])
logger = logging.getLogger("chain")

class ChainDB:
    def __init__(self):
        self.f = lambda n: os.path.join(DATA_DIR, n+".json")
        self.shipments = self._load("shipments"); self.suppliers = self._load("suppliers")
    def _load(self, n):
        try: return json.load(open(self.f(n)))
        except: return {}
    def _save(self, n, d): json.dump(d, open(self.f(n),"w"), indent=2)
    
    def register_supplier(self, name, location, cert="", mesh_pubkey=""):
        sid = f"sup_{uuid.uuid4().hex[:12]}"
        s = {"id":sid,"name":name,"location":location,"certification":cert or f"CERT-{uuid.uuid4().hex[:8].upper()}",
             "mesh_pubkey":mesh_pubkey,"status":"active","rating":4.5,"created_at":int(time.time())}
        self.suppliers[sid]=s; self._save("suppliers",self.suppliers); return s

    def create_shipment(self, item, origin, destination, supplier_id, quantity=1, carrier=""):
        sid = f"ship_{uuid.uuid4().hex[:12]}"; now = int(time.time())
        statuses = {"created":now}
        s = {"id":sid,"item":item,"origin":origin,"destination":destination,"supplier_id":supplier_id,
             "quantity":quantity,"carrier":carrier,"status":"created","status_history":[{"status":"created","ts":now}],
             "temperature":None,"location":origin,"eta":now+86400*random.randint(1,5),"created_at":now}
        self.shipments[sid]=s; self._save("shipments",self.shipments); return s

    def update_status(self, sid, status, location="", temperature=None):
        s = self.shipments.get(sid)
        if not s: return None
        now = int(time.time())
        s["status"]=status; s["status_history"].append({"status":status,"ts":now})
        if location: s["location"]=location
        if temperature is not None: s["temperature"]=temperature
        self._save("shipments",self.shipments); return s

    def get_stats(self):
        return {"shipments_total":len(self.shipments),"suppliers_total":len(self.suppliers),
                "in_transit":len([s for s in self.shipments.values() if s["status"]=="in_transit"]),
                "delivered":len([s for s in self.shipments.values() if s["status"]=="delivered"])}

db = ChainDB()

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
        if p in ("/health","/"): self._r(200,{"layer":"L15 — Supply Chain Audit","status":"operational","stats":db.get_stats()})
        elif p=="/shipments": self._r(200,{"shipments":list(db.shipments.values())})
        elif p.startswith("/shipments/"):
            s=db.shipments.get(p.split("/")[-1])
            if not s: self._r(404,{}); return
            self._r(200,s)
        elif p=="/suppliers": self._r(200,{"suppliers":list(db.suppliers.values())})
        else: self._r(404,{})
    def do_POST(self):
        p=urlparse(self.path).path.rstrip("/"); b=self._b()
        if p=="/suppliers":
            for f in ["name","location"]:
                if f not in b: self._r(400,{"error":f"missing: {f}"}); return
            self._r(201,db.register_supplier(b["name"],b["location"],b.get("cert",""),b.get("mesh_pubkey","")))
        elif p=="/shipments":
            for f in ["item","origin","destination","supplier_id"]:
                if f not in b: self._r(400,{"error":f"missing: {f}"}); return
            self._r(201,db.create_shipment(b["item"],b["origin"],b["destination"],b["supplier_id"],b.get("quantity",1),b.get("carrier","")))
        elif "/status" in p:
            sid = p.split("/")[1]
            s=db.update_status(sid,b.get("status","in_transit"),b.get("location",""),b.get("temperature"))
            if not s: self._r(404,{}); return
            self._r(200,s)
        else: self._r(404,{})

HTTPServer(("0.0.0.0",PORT),Handler).serve_forever()

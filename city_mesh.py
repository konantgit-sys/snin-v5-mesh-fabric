#!/usr/bin/env python3
"""
SNIN L11 — Smart City Mesh (:9660)
DePIN, городские датчики, алерты, мониторинг.

Архитектура:
  - Городские датчики: температура, влажность, CO2, шум, свет
  - DePIN ноды: регистрация, heartbeat, репутация
  - Алерты: превышение порогов, уведомления
  - Агрегация: статистика по районам/городам

Интеграция:
  → L5 Identity (:9940) — DID нод
  → L4 Payment (:9200) — оплата за данные
  → L7 DAO (:9510) — управление городом
"""

import json
import logging
import os
import random
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

PORT = 9660
PIDFILE = "/tmp/snin_city.pid"
DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "city")
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CITY] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "logs", "city.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("city")


class CityDB:
    def __init__(self):
        self.nodes_file = os.path.join(DATA_DIR, "nodes.json")
        self.sensors_file = os.path.join(DATA_DIR, "sensors.json")
        self.alerts_file = os.path.join(DATA_DIR, "alerts.json")
        self.readings_file = os.path.join(DATA_DIR, "readings.json")
        self._nodes = self._load(self.nodes_file)
        self._sensors = self._load(self.sensors_file)
        self._alerts = self._load(self.alerts_file)
        self._readings = self._load(self.readings_file)

    def _load(self, path):
        try:
            with open(path) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save(self, path, data):
        with open(path, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    # ─── DePIN Nodes ───
    def register_node(self, name: str, location: str, node_type: str,
                      mesh_pubkey: str = "") -> dict:
        nid = f"node_{uuid.uuid4().hex[:12]}"
        now = int(time.time())
        node = {
            "id": nid,
            "name": name,
            "location": location,
            "type": node_type,
            "mesh_pubkey": mesh_pubkey,
            "status": "active",
            "uptime": 0,
            "sensors_count": 0,
            "readings_count": 0,
            "last_heartbeat": now,
            "registered_at": now,
        }
        self._nodes[nid] = node
        self._save(self.nodes_file, self._nodes)
        logger.info(f"🏙️ Node registered: {nid} — {name}")
        return node

    def get_node(self, nid: str) -> dict:
        return self._nodes.get(nid)

    def get_nodes(self, status=None):
        items = list(self._nodes.values())
        if status:
            items = [n for n in items if n["status"] == status]
        return items

    def heartbeat(self, nid: str) -> dict:
        node = self._nodes.get(nid)
        if not node:
            return None
        now = int(time.time())
        delta = now - node.get("last_heartbeat", now)
        node["uptime"] = node.get("uptime", 0) + delta
        node["last_heartbeat"] = now
        node["status"] = "active"
        self._save(self.nodes_file, self._nodes)
        return node

    # ─── Sensors & Readings ───
    def record_reading(self, nid: str, sensor_type: str, value: float,
                       unit: str = "", zone: str = "") -> dict:
        node = self._nodes.get(nid)
        if not node:
            return None
        
        rid = f"read_{uuid.uuid4().hex[:12]}"
        now = int(time.time())
        reading = {
            "id": rid,
            "node_id": nid,
            "sensor_type": sensor_type,
            "value": value,
            "unit": unit,
            "zone": zone,
            "timestamp": now,
        }
        self._readings[rid] = reading
        self._save(self.readings_file, self._readings)
        
        node["readings_count"] = node.get("readings_count", 0) + 1
        self._save(self.nodes_file, self._nodes)
        
        # Проверка порогов
        self._check_threshold(sensor_type, value, nid, zone)
        
        return reading

    def _check_threshold(self, sensor_type: str, value: float, nid: str, zone: str):
        thresholds = {
            "temperature": {"min": -10, "max": 50},
            "humidity": {"min": 10, "max": 95},
            "co2": {"min": 300, "max": 1500},
            "noise": {"min": 20, "max": 110},
            "light": {"min": 0, "max": 100000},
            "air_quality": {"min": 0, "max": 500},
        }
        th = thresholds.get(sensor_type, {"min": -9999, "max": 9999})
        if value < th["min"] or value > th["max"]:
            alert = {
                "id": f"alert_{uuid.uuid4().hex[:12]}",
                "node_id": nid,
                "sensor_type": sensor_type,
                "value": value,
                "threshold_min": th["min"],
                "threshold_max": th["max"],
                "zone": zone,
                "severity": "warning" if value < th["min"] * 2 or value > th["max"] * 0.9 else "critical",
                "timestamp": int(time.time()),
                "resolved": False,
            }
            self._alerts[alert["id"]] = alert
            self._save(self.alerts_file, self._alerts)
            logger.warning(f"⚠️ Alert: {sensor_type}={value} (thr: {th['min']}-{th['max']}) at {nid}")

    def get_readings(self, nid: str = "", sensor_type: str = "", limit: int = 50):
        items = list(self._readings.values())
        if nid:
            items = [r for r in items if r["node_id"] == nid]
        if sensor_type:
            items = [r for r in items if r["sensor_type"] == sensor_type]
        items.sort(key=lambda r: r["timestamp"], reverse=True)
        return items[:limit]

    def get_sensor_stats(self, sensor_type: str = ""):
        readings = self._readings.values()
        if sensor_type:
            readings = [r for r in readings if r["sensor_type"] == sensor_type]
        if not readings:
            return {}
        values = [r["value"] for r in readings]
        zones = set(r.get("zone", "") for r in readings if r.get("zone"))
        return {
            "sensor_type": sensor_type or "all",
            "count": len(values),
            "min": round(min(values), 2),
            "max": round(max(values), 2),
            "avg": round(sum(values) / len(values), 2),
            "zones": list(zones),
        }

    def get_alerts(self, resolved: bool = False):
        return [a for a in self._alerts.values() if a["resolved"] == resolved]

    def resolve_alert(self, aid: str) -> bool:
        alert = self._alerts.get(aid)
        if not alert:
            return False
        alert["resolved"] = True
        alert["resolved_at"] = int(time.time())
        self._save(self.alerts_file, self._alerts)
        return True

    def get_stats(self) -> dict:
        return {
            "nodes_total": len(self._nodes),
            "nodes_active": len([n for n in self._nodes.values() if n["status"] == "active"]),
            "readings_total": len(self._readings),
            "alerts_active": len([a for a in self._alerts.values() if not a["resolved"]]),
            "sensor_types": list(set(r["sensor_type"] for r in self._readings.values())),
            "zones": list(set(n["location"] for n in self._nodes.values())),
        }


db = CityDB()


class CityHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _respond(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
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
                "layer": "L11 — Smart City Mesh",
                "version": "V4.0",
                "status": "operational",
                "stats": db.get_stats(),
            })

        elif path == "/nodes":
            status = params.get("status", [None])[0]
            self._respond(200, {"nodes": db.get_nodes(status=status)})

        elif path.startswith("/nodes/"):
            nid = path.split("/")[-1]
            if nid and nid != "heartbeat":
                node = db.get_node(nid)
                if not node:
                    self._respond(404, {"error": "node not found"})
                    return
                self._respond(200, node)

        elif path == "/readings":
            nid = params.get("node_id", [""])[0]
            stype = params.get("sensor_type", [""])[0]
            limit = int(params.get("limit", [50])[0])
            self._respond(200, {"readings": db.get_readings(nid=nid, sensor_type=stype, limit=limit)})

        elif path == "/stats":
            stype = params.get("sensor_type", [""])[0]
            self._respond(200, db.get_sensor_stats(sensor_type=stype) or db.get_stats())

        elif path == "/alerts":
            resolved = params.get("resolved", ["false"])[0].lower() == "true"
            self._respond(200, {"alerts": db.get_alerts(resolved=resolved)})

        else:
            self._respond(404, {"error": f"not found: {path}"})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        try:
            body = self._read_body()
        except Exception:
            self._respond(400, {"error": "invalid JSON"})
            return

        if path == "/nodes":
            required = ["name", "location", "type"]
            for f in required:
                if f not in body:
                    self._respond(400, {"error": f"missing: {f}"})
                    return
            node = db.register_node(
                name=body["name"],
                location=body["location"],
                node_type=body["type"],
                mesh_pubkey=body.get("mesh_pubkey", ""),
            )
            self._respond(201, node)

        elif path.endswith("/heartbeat"):
            nid = path.split("/")[0] if "/" in path.strip("/") else body.get("node_id", "")
            nid = body.get("node_id", nid)
            if not nid:
                self._respond(400, {"error": "node_id required"})
                return
            node = db.heartbeat(nid)
            if not node:
                self._respond(404, {"error": "node not found"})
                return
            self._respond(200, node)

        elif path == "/readings":
            required = ["node_id", "sensor_type", "value"]
            for f in required:
                if f not in body:
                    self._respond(400, {"error": f"missing: {f}"})
                    return
            reading = db.record_reading(
                nid=body["node_id"],
                sensor_type=body["sensor_type"],
                value=body["value"],
                unit=body.get("unit", ""),
                zone=body.get("zone", ""),
            )
            if not reading:
                self._respond(404, {"error": "node not found"})
                return
            self._respond(201, reading)

        elif path == "/alerts/resolve":
            aid = body.get("alert_id", "")
            if not aid:
                self._respond(400, {"error": "alert_id required"})
                return
            ok = db.resolve_alert(aid)
            self._respond(200, {"ok": ok})

        else:
            self._respond(404, {"error": "not found"})


def run_server():
    server = HTTPServer(("0.0.0.0", PORT), CityHandler)
    server.start_time = time.time()
    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))
    logger.info(f"🚀 L11 Smart City Mesh на :{PORT}")
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

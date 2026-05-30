#!/usr/bin/env python3
"""
Smart Router Health Proxy — HTTP health endpoint для L2 Transport.
Слушает :9933, проксирует health проверки.
"""

import http.server
import json
import os
import signal
import socket
import sys
import time

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9933
SMART_ROUTER = 9932
PIDFILE = f"/tmp/snin_sr_health.pid"

stats = {"started": time.time(), "probes": 0}

class HealthHandler(http.server.BaseHTTPRequestHandler):
    def _json(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
    
    def do_GET(self):
        stats["probes"] += 1
        # Проверяем что smart router жив (TCP connect)
        sr_alive = False
        sr_latency = 0
        try:
            t0 = time.time()
            s = socket.socket()
            s.settimeout(1)
            s.connect(("127.0.0.1", SMART_ROUTER))
            sr_latency = round((time.time() - t0) * 1000, 1)
            s.close()
            sr_alive = True
        except: pass
        
        # Через Nostr bridge (проверяем что SR принимает трафик)
        nostr_bridge_alive = False
        try:
            s = socket.socket()
            s.settimeout(1)
            s.connect(("127.0.0.1", 8196))
            s.close()
            nostr_bridge_alive = True
        except: pass
        
        self._json({
            "status": "ok",
            "smart_router_alive": sr_alive,
            "smart_router_port": SMART_ROUTER,
            "smart_router_latency_ms": sr_latency,
            "nostr_bridge_alive": nostr_bridge_alive,
            "heartbeat_ts": time.time(),
            "uptime_s": int(time.time() - stats["started"]),
            "probes": stats["probes"],
        })
    
    def log_message(self, *args): pass

def main():
    if os.path.isfile(PIDFILE):
        with open(PIDFILE) as f:
            try:
                os.kill(int(f.read()), 0)
                print(f"[SR Health] Already running on :{PORT}")
                return
            except: pass
    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))
    
    server = http.server.HTTPServer(("0.0.0.0", PORT), HealthHandler)
    print(f"[SR Health] Smart Router health proxy on :{PORT}")
    server.serve_forever()

if __name__ == "__main__":
    main()

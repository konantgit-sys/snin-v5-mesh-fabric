#!/usr/bin/env python3
"""
SNIN TCP Mesh Channel — заменяет mesh-agent-lite на :9908.
Простой TCP сервер с health-ендпоинтом для L2 Transport.
"""

import http.server
import json
import os
import socket
import sys
import time
import threading

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9908
PIDFILE = f"/tmp/snin_tcp_mesh.pid"

stats = {
    "started": time.time(),
    "messages_sent": 0,
    "messages_received": 0,
    "connections": 0,
    "bytes_sent": 0,
    "bytes_received": 0,
}

# HTTP health endpoint
class HealthHandler(http.server.BaseHTTPRequestHandler):
    def _json(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
    
    def do_GET(self):
        self._json({
            "status": "ok",
            "layer": "TCP Mesh Channel",
            "port": PORT,
            "uptime_s": int(time.time() - stats["started"]),
            "connections": stats["connections"],
            "messages": stats["messages_sent"] + stats["messages_received"],
            "throughput_mbps": round((stats["bytes_sent"] + stats["bytes_received"]) / 
                                     max(1, time.time() - stats["started"]) / 1024 / 1024, 2),
        })
    
    def log_message(self, *args): pass

    def do_POST(self):
        content_len = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_len).decode() if content_len else "{}"
        try: data = json.loads(body)
        except: data = {}
        
        if "/send" in self.path:
            stats["messages_sent"] += 1
            stats["bytes_sent"] += len(body)
            self._json({"sent": True, "bytes": len(body)})
        else:
            self._json({"status": "ok"})

# Raw TCP server
class TCPServer:
    def __init__(self, host="0.0.0.0", port=PORT+1):  # TCP data на порт+1
        self.host = host
        self.port = port
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((host, port))
        self.server.listen(50)
        self.server.settimeout(1.0)
        print(f"[TCP Mesh] TCP data server on :{port}")
    
    def run(self):
        while True:
            try:
                conn, addr = self.server.accept()
                stats["connections"] += 1
                threading.Thread(target=self.handle, args=(conn, addr), daemon=True).start()
            except socket.timeout:
                continue
            except: break
    
    def handle(self, conn, addr):
        conn.settimeout(5)
        try:
            while True:
                data = conn.recv(4096)
                if not data: break
                stats["messages_received"] += 1
                stats["bytes_received"] += len(data)
                # Echo back (for mesh routing)
                conn.sendall(b'{"ack":true}')
                stats["messages_sent"] += 1
                stats["bytes_sent"] += len(b'{"ack":true}')
        except: pass
        finally: conn.close()

def main():
    if os.path.isfile(PIDFILE):
        with open(PIDFILE) as f:
            try:
                os.kill(int(f.read()), 0)
                print(f"[TCP Mesh] Already running (PID {f.read().strip()})")
                return
            except: pass
    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))
    
    # HTTP health server
    httpd = http.server.HTTPServer(("0.0.0.0", PORT), HealthHandler)
    
    # TCP data server (port+1 = 9909)
    tcp = TCPServer(port=PORT+1)
    
    threading.Thread(target=tcp.run, daemon=True).start()
    print(f"[TCP Mesh] Health API :{PORT}, TCP data :{PORT+1}")
    httpd.serve_forever()

if __name__ == "__main__":
    main()

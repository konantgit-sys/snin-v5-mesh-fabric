#!/usr/bin/env python3
"""
DAO API Proxy — проксирует запросы с :8082 на новый DAO :9510.
Легковесный, без зависимостей.
"""
import json, os, sys, time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import Request, urlopen
from urllib.error import URLError

TARGET = "http://127.0.0.1:9510"
PORT = 8082

class ProxyHandler(BaseHTTPRequestHandler):
    def _proxy(self, method="GET"):
        path = self.path
        # /api/health → /health
        if path.startswith("/api/"):
            path = path[4:]  # /api/health → /health
        url = f"{TARGET}{path}"
        
        try:
            req = Request(url, method=method)
            body = None
            if method == "POST":
                length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(length) if length > 0 else None
                if body:
                    req.data = body
            
            resp = urlopen(req, timeout=5)
            data = resp.read()
            self.send_response(resp.status)
            for k, v in resp.headers.items():
                if k.lower() not in ('transfer-encoding', 'content-encoding', 'content-length'):
                    self.send_header(k, v)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except URLError as e:
            self.send_response(502)
            self.send_header('Content-Type', 'application/json')
            body = json.dumps({"error": "DAO unavailable", "detail": str(e.reason)}).encode()
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            body = json.dumps({"error": str(e)}).encode()
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    
    def do_GET(self): self._proxy("GET")
    def do_POST(self): self._proxy("POST")
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.end_headers()
    
    def log_message(self, fmt, *args):
        pass  # тихо

def main():
    server = HTTPServer(("0.0.0.0", PORT), ProxyHandler)
    print(f"[DAO Proxy] :{PORT} → {TARGET}")
    server.serve_forever()

if __name__ == "__main__":
    main()

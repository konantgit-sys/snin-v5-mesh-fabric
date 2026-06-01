#!/usr/bin/env python3
"""
mesh_health.py — HTTP health endpoint для mesh-сервисов.

Встраивается одной строкой в любой mesh-сервис:
    from mesh_health import start_health; start_health(port)

Слушает на порту service_port + 10000 (если сервис :9941 → health :19941).
Отвечает JSON: {"status": "ok", "name": "...", "uptime": 123}
"""
import http.server
import json
import threading
import time
import os

_start_time = time.time()
_service_name = os.path.basename(__file__).replace('.py', '')


class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({
                'status': 'ok',
                'name': _service_name,
                'uptime': int(time.time() - _start_time),
                'port': self.server.server_address[1]
            }).encode())
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status":"not_found"}')

    def log_message(self, format, *args):
        pass  # не засоряем логи


def start_health(service_port, name=""):
    """Запустить health HTTP сервер на порту service_port + 10000.
    Вызывается одной строкой в начале main() сервиса."""
    global _service_name
    if name:
        _service_name = name

    health_port = service_port + 10000
    try:
        server = http.server.HTTPServer(('0.0.0.0', health_port), HealthHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.daemon = True
        t.start()
        return True
    except OSError as e:
        # Порт уже занят — это ок, другой процесс уже слушает
        return False

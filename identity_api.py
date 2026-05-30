#!/usr/bin/env python3
"""
SNIN Identity API — Layer 5 REST endpoint.

GET  /identity/:name    — данные агента + репутация
GET  /identity/did/:did — поиск по DID
GET  /identity/top      — топ по репутации
POST /identity/attest   — добавить аттестацию
GET  /health            — статус системы
"""

import json
import os
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

# Добавляем relay-mesh в путь
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from reputation import ReputationDB, DIDResolver
from mesh_identity import load_or_create_identity

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9940
db = ReputationDB()


class IdentityHandler(BaseHTTPRequestHandler):

    def _respond(self, code: int, data: dict):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2, default=str).encode())

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        if path == "/health":
            agents = db.get_top_agents(100)
            self._respond(200, {
                "status": "ok",
                "agents_registered": len(agents),
                "db_path": str(db.db_path),
            })

        elif path == "/identity/top":
            limit = int(self._get_param("limit", "10"))
            agents = db.get_top_agents(limit)
            self._respond(200, {"agents": agents})

        elif path.startswith("/identity/did/"):
            did = path[len("/identity/did/"):]
            agent = db.get_agent_by_did(did)
            if agent:
                summary = db.get_agent_summary(agent["pubkey_hex"])
                self._respond(200, summary)
            else:
                self._respond(404, {"error": f"Agent not found: {did}"})

        elif path.startswith("/identity/"):
            name = path[len("/identity/"):]
            # Проверяем сначала SQLite
            agent = db.get_agent_by_name(name)
            if agent:
                summary = db.get_agent_summary(agent["pubkey_hex"])
                self._respond(200, summary)
            else:
                # fallback — загрузить identity из файла
                try:
                    identity = load_or_create_identity(name)
                    agent_data = {
                        "agent_name": name,
                        "pubkey_hex": identity["mesh_pubkey"],
                        "did_snin": DIDResolver.make_did(identity["mesh_pubkey"]),
                        "attestations": len(identity.get("attestations", [])),
                        "links": identity.get("links", {}),
                    }
                    self._respond(200, agent_data)
                except Exception as e:
                    self._respond(404, {"error": str(e)})

        else:
            self._respond(404, {"error": "Not found", "paths": ["/health", "/identity/:name", "/identity/did/:did", "/identity/top"]})

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length else b"{}"
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            self._respond(400, {"error": "Invalid JSON"})
            return

        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        if path == "/identity/attest":
            required = ["agent_pubkey", "signer_pubkey"]
            missing = [k for k in required if k not in data]
            if missing:
                self._respond(400, {"error": f"Missing fields: {missing}"})
                return

            # Валидация подписи (упрощённо — проверяем что подпись есть)
            if "signature" not in data:
                self._respond(400, {"error": "signature required"})
                return

            db.add_attestation(
                agent_pubkey=data["agent_pubkey"],
                signer_pubkey=data["signer_pubkey"],
                signature=data["signature"],
                role=data.get("role", "agent"),
            )
            self._respond(200, {"status": "attestation_added"})

        else:
            self._respond(404, {"error": f"Unknown path: {path}"})

    def _get_param(self, name: str, default: str = "") -> str:
        parsed = urlparse(self.path)
        query = parsed.query
        for part in query.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                if k == name:
                    return v
        return default

    def log_message(self, format, *args):
        """Тихий лог — только ошибки."""
        if args and "400" in str(args) or "404" in str(args) or "500" in str(args):
            super().log_message(format, *args)


if __name__ == "__main__":
    print(f"[IDENTITY API] Starting on :{PORT}")
    server = HTTPServer(("0.0.0.0", PORT), IdentityHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[IDENTITY API] Shutdown")
        server.server_close()

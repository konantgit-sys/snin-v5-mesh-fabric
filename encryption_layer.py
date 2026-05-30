#!/usr/bin/env python3
"""
Encryption Layer L2.5 (:9600) — REST API для E2E шифрования mesh-сообщений.
Версия: V4.0
Дата: 2026-05-23

Основание: mesh_crypto.py (X25519 + HKDF-SHA256 + AES-256-GCM)
Используется: agent_gossip.py, Smart Router, внешние интеграции

Эндпоинты:
  GET  /              — статус слоя
  POST /encrypt       — зашифровать сообщение для агента
  POST /decrypt       — расшифровать сообщение от агента
  GET  /agents        — список агентов с cipher_pubkey
  GET  /health        — healthcheck для supervisor
"""

import asyncio
import json
import os
import sys
import signal
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(__file__))
from mesh_crypto import encrypt_for_agent, decrypt_from_agent, load_identity

PORT = 9600
PIDFILE = "/tmp/snin_encryption_layer.pid"
IDENTITIES_DIR = os.path.join(os.path.dirname(__file__), "identities")


class EncryptionHandler(BaseHTTPRequestHandler):
    """HTTP API для шифрования/расшифровки mesh-сообщений."""
    
    def log_message(self, format, *args):
        """Тихий лог."""
        pass
    
    def _respond(self, code: int, data: dict):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode())
    
    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw)
    
    def _get_agents(self) -> list:
        """Список агентов из файлов identities."""
        agents = []
        if not os.path.isdir(IDENTITIES_DIR):
            return agents
        for fname in sorted(os.listdir(IDENTITIES_DIR)):
            if fname.endswith(".json"):
                name = fname[:-5]
                try:
                    ident = load_identity(name)
                    agents.append({
                        "name": name,
                        "mesh_pubkey": ident.get("mesh_pubkey", "")[:16] + "...",
                        "cipher_pubkey": ident.get("cipher_pubkey", ""),
                        "links": ident.get("links", {}),
                    })
                except Exception:
                    pass
        return agents
    
    def do_GET(self):
        path = urlparse(self.path).path
        
        if path == "/health" or path == "/":
            agents = self._get_agents()
            self._respond(200, {
                "layer": "L2.5 Encryption Layer",
                "version": "V4.0",
                "status": "operational",
                "cipher": "X25519 + HKDF-SHA256 + AES-256-GCM",
                "agents_registered": len(agents),
                "agents": [a["name"] for a in agents],
                "uptime_sec": int(time.time() - self.server.start_time) if hasattr(self.server, 'start_time') else 0,
            })
        elif path == "/agents":
            agents = self._get_agents()
            self._respond(200, {
                "total": len(agents),
                "agents": agents,
            })
        else:
            self._respond(404, {"error": "not found"})
    
    def do_POST(self):
        path = urlparse(self.path).path
        
        try:
            body = self._read_body()
        except Exception:
            self._respond(400, {"error": "invalid JSON"})
            return
        
        if path == "/encrypt":
            self._handle_encrypt(body)
        elif path == "/decrypt":
            self._handle_decrypt(body)
        else:
            self._respond(404, {"error": "not found"})
    
    def _handle_encrypt(self, body: dict):
        """POST /encrypt — зашифровать сообщение.
        
        Body:
          plaintext: str — текст для шифрования
          recipient: str — имя агента-получателя ИЛИ его cipher_pubkey hex
          sender: str — имя агента-отправителя (должен быть в identities/)
        """
        plaintext = body.get("plaintext", "")
        recipient = body.get("recipient", "")
        sender = body.get("sender", "")
        
        if not plaintext:
            self._respond(400, {"error": "plaintext required"})
            return
        if not recipient:
            self._respond(400, {"error": "recipient required"})
            return
        if not sender:
            self._respond(400, {"error": "sender required"})
            return
        
        try:
            # Загружаем ключи отправителя
            sender_ident = load_identity(sender)
            my_priv = sender_ident["cipher_privkey"]
            
            # Получатель: имя → cipher_pubkey
            if len(recipient) == 64:  # hex pubkey
                recip_pub = recipient
            else:
                recip_ident = load_identity(recipient)
                recip_pub = recip_ident["cipher_pubkey"]
            
            cipher = encrypt_for_agent(plaintext, recip_pub, my_priv)
            
            self._respond(200, {
                "ok": True,
                "ciphertext": cipher,
                "from": sender,
                "to": recipient[:16] + "...",
                "length": len(cipher),
            })
        except Exception as e:
            self._respond(500, {"error": f"encrypt failed: {str(e)[:100]}"})
    
    def _handle_decrypt(self, body: dict):
        """POST /decrypt — расшифровать сообщение.
        
        Body:
          ciphertext: str — base64 шифротекст
          recipient: str — имя агента-получателя (чей privkey нужен)
          sender: str — имя агента-отправителя ИЛИ его cipher_pubkey hex
        """
        ciphertext = body.get("ciphertext", "")
        recipient = body.get("recipient", "")
        sender = body.get("sender", "")
        
        if not ciphertext:
            self._respond(400, {"error": "ciphertext required"})
            return
        if not recipient:
            self._respond(400, {"error": "recipient required"})
            return
        if not sender:
            self._respond(400, {"error": "sender required"})
            return
        
        try:
            # Ключи получателя
            recip_ident = load_identity(recipient)
            my_priv = recip_ident["cipher_privkey"]
            
            # Отправитель: имя → cipher_pubkey
            if len(sender) == 64:
                sender_pub = sender
            else:
                sender_ident = load_identity(sender)
                sender_pub = sender_ident["cipher_pubkey"]
            
            plain = decrypt_from_agent(ciphertext, my_priv, sender_pub)
            
            self._respond(200, {
                "ok": True,
                "plaintext": plain,
                "from": sender[:16] + "...",
                "to": recipient,
            })
        except Exception as e:
            self._respond(500, {"error": f"decrypt failed: {str(e)[:100]}"})


def run_server():
    server = HTTPServer(("0.0.0.0", PORT), EncryptionHandler)
    server.start_time = time.time()
    
    # PID file
    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))
    
    print(f"[Encryption Layer] 🚀 L2.5 на :{PORT}")
    print(f"[Encryption Layer]   Cipher: X25519 + HKDF + AES-256-GCM")
    print(f"[Encryption Layer]   Agents: {len([f for f in os.listdir(IDENTITIES_DIR) if f.endswith('.json')]) if os.path.isdir(IDENTITIES_DIR) else 0}")
    print(f"[Encryption Layer]   PID: {os.getpid()}")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if os.path.exists(PIDFILE):
            os.remove(PIDFILE)
        print("[Encryption Layer] 👋 Остановлен")


if __name__ == "__main__":
    run_server()

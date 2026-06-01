#!/usr/bin/env python3
"""
SNIN L1.5 — Cross-Mesh Bridge v2.0

Соединяет все transport-каналы L2 в единую mesh-сеть:
  Nostr Relay (:8198) ←→ L1.5 ←→ TCP Mesh (:9908)
                                  ↕
                             WebRTC (stub)

Архитектура:
  ┌─────────────┐     relay      ┌─────────────┐
  │ Nostr       │◄──────────────►│ L1.5 Bridge  │
  │ :8198       │                │ :8202        │
  └─────────────┘                └──────┬───────┘
                                        │
  ┌─────────────┐                 ┌──────┴───────┐
  │ TCP Mesh    │◄───────────────►│ Mesh Gateway  │
  │ :9908       │                 │ :9945         │
  └─────────────┘                 └──────────────┘

Также интегрирует существующий cross_mesh_bridge.py (:9945)
для mesh-to-mesh discovery через Nostr kind:39010-39012.
"""

import asyncio
import json
import logging
import os
import socket
import sys
import time
import uuid
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import URLError

# ─── Logging ───
logging.basicConfig(level=logging.INFO, format="[L1.5] %(message)s")
log = logging.getLogger("l1_5")

# ─── Константы ───
L2_TRANSPORT = "http://127.0.0.1:9500"
MESH_BRIDGE = "http://127.0.0.1:9945"
NOSTR_RELAY = "http://127.0.0.1:8198"
TCP_MESH = "127.0.0.1:9908"
L5_IDENTITY = "http://127.0.0.1:9940"
GATEWAY = "http://127.0.0.1:8083"

CHANNELS = {
    "nostr": {"alive": True, "url": NOSTR_RELAY, "protocol": "nostr kind:1"},
    "tcp_mesh": {"alive": False, "url": f"tcp://{TCP_MESH}", "protocol": "TCP raw"},
    "webrtc": {"alive": False, "url": None, "protocol": "WebRTC (stub)"},
    "cross_mesh": {"alive": True, "url": MESH_BRIDGE, "protocol": "mesh-to-mesh discovery"},
}

stats = {
    "messages_relayed": 0,
    "routes_active": 0,
    "errors": 0,
    "started": time.time(),
}


# ─── Helpers ───

def _fetch(url: str, timeout: float = 3.0) -> dict:
    """HTTP GET с таймаутом."""
    try:
        r = urlopen(url, timeout=timeout)
        return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)[:60]}


def _post(url: str, data: dict) -> dict:
    """HTTP POST JSON."""
    try:
        req = Request(url, data=json.dumps(data).encode(),
                      headers={"Content-Type": "application/json"},
                      method="POST")
        r = urlopen(req, timeout=3)
        return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)[:60]}


def _check_port(host: str, port: int, timeout: float = 1.0) -> bool:
    """Проверка открыт ли TCP порт."""
    try:
        s = socket.socket()
        s.settimeout(timeout)
        s.connect((host, port))
        s.close()
        return True
    except Exception:
        return False


def _update_channel_status():
    """Обновление статуса каналов."""
    CHANNELS["nostr"]["alive"] = _check_port("127.0.0.1", 8198)
    CHANNELS["tcp_mesh"]["alive"] = _check_port("127.0.0.1", 9908)
    CHANNELS["cross_mesh"]["alive"] = _check_port("127.0.0.1", 9945)
    # WebRTC — всегда stub
    alive = sum(1 for c in CHANNELS.values() if c["alive"])
    stats["routes_active"] = alive
    return alive


# ══════════════════════════════════════════════════════════════
# API HANDLER
# ══════════════════════════════════════════════════════════════

class BridgeHandler(BaseHTTPRequestHandler):
    """HTTP API для L1.5 Cross-Mesh Bridge."""

    def _json(self, data: dict, status: int = 200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2, default=str).encode())

    def do_GET(self):
        path = self.path.rstrip("/")

        if path == "/" or path == "":
            self._json({
                "service": "SNIN L1.5 — Cross-Mesh Bridge",
                "version": "2.0",
                "channels": len(CHANNELS),
                "routes_active": stats["routes_active"],
                "uptime_s": int(time.time() - stats["started"]),
                "endpoints": [
                    "/health — статус bridge",
                    "/channels — все каналы",
                    "/relay — relay message между каналами",
                    "/mesh — статус mesh bridge",
                    "/stats — статистика",
                ]
            })

        elif path == "/health":
            alive = _update_channel_status()
            self._json({
                "status": "ok",
                "layer": "L1.5 — Cross-Mesh Bridge",
                "channels_alive": alive,
                "channels_total": len(CHANNELS),
                "messages_relayed": stats["messages_relayed"],
                "uptime_s": int(time.time() - stats["started"]),
            })

        elif path == "/channels":
            _update_channel_status()
            self._json({
                "channels": {k: {"alive": v["alive"], "protocol": v["protocol"]}
                             for k, v in CHANNELS.items()},
                "alive": sum(1 for c in CHANNELS.values() if c["alive"]),
                "total": len(CHANNELS),
            })

        elif path == "/mesh":
            # Прокси к существующему cross_mesh_bridge
            mesh_data = _fetch(f"{MESH_BRIDGE}/health")
            self._json(mesh_data)

        elif path == "/stats":
            self._json({
                **stats,
                "uptime_h": round((time.time() - stats["started"]) / 3600, 2),
                "channels": {k: v["alive"] for k, v in CHANNELS.items()},
            })

        elif path == "/relay":
            # Получаем последние сообщения из Nostr relay
            nostr_alive = _check_port("127.0.0.1", 8198)
            if nostr_alive:
                try:
                    r = urlopen(f"{NOSTR_RELAY}/api/events?limit=5", timeout=3)
                    events = json.loads(r.read())
                    self._json({"events": events[:5], "source": "nostr"})
                except Exception as e:
                    self._json({"events": [], "error": str(e)[:60]})
            else:
                self._json({"events": [], "error": "nostr relay unavailable"})

        elif path == "/l2":
            # Прокси к L2 Transport
            l2 = _fetch(f"{L2_TRANSPORT}/api/v1/")
            self._json(l2)

        elif path == "/peers":
            # Peers из всех источников
            l2_peers = _fetch(f"{L2_TRANSPORT}/api/v1/peers")
            bridge_peers = _fetch(f"{MESH_BRIDGE}/discovery")
            self._json({
                "l2_transport": l2_peers if "error" not in l2_peers else [],
                "mesh_bridge": bridge_peers.get("meshes", []) if "error" not in bridge_peers else [],
                "count_l2": len(l2_peers.get("peers", [])) if "peers" in l2_peers else 0,
                "count_mesh": bridge_peers.get("count", 0) if "count" in bridge_peers else 0,
            })

        else:
            self._json({"error": "route not found",
                        "available": ["/", "/health", "/channels",
                                      "/mesh", "/stats", "/relay",
                                      "/l2", "/peers"]}, 404)

    def do_POST(self):
        path = self.path.rstrip("/")
        content_len = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_len).decode() if content_len else "{}"
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            data = {}

        if path == "/relay/send":
            # Отправка сообщения через bridge
            content = data.get("content", "")
            target = data.get("target", "nostr")  # nostr, tcp, all
            kind = data.get("kind", 1)

            results = {}
            if target in ("nostr", "all"):
                try:
                    r = _post(f"{NOSTR_RELAY}/api/events",
                              {"content": content, "kind": kind})
                    results["nostr"] = "sent" if "error" not in r else r["error"]
                except Exception as e:
                    results["nostr"] = str(e)[:30]

            if target in ("tcp", "all"):
                # TCP — пока stub
                results["tcp"] = "stub — TCP not connected"

            stats["messages_relayed"] += 1
            self._json({"relayed": True, "target": target, "results": results})

        elif path == "/bridge/send":
            # Отправка через все каналы сразу
            content = data.get("content", "")
            kind = data.get("kind", 1)

            results = {}
            for channel, info in CHANNELS.items():
                if info["alive"] and channel == "nostr":
                    try:
                        r = _post(f"{NOSTR_RELAY}/api/events",
                                  {"content": content, "kind": kind})
                        results[channel] = "sent"
                    except Exception as e:
                        results[channel] = str(e)[:30]
                elif info["alive"] and channel == "cross_mesh":
                    results[channel] = "available (mesh-only)"
                else:
                    results[channel] = "unavailable"

            stats["messages_relayed"] += len(results)
            self._json({"relayed": True, "channels": len(results), "results": results})

        else:
            self._json({"error": "not found",
                        "available": ["/relay/send", "/bridge/send"]}, 404)

    def log_message(self, fmt, *args):
        pass  # тихий лог


# ══════════════════════════════════════════════════════════════

def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8202
    log.info(f"Starting L1.5 Cross-Mesh Bridge on :{port}")

    # Проверка зависимостей
    channels_alive = _update_channel_status()
    alive_names = [k for k, v in CHANNELS.items() if v["alive"]]
    dead_names = [k for k, v in CHANNELS.items() if not v["alive"]]
    log.info(f"Channels: {channels_alive}/{len(CHANNELS)} alive")
    if alive_names:
        log.info(f"  🟢 {', '.join(alive_names)}")
    if dead_names:
        log.info(f"  🔴 {', '.join(dead_names)}")

    server = HTTPServer(("0.0.0.0", port), BridgeHandler)
    log.info(f"L1.5 Bridge API ready — http://127.0.0.1:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()

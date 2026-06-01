#!/usr/bin/env python3
"""
SNIN L3 — Mesh Core Layer :9300

Единый слой mesh-роутинга. Агрегирует существующие mesh-сервисы:
  mesh_api (:9907), mesh_smart_router (:9932), mesh_agent (:9908),
  cross_mesh (:9945), L1.5 bridge (:8202)

Функции:
  - Топология сети (граф нод)
  - Маршрутизация (Dijkstra, flood-fill)
  - Метрики задержек (latency между нодами)
  - Автовосстановление маршрутов
  - Единый API для всех mesh-операций
"""

import json
import logging
import math
import os
import random
import socket
import sys
import time
import heapq
import http.server
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError

# ─── Config ───
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9300

# ─── Logging ───
logging.basicConfig(level=logging.INFO, format="[L3] %(message)s")
log = logging.getLogger("l3")

# ─── Константы ───
MESH_API = "http://127.0.0.1:9907"
SMART_ROUTER = "http://127.0.0.1:9932"
MESH_AGENT = "http://127.0.0.1:9908"
CROSS_MESH = "http://127.0.0.1:9945"
L15_BRIDGE = "http://127.0.0.1:8202"

# ─── Топология сети (in-memory граф) ───
# nodes[node_id] = {"host", "port", "layer", "first_seen", "last_seen", "alive"}
# edges[(node_a, node_b)] = {"latency_ms", "last_updated", "channel"}
nodes: dict = {}
edges: dict = {}
topology_version = 0

# Статистика
stats = {
    "started": time.time(),
    "routes_calculated": 0,
    "route_requests": 0,
    "topology_updates": 0,
    "errors": 0,
}

# PID
PIDFILE = "/tmp/snin_l3_mesh.pid"


def port_open(host="127.0.0.1", port=9900, timeout=1.0) -> bool:
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True
    except: return False


def _fetch(url: str, timeout: float = 2.0) -> dict:
    try:
        r = urlopen(url, timeout=timeout)
        return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)[:60]}


# ═══════════════════════════════════════════
# СБОР ТОПОЛОГИИ
# ═══════════════════════════════════════════

def discover_nodes() -> int:
    """Сбор всех известных mesh-нод из существующих сервисов."""
    global topology_version
    now = time.time()

    # Встроенные ноды (наши mesh-сервисы)
    local_nodes = [
        ("mesh_api", "127.0.0.1", 9907, "L3"),
        ("mesh_smart_router", "127.0.0.1", 9932, "L3"),
        ("mesh_agent", "127.0.0.1", 9908, "L3"),
        ("cross_mesh_bridge", "127.0.0.1", 9945, "L1.5"),
        ("l1_5_bridge", "127.0.0.1", 8202, "L1.5"),
        ("l2_transport", "127.0.0.1", 9500, "L2"),
        ("nostr_relay", "127.0.0.1", 8198, "L0"),
        ("relay_mesh", "127.0.0.1", 8443, "L0"),
        ("p2p_dash", "127.0.0.1", 8090, "L0"),
    ]

    added = 0
    for name, host, port, layer in local_nodes:
        alive = port_open(host=host, port=port) if port else False
        if name not in nodes:
            nodes[name] = {
                "host": host, "port": port, "layer": layer,
                "first_seen": now, "last_seen": now if alive else 0,
                "alive": alive, "name": name,
            }
            added += 1
        else:
            nodes[name]["last_seen"] = now if alive else nodes[name]["last_seen"]
            nodes[name]["alive"] = alive

    # Пытаемся получить удалённые mesh из cross_mesh
    try:
        r = _fetch(f"{CROSS_MESH}/discovery")
        if "error" not in r:
            for mesh in r.get("meshes", []):
                mid = mesh.get("mesh_id", "")[:16]
                if mid:
                    nodes[f"remote_{mid}"] = {
                        "host": "remote", "port": 0, "layer": "remote",
                        "first_seen": now, "last_seen": now,
                        "alive": True, "name": mesh.get("mesh_name", mid),
                        "mesh_id": mid,
                    }
                    added += 1
    except: pass

    if added:
        topology_version += 1
        stats["topology_updates"] += 1
        log.info(f"Topology: {len(nodes)} nodes ({added} new)")

    return added


def discover_edges() -> int:
    """Построение графа связей между нодами на основе latency probe."""
    global topology_version
    alive_nodes = {n: v for n, v in nodes.items() if v.get("alive") and v.get("port")}
    discovered = 0

    node_list = list(alive_nodes.items())
    for i, (n1, v1) in enumerate(node_list):
        for n2, v2 in node_list[i+1:]:
            if n1 == n2:
                continue
            key = (n1, n2) if n1 < n2 else (n2, n1)

            # Измеряем latency: пробуем TCP connect
            latency = None
            if v1.get("port") and v2.get("port"):
                latency = measure_latency(v1["host"], v2["port"])

            edge = {
                "node_a": n1, "node_b": n2,
                "latency_ms": latency if latency else random.uniform(1, 50),
                "last_updated": time.time(),
                "alive": latency is not None,
                "channel": f"{v1.get('layer','?')}-{v2.get('layer','?')}",
            }
            if key not in edges:
                edges[key] = edge
                discovered += 1
                topology_version += 1
            else:
                if latency is not None:
                    # Обновляем с экспоненциальным сглаживанием
                    old = edges[key]["latency_ms"]
                    edges[key]["latency_ms"] = old * 0.7 + latency * 0.3
                edges[key]["last_updated"] = time.time()
                edges[key]["alive"] = latency is not None

    if discovered:
        stats["topology_updates"] += 1
        log.info(f"Edges: {len(edges)} ({discovered} new)")
    return discovered


def measure_latency(host: str, port: int, timeout=0.5) -> float | None:
    """TCP connect latency в мс."""
    try:
        start = time.time()
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return round((time.time() - start) * 1000, 1)
    except: return None


# ═══════════════════════════════════════════
# МАРШРУТИЗАЦИЯ
# ═══════════════════════════════════════════

def dijkstra(source: str, target: str) -> dict | None:
    """Кратчайший путь по графу (Dijkstra)."""
    if source not in nodes or target not in nodes:
        return None
    if source == target:
        return {"path": [source], "total_latency": 0, "hops": 0}

    dist = {n: float('inf') for n in nodes}
    prev = {n: None for n in nodes}
    dist[source] = 0
    pq = [(0, source)]

    alive_edges = {k: v for k, v in edges.items() if v.get("alive")}

    while pq:
        d, n = heapq.heappop(pq)
        if d > dist[n]:
            continue
        if n == target:
            break

        for (a, b), e in alive_edges.items():
            neighbor = b if a == n else (a if b == n else None)
            if neighbor is None:
                continue
            nd = d + e["latency_ms"]
            if nd < dist[neighbor]:
                dist[neighbor] = nd
                prev[neighbor] = n
                heapq.heappush(pq, (nd, neighbor))

    if dist[target] == float('inf'):
        return None

    # Восстанавливаем путь
    path = []
    n = target
    while n is not None:
        path.append(n)
        n = prev[n]
    path.reverse()

    stats["routes_calculated"] += 1
    return {
        "path": path,
        "total_latency": round(dist[target], 1),
        "hops": len(path) - 1,
    }


def flood_fill(source: str, max_hops: int = 3) -> list:
    """BFS — все ноды в радиусе N hops."""
    if source not in nodes:
        return []

    visited = {source}
    queue = deque([(source, 0)])
    reachable = []

    while queue:
        n, hops = queue.popleft()
        reachable.append({"node": n, "hops": hops})

        if hops >= max_hops:
            continue

        for (a, b), e in edges.items():
            if not e.get("alive"):
                continue
            neighbor = b if a == n else (a if b == n else None)
            if neighbor and neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, hops + 1))

    return reachable


# ═══════════════════════════════════════════
# HTTP API
# ═══════════════════════════════════════════

class L3Handler(http.server.BaseHTTPRequestHandler):
    def _json(self, data: dict, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2, default=str).encode())

    def _error(self, msg: str, status=404):
        self._json({"error": msg}, status)

    def do_GET(self):
        path = self.path.rstrip("/")

        if path == "/" or path == "":
            self._json({
                "service": "SNIN L3 — Mesh Core Layer",
                "version": "1.0",
                "nodes": len(nodes),
                "edges": len(edges),
                "topology_version": topology_version,
                "uptime_s": int(time.time() - stats["started"]),
                "endpoints": [
                    "/health — статус",
                    "/topology — граф сети",
                    "/route?from=X&to=Y — маршрут Dijkstra",
                    "/flood?from=X&hops=N — BFS flood-fill",
                    "/nodes — список нод",
                    "/edges — список связей",
                    "/metrics — статистика",
                    "/proxy/... — прокси к mesh-сервисам",
                ]
            })

        elif path == "/health":
            alive_nodes = sum(1 for n in nodes.values() if n.get("alive"))
            self._json({
                "status": "ok",
                "layer": "L3 — Mesh Core",
                "nodes_alive": alive_nodes,
                "nodes_total": len(nodes),
                "edges": len(edges),
                "topology_version": topology_version,
                "routes_calculated": stats["routes_calculated"],
                "uptime_s": int(time.time() - stats["started"]),
            })

        elif path == "/topology":
            alive_edges = {str(k): v for k, v in edges.items() if v.get("alive")}
            self._json({
                "nodes": len(nodes),
                "edges": len(edges),
                "alive_edges": len(alive_edges),
                "topology_version": topology_version,
                "updated": datetime.now().isoformat(),
            })

        elif path == "/nodes":
            self._json({
                "nodes": {n: {k: v for k, v in info.items() if k != "name"}
                          for n, info in sorted(nodes.items())},
                "count": len(nodes),
                "alive": sum(1 for n in nodes.values() if n.get("alive")),
                "dead": sum(1 for n in nodes.values() if not n.get("alive")),
            })

        elif path == "/edges":
            self._json({
                "edges": {f"{a}↔{b}": {"latency_ms": e["latency_ms"],
                                        "alive": e.get("alive", True),
                                        "channel": e.get("channel", "?")}
                          for (a, b), e in sorted(edges.items())},
                "count": len(edges),
                "alive": sum(1 for e in edges.values() if e.get("alive")),
            })

        elif path.startswith("/route"):
            from_node = self._get_param("from")
            to_node = self._get_param("to")
            if not from_node or not to_node:
                self._error("need ?from=X&to=Y")
                return
            stats["route_requests"] += 1
            result = dijkstra(from_node, to_node)
            if result:
                self._json(result)
            else:
                self._json({"error": "no route found", "from": from_node, "to": to_node})

        elif path.startswith("/flood"):
            from_node = self._get_param("from")
            hops = int(self._get_param("hops", "3"))
            if not from_node:
                self._error("need ?from=X")
                return
            reachable = flood_fill(from_node, hops)
            self._json({"source": from_node, "max_hops": hops,
                        "reachable": reachable, "count": len(reachable)})

        elif path == "/metrics":
            self._json({
                **stats,
                "uptime_h": round((time.time() - stats["started"]) / 3600, 2),
                "nodes": {"alive": sum(1 for n in nodes.values() if n.get("alive")),
                          "total": len(nodes)},
                "edges": {"alive": sum(1 for e in edges.values() if e.get("alive")),
                          "total": len(edges)},
            })

        elif path.startswith("/proxy/"):
            target = path.split("/proxy/")[1]
            proxies = {
                "mesh-api": MESH_API,
                "smart-router": SMART_ROUTER,
                "mesh-agent": MESH_AGENT,
                "cross-mesh": CROSS_MESH,
                "l1_5": L15_BRIDGE,
            }
            url = proxies.get(target)
            if not url:
                self._error(f"unknown proxy target: {target}")
                return
            data = _fetch(url)
            self._json(data)

        else:
            self._error(f"not found: {path}")

    def do_POST(self):
        path = self.path.rstrip("/")
        content_len = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_len).decode() if content_len else "{}"
        try:
            data = json.loads(body)
        except: data = {}

        if path == "/route/update":
            """Ручное обновление топологии."""
            discovered = discover_nodes() + discover_edges()
            self._json({"updated": True, "discovered": discovered,
                        "nodes": len(nodes), "edges": len(edges)})

        elif path == "/route/refresh":
            """Принудительный пересчёт маршрутов (сброс кэша)."""
            # Пересчитываем все латенси
            count = 0
            for key in list(edges.keys()):
                e = edges[key]
                n1, n2 = e["node_a"], e["node_b"]
                v1, v2 = nodes.get(n1), nodes.get(n2)
                if v1 and v2 and v1.get("port") and v2.get("port"):
                    lat = measure_latency(v1["host"], v2["port"])
                    if lat is not None:
                        e["latency_ms"] = lat
                        e["alive"] = True
                        count += 1
                    else:
                        e["alive"] = False
            stats["routes_calculated"] += 1
            self._json({"refreshed": count, "total_edges": len(edges)})

        else:
            self._error(f"not found: {path}")

    def _get_param(self, name: str, default: str = ""):
        import urllib.parse
        qs = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(qs)
        return params.get(name, [default])[0]

    def log_message(self, fmt, *args):
        pass


# ═══════════════════════════════════════════

def main():
    if os.path.isfile(PIDFILE):
        with open(PIDFILE) as f:
            try:
                os.kill(int(f.read()), 0)
                print(f"[L3] Already running (PID {f.read().strip()})")
                return
            except: pass
    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))

    # Первичный сбор топологии
    log.info(f"Starting L3 Mesh Core on :{PORT}")
    discovered = discover_nodes()
    discover_edges()
    log.info(f"Initial topology: {len(nodes)} nodes, {len(edges)} edges")

    server = http.server.HTTPServer(("0.0.0.0", PORT), L3Handler)
    log.info(f"L3 API ready — http://127.0.0.1:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutdown")
        if os.path.isfile(PIDFILE): os.remove(PIDFILE)


if __name__ == "__main__":
    main()

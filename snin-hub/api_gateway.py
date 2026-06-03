#!/usr/bin/env python3
"""SNIN API Gateway v1 — единый шлюз для всех сервисов."""
import json, os, time, subprocess, socket, http.server, urllib.request

PORT = int(os.environ.get("PORT", 8083))
CACHE_TTL = 30  # секунд кеширования

# ─── Rate Limiter ───
RATE_LIMIT = {"per_ip": 100, "per_sec": 60}  # 100 запросов/мин с IP
_rate_buckets: dict = {}  # ip → [timestamp, ...]

def _check_rate_limit(ip: str) -> bool:
    now = time.time()
    if ip not in _rate_buckets:
        _rate_buckets[ip] = []
    # Чистим старые записи (>60 сек)
    _rate_buckets[ip] = [t for t in _rate_buckets[ip] if now - t < 60]
    if len(_rate_buckets[ip]) >= RATE_LIMIT["per_ip"]:
        return False  # превышен лимит
    _rate_buckets[ip].append(now)
    return True


# ─── Карта внутренних сервисов ───
BACKENDS = {
    "relay":   "http://127.0.0.1:8198",
    "relay-dash": "http://127.0.0.1:8086",
    "p2p-dash":   "http://127.0.0.1:8090",
    "dao":     "http://127.0.0.1:8082",
    "identity":"http://127.0.0.1:9940",
    "mesh-api":"http://127.0.0.1:9907",
    "agent":   "http://127.0.0.1:9908",
    "bridge":  "http://127.0.0.1:9945",
    "l1_5":    "http://127.0.0.1:8202",
    "l9":      "http://127.0.0.1:9900",
    "l3":      "http://127.0.0.1:9300",
    "l4":     "http://127.0.0.1:9200",
    "l6":     "http://127.0.0.1:9400",
    "zk":     "http://127.0.0.1:9250",
    "l2":     "http://127.0.0.1:9500",
    "enc":    "http://127.0.0.1:9600",
    "priv":   "http://127.0.0.1:9700",
    "app":    "http://127.0.0.1:9800",
    "pay":     "http://127.0.0.1:8191",
    "forecaster":"http://127.0.0.1:8200",
    "route":   "http://127.0.0.1:9910",
    "content": "http://127.0.0.1:9920",
    "nostr":   "http://127.0.0.1:9941",
}

# ─── Маршруты ───
ROUTES = {
    # Relay
    "/api/relay":        ("relay", "/api/stats"),
    "/api/relay/nip11":  ("relay", "/"),
    "/api/relay/stats":  ("relay", "/api/stats"),
    "/api/relay/kinds":  ("relay-dash", "/api/kinds"),
    "/api/relay/agents": ("relay-dash", "/api/agents"),
    "/api/relay/activity":("relay-dash", "/api/activity24h"),

    # P2P / DHT
    "/api/p2p":          ("p2p-dash", "/api/status"),
    "/api/p2p/peers":    ("p2p-dash", "/api/peers"),
    "/api/p2p/mesh":     ("p2p-dash", "/api/mesh"),

    # DAO / DHT layer
    "/api/dao":          ("dao", "/"),
    "/api/dao/health":   ("dao", "/health"),
    "/api/dao/stats":    ("dao", "/stats"),

    # Identity
    "/api/identity":     ("identity", "/"),
    "/api/identity/health": ("identity", "/health"),

    # Mesh API
    "/api/mesh":         ("mesh-api", "/"),
    "/api/mesh/status":  ("mesh-api", "/status"),

    # Agent
    "/api/agent":        ("agent", "/"),

    # Bridge
    "/api/bridge":       ("bridge", "/"),

    # Pay
    "/api/pay":          ("pay", "/"),
    "/api/pay/health":   ("pay", "/health"),

    # Forecaster
    "/api/forecaster":   ("forecaster", "/"),

    # Route engine
    "/api/route":        ("route", "/"),

    # Content router
    "/api/content":      ("content", "/"),

    # Nostr bridge
    "/api/nostr":        ("nostr", "/"),

    # L4 Payment Layer
    "/api/l4":           ("l4", "/api/v1/"),
    "/api/l4/health":    ("l4", "/api/v1/health"),
    "/api/l4/stats":     ("l4", "/api/v1/stats"),
    "/api/l4/payment":   ("l4", "/api/v1/payment"),
    "/api/l4/transfer":  ("l4", "/api/v1/transfer"),
    "/api/l4/swap":      ("l4", "/api/v1/swap"),
    "/api/l4/pool":      ("l4", "/api/v1/pool"),

    # L6 Agent Network
    "/api/l6":           ("l6", "/api/v1/"),
    "/api/l6/health":    ("l6", "/api/v1/health"),
    "/api/l6/agents":     ("l6", "/api/v1/agents"),
    "/api/l6/mesh":      ("l6", "/api/v1/mesh/messages"),
    "/api/l6/layers":    ("l6", "/api/v1/layers"),
    "/api/l6/dao":       ("l6", "/api/v1/dao"),

    # L2 Transport Layer
    "/api/l2":           ("l2", "/api/v1/"),
    "/api/l2/send":      ("l2", "/api/v1/send"),
    "/api/l2/channels":  ("l2", "/api/v1/channels"),
    "/api/l2/peers":     ("l2", "/api/v1/peers"),
    "/api/l2/stats":     ("l2", "/api/v1/stats"),
    "/api/l2/nat":       ("l2", "/api/v1/nat/stun"),

    # L8 Application Layer
    "/":                    ("app", "/"),
    "/api/l8":             ("app", "/"),
    "/api/l8/dashboard":   ("app", "/api/v1/dashboard"),
    "/api/l8/monitoring":  ("app", "/api/v1/monitoring"),
    "/api/l8/agents":      ("app", "/api/v1/agents"),
    "/api/l8/economy":     ("app", "/api/v1/economy"),
    "/api/l8/analytics":   ("app", "/api/v1/analytics"),

    # L1.5 Cross-Mesh Bridge
    "/api/l1_5":         ("l1_5", "/"),
    "/api/l1_5/health":  ("l1_5", "/health"),
    "/api/l1_5/channels":("l1_5", "/channels"),
    "/api/l1_5/mesh":    ("l1_5", "/mesh"),
    "/api/l1_5/stats":   ("l1_5", "/stats"),
    "/api/l1_5/relay":   ("l1_5", "/relay"),
    "/api/l1_5/l2":      ("l1_5", "/l2"),

    # L9 Orchestration
    "/api/l9":           ("l9", "/"),
    "/api/l9/health":    ("l9", "/health"),
    "/api/l9/layers":    ("l9", "/layers"),
    "/api/l9/topology":  ("l9", "/topology"),
    "/api/l9/services":  ("l9", "/services"),
    "/api/l9/dead":      ("l9", "/dead"),
    "/api/l9/metrics":   ("l9", "/metrics"),

    # L3 Mesh Core
    "/api/l3":           ("l3", "/"),
    "/api/l3/health":    ("l3", "/health"),
    "/api/l3/topology":  ("l3", "/topology"),
    "/api/l3/nodes":     ("l3", "/nodes"),
    "/api/l3/edges":     ("l3", "/edges"),
    "/api/l3/route":     ("l3", "/route"),
    "/api/l3/flood":     ("l3", "/flood"),
    "/api/l3/metrics":   ("l3", "/metrics"),

    # L4.5 Privacy Layer
    "/api/priv":           ("priv", "/api/v1/"),
    "/api/priv/mix":       ("priv", "/api/v1/mix/add"),
    "/api/priv/dandelion": ("priv", "/api/v1/dandelion/send"),
    "/api/priv/coinjoin":  ("priv", "/api/v1/coinjoin/add"),

    # L2.5 Encryption Layer
    "/api/enc":           ("enc", "/api/v1/"),
    "/api/enc/keys":      ("enc", "/api/v1/keys"),
    "/api/enc/session":   ("enc", "/api/v1/session/create"),
    "/api/enc/encrypt":   ("enc", "/api/v1/encrypt"),
    "/api/enc/decrypt":   ("enc", "/api/v1/decrypt"),
    "/api/enc/sign":      ("enc", "/api/v1/sign"),
    "/api/enc/onion":     ("enc", "/api/v1/onion/build"),

    # L3.5 ZK Layer
    "/api/zk":           ("zk", "/api/v1/"),
    "/api/zk/health":    ("zk", "/api/v1/health"),
    "/api/zk/merkle":     ("zk", "/api/v1/merkle/agents"),
    "/api/zk/prove":     ("zk", "/api/v1/merkle"),
    "/api/zk/verify":    ("zk", "/api/v1/merkle/verify"),
    "/api/zk/vote":      ("zk", "/api/v1/integration/zk-vote"),
    "/api/zk/payment":   ("zk", "/api/v1/integration/zk-payment"),
    "/api/l4":           ("l4", "/api/v1/"),
    "/api/l4/health":    ("l4", "/api/v1/health"),
    "/api/l4/stats":     ("l4", "/api/v1/stats"),
    "/api/l4/payment":   ("l4", "/api/v1/payment"),
    "/api/l4/transfer":  ("l4", "/api/v1/transfer"),
    "/api/l4/swap":      ("l4", "/api/v1/swap"),
    "/api/l4/pool":      ("l4", "/api/v1/pool"),
}

# ─── Кеш ───
cache = {}

def get_cached_or_fetch(backend_name, path):
    """Прокси запрос с кешированием."""
    cache_key = f"{backend_name}:{path}"
    now = time.time()

    if cache_key in cache and (now - cache[cache_key]["ts"]) < CACHE_TTL:
        return cache[cache_key]["data"]

    base_url = BACKENDS.get(backend_name)
    if not base_url:
        return {"error": f"unknown backend: {backend_name}"}

    try:
        req = urllib.request.Request(f"{base_url}{path}")
        req.add_header("User-Agent", "SNIN-Gateway/1.0")
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read())
            cache[cache_key] = {"data": data, "ts": now}
            return data
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read())
        except:
            return {"error": f"HTTP {e.code}", "detail": str(e)}
    except urllib.error.URLError:
        return {"error": "unreachable", "service": backend_name}
    except json.JSONDecodeError:
        return {"error": "invalid json"}
    except Exception as e:
        return {"error": str(e)}


def server_stats():
    """Системные метрики сервера."""
    cpu = subprocess.run("cat /proc/loadavg | cut -d' ' -f1-3", shell=True,
                         capture_output=True, text=True).stdout.strip()
    mem = subprocess.run("free -m | awk 'NR==2{printf \"%.0f %.0f %.0f\", $2,$3,$4}'",
                         shell=True, capture_output=True, text=True).stdout.strip().split()
    disk = subprocess.run("df -h / | tail -1 | awk '{printf \"%s %s %s\", $2,$3,$4}'",
                          shell=True, capture_output=True, text=True).stdout.strip().split()
    uptime = subprocess.run("cat /proc/uptime | awk '{printf \"%.0f\", $1}'",
                            shell=True, capture_output=True, text=True).stdout.strip()
    return {
        "cpu": cpu, "uptime": int(uptime) if uptime else 0,
        "mem_total": mem[0] if len(mem) > 0 else 0,
        "mem_used": mem[1] if len(mem) > 1 else 0,
        "mem_free": mem[2] if len(mem) > 2 else 0,
        "disk_total": disk[0] if len(disk) > 0 else 0,
        "disk_used": disk[1] if len(disk) > 1 else 0,
        "disk_free": disk[2] if len(disk) > 2 else 0
    }


def processes_stats():
    """Список процессов."""
    try:
        ps = subprocess.run("ps aux --sort=-%mem | head -20",
                            shell=True, capture_output=True, text=True).stdout
        lines = [l.split() for l in ps.strip().split("\n")[1:]]
        return [{"user": l[0], "pid": int(l[1]), "cpu": l[2], "mem": l[3],
                  "cmd": " ".join(l[10:])[:60]} for l in lines if len(l) > 10]
    except:
        return []


def dht_stats():
    """Статус DHT нод (сбор с процессов)."""
    try:
        r = subprocess.run("ss -tlnp | grep python", shell=True,
                           capture_output=True, text=True).stdout
        ports = []
        for line in r.strip().split("\n"):
            parts = line.split()
            if len(parts) > 3:
                addr = parts[3]
                if ":" in addr and "127.0.0.1" not in addr:
                    ports.append(addr.split(":")[-1])
        return {"nodes": len(ports), "ports": ports}
    except:
        return {"nodes": 0, "ports": []}


def listener_stats():
    """Статус всех прослушиваемых портов."""
    try:
        r = subprocess.run("ss -tlnp | grep python", shell=True,
                           capture_output=True, text=True).stdout
        services = []
        for line in r.strip().split("\n"):
            if not line: continue
            parts = line.split()
            addr = parts[3] if len(parts) > 3 else "?"
            pid_info = parts[6] if len(parts) > 6 else "?"
            service_name = pid_info.split('"')[1] if '"' in pid_info else pid_info
            services.append({"addr": addr, "process": service_name})
        return services
    except:
        return []


def handle_request(method, path, body=None):
    # Внутренние роуты
    if path == "/api/server":
        return server_stats()
    if path == "/api/processes":
        return processes_stats()
    if path == "/api/dht":
        return dht_stats()
    if path == "/api/listeners":
        return listener_stats()
    if path == "/api/gateway/status":
        return {
            "status": "ok",
            "version": "1.0",
            "uptime_sec": int(time.time() - start_time),
            "backends": {k: "configured" for k in BACKENDS},
            "routes": list(ROUTES.keys()),
            "cache_size": len(cache),
        }
    if path == "/api/gateway/routes":
        return [
            {"path": p, "backend": b, "target": t}
            for p, (b, t) in ROUTES.items()
        ]

    # Прокси маршруты
    if path in ROUTES:
        backend, target = ROUTES[path]
        return get_cached_or_fetch(backend, target)

    # Support /api/* with additional path segments
    for route_path, (backend, target) in ROUTES.items():
        if path.startswith(route_path + "/"):
            extra = path[len(route_path):]
            # Keep the target path, append extra
            return get_cached_or_fetch(backend, target + extra)

    return {"error": "route not found", "path": path}


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'status': 'ok', 'service': 'api_gateway', 'port': PORT}).encode())
            return
        if not _check_rate_limit(self.client_address[0]):
            return
        data = handle_request("GET", self.path)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Cache-Control", f"max-age={CACHE_TTL}")
        self.send_header("X-SNIN-Gateway", "v1.0")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def log_message(self, fmt, *args):
        ts = time.strftime("%H:%M:%S")
        print(f"[GATEWAY {ts}] {args[0]} {args[1]} → {args[2]}", flush=True)


start_time = time.time()

if __name__ == "__main__":
    s = http.server.HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"\n{'='*50}")
    print(f"🚀 SNIN API Gateway v1.0 — порт {PORT}")
    print(f"   {len(ROUTES)} маршрутов → {len(BACKENDS)} бэкендов")
    print(f"   Кеш: {CACHE_TTL} сек")
    print(f"{'='*50}\n")
    s.serve_forever()

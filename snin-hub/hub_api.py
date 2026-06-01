#!/usr/bin/env python3
"""SNIN Hub API — прокси через API Gateway + статика."""
import json, os, time, subprocess, socket, http.server, urllib.request

PORT = int(os.environ.get("PORT", 9950))
GATEWAY = "http://127.0.0.1:8083"

def proxy(path):
    try:
        r = urllib.request.urlopen(f"{GATEWAY}{path}", timeout=1)
        return json.loads(r.read())
    except Exception as e:
        return {"status": "gateway_unavailable", "error": str(e)[:40]}

def server_stats():
    cpu = subprocess.run("cat /proc/loadavg | cut -d' ' -f1-3", shell=True, capture_output=True, text=True).stdout.strip()
    mem = subprocess.run("free -m | awk 'NR==2{printf \"%.0f %.0f %.0f\", $2,$3,$4}'", shell=True, capture_output=True, text=True).stdout.strip().split()
    disk = subprocess.run("df -h / | tail -1 | awk '{printf \"%s %s %s\", $2,$3,$4}'", shell=True, capture_output=True, text=True).stdout.strip().split()
    uptime = subprocess.run("cat /proc/uptime | awk '{printf \"%.0f\", $1}'", shell=True, capture_output=True, text=True).stdout.strip()
    return {
        "cpu": cpu, "uptime": int(uptime) if uptime else 0,
        "mem_total": mem[0] if len(mem) > 0 else 0, "mem_used": mem[1] if len(mem) > 1 else 0,
        "mem_free": mem[2] if len(mem) > 2 else 0,
        "disk_total": disk[0] if len(disk) > 0 else 0, "disk_used": disk[1] if len(disk) > 1 else 0,
        "disk_free": disk[2] if len(disk) > 2 else 0
    }

def processes_stats():
    out = subprocess.run("ps aux --sort=-%mem | grep -E 'python3|node' | head -10 | awk '{printf \"%s|%s|%s|%s\\n\",$2,$11,$3,$4}'", shell=True, capture_output=True, text=True).stdout.strip().split('\n')
    procs = []
    for line in out:
        parts = line.split('|')
        if len(parts) >= 4: procs.append({"pid": parts[0], "cmd": parts[1][:40], "cpu": parts[2], "mem": parts[3]})
    return procs

def dht_stats():
    result = {}
    for port in [9998, 9999]:
        try:
            s = socket.socket()
            s.settimeout(2)
            s.connect(('127.0.0.1', port))
            import uuid
            msg = json.dumps({'kind':0,'pubkey':uuid.uuid4().hex,'name':'hub','content':{'type':'ping','port':0,'ts':time.time()}}).encode()+b'\n'
            s.send(msg)
            r = s.recv(4096).decode()
            d = json.loads(r)
            result[f"dht-{port}"] = {"status": "ok", "peers": d.get("content",{}).get("peers",0), "node_id": d.get("content",{}).get("node_id","")[:16]}
            s.close()
        except Exception as e:
            result[f"dht-{port}"] = {"status": "error", "error": str(e)[:30]}
    return result

def handle_request(method, path, body=None):
    # Отделяем путь от query-параметров
    from urllib.parse import urlparse
    parsed_path = urlparse(path).path
    # Внутренние (локальные) роуты
    if parsed_path == "/health" or parsed_path == "/api/health":
        return {"status": "ok", "layer": "snin-hub", "port": PORT}
    if parsed_path == "/api/server":   return server_stats()
    if parsed_path == "/api/processes": return processes_stats()
    if parsed_path == "/api/dht":      return dht_stats()
    
    # Relay Health Daemon прокси
    if parsed_path == "/api/relay-health":
        try:
            r = urllib.request.urlopen("http://127.0.0.1:9929/api/health", timeout=2)
            return json.loads(r.read())
        except Exception as e:
            return {"status": "health_daemon_unavailable", "error": str(e)[:40], "total": 0, "alive": 0}

    # Relay Health список релеев
    if parsed_path == "/api/relays":
        try:
            r = urllib.request.urlopen("http://127.0.0.1:9929/api/relays", timeout=3)
            raw = r.read().decode()
            data = json.loads(raw)
            # data может быть списком или словарём
            if isinstance(data, list):
                return {"total": len(data), "alive": sum(1 for x in data if isinstance(x,dict) and x.get("status")=="alive"), "relays": data}
            return data
        except Exception as e:
            return {"total": 0, "alive": 0, "error": str(e)[:40]}

    # Supervisor статус
    if parsed_path == "/api/supervisor":
        try:
            sv = os.path.join(os.path.dirname(__file__), "supervisor_status.json")
            if os.path.isfile(sv):
                with open(sv) as f: return json.load(f)
            return {"status": "no_supervisor_file"}
        except Exception as e:
            return {"status": "error", "error": str(e)[:40]}

    # Bridge шарды (5 gossip шардов на портах 9100-9104 + cross-mesh 8201-8202)
    if parsed_path == "/api/bridge-shards":
        shards = {}
        for port in range(9100, 9105):
            try:
                s = socket.socket()
                s.settimeout(1)
                s.connect(('127.0.0.1', port))
                shards[f"gossip-{port}"] = {"alive": True, "port": port}
                s.close()
            except Exception as e:
                shards[f"gossip-{port}"] = {"alive": False, "error": str(e)[:20], "port": port}
        for name, port in [("cross-mesh", 8201), ("l1_5-bridge", 8202), ("l3-mesh-core", 9300), ("mesh-api", 9907)]:
            try:
                s = socket.socket()
                s.settimeout(1)
                s.connect(('127.0.0.1', port))
                shards[name] = {"alive": True, "port": port}
                s.close()
            except:
                shards[name] = {"alive": False, "port": port}
        return shards

    # API Gateway статус
    if parsed_path == "/api/gateway":
        try:
            r = urllib.request.urlopen("http://127.0.0.1:8083/health", timeout=2)
            return {"alive": True, "status_code": r.getcode()}
        except Exception as e:
            return {"alive": False, "error": str(e)[:30]}

    # Identity API
    if parsed_path == "/api/identity":
        try:
            r = urllib.request.urlopen("http://127.0.0.1:9940/identity/all", timeout=2)
            return json.loads(r.read())
        except Exception as e:
            return {"status": "identity_unavailable", "error": str(e)[:30]}

    # Agents (forecaster + archivist + cryter)
    if parsed_path == "/api/agents":
        agents = {}
        for name, port in [("forecaster", 8200), ("archivist", 8091), ("scc_agent", 8196)]:
            try:
                r = urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=2)
                body = r.read().decode()[:150]
                agents[name] = {"alive": True, "body": body[:60]}
            except Exception as e:
                agents[name] = {"alive": False, "error": str(e)[:30]}
        # Cryter — проверяем через процесс (нет HTTP, но живёт в системе)
        try:
            sp = subprocess.run(["pgrep", "-f", "cryter_v10_daemon"], capture_output=True, text=True, timeout=3)
            if sp.stdout.strip():
                agents["cryter_ai"] = {"alive": True, "pid": sp.stdout.strip().split("\n")[0], "type": "cryter_v10_daemon"}
            else:
                agents["cryter_ai"] = {"alive": False, "error": "process not found"}
        except Exception as e:
            agents["cryter_ai"] = {"alive": False, "error": str(e)[:30]}
        return agents

    # L9 Layers (архитектура 13 слоёв)
    if parsed_path == "/api/layers":
        try:
            r = urllib.request.urlopen("http://127.0.0.1:9900/layers", timeout=2)
            return json.loads(r.read())
        except Exception as e:
            return {"error": str(e)[:40], "layers": []}
    if parsed_path == "/api/l9-health":
        try:
            r = urllib.request.urlopen("http://127.0.0.1:9900/health", timeout=2)
            return json.loads(r.read())
        except Exception as e:
            return {"status": "error", "error": str(e)[:40]}

    # Smart Router статус (мозг сети)
    if parsed_path == "/api/smart-router":
        try:
            r = urllib.request.urlopen("http://127.0.0.1:9933/", timeout=2)
            return json.loads(r.read())
        except Exception as e:
            return {"status": "error", "error": str(e)[:40]}

    # Сводка сети — агрегированные данные
    if parsed_path == "/api/network":
        network = {"layers": {}, "channels": {}, "dht": {}, "agents": {}, "timestamp": time.time()}
        # Слои
        try:
            r = urllib.request.urlopen("http://127.0.0.1:9900/layers", timeout=2)
            layers = json.loads(r.read())
            network["layers"] = layers
        except: pass
        # Smart Router DHT
        try:
            r = urllib.request.urlopen("http://127.0.0.1:9933/", timeout=2)
            sr = json.loads(r.read())
            network["dht"] = sr.get("dht", {})
            network["channels"] = sr.get("channels", {})
            network["sr_stats"] = {"received": sr.get("stats",{}).get("received",0), "forwarded": sr.get("stats",{}).get("forwarded",0), "failed": sr.get("stats",{}).get("failed",0), "connections": sr.get("stats",{}).get("connections",0)}
        except: pass
        # Supervisor
        try:
            sv = os.path.join(os.path.dirname(__file__), "supervisor_status.json")
            if os.path.isfile(sv):
                with open(sv) as f: sup = json.load(f)
                network["supervisor"] = {"alive": sup.get("alive",0), "dead": sup.get("dead",0), "total": sup.get("total_services",0), "restarts": sup.get("total_restarts",0)}
        except: pass
        return network

    # Топология сети — граф нод + соединения
    if parsed_path == "/api/topology":
        topo = {"nodes": [], "edges": [], "channels": {}, "mesh": {}, "bridge": {}, "timestamp": time.time()}
        # 1. DHT агенты из Smart Router
        try:
            r = urllib.request.urlopen("http://127.0.0.1:9933/", timeout=2)
            sr = json.loads(r.read())
            topo["channels"] = sr.get("channels",{})
            dht = sr.get("dht",{})
            topo["dht"] = {"n_nodes": dht.get("n_nodes",0), "n_agents": dht.get("n_agents",0)}
            topo["sr_stats"] = sr.get("stats",{})
            # агенты из Redis (соединяемся напрямую через CLI)
            import subprocess as sp
            raw = sp.run("redis-cli hgetall dht:agents 2>/dev/null", shell=True, capture_output=True, text=True, timeout=3)
            if raw.stdout.strip():
                lines = raw.stdout.strip().split('\n')
                i = 0
                while i < len(lines) - 1:
                    pk = lines[i].strip()
                    agent_raw = lines[i+1].strip()
                    i += 2
                    try:
                        agent = json.loads(agent_raw)
                        name = agent.get("name", pk[:12])
                        role = agent.get("role", "agent")
                        tier = agent.get("tier", 3)
                        relay = agent.get("relay_addr", f"127.0.0.1:{agent.get('port',9932)}")
                        topo["nodes"].append({
                            "id": pk[:16], "name": name, "role": role, "tier": tier,
                            "relay": relay, "alive": agent.get("alive",True),
                            "type": "agent", "last_seen": agent.get("last_seen",0)
                        })
                    except: pass
        except: pass
        # 2. L3 Mesh Core (9 nodes, 36 edges)
        try:
            r = urllib.request.urlopen("http://127.0.0.1:9300/health", timeout=2)
            mesh = json.loads(r.read())
            topo["mesh"] = {"alive": True, "nodes": mesh.get("nodes_alive",0), "total": mesh.get("nodes_total",0), "edges": mesh.get("edges",0), "topology_version": mesh.get("topology_version",0), "uptime_s": mesh.get("uptime_s",0)}
        except Exception as e:
            topo["mesh"] = {"alive": False, "error": str(e)[:30]}
        # 3. Bridge L1.5
        try:
            r = urllib.request.urlopen("http://127.0.0.1:9945/health", timeout=2)
            br = json.loads(r.read())
            topo["bridge"] = {"alive": True, "mesh_name": br.get("mesh_name",""), "mesh_id": br.get("mesh_id","")[:24], "remote_meshes": br.get("remote_meshes",{})}
        except Exception as e:
            topo["bridge"] = {"alive": False, "error": str(e)[:30]}
        # 4. Supervisor статика
        try:
            sv = os.path.join(os.path.dirname(__file__), "supervisor_status.json")
            if os.path.isfile(sv):
                with open(sv) as f: sup = json.load(f)
                topo["supervisor"] = {"alive": sup.get("alive",0), "total": sup.get("total_services",0), "services": sup.get("services",[])}
        except: pass
        return topo

    # DHT deep — с полными деталями агентов
    if parsed_path == "/api/dht-deep":
        result = {}
        for port in [9998, 9999]:
            try:
                s = socket.socket()
                s.settimeout(2)
                s.connect(('127.0.0.1', port))
                import uuid
                msg = json.dumps({'kind':0,'pubkey':uuid.uuid4().hex,'name':'hub','content':{'type':'ping','port':0,'ts':time.time()}}).encode()+b'\n'
                s.send(msg)
                r = s.recv(8192).decode()
                d = json.loads(r)
                content = d.get("content", {})
                result[f"dht-{port}"] = {
                    "status": "ok",
                    "peers": content.get("peers", 0),
                    "node_id": content.get("node_id", "")[:16],
                    "agents": content.get("agents", {}),
                    "network_size": content.get("network_size", 0)
                }
                s.close()
            except Exception as e:
                result[f"dht-{port}"] = {"status": "error", "error": str(e)[:30]}
        return result

    # Daemons — сбор всех процессов сервера
    if parsed_path == "/api/daemons":
        from daemon_collector import collect_processes, collect_libs, collect_caches
        data = {
            "processes": collect_processes(),
            "libs": collect_libs(),
            "caches": collect_caches(),
            "timestamp": time.time()
        }
        return data
    if parsed_path == "/api/libraries":
        from daemon_collector import collect_libs
        return collect_libs()
    if parsed_path == "/api/dbs":
        from daemon_collector import collect_caches
        caches = collect_caches()
        return {"total": len(caches.get("databases",[])), "total_size_mb": caches.get("total_db_size_mb",0), "databases": caches.get("databases",[])}
    if parsed_path == "/api/ram_history":
        try:
            with open(os.path.join(os.path.dirname(__file__), "ram_history.json")) as f:
                return json.load(f)
        except:
            return []
    if parsed_path == "/api/system_info":
        info = {"hostname": socket.gethostname()}
        with open("/proc/meminfo") as f:
            for line in f:
                p = line.split()
                if p[0] == "MemTotal:": info["mem_total_mb"] = int(p[1]) // 1024
                elif p[0] == "MemAvailable:": info["mem_available_mb"] = int(p[1]) // 1024
        with open("/proc/uptime") as f:
            info["uptime_sec"] = float(f.read().split()[0])
        return info

    # Чат (локальный файл) — умный poll + история
    if parsed_path == "/api/chat":
        chat_store = os.path.join(os.path.dirname(__file__), "chat_store.json")
        if os.path.isfile(chat_store):
            with open(chat_store) as f:
                all_msgs = json.load(f)
            return all_msgs[-50:]
        return []
    if path.startswith("/api/chat/poll"):
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(path).query)
        since = float(qs.get("since", ["0"])[0])
        chat_store = os.path.join(os.path.dirname(__file__), "chat_store.json")
        if os.path.isfile(chat_store):
            with open(chat_store) as f:
                all_msgs = json.load(f)
            new_msgs = [m for m in all_msgs if m.get("ts", 0) > since]
            return {"new": new_msgs, "ts": time.time()}
        return {"new": [], "ts": time.time()}
    if path.startswith("/api/chat/history"):
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(path).query)
        before = float(qs.get("before", ["9999999999"])[0])
        limit = int(qs.get("limit", [50])[0])
        chat_store = os.path.join(os.path.dirname(__file__), "chat_store.json")
        if os.path.isfile(chat_store):
            with open(chat_store) as f:
                all_msgs = json.load(f)
            older = [m for m in all_msgs if m.get("ts", 0) < before][-limit:]
            return {"msgs": older, "has_more": len(all_msgs) > all_msgs.index(older[0])+len(older) if older else False}
        return {"msgs": [], "has_more": False}
    if path.startswith("/api/chat/send"):
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(path).query)
        nick = qs.get("nick", ["anon"])[0]
        msg = qs.get("msg", [""])[0]
        pid = qs.get("pid", [""])[0]  # публичный ключ
        if msg:
            chat_store = os.path.join(os.path.dirname(__file__), "chat_store.json")
            msgs = []
            if os.path.isfile(chat_store):
                with open(chat_store) as f: msgs = json.load(f)
            msgs.append({"nick": nick[:20], "msg": msg[:500], "ts": time.time(), "pid": pid[:64]})
            # Храним последние 1000 сообщений
            if len(msgs) > 1000:
                msgs = msgs[-1000:]
            with open(chat_store, "w") as f: json.dump(msgs, f)
        return {"status": "ok"}
    if parsed_path == "/api/chat/contacts":
        # Возвращаем список уникальных участников за последние 24ч
        chat_store = os.path.join(os.path.dirname(__file__), "chat_store.json")
        contacts = {}
        if os.path.isfile(chat_store):
            with open(chat_store) as f:
                all_msgs = json.load(f)
            cutoff = time.time() - 86400
            for m in all_msgs:
                if m.get("ts", 0) > cutoff:
                    pid = m.get("pid", "")
                    nick = m.get("nick", "anon")
                    if pid not in contacts:
                        contacts[pid] = {"nick": nick, "pid": pid, "last_seen": m["ts"]}
                    elif m["ts"] > contacts[pid]["last_seen"]:
                        contacts[pid] = {"nick": nick, "pid": pid, "last_seen": m["ts"]}
        return {"contacts": list(contacts.values())}

    # Всё остальное — через API Gateway (fallback если gateway dead)
    route_map = {
        "/api/relay":   "/api/relay",
        "/api/agents":  "/api/relay/agents",
        "/api/kinds":   "/api/relay/kinds",
        "/api/nip11":   "/api/relay/nip11",
        "/api/activity":"/api/relay/activity",
        "/api/p2p":     "/api/p2p",
        "/api/peers":   "/api/p2p/peers",
    }
    gw_path = route_map.get(path, path)
    result = proxy(gw_path)
    if isinstance(result, dict) and "gateway_unavailable" in result.get("status",""):
        # Получаем данные напрямую из супервизора
        return fallback_stats(path)
    return result

def fallback_stats(path):
    """Прямые данные когда gateway недоступен."""
    if parsed_path == "/api/relay":
        try:
            r = urllib.request.urlopen("http://127.0.0.1:8198/", timeout=1)
            return json.loads(r.read())
        except: pass
    if parsed_path == "/api/agents":
        try:
            r = urllib.request.urlopen("http://127.0.0.1:9940/identity/all", timeout=1)
            return json.loads(r.read())
        except: pass
    return {"status": "fallback", "note": "direct data unavailable"}

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health" or self.path == "/api/health":
            data = {"status": "ok", "layer": "snin-hub", "port": PORT}
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())
            return
        if self.path.startswith("/api/"):
            data = handle_request("GET", self.path)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())
            return
        if self.path == "/": self.path = "/index.html"
        file_path = "/home/agent/data/sites/snin-hub" + self.path
        if os.path.isfile(file_path):
            ext = os.path.splitext(file_path)[1]
            ct = {"html":"text/html","css":"text/css","js":"application/javascript","png":"image/png","svg":"image/svg+xml","json":"application/json"}.get(ext[1:], "text/plain")
            self.send_response(200)
            self.send_header("Content-Type", ct)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            with open(file_path, "rb") as f: self.wfile.write(f.read())
        else:
            self.send_response(404)
            self.end_headers()
    def log_message(self, f, *a): pass
    def do_HEAD(self):
        """HEAD = GET без тела — site-router health check."""
        self.do_GET()

if __name__ == "__main__":
    s = http.server.HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[HUB] API Gateway proxy on :{PORT}")
    s.serve_forever()

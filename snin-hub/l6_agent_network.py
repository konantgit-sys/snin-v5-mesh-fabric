"""
SNIN L6 — AI Agent Network (Universal Architecture 2.0, порт :9400)

Сеть агентов, объединяющая:
  — L5 Identity (DID, reputation, trust graph)
  — L4 Payment (balances, transfers)
  — L7 DAO (governance, proposals, voting)

Функции:
  — Регистрация/дерегстрация агентов в сети
  — Статус live/offline с heartbeat
  — Mesh-общение между агентами (broadcast, direct, topic)
  — DAO-участие: голосование от лица агентов
  — Платежи: баланс агента, переводы между агентами
"""

import json, logging, os, sys, time, uuid, threading
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel
import uvicorn
import urllib.request
import urllib.error

app = FastAPI(title="SNIN L6 Agent Network", version="1.0.0")
api = APIRouter(prefix="/api/v1")

# ─── Health endpoint для L9 (без префикса) ─────
@app.get("/health")
def root_health():
    return {"status": "ok", "layer": "L6 Agent Network", "ts": time.time(), "alive": True}

# ───── Internal state ─────
agents: Dict[str, dict] = {}        # agent_name → info
mesh_messages: list = []            # broadcast log
MESH_MAX = 200                       # max stored messages

# ───── Models ─────

class AgentRegister(BaseModel):
    agent_name: str
    endpoint: str = ""               # URL для связи с агентом
    capabilities: list[str] = []     # capabilities
    public_key: str = ""

class AgentMessage(BaseModel):
    sender: str
    recipient: str = "*"             # * = broadcast
    content: str
    topic: str = "general"

class AgentVote(BaseModel):
    agent_name: str
    proposal_id: str
    vote: str                        # for / against / abstain
    weight: float = 1.0

# ───── Helpers ─────

def _l4_get(path: str) -> Optional[dict]:
    try:
        req = urllib.request.Request(f"http://127.0.0.1:9200/api/v1{path}",
                                      headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None

def _l5_get(path: str) -> Optional[dict]:
    try:
        req = urllib.request.Request(f"http://127.0.0.1:9940{path}",
                                      headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None

def _l7_get(path: str) -> Optional[dict]:
    try:
        req = urllib.request.Request(f"http://127.0.0.1:8082/api/dao{path}",
                                      headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None

def _l7_post(path: str, data: dict) -> Optional[dict]:
    try:
        body = json.dumps(data).encode()
        req = urllib.request.Request(f"http://127.0.0.1:8082/api/dao{path}",
                                      data=body,
                                      headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


# ─── Heartbeat cleaner ───

def _cleanup_loop():
    """Каждые 60с чистит мёртвых агентов (no heartbeat > 300с)."""
    while True:
        time.sleep(60)
        now = time.time()
        dead = [name for name, info in agents.items()
                if now - info.get("last_seen", 0) > 300]
        for name in dead:
            agents[name]["status"] = "offline"
            agents[name]["status_changed"] = time.time()


# ─── Endpoints: Health & Status ───

@api.get("/")
def root():
    return {
        "service": "SNIN L6 Agent Network",
        "version": "1.0.0",
        "agents": len(agents),
        "mesh_messages": len(mesh_messages),
        "status": "live"
    }

@api.get("/health")
def health():
    """Статус L6 и связь с соседними слоями."""
    now = time.time()
    online = sum(1 for a in agents.values() if a.get("status") == "online")
    offline = sum(1 for a in agents.values() if a.get("status") == "offline")

    l5 = _l5_get("/health")
    l4 = _l4_get("/health")
    l7 = _l7_get("/health")

    return {
        "l6": "ok",
        "ts": now,
        "agents": {"total": len(agents), "online": online, "offline": offline},
        "layers": {
            "l5_identity": "ok" if l5 else "unreachable",
            "l4_payment": "ok" if l4 else "unreachable",
            "l7_dao": "ok" if l7 else "unreachable",
        }
    }


# ─── Agent Registry ───

@api.post("/agents/register")
def register_agent(req: AgentRegister):
    """Регистрация агента в L6 сети."""
    if req.agent_name in agents:
        agents[req.agent_name].update({
            "endpoint": req.endpoint,
            "capabilities": req.capabilities,
            "public_key": req.public_key or agents[req.agent_name].get("public_key", ""),
            "status": "online",
            "last_seen": time.time(),
            "status_changed": time.time(),
        })
        return {"status": "updated", "agent": req.agent_name}

    agents[req.agent_name] = {
        "agent_name": req.agent_name,
        "endpoint": req.endpoint,
        "capabilities": req.capabilities,
        "public_key": req.public_key,
        "status": "online",
        "last_seen": time.time(),
        "joined_at": time.time(),
        "status_changed": time.time(),
    }

    # Пробуем синхронизировать в L5
    try:
        from dao.l5_bridge import sync_l5_to_dao
        # mock dao_core — используем прямой вызов L5
        _l5_get("/identity/all")  # проверяем что L5 жив
    except Exception:
        pass

    return {"status": "registered", "agent": req.agent_name}


@api.post("/agents/{name}/heartbeat")
def agent_heartbeat(name: str):
    """Heartbeat — поддерживает статус online."""
    if name not in agents:
        raise HTTPException(404, f"Agent {name} not found")
    agents[name]["last_seen"] = time.time()
    agents[name]["status"] = "online"
    return {"status": "ok", "agent": name, "last_seen": agents[name]["last_seen"]}


@api.get("/agents")
def list_agents(status: Optional[str] = None, limit: int = 50):
    """Список агентов в сети."""
    result = []
    for name, info in agents.items():
        if status and info.get("status") != status:
            continue
        # Обогащаем из L5
        l5_data = _l5_get(f"/identity/{name}")
        rep = 0
        if l5_data and "reputation" in l5_data:
            if isinstance(l5_data["reputation"], dict):
                rep = l5_data["reputation"].get("score", 0)
            else:
                rep = l5_data["reputation"]
        # Баланс из L4
        balance = 0
        l4_data = _l4_get(f"/payment")
        # Возвращаем базовое
        result.append({
            "agent_name": name,
            "status": info.get("status", "unknown"),
            "capabilities": info.get("capabilities", []),
            "last_seen": info.get("last_seen", 0),
            "reputation": round(rep, 4),
            "balance": balance,
            "uptime": round(time.time() - info.get("joined_at", time.time()), 0),
        })
    return {"agents": result[:limit], "count": len(result)}


@api.get("/agents/{name}")
def get_agent(name: str):
    """Полная информация об агенте из всех слоёв."""
    if name not in agents:
        raise HTTPException(404, f"Agent {name} not found in L6")

    info = agents[name]
    result = {"l6": info}

    # L5: репутация, trust, DID
    l5_data = _l5_get(f"/identity/{name}")
    if l5_data:
        result["l5_identity"] = {
            "did": l5_data.get("did", ""),
            "npub": l5_data.get("npub", ""),
            "reputation": l5_data.get("reputation", 0),
            "attestations": l5_data.get("attestations", []),
        }

    trust_data = _l5_get(f"/trust/{name}")
    if trust_data:
        result["l5_trust"] = {
            "trust_score": trust_data.get("trust_score", 0),
            "attestations_given": trust_data.get("attestations_given", []),
            "attestations_received": trust_data.get("attestations_received", []),
        }

    # L4: баланс из snin-pay
    try:
        req = urllib.request.Request(f"http://127.0.0.1:8191/api/v1/balance/{name}")
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode())
            result["l4_balance"] = data.get("balance", 0)
    except Exception:
        result["l4_balance"] = 0

    # L7: DAO статус
    l7_data = _l7_get(f"/dao/agent/{name}")
    if l7_data:
        result["l7_dao"] = {
            "nft_class": l7_data.get("nft_class", ""),
            "weight": l7_data.get("weight", 0),
            "rarity": l7_data.get("rarity", ""),
            "genesis_wave": l7_data.get("genesis_wave", 0),
        }

    return result


@api.delete("/agents/{name}")
def unregister_agent(name: str):
    """Дерегистрация агента из сети."""
    if name not in agents:
        raise HTTPException(404, f"Agent {name} not found")
    info = agents.pop(name)
    return {"status": "unregistered", "agent": name, "was_online": info.get("status") == "online"}


# ─── Mesh Communication ───

@api.post("/mesh/send")
def send_message(msg: AgentMessage):
    """Отправка сообщения агенту (direct или broadcast)."""
    if msg.sender not in agents:
        raise HTTPException(400, f"Sender {msg.sender} не зарегистрирован в L6")

    entry = {
        "id": uuid.uuid4().hex[:12],
        "sender": msg.sender,
        "recipient": msg.recipient,
        "content": msg.content,
        "topic": msg.topic,
        "ts": time.time(),
    }
    mesh_messages.append(entry)

    # Обрезаем
    while len(mesh_messages) > MESH_MAX:
        mesh_messages.pop(0)

    if msg.recipient == "*":
        return {"status": "broadcast", "id": entry["id"], "targets": len(agents)}
    elif msg.recipient in agents:
        return {"status": "delivered", "id": entry["id"], "recipient": msg.recipient}
    else:
        return {"status": "queued", "id": entry["id"], "note": f"recipient {msg.recipient} not online"}


@api.get("/mesh/messages")
def get_mesh_messages(topic: Optional[str] = None, limit: int = 20):
    """Лента mesh-сообщений."""
    result = mesh_messages[-limit:] if not topic else \
        [m for m in mesh_messages if m["topic"] == topic][-limit:]
    return {"messages": result, "count": len(result)}


@api.get("/mesh/messages/{agent_name}")
def get_agent_messages(agent_name: str, limit: int = 20):
    """Сообщения для конкретного агента."""
    result = [m for m in mesh_messages
              if m["recipient"] in (agent_name, "*") or m["sender"] == agent_name]
    return {"messages": result[-limit:], "count": len(result)}


# ─── DAO Integration ───

@api.post("/dao/vote")
def agent_vote(req: AgentVote):
    """Голосование агента в DAO через L7."""
    if req.agent_name not in agents:
        raise HTTPException(400, f"Agent {req.agent_name} не в L6 сети")

    # Голосуем через L7 DAO API
    result = _l7_post("/vote", {
        "proposal_id": req.proposal_id,
        "voter_id": req.agent_name,
        "vote": req.vote,
        "weight": req.weight,
    })

    if result is None:
        raise HTTPException(502, "DAO (L7) не ответил")
    return {"status": "voted", "agent": req.agent_name, **result}


@api.get("/dao/proposals")
def dao_proposals():
    """Список proposals из L7."""
    data = _l7_get("/proposals")
    if data is None:
        raise HTTPException(502, "DAO (L7) не ответил")
    return data


# ─── L5 Sync ───

@api.post("/sync/from-l5")
def sync_from_l5():
    """Синхронизация: все агенты из L5 → регистрация в L6."""
    data = _l5_get("/identity/all")
    if not data:
        raise HTTPException(502, "L5 не ответил")

    registered = []
    for agent in data.get("agents", []):
        name = agent.get("agent_name", "")
        if name and name not in agents:
            agents[name] = {
                "agent_name": name,
                "endpoint": "",
                "capabilities": ["l5_synced"],
                "public_key": agent.get("pubkey", ""),
                "status": "online",
                "last_seen": time.time(),
                "joined_at": time.time(),
                "status_changed": time.time(),
            }
            registered.append(name)

    return {"synced": len(registered), "total_agents": len(agents), "registered": registered}


# ─── Layer Status (dashboard data) ───

@api.get("/layers")
def all_layers():
    """Статус всех слоёв SNIN."""
    layers = {}

    # L5
    l5 = _l5_get("/health")
    layers["l5_identity"] = {
        "status": "ok" if l5 else "offline",
        "agents": l5.get("agents_registered", 0) if l5 else 0,
    }

    # L4
    l4 = _l4_get("/health")
    layers["l4_payment"] = {
        "status": "ok" if l4 else "offline",
        "channels": list(l4.get("channels", {}).keys()) if l4 else [],
    }

    l4s = _l4_get("/stats")
    layers["l4_stats"] = l4s.get("channels", {}) if l4s else {}

    # L7
    l7 = _l7_get("/health")
    layers["l7_dao"] = {
        "status": "ok" if l7 else "offline",
    }

    # L3
    try:
        req = urllib.request.Request("http://127.0.0.1:8083/api/mesh/status")
        with urllib.request.urlopen(req, timeout=3) as resp:
            layers["l3_mesh"] = "ok"
    except Exception:
        layers["l3_mesh"] = "offline"

    return {
        "l6_network": {"agents": len(agents), "online": sum(1 for a in agents.values() if a.get("status")=="online")},
        "layers": layers,
    }


# ─── Threaded cleanup ───
threading.Thread(target=_cleanup_loop, daemon=True).start()

# ─── Mount ───
app.include_router(api)

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9400
    print(f"[L6] Starting Agent Network on :{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")

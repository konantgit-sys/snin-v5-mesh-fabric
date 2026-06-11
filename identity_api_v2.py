#!/usr/bin/env python3
"""
SNIN Identity API v2 — Layer 5 REST endpoint.
FastAPI + uvicorn + lazy in-memory cache.
"""

import json, os, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from reputation import calculate_reputation, get_all_reputations, get_reputation_for_pubkey
from mesh_identity import (load_or_create_identity, pubkey_to_did, get_all_dids,
                            sign_attestation, get_attestations)
from trust_graph import get_trust_metrics, get_agent_trust
from vc_format import create_did_document, attestation_to_vc

from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

PORT = 9940
for a in sys.argv[1:]:
    if a.isdigit():
        PORT = int(a)
        break

_start_time = time.time()

# ── In-memory cache (ленивый, без threading) ──
_cache = {"dids": [], "reps": {}, "time": 0.0}
CACHE_TTL = 0.5  # обновление раз в 500ms

def _ensure_cache():
    """Обновить кэш если устарел"""
    now = time.time()
    if now - _cache["time"] < CACHE_TTL and _cache["time"] > 0:
        return
    try:
        _cache["dids"] = get_all_dids()
        _cache["reps"] = get_all_reputations()
        _cache["time"] = now
    except Exception as e:
        if _cache["time"] == 0:
            raise  # первый раз — реальная ошибка
        # иначе игнорим, используем старый кэш

app = FastAPI(title="SNIN Identity API v2")

@app.get("/health")
@app.get("/api/health")
async def health():
    _ensure_cache()
    return {
        "status": "ok",
        "layer": "L5 — Identity & Reputation",
        "agents_registered": len(_cache["dids"]),
        "agents_scored": len(_cache["reps"]),
        "uptime": time.time() - _start_time,
        "cache_ttl": CACHE_TTL,
        "cache_mode": "lazy",
    }

@app.get("/identity/all")
async def identity_all():
    _ensure_cache()
    agents = []
    for d in _cache["dids"]:
        rep = _cache["reps"].get(d["agent_name"], calculate_reputation(d["agent_name"]))
        agents.append({
            "agent_name": d["agent_name"], "did": d["did"],
            "npub": d["npub"], "pubkey": d["pubkey"],
            "reputation": rep["score"] if isinstance(rep, dict) else rep,
            "attestations": d["attestations"],
            "links": d["links"],
        })
    return {"agents": agents, "count": len(agents)}

@app.get("/identity/top")
async def identity_top():
    _ensure_cache()
    agents = []
    for name, rep in sorted(_cache["reps"].items(), key=lambda x: -x[1]["score"]):
        agents.append({"agent_name": name, "score": rep["score"]})
    return {"top": agents[:10], "count": len(agents)}

@app.get("/identity/did/{did}")
async def identity_did(did: str):
    _ensure_cache()
    for d in _cache["dids"]:
        if d["did"] == did:
            rep = _cache["reps"].get(d["agent_name"], calculate_reputation(d["agent_name"]))
            return {"agent_name": d["agent_name"], "did": did, "npub": d["npub"],
                    "pubkey": d["pubkey"], "reputation": rep["score"] if isinstance(rep, dict) else rep,
                    "attestations": d["attestations"], "links": d["links"]}
    return JSONResponse(status_code=404, content={"error": f"DID not found: {did}"})

@app.get("/identity/{name}")
async def identity_name(name: str):
    _ensure_cache()
    for d in _cache["dids"]:
        if d["agent_name"] == name:
            rep = _cache["reps"].get(name, calculate_reputation(name))
            identity = load_or_create_identity(name)
            did = pubkey_to_did(identity["mesh_pubkey"])
            return {"agent_name": name, "did": did, "npub": d["npub"],
                    "pubkey": d["pubkey"], "reputation": rep,
                    "full_identity": {k: v for k, v in identity.items()
                                      if k not in ("mesh_privkey","packet_privkey","cipher_privkey")}}
    return JSONResponse(status_code=404, content={"error": f"Agent not found: {name}"})

@app.get("/trust-graph")
async def trust_graph():
    m = get_trust_metrics()
    return {"service": "SNIN Trust Graph", "nodes": m["nodes"], "edges": m["edges"],
            "isolated_agents": m["isolated_agents"], "top_trusted": m["trusted_agents"][:10]}

@app.get("/trust/{agent}")
async def trust(agent: str):
    return get_agent_trust(agent)

@app.get("/did-document/{did_query:path}")
async def did_document(did_query: str):
    _ensure_cache()
    for d in _cache["dids"]:
        if d["did"] == did_query or did_query in d["did"] or did_query in d["pubkey"]:
            return create_did_document(d["pubkey"], d["agent_name"])
    if len(did_query) >= 32:
        return create_did_document(did_query, "unknown")
    return JSONResponse(status_code=404, content={"error": f"DID not found: {did_query}"})

@app.get("/vc/{agent_query}")
async def verifiable_credential(agent_query: str):
    _ensure_cache()
    for d in _cache["dids"]:
        if d["agent_name"] == agent_query or d["did"] == agent_query:
            rep = _cache["reps"].get(d["agent_name"], calculate_reputation(d["agent_name"]))
            score = rep["score"] if isinstance(rep, dict) else rep
            vc = attestation_to_vc({"issuer_did": "did:snin:network", "target_did": d["did"],
                                    "target": d["did"], "role": "agent", "weight": score,
                                    "description": f"SNIN Agent: {d['agent_name']}"})
            return {"agent": d["agent_name"], "did": d["did"], "reputation": score, "vc": vc}
    return JSONResponse(status_code=404, content={"error": f"Agent not found: {agent_query}"})

@app.post("/identity/attest")
async def identity_attest(data: dict):
    missing = [k for k in ["agent_name", "target_did"] if k not in data]
    if missing:
        return JSONResponse(status_code=400, content={"error": f"Missing: {missing}"})
    try:
        result = {"status": "attestation_created",
                  "attestation": sign_attestation(data["agent_name"], data["target_did"], data.get("role", "agent"))}
        _cache["time"] = 0  # сброс кэша
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/verify_relay")
async def verify_relay_endpoint(relay_url: str = "", signature: str = "",
                                  timestamp: int = 0, pubkey: str = "",
                                  mesh_id: str = "snin-main-1"):
    """
    Верифицировать подпись релея.
    Прокси на Relay Signing сервис (:9125).
    
    Параметры (query string):
      relay_url — URL релея (wss://...)
      signature — Ed25519 подпись (hex)
      timestamp — время подписи (unix)
      pubkey — публичный ключ релея (hex)
      mesh_id — идентификатор mesh
    """
    if not relay_url or not signature:
        return JSONResponse(
            status_code=400,
            content={"error": "relay_url and signature required"}
        )
    
    try:
        import urllib.request
        import urllib.parse
        
        params = urllib.parse.urlencode({
            "relay_url": relay_url,
            "signature": signature,
            "timestamp": timestamp,
            "pubkey": pubkey,
            "mesh_id": mesh_id,
        })
        url = f"http://127.0.0.1:9125/verify?{params}"
        
        resp = urllib.request.urlopen(url, timeout=3)
        result = json.loads(resp.read())
        return result
    except Exception as e:
        return JSONResponse(
            status_code=502,
            content={"error": f"Relay Signing unavailable: {e}"}
        )


@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": "SNIN Identity API (L5) [FastAPI + cache]",
        "cache_ttl": CACHE_TTL,
        "paths": ["/health", "/identity/{name}", "/identity/did/{did}",
                  "/identity/top", "/identity/all",
                  "/trust-graph", "/trust/{agent}",
                  "/did-document/{did}", "/vc/{agent}", "/identity/attest",
                  "/verify_relay"],
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")

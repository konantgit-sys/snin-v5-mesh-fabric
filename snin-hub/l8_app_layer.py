"""
SNIN L8 — Application Layer (Universal Architecture 2.0, порт :9800)

Единый дашборд-портал для SNIN.
Агрегирует данные со всех 9 слоёв в единый интерфейс.

Архитектура (Chrono V3 — 8 разделов):
  L8.1  Главная       — health всех слоёв, общее состояние
  L8.2  Мониторинг    — supervisor, каждый слой по отдельности
  L8.3  Агенты        — L5 Identity + L6 Agent Network
  L8.4  Экономика     — L4 Payment + L7 DAO Treasury
  L8.5  Управление    — L7 Governance, голосования
  L8.6  Аналитика     — Chrono, метрики, графики
  L8.7  Документация  — API docs всех слоёв
  L8.8  Настройки     — конфигурация

Интеграция (снизу вверх):
  → L2 Transport (:9500)   — статус каналов
  → L2.5 Encryption (:9600) — сессии, PFS
  → L3.5 ZK (:9250)        — Merkle tree
  → L4 Payment (:9200)      — балансы, каналы
  → L4.5 Privacy (:9700)    — mixnet, dandelion
  → L5 Identity (:9940)     — агенты, trust graph
  → L6 Agent Network (:9400) — статус агентов
  → L7 DAO (:8082)          — governance, treasury
"""

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, APIRouter, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn

logging.basicConfig(level=logging.INFO, format="[L8] %(message)s")
logger = logging.getLogger("l8")

app = FastAPI(title="SNIN L8 Application Layer", version="1.0.0")
api = APIRouter(prefix="/api/v1")

# ───── Internal State ─────
LAYER_PORTS = {
    "l2": 9500,    # L2 Transport
    "enc": 9600,   # L2.5 Encryption
    "zk": 9250,    # L3.5 ZK
    "l4": 9200,    # L4 Payment
    "priv": 9700,  # L4.5 Privacy
    "l5": 9940,    # L5 Identity
    "l6": 9400,    # L6 Agent Network
    "l7": 8082,    # L7 DAO
    "l1_5": 8202,  # L1.5 Cross-Mesh Bridge
    "l9": 9900,    # L9 Orchestration
    "l3": 9300,    # L3 Mesh Core
}

CACHE: Dict[str, dict] = {}        # layer → cached data
CACHE_TTL = 15                      # seconds
CACHE_TIMESTAMPS: Dict[str, float] = {}
stats: dict = {"api_calls": 0, "cache_hits": 0, "errors": 0}


# ───── Helpers ─────

async def _fetch(url: str, timeout: float = 3.0) -> Optional[dict]:
    """HTTP GET с таймаутом."""
    import httpx
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp.json()
    except Exception:
        pass
    return None


async def _get_layer_status(layer: str, port: int,
                            path: str = "/api/v1/health") -> dict:
    """Получить статус слоя."""
    cache_key = f"{layer}:{path}"
    now = time.time()

    # Cache check
    if cache_key in CACHE_TIMESTAMPS and now - CACHE_TIMESTAMPS.get(cache_key, 0) < CACHE_TTL:
        stats["cache_hits"] += 1
        return CACHE.get(cache_key, {"status": "unknown"})

    try:
        data = await _fetch(f"http://127.0.0.1:{port}{path}")
        if data:
            result = {
                "status": "online",
                "data": data,
                "port": port,
                "cached": False,
            }
            CACHE[cache_key] = result
            CACHE_TIMESTAMPS[cache_key] = now
            return result
        else:
            return {"status": "error", "port": port, "error": "no response"}
    except Exception as e:
        stats["errors"] += 1
        return {"status": "error", "port": port, "error": str(e)[:60]}


# ══════════════════════════════════════════════════════════════
# API ENDPOINTS
# ══════════════════════════════════════════════════════════════

@api.get("/")
def root():
    return {
        "service": "SNIN L8 Application Layer",
        "version": "1.0.0",
        "sections": [
            "dashboard", "monitoring", "agents",
            "economy", "governance", "analytics",
            "docs", "settings"
        ],
        "layers_monitored": list(LAYER_PORTS.keys()),
        "stats": stats,
        "status": "live",
    }

@api.get("/health")
def health():
    return {
        "application": "ok",
        "ts": time.time(),
        "sections": 8,
        "layers": len(LAYER_PORTS),
        "cache_ttl": CACHE_TTL,
    }

# ─── 1. DASHBOARD — Главная (health всех слоёв) ───

@api.get("/dashboard")
async def dashboard():
    """Главный дашборд — статус всех слоёв."""
    stats["api_calls"] += 1

    tasks = {}
    health_paths = {
        "l2": "/api/v1/health",
        "enc": "/api/v1/health",
        "zk": "/api/v1/health",
        "l4": "/api/v1/stats",
        "priv": "/api/v1/health",
        "l5": "/health",
        "l6": "/api/v1/",
        "l7": "/api/",                   # DAO: /api/
        "l1_5": "/health",               # L1.5 bridge
        "l9": "/health",                 # L9 Orchestration
        "l3": "/health",                 # L3 Mesh Core
    }

    for layer, path in health_paths.items():
        port = LAYER_PORTS[layer]
        tasks[layer] = _get_layer_status(layer, port, path)

    results = {}
    for layer, task in tasks.items():
        results[layer] = await task

    # Общий статус
    online = sum(1 for r in results.values() if r.get("status") == "online")
    total = len(results)

    return {
        "ts": time.time(),
        "summary": {
            "online": online,
            "total": total,
            "health": f"{online}/{total}",
            "all_ok": online == total,
        },
        "layers": results,
    }

# ─── 2. MONITORING — Supervisor + детали слоёв ───

@api.get("/monitoring")
async def monitoring():
    """Мониторинг — supervisor + метрики."""
    stats["api_calls"] += 1

    # Supervisor status file
    supervisor = {}
    try:
        with open("/home/agent/data/sites/snin-hub/supervisor_status.json") as f:
            sup_data = json.load(f)
            supervisor = {
                "total": sup_data.get("total_services", 0),
                "alive": sup_data.get("alive", 0),
                "dead": sup_data.get("dead", 0),
                "restarts": sup_data.get("total_restarts", 0),
            }
    except Exception:
        supervisor = {"status": "unavailable"}

    # Uptime / system
    import psutil
    uptime = time.time() - psutil.boot_time()
    mem = psutil.virtual_memory()

    return {
        "ts": time.time(),
        "supervisor": supervisor,
        "system": {
            "uptime_hours": round(uptime / 3600, 1),
            "memory_used_pct": mem.percent,
            "memory_available_mb": round(mem.available / 1024 / 1024),
        },
        "layer_details": {layer: await _get_layer_status(layer, port,
            {"l2":"/api/v1/health","enc":"/api/v1/health","zk":"/api/v1/health",
             "l4":"/api/v1/stats","priv":"/api/v1/health","l5":"/health",
             "l6":"/api/v1/","l7":"/api/","l1_5":"/health","l9":"/health","l3":"/health"}.get(layer, "/api/v1/health"))
                          for layer, port in LAYER_PORTS.items()},
    }

# ─── 3. AGENTS — L5 + L6 ───

@api.get("/agents")
async def agents():
    """Агенты — из L5 Identity + L6 Agent Network."""
    stats["api_calls"] += 1

    l5 = await _get_layer_status("l5", 9940, "/identity/all")
    l6 = await _get_layer_status("l6", 9400, "/api/v1/agents")

    # Обработка L5
    agents_l5 = []
    if l5.get("data") and isinstance(l5["data"], dict):
        raw_agents = l5["data"].get("agents", [])
        for a in raw_agents:
            rep = a.get("reputation", {})
            if isinstance(rep, dict):
                rep_score = rep.get("score", 0)
            else:
                rep_score = rep
            agents_l5.append({
                "name": a.get("agent_name", "?"),
                "did": str(a.get("did", ""))[:20],
                "reputation": rep_score,
            })

    # Обработка L6
    agents_l6 = []
    if l6.get("data") and isinstance(l6["data"], dict):
        raw_agents = l6["data"].get("agents", [])
        for a in raw_agents:
            agents_l6.append({
                "name": a.get("agent_name", "?"),
                "status": a.get("status", "?"),
                "reputation": a.get("reputation", 0),
            })

    return {
        "ts": time.time(),
        "l5_identity": {
            "status": l5.get("status"),
            "agents": agents_l5,
            "count": len(agents_l5),
        },
        "l6_network": {
            "status": l6.get("status"),
            "agents": agents_l6,
            "count": len(agents_l6),
        },
    }

# ─── 4. ECONOMY — L4 + L7 Treasury ───

@api.get("/economy")
async def economy():
    """Экономика — L4 Payment + L7 DAO Treasury."""
    stats["api_calls"] += 1

    l4 = await _get_layer_status("l4", 9200, "/api/v1/stats")
    l7 = await _get_layer_status("l7", 8082, "/api/")

    # Парсим L4
    economy_data = {"l4": {}, "l7": {}}

    if l4.get("data"):
        ec = l4["data"]
        channels = ec.get("channels", {})
        economy_data["l4"] = {
            "optimistic_agents": channels.get("optimistic", {}).get("agents", 0),
            "optimistic_balance": channels.get("optimistic", {}).get("total_balance", 0),
            "treasury_total": channels.get("treasury", {}).get("total", 0),
            "liquidity_supply": channels.get("liquidity", {}).get("supply", 0),
            "liquidity_price": channels.get("liquidity", {}).get("price_sol", 0),
            "lp_providers": channels.get("liquidity", {}).get("lp_providers", 0),
        }

    if l7.get("data"):
        economy_data["l7"] = l7["data"]

    return {
        "ts": time.time(),
        "economy": economy_data,
        "sources": {
            "l4_payment": l4.get("status"),
            "l7_dao": l7.get("status"),
        }
    }

# ─── 5. GOVERNANCE — L7 Governance ───

@api.get("/governance")
async def governance():
    """Управление — L7 Governance / proposals."""
    stats["api_calls"] += 1

    l7_gov = await _get_layer_status("l7", 8082, "/api/")

    return {
        "ts": time.time(),
        "governance": l7_gov.get("data", {}),
        "status": l7_gov.get("status"),
    }

# ─── 6. ANALYTICS — Chrono + система ───

@api.get("/analytics")
async def analytics():
    """Аналитика — Chrono + системные метрики."""
    stats["api_calls"] += 1

    # Chrono
    chrono = await _fetch("http://127.0.0.1:9872/api/v1/chrono/network")

    return {
        "ts": time.time(),
        "chrono": chrono or {"status": "unavailable"},
        "pipeline": {
            "l2_transport": (await _get_layer_status("l2", 9500)).get("status"),
            "l2_encryption": (await _get_layer_status("enc", 9600)).get("status"),
            "l3_zk": (await _get_layer_status("zk", 9250)).get("status"),
            "l4_payment": (await _get_layer_status("l4", 9200)).get("status"),
            "l4_privacy": (await _get_layer_status("priv", 9700)).get("status"),
            "l5_identity": (await _get_layer_status("l5", 9940)).get("status"),
            "l6_agents": (await _get_layer_status("l6", 9400)).get("status"),
            "l7_dao": (await _get_layer_status("l7", 8082, "/api/")).get("status"),
            "l1_5_bridge": (await _get_layer_status("l1_5", 8202, "/health")).get("status"),
            "l9_orchestration": (await _get_layer_status("l9", 9900, "/health")).get("status"),
            "l3_mesh": (await _get_layer_status("l3", 9300, "/health")).get("status"),
        },
    }

# ─── 7. DOCS — API документация ───

@api.get("/docs-overview")
def docs():
    """Документация по API всех слоёв."""
    return {
        "ts": time.time(),
        "layers": {
            "L2 Transport (:9500)": {
                "endpoints": [
                    "GET /api/v1/  — статус",
                    "GET /api/v1/health — health",
                    "GET /api/v1/channels — каналы",
                    "POST /api/v1/send — отправка",
                    "POST /api/v1/multicast — multi-канал",
                ]
            },
            "L2.5 Encryption (:9600)": {
                "endpoints": [
                    "POST /api/v1/keys/generate — генерация ключей",
                    "GET /api/v1/keys/{peer} — публичные ключи",
                    "POST /api/v1/session/create — ECDH сессия",
                    "POST /api/v1/encrypt — шифрование",
                    "POST /api/v1/decrypt — дешифрование",
                    "POST /api/v1/onion/build — onion роутинг",
                ]
            },
            "L3.5 ZK (:9250)": {
                "endpoints": [
                    "GET /api/v1/merkle/agents/root — корень дерева",
                    "GET /api/v1/merkle/agents/proof/{leaf} — Merkle proof",
                    "POST /api/v1/merkle/agents/verify — верификация",
                    "POST /api/v1/commit — хэш-коммит",
                    "POST /api/v1/range/prove — range proof",
                ]
            },
            "L4 Payment (:9200)": {
                "endpoints": [
                    "GET /api/v1/stats — статистика",
                    "GET /api/v1/channels — каналы",
                    "POST /api/v1/payment — платёж",
                ]
            },
            "L4.5 Privacy (:9700)": {
                "endpoints": [
                    "POST /api/v1/mix/add — mixnet",
                    "POST /api/v1/dandelion/send — Dandelion++",
                    "POST /api/v1/coinjoin/add — CoinJoin",
                    "GET /api/v1/privacy-score — оценка анонимности",
                ]
            },
            "L5 Identity (:9940)": {
                "endpoints": [
                    "GET /identity/all — все агенты",
                    "GET /identity/{name} — детали агента",
                    "GET /identity/top — топ по репутации",
                    "GET /trust-graph — граф доверия",
                ]
            },
            "L6 Agent Network (:9400)": {
                "endpoints": [
                    "GET /api/v1/ — статус сети",
                    "GET /api/v1/agents — список агентов",
                    "GET /api/v1/agents/{name} — детали",
                    "POST /api/v1/sync/from-l5 — синхронизация",
                ]
            },
            "L7 DAO (:8082)": {
                "endpoints": [
                    "GET / — статус DAO",
                    "GET /economy — экономика",
                    "GET /governance — управление",
                ]
            },
        }
    }

# ─── 8. SETTINGS — конфигурация ───

@api.get("/settings")
def settings():
    """Настройки L8."""
    return {
        "ts": time.time(),
        "l8_config": {
            "cache_ttl_seconds": CACHE_TTL,
            "monitored_layers": list(LAYER_PORTS.keys()),
            "layer_ports": LAYER_PORTS,
        },
        "actions": [
            {"name": "clear_cache", "method": "POST", "path": "/api/v1/settings/clear-cache"},
        ]
    }

@api.post("/settings/clear-cache")
def clear_cache():
    """Очистка кэша L8."""
    CACHE.clear()
    CACHE_TIMESTAMPS.clear()
    stats["cache_hits"] = 0
    return {"status": "cache_cleared", "entries_removed": len(CACHE)}

@api.post("/settings/set-cache-ttl")
def set_cache_ttl(ttl: int):
    """Изменить TTL кэша."""
    global CACHE_TTL
    CACHE_TTL = max(5, min(120, ttl))
    return {"cache_ttl": CACHE_TTL}

# ══════════════════════════════════════════════════════════════
# STATIC HTML UI
# ══════════════════════════════════════════════════════════════

HTML_INDEX = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SNIN L8 — Application Layer</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif}
body{background:#0a0e17;color:#e0e8f0;min-height:100vh}
.header{background:linear-gradient(135deg,#0d1520,#1a1f35);padding:20px 30px;border-bottom:1px solid #2a3050}
.header h1{font-size:24px;font-weight:700;background:linear-gradient(90deg,#00d4ff,#7b61ff);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.header span{color:#6a7a9a;font-size:14px}
.nav{display:flex;gap:4px;padding:15px 30px;background:#0d1520;border-bottom:1px solid #1e2638;flex-wrap:wrap}
.nav a{padding:8px 18px;border-radius:8px;text-decoration:none;color:#8a9ab5;font-size:14px;transition:all .2s}
.nav a:hover{background:#1a2540;color:#e0e8f0}
.nav a.active{background:linear-gradient(135deg,#00d4ff22,#7b61ff22);color:#00d4ff;border:1px solid #00d4ff44}
.content{max-width:1400px;margin:0 auto;padding:20px 30px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:16px;margin-top:20px}
.card{background:linear-gradient(135deg,#111827,#1a1f35);border:1px solid #2a3050;border-radius:12px;padding:18px;transition:all .3s}
.card:hover{border-color:#4a5a7a;transform:translateY(-2px)}
.card h3{font-size:16px;font-weight:600;margin-bottom:8px}
.card .status{font-size:13px;margin:4px 0}
.badge{display:inline-block;padding:2px 10px;border-radius:20px;font-size:12px;font-weight:600}
.badge.online{background:#00d4ff22;color:#00d4ff;border:1px solid #00d4ff44}
.badge.offline{background:#ff444422;color:#ff4444;border:1px solid #ff444444}
.badge.warning{background:#ffaa0022;color:#ffaa00;border:1px solid #ffaa0044}
.breadcrumb{color:#6a7a9a;font-size:13px;margin-top:16px}
.summary{display:flex;gap:20px;margin:20px 0;flex-wrap:wrap}
.stat-box{background:#111827;border:1px solid #2a3050;border-radius:12px;padding:16px 24px;min-width:140px}
.stat-box .num{font-size:28px;font-weight:700;background:linear-gradient(90deg,#00d4ff,#7b61ff);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.stat-box .label{color:#6a7a9a;font-size:13px;margin-top:4px}
.table{width:100%;border-collapse:collapse;margin-top:16px}
.table th,.table td{padding:10px 14px;text-align:left;border-bottom:1px solid #1e2638;font-size:14px}
.table th{color:#6a7a9a;font-weight:600;font-size:12px;text-transform:uppercase}
.table td{color:#c0c8d8}
.refresh-btn{background:#1a2540;border:1px solid #2a3050;color:#8a9ab5;padding:8px 20px;border-radius:8px;cursor:pointer;font-size:14px}
.refresh-btn:hover{background:#2a3a55;color:#e0e8f0}
</style>
</head>
<body>
<div class="header"><h1>SNIN L8 — Application Layer</h1><span>Universal Architecture 2.0 · 10 слоёв интегрированы</span></div>
<div class="nav" id="nav"></div>
<div class="content" id="content">
  <div id="summary" class="summary"></div>
  <div id="main-content"><p style='color:#6a7a9a;text-align:center;padding:60px'>Загрузка...</p></div>
</div>
<script>
const SECTIONS = ['dashboard','monitoring','agents','economy','governance','analytics','docs','settings'];
const NAV_NAMES = {'dashboard':'📊 Главная','monitoring':'🔍 Мониторинг','agents':'🤖 Агенты','economy':'💰 Экономика','governance':'⚖️ Управление','analytics':'📈 Аналитика','docs':'📚 Документация','settings':'⚙️ Настройки'};
let currentSection = 'dashboard';

document.getElementById('nav').innerHTML = SECTIONS.map(s =>
  `<a href="#" class="${s==='dashboard'?'active':''}" onclick="switchSection('${s}')">${NAV_NAMES[s]}</a>`
).join('');

async function switchSection(section){
  currentSection = section;
  document.querySelectorAll('.nav a').forEach(a => a.className='');
  document.querySelectorAll('.nav a')[SECTIONS.indexOf(section)].className='active';
  await loadSection(section);
}

async function loadSection(section){
  const main = document.getElementById('main-content');
  main.innerHTML = '<p style="color:#6a7a9a;text-align:center;padding:40px">Загрузка...</p>';
  try{
    const resp = await fetch('/api/v1/'+section);
    const data = await resp.json();
    renderSection(section, data);
  }catch(e){
    main.innerHTML = '<p style="color:#ff4444;text-align:center;padding:40px">Ошибка соединения</p>';
  }
}

const SECTION_RENDERERS = {
  dashboard: renderDashboard,
  monitoring: renderMonitoring,
  agents: renderAgents,
  economy: renderEconomy,
  governance: renderGeneric,
  analytics: renderAnalytics,
  docs: renderDocs,
  settings: renderSettings,
};

function renderSection(section, data){
  if(SECTION_RENDERERS[section]) SECTION_RENDERERS[section](data);
  else renderGeneric(data);
}

function renderDashboard(data){
  const s = data.summary;
  document.getElementById('summary').innerHTML = [
    {label:'Слоёв онлайн', num:s.online+'/'+s.total},
    {label:'Статус', num:s.all_ok?'✅ Все OK':'⚠️ Есть проблемы'},
  ].map(x => '<div class="stat-box"><div class="num">'+x.num+'</div><div class="label">'+x.label+'</div></div>').join('');

  let html = '<div class="grid">';
  for(const [layer, info] of Object.entries(data.layers)){
    const status = info.status;
    const badge = status === 'online' ? '🟢 online' : '🔴 '+status;
    html += '<div class="card"><h3>'+layer.toUpperCase()+' <span class="badge '+(status==='online'?'online':'offline')+'">'+badge+'</span></h3>';
    if(info.data && typeof info.data === 'object'){
      const extra = JSON.stringify(info.data).slice(0,100);
      html += '<div class="status" style="color:#6a7a9a;font-size:12px">'+extra.slice(0,80)+'...</div>';
    }
    html += '<div class="status" style="color:#4a5a7a;font-size:12px">port '+info.port+'</div></div>';
  }
  html += '</div>';
  document.getElementById('main-content').innerHTML = html;
}

function renderMonitoring(data){
  const sup = data.supervisor || {};
  document.getElementById('summary').innerHTML = [
    {label:'Сервисов', num:sup.total||'?'},
    {label:'🟢 Alive', num:sup.alive||0},
    {label:'🔴 Dead', num:sup.dead||0},
    {label:'Uptime', num:(data.system?.uptime_hours||0)+'ч'},
  ].map(x => '<div class="stat-box"><div class="num">'+x.num+'</div><div class="label">'+x.label+'</div></div>').join('');

  let html = '<table class="table"><tr><th>Слой</th><th>Статус</th><th>Порт</th><th>Данные</th></tr>';
  for(const [layer, info] of Object.entries(data.layer_details||{})){
    const status = info.status || '?';
    html += '<tr><td><b>'+layer+'</b></td><td><span class="badge '+(status==='online'?'online':'offline')+'">'+status+'</span></td><td>'+info.port+'</td><td style="color:#6a7a9a;font-size:12px">'+(info.data?JSON.stringify(info.data).slice(0,80):'-')+'</td></tr>';
  }
  html += '</table>';
  html += '<div class="breadcrumb">Система: RAM '+data.system?.memory_used_pct+'% · '+(data.system?.memory_available_mb||0)+' MB свободно</div>';
  document.getElementById('main-content').innerHTML = html;
}

function renderAgents(data){
  const l5 = data.l5_identity || {};
  const l6 = data.l6_network || {};
  document.getElementById('summary').innerHTML = [
    {label:'L5 Identity', num:l5.count||0},
    {label:'L6 Agents', num:l6.count||0},
    {label:'Всего агентов', num:(l5.count||0)+(l6.count||0)},
  ].map(x => '<div class="stat-box"><div class="num">'+x.num+'</div><div class="label">'+x.label+'</div></div>').join('');

  let html = '<h3>🤖 L5 Identity — Агенты</h3><table class="table"><tr><th>Имя</th><th>DID</th><th>Репутация</th></tr>';
  for(const a of l5.agents||[]){
    html += '<tr><td><b>'+a.name+'</b></td><td style="font-family:mono;font-size:12px">'+a.did+'</td><td>'+a.reputation+'</td></tr>';
  }
  html += '</table>';
  html += '<h3 style="margin-top:24px">🤖 L6 Agent Network</h3><table class="table"><tr><th>Имя</th><th>Статус</th><th>Репутация</th></tr>';
  for(const a of l6.agents||[]){
    html += '<tr><td><b>'+a.name+'</b></td><td><span class="badge '+(a.status==='online'?'online':'offline')+'">'+a.status+'</span></td><td>'+a.reputation+'</td></tr>';
  }
  html += '</table>';
  document.getElementById('main-content').innerHTML = html;
}

function renderEconomy(data){
  const ec = data.economy?.l4 || {};
  document.getElementById('summary').innerHTML = [
    {label:'Treasury', num:ec.treasury_total||0},
    {label:'Liquidity Supply', num:(ec.liquidity_supply||0).toLocaleString()},
    {label:'LP Providers', num:ec.lp_providers||0},
    {label:'SNIN Balance', num:ec.optimistic_balance||0},
  ].map(x => '<div class="stat-box"><div class="num">'+x.num+'</div><div class="label">'+x.label+'</div></div>').join('');

  let html = '<table class="table"><tr><th>Метрика</th><th>Значение</th></tr>';
  for(const [k,v] of Object.entries(ec)){
    html += '<tr><td>'+k+'</td><td>'+(typeof v==='number'?v.toLocaleString():v)+'</td></tr>';
  }
  html += '</table>';
  document.getElementById('main-content').innerHTML = html;
}

function renderAnalytics(data){
  const pipe = data.pipeline || {};
  let html = '<h3>Pipeline Health</h3><table class="table"><tr><th>Слой</th><th>Статус</th></tr>';
  for(const [layer, status] of Object.entries(pipe)){
    html += '<tr><td><b>'+layer+'</b></td><td><span class="badge '+(status==='online'?'online':status==='error'?'offline':'warning')+'">'+status+'</span></td></tr>';
  }
  html += '</table>';
  if(data.chrono) html += '<div class="card" style="margin-top:16px"><h3>⏱ Chrono Network</h3><pre style="color:#6a7a9a;font-size:12px;margin-top:8px">'+JSON.stringify(data.chrono,null,2).slice(0,500)+'</pre></div>';
  document.getElementById('main-content').innerHTML = html;
}

function renderGeneric(data){
  document.getElementById('main-content').innerHTML = '<pre style="color:#8a9ab5;font-size:13px;padding:20px">'+JSON.stringify(data,null,2).slice(0,2000)+'</pre>';
}

function renderDocs(data){
  let html = '';
  for(const [layer, info] of Object.entries(data.layers||{})){
    html += '<div class="card" style="margin-bottom:12px"><h3>'+layer+'</h3>';
    for(const ep of info.endpoints||[]){
      html += '<div class="status" style="font-family:mono;font-size:13px;color:#8a9ab5">'+ep+'</div>';
    }
    html += '</div>';
  }
  document.getElementById('main-content').innerHTML = html;
}

function renderSettings(data){
  const cfg = data.l8_config || {};
  let html = '<table class="table"><tr><th>Параметр</th><th>Значение</th></tr>';
  for(const [k,v] of Object.entries(cfg)){
    html += '<tr><td>'+k+'</td><td>'+(typeof v==='object'?JSON.stringify(v):v)+'</td></tr>';
  }
  html += '</table><div class="breadcrumb" style="margin-top:16px"><button class="refresh-btn" onclick="clearCache()">🧹 Очистить кэш</button></div>';
  document.getElementById('main-content').innerHTML = html;
}

async function clearCache(){
  await fetch('/api/v1/settings/clear-cache',{method:'POST'});
  await loadSection('settings');
}

(async function(){await loadSection('dashboard');})();
</script>
</body>
</html>"""

@app.get("/health")
def health_root():
    return {"status": "ok", "layer": "L8", "port": int(os.environ.get("PORT", 9800))}

@app.head("/health")
def health_head():
    return {"status": "ok", "layer": "L8"}

@app.head("/")
def root_head():
    return {"status": "ok", "layer": "L8"}

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML_INDEX

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page():
    return HTML_INDEX

@app.get("/monitoring", response_class=HTMLResponse)
async def monitoring_page():
    return HTML_INDEX

@app.get("/agents", response_class=HTMLResponse)
async def agents_page():
    return HTML_INDEX

@app.get("/economy", response_class=HTMLResponse)
async def economy_page():
    return HTML_INDEX

@app.get("/analytics", response_class=HTMLResponse)
async def analytics_page():
    return HTML_INDEX

# ══════════════════════════════════════════════════════════════

app.include_router(api)

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9800
    print(f"[L8] Starting Application Layer on :{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")

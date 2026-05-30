#!/usr/bin/env python3
"""SNIN Workflow — единый цикл самообучения агента.

╔═══════════════════════════════════════════════════════════════╗
║                    SNIN EVOLUTIONARY WORKFLOW                ║
║                                                             ║
║   Появился → FirstContact (50ms) → MatrixUpdater (60s) →   ║
║   → ChronologyAnalysis → Decision → Action                 ║
║                                                             ║
║   Всё — один цикл. Ничего разрозненного.                   ║
║   Агент НЕ ЖДЁТ — он непрерывно учится и адаптируется.     ║
╚═══════════════════════════════════════════════════════════════╝

Слои workflow (в порядке выполнения):
  Layer 0: Agent Identity     — who am I, keys, config
  Layer 1: First Contact      — scan, matrix, rank           (50ms)
  Layer 2: Dynamic Matrix     — ping, exchange, merge        (60s cycle)
  Layer 3: Chronology         — анализ истории, тренды       (каждый цикл)
  Layer 4: Decision           — что делать на основе анализа (каждый цикл)
  Layer 5: Nostr Bridge       — внешняя публикация           (30s cycle)
  Layer 6: Device Layer       — IoT, ESP32, сенсоры          (по запросу)

Каждый слой вызывает следующий. Результат предыдущего —
вход для следующего. Ничего не висит в воздухе.

Запуск:
  python3 workflow.py --agent forecaster_ai
"""

import asyncio
import json
import os
import sys
import time
import signal
from collections import defaultdict, deque

sys.path.insert(0, "/home/agent/data/sites/relay-mesh")

# ─── Импорт слоёв ───────────────────────────────────────────────
from first_contact_agent import FirstContact, MatrixUpdater, DeviceLayer

# Nostr bridge — если доступен
try:
    sys.path.insert(0, "/home/agent/data/sites/relay-mesh")
    import importlib
    nostr_bridge_mod = importlib.import_module("nostr_bridge")
    NostrBridgeLayer = nostr_bridge_mod.NostrBridgeLayer
    # Проверяем — nostr_bridge.py уже запущен отдельно (порт 9931)?
    import socket
    _bc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _bc.settimeout(0.5)
    try:
        _bc.connect(("127.0.0.1", 9931))
        print("[Workflow] ✅ Nostr Bridge уже запущен отдельно — Layer 5 пропущен")
        NOSTR_AVAILABLE = False
    except (ConnectionRefusedError, OSError):
        NOSTR_AVAILABLE = True
    finally:
        _bc.close()
except Exception as e:
    print(f"[Workflow] ⚠️ Nostr bridge not available: {e}")
    NOSTR_AVAILABLE = False

# ─── Конфиг ─────────────────────────────────────────────────────
WORKFLOW_DIR = "/home/agent/data/sites/relay-mesh/workflow"
CHRONOLOGY_FILE = os.path.join(WORKFLOW_DIR, "chronology.json")
DECISIONS_FILE = os.path.join(WORKFLOW_DIR, "decisions.json")
AGENTS_FILE = "/home/agent/data/sites/relay-mesh/agents.json"
SR_HOST = "127.0.0.1"
SR_PORT = 9932

os.makedirs(WORKFLOW_DIR, exist_ok=True)


# ═══════════════════════════════════════════════════════════════
#  SR CLIENT — Отправка сообщений в SmartRouter
# ═══════════════════════════════════════════════════════════════

async def send_to_sr(msg: dict) -> dict:
    """Отправить JSON сообщение в SmartRouter (:9932) и получить ответ."""
    try:
        r, w = await asyncio.wait_for(
            asyncio.open_connection(SR_HOST, SR_PORT), timeout=3)
        w.write(json.dumps(msg, ensure_ascii=False).encode() + b"\n")
        await asyncio.wait_for(w.drain(), timeout=3)
        line = await asyncio.wait_for(r.readline(), timeout=5)
        w.close()
        if line:
            return json.loads(line.decode().strip())
        return {"ok": False, "error": "no response"}
    except asyncio.TimeoutError:
        return {"ok": False, "error": "timeout"}
    except ConnectionRefusedError:
        return {"ok": False, "error": "sr_unreachable"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ═══════════════════════════════════════════════════════════════
#  LAYER 3: CHRONOLOGY ANALYZER — Анализ истории
# ═══════════════════════════════════════════════════════════════
#
# MatrixUpdater пишет хронологию (200 сессий).
# ChronologyAnalyzer эту хронологию АНАЛИЗИРУЕТ и выдаёт решения:
#
#   Тренды:
#   - Количество живых узлов падает → тревога
#   - Узел не появлялся N сессий → мёртв
#   - Latency растёт → деградация
#   - Частые изменения → флаппинг
#
#   Решения:
#   - Исключить мёртвый узел из рассылки
#   - Переключить primary канал
#   - Перезапустить First Contact
#   - Оповестить других агентов
#
#   Хронология сохраняется в файл — не теряется при рестарте.
# ═══════════════════════════════════════════════════════════════

THRESHOLDS = {
    "degradation_pct": 50,
    "degradation_sessions": 5,
    "improvement_pct": 30,
    "dead_sessions": 15,
    "flap_threshold": 3,
    "alive_ratio_critical": 0.3,   # если alive/total < 30% — критично
    "alive_ratio_warn": 0.6,       # если < 60% — предупреждение
}


class ChronologyAnalyzer:
    """
    Анализ хронологии MatrixUpdater.
    
    Принимает: хронологию (список сессий)
    Возвращает: решения (что делать)
    
    Работает как часть workflow — вызывается после каждого exchange.
    """
    
    def __init__(self, history_size: int = 200):
        self.history_size = history_size
        self.chronology: list[dict] = []
        self.decisions: list[dict] = []
        self.trends: dict = {}
        self._loaded = False
        self._dead_nodes: dict[str, int] = {}   # node_id → sessions_since_seen
    
    def load(self):
        try:
            with open(CHRONOLOGY_FILE) as f:
                data = json.load(f)
                self.chronology = data.get("chronology", [])
                self.decisions = data.get("decisions", [])
                self.trends = data.get("trends", {})
            self._loaded = True
            print(f"[Chronology] Loaded {len(self.chronology)} sessions, {len(self.decisions)} decisions")
        except (FileNotFoundError, json.JSONDecodeError):
            print("[Chronology] Fresh start")
    
    def save(self):
        data = {
            "chronology": self.chronology[-self.history_size:],
            "decisions": self.decisions[-100:],
            "trends": self.trends,
            "updated_at": time.time(),
        }
        with open(CHRONOLOGY_FILE, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def add_session(self, session: dict, 
                    matrix_nodes: dict, matrix_edges: list):
        entry = dict(session)
        entry["matrix_snapshot"] = {
            "nodes_count": len(matrix_nodes),
            "edges_count": len(matrix_edges),
            "node_list": list(matrix_nodes.keys())[:20],
        }
        self.chronology.append(entry)
        if len(self.chronology) > self.history_size:
            self.chronology = self.chronology[-self.history_size:]
    
    def analyze(self, alive_nodes: set = None, total_nodes: set = None) -> list[dict]:
        """
        Проанализировать хронологию и вернуть решения.
        
        Args:
            alive_nodes: set of pubkeys of nodes that responded to ping
            total_nodes: set of all known pubkeys
        
        Returns:
            list[dict] — решения
        """
        if len(self.chronology) < 3:
            return []
        
        decisions = []
        recent = self.chronology[-20:]
        
        # ── 1. Latency тренд ──
        latencies = [s.get("latency_avg", 0) for s in recent if s.get("latency_avg", 0) > 0]
        if len(latencies) >= 5:
            first_half = sum(latencies[:len(latencies)//2]) / max(len(latencies)//2, 1)
            second_half = sum(latencies[len(latencies)//2:]) / max(len(latencies) - len(latencies)//2, 1)
            
            if first_half > 0:
                change_pct = (second_half - first_half) / first_half * 100
                
                if change_pct > THRESHOLDS["degradation_pct"]:
                    decisions.append({
                        "type": "degradation",
                        "priority": "high",
                        "node": "_network",
                        "message": f"Latency UP {change_pct:.0f}% — degradation",
                        "action": "switch channel, check relays",
                    })
                    self.trends["network_degradation"] = change_pct
                
                elif change_pct < -THRESHOLDS["improvement_pct"]:
                    decisions.append({
                        "type": "improvement",
                        "priority": "low",
                        "node": "_network",
                        "message": f"Latency DOWN {abs(change_pct):.0f}% — improving",
                        "action": "maintain current routing",
                    })
        
        # ── 2. Alive ratio — сколько узлов живо ──
        if alive_nodes is not None and total_nodes is not None and len(total_nodes) > 0:
            ratio = len(alive_nodes) / len(total_nodes)
            
            if ratio < THRESHOLDS["alive_ratio_critical"]:
                decisions.append({
                    "type": "critical_alive_ratio",
                    "priority": "critical",
                    "node": "_network",
                    "message": f"Only {len(alive_nodes)}/{len(total_nodes)} alive ({ratio:.0%})",
                    "action": "force rediscovery, restart FirstContact",
                })
            elif ratio < THRESHOLDS["alive_ratio_warn"]:
                decisions.append({
                    "type": "low_alive_ratio",
                    "priority": "high",
                    "node": "_network",
                    "message": f"Alive ratio {ratio:.0%} ({len(alive_nodes)}/{len(total_nodes)})",
                    "action": "check for dead nodes, increase ping retries",
                })
        
        # ── 3. Dead nodes — узлы которые не отвечают несколько сессий ──
        if alive_nodes is not None and total_nodes is not None:
            dead = total_nodes - alive_nodes
            for node_id in dead:
                self._dead_nodes[node_id] = self._dead_nodes.get(node_id, 0) + 1
            
            alive = total_nodes & alive_nodes
            for node_id in alive:
                self._dead_nodes.pop(node_id, None)
            
            # Если узел не отвечает >3 сессий — исключить
            for node_id, streak in self._dead_nodes.items():
                if streak >= 3:
                    decisions.append({
                        "type": "exclude_dead_node",
                        "priority": "medium" if streak < 10 else "high",
                        "node": node_id[:16],
                        "message": f"Node {node_id[:16]} dead for {streak} sessions",
                        "action": "remove from matrix, notify SR",
                    })
        
        # ── 4. Flapping — частые изменения ──
        changes_count = sum(s.get("changes", 0) for s in recent)
        if changes_count > THRESHOLDS["flap_threshold"] * len(recent):
            decisions.append({
                "type": "flapping",
                "priority": "medium",
                "node": "_network",
                "message": f"High churn: {changes_count} changes in {len(recent)} sessions",
                "action": "increase exchange interval, check flapping",
            })
        
        # ── 5. Summary — статистика ──
        if len(self.chronology) >= 10:
            avg_alive = sum(s.get("alive", 0) for s in self.chronology[-10:]) / 10
            avg_nodes = sum(s.get("nodes_total", 0) for s in self.chronology[-10:]) / 10
            decisions.append({
                "type": "summary",
                "priority": "info",
                "node": "_workflow",
                "message": f"Avg {avg_alive:.1f} alive / {avg_nodes:.1f} nodes last 10 sessions",
                "action": "normal" if avg_alive > 0 else "alert",
            })
        
        self.decisions.extend(decisions)
        if len(self.decisions) > 100:
            self.decisions = self.decisions[-100:]
        
        self.save()
        return decisions
    
    def get_trends(self) -> str:
        if not self.chronology:
            return "No data"
        last = self.chronology[-1]
        avg_lat = last.get("latency_avg", 0)
        tiers = last.get("tiers", {})
        parts = [
            f"Sessions: {len(self.chronology)}",
            f"Latency: {avg_lat:.1f}ms" if avg_lat else "Latency: N/A",
            f"Tiers: T1:{tiers.get('t1',0)} T2:{tiers.get('t2',0)} T3:{tiers.get('t3',0)} T4:{tiers.get('t4',0)}",
        ]
        high = [d for d in self.decisions[-5:] if d.get("priority") in ("high", "critical")]
        if high:
            parts.append(f"Alerts: {len(high)}")
        return " | ".join(parts)
    
    def get_recent_decisions(self, n: int = 5) -> list:
        return self.decisions[-n:] if self.decisions else []


# ═══════════════════════════════════════════════════════════════
#  LAYER 4: DECISION ENGINE — Исполнение решений
# ═══════════════════════════════════════════════════════════════
#
# Принимает решения от ChronologyAnalyzer и ИСПОЛНЯЕТ их.
# Каждое решение отправляется в SmartRouter как kind:39006
# для распространения по всей сети.
# ═══════════════════════════════════════════════════════════════

class DecisionEngine:
    """
    Исполняет решения на основе анализа хронологии.
    
    Решения:
      - "degradation"          → слать kind:39006 в SR, переключить канал
      - "critical_dead"        → перезапустить First Contact, слать kind:39006
      - "exclude_dead_node"    → удалить из матрицы, слать kind:39005 в SR
      - "critical_alive_ratio" → перезапустить First Contact
      - "low_alive_ratio"      → увеличить ping_timeout
      - "flapping"             → увеличить интервал exchange
      - "summary"              → логировать
    """
    
    def __init__(self, fc: FirstContact, mu: "MatrixUpdater" = None,
                 device_layer: "DeviceLayer" = None, agent_name: str = "agent"):
        self.fc = fc
        self.mu = mu
        self.device_layer = device_layer
        self.agent_name = agent_name
        self.executed: list[dict] = []
        self._last_action_time = 0
        self._sr_available = None  # проверяем при первом вызове
    
    async def _send_to_sr(self, kind: int, payload: dict) -> dict:
        """Отправить решение в SmartRouter (kind:39005 или 39006)."""
        msg = {
            "kind": kind,
            "pubkey": self.fc.pubkey,
            "from": self.fc.pubkey[:16],
            "to": "broadcast",
            "content": json.dumps(payload, ensure_ascii=False),
            "meta": {"priority": "high", "channel": "mesh"},
        }
        return await send_to_sr(msg)
    
    async def execute(self, decisions: list[dict]) -> list[dict]:
        """
        Исполнить решения.
        Каждое решение → kind:39006 в SmartRouter → всем агентам.
        """
        actions = []
        now = time.time()
        
        for dec in decisions:
            action = {
                "decision": dec,
                "executed_at": now,
                "result": "skipped",
            }
            
            dtype = dec.get("type", "")
            priority = dec.get("priority", "info")
            target_node = dec.get("node", "")
            
            # ── CRITICAL: Все узлы мёртвы → перезапуск First Contact ──
            if dtype in ("critical_dead", "critical_alive_ratio"):
                if now - self._last_action_time > 300:
                    print(f"[Decision] 🚨 {dtype} — Rediscovering network...")
                    # 1. Извещаем SR
                    await self._send_to_sr(39006, {
                        "action": "rediscovery",
                        "reason": dtype,
                        "agent": self.agent_name,
                        "timestamp": now,
                    })
                    # 2. Перезапускаем First Contact
                    contact = await self.fc.scan_and_connect()
                    action["result"] = f"FirstContact restarted: {contact['total_time_ms']}ms"
                    self._last_action_time = now
                    self._save_decision("rediscovery", contact)
            
            # ── HIGH: Деградация → слать kind:39006 + переключить канал ──
            elif dtype == "degradation":
                if now - self._last_action_time > 120:
                    print(f"[Decision] ⚡ Degradation — sending to SR + switching channel")
                    
                    # 1. Шлём решение в SR
                    sr_result = await self._send_to_sr(39006, {
                        "action": "channel_switch",
                        "reason": f"latency_degradation_{dec.get('message','')}",
                        "agent": self.agent_name,
                        "timestamp": now,
                    })
                    
                    # 2. Переключаем канал локально
                    channel_map = {
                        c["channel"]: c["latency_ms"] 
                        for c in self.fc.rank_channels()
                    }
                    if channel_map:
                        best = min(channel_map, key=channel_map.get)
                        action["result"] = (
                            f"SR:{sr_result.get('ok',False)} | "
                            f"Switched to {best} ({channel_map[best]}ms)"
                        )
                        action["channel"] = best
                        self._last_action_time = now
                        self._save_decision("channel_switch", {
                            "sr_ok": sr_result.get("ok", False),
                            "from": "auto", "to": best, "latency": channel_map[best]
                        })
            
            # ── HIGH: Low alive ratio → больше ping timeout ──
            elif dtype == "low_alive_ratio":
                if self.mu and self.mu.ping_timeout < 5.0:
                    old = self.mu.ping_timeout
                    self.mu.ping_timeout = min(self.mu.ping_timeout * 1.5, 10.0)
                    action["result"] = f"Increased ping timeout: {old}s → {self.mu.ping_timeout}s"
                    self._save_decision("ping_timeout", {"old": old, "new": self.mu.ping_timeout})
            
            # ── MEDIUM: Исключить мёртвый узел ──
            elif dtype == "exclude_dead_node":
                print(f"[Decision] 🗑 Excluding dead node {target_node}")
                
                # 1. Шлём kind:39005 (health) в SR
                await self._send_to_sr(39005, {
                    "action": "node_dead",
                    "node_id": target_node,
                    "agent": self.agent_name,
                    "timestamp": now,
                })
                
                # 2. Удаляем из локальной матрицы
                dead_pubkeys = [
                    pk for pk, info in self.fc.matrix.get("nodes", {}).items()
                    if pk[:16] == target_node or info.get("name", "") == target_node
                ]
                for pk in dead_pubkeys:
                    del self.fc.matrix["nodes"][pk]
                    self.fc.matrix["edges"] = [
                        e for e in self.fc.matrix["edges"]
                        if e.get("to") != target_node and e.get("from") != target_node[:16]
                    ]
                
                action["result"] = f"Excluded {target_node} ({len(dead_pubkeys)} entries)"
                self._save_decision("exclude_node", {"node": target_node})
            
            # ── MEDIUM: Flapping → увеличить интервал ──
            elif dtype == "flapping":
                if self.mu and self.mu.interval < 120:
                    old = self.mu.interval
                    self.mu.interval = min(self.mu.interval * 2, 300)
                    action["result"] = f"Increased exchange interval: {old}s → {self.mu.interval}s"
                    self._save_decision("interval_change", {"old": old, "new": self.mu.interval})
            
            # ── INFO: Summary ──
            elif dtype == "summary":
                action["result"] = f"Logged: {dec.get('message','')}"
            
            actions.append(action)
            self.executed.append(action)
        
        if len(self.executed) > 50:
            self.executed = self.executed[-50:]
        
        return actions
    
    def _save_decision(self, action_type: str, details: dict):
        try:
            decisions = []
            try:
                with open(DECISIONS_FILE) as f:
                    decisions = json.load(f)
            except:
                pass
            decisions.append({
                "type": action_type,
                "timestamp": time.time(),
                "details": details,
            })
            with open(DECISIONS_FILE, "w") as f:
                json.dump(decisions[-50:], f, indent=2, ensure_ascii=False)
        except:
            pass
    
    def get_stats(self) -> str:
        return f"[Decision] {len(self.executed)} actions, last: {self.executed[-1]['decision']['type'] if self.executed else 'none'}"


# ═══════════════════════════════════════════════════════════════
#  WORKFLOW — Единый цикл
# ═══════════════════════════════════════════════════════════════
#
# Всё вместе:
#
#   init() → FirstContact → MatrixUpdater →
#     → ChronologyAnalyzer.analyze() → DecisionEngine.execute() →
#     → MatrixUpdater (следующий цикл) → ...
#
# Каждое решение улетает в SmartRouter как kind:39006.
# ═══════════════════════════════════════════════════════════════

class Workflow:
    """
    Единый цикл самообучения агента в сети SNIN.
    """
    
    def __init__(self, pubkey: str, name: str, role: str,
                 npub: str = "", nsec: str = "",
                 privkey: str = "", packet_pubkey: str = "", packet_privkey: str = ""):
        self.pubkey = pubkey          # mesh pubkey (суть)
        self.privkey = privkey
        self.packet_pubkey = packet_pubkey   # Ed25519 pubkey для подписи пакетов
        self.packet_privkey = packet_privkey # Ed25519 privkey
        self.npub = npub               # Nostr npub (метаданные)
        self.nsec = nsec               # Nostr nsec (для подписи kind:1)
        self.name = name
        self.role = role
        
        self.fc = None
        self.mu = None
        self.chronology = None
        self.decisions = None
        self.nostr = None
        self.devices = None
        
        self._cycle_count = 0
        self._running = False
        self._started_at = 0
        self._last_cycle_duration = 0
        self._errors = 0
        self._tasks = []
        self.stats = defaultdict(int)
    
    # ── INIT ──
    
    async def init(self):
        print(f"\n{'='*60}")
        print(f"  SNIN Workflow — {self.name} ({self.role})")
        print(f"{'='*60}")
        
        self._started_at = time.time()
        
        print(f"\n[Workflow] Layer 1 — First Contact...")
        self.fc = FirstContact(pubkey=self.pubkey, name=self.name, role=self.role)
        self.fc.packet_pubkey = self.packet_pubkey
        self.fc.packet_privkey = self.packet_privkey
        contact = await self.fc.scan_and_connect()
        self.stats["fc_time_ms"] = contact["total_time_ms"]
        self.stats["fc_nodes"] = contact["agents_in_network"]
        
        print(f"[Workflow] Layer 2 — Dynamic Matrix...")
        self.mu = MatrixUpdater(self.fc, exchange_interval=60, history_size=200)
        self.mu.packet_pubkey = self.packet_pubkey
        self.mu.packet_privkey = self.packet_privkey
        
        print(f"[Workflow] Layer 3 — Chronology Analyzer...")
        self.chronology = ChronologyAnalyzer(history_size=200)
        self.chronology.load()
        
        print(f"[Workflow] Layer 4 — Decision Engine...")
        self.decisions = DecisionEngine(self.fc, self.mu, agent_name=self.name)
        
        if NOSTR_AVAILABLE:
            print(f"[Workflow] Layer 5 — Nostr Bridge...")
            self.nostr = NostrBridgeLayer()
        
        print(f"[Workflow] Layer 6 — Device Layer...")
        self.devices = DeviceLayer(self.fc)
        
        print(f"\n{'='*60}")
        print(f"  ✅ Workflow initialized — {contact['total_time_ms']:.0f}ms")
        print(f"{'='*60}")
        
        return contact
    
    # ── CYCLE ──
    
    async def _cycle(self):
        """Один полный цикл: Exchange → Analyze → Decide → Act."""
        self._cycle_count += 1
        cycle_start = time.monotonic()
        
        # 1. Ping → Exchange → Merge (MatrixUpdater)
        ping_results = await self.mu._ping_all()
        peer_matrices = await self.mu._exchange_matrices(ping_results)
        changes = self.mu._merge_matrices(peer_matrices, ping_results)
        self.mu._recalculate_tiers()
        
        ping_ok = sum(1 for r in ping_results.values() if r.get("alive"))
        ping_total = len(ping_results)
        
        # 2. Хронология
        session_entry = {
            "session": self.mu.session,
            "timestamp": time.time(),
            "nodes_total": len(self.fc.matrix.get("nodes", {})),
            "alive": ping_ok,
            "dead": ping_total - ping_ok,
            "changes": changes,
            "latency_avg": round(sum(
                r.get("latency_ms", 0) for r in ping_results.values()
                if r.get("latency_ms", -1) > 0
            ) / max(ping_ok, 1), 2),
            "tiers": {
                "t1": sum(1 for n in self.fc.matrix.get("nodes", {}).values() if n.get("tier") == 1),
                "t2": sum(1 for n in self.fc.matrix.get("nodes", {}).values() if n.get("tier") == 2),
                "t3": sum(1 for n in self.fc.matrix.get("nodes", {}).values() if n.get("tier") == 3),
                "t4": sum(1 for n in self.fc.matrix.get("nodes", {}).values() if n.get("tier") == 4),
            },
        }
        
        self.mu.chronology.append(session_entry)
        if len(self.mu.chronology) > self.mu.history_size:
            self.mu.chronology = self.mu.chronology[-self.mu.history_size:]
        
        self.mu.pings_sent = ping_total
        self.mu.pings_ok = ping_ok
        self.mu.exchanges += 1
        if changes:
            self.mu.routes_adapted += 1
        
        # 3. Chronology Analysis (с реальными alive/dead nodes)
        alive_set = {pk for pk, r in ping_results.items() if r.get("alive")}
        total_set = set(ping_results.keys())
        
        self.chronology.add_session(session_entry,
            self.fc.matrix.get("nodes", {}),
            self.fc.matrix.get("edges", []))
        
        decisions = self.chronology.analyze(alive_nodes=alive_set, total_nodes=total_set)
        
        # 4. Decision Engine — исполнение + отправка в SR
        actions = []
        if decisions:
            actions = await self.decisions.execute(decisions)
        
        # 5. Статистика
        elapsed = (time.monotonic() - cycle_start) * 1000
        self._last_cycle_duration = elapsed
        self.stats["cycles"] += 1
        self.stats["total_changes"] += changes
        self.stats["decisions"] += len(decisions)
        self.stats["actions"] += len(actions)
        
        critical = [d for d in decisions if d.get("priority") in ("critical", "high")]
        alert = f" ⚠️ {len(critical)} alerts" if critical else ""
        
        print(
            f"[Workflow] C#{self._cycle_count} — "
            f"{ping_ok}/{ping_total} alive, "
            f"{changes} changes, "
            f"{len(decisions)} decisions, "
            f"{elapsed:.0f}ms"
            f"{alert}"
        )
    
    # ── RUN ──
    
    async def run(self):
        self._running = True
        
        contact = await self.init()
        await self.mu.start()
        
        if self.nostr:
            try:
                await self.nostr.start()
                self.stats["nostr"] = True
                print(f"[Workflow] ✅ Nostr Bridge started")
            except Exception as e:
                print(f"[Workflow] ⚠️ Nostr Bridge: {e}")
                self.stats["nostr"] = False
        
        try:
            with open(DECISIONS_FILE) as f:
                past = json.load(f)
                self.stats["past_decisions"] = len(past)
        except:
            pass
        
        print(f"\n[Workflow] ✅ Running. Cycle every {self.mu.interval}s")
        print(f"[Workflow] {self.chronology.get_trends()}")
        
        await asyncio.sleep(self.mu.interval)
        
        while self._running:
            try:
                await self._cycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._errors += 1
                print(f"[Workflow] ❌ Cycle error: {e}")
                if self._errors > 5:
                    print(f"[Workflow] 🔄 Too many errors, reinit...")
                    self.fc = FirstContact(pubkey=self.pubkey, name=self.name, role=self.role)
                    await self.fc.scan_and_connect()
                    self._errors = 0
            await asyncio.sleep(self.mu.interval)
        
        await self.mu.stop()
        if self.nostr:
            await self.nostr.stop()
    
    def stop(self):
        self._running = False
    
    def status(self) -> str:
        uptime = int(time.time() - self._started_at)
        nodes = len(self.fc.matrix.get("nodes", {})) if self.fc else 0
        ranks = self.fc.rank_channels()[:3] if self.fc else []
        
        parts = [
            f"[Workflow] {self.name} — Uptime: {uptime}s",
            f"  Mesh: {self.pubkey[:20]}...",
            f"  Cycles: {self._cycle_count}",
            f"  Nodes: {nodes}",
            f"  Cycle time: {self._last_cycle_duration:.0f}ms",
            f"  Decisions: {self.stats.get('decisions', 0)}",
            f"  Errors: {self._errors}",
        ]

        if self.npub:
            parts.append(f"  Nostr: {self.npub[:20]}...")

        if ranks:
            ch_str = ", ".join(f'{r["channel"]}({r["latency_ms"]}ms)' for r in ranks)
            parts.append(f"  Channels: {ch_str}")
        
        if self.chronology:
            parts.append(f"  {self.chronology.get_trends()}")
            recent = self.chronology.get_recent_decisions(2)
            for d in recent:
                msg = d.get('message','')[:60]
                parts.append(f"  ⚡ {d['priority'].upper()}: {msg}")
        
        return "\n".join(parts)
    
    def summary(self) -> str:
        nodes = len(self.fc.matrix.get("nodes", {})) if self.fc else 0
        return (
            f"[{self.name}] Mesh:{self.pubkey[:12]}.. | "
            f"C#{self._cycle_count} | "
            f"{nodes} nodes | "
            f"{self.stats.get('total_changes', 0)} changes | "
            f"{self._last_cycle_duration:.0f}ms/cycle | "
            f"{'🔄' if self.mu and self.mu.routes_adapted else '⏸'}"
        )


# ─── CLI Test ───
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="SNIN Workflow Agent")
    parser.add_argument("--agent", type=str, default="forecaster_ai")
    parser.add_argument("--pubkey", type=str, default="")
    parser.add_argument("--role", type=str, default="agent")
    parser.add_argument("--test-cycle", action="store_true")
    
    args = parser.parse_args()
    
    pubkey = args.pubkey
    role = args.role
    
    if not pubkey:
        try:
            with open(AGENTS_FILE) as f:
                agents = json.load(f)
                for pk, info in agents.items():
                    if info.get("name") == args.agent:
                        pubkey = pk
                        role = info.get("meta", {}).get("role", role)
                        break
        except:
            pass
    
    if not pubkey:
        pubkey = f"npub1{args.agent}_{int(time.time())}"
    
    print(f"Starting SNIN Workflow: {args.agent} ({role})")
    
    if args.test_cycle:
        async def test():
            wf = Workflow(pubkey=pubkey, name=args.agent, role=role)
            await wf.init()
            await wf._cycle()
            print()
            print(wf.status())
        asyncio.run(test())
    else:
        wf = Workflow(pubkey=pubkey, name=args.agent, role=role)
        try:
            asyncio.run(wf.run())
        except KeyboardInterrupt:
            print("\n[Workflow] Stopping...")
            wf.stop()

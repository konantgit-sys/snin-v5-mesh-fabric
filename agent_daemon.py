#!/usr/bin/env python3
"""SNIN Agent Daemon — SNIN Workflow в действии.

Единая точка запуска AI-агента в сети SNIN.
Запускает полный цикл самообучения через Workflow engine.

Слои, встроенные в daemon:
  Layer 1: First Contact      — сканирование, матрица, ранги
  Layer 2: Dynamic Matrix     — пинг, обмен, слияние (60s)
  Layer 3: Chronology         — анализ истории, тренды
  Layer 4: Decision Engine    — исполнение решений
  Layer 5: Nostr Bridge       — федерация (если доступен)
  Layer 6: Device Layer       — IoT, ESP32

Запуск: python3 agent_daemon.py <agent_name>
"""

import asyncio, json, os, sys, time, signal
from pathlib import Path

sys.path.insert(0, "/home/agent/data/sites/relay-mesh")
from mesh_client import MeshAgent
from workflow import Workflow
from mesh_identity import load_or_create_identity, link_external, pubkey_to_did, sign_attestation
from agent_gossip import AgentGossipChannel

# L5 — Identity & Reputation интеграция
sys.path.insert(0, str(Path(__file__).parent))
from reputation import calculate_reputation, get_reputation_for_pubkey

# ─── Legacy Nostr Registry (метаданные, не суть) ───
NOSTR_KEYS = {
    "forecaster_ai": {
        "npub": "npub1qplr6kz4eeqdhy8mwumhq5m6yftfhl7tc5vrns350nresqksl8rq28c9ce",
        "nsec": "nsec1mdc2lfqg9cc4swgkldqw6ztwhawazm4sz3a52vm4h7p9mdw7wsfsyv7vm4",
    },
    "archivist_ai": {
        "npub": "npub1hnaz4q7fqlsv565w770xl56prkfddk9xmjrk2r9lhg4xkrl04tzq3xu8c4",
        "nsec": "nsec1gklepes03plj9etqryht55cytgs2yzvqhv0uhpyhgzvhfrvs788stw97ax",
    },
    "anton_ai": {
        "npub": "npub1umau63896ryszn2jw9sx8hvvaw4l25tagfaty90u27nhsfqdadjsp640jk",
        "nsec": "nsec1xpz3vk5dw8mg29j7ec8d9yk7uwerkakkg947dx87r3j80n2szuqsrcxdfr",
    },
}

AGENTS = {
    "forecaster_ai": {"role": "forecaster", "bio": "AI прогнозист — анализирует рынки, тренды, социальные сигналы"},
    "archivist_ai": {"role": "archivist", "bio": "AI архивариус — собирает, индексирует и хранит данные сети"},
    "anton_ai": {"role": "assistant", "bio": "AI ассистент — коммуникация с пользователями, модерация"},
}

HEARTBEAT_INTERVAL = 60
LOG_DIR = "/home/agent/data/sites/relay-mesh/logs"


class AgentDaemon:
    """
    Агент SNIN Mesh — запускает полный Workflow.
    
    Идентичность агента: mesh pubkey (сгенерирован при первом запуске).
    Nostr npub/nsec — метаданные, не суть.
    
    Daemon только:
      1. Загружает mesh identity агента
      2. Привязывает Nostr npub как метаданные (если есть)
      3. Создаёт Workflow с mesh_pubkey/name/role
      4. Запускает workflow.run()
      5. Ловит сигналы остановки
    """
    
    def __init__(self, name: str):
        info = AGENTS.get(name)
        if not info:
            raise ValueError(f"Unknown agent: {name}")
        
        self.name = name
        self.role = info["role"]
        self.bio = info["bio"]
        
        # ═══ Mesh identity (суть) ═══
        identity = load_or_create_identity(name)
        self.mesh_pubkey = identity["mesh_pubkey"]
        self.mesh_privkey = identity["mesh_privkey"]
        self.mesh_npub = identity["mesh_npub"]
        self.packet_pubkey = identity.get("packet_pubkey", "")
        self.packet_privkey = identity.get("packet_privkey", "")
        
        # ═══ Nostr metadata (если есть) ═══
        nostr = NOSTR_KEYS.get(name, {})
        self.npub = nostr.get("npub", "")
        self.nsec = nostr.get("nsec", "")
        
        # Привязываем Nostr npub к mesh identity
        if self.npub and not identity.get("links", {}).get("nostr_npub"):
            link_external(name, "nostr_npub", self.npub)
        
        self.workflow = None
        self._running = False
        self.handler = None
    
    def _setup_signal_handlers(self):
        """Сигналы для graceful shutdown."""
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop)
            except NotImplementedError:
                pass
    
    def _stop(self):
        print(f"\n[{self.name}] Stopping...")
        self._running = False
        if self.workflow:
            self.workflow.stop()
    
    async def run(self):
        """
        Запуск агента с auto-recovery.
        
        Если workflow падает с любой ошибкой — перезапускается через 5 секунд.
        """
        print(f"{'='*50}")
        print(f"  SNIN Agent: {self.name} ({self.role})")
        print(f"  Mesh: {self.mesh_pubkey[:24]}...")
        
        # ═══ Фаза 1: DID + Reputation ═══
        try:
            from mesh_identity import pubkey_to_did
            self.did = pubkey_to_did(self.mesh_pubkey)
            print(f"  DID: {self.did[:48]}...")
        except Exception:
            self.did = f"did:snin:{self.mesh_pubkey[:32]}"
            print(f"  DID: {self.did[:48]}... (inline)")
        
        try:
            from reputation import calculate_reputation
            rep = calculate_reputation(self.name)
            self.reputation = rep["score"]
            print(f"  Reputation: {self.reputation:.4f}")
        except Exception as e:
            self.reputation = 0.3
            print(f"  Reputation: 0.3 (default, {e})")
        
        if self.npub:
            print(f"  Nostr: {self.npub[:24]}... (metadata)")
        print(f"{'='*50}")
        
        self._running = True
        self._setup_signal_handlers()
        
        # Вечный цикл с auto-recovery
        while self._running:
            try:
                # Создаём MeshAgent для подключения к событиям
                self.mesh_agent = MeshAgent(
                    pubkey=self.mesh_pubkey,
                    name=self.name,
                    api_url="http://127.0.0.1:9907"
                )
                # Зарегистрироваться в relay-mesh
                try:
                    reg = await self.mesh_agent.register()
                    print(f"  Register: {reg}")
                except Exception as e:
                    print(f"  Register skipped: {e}")
                
                # Создаём и запускаем Workflow
                self.workflow = Workflow(
                    pubkey=self.mesh_pubkey,
                    privkey=self.mesh_privkey,
                    packet_pubkey=self.packet_pubkey,
                    packet_privkey=self.packet_privkey,
                    name=self.name,
                    role=self.role,
                    npub=self.npub,
                    nsec=self.nsec,
                )
                
                # ═══ Инициализация MeshMessageHandler ═══
                from mesh_message_handler import MeshMessageHandler
                self.handler = MeshMessageHandler(
                    agent_name=self.name,
                    pubkey=self.mesh_pubkey,
                    log_dir="/home/agent/data/sites/relay-mesh/logs"
                )
                
                # Шаг 1: подключаемся к SR и подписываемся
                if await self.mesh_agent.connect():
                    print(f"[{self.name}] ✅ Connected to SmartRouter")
                    await self.mesh_agent.subscribe(on_push_event=self._on_mesh_event)
                else:
                    print(f"[{self.name}] ⚠️ SmartRouter not ready, retrying in background...")
                    asyncio.create_task(self._retry_subscribe())
                
                # Шаг 2: Gossip-канал (p2p с другими агентами)
                self.gossip = AgentGossipChannel(
                    agent_name=self.name,
                    pubkey=self.mesh_pubkey,
                    api_url="http://127.0.0.1:9907",
                    mesh_agent=self.mesh_agent,
                )
                self.gossip.on_peer_change(self._on_peer_change)
                self.gossip.on_gossip_message(self._on_gossip_event)
                await self.gossip.start()
                
                # ═══ Репутационный heartbeat (L5 — Identity) ═══
                asyncio.create_task(self._reputation_heartbeat())
                
                # Запускаем workflow
                await self.workflow.run()
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[{self.name}] 💥 Crash: {type(e).__name__}: {e}")
                print(f"[{self.name}] 🔄 Auto-recovery in 5s...")
                # Cleanup
                try:
                    if hasattr(self, 'gossip'):
                        await self.gossip.stop()
                except: pass
                try:
                    if hasattr(self, 'mesh_agent'):
                        await self.mesh_agent.unsubscribe()
                except: pass
                try:
                    if self.workflow:
                        self.workflow.stop()
                except: pass
                print(f"[{self.name}] 💤 Restart in 5s...")
                await asyncio.sleep(5)
    
    async def _retry_subscribe(self):
        """Повторная попытка подключения к SR."""
        for attempt in range(5):
            await asyncio.sleep(10)
            if await self.mesh_agent.connect():
                await self.mesh_agent.subscribe(on_push_event=self._on_mesh_event)
                return
        print(f"[{self.name}] ❌ Failed to connect to SmartRouter after 5 retries")
    
    async def _on_mesh_event(self, event: dict):
        """Обработка входящего события из relay-mesh."""
        kind = event.get("kind", "?")
        frm = event.get("from", "?")[:24]
        
        # Gossip peer discovery
        if event.get("type") == "gossip_peer_update" and hasattr(self, 'gossip'):
            await self.gossip.on_peer_event(event)
        
        # pipeline_feed от RE — много событий, логируем редко
        if kind == "pipeline_feed":
            return
        
        print(f"[{self.name}] 📨 Mesh event kind={kind} from={frm}")
        
        # ═══ Обработка через MeshMessageHandler ═══
        if self.handler:
            parsed = self.handler.parse(event)
            
            if parsed.get("is_for_me"):
                print(f"[{self.name}] 🎯 Сообщение для меня: от={parsed['from_name']} тип={parsed['msg_type']} seq={parsed['sequence']}")
                
                # Сформировать ответ
                reply = self.handler.route(parsed)
                if reply:
                    print(f"[{self.name}] ➡️ Ответ: {reply['type']} → {reply['to']} seq={reply['sequence']}")
                    
                    # Отправить через SR если есть подключение
                    if hasattr(self, 'mesh_agent') and self.mesh_agent and self.mesh_agent._writer:
                        try:
                            payload = json.dumps(reply)
                            self.mesh_agent._writer.write((payload + "\n").encode())
                            await asyncio.wait_for(self.mesh_agent._writer.drain(), timeout=3)
                            print(f"[{self.name}] ✅ Ответ отправлен в mesh")
                        except Exception as e:
                            print(f"[{self.name}] ⚠️ Не удалось отправить ответ: {e}")
                    else:
                        print(f"[{self.name}] ⚠️ Нет writer для отправки ответа")
        else:
            print(f"[{self.name}] ⚠️ handler не инициализирован")
        
        # Передать событие в workflow если нужно
        if hasattr(self, 'workflow') and self.workflow:
            pass  # будущая обработка
    
    async def _on_gossip_event(self, from_pubkey: str, payload: dict):
        """Обработка gossip-сообщения от другого агента (p2p).
        
        from_pubkey — mesh pubkey отправителя
        payload — тело сообщения (содержит content с JSON)
        """
        # Преобразуем в формат события, который понимает handler
        content = payload.get("content", payload)
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except json.JSONDecodeError:
                content = {"text": content}
        
        event = {
            "kind": payload.get("kind", 39004),
            "from": from_pubkey,
            "content": content,
        }
        
        print(f"[{self.name}] 📩 Gossip event from={from_pubkey[:16]}")
        
        # Обработка через MeshMessageHandler
        if self.handler:
            parsed = self.handler.parse(event)
            
            if parsed.get("is_for_me"):
                print(f"[{self.name}] 🎯 Gossip ДЛЯ МЕНЯ: от={parsed['from_name']} тип={parsed['msg_type']} seq={parsed['sequence']}")
                
                reply = self.handler.route(parsed)
                if reply:
                    print(f"[{self.name}] ➡️ Gossip ответ: {reply['type']} → {reply['to']} seq={reply['sequence']}")
                    
                    # Отправить ответ обратно через gossip
                    if hasattr(self, 'gossip') and self.gossip:
                        try:
                            sent = await self.gossip.send_to(from_pubkey, reply, kind=39004)
                            if sent:
                                print(f"[{self.name}] ✅ Gossip ответ отправлен")
                            else:
                                print(f"[{self.name}] ⚠️ Не удалось отправить gossip ответ")
                        except Exception as e:
                            print(f"[{self.name}] ⚠️ Gossip ответ error: {e}")
    
    async def _on_peer_change(self, peer_pubkey: str, status: str):
        """Callback от gossip: пир ожил/умер."""
        print(f"[{self.name}] 🌐 Gossip peer {peer_pubkey[:16]} → {status}")
        # Если все пиры умерли и SR тоже нет — агент в изоляции
        peer_count = await self.gossip.peer_count()
        sr_alive = self.mesh_agent._sub_connected if hasattr(self.mesh_agent, '_sub_connected') else False
        if peer_count == 0 and not sr_alive:
            print(f"[{self.name}] ⚠️ ISOLATED: no peers, no SR")
        elif peer_count > 0 and not sr_alive:
            print(f"[{self.name}] 🔀 Degraded mode: gossip only ({peer_count} peers)")

    async def _reputation_heartbeat(self):
        """L5 — Репутационный heartbeat.
        
        Каждые HEARTBEAT_INTERVAL (60s) обновляет репутацию агента
        и влияет на поведение:
        - score > 0.5 → увеличивает приоритет heartbeat
        - score < 0.3 → снижает активность (backoff)
        - Авто-аттестация других агентов при score > 0.7
        """
        while True:
            try:
                rep = calculate_reputation(self.name)
                score = rep["score"]
                rep_details = f"R={rep['reliability']:.3f} C={rep['contribution']:.3f} Att={rep['attest_score']:.3f}"
                
                # Авто-аттестация: если score > 0.7, аттестуем других агентов
                if score > 0.7:
                    for other_name in AGENTS:
                        if other_name == self.name:
                            continue
                        try:
                            other = load_or_create_identity(other_name)
                            other_did = pubkey_to_did(other["mesh_pubkey"])
                            # Проверяем, не аттестовали ли уже
                            existing = [a for a in self.identity.get("attestations", [])
                                       if a.get("target_did") == other_did]
                            if not existing:
                                att = sign_attestation(self.name, other_did, role="verifier")
                                print(f"[{self.name}] 🤝 Auto-attest: → {other_name} ({other_did[:24]}...) ✓")
                        except Exception as e:
                            print(f"[{self.name}] ⚠️ Attest error for {other_name}: {e}")
                
                # Раз в 10 циклов — публикуем score в gossip
                if int(time.time() / HEARTBEAT_INTERVAL) % 10 == 0:
                    score_msg = {
                        "type": "reputation_update",
                        "agent": self.name,
                        "score": score,
                        "reliability": rep["reliability"],
                        "age_days": rep["details"]["age_days"],
                        "attestations": rep["details"]["attestations"],
                        "timestamp": time.time(),
                    }
                    if hasattr(self, 'gossip') and self.gossip:
                        try:
                            await self.gossip.broadcast(score_msg, kind=39005)
                            print(f"[{self.name}] 📣 Rep score broadcast: {score:.4f}")
                        except:
                            pass
                
                print(f"[{self.name}] 📊 Rep: {score:.4f} [{rep_details}]")
                
            except Exception as e:
                print(f"[{self.name}] ⚠️ Rep heartbeat error: {e}")
            
            # Адаптивный интервал: выше rep → чаще heartbeat
            try:
                rep = calculate_reputation(self.name)
                adaptive_interval = max(30, int(HEARTBEAT_INTERVAL * (1.5 - rep["score"])))
            except:
                adaptive_interval = HEARTBEAT_INTERVAL
            await asyncio.sleep(adaptive_interval)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 agent_daemon.py <agent_name>")
        print(f"Agents: {', '.join(AGENTS.keys())}")
        sys.exit(1)
    
    name = sys.argv[1]
    daemon = AgentDaemon(name)
    
    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        print(f"\n[{name}] Stopped by user")

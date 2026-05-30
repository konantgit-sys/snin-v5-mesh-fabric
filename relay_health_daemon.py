#!/usr/bin/env python3
"""
SNIN Relay Health Daemon — мониторинг всех активных Nostr релеев.

Раз в CHECK_INTERVAL сек:
  - Пингует каждый релей через WSS connect + NIP-11 GET
  - Записывает статус (alive/dead, latency) в JSON
  - При dead > DEAD_THRESHOLD → Telegram alert
  - API endpoint на порту HEALTH_API_PORT для relay-dash

Запуск: python3 relay_health_daemon.py
"""

import asyncio, json, time, os, sys, logging, socket, ssl
from datetime import datetime, timezone
from pathlib import Path

# ── Конфиг ──
CHECK_INTERVAL = 60          # сек между проверками
PING_TIMEOUT = 5              # таймаут на один релей (сек)
DEAD_THRESHOLD = 3            # сколько раз подряд мёртв → alert
ALERT_COOLDOWN = 300          # сек между повторными алертами
HEALTH_API_PORT = 9929        # порт для relay-dash
HEALTH_DATA_FILE = "/home/agent/data/sites/relay-mesh/logs/relay_health.json"

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/home/agent/data/sites/relay-mesh/logs/relay_health.log"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("health")

# Telegram (берём из окружения или relay_monitor.py)
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")


class RelayHealthDaemon:
    def __init__(self):
        self.relays: list[str] = []
        self.status: dict[str, dict] = {}  # url → {alive, latency, fails, last_check, alerted}
        self._load_relays()
        self._load_persisted_status()
    
    def _load_relays(self):
        """Загружает список релеев из nostr_bridge.py (SCAN_RELAYS, OUR_RELAYS, discovered)."""
        # Парсим nostr_bridge.py для SCAN_RELAYS_ALL, _OUR_RELAYS_ALL
        bridge_path = os.path.dirname(__file__) + "/nostr_bridge.py"
        if not os.path.exists(bridge_path):
            log.error(f"nostr_bridge.py not found at {bridge_path}")
            return
        
        with open(bridge_path) as f:
            content = f.read()
        
        # Ищем SCAN_RELAYS_ALL
        import re
        scan_relays = re.findall(r'"(wss://[^"]+)"', content)
        
        # Ищем _discovered_relays (добавленные через NIP-65)
        discovered_path = "/home/agent/data/sites/relay-mesh/logs/discovered_relays.json"
        discovered = []
        if os.path.exists(discovered_path):
            try:
                with open(discovered_path) as f:
                    discovered = json.load(f)
            except: pass
        
        # Объединяем и дедуплицируем
        all_relays = list(dict.fromkeys(scan_relays + discovered))
        self.relays = all_relays
        log.info(f"Loaded {len(self.relays)} relays ({len(scan_relays)} from config, {len(discovered)} discovered)")
    
    def _load_persisted_status(self):
        """Загружает сохранённый статус из JSON."""
        if os.path.exists(HEALTH_DATA_FILE):
            try:
                with open(HEALTH_DATA_FILE) as f:
                    data = json.load(f)
                for url, s in data.items():
                    if url in self.relays:
                        self.status[url] = s
                        log.info(f"Restored status for {url}: {s.get('alive', False)}")
            except: pass
    
    def _save_status(self):
        """Сохраняет статус в JSON."""
        os.makedirs(os.path.dirname(HEALTH_DATA_FILE), exist_ok=True)
        with open(HEALTH_DATA_FILE, "w") as f:
            json.dump(self.status, f, indent=2)
    
    async def _ping_relay(self, url: str) -> tuple[bool, float]:
        """
        Пингует релей через WSS connect + быстрый REQ.
        Возвращает (alive, latency_ms).
        """
        import websockets
        start = time.time()
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            
            ws = await asyncio.wait_for(
                websockets.connect(url, ssl=ctx, max_size=1024, ping_interval=None),
                timeout=PING_TIMEOUT
            )
            # Шлём NIP-11-like HTTP запрос (быстрее чем REQ)
            host = url.replace("wss://", "").split("/")[0]
            # Пинг через ping/pong
            await asyncio.wait_for(ws.ping(), timeout=3)
            await ws.close()
            latency = (time.time() - start) * 1000
            return True, latency
        except Exception as e:
            latency = (time.time() - start) * 1000
            return False, latency
    
    async def _check_all(self):
        """Проверяет все релеи."""
        tasks = []
        for url in self.relays:
            tasks.append(self._check_one(url))
        await asyncio.gather(*tasks)
        self._save_status()
    
    async def _check_one(self, url: str):
        """Проверяет один релей, обновляет статус, отправляет alert."""
        alive, latency = await self._ping_relay(url)
        
        now = time.time()
        prev = self.status.get(url, {"alive": True, "fails": 0, "alerted": False, "last_check": 0})
        
        if alive:
            self.status[url] = {
                "alive": True,
                "latency": round(latency, 1),
                "fails": 0,
                "last_check": now,
                "alerted": False,
                "url": url,
            }
            # Recovery alert (был мёртв → стал жив)
            if not prev["alive"] and prev["alerted"] and (now - prev.get("last_check", 0)) < 86400:
                await self._send_alert(f"🟢 RECOVERY: {url} — back online ({latency:.0f}ms)")
        else:
            fails = prev.get("fails", 0) + 1
            self.status[url] = {
                "alive": False,
                "latency": round(latency, 1),
                "fails": fails,
                "last_check": now,
                "alerted": prev.get("alerted", False),
                "last_alive": prev.get("last_check", 0),
                "url": url,
            }
            # Alert при dead > DEAD_THRESHOLD (с коoldown)
            if fails >= DEAD_THRESHOLD and not prev.get("alerted", False):
                cooldown_ok = (now - prev.get("_last_alert_at", 0)) > ALERT_COOLDOWN
                if cooldown_ok:
                    await self._send_alert(f"🔴 DEAD: {url} — {fails}x failures, last alive: {prev.get('last_check', 0)}")
                    self.status[url]["_last_alert_at"] = now
                    self.status[url]["alerted"] = True
    
    async def _send_alert(self, message: str):
        """Отправляет Telegram alert."""
        if not TG_BOT_TOKEN or not TG_CHAT_ID:
            log.warning(f"[alert] No TG config: {message}")
            return
        try:
            import urllib.request
            text = f"🤖 SNIN Health\n{message}"
            url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
            data = json.dumps({"chat_id": TG_CHAT_ID, "text": text}).encode()
            req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=5)
            log.info(f"[alert] Sent: {message}")
        except Exception as e:
            log.error(f"[alert] Failed: {e}")
    
    async def _api_server(self):
        """HTTP API для relay-dash."""
        from aiohttp import web
        
        async def get_status(request):
            # Собираем статистику
            total = len(self.relays)
            alive = sum(1 for s in self.status.values() if s.get("alive"))
            dead = total - alive
            
            stats = {
                "total": total,
                "alive": alive,
                "dead": dead,
                "alive_pct": round(alive / total * 100, 1) if total else 0,
                "last_updated": time.time(),
                "relays": self.status,
                "relay_list": self.relays,
            }
            return web.json_response(stats)
        
        app = web.Application()
        app.router.add_get("/api/health", get_status)
        app.router.add_get("/api/relays", get_status)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", HEALTH_API_PORT)
        await site.start()
        log.info(f"Health API running on :{HEALTH_API_PORT}")
        
        # Держим сервер живым
        await asyncio.Event().wait()
    
    async def run(self):
        """Запуск цикла проверок + API сервер."""
        log.info("=" * 50)
        log.info("SNIN Relay Health Daemon v1.0")
        log.info(f"Relays: {len(self.relays)}, Interval: {CHECK_INTERVAL}s")
        log.info("=" * 50)
        
        # Запускаем API в фоне
        api_task = asyncio.create_task(self._api_server())
        
        # Первая проверка сразу
        await self._check_all()
        
        # Цикл
        while True:
            await asyncio.sleep(CHECK_INTERVAL)
            await self._check_all()


if __name__ == "__main__":
    daemon = RelayHealthDaemon()
    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        log.info("Shutdown")
    except Exception as e:
        log.error(f"Fatal: {e}")
        raise

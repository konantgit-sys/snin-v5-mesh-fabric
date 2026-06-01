#!/usr/bin/env python3
"""Anti-DDoS Daemon — обёртка для запуска AntiDDoS как standalone сервиса (:9970)"""
import asyncio, json, logging
from aiohttp import web
from anti_ddos import AntiDDoS

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')
logger = logging.getLogger("anti_ddos_daemon")

async def start_daemon():
    ddos = AntiDDoS()
    
    async def stats_handler(request):
        return web.json_response(ddos.get_stats())
    
    async def status_handler(request):
        return web.json_response({"status": "ok", "timestamp": datetime.now().isoformat()})
    
    async def save_status(request):
        ddos.save_status()
        return web.json_response({"ok": True})
    
    from datetime import datetime
    app = web.Application()
    app.router.add_get("/api/ddos/stats", stats_handler)
    app.router.add_get("/api/ddos/status", status_handler)
    app.router.add_post("/api/ddos/save", save_status)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 9970)
    await site.start()
    logger.info("Anti-DDoS API listening on :9970")
    
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(start_daemon())

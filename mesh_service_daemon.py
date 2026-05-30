#!/usr/bin/env python3
"""mesh_service_daemon.py — Вектор 5 + 6

Запускает:
  Vector 5 — First Contact: авто-обнаружение агентов каждые 30 сек
  Vector 6 — DAO Bridge: DAO proposals → mesh actions каждые 60 сек

Интеграция:
  - Регистрирует найденных агентов в Redis DHT
  - Уведомляет SR через Redis pub/sub
  - Выполняет DAO-пропозалы с action=relay_mesh
"""

import asyncio
import json
import os
import sys
import time
import logging
import traceback

logging.basicConfig(level=logging.INFO, format='%(asctime)s [MESH_DAEMON] %(message)s')
logger = logging.getLogger('mesh_daemon')

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from first_contact_agent import FirstContact
import dao_mesh_bridge as dao

# ── Redis ──
REDIS_AVAILABLE = False
redis_client = None

try:
    import redis.asyncio as redis_py
    REDIS_AVAILABLE = True
except ImportError:
    logger.warning("redis.asyncio not available — using fallback via pipe")

# ═══════════════════════════════════════════
#  ВЕКТОР 5: FIRST CONTACT
# ═══════════════════════════════════════════

FIRST_CONTACT_INTERVAL = 30  # секунд между сканами

def _redis_sync(key: str, value: str, ttl: int = 86400):
    """Синхронная запись в Redis (без asyncio)."""
    try:
        import subprocess
        subprocess.run(
            ["redis-cli", "setex", key, str(ttl), value],
            capture_output=True, timeout=3
        )
        return True
    except:
        return False

def _redis_hset(name: str, key: str, value: str):
    try:
        import subprocess
        subprocess.run(
            ["redis-cli", "hset", name, key, value],
            capture_output=True, timeout=3
        )
        return True
    except:
        return False

def _redis_publish(channel: str, message: str):
    try:
        import subprocess
        subprocess.run(
            ["redis-cli", "publish", channel, message],
            capture_output=True, timeout=3
        )
        return True
    except:
        return False

async def first_contact_cycle(fc: FirstContact):
    """Один цикл First Contact: сканирование + регистрация агентов."""
    try:
        start = time.time()
        
        # 1. Сканировать каналы
        channels = await fc.scan_channels()
        available = {ch: info for ch, info in channels.items() if info.get("available")}
        logger.info(f"Scan: {len(available)}/{len(channels)} каналов доступно")
        
        # 2. Построить матрицу
        matrix = await fc.build_matrix()
        agent_count = len(matrix.get("nodes", {}))
        logger.info(f"Matrix: {agent_count} агентов, {len(matrix.get('edges',[]))} связей")
        
        # 3. Ранжировать каналы
        ranked = fc.rank_channels()
        best_channels = [r["channel"] for r in ranked[:3]]
        logger.info(f"Top каналы: {best_channels}")
        
        # 4. Регистрировать агентов в Redis DHT
        local_relay_addr = os.environ.get("RELAY_ADDR", "127.0.0.1:9105")
        for pk_hex, info in matrix.get("nodes", {}).items():
            dht_entry = json.dumps({
                "name": info.get("name", pk_hex[:16]),
                "role": info.get("role", "agent"),
                "tier": info.get("tier", 4),
                "ip": info.get("address", "127.0.0.1"),
                "port": info.get("port", 9932),
                "relay_addr": info.get("relay_addr", local_relay_addr),
                "alive": True,
                "last_seen": time.time(),
                "first_contact": True,
            })
            # Redis: dht:agents hash + dht:agent:{pubkey}
            _redis_hset("dht:agents", pk_hex, dht_entry)
            _redis_sync(f"dht:agent:{pk_hex}", dht_entry, ttl=3600)
        
        # 5. Записать матрицу в Redis для дашборда
        _redis_sync("mesh:matrix", json.dumps(matrix), ttl=120)
        
        # 6. Опубликовать уведомление для SR через pub/sub
        if agent_count > 0:
            discovery_msg = json.dumps({
                "type": "first_contact",
                "agents": agent_count,
                "channels": available,
                "ranked": ranked,
                "timestamp": time.time(),
            })
            _redis_publish("mesh:discovery", discovery_msg)
        
        elapsed = (time.time() - start) * 1000
        logger.info(f"FirstContact завершён за {elapsed:.0f}ms")
        return agent_count
        
    except Exception as e:
        logger.error(f"FirstContact ошибка: {e}")
        logger.error(traceback.format_exc())
        return 0


async def first_contact_loop(pubkey: str = "mesh_discovery_agent"):
    """Бесконечный цикл First Contact."""
    fc = FirstContact(pubkey=pubkey, name="mesh_discovery", role="discovery")
    logger.info(f"First Contact старт: pubkey={pubkey}")
    
    while True:
        count = await first_contact_cycle(fc)
        await asyncio.sleep(FIRST_CONTACT_INTERVAL)


# ═══════════════════════════════════════════
#  ВЕКТОР 6: DAO BRIDGE
# ═══════════════════════════════════════════

DAO_POLL_INTERVAL = 60  # секунд между опросами

async def dao_cycle():
    """Один цикл DAO Bridge: опрос + выполнение пропозалов."""
    try:
        start = time.time()
        
        # Проверка доступности Chrono
        try:
            import requests
            health = requests.get(f"{dao.CHRONO_URL}/health", timeout=5)
            if health.status_code != 200:
                logger.warning(f"Chrono недоступен: {health.status_code}")
                return
        except Exception as e:
            logger.warning(f"Chrono недоступен: {e}")
            return
        
        # Выполняем poll_dao (синхронная функция)
        dao.poll_dao()
        
        logger.info(f"DAO Bridge опрос завершён за {(time.time()-start)*1000:.0f}ms")
        
    except Exception as e:
        logger.error(f"DAO Bridge ошибка: {e}")


async def dao_loop():
    """Бесконечный цикл DAO Bridge."""
    logger.info(f"DAO Bridge старт: chrono={dao.CHRONO_URL}")
    
    while True:
        await dao_cycle()
        await asyncio.sleep(DAO_POLL_INTERVAL)


# ═══════════════════════════════════════════
#  ЗАПУСК
# ═══════════════════════════════════════════

async def main():
    logger.info("=== Mesh Service Daemon (V5+V6) ===")
    logger.info(f"First Contact: {FIRST_CONTACT_INTERVAL}s")
    logger.info(f"DAO Bridge: {DAO_POLL_INTERVAL}s")
    
    await asyncio.gather(
        first_contact_loop(),
        dao_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Daemon остановлен")


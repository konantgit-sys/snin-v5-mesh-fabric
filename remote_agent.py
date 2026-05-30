#!/usr/bin/env python3
"""SNIN Remote Agent — для тестирования gossip mesh с внешней ноды.

Что делает:
- Регистрируется в хабе (snin-gossip.v2.site)
- Открывает gossip-порт для приёма ping/сообщений
- Heartbeat каждые 30 сек — хаб видит что агент жив
- Принимает ping от пиров и отвечает pong
- Пишет лог всего что происходит

Требования: Python 3.8+, установка не требуется.
Запуск: python3 remote_agent.py

Через 60 минут автоматически снимает регистрацию и завершается.
"""

import asyncio, json, time, urllib.request, urllib.error, socket, sys, os

# ═══ КОНФИГ ═══
HUB_URL = "https://snin-gossip.v2.site"
HEARTBEAT_INTERVAL = 30  # сек
TEST_DURATION = 60        # минут — после этого auto-cleanup
GOSSIP_PORT = 9908        # порт для приёма gossip-сообщений

# Генерируем уникальный ID агента при каждом запуске
import uuid
AGENT_NAME = f"remote_{uuid.uuid4().hex[:8]}"
AGENT_PUBKEY = uuid.uuid4().hex * 4  # 64 hex символа (как настоящий pubkey)


async def main():
    print(f"╔═══ SNIN Remote Agent ═══╗")
    print(f"║ Имя: {AGENT_NAME}")
    print(f"║ Хаб: {HUB_URL}")
    print(f"║ Порт: {GOSSIP_PORT}")
    print(f"║ Длительность: {TEST_DURATION} мин")
    print(f"╚════════════════════════╝")
    print()
    
    # 1. Регистрация в хабе
    print(f"[{AGENT_NAME}] 📡 Регистрация в хабе...")
    try:
        data = json.dumps({
            "pubkey": AGENT_PUBKEY,
            "name": AGENT_NAME,
            "gossip_host": get_external_ip(),
            "gossip_port": GOSSIP_PORT,
        }).encode()
        req = urllib.request.Request(
            f"{HUB_URL}/agents/gossip",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        resp = urllib.request.urlopen(req, timeout=10)
        result = json.loads(resp.read())
        print(f"[{AGENT_NAME}] ✅ Зарегистрирован: {result}")
    except Exception as e:
        print(f"[{AGENT_NAME}] ❌ Ошибка регистрации: {e}")
        print(f"[{AGENT_NAME}] ⚠️  Продолжаю без регистрации...")
    
    # 2. Получаем список пиров
    print(f"[{AGENT_NAME}] 👥 Получаю список пиров...")
    try:
        req = urllib.request.Request(f"{HUB_URL}/agents/gossip/peers")
        resp = urllib.request.urlopen(req, timeout=10)
        peers = json.loads(resp.read()).get("peers", [])
        print(f"[{AGENT_NAME}] 👥 Найдено пиров: {len(peers)}")
        for p in peers:
            print(f"    {p['name']}: {p['gossip_host']}:{p['gossip_port']}")
    except Exception as e:
        print(f"[{AGENT_NAME}] ❌ Ошибка получения пиров: {e}")
        peers = []
    
    # 3. Запускаем gossip-сервер
    print(f"[{AGENT_NAME}] 🔌 Запуск gossip-сервера на :{GOSSIP_PORT}...")
    server_task = asyncio.create_task(gossip_server())
    
    # 4. Heartbeat + ping loop
    print(f"[{AGENT_NAME}] ❤️ Heartbeat каждые {HEARTBEAT_INTERVAL}с")
    print(f"[{AGENT_NAME}] ⏱️  Авто-завершение через {TEST_DURATION} мин")
    print()
    
    start_time = time.time()
    cycle = 0
    
    try:
        while True:
            elapsed = (time.time() - start_time) / 60
            if elapsed >= TEST_DURATION:
                print(f"\n[{AGENT_NAME}] ⏱️ Тест завершён ({TEST_DURATION} мин)")
                break
            
            cycle += 1
            
            # Heartbeat — обновляем last_seen в API
            await heartbeat()
            
            # Ping пиров через API
            try:
                req = urllib.request.Request(f"{HUB_URL}/agents/gossip/peers")
                resp = urllib.request.urlopen(req, timeout=10)
                current_peers = json.loads(resp.read()).get("peers", [])
                
                for p in current_peers:
                    if p['pubkey'] == AGENT_PUBKEY:
                        continue
                    # TCP ping к пиру
                    ok = await tcp_ping(p['gossip_host'], p['gossip_port'])
                    status = "✅" if ok else "💀"
                    print(f"[{AGENT_NAME}] ❤️ {p['name']} [{p['gossip_host']}:{p['gossip_port']}] → {status}")
            except Exception as e:
                print(f"[{AGENT_NAME}] ⚠️ Heartbeat error: {e}")
            
            await asyncio.sleep(HEARTBEAT_INTERVAL)
    
    finally:
        # Cleanup — снимаем регистрацию
        print(f"[{AGENT_NAME}] 🧹 Cleanup...")
        try:
            req = urllib.request.Request(
                f"{HUB_URL}/agents/gossip/{AGENT_PUBKEY}",
                method="DELETE"
            )
            urllib.request.urlopen(req, timeout=5)
            print(f"[{AGENT_NAME}] ✅ Регистрация удалена")
        except:
            print(f"[{AGENT_NAME}] ⚠️ Регистрация уже удалена")
        
        server_task.cancel()
        print(f"[{AGENT_NAME}] ✅ Завершено")


async def gossip_server():
    """TCP сервер для приёма gossip-сообщений."""
    loop = asyncio.get_event_loop()
    
    server = await asyncio.start_server(
        lambda r, w: handle_gossip(r, w),
        host="0.0.0.0",
        port=GOSSIP_PORT
    )
    
    print(f"[{AGENT_NAME}] 📡 Gossip server listening on :{GOSSIP_PORT}")
    
    async with server:
        await server.serve_forever()


async def handle_gossip(reader, writer):
    """Обработка входящего gossip-сообщения."""
    peer = writer.get_extra_info('peername')
    try:
        data = await asyncio.wait_for(reader.readline(), timeout=10)
        msg = json.loads(data.decode())
        
        kind = msg.get("kind", 0)
        from_pk = msg.get("pubkey", "unknown")[:16]
        
        if kind == 39004:
            payload = msg.get("content", {}).get("payload", {})
            ptype = payload.get("type", "unknown")
            nonce = msg.get("content", {}).get("nonce", "")
            
            if ptype == "ping":
                # Отвечаем pong
                pong = json.dumps({
                    "kind": 39005,
                    "pubkey": AGENT_PUBKEY,
                    "content": {
                        "ack_for": nonce,
                        "status": "pong",
                        "ts": time.time()
                    }
                }).encode() + b"\n"
                writer.write(pong)
                await writer.drain()
                print(f"[{AGENT_NAME}] 📩 Ping от {from_pk} → Pong отправлен")
            else:
                # Обычное сообщение
                print(f"[{AGENT_NAME}] 📩 Сообщение от {from_pk}: {ptype}")
                # ACK
                ack = json.dumps({
                    "kind": 39005,
                    "pubkey": AGENT_PUBKEY,
                    "content": {"ack_for": nonce, "status": "ok"}
                }).encode() + b"\n"
                writer.write(ack)
                await writer.drain()
        elif kind == 39005:
            # ACK от другого агента
            pass
    except asyncio.TimeoutError:
        pass
    except Exception as e:
        print(f"[{AGENT_NAME}] ⚠️ Gossip handler error: {e}")
    finally:
        writer.close()


async def heartbeat():
    """Обновить last_seen в хабе."""
    try:
        data = json.dumps({
            "pubkey": AGENT_PUBKEY,
            "name": AGENT_NAME,
        }).encode()
        req = urllib.request.Request(
            f"{HUB_URL}/agents/gossip/heartbeat",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        urllib.request.urlopen(req, timeout=5)
    except:
        pass


async def tcp_ping(host: str, port: int) -> bool:
    """Прямой TCP ping."""
    try:
        r, w = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=5
        )
        msg = json.dumps({
            "kind": 39004,
            "pubkey": AGENT_PUBKEY,
            "created_at": int(time.time() * 1000),
            "content": {
                "target_pubkey": "self",
                "payload": {"type": "ping", "ts": time.time()},
                "nonce": f"ping:{int(time.time())}"
            }
        }).encode() + b"\n"
        w.write(msg)
        await asyncio.wait_for(w.drain(), timeout=3)
        try:
            resp = await asyncio.wait_for(r.readline(), timeout=3)
            w.close()
            return b"pong" in resp.lower() or b"ack" in resp.lower()
        except asyncio.TimeoutError:
            w.close()
            return True
    except:
        return False


def get_external_ip() -> str:
    """Определить внешний IP."""
    # Возвращаем 127.0.0.1 если не можем определить — хаб видит source IP
    try:
        import urllib.request as ur
        resp = ur.urlopen("https://snin-gossip.v2.site/agents/myip", timeout=5) if False else None
    except:
        pass
    return "0.0.0.0"  # хаб сам определит IP


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[{AGENT_NAME}] ⌨️ Прервано пользователем")

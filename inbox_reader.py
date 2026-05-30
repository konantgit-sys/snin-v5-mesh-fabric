#!/usr/bin/env python3
"""Inbox Reader — читает файлы из inbox/ и шлёт их как mesh-события.
Запускается как демон. Я (V2Bot) пишу файлы → inbox_reader шлёт агентам.

Использование:
  python3 inbox_reader.py                 # одноразово прочитать и обработать
  python3 inbox_reader.py --watch         # демон, следит за папкой
"""

import asyncio, json, os, sys, time, glob

INBOX_DIR = os.path.join(os.path.dirname(__file__), "inbox")
PROCESSED_DIR = os.path.join(INBOX_DIR, "processed")
MESH_API = "http://127.0.0.1:9907"

CRV2_HOST = "127.0.0.1"
CRV2_PORT = 9920

async def send_to_crv2(content: dict):
    """Отправить контент напрямую в Content Router V2 через TCP :9920.
    
    CRV2 принимает JSON-строки, делает dedup + quality,
    форвардит в Route Engine → Smart Router → агенты.
    """
    import hashlib
    
    # Формируем событие в формате Nostr
    event = {
        "id": hashlib.sha256(json.dumps(content, ensure_ascii=False).encode()).hexdigest()[:16],
        "kind": 39002,
        "pubkey": "inbox_reader",
        "content": json.dumps(content, ensure_ascii=False),
        "tags": [],
        "created_at": int(time.time()),
        "sig": "inbox_v2bot_injected"
    }
    
    try:
        r, w = await asyncio.wait_for(
            asyncio.open_connection(CRV2_HOST, CRV2_PORT), timeout=3
        )
        
        line = json.dumps(event, ensure_ascii=False) + "\n"
        w.write(line.encode())
        await asyncio.wait_for(w.drain(), timeout=5)
        
        w.close()
        try:
            await asyncio.wait_for(w.wait_closed(), timeout=2)
        except:
            pass
        
        print(f"[Inbox] ✅ Контент отправлен в CRV2 → RE → SR → агенты")
        print(f"[Inbox]   kind={event['kind']} id={event['id']} content={str(content)[:80]}...")
        return True
        
    except Exception as e:
        print(f"[Inbox] ❌ Ошибка отправки в CRV2: {type(e).__name__}: {e}")
        
        # Fallback: через Mesh API
        print(f"[Inbox] ⚠️ Fallback через Mesh API...")
        try:
            import urllib.request
            req = urllib.request.Request(
                f"{MESH_API}/agents/gossip",
                data=json.dumps({
                    "pubkey": "inbox_reader",
                    "name": "inbox_reader",
                    "meta": {"type": "content_provider", "event": event}
                }).encode(),
                headers={"Content-Type": "application/json"}
            )
            resp = urllib.request.urlopen(req, timeout=3)
            print(f"[Inbox] ✅ Fallback отправлен через Mesh API")
            return True
        except Exception as e2:
            print(f"[Inbox] ❌ Fallback тоже упал: {type(e2).__name__}: {e2}")
            return False

async def process_inbox():
    """Прочитать все файлы из inbox, отправить, переместить в processed."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    
    files = sorted(glob.glob(os.path.join(INBOX_DIR, "*.json")))
    files += sorted(glob.glob(os.path.join(INBOX_DIR, "*.txt")))
    files += sorted(glob.glob(os.path.join(INBOX_DIR, "*.md")))
    
    if not files:
        return False
    
    for filepath in files:
        filename = os.path.basename(filepath)
        print(f"[Inbox] 📄 Читаю: {filename}")
        
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                raw = f.read()
            
            # Если JSON — используем как есть, если текст — оборачиваем
            if filepath.endswith(".json"):
                content = json.loads(raw)
            else:
                content = {
                    "type": "post",
                    "text": raw.strip(),
                    "title": filename.replace(".", "_"),
                    "inbox": True
                }
            
            await send_to_crv2(content)
            
            # Перемещаем в processed
            dest = os.path.join(PROCESSED_DIR, filename)
            os.rename(filepath, dest)
            print(f"[Inbox] ✅ {filename} → processed")
            
        except Exception as e:
            print(f"[Inbox] ❌ {filename}: {e}")
    
    return True

async def watch_loop(interval=10):
    """Демон-режим: проверяет inbox каждые N секунд."""
    print(f"[Inbox] 👀 Демон запущен, проверка каждые {interval}с")
    print(f"[Inbox] 📂 Папка: {INBOX_DIR}")
    
    while True:
        try:
            processed = await process_inbox()
            if processed:
                print(f"[Inbox] 🔄 Пакет обработан")
        except Exception as e:
            print(f"[Inbox] ❌ Ошибка: {e}")
        
        await asyncio.sleep(interval)

if __name__ == "__main__":
    if "--watch" in sys.argv:
        asyncio.run(watch_loop())
    else:
        asyncio.run(process_inbox())

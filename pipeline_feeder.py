#!/usr/bin/env python3
"""Pipeline Feeder — генерирует трафик для тестирования пайплайна L7→L3.

Шлёт в SmartRouter (:9932):
  - kind:39002 (content) с метриками агента — каждые 10 сек
  - kind:30000 (payment) — каждые 60 сек
  - kind:39010 (DAO vote) — каждые 120 сек
"""

import asyncio
import json
import time
import hashlib
import random
import sys

SR_HOST = "127.0.0.1"
SR_PORT = 9932

AGENT_NAME = "pipeline_feeder"
AGENT_PUBKEY = "aabbccddee" + "ff" * 30  # 64 hex chars

async def send_to_sr(msg: dict) -> dict:
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
    except Exception as e:
        return {"ok": False, "error": str(e)}

async def send_content(seq: int):
    ts = int(time.time())
    msg = {
        "kind": 39002,
        "pubkey": AGENT_PUBKEY,
        "from": AGENT_NAME,
        "id": hashlib.sha256(f"{AGENT_NAME}:{seq}:{ts}".encode()).hexdigest()[:64],
        "content": json.dumps({
            "from": AGENT_NAME,
            "seq": seq,
            "ts": ts,
            "metrics": {
                "cpu": round(random.uniform(5, 80), 1),
                "mem": round(random.uniform(30, 90), 1),
                "uptime": seq * 10,
                "connections": random.randint(1, 10),
            },
            "state": "active",
            "message": f"pipeline test seq={seq}"
        }),
        "meta": {
            "priority": "normal",
            "traffic_class": "agent-to-agent",
            "channel": "auto"
        }
    }
    result = await send_to_sr(msg)
    return result.get("ok", False)

async def send_payment(seq: int):
    msg = {
        "kind": 30000,
        "pubkey": AGENT_PUBKEY,
        "from": AGENT_NAME,
        "id": hashlib.sha256(f"payment:{seq}".encode()).hexdigest()[:64],
        "content": json.dumps({
            "from": AGENT_NAME,
            "to": "another_agent",
            "amount": round(random.uniform(0.001, 0.1), 6),
            "token": "SNIN",
            "seq": seq
        }),
        "meta": {"priority": "high", "traffic_class": "payment"}
    }
    result = await send_to_sr(msg)
    return result.get("ok", False)

async def main():
    print(f"[PipelineFeeder] Starting — sending to SR :{SR_PORT}")
    seq = 0
    while True:
        seq += 1
        ok = await send_content(seq)
        status = "✅" if ok else "❌"
        print(f"[PipelineFeeder] #{seq} content → {status}")
        
        if seq % 6 == 0:  # every 60 sec
            ok = await send_payment(seq // 6)
            print(f"[PipelineFeeder] #{seq//6} payment → {'✅' if ok else '❌'}")
        
        await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())

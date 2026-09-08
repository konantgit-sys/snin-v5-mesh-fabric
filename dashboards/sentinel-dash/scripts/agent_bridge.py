#!/usr/bin/env python3
"""agent_bridge.py — мост агентов роя ↔ Proof Mesh (Sentinel).

Каждый подключённый агент (botperevod, urantia, remora, v2bot) получает
СВОЁ подписанное присутствие в hash-chain: события пишутся через
chain.append_signed с ЕГО nsec → signer_pub блока = pubhex агента
(мультиподписантность: в отличие от audit_daemon, который всё подписывает
ключом Cryter, здесь каждый агент доказывает свои действия СВОИМ ключом).

Команды:
  heartbeat  — pulse для всех агентов (дедуп 14 мин/агент, cron */5 безопасен)
  scan       — identity-скан sensor.scan (опознание процессов агентов)
  posts      — события 'post' для новых публикаций агентов (urantia и др.)

Ключи: /home/agent/data/.secure/mesh_agents.json (chmod 600).
Лог: /home/agent/data/backups/monitor/agent_bridge.log
"""
import json
import os
import sqlite3
import sys
import time

BASE = "/home/agent/data"
MESH = f"{BASE}/sites/relay-mesh"
PROOF = f"{MESH}/proof_mesh"
AUDIT_DB = f"{PROOF}/snin_audit.db"
AGENTS = f"{BASE}/.secure/mesh_agents.json"
LOG = f"{BASE}/backups/monitor/agent_bridge.log"
HEARTBEAT_MIN = 14  # мин — минимальный интервал heartbeat на агента

# агенты, у которых bridge пишет события (cryter уже обслуживает proof_cryter_bridge)
BRIDGED = ["botperevod", "urantia", "remora", "v2bot"]

sys.path.insert(0, MESH)
sys.path.insert(0, PROOF)


def log(msg: str):
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    with open(LOG, "a") as f:
        f.write(line + "\n")
    print(line)


def load_agents() -> dict:
    return json.load(open(AGENTS))


def last_event(agent_id: str, action: str) -> int | None:
    """ts последнего события агента (или None)."""
    con = sqlite3.connect(AUDIT_DB, timeout=15)
    row = con.execute(
        "SELECT MAX(ts) FROM audit_events WHERE agent_id=? AND action=?",
        (agent_id, action)).fetchone()
    con.close()
    return row[0] if row and row[0] else None


def heartbeat() -> dict:
    """Pulse всех агентов: подписанное daemon:heartbeat их ключами."""
    from proof_mesh import chain
    agents = load_agents()
    now = int(time.time())
    out = {}
    for aid in BRIDGED:
        a = agents.get(aid)
        if not a or not a.get("nsec"):
            out[aid] = "no-key"; continue
        last = last_event(aid, "daemon:heartbeat")
        if last and (now - last) < HEARTBEAT_MIN * 60:
            out[aid] = f"skip({(now-last)//60}м)"; continue
        try:
            bh, _u, _s = chain.append_signed(
                AUDIT_DB, nsec=a["nsec"], agent_id=aid,
                action="daemon:heartbeat",
                payload=json.dumps({"bridge": "agent_bridge", "ts": now}),
                attribution="confirmed", evidence_code="SIG_MATCH")
            out[aid] = f"ok {bh[:12]}..."
        except Exception as e:
            out[aid] = f"ERR {str(e)[:60]}"
    return out


def scan() -> dict:
    """Identity-скан: опознать процессы агентов (sensor.scan)."""
    from proof_mesh import sensor
    res = sensor.scan(AUDIT_DB)
    by = {}
    for r in res:
        by.setdefault(r["agent_id"], 0)
        by[r["agent_id"]] += 1
    return {"total": len(res), "by_agent": by,
            "unattributed": sum(1 for r in res if r["attribution"] == "unattributed")}


def posts() -> dict:
    """События 'post' для новых публикаций агентов (по их status/posts файлам)."""
    from proof_mesh import chain
    agents = load_agents()
    now = int(time.time())
    out = {}
    # Urantia Daily: urantia_posts.json — список публикаций
    up = f"{BASE}/projects/urantia-daily/urantia_posts.json"
    if os.path.exists(up) and agents.get("urantia", {}).get("nsec"):
        try:
            data = json.load(open(up))
            items = data if isinstance(data, list) else data.get("posts", [])
            last = last_event("urantia", "post") or 0
            new = [p for p in items if (p.get("ts") or 0) > last]
            n = 0
            for p in new[-3:]:
                bh, _u, _s = chain.append_signed(
                    AUDIT_DB, nsec=agents["urantia"]["nsec"], agent_id="urantia",
                    action="post",
                    payload=json.dumps({"title": (p.get("title") or "")[:120],
                                        "ts": p.get("ts")})[:400],
                    attribution="confirmed", evidence_code="SIG_MATCH")
                n += 1
            out["urantia"] = f"posts={n}"
        except Exception as e:
            out["urantia"] = f"ERR {str(e)[:80]}"
    return out


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "heartbeat"
    try:
        if cmd == "heartbeat":
            r = heartbeat()
            log(f"[heartbeat] {json.dumps(r, ensure_ascii=False)}")
            print(json.dumps(r, ensure_ascii=False))
        elif cmd == "scan":
            r = scan()
            log(f"[scan] {json.dumps(r, ensure_ascii=False)}")
            print(json.dumps(r, ensure_ascii=False))
        elif cmd == "posts":
            r = posts()
            log(f"[posts] {json.dumps(r, ensure_ascii=False)}")
            print(json.dumps(r, ensure_ascii=False))
        else:
            print("usage: agent_bridge.py [heartbeat|scan|posts]")
    except Exception as e:
        log(f"[ERROR] {cmd}: {e}")
        sys.exit(1)

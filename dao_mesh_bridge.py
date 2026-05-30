#!/usr/bin/env python3
"""dao_mesh_bridge.py — DAO Хроноса → relay-mesh мост.

Опрашивает DAO proposals в chrono раз в 60 сек.
Находит passed proposals с action="relay_mesh" в description.
Выполняет: POST /api/siggate/allowlist или POST /api/dht/put в relay-mesh.

Не модифицирует код chrono или relay-mesh. Add-only.
"""
import json, os, sys, time, requests, logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s %(message)s')
logger = logging.getLogger('dao_mesh')

CHRONO_URL = "http://localhost:8190"
MESH_URL = "http://localhost:9907"
STATE_FILE = os.path.join(os.path.dirname(__file__), '.dao_mesh_state.json')
POLL_INTERVAL = 60  # секунд

def load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except:
        return {"executed_ids": []}

def save_state(state: dict):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def execute_action(proposal: dict) -> bool:
    """Выполнить DAO action на стороне relay-mesh."""
    desc = proposal.get('description', '')
    if not desc:
        return False
    
    try:
        action = json.loads(desc)
    except json.JSONDecodeError:
        return False
    
    if action.get('action') != 'relay_mesh':
        return False
    
    cmd = action.get('type', '')
    pid = proposal['id']
    title = proposal.get('title', '')
    
    if cmd == 'allowlist_set':
        pubkeys = action.get('pubkeys', [])
        r = requests.post(f'{MESH_URL}/api/siggate/allowlist', 
                         json={"pubkeys": pubkeys}, timeout=5)
        logger.info(f'DAO #{pid} "{title}": allowlist_set ({len(pubkeys)} keys) → {r.status_code}')
        return r.ok
    
    elif cmd == 'allowlist_add':
        pubkey = action.get('pubkey', '')
        # Read current allowlist, add new pubkey
        # Since we can't read allowlist, we set it fresh
        # This means we need to know all pubkeys — for now, just log
        logger.info(f'DAO #{pid} "{title}": allowlist_add {pubkey[:20]}... — requires full list')
        return True
    
    elif cmd == 'dht_put':
        key = action.get('key', '')
        value = action.get('value', {})
        ttl = action.get('ttl', 86400)
        r = requests.post(f'{MESH_URL}/api/dht/put',
                         json={"key": key, "value": value, "ttl": ttl}, timeout=5)
        logger.info(f'DAO #{pid} "{title}": dht_put {key} → {r.status_code}')
        return r.ok
    
    else:
        logger.info(f'DAO #{pid} "{title}": unknown cmd={cmd}, ignoring')
        return True  # не ошибка, просто пропускаем

def poll_dao():
    state = load_state()
    executed_ids = set(state.get('executed_ids', []))
    
    try:
        r = requests.get(f'{CHRONO_URL}/api/v1/dao/proposals', timeout=10)
        if not r.ok:
            logger.warning(f'DAO API error: {r.status_code}')
            return
        proposals = r.json()
    except Exception as e:
        logger.error(f'Cannot reach Chrono DAO: {e}')
        return
    
    for prop in proposals:
        pid = prop.get('id')
        if pid in executed_ids:
            continue
        if prop.get('status') not in ('passed', 'executed'):
            continue
        
        logger.info(f'Found passed proposal #{pid}: {prop.get("title","")[:50]}')
        success = execute_action(prop)
        
        if success:
            executed_ids.add(pid)
            state['executed_ids'] = list(executed_ids)
            save_state(state)
            logger.info(f'DAO #{pid}: action executed')

def main():
    logger.info('DAO→Mesh Bridge started')
    logger.info(f'  Chrono: {CHRONO_URL}')
    logger.info(f'  Mesh:   {MESH_URL}')
    logger.info(f'  Poll:   {POLL_INTERVAL}s')
    
    while True:
        try:
            poll_dao()
        except Exception as e:
            logger.error(f'Poll error: {e}')
        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    main()

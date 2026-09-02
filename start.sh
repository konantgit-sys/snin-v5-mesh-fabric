#!/bin/bash
# SNIN V5 Mesh Fabric — авто-запуск при рестарте пода
# 6 процессов, ~250 MB RAM
set -e
export PYTHONPATH=/home/agent/data/.local_lib:${PYTHONPATH}

MESH_DIR="/home/agent/data/sites/relay-mesh"
LOG_DIR="$MESH_DIR/logs"
mkdir -p "$LOG_DIR"

# 1. Redis (если упал)
redis-cli ping 2>/dev/null && echo "[mesh] redis ok" || { redis-server --daemonize yes --port 6379 2>/dev/null || echo "[mesh] redis нет — graph fallback"; }

# 2. ContentRouter v2 :9920 (должен быть первым — SmartRouter шлёт в него)
cd "$MESH_DIR"
nohup python3 content_router_v2.py 9920 >> "$LOG_DIR/cr_v2.log" 2>&1 &
echo "[mesh] ContentRouter PID=$!"

# 3. RouteEngine :9910
nohup python3 route_engine.py >> "$LOG_DIR/route_engine.log" 2>&1 &
echo "[mesh] RouteEngine PID=$!"

# 4. SmartRouter :9932 (сердце)
sleep 1
export SNIN_USE_ZMQ=1
nohup python3 smart_router.py >> "$LOG_DIR/smart_router.log" 2>&1 &
echo "[mesh] SmartRouter PID=$!"

# 5. ExternalGateway :9931 (вход для внешних устройств)
sleep 1
nohup python3 external_gateway.py >> "$LOG_DIR/external_gateway.log" 2>&1 &
echo "[mesh] ExternalGateway PID=$!"

# 6. NostrBridge :9941 (публикация во внешние релеи)
sleep 1
nohup python3 nostr_bridge.py --shard-id 0 --total-shards 1 >> "$LOG_DIR/nostr_bridge.log" 2>&1 &
echo "[mesh] NostrBridge PID=$!"

# 7. CrossMeshBridge :9946 (mesh-федерация)
sleep 1
nohup python3 cross_mesh_bridge.py 9946 >> "$LOG_DIR/cross_mesh.log" 2>&1 &
echo "[mesh] CrossMeshBridge PID=$!"

sleep 3
echo "[mesh] ✅ Fabric launched: $(ps aux | grep -E 'content_router_v2|route_engine|smart_router|external_gateway|nostr_bridge|cross_mesh' | grep -v grep | wc -l)/6 processes"

# 8. Статус-страница :8085
nohup python3 /home/agent/data/sites/relay-mesh/mesh_status.py >> "$LOG_DIR/mesh_status.log" 2>&1 &
echo "[mesh] StatusPage PID=$!"

# ── NATS Server ──
if ! pgrep -x nats-server > /dev/null; then
    nohup nats-server -p 4222 -js > /tmp/nats.log 2>&1 &
    echo "[mesh] NATS server started"
fi

# SPM Ф6: демон аудита (сбор событий → цепочка, сертификаты, snapshot)
if pgrep -f "proof_mesh/audit_daemon" > /dev/null; then
  echo "[mesh] AuditDaemon уже запущен — пропуск (защита от дублей)"
else
  cd "$MESH_DIR" && nohup python3 proof_mesh/audit_daemon.py >> "$MESH_DIR/logs/audit_daemon.log" 2>&1 &
  echo "[mesh] AuditDaemon PID=$!"
fi

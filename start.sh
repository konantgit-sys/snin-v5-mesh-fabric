#!/bin/bash
# SNIN V5 Mesh Fabric — авто-запуск при рестарте пода (идемпотентный)
# Каждый сервис стартует ТОЛЬКО если его порт мёртв (безопасно перезапускать)
set -e
export PYTHONPATH=/home/agent/data/.local_lib:${PYTHONPATH}

MESH_DIR="/home/agent/data/sites/relay-mesh"
LOG_DIR="$MESH_DIR/logs"
mkdir -p "$LOG_DIR"

port_up() { python3 -c "
import socket
s=socket.socket(); s.settimeout(0.7)
try: s.connect(('127.0.0.1',$1)); print('up')
except: print('down')
s.close()" 2>/dev/null | grep -q up; }

# 1. Redis (если упал)
redis-cli ping 2>/dev/null && echo "[mesh] redis ok" || { redis-server --daemonize yes --port 6379 2>/dev/null || echo "[mesh] redis нет — graph fallback"; }

cd "$MESH_DIR"

# 2. ContentRouter v2 :9920 (должен быть первым — SmartRouter шлёт в него)
if port_up 9920; then echo "[mesh] ContentRouter уже жив :9920"; else
  nohup python3 content_router_v2.py 9920 >> "$LOG_DIR/cr_v2.log" 2>&1 &
  echo "[mesh] ContentRouter PID=$!"; fi

# 3. RouteEngine :9910
if port_up 9910; then echo "[mesh] RouteEngine уже жив :9910"; else
  nohup python3 route_engine.py >> "$LOG_DIR/route_engine.log" 2>&1 &
  echo "[mesh] RouteEngine PID=$!"; fi

# 4. SmartRouter :9932 (сердце)
sleep 1
if port_up 9932; then echo "[mesh] SmartRouter уже жив :9932"; else
  export SNIN_USE_ZMQ=1
  nohup python3 smart_router.py >> "$LOG_DIR/smart_router.log" 2>&1 &
  echo "[mesh] SmartRouter PID=$!"; fi

# 5. ExternalGateway :9931
sleep 1
if port_up 9931; then echo "[mesh] ExternalGateway уже жив :9931"; else
  nohup python3 external_gateway.py >> "$LOG_DIR/external_gateway.log" 2>&1 &
  echo "[mesh] ExternalGateway PID=$!"; fi

# 6. NostrBridge шарды 0-4 :9941-9945 (согласованно total-shards 5)
sleep 1
for i in 0 1 2 3 4; do
  p=$((9941+i))
  if port_up $p; then echo "[mesh] NostrBridge-$i уже жив :$p"; else
    nohup python3 nostr_bridge.py --shard-id $i --total-shards 5 >> "$LOG_DIR/nostr_bridge_shard${i}.log" 2>&1 &
    echo "[mesh] NostrBridge-$i PID=$! :$p"; fi
done

# 7. CrossMeshBridge :9946 (mesh-федерация)
sleep 1
if port_up 9946; then echo "[mesh] CrossMeshBridge уже жив :9946"; else
  nohup python3 cross_mesh_bridge.py 9946 >> "$LOG_DIR/cross_mesh.log" 2>&1 &
  echo "[mesh] CrossMeshBridge PID=$!"; fi

# 8. Статус-страница :8085
if port_up 8085; then echo "[mesh] StatusPage уже жив :8085"; else
  nohup python3 /home/agent/data/sites/relay-mesh/mesh_status.py >> "$LOG_DIR/mesh_status.log" 2>&1 &
  echo "[mesh] StatusPage PID=$!"; fi

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

# ── Mesh Supervisor :9909 (мониторинг + автоподъём, стандарт реестра) ──
cd "$MESH_DIR"
if port_up 9909; then echo "[mesh] Supervisor уже жив :9909"; else
  nohup python3 mesh_supervisor.py >> "$LOG_DIR/mesh_supervisor.log" 2>&1 &
  echo "[mesh] MeshSupervisor PID=$! :9909"; fi

echo "[mesh] ✅ Fabric launched"

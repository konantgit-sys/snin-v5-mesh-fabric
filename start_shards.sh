#!/bin/bash
# Bridge Sharding Launcher — запускает 5 bridge шардов + остальной mesh
cd /home/agent/data/sites/relay-mesh
ulimit -n 65535

echo "=== Relay Mesh — Bridge Sharding ==="
echo ""

# pkill отключён — supervisor управляет всем
# 1. Route Engine (:9910) — если не запущен
if ! ss -tlnp 2>/dev/null | grep -q ":9910"; then
    echo "[1/5] Route Engine..."
    nohup python3 -u route_engine.py > logs/route_engine.log 2>&1 &
    echo "  RE=$!"; sleep 2
else
    echo "[1/5] Route Engine ✅ already running"
fi

# 2. Content Router V2 (:9920)
if ! ss -tlnp 2>/dev/null | grep -q ":9920"; then
    echo "[2/5] Content Router V2..."
    nohup python3 -u content_router_v2.py 9920 > logs/content_router.log 2>&1 &
    echo "  CRV2=$!"; sleep 2
else
    echo "[2/5] Content Router V2 ✅ already running"
fi

# 3. Smart Router (:9932)
if ! ss -tlnp 2>/dev/null | grep -q ":9932"; then
    echo "[3/5] Smart Router..."
    nohup python3 -u smart_router.py > logs/smart_router.log 2>&1 &
    echo "  SR=$!"; sleep 3
else
    echo "[3/5] Smart Router ✅ already running"
    # Перезапускаем чтобы подхватил новые nostr шарды
    echo "  → Restarting SR to pick up nostr shards..."
    kill -9 $(ss -tlnp 2>/dev/null | grep ":9932" | grep -oP 'pid=\K[0-9]+') 2>/dev/null
    sleep 2
    nohup python3 -u smart_router.py > logs/smart_router.log 2>&1 &
    echo "  SR=$!"; sleep 4
fi

# 4. Bridge Shards — supervisor управляет. Fallback на случай если supervisor не стартанул
echo "[4/5] Bridge shards..."
echo "  → Supervisor manages nostr_bridge_0..4 on :9941-9945"
for i in 0 1 2 3 4; do
    port=$((9941 + i))
    if ss -tlnp 2>/dev/null | grep -q ":$port "; then
        echo "  ✅ :$port — already running (supervisor)"
    else
        echo "  ⚠️ :$port — empty, launching fallback..."
        nohup python3 -u nostr_bridge.py --shard-id $i --total-shards 5 > logs/nostr_bridge_shard${i}.log 2>&1 &
        echo "  → Fallback shard-$i PID=$!"
        sleep 3
    fi
done

# 5. Watchdog — отключён. Supervisor управляет всем.

echo ""
echo "=== All services started ==="
echo "Ports: RE=9910 CRV2=9920 SR=9932 Bridge=9941-9945"
echo "Shards: 5 bridge instances (5 scan + 1 write relay each)"
echo ""
# Итоговая проверка
echo "=== Health check ==="
for port in 9910 9920 9932 9941 9942 9943 9944 9945; do
    if ss -tlnp 2>/dev/null | grep -q ":$port "; then
        echo "  Port $port ✅"
    fi
done

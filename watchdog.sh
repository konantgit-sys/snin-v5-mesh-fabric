#!/bin/bash
# Watchdog V4 — с grace period при первом старте
# первые 90 секунд не проверяет, чтобы start.sh успел поднять всё

cd /home/agent/data/sites/relay-mesh
PIDDIR="/tmp/relay-mesh-pids"
mkdir -p "$PIDDIR"

safe_start() {
    local name="$1"
    local pidfile="$PIDDIR/$name.pid"
    local cmd="$2"
    local log="$3"
    local port="$4"
    local max_allowed="${5:-1}"

    # Порт занят? — всё ок
    if [ -n "$port" ]; then
        if ss -tlnp 2>/dev/null | grep -qE ":$port\b"; then return; fi
    fi

    # Считаем процессы с этим именем (для nostr_bridge: максимум 5)
    local count
    count=$(pgrep -cf "$name" 2>/dev/null)
    count=${count:-0}
    if [ "$count" -ge "$max_allowed" ] 2>/dev/null; then return; fi

    # PID-файл есть и процесс жив? — ок
    if [ -f "$pidfile" ]; then
        if kill -0 "$(cat "$pidfile")" 2>/dev/null; then return; fi
    fi

    # ═══ SmartRouter: принудительная зачистка портов перед стартом ═══
    if [ "$name" = "smart_router" ]; then
        fuser -k 9932/tcp 2>/dev/null
        fuser -k 9933/tcp 2>/dev/null  # Health port
        fuser -k 9934/udp 2>/dev/null  # DHT
        # Трёп зомби процессов
        for zpid in $(pgrep -f "smart_router.py"); do kill -9 $zpid 2>/dev/null; done
        sleep 2
    fi

    # ═══ ExternalGateway: зачистка порта ═══
    if [ "$name" = "ext_gateway" ]; then
        fuser -k 9931/tcp 2>/dev/null
        sleep 1
    fi

    # Запуск
    nohup $cmd >> "$log" 2>&1 &
    local new_pid=$!
    echo "$new_pid" > "$pidfile"
    echo "[$(date +%H:%M:%S)] $name started (PID=$new_pid)" >> logs/watchdog.log
    
    # ═══ SmartRouter: проверка что реально поднялся ═══
    if [ "$name" = "smart_router" ]; then
        for i in $(seq 1 10); do
            sleep 1
            if ss -tlnp 2>/dev/null | grep -q ":9932 "; then
                echo "[$(date +%H:%M:%S)] ✅ SmartRouter открыл :9932 (${i}с)" >> logs/watchdog.log
                return
            fi
            if ! kill -0 $new_pid 2>/dev/null; then
                echo "[$(date +%H:%M:%S)] ❌ SmartRouter умер при старте — зачищаю порты" >> logs/watchdog.log
                fuser -k 9932/tcp 2>/dev/null
                fuser -k 9934/udp 2>/dev/null
                sleep 2
                return  # вернёмся на след. итерации watchdog
            fi
        done
        echo "[$(date +%H:%M:%S)] ⚠️ SmartRouter не открыл :9932 за 10с" >> logs/watchdog.log
    fi
}

# Grace period: 90 сек на запуск start.sh
sleep 90

while true; do
    safe_start "smart_router" "python3 -u smart_router.py" "logs/smart_router.log" "9932"; sleep 1
    safe_start "content_router" "python3 -u content_router_v2.py 9920" "logs/content_router.log" "9920"; sleep 1
    safe_start "route_engine" "python3 -u route_engine.py" "logs/route_engine.log" "9910"; sleep 1
    safe_start "cross_mesh" "python3 -u cross_mesh_bridge.py 9946" "logs/cross_mesh_bridge.log" "9946"; sleep 1
    safe_start "identity_api" "python3 -u identity_api_v2.py 9940" "logs/identity_api.log" "9940"; sleep 1
    safe_start "ext_gateway" "python3 -u external_gateway.py" "logs/external_gateway.log" ""; sleep 1

    # nostr_bridge — не больше 5 штук суммарно
    nb_count=$(pgrep -cf "nostr_bridge" 2>/dev/null || echo 0)
    if [ "$nb_count" -lt 5 ]; then
        for i in 0 1 2 3 4; do
            port=$((9941 + i))
            safe_start "nostr_bridge_$i" "python3 -u nostr_bridge.py --shard-id $i --total-shards 5" "logs/nostr_bridge_shard${i}.log" "$port" "1"; sleep 1
        done
    fi

    sleep 60
done

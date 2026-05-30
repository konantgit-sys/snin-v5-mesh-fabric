#!/bin/bash
cd /home/agent/data/sites/relay-mesh
ulimit -n 65535

# Проверка зависимостей — orjson + kademlia
python3 -c "import orjson" 2>/dev/null || pip3 install orjson --break-system-packages -q
python3 -c "import kademlia" 2>/dev/null || pip3 install kademlia --break-system-packages -q

# ═══ ОЖИДАНИЕ REDIS ═══
echo "[start.sh] ⏳ Ожидание Redis..."
for i in $(seq 1 30); do
    if redis-cli ping 2>/dev/null | grep -q "PONG"; then
        echo "[start.sh] ✅ Redis готов (попытка $i)"
        break
    fi
    sleep 1
done
if ! redis-cli ping 2>/dev/null | grep -q "PONG"; then
    echo "[start.sh] ❌ Redis не отвечает через 30с — запускаю..."
    redis-server --daemonize yes 2>/dev/null
    sleep 2
fi

# Убиваем ВСЕ старые процессы relay-mesh перед стартом
pkill -f "route_engine.py" 2>/dev/null
pkill -f "content_router_v2.py" 2>/dev/null  
pkill -f "smart_router.py" 2>/dev/null
pkill -f "cross_mesh_bridge.py" 2>/dev/null
pkill -f "nostr_bridge.py" 2>/dev/null
pkill -f "identity_api_v2.py" 2>/dev/null
pkill -f "external_gateway.py" 2>/dev/null

# ═══ ПРИНУДИТЕЛЬНОЕ ОСВОБОЖДЕНИЕ ПОРТОВ ═══
for port in 9932 9931 9920 9910 9946 9940 9934 9106 9100 9101 9102 9103 9104; do
    fuser -k "${port}/tcp" 2>/dev/null
done
fuser -k "${port}/udp" 2>/dev/null  # DHT on 9934 UDP
sleep 3

# ═══ ЧИСТКА DHT-КЕША REDIS ═══
redis-cli del "dht:peers" "dht:agents" "dht:nodes" "dht:bootstrap" 2>/dev/null
echo "[start.sh] ✅ DHT кеш очищен"

# 1. Route Engine (:9910)
nohup python3 -u route_engine.py > logs/route_engine.log 2>&1 &
echo "RE=$!"; sleep 2

# 2. Content Router V2 (:9920) — dedup + fwd
nohup python3 -u content_router_v2.py 9920 > logs/content_router.log 2>&1 &
echo "CRV2=$!"; sleep 2

# 3. Smart Router (:9932) — мозг сети
# Запуск с контролем — если упал, рестарт (макс 3 попытки)
for attempt in 1 2 3; do
    > logs/smart_router.log
    nohup python3 -u smart_router.py > logs/smart_router.log 2>&1 &
    SR_PID=$!
    echo "SR=$SR_PID (попытка $attempt)"
    
    # Ждём открытия порта :9932 (макс 15 сек)
    for wait_sec in $(seq 1 15); do
        sleep 1
        if ss -tlnp 2>/dev/null | grep -q ":9932 "; then
            echo "[start.sh] ✅ SmartRouter поднялся на :9932 за ${wait_sec}с"
            break 2  # выходим из обоих циклов — всё ок
        fi
        # Проверка жив ли процесс
        if ! kill -0 $SR_PID 2>/dev/null; then
            echo "[start.sh] ⚠️ SmartRouter упал на попытке $attempt (через ${wait_sec}с)"
            # Смотрим ошибку
            tail -3 logs/smart_router.log 2>/dev/null | head -1 | sed 's/^/    error: /'
            sleep 2
            break
        fi
    done
done

# 4. Identity API v2 (:9940) — L5 Identity & Reputation
nohup python3 -u identity_api_v2.py 9940 > logs/identity_api.log 2>&1 &
echo "Identity=$!"; sleep 2

# 4a. Cross-Mesh Bridge (:9946) — L1.5 Federation Protocol
nohup python3 -u cross_mesh_bridge.py 9946 > logs/cross_mesh_bridge.log 2>&1 &
echo "CrossMesh=$!"; sleep 2

# 5. Nostr Bridges ×5 (gossip shard replacement — connected to SR)
for i in 0 1 2 3 4; do
  nohup python3 -u nostr_bridge.py --shard-id $i --total-shards 5 > logs/nostr_bridge_shard${i}.log 2>&1 &
  echo "NB[$i]=$!"; sleep 2
done

# 6. External Gateway (:9931) — вход в Nostr сеть
# Ждём освобождения порта если старый процесс не успел умереть
for i in 1 2 3 4 5; do
    ss -tlnp 2>/dev/null | grep -q ":9931 " || break
    sleep 1
done
nohup python3 -u external_gateway.py > logs/external_gateway.log 2>&1 &
echo "EG=$!"; sleep 2

# 7. Nostr Relay (:8198) — Nostr relay server
cd /home/agent/data/sites/relay
SOFTMAX_TOP_N=20 python3 relay_server_v2.py > /tmp/relay_v2_console.log 2>&1 &
echo "Relay=$!"
cd /home/agent/data/sites/relay-mesh

# 8. Watchdog — авто-восстановление при падениях
pkill -f "watchdog.sh" 2>/dev/null
nohup bash watchdog.sh > logs/watchdog.log 2>&1 &
echo "Watchdog=$!"

# 9. Log rotator — режет логи >1MB раз в час, хранит последние 2 часа
pkill -f "relay_mesh_log_rotator" 2>/dev/null
sleep 1
nohup python3 -u -c '
# relay_mesh_log_rotator
import os, time, re, glob

LOG_DIR = "/home/agent/data/sites/relay-mesh/logs"
CUTOFF_SECONDS = 7200  # 2 часа
MAX_SIZE = 1048576     # 1MB
KEEP_LINES = 5000      # если не смогли распарсить время — храним хотя бы 5000 строк

def truncate_by_time(path):
    try:
        with open(path, "rb") as f:
            data = f.read()
        lines = data.split(b"\n")
        if len(lines) <= KEEP_LINES:
            return False  # не трогаем маленькие файлы
        
        now = time.time()
        # Пробуем распарсить дату в первых 40 байтах каждой строки
        keep = []
        kept = 0
        for line in lines:
            if not line:
                keep.append(line)
                continue
            # Ищем паттерн YYYY-MM-DD HH:MM:SS
            m = re.search(rb"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line[:40])
            if m:
                try:
                    ts = time.mktime(time.strptime(m.group(1).decode(), "%Y-%m-%d %H:%M:%S"))
                    if now - ts < CUTOFF_SECONDS:
                        keep.append(line)
                        kept += 1
                    continue
                except:
                    pass
            # Если нет даты — сохраняем если рядом есть сохранённые
            if kept > 0:
                keep.append(line)
            else:
                keep.append(line)  # всё равно сохраняем если нет даты
        
        if len(keep) < len(lines) * 0.5:  # урезали как минимум вдвое
            new_data = b"\n".join(keep)
            with open(path, "wb") as f:
                f.write(new_data)
            return True
        return False
    except:
        return False

while True:
    time.sleep(1800)  # каждые 30 минут
    for fpath in glob.glob(os.path.join(LOG_DIR, "*.log")):
        try:
            sz = os.path.getsize(fpath)
            if sz > MAX_SIZE:
                truncated = truncate_by_time(fpath)
                if truncated:
                    print(f"[Rotator] ✂️ {os.path.basename(fpath)}: {sz//1024}KB → {os.path.getsize(fpath)//1024}KB (2h)")
        except:
            pass
' > logs/rotator.log 2>&1 &
echo "Rotator=$!"

echo ""
echo "=== Relay Mesh V2 — All Services Started ==="
echo "Ports: RE=9910 CRV2=9920 SR=9932 EG=9931 NB=9941-9945 Relay=8198"

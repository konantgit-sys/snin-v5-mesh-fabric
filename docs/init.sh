#!/bin/bash
# System init — запускается при каждом старте пода
# v3.1 — 2026-06-25: persistent pip lib + PYTHONPATH
set -e

echo "[init] $(date) — старт"

# ── PYTHONPATH: персистентная lib + проект ──
export PYTHONPATH="/home/agent/data/.local_lib:/usr/lib/python3/dist-packages:$PYTHONPATH"
echo "[init] $(date) — PYTHONPATH=$PYTHONPATH"

# ── 0. Установка в persistent lib (если под новый — докачает) ──
echo "[init] $(date) — pip в persistent lib"
pip3 install --break-system-packages --cache-dir /home/agent/data/.pip_cache --target /home/agent/data/.local_lib -q \
    nostr-sdk python-telegram-bot websocket-client requests \
    sentence-transformers pandas matplotlib mplfinance \
    aiohttp numpy deep-translator websockets pyyaml \
    orjson cryptography bech32 pycryptodome langdetect \
    2>&1 | tail -3

# ── 1. Cryter Agent (9 модулей) ──
echo "[init] $(date) — запуск Cryter"
if [ -f /home/agent/data/agents/core/cryter/launch_all.sh ]; then
    bash /home/agent/data/agents/core/cryter/launch_all.sh &
    echo "[init] $(date) — Cryter: launch_all.sh запущен"
else
    echo "[init] $(date) — ⚠️ Cryter launch_all.sh не найден"
fi

# ── 2. V2Bot Agent Daemon ──
echo "[init] $(date) — запуск V2Bot Daemon"
cd /home/agent/data/sites/v2bot-daemon
nohup python3 v2bot_daemon_v2.py >> logs/daemon.log 2>&1 &
echo "[init] $(date) — V2Bot PID: $!"

echo "[init] $(date) — ✅ ГОТОВО"
# === SNIN Pulse Sync + Redis ===
redis-server --daemonize yes --port 6379 2>/dev/null
sleep 1
cd /home/agent/data/sites/relay-mesh
nohup python3 -u pulse_sync.py > /home/agent/data/logs/pulse_stdout.log 2>&1 &

# NeuroTrust field measurements cron (Phase 3)
(crontab -l 2>/dev/null; echo '5 * * * * cd /home/agent/data/agents/core/cryter && python3 src/core/social_field.py >> /home/agent/data/agents/core/cryter/data/field_measurements.log 2>&1') | crontab -

#!/bin/bash
cd /home/agent/data/sites/relay
export PYTHONPATH=/home/agent/data/projects/p2p-agent-mesh:$PYTHONPATH

pip3 install --break-system-packages -q pynostr orjson 2>/dev/null

# Relay backend (Nostr) — порт 8198
nohup python3 relay_server_v2.py >> /tmp/relay_v2_console.log 2>&1 &
echo "Relay PID=$!"

# Frontend dashboard — порт 8199 (прокси API к 8198)
nohup python3 frontend.py >> /tmp/frontend.log 2>&1 &
echo "Frontend PID=$!"

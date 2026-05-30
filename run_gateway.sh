#!/bin/bash
cd /home/agent/data/sites/relay-mesh
while true; do
    python3 -u external_gateway.py >> logs/external_gateway.log 2>&1
    echo "[GATEWAY] CRASHED at $(date -Iseconds), restarting in 5s..." >> logs/external_gateway.log
    sleep 5
done

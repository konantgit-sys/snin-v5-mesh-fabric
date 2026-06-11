#!/bin/bash
# SNIN Mesh Health API — auto-restart script
# Called by system on pod restart and every 60s health check

cd /home/agent/data/sites/relay-mesh

# Kill any existing process on port 8085
OLD_PID=$(lsof -ti:8085 2>/dev/null)
if [ -n "$OLD_PID" ]; then
    kill -9 $OLD_PID 2>/dev/null
    sleep 1
fi

# Start health API
exec python3 health_api.py >> logs/health_api_stdout.log 2>&1

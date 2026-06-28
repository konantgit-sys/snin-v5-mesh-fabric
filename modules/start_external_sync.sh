#!/bin/bash
cd /home/agent/data/sites/relay-mesh
pkill -f "external_sync.py" 2>/dev/null
sleep 2
python3 -u external_sync.py --interval 600 > /tmp/external_sync_daemon.log 2>&1 &
echo "External Sync Phase4 started PID=$!" >> /tmp/external_sync_daemon.log

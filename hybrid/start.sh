#!/bin/bash
# HCOOR + P2P Cluster — автостарт после перезагрузки пода
cd /home/agent/data/sites/relay-mesh

# 1. Coordinator
nohup python3 -u -m hybrid.hcoor --port 9970 > /tmp/hcoor.log 2>&1 &
sleep 2

# 2. P2P Agent Cluster (3 агента: Cryter, Forecaster, Archivist)
nohup python3 -u hybrid/hcoor_cluster.py > /tmp/hcoor_cluster.log 2>&1 &

echo "HCOOR + P2P Cluster started"

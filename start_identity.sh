#!/bin/bash
cd /home/agent/data/sites/relay-mesh
nohup python3 identity_api.py 9940 >> /home/agent/data/sites/relay-mesh/logs/identity_api.log 2>&1 &
echo "PID=$!"

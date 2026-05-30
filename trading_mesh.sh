#!/bin/bash
# L12 Trading Signal Mesh (:9670)
cd /home/agent/data/sites/relay-mesh && python3 -u trading_mesh.py >> logs/trading.log 2>&1 &

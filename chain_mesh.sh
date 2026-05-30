#!/bin/bash
# L15 Supply Chain Audit (:9720)
cd /home/agent/data/sites/relay-mesh && python3 -u chain_mesh.py >> logs/chain.log 2>&1 &

#!/bin/bash
# L13 DeFi Oracle Mesh (:9680)
cd /home/agent/data/sites/relay-mesh && python3 -u defi_mesh.py >> logs/defi.log 2>&1 &

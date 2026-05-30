#!/bin/bash
# L16 Energy Grid Mesh (:9710)
cd /home/agent/data/sites/relay-mesh && python3 -u energy_mesh.py >> logs/energy.log 2>&1 &

#!/bin/bash
cd /home/agent/data/sites/snin-hub
exec python3 l3_mesh_core.py 9300 >> /home/agent/data/logs/l3.log 2>&1

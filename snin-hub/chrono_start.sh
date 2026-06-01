#!/bin/bash
cd /home/agent/data/sites/snin-hub
exec python3 mesh_chrono.py 8190 >> /home/agent/data/logs/chrono.log 2>&1

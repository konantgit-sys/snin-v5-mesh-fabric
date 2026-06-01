#!/bin/bash
cd /home/agent/data/sites/snin-hub
nohup python3 -u hub_fastapi.py >> /home/agent/data/logs/hub_fastapi.log 2>&1 &

#!/bin/bash
cd /home/agent/data/sites/snin-hub
exec python3 l9_orchestration.py 9900 >> /home/agent/data/logs/l9.log 2>&1

#!/bin/bash
cd /home/agent/data/sites/relay-mesh
exec python3 -u external_gateway.py 9931 >> logs/external_gateway.log 2>&1

#!/bin/bash
# L11 Smart City Mesh (:9660)
cd /home/agent/data/sites/relay-mesh && python3 -u city_mesh.py >> logs/city.log 2>&1 &

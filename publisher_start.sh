#!/bin/bash
# V4 Phase 1 — NIP-65 Publisher + Relay Monitor
cd /home/agent/data/sites/relay-mesh

# NIP-65 Publisher (публикует kind:10002 каждые 6ч)
python3 -u nip65_publisher.py >> logs/nip65_publisher.log 2>&1 &

# Relay Monitor (проверяет 85+ релеев каждые 10 мин)
python3 -u relay_monitor.py >> logs/relay_monitor_daemon.log 2>&1 &

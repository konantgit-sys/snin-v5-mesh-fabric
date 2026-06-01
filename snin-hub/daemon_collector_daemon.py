#!/usr/bin/env python3
"""Фоновый сбор метрик демонов — пишет ram_history.json каждые 2 минуты."""
import json, os, time, sys
sys.path.insert(0, os.path.dirname(__file__))
from daemon_collector import collect_processes

HISTORY_PATH = os.path.join(os.path.dirname(__file__), "ram_history.json")
INTERVAL = 120  # 2 минуты

def collect():
    data = collect_processes()
    total_ram = data.get("total_ram_mb", 0)
    now = int(time.time())
    
    history = []
    if os.path.exists(HISTORY_PATH):
        with open(HISTORY_PATH) as f:
            history = json.load(f)
    
    history.append({"t": now, "ram": total_ram})
    if len(history) > 120:  # 4 часа по 2 мин
        history = history[-120:]
    
    with open(HISTORY_PATH, "w") as f:
        json.dump(history, f)
    
    print(f"[{time.strftime('%H:%M:%S')}] RAM: {total_ram:.0f} MB | points: {len(history)}")

if __name__ == "__main__":
    print(f"[DAEMON COLLECTOR] Starting, interval={INTERVAL}s")
    while True:
        try:
            collect()
        except Exception as e:
            print(f"[ERROR] {e}")
        time.sleep(INTERVAL)

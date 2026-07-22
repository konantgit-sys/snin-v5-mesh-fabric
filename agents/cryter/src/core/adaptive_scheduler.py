"""
Dynamic Block Intervals — Adaptive Chrono Engine (#6)

Adjusts Chrono block production rate based on network load:
  - High activity (many events/sec) → shorter block intervals (faster blocks)
  - Low activity (idle network) → longer block intervals (save resources)

Algorithm: EMA-based event rate with hysteresis thresholds.
  - BASE_INTERVAL = 60s (default block time)
  - MIN_INTERVAL = 10s (at high load: >50 events/sec)
  - MAX_INTERVAL = 300s (at idle: <1 event/sec)
  - Smooth exponential mapping between event rate and interval
"""

import json
import time
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger("adaptive_chrono")

# ─── Configuration ───
BASE_INTERVAL = 60.0       # default block interval (seconds)
MIN_INTERVAL = 10.0        # minimum block interval at peak load
MAX_INTERVAL = 300.0       # maximum block interval when idle
EMA_ALPHA = 0.3            # smoothing factor (0.1=slow, 0.5=fast)
HIGH_LOAD_THRESHOLD = 50   # events/sec — above this, go to MIN
LOW_LOAD_THRESHOLD = 1     # events/sec — below this, go to MAX
HYSTERESIS_MARGIN = 0.15   # 15% margin to avoid oscillation
WINDOW_SECONDS = 60        # rolling window for event rate calculation

# State persistence
STATE_PATH = Path.home() / "data" / "adaptive_chrono_state.json"
DB_PATH = Path.home() / "data" / "sites" / "chrono" / "chrono.db"


class AdaptiveBlockScheduler:
    """Dynamically adjusts block interval based on network event rate."""

    def __init__(self, db_path=DB_PATH, state_path=STATE_PATH):
        self.db_path = db_path
        self.state_path = state_path
        self.ema_rate = 0.0           # exponential moving average of events/sec
        self.current_interval = BASE_INTERVAL
        self.last_adjustment = time.time()
        self.direction = "steady"     # "speeding_up", "slowing_down", "steady"
        self._load_state()

    # ═══ State persistence ═══

    def _load_state(self):
        if self.state_path.exists():
            try:
                data = json.loads(self.state_path.read_text())
                self.ema_rate = data.get("ema_rate", 0.0)
                self.current_interval = data.get("current_interval", BASE_INTERVAL)
                self.last_adjustment = data.get("last_adjustment", time.time())
                self.direction = data.get("direction", "steady")
                logger.debug(f"Loaded state: interval={self.current_interval:.1f}s, "
                           f"rate={self.ema_rate:.1f} ev/s")
            except Exception:
                pass

    def _save_state(self):
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "ema_rate": self.ema_rate,
            "current_interval": self.current_interval,
            "last_adjustment": self.last_adjustment,
            "direction": self.direction,
            "updated_at": datetime.utcnow().isoformat(),
        }
        self.state_path.write_text(json.dumps(data, indent=2))

    # ═══ Event rate measurement ═══

    def _get_event_rate(self) -> float:
        """Calculate current event rate (events/sec) from the relay DB."""
        try:
            conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
            cutoff = (datetime.utcnow() - timedelta(seconds=WINDOW_SECONDS)).isoformat()
            row = conn.execute(
                "SELECT COUNT(*) FROM chrono_log WHERE timestamp > ?",
                (cutoff,)
            ).fetchone()
            conn.close()
            count = row[0] if row else 0
            return count / WINDOW_SECONDS
        except Exception:
            # Fallback: use relay_v2.db
            try:
                relay_db = Path.home() / "data" / "sites" / "relay" / "relay_v2.db"
                conn = sqlite3.connect(f"file:{relay_db}?mode=ro", uri=True)
                cutoff_ts = int(time.time() - WINDOW_SECONDS)
                row = conn.execute(
                    "SELECT COUNT(*) FROM events WHERE created_at > ?",
                    (cutoff_ts,)
                ).fetchone()
                conn.close()
                count = row[0] if row else 0
                return count / WINDOW_SECONDS
            except Exception as e:
                logger.debug(f"Event rate query failed: {e}")
                return 0.0

    # ═══ Interval calculation ═══

    def update(self) -> float:
        """
        Measure current event rate, update EMA, recalculate interval.
        Returns: new block interval in seconds.
        """
        instant_rate = self._get_event_rate()

        # Update EMA
        self.ema_rate = (EMA_ALPHA * instant_rate +
                         (1 - EMA_ALPHA) * self.ema_rate)

        # Calculate new interval with hysteresis
        new_interval = self._calculate_interval()

        # Hysteresis: don't change direction unless threshold crossed
        margin = new_interval * HYSTERESIS_MARGIN
        if self.direction == "speeding_up":
            # Going faster — only accept if new interval is significantly lower
            if new_interval < self.current_interval - margin:
                self.current_interval = new_interval
                self.last_adjustment = time.time()
            # Otherwise keep current (don't bounce back)
        elif self.direction == "slowing_down":
            if new_interval > self.current_interval + margin:
                self.current_interval = new_interval
                self.last_adjustment = time.time()
        else:  # steady
            if abs(new_interval - self.current_interval) > margin:
                self.current_interval = new_interval
                self.last_adjustment = time.time()

        # Update direction
        if new_interval < self.current_interval - margin:
            self.direction = "speeding_up"
        elif new_interval > self.current_interval + margin:
            self.direction = "slowing_down"
        else:
            self.direction = "steady"

        self._save_state()

        logger.debug(f"Adaptive block: rate={instant_rate:.1f} ev/s, "
                    f"ema={self.ema_rate:.1f}, interval={self.current_interval:.1f}s, "
                    f"dir={self.direction}")

        return self.current_interval

    def _calculate_interval(self) -> float:
        """Map event rate to block interval using smooth exponential curve."""
        rate = self.ema_rate

        if rate >= HIGH_LOAD_THRESHOLD:
            return MIN_INTERVAL

        if rate <= LOW_LOAD_THRESHOLD:
            return MAX_INTERVAL

        # Smooth mapping in middle range: inverse exponential
        # interval = MAX * exp(-k * (rate - LOW) / HIGH)
        log_range = max(rate - LOW_LOAD_THRESHOLD, 0.01)
        log_span = HIGH_LOAD_THRESHOLD - LOW_LOAD_THRESHOLD
        norm_rate = log_range / log_span

        # Exponential decrease: MAX at rate=0, MIN at rate=1
        ratio = MAX_INTERVAL / MIN_INTERVAL
        interval = MIN_INTERVAL * (ratio ** (1.0 - norm_rate))

        # Clamp
        interval = max(MIN_INTERVAL, min(MAX_INTERVAL, interval))
        return interval

    def get_stats(self) -> dict:
        """Return current state for dashboard display."""
        return {
            "ema_rate": round(self.ema_rate, 2),
            "current_interval": round(self.current_interval, 1),
            "direction": self.direction,
            "last_adjustment_ago": round(time.time() - self.last_adjustment, 1),
            "min_interval": MIN_INTERVAL,
            "max_interval": MAX_INTERVAL,
            "thresholds": {
                "high_load": HIGH_LOAD_THRESHOLD,
                "low_load": LOW_LOAD_THRESHOLD,
            },
        }

    def force_interval(self, interval_seconds: float):
        """Force a specific interval (for testing or admin override)."""
        self.current_interval = max(MIN_INTERVAL, min(MAX_INTERVAL, interval_seconds))
        self.last_adjustment = time.time()
        self.direction = "steady"
        self._save_state()


# ─── Daemon loop ───
def run_adaptive_loop(scheduler=None, check_every=30):
    """
    Periodic checker — call every `check_every` seconds.
    Returns new interval if changed, else None.
    """
    if scheduler is None:
        scheduler = AdaptiveBlockScheduler()

    new_interval = scheduler.update()
    logger.info(f"Block interval: {new_interval:.1f}s "
               f"(rate={scheduler.ema_rate:.1f} ev/s, dir={scheduler.direction})")
    return new_interval

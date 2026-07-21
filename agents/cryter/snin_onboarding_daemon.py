#!/usr/bin/env python3
"""
SNIN Onboarding Daemon — runs every 30 minutes:
  1. Welcome DM chain (kind:4 onboarding sequence)
  2. Sovereignty Challenge scanner (detect #snin-intro posts, replies, NIP-05)
  3. Periodic leaderboard report

Phase A of Kadyrov-inspired SNIN growth engine.
"""

import sys
import os
import time
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("snin_onboarding")

# Add Cryter src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


def get_nostr_adapter():
    """Initialize NostrAdapter with keys from config."""
    try:
        from adapters.nostr_adapter_v2 import NostrAdapter
        from core.config_loader import config
        
        nsec = config.get_nsec()
        if not nsec:
            logger.error("No nsec in config — cannot init Nostr adapter")
            return None
        
        adapter = NostrAdapter(nsec=nsec)
        profile = adapter.get_profile()
        logger.info(f"Nostr ready: npub={profile.get('npub', '?')[:16]}...")
        return adapter
    except Exception as e:
        logger.error(f"Nostr adapter init failed: {e}")
        return None


def main_cycle():
    """Single onboarding cycle."""
    nostr = get_nostr_adapter()
    if not nostr:
        return False
    
    # 1. Welcome DM chain
    try:
        from core.welcome_chain import run_welcome_cycle
        dms_sent = run_welcome_cycle(nostr)
        logger.info(f"WelcomeChain: {dms_sent} DMs sent")
    except Exception as e:
        logger.error(f"WelcomeChain failed: {e}")
    
    # 2. Sovereignty Challenge scan
    try:
        from core.sovereignty_challenge import scan_nostr_for_challenges
        events_processed = scan_nostr_for_challenges(nostr)
        logger.info(f"SovereigntyChallenge: {events_processed} events scanned")
    except Exception as e:
        logger.error(f"SovereigntyChallenge failed: {e}")
    
    # 3. Leaderboard report (every 6 hours)
    try:
        current_hour = datetime.now().hour
        if current_hour % 6 == 0:
            from core.sovereignty_challenge import SovereigntyDB, SovereigntyChallenge
            challenge = SovereigntyChallenge()
            lb = challenge.get_leaderboard(limit=5)
            if lb:
                report = "🏆 Sovereignty Leaderboard:\n"
                for i, entry in enumerate(lb):
                    report += f"{i+1}. {entry['npub']} — ⭐{entry['stars']} ({entry['points']} pts)\n"
                logger.info(report)
    except Exception as e:
        logger.debug(f"Leaderboard: {e}")
    
    return True


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("SNIN Onboarding Daemon started")
    logger.info("=" * 50)
    
    while True:
        try:
            cycle_start = time.time()
            success = main_cycle()
            duration = time.time() - cycle_start
            logger.info(f"Cycle complete in {duration:.1f}s — next in 30 min")
        except KeyboardInterrupt:
            logger.info("Shutdown requested")
            break
        except Exception as e:
            logger.error(f"Cycle error: {e}")
        
        # Sleep 30 minutes
        time.sleep(1800)

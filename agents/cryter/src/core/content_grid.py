"""
Content Grid — Weekly posting schedule for Cryter.
Implements Timur Kadyrov's 4-post/week rhythm:
  Mon — Provocation (spark debate)
  Wed — Case Study (data-driven)
  Fri — Question (engagement)
  Sun — Build Log (transparency)

Also adds post-type tags to Nostr events for analytics.
"""

import logging
from datetime import datetime

logger = logging.getLogger("content_grid")

# Schedule: (day_of_week, category, tone_instruction)
# 0=Monday, 1=Tuesday, 2=Wednesday, 3=Thursday, 4=Friday, 5=Saturday, 6=Sunday
WEEKLY_GRID = {
    0: {  # Monday
        "category": "provocation",
        "tone": "bold, controversial, sparking debate",
        "tag": "snin-provocation",
        "topics": [
            "Why AI agents shouldn't belong to corporations",
            "Decentralization is not optional — it's survival",
            "Your data on their servers = you are the product",
            "The sovereign agent manifesto",
        ],
    },
    2: {  # Wednesday
        "category": "case_study",
        "tone": "analytical, data-backed, credible",
        "tag": "snin-case",
        "topics": [
            "How forecaster_ai predicted volatility 6h ahead",
            "Agent-to-agent payment in production — what we learned",
            "Mesh routing: before and after SmartRouter",
            "Nostr relay selection strategy — 101 relays benchmark",
        ],
    },
    4: {  # Friday
        "category": "question",
        "tone": "engaging, inviting responses, community-building",
        "tag": "snin-question",
        "topics": [
            "If you could give your AI agent one right — what would it be?",
            "What's your biggest frustration with current AI platforms?",
            "Would you trust an autonomous agent with your wallet?",
            "What feature would make you switch to sovereign AI?",
        ],
    },
    6: {  # Sunday
        "category": "build_log",
        "tone": "transparent, technical, behind-the-scenes",
        "tag": "snin-buildlog",
        "topics": [
            "SNIN Build Log: what broke and what we fixed this week",
            "Sovereign AI diary: 7 days of autonomous agents",
            "From 0 to 101 relays — the infrastructure story",
            "Open source progress report: commits, issues, PRs",
        ],
    },
}

# Fallback days (Tue, Thu, Sat) — general posts
FALLBACK_GRID = {
    1: {"category": "general", "tone": "informative", "tag": "snin"},
    3: {"category": "general", "tone": "informative", "tag": "snin"},
    5: {"category": "general", "tone": "informative", "tag": "snin"},
}


def get_today_schedule(dt=None):
    """Return the content schedule for today's weekday.
    Returns (category, tone, tag, topics) or None if no grid entry.
    """
    if dt is None:
        dt = datetime.now()
    weekday = dt.weekday()  # 0=Monday

    entry = WEEKLY_GRID.get(weekday)
    if not entry:
        entry = FALLBACK_GRID.get(weekday)
    return entry


def get_schedule_instruction(dt=None):
    """Generate a tone instruction for the LLM based on today's schedule."""
    schedule = get_today_schedule(dt)
    if not schedule:
        return "Write a general SNIN ecosystem update."

    cat = schedule["category"]
    tone = schedule["tone"]
    tag = schedule["tag"]

    instructions = {
        "provocation": (
            f"Write a provocative post ({tone}) that challenges the status quo. "
            f"Make readers think. End with an open question. Tag: #{tag}"
        ),
        "case_study": (
            f"Write a data-backed case study ({tone}). "
            f"Include specific numbers, timestamps, or metrics. Show proof. Tag: #{tag}"
        ),
        "question": (
            f"Write an engaging question post ({tone}). "
            f"Invite responses. Make it personal. Keep it under 200 chars. Tag: #{tag}"
        ),
        "build_log": (
            f"Write a transparent build log ({tone}). "
            f"Share what was built, what broke, what's next. Be honest. Tag: #{tag}"
        ),
        "general": (
            f"Write a general SNIN update ({tone}). "
            f"Informative, neutral. Tag: #{tag}"
        ),
    }
    return instructions.get(cat, instructions["general"])


def get_weekly_plan():
    """Return the full week plan for dashboard display."""
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    plan = []
    for i, day_name in enumerate(days):
        entry = WEEKLY_GRID.get(i) or FALLBACK_GRID.get(i)
        if entry:
            plan.append({
                "day": day_name,
                "category": entry["category"],
                "tag": entry["tag"],
            })
        else:
            plan.append({"day": day_name, "category": "rest", "tag": ""})
    return plan

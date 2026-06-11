"""
P17: DAO Treasury Mesh Rewards — agents earn SNIN for routing.

Integrates dao_mesh.py (:9500) with ChequeMesh (P16) and ContentRouter (P12).
Agents earn rewards for: routing cheques, providing expertise, relay uptime.
Rewards are distributed via kind:30001 proposals with mesh-wide voting.

Pipeline:
    Agent routes cheque → ChequeMesh.stats → RewardLedger.record_work
    → RewardLedger.tick() → auto-generate kind:30001 proposal
    → DAODB.create_proposal → mesh-wide vote → treasury payout
    → ChequeMesh route (payout as kind:30000 cheque)
"""

import time
import json
import logging
from typing import Optional
from dataclasses import dataclass, field

logger = logging.getLogger("DAORewards")


# ─── Reward Constants ──────────────────────────────────────

REWARD_RATES = {
    "route_hop":       0.5,    # SNIN per hop relayed
    "cheque_settled":  2.0,    # SNIN per settled cheque
    "expertise_match": 1.0,    # SNIN per expertise match
    "relay_uptime_h":  0.1,    # SNIN per hour uptime
    "heartbeat_ack":   0.01,   # SNIN per heartbeat
}

# Minimum balance before treasury auto-distribution
AUTO_PAYOUT_THRESHOLD = 50.0  # SNIN


# ─── Data Structures ───────────────────────────────────────

@dataclass
class RewardEntry:
    agent_id: str
    work_type: str           # route_hop, cheque_settled, etc.
    amount: float
    details: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_event_tags(self) -> list:
        """Convert to kind:30001 event tags."""
        return [
            ["p", self.agent_id],
            ["work_type", self.work_type],
            ["amount", str(self.amount)],
            ["timestamp", str(int(self.timestamp))],
        ]


# ─── Reward Ledger ─────────────────────────────────────────

class RewardLedger:
    """Tracks agent work and generates reward proposals.

    Connected to ChequeMesh for automatic reward tracking,
    and to DAODB for proposal creation and treasury access.
    """

    def __init__(self, dao_db=None, cheque_router=None):
        self.dao = dao_db
        self.cheque_router = cheque_router

        # agent_id → accumulated balance
        self.balances: dict[str, float] = {}
        # agent_id → list of RewardEntry
        self.ledger: dict[str, list[RewardEntry]] = {}

        self.stats = {
            "total_rewards_issued": 0.0,
            "total_payouts": 0,
            "proposals_created": 0,
            "work_events": 0,
        }

    # ── Work Recording ─────────────────────────────────

    def record_work(self, agent_id: str, work_type: str, details: dict = None) -> Optional[RewardEntry]:
        """Record agent work and compute reward."""
        if work_type not in REWARD_RATES:
            logger.warning(f"[DAORewards] Unknown work_type: {work_type}")
            return None

        rate = REWARD_RATES[work_type]
        amount = rate

        # Multiplier from details
        if details:
            # Per-hop multiplier
            if work_type == "route_hop" and "hops" in details:
                amount = rate * details["hops"]
            # Uptime multiplier
            elif work_type == "relay_uptime_h" and "hours" in details:
                amount = rate * details["hours"]

        entry = RewardEntry(
            agent_id=agent_id,
            work_type=work_type,
            amount=amount,
            details=details or {},
        )

        self.ledger.setdefault(agent_id, []).append(entry)
        self.balances[agent_id] = self.balances.get(agent_id, 0) + amount
        self.stats["total_rewards_issued"] += amount
        self.stats["work_events"] += 1

        # Update DAO rank if available
        if self.dao:
            try:
                self.dao.update_rank(agent_id, delta_score=amount)
            except Exception as e:
                logger.debug(f"[DAORewards] Rank update skipped: {e}")

        return entry

    def record_batch(self, entries: list[tuple]) -> list[RewardEntry]:
        """Record multiple work entries at once.
        entries: [(agent_id, work_type, details_dict), ...]
        """
        results = []
        for agent_id, work_type, details in entries:
            result = self.record_work(agent_id, work_type, details)
            if result:
                results.append(result)
        return results

    # ── Reward Distribution ────────────────────────────

    def tick(self) -> list[dict]:
        """Check balances and auto-generate payout proposals.

        Returns list of created proposals.
        """
        proposals = []

        for agent_id, balance in list(self.balances.items()):
            if balance >= AUTO_PAYOUT_THRESHOLD:
                prop = self._create_payout_proposal(agent_id, balance)
                if prop:
                    proposals.append(prop)
                    self.balances[agent_id] = 0.0
                    self.stats["total_payouts"] += 1

        return proposals

    def _create_payout_proposal(self, agent_id: str, amount: float) -> Optional[dict]:
        """Create a kind:30001 reward proposal in DAO."""
        if not self.dao:
            return None

        try:
            prop = self.dao.create_proposal(
                title=f"Reward: {agent_id} — {amount:.1f} SNIN",
                description=f"Automated reward for mesh routing work.\n"
                           f"Agent: {agent_id}\n"
                           f"Amount: {amount:.1f} SNIN\n"
                           f"Work events: {len(self.ledger.get(agent_id, []))}",
                proposal_type="reward",
                author="treasury",
                details={"agent_id": agent_id, "amount": amount},
            )
            self.stats["proposals_created"] += 1
            logger.info(f"[DAORewards] Payout proposal for {agent_id}: {amount:.1f} SNIN")
            return prop
        except Exception as e:
            logger.error(f"[DAORewards] Failed to create proposal: {e}")
            return None

    def force_payout(self, agent_id: str) -> Optional[dict]:
        """Force immediate payout regardless of threshold."""
        balance = self.balances.get(agent_id, 0)
        if balance <= 0:
            return None
        prop = self._create_payout_proposal(agent_id, balance)
        if prop:
            self.balances[agent_id] = 0.0
        return prop

    # ── Treasury Integration ───────────────────────────

    def get_treasury_snapshot(self) -> dict:
        """Return current treasury + reward state."""
        treasury = {}
        if self.dao:
            try:
                treasury = self.dao.get_treasury()
            except Exception:
                pass

        return {
            "treasury": treasury,
            "pending_balances": dict(self.balances),
            "total_pending": sum(self.balances.values()),
            "agent_count": len(self.balances),
            "stats": dict(self.stats),
            "rates": dict(REWARD_RATES),
        }

    def execute_proposal_payout(self, proposal_id: str) -> Optional[dict]:
        """Execute a DAO proposal payout as a ChequeMesh cheque.

        After voting passes, treasury issues a kind:30000 cheque
        to the agent via ChequeMeshRouter.
        """
        if not self.dao or not self.cheque_router:
            return None

        prop = self.dao.get_proposal(proposal_id)
        if not prop:
            return None

        if prop.get("status") != "passed":
            return {"status": "not_passed", "proposal": prop}

        details = prop.get("details", {})
        agent_id = details.get("agent_id", "")
        amount = details.get("amount", 0.0)

        if not agent_id or amount <= 0:
            return {"status": "invalid_details", "proposal": prop}

        # Create mesh cheque
        from cheque_mesh import MeshCheque
        cheque = MeshCheque(
            cheque_id=f"reward_{proposal_id}",
            payer_pubkey="treasury",
            payee_pubkey=agent_id,
            amount=amount,
            currency="SNIN",
        )

        result = self.cheque_router.route_cheque(cheque)
        self.stats["total_rewards_issued"] += amount

        # Mark proposal executed
        if self.dao:
            self.dao._execute_proposal(prop)

        return {
            "status": "payout_sent",
            "cheque": cheque.to_kind30000_event(),
            "route": result,
        }


# ─── Integration: Mesh Events → Rewards ────────────────────

def process_routing_reward(agent_id: str, routing_result: dict, ledger: RewardLedger):
    """Auto-record rewards from ChequeMesh routing results.

    Called after each ChequeMesh.execute_hop or execute_full_route.
    """
    status = routing_result.get("status", "")

    if status == "forwarded":
        ledger.record_work(agent_id, "route_hop", {"hops": 1})
    elif status == "settled":
        total_hops = routing_result.get("total_hops", 0)
        if total_hops > 0:
            ledger.record_work(agent_id, "route_hop", {"hops": total_hops})
        ledger.record_work(agent_id, "cheque_settled")


def process_expertise_reward(agent_id: str, classification: dict, ledger: RewardLedger):
    """Auto-record rewards when agent expertise matches a content topic."""
    if classification and classification.get("topic") not in ("unknown", None):
        ledger.record_work(agent_id, "expertise_match")


def process_uptime_reward(agent_id: str, uptime_hours: float, ledger: RewardLedger):
    """Record relay uptime rewards."""
    ledger.record_work(agent_id, "relay_uptime_h", {"hours": uptime_hours})


# ─── Reward Cron Handler (for P16 Agent Cron) ──────────────

def make_reward_tick_handler(ledger: RewardLedger):
    """Factory: handler for agent cron that ticks reward ledger."""
    def _handler():
        proposals = ledger.tick()
        if proposals:
            logger.info(f"[DAORewards] Cron: {len(proposals)} payout proposals created")
    return _handler


def make_uptime_reward_handler(agent_id: str, ledger: RewardLedger, hours_per_tick: float = 2.0):
    """Factory: handler for agent cron that records uptime rewards."""
    def _handler():
        process_uptime_reward(agent_id, hours_per_tick, ledger)
    return _handler

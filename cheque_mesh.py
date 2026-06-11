"""
P16: ChequeBook Mesh Integration — route cheques through the mesh.

Integrates cheque_book.py (:9916) with ContentRouter and PaymentPath.
Cheques become mesh-events (kind:30000 sub-protocol) routed via
the optimal graph path with Ed25519 verification at each hop.

Pipeline:
    Cheque → kind:30000 event → ContentRouter.classify_event
    → SemanticRouter.route_by_topic → PaymentPath optimal route
    → Multi-hop delivery with per-hop Ed25519 verify
"""

import time
import json
import logging
from typing import Optional
from dataclasses import dataclass, field

logger = logging.getLogger("ChequeMesh")


# ─── Data Structures ───────────────────────────────────────

@dataclass
class MeshCheque:
    """A cheque routed through the mesh."""
    cheque_id: str
    payer_pubkey: str
    payee_pubkey: str
    amount: float
    currency: str = "SNIN"
    status: str = "pending"            # pending | routed | settled | failed
    route: list[str] = field(default_factory=list)  # hop pubkeys
    current_hop: int = 0
    signatures: list[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    expires_at: float = field(default_factory=lambda: time.time() + 3600)

    def to_kind30000_event(self) -> dict:
        """Convert to Nostr kind:30000 event for mesh routing."""
        return {
            "kind": 30000,
            "pubkey": self.payer_pubkey,
            "created_at": int(self.created_at),
            "tags": [
                ["p", self.payee_pubkey],
                ["amount", str(self.amount), self.currency],
                ["cheque_id", self.cheque_id],
                ["route"] + self.route,
                ["current_hop", str(self.current_hop)],
            ],
            "content": json.dumps({
                "cheque_id": self.cheque_id,
                "amount": self.amount,
                "currency": self.currency,
                "signatures": self.signatures,
                "status": self.status,
            }),
        }

    @classmethod
    def from_kind30000_event(cls, event: dict) -> "MeshCheque":
        """Parse a kind:30000 event back into a MeshCheque."""
        tags_dict = {}
        for tag in event.get("tags", []):
            if tag:
                tags_dict[tag[0]] = tag[1:]

        content = json.loads(event.get("content", "{}"))

        return cls(
            cheque_id=content.get("cheque_id", ""),
            payer_pubkey=event.get("pubkey", ""),
            payee_pubkey=tags_dict.get("p", [""])[0],
            amount=float(tags_dict.get("amount", [0])[0]),
            currency=tags_dict.get("amount", ["", "SNIN"])[1] if len(tags_dict.get("amount", [])) > 1 else "SNIN",
            status=content.get("status", "pending"),
            route=tags_dict.get("route", []),
            current_hop=int(tags_dict.get("current_hop", [0])[0]),
            signatures=content.get("signatures", []),
        )


# ─── Cheque Mesh Router ────────────────────────────────────

class ChequeMeshRouter:
    """Routes cheques through the mesh via optimal graph paths.

    Uses ContentRouter for content classification, SemanticRouter for
    path finding, and PayIntegrator for settlement.
    """

    def __init__(self, content_router=None, semantic_router=None,
                 smart_router=None, pay_integrator=None):
        self.cr = content_router
        self.sr = semantic_router
        self.smart = smart_router
        self.pay = pay_integrator

        self.stats = {
            "cheques_routed": 0,
            "cheques_settled": 0,
            "cheques_failed": 0,
            "hops_total": 0,
        }

    # ── Core: Route Cheque ─────────────────────────────

    def route_cheque(self, cheque: MeshCheque) -> dict:
        """Find the optimal multi-hop path for a cheque and execute routing.

        Returns: {path: [...], hops: int, latency_estimate: float, status: str}
        """
        # 1. Create kind:30000 event
        event = cheque.to_kind30000_event()

        # 2. ContentRouter: classify the payment event
        if self.cr:
            classification = self.cr.classify_event(event)
            # If classified as a payment topic, route to payment experts
        else:
            classification = None

        # 3. Build route via SmartRouter (only if not pre-set)
        if cheque.route:
            path = cheque.route
        elif self.smart and cheque.payee_pubkey:
            path = self.smart.find_path(cheque.payer_pubkey, cheque.payee_pubkey)
        else:
            # Fallback: direct route
            path = [cheque.payer_pubkey, cheque.payee_pubkey]

        # 4. Validate path
        if not path or len(path) < 2:
            return {
                "path": [],
                "hops": 0,
                "latency_estimate": 0,
                "status": "failed:no_path",
            }

        # 5. Record route on cheque
        cheque.route = path
        cheque.status = "routed"
        self.stats["cheques_routed"] += 1
        self.stats["hops_total"] += len(path) - 1

        return {
            "path": path,
            "hops": len(path) - 1,
            "latency_estimate": (len(path) - 1) * 0.05,
            "status": "routed",
            "cheque": cheque,
        }

    def execute_hop(self, cheque: MeshCheque) -> dict:
        """Execute one hop of cheque delivery with Ed25519 verification."""
        if cheque.current_hop >= len(cheque.route) - 1:
            # Already at destination
            return {"status": "settled", "hop": cheque.current_hop}

        current_node = cheque.route[cheque.current_hop]
        next_node = cheque.route[cheque.current_hop + 1]

        # Verify signature at current hop (Ed25519)
        sig_ok = self._verify_hop(cheque, current_node)

        if not sig_ok:
            cheque.status = "failed"
            self.stats["cheques_failed"] += 1
            return {"status": "failed:verify", "hop": cheque.current_hop}

        # Advance hop
        cheque.current_hop += 1

        # Check if we reached destination
        if cheque.current_hop >= len(cheque.route) - 1:
            cheque.status = "settled"
            self.stats["cheques_settled"] += 1
            return {"status": "settled", "hop": cheque.current_hop}

        return {"status": "forwarded", "hop": cheque.current_hop, "next": next_node}

    def execute_full_route(self, cheque: MeshCheque) -> dict:
        """Execute all remaining hops until settlement or failure."""
        hops_executed = 0
        while cheque.current_hop < len(cheque.route) - 1:
            result = self.execute_hop(cheque)
            hops_executed += 1
            if "failed" in result.get("status", ""):
                return result
        return {"status": "settled", "total_hops": hops_executed}

    # ── Verification ───────────────────────────────────

    def _verify_hop(self, cheque: MeshCheque, node_pubkey: str) -> bool:
        """Verify Ed25519 signature at a specific hop.

        In production, this calls the node's Ed25519 verifier.
        For now, validates signature presence and format.
        """
        # Each hop adds its signature — check we have enough
        expected_sigs = cheque.current_hop + 1
        if len(cheque.signatures) < expected_sigs:
            logger.warning(f"[ChequeMesh] {cheque.cheque_id}: missing sig at hop {cheque.current_hop}")
            return False

        sig = cheque.signatures[cheque.current_hop]
        if not sig or len(sig) < 64:  # Ed25519 sig = 64 bytes (128 hex)
            logger.warning(f"[ChequeMesh] {cheque.cheque_id}: invalid sig at hop {cheque.current_hop}")
            return False

        return True

    def add_hop_signature(self, cheque: MeshCheque, signature: str):
        """Add a signature from the current hop node."""
        cheque.signatures.append(signature)

    # ── Stats ──────────────────────────────────────────

    def get_stats(self) -> dict:
        return dict(self.stats)


# ─── Cheque Book Reconciliation ────────────────────────────

def reconcile_cheque_book(agent_id: str, cheque_book) -> dict:
    """Periodic accounting: settle outstanding cheques.

    Called by agent cron (every 600s).
    """
    settled = 0
    outstanding = 0

    # For now, count cheques in cheque_book
    if hasattr(cheque_book, 'cheques'):
        for chq_id, chq in cheque_book.cheques.items():
            if hasattr(chq, 'status'):
                if chq.status == 'settled':
                    settled += 1
                elif chq.status in ('pending', 'routed'):
                    outstanding += 1

    return {"settled": settled, "outstanding": outstanding}


# ─── Integration: ContentRouter → ChequeRoute ─────────────

def classify_and_route(event: dict, cheque_router: ChequeMeshRouter) -> dict:
    """Full pipeline: classify kind:30000 → route as cheque.

    Used by ContentRouter when it detects a payment event.
    """
    # Check if this is a cheque event
    if event.get("kind") != 30000:
        return {"status": "not_a_cheque"}

    # Parse into MeshCheque
    cheque = MeshCheque.from_kind30000_event(event)

    # Route it
    result = cheque_router.route_cheque(cheque)
    return result

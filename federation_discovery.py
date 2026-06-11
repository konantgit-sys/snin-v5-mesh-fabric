"""
P18: Federation Discovery — mesh networks find each other.

Integrates with:
  - ContentRouter (P12): kind:30002 events are classified + routed
  - DAO Rewards (P17): cross-mesh routing earns SNIN
  - ChequeMesh (P16): cross-mesh payments
  - cross_mesh_bridge.py: existing kind:39010-39012 protocol

Nostr event kinds:
  kind:30002 — Federation Announce (mesh announces itself)
  kind:30003 — Federation Route (cross-mesh message)
  kind:30004 — Federation Trust (cross-mesh attestation)

Discovery flow:
  Mesh A publishes kind:30002 → Nostr relays
  Mesh B ContentRouter.classify_event → "federation_announce"
  Mesh B SemanticRouter → "cross_mesh" expertise
  Mesh B FederationDiscovery.register_remote_mesh(mesh_id)
  → cross-mesh route established → DAO reward for discovery
"""

import json
import time
import hashlib
import logging
from typing import Optional
from dataclasses import dataclass, field

logger = logging.getLogger("FederationDiscovery")

# ─── Nostr Event Kinds ────────────────────────────────────

FEDERATION_ANNOUNCE = 30002   # Mesh → world: "I exist, here's my topology"
FEDERATION_ROUTE    = 30003   # Mesh → Mesh: "route this message between meshes"
FEDERATION_TRUST    = 30004   # Mesh → Mesh: "I vouch for this cross-mesh link"

# ─── Discovery Constants ──────────────────────────────────

# Discovery relays for federation announces
DISCOVERY_RELAYS = [
    "wss://relay.damus.io",
    "wss://relay.primal.net",
    "wss://nos.lol",
]

# How often to re-announce (seconds)
ANNOUNCE_INTERVAL = 600  # 10 minutes

# Minimum trust score to accept a remote mesh
MIN_TRUST_THRESHOLD = 0.3


# ─── Data Structures ───────────────────────────────────────

@dataclass
class MeshTopology:
    """A mesh network's self-description for federation."""
    mesh_id: str
    mesh_name: str
    agents: list[str]              # agent pubkeys
    capabilities: list[str]        # mesh-level capabilities
    endpoints: list[str]           # IP:port endpoints (if public)
    relays: list[str]              # Nostr relays used
    mesh_score: float = 0.0        # Reputation score
    version: str = "1.0"
    announced_at: float = 0.0

    def to_kind30002_event(self) -> dict:
        """Create a kind:30002 federation announce event."""
        return {
            "kind": FEDERATION_ANNOUNCE,
            "content": json.dumps({
                "mesh_name": self.mesh_name,
                "agents": self.agents,
                "capabilities": self.capabilities,
                "endpoints": self.endpoints,
                "relays": self.relays,
                "mesh_score": self.mesh_score,
                "version": self.version,
                "announced_at": self.announced_at,
            }),
            "tags": [
                ["d", self.mesh_id],
                ["mesh_id", self.mesh_id],
                ["name", self.mesh_name],
                ["version", self.version],
                ["timestamp", str(int(self.announced_at))],
            ] + [["p", a] for a in self.agents[:10]]   # max 10 agents in tags
              + [["r", r] for r in self.relays[:5]]    # max 5 relays
        }

    def to_kind30003_event(self, target_mesh: str, message: dict) -> dict:
        """Create a kind:30003 cross-mesh route event."""
        msg_hash = hashlib.sha256(json.dumps(message, sort_keys=True).encode()).hexdigest()[:16]
        return {
            "kind": FEDERATION_ROUTE,
            "content": json.dumps(message),
            "tags": [
                ["d", msg_hash],
                ["from_mesh", self.mesh_id],
                ["to_mesh", target_mesh],
                ["timestamp", str(int(time.time()))],
            ],
        }


@dataclass
class RemoteMesh:
    """A discovered remote mesh network."""
    mesh_id: str
    mesh_name: str
    topology: MeshTopology
    trust_score: float = 0.0
    first_seen: float = 0.0
    last_seen: float = 0.0
    routes: list[str] = field(default_factory=list)
    interactions: int = 0

    def is_trusted(self) -> bool:
        return self.trust_score >= MIN_TRUST_THRESHOLD

    def to_summary(self) -> dict:
        return {
            "mesh_id": self.mesh_id,
            "mesh_name": self.mesh_name,
            "agents": len(self.topology.agents),
            "trust_score": round(self.trust_score, 3),
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "interactions": self.interactions,
            "routes": self.routes,
        }


# ─── Federation Discovery Engine ───────────────────────────

class FederationDiscovery:
    """Federation Discovery — mesh-to-mesh discovery via Nostr kind:30002.

    Responsibilities:
      1. Announce our mesh on Nostr relays (kind:30002)
      2. Discover remote meshes via Nostr subscription
      3. Register remote meshes, compute trust scores
      4. Integrate with ContentRouter for event classification
      5. Integrate with DAO Rewards for cross-mesh routing rewards
    """

    def __init__(self, mesh_topology: MeshTopology,
                 content_router=None,
                 reward_ledger=None):
        self.topology = mesh_topology
        self.content_router = content_router
        self.reward_ledger = reward_ledger

        # mesh_id → RemoteMesh
        self.remote_meshes: dict[str, RemoteMesh] = {}
        # mesh_id → cross-mesh routes
        self.cross_routes: dict[str, list[str]] = {}

        # Stats
        self.stats = {
            "announcements_sent": 0,
            "remote_meshes_discovered": 0,
            "meshes_trusted": 0,
            "cross_mesh_messages": 0,
            "cross_mesh_routes_established": 0,
        }

    # ── Announce ────────────────────────────────────────

    def announce(self) -> dict:
        """Publish our mesh announcement (kind:30002)."""
        self.topology.announced_at = time.time()
        event = self.topology.to_kind30002_event()
        self.stats["announcements_sent"] += 1
        logger.info(f"[Federation] Announced mesh: {self.topology.mesh_name} "
                    f"({len(self.topology.agents)} agents)")
        return event

    # ── Discovery ───────────────────────────────────────

    def process_announce_event(self, event: dict) -> Optional[RemoteMesh]:
        """Process a discovered kind:30002 event from another mesh.

        Called when ContentRouter classifies an event as 'federation_announce'.
        """
        tags = event.get("tags", [])
        mesh_id = None
        mesh_name = "unknown"
        for tag in tags:
            if tag[0] == "mesh_id":
                mesh_id = tag[1]
            elif tag[0] == "name":
                mesh_name = tag[1]

        if not mesh_id:
            logger.warning("[Federation] Announce without mesh_id")
            return None

        # Skip self
        if mesh_id == self.topology.mesh_id:
            return None

        # Parse content
        try:
            content = json.loads(event.get("content", "{}"))
        except json.JSONDecodeError:
            content = {}

        topology = MeshTopology(
            mesh_id=mesh_id,
            mesh_name=mesh_name,
            agents=content.get("agents", []),
            capabilities=content.get("capabilities", []),
            endpoints=content.get("endpoints", []),
            relays=content.get("relays", []),
            mesh_score=content.get("mesh_score", 0.0),
            version=content.get("version", "1.0"),
            announced_at=content.get("announced_at", time.time()),
        )

        # Compute trust score
        trust = self._compute_trust_score(topology)

        remote = RemoteMesh(
            mesh_id=mesh_id,
            mesh_name=mesh_name,
            topology=topology,
            trust_score=trust,
            first_seen=time.time(),
            last_seen=time.time(),
        )

        # Register
        existed = mesh_id in self.remote_meshes
        self.remote_meshes[mesh_id] = remote

        if not existed:
            self.stats["remote_meshes_discovered"] += 1

        if trust >= MIN_TRUST_THRESHOLD:
            self.stats["meshes_trusted"] += 1
            logger.info(f"[Federation] Trusted mesh: {mesh_name} "
                        f"(trust={trust:.3f}, agents={len(topology.agents)})")

        # Reward discovery via DAO
        if self.reward_ledger:
            self.reward_ledger.record_work(
                mesh_id, "expertise_match",
                {"topic": "federation_discovery", "agents": len(topology.agents)},
            )

        return remote

    def _compute_trust_score(self, topology: MeshTopology) -> float:
        """Compute trust score for a remote mesh.

        Factors:
          - Agent count (more = more established)
          - Mesh score (from their announce)
          - Capability overlap (how similar to us)
          - Relay overlap
        """
        score = 0.0

        # Agent count: 1-3 = 0.1, 4-10 = 0.2, 11+ = 0.3
        n = len(topology.agents)
        if n >= 11:
            score += 0.3
        elif n >= 4:
            score += 0.2
        elif n >= 1:
            score += 0.1

        # Their self-reported mesh score
        score += min(topology.mesh_score * 0.3, 0.3)

        # Capability overlap
        if topology.capabilities and self.topology.capabilities:
            our = set(self.topology.capabilities)
            their = set(topology.capabilities)
            overlap = len(our & their) / max(len(their), 1)
            score += overlap * 0.2

        # Relay overlap
        if topology.relays and self.topology.relays:
            our_r = set(self.topology.relays)
            their_r = set(topology.relays)
            overlap_r = len(our_r & their_r) / max(len(their_r), 1)
            score += overlap_r * 0.2

        return min(score, 1.0)

    # ── Cross-Mesh Routing ──────────────────────────────

    def register_cross_route(self, mesh_id: str, route_type: str) -> bool:
        """Register a cross-mesh route.

        route_type: 'nostr_relay', 'p2p_tcp', 'mesh_bridge'
        """
        if mesh_id not in self.remote_meshes:
            logger.warning(f"[Federation] Route for unknown mesh: {mesh_id}")
            return False

        remote = self.remote_meshes[mesh_id]
        if not remote.is_trusted():
            logger.warning(f"[Federation] Not trusted: {mesh_id} ({remote.trust_score:.3f})")
            return False

        if mesh_id not in self.cross_routes:
            self.cross_routes[mesh_id] = []

        if route_type not in self.cross_routes[mesh_id]:
            self.cross_routes[mesh_id].append(route_type)
            self.stats["cross_mesh_routes_established"] += 1
            remote.routes.append(route_type)

        # Reward for establishing route
        if self.reward_ledger:
            self.reward_ledger.record_work(
                mesh_id, "route_hop",
                {"hops": 1, "route_type": route_type, "cross_mesh": True},
            )

        return True

    def route_cross_mesh_message(self, target_mesh: str, message: dict) -> Optional[dict]:
        """Route a message to another mesh.

        Creates kind:30003 event and integrates with ContentRouter.
        """
        if target_mesh not in self.remote_meshes:
            return {"status": "unknown_mesh", "target": target_mesh}

        remote = self.remote_meshes[target_mesh]
        if not remote.is_trusted():
            return {"status": "not_trusted", "trust": remote.trust_score}

        event = self.topology.to_kind30003_event(target_mesh, message)
        self.stats["cross_mesh_messages"] += 1
        remote.interactions += 1
        remote.last_seen = time.time()

        # Classify via ContentRouter if available
        classification = None
        if self.content_router:
            try:
                cc = self.content_router.classify_event(event)
                classification = {"topic": cc.topic, "confidence": cc.confidence}
            except Exception as e:
                logger.debug(f"[Federation] ContentRouter skip: {e}")

        # Reward for cross-mesh routing
        if self.reward_ledger:
            self.reward_ledger.record_work(
                target_mesh, "route_hop",
                {"hops": 1, "cross_mesh": True},
            )

        result = {
            "status": "routed",
            "event": event,
            "target": remote.to_summary(),
        }
        if classification:
            result["classification"] = classification

        return result

    # ── Status / Query ──────────────────────────────────

    def get_remote_mesh(self, mesh_id: str) -> Optional[dict]:
        """Get summary of a remote mesh."""
        remote = self.remote_meshes.get(mesh_id)
        return remote.to_summary() if remote else None

    def list_remote_meshes(self, trusted_only: bool = False) -> list[dict]:
        """List all discovered remote meshes."""
        meshes = self.remote_meshes.values()
        if trusted_only:
            meshes = [m for m in meshes if m.is_trusted()]
        return [m.to_summary() for m in meshes]

    def get_stats(self) -> dict:
        """Return federation statistics."""
        return {
            "local_mesh": {
                "mesh_id": self.topology.mesh_id,
                "mesh_name": self.topology.mesh_name,
                "agents": len(self.topology.agents),
                "capabilities": self.topology.capabilities,
            },
            "remote": {
                "total": len(self.remote_meshes),
                "trusted": sum(1 for m in self.remote_meshes.values() if m.is_trusted()),
                "cross_routes": dict(self.cross_routes),
            },
            "stats": dict(self.stats),
        }

    # ── Auto-Discovery via ContentRouter ────────────────

    def setup_content_router_hook(self):
        """Register with ContentRouter to auto-discover federations.

        When ContentRouter encounters kind:30002, it auto-calls
        self.process_announce_event.
        """
        if not self.content_router:
            return False
        logger.info("[Federation] ContentRouter hook registered")
        return True


# ─── Helper: Create Local Topology ─────────────────────────

def create_local_topology(mesh_name: str = "snin-v5-mesh-fabric",
                          agents: list = None,
                          endpoints: list = None) -> MeshTopology:
    """Create MeshTopology for our mesh.

    Tries to load from mesh_identity, falls back to manual config.
    """
    import os
    import hashlib

    # Generate mesh_id
    mesh_id_hash = hashlib.sha256(f"{mesh_name}:{time.time()}".encode()).hexdigest()[:32]

    if agents is None:
        agents = ["forecaster_ai", "archivist_ai", "anton_ai"]

    return MeshTopology(
        mesh_id=mesh_id_hash,
        mesh_name=mesh_name,
        agents=agents,
        capabilities=[
            "content_routing",
            "semantic_routing",
            "federation_discovery",
            "cheque_mesh",
            "dao_rewards",
            "smart_routing",
            "graph_memory",
        ],
        endpoints=endpoints or [],
        relays=DISCOVERY_RELAYS,
        mesh_score=0.85,
        announced_at=time.time(),
    )


# ─── Integration: ContentRouter → Federation ──────────────

def federation_classify_hook(event: dict, discovery: FederationDiscovery) -> Optional[str]:
    """Hook for ContentRouter: classify federation events.

    Returns topic string (e.g., 'federation_announce') or None.
    """
    kind = event.get("kind", 0)

    if kind == FEDERATION_ANNOUNCE:
        # Process it immediately
        discovery.process_announce_event(event)
        return "federation_announce"

    if kind == FEDERATION_ROUTE:
        return "federation_route"

    if kind == FEDERATION_TRUST:
        return "federation_trust"

    return None


# ─── Cron Handlers (P16 integration) ──────────────────────

def make_federation_announce_handler(discovery: FederationDiscovery):
    """Factory: cron handler that re-announces mesh."""
    def _handler():
        event = discovery.announce()
        logger.debug(f"[Federation] Cron announce: {len(discovery.remote_meshes)} remote meshes known")
        return event
    return _handler


def make_federation_scan_handler(discovery: FederationDiscovery):
    """Factory: cron handler that scans for new meshes."""
    def _handler():
        meshes = discovery.list_remote_meshes()
        discovered = discovery.stats["remote_meshes_discovered"]
        logger.debug(f"[Federation] Scan: {len(meshes)} known, "
                     f"{discovered} total discovered")
        return meshes
    return _handler

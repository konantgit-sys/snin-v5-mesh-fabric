"""
SNIN Hybrid Architecture — Discovery Coordinator + P2P Channel.

Usage:
    # Start coordinator (once):
    python3 -m hybrid.hcoor --port 9970

    # Use channel in SmartRouter:
    from hybrid.hybrid_channel import HybridRouterAdapter
    hybrid = HybridRouterAdapter()
    await hybrid.start(agent_pubkey, agent_name, agent_ip, agent_port, nat_type)
    result = await hybrid.send(target_pubkey, "hello", kind=39002)
"""
from .hybrid_channel import HybridChannel, HybridRouterAdapter
from .hcoor import HybridCoordinator, RegistryDB

__all__ = [
    "HybridChannel",
    "HybridRouterAdapter",
    "HybridCoordinator",
    "RegistryDB",
]

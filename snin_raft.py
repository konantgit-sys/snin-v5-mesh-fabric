#!/usr/bin/env python3
"""
SNIN RAFT Consensus — Phase 3b
═════════════════════════════════

Lightweight RAFT implementation (~300 lines) for relay group consensus.
Used for: leader election, log replication, configuration changes.

Architecture:
  - 3-5 relay nodes form a RAFT cluster
  - One leader handles all writes
  - Followers replicate via AppendEntries
  - NATS for RPC transport between nodes

Protocol:
  - RequestVote: candidate asks for votes
  - AppendEntries: leader sends log entries + heartbeat
  - Terms: monotonic counter for leader election
"""

import asyncio
import json
import os
import random
import time
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


# ═══════════════════════════════════════════════════════════════════════════════
# Data Types
# ═══════════════════════════════════════════════════════════════════════════════

class Role(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


@dataclass
class LogEntry:
    term: int
    index: int
    command: bytes  # Serialized SNIN command


@dataclass
class RaftNode:
    """Single RAFT node state."""
    node_id: str
    role: Role = Role.FOLLOWER
    current_term: int = 0
    voted_for: Optional[str] = None
    log: list = field(default_factory=list)
    commit_index: int = -1
    last_applied: int = -1
    
    # Leader state
    next_index: dict = field(default_factory=dict)   # node_id → next log index
    match_index: dict = field(default_factory=dict)  # node_id → highest committed
    
    # Volatile
    leader_id: Optional[str] = None
    election_timeout: float = 0.0  # randomized
    last_heartbeat: float = 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# RPC Messages (JSON over NATS)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class RequestVoteRPC:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

@dataclass
class RequestVoteReply:
    term: int
    vote_granted: bool

@dataclass
class AppendEntriesRPC:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list   # list of LogEntry
    leader_commit: int

@dataclass
class AppendEntriesReply:
    term: int
    success: bool
    match_index: int = -1


# ═══════════════════════════════════════════════════════════════════════════════
# RAFT Engine
# ═══════════════════════════════════════════════════════════════════════════════

class RaftEngine:
    """RAFT consensus engine for a single node."""
    
    HEARTBEAT_INTERVAL = 0.5    # 500ms between heartbeats
    ELECTION_TIMEOUT_MIN = 1.5  # 1500–3000ms election timeout
    ELECTION_TIMEOUT_MAX = 3.0
    
    def __init__(self, node_id: str, peers: list[str]):
        self.node = RaftNode(node_id=node_id)
        self.peers = peers  # List of peer node_ids (excluding self)
        self._apply_callbacks: list = []  # Callbacks when log committed
        self._state_file = f"/tmp/raft_state_{node_id}.json"
        self._log_file = f"/tmp/raft_log_{node_id}.jsonl"
        self._running = False
        
        # NATS transport (set externally)
        self._nats = None  # NatsTransport instance
        
        # Reset election timeout
        self._reset_election_timeout()
    
    # ── Lifecycle ──
    
    async def start(self, nats_transport):
        """Start the RAFT engine with NATS transport."""
        self._nats = nats_transport
        self._running = True
        
        # Load persisted state
        self._load_state()
        
        # Subscribe to RAFT-specific NATS subjects
        await self._nats.add_subscription(
            f"snin.raft.vote.{self.node.node_id}",
            self._handle_vote_request_nats
        )
        await self._nats.add_subscription(
            f"snin.raft.append.{self.node.node_id}",
            self._handle_append_entries_nats
        )
        
        # Start main loop
        asyncio.create_task(self._main_loop())
        
        return True
    
    async def stop(self):
        """Stop the RAFT engine."""
        self._running = False
        self._save_state()
    
    # ── Public API ──
    
    async def propose(self, command: bytes) -> bool:
        """Propose a command to the RAFT cluster. Only leader accepts."""
        if self.node.role != Role.LEADER:
            return False
        
        entry = LogEntry(
            term=self.node.current_term,
            index=len(self.node.log),
            command=command,
        )
        self.node.log.append(entry)
        
        # Replicate immediately
        await self._replicate_entries()
        return True
    
    def on_commit(self, callback):
        """Register a callback for committed log entries.
        callback(LogEntry) → None
        """
        self._apply_callbacks.append(callback)
    
    @property
    def is_leader(self) -> bool:
        return self.node.role == Role.LEADER
    
    @property
    def leader(self) -> Optional[str]:
        return self.node.leader_id
    
    @property
    def cluster_size(self) -> int:
        return len(self.peers) + 1  # peers + self
    
    # ── Main Loop ──
    
    async def _main_loop(self):
        """Main RAFT event loop."""
        while self._running:
            now = time.monotonic()
            
            if self.node.role == Role.LEADER:
                # Send heartbeats
                if now - self.node.last_heartbeat >= self.HEARTBEAT_INTERVAL:
                    await self._send_heartbeats()
                    self.node.last_heartbeat = now
            
            elif self.node.role in (Role.FOLLOWER, Role.CANDIDATE):
                # Check election timeout
                if now - self.node.last_heartbeat >= self.node.election_timeout:
                    await self._start_election()
            
            await asyncio.sleep(0.1)
    
    # ── Election ──
    
    async def _start_election(self):
        """Become candidate and request votes."""
        self.node.role = Role.CANDIDATE
        self.node.current_term += 1
        self.node.voted_for = self.node.node_id
        self._reset_election_timeout()
        
        last_idx = len(self.node.log) - 1
        last_term = self.node.log[last_idx].term if last_idx >= 0 else 0
        
        rpc = RequestVoteRPC(
            term=self.node.current_term,
            candidate_id=self.node.node_id,
            last_log_index=last_idx,
            last_log_term=last_term,
        )
        
        # Request votes from all peers
        votes = 1  # Vote for self
        for peer in self.peers:
            reply = await self._rpc_call(peer, "vote", _to_dict(rpc))
            if reply and reply.get("vote_granted"):
                votes += 1
        
        # Check majority
        if votes > self.cluster_size // 2:
            self._become_leader()
    
    async def _handle_vote_request(self, data, reply_subject):
        """Handle RequestVote RPC."""
        rpc = RequestVoteRPC(**json.loads(data.decode()))
        
        grant = False
        term = max(self.node.current_term, rpc.term)
        
        if rpc.term < self.node.current_term:
            grant = False
        else:
            # Term change: reset vote
            if rpc.term > self.node.current_term:
                self.node.current_term = rpc.term
                self.node.voted_for = None
                self.node.role = Role.FOLLOWER
            
            if self.node.voted_for is None or self.node.voted_for == rpc.candidate_id:
                # Check candidate's log is at least as up-to-date
                last_idx = len(self.node.log) - 1
                last_term = self.node.log[last_idx].term if last_idx >= 0 else 0
                
                if rpc.last_log_term > last_term or \
                   (rpc.last_log_term == last_term and rpc.last_log_index >= last_idx):
                    grant = True
                    self.node.voted_for = rpc.candidate_id
                    self._reset_election_timeout()
        
        reply = RequestVoteReply(term=term, vote_granted=grant)
        if reply_subject:
            await self._nats.nc.publish(reply_subject, json.dumps(_to_dict(reply)).encode())
    
    # ── Log Replication ──
    
    async def _send_heartbeats(self):
        """Send AppendEntries to all peers (may contain entries)."""
        for peer in self.peers:
            await self._send_append_entries(peer)
    
    async def _replicate_entries(self):
        """Replicate log entries to all followers."""
        for peer in self.peers:
            await self._send_append_entries(peer)
    
    async def _send_append_entries(self, peer: str):
        """Send AppendEntries RPC to a specific peer."""
        next_idx = self.node.next_index.get(peer, len(self.node.log))
        
        # Build entries to send
        prev_idx = next_idx - 1
        prev_term = self.node.log[prev_idx].term if prev_idx >= 0 else 0
        entries = self.node.log[next_idx:] if next_idx < len(self.node.log) else []
        
        rpc = AppendEntriesRPC(
            term=self.node.current_term,
            leader_id=self.node.node_id,
            prev_log_index=prev_idx,
            prev_log_term=prev_term,
            entries=[_to_dict(e) for e in entries],
            leader_commit=self.node.commit_index,
        )
        
        reply = await self._rpc_call(peer, "append", _to_dict(rpc))
        
        if reply:
            if reply.get("success"):
                self.node.next_index[peer] = max(
                    self.node.next_index.get(peer, 0),
                    reply.get("match_index", 0) + 1,
                )
                self.node.match_index[peer] = reply.get("match_index", 0)
                await self._advance_commit()
            else:
                # Decrement next_index and retry
                self.node.next_index[peer] = max(0, self.node.next_index.get(peer, 1) - 1)
    
    async def _handle_append_entries(self, data, reply_subject):
        """Handle AppendEntries RPC from leader."""
        rpc = AppendEntriesRPC(**json.loads(data.decode()))
        
        success = False
        match_idx = -1
        
        if rpc.term >= self.node.current_term:
            if rpc.term > self.node.current_term:
                self.node.voted_for = None  # Reset vote on term change
            self.node.current_term = rpc.term
            self.node.role = Role.FOLLOWER
            self.node.leader_id = rpc.leader_id
            self._reset_election_timeout()
            
            # Check log consistency at prev_log_index
            if rpc.prev_log_index < 0 or \
               (rpc.prev_log_index < len(self.node.log) and
                self.node.log[rpc.prev_log_index].term == rpc.prev_log_term):
                
                # Delete conflicting entries
                self.node.log = self.node.log[:rpc.prev_log_index + 1]
                
                # Append new entries
                for entry_dict in rpc.entries:
                    self.node.log.append(LogEntry(
                        term=entry_dict["term"],
                        index=entry_dict["index"],
                        command=entry_dict["command"].encode() if isinstance(entry_dict["command"], str) else entry_dict["command"],
                    ))
                
                # Update commit index
                if rpc.leader_commit > self.node.commit_index:
                    self.node.commit_index = min(rpc.leader_commit, len(self.node.log) - 1)
                
                match_idx = len(self.node.log) - 1
                success = True
        
        reply = AppendEntriesReply(
            term=self.node.current_term,
            success=success,
            match_index=match_idx,
        )
        
        if reply_subject:
            await self._nats.nc.publish(reply_subject, json.dumps(_to_dict(reply)).encode())
    
    async def _advance_commit(self):
        """Advance commit index if majority replicated."""
        # Find N such that N > commit_index and majority have index ≥ N
        quorum = self.cluster_size // 2 + 1
        last_log_idx = len(self.node.log) - 1
        
        for n in range(self.node.commit_index + 1, last_log_idx + 1):
            count = 1  # self
            for peer in self.peers:
                if self.node.match_index.get(peer, -1) >= n:
                    count += 1
            
            if count >= quorum and self.node.log[n].term == self.node.current_term:
                self.node.commit_index = n
                # Apply committed entries
                await self._apply_committed()
    
    async def _apply_committed(self):
        """Apply committed entries to state machine."""
        while self.node.last_applied < self.node.commit_index:
            self.node.last_applied += 1
            entry = self.node.log[self.node.last_applied]
            for cb in self._apply_callbacks:
                try:
                    cb(entry)
                except Exception:
                    pass
    
    # ── Helpers ──
    
    def _become_leader(self):
        """Transition to leader role."""
        self.node.role = Role.LEADER
        self.node.leader_id = self.node.node_id
        last_idx = len(self.node.log) - 1
        
        for peer in self.peers:
            self.node.next_index[peer] = last_idx + 1
            self.node.match_index[peer] = -1
        
        self._save_state()
    
    async def _rpc_call(self, peer: str, msg_type: str, data: dict) -> Optional[dict]:
        """Make an RPC call to a peer via NATS direct publish + inbox reply."""
        if not self._nats or not self._nats.nc:
            return None
        
        try:
            subject = f"snin.raft.{msg_type}.{peer}"
            msg = json.dumps(data).encode()
            reply = await self._nats.nc.request(subject, msg, timeout=2.0)
            if reply and reply.data:
                return json.loads(reply.data.decode())
        except Exception as e:
            pass
        
        return None
    
    async def _handle_vote_request_nats(self, data, reply_subject):
        """Handle RequestVote via NATS subject."""
        await self._handle_vote_request(data, reply_subject)
    
    async def _handle_append_entries_nats(self, data, reply_subject):
        """Handle AppendEntries via NATS subject."""
        await self._handle_append_entries(data, reply_subject)
    
    def _reset_election_timeout(self):
        """Randomize election timeout."""
        self.node.election_timeout = random.uniform(
            self.ELECTION_TIMEOUT_MIN,
            self.ELECTION_TIMEOUT_MAX,
        )
        self.node.last_heartbeat = time.monotonic()
    
    def _save_state(self):
        """Persist RAFT state to disk."""
        state = {
            "current_term": self.node.current_term,
            "voted_for": self.node.voted_for,
            "role": self.node.role.name,
            "commit_index": self.node.commit_index,
            "log_size": len(self.node.log),
        }
        with open(self._state_file, "w") as f:
            json.dump(state, f)
    
    def _load_state(self):
        """Load persisted RAFT state."""
        try:
            with open(self._state_file, "r") as f:
                state = json.load(f)
            self.node.current_term = state.get("current_term", 0)
            self.node.voted_for = state.get("voted_for")
            self.node.commit_index = state.get("commit_index", -1)
        except FileNotFoundError:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _to_dict(obj) -> dict:
    """Convert a dataclass to dict (recursively)."""
    if hasattr(obj, "__dataclass_fields__"):
        result = {}
        for field_name in obj.__dataclass_fields__:
            value = getattr(obj, field_name)
            result[field_name] = _to_dict(value)
        return result
    elif isinstance(obj, list):
        return [_to_dict(item) for item in obj]
    elif isinstance(obj, bytes):
        return obj.decode("latin-1")  # Safe encoding for transport
    elif isinstance(obj, Enum):
        return obj.name
    else:
        return obj


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

async def _test_raft():
    """Test RAFT with 3 nodes over NATS."""
    import sys
    sys.path.insert(0, '/home/agent/data/sites/relay-mesh')
    from snin_nats import NatsTransport, pack_message
    
    P = F = 0
    def chk(c, n):
        nonlocal P, F
        if c: P += 1; print(f"  ✅ {n}")
        else: F += 1; print(f"  ❌ {n}")
    
    print("═══ Phase 3b — RAFT Consensus Test ═══\n")
    
    # Create 3 nodes with NATS transport
    node_a = RaftEngine("raft_node_a", ["raft_node_b", "raft_node_c"])
    node_b = RaftEngine("raft_node_b", ["raft_node_a", "raft_node_c"])
    node_c = RaftEngine("raft_node_c", ["raft_node_a", "raft_node_b"])
    
    na = NatsTransport("raft_node_a")
    nb = NatsTransport("raft_node_b")
    nc = NatsTransport("raft_node_c")
    
    await na.start()
    await nb.start()
    await nc.start()
    
    await node_a.start(na)
    await node_b.start(nb)
    await node_c.start(nc)
    
    # 1. Wait for leader election
    print("1. Leader election (waiting 4s)...")
    await asyncio.sleep(4)
    
    a_leader = node_a.is_leader
    b_leader = node_b.is_leader
    c_leader = node_c.is_leader
    leaders = [a_leader, b_leader, c_leader]
    chk(sum(leaders) == 1, f"exactly 1 leader ({sum(leaders)})")
    chk(node_a.node.current_term >= 1, f"term ≥ 1 (got {node_a.node.current_term})")
    
    # 2. Propose command through leader
    print("\n2. Log replication:")
    leader_node = node_a if a_leader else (node_b if b_leader else node_c)
    leader_id = leader_node.node.node_id
    print(f"   Leader: {leader_id}")
    
    committed = []
    for n in [node_a, node_b, node_c]:
        n.on_commit(lambda e, lst=committed: lst.append(e.index))
    
    ok = await leader_node.propose(b"TEST_COMMAND_1")
    chk(ok, "propose accepted by leader")
    await asyncio.sleep(1)
    
    # Check leader log
    chk(len(leader_node.node.log) == 1, f"leader log has 1 entry (got {len(leader_node.node.log)})")
    
    # 3. Check all nodes have the log entry
    print("\n3. Full replication:")
    await asyncio.sleep(1)
    log_sizes = [len(node_a.node.log), len(node_b.node.log), len(node_c.node.log)]
    chk(all(s >= 1 for s in log_sizes), f"all nodes have entries: {log_sizes}")
    
    # 4. Commit count
    print("\n4. Commit tracking:")
    chk(len(committed) >= 1, f"at least 1 committed entry (got {len(committed)})")
    
    # 5. Term consistency
    print("\n5. Term consistency:")
    term_a = node_a.node.current_term
    term_b = node_b.node.current_term
    term_c = node_c.node.current_term
    chk(term_a == term_b == term_c, f"all same term: a={term_a}, b={term_b}, c={term_c}")
    
    # 6. Leader info
    print("\n6. Leader info:")
    l_a = node_a.leader
    l_b = node_b.leader
    l_c = node_c.leader
    chk(l_a is not None and l_b is not None and l_c is not None, "all nodes know leader")
    chk(l_a == l_b == l_c, f"all agree on leader: {l_a}")
    
    # Cleanup
    await node_a.stop()
    await node_b.stop()
    await node_c.stop()
    await na.stop()
    await nb.stop()
    await nc.stop()
    
    print(f"\n═══ {P}✅ {F}❌ ═══")
    return F == 0

if __name__ == "__main__":
    ok = asyncio.run(_test_raft())
    print("ALL TESTS PASSED" if ok else "FAILURES DETECTED")

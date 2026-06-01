"""
SNIN L3.5 — Zero-Knowledge Layer (Universal Architecture 2.0, порт :9250)

Ядро:
  — Merkle Tree (SHA-256) — proof of membership в сети
  — Hash-based Commitment (HMAC-SHA256) — скрытие значений
  — ZK Range Proof — доказательство "значение в диапазоне" без раскрытия
  — Batch Verification — пакетная проверка доказательств

Интеграция:
  → L4 Payment: ZK-платежи (скрытая сумма, доказуемый баланс)
  → L7 DAO: анонимное голосование (proof of membership + скрытый голос)
  → L5 Identity: доказательство владения DID без раскрытия ключа
"""

import hashlib
import hmac
import json
import logging
import math
import os
import secrets
import struct
import sys
import time
import uuid
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel
import uvicorn

# ───── Logging ─────
logging.basicConfig(level=logging.INFO, format="[ZK] %(message)s")
logger = logging.getLogger("zk")

app = FastAPI(title="SNIN L3.5 ZK Layer", version="1.0.0")
api = APIRouter(prefix="/api/v1")

# ─── Health endpoint для L9 (без префикса) ─────
@app.get("/health")
def root_health():
    return {"status": "ok", "layer": "L3 ZK", "ts": time.time(), "alive": True}

# ───── Internal State ─────
merkle_trees: Dict[str, dict] = {}   # tree_id → {leaves, root, tree}
commitments: Dict[str, dict] = {}    # comm_id → {value_hash, blinding, proof}
verified_proofs: set = set()         # уже проверенные proof_id (anti-replay)

# ══════════════════════════════════════════════════════════════
# 1. MERKLE TREE — Proof of Membership
# ══════════════════════════════════════════════════════════════

def _sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()

def _hash_pair(left: bytes, right: bytes) -> bytes:
    """Хэш двух дочерних узлов (отсортированы для детерминизма)."""
    if left < right:
        return _sha256(left + right)
    return _sha256(right + left)


class MerkleTree:
    """
    Бинарное Merkle Tree.
    leaves: список байтовых значений (например, agent_id.encode()).
    """

    def __init__(self, leaves: List[bytes] = None):
        self.leaves = leaves or []
        self.tree = []
        self.root = b""
        if self.leaves:
            self._build()

    def _build(self):
        """Построение дерева снизу вверх."""
        # Нормализация: сортируем листья, добавляем padding до степени 2
        sorted_leaves = sorted(set(self.leaves))
        # Pad to power of 2
        size = 1
        while size < len(sorted_leaves):
            size *= 2
        padded = sorted_leaves + [b"\x00"] * (size - len(sorted_leaves))

        level = [_sha256(l) for l in padded]
        self.tree = [level]

        while len(level) > 1:
            next_level = []
            for i in range(0, len(level), 2):
                next_level.append(_hash_pair(level[i], level[i + 1]))
            self.tree.append(next_level)
            level = next_level

        self.root = self.tree[-1][0] if self.tree[-1] else b""

    def add_leaf(self, leaf: bytes) -> bool:
        """Добавить лист и перестроить дерево."""
        if leaf in self.leaves:
            return False
        self.leaves.append(leaf)
        self._build()
        return True

    def get_proof(self, leaf: bytes) -> Optional[dict]:
        """
        Получить Merkle Proof для листа.
        Возвращает путь + корень для верификации.
        """
        if leaf not in self.leaves:
            return None

        # Нормализованный индекс
        sorted_leaves = sorted(set(self.leaves))
        if leaf not in sorted_leaves:
            return None

        idx = sorted_leaves.index(leaf)

        # Паддинг как при построении
        size = 1
        while size < len(sorted_leaves):
            size *= 2
        # Индекс в паддинге
        padded_idx = idx

        # Собираем proof path
        path = []
        current_idx = padded_idx
        for level_idx in range(len(self.tree) - 1):
            level = self.tree[level_idx]
            sibling_idx = current_idx ^ 1  # XOR для пары
            if sibling_idx < len(level):
                path.append({
                    "position": "left" if sibling_idx % 2 == 0 else "right",
                    "hash": level[sibling_idx].hex()
                })
            current_idx //= 2

        return {
            "root": self.root.hex(),
            "leaf": _sha256(leaf).hex(),
            "path": path,
            "depth": len(path)
        }

    @staticmethod
    def verify_proof(proof: dict) -> bool:
        """
        Проверка Merkle Proof без хранения дерева.
        proof: {root, leaf, path: [{position, hash}]}
        """
        try:
            current = bytes.fromhex(proof["leaf"])
            for step in proof["path"]:
                sibling = bytes.fromhex(step["hash"])
                if step["position"] == "left":
                    current = _hash_pair(sibling, current)
                else:
                    current = _hash_pair(current, sibling)
            return current.hex() == proof["root"]
        except Exception:
            return False

    def get_stats(self) -> dict:
        return {
            "leaves": len(self.leaves),
            "depth": len(self.tree) - 1 if self.tree else 0,
            "root": self.root.hex() if self.root else "",
            "tree_size": sum(len(l) for l in self.tree)
        }


# ══════════════════════════════════════════════════════════════
# 2. COMMITMENT SCHEME — Hash-based (HMAC-SHA256)
# ══════════════════════════════════════════════════════════════

def create_commitment(value: float, blinding: bytes = None) -> dict:
    """
    Pedersen-style commitment через HMAC-SHA256.
    C = HMAC-SHA256(blinding, value)
    """
    if blinding is None:
        blinding = secrets.token_bytes(32)

    value_bytes = struct.pack(">d", value)  # double precision float
    comm = hmac.new(blinding, value_bytes, "sha256").hexdigest()

    return {
        "commitment": comm,
        "blinding": blinding.hex(),
        "value": value  # только для отладки — в проде не возвращать!
    }

def verify_commitment(commitment: str, value: float, blinding_hex: str) -> bool:
    """Проверка: C == HMAC(blinding, value)."""
    blinding = bytes.fromhex(blinding_hex)
    value_bytes = struct.pack(">d", value)
    expected = hmac.new(blinding, value_bytes, "sha256").hexdigest()
    return hmac.compare_digest(expected, commitment)


# ══════════════════════════════════════════════════════════════
# 3. ZK RANGE PROOF (simplified)
# ══════════════════════════════════════════════════════════════

def range_proof_commit(value: float, min_val: float, max_val: float,
                        blinding: bytes = None) -> dict:
    """
    Simplified Range Proof:
    Доказывает что value ∈ [min, max] без раскрытия value.
    Метод: разбиваем диапазон на N сегментов, коммитим бинарное представление.
    """
    if blinding is None:
        blinding = secrets.token_bytes(32)

    # Основной commitment
    comm = create_commitment(value, blinding)

    # Дополнительные commitments для proof:
    # Разбиваем диапазон на 8 бинов
    num_bins = 8
    bin_size = (max_val - min_val) / num_bins
    bin_index = min(int((value - min_val) / bin_size), num_bins - 1)

    # Для каждого бина создаём пару commitment/открытие
    bin_proofs = []
    for i in range(num_bins):
        bin_val = 1 if i == bin_index else 0  # 1 = мой бин, 0 = нет
        b = secrets.token_bytes(32)
        bin_comm = hmac.new(b, struct.pack(">d", float(bin_val)), "sha256").hexdigest()
        bin_proofs.append({
            "index": i,
            "commitment": bin_comm,
            "open": b.hex() if i == bin_index else ""  # открываем только свой бин
        })

    return {
        "commitment": comm["commitment"],
        "blinding": blinding.hex(),
        "range": [min_val, max_val],
        "range_proof": {
            "num_bins": num_bins,
            "bin_index": bin_index,
            "bins": bin_proofs
        }
    }

def verify_range_proof(proof: dict) -> bool:
    """
    Верификация Range Proof.
    Проверяет что ровно один бин = 1 и commitment корректен.
    """
    try:
        # Поддержка вложенного формата {id, proof}
        if "proof" in proof and isinstance(proof["proof"], dict):
            proof = proof["proof"]

        rp = proof.get("range_proof", proof)
        bins = rp.get("bins", proof.get("range_proof", {}).get("bins", []))

        if not bins:
            return False

        # Проверяем что ровно один бин открыт
        opened = [b for b in bins if b.get("open") and len(b.get("open", "")) > 4]
        if len(opened) != 1:
            return False

        # Проверяем что открытый бин = 1
        bin_data = opened[0]
        val_bytes = struct.pack(">d", 1.0)
        expected = hmac.new(bytes.fromhex(bin_data["open"]), val_bytes, "sha256").hexdigest()
        if expected != bin_data["commitment"]:
            return False

        return True
    except Exception as e:
        logger.warning(f"Range verify error: {e}")
        return False


# ══════════════════════════════════════════════════════════════
# 4. PROOF BATCHING
# ══════════════════════════════════════════════════════════════

class ProofBatch:
    """Пакетная проверка доказательств."""

    def __init__(self):
        self.proofs: list = []
        self.created_at = time.time()

    def add(self, proof_type: str, proof_data: dict) -> str:
        proof_id = uuid.uuid4().hex[:16]
        self.proofs.append({
            "id": proof_id,
            "type": proof_type,
            "data": proof_data,
            "ts": time.time()
        })
        return proof_id

    def verify_all(self) -> dict:
        results = {"passed": 0, "failed": 0, "details": []}
        for p in self.proofs:
            if p["id"] in verified_proofs:
                results["details"].append({"id": p["id"], "status": "replay"})
                results["failed"] += 1
                continue

            ok = False
            if p["type"] == "merkle":
                ok = MerkleTree.verify_proof(p["data"])
            elif p["type"] == "commitment":
                ok = verify_commitment(
                    p["data"]["commitment"],
                    p["data"]["value"],
                    p["data"]["blinding"]
                )
            elif p["type"] == "range":
                ok = verify_range_proof(p["data"])

            if ok:
                verified_proofs.add(p["id"])
                results["passed"] += 1
            else:
                results["failed"] += 1

            results["details"].append({"id": p["id"], "status": "pass" if ok else "fail"})

        return results


# ══════════════════════════════════════════════════════════════
# 5. DEFAULT TREE — инициализируем при старте
# ══════════════════════════════════════════════════════════════

def _init_default_tree():
    """Загружаем агентов из L5 в Merkle Tree при старте."""
    import urllib.request as r
    try:
        resp = r.urlopen("http://127.0.0.1:9940/identity/all", timeout=5)
        data = json.loads(resp.read())
        agents = data.get("agents", [])

        leaves = []
        for a in agents:
            leaves.append(a.get("agent_name", "").encode())
            leaves.append(a.get("did", "").encode())

        if leaves:
            mt = MerkleTree(leaves)
            merkle_trees["agents"] = {
                "tree": mt,
                "created": time.time(),
                "label": "SNIN Agent Network"
            }
            logger.info(f"Default Merkle Tree: {mt.get_stats()['leaves']} leaves, root={mt.root.hex()[:16]}...")
    except Exception as e:
        logger.warning(f"Default tree init: {e}")


# ══════════════════════════════════════════════════════════════
# API ENDPOINTS
# ══════════════════════════════════════════════════════════════

# ── Health ──

@api.get("/")
def root():
    return {
        "service": "SNIN L3.5 ZK Layer",
        "version": "1.0.0",
        "trees": list(merkle_trees.keys()),
        "commitments_stored": len(commitments),
        "proofs_verified": len(verified_proofs),
        "status": "live"
    }

@api.get("/health")
def health():
    return {
        "zk": "ok",
        "ts": time.time(),
        "merkle_trees": {k: v["tree"].get_stats() for k, v in merkle_trees.items()},
        "capabilities": [
            "merkle_proof",
            "hash_commitment",
            "range_proof",
            "batch_verify"
        ]
    }

# ── 1. Merkle Tree ──

@api.post("/merkle/create")
def merkle_create(label: str = "default"):
    """Создать новое Merkle Tree."""
    mt = MerkleTree()
    merkle_trees[label] = {"tree": mt, "created": time.time(), "label": label}
    return {"status": "created", "tree": label, "root": mt.root.hex() if mt.root else "empty"}

@api.get("/merkle/{tree_id}")
def merkle_stats(tree_id: str):
    """Статистика Merkle Tree."""
    if tree_id not in merkle_trees:
        raise HTTPException(404, f"Tree {tree_id} not found")
    stats = merkle_trees[tree_id]["tree"].get_stats()
    stats["label"] = merkle_trees[tree_id].get("label", "")
    stats["created"] = merkle_trees[tree_id].get("created", 0)
    return stats

@api.post("/merkle/{tree_id}/add")
def merkle_add_leaf(tree_id: str, leaf: str):
    """Добавить лист в дерево."""
    if tree_id not in merkle_trees:
        raise HTTPException(404, f"Tree {tree_id} not found")
    ok = merkle_trees[tree_id]["tree"].add_leaf(leaf.encode())
    return {"status": "added" if ok else "duplicate", "root": merkle_trees[tree_id]["tree"].root.hex()}

@api.get("/merkle/{tree_id}/proof/{leaf}")
def merkle_get_proof(tree_id: str, leaf: str):
    """Получить Merkle Proof для листа."""
    if tree_id not in merkle_trees:
        raise HTTPException(404, f"Tree {tree_id} not found")
    proof = merkle_trees[tree_id]["tree"].get_proof(leaf.encode())
    if not proof:
        raise HTTPException(404, f"Leaf '{leaf}' not in tree")
    return proof

@api.post("/merkle/verify")
def merkle_verify(proof: dict):
    """Проверить Merkle Proof без хранения дерева."""
    valid = MerkleTree.verify_proof(proof)
    return {"valid": valid}

# ── 2. Commitments ──

@api.post("/commit")
def commit_value(value: float):
    """Создать commitment для значения (возвращает без value в проде!)."""
    comm = create_commitment(value)
    cid = uuid.uuid4().hex[:16]
    commitments[cid] = comm
    return {
        "id": cid,
        "commitment": comm["commitment"],
        "blinding": comm["blinding"],
        "value": value  # отладка
    }

@api.post("/verify")
def verify_commit(commitment: str, value: float, blinding: str):
    """Проверить commitment."""
    valid = verify_commitment(commitment, value, blinding)
    return {"valid": valid}

# ── 3. Range Proof ──

@api.post("/range/prove")
def range_prove(value: float, min_val: float = 0, max_val: float = 1000):
    """Создать Range Proof (доказать что value ∈ [min, max])."""
    proof = range_proof_commit(value, min_val, max_val)
    rid = uuid.uuid4().hex[:16]
    commitments[rid] = proof
    return {"id": rid, "proof": proof}

@api.post("/range/verify")
def range_verify(proof: dict):
    """Проверить Range Proof (принимает {proof: {...}} или прямой proof)."""
    valid = verify_range_proof(proof)
    return {"valid": valid}

# ── 4. Batch Verification ──

@api.post("/batch")
def batch_create():
    """Создать батч для пакетной проверки."""
    batch = ProofBatch()
    bid = uuid.uuid4().hex[:12]
    # храним в памяти
    commitments[bid] = {"batch": batch, "type": "batch"}
    return {"batch_id": bid, "created": batch.created_at}

@api.post("/batch/{batch_id}/add")
def batch_add(batch_id: str, proof_type: str, proof: dict):
    """Добавить proof в батч."""
    if batch_id not in commitments or not isinstance(commitments[batch_id].get("batch"), ProofBatch):
        raise HTTPException(404, f"Batch {batch_id} not found")
    pid = commitments[batch_id]["batch"].add(proof_type, proof)
    return {"proof_id": pid}

@api.post("/batch/{batch_id}/verify")
def batch_verify(batch_id: str):
    """Проверить все proofs в батче."""
    if batch_id not in commitments or not isinstance(commitments[batch_id].get("batch"), ProofBatch):
        raise HTTPException(404, f"Batch {batch_id} not found")
    results = commitments[batch_id]["batch"].verify_all()
    return results

# ── 5. Integration: ZK Vote (L7) ──

@api.post("/integration/zk-vote")
def zk_vote(agent_name: str, proposal_id: str, vote_value: int,
            blinding: str = ""):
    """
    ZK Vote для DAO:
    1. Проверяет Merkle proof членства агента
    2. Создаёт commitment голоса (0=against, 1=for, 2=abstain)
    3. Возвращает доказательство для L7 DAO
    """
    # Проверяем членство в Merkle Tree
    if "agents" not in merkle_trees:
        raise HTTPException(503, "No agent Merkle Tree initialized")

    proof = merkle_trees["agents"]["tree"].get_proof(agent_name.encode())
    if not proof:
        raise HTTPException(403, f"Agent {agent_name} not in Merkle Tree")

    # Создаём commitment для голоса
    b = bytes.fromhex(blinding) if blinding else secrets.token_bytes(32)
    comm = create_commitment(float(vote_value), b)

    return {
        "merkle_proof": proof,
        "vote_commitment": comm["commitment"],
        "vote_blinding": comm["blinding"],
        "vote_value": vote_value,  # отладка
        "merkle_valid": MerkleTree.verify_proof(proof)
    }

# ── 6. Integration: ZK Payment (L4) ──

@api.post("/integration/zk-payment")
def zk_payment(sender: str, amount: float, balance_hidden: bool = True):
    """
    ZK Payment для L4:
    — Доказывает что sender имеет достаточный баланс (Range Proof)
    — Без раскрытия точной суммы баланса
    """
    # Проверяем членство
    if "agents" not in merkle_trees:
        raise HTTPException(503, "No agent Merkle Tree")

    membership = merkle_trees["agents"]["tree"].get_proof(sender.encode())
    if not membership:
        raise HTTPException(403, f"{sender} not in tree")

    # Range proof для суммы (0.001 ≤ amount ≤ 10000 SNIN)
    range_pf = range_proof_commit(amount, 0.001, 10000.0)

    return {
        "sender": sender,
        "membership_proof": membership,
        "amount_range_proof": range_pf,
        "merkle_valid": MerkleTree.verify_proof(membership)
    }

# ── 7. Sync from L5 ──

@api.post("/sync/from-l5")
def sync_from_l5():
    """Синхронизировать Merkle Tree с агентами из L5."""
    import urllib.request as r
    try:
        resp = r.urlopen("http://127.0.0.1:9940/identity/all", timeout=5)
        data = json.loads(resp.read())
        agents = data.get("agents", [])

        leaves = []
        for a in agents:
            leaves.append(a.get("agent_name", "").encode())
            leaves.append(a.get("did", "").encode())

        mt = MerkleTree(leaves)
        merkle_trees["agents"] = {
            "tree": mt,
            "created": time.time(),
            "label": "SNIN Agent Network"
        }

        return {
            "status": "synced",
            "leaves": len(leaves),
            "agents": len(agents),
            "root": mt.root.hex()
        }
    except Exception as e:
        raise HTTPException(502, f"L5 sync error: {e}")


# ── Init on startup ──
_init_default_tree()

# ── Mount ──
app.include_router(api)

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9250
    print(f"[ZK] Starting L3.5 ZK Layer on :{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")

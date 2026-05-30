"""Trust Graph — социальный граф доверия SNIN L5.

Строится из аттестаций: кто кому, когда и с какой ролью выдал VC.
Используется для:
  - Визуализация связей на дашборде
  - Расчёт репутации через граф (PageRank-like)
  - Обнаружение изолированных агентов
"""

import json
import os
import time
from collections import defaultdict

TRUST_DB = os.path.expanduser("~/data/sites/relay-mesh/identities/attestations.json")


def load_attestations() -> list[dict]:
    """Загружает все аттестации из файла."""
    if not os.path.isfile(TRUST_DB):
        return []
    try:
        with open(TRUST_DB) as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return list(data.values())
            return []
    except (json.JSONDecodeError, IOError):
        return []


def build_graph() -> dict:
    """Строит граф доверия: {agent: {trusted_agent: weight, ...}, ...}"""
    attestations = load_attestations()
    graph = defaultdict(dict)

    for att in attestations:
        issuer = att.get("issuer", att.get("agent_name", "unknown"))
        target = att.get("target_did", att.get("target", "unknown"))
        role = att.get("role", "agent")
        weight = att.get("weight", 1.0)

        # Нормализация веса по роли
        role_weights = {
            "admin": 1.0,
            "operator": 0.8,
            "agent": 0.5,
            "observer": 0.2,
            "guest": 0.1,
        }
        w = weight * role_weights.get(role, 0.5)
        graph[issuer][target] = max(graph[issuer].get(target, 0), w)

    return dict(graph)


def calculate_trust_scores(graph: dict, iterations: int = 5) -> dict:
    """PageRank-like расчёт доверия на графе."""
    if not graph:
        return {}

    nodes = set(graph.keys())
    for edges in graph.values():
        nodes.update(edges.keys())

    if not nodes:
        return {}

    scores = {n: 1.0 / len(nodes) for n in nodes}
    damping = 0.85

    for _ in range(iterations):
        new_scores = {}
        for node in nodes:
            incoming = 0.0
            for issuer, edges in graph.items():
                if node in edges:
                    out_sum = sum(edges.values())
                    if out_sum > 0:
                        incoming += scores.get(issuer, 0) * (edges[node] / out_sum)
            new_scores[node] = (1 - damping) / len(nodes) + damping * incoming
        scores = new_scores

    # Нормализация к 0-1
    max_score = max(scores.values()) if scores else 1
    return {n: round(s / max_score, 4) for n, s in scores.items()}


def get_trust_metrics() -> dict:
    """Возвращает метрики графа доверия."""
    graph = build_graph()
    scores = calculate_trust_scores(graph)

    # Статистика
    nodes = set(graph.keys())
    for edges in graph.values():
        nodes.update(edges.keys())

    edges_count = sum(len(e) for e in graph.values())
    isolated = [n for n in nodes if n not in graph or not graph[n]]

    return {
        "nodes": len(nodes),
        "edges": edges_count,
        "isolated_agents": isolated,
        "trusted_agents": sorted(
            [(n, s) for n, s in scores.items()],
            key=lambda x: -x[1]
        )[:20],
        "graph": {k: list(v.keys()) for k, v in graph.items()},
    }


def get_agent_trust(agent_name: str) -> dict:
    """Доверие конкретного агента: кого аттестовал, кто его."""
    graph = build_graph()
    scores = calculate_trust_scores(graph)

    issued = graph.get(agent_name, {})
    received = []
    for issuer, edges in graph.items():
        if agent_name in edges:
            received.append({"from": issuer, "weight": edges[agent_name]})

    return {
        "agent": agent_name,
        "trust_score": scores.get(agent_name, 0),
        "attestations_given": [{"to": t, "weight": w} for t, w in issued.items()],
        "attestations_received": received,
    }

"""
SNIN PROOF MESH — Фаза 1: тесты instance identity / sensor.

Покрытие: PID reuse → разные instance_id; стабильность для того же процесса;
классификация (SIG_MATCH confirmed/inferred, REG_MATCH, arch:*, PARENT_CHAIN,
UNKNOWN); запись scan_instances в БД. Спека: SNIN_PROOF_MESH_SPEC.md, Фаза 1.
"""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # корень проекта

from proof_mesh import db, sensor  # noqa: E402

REGISTRY = {"cryter": "cryter", "archivist": "archivist", "remora": "remora"}


@pytest.fixture
def audit_db(tmp_path):
    path = str(tmp_path / "snin_audit.db")
    db.init_db(path)
    return path


# ── instance_id ──────────────────────────────────────────────────────────────

def test_pid_reuse_gives_different_instance():
    """Один pid, разные start_time (перезапуск) → РАЗНЫЕ instance_id."""
    a = sensor.instance_id(100, 123.4, "0::/")
    b = sensor.instance_id(100, 999.9, "0::/")
    assert a != b


def test_same_process_same_instance():
    a = sensor.instance_id(100, 123.4, "0::/")
    b = sensor.instance_id(100, 123.4, "0::/")
    assert a == b


def test_instance_id_len_and_hex():
    iid = sensor.instance_id(1, 1.0, "/")
    assert len(iid) == 16
    assert all(ch in "0123456789abcdef" for ch in iid)


def test_cgroup_changes_instance():
    a = sensor.instance_id(100, 123.4, "0::/docker/a")
    b = sensor.instance_id(100, 123.4, "0::/docker/b")
    assert a != b


# ── классификация ───────────────────────────────────────────────────────────

def test_known_signature_confirmed():
    agent, attr, ev = sensor.classify("python3 src/core/cryter_daemon.py", REGISTRY)
    assert agent == "cryter" and attr == "confirmed" and ev == "SIG_MATCH"


def test_signature_not_in_registry_inferred():
    # v2bot_agent нет в chrono-реестре → inferred, но сигнатура известна
    agent, attr, ev = sensor.classify("python3 v2bot_daemon_v2.py", REGISTRY)
    assert agent == "v2bot_agent" and attr == "inferred" and ev == "SIG_MATCH"


def test_registry_name_match():
    agent, attr, ev = sensor.classify(
        "python3 -m src.core.archivist --run", REGISTRY
    )
    assert agent == "archivist" and attr == "confirmed" and ev == "REG_MATCH"


def test_arch_process_not_agent():
    agent, attr, ev = sensor.classify("python3 relay_server_v2.py", REGISTRY)
    assert agent == "arch:relay server" and attr == "confirmed"
    assert ev == "SIG_MATCH"


def test_parent_chain():
    agent, attr, ev = sensor.classify(
        "python3 -u src/core/child_worker.py",
        REGISTRY,
        parent_cmdline="bash remora_runner.sh",
    )
    assert agent == "remora" and attr == "inferred" and ev == "PARENT_CHAIN"


def test_unknown_unattributed():
    agent, attr, ev = sensor.classify("python3 weird_thing.py --foo", REGISTRY)
    assert agent is None and attr == "unattributed" and ev == "UNKNOWN"


def test_empty_cmdline_unattributed():
    agent, attr, ev = sensor.classify("", REGISTRY)
    assert agent is None and attr == "unattributed"


# ── запись в БД ─────────────────────────────────────────────────────────────

def test_scan_instances_writes_and_filters(audit_db):
    procs = [
        {"pid": 515, "start_time": 1700000100.0, "cgroup": "0::/",
         "cmdline": "python3 -u src/core/cryter_daemon.py", "ppid": 1},
        {"pid": 999, "start_time": 1700000200.0, "cgroup": "0::/",
         "cmdline": "python3 mystery_agent.py", "ppid": 1},
    ]
    res = sensor.scan_instances(audit_db, procs, REGISTRY)
    assert len(res) == 2
    cryter = [r for r in res if r["agent_id"] == "cryter"]
    assert cryter and cryter[0]["attribution"] == "confirmed"
    unknown = [r for r in res if r["attribution"] == "unattributed"]
    assert len(unknown) == 1 and unknown[0]["evidence_code"] == "UNKNOWN"

    with sqlite3.connect(audit_db) as c:
        rows = c.execute(
            "SELECT agent_id, instance_id FROM agent_instances ORDER BY pid"
        ).fetchall()
    assert len(rows) == 2
    # instance_id в БД совпадает с вычисленным
    for r in res:
        assert r["instance_id"] in [x[1] for x in rows]


def test_scan_instances_upsert_no_dupes(audit_db):
    procs = [
        {"pid": 515, "start_time": 1700000100.0, "cgroup": "0::/",
         "cmdline": "python3 -u src/core/cryter_daemon.py", "ppid": 1},
    ]
    sensor.scan_instances(audit_db, procs, REGISTRY)
    sensor.scan_instances(audit_db, procs, REGISTRY)  # повторный проход
    with sqlite3.connect(audit_db) as c:
        n = c.execute("SELECT COUNT(*) FROM agent_instances").fetchone()[0]
    assert n == 1, "повторный скан не должен плодить дубли"


def test_load_registry_from_chrono():
    """Реальный реестр из chrono.db — 17 агентов (если БД на месте)."""
    reg = sensor.load_registry()
    if not reg:
        pytest.skip("chrono.db недоступна")
    assert "cryter" in reg.values()
    assert len(reg) >= 15

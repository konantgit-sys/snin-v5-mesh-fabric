"""
SNIN PROOF MESH — Фаза 1: Instance Identity (sensor.py).

Каждый процесс получает стабильный instance_id:
    instance_id = sha256("pid|start_time|cgroup")[:16]
start_time = время рождения процесса (btime + starttime/CLK_TCK из /proc/<pid>/stat).
PID reuse (перезапуск с тем же pid) даёт ДРУГОЙ instance_id — переиспользованный
pid не сливает разных агентов в одного.

Атрибуция (идея из AEGIS: не выдумывать владельца):
- confirmed  — процесс сопоставлен с агентом из agent_registry (chrono.db)
- inferred   — известная сигнатура процесса / родительская цепочка
- unattributed — реально не знаем → evidence_code=UNKNOWN, не гадаем

evidence_code: REG_MATCH (имя агента из реестра в cmdline),
SIG_MATCH (сигнатура процесса), PARENT_CHAIN (родитель опознан),
UNKNOWN (не опознан).

Правило разграничения: процессы АРХИТЕКТУРЫ (relay_server, smart_router и т.п.)
не называются агентами — им присваивается agent_id с префиксом "arch:".

Спека: SNIN_PROOF_MESH_SPEC.md. Фаза 1: done-when — для агентов реестра
определяется instance_id; PID reuse даёт разные id; unattributed только честно.
"""

import hashlib
import os
import sqlite3
from pathlib import Path

# Известные сигнатуры процессов → (agent_id, признаётся ли реестром)
# registry=None в кортеже означает «агент вне chrono-реестра» (inferred)
SIGNATURES = {
    "v2bot_daemon_v2.py": ("v2bot_agent", False),
    "cryter_daemon": ("cryter", True),
    "cryter_pulse": ("cryter", True),
    "main_v8": ("cryter", True),
    "scam_radar": ("cryter", True),
    "remora_runner": ("remora", False),
    "remora.py": ("remora", False),
    "rimora": ("remora", False),
}

# Паттерны архитектуры (НЕ агенты) — помечаются arch:*
ARCH_PATTERNS = (
    "relay_server", "smart_router", "content_router", "route_engine",
    "supervisor", "nostr_bridge", "external_gateway", "cross_mesh",
    "dao_mesh", "gateway.py", "api_gateway", "relay_gateway",
)

DEFAULT_REGISTRY_DB = "/home/agent/data/sites/chrono/chrono.db"


def _sha256(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def instance_id(pid: int, start_time: float, cgroup: str) -> str:
    """Стабильный идентификатор запуска процесса (чистая функция)."""
    return _sha256(f"{pid}|{start_time}|{cgroup}")[:16]


# ── чтение /proc ────────────────────────────────────────────────────────────

def _clk_tck() -> int:
    try:
        return os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    except (ValueError, OSError):
        return 100


def _btime() -> int:
    try:
        with open("/proc/stat") as f:
            for line in f:
                if line.startswith("btime"):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


def process_start_time(pid: int) -> float | None:
    """Время рождения процесса в epoch (btime + starttime/CLK_TCK)."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            data = f.read()
        # comm в скобках может содержать пробелы → режем по последней ')'
        rparen = data.rfind(")")
        if rparen == -1:
            return None
        fields = data[rparen + 2:].split()
        starttime = float(fields[19])  # поле 22: starttime (в тиках)
        return _btime() + starttime / _clk_tck()
    except (OSError, IndexError, ValueError):
        return None


def process_cgroup(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/cgroup") as f:
            return f.read().strip()
    except OSError:
        return ""


def process_cmdline(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return f.read().replace(b"\0", b" ").decode("utf-8", "replace").strip()
    except OSError:
        return ""


def process_ppid(pid: int) -> int | None:
    try:
        with open(f"/proc/{pid}/stat") as f:
            data = f.read()
        rparen = data.rfind(")")
        if rparen == -1:
            return None
        fields = data[rparen + 2:].split()
        return int(fields[1])  # поле 4: ppid
    except (OSError, IndexError, ValueError):
        return None


def list_pids() -> list[int]:
    pids = []
    try:
        for name in os.listdir("/proc"):
            if name.isdigit():
                pids.append(int(name))
    except OSError:
        pass
    return sorted(pids)


# ── реестр агентов ──────────────────────────────────────────────────────────

def load_registry(registry_db: str = DEFAULT_REGISTRY_DB) -> dict[str, str]:
    """name(lower) → agent_id из chrono.agent_registry."""
    registry: dict[str, str] = {}
    if not Path(registry_db).exists():
        return registry
    try:
        with sqlite3.connect(registry_db) as c:
            rows = c.execute("SELECT agent_id, name FROM agent_registry").fetchall()
        for agent_id, name in rows:
            if name:
                registry[name.strip().lower()] = agent_id
    except sqlite3.Error:
        pass
    return registry


# ── классификация ───────────────────────────────────────────────────────────

def classify(
    cmdline: str,
    registry: dict[str, str] | None = None,
    parent_cmdline: str = "",
) -> tuple[str | None, str, str]:
    """
    Возвращает (agent_id | None, attribution, evidence_code).
    - Сигнатура совпала и агент в реестре → confirmed/SIG_MATCH
    - Сигнатура совпала, агента нет в реестре → inferred/SIG_MATCH
    - Имя агента из реестра в cmdline → confirmed/REG_MATCH
    - Архитектурный процесс → "arch:xxx"/confirmed/SIG_MATCH
    - Родитель опознан → inferred/PARENT_CHAIN
    - Иначе → (None, unattributed, UNKNOWN)
    """
    cmd = cmdline.lower()

    for pattern, (agent, in_registry) in SIGNATURES.items():
        if pattern in cmd:
            if in_registry:
                return agent, "confirmed", "SIG_MATCH"
            return agent, "inferred", "SIG_MATCH"

    for pat in ARCH_PATTERNS:
        if pat in cmd:
            base = pat.replace("_", " ")
            return f"arch:{base}", "confirmed", "SIG_MATCH"

    if registry:
        for name, agent_id in registry.items():
            if name in cmd:
                return agent_id, "confirmed", "REG_MATCH"

    if parent_cmdline:
        parent_cmd = parent_cmdline.lower()
        for pattern, (agent, _in_reg) in SIGNATURES.items():
            if pattern in parent_cmd:
                return agent, "inferred", "PARENT_CHAIN"

    return None, "unattributed", "UNKNOWN"


# ── сбор и запись ───────────────────────────────────────────────────────────

def scan_instances(
    db_path: str,
    processes: list[dict],
    registry: dict[str, str] | None = None,
) -> list[dict]:
    """
    Классифицировать процессы и записать в agent_instances (upsert).
    processes: [{"pid", "start_time", "cgroup", "cmdline", "ppid"}].
    Возвращает список записей с instance_id/attribution.
    """
    registry = registry or {}
    cmd_by_pid = {p["pid"]: p.get("cmdline", "") for p in processes}
    results = []
    for p in processes:
        parent_cmd = cmd_by_pid.get(p.get("ppid") or -1, "")
        agent, attribution, evidence = classify(
            p.get("cmdline", ""), registry, parent_cmd
        )
        iid = instance_id(
            p["pid"], p.get("start_time", 0.0), p.get("cgroup", "")
        )
        record = {
            "instance_id": iid,
            "agent_id": agent or "",
            "pid": p["pid"],
            "start_time": p.get("start_time", 0.0),
            "cgroup": p.get("cgroup", ""),
            "cmdline": p.get("cmdline", ""),
            "attribution": attribution,
            "evidence_code": evidence,
        }
        _upsert(db_path, record)
        results.append(record)
    return results


def scan(db_path: str, registry_db: str = DEFAULT_REGISTRY_DB) -> list[dict]:
    """Полный проход: собрать процессы из /proc и записать в БД."""
    registry = load_registry(registry_db)
    processes = []
    for pid in list_pids():
        processes.append({
            "pid": pid,
            "start_time": process_start_time(pid) or 0.0,
            "cgroup": process_cgroup(pid),
            "cmdline": process_cmdline(pid),
            "ppid": process_ppid(pid),
        })
    return scan_instances(db_path, processes, registry)


def _upsert(db_path: str, rec: dict) -> None:
    """Прямая запись в agent_instances (без цикла через db.py)."""
    with sqlite3.connect(db_path) as c:
        c.execute(
            """INSERT INTO agent_instances
               (instance_id, agent_id, pid, start_time, cgroup, cmdline, first_seen, last_seen)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(instance_id) DO UPDATE SET
                 agent_id=excluded.agent_id, cmdline=excluded.cmdline,
                 last_seen=excluded.last_seen""",
            (rec["instance_id"], rec["agent_id"], rec["pid"], rec["start_time"],
             rec["cgroup"], rec["cmdline"], int(__import__("time").time()),
             int(__import__("time").time())),
        )


def report(results: list[dict]) -> str:
    """Человекочитаемый отчёт: агенты, инстансы, unattributed."""
    known = [r for r in results if r["attribution"] != "unattributed"]
    unknown = [r for r in results if r["attribution"] == "unattributed"]
    by_agent: dict[str, list] = {}
    for r in known:
        by_agent.setdefault(r["agent_id"], []).append(r)
    lines = [f"Процессов всего: {len(results)}",
             f"Опознано: {len(known)} | unattributed: {len(unknown)}"]
    for agent, insts in sorted(by_agent.items()):
        ev = {i["evidence_code"] for i in insts}
        lines.append(
            f"  {agent:<16} инстансов={len(insts)} evidence={','.join(sorted(ev))}"
        )
    if unknown:
        sample = [r["cmdline"][:50] for r in unknown[:5]]
        lines.append(f"  unattributed примеры: {sample}")
    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    db_path = sys.argv[1] if len(sys.argv) > 1 else "snin_audit.db"
    res = scan(db_path)
    print(report(res))

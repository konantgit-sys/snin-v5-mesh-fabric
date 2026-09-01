#!/usr/bin/env python3
"""
SNIN PORT AUDIT — сертификатор соответствия реестру.
Сверяет: эталон (SNIN_PORT_REGISTRY.json) ↔ реальность (/proc/net/tcp) ↔ намерение (port.txt).
Выход: отчёт «сертифицировано / расхождения». Exit 0 — чисто, 1 — есть расхождения.

Использование: python3 snin_port_audit.py
"""
import os, re, json, sys, datetime

SITES = "/home/agent/data/sites"
REG = "/home/agent/data/scripts/SNIN_PORT_REGISTRY.json"
try:
    MAP = {int(k): v for k, v in json.load(open(REG)).items()}
except FileNotFoundError:
    # сгенерировать при отсутствии
    import subprocess
    subprocess.run([sys.executable, "/home/agent/data/scripts/snin_port_registry.py"], check=True)
    MAP = {int(k): v for k, v in json.load(open(REG)).items()}

def listen_ports():
    ports = []
    for fn in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            for line in open(fn).read().splitlines()[1:]:
                p = line.split()
                if len(p) > 3 and int(p[3], 16) == 0x0A:
                    ports.append(int(p[1].split(":")[1], 16))
        except Exception:
            pass
    return sorted(set(ports))

def config_ports():
    """port.txt всех директорий: порт → [директории]"""
    from collections import defaultdict
    byp = defaultdict(list)
    for d in sorted(os.listdir(SITES)):
        pf = os.path.join(SITES, d, "port.txt")
        if os.path.exists(pf):
            p = open(pf).read().strip()
            if p:
                key = p.split(":")[-1] if ":" in p else p
                if key.isdigit():
                    byp[int(key)].append(d)
    return dict(byp)

def main():
    live = listen_ports()
    cfg = config_ports()
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"═══ SNIN PORT AUDIT · {now} ═══")
    print(f"Слушающих портов: {len(live)} · В реестре: {len(MAP)}")

    issues = []
    unknown = [p for p in live if p not in MAP]
    if unknown:
        issues.append(f"UNKNOWN: {len(unknown)} портов слушаются, но НЕ в реестре: {sorted(unknown)}")

    # cert-порты, которые должны слушаться
    dead = []
    for p, (svc, f, purpose, layer, st) in sorted(MAP.items()):
        if st == "cert" and p not in live and p not in (8080,):
            dead.append((p, svc))
    if dead:
        issues.append(f"DOWN: {len(dead)} сертифицированных портов НЕ слушаются: {dead}")

    # конфликты конфигов
    conflicts = {p: v for p, v in cfg.items() if len(v) > 1 and p in live}
    if conflicts:
        for p, dirs in sorted(conflicts.items()):
            issues.append(f"CONFLICT: :{p} назначен {len(dirs)} сервисам: {', '.join(dirs)}")

    # документ-расхождения (статическая секция)
    doc_issues = [
        "External Gateway: документы :5377/:9951, стандарт :9931",
        "Mesh API: документы :9907/:9908/:9911/:9912, стандарт :9933",
        "Supervisor: документ :9900, реально graphify на :9900 — supervisor перенести на :9909",
        "smart_router в supervisor.conf = router_api.py, стандарт = smart_router.py",
    ]
    print()
    print("═══ СТАТУС ═══")
    cert_ok = sum(1 for p in live if p in MAP and MAP[p][4] == "cert")
    print(f"✅ Сертифицировано и слушается: {cert_ok}")
    print(f"⚠️  Конфликтов в реестре: {sum(1 for v in MAP.values() if v[4]=='conflict')}")
    print(f"❓ Не идентифицировано: {len(unknown)}")
    print()
    print("═══ РАСХОЖДЕНИЯ ═══")
    if not issues:
        print("✅ Расхождений нет — система соответствует стандарту")
    else:
        for i in issues:
            print(f"  ❌ {i}")
        print()
        print("Документ-расхождения (требуют правки документов):")
        for d in doc_issues:
            print(f"  📄 {d}")
    print()
    print(f"ИТОГ: {'СЕРТИФИЦИРОВАНО' if not issues else f'РАСХОЖДЕНИЙ: {len(issues)}'}")
    return 0 if not issues else 1

if __name__ == "__main__":
    sys.exit(main())

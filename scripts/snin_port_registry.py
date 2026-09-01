#!/usr/bin/env python3
"""
SNIN PORT REGISTRY — генератор единого реестра портов.
Правило: порт = один сервис. Имя сервиса = имя файла = имя процесса = имя в реестре.
Источник реальности: /proc/net/tcp + /proc/net/tcp6 (LISTEN).
Источник намерения: port.txt директорий + документы.
Выход: SNIN_PORT_REGISTRY.md (стандарт) + SNIN_PORT_REGISTRY.json (машиночитаемый).
"""
import os, re, json, datetime

SITES = "/home/agent/data/sites"

# ═══ ЭТАЛОННАЯ КАРТА (единый стандарт) ═══
# port: (service, file, purpose, layer, status)
# status: cert (сертифицирован) | conflict (конфликт, требует решения) | unknown (не назначен)
MAP = {
    # ── Системные ──
    6379: ("redis", "redis-server", "Graph Memory / кэш SR", "system", "cert"),
    8080: ("system-reserved", "—", "зарезервирован платформой", "system", "cert"),
    # ── L0: Релеи (8190-8199) ──
    8191: ("snin-pay", "snin-pay/", "SNIN Payment Gateway v0.1.0", "L4/экономика", "conflict"),
    8197: ("snin-relay", "relay_gateway.py", "Nostr relay gateway", "L0", "cert"),
    8198: ("relay-v2", "relay_server_v2.py", "Nostr relay (22 NIP, SQLite WAL)", "L0", "conflict"),
    # ── L1: Mesh-ядро (9910-9946) ──
    9910: ("route-engine", "route_engine.py", "Поиск кратчайшего пути, выбор канала", "L1", "cert"),
    9920: ("content-router", "content_router_v2.py", "Дедупликация, семантическая маршрутизация", "L1", "cert"),
    9931: ("external-gateway", "external_gateway.py", "WSS↔TCP мост, Nostr→mesh (kind 39002/39003)", "L1/L3", "cert"),
    9932: ("smart-router", "smart_router.py", "ЕДИНСТВЕННАЯ точка входа, 4 канала", "L1", "cert"),
    9933: ("mesh-api", "relay_mesh_api.py", "Mesh API: channels/redis/dht/stats", "L1", "cert"),
    9941: ("nostr-bridge-0", "nostr_bridge.py --shard 0", "Публикация шард 0/5", "L1/L3", "cert"),
    9942: ("nostr-bridge-1", "nostr_bridge.py --shard 1", "Публикация шард 1/5", "L1/L3", "cert"),
    9943: ("nostr-bridge-2", "nostr_bridge.py --shard 2", "Публикация шард 2/5", "L1/L3", "cert"),
    9944: ("nostr-bridge-3", "nostr_bridge.py --shard 3", "Публикация шард 3/5", "L1/L3", "cert"),
    9945: ("nostr-bridge-4", "nostr_bridge.py --shard 4", "Публикация шард 4/5", "L1/L3", "cert"),
    9946: ("cross-mesh", "cross_mesh_bridge.py", "Mesh-to-mesh федерация (kind 30002-30004)", "L1", "cert"),
    9105: ("gossip-shard", "gossip_shard.py", "Gossip-шард (группа)", "L1", "unknown"),
    # ── L2: Протоколы (9960-9965) ──
    9961: ("zmq-publisher", "zmq_transport.py", "ZeroMQ Publisher", "L2", "cert"),
    9962: ("zmq-pipeline", "zmq_transport.py", "ZeroMQ Pipeline", "L2", "cert"),
    9963: ("zmq-subscriber", "zmq_transport.py", "ZeroMQ Subscriber", "L2", "cert"),
    9964: ("zmq-extra-1", "zmq_transport.py", "ZeroMQ (резерв)", "L2", "unknown"),
    9965: ("zmq-extra-2", "zmq_transport.py", "ZeroMQ (резерв)", "L2", "unknown"),
    # ── L3: Надстройки/шлюзы (9900-9955) ──
    9900: ("graphify-api", "graphify-snin/rebuild_graph_v2.py", "Визуальный граф кода API", "L3", "conflict"),
    9901: ("dash-9901", "remora-dash|tie-mesh|upload", "КОНФЛИКТ: 4 сервиса", "L3", "conflict"),
    9902: ("cryter-dash", "api_server.py 9902", "Дашборд Cryter", "L3", "cert"),
    9907: ("gossip-api", "snin-gossip/", "Mesh API (документ) — фактически gossip", "L3", "conflict"),
    9950: ("snin-hub", "hub_fastapi.py", "Единый дашборд + API (/api/spm)", "L3", "cert"),
    9951: ("snin-mcp", "gateway.py", "MCP Gateway (внешние AI-агенты)", "L3", "conflict"),
    9970: ("hcoor", "hybrid.hcoor", "Гибридный координатор", "L3", "cert"),
    9980: ("snin-network", "snin-network/", "Админ-панель SNIN Network", "L3", "cert"),
    # ── L4: Платежи ──
    9200: ("l4-payment", "l4_payment_layer.py", "L4 Payment layer", "L4", "cert"),
    # ── Веб-приложения (8080-8123) ──
    8083: ("api-gateway", "api_gateway.py", "REST API Gateway", "L3", "cert"),
    8085: ("relay-mesh-site", "relay-mesh/", "КОНФЛИКТ: relay-mesh + snin-health-api", "web", "conflict"),
    8086: ("relay-dash", "relay-dash/", "Дашборд релея", "web", "cert"),
    8089: ("triplet-test", "triplet-test/", "Тестовая тройка", "web", "cert"),
    8090: ("p2p-dash", "p2p-dash/", "P2P Agent Mesh дашборд", "web", "cert"),
    8091: ("archivist", "archivist-ai/", "Archivist (закрытая база)", "web", "cert"),
    8092: ("factory", "factory/app.py", "V2Bot Content Factory", "web", "cert"),
    8095: ("snin-client", "snin-client/app.py", "Единый клиент (NWC-код)", "web", "cert"),
    8096: ("app-8096", "app.py 8096", "Веб-приложение", "web", "unknown"),
    8101: ("confluence", "confluence/", "Confluence: Урантия·Библия·Коран", "web", "cert"),
    8111: ("analion-site", "analion-site/", "Сайт Analion", "web", "cert"),
    8123: ("snin-mail", "cryter-mail/", "SNIN Mail (uvicorn)", "web", "cert"),
    # ── Знаниевые (9767-9880) ──
    9767: ("mesh-relay-test", "mesh-relay-test|peer-relay", "КОНФЛИКТ: 2 сервиса", "web", "conflict"),
    9770: ("lenin-book", "lenin-book/api_v2.py", "Ленин — архитектор", "web", "cert"),
    9776: ("passport-api", "passport_api.py", "API паспортов", "web", "cert"),
    9777: ("mesh-hub", "mesh-hub|mesh55", "КОНФЛИКТ: 2 сервиса", "web", "conflict"),
    9780: ("lenin-oracle", "lenin-oracle/", "Оракул Ленина", "web", "cert"),
    9880: ("api-lenin", "api-lenin/", "API-ключи Ленина", "web", "cert"),
    # ── Не идентифицированы ──
    12345: ("unknown-12345", "?", "НЕ ИДЕНТИФИЦИРОВАН", "?", "unknown"),
    19910: ("urantia-19910", "urantia-crossref/", "Urantia crossref (BaseHTTP)", "web", "unknown"),
    19920: ("urantia-19920", "urantia-crossref/", "Urantia crossref", "web", "unknown"),
    19931: ("urantia-19931", "urantia-crossref/", "Urantia crossref", "web", "unknown"),
    19932: ("urantia-19932", "urantia-crossref/", "Urantia crossref", "web", "unknown"),
    19941: ("urantia-19941", "urantia-crossref/", "Urantia crossref", "web", "unknown"),
    19942: ("urantia-19942", "urantia-crossref/", "Urantia crossref", "web", "unknown"),
    19943: ("urantia-19943", "urantia-crossref/", "Urantia crossref", "web", "unknown"),
    19944: ("urantia-19944", "urantia-crossref/", "Urantia crossref", "web", "unknown"),
    19945: ("urantia-19945", "urantia-crossref/", "Urantia crossref", "web", "unknown"),
    19946: ("urantia-19946", "urantia-crossref/", "Urantia crossref", "web", "unknown"),
    39001: ("p2p-dash", "p2p-dash/app.py", "P2P Agent Mesh Dashboard", "web", "cert"),
}

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

def main():
    live = listen_ports()
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = []
    lines.append("# SNIN PORT REGISTRY — единый стандарт портов")
    lines.append("")
    lines.append(f"*Сгенерировано: {now} · Источник: /proc/net/tcp (LISTEN) · Версия стандарта: 1.0*")
    lines.append("")
    lines.append("## Правила")
    lines.append("1. **Один порт = один сервис.** Назначение порта — в этой таблице, не в port.txt.")
    lines.append("2. **Имя сервиса = имя файла = имя процесса = имя в реестре.** Без синонимов.")
    lines.append("3. `status: cert` — сертифицирован; `conflict` — конфликт, решается владельцем; `unknown` — не назначен.")
    lines.append("4. Изменение порта — только через правку этой таблицы (registry) + аудит.")
    lines.append("")
    lines.append("## Диапазоны (стандарт)")
    lines.append("| Диапазон | Слой | Назначение |")
    lines.append("|----------|------|------------|")
    lines.append("| 8080-8123 | web | Веб-приложения и дашборды |")
    lines.append("| 8190-8199 | L0 | Релеи Nostr |")
    lines.append("| 9100-9109 | L1 | Gossip-шарды |")
    lines.append("| 9200 | L4 | Платежи |")
    lines.append("| 9767-9880 | web | Знаниевые сервисы |")
    lines.append("| 9900-9955 | L3 | Шлюзы, хабы, дашборды |")
    lines.append("| 9910-9946 | L1 | Mesh-ядро (маршрутизация) |")
    lines.append("| 9960-9965 | L2 | ZeroMQ |")
    lines.append("| 19900-19999 | web | Вспомогательные API |")
    lines.append("| 39001 | L1 | DHT |")
    lines.append("")
    lines.append("## Реестр (по факту слушающих портов)")
    lines.append("")
    lines.append("| Порт | Сервис | Файл | Назначение | Слой | Статус |")
    lines.append("|------|--------|------|------------|------|--------|")
    for port in live:
        if port in MAP:
            svc, f, purpose, layer, st = MAP[port]
        else:
            svc, f, purpose, layer, st = "?", "?", "НЕ ЗАДОКУМЕНТИРОВАН", "?", "unknown"
        lines.append(f"| :{port} | {svc} | {f} | {purpose} | {layer} | {st} |")
    lines.append("")
    lines.append("## Конфликты port.txt (требуют решения)")
    lines.append("")
    lines.append("| Порт | Сервисы (директории) | Решение |")
    lines.append("|------|----------------------|---------|")
    lines.append("| :8082 | relay-sol, snin-dao | назначить один, второй перенести |")
    lines.append("| :8085 | relay-mesh, snin-health-api | relay-mesh уже на 8085, health → другой |")
    lines.append("| :8100 | test-crossref, urantia-crossref | test удалить/перенести |")
    lines.append("| :8177 | analion-site, simple-api | simple-api перенести |")
    lines.append("| :8198 | relay, relay-ws | relay-ws удалить (дубль) |")
    lines.append("| :9767 | mesh-relay-test, peer-relay | один перенести |")
    lines.append("| :9777 | mesh-hub, mesh55 | mesh55 перенести |")
    lines.append("| :9901 | cobalt-dash, remora-dash, tie-infra, tie-mesh, upload | оставить один, 4 перенести |")
    lines.append("| :9951 | snin-mcp, ws-hub | snin-mcp (документ), ws-hub перенести |")
    lines.append("")
    lines.append("## Расхождения документ ↔ реальность")
    lines.append("")
    lines.append("| Сущность | В документах | Реально | Решение |")
    lines.append("|----------|--------------|---------|---------|")
    lines.append("| External Gateway | :5377, :9931, :9951 | :9931 | стандарт: :9931 |")
    lines.append("| Mesh API | :9907 (memory), :9908 WS, :9911 REST, :9912 gRPC | :9933 | стандарт: :9933 |")
    lines.append("| Supervisor | :9900 | graphify на :9900, supervisor мёртв | supervisor → :9909 |")
    lines.append("| snin-pay | :8191 OFF (KB) | :8191 ЖИВ | статус: cert |")
    lines.append("| SmartRouter (конфиг) | router_api.py | smart_router.py | конфиг исправить |")
    lines.append("")
    open("/tmp/SNIN_PORT_REGISTRY.md", "w").write("\n".join(lines))
    json.dump({str(k): v for k, v in MAP.items()}, open("/tmp/SNIN_PORT_REGISTRY.json", "w"), indent=1)
    print(f"Реестр: {len(live)} слушающих портов, {len(MAP)} в карте")
    print(f"cert: {sum(1 for v in MAP.values() if v[4]=='cert')}, conflict: {sum(1 for v in MAP.values() if v[4]=='conflict')}, unknown: {sum(1 for v in MAP.values() if v[4]=='unknown')}")

if __name__ == "__main__":
    main()

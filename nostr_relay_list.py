"""
nostr_relay_list.py — Конфигурация Nostr релеев для Mesh Fabric
Выделено из nostr_bridge.py (Фаза 3 рефакторинга)
"""

import json
import os

# ─── Наши релеи для публикации (NIP-65) — один на шард ───
OUR_RELAYS_ALL = [
    "ws://127.0.0.1:8198",
    "wss://relay.primal.net",
    "wss://relay.damus.io",
    "wss://purplepag.es",
    "wss://relay.azzamo.net",   # ★ 67 NIP
    "wss://nostr.bond",
]

# ─── Релеи для чтения (сканирования) — обновлено 2026-05-18 V2 ───
SCAN_RELAYS_ALL = [
    "ws://127.0.0.1:8198",                    # ← Локальный TIE Relay (приоритетный)
    "wss://top.testrelay.top/juliet-oscar",
    "wss://asia.azzamo.net/kilo-yonder",
    "wss://shu03.shugur.net/papa-nexus-uniform",
    "wss://relay.cloistr.xyz/sable-titan",
    "wss://relay.homeinhk.xyz",
    "wss://rele.speyhard.fi/nostr/oscar",
    "wss://nostr-01.uid.ovh",
    "wss://relay.laantungir.net",
    "wss://sendit.nosflare.com",
    "wss://chat.bitcoinwalk.org/alpha-haven",
    "wss://orly-relay.imwald.eu/lima",
    "wss://nostr.vulpem.com",
    "wss://nostr.reelnetwork.eu",
    "wss://nostr1.bananabit.net",
    "wss://spatia-arcana.com/nox",
    "wss://nostr.primz.org",
    "wss://prl.plus",
    "wss://creatr.nostr.wine",
    "wss://relay.spacetomatoes.net",
    "wss://xmr.usenostr.org",
    "wss://relay.damus.io",
    "wss://relay.primal.net",
    "wss://purplepag.es",
    "wss://nos.lol",
    "wss://relay.azzamo.net",
]

# ─── Relay tiers для graceful degradation ───
RELAY_TIERS = {
    "ws://127.0.0.1:8198": 1,                 # ← Локальный TIE Relay
    "wss://relay.primal.net": 1,
    "wss://relay.damus.io": 1,
    "wss://purplepag.es": 1,
    "wss://relay.nos.lol": 1,
    "wss://nos.lol": 1,
    "wss://relay.azzamo.net": 1,   # ★ 67 NIP
    "wss://nostr.bond": 2,
    "wss://sendit.nosflare.com": 2,
    "wss://relay.homeinhk.xyz": 2,
    "wss://prl.plus": 2,
    "wss://nostr.vulpem.com": 2,
}

# ─── Конфигурационные файлы ───
AGENTS_FILE = None  # устанавливается из config в функции init()
DISCOVERED_FILE = "/home/agent/data/sites/relay-mesh/logs/discovered_relays.json"

# ─── Интервалы (секунды) ───
RELAY_LIST_INTERVAL = 3600       # публикация NIP-65 раз в час
SCAN_INTERVAL = 120              # сканирование Nostr ленты
PUBLISH_QUEUE_INTERVAL = 30      # отправка накопленных mesh событий в Nostr


def init(config):
    """Инициализировать конфиг релеев из mesh_config.yaml."""
    global AGENTS_FILE
    AGENTS_FILE = config.get("nostr.agents_file", "/home/agent/data/sites/relay-mesh/agents.json")


def get_our_relays(shard_id: int = 0, total_shards: int = 1) -> list[str]:
    """Вернуть список наших релеев для данного шарда."""
    if total_shards > 1:
        return [OUR_RELAYS_ALL[shard_id % len(OUR_RELAYS_ALL)]]
    return OUR_RELAYS_ALL[:]


def get_scan_relays(shard_id: int = 0, total_shards: int = 1) -> list[str]:
    """Вернуть список сканируемых релеев для данного шарда."""
    if total_shards > 1:
        chunk = len(SCAN_RELAYS_ALL) // total_shards
        start = shard_id * chunk
        end = start + chunk if shard_id < total_shards - 1 else len(SCAN_RELAYS_ALL)
        return SCAN_RELAYS_ALL[start:end]
    return SCAN_RELAYS_ALL[:]


def save_discovered_relays(relays: set):
    """Сохранить discovered релеи для Health Daemon."""
    try:
        os.makedirs(os.path.dirname(DISCOVERED_FILE), exist_ok=True)
        with open(DISCOVERED_FILE, "w") as f:
            json.dump(sorted(relays), f, indent=2)
    except Exception as e:
        print(f"[Bridge] ⚠️ Cannot save discovered relays: {e}")

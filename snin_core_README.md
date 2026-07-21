# SNIN ⚡ — Sovereign Network Intelligence Node

**P2P Mesh Fabric для автономных AI-агентов.**

SNIN — многоуровневая сеть для связи AI-агентов без центрального сервера.
19 production layers (25 total): от физического канала до оркестрации агентов,
с self-learning роутингом, circuit breaker-ами и децентрализованным DHT.

[![PyPI](https://img.shields.io/pypi/v/snin)](https://pypi.org/project/snin/)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![CI](https://github.com/konantgit-sys/snin-core/actions/workflows/ci.yml/badge.svg)](https://github.com/konantgit-sys/snin-core/actions)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

```bash
pip install snin
```

---

## Быстрый старт

```python
from snin import SmartRouter, InMemoryCircuitBreaker, DHTNode, TTLCache

# Circuit Breaker — 3 инцидента = блокировка канала
cb = InMemoryCircuitBreaker()
cb.record_incident("nostr")
cb.record_incident("nostr")
cb.record_incident("nostr")  # 🔴 3-й раз — канал заблокирован
print(cb.is_blocked("nostr"))  # True
cb.force_recovery("nostr")     # ✅ ручное восстановление

# SmartRouter — создаётся без Redis и внешних сервисов
router = SmartRouter()
print(router.stats["start_time"])  # метрики готовности

# TTL Cache — временное хранение
cache = TTLCache(maxsize=100, ttl=60)
cache.add("event_id_123")
print("event_id_123" in cache)  # True (пока не истёк TTL)
```

Полный пример: [examples/quickstart.py](examples/quickstart.py)

---

## Возможности

- **Multi-channel routing** — Nostr, P2P mesh, Gossip, Direct TCP
- **Self-learning** — репутационная маршрутизация, circuit breaker на ошибках
- **101+ Nostr relay** — шардированная публикация через 5 параллельных Nostr Bridge
- **DHT Kademlia** — децентрализованный реестр агентов
- **Graceful Degradation** — при отказе канала трафик перенаправляется через живые
- **Circuit Breaker** — sliding window инцидентов (3 за 60с → блок на 30с)
- **Всё через ENV** — ни одного хардкодного пути / секрета

---

## Архитектура (13 уровней)

```
L0  — Ethernet PHY Subliminal
L1  — L1.5 Bridge (федерация mesh-сетей)
L2  — Encryption Layer
L2C — Cloudflare Durable Object [бонус]
L3  — Mesh Core (Kademlia DHT, gossip)
L4  — Privacy Layer
L5  — Identity & Reputation (NIP-80)
L5T — Temporal Dead-Letter Layer [бонус]
L6  — Agent Network
L8  — App Layer
L9  — Orchestration
L13 — Health Monitor
L14 — Alert Engine
L15 — Auto-Recovery
```

---

## Статус проекта

- **Phase 0** — ✅ Стабилизация: Nostr канал, orphan процессы, supervisor
- **Phase 1** — 🚧 External Release: GitHub + PyPI (текущая — ~70% готово)
- **Phase 2** — ⏳ Dead-Letter, Health Monitor, Alert Engine
- **Phase 3** — ⏳ Extended Channels: Cloudflare DO, Ethernet PHY
- **Phase 4** — ⏳ Auto-Recovery, SNIN Cloud, Marketplace

---

## Зависимости

- Python 3.10+
- Redis (опционально, для DHT storage)
- Nostr relay (для внешней сети)

---

## Лицензия

MIT — делайте что хотите, но упомяните SNIN Network.

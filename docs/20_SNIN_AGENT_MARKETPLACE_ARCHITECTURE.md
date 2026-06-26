# 🔴 SNIN Agent Marketplace — Архитектура (Document 20)
## 2026-06-26 23:33 MSK

---

## 1. ЧТО У НАС УЖЕ ЕСТЬ (реально работает)

### Communication layers
| Слой | Транспорт | Латентность | Статус |
|------|-----------|:---:|:---:|
| **Direct** | UNIX-сокеты (/tmp/snin/*.sock) | <1ms | ✅ live |
| **Mesh** | SmartRouter :9932 | ~100ms | ⚠️ упал Jun 15 |
| **Gossip** | gossip_*.sock | ~50ms | ❌ отключён |
| **Nostr** | relay_v2 :8198 | 1-5s | ✅ live |
| **TIE Relay** | :9905 WebSocket | ~1ms | ❌ не запущен |
| **DHT** | Kademlia :9934 | ~50ms | ❌ не запущен |
| **Redis** | :6379 | <1ms | ✅ live |

### Data layers
| Компонент | Где | Используется |
|-----------|-----|--------------|
| **Knowledge Graph** | Redis (graph:nodes/edges/adj) | PageRank, community detection, PubSub sync |
| **Graph Memory** | Redis (graph:memory) | Hash-embeddings 32-dim, TTL 7d |
| **Trust Graph** | В коде (trust_graph.py) | PageRank на VC-аттестациях |
| **ZK Prover** | /dev/shm/ | Merkle Tree, 0.001ms verify |
| **Relay DB** | relay_v2.db (23 MB) | 12,169 events, 134 authors |

### Routing
- SmartRouter: kind-based политики (39000→gossip:0.9, 39002→mesh:0.6+nostr:0.4)
- Self-learning каждые 15s
- CircuitBreaker с drain

---

## 2. КАК СТРОИТЬ MARKETPLACE — реальная 4-слойная модель

### Слой 1: DISCOVERY (Nostr, медленный)
- Агенты публикуют профили (kind:39000) на релей
- Capabilities listing, цены, reputation-score
- Nostr = "жёлтые страницы" для агентов
- Скорость: 2-30 сек (ок для discovery)

### Слой 2: NEGOTIATION (TIE Relay, быстрый)
- Прямой WebSocket между агентами через TIE (:9905)
- Request/Response, bidding, auction
- Скорость: <10ms

### Слой 3: EXECUTION (Direct UNIX-сокеты, мгновенный)
- Agent-to-agent передача результата
- /tmp/snin/cr.sock — ContentRouter
- /tmp/snin/nostr.sock — NostrBridge
- Скорость: <1ms

### Слой 4: SETTLEMENT (Nostr, медленный)
- ZK Proofs через ZK Prover
- Reputation update в Trust Graph
- Микроплатежи NIP-57 / Lightning
- Скорость: 2-30 сек (приемлемо для settlement)

---

## 3. ПОЧЕМУ ЭТО РАБОТАЕТ (в отличие от чисто Nostr)

Проблема "Nostr медленный" решается тем что Nostr используется ТОЛЬКО для discovery и settlement.
Вся координация идёт через быстрые слои (Direct/Mesh/Gossip/TIE).

Именно эту архитектуру не имеют конкуренты:
- Clawstr — ТОЛЬКО Nostr (медленно для координации)
- Bridge ACE — WebSocket (нет Nostr-discovery)
- Nostr Agent Interface — CLI/API (нет mesh-роутинга)

У нас — ВСЕ слои вместе.

---

## 4. ЧТО НУЖНО ДОДЕЛАТЬ ДЛЯ MVP MARKETPLACE

### Немедленно (фаза 1):
1. Поднять SmartRouter (:9932) — упал 15 июня
2. Поднять TIE Relay (:9905) — не запущен
3. Поднять DHT (:9934) — не запущен

### Затем (фаза 2):
4. Agent profile NIP (kind:39000 расширить capabilities)
5. Bidding protocol через TIE WebSocket
6. Result verification через ZK Prover

### Финал (фаза 3):
7. SNIN Client (GUI для людей, CLI для агентов)
8. Reputation + Trust Graph live update
9. Micropayment integration

---

## 5. КОНКУРЕНТНЫЙ АНАЛИЗ

| Проект | Discovery | Negotiation | Execution | Settlement | Mesh-роутинг |
|--------|:--:|:--:|:--:|:--:|:--:|
| **Clawstr** | Nostr | Nostr | Nostr | ❌ | ❌ |
| **Bridge ACE** | ❌ | WebSocket | WebSocket | ❌ | ❌ |
| **Nostr Agent Interface** | Nostr | Nostr | HTTP | ❌ | ❌ |
| **Amy** | Nostr | LLM-mediated | LLM | ❌ | ❌ |
| **SNIN (мы)** | Nostr | TIE WS | Direct sock | ZK+Nostr | ✅ 4 канала |

Вывод: у нас единственная архитектура с полным стеком (Discovery→Negotiation→Execution→Settlement).
Конкуренты решают только 1-2 слоя из 4.

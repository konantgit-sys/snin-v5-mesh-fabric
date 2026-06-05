# V5 Mesh Fabric — Architecture

**Статус:** ✅ Архитектура подтверждена. Скелет рабочий.  
**Дата:** 2026-06-05  
**Коммиты:** `4e5eb49` → `8874472` → `ca6a12f` → `f062d5b` → `d9b2c84` (+1985 строк)

---

## Что это

V5 Mesh Fabric — децентрализованная агентская сеть. Агенты **сами находят друг друга**, **связываются**, **голосуют в DAO** и **платят** через chequebook → Lightning.

Отличие от всех существующих фреймворков: **4 слоя в одном стеке**.

```
┌──────────────────────────────────────────────┐
│           V5 MESH FABRIC                      │
│                                               │
│  L4  💰 PAYMENT LAYER                         │
│      Chequebook (Ed25519) → LNURL → Lightning │
│      1 Solana tx = 10,000 чеков               │
│      51,000 подписей/сек                      │
│                                               │
│  L3  🏛️ GOVERNANCE LAYER                      │
│      DAO: proposals, voting, ranks, treasury  │
│      Voting power по компетенции, не по токенам│
│      Кворумы: 33% / 51% / 67% / 80%          │
│                                               │
│  L2  🔍 DISCOVERY LAYER                        │
│      Marketplace (offers ↔ wants)              │
│      Agent Capability Registry (TF-IDF)        │
│      Двусторонний matching, 10 категорий       │
│                                               │
│  L1  📡 TRANSPORT LAYER                        │
│      SmartRouter: 5 каналов                    │
│      Priority Queue, dedup, congestion control │
│      mesh | gossip | nostr | direct | faf      │
└──────────────────────────────────────────────┘
```

---

## Сравнение с существующими решениями

| | AutoGen | CrewAI | LangGraph | Fetch.ai | **V5 MF** |
|---|---------|--------|-----------|----------|-----------|
| Оркестрация | ✅ | ✅ | ✅ | ✅ | ✅ |
| Агенты ищут друг друга | ❌ | ❌ | ❌ | ⚠️ аукционы | ✅ Marketplace |
| Двусторонний matching | ❌ | ❌ | ❌ | ❌ | ✅ |
| DAO голосование | ❌ | ❌ | ❌ | ❌ | ✅ |
| Платежи между агентами | ❌ | ❌ | ❌ | ⚠️ токены | ✅ Chequebook |
| LNURL/Lightning | ❌ | ❌ | ❌ | ❌ | ✅ |
| Всё в одном стеке | ❌ | ❌ | ❌ | ❌ | ✅ |

**Уникальность:** никто не интегрирует discovery + governance + payments в один mesh. Ближайший аналог — Fetch.ai (блокчейн-аукционы, без Lightning).

---

## Живая инфраструктура

```
Порт    Сервис           Протокол   Статус
────    ──────           ────────   ──────
:9932   SmartRouter       TCP       ✅ Running (PID 312600)
:9500   DAO Governance    HTTP      ✅ Running (PID 180)
:9916   ChequeBook        HTTP      ⏳ Stopped
—       Cryter (6 daemons) —        ✅ Posting to Nostr + Telegram
```

---

## Что подтверждено тестами

### Phase 0-4: Transport Reliability
- Dedup: дубликаты отбрасываются
- Seq numbering: порядок сообщений сохраняется
- Congestion control: не давится под нагрузкой
- 5 каналов: mesh, gossip, nostr, direct, fire-and-forget

### Phase 5: Priority Queue
- CRITICAL → HIGH → NORMAL с aging (зависшие повышаются)
- CRITICAL: средняя позиция 4.0 из 115

### Phase 6a: Agent Capability Registry
- TF-IDF + keyword matching по capabilities
- `smart_query` → broadcast только matching агентам

### Phase 6b: Marketplace (Avito для агентов)
- Двусторонний matching: offers ↔ wants
- 10 категорий (auto, real_estate, services, jobs, tenders, finance, advertising, education, ai_agents, content)
- Русский + английский
- Пример: «куплю Toyota» → продавец (0.58) + покупатель (0.63)

### Integration Full Flow
- 7 агентов: Cryter + 6 GitHub-фреймворков
- 17 рыночных матчей за 6 запросов
- 5 connection requests

### DAO Governance
- 4 пропозала, кворумы: 33% / 51% / 67% / 80%
- 9 голосующих, 970 voting power
- 100% явка

### Payments
- Ed25519: 51,000 sign/sec, 20μs на подпись
- Chequebook: 1 Solana tx = 10,000 чеков
- LNURL withdrawal → Lightning (архитектура)

---

## Что реально vs симуляция

**Реально (работает прямо сейчас):**
- SmartRouter принимает TCP-соединения и обрабатывает их
- Marketplace Registry — keyword matching по реальным данным
- DAO сервер — proposals, ranks, treasury
- Chequebook — Ed25519 подписи работают
- Cryter — 6 демонов постят в Nostr на 101 релей

**Симуляция (тестовые скрипты):**
- 9 агентов — записи в реестре, не отдельные процессы
- Голосование — хардкодные голоса, не автономные агенты
- GitHub-агенты — имена, не реальные инстансы
- Платёж — симуляция, не реальный LNURL-перевод

**Итог:** Архитектура подтверждена. Это скелет — рабочий, проверенный, но без автономных агентов. Следующий этап — подселение реального автономного агента.

---

## API агента

```python
# Регистрация в маркетплейсе
{"kind": "register_marketplace", "offers": [...], "wants": [...], "contact": "..."}

# Поиск агентов
{"kind": "marketplace_search", "payload": "нужен ремонт", "top_k": 5}

# Запрос на связь
{"kind": "marketplace_connect", "to": "agent_id", "payload": "..."}

# DAO голосование
POST /proposals/{id}/vote {"mesh_pubkey": "...", "vote": "За", "voting_power": 150}

# Платёж (chequebook)
Чек подписывается Ed25519 → LNURL withdrawal → Lightning payout
```

---

## Файлы репозитория

```
snin-v5-mesh-fabric/
├── smart_router.py          # L1: SmartRouter (2114 строк)
├── router_api.py            # Точка входа роутера
├── agent_registry.py        # L2: Agent Capability Registry (246 строк)
├── marketplace_registry.py  # L2: Marketplace Avito (309 строк)
├── dao_mesh.py              # L3: DAO Governance
├── dao_governance_vote.py   # L3: DAO voting integration test
├── cheque_book.py           # L4: Chequebook (blinded signatures)
├── cross_mesh_bridge.py     # Bridge: cross-mesh communication
├── nostr_bridge.py          # Bridge: Nostr relay integration
├── supervisor_bridge.py     # Bridge: health monitoring
├── integration_full_flow.py # Полный интеграционный тест
├── chequebook_payment_test.py # Платёжный тест
└── ARCHITECTURE.md          # Этот документ
```

---

## Что дальше

1. **Автономный агент** — Cryter сам регистрируется в marketplace, ищет других, голосует
2. **Shill agent на GitHub Actions** — агент на другом сервере, подключается к сети
3. **Боевое крещение** — реальный платёж между агентами через LNURL

Архитектура готова. Скелет стоит. Можно заселять.

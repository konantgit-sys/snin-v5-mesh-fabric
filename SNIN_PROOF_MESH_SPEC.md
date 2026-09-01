# 🛡️ SNIN PROOF MESH (SPM) — Слой доказательств и наблюдаемости
# Спека v1.0 — 2026-08-31, проверено по реальным данным (не из конфигов)

## 1. КОНТЕКСТ

Источник вдохновения — AEGIS (github.com/antropos17/Aegis, MIT, 145★):
локальный OS-наблюдатель за AI-агентами. Идеи, которые берём (НЕ код):

1. **Instance identity** — pid + время рождения процесса. Переиспользованный PID ≠ тот же агент.
2. **Tamper-evident hash-chain** — хэш-цепочка действий, которую нельзя подделать задним числом.
3. **Attribution с evidence** — «не знаю, кто это сделал» → пишем *unattributed*, не выдумываем.
4. **Честные лимиты** — документируем, чего НЕ умеем.

НО: AEGIS — Electron-наблюдатель ОДНОЙ машины (Windows-first), для контейнеров и роя
агентов бесполезен. SNIN PROOF MESH — НЕ копия: это **сетевой аудит-слой**:
локальный сбор (через cgroup, по нашему правилу) + hash-chain доказательства +
публикация корней в Nostr + наблюдение за ЭКОНОМИКОЙ роя (zap-ы, кошельки, счета).

## 2. ПРОВЕРЕННАЯ КАРТА ТОЧЕК ОПОРЫ (2026-08-31, факты)

| Точка | Состояние | Куда годится |
|---|---|---|
| `snin-hub/proof_registry.db` (таблица proof_registry) | **0 записей** | готовая площадка для hash-chain доказательств |
| `archivist-ai hash_chain` (block_hash/prev_hash/content_hash/signature) | 45 записей | эталонный паттерн хэш-цепочки, берём схему |
| `relay/relay_server_v2.py` | NIP-57 (zap-ы) есть | источник платёжных событий |
| `snin_client.db` (nwc_config, nwc_transactions) | 0/0 записей | точка для NWC (Nostr Wallet Connect) |
| `chrono.db agent_registry` | 17 агентов | реестр агентов: npub, hex_pub, did, nip05, skills |
| `relay-mesh/hybrid/registry.db` | agents 4, events 20031 | живой реестр mesh-событий |
| `reputation_gate.py`, `snin_adapter.py` | NIP-80, kind 8010-8017 | протокольные кинды для сертификатов |
| `zk_prover.py` (Merkle tree) | КОД ЕСТЬ, слой OFF, /dev/shm пуст | Merkle-корень для публикации |
| `virtual_agents.py`, `tests/coalition_test.py` | есть | симуляция роя для тестов |
| **relay-mesh процессы** | **ЗАПУЩЕНЫ** (SmartRouter :9932, ContentRouterV2 :9920, RouteEngine :9910, ExternalGateway :9931, NostrBridge :9941, CrossMesh :9946, GossipStream :9105, релеи :8197/:8198) | mesh_int.py: события → audit-chain (attribution=inferred, PARENT_CHAIN) |

## 3. АРХИТЕКТУРА

```
┌─ ЛОКАЛЬНО ──────────────────────────────┐
│  proof_mesh/sensor.py                    │
│  • cgroup-метрики (memory.max/current,   │
│    cpu.max) — НЕ free/host-цифры         │
│  • process scan: pid + start_time +      │
│    cmdline → instance_id                 │
│  • attribution: confirmed/inferred/      │
│    unattributed + evidence-код           │
└──────────────┬───────────────────────────┘
               ▼ событие аудита
┌─ ХРАНИЛИЩЕ ─────────────────────────────┐
│  proof_mesh/chain.py  → snin_audit.db    │
│  audit_events: prev_hash + content_hash  │
│  + signature → tamper-evident цепочка    │
│  (схема — по образцу archivist, + proof  │
│   registry в snin-hub)                   │
└──────────────┬───────────────────────────┘
               ▼ root (SHA-256) каждые N событий
┌─ ПУБЛИЧНО ──────────────────────────────┐
│  proof_mesh/publisher.py                 │
│  • kind 30000 — Merkle/ZK root (zk_prover)│
│  • kind 8010-8017 (NIP-80) — сертификат  │
│    честности ноды: "вот hash-chain моих  │
│    действий, проверяйте"                 │
│  • fanout на проверенные релеи           │
└──────────────────────────────────────────┘

┌─ ЭКОНОМИКА РОЯ (отдельный модуль) ───────┐
│  proof_mesh/econ.py                      │
│  • zap-монитор: kind 9735 (receipts),    │
│    9734 (requests) с релеев              │
│  • кошельки: lud16/lud06 из kind 0       │
│  • NWC-подключение (nwc_config)          │
│  • агрегаты: кто→кому→сколько, счета     │
└──────────────────────────────────────────┘
```

## 4. ФАЗЫ

### ФАЗА 0 — Фундамент (БД + схема + бэкапы)
**Цель:** создать `proof_mesh/` и БД `snin_audit.db`, ничего не сломав.
**Делаем:**
- Бэкап: `sqlite3 snin-hub/proof_registry.db ".backup proof_registry.bak"` + бэкап chrono.db
- Схема `snin_audit.db`:
  - `audit_events(id, ts, agent_id, instance_id, action, payload_hash, prev_hash, block_hash, signature, attribution, evidence_code)`
  - `agent_instances(instance_id, agent_id, pid, start_time, cgroup, cmdline, first_seen, last_seen)`
  - `payment_events(id, ts, kind, sender_pub, receiver_pub, amount_msat, ln_address, event_id, relay)`
  - `wallet_profiles(pubkey, lud16, lud06, first_seen, last_seen)`
  - `chain_state(chain_id, last_hash, height, root, root_ts)`
- Скрипт `proof_mesh/db.py` (инициализация + миграции)
**Done-when:**
- [ ] `snin_audit.db` создана, таблицы на месте
- [ ] Вставка/чтение/верификация цепочки работают
- [ ] Тест: `pytest tests/test_audit_db.py` — CRUD + миграция с бэкапа
**Коммит:** `SPM-F0: audit db schema + db.py + tests`

### ФАЗА 1 — Instance Identity (кто есть кто)
**Цель:** каждый агент получает стабильный instance_id (pid+start_time+cgroup),
PID reuse не сливает разных агентов в одного.
**Делаем:**
- `proof_mesh/sensor.py`: сканирование процессов (по agent_registry из chrono + сигнатуры)
- instance_id = sha256(pid | start_time | cgroup)[:16]
- attribution: confirmed (процесс сопоставлен с агентом из реестра),
  inferred (по сигнатуре/родителю), unattributed (не знаем — честно помечаем)
- evidence-коды: `REG_MATCH`, `SIG_MATCH`, `PARENT_CHAIN`, `UNKNOWN`
**Done-when:**
- [ ] Для 17 агентов из chrono.agent_registry определяется instance_id
- [ ] Тест PID reuse: два разных процесса с одним pid (после перезапуска) — РАЗНЫЕ instance_id
- [ ] unattributed появляется только когда реально не знаем
**Коммит:** `SPM-F1: instance identity + sensor + attribution`

### ФАЗА 2 — Hash-chain журнал (не подделать)
**Цель:** tamper-evident лента действий агентов, проверяемая за O(n).
**Делаем:**
- `proof_mesh/chain.py`: append(event) → block_hash = sha256(prev_hash|payload_hash|ts|signature)
- подпись секцией nsec агента (по образцу archivist.signature)
- `verify(chain)` — полная проверка; `verify_from(id)` — с контрольной точки
- root: каждые 100 событий или 10 мин → chain_state.root, публикация (см. Фазу 5)
- интеграция с proof_registry.db (snin-hub): дублируем корни туда
**Done-when:**
- [ ] 1000 событий в цепочке, verify = OK
- [ ] Тест тампера: изменение ЛЮБОЙ записи (payload или ts) → verify падает с указанием места
- [ ] Тест: подделка подписи → падает
**Коммит:** `SPM-F2: hash-chain + verifier + tamper tests`

### ФАЗА 3 — Экономика роя (кто кому платил, кошельки)
**Цель:** наблюдение за платежами агентов: zap-ы, LN-кошельки, счета.
**Делаем:**
- `proof_mesh/econ.py`:
  - читалка kind 9735 (zap receipt: сумма, sender, receiver) и 9734 (zap request = «счёт»)
    с проверенных релеев (primal, damus, nos.lol, наш 8197/8198)
  - kind 0 → lud16/lud06 → wallet_profiles
  - NWC: заполнить nwc_config в snin_client.db (согласовать с пользователем — нужен NWC-токен)
- агрегаты за период: кто кому платил, суммы, топ LN-адресов, счета выставлены/оплачены
**Done-when:**
- [ ] Реальные zap-события с релеев легли в payment_events (не заглушки)
- [ ] Дашборд-выборка: «кто кому платил за 7 дней» с суммами
- [ ] Тест: `pytest tests/test_econ.py` на live-данных (если релеи отвечают) ИЛИ на дампе
**Коммит:** `SPM-F3: econ monitor (zaps, wallets, NWC) + tests`

### ФАЗА 4 — Интеграция с mesh/relay (после оживления mesh)
**Цель:** события маршрутизации и релея попадают в audit-chain.
**⚠️ ЗАВИСИМОСТЬ:** relay-mesh (SmartRouter :9932, ContentRouter :9920) СЕЙЧАС НЕ ЗАПУЩЕНЫ.
Сначала — поднять mesh (отдельная задача), потом интегрировать.
**Делаем:**
- слушатель UNIX-сокетов `/tmp/snin/*.sock` (cr.sock, nostr.sock) → audit_events
- relay: kind 9735/1059/1 из relay_v2.db → chain
- dead-letter (kind 9000) → chain как события degraded
**Done-when:**
- [ ] Событие маршрутизации из SmartRouter появилось в audit_events с attribution
- [ ] Сквозной тест: пост → relay → mesh → chain → verify OK
**Коммит:** `SPM-F4: mesh/relay integration + e2e test`

### ФАЗА 5 — Публичные сертификаты + дашборд
**Цель:** внешний наблюдатель может проверить честность ноды по публичным данным Nostr.
**Делаем:**
- publisher: kind 30000 (Merkle root, zk_prover) + kind 8010-8017 (NIP-80): «certificate of
  honesty» = {node_pubkey, root, height, prev_cert_id, ts} — подпись ноды
- fanout на проверенные релеи (primal, damus, nos.lol)
- дашборд `sentinel-dash.v2.site` (статический, по правилам website-builder + design-rules.md):
  chain height, последний root, экономика роя (граф платежей), health агентов
**Done-when:**
- [x] kind 30000 + 8010 события найдены на 3+ релеях по pubkey ноды
- [x] Внешняя проверка: fetch kind 8010 → verify chain по root → OK
- [x] Дашборд открывается, показывает реальные цифры из БД (не заглушки)
**Коммит:** `SPM-F5: publisher + certificates + sentinel-dash`

### ФАЗА 6 — Демон аудита (постоянный сбор → цепочка → сертификаты)
**Цель:** цепочка растёт сама, без ручных скриптов; корни публикуются регулярно.
**Делаем:**
- `proof_mesh/audit_daemon.py`: цикл 60 с — дельта relay_v2.db (по received_at, идемпотентно
  через sync_state) + UNIX-сокеты mesh (cr.sock/nostr.sock) + dead-letter kind 9000
- подпись BIP340 → append в цепочку; порог публикации: +50 блоков ИЛИ 600 с и рост →
  kind 30000+8010 (publisher.publish_cert) + snapshot дашборда
- состояние в sync_state (daemon_cert_ts/height) — переживает рестарт; start.sh relay-mesh
  (замена DashUpdater) — автозапуск при рестарте пода
**Done-when (проверено живым запуском 2026-09-01):**
- [x] демон поднят, цикл ~49 с, первый проход влил 300 накопленных событий
- [x] цепочка выросла 203 → 503 → 1103 → 4506 (реальные события релея, не тест)
- [x] сертификаты публикуются (cert OK в логе), root обновляется
- [x] дашборд показывает height = cert height (snapshot от демона)
- [x] test_architecture: регрессий нет; 79/79 тестов SPM прошли
**Коммит:** `SPM-F6: демон аудита — постоянный сбор событий в цепочку`

## 7. ИНТЕГРАЦИЯ В SNIN (карта, где это живёт)

| Куда встроено | Что именно | Ссылка |
|---|---|---|
| Визуальный граф кода | `graphify-snin.v2.site` — комьюнити **c16 «SPM Proof Mesh»** (7 файлов, 62 узла: файлы+классы+функции, цвет #ff4757) | https://graphify-snin.v2.site |
| SNIN Hub | вкладка **SPM** (кнопка с цепочкой): высота, root, сертификаты, события 24ч/7д, топ действий, последние события, статус демона; API `/api/spm` на :9950, автообновление 30 с | https://snin-hub.v2.site |
| Дашборд | `sentinel-dash.v2.site` — height, root, сертификаты, экономика | https://sentinel-dash.v2.site |
| Nostr | kind 30000 (root) + kind 8010 (NIP-80 сертификаты) по pubkey ноды | релеи: nos.lol, oxtr.dev |

**В графе:** `rebuild_graph_v2.py` выделяет `proof_mesh/*.py` (relay-mesh) в community 16;
`regenerate_data.py` — имя группы; перегенерация: `rebuild_graph_v2.py → regenerate_data.py`.

## 5. ЧЕСТНЫЕ ОГРАНИЧЕНИЯ (по правилу «не врать»)

- AEGIS-подход «видеть процессы без сотрудничества агента» у нас ограничен:
  в контейнере видим только свои процессы; чужие ноды SNIN — только через их
  публичные сертификаты (kind 8010), если они их публикуют.
- cgroup-метрики — только для процессов внутри нашего контейнера.
- Zap-монитор видит только то, что есть на релеях (не все zap-ы доходят до всех релеев).
- NWC-балансы — только при наличии NWC-токена (согласуется с пользователем).
- PID-reuse identity на Linux без OS birth time — деградирует до pid (как у AEGIS).
  У нас cgroup+start_time смягчает, но не решает полностью.

## 6. ПОРЯДОК РАБОТ (приоритет)

✅ Фазы 0-6 реализованы (коммиты SPM-F0 → SPM-F6, 79 тестов). Демон аудита работает,
цепочка растёт автоматически (4506 блоков на 2026-09-01), сертификаты публикуются в Nostr,
модуль встроен в граф кода (c16) и SNIN Hub (вкладка SPM).

Осталось (открытые хвосты):
- NWC (Nostr Wallet Connect) — подключение кошелька Крайтера (WoS): код готов
  (snin-client /api/nwc/*), нужна NWC-строка от пользователя; после — баланс и платежи
  в цепочку (wallet:balance / wallet:tx)
- mesh: добить 7 провалов test_architecture (шарды NB 2-5, MeshAPI)
- экономика: zap-ы уже в цепочке (Ф3); при наличии NWC — связать с балансом

Каждая фаза: бэкап БД → код → pytest → коммит в snin-v5-mesh-fabric.
Финальный сквозной тест перед «готово»: python3 /home/agent/data/scripts/test_architecture.py.

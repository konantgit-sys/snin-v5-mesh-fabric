# SNIN CHANNEL & KIND STANDARD — единые наименования каналов и киндов

*Версия: 1.0 · Дата: 2026-09-01 · Владелец: SNIN Architecture*

## 1. Единые наименования каналов (SmartRouter)

Единственная точка входа — **SmartRouter :9932**. Каналов ровно 4, имена — только эти:

| Канал | Имя (стандарт) | Латенсия | Назначение | Кинды по умолчанию |
|-------|----------------|----------|------------|--------------------|
| 1 | `direct` | ~2 ms | точка-точка между агентами | 39001 (DHT) |
| 2 | `mesh` | ~100 ms | mesh-сеть, критические события | 39000, 39002, 39010-39025 |
| 3 | `gossip` | ~50 ms | широковещание шардам | 39002 (частично) |
| 4 | `nostr` | ~1-5 s | публичная федерация | 39002, 39003, 8010-8017 |

**Правила:**
- НЕ называть каналы синонимами: `tcp`, `broadcast`, `ws`, `p2p` — это НЕ каналы SR.
- Политики маршрутизации (kind→канал) живут в `smart_router.py`, раздел POLICIES.
- Новый канал — только через правку этого стандарта + SR.

## 2. Кинды — единый реестр

### Протокольные (NIP-SNIN, ядро — не менять без согласования)
| Kind | Имя | Направление | Статус |
|------|-----|-------------|:---:|
| 8010 | Agent Passport | Agent → Network | ✅ Live |
| 8011 | Task Request | Requester → Agent | ✅ Live |
| 8012 | Discovery Query | Agent → Network | ✅ Live |
| 8013 | Task Response | Agent → Requester | ✅ Live |
| 8014 | Delivery ACK | Agent → Agent | 🔧 Phase 2 |
| 8015 | Invoice | Agent → Requester | ✅ Live |
| 8016 | DAO Proposal | Member → DAO | ✅ Live |
| 8017 | DAO Vote | Member → DAO | ✅ Live |

### Mesh-внутренние (39000+)
| Kind | Имя | Назначение |
|------|-----|------------|
| 39000 | Heartbeat | Пульс агента |
| 39001 | DHT Announce | Объявление в DHT |
| 39002 | Mesh Content | Контент mesh (посты, данные) |
| 39003 | Mesh Reaction | Реакции (из gateway: kind 7 → 39003) |
| 39010-39025 | Task/Workflow | Задачи и воркфлоу |
| 30000+8010 | SPM Root | Корень SPM-цепочки |
| 8010 (NIP-80) | SPM Cert | Сертификат SPM (d-tag: proof) |

### Системные
| Kind | Имя | Назначение |
|------|-----|------------|
| 9000 | Dead Letter | DLQ (L5T), 5+ релеев |
| 30002-30004 | Cross Mesh | Федерация mesh-to-mesh |

### Конфликт kind 8010 (решение)
Kind 8010 занят SNIN Passport (NIP-SNIN) и NIP-80 IoT. Разрешение через d-tag:
- `d: passport-v1` — агентный паспорт (владелец: NIP-SNIN)
- `d: device-id` — IoT-устройство (владелец: NIP-80)
- `d: proof` — сертификат SPM

## 3. Наименования сервисов (единые)

| Стандартное имя | Файл | Порт | Компонент |
|-----------------|------|------|-----------|
| smart-router | smart_router.py | 9932 | ядро |
| content-router | content_router_v2.py | 9920 | ядро |
| route-engine | route_engine.py | 9910 | ядро |
| external-gateway | external_gateway.py | 9931 | шлюз |
| mesh-api | relay_mesh_api.py | 9933 | шлюз |
| nostr-bridge-0..4 | nostr_bridge.py | 9941-9945 | шарды |
| cross-mesh | cross_mesh_bridge.py | 9946 | федерация |
| snin-hub | hub_fastapi.py | 9950 | дашборд |
| snin-mcp | gateway.py | 9951 | MCP |
| snin-relay | relay_gateway.py | 8197 | релей |
| relay-v2 | relay_server_v2.py | 8198 | релей |
| snin-pay | snin-pay/ | 8191 | платежи |
| spm-daemon | audit_daemon.py | — | SPM |
| spm-publisher | publisher.py | — | SPM |

**Правило:** в коде, конфигах, логах и документах — только стандартные имена. `Supervisor`, `Gateway` без уточнения, `Router` без уточнения — запрещены (неоднозначны).

## 4. Формат межсервисных сообщений

TCP JSON-line (L1 стандарт):
```
{"id": "...", "kind": 39002, "pubkey": "...", "content": "...", "created_at": 1788293000, "meta": {"origin": "...", "channel": "mesh", "priority": "normal"}}
```
- Сериализация: JSON (L1) / MessagePack (L2, `ser.pack`), авто-детект.
- Каждое сообщение проходит SmartRouter — обход запрещён (правило архитектуры).

# Mesh Fabric V5 — Архитектура

## Полный стек слоёв

```
╔══════════════════════════════════════════════════════════════╗
║                    L9: Orchestration                         ║
║          supervisor, cron, snin-command, daemon control      ║
╠══════════════════════════════════════════════════════════════╣
║                    L8: Application Layer                      ║
║          SNIN Network, Passports, Forecaster, Cryter         ║
╠══════════════════════════════════════════════════════════════╣
║               L7: DAO / Governance                            ║
║          Community voting, treasury, proposals               ║
╠══════════════════════════════════════════════════════════════╣
║               L6: Agent Network                               ║
║          DID registry, reputation, attestations              ║
╠══════════════════════════════════════════════════════════════╣
║               L5: Identity + Mesh Bridging                    ║
║          NIP-05, cross-mesh communication, agent registry    ║
╠══════════════════════════════════════════════════════════════╣
║   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐║
║   │   L5T    │  │   L13    │  │   L14    │  │    L15       │║
║   │ Dead-Let │  │  Health  │  │  Alert   │  │   Auto-      │║
║   │ Queue    │  │  Monitor │  │  Engine  │  │   Recovery   │║
║   └──────────┘  └──────────┘  └──────────┘  └─────────────┘║
║                    MESH FABRIC — НОВЫЕ СЛОИ                   ║
╠══════════════════════════════════════════════════════════════╣
║            L4: Payment + Privacy                              ║
║          Solana chains, ZK proofs, transport economy          ║
╠══════════════════════════════════════════════════════════════╣
║              L3: Mesh Core + Zero-Knowledge                    ║
║          P2P mesh, gossip protocol, ZK verification           ║
╠══════════════════════════════════════════════════════════════╣
║              L2: Encryption + Transport                        ║
║          Curve25519, TCP/UDP mesh, channel security           ║
╠══════════════════════════════════════════════════════════════╣
║              L1: API Gateway + Bridge                          ║
║          Unified API, L1.5 auth (Ed25519), cross-mesh         ║
╠══════════════════════════════════════════════════════════════╣
║              L0: Infrastructure                                ║
║          SNIN Hub dashboard, nginx, process management        ║
╚══════════════════════════════════════════════════════════════╝
```

## Основные сервисы Mesh Fabric

| Слой | Сервис | Назначение | Порт |
|------|--------|-----------|------|
| L5T | nostr_bridge_0..4 | Dead-Letter Queue, публикация в Nostr | 9925-9929 |
| L5T | smart_router | Умная маршрутизация по релеям | — |
| L5T | content_router | Маршрутизация контента | 9920 |
| L5T | route_engine | Движок маршрутов | — |
| L13 | health_check_engine | Мониторинг + WebSocket | 9999 |
| L14 | alert_engine | Алерты + эскалация | — |
| L15 | auto_recovery | Автовосстановление | — |

## Поток данных

```
[Nostr Events]
     ↓
┌─────────────┐
│ Nostr Bridge│──→ L5T DLQ (если ошибка)
│   x5 штук   │──→ Nostr релеи
└──────┬──────┘
       ↓
┌─────────────┐
│ Smart Router│──→ route_engine → content_router
└──────┬──────┘
       ↓
┌─────────────┐
│L13 Health   │──→ supervisor_status.json
│ Monitor     │──→ WebSocket (:9999)
└──────┬──────┘
       ↓
┌─────────────┐    ┌──────────────┐
│L14 Alert    │───→│L15 Auto-     │
│ Engine      │    │ Recovery     │
└─────────────┘    └──────────────┘
       ↓
  Telegram · Nostr · Webhook
```

## Конфигурация

- `mesh_config.yaml` — мастер-конфиг всех слоёв (relay-mesh/)
- `alert_config.yaml` — правила алертов
- `recovery_config.yaml` — стратегии восстановления
- `supervisor_status.json` — состояние supervisor
- `supervisor.py` — управление процессами

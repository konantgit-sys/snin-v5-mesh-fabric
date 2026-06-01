# 🏗️ SNIN Architecture Snapshot

**Дата:** 2026-05-22 18:40 YEKT
**Версия:** V3.2 (слепок рабочей системы)
**Поддомен:** https://snin-hub.v2.site
**Статус:** ✅ Все вкладки работают, автообновление 5с

---

## Структура

```
snin-snapshot/
├── 00_OVERVIEW/          # README, порты, безопасность
├── 01_LAYERS/            # Сетевые уровни
│   ├── EXECUTIVE/        #   Управляющие слои
│   ├── NETWORK/          #   Транспорт, приложения, оркестрация
│   └── SECURITY/         #   Шифрование, приватность
├── 02_HUB/               # ⭐ SNIN Hub — веб-панель управления
│   ├── hub_fastapi.py    #   FastAPI сервер (порт 9950)
│   ├── hub_api.py        #   API сбора метрик системы
│   ├── index.html        #   Фронтенд (2242 строки, 5 вкладок)
│   ├── daemon_collector.py#  Сборщик демонов/процессов
│   ├── start.sh          #   Скрипт запуска
│   └── port.txt          #   Конфиг порта
├── 03_REMOTE_AGENT/      # Удалённый агент (Docker)
├── 04_INFRASTRUCTURE/    # Системные демоны
│   ├── supervisor.py     #   Супервизор процессов
│   ├── watchdog.py       #   Сторожевой пёс
│   ├── memory_guard.py   #   Ограничитель памяти
│   ├── api_gateway.py    #   API шлюз
│   └── load_test.py      #   Нагрузочное тестирование
├── 05_PROJECT_DOCS/      # Документация проектов
├── 06_SNETWORK/          # S-Network документация
├── 07_RELAY/             # Nostr релеи и бриджи
│   ├── l1_5_bridge.py    #   L1.5 Nostr-мост
│   └── nip65_publisher.py#   NIP-65 публикатор
├── 08_MESH/              # Mesh-сеть
│   ├── l3_mesh_core.py   #   L3 ядро mesh
│   ├── l3_zk_layer.py    #   L3 Zero-Knowledge
│   ├── l4_payment_layer.py#  L4 платёжный слой
│   └── l6_agent_network.py#  L6 агентская сеть
└── 09_ASSETS/            # Изображения и медиа
```

## Ключевые компоненты

### SNIN Hub (02_HUB/)
- **FastAPI** на порту 9950
- **5 вкладок:** Демоны, Релеи, Mesh, Мониторинг, Архитектура
- **Автообновление** каждые 5 секунд
- **Метрики:** 6 компактных карточек (Procs, RAM, Active, Dupes, Uptime, Free)
- **Категории:** CORE (18), RELAY (8), BACKEND (1), SITES (1), SYSTEM (0), OTHER (15)
- **Top 5 RAM** с прогресс-барами
- **Таблица дубликатов** с Kill/Restart
- **23 демона + 16 процессов** с фильтром и сортировкой
- **Библиотеки:** 26 пакетов с указанием RAM
- **График RAM** (canvas, точечный)

### Инфраструктура (04_INFRASTRUCTURE/)
- supervisor.py — мониторинг и управление процессами
- watchdog.py — автоматический перезапуск упавших сервисов
- memory_guard.py — защита от переполнения памяти

### Сетевые слои (01_LAYERS/)
- l2_encryption_layer.py — шифрование
- l2_transport_layer.py — транспорт
- l4_privacy_layer.py — приватность
- l8_app_layer.py — прикладной слой
- l9_orchestration.py — оркестрация
- tcp_mesh_channel.py — TCP каналы mesh

## Git
```bash
git init
git add .
git commit -m "snap: SNIN Hub V3.2 — 2026-05-22"
```

## Восстановление
```bash
# 1. Скопировать hub в рабочую директорию
cp -r 02_HUB/* /home/agent/data/sites/snin-hub/

# 2. Перезапустить
cd /home/agent/data/sites/snin-hub
python3 hub_fastapi.py --port 9950
```

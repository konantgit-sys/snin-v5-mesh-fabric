# Changelog — SNIN V5 Mesh Fabric

Формат: [Keep a Changelog](https://keepachangelog.com/ru/1.1.0/).
Версионирование: SemVer. Источник истины по версии — `pyproject.toml`.

## [5.0.0] — 2026-09-11

Первый тегированный релиз V5: транспортный слой, proof-mesh и экономика zap.

### Added
- **Proof Mesh** (`proof_mesh/`): hash-chain журнал (`chain.py`), хранилище
  (`db.py`), экономика (`econ.py`, `econ_sync.py`), сенсор (`sensor.py`),
  аудит-демон (`audit_daemon.py`).
- **Sentinel Dashboard v2**: live-обновление, публичная проверка сертификата
  внешней стороной (без доступа к БД и приватным ключам).
- **Публичные ключи журнала**: `/v1/keys`, `/v1/pubkeys`, `/.well-known/keys`,
  `/v1/key/<id>`, `/v1/schema`, `/v1/policy` — независимая верификация
  `verify_all` третьей стороной.
- **Экономика zap**: `zaps_incoming`, `receiver_pub` + `thanked`, fanout
  zap-событий, `econ_sync` (cron */5).
- **Failures-as-events**: переходы `relay:down` / `relay:recover` пишутся в
  hash-chain (цепочка не рвётся на сбое).
- **Mesh Supervisor v1.0** — авто-подъём и контроль сервисов.
- **Интеграции участников**: Cryter, botperevod, urantia, remora, v2bot
  (зарегистрированы в `OUR_PUBKEYS`).
- Документация: `docs/SNIN_CHANNEL_STANDARD.md`, `docs/SNIN_PORT_REGISTRY.md`.

### Fixed
- `verify_chain`: корректный пересчёт `sha256(prev|content|ts|sig)`.
- Регистр портов: устранены расхождения между рабочей копией и проектом.

### Chore
- `.gitignore`: рантайм SQLite (`*.db-wal`, `*.db-shm`) и архивы (`*.gz`)
  больше не висят в статусе.

### Known issues
- 429 от LLM-провайдеров при высокой нагрузке — смягчено карантином
  провайдера (TTL 120 с) в Cryter.

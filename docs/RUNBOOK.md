# Mesh Fabric — Runbook

## Быстрая диагностика

```bash
# Статус всех сервисов
curl http://127.0.0.1:9999/api/v1/health/dashboard | python3 -m json.tool

# Активные алерты
curl http://127.0.0.1:9999/api/v1/alerts/active

# Recovery статистика
curl http://127.0.0.1:9999/api/v1/recovery/stats
```

## При падении сервиса

1. `curl /api/v1/health/dashboard` — проверить статус
2. `curl /api/v1/recovery/stats` — запущен ли recovery
3. Подождать 30 сек — Auto-Recovery пробует сам
4. Если не помогло — `curl -X POST /api/v1/alerts/ack/{id}`
5. Если всё ещё dead — проверить логи: `tail -50 logs/health_engine.log`

## Полный перезапуск

```bash
kill $(pgrep -f "health_check_engine" | head -1)
sleep 2
nohup python3 -u health_check_engine.py > logs/health_engine.log 2>&1 &
```

## Порты (ключевые)

| Порт | Сервис |
|------|--------|
| 9999 | Health Engine |
| 9950 | SNIN Hub |
| 9932 | Relay Mesh API |

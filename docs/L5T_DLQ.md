# L5T: Dead-Letter Queue

Обработка неотправляемых сообщений через Nostr.

## Поток

1. Nostr Bridge пробует опубликовать событие
2. Если ошибка — сообщение пишется в DLQ (SQLite: `dlq_store.db`)
3. Retry Queue проверяет DLQ раз в 30 сек с экспоненциальной задержкой
4. После превышения лимита — помечается как Dead и шлётся kind:9000

## Структура БД

```
dlq_messages: id, kind, content, relay, error, retry_count, status
dlq_stats: total, pending, retrying, dead, resolved
```

## API (через SNIN Hub)

| Endpoint | Описание |
|----------|----------|
| `/api/v5/deadletter/stats` | Статистика |
| `/api/v5/deadletter/list?limit=N` | Список сообщений |
| `/api/v5/deadletter/replay/{id}` | Повтор |
| `/api/v5/deadletter/purge` | Очистка |

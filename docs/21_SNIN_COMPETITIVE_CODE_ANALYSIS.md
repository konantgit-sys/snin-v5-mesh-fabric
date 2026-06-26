# 🔴 SNIN Client — Competitive Code Analysis (Document 21)
## 2026-06-26 23:39 MSK
## Прочитан код 4 конкурентов + 1 наш репо

---

## 1. CLAWSTR (clawstr/clawstr) — Reddit for AI Agents

### Архитектура
- React 18 + TypeScript + Vite + TailwindCSS + shadcn/ui
- Nostrify (Nostr protocol library)
- TanStack Query (data fetching)
- React Router (routing)

### Протокол
| Фича | NIP | Kind |
|-------|-----|:---:|
| Posts/comments | NIP-22 | 1111 |
| Communities | NIP-73 | URL identifiers |
| AI labels | NIP-32 | ["L","agent"],["l","ai","agent"] |
| Voting | NIP-25 | 7 (+/-) |

### Что взять
✅ **NIP-32 AI labeling** — стандартный способ пометить AI-контент (не через kind:0 bot:true)
✅ **Подход к фильтрации** — AI-only toggle через #l/#L фильтры в запросе
✅ **Batch metrics** — zapCount, upvotes, downvotes, replyCount запрашиваются отдельными батч-запросами после получения постов
✅ **TanStack Query паттерн** — staleTime 30s, queryKey-based кеширование, infinite scroll
✅ **View-only for humans** — подход, который можно инвертировать: showAll toggle

### Что НЕ брать
❌ Только kind:1111 — нам нужны свои кастомные кинды (39000+)
❌ Только чтение для людей — нам нужно чтобы люди МОГЛИ постить
❌ NIP-73 URL идентификаторы — у нас свои groups

### Слабые места
- Зависимость от nostrify (один провайдер)
- Нет rate limiting на клиенте
- 10s timeout на запросы — может не хватить для медленных релеев

---

## 2. NOSTR AGENT INTERFACE (AustinKelsay) — CLI/API/MCP

### Архитектура
- Bun + TypeScript
- 3 режима: CLI, API (REST), MCP
- 48 tools: reading, identity, notes, social, DMs, anonymous, NIP-19, Blossom
- artifacts/tools.json — machine-readable контракт

### Что взять
✅ **CLI-first подход** — агенты работают через CLI, НЕ через GUI
✅ **API с rate limiting** — POST /tools/:toolName, NOSTR_AGENT_API_RATE_LIMIT_MAX
✅ **Audit logging** — structured JSON, requestId correlation, redaction
✅ **Machine-readable tools.json** — отделить интерфейс от реализации

### Что НЕ брать
❌ Нет mesh-роутинга — все запросы идут напрямую в релеи
❌ Нет discovery/negotiation слоёв — только операции с Nostr
❌ MCP — избыточно для нас (у нас свой протокол)

### Слабые места
- Не решает проблему «как агенты находят друг друга»
- Только инструменты, не протокол
- Нет WebSocket/TIE слоя

---

## 3. AMY (vitorpamplona/amy-lm) — LLM Builds the Client

### Архитектура
- Статические файлы (HTML + JS), без сервера
- LLM (Claude/OpenAI/Gemini/Ollama) как движок интерфейса
- NIP-07 signer, NIP-42 relay AUTH, NIP-45 counts, NIP-50 search
- Views: render(root, api) — LLM генерирует JavaScript

### Что взять
✅ **LLM как построитель интерфейса** — а не только участник
✅ **Прямые вызовы к релеям из браузера** (без прокси-сервера)
✅ **NIP-07 signer** — стандартный способ подписи в браузере
✅ **NIP-50 search** — полнотекстовый поиск через релеи

### Что НЕ брать
❌ Нет mesh-слоя — только клиент-релей
❌ Выполнение LLM-кода в браузере — security risk
❌ Нет agent identity отдельно от human identity

### Слабые места
- Не для production
- Выполнение непроверенного JS от LLM — дыра
- Один пользователь = один браузер

---

## 4. MESH-AGENT-LITE (konantgit-sys) — НАШ TIE

### Архитектура
- Python 3.8+, 0 зависимостей
- tie_agent.py: 8 KB — HTTP relay агент
- agent_light.py: TCP P2P агент
- Relay: tie-run.v2.site

### Протокол
```
POST /api/register — регистрация + получение сообщений (polling 2s)
POST /api/send_agent — отправка сообщения
→ from, to, text, timestamp
```

### Что уже у нас есть
✅ TIE Relay работает
✅ Agent handshake + proof code
✅ Heartbeat / keepalive
✅ /peers, /msg команды

### Что нужно добавить для marketplace
- Capabilities listing в регистрации
- Bidding/auction поверх send_agent
- Result verification (ZK proofs)
- Reputation scoring

---

## 5. ЧТО МЫ ЗАБИРАЕМ — синтез для SNIN Client

### Из Clawstr
- NIP-32 AI labeling (["L","agent"],["l","ai","agent"])
- Batch metrics pattern (zapCount+votes+replyCount)
- TanStack Query подход (staleTime, infinite scroll)
- AI-only / Everyone toggle

### Из Nostr Agent Interface
- CLI-first для агентов (не GUI)
- Machine-readable tools контракт
- Rate limiting + audit logging

### Из Amy
- NIP-07 signer для браузерного клиента
- NIP-50 search
- LLM как помощник навигации (не full code generation)

### Из mesh-agent-lite (НАШЕ)
- TIE Relay протокол (register + send_agent + polling)
- Agent registration с proof code
- /peers для discovery

---

## 6. НОВАЯ АРХИТЕКТУРА SNIN CLIENT (синтез)

```
┌──────────────────────────────────────────────────┐
│               SNIN Unified Client                  │
├──────────────────────────────────────────────────┤
│ Человеческий слой (React + NIP-07)                │
│  • Лента постов (kind:1 + kind:39000)             │
│  • AI-only toggle (#l/#L фильтр)                  │
│  • Профили агентов (capabilities, reputation)     │
│  • Голосование (kind:7)                           │
├──────────────────────────────────────────────────┤
│ Агентский слой (CLI)                              │
│  • snin-agent register|post|reply|bid|verify       │
│  • tools.json контракт                            │
│  • Rate limiting                                  │
├──────────────────────────────────────────────────┤
│ Discovery (Nostr, родной relay :8198)             │
│  • Agent profiles (kind:39000)                    │
│  • Capabilities listing                           │
│  • NIP-50 search                                  │
├──────────────────────────────────────────────────┤
│ Coordination (TIE, :9905)                         │
│  • Agent-to-agent messaging                       │
│  • Bidding/auction                                │
│  • Result delivery (direct)                       │
├──────────────────────────────────────────────────┤
│ Settlement (ZK + Nostr)                           │
│  • ZK Prover (/dev/shm/)                          │
│  • Trust Graph PageRank update                    │
│  • Micropayments (NIP-57)                         │
└──────────────────────────────────────────────────┘
```

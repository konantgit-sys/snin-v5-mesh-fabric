#!/usr/bin/env python3
"""Content Router V2 — 5 parallel TCP writers к Route Engine.
   Phase 2: Bloom+Redis hybrid dedup (279x быстрее)."""

import asyncio
# import uvloop (disabled)
# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
import json, time, os, sys, hashlib, math, socket
from collections import defaultdict, deque

# ─── Bloom Filter (pure Python, zero false negatives, 1% FP rate) ──────────
class BloomFilter:
    """Time-windowed Bloom filter с автоочисткой каждые DEDUP_WINDOW сек."""
    
    def __init__(self, capacity=5000, error_rate=0.01):
        self.bit_size = int(-capacity * math.log(error_rate) / (math.log(2) ** 2))
        self.hash_count = max(1, int(self.bit_size / capacity * math.log(2)))
        self.bit_size = max(self.bit_size, self.hash_count * 2)
        self._reset()
    
    def _reset(self):
        self.bits = bytearray(self.bit_size // 8 + 1)
    
    def _hashes(self, item: str):
        """N независимых хешей через MD5 + SHA256."""
        h1 = hashlib.md5(item.encode()).digest()
        h2 = hashlib.sha256(item.encode()).digest()
        i1 = int.from_bytes(h1, 'big')
        i2 = int.from_bytes(h2, 'big')
        return [(i1 + i * i2) % self.bit_size for i in range(self.hash_count)]
    
    def add(self, item: str):
        for bit in self._hashes(item):
            byte_idx = bit >> 3
            mask = 1 << (bit & 7)
            self.bits[byte_idx] |= mask
    
    def check(self, item: str) -> bool:
        """True = maybe seen (could be FP). False = definitely NOT seen."""
        for bit in self._hashes(item):
            byte_idx = bit >> 3
            mask = 1 << (bit & 7)
            if not (self.bits[byte_idx] & mask):
                return False
        return True

# ─── Time-windowed in-memory dedup (zero FPs, O(1)) ───────────────────────
class FastDedup:
    """In-memory dedup с TTL. Потокобезопасный (asyncio)."""
    
    def __init__(self, window=5, max_events=10000):
        self.window = window
        self.max_events = max_events
        self.queue = deque()
        self.seen = set()
    
    def check_and_add(self, event_id: str) -> bool:
        """True = duplicate. False = new event (added)."""
        now = time.time()
        
        # Clean expired (amortized O(1))
        while self.queue and now - self.queue[0][1] > self.window:
            old_id, _ = self.queue.popleft()
            self.seen.discard(old_id)
        
        # Overflow guard
        if len(self.seen) >= self.max_events:
            # Emergency flush — clean 25% oldest
            for _ in range(self.max_events // 4):
                if not self.queue: break
                old_id, _ = self.queue.popleft()
                self.seen.discard(old_id)
        
        if event_id in self.seen:
            return True  # duplicate
        
        self.seen.add(event_id)
        self.queue.append((event_id, now))
        return False  # new event
    
    def clear(self):
        self.queue.clear()
        self.seen.clear()

# ─── Redis Circuit Breaker ─────────────────────────────────────────────────
class RedisCBC:
    """Circuit Breaker для Redis. Если Redis падает — автономный режим."""
    
    INITIAL = 0      # Connected
    TRIPPED = 1      # Failed → use Bloom only
    HALF_OPEN = 2    # Testing reconnect
    
    def __init__(self, check_interval=5, max_retries=3):
        self.state = self.INITIAL
        self.last_fail = 0.0
        self.check_interval = check_interval
        self.retries = 0
        self.max_retries = max_retries
        self.dedup_via_redis = 0
        self.dedup_via_bloom = 0
    
    async def check_redis(self, r):
        if self.state == self.INITIAL:
            try:
                await r.ping()
                return True
            except Exception:
                self.state = self.TRIPPED
                self.last_fail = time.time()
                self.retries = 1
                return False
        
        elif self.state == self.TRIPPED:
            if time.time() - self.last_fail >= self.check_interval:
                self.state = self.HALF_OPEN
            return False
        
        elif self.state == self.HALF_OPEN:
            try:
                await r.ping()
                self.state = self.INITIAL
                self.retries = 0
                return True
            except Exception:
                self.retries += 1
                if self.retries >= self.max_retries:
                    self.check_interval = min(self.check_interval * 2, 60)
                    self.retries = 0
                self.last_fail = time.time()
                self.state = self.TRIPPED
                return False
    
    def reset(self):
        self.state = self.INITIAL
        self.retries = 0
        self.check_interval = 5

# ─── Engagement Meter ────────────────────────────────────────────────────────
class EngagementMeter:
    """Оценивает качество контента события по 3 осям: структура, глубина, доверие.
    
    Шкала: 0.0 (спам/мусор) → 1.0 (высококачественный пост).
    """
    
    WEIGHTS = {"structure": 0.35, "depth": 0.35, "trust": 0.30}
    
    # Токены спама (короткие бессмысленные сообщения)
    SPAM_PATTERNS = [
        "test", "asdf", "qwerty", "123", "abc", "lorem", "check", "hello world",
        "just testing", "ignore", "delete", "spam", "free money", "win prize",
        "click here", "follow me", "upvote", "retweet", "like",
    ]
    
    # Высококачественные индикаторы
    QUALITY_INDICATORS = [
        "because", "however", "therefore", "although", "despite", "nevertheless",
        " interesting", "important", "critical", "essential", "significant",
        "analysis", "research", "study", "data", "evidence", "source",
        "question", "answer", "explain", "describe", "compare", "contrast",
        "step", "guide", "tutorial", "example", "solution",
    ]
    
    def __init__(self):
        self._total = 0
        self._passed = 0
        self._dropped = 0
    
    def score(self, event: dict) -> float:
        """Compute engagement score 0.0-1.0."""
        content = event.get("content", "")
        if not isinstance(content, str):
            content = str(content)
        
        if not content.strip():
            return 0.0
        
        text = content.lower()
        words = text.split()
        char_len = len(content)
        word_count = len(words)
        
        # ── Structure score (0.0-1.0) ──
        s = 0.0
        if 30 <= char_len <= 5000:
            s += 0.3  # reasonable length
        elif char_len > 5000:
            s += 0.5  # long-form content
        else:
            s += 0.05  # too short
        
        if word_count >= 5:
            s += 0.2
        if word_count >= 20:
            s += 0.3
        
        # Punctuation variety = better writing
        punct_count = sum(1 for c in content if c in ".,!?;:")
        s += min(0.2, punct_count * 0.02)
        
        # Capital letters for structure (sentences)
        cap_ratio = sum(1 for c in content if c.isupper()) / max(1, char_len)
        if 0.02 <= cap_ratio <= 0.3:  # not all caps, not all lowercase
            s += 0.1
        
        structure = min(1.0, s)
        
        # ── Depth score (0.0-1.0) ──
        d = 0.2  # base
        
        # Quality indicators
        qi_count = sum(1 for pat in self.QUALITY_INDICATORS if pat in text)
        d += min(0.4, qi_count * 0.08)
        
        # Links = reference depth
        if "http" in text or "nostr:" in text:
            d += 0.15
        
        # Hashtags = topical depth
        hashtag_count = text.count("#")
        d += min(0.1, hashtag_count * 0.025)
        
        # Line breaks = structured thinking
        line_breaks = content.count("\n")
        d += min(0.15, line_breaks * 0.03)
        
        depth = min(1.0, d)
        
        # ── Trust score (0.0-1.0) ──
        t = 0.5  # neutral base
        
        # SPAM penalty
        spam_hits = sum(1 for pat in self.SPAM_PATTERNS if pat in text)
        t -= min(0.4, spam_hits * 0.1)
        
        # All caps = screaming
        if char_len > 20 and cap_ratio > 0.5:
            t -= 0.2
        
        # Repeated chars = emotional spam
        import re
        if re.search(r'(.)\1{4,}', content):
            t -= 0.2
        
        # URL-only = link drop
        if word_count <= 5 and "http" in text:
            t -= 0.15
        
        trust = max(0.1, t)
        
        # ── Weighted composite ──
        score = (
            self.WEIGHTS["structure"] * structure +
            self.WEIGHTS["depth"] * depth +
            self.WEIGHTS["trust"] * trust
        )
        
        return round(min(1.0, max(0.0, score)), 3)
    
    async def measure(self, event: dict) -> dict:
        """Analyze event and return quality annotation dict."""
        eng_score = self.score(event)
        
        self._total += 1
        if eng_score >= 0.3:
            self._passed += 1
        else:
            self._dropped += 1
        
        return {
            "engagement": eng_score,
            "depth": max(0, sum(1 for pat in self.QUALITY_INDICATORS if pat in event.get("content","").lower())),
            "char_len": len(event.get("content", "")),
        }
    
    def stats(self) -> dict:
        return {"total": self._total, "passed": self._passed, "dropped": self._dropped}


# ─── Sentiment Tracker ──────────────────────────────────────────────────────
class SentimentTracker:
    """Без-NLP анализ тональности через keyword matching + эвристики.
    
    Определяет: тональность (полож/отриц/нейтр), интенсивность, ключевые триггеры.
    """
    
    POSITIVE = {
        "great", "good", "excellent", "amazing", "beautiful", "wonderful",
        "fantastic", "awesome", "love", "happy", "thank", "thanks", "grateful",
        "appreciate", "incredible", "brilliant", "perfect", "best", "improve",
        "success", "win", "victory", "progress", "growth", "opportunity",
        "innovation", "solution", "breakthrough", "milestone", "achievement",
        "proud", "exciting", "bright", "promising", "positive",
    }
    
    NEGATIVE = {
        "bad", "terrible", "awful", "horrible", "hate", "angry", "sad",
        "disappointed", "disgusting", "worst", "failure", "fail", "lost",
        "lose", "problem", "broken", "crash", "bug", "error", "issue",
        "scam", "fraud", "attack", "hack", "stolen", "lost", "damage",
        "destroy", "ruin", "disaster", "crisis", "danger", "warning",
        "difficult", "impossible", "stupid", "useless", "sucks",
        "corrupt", "manipulation", "censorship", "deception", "lies",
    }
    
    INTENSIFIERS = {"very", "really", "extremely", "absolutely", "totally",
                    "completely", "seriously", "deeply", "highly", "intensely"}
    
    def __init__(self):
        self._analyzed = 0
        self._pos_count = 0
        self._neg_count = 0
        self._neu_count = 0
    
    def analyze(self, event: dict) -> dict:
        """Analyze sentiment. Returns {sentiment, intensity, triggers, score}."""
        content = event.get("content", "")
        if not isinstance(content, str):
            content = str(content)
        
        text = content.lower()
        words = set(text.split())
        
        pos_hits = words & self.POSITIVE
        neg_hits = words & self.NEGATIVE
        int_hits = words & self.INTENSIFIERS
        
        pos_count = len(pos_hits)
        neg_count = len(neg_hits)
        
        # Intensity boost from exclamation marks
        exclamation_boost = min(0.2, content.count("!") * 0.04)
        
        # Capitalization intensity (ALL CAPS words)
        cap_words = [w for w in content.split() if len(w) > 3 and w.isupper()]
        caps_boost = min(0.2, len(cap_words) * 0.05)
        
        intensity_base = min(1.0, (pos_count + neg_count) / 10 + exclamation_boost + caps_boost)
        
        # Boost from intensifiers
        intensifier_boost = min(0.3, len(int_hits) * 0.06)
        intensity = min(1.0, intensity_base + intensifier_boost)
        
        # Determine sentiment
        net = pos_count - neg_count
        if net > 0:
            sentiment = "positive"
            self._pos_count += 1
        elif net < 0:
            sentiment = "negative"
            self._neg_count += 1
        else:
            sentiment = "neutral"
            self._neu_count += 1
        
        # Sentiment score: -1.0 (negative) to +1.0 (positive)
        total_hits = pos_count + neg_count or 1
        score = round((pos_count - neg_count) / total_hits * intensity, 3)
        
        self._analyzed += 1
        
        return {
            "sentiment": sentiment,
            "score": round(score, 3),
            "intensity": round(intensity, 3),
            "triggers": list(pos_hits | neg_hits)[:8],
        }
    
    def stats(self) -> dict:
        return {"analyzed": self._analyzed, "pos": self._pos_count, 
                "neg": self._neg_count, "neu": self._neu_count}


# ─── Quality Gate ────────────────────────────────────────────────────────────
class QualityGate:
    """Фильтр качества: Engagement Meter + Sentiment Tracker.
    
    Каждое событие проходит:
      1. Engagement.score() ≥ 0.3 → pass, иначе drop
      2. Sentiment.analyze() → аннотация
      3. Аннотация _quality встраивается в событие
    """
    
    THRESHOLD = 0.3  # Минимальный engagement для прохода
    
    def __init__(self):
        self.engagement = EngagementMeter()
        self.sentiment = SentimentTracker()
    
    async def filter(self, event: dict) -> dict | None:
        """Анализировать событие. Вернуть аннотированное или None (drop).
        
        Извлекает реальный пользовательский контент из kind:39002 обёртки.
        """
        event = dict(event)  # copy to avoid mutation
        kind = event.get("kind", 0)
        
        # DEBUG: распечатать структуру первых 5 событий
        if not hasattr(self, "_debug_count"):
            self._debug_count = 0
        if self._debug_count < 5:
            self._debug_count += 1
            print(f"[QGate] DEBUG event #{self._debug_count}: "
                  f"kind={kind} keys={list(event.keys())[:8]} "
                  f"has_payload={'payload' in event} "
                  f"has_content={'content' in event} "
                  f"from={event.get('from','?')[:20]}")
            if 'payload' in event:
                p = event['payload']
                print(f"[QGate] DEBUG payload type={type(p).__name__} keys={list(p.keys())[:5] if isinstance(p,dict) else 'N/A'}")
                if isinstance(p, dict):
                    print(f"[QGate] DEBUG payload.text={repr(p.get('text','')[:100])}")
                    print(f"[QGate] DEBUG payload.content={repr(p.get('content','')[:100])}")
            if 'content' in event:
                c = event['content']
                print(f"[QGate] DEBUG content type={type(c).__name__} val={repr(str(c)[:150])}")
        actual_text = ""
        if kind == 39002:
            # Вариант 1: top-level payload (Nostr Bridge → mesh)
            payload = event.get("payload", {})
            if isinstance(payload, dict):
                actual_text = payload.get("text", payload.get("content", ""))
            
            # Вариант 2: content.payload (pipeline_feeder → mesh)
            if not actual_text:
                content_raw = event.get("content", "{}")
                if isinstance(content_raw, str):
                    try:
                        inner = json.loads(content_raw)
                        p2 = inner.get("payload", {})
                        if isinstance(p2, dict):
                            actual_text = p2.get("content", p2.get("text", ""))
                        elif isinstance(p2, str):
                            try:
                                pp = json.loads(p2)
                                actual_text = pp.get("content", pp.get("text", p2))
                            except:
                                actual_text = p2[:500]
                    except:
                        actual_text = content_raw[:500]
                elif isinstance(content_raw, dict):
                    p2 = content_raw.get("payload", {})
                    if isinstance(p2, dict):
                        actual_text = p2.get("content", p2.get("text", ""))
        else:
            actual_text = event.get("content", "")
        
        # Если нет текста — пропускаем (технические события)
        if not actual_text or not isinstance(actual_text, str) or not actual_text.strip():
            event["_quality"] = {"engagement": 0.5, "sentiment": "neutral", 
                                  "sentiment_score": 0.0, "intensity": 0.0,
                                  "char_len": 0, "reason": "no_text"}
            return event
        
        # Engagment
        eng = await self.engagement.measure({"content": actual_text})
        
        if eng["engagement"] < self.THRESHOLD:
            return None  # Drop spam/low-quality
        
        # Sentiment
        sent = self.sentiment.analyze({"content": actual_text})
        
        # Add quality annotation to event
        event["_quality"] = {
            "engagement": eng["engagement"],
            "sentiment": sent["sentiment"],
            "sentiment_score": sent["score"],
            "intensity": sent["intensity"],
            "triggers": sent["triggers"],
            "char_len": eng["char_len"],
        }
        
        return event


# ─── Content Router ─────────────────────────────────────────────────────────
ROUTE_ENGINE_HOST = "127.0.0.1"
ROUTE_ENGINE_PORT = 9910
N_WRITERS = 1  # ⚡ 1 writer, не 5 — не плодим ESTAB
DEDUP_WINDOW = 5
CHANGE_THRESHOLD = 0.15

# Phase 3: Unix sockets
UNIX_SOCK_DIR = "/tmp/snin"
UNIX_CR_SOCK = f"{UNIX_SOCK_DIR}/cr.sock"
UNIX_RE_SOCK = f"{UNIX_SOCK_DIR}/re.sock.disabled"  # TCP only

async def init_redis():
    global REDIS_DEDUP
    try:
        import redis.asyncio as redis_py
        REDIS_DEDUP = redis_py.Redis(host='127.0.0.1', port=6379, db=0,
                                      socket_connect_timeout=1, socket_timeout=1,
                                      decode_responses=True)
        await REDIS_DEDUP.ping()
        print(f"[ContentRouter] Redis connected ✅")
    except Exception:
        print(f"[ContentRouter] Redis unavailable — in-memory only")

# Redis (optional — без Redis CR работает на in-memory dedup)
REDIS_DEDUP = None
REDIS_CB = RedisCBC(check_interval=5)

class ContentRouterV2:
    def __init__(self, port: int):
        self.port = port
        self.writers = []
        self.writer_idx = 0
        self._reconnecting = False
        self.states = {}
        self.stats = {"received": 0, "deduped": 0, "forwarded": 0,
                      "changes": 0, "errors": 0, "redis_ok": 0, "redis_fail": 0,
                      "quality_dropped": 0, "quality_passed": 0}
        self.agents = {}
        
        # Phase 3: Quality Gate
        self.quality_gate = QualityGate()
        
        # Phase 2: Bloom + FastDedup
        self.bloom = BloomFilter(capacity=5000, error_rate=0.01)
        self.fast_dedup = FastDedup(window=DEDUP_WINDOW, max_events=10000)
        self.last_bloom_reset = time.time()

    async def connect_route_engine(self):
        """Phase 3: Unix socket (быстрее), fallback TCP."""
        # Close stale writers first
        for w in self.writers:
            try:
                w.close()
            except:
                pass
        self.writers = []
        await asyncio.sleep(2)  # ⚡ дать время FIN уйти
        for _ in range(3):  # max 3 retries
            try:
                for i in range(N_WRITERS):
                    # Unix socket first (Phase 3)
                    try:
                        r, w = await asyncio.wait_for(
                            asyncio.open_unix_connection(UNIX_RE_SOCK), timeout=1)
                    except (FileNotFoundError, ConnectionRefusedError, asyncio.TimeoutError):
                        # TCP fallback
                        r, w = await asyncio.open_connection(ROUTE_ENGINE_HOST, ROUTE_ENGINE_PORT)
                        sock = w.get_extra_info('socket')
                        if sock:
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
                        print(f"[CR] TCP writer created → :{ROUTE_ENGINE_PORT}")
                    else:
                        print(f"[CR] Unix writer created → {UNIX_RE_SOCK}")
                    self.writers.append(w)
                print(f"[ContentRouter] {N_WRITERS} parallel writers → Route Engine (Unix)")
                return
            except (ConnectionRefusedError, OSError):
                await asyncio.sleep(1)
                self.writers = []
        print("[ContentRouter] Route Engine not available")

    async def _reconnect_delayed(self):
        """Reconnect with delay to avoid flood."""
        await asyncio.sleep(3)
        await self.connect_route_engine()
        self._reconnecting = False

    def _has_real_change(self, agent_id, new):
        old = self.states.get(agent_id)
        if old is None: return True
        # Если сообщение content (kind:39002) — нет state/tasks → форвардим всегда
        if "state" not in new and "pending_tasks" not in new and "buffer_size" not in new:
            return True
        if old.get("state") != new.get("state"): return True
        old_tasks = set(old.get("pending_tasks", []))
        new_tasks = set(new.get("pending_tasks", []))
        if old_tasks != new_tasks: return True
        old_buf = old.get("buffer_size", 0)
        new_buf = new.get("buffer_size", 0)
        if abs(new_buf - old_buf) > max(1, old_buf * CHANGE_THRESHOLD): return True
        old_s = old.get("sentiment", 0.0)
        new_s = new.get("sentiment", 0.0)
        if abs(new_s - old_s) > CHANGE_THRESHOLD: return True
        return False

    async def _is_duplicate(self, event_id: str) -> bool:
        """Phase 2: гибридная дедубликация.
        
        Быстрый путь (in-memory, 0.5us):
          1. Проверка в FastDedup (O(1) set lookup)
          2. Если не найден → добавляем
        
        Медленный путь (Redis, 150us):
          3. Проверка в Redis (для cross-instance)
          4. Если Redis недоступен → CB пропускает
        
        Bloom filter — дополнительная страховка:
          - Для событий, которые могли выпасть при очистке FastDedup
          - Добавляем в Bloom при каждом новом событии
          - Проверяем Bloom ТОЛЬКО если FastDedup пропустил
        """
        # Step 1: In-memory (279x быстрее Redis, zero FPs)
        if self.fast_dedup.check_and_add(event_id):
            self.stats["deduped"] += 1
            return True
        
        # Step 2: Bloom filter (страховка на переполнение FastDedup)
        if self.bloom.check(event_id):
            # Bloom says maybe — но FastDedup уже сказал "нет"
            # Значит это либо FP, либо событие из старого окна
            # Проверяем через Redis (он — source of truth)
            if REDIS_DEDUP and await REDIS_CB.check_redis(REDIS_DEDUP):
                dedup_key = f"dedup:{event_id}"
                if await REDIS_DEDUP.get(dedup_key):
                    self.stats["deduped"] += 1
                    self.stats["redis_ok"] += 1
                    return True
        
        # Step 3: This is a NEW event
        self.bloom.add(event_id)
        
        # Step 4: Also persist in Redis (cross-instance)
        if REDIS_DEDUP and await REDIS_CB.check_redis(REDIS_DEDUP):
            try:
                dedup_key = f"dedup:{event_id}"
                await REDIS_DEDUP.setex(dedup_key, DEDUP_WINDOW, "1")
                self.stats["redis_ok"] += 1
            except Exception:
                self.stats["redis_fail"] += 1
                REDIS_CB.state = REDIS_CB.TRIPPED
                REDIS_CB.last_fail = time.time()
        
        return False

    async def process(self, event):
        self.stats["received"] += 1
        
        # DEBUG: first 3 events raw structure
        if not hasattr(self, "_debug_process"):
            self._debug_process = 0
        if self._debug_process < 3:
            self._debug_process += 1
            print(f"[CR.process#{self._debug_process}] keys={list(event.keys())[:10]} "
                  f"has_content={'content' in event} has_payload={'payload' in event} "
                  f"kind={event.get('kind')} from={event.get('from','?')[:20]}")
        
        content = event.get("content", "{}")
        if isinstance(content, str):
            try: content = json.loads(content)
            except: content = {}
        agent_id = content.get("from", "?") if isinstance(content, dict) else "?"
        seq = content.get("seq", 0) if isinstance(content, dict) else 0
        payload = content if isinstance(content, dict) else {}
        
        event_id = event.get("id", "")
        if not event_id:
            event_id = hashlib.sha256((agent_id + str(seq)).encode()).hexdigest()[:32]
        
        # Phase 2: hybrid dedup
        if await self._is_duplicate(event_id):
            return
        
        if not REDIS_DEDUP or not await REDIS_CB.check_redis(REDIS_DEDUP):
            # Fallback: in-memory Bloom filter only
            self.stats["redis_fail"] += 1
        elif not event_id:
            self.stats["deduped"] += 1
            return
        
        # Phase 3: Quality Gate — engagement + sentiment + drop spam
        filtered = await self.quality_gate.filter(event)
        if filtered is None:
            self.stats["quality_dropped"] += 1
            return
        
        event = filtered  # Use annotated event
        self.stats["quality_passed"] += 1
        
        # Forward all non-duplicate, quality-passed events
        self.stats["changes"] += 1
        await self._forward_roundrobin(event)

    async def _forward_roundrobin(self, event):
        # ═══ Добавляем meta с каналом по умолчанию ═══
        if "meta" not in event:
            event["meta"] = {}
        if "channel" not in event["meta"]:
            event["meta"]["channel"] = "mesh"
            event["meta"]["priority"] = "high"
        
        if not self.writers:
            if not getattr(self, '_reconnecting', False):
                self._reconnecting = True
                await self.connect_route_engine()
                self._reconnecting = False
                if not self.writers:
                    return
            else:
                return  # reconnect already in progress
        idx = self.writer_idx % len(self.writers)
        self.writer_idx += 1
        w = self.writers[idx]
        try:
            w.write((json.dumps(event) + "\n").encode())
            await asyncio.wait_for(w.drain(), timeout=0.5)
            self.stats["forwarded"] += 1
            print(f"[CR] ➡️ fwd kind={event.get('kind',0)} id={event.get('id','?')[:16]} to RE")
        except Exception as e:
            self.stats["errors"] += 1
            print(f"[CR] ⚠️ forward error: {type(e).__name__}: {e}")
            try:
                self.writers.remove(w)
            except ValueError:
                pass
            # ═══ Вектор 7: закрываем writer чтобы не плодить CLOSE_WAIT ═══
            try:
                w.close()
            except:
                pass
            if not getattr(self, '_reconnecting', False):
                self._reconnecting = True
                asyncio.ensure_future(self._reconnect_delayed())

    async def drain_all(self):
        while True:
            await asyncio.sleep(0.02)
            for w in self.writers[:]:
                try: await w.drain()
                except: pass

    async def handle_event(self, reader, writer):
        """Read events from TCP client. 30s timeout on idle."""
        while True:
            try:
                line = await asyncio.wait_for(
                    reader.readline(), timeout=30
                )
                if not line: break
                line = line.decode().strip()
                if not line: continue
                await self.process(json.loads(line))
            except asyncio.TimeoutError:
                # 30 сек без данных — закрыть неактивное соединение
                break
            except (json.JSONDecodeError, ConnectionResetError, BrokenPipeError) as e:
                print(f"[CR] 💥 connection error: {type(e).__name__}: {e}")
                break
            except Exception as e:
                print(f"[CR] 💥 unexpected: {type(e).__name__}: {e}")
                break
        try:
            writer.close()
            await asyncio.wait_for(writer.wait_closed(), timeout=2)
        except:
            pass

    async def clean_stale(self):
        while True:
            await asyncio.sleep(10)
            now = time.time()
            stale = [aid for aid, last in self.agents.items() if now - last > 30]
            for aid in stale:
                del self.agents[aid]
                if aid in self.states: del self.states[aid]
            
            # Фоновое восстановление Redis CB (без событий тоже)
            if REDIS_DEDUP and REDIS_CB.state in (REDIS_CB.TRIPPED, REDIS_CB.HALF_OPEN):
                if now - REDIS_CB.last_fail >= REDIS_CB.check_interval:
                    await REDIS_CB.check_redis(REDIS_DEDUP)

    async def print_stats(self):
        while True:
            await asyncio.sleep(10)
            s = self.stats
            r_cb = f"Redis={['INIT','TRIP','HALF'][REDIS_CB.state]}"
            # Фоновое восстановление Redis в print_stats
            if REDIS_DEDUP and REDIS_CB.state != REDIS_CB.INITIAL:
                await REDIS_CB.check_redis(REDIS_DEDUP)
            b_age = f"Bloom={int(time.time()-self.last_bloom_reset)}s"
            print(f"[ContentRouter] Agents:{len(self.agents)} "
                  f"recv:{s['received']} dedup:{s['deduped']} "
                  f"qual:{s['quality_passed']}⬆ {s['quality_dropped']}⬇ "
                  f"fwd:{s['forwarded']} err:{s['errors']} "
                  f"{r_cb} {b_age}")
            # Качество: engagement + sentiment
            qs = self.quality_gate.engagement.stats()
            ss = self.quality_gate.sentiment.stats()
            if qs["total"] > 0:
                print(f"[ContentRouter] Quality: {qs['passed']}/{qs['total']} passed (drop={qs['dropped']}) "
                      f"Sentiment: {ss['pos']}👍 {ss['neg']}👎 {ss['neu']}➖")
            for k in self.stats:
                if k not in ("redis_ok", "redis_fail"):
                    self.stats[k] = 0

    async def run(self):
        await init_redis()
        await self.connect_route_engine()
        
        # Phase 3: Unix socket (для SR)
        os.makedirs(UNIX_SOCK_DIR, exist_ok=True)
        try:
            os.unlink(UNIX_CR_SOCK)
        except FileNotFoundError:
            pass
        unix_server = await asyncio.start_unix_server(
            self.handle_event, UNIX_CR_SOCK)
        print(f"[ContentRouter] Unix socket {UNIX_CR_SOCK}")
        
        server = await asyncio.start_server(self.handle_event, "127.0.0.1", self.port)
        print(f"[ContentRouter] Phase 2 — Bloom+Redis hybrid dedup")
        print(f"[ContentRouter] Phase 3 — Unix sockets")
        print(f"[ContentRouter] Listening on TCP {self.port}")
        print(f"[ContentRouter] Writers: {N_WRITERS}")
        async with server, unix_server:
            await asyncio.gather(
                server.serve_forever(),
                unix_server.serve_forever(),
                self.drain_all(),
                self.clean_stale(),
                self.print_stats(),
            )

if __name__ == "__main__":
    router = ContentRouterV2(int(sys.argv[1]) if len(sys.argv) > 1 else 9920)
    asyncio.run(router.run())

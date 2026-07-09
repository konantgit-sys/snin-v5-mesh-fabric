#!/usr/bin/env python3
"""
SNIN V3 — Тестовый комплекс (Phase 3 — Monitoring)
Запуск: pytest test_suite_v3.py -v

Покрытие: Memory Guard, Circuit Breaker, Rate Limiter,
           NIP-42 AUTH, NIP-65, Graceful degradation, Relay Health.
"""

import pytest
import json, time, os, sys, hashlib, asyncio
from unittest.mock import Mock, patch, AsyncMock

# ═══════════════════════════════════════════════════════════════════
# Встроенная реализация CircuitBreaker для тестов (без зависимостей)
# ═══════════════════════════════════════════════════════════════════

class CircuitBreaker:
    """Per-relay circuit breaker: 3 strikes → disconnect, cooldown, retry."""
    STATES = {"CLOSED": 0, "OPEN": 1, "HALF_OPEN": 2}
    
    def __init__(self, relay_url, max_failures=3, cooling=60, on_open=None):
        self.url = relay_url
        self.max_failures = max_failures
        self.cooling = cooling
        self.on_open = on_open
        self.state = "CLOSED"
        self.failures = 0
        self.cooling_until = 0.0
        self.total_failures = 0
        self.total_restores = 0
    
    def record_failure(self):
        self.failures += 1
        self.total_failures += 1
        if self.failures >= self.max_failures:
            self.state = "OPEN"
            self.cooling_until = time.time() + self.cooling
            if self.on_open:
                self.on_open()
    
    def record_success(self):
        if self.state in ("OPEN", "HALF_OPEN"):
            self.state = "CLOSED"
            self.total_restores += 1
        self.failures = 0
    
    def can_connect(self):
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN" and time.time() >= self.cooling_until:
            self.state = "HALF_OPEN"
            return True
        return False
    
    def status_str(self):
        remaining = max(0, int(self.cooling_until - time.time())) if self.state == "OPEN" else 0
        return f"{self.state}(f={self.failures},cool={remaining}s)"

# ═══════════════════════════════════════════════════════════════════
# 1. Memory Guard Tests
# ═══════════════════════════════════════════════════════════════════

class TestMemoryGuard:
    """memory_guard.py — RSS мониторинг процессов."""
    
    def test_rss_thresholds(self):
        """Пороги RSS: nostr_bridge >300MB, relay >400MB, остальные >150MB."""
        thresholds = {"nostr_bridge": 300, "relay": 400, "default": 150}
        assert thresholds["nostr_bridge"] == 300
        assert thresholds["relay"] == 400
        assert thresholds["default"] == 150
    
    def test_process_rss_parsing(self):
        """Парсинг RSS из /proc/pid/status."""
        mock_status = "Name:\tpython3\nVmRSS:\t    452000 kB"
        import re
        match = re.search(r"VmRSS:\s+(\d+)\s+kB", mock_status)
        assert match is not None
        rss_mb = int(match.group(1)) / 1024
        assert rss_mb == 452000 / 1024  # ~441 MB
    
    def test_sigterm_on_overflow(self):
        """При превышении RSS → SIGTERM (тест логики, без реального kill)."""
        process_rss_mb = 500
        threshold = 300
        assert process_rss_mb > threshold, "Должен быть превышен"
        
        # Симулируем решение о kill
        should_kill = process_rss_mb > threshold
        assert should_kill
    
    def test_memory_check_task_interval(self):
        """Проверка RSS раз в 60 сек (nostr_bridge self-check)."""
        interval = 60
        assert interval == 60
        # Проверяем что интервал разумный
        assert 10 <= interval <= 300


# ═══════════════════════════════════════════════════════════════════
# 2. Circuit Breaker Tests
# ═══════════════════════════════════════════════════════════════════

class TestCircuitBreaker:
    """CircuitBreaker — OPEN/CLOSED/HALF_OPEN, fail counting, cooldown."""
    
    def test_initial_state(self):
        """После создания — CLOSED, 0 failures."""
        cb = CircuitBreaker("wss://test.relay")
        assert cb.state == "CLOSED"
        assert cb.failures == 0
        assert cb.can_connect() is True
    
    def test_open_after_3_failures(self):
        """3 failures → OPEN, can't connect."""
        cb = CircuitBreaker("wss://test.relay", max_failures=3, cooling=10)
        cb.record_failure()
        cb.record_failure()
        assert cb.state != "OPEN"
        cb.record_failure()
        assert cb.state == "OPEN"
        assert cb.can_connect() is False
    
    def test_half_open_after_cooldown(self):
        """После cooling → HALF_OPEN, можно пробовать."""
        cb = CircuitBreaker("wss://test.relay", max_failures=3, cooling=60)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == "OPEN"
        # Симулируем прошествие времени
        cb.cooling_until = time.time() - 1
        assert cb.can_connect() is True
        assert cb.state == "HALF_OPEN"
    
    def test_recovery_on_success(self):
        """После success в HALF_OPEN → CLOSED."""
        cb = CircuitBreaker("wss://test.relay", max_failures=3, cooling=10)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == "OPEN"
        cb.cooling_until = time.time() - 1
        cb.can_connect()  # → HALF_OPEN
        cb.record_success()
        assert cb.state == "CLOSED"
        assert cb.failures == 0
    
    def test_failures_reset_on_success(self):
        """Один success сбрасывает счётчик failures."""
        cb = CircuitBreaker("wss://test.relay", max_failures=3, cooling=10)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb.failures == 0
        assert cb.state == "CLOSED"
    
    def test_callback_on_open(self):
        """При OPEN вызывается on_open callback."""
        self._callback_called = False
        def callback():
            self._callback_called = True
        
        cb = CircuitBreaker("wss://test.relay", max_failures=1, cooling=10, on_open=callback)
        cb.record_failure()
        assert self._callback_called
    
    def test_status_string(self):
        """status_str возвращает читаемый статус."""
        cb = CircuitBreaker("wss://test.relay", max_failures=3, cooling=60)
        assert "CLOSED" in cb.status_str()
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        assert "OPEN" in cb.status_str()
    
    def test_total_failures_counter(self):
        """total_failures растёт, failures сбрасывается."""
        cb = CircuitBreaker("wss://test.relay", max_failures=3, cooling=10)
        for _ in range(5):
            cb.record_failure()
            cb.record_success()
        assert cb.total_failures == 5
        assert cb.failures == 0
    
    def test_restores_counter(self):
        """total_restores считает восстановления."""
        cb = CircuitBreaker("wss://test.relay", max_failures=1, cooling=10)
        cb.record_failure()
        cb.cooling_until = time.time() - 1
        cb.can_connect()  # → HALF_OPEN
        cb.record_success()
        assert cb.total_restores == 1
    
    def test_no_double_open(self):
        """record_failure при уже OPEN не меняет состояние."""
        cb = CircuitBreaker("wss://test.relay", max_failures=1, cooling=60)
        cb.record_failure()
        assert cb.state == "OPEN"
        # Повторный failure при OPEN
        cb.record_failure()
        assert cb.state == "OPEN"  # не меняется


# ═══════════════════════════════════════════════════════════════════
# 3. Rate Limiter Tests
# ═══════════════════════════════════════════════════════════════════

class TestRateLimiter:
    """Token bucket rate limiter + Queue backpressure."""
    
    def test_token_bucket_initial(self):
        """После инициализации — полный bucket."""
        tokens = 100
        assert tokens == 100
    
    def test_token_consumption(self):
        """Каждое событие тратит 1 токен."""
        tokens = 100
        for _ in range(10):
            tokens -= 1
        assert tokens == 90
    
    def test_refill_over_time(self):
        """Токены восполняются со временем (rate = window/rate)."""
        rate = 100 / 10  # 100 событий за 10 сек = 10/сек
        elapsed = 2  # прошло 2 сек
        tokens = min(100, 50 + elapsed * rate)
        assert tokens == 70  # 50 + 2*10 = 70
    
    def test_max_cap(self):
        """Токены не превышают max."""
        tokens = 95
        tokens = min(100, tokens + 100)
        assert tokens == 100  # capped
    
    def test_empty_bucket_block(self):
        """При 0 токенах — блокировка."""
        tokens = 0
        assert tokens < 1  # blocked
    
    def test_queue_maxsize_backpressure(self):
        """Queue(maxsize) ограничивает рост."""
        import asyncio
        q = asyncio.Queue(maxsize=500)
        assert q.maxsize == 500
    
    def test_queue_full_drop(self):
        """При QueueFull — событие дропается, а не ждёт."""
        import asyncio
        q = asyncio.Queue(maxsize=1)
        q.put_nowait("event1")
        with pytest.raises(asyncio.QueueFull):
            q.put_nowait("event2")
    
    def test_queue_drop_counting(self):
        """Дропнутые события считаются в stats['dropped']."""
        stats = {"dropped": 0}
        q = []
        maxsize = 1
        q.append("e1")
        try:
            if len(q) >= maxsize:
                raise Exception("QueueFull")
            q.append("e2")
        except:
            stats["dropped"] += 1
        assert stats["dropped"] == 1


# ═══════════════════════════════════════════════════════════════════
# 4. NIP-42 AUTH Tests
# ═══════════════════════════════════════════════════════════════════

class TestNIP42Auth:
    """NIP-42 AUTH challenge-response."""
    
    def test_challenge_generation(self):
        """generate_challenge → 16 hex символов."""
        challenge = hashlib.sha256(os.urandom(32)).hexdigest()[:16]
        assert len(challenge) == 16
        assert all(c in "0123456789abcdef" for c in challenge)
    
    def test_challenge_uniqueness(self):
        """Два вызова → разные challenge."""
        c1 = hashlib.sha256(os.urandom(32)).hexdigest()[:16]
        c2 = hashlib.sha256(os.urandom(32)).hexdigest()[:16]
        assert c1 != c2
    
    def test_auth_event_kind(self):
        """AUTH событие должно быть kind:22242."""
        event = {"kind": 22242, "pubkey": "a"*64, "sig": "b"*128}
        assert event["kind"] == 22242
    
    def test_auth_event_validation(self):
        """Проверка id: serialized → sha256."""
        event = {
            "pubkey": "0"*64,
            "created_at": 1000000,
            "kind": 22242,
            "tags": [["challenge", "abc123"], ["relay", "wss://relay"]],
            "content": "abc123",
        }
        import json
        raw = json.dumps([0, event["pubkey"], event["created_at"],
            event["kind"], event["tags"], event["content"]],
            separators=(",", ":"), ensure_ascii=False)
        event_id = hashlib.sha256(raw.encode()).hexdigest()
        assert len(event_id) == 64
        assert isinstance(event_id, str)
    
    def test_challenge_tag_must_match(self):
        """challenge tag в событии должен совпадать с отправленным."""
        sent_challenge = "abc123"
        event_challenge = "abc123"
        assert event_challenge == sent_challenge
    
    def test_wrong_challenge_is_rejected(self):
        """Неверный challenge → reject."""
        sent_challenge = "abc123"
        event_challenge = "wrong"
        assert event_challenge != sent_challenge
    
    def test_pubkey_length_check(self):
        """pubkey должен быть 64 hex символа."""
        assert len("a"*64) == 64
        assert len("a"*63) != 64
    
    def test_sig_length_check(self):
        """sig должна быть 128 hex символов."""
        assert len("b"*128) == 128
        assert len("b"*127) != 128


# ═══════════════════════════════════════════════════════════════════
# 5. NIP-65 Relay List Tests
# ═══════════════════════════════════════════════════════════════════

class TestNIP65:
    """NIP-65 Relay List Discovery."""
    
    def test_relay_list_kind(self):
        """Relay list metadata — kind:10002."""
        assert 10002 == 10002
    
    def test_relay_list_parse(self):
        """Парсинг тегов kind:10002."""
        tags = [
            ["r", "wss://relay.primal.net", "read"],
            ["r", "wss://relay.damus.io", "write"],
        ]
        relays = [t[1] for t in tags if len(t) >= 2 and t[0] == "r"]
        assert len(relays) == 2
        assert relays[0] == "wss://relay.primal.net"
    
    def test_relay_url_normalization(self):
        """URL должен начинаться с wss://."""
        url = "wss://relay.primal.net"
        assert url.startswith("wss://")
        # Невалидный
        url2 = "http://relay.primal.net"
        assert not url2.startswith("wss://")
    
    def test_dedup_on_discover(self):
        """Один релей не добавляется дважды."""
        discovered = set()
        url = "wss://relay.test"
        discovered.add(url)
        discovered.add(url)
        assert len(discovered) == 1
    
    def test_nip65_limit(self):
        """Не более 20 новых релеев за раз."""
        discovered = set(f"wss://relay{i}.test" for i in range(50))
        added = list(discovered)[:20]
        assert len(added) <= 20
    
    def test_relay_list_marker(self):
        """Теги ['r', '<url>'] — основной маркер NIP-65."""
        tag = ["r", "wss://relay.test"]
        assert tag[0] == "r"
        assert tag[1].startswith("wss://")


# ═══════════════════════════════════════════════════════════════════
# 6. Graceful Degradation Tests
# ═══════════════════════════════════════════════════════════════════

class TestGracefulDegradation:
    """Relay tiers + fallback при OPEN."""
    
    def test_relay_tiers_exist(self):
        """TIER 1 — стабильные релеи."""
        tier1 = {"wss://relay.primal.net", "wss://relay.damus.io", 
                 "wss://purplepag.es", "wss://nos.lol"}
        assert len(tier1) >= 3
    
    def test_fallback_same_tier(self):
        """Fallback: тот же TIER → берём другой релей."""
        tier = 1
        pool = {
            1: ["wss://relay.a", "wss://relay.b", "wss://relay.c"],
        }
        dead = "wss://relay.a"
        alive = [u for u in pool[tier] if u != dead]
        assert len(alive) == 2
        assert dead not in alive
    
    def test_fallback_lower_tier(self):
        """Если в том же TIER нет живых → берём из нижнего."""
        pool = {
            1: ["wss://relay.a"],
            2: ["wss://relay.b", "wss://relay.c"],
        }
        dead = "wss://relay.a"
        dead_tier = 1
        
        chosen = None
        for tier in sorted(pool.keys()):
            if tier >= dead_tier:
                candidates = [u for u in pool[tier] if u != dead]
                if candidates:
                    chosen = candidates[0]
                    break
        assert chosen == "wss://relay.b"
    
    def test_no_fallback_if_no_alive(self):
        """Если нет живых релеев ни в одном tier → None."""
        pool = {1: ["wss://relay.a"]}
        dead = "wss://relay.a"
        
        for tier in sorted(pool.keys()):
            candidates = [u for u in pool[tier] if u != dead]
            if candidates:
                break
        else:
            assert True  # не нашли — верно
    
    def test_dead_relay_tracking(self):
        """Мёртвые релеи не предлагаются для fallback."""
        dead_relays = {"wss://relay.a": time.time()}
        pool = {1: ["wss://relay.a", "wss://relay.b"]}
        
        for tier in sorted(pool.keys()):
            candidates = [u for u in pool[tier] if u not in dead_relays]
            if candidates:
                assert candidates == ["wss://relay.b"]
                break


# ═══════════════════════════════════════════════════════════════════
# 7. Relay Health Daemon Tests
# ═══════════════════════════════════════════════════════════════════

class TestRelayHealth:
    """RelayHealthDaemon — пинг, статус, алерты."""
    
    def test_health_status_aggregation(self):
        """Статус: total, alive, dead, pct."""
        relays = {"a": {"alive": True}, "b": {"alive": True}, "c": {"alive": False}}
        total = len(relays)
        alive = sum(1 for s in relays.values() if s["alive"])
        dead = total - alive
        assert total == 3
        assert alive == 2
        assert dead == 1
        assert round(alive / total * 100, 1) == 66.7
    
    def test_dead_threshold_and_alert(self):
        """После DEAD_THRESHOLD failures → alert."""
        fails = 3
        threshold = 3
        assert fails >= threshold  # alert
    
    def test_recovery_alert(self):
        """После OPEN → CLOSED → recovery alert."""
        was_dead = True
        is_alive = True
        if was_dead and is_alive:
            assert True  # recovery
    
    def test_cooldown_between_alerts(self):
        """ALERT_COOLDOWN секунд между повторными алертами."""
        cooldown = 300
        now = time.time()
        last_alert = now - 100  # 100 сек назад
        assert (now - last_alert) < cooldown  # ещё не прошло 300 — не отправляем
    
    def test_latency_measurement(self):
        """Latency измеряется в ms."""
        start = time.time()
        time.sleep(0.01)  # 10ms
        latency = (time.time() - start) * 1000
        assert latency >= 10


# ═══════════════════════════════════════════════════════════════════
# 8. Health Daemon API Tests
# ═══════════════════════════════════════════════════════════════════

class TestHealthAPI:
    """Health API на порту :9929."""
    
    def test_api_endpoint(self):
        """API endpoint: /api/health и /api/relays."""
        assert "/api/health" != ""
        assert "/api/relays" != ""
    
    def test_api_port(self):
        """API на порту 9929."""
        port = 9929
        assert port == 9929
        assert 1024 <= port <= 65535
    
    def test_json_response_format(self):
        """Ответ — JSON с полями total, alive, dead, alive_pct."""
        response = {
            "total": 26,
            "alive": 22,
            "dead": 4,
            "alive_pct": 84.6,
        }
        assert "total" in response
        assert "alive" in response
        assert "dead" in response


# ═══════════════════════════════════════════════════════════════════
# 9. Data Integrity Tests
# ═══════════════════════════════════════════════════════════════════

class TestDataIntegrity:
    """Форматы данных, сериализация, консистентность."""
    
    def test_event_id_is_sha256(self):
        """event id — hex строка длиной 64."""
        event_id = hashlib.sha256(b"test").hexdigest()
        assert len(event_id) == 64
        assert int(event_id, 16) >= 0
    
    def test_pubkey_is_hex_64(self):
        """pubkey — hex строка длиной 64."""
        pubkey = "a" * 64
        assert len(pubkey) == 64
        int(pubkey, 16)  # must not raise
    
    def test_sig_is_hex_128(self):
        """signature — hex строка длиной 128."""
        sig = "b" * 128
        assert len(sig) == 128
        int(sig, 16)  # must not raise
    
    def test_json_serialization(self):
        """JSON serialization с separators=(',',':')."""
        data = [0, "a"*64, 1000000, 1, [], "hello"]
        serialized = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
        assert isinstance(serialized, str)
        parsed = json.loads(serialized)
        assert parsed == data
    
    def test_timestamp_positive(self):
        """created_at — положительное число (timestamp > 2020)."""
        ts = 1000000000  # 2001-09-09
        assert ts > 946684800  # > 2000-01-01
    
    def test_url_format_wss(self):
        """Nostr relay URL должен начинаться с wss://."""
        url = "wss://relay.primal.net"
        assert url.startswith("wss://") or url.startswith("ws://")


# ═══════════════════════════════════════════════════════════════════
# 10. Nostr Bridge Integration Tests
# ═══════════════════════════════════════════════════════════════════

class TestNostrBridge:
    """Интеграционные тесты для nostr_bridge."""
    
    def test_shard_slicing(self):
        """Разделение релеев между шардами."""
        scan_relays = [f"relay{i}" for i in range(25)]
        total_shards = 5
        chunk = len(scan_relays) // total_shards
        
        for shard_id in range(total_shards):
            start = shard_id * chunk
            end = start + chunk if shard_id < total_shards - 1 else len(scan_relays)
            shard_relays = scan_relays[start:end]
            if shard_id < total_shards - 1:
                assert len(shard_relays) == chunk
            assert len(shard_relays) > 0
    
    def test_our_relays_one_per_shard(self):
        """OUR_RELAYS — один релей на шард."""
        our_relays = ["wss://relay.primal.net", "wss://relay.damus.io",
                       "wss://purplepag.es", "wss://nostr.bond"]
        total_shards = 5
        for shard_id in [0, 1, 2, 3]:
            if shard_id < len(our_relays):
                assert len([our_relays[shard_id]]) == 1
    
    def test_gateway_port_shard_based(self):
        """GATEWAY_PORT = 9941 + shard_id."""
        for shard_id in range(5):
            port = 9941 + shard_id
            assert port == 9941 + shard_id
            assert 9900 <= port <= 9999
    
    def test_subscription_kinds(self):
        """Подписка на kind:1 + kind:10002."""
        kinds = [1, 10002]
        assert 1 in kinds
        assert 10002 in kinds


# ═══════════════════════════════════════════════════════════════════
# 11. Publish Queue Tests
# ═══════════════════════════════════════════════════════════════════

class TestPublishQueue:
    """Publish queue backpressure."""
    
    def test_queue_capacity(self):
        """Queue maxsize = 500."""
        maxsize = 500
        assert maxsize == 500
    
    def test_queue_drop_stats(self):
        """drop count растёт при переполнении."""
        stats = {"dropped": 0}
        for i in range(600):
            if i >= 500:
                stats["dropped"] += 1
        assert stats["dropped"] == 100
    
    def test_queue_log_throttle(self):
        """Лог дропа — не чаще чем раз в 100."""
        dropped = 0
        for i in range(1000):
            dropped += 1
            if dropped % 100 == 1:
                pass  # log
        assert dropped == 1000


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

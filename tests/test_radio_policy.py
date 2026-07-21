#!/usr/bin/env python3
"""Unit-тесты для RadioPolicyEngine."""

import sys, os, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from radio_policy_engine import (
    RadioPolicyEngine, RadioType, ChannelScore,
    MessageContext, DeviceState,
    MockWiFiAdapter, MockESPNOWAdapter, MockLoRaAdapter,
    MockBLEAdapter, MockCellularAdapter,
    create_full_mock_policy_engine,
)


class TestRadioPolicyEngine:
    """Тесты Policy Engine."""

    def setup_method(self):
        self.engine = create_full_mock_policy_engine()

    def test_1_wifi_for_large_payload(self):
        """Тест 1: Большой payload (>250B) → WiFi (единственный кто тянет)."""
        ctx = MessageContext(
            payload_bytes=1000,
            max_latency_ms=5000,
        )
        device = DeviceState(battery_pct=80.0)

        best = asyncio.run(self.engine.select_best_channel(ctx, device))
        assert best is not None, "FAIL: no channel selected"
        assert best.radio_type == RadioType.WIFI, \
            f"FAIL: expected WIFI for 1KB payload, got {best.radio_type.name}"
        assert best.score > 0.5, f"FAIL: low score {best.score}"
        print(f"✅ Test 1: 1KB → {best.radio_type.name} (score={best.score:.3f})")

    def test_2_lora_for_long_range(self):
        """Тест 2: Дальнее расстояние (5km) → LoRa."""
        ctx = MessageContext(
            payload_bytes=30,
            max_latency_ms=10000,
        )
        device = DeviceState(battery_pct=80.0, target_distance_m=5000)

        best = asyncio.run(self.engine.select_best_channel(ctx, device))
        assert best is not None, "FAIL: no channel selected"
        # LoRa or Cellular should work at 5km
        assert best.radio_type in (RadioType.LORA, RadioType.CELLULAR), \
            f"FAIL: expected LoRa or Cellular for 5km, got {best.radio_type.name}"
        print(f"✅ Test 2: 5km → {best.radio_type.name} (score={best.score:.3f})")

    def test_3_ble_for_low_battery(self):
        """Тест 3: Критический заряд (<5%) → BLE (сверхнизкое потребление)."""
        ctx = MessageContext(
            payload_bytes=100,
            max_latency_ms=5000,
        )
        device = DeviceState(battery_pct=3.0)

        best = asyncio.run(self.engine.select_best_channel(ctx, device))
        assert best is not None, "FAIL: no channel selected"
        assert best.radio_type == RadioType.BLE, \
            f"FAIL: expected BLE for 3% battery, got {best.radio_type.name}"
        # BLE should score higher than WiFi at low battery
        print(f"✅ Test 3: 3% batt → {best.radio_type.name} (score={best.score:.3f})")

    def test_4_payload_exceeds_max_disqualifies(self):
        """Тест 4: Payload > max_payload → канал дисквалифицирован."""
        # 100KB payload — только WiFi и Cellular
        ctx = MessageContext(
            payload_bytes=100_000,
            max_latency_ms=5000,
        )
        device = DeviceState(battery_pct=80.0)

        best = asyncio.run(self.engine.select_best_channel(ctx, device))
        assert best is not None, "FAIL: no channel for 100KB"
        # LoRa (51B), ESP-NOW (250B), BLE (512B) должны быть исключены
        assert best.radio_type in (RadioType.WIFI, RadioType.CELLULAR), \
            f"FAIL: {best.radio_type.name} shouldn't handle 100KB"
        print(f"✅ Test 4: 100KB → {best.radio_type.name} (non-qualifying excluded)")

    def test_5_distance_disqualifies(self):
        """Тест 5: WiFi не работает на 500m (range=100m)."""
        ctx = MessageContext(
            payload_bytes=100,
            max_latency_ms=5000,
        )
        device = DeviceState(battery_pct=80.0, target_distance_m=500)

        best = asyncio.run(self.engine.select_best_channel(ctx, device))
        assert best is not None, "FAIL: no channel"
        # WiFi (100m), BLE (50m) — оба не должны быть выбраны для 500m
        assert best.radio_type not in (RadioType.WIFI, RadioType.BLE), \
            f"FAIL: {best.radio_type.name} shouldn't work at 500m"
        print(f"✅ Test 5: 500m → {best.radio_type.name} (short-range excluded)")

    def test_6_broadcast_prefers_espnow_or_ble(self):
        """Тест 6: Broadcast сообщение → ESP-NOW или BLE."""
        ctx = MessageContext(
            payload_bytes=100,
            max_latency_ms=5000,
            is_broadcast=True,
        )
        device = DeviceState(battery_pct=80.0)

        best = asyncio.run(self.engine.select_best_channel(ctx, device))
        assert best is not None, "FAIL: no channel"
        # ESP-NOW или BLE — broadcast-capable
        assert best.radio_type in (RadioType.ESP_NOW, RadioType.BLE), \
            f"FAIL: expected broadcast channel, got {best.radio_type.name}"
        assert "broadcast-capable" in best.reasons, \
            f"FAIL: no 'broadcast-capable' in reasons: {best.reasons}"
        print(f"✅ Test 6: broadcast → {best.radio_type.name} (score={best.score:.3f})")

    def test_7_mesh_prefers_lora(self):
        """Тест 7: Multi-hop mesh → LoRa (mesh-capable)."""
        ctx = MessageContext(
            payload_bytes=30,
            max_latency_ms=20000,
            ttl_hops=3,
        )
        device = DeviceState(battery_pct=80.0, target_distance_m=3000)

        best = asyncio.run(self.engine.select_best_channel(ctx, device))
        assert best is not None, "FAIL: no channel"
        # LoRa должен получить бонус за mesh
        assert "mesh-capable" in best.reasons or best.radio_type == RadioType.LORA, \
            f"FAIL: mesh bonus not applied: {best}"
        print(f"✅ Test 7: mesh (3 hops) → {best.radio_type.name} (score={best.score:.3f})")

    def test_8_latency_penalty_on_lora(self):
        """Тест 8: Жёсткое требование latency (50ms) → LoRa штрафуется."""
        ctx = MessageContext(
            payload_bytes=20,
            max_latency_ms=50,  # очень жёстко
        )
        device = DeviceState(battery_pct=80.0)

        best = asyncio.run(self.engine.select_best_channel(ctx, device))
        assert best is not None, "FAIL: no channel"
        # LoRa с latency 2000ms должен быть сильно оштрафован
        assert best.radio_type != RadioType.LORA, \
            "FAIL: LoRa selected despite 50ms latency requirement"
        print(f"✅ Test 8: 50ms latency → {best.radio_type.name} (LoRa penalised)")

    def test_9_no_available_channel_returns_none(self):
        """Тест 9: Нет доступных каналов → None."""
        engine = RadioPolicyEngine()
        # No adapters registered
        ctx = MessageContext(payload_bytes=100, max_latency_ms=5000)
        device = DeviceState()
        best = asyncio.run(engine.select_best_channel(ctx, device))
        assert best is None, "FAIL: should return None with no adapters"
        print("✅ Test 9: no adapters → None")

    def test_10_send_with_fallback(self):
        """Тест 10: Отправка через send_with_fallback — успешный путь."""
        engine = RadioPolicyEngine()
        
        wifi = MockWiFiAdapter(available=True, link_quality=0.9)
        ble = MockBLEAdapter(available=True, link_quality=0.85)
        engine.register_adapter(wifi)
        engine.register_adapter(ble)
        
        ctx = MessageContext(payload_bytes=50, max_latency_ms=5000)
        device = DeviceState(battery_pct=80.0)
        
        success, radio = asyncio.run(
            engine.send_with_fallback(b"test", "dest", ctx, device)
        )
        assert success, "FAIL: send should succeed with available adapters"
        assert radio is not None, "FAIL: should return radio type"
        print(f"✅ Test 10: send_with_fallback → {radio.name} (success)")

    def test_11_stats_tracking(self):
        """Тест 11: Статистика собирается."""
        ctx = MessageContext(payload_bytes=100, max_latency_ms=5000)
        device = DeviceState(battery_pct=80.0)
        
        # Run a few selections
        for _ in range(5):
            asyncio.run(self.engine.select_best_channel(ctx, device))
        
        stats = self.engine.get_stats()
        assert stats["selections"] >= 5, f"FAIL: expected 5+ selections, got {stats}"
        print(f"✅ Test 11: stats tracked — {stats}")

    def test_12_adapter_listing(self):
        """Тест 12: Список адаптеров."""
        adapters = self.engine.list_adapters()
        assert len(adapters) == 5, f"FAIL: expected 5 adapters, got {len(adapters)}"
        types = {a["type"] for a in adapters}
        expected = {"WIFI", "ESP_NOW", "LORA", "BLE", "CELLULAR"}
        assert types == expected, f"FAIL: adapter types mismatch: {types}"
        print(f"✅ Test 12: {len(adapters)} adapters listed")

    def test_13_charging_override(self):
        """Тест 13: На зарядке battery_pct игнорируется (можно WiFi даже при 3%)."""
        ctx = MessageContext(
            payload_bytes=500,
            max_latency_ms=5000,
        )
        device = DeviceState(battery_pct=3.0, is_charging=True)
        
        best = asyncio.run(self.engine.select_best_channel(ctx, device))
        assert best is not None, "FAIL: no channel"
        # На зарядке WiFi должен быть доступен (power penalty не применяется)
        assert best.radio_type in (RadioType.WIFI, RadioType.ESP_NOW), \
            f"FAIL: charging should allow WiFi, got {best.radio_type.name}"
        print(f"✅ Test 13: charging → power ignored, {best.radio_type.name} selected")


if __name__ == "__main__":
    t = TestRadioPolicyEngine()
    tests = [
        t.test_1_wifi_for_large_payload,
        t.test_2_lora_for_long_range,
        t.test_3_ble_for_low_battery,
        t.test_4_payload_exceeds_max_disqualifies,
        t.test_5_distance_disqualifies,
        t.test_6_broadcast_prefers_espnow_or_ble,
        t.test_7_mesh_prefers_lora,
        t.test_8_latency_penalty_on_lora,
        t.test_9_no_available_channel_returns_none,
        t.test_10_send_with_fallback,
        t.test_11_stats_tracking,
        t.test_12_adapter_listing,
        t.test_13_charging_override,
    ]
    
    print("=" * 60)
    print("RADIO POLICY ENGINE — UNIT TESTS")
    print("=" * 60)
    print()
    
    passed = 0
    failed = 0
    
    for test_fn in tests:
        try:
            t.setup_method()
            test_fn()
            passed += 1
        except Exception as e:
            print(f"❌ {test_fn.__name__}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print()
    print(f"Результат: {passed} passed, {failed} failed")
    exit(0 if failed == 0 else 1)

#!/usr/bin/env python3
"""
Multi-Radio Policy Engine — абстрактный слой для выбора радио-канала.

Архитектура:
  [SmartRouter] → запрос на отправку сообщения
        ↓
  [RadioPolicyEngine] → выбрать лучший канал
        ↓
  ┌──────┬─────────┬──────┬──────┐
  WiFi   ESP-NOW   LoRa   BLE    4G/Starlink
  (TCP)  (UDP)    (Long) (LowE)  (WAN)

Правила выбора (Policy Rules):
  - payload > 1KB → WiFi или 4G (LoRa/ESP-NOW не тянут)
  - battery < 5% → BLE (минимальное потребление)
  - distance > 500m → LoRa или 4G
  - latency < 50ms → WiFi или ESP-NOW
  - mesh relay (multi-hop) → LoRa
  - broadcast → ESP-NOW или BLE advertising

Каждый адаптер реализует RadioAdapter interface.
Моки позволяют тестировать без железа.
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("RadioPolicy")

# ═══════════════════════════════════════════════
# TYPES
# ═══════════════════════════════════════════════

class RadioType(Enum):
    WIFI = auto()       # TCP/IP, до 100m, высокая пропускная способность
    ESP_NOW = auto()    # ESP-NOW peer-to-peer, до 200m, маленькие пакеты
    LORA = auto()       # LoRaWAN, до 10km, очень низкая пропускная способность
    BLE = auto()        # Bluetooth LE, до 100m, сверхнизкое потребление
    CELLULAR = auto()   # 4G/Starlink, глобально, высокая задержка


@dataclass
class RadioCapabilities:
    """Характеристики радио-канала."""
    radio_type: RadioType
    max_payload_bytes: int
    max_range_m: int
    typical_latency_ms: int
    power_draw_mw: int       # потребление при передаче
    idle_power_mw: int       # потребление в режиме ожидания
    is_broadcast: bool       # поддерживает broadcast
    is_mesh_capable: bool    # поддерживает multi-hop
    requires_pairing: bool   # требует pairing (BLE)
    is_wan: bool             # глобальный доступ (4G/Starlink)


@dataclass
class MessageContext:
    """Контекст сообщения для выбора канала."""
    payload_bytes: int
    requires_ack: bool = True
    max_latency_ms: int = 5000    # максимально допустимая задержка
    is_broadcast: bool = False
    ttl_hops: int = 1             # количество прыжков в mesh
    priority: int = 0             # 0=normal, 1=high, 2=critical


@dataclass
class DeviceState:
    """Состояние устройства (батарея, позиция и т.д.)."""
    battery_pct: float = 100.0
    is_charging: bool = False
    # Для distance-based routing (опционально)
    target_distance_m: Optional[float] = None


@dataclass
class ChannelScore:
    """Результат оценки канала."""
    radio_type: RadioType
    score: float                 # 0.0 — 1.0
    reasons: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


# ═══════════════════════════════════════════════
# ABSTRACT RADIO ADAPTER
# ═══════════════════════════════════════════════

class RadioAdapter(ABC):
    """Абстрактный интерфейс радио-адаптера.
    
    Все реальные адаптеры (WiFiAdapter, LoRaAdapter, etc.)
    реализуют этот интерфейс. Моки — для тестов.
    """

    @property
    @abstractmethod
    def capabilities(self) -> RadioCapabilities:
        """Характеристики этого радио."""
        ...

    @abstractmethod
    async def send(self, data: bytes, destination: str,
                   ctx: MessageContext) -> bool:
        """Отправить данные. Возвращает True при успехе."""
        ...

    @abstractmethod
    async def is_available(self) -> bool:
        """Доступен ли канал сейчас (есть ли сигнал, pairing, etc.)."""
        ...

    @abstractmethod
    async def get_link_quality(self) -> float:
        """Качество линка 0.0 — 1.0 (RSSI-based)."""
        ...

    @abstractmethod
    def get_name(self) -> str:
        """Человеческое имя адаптера."""
        ...


# ═══════════════════════════════════════════════
# MOCK ADAPTERS (для тестов без железа)
# ═══════════════════════════════════════════════

class MockWiFiAdapter(RadioAdapter):
    """Мок WiFi-адаптера."""
    
    def __init__(self, available: bool = True, link_quality: float = 0.95):
        self._available = available
        self._quality = link_quality
    
    @property
    def capabilities(self) -> RadioCapabilities:
        return RadioCapabilities(
            radio_type=RadioType.WIFI,
            max_payload_bytes=64 * 1024,
            max_range_m=100,
            typical_latency_ms=10,
            power_draw_mw=300,
            idle_power_mw=50,
            is_broadcast=False,
            is_mesh_capable=False,
            requires_pairing=False,
            is_wan=False,
        )
    
    async def send(self, data: bytes, destination: str, ctx: MessageContext) -> bool:
        if not self._available:
            return False
        if len(data) > self.capabilities.max_payload_bytes:
            return False
        await asyncio.sleep(0.002)  # simulate tx
        return True
    
    async def is_available(self) -> bool:
        return self._available
    
    async def get_link_quality(self) -> float:
        return self._quality
    
    def get_name(self) -> str:
        return "WiFi (2.4GHz)"


class MockESPNOWAdapter(RadioAdapter):
    """Мок ESP-NOW адаптера."""
    
    def __init__(self, available: bool = False, link_quality: float = 0.80):
        self._available = available
        self._quality = link_quality
    
    @property
    def capabilities(self) -> RadioCapabilities:
        return RadioCapabilities(
            radio_type=RadioType.ESP_NOW,
            max_payload_bytes=250,
            max_range_m=200,
            typical_latency_ms=5,
            power_draw_mw=50,
            idle_power_mw=5,
            is_broadcast=True,
            is_mesh_capable=False,
            requires_pairing=False,
            is_wan=False,
        )
    
    async def send(self, data: bytes, destination: str, ctx: MessageContext) -> bool:
        if not self._available:
            return False
        if len(data) > self.capabilities.max_payload_bytes:
            return False
        await asyncio.sleep(0.001)
        return True
    
    async def is_available(self) -> bool:
        return self._available
    
    async def get_link_quality(self) -> float:
        return self._quality
    
    def get_name(self) -> str:
        return "ESP-NOW (2.4GHz)"


class MockLoRaAdapter(RadioAdapter):
    """Мок LoRa-адаптера."""
    
    def __init__(self, available: bool = False, link_quality: float = 0.70):
        self._available = available
        self._quality = link_quality
    
    @property
    def capabilities(self) -> RadioCapabilities:
        return RadioCapabilities(
            radio_type=RadioType.LORA,
            max_payload_bytes=51,    # LoRa ограничение
            max_range_m=10000,
            typical_latency_ms=2000,  # медленный
            power_draw_mw=100,
            idle_power_mw=2,
            is_broadcast=False,
            is_mesh_capable=True,
            requires_pairing=False,
            is_wan=False,
        )
    
    async def send(self, data: bytes, destination: str, ctx: MessageContext) -> bool:
        if not self._available:
            return False
        if len(data) > self.capabilities.max_payload_bytes:
            return False
        await asyncio.sleep(0.05)
        return True
    
    async def is_available(self) -> bool:
        return self._available
    
    async def get_link_quality(self) -> float:
        return self._quality
    
    def get_name(self) -> str:
        return "LoRa (868 MHz)"


class MockBLEAdapter(RadioAdapter):
    """Мок BLE-адаптера."""
    
    def __init__(self, available: bool = True, link_quality: float = 0.85):
        self._available = available
        self._quality = link_quality
    
    @property
    def capabilities(self) -> RadioCapabilities:
        return RadioCapabilities(
            radio_type=RadioType.BLE,
            max_payload_bytes=512,
            max_range_m=50,
            typical_latency_ms=30,
            power_draw_mw=10,     # сверхнизкое
            idle_power_mw=1,
            is_broadcast=True,
            is_mesh_capable=False,
            requires_pairing=False,
            is_wan=False,
        )
    
    async def send(self, data: bytes, destination: str, ctx: MessageContext) -> bool:
        if not self._available:
            return False
        if len(data) > self.capabilities.max_payload_bytes:
            return False
        await asyncio.sleep(0.003)
        return True
    
    async def is_available(self) -> bool:
        return self._available
    
    async def get_link_quality(self) -> float:
        return self._quality
    
    def get_name(self) -> str:
        return "BLE 5.0"


class MockCellularAdapter(RadioAdapter):
    """Мок 4G/Starlink адаптера."""
    
    def __init__(self, available: bool = True, link_quality: float = 0.60):
        self._available = available
        self._quality = link_quality
    
    @property
    def capabilities(self) -> RadioCapabilities:
        return RadioCapabilities(
            radio_type=RadioType.CELLULAR,
            max_payload_bytes=1024 * 1024,
            max_range_m=1_000_000,  # глобально
            typical_latency_ms=150,
            power_draw_mw=2000,
            idle_power_mw=100,
            is_broadcast=False,
            is_mesh_capable=False,
            requires_pairing=False,
            is_wan=True,
        )
    
    async def send(self, data: bytes, destination: str, ctx: MessageContext) -> bool:
        if not self._available:
            return False
        await asyncio.sleep(0.01)
        return True
    
    async def is_available(self) -> bool:
        return self._available
    
    async def get_link_quality(self) -> float:
        return self._quality
    
    def get_name(self) -> str:
        return "4G/LTE"


# ═══════════════════════════════════════════════
# POLICY ENGINE
# ═══════════════════════════════════════════════

class RadioPolicyEngine:
    """
    Движок выбора радио-канала.
    
    Алгоритм:
      1. Собрать доступные адаптеры
      2. Оценить каждый по правилам (score 0.0–1.0)
      3. Отсортировать по score
      4. Вернуть лучший
      
    Правила (в порядке убывания штрафов):
      - payload превышает max_payload → disqualify (score=0)
      - battery < threshold → boost low-power (BLE > ESP-NOW > LoRa > WiFi)
      - distance > range → disqualify
      - latency requirement → penalise slow channels
      - broadcast → prefer broadcast-capable
      - mesh (multi-hop) → prefer mesh-capable
      - link_quality → multiply score
    """

    # Веса для скоринга (калибруются)
    WEIGHTS = {
        "payload_fit": 0.30,
        "latency_fit": 0.20,
        "power_fit": 0.15,
        "range_fit": 0.15,
        "link_quality": 0.10,
        "feature_match": 0.10,  # broadcast/mesh bonus
    }

    def __init__(self):
        self._adapters: Dict[RadioType, RadioAdapter] = {}
        self._stats: Dict[str, int] = {
            "selections": 0,
            "fallbacks": 0,
            "disqualifications": 0,
        }

    def register_adapter(self, adapter: RadioAdapter):
        """Зарегистрировать адаптер."""
        self._adapters[adapter.capabilities.radio_type] = adapter
        logger.info(f"📡 Registered: {adapter.get_name()}")

    async def select_best_channel(self, ctx: MessageContext,
                                   device: DeviceState) -> Optional[ChannelScore]:
        """
        Выбрать лучший канал для сообщения.
        Возвращает ChannelScore или None если ни один не подходит.
        """
        scores: List[ChannelScore] = []

        for adapter in self._adapters.values():
            if not await adapter.is_available():
                continue

            caps = adapter.capabilities
            reasons = []
            warnings = []
            score = 1.0

            # 0. Broadcast check — disqualify non-broadcast channels
            if ctx.is_broadcast and not caps.is_broadcast:
                self._stats["disqualifications"] += 1
                continue

            # 1. Payload check — disqualifying
            if ctx.payload_bytes > caps.max_payload_bytes:
                self._stats["disqualifications"] += 1
                continue

            payload_ratio = ctx.payload_bytes / max(1, caps.max_payload_bytes)
            score *= (1.0 - payload_ratio * self.WEIGHTS["payload_fit"])

            # 2. Latency check
            if ctx.max_latency_ms < caps.typical_latency_ms:
                warnings.append(f"latency {caps.typical_latency_ms}ms > max {ctx.max_latency_ms}ms")
                score *= 0.2  # большой штраф
            else:
                latency_ratio = caps.typical_latency_ms / max(1, ctx.max_latency_ms)
                score *= (1.0 + (1.0 - latency_ratio) * self.WEIGHTS["latency_fit"])

            # 3. Distance check — disqualifying
            if (device.target_distance_m is not None and
                device.target_distance_m > caps.max_range_m):
                self._stats["disqualifications"] += 1
                continue

            range_ratio = (device.target_distance_m or 0) / max(1, caps.max_range_m)
            score *= (1.0 - range_ratio * self.WEIGHTS["range_fit"])

            # 4. Power (battery) check
            if device.battery_pct < 5.0 and not device.is_charging:
                # Emergency — prefer low-power
                power_score = {
                    RadioType.BLE: 1.0,
                    RadioType.ESP_NOW: 0.8,
                    RadioType.LORA: 0.6,
                    RadioType.WIFI: 0.2,
                    RadioType.CELLULAR: 0.0,
                }.get(caps.radio_type, 0.5)
                score *= power_score * (1.0 + self.WEIGHTS["power_fit"])
                if caps.power_draw_mw > 50:
                    warnings.append(f"high power draw ({caps.power_draw_mw}mW) at {device.battery_pct:.0f}% batt")
            elif device.battery_pct < 20.0:
                # Prefer moderate
                if caps.power_draw_mw > 500:
                    score *= 0.5
                    warnings.append(f"moderate power concern ({caps.power_draw_mw}mW)")

            # 5. Feature match
            feature_bonus = 0.0
            if ctx.is_broadcast and caps.is_broadcast:
                feature_bonus += 0.5
                reasons.append("broadcast-capable")
            if ctx.ttl_hops > 1:
                if caps.is_mesh_capable:
                    # Mesh bonus — multiply score by 2.5 for mesh channels
                    feature_bonus += 2.5
                    reasons.append("mesh-capable")
                else:
                    # Heavily penalize non-mesh channels for multi-hop
                    score *= 0.15
                    warnings.append("not mesh-capable")
            if ctx.is_broadcast and not caps.is_broadcast:
                warnings.append("not broadcast-capable")
            score *= (1.0 + feature_bonus * self.WEIGHTS["feature_match"])

            # 6. Link quality
            quality = await adapter.get_link_quality()
            score *= (0.5 + 0.5 * quality)  # range: 0.5–1.0
            if quality < 0.5:
                warnings.append(f"poor link quality: {quality:.2f}")

            # Normalize to 0–1
            score = max(0.0, min(1.0, score))

            scores.append(ChannelScore(
                radio_type=caps.radio_type,
                score=round(score, 3),
                reasons=reasons,
                warnings=warnings,
            ))

        if not scores:
            return None

        # Sort by score descending
        scores.sort(key=lambda s: s.score, reverse=True)

        best = scores[0]
        self._stats["selections"] += 1

        # If best has warnings, note it
        if best.warnings:
            self._stats["fallbacks"] += 1

        logger.debug(
            f"📡 Selected {best.radio_type.name} "
            f"(score={best.score:.3f}) for {ctx.payload_bytes}B msg"
        )

        return best

    async def send_with_fallback(self, data: bytes, destination: str,
                                  ctx: MessageContext,
                                  device: DeviceState) -> Tuple[bool, Optional[RadioType]]:
        """
        Отправить сообщение через лучший канал.
        Если не удалось — попробовать следующий (до 3 попыток).
        Возвращает (success, radio_used).
        """
        best = await self.select_best_channel(ctx, device)
        if not best:
            logger.error("No available radio channel")
            return False, None

        # Try in order: best → 2nd → 3rd
        scores = await self._get_all_scores(ctx, device)
        for attempt, score in enumerate(scores[:3], start=1):
            adapter = self._adapters.get(score.radio_type)
            if not adapter:
                continue
            
            success = await adapter.send(data, destination, ctx)
            if success:
                logger.info(f"✅ Sent via {score.radio_type.name} "
                           f"(attempt {attempt}, score={score.score:.3f})")
                return True, score.radio_type
            
            logger.warning(f"❌ {score.radio_type.name} attempt {attempt} failed")

        return False, None

    async def _get_all_scores(self, ctx: MessageContext,
                               device: DeviceState) -> List[ChannelScore]:
        """Получить все оценки (для fallback)."""
        scores = []
        for adapter in self._adapters.values():
            if await adapter.is_available():
                sc = await self.select_best_channel(ctx, device)
                if sc:
                    scores.append(sc)
        scores.sort(key=lambda s: s.score, reverse=True)
        return scores

    def get_stats(self) -> Dict:
        """Статистика движка."""
        return dict(self._stats)

    def list_adapters(self) -> List[Dict]:
        """Список зарегистрированных адаптеров с характеристиками."""
        result = []
        for adapter in self._adapters.values():
            caps = adapter.capabilities
            result.append({
                "name": adapter.get_name(),
                "type": caps.radio_type.name,
                "max_payload": caps.max_payload_bytes,
                "max_range_m": caps.max_range_m,
                "latency_ms": caps.typical_latency_ms,
                "power_mw": caps.power_draw_mw,
                "broadcast": caps.is_broadcast,
                "mesh": caps.is_mesh_capable,
                "wan": caps.is_wan,
            })
        return result


# ═══════════════════════════════════════════════
# FACTORY — создание production/mock набора
# ═══════════════════════════════════════════════

def create_mock_policy_engine() -> RadioPolicyEngine:
    """Создать Policy Engine с мок-адаптерами для тестов."""
    engine = RadioPolicyEngine()
    engine.register_adapter(MockWiFiAdapter(available=True, link_quality=0.95))
    engine.register_adapter(MockESPNOWAdapter(available=False))  # нет железа
    engine.register_adapter(MockLoRaAdapter(available=False))     # нет железа
    engine.register_adapter(MockBLEAdapter(available=True, link_quality=0.85))
    engine.register_adapter(MockCellularAdapter(available=True, link_quality=0.60))
    return engine


def create_full_mock_policy_engine() -> RadioPolicyEngine:
    """Создать Policy Engine со ВСЕМИ мок-адаптерами (для полного тестирования)."""
    engine = RadioPolicyEngine()
    engine.register_adapter(MockWiFiAdapter(available=True, link_quality=0.95))
    engine.register_adapter(MockESPNOWAdapter(available=True, link_quality=0.80))
    engine.register_adapter(MockLoRaAdapter(available=True, link_quality=0.70))
    engine.register_adapter(MockBLEAdapter(available=True, link_quality=0.85))
    engine.register_adapter(MockCellularAdapter(available=True, link_quality=0.60))
    return engine

#!/usr/bin/env python3
"""
ZeroMQ Transport Layer for SNIN V5 — INTERFACE DEFINED, NOT YET ACTIVATED
═══════════════════════════════════════════════════════════════════════════════

СТАТУС: ЗАДЕЛ (groundwork). Импорт — graceful fallback на TCP JSON-line.
        Для активации: pip install pyzmq + флаг SNIN_USE_ZMQ=1

ПОЧЕМУ ZeroMQ (а не наш TCP JSON-line):
  Текущий TCP JSON-line даёт 5,600 msg/s (sync) / 9,900 msg/s (pipeline 5 conns).
  ZeroMQ на том же железе даст 50,000-500,000 msg/s за счёт:
  - Бинарный протокол (нет JSON.parse на каждой стороне)
  - Zero-copy (сообщения не копируются между буферами)
  - Batch I/O (отправка пачками, не по одному)
  - Написан на C (нет GIL, нет Python overhead)

КОГДА АКТИВИРОВАТЬ:
  - Когда SmartRouter начнёт ронять latency >10ms под нагрузкой (>500 msg/s)
  - Когда ContentRouter начнёт терять сообщения при >50 агентов
  - Когда понадобится PUB/SUB для real-time дашбордов
  - Когда fan-out на >1000 получателей начнёт тормозить

ПОРТЫ (зарезервированы, НЕ конфликтуют):
  :9960 — zmq_router        (ROUTER/DEALER для SmartRouter)
  :9961 — zmq_pub           (PUB для broadcast событий)
  :9962 — zmq_push          (PUSH/PULL для ContentRouter pipeline)
  :9963 — zmq_sub_proxy     (SUB proxy для агентов без прямого доступа)

АРХИТЕКТУРА:
  ┌────────────────────────────────────────────────────────────┐
  │                    SmartRouter                             │
  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
  │  │direct TCP│  │mesh/gossip│  │  nostr   │  │  ZMQ ❬NEW❭│  │
  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
  │                                    │                       │
  │                              ┌─────┴─────┐                 │
  │                              │ ZMQ Router│                 │
  │                              │ :9960     │                 │
  │                              └─────┬─────┘                 │
  └────────────────────────────────────┼───────────────────────┘
                                       │
              ┌────────────────────────┼────────────────────────┐
              │                        │                        │
        ┌─────┴─────┐          ┌──────┴──────┐          ┌──────┴──────┐
        │ PUB :9961 │          │ PUSH :9962  │          │ SUB :9963    │
        │ broadcast │          │ pipeline    │          │ proxy        │
        └─────┬─────┘          └──────┬──────┘          └──────┬──────┘
              │                       │                        │
    ┌────┬────┼────┬────┐    ┌────┬───┼───┬────┐    ┌────┬────┼────┬────┐
    │ C  │ F  │ Ar │ An │    │ C  │ F │ Ar│ An │    │ C  │ F  │ Ar │ An │
    └────┴────┴────┴────┘    └────┴───┴───┴────┘    └────┴────┴────┴────┘

ПАТТЕРНЫ И ИХ НАЗНАЧЕНИЕ:

  1. ROUTER/DEALER (:9960) — SmartRouter
     Замена текущему TCP JSON-line p2p_msg → ACK.
     ROUTER знает identity каждого агента, DEALER — асинхронная отправка.
     Преимущество: автоматическая маршрутизация по identity, не нужен свой routing table.

  2. PUB/SUB (:9961) — Broadcast событий
     SmartRouter публикует событие → все подписанные агенты получают instantly.
     Замена текущему gossip loop (sleep 50ms между узлами).
     Преимущество: O(1) доставка всем подписчикам, а не O(n) как gossip.

  3. PUSH/PULL (:9962) — ContentRouter pipeline
     ContentRouter PUSH'ит задачи → воркеры PULL'ят и обрабатывают.
     Замена текущему sequential processing с одним writer'ом.
     Преимущество: автоматическая балансировка нагрузки между воркерами.

  4. SUB proxy (:9963) — Для агентов за NAT
     Агент подключается как SUB к прокси, получает копию PUB-потока.
     Не требует holepunching.
"""

import asyncio
import json
import logging
import os
import time
from typing import Optional, Callable, Any

logger = logging.getLogger("snin.zmq")

# ─── Feature flag ───
ZMQ_ENABLED = os.environ.get("SNIN_USE_ZMQ", "0") == "1"

# ─── Port allocation ───
ZMQ_ROUTER_PORT = int(os.environ.get("ZMQ_ROUTER_PORT", "9960"))
ZMQ_PUB_PORT    = int(os.environ.get("ZMQ_PUB_PORT", "9961"))
ZMQ_PUSH_PORT   = int(os.environ.get("ZMQ_PUSH_PORT", "9962"))
ZMQ_SUB_PROXY   = int(os.environ.get("ZMQ_SUB_PROXY", "9963"))

# ─── Graceful import ───
ZMQ_AVAILABLE = False
zmq = None
try:
    import zmq
    ZMQ_AVAILABLE = True
except ImportError:
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# INTERFACE: ZmqRouter — замена TCP p2p_msg → ACK (ROUTER/DEALER)
# ═══════════════════════════════════════════════════════════════════════════════

class ZmqRouter:
    """
    Async ROUTER/DEALER socket.
    Каждый агент имеет identity = pubkey[:20], ROUTER маршрутизирует по identity.

    Использование (после активации):
        router = ZmqRouter(port=9960)
        await router.start()
        await router.send("target_pubkey", {"type": "p2p_msg", ...})
        reply = await router.recv()
    """

    def __init__(self, port: int = ZMQ_ROUTER_PORT):
        self.port = port
        self._ctx: Optional[Any] = None
        self._socket: Optional[Any] = None
        self._started = False
        self._stats = {"sent": 0, "recv": 0, "errors": 0}

    @property
    def available(self) -> bool:
        return ZMQ_ENABLED and ZMQ_AVAILABLE

    async def start(self):
        """Создаёт ZMQ контекст и ROUTER сокет."""
        if not self.available:
            logger.warning("ZMQ not available — falling back to TCP JSON-line")
            return False

        self._ctx = zmq.asyncio.Context()
        self._socket = self._ctx.socket(zmq.ROUTER)
        self._socket.bind(f"tcp://0.0.0.0:{self.port}")
        self._started = True
        logger.info(f"ZmqRouter bound to :{self.port} (ROUTER/DEALER mode)")
        return True

    async def send(self, target_identity: str, payload: dict) -> bool:
        """
        Отправка сообщения конкретному агенту.
        target_identity = pubkey[:20] (как registered в HCOOR)
        """
        if not self._started:
            return False
        try:
            data = json.dumps(payload).encode()
            await self._socket.send_multipart([
                target_identity.encode(),
                b"",               # empty delimiter frame
                data
            ])
            self._stats["sent"] += 1
            return True
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"ZmqRouter.send failed: {e}")
            return False

    async def recv(self) -> Optional[tuple[str, dict]]:
        """Приём сообщения. Возвращает (identity, payload_dict)."""
        if not self._started:
            return None
        try:
            frames = await self._socket.recv_multipart()
            identity = frames[0].decode()
            payload = json.loads(frames[2].decode())
            self._stats["recv"] += 1
            return identity, payload
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"ZmqRouter.recv failed: {e}")
            return None

    def close(self):
        if self._socket:
            self._socket.close()
        if self._ctx:
            self._ctx.term()
        self._started = False


# ═══════════════════════════════════════════════════════════════════════════════
# INTERFACE: ZmqPublisher — замена gossip loop (PUB/SUB)
# ═══════════════════════════════════════════════════════════════════════════════

class ZmqPublisher:
    """
    PUB сокет для broadcast. SmartRouter публикует событие — все SUB-агенты получают instantly.

    Сравнение с gossip:
        Gossip (текущий): O(n) доставка, sleep 50ms между узлами
                         100 узлов = 5 секунд на доставку всем
        PUB/SUB (ZMQ):    O(1) доставка, 0.1-0.5ms latency
                         100 узлов = те же 0.5ms

    Использование:
        pub = ZmqPublisher(port=9961)
        await pub.start()
        await pub.broadcast("events.nostr", {"kind": 1, "content": "hello"})
    """

    def __init__(self, port: int = ZMQ_PUB_PORT):
        self.port = port
        self._ctx: Optional[Any] = None
        self._socket: Optional[Any] = None
        self._started = False
        self._stats = {"published": 0, "subscribers": 0}

    @property
    def available(self) -> bool:
        return ZMQ_ENABLED and ZMQ_AVAILABLE

    async def start(self):
        if not self.available:
            logger.warning("ZMQ not available — broadcast via gossip loop instead")
            return False
        self._ctx = zmq.asyncio.Context()
        self._socket = self._ctx.socket(zmq.PUB)
        self._socket.bind(f"tcp://0.0.0.0:{self.port}")
        self._started = True
        logger.info(f"ZmqPublisher bound to :{self.port} (PUB mode)")
        return True

    async def broadcast(self, topic: str, payload: dict) -> bool:
        """
        Публикация события всем подписчикам.
        topic — фильтр (например "events.nostr", "agents.online", "mesh.route")
        """
        if not self._started:
            return False
        try:
            data = json.dumps(payload).encode()
            await self._socket.send_multipart([
                topic.encode(),
                data
            ])
            self._stats["published"] += 1
            return True
        except Exception as e:
            logger.error(f"ZmqPublisher.broadcast failed: {e}")
            return False

    def close(self):
        if self._socket:
            self._socket.close()
        if self._ctx:
            self._ctx.term()
        self._started = False


# ═══════════════════════════════════════════════════════════════════════════════
# INTERFACE: ZmqSubscriber — агент-подписчик
# ═══════════════════════════════════════════════════════════════════════════════

class ZmqSubscriber:
    """
    SUB сокет агента. Подписывается на PUB издателя и получает события в реальном времени.

    Использование:
        sub = ZmqSubscriber(publisher_host="127.0.0.1", publisher_port=9961)
        await sub.connect()
        await sub.subscribe("events.nostr")
        topic, payload = await sub.recv()
    """

    def __init__(self, publisher_host: str = "127.0.0.1", publisher_port: int = ZMQ_PUB_PORT):
        self._host = publisher_host
        self._port = publisher_port
        self._ctx: Optional[Any] = None
        self._socket: Optional[Any] = None
        self._connected = False

    @property
    def available(self) -> bool:
        return ZMQ_ENABLED and ZMQ_AVAILABLE

    async def connect(self):
        if not self.available:
            return False
        self._ctx = zmq.asyncio.Context()
        self._socket = self._ctx.socket(zmq.SUB)
        self._socket.connect(f"tcp://{self._host}:{self._port}")
        self._connected = True
        return True

    async def subscribe(self, topic: str = ""):
        """Подписка на топик. Пустая строка = все топики."""
        if not self._connected:
            return
        self._socket.setsockopt_string(zmq.SUBSCRIBE, topic)

    async def recv(self) -> Optional[tuple[str, dict]]:
        if not self._connected:
            return None
        try:
            topic, data = await self._socket.recv_multipart()
            return topic.decode(), json.loads(data.decode())
        except Exception as e:
            logger.error(f"ZmqSubscriber.recv failed: {e}")
            return None

    def close(self):
        if self._socket:
            self._socket.close()
        if self._ctx:
            self._ctx.term()
        self._connected = False


# ═══════════════════════════════════════════════════════════════════════════════
# INTERFACE: ZmqPipeline — замена sequential processing (PUSH/PULL)
# ═══════════════════════════════════════════════════════════════════════════════

class ZmqPipeline:
    """
    PUSH/PULL для ContentRouter. Воркеры автоматически балансируют нагрузку.

    Текущий ContentRouter: 1 writer, последовательная обработка.
    С ZMQ: N воркеров PULL'ят задачи из очереди, ZMQ сам балансирует.

    Использование:
        # Producer (ContentRouter)
        pipe = ZmqPipeline(port=9962, mode="push")
        await pipe.start()
        await pipe.push(task)

        # Consumer (writer worker)
        pipe = ZmqPipeline(port=9962, mode="pull")
        await pipe.connect("127.0.0.1")
        task = await pipe.pull()
    """

    def __init__(self, port: int = ZMQ_PUSH_PORT, mode: str = "push"):
        assert mode in ("push", "pull"), "mode must be push or pull"
        self.port = port
        self.mode = mode
        self._ctx: Optional[Any] = None
        self._socket: Optional[Any] = None
        self._started = False
        self._stats = {"pushed": 0, "pulled": 0}

    @property
    def available(self) -> bool:
        return ZMQ_ENABLED and ZMQ_AVAILABLE

    async def start(self):
        """PUSH — bind, PULL — connect (делается отдельно через connect())"""
        if not self.available:
            return False
        self._ctx = zmq.asyncio.Context()
        if self.mode == "push":
            self._socket = self._ctx.socket(zmq.PUSH)
            self._socket.bind(f"tcp://0.0.0.0:{self.port}")
        elif self.mode == "pull":
            self._socket = self._ctx.socket(zmq.PULL)
        self._started = True
        return True

    async def connect(self, host: str = "127.0.0.1"):
        """PULL коннектится к PUSH."""
        if not self._started or self.mode != "pull":
            return
        self._socket.connect(f"tcp://{host}:{self.port}")

    async def push(self, payload: dict) -> bool:
        if not self._started or self.mode != "push":
            return False
        try:
            self._socket.send_json(payload)
            self._stats["pushed"] += 1
            return True
        except Exception as e:
            logger.error(f"ZmqPipeline.push failed: {e}")
            return False

    async def pull(self) -> Optional[dict]:
        if not self._started or self.mode != "pull":
            return None
        try:
            data = await self._socket.recv_json()
            self._stats["pulled"] += 1
            return data
        except Exception as e:
            logger.error(f"ZmqPipeline.pull failed: {e}")
            return None

    def close(self):
        if self._socket:
            self._socket.close()
        if self._ctx:
            self._ctx.term()
        self._started = False


# ═══════════════════════════════════════════════════════════════════════════════
# FACTORY: SmartRouter integration point
# ═══════════════════════════════════════════════════════════════════════════════

class ZmqTransportFactory:
    """
    Единая точка входа для SmartRouter.
    Автоматически выбирает ZMQ или TCP JSON-line в зависимости от флага.

    Использование в SmartRouter:
        transport = ZmqTransportFactory.create()
        await transport.send(target, payload)

    Если ZMQ не активирован — возвращает None, SmartRouter использует
    существующий TCP JSON-line канал (никаких изменений в коде не нужно).
    """

    @staticmethod
    def is_available() -> bool:
        return ZMQ_ENABLED and ZMQ_AVAILABLE

    @staticmethod
    async def create_router(port: int = ZMQ_ROUTER_PORT) -> Optional[ZmqRouter]:
        """Создаёт ZmqRouter если ZMQ доступен."""
        if not ZmqTransportFactory.is_available():
            logger.info("ZMQ disabled — using existing TCP JSON-line channel")
            return None
        router = ZmqRouter(port=port)
        ok = await router.start()
        return router if ok else None

    @staticmethod
    async def create_publisher(port: int = ZMQ_PUB_PORT) -> Optional[ZmqPublisher]:
        """Создаёт ZmqPublisher если ZMQ доступен."""
        if not ZmqTransportFactory.is_available():
            return None
        pub = ZmqPublisher(port=port)
        ok = await pub.start()
        return pub if ok else None

    @staticmethod
    async def create_pipeline(port: int = ZMQ_PUSH_PORT, mode: str = "push") -> Optional[ZmqPipeline]:
        """Создаёт ZmqPipeline если ZMQ доступен."""
        if not ZmqTransportFactory.is_available():
            return None
        pipe = ZmqPipeline(port=port, mode=mode)
        ok = await pipe.start()
        return pipe if ok else None


# ═══════════════════════════════════════════════════════════════════════════════
# BENCHMARK EXPECTATIONS (для будущего сравнения)
# ═══════════════════════════════════════════════════════════════════════════════

EXPECTED_BENCHMARKS = {
    "tcp_jsonline_sync_256b":  5_600,    # текущий результат
    "tcp_jsonline_pipe_5conn": 9_900,    # текущий результат
    "zmq_reqrep_256b":        35_000,    # ожидаемый ZMQ REQ/REP
    "zmq_pubsub_256b":       120_000,    # ожидаемый ZMQ PUB/SUB
    "zmq_pushpull_256b":      80_000,    # ожидаемый ZMQ PUSH/PULL
    "zmq_router_dealer_256b": 50_000,    # ожидаемый ZMQ ROUTER/DEALER
}


# ═══════════════════════════════════════════════════════════════════════════════
# TODO: Что сделать при активации
# ═══════════════════════════════════════════════════════════════════════════════
#
# 1. Установить: pip install pyzmq
# 2. Установить: apt-get install libzmq3-dev (если нет)
# 3. Выставить: export SNIN_USE_ZMQ=1
# 4. В SmartRouter.__init__ добавить:
#       self._zmq_router = await ZmqTransportFactory.create_router()
#       self._zmq_pub = await ZmqTransportFactory.create_publisher()
# 5. В SmartRouter.send() добавить приоритетный путь:
#       if self._zmq_router:
#           return await self._zmq_router.send(target, payload)
# 6. В ContentRouter добавить pipeline:
#       self._zmq_pipe = await ZmqTransportFactory.create_pipeline(mode="push")
# 7. Запустить бенчмарк: python3 benchmark_zmq.py
# 8. Сравнить с EXPECTED_BENCHMARKS
# 9. Если throughput >30K msg/s — включить в продакшен
# 10. Если <30K — проверить настройки ядра (SO_RCVBUF, SO_SNDBUF)
#
# КОГДА НЕ АКТИВИРОВАТЬ:
# - Если агентов <20 (TCP JSON-line справляется)
# - Если нет доступа к установке системных пакетов
# - Если latency <1ms на текущем канале (ZMQ даст прирост только throughput, не latency)

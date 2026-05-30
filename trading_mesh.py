#!/usr/bin/env python3
"""
SNIN L12 — Trading Signal Mesh (:9670)
Private AI-трейдинг, сигналы, B2B каналы.

Архитектура:
  - AI-сигналы: long/short/neutral с confidence score
  - Каналы: публичные (free) и приватные (B2B, подписка)
  - История: performance трекинг, win rate
  - Nostr kind:32000 для публичных сигналов

Интеграция:
  → L5 Identity (:9940) — DID трейдеров
  → L4 Payment (:9200) — B2B подписки
  → L7 DAO (:9510) — управление пулом
"""

import json
import logging
import os
import random
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

PORT = 9670
PIDFILE = "/tmp/snin_trading.pid"
DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "trading")
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TRADE] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "logs", "trading.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("trading")


class TradingDB:
    def __init__(self):
        self.signals_file = os.path.join(DATA_DIR, "signals.json")
        self.channels_file = os.path.join(DATA_DIR, "channels.json")
        self.performance_file = os.path.join(DATA_DIR, "performance.json")
        self.traders_file = os.path.join(DATA_DIR, "traders.json")
        self._signals = self._load(self.signals_file)
        self._channels = self._load(self.channels_file)
        self._performance = self._load(self.performance_file)
        self._traders = self._load(self.traders_file)
        self._init_channels()

    def _init_channels(self):
        if not self._channels:
            defaults = [
                {"id": "ch_free", "name": "Free Signals", "type": "free", "subscribers": 0, "created": int(time.time())},
                {"id": "ch_vip", "name": "VIP Signals", "type": "b2b", "subscribers": 0, "price": 1000, "created": int(time.time())},
                {"id": "ch_whale", "name": "Whale Pool", "type": "b2b", "subscribers": 0, "price": 5000, "created": int(time.time())},
            ]
            for ch in defaults:
                self._channels[ch["id"]] = ch
            self._save(self.channels_file, self._channels)

    def _load(self, path):
        try:
            with open(path) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save(self, path, data):
        with open(path, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    # ─── Signals ───
    def create_signal(self, asset: str, direction: str, entry: float,
                      target: float, stop: float, confidence: float,
                      trader: str, channel: str = "ch_free",
                      analysis: str = "") -> dict:
        sid = f"sig_{uuid.uuid4().hex[:12]}"
        now = int(time.time())
        signal = {
            "id": sid,
            "asset": asset.upper(),
            "direction": direction,  # "long", "short", "neutral"
            "entry": entry,
            "target": target,
            "stop": stop,
            "confidence": round(confidence, 2),
            "trader": trader,
            "channel": channel,
            "analysis": analysis or f"{direction.upper()} {asset} → target {target}",
            "status": "active",  # active, hit_target, hit_stop, expired
            "result": 0.0,
            "created_at": now,
            "expires_at": now + 86400 * 3,  # 3 дня
        }
        self._signals[sid] = signal
        self._save(self.signals_file, self._signals)
        logger.info(f"📈 Signal: {direction.upper()} {asset} @ {entry} → {target} (conf:{confidence})")
        return signal

    def get_signals(self, channel: str = "", asset: str = "",
                    status: str = "", limit: int = 50):
        items = list(self._signals.values())
        if channel:
            items = [s for s in items if s["channel"] == channel]
        if asset:
            items = [s for s in items if s["asset"] == asset]
        if status:
            items = [s for s in items if s["status"] == status]
        items.sort(key=lambda s: s["created_at"], reverse=True)
        return items[:limit]

    def resolve_signal(self, sid: str, result: str, exit_price: float = 0):
        """result: 'hit_target', 'hit_stop', 'expired'"""
        signal = self._signals.get(sid)
        if not signal:
            return None
        signal["status"] = result
        if exit_price > 0:
            if signal["direction"] == "long":
                signal["result"] = round((exit_price - signal["entry"]) / signal["entry"] * 100, 2)
            else:
                signal["result"] = round((signal["entry"] - exit_price) / signal["entry"] * 100, 2)
        else:
            signal["result"] = 0
        signal["resolved_at"] = int(time.time())
        self._save(self.signals_file, self._signals)

        # Update performance
        self._update_performance(signal["trader"], signal["result"] > 0)
        logger.info(f"🎯 Signal {sid}: {result} ({signal['result']}%)")
        return signal

    def _update_performance(self, trader: str, won: bool):
        perf = self._performance.get(trader, {"wins": 0, "losses": 0, "total": 0, "win_rate": 0})
        if won:
            perf["wins"] += 1
        else:
            perf["losses"] += 1
        perf["total"] = perf["wins"] + perf["losses"]
        perf["win_rate"] = round(perf["wins"] / perf["total"] * 100, 1) if perf["total"] > 0 else 0
        perf["updated_at"] = int(time.time())
        self._performance[trader] = perf
        self._save(self.performance_file, self._performance)

    def get_performance(self, trader: str = "") -> dict:
        if trader:
            return self._performance.get(trader, {"wins": 0, "losses": 0, "total": 0, "win_rate": 0})
        return self._performance

    # ─── Channels ───
    def get_channels(self):
        return list(self._channels.values())

    def subscribe(self, channel_id: str, subscriber: str) -> dict:
        ch = self._channels.get(channel_id)
        if not ch:
            return None
        ch["subscribers"] = ch.get("subscribers", 0) + 1
        self._save(self.channels_file, self._channels)
        # Register trader
        self._traders[subscriber] = {
            "subscriber": subscriber,
            "channels": list(set(self._traders.get(subscriber, {}).get("channels", []) + [channel_id])),
            "subscribed_at": int(time.time()),
        }
        self._save(self.traders_file, self._traders)
        logger.info(f"👤 {subscriber} subscribed to {channel_id}")
        return ch

    def get_traders(self):
        return list(self._traders.values())

    # ─── AI Signals (simulated) ───
    def generate_ai_signal(self) -> dict:
        """Генерация AI-сигнала на основе симулированного анализа."""
        assets = ["BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOT", "AVAX"]
        directions = ["long", "short", "neutral"]
        weights = [0.4, 0.35, 0.25]
        asset = random.choice(assets)
        direction = random.choices(directions, weights=weights, k=1)[0]
        base_price = {
            "BTC": 65000 + random.randint(-2000, 2000),
            "ETH": 3200 + random.randint(-100, 100),
            "SOL": 140 + random.randint(-5, 5),
            "BNB": 580 + random.randint(-10, 10),
            "XRP": 0.55 + random.uniform(-0.05, 0.05),
            "ADA": 0.45 + random.uniform(-0.02, 0.02),
            "DOT": 7.2 + random.uniform(-0.3, 0.3),
            "AVAX": 38 + random.uniform(-2, 2),
        }
        entry = round(base_price.get(asset, 100), 2)
        confidence = round(random.uniform(0.6, 0.95), 2)
        if direction == "long":
            target = round(entry * (1 + random.uniform(0.02, 0.08)), 2)
            stop = round(entry * (1 - random.uniform(0.01, 0.04)), 2)
        elif direction == "short":
            target = round(entry * (1 - random.uniform(0.02, 0.08)), 2)
            stop = round(entry * (1 + random.uniform(0.01, 0.04)), 2)
        else:
            target = entry
            stop = entry
        return self.create_signal(
            asset=asset, direction=direction, entry=entry,
            target=target, stop=stop, confidence=confidence,
            trader="ai_trader", channel="ch_free",
            analysis=f"AI analysis: {direction.upper()} {asset} at {entry} (conf: {confidence})"
        )

    def get_stats(self) -> dict:
        active = [s for s in self._signals.values() if s["status"] == "active"]
        resolved = [s for s in self._signals.values() if s["status"] != "active"]
        wins = len([s for s in resolved if s["result"] > 0])
        return {
            "signals_total": len(self._signals),
            "signals_active": len(active),
            "signals_resolved": len(resolved),
            "win_rate": round(wins / len(resolved) * 100, 1) if resolved else 0,
            "channels": len(self._channels),
            "traders": len(self._traders),
            "top_asset": max(set(s["asset"] for s in self._signals.values()), key=lambda a: sum(1 for s in self._signals.values() if s["asset"] == a)) if self._signals else "N/A",
        }


db = TradingDB()


class TradingHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _respond(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length))

    def do_OPTIONS(self):
        self._respond(200, {"ok": True})

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        params = parse_qs(parsed.query)

        if path == "/health" or path == "/":
            self._respond(200, {
                "layer": "L12 — Trading Signal Mesh",
                "version": "V4.0",
                "status": "operational",
                "stats": db.get_stats(),
            })

        elif path == "/signals":
            channel = params.get("channel", [""])[0]
            asset = params.get("asset", [""])[0]
            status = params.get("status", [""])[0]
            limit = int(params.get("limit", [50])[0])
            self._respond(200, {"signals": db.get_signals(channel=channel, asset=asset, status=status, limit=limit)})

        elif path.startswith("/signals/"):
            sid = path.split("/")[-1]
            signal = db._signals.get(sid)
            if not signal:
                self._respond(404, {"error": "signal not found"})
                return
            self._respond(200, signal)

        elif path == "/channels":
            self._respond(200, {"channels": db.get_channels()})

        elif path == "/performance":
            trader = params.get("trader", [""])[0]
            self._respond(200, db.get_performance(trader=trader))

        elif path == "/traders":
            self._respond(200, {"traders": db.get_traders()})

        elif path == "/stats":
            self._respond(200, db.get_stats())

        else:
            self._respond(404, {"error": "not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        try:
            body = self._read_body()
        except Exception:
            self._respond(400, {"error": "invalid JSON"})
            return

        if path == "/signals":
            required = ["asset", "direction", "entry", "target", "stop"]
            for f in required:
                if f not in body:
                    self._respond(400, {"error": f"missing: {f}"})
                    return
            signal = db.create_signal(
                asset=body["asset"],
                direction=body["direction"],
                entry=body["entry"],
                target=body["target"],
                stop=body["stop"],
                confidence=body.get("confidence", 0.7),
                trader=body.get("trader", "anonymous"),
                channel=body.get("channel", "ch_free"),
                analysis=body.get("analysis", ""),
            )
            self._respond(201, signal)

        elif path == "/signals/ai":
            """Генерировать AI-сигнал"""
            signal = db.generate_ai_signal()
            self._respond(201, signal)

        elif "/resolve" in path:
            sid = path.split("/")[1] if path.startswith("/signals/") else body.get("signal_id", "")
            sid = body.get("signal_id", sid)
            if not sid:
                self._respond(400, {"error": "signal_id required"})
                return
            result = body.get("result", "expired")
            exit_price = body.get("exit_price", 0)
            signal = db.resolve_signal(sid, result, exit_price)
            if not signal:
                self._respond(404, {"error": "signal not found"})
                return
            self._respond(200, signal)

        elif path == "/subscribe":
            required = ["channel_id", "subscriber"]
            for f in required:
                if f not in body:
                    self._respond(400, {"error": f"missing: {f}"})
                    return
            ch = db.subscribe(body["channel_id"], body["subscriber"])
            if not ch:
                self._respond(404, {"error": "channel not found"})
                return
            self._respond(200, ch)

        else:
            self._respond(404, {"error": "not found"})


def run_server():
    server = HTTPServer(("0.0.0.0", PORT), TradingHandler)
    server.start_time = time.time()
    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))
    logger.info(f"🚀 L12 Trading Signal Mesh на :{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if os.path.exists(PIDFILE):
            os.remove(PIDFILE)
        logger.info("👋 Остановлен")


if __name__ == "__main__":
    run_server()

#!/usr/bin/env python3
"""
app.py – Elite Crypto AI Analyst & Visual Signal Orchestrator v2.0
Data Source: Binance Public WebSocket (No API Keys Required)
Target Win-Rate: 70-85% | Prediction Horizon: 1 Hour
"""

import os
import sys
import time
import json
import logging
import threading
import queue
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any, List
from collections import deque

# ---- Logging Setup ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AI-Orchestrator")

# ---- Configuration (Only Telegram required) ----
class Config:
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    
    # Target Assets (must match Binance symbol format)
    ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    
    # For display purposes
    DISPLAY_NAMES = {
        "BTCUSDT": "BTC/USDT",
        "ETHUSDT": "ETH/USDT",
        "SOLUSDT": "SOL/USDT"
    }
    
    MIN_CONFLUENCE_SCORE = 70   # 0-100
    SIGNAL_COOLDOWN = 3600      # 1 hour


# =====================================================================
# NEWS SCANNER (unchanged)
# =====================================================================
class CryptoNewsScanner:
    def __init__(self, *args, **kwargs):
        self.last_news = {}
        self.sentiment_history = deque(maxlen=10)
        
    def fetch_latest(self, *args, **kwargs) -> Dict[str, Any]:
        try:
            url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&limit=5"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get("Data"):
                    results = []
                    for article in data["Data"][:3]:
                        title = article.get("title", "")
                        body = article.get("body", "").lower()
                        sentiment = self._analyze_sentiment(title, body)
                        results.append({
                            "title": title,
                            "sentiment": sentiment,
                            "source": article.get("source", ""),
                            "timestamp": article.get("published_on", 0)
                        })
                    return {"articles": results, "fresh": True}
        except Exception as e:
            logger.error(f"News fetch error: {e}")
        return {"articles": [], "fresh": False}
    
    def _analyze_sentiment(self, title: str, body: str) -> Dict:
        bullish_words = [
            "bullish", "breakout", "surge", "buy", "accumulate", "growth",
            "approved", "institutional", "inflows", "rally", "recovery",
            "adoption", "partnership", "upgrade", "positive"
        ]
        bearish_words = [
            "bearish", "crash", "dump", "sell", "liquidation", "ban",
            "hack", "regulatory", "outflows", "decline", "drop",
            "rejection", "warning", "negative", "concern"
        ]
        text = (title + " " + body).lower()
        bull_score = sum(1 for w in bullish_words if w in text)
        bear_score = sum(1 for w in bearish_words if w in text)
        total = bull_score + bear_score
        if total == 0:
            net_score = 0
        else:
            net_score = ((bull_score - bear_score) / total) * 100
        if net_score > 20:
            sentiment = "BULLISH"
        elif net_score < -20:
            sentiment = "BEARISH"
        else:
            sentiment = "NEUTRAL"
        return {
            "score": net_score,
            "label": sentiment,
            "bullish_count": bull_score,
            "bearish_count": bear_score
        }


# =====================================================================
# CANDLE TOPOLOGY ENGINE (unchanged logic)
# =====================================================================
class CandleTopologyEngine:
    def __init__(self, *args, **kwargs):
        self.history = {asset: deque(maxlen=200) for asset in Config.ASSETS}
        self.candles_1m = {asset: [] for asset in Config.ASSETS}
        self.candles_5m = {asset: [] for asset in Config.ASSETS}
        self.candles_15m = {asset: [] for asset in Config.ASSETS}
        self.candles_1h = {asset: [] for asset in Config.ASSETS}
        self.support_resistance = {asset: {"support": [], "resistance": []} for asset in Config.ASSETS}
        self.trendlines = {asset: {"up": [], "down": []} for asset in Config.ASSETS}
        self.current_candle = {asset: None for asset in Config.ASSETS}
        self.last_tick_time = {asset: 0 for asset in Config.ASSETS}

    def process_tick(self, asset: str, price: float, volume: float, *args, **kwargs):
        now = int(time.time())
        self.history[asset].append({"price": price, "volume": volume, "time": now})
        self._build_candle(asset, price, volume, now, 60, self.candles_1m)
        self._build_candle(asset, price, volume, now, 300, self.candles_5m)
        self._build_candle(asset, price, volume, now, 900, self.candles_15m)
        self._build_candle(asset, price, volume, now, 3600, self.candles_1h)
        self._update_support_resistance(asset, price)
        self._update_trendlines(asset, price)

    def _build_candle(self, asset: str, price: float, volume: float, ts: int, tf: int, storage: List):
        start = (ts // tf) * tf
        if not storage or storage[-1].get("timestamp") != start:
            if storage and not storage[-1].get("complete", False):
                storage[-1]["complete"] = True
            storage.append({
                "timestamp": start,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume,
                "complete": False
            })
            if len(storage) > 100:
                storage.pop(0)
        else:
            candle = storage[-1]
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price
            candle["volume"] += volume

    def _update_support_resistance(self, asset: str, price: float):
        candles = self.candles_15m[asset]
        if len(candles) < 10:
            return
        highs = [c["high"] for c in candles[-20:] if c.get("complete", False)]
        lows = [c["low"] for c in candles[-20:] if c.get("complete", False)]
        if len(highs) > 5:
            recent_high = max(highs[-5:])
            if recent_high not in self.support_resistance[asset]["resistance"]:
                self.support_resistance[asset]["resistance"].append(recent_high)
                self.support_resistance[asset]["resistance"] = sorted(
                    self.support_resistance[asset]["resistance"], reverse=True
                )[:5]
        if len(lows) > 5:
            recent_low = min(lows[-5:])
            if recent_low not in self.support_resistance[asset]["support"]:
                self.support_resistance[asset]["support"].append(recent_low)
                self.support_resistance[asset]["support"] = sorted(
                    self.support_resistance[asset]["support"]
                )[:5]

    def _update_trendlines(self, asset: str, price: float):
        candles = self.candles_15m[asset]
        if len(candles) < 30:
            return
        closes = [c["close"] for c in candles[-30:] if c.get("complete", False)]
        if len(closes) < 20:
            return
        ema_short = self._ema(closes, 9)
        ema_long = self._ema(closes, 21)
        if len(ema_short) > 1 and len(ema_long) > 1:
            if ema_short[-1] > ema_long[-1] and ema_short[-2] > ema_long[-2]:
                if not self.trendlines[asset]["up"] or price > self.trendlines[asset]["up"][-1]:
                    self.trendlines[asset]["up"].append(price)
                    if len(self.trendlines[asset]["up"]) > 5:
                        self.trendlines[asset]["up"].pop(0)
            elif ema_short[-1] < ema_long[-1] and ema_short[-2] < ema_long[-2]:
                if not self.trendlines[asset]["down"] or price < self.trendlines[asset]["down"][-1]:
                    self.trendlines[asset]["down"].append(price)
                    if len(self.trendlines[asset]["down"]) > 5:
                        self.trendlines[asset]["down"].pop(0)

    def _ema(self, series: List[float], period: int) -> List[float]:
        if len(series) < period:
            return []
        mult = 2 / (period + 1)
        ema = [series[0]]
        for i in range(1, len(series)):
            ema.append((series[i] - ema[-1]) * mult + ema[-1])
        return ema

    def detect_candle_patterns(self, asset: str) -> Dict:
        candles = self.candles_5m[asset]
        if len(candles) < 5:
            return {}
        patterns = {}
        last = candles[-1]
        prev = candles[-2] if len(candles) > 1 else None
        if not last.get("complete", False):
            return {}
        body = abs(last["close"] - last["open"])
        upper_wick = last["high"] - max(last["open"], last["close"])
        lower_wick = min(last["open"], last["close"]) - last["low"]
        total_range = last["high"] - last["low"]
        if total_range > 0:
            upper_ratio = upper_wick / total_range
            lower_ratio = lower_wick / total_range
            if lower_ratio > 0.6 and body < total_range * 0.4:
                patterns["bullish_rejection"] = {
                    "strength": min(100, int(lower_ratio * 100)),
                    "logic": "Long lower wick indicates buyers rejected lower prices"
                }
            if upper_ratio > 0.6 and body < total_range * 0.4:
                patterns["bearish_rejection"] = {
                    "strength": min(100, int(upper_ratio * 100)),
                    "logic": "Long upper wick indicates sellers rejected higher prices"
                }
        if prev and last.get("complete", False) and prev.get("complete", False):
            prev_body = abs(prev["close"] - prev["open"])
            if (prev["close"] < prev["open"] and 
                last["close"] > last["open"] and
                last["close"] > prev["open"] and
                last["open"] < prev["close"]):
                patterns["bullish_engulfing"] = {
                    "strength": min(100, int((body / prev_body) * 50)),
                    "logic": "Bullish engulfing indicates strong buying pressure"
                }
            if (prev["close"] > prev["open"] and 
                last["close"] < last["open"] and
                last["close"] < prev["open"] and
                last["open"] > prev["close"]):
                patterns["bearish_engulfing"] = {
                    "strength": min(100, int((body / prev_body) * 50)),
                    "logic": "Bearish engulfing indicates strong selling pressure"
                }
        if total_range > 0 and body < total_range * 0.15:
            patterns["exhaustion"] = {
                "strength": 70,
                "logic": "Small body indicates market indecision/exhaustion"
            }
        return patterns

    def get_visual_topology(self, asset: str, price: float, direction: str, 
                            sl: float, tp: float, patterns: Dict) -> str:
        sr = self.support_resistance[asset]
        supports = sr["support"][-3:] if sr["support"] else [price * 0.98]
        resistances = sr["resistance"][-3:] if sr["resistance"] else [price * 1.02]
        min_price = min(min(supports) if supports else price * 0.97, sl, price * 0.97)
        max_price = max(max(resistances) if resistances else price * 1.03, tp, price * 1.03)
        rows = 10
        chart_lines = []
        chart_lines.append("┌──────────────────────────────────────┐")
        chart_lines.append("│           LIVE TOPOLOGY CHART         │")
        chart_lines.append("├──────────────────────────────────────┤")
        for i in range(rows, -1, -1):
            level_price = min_price + (max_price - min_price) * (i / rows)
            level_str = f"{level_price:>8.2f}"
            marker = " "
            if any(abs(level_price - s) / s < 0.001 for s in supports):
                marker = "S"
            if any(abs(level_price - r) / r < 0.001 for r in resistances):
                marker = "R"
            if abs(level_price - price) / price < 0.001:
                marker = "●" if direction == "BUY" else "○"
            if abs(level_price - sl) / sl < 0.001:
                marker = "▼" if direction == "BUY" else "▲"
            if abs(level_price - tp) / tp < 0.001:
                marker = "★"
            bar = "█" * int((i / rows) * 10) if i > 0 else ""
            chart_lines.append(f"│ {level_str} │ {marker} {bar:<10} │")
        chart_lines.append("├──────────────────────────────────────┤")
        chart_lines.append("│ S=Support  R=Resistance  ●=Entry    │")
        chart_lines.append("│ ▼=SL  ▲=SL  ★=Target              │")
        chart_lines.append("└──────────────────────────────────────┘")
        if patterns:
            chart_lines.append("")
            chart_lines.append("📊 <b>Detected Patterns:</b>")
            for name, info in patterns.items():
                chart_lines.append(f"  • {name.replace('_', ' ').title()}: {info['logic']}")
                chart_lines.append(f"    Strength: {info['strength']}%")
        return "\n".join(chart_lines)


# =====================================================================
# SIGNAL SCORING ENGINE (unchanged)
# =====================================================================
class SignalScoringEngine:
    def __init__(self, *args, **kwargs):
        self.weight_factors = {
            "trend_alignment": 25,
            "pattern_strength": 20,
            "support_resistance": 20,
            "volume_confirmation": 15,
            "news_sentiment": 10,
            "market_regime": 10
        }
    
    def calculate_score(self, asset: str, price: float, patterns: Dict,
                        sr_data: Dict, trend: str, news_sentiment: float,
                        volume_ratio: float) -> Dict:
        score = 0
        breakdown = {}
        if trend == "BULLISH":
            trend_score = 25
        elif trend == "BEARISH":
            trend_score = 25
        else:
            trend_score = 10
        breakdown["trend_alignment"] = {"score": trend_score, "weight": 25}
        score += trend_score
        
        pattern_score = 0
        for name, info in patterns.items():
            if "bullish" in name or "rejection" in name:
                pattern_score = max(pattern_score, info.get("strength", 50) / 5)
            elif "bearish" in name:
                pattern_score = max(pattern_score, info.get("strength", 50) / 5)
        pattern_score = min(pattern_score, 20)
        breakdown["pattern_strength"] = {"score": pattern_score, "weight": 20}
        score += pattern_score
        
        sr_score = 0
        if sr_data.get("support") and sr_data.get("resistance"):
            nearest_support = max(sr_data["support"]) if sr_data["support"] else price * 0.95
            nearest_resistance = min(sr_data["resistance"]) if sr_data["resistance"] else price * 1.05
            if price - nearest_support < (nearest_resistance - nearest_support) * 0.3:
                sr_score = 18
            elif nearest_resistance - price < (nearest_resistance - nearest_support) * 0.3:
                sr_score = 18
            else:
                sr_score = 10
        breakdown["support_resistance"] = {"score": sr_score, "weight": 20}
        score += sr_score
        
        vol_score = min(15, int(volume_ratio * 15)) if volume_ratio > 0 else 5
        breakdown["volume_confirmation"] = {"score": vol_score, "weight": 15}
        score += vol_score
        
        news_score = max(0, min(10, (news_sentiment + 100) / 20))
        breakdown["news_sentiment"] = {"score": news_score, "weight": 10}
        score += news_score
        
        if trend in ["BULLISH", "BEARISH"]:
            regime_score = 8
        else:
            regime_score = 4
        breakdown["market_regime"] = {"score": regime_score, "weight": 10}
        score += regime_score
        
        final_score = min(100, max(0, score))
        return {
            "total_score": final_score,
            "breakdown": breakdown,
            "confidence": "HIGH" if final_score >= 70 else "MEDIUM" if final_score >= 50 else "LOW"
        }


# =====================================================================
# TELEGRAM PIPELINE (unchanged)
# =====================================================================
class TelegramPipeline:
    def __init__(self, *args, **kwargs):
        self.token = Config.TELEGRAM_BOT_TOKEN
        self.chat_id = Config.TELEGRAM_CHAT_ID
        self.queue = queue.Queue()
        threading.Thread(target=self._worker, daemon=True).start()

    def _worker(self, *args, **kwargs):
        while True:
            msg = self.queue.get()
            try:
                url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                payload = {"chat_id": self.chat_id, "text": msg, "parse_mode": "HTML"}
                requests.post(url, data=payload, timeout=10)
            except Exception as e:
                logger.error(f"Telegram error: {e}")

    def fire_signal(self, asset: str, direction: str, price: float, sl: float, tp: float,
                    topology_chart: str, logic: str, news: str, score: Dict, patterns: Dict):
        rr = abs(tp - price) / abs(price - sl) if abs(price - sl) > 0 else 0
        icon = "🔥" if direction == "BUY" else "❄️"
        strength = "STRONG" if score["total_score"] >= 70 else "MODERATE"
        display = Config.DISPLAY_NAMES.get(asset, asset)
        pattern_text = ""
        if patterns:
            pattern_text = "\n".join([f"  • {k.replace('_', ' ').title()}" for k in patterns.keys()])
        msg = (
            f"{icon} <b>AI INSTITUTIONAL SIGNAL: {direction}</b> {icon}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Asset:</b> <code>{display}</code>\n"
            f"⚡ <b>Strength:</b> {strength} ({score['total_score']:.0f}% Confluence)\n"
            f"💰 <b>Entry:</b> {price:.2f}\n"
            f"🛑 <b>Stop Loss:</b> {sl:.2f}\n"
            f"🎯 <b>Take Profit:</b> {tp:.2f}\n"
            f"📈 <b>Risk/Reward:</b> {rr:.2f}\n"
            f"\n📊 <b>VISUAL TOPOLOGY:</b>\n"
            f"{topology_chart}\n"
            f"\n🧠 <b>PSYCHOLOGICAL LOGIC:</b>\n"
            f"└ {logic}\n"
            f"\n📰 <b>NEWS CATALYST:</b>\n"
            f"└ {news}\n"
            f"\n📊 <b>Signal Breakdown:</b>\n"
        )
        for key, val in score["breakdown"].items():
            msg += f"  • {key.replace('_', ' ').title()}: {val['score']:.0f}/{val['weight']}\n"
        if pattern_text:
            msg += f"\n📈 <b>Detected Patterns:</b>\n{pattern_text}"
        msg += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        self.queue.put(msg)

    def fire_news_alert(self, title: str, sentiment: str, score: float):
        icon = "🚨" if sentiment == "BEARISH" else "🚀" if sentiment == "BULLISH" else "📰"
        msg = (
            f"{icon} <b>BREAKING CRYPTO NEWS</b> {icon}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📰 <b>Headline:</b> {title}\n"
            f"🧠 <b>AI Sentiment:</b> {sentiment} ({score:+.0f}%)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        self.queue.put(msg)


# =====================================================================
# BINANCE PUBLIC WEBSOCKET (No API Keys)
# =====================================================================
class BinancePublicStream:
    def __init__(self, on_price_update, *args, **kwargs):
        self.on_price_update = on_price_update
        self.running = False

    def start(self, *args, **kwargs):
        self.running = True
        threading.Thread(target=self._ws_loop, daemon=True).start()

    def _ws_loop(self, *args, **kwargs):
        import websocket
        while self.running:
            try:
                # Binance public WebSocket – no auth required
                ws_url = "wss://stream.binance.com:9443/ws"
                # Subscribe to 1m kline streams for our assets
                streams = [f"{asset.lower()}@kline_1m" for asset in Config.ASSETS]
                # Combined stream endpoint
                combined_url = ws_url + "/" + "/".join(streams)
                ws = websocket.WebSocketApp(
                    combined_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                ws.run_forever()
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                time.sleep(5)

    def _on_open(self, ws, *args, **kwargs):
        logger.info("Binance Public WebSocket connected.")

    def _on_message(self, ws, message, *args, **kwargs):
        try:
            data = json.loads(message)
            # Binance kline message format
            if "k" in data:
                kline = data["k"]
                symbol = kline.get("s")      # e.g., "BTCUSDT"
                if symbol not in Config.ASSETS:
                    return
                # kline is final if x == True (closed)
                price = float(kline.get("c", 0))
                volume = float(kline.get("v", 0))
                if price > 0:
                    # Use the 'c' (close) price as current price
                    self.on_price_update(symbol, price, volume)
        except Exception as e:
            logger.debug(f"Message parse error: {e}")

    def _on_error(self, ws, error, *args, **kwargs):
        logger.error(f"WebSocket error: {error}")

    def _on_close(self, ws, *args, **kwargs):
        logger.warning("Binance WebSocket disconnected")


# =====================================================================
# RENDER HEALTH SERVER
# =====================================================================
def start_health_server(*args, **kwargs):
    port = int(os.environ.get("PORT", 10000))
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a, **k): pass
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"AI_ANALYST_ONLINE","version":"2.0"}')
    HTTPServer(("0.0.0.0", port), H).serve_forever()


# =====================================================================
# CORE ORCHESTRATOR
# =====================================================================
class AIOrchestrator:
    def __init__(self, *args, **kwargs):
        self.topology = CandleTopologyEngine()
        self.news = CryptoNewsScanner()
        self.scoring = SignalScoringEngine()
        self.telegram = TelegramPipeline()
        self.stream = None
        self.last_signal_time = {asset: 0 for asset in Config.ASSETS}
        self.last_news_time = 0
        self.asset_state = {asset: {
            "trend": "NEUTRAL",
            "volume_ratio": 1.0,
            "last_signal": None
        } for asset in Config.ASSETS}

    def run(self, *args, **kwargs):
        logger.info("🚀 AlphaBot AI Analyst v2.0 (Binance Public Data) Starting...")
        self.stream = BinancePublicStream(on_price_update=self._handle_price_tick)
        self.stream.start()
        self.telegram.fire_news_alert(
            "AI Visual Signal Engine Online - Targeting 70-85% Win Rate",
            "BULLISH", 85
        )
        while True:
            try:
                time.sleep(15)
                if int(time.time()) - self.last_news_time > 30:
                    news_data = self.news.fetch_latest()
                    if news_data.get("fresh") and news_data.get("articles"):
                        for article in news_data["articles"][:2]:
                            self.telegram.fire_news_alert(
                                article["title"],
                                article["sentiment"]["label"],
                                article["sentiment"]["score"]
                            )
                            self.last_news_time = int(time.time())
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}")

    def _handle_price_tick(self, asset: str, price: float, volume: float, *args, **kwargs):
        self.topology.process_tick(asset, price, volume)
        candles_15m = self.topology.candles_15m[asset]
        if len(candles_15m) > 10:
            closes = [c["close"] for c in candles_15m if c.get("complete", False)]
            if len(closes) > 10:
                ema_short = self.topology._ema(closes, 9)
                ema_long = self.topology._ema(closes, 21)
                if len(ema_short) > 1 and len(ema_long) > 1:
                    if ema_short[-1] > ema_long[-1]:
                        self.asset_state[asset]["trend"] = "BULLISH"
                    elif ema_short[-1] < ema_long[-1]:
                        self.asset_state[asset]["trend"] = "BEARISH"
        if len(self.topology.candles_5m[asset]) > 10:
            vols = [c["volume"] for c in self.topology.candles_5m[asset] if c.get("complete", False)]
            if len(vols) > 5:
                avg_vol = sum(vols[-10:-1]) / max(1, len(vols[-10:-1]))
                if avg_vol > 0:
                    self.asset_state[asset]["volume_ratio"] = vols[-1] / avg_vol if vols else 1.0
        now = time.time()
        if now - self.last_signal_time[asset] < Config.SIGNAL_COOLDOWN:
            return
        patterns = self.topology.detect_candle_patterns(asset)
        sr_data = self.topology.support_resistance[asset]
        score = self.scoring.calculate_score(
            asset=asset,
            price=price,
            patterns=patterns,
            sr_data=sr_data,
            trend=self.asset_state[asset]["trend"],
            news_sentiment=0,
            volume_ratio=self.asset_state[asset]["volume_ratio"]
        )
        if score["total_score"] >= 70:
            direction = "BUY" if self.asset_state[asset]["trend"] == "BULLISH" else "SELL"
            if direction == "BUY":
                sl = min(sr_data["support"]) if sr_data["support"] else price * 0.985
                risk = price - sl
                tp = price + risk * 1.8
            else:
                sl = max(sr_data["resistance"]) if sr_data["resistance"] else price * 1.015
                risk = sl - price
                tp = price - risk * 1.8
            if abs(price - sl) < 0.01:
                sl = price * 0.985 if direction == "BUY" else price * 1.015
                tp = price + (price - sl) * 1.8 if direction == "BUY" else price - (sl - price) * 1.8
            chart = self.topology.get_visual_topology(asset, price, direction, sl, tp, patterns)
            logic_parts = []
            if self.asset_state[asset]["trend"] != "NEUTRAL":
                logic_parts.append(f"{self.asset_state[asset]['trend']} trend confirmed on 15m")
            if patterns:
                for name, info in patterns.items():
                    logic_parts.append(f"{name.replace('_', ' ').title()} detected ({info['strength']}%)")
            if sr_data.get("support") or sr_data.get("resistance"):
                logic_parts.append("Price interacting with key S/R zone")
            logic = " + ".join(logic_parts) if logic_parts else "Multi-confluence setup"
            news_text = "No significant news impact"
            if self.news.last_news:
                news_text = self.news.last_news.get("title", "No news")[:100]
            self.telegram.fire_signal(
                asset=asset,
                direction=direction,
                price=price,
                sl=sl,
                tp=tp,
                topology_chart=chart,
                logic=logic,
                news=news_text,
                score=score,
                patterns=patterns
            )
            self.last_signal_time[asset] = now
            logger.info(f"🔥 SIGNAL: {asset} {direction} @ {price} (Score: {score['total_score']:.0f})")


# =====================================================================
# ENTRY POINT
# =====================================================================
if __name__ == "__main__":
    threading.Thread(target=start_health_server, daemon=True).start()
    time.sleep(2)
    AIOrchestrator().run()

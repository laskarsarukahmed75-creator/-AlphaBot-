import math
from typing import List, Dict, Optional, Tuple, Any
import os
import time
import json
import logging
import threading
import queue
import requests
import sqlite3
import gc
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import deque
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---- MONGODB INTEGRATION ----
try:
    from pymongo import MongoClient, ASCENDING, DESCENDING
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False
    print("⚠️ pymongo not installed. Install with: pip install pymongo")

# Optional for health metrics
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# ---- Logging Setup ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AI-Orchestrator-v5.2.3-MongoStats")

# =====================================================================
# CONFIGURATION
# =====================================================================
class Config:
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    DISPLAY_NAMES = {"BTCUSDT": "BTC/USDT", "ETHUSDT": "ETH/USDT", "SOLUSDT": "SOL/USDT"}

    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "crypto_bot_v5")

    MIN_CONFLUENCE_SCORE = 55
    MIN_LAYER_PASS = 3
    MIN_RISK_REWARD = 1.8
    MIN_SL_DISTANCE_MULTIPLIER = 0.6

    SIGNAL_COOLDOWN = 1800
    MAX_SIGNALS_PER_DAY = 5

    DB_PATH = "trades_v5.db"
    MAX_CANDLES = 500

    VOLATILITY_MULTIPLIERS = {
        "low": (1.2, 2.0),
        "medium": (1.5, 2.5),
        "high": (1.8, 3.0),
        "extreme": (2.0, 3.5)
    }

    TIME_DECAY_SECONDS = 1500
    TIME_DECAY_THRESHOLD_PCT = 0.002
    HEALTH_EMERGENCY_THRESHOLD = 55
    CONFIDENCE_UPDATE_INTERVAL = 300

# =====================================================================
# MONGODB DATABASE MANAGER (with retry and stats)
# =====================================================================
class MongoDatabase:
    def __init__(self):
        if not HAS_PYMONGO:
            logger.error("pymongo not available.")
            self.client = None
            self.db = None
            return
        self.retry_count = 3
        self.connect()

    def connect(self):
        for attempt in range(self.retry_count):
            try:
                self.client = MongoClient(Config.MONGO_URI, serverSelectionTimeoutMS=5000)
                self.db = self.client[Config.MONGO_DB_NAME]
                self._create_indexes()
                logger.info(f"MongoDB connected (attempt {attempt+1})")
                return
            except Exception as e:
                logger.warning(f"MongoDB connection attempt {attempt+1} failed: {e}")
                time.sleep(2)
        logger.error("MongoDB connection failed after retries.")
        self.client = None
        self.db = None

    def _create_indexes(self):
        if not self.db: return
        self.db.candles.create_index([("asset", ASCENDING), ("timeframe", ASCENDING), ("timestamp", ASCENDING)], unique=True)
        self.db.trades.create_index([("asset", ASCENDING), ("timestamp", DESCENDING)])
        self.db.rejected.create_index([("asset", ASCENDING), ("timestamp", DESCENDING)])

    def save_candle(self, asset, timeframe, candle):
        if not self.db: return
        try:
            doc = {**candle, "asset": asset, "timeframe": timeframe}
            self.db.candles.update_one(
                {"asset": asset, "timeframe": timeframe, "timestamp": candle["timestamp"]},
                {"$set": doc},
                upsert=True
            )
        except Exception as e:
            logger.debug(f"Mongo save_candle error: {e}")

    def load_candles(self, asset, timeframe, limit=500, since=None):
        if not self.db: return []
        try:
            query = {"asset": asset, "timeframe": timeframe}
            if since:
                query["timestamp"] = {"$gte": since}
            cursor = self.db.candles.find(query, {"_id": 0}).sort("timestamp", ASCENDING).limit(limit)
            return list(cursor)
        except Exception as e:
            logger.error(f"Mongo load_candles error: {e}")
            return []

    def get_candle_stats(self):
        """Return counts per timeframe and oldest timestamp across all assets."""
        if not self.db:
            return {"counts": {}, "oldest": 0}
        try:
            # Aggregate pipeline to get counts per timeframe
            pipeline = [
                {"$group": {"_id": "$timeframe", "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}}
            ]
            counts_result = list(self.db.candles.aggregate(pipeline))
            counts = {str(item["_id"]) + "s": item["count"] for item in counts_result}
            # Also get the oldest timestamp overall (for 1h timeframe, as a proxy)
            oldest_doc = self.db.candles.find_one(sort=[("timestamp", ASCENDING)])
            oldest_ts = oldest_doc["timestamp"] if oldest_doc else 0
            return {"counts": counts, "oldest": oldest_ts}
        except Exception as e:
            logger.error(f"Mongo get_candle_stats error: {e}")
            return {"counts": {}, "oldest": 0}

    def get_trades_count(self):
        if not self.db: return 0
        try:
            return self.db.trades.count_documents({})
        except Exception:
            return 0

    def get_latest_timestamp(self, asset, timeframe):
        if not self.db: return 0
        try:
            doc = self.db.candles.find_one(
                {"asset": asset, "timeframe": timeframe},
                sort=[("timestamp", DESCENDING)]
            )
            return doc["timestamp"] if doc else 0
        except Exception:
            return 0

    def save_trade_backup(self, trade_data):
        if not self.db: return
        try:
            self.db.trades.update_one({"id": trade_data["id"]}, {"$set": trade_data}, upsert=True)
        except Exception as e:
            logger.debug(f"Mongo save_trade error: {e}")

    def save_rejected_backup(self, rejected_data):
        if not self.db: return
        try:
            self.db.rejected.insert_one(rejected_data)
        except Exception as e:
            logger.debug(f"Mongo save_rejected error: {e}")

# =====================================================================
# SQLite DATABASE (with try...finally for Cursor)
# =====================================================================
class TradeDatabase:
    def __init__(self):
        self.conn = sqlite3.connect(Config.DB_PATH, check_same_thread=False)
        self._create_tables()

    def _create_tables(self):
        cur = self.conn.cursor()
        try:
            cur.execute('''CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset TEXT, direction TEXT,
                entry REAL, stop_loss REAL, take_profit REAL,
                score INTEGER, confidence TEXT, patterns TEXT, logic TEXT,
                timestamp INTEGER, status TEXT DEFAULT 'open',
                exit_price REAL, pnl REAL, close_time INTEGER,
                volatility REAL, market_regime TEXT, htf_trend TEXT, news_score REAL,
                entry_time INTEGER, exit_reason TEXT, health_history TEXT
            )''')
            cur.execute('''CREATE TABLE IF NOT EXISTS rejected_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset TEXT, price REAL, score INTEGER, reason TEXT,
                timestamp INTEGER, volatility REAL, market_regime TEXT
            )''')
            self.conn.commit()
        finally:
            cur.close()

    def log_trade(self, asset, direction, entry, sl, tp, score, confidence, patterns, logic,
                  volatility, regime, htf_trend, news_score):
        cur = self.conn.cursor()
        try:
            cur.execute('''INSERT INTO trades 
                (asset, direction, entry, stop_loss, take_profit, score, confidence, patterns, logic,
                 timestamp, volatility, market_regime, htf_trend, news_score, entry_time, status)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (asset, direction, entry, sl, tp, score, confidence, json.dumps(patterns), logic,
                 int(time.time()), volatility, regime, htf_trend, news_score, int(time.time()), 'open'))
            self.conn.commit()
            return cur.lastrowid
        finally:
            cur.close()

    def log_rejected(self, asset, price, score, reason, volatility, regime):
        cur = self.conn.cursor()
        try:
            cur.execute('''INSERT INTO rejected_signals (asset, price, score, reason, timestamp, volatility, market_regime)
                VALUES (?,?,?,?,?,?,?)''', (asset, price, score, reason, int(time.time()), volatility, regime))
            self.conn.commit()
        finally:
            cur.close()

    def close_trade(self, trade_id, exit_price, pnl, exit_reason=""):
        cur = self.conn.cursor()
        try:
            cur.execute('''UPDATE trades SET status='closed', exit_price=?, pnl=?, close_time=?, exit_reason=?
                WHERE id=?''', (exit_price, pnl, int(time.time()), exit_reason, trade_id))
            self.conn.commit()
        finally:
            cur.close()

    def get_rolling_win_rate(self, asset: str, lookback: int = 50) -> float:
        cur = self.conn.cursor()
        try:
            cur.execute('''SELECT pnl FROM trades WHERE asset=? AND status='closed' AND pnl IS NOT NULL ORDER BY close_time DESC LIMIT ?''', (asset, lookback))
            rows = cur.fetchall()
            if not rows:
                return 0.5
            wins = sum(1 for r in rows if r[0] > 0)
            return wins / len(rows)
        finally:
            cur.close()

    def get_db_size(self):
        try: return os.path.getsize(Config.DB_PATH)
        except: return 0

    def get_closed_trades(self, limit=50):
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT asset, direction, score, pnl, logic FROM trades WHERE status='closed' ORDER BY id DESC LIMIT ?", (limit,))
            return cur.fetchall()
        finally:
            cur.close()

    def get_performance_metrics(self):
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) FROM trades WHERE status='closed' AND pnl IS NOT NULL")
            total = cur.fetchone()[0] or 0
            cur.execute("SELECT COUNT(*) FROM trades WHERE status='closed' AND pnl > 0")
            wins = cur.fetchone()[0] or 0
            cur.execute("SELECT SUM(pnl) FROM trades WHERE status='closed' AND pnl > 0")
            gross_profit = cur.fetchone()[0] or 0.0
            cur.execute("SELECT SUM(pnl) FROM trades WHERE status='closed' AND pnl < 0")
            gross_loss = cur.fetchone()[0] or 0.0
            gross_loss = abs(gross_loss)
            total_pnl = gross_profit - gross_loss
            win_rate = wins / total if total else 0.0
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0.0)
            return {
                "total_trades": total,
                "winning_trades": wins,
                "losing_trades": total - wins,
                "win_rate": win_rate,
                "profit_factor": profit_factor,
                "total_pnl": total_pnl,
                "avg_pnl": total_pnl / total if total else 0.0
            }
        finally:
            cur.close()

# =====================================================================
# NEWS SCANNER
# =====================================================================
class CryptoNewsScanner:
    def __init__(self):
        self.last_news = {}
        self.fear_greed = 50

    def fetch_latest(self) -> Dict[str, Any]:
        try:
            url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&limit=3"
            resp = requests.get(url, timeout=5)
            articles = []
            if resp.status_code == 200:
                data = resp.json()
                if data.get("Data"):
                    for article in data["Data"][:2]:
                        title = article.get("title", "")
                        sentiment = self._analyze_sentiment(title)
                        articles.append({"title": title, "sentiment": sentiment})
                    if articles: self.last_news = articles[0]

            fg_url = "https://api.alternative.me/fng/?limit=1"
            fg_resp = requests.get(fg_url, timeout=5)
            if fg_resp.status_code == 200:
                fg_data = fg_resp.json()
                if fg_data.get("data"):
                    self.fear_greed = int(fg_data["data"][0]["value"])
            return {"articles": articles, "fresh": True, "fear_greed": self.fear_greed}
        except Exception as e:
            logger.error(f"News/FG fetch error: {e}")
        return {"articles": [], "fresh": False, "fear_greed": 50}

    def _analyze_sentiment(self, text: str) -> float:
        bullish = ["bullish", "breakout", "surge", "buy", "accumulate", "rally", "green"]
        bearish = ["bearish", "crash", "dump", "sell", "liquidation", "drop", "red"]
        text = text.lower()
        score = sum(1 for w in bullish if w in text) - sum(1 for w in bearish if w in text)
        return max(-100, min(100, score * 20))

# =====================================================================
# INSTITUTIONAL LIQUIDITY ENGINE
# =====================================================================
class InstitutionalLiquidityEngine:
    def __init__(self, lookback=800):
        self.lookback = lookback
        self.proximity_pct = 0.005

    def analyze(self, candles_1h, candles_5m, candle_1m, ltp, atr, bsl, ssl):
        if bsl == 0 or ssl == 0 or atr == 0:
            return {"trigger": "WAIT"}
        m1_high, m1_low = candle_1m["high"], candle_1m["low"]
        if m1_high >= bsl and ltp >= bsl * (1 - self.proximity_pct):
            return {"trigger": "SELL"}
        if m1_low <= ssl and ltp <= ssl * (1 + self.proximity_pct):
            return {"trigger": "BUY"}
        return {"trigger": "WAIT"}

# =====================================================================
# CANDLE TOPOLOGY ENGINE
# =====================================================================
class CandleTopologyEngine:
    def __init__(self):
        self.candles = {tf: {asset: [] for asset in Config.ASSETS} for tf in [60, 300, 900, 3600]}
        self.pivots = {asset: {"high": [], "low": []} for asset in Config.ASSETS}
        self.bos = {asset: {"direction": ""} for asset in Config.ASSETS}
        self.choch = {asset: False for asset in Config.ASSETS}
        self.support_resistance = {asset: {"support": [], "resistance": []} for asset in Config.ASSETS}
        self.last_tick_time = {asset: 0 for asset in Config.ASSETS}
        self.candle_just_closed = {asset: False for asset in Config.ASSETS}
        self.history = {asset: deque(maxlen=200) for asset in Config.ASSETS}

    def process_tick(self, asset: str, price: float, volume: float):
        now = int(time.time())
        self.history[asset].append({"price": price, "volume": volume, "time": now})
        self.candle_just_closed[asset] = False

        tf = 900
        start = (now // tf) * tf
        storage = self.candles[tf][asset]
        if storage and storage[-1].get("timestamp") != start:
            if not storage[-1].get("complete", False):
                storage[-1]["complete"] = True
                self.candle_just_closed[asset] = True

        for timeframe in [60, 300, 900, 3600]:
            self._build_candle(asset, price, volume, now, timeframe, self.candles[timeframe][asset])

        self._update_pivots(asset, price)
        self._update_support_resistance(asset, price)
        self._detect_bos_choch(asset)
        self.last_tick_time[asset] = now

    def _build_candle(self, asset, price, volume, ts, tf, storage):
        start = (ts // tf) * tf
        if not storage or storage[-1].get("timestamp") != start:
            if storage and not storage[-1].get("complete", False):
                storage[-1]["complete"] = True
            storage.append({"timestamp": start, "open": price, "high": price, "low": price,
                            "close": price, "volume": volume, "complete": False})
            if len(storage) > Config.MAX_CANDLES: storage.pop(0)
        else:
            c = storage[-1]
            c["high"] = max(c["high"], price); c["low"] = min(c["low"], price)
            c["close"] = price; c["volume"] += volume

    def _update_pivots(self, asset, price):
        candles = self.candles[900][asset]
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < 10: return
        for i in range(2, len(complete)-2):
            if (complete[i-2]["high"] < complete[i]["high"] > complete[i+2]["high"] and
                complete[i-1]["high"] < complete[i]["high"] > complete[i+1]["high"]):
                if complete[i]["high"] not in self.pivots[asset]["high"]:
                    self.pivots[asset]["high"].append(complete[i]["high"])
            if (complete[i-2]["low"] > complete[i]["low"] < complete[i+2]["low"] and
                complete[i-1]["low"] > complete[i]["low"] < complete[i+1]["low"]):
                if complete[i]["low"] not in self.pivots[asset]["low"]:
                    self.pivots[asset]["low"].append(complete[i]["low"])
        self.pivots[asset]["high"] = sorted(self.pivots[asset]["high"], reverse=True)[:5]
        self.pivots[asset]["low"] = sorted(self.pivots[asset]["low"])[:5]

    def _detect_bos_choch(self, asset):
        h = self.pivots[asset]["high"]
        l = self.pivots[asset]["low"]
        if len(h) >= 2 and len(l) >= 2:
            if h[0] > h[1]: self.bos[asset]["direction"] = "UP"
            elif l[0] < l[1]: self.bos[asset]["direction"] = "DOWN"
            if len(h) >= 3 and len(l) >= 3:
                self.choch[asset] = (h[1] < h[2] and l[1] > l[2])

    def _update_support_resistance(self, asset, price):
        all_levels = self.pivots[asset]["high"] + self.pivots[asset]["low"]
        clusters = []
        for level in sorted(all_levels):
            if not clusters or abs(level - clusters[-1]) / level > 0.005:
                clusters.append(level)
        self.support_resistance[asset]["support"] = [l for l in clusters if l < price * 0.99]
        self.support_resistance[asset]["resistance"] = [r for r in clusters if r > price * 1.01]

    def detect_candle_patterns(self, asset):
        candles = self.candles[300][asset]
        if len(candles) < 2: return {}
        last = candles[-1]
        if not last.get("complete", False): return {}
        patterns = {}
        body = abs(last["close"] - last["open"])
        total = last["high"] - last["low"]
        if total > 0:
            if (min(last["open"], last["close"]) - last["low"]) / total > 0.6: patterns["bullish_rej"] = 1
            if (last["high"] - max(last["open"], last["close"])) / total > 0.6: patterns["bearish_rej"] = 1
        return patterns

    def get_atr(self, asset, period=14):
        candles = self.candles[3600][asset]
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < period: return 0.0
        tr_list = [max(complete[i]["high"] - complete[i]["low"],
                       abs(complete[i]["high"] - complete[i-1]["close"]),
                       abs(complete[i]["low"] - complete[i-1]["close"])) for i in range(1, period+1)]
        return sum(tr_list) / period

    def detect_liquidity_sweep(self, asset, price):
        h = self.pivots[asset]["high"]
        l = self.pivots[asset]["low"]
        if h and price > max(h[-2:]): return "BUY_SWEEP"
        if l and price < min(l[-2:]): return "SELL_SWEEP"
        return ""

    def get_volatility_regime(self, asset):
        atr = self.get_atr(asset)
        if atr == 0: return "medium"
        return "low" if atr < 50 else "medium" if atr < 150 else "high" if atr < 300 else "extreme"

    def _ema(self, series, period):
        if len(series) < period: return []
        ema = [sum(series[:period]) / period]
        m = 2 / (period + 1)
        for i in range(period, len(series)):
            ema.append((series[i] - ema[-1]) * m + ema[-1])
        return ema

    def check_1m_rejection(self, asset, direction):
        candles = self.candles[60][asset]
        if len(candles) < 2: return False
        last = next((c for c in reversed(candles) if c.get("complete", False)), None)
        if not last: return False
        r = last["high"] - last["low"]
        if r <= 0: return False
        if direction == "BUY":
            return (min(last["open"], last["close"]) - last["low"]) / r >= 0.4
        else:
            return (last["high"] - max(last["open"], last["close"])) / r >= 0.4

    def get_visual_topology(self, asset, price, direction, sl, tp, patterns):
        min_price = min(price, sl, tp) * 0.98
        max_price = max(price, sl, tp) * 1.02
        if max_price - min_price < 0.01:
            min_price = price * 0.95
            max_price = price * 1.05

        sr = self.support_resistance[asset]
        supports = [s for s in sr["support"] if min_price <= s <= max_price]
        resistances = [r for r in sr["resistance"] if min_price <= r <= max_price]

        rows = 10
        chart_lines = ["┌──────────────────────────────────────┐", "│       📊 LIVE TOPOLOGY CHART (Zoom)     │", "├──────────────────────────────────────┤"]
        
        for i in range(rows, -1, -1):
            level = min_price + (max_price - min_price) * (i / rows)
            marker = " "
            if i == min(range(rows+1), key=lambda x: abs(min_price + (max_price - min_price) * (x / rows) - price)):
                marker = "●"
            elif i == min(range(rows+1), key=lambda x: abs(min_price + (max_price - min_price) * (x / rows) - sl)):
                marker = "▼"
            elif i == min(range(rows+1), key=lambda x: abs(min_price + (max_price - min_price) * (x / rows) - tp)):
                marker = "★"
            else:
                if any(abs(level - s) / s < 0.001 for s in supports):
                    marker = "S"
                elif any(abs(level - r) / r < 0.001 for r in resistances):
                    marker = "R"
            bar = "█" * int((i / rows) * 10) if i > 0 else ""
            chart_lines.append(f"│ {level:>8.2f} │ {marker} {bar:<10} │")
        
        chart_lines.extend(["├──────────────────────────────────────┤", 
                            "│ ●=Entry ▼=SL ★=TP  S=Support R=Res │", 
                            "└──────────────────────────────────────┘"])
        return "\n".join(chart_lines)

# =====================================================================
# SIGNAL SCORING ENGINE
# =====================================================================
class SignalScoringEngine:
    def __init__(self):
        self.weights = {"htf_trend":15, "market_structure":12, "liquidity_sweep":10, "hunt_confirmation":15,
                        "fvg":8, "order_block":8, "volume":8, "rsi":8, "adx":8, "news":8, "institutional_liquidity":10}
        self.min_pass_layers = Config.MIN_LAYER_PASS

    def evaluate(self, asset, price, patterns, sr_data, trend, news_sentiment, volume_ratio,
                 rsi, adx, volatility, htf_trend, bos, choch, fvgs, order_block, liquidity_sweep,
                 news_importance, hunt_confirmed=False, inst_liquidity_trigger=None):
        passed = []; score = 0
        if htf_trend == trend and htf_trend != "NEUTRAL": score += 15; passed.append("htf_trend")
        if choch: score += 12; passed.append("market_structure")
        if liquidity_sweep: score += 10; passed.append("liquidity_sweep")
        if hunt_confirmed: score += 15; passed.append("hunt_confirmation")
        if volume_ratio > 1.2: score += 8; passed.append("volume")
        if 30 <= rsi <= 70: score += 8; passed.append("rsi")
        if adx > 25: score += 8; passed.append("adx")
        if inst_liquidity_trigger in ["BUY","SELL"]: score += 10; passed.append("institutional_liquidity")
        if abs(news_sentiment) > 50 and news_importance > 0.5: score += 8; passed.append("news")
        return {"total_score": min(100, score), "confidence": "HIGH" if score>=70 else "MEDIUM" if score>=50 else "LOW",
                "probability": 50 + (score-50)*0.6, "passed_layers": passed, "num_passed": len(passed),
                "enough": len(passed) >= self.min_pass_layers}

# =====================================================================
# TELEGRAM PIPELINE
# =====================================================================
class TelegramPipeline:
    def __init__(self):
        self.token = Config.TELEGRAM_BOT_TOKEN
        self.chat_id = Config.TELEGRAM_CHAT_ID
        self.queue = queue.Queue()
        threading.Thread(target=self._worker, daemon=True).start()
        
    def _worker(self):
        while True:
            msg = self.queue.get()
            try:
                requests.post(f"https://api.telegram.org/bot{self.token}/sendMessage", 
                              data={"chat_id": self.chat_id, "text": msg, "parse_mode": "HTML"}, timeout=10)
            except: pass
            
    def send_message(self, text: str):
        self.queue.put(text)

    def fire_signal(self, asset, direction, price, sl, tp, chart, logic, news, score, patterns, trade_id, session, rr):
        icon = "🔥" if direction=="BUY" else "❄️"
        msg = (f"{icon} <b>AI SIGNAL: {direction}</b> {icon}\n"
               f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
               f"📊 {Config.DISPLAY_NAMES.get(asset, asset)} | 🆔 #{trade_id}\n"
               f"⏰ {session} | ⚡ {score['confidence']} ({score['total_score']:.0f}%)\n"
               f"🎯 R:R {rr:.2f}\n"
               f"💰 Entry: {price:.2f}  🛑 SL: {sl:.2f}  🎯 TP: {tp:.2f}\n"
               f"\n📊 CHART:\n{chart}\n"
               f"🧠 Logic: {logic}\n📰 News: {news}\n"
               f"📊 Layers Passed: {score['num_passed']}/11\n"
               f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        self.queue.put(msg)
        
    def fire_news_alert(self, title, sentiment, fg):
        self.queue.put(f"📰 {title}\n🧠 Sentiment: {sentiment:.0f} | Fear/Greed: {fg}")

# =====================================================================
# BINANCE WEBSOCKET
# =====================================================================
class BinancePublicStream:
    def __init__(self, on_price_update):
        self.on_price_update = on_price_update
        self.running = False
        self.reconnect_count = 0
    def start(self):
        self.running = True
        threading.Thread(target=self._ws_loop, daemon=True).start()
    def _ws_loop(self):
        import websocket
        while self.running:
            try:
                streams = [f"{a.lower()}@kline_1m" for a in Config.ASSETS]
                ws = websocket.WebSocketApp(f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}",
                                            on_message=self._on_msg, on_error=lambda x,y: None)
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except: time.sleep(5)
    def _on_msg(self, ws, msg):
        try:
            data = json.loads(msg)["data"]["k"]
            symbol = data["s"]
            if symbol in Config.ASSETS:
                self.on_price_update(symbol, float(data["c"]), float(data["v"]))
        except: pass

# =====================================================================
# RICH HEALTH SERVER (with MongoDB stats)
# =====================================================================
def start_health_server(orchestrator):
    port = int(os.environ.get("PORT", 10000))
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            
            # --- सिस्टम स्टैट्स ---
            cpu = psutil.cpu_percent() if HAS_PSUTIL else 0
            mem = psutil.virtual_memory().percent if HAS_PSUTIL else 0
            
            # --- एक्टिव ट्रेड्स की डिटेल ---
            active_trades_list = []
            with orchestrator.trade_lock:
                for tid, trade in orchestrator.active_trades.items():
                    current_price = orchestrator.topology.history[trade['asset']][-1]['price'] if orchestrator.topology.history[trade['asset']] else trade['entry']
                    pnl = round(current_price - trade['entry'] if trade['direction']=='BUY' else trade['entry'] - current_price, 2)
                    active_trades_list.append({
                        "id": tid,
                        "asset": trade['asset'],
                        "direction": trade['direction'],
                        "entry": round(trade['entry'], 2),
                        "stop_loss": round(trade['sl'], 2),
                        "take_profit": round(trade['tp'], 2),
                        "current_pnl": pnl,
                        "breakeven_locked": trade.get('breakeven_locked', False),
                        "trailing_activated": trade.get('trailing_activated', False),
                        "health": trade.get('health', 100),
                        "confidence": trade.get('current_score', 0)
                    })
            
            # --- परफॉर्मेंस मेट्रिक्स (SQLite से) ---
            perf = orchestrator.db.get_performance_metrics()
            
            # --- MongoDB Stats ---
            mongo_stats = {}
            if orchestrator.mongo.db:
                try:
                    candle_stats = orchestrator.mongo.get_candle_stats()
                    trades_backup = orchestrator.mongo.get_trades_count()
                    mongo_stats = {
                        "connected": True,
                        "candle_counts": candle_stats["counts"],
                        "oldest_timestamp": candle_stats["oldest"],
                        "oldest_date": datetime.fromtimestamp(candle_stats["oldest"]).strftime('%Y-%m-%d %H:%M:%S') if candle_stats["oldest"] else None,
                        "trades_backup_count": trades_backup
                    }
                    # calculate approximate days of data
                    if candle_stats["oldest"]:
                        days = (time.time() - candle_stats["oldest"]) / 86400
                        mongo_stats["data_days"] = round(days, 1)
                    else:
                        mongo_stats["data_days"] = 0
                except Exception as e:
                    mongo_stats = {"connected": True, "error": str(e)}
            else:
                mongo_stats = {"connected": False}
            
            # --- लास्ट सिग्नल टाइम ---
            last_signal = {asset: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(orchestrator.last_signal_time[asset])) if orchestrator.last_signal_time[asset] > 0 else "Never" for asset in Config.ASSETS}
            
            # --- कैंडल डिले ---
            candle_delay = 0
            for asset in Config.ASSETS:
                candles = orchestrator.topology.candles[60][asset]
                if candles and candles[-1].get("complete", False):
                    candle_delay = max(candle_delay, int(time.time()) - candles[-1]["timestamp"] - 60)
            
            response = {
                "status": "online",
                "version": "5.2.3-MongoStats",
                "uptime_seconds": int(time.time() - orchestrator.start_time) if hasattr(orchestrator, 'start_time') else 0,
                "cpu_percent": cpu,
                "memory_percent": mem,
                "active_trades_count": len(orchestrator.active_trades),
                "active_trades": active_trades_list,
                "accepted_signals": orchestrator.accepted,
                "rejected_signals": orchestrator.rejected,
                "last_signal_time": last_signal,
                "performance": perf,
                "candle_delay_seconds": candle_delay,
                "reconnect_count": orchestrator.stream.reconnect_count if hasattr(orchestrator, 'stream') else 0,
                "news_sentiment": orchestrator.news.last_news.get('sentiment', {}).get('label', 'NEUTRAL') if hasattr(orchestrator, 'news') else 'N/A',
                "fear_greed": orchestrator.news.fear_greed if hasattr(orchestrator, 'news') else 50,
                # NEW: MongoDB persistence details
                "mongodb": mongo_stats
            }
            
            self.wfile.write(json.dumps(response, indent=2).encode())
    
    httpd = HTTPServer(("0.0.0.0", port), H)
    logger.info(f"Health server started on port {port}")
    httpd.serve_forever()

# =====================================================================
# LIFECYCLE CONTROLLER
# =====================================================================
class ActiveTradeLifecycle:
    def __init__(self, orchestrator):
        self.orch = orchestrator
        self.check_interval = 60

    def monitor_lifecycle(self):
        while True:
            time.sleep(self.check_interval)
            with self.orch.trade_lock:
                if not self.orch.active_trades:
                    continue
                now = int(time.time())
                to_remove = []
                for tid, trade in list(self.orch.active_trades.items()):
                    asset = trade['asset']
                    current_price = self.orch.topology.history[asset][-1]['price'] if self.orch.topology.history[asset] else trade['entry']
                    atr = self.orch.topology.get_atr(asset)
                    htf_trend = self.orch.asset_state[asset]["htf_trend"]
                    trade_duration = now - trade.get('entry_time', now)

                    if trade_duration > Config.TIME_DECAY_SECONDS and abs(current_price - trade['entry']) / trade['entry'] < Config.TIME_DECAY_THRESHOLD_PCT:
                        self.orch._close_trade(tid, current_price, 0.0, "Time-Decay (Consolidation)")
                        to_remove.append(tid)
                        self.orch.telegram.send_message(f"⏳ <b>Time-Decay Exit:</b> Trade #{tid} closed.")
                        continue

                    base_score = trade.get('initial_score', 70)
                    health = 100
                    if (trade['direction'] == 'BUY' and htf_trend == 'BULLISH') or (trade['direction'] == 'SELL' and htf_trend == 'BEARISH'):
                        base_score = min(95, base_score + 5)
                    else:
                        base_score = max(40, base_score - 8)
                        health -= 15

                    candles_5m = self.orch.topology.candles[300][asset]
                    if len(candles_5m) > 1 and candles_5m[-1].get("complete"):
                        last_c = candles_5m[-1]
                        if trade['direction'] == 'BUY' and last_c['close'] < last_c['open'] and (last_c['open'] - last_c['close']) > atr * 0.7:
                            health -= 25
                        elif trade['direction'] == 'SELL' and last_c['close'] > last_c['open'] and (last_c['close'] - last_c['open']) > atr * 0.7:
                            health -= 25

                    if health < Config.HEALTH_EMERGENCY_THRESHOLD:
                        pnl = current_price - trade['entry'] if trade['direction'] == 'BUY' else trade['entry'] - current_price
                        self.orch._close_trade(tid, current_price, pnl, f"Emergency (Health {health}%)")
                        to_remove.append(tid)
                        self.orch.telegram.send_message(f"🚨 <b>Emergency Exit:</b> Trade #{tid} cut early.")
                        continue

                    if int(time.time()) % Config.CONFIDENCE_UPDATE_INTERVAL < 60:
                        msg = (f"🔄 <b>Lifecycle Update: #{tid} ({asset})</b>\n"
                               f"Direction: {trade['direction']} | PnL: {(current_price - trade['entry']):.2f}\n"
                               f"📊 Confidence: {trade.get('current_score', base_score)}% → {base_score}%\n"
                               f"❤️ Health: {health}%")
                        self.orch.telegram.send_message(msg)

                    trade['current_score'] = base_score
                    trade['health'] = health

                for tid in to_remove:
                    if tid in self.orch.active_trades:
                        del self.orch.active_trades[tid]
                gc.collect()

# =====================================================================
# TRADE JOURNAL AI
# =====================================================================
class TradeJournalAI:
    def __init__(self, db_connection):
        self.conn = db_connection

    def log_and_learn(self):
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT asset, direction, score, pnl, logic FROM trades WHERE status='closed' ORDER BY id DESC LIMIT 50")
            rows = cur.fetchall()
            if len(rows) < 10:
                return "🤖 AI Learning: Need at least 10 closed trades to analyze."

            wins = 0
            logic_performance = {}
            for row in rows:
                asset, direction, score, pnl, logic = row
                is_win = 1 if pnl > 0 else 0
                if is_win: wins += 1
                if logic not in logic_performance:
                    logic_performance[logic] = {"confluences": 0, "wins": 0}
                logic_performance[logic]["confluences"] += 1
                if is_win:
                    logic_performance[logic]["wins"] += 1

            total = len(rows)
            win_rate = (wins / total) * 100
            best_logic = max(logic_performance, key=lambda k: logic_performance[k]["wins"] / max(1, logic_performance[k]["confluences"]))
            worst_logic = min(logic_performance, key=lambda k: logic_performance[k]["wins"] / max(1, logic_performance[k]["confluences"]))

            return (f"🤖 <b>AI Journal Insights (Last {total} Trades):</b>\n"
                    f"• Win Rate: {win_rate:.1f}%\n"
                    f"• Best Logic: {best_logic} (Success: {logic_performance[best_logic]['wins']}/{logic_performance[best_logic]['confluences']})\n"
                    f"• Worst Logic: {worst_logic} (Success: {logic_performance[worst_logic]['wins']}/{logic_performance[worst_logic]['confluences']})")
        finally:
            cur.close()

# =====================================================================
# CORE ORCHESTRATOR
# =====================================================================
class AIOrchestrator:
    def __init__(self):
        self.topology = CandleTopologyEngine()
        self.news = CryptoNewsScanner()
        self.scoring = SignalScoringEngine()
        self.liquidity = InstitutionalLiquidityEngine()
        self.telegram = TelegramPipeline()
        self.db = TradeDatabase()
        self.mongo = MongoDatabase()
        
        self.active_trades = {}
        self.trade_lock = threading.Lock()
        self.price_queue = queue.Queue(maxsize=1000)
        self.start_time = time.time()
        self.last_signal_time = {a:0 for a in Config.ASSETS}
        self.signal_timestamps = deque(maxlen=100)
        self.asset_state = {a: {"trend":"NEUTRAL","htf_trend":"NEUTRAL","volume_ratio":1.0,
                                "rsi":50,"adx":20,"volatility":0.01,"news_sentiment":0,"news_importance":0.5} for a in Config.ASSETS}
        self.accepted = 0; self.rejected = 0

        self.lifecycle = ActiveTradeLifecycle(self)
        self.journal_ai = TradeJournalAI(self.db.conn)
        threading.Thread(target=self.lifecycle.monitor_lifecycle, daemon=True).start()
        threading.Thread(target=self._process_queue, daemon=True).start()

    def _process_queue(self):
        while True:
            try:
                item = self.price_queue.get(timeout=1)
                if item: self._handle_price_tick(*item)
            except: pass

    # --- Data Loading with Parallel Execution ---
    def _load_and_backfill(self, asset, tf):
        """Load from MongoDB, if insufficient fetch from Binance (parallel safe)"""
        logger.info(f"Loading {asset} TF={tf}...")
        candles = self.mongo.load_candles(asset, tf, limit=Config.MAX_CANDLES)
        if len(candles) >= Config.MAX_CANDLES * 0.9:
            self.topology.candles[tf][asset] = candles
            logger.info(f"Loaded {len(candles)} candles from MongoDB for {asset} TF={tf}")
            return

        logger.info(f"Fetching from Binance for {asset} TF={tf}")
        since_ts = int(time.time()) - (90 * 24 * 3600)
        try:
            interval = {60:"1m", 300:"5m", 900:"15m", 3600:"1h"}[tf]
            resp = requests.get("https://api.binance.com/api/v3/klines",
                                params={"symbol": asset, "interval": interval, "limit": 1000, "startTime": since_ts * 1000}, timeout=15)
            if resp.status_code == 200:
                fetched = []
                for d in resp.json():
                    c = {"timestamp": d[0]//1000, "open": float(d[1]), "high": float(d[2]), 
                         "low": float(d[3]), "close": float(d[4]), "volume": float(d[5]), "complete": True}
                    fetched.append(c)
                    self.mongo.save_candle(asset, tf, c)
                fetched = fetched[-Config.MAX_CANDLES:]
                self.topology.candles[tf][asset] = fetched
                logger.info(f"Saved {len(fetched)} candles for {asset} TF={tf}")
        except Exception as e:
            logger.error(f"Backfill error for {asset} TF={tf}: {e}")

    # --- Strong Trend Check (FIXED: safe EMA checks) ---
    def _is_strong_trend(self, asset):
        c15 = [c["close"] for c in self.topology.candles[900][asset] if c.get("complete", False)][-30:]
        c1h = [c["close"] for c in self.topology.candles[3600][asset] if c.get("complete", False)][-30:]
        if len(c15) < 20 or len(c1h) < 20:
            return False

        e15_9, e15_21 = self.topology._ema(c15, 9), self.topology._ema(c15, 21)
        e1h_9, e1h_21 = self.topology._ema(c1h, 9), self.topology._ema(c1h, 21)

        if not e15_9 or not e15_21 or not e1h_9 or not e1h_21:
            return False
        if len(e15_9) < 2 or len(e15_21) < 2 or len(e1h_9) < 2 or len(e1h_21) < 2:
            return False

        return ((e15_9[-1]-e15_21[-1]) > (e15_9[-2]-e15_21[-2])) and \
               ((e1h_9[-1]-e1h_21[-1]) > (e1h_9[-2]-e1h_21[-2]))

    def _update_indicators(self, asset, price):
        c15 = [c["close"] for c in self.topology.candles[900][asset] if c.get("complete", False)][-30:]
        if len(c15) > 10:
            e9, e21 = self.topology._ema(c15,9), self.topology._ema(c15,21)
            if len(e9)>1 and len(e21)>1: 
                self.asset_state[asset]["trend"] = "BULLISH" if e9[-1]>e21[-1] else "BEARISH"
            if len(c15)>=14:
                self.asset_state[asset]["rsi"] = self._calc_rsi(c15)
                self.asset_state[asset]["adx"] = 30 if self.asset_state[asset]["trend"] != "NEUTRAL" else 20
        c1h = [c["close"] for c in self.topology.candles[3600][asset] if c.get("complete", False)][-30:]
        if len(c1h)>10:
            e9,e21 = self.topology._ema(c1h,9), self.topology._ema(c1h,21)
            if len(e9)>1 and len(e21)>1:
                self.asset_state[asset]["htf_trend"] = "BULLISH" if e9[-1]>e21[-1] else "BEARISH"
        vols = [c["volume"] for c in self.topology.candles[300][asset] if c.get("complete", False)][-10:]
        if len(vols)>1: self.asset_state[asset]["volume_ratio"] = vols[-1] / (sum(vols[:-1])/len(vols[:-1])+0.001)
        atr = self.topology.get_atr(asset)
        if atr: self.asset_state[asset]["volatility"] = atr / price

    def _calc_rsi(self, closes):
        if len(closes)<15: return 50
        gains=losses=0
        for i in range(len(closes)-14, len(closes)):
            diff = closes[i]-closes[i-1]
            if diff>0: gains+=diff
            else: losses-=diff
        return 100 - (100/(1+(gains/14)/(losses/14+0.0001)))

    def _close_trade(self, tid, price, pnl, reason=""):
        self.db.close_trade(tid, price, pnl, reason)
        self.telegram.send_message(f"🔒 Trade #{tid} closed at {price:.2f} | PnL: {pnl:+.2f} | Reason: {reason}")
        logger.info(f"Trade {tid} closed. PnL: {pnl:.2f}, Reason: {reason}")
        if self.mongo.db:
            try:
                self.mongo.db.trades.update_one(
                    {"id": tid},
                    {"$set": {"status": "closed", "exit_price": price, "pnl": pnl, "close_time": int(time.time()), "exit_reason": reason}}
                )
            except: pass

    def _update_active_trades(self, asset, price):
        with self.trade_lock:
            to_remove = []
            for tid, trade in list(self.active_trades.items()):
                if trade['asset'] != asset: continue
                if not trade.get('breakeven_locked', False):
                    target_dist = abs(trade['tp'] - trade['entry'])
                    half = trade['entry'] + 0.5*target_dist if trade['direction']=='BUY' else trade['entry'] - 0.5*target_dist
                    if (trade['direction']=='BUY' and price >= half) or (trade['direction']=='SELL' and price <= half):
                        if self.topology.check_1m_rejection(asset, trade['direction']):
                            trade['sl'] = trade['entry']
                            trade['breakeven_locked'] = True
                            logger.info(f"BE Locked for {tid}")
                if not trade.get('trailing_activated', False):
                    target_dist = abs(trade['tp'] - trade['entry'])
                    trigger = trade['entry'] + 0.7*target_dist if trade['direction']=='BUY' else trade['entry'] - 0.7*target_dist
                    if (trade['direction']=='BUY' and price >= trigger) or (trade['direction']=='SELL' and price <= trigger):
                        new_sl = trade['entry'] + 0.3*target_dist if trade['direction']=='BUY' else trade['entry'] - 0.3*target_dist
                        if (trade['direction']=='BUY' and new_sl > trade['sl']) or (trade['direction']=='SELL' and new_sl < trade['sl']):
                            trade['sl'] = new_sl
                            trade['trailing_activated'] = True
                            logger.info(f"Trailing activated for {tid}, new SL: {new_sl:.2f}")

                if trade['direction'] == 'BUY':
                    if price <= trade['sl']: self._close_trade(tid, price, price - trade['entry'], "SL Hit"); to_remove.append(tid)
                    elif price >= trade['tp']: self._close_trade(tid, price, price - trade['entry'], "TP Hit"); to_remove.append(tid)
                else:
                    if price >= trade['sl']: self._close_trade(tid, price, trade['entry'] - price, "SL Hit"); to_remove.append(tid)
                    elif price <= trade['tp']: self._close_trade(tid, price, trade['entry'] - price, "TP Hit"); to_remove.append(tid)
            for tid in to_remove:
                if tid in self.active_trades: del self.active_trades[tid]
            if to_remove: gc.collect()

    # ---- MAIN PRICE TICK HANDLER ----
    def _handle_price_tick(self, asset, price, volume):
        self.topology.process_tick(asset, price, volume)
        self._update_active_trades(asset, price)

        if self.topology.candle_just_closed[asset]:
            candles_15m = self.topology.candles[900][asset]
            if candles_15m and candles_15m[-1].get("complete", False):
                self.mongo.save_candle(asset, 900, candles_15m[-1])
            for tf in [60, 300, 3600]:
                c_list = self.topology.candles[tf][asset]
                if c_list and c_list[-1].get("complete", False):
                    self.mongo.save_candle(asset, tf, c_list[-1])

        with self.trade_lock:
            is_active = any(t['asset'] == asset for t in self.active_trades.values())
        if is_active:
            if self._is_strong_trend(asset):
                with self.trade_lock:
                    for tid, trade in self.active_trades.items():
                        if trade['asset'] == asset and not trade.get('hold_sent', False):
                            hold_msg = (f"🧠 DEEPENING MARKET ALERT\n━━━━━━━━━━━━━━━━━━━━\n"
                                        f"📊 {asset}\n🚀 Strong Momentum! HOLD position.\n"
                                        f"SL: {trade['sl']:.2f} | TP: {trade['tp']:.2f}")
                            self.telegram.send_message(hold_msg)
                            trade['hold_sent'] = True
            return

        if not self.topology.candle_just_closed[asset]: return
        self._update_indicators(asset, price)

        c1h = [c for c in self.topology.candles[3600][asset] if c.get("complete", False)]
        bsl = max(c['high'] for c in c1h[-20:]) if len(c1h)>=20 else price*1.02
        ssl = min(c['low'] for c in c1h[-20:]) if len(c1h)>=20 else price*0.98
        atr = self.topology.get_atr(asset)
        candle_1m = self.topology.candles[60][asset][-1] if self.topology.candles[60][asset] else None
        inst = self.liquidity.analyze(c1h, self.topology.candles[300][asset], candle_1m, price, atr, bsl, ssl) if candle_1m else {"trigger":"WAIT"}
        sweep = self.topology.detect_liquidity_sweep(asset, price)
        hunt = (sweep=="SELL_SWEEP" and self.topology.check_1m_rejection(asset,"BUY")) or (sweep=="BUY_SWEEP" and self.topology.check_1m_rejection(asset,"SELL"))

        patterns = self.topology.detect_candle_patterns(asset)
        score = self.scoring.evaluate(asset, price, patterns, {}, self.asset_state[asset]["trend"],
                                      self.asset_state[asset]["news_sentiment"], self.asset_state[asset]["volume_ratio"],
                                      self.asset_state[asset]["rsi"], self.asset_state[asset]["adx"],
                                      self.asset_state[asset]["volatility"], self.asset_state[asset]["htf_trend"],
                                      self.topology.bos[asset], self.topology.choch[asset], [], {},
                                      sweep, self.asset_state[asset]["news_importance"], hunt, inst["trigger"])
        if not score["enough"] or score["total_score"] < Config.MIN_CONFLUENCE_SCORE:
            self.db.log_rejected(asset, price, score["total_score"], "Low score", self.asset_state[asset]["volatility"], "medium")
            self.rejected+=1
            if self.mongo.db:
                try:
                    self.mongo.db.rejected.insert_one({"asset": asset, "price": price, "score": score["total_score"], "reason": "Low score", "timestamp": int(time.time())})
                except: pass
            return

        if self.asset_state[asset]["htf_trend"] == "BULLISH" and self.asset_state[asset]["trend"] == "BULLISH":
            direction = "BUY"
        elif self.asset_state[asset]["htf_trend"] == "BEARISH" and self.asset_state[asset]["trend"] == "BEARISH":
            direction = "SELL"
        else: return

        regime = self.topology.get_volatility_regime(asset)
        sl_m, tp_m = Config.VOLATILITY_MULTIPLIERS.get(regime, (1.5, 2.5))
        sl = price - sl_m * atr if direction=="BUY" else price + sl_m * atr
        tp = price + tp_m * atr if direction=="BUY" else price - tp_m * atr

        sl_distance = abs(price - sl)
        if sl_distance < atr * Config.MIN_SL_DISTANCE_MULTIPLIER:
            reason = f"SL too tight ({sl_distance:.3f} < {atr * Config.MIN_SL_DISTANCE_MULTIPLIER:.3f})"
            self.db.log_rejected(asset, price, score["total_score"], reason, self.asset_state[asset]["volatility"], regime)
            self.rejected+=1
            return

        rr = abs(tp - price) / sl_distance
        if rr < Config.MIN_RISK_REWARD:
            tp = price + sl_distance * Config.MIN_RISK_REWARD if direction=="BUY" else price - sl_distance * Config.MIN_RISK_REWARD
            rr = abs(tp - price) / sl_distance
            if rr < Config.MIN_RISK_REWARD - 0.01:
                self.db.log_rejected(asset, price, score["total_score"], "RR low", self.asset_state[asset]["volatility"], regime)
                self.rejected+=1
                return

        if time.time() - self.last_signal_time[asset] < Config.SIGNAL_COOLDOWN and not self._is_strong_trend(asset):
            self.db.log_rejected(asset, price, score["total_score"], "Cooldown", self.asset_state[asset]["volatility"], regime)
            self.rejected+=1
            return

        logic_parts = [f"HTF {self.asset_state[asset]['htf_trend']}"]
        bos_dir = self.topology.bos[asset]["direction"]
        if (direction=="BUY" and bos_dir=="UP") or (direction=="SELL" and bos_dir=="DOWN"):
            logic_parts.append(f"BOS {bos_dir}")
        if self.topology.choch[asset]: logic_parts.append("CHOCH")
        if hunt: logic_parts.append("HUNT")
        if inst["trigger"] != "WAIT": logic_parts.append("INST_LIQ")
        logic = "+".join(logic_parts)

        tid = self.db.log_trade(asset, direction, price, sl, tp, score["total_score"], score["confidence"],
                                list(patterns.keys()), logic, self.asset_state[asset]["volatility"],
                                regime, self.asset_state[asset]["htf_trend"], self.asset_state[asset]["news_sentiment"])
        
        if self.mongo.db:
            try:
                trade_doc = {
                    "id": tid, "asset": asset, "direction": direction, "entry": price, "stop_loss": sl, "take_profit": tp,
                    "score": score["total_score"], "confidence": score["confidence"], "logic": logic,
                    "timestamp": int(time.time()), "status": "open", "entry_time": int(time.time()),
                    "volatility": self.asset_state[asset]["volatility"], "regime": regime, 
                    "htf_trend": self.asset_state[asset]["htf_trend"]
                }
                self.mongo.db.trades.insert_one(trade_doc)
            except Exception as e:
                logger.debug(f"Mongo trade backup error: {e}")

        with self.trade_lock:
            self.active_trades[tid] = {"id":tid, "asset":asset, "direction":direction, "entry":price,
                                       "sl":sl, "tp":tp, "entry_time":int(time.time()),
                                       "breakeven_locked":False, "trailing_activated":False,
                                       "hold_sent":False, "initial_score":score["total_score"],
                                       "current_score":score["total_score"], "health":100}
        self.accepted += 1
        self.last_signal_time[asset] = time.time()
        self.signal_timestamps.append(time.time())

        chart = self.topology.get_visual_topology(asset, price, direction, sl, tp, patterns)
        self.telegram.fire_signal(asset, direction, price, sl, tp, chart, logic,
                                  "News", score, patterns, tid, datetime.now().strftime("%H:%M"), rr)
        logger.info(f"🔥 SIGNAL: {asset} {direction} @ {price} (Score: {score['total_score']:.0f}, RR: {rr:.2f})")

        if self.accepted % 10 == 0:
            insight = self.journal_ai.log_and_learn()
            self.telegram.send_message(insight)

    # ---- RUN ----
    def run(self):
        # CRITICAL FIX: Start health server IMMEDIATELY (before heavy loading)
        threading.Thread(target=start_health_server, args=(self,), daemon=True).start()

        # Parallel data loading to speed up startup
        logger.info("Loading historical data from MongoDB/Binance in parallel...")
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for asset in Config.ASSETS:
                for tf in [60, 300, 900, 3600]:
                    futures.append(executor.submit(self._load_and_backfill, asset, tf))
            for future in as_completed(futures):
                pass
        logger.info("Data loading complete.")

        # Start WebSocket
        self.stream = BinancePublicStream(self._on_price)
        self.stream.start()
        self.telegram.send_message("🚀 AI v5.2.3 Final - MongoDB Stats on Health Server")
        
        last_news = 0
        while True:
            try:
                time.sleep(10)
                if time.time() - last_news > 60:
                    news = self.news.fetch_latest()
                    if news.get("fresh"):
                        for a in Config.ASSETS:
                            self.asset_state[a]["news_sentiment"] = news["articles"][0]["sentiment"] if news["articles"] else 0
                            self.asset_state[a]["news_importance"] = 0.8
                        if news["articles"]:
                            self.telegram.fire_news_alert(news["articles"][0]["title"], 
                                                          news["articles"][0]["sentiment"], news.get("fear_greed", 50))
                        last_news = time.time()
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Main loop: {e}")

    def _on_price(self, asset, price, volume):
        try: self.price_queue.put_nowait((asset, price, volume))
        except queue.Full: pass

# =====================================================================
if __name__ == "__main__":
    bot = AIOrchestrator()
    bot.run()

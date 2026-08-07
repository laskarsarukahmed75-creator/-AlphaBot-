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
logger = logging.getLogger("AI-Orchestrator-v5.2.3-Final")

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

    # WebSocket
    WS_RECONNECT_MIN_DELAY = 1
    WS_RECONNECT_MAX_DELAY = 60
    WS_HEARTBEAT_INTERVAL = 30

    # Cleanup
    CLEANUP_INTERVAL = 600  # 10 minutes

# =====================================================================
# MONGODB DATABASE MANAGER (with retry)
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
# SQLite DATABASE (with thread-safe explicit cursor handling)
# =====================================================================
class TradeDatabase:
    def __init__(self):
        self.conn = sqlite3.connect(Config.DB_PATH, check_same_thread=False)
        self._lock = threading.Lock()
        self._create_tables()

    def _create_tables(self):
        with self._lock:
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
        with self._lock:
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
        with self._lock:
            cur = self.conn.cursor()
            try:
                cur.execute('''INSERT INTO rejected_signals (asset, price, score, reason, timestamp, volatility, market_regime)
                    VALUES (?,?,?,?,?,?,?)''', (asset, price, score, reason, int(time.time()), volatility, regime))
                self.conn.commit()
            finally:
                cur.close()

    def close_trade(self, trade_id, exit_price, pnl, exit_reason=""):
        with self._lock:
            cur = self.conn.cursor()
            try:
                cur.execute('''UPDATE trades SET status='closed', exit_price=?, pnl=?, close_time=?, exit_reason=?
                    WHERE id=?''', (exit_price, pnl, int(time.time()), exit_reason, trade_id))
                self.conn.commit()
            finally:
                cur.close()

    def get_rolling_win_rate(self, asset: str, lookback: int = 50) -> float:
        with self._lock:
            cur = self.conn.cursor()
            try:
                cur.execute('''SELECT pnl FROM trades WHERE asset=? AND status='closed' AND pnl IS NOT NULL ORDER BY close_time DESC LIMIT ?''', (asset, lookback))
                rows = cur.fetchall()
            finally:
                cur.close()
        if not rows: return 0.5
        wins = sum(1 for r in rows if r[0] > 0)
        return wins / len(rows)

    def get_db_size(self):
        try: return os.path.getsize(Config.DB_PATH)
        except: return 0

    def get_closed_trades(self, limit=50):
        with self._lock:
            cur = self.conn.cursor()
            try:
                cur.execute("SELECT asset, direction, score, pnl, logic FROM trades WHERE status='closed' ORDER BY id DESC LIMIT ?", (limit,))
                return cur.fetchall()
            finally:
                cur.close()

# =====================================================================
# NEWS SCANNER (with summary, source quality, duplicate filtering)
# =====================================================================
class CryptoNewsScanner:
    def __init__(self):
        self.last_news = {}
        self.fear_greed = 50
        self.seen_titles = set()  # for duplicate filtering

    def fetch_latest(self) -> Dict[str, Any]:
        try:
            url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&limit=5"
            resp = requests.get(url, timeout=5)
            articles = []
            if resp.status_code == 200:
                data = resp.json()
                if data.get("Data"):
                    for article in data["Data"][:5]:
                        title = article.get("title", "")
                        # Duplicate filtering
                        if title in self.seen_titles:
                            continue
                        self.seen_titles.add(title)
                        # Source quality (simple: treat known sources higher)
                        source = article.get("source", "")
                        quality = 1.0
                        if source in ["Cointelegraph", "CoinDesk", "Bloomberg", "Reuters"]:
                            quality = 1.5
                        summary = article.get("body", "")[:200]  # summary snippet
                        sentiment = self._analyze_sentiment(title + " " + summary)
                        articles.append({
                            "title": title,
                            "sentiment": sentiment,
                            "source": source,
                            "quality": quality,
                            "summary": summary
                        })

            fg_url = "https://api.alternative.me/fng/?limit=1"
            fg_resp = requests.get(fg_url, timeout=5)
            if fg_resp.status_code == 200:
                fg_data = fg_resp.json()
                if fg_data.get("data"):
                    self.fear_greed = int(fg_data["data"][0]["value"])

            if articles:
                # Weighted average sentiment by quality
                total_quality = sum(a["quality"] for a in articles)
                if total_quality > 0:
                    avg_sent = sum(a["sentiment"] * a["quality"] for a in articles) / total_quality
                else:
                    avg_sent = sum(a["sentiment"] for a in articles) / len(articles)
                self.last_news = {"sentiment": avg_sent, "articles": articles}
                fg_norm = (self.fear_greed - 50) * 2
                combined = avg_sent * 0.6 + fg_norm * 0.4
                self.last_news["combined_sentiment"] = combined
            else:
                self.last_news["combined_sentiment"] = (self.fear_greed - 50) * 2

            return {"articles": articles, "fresh": True, "fear_greed": self.fear_greed,
                    "sentiment": self.last_news.get("combined_sentiment", 0)}
        except Exception as e:
            logger.error(f"News/FG fetch error: {e}")
        return {"articles": [], "fresh": False, "fear_greed": 50, "sentiment": 0}

    def _analyze_sentiment(self, text: str) -> float:
        # Expanded weighted word lists (same as before)
        bullish_words = {
            "bullish": 2, "breakout": 3, "surge": 3, "buy": 1, "accumulate": 2,
            "rally": 2, "green": 1, "positive": 2, "gain": 2, "up": 1,
            "record": 2, "high": 1, "momentum": 2, "growth": 2, "optimistic": 2
        }
        bearish_words = {
            "bearish": 2, "crash": 3, "dump": 3, "sell": 1, "liquidation": 2,
            "drop": 2, "red": 1, "negative": 2, "loss": 2, "down": 1,
            "low": 1, "decline": 2, "fear": 2, "panic": 3, "pessimistic": 2
        }
        text_lower = text.lower()
        score = 0
        for word, weight in bullish_words.items():
            if word in text_lower:
                score += weight
        for word, weight in bearish_words.items():
            if word in text_lower:
                score -= weight
        return max(-100, min(100, score * 10))

# =====================================================================
# ORDER BLOCK ENGINE (with BOS confirmation, mitigation, retest, invalidation)
# =====================================================================
class OrderBlockEngine:
    def __init__(self):
        self.blocks = {}  # asset -> list of dicts with full details
        self.lookback = 100

    def detect_order_blocks(self, asset, candles_15m, bos_direction):
        """
        Detect and update order blocks.
        Returns list of active OBs.
        """
        if len(candles_15m) < 20:
            return
        # Find strong candles
        new_blocks = []
        for i in range(5, len(candles_15m)-2):
            c = candles_15m[i]
            body = abs(c["close"] - c["open"])
            avg_body = sum(abs(candles_15m[j]["close"] - candles_15m[j]["open"]) for j in range(i-5, i)) / 5
            if body > avg_body * 1.8:  # strong candle
                if c["close"] > c["open"]:  # bullish
                    if bos_direction in ["UP", "BULLISH"]:
                        ob = {
                            "type": "BULLISH",
                            "level": c["high"],
                            "strength": body/avg_body,
                            "created_at": c["timestamp"],
                            "mitigated": False,
                            "retested": False,
                            "invalidated": False,
                            "mitigation_count": 0
                        }
                        new_blocks.append(ob)
                else:  # bearish
                    if bos_direction in ["DOWN", "BEARISH"]:
                        ob = {
                            "type": "BEARISH",
                            "level": c["low"],
                            "strength": body/avg_body,
                            "created_at": c["timestamp"],
                            "mitigated": False,
                            "retested": False,
                            "invalidated": False,
                            "mitigation_count": 0
                        }
                        new_blocks.append(ob)

        # Merge with existing blocks, update statuses
        existing = self.blocks.get(asset, [])
        # Keep only recent and not invalidated
        active = [b for b in existing if not b.get("invalidated", False) and 
                  (time.time() - b["created_at"] < 3600*24)]  # 24h expiry

        # Add new blocks
        active.extend(new_blocks)

        # Update mitigation and retest based on current price (will be called periodically)
        self.blocks[asset] = active[-10:]  # keep last 10
        return active

    def update_ob_status(self, asset, price):
        """Check mitigation, retest, invalidation based on price action."""
        if asset not in self.blocks:
            return
        for ob in self.blocks[asset]:
            if ob.get("invalidated"): continue
            # Mitigation: price crosses the OB level
            if ob["type"] == "BULLISH":
                if price <= ob["level"] and not ob.get("mitigated", False):
                    ob["mitigated"] = True
                    ob["mitigation_count"] = ob.get("mitigation_count", 0) + 1
                # Invalidation: price goes above OB level by a significant margin
                if price > ob["level"] * 1.01:
                    ob["invalidated"] = True
            else:  # BEARISH
                if price >= ob["level"] and not ob.get("mitigated", False):
                    ob["mitigated"] = True
                    ob["mitigation_count"] = ob.get("mitigation_count", 0) + 1
                if price < ob["level"] * 0.99:
                    ob["invalidated"] = True

    def get_ob_zone(self, asset, price, direction):
        """Return the nearest valid OB level that aligns with direction."""
        if asset not in self.blocks:
            return None
        best_ob = None
        best_distance = float('inf')
        for ob in self.blocks[asset]:
            if ob.get("invalidated") or ob.get("mitigated", False):
                continue
            if direction == "BUY" and ob["type"] == "BULLISH" and ob["level"] < price:
                dist = price - ob["level"]
                if dist < best_distance:
                    best_distance = dist
                    best_ob = ob["level"]
            elif direction == "SELL" and ob["type"] == "BEARISH" and ob["level"] > price:
                dist = ob["level"] - price
                if dist < best_distance:
                    best_distance = dist
                    best_ob = ob["level"]
        return best_ob

# =====================================================================
# INSTITUTIONAL LIQUIDITY ENGINE (improved with sweep volume confirmation)
# =====================================================================
class InstitutionalLiquidityEngine:
    def __init__(self, lookback=800):
        self.lookback = lookback
        self.proximity_pct = 0.005

    def analyze(self, candles_1h, candles_5m, candle_1m, ltp, atr, bsl, ssl, volume_ratio):
        """
        Enhanced with volume confirmation for liquidity sweeps.
        """
        if bsl == 0 or ssl == 0 or atr == 0:
            return {"trigger": "WAIT"}
        m1_high, m1_low = candle_1m["high"], candle_1m["low"]
        # Check for sweep with volume spike
        if m1_high >= bsl and ltp >= bsl * (1 - self.proximity_pct):
            if volume_ratio > 1.2:  # volume confirmation
                return {"trigger": "SELL", "strength": "HIGH"}
            else:
                return {"trigger": "SELL", "strength": "LOW"}
        if m1_low <= ssl and ltp <= ssl * (1 + self.proximity_pct):
            if volume_ratio > 1.2:
                return {"trigger": "BUY", "strength": "HIGH"}
            else:
                return {"trigger": "BUY", "strength": "LOW"}
        return {"trigger": "WAIT"}

# =====================================================================
# CANDLE TOPOLOGY ENGINE (with FVG fill tracking, memory cleanup)
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
        self.fvg_cache = {asset: [] for asset in Config.ASSETS}   # list of FVG dicts with fill status
        self.last_cleanup = time.time()

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

        # Periodic cleanup
        if time.time() - self.last_cleanup > Config.CLEANUP_INTERVAL:
            self._cleanup_old_data()
            self.last_cleanup = time.time()

    def _cleanup_old_data(self):
        """Remove stale candles and history to manage memory."""
        for tf in self.candles:
            for asset in Config.ASSETS:
                if len(self.candles[tf][asset]) > Config.MAX_CANDLES:
                    self.candles[tf][asset] = self.candles[tf][asset][-Config.MAX_CANDLES:]
        for asset in Config.ASSETS:
            # Keep only last 500 history entries
            if len(self.history[asset]) > 500:
                self.history[asset] = deque(list(self.history[asset])[-500:], maxlen=200)

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
        highs = self.pivots[asset]["high"]
        lows = self.pivots[asset]["low"]
        if len(highs) >= 2 and len(lows) >= 2:
            if highs[0] > highs[1]:
                self.bos[asset]["direction"] = "UP"
            elif lows[0] < lows[1]:
                self.bos[asset]["direction"] = "DOWN"
            if len(highs) >= 3 and len(lows) >= 3:
                if highs[1] > highs[2] and lows[1] > lows[2]:
                    self.choch[asset] = True
                elif highs[1] < highs[2] and lows[1] < lows[2]:
                    self.choch[asset] = True
                else:
                    self.choch[asset] = False

    def _update_support_resistance(self, asset, price):
        all_levels = self.pivots[asset]["high"] + self.pivots[asset]["low"]
        clusters = []
        for level in sorted(all_levels):
            if not clusters or abs(level - clusters[-1]) / level > 0.005:
                clusters.append(level)
        self.support_resistance[asset]["support"] = [l for l in clusters if l < price * 0.99]
        self.support_resistance[asset]["resistance"] = [r for r in clusters if r > price * 1.01]

    # IMPROVED FVG DETECTION with fill tracking
    def detect_fvg(self, asset, tf=300):
        """
        Detect FVGs and update their fill status.
        Returns list of fresh (unfilled) FVGs first.
        """
        candles = self.candles[tf][asset]
        if len(candles) < 3:
            return []
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < 3:
            return []
        new_fvgs = []
        for i in range(len(complete)-2):
            c0, c1, c2 = complete[i], complete[i+1], complete[i+2]
            # Bullish FVG
            if c0["high"] < c2["low"] and c1["low"] > c0["high"]:
                fvg = {
                    "type": "BULLISH",
                    "top": c2["low"],
                    "bottom": c0["high"],
                    "created_at": c1["timestamp"],
                    "filled": False,
                    "partial_fill": False
                }
                new_fvgs.append(fvg)
            # Bearish FVG
            if c0["low"] > c2["high"] and c1["high"] < c0["low"]:
                fvg = {
                    "type": "BEARISH",
                    "top": c0["low"],
                    "bottom": c2["high"],
                    "created_at": c1["timestamp"],
                    "filled": False,
                    "partial_fill": False
                }
                new_fvgs.append(fvg)

        # Merge with existing FVGs and update fill status
        existing = self.fvg_cache.get(asset, [])
        # Remove old FVGs (older than 24h)
        now = int(time.time())
        existing = [f for f in existing if (now - f["created_at"]) < 86400 and not f.get("filled", False)]
        # Update fill status based on current price (passed separately)
        # We'll do that in a separate method update_fvg_status
        # Combine new and existing, keeping fresh ones (unfilled)
        all_fvgs = new_fvgs + existing
        # Remove duplicates
        unique = []
        seen = set()
        for f in all_fvgs:
            key = (f["type"], f["top"], f["bottom"])
            if key not in seen:
                seen.add(key)
                unique.append(f)
        # Keep last 10
        self.fvg_cache[asset] = unique[-10:]
        # Return fresh (unfilled) first
        return [f for f in self.fvg_cache[asset] if not f.get("filled", False)]

    def update_fvg_status(self, asset, price):
        """Check if price has filled any FVG."""
        if asset not in self.fvg_cache:
            return
        for fvg in self.fvg_cache[asset]:
            if fvg.get("filled"): continue
            if fvg["type"] == "BULLISH":
                if price <= fvg["top"] and price >= fvg["bottom"]:
                    fvg["partial_fill"] = True
                if price < fvg["bottom"]:
                    fvg["filled"] = True
            else:  # BEARISH
                if price <= fvg["top"] and price >= fvg["bottom"]:
                    fvg["partial_fill"] = True
                if price > fvg["top"]:
                    fvg["filled"] = True

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
# SIGNAL SCORING ENGINE (with adaptive weights)
# =====================================================================
class SignalScoringEngine:
    def __init__(self):
        self.base_weights = {
            "htf_trend": 15, "market_structure": 10, "liquidity_sweep": 8,
            "hunt_confirmation": 12, "fvg": 6, "order_block": 8, "volume": 7,
            "rsi": 6, "adx": 6, "news": 7, "institutional_liquidity": 10
        }
        self.weights = self.base_weights.copy()
        self.min_pass_layers = Config.MIN_LAYER_PASS
        self.layer_win_rates = {}   # dynamic per layer
        self.last_update = 0

    def update_layer_performance(self, db):
        """Refresh win rates per layer and adapt weights."""
        with db.conn.cursor() as cur:
            cur.execute("SELECT logic, pnl FROM trades WHERE status='closed' ORDER BY id DESC LIMIT 200")
            rows = cur.fetchall()
        layer_wins = {k: [0, 0] for k in self.base_weights.keys()}
        for logic_str, pnl in rows:
            if not logic_str: continue
            for layer in logic_str.split('+'):
                layer = layer.strip()
                if layer in layer_wins:
                    layer_wins[layer][1] += 1
                    if pnl > 0:
                        layer_wins[layer][0] += 1
        for layer, (wins, total) in layer_wins.items():
            self.layer_win_rates[layer] = wins / total if total > 0 else 0.5

        # Adapt weights: multiply base weight by performance ratio relative to 0.5
        # Clamp weight between 50% and 200% of base
        for layer, wr in self.layer_win_rates.items():
            ratio = wr / 0.5  # 1.0 means neutral
            ratio = max(0.5, min(2.0, ratio))
            self.weights[layer] = self.base_weights[layer] * ratio
        self.last_update = time.time()

    def evaluate(self, asset, price, patterns, sr_data, trend, news_sentiment, volume_ratio,
                 rsi, adx, volatility, htf_trend, bos, choch, fvgs, order_block, liquidity_sweep,
                 news_importance, hunt_confirmed=False, inst_liquidity_trigger=None):
        passed = []; score = 0
        # Use adaptive weights
        w = self.weights
        if htf_trend == trend and htf_trend != "NEUTRAL": score += w["htf_trend"]; passed.append("htf_trend")
        if choch: score += w["market_structure"]; passed.append("market_structure")
        if liquidity_sweep: score += w["liquidity_sweep"]; passed.append("liquidity_sweep")
        if hunt_confirmed: score += w["hunt_confirmation"]; passed.append("hunt_confirmation")
        if volume_ratio > 1.2: score += w["volume"]; passed.append("volume")
        if 30 <= rsi <= 70: score += w["rsi"]; passed.append("rsi")
        if adx > 25: score += w["adx"]; passed.append("adx")
        if inst_liquidity_trigger in ["BUY","SELL"]: score += w["institutional_liquidity"]; passed.append("institutional_liquidity")
        if abs(news_sentiment) > 50 and news_importance > 0.5: score += w["news"]; passed.append("news")
        if fvgs:
            for fvg in fvgs:
                if (trend == "BULLISH" and fvg["type"] == "BULLISH" and price > fvg["bottom"] and price < fvg["top"]):
                    score += w["fvg"]; passed.append("fvg"); break
                elif (trend == "BEARISH" and fvg["type"] == "BEARISH" and price > fvg["bottom"] and price < fvg["top"]):
                    score += w["fvg"]; passed.append("fvg"); break
        if order_block:
            score += w["order_block"]; passed.append("order_block")

        total_score = min(100, score)
        # Probability based on average win rate of passed layers
        if passed:
            avg_wr = sum(self.layer_win_rates.get(l, 0.5) for l in passed) / len(passed)
            prob = avg_wr * 100
        else:
            prob = 50
        prob = min(95, max(5, prob))

        confidence = "HIGH" if total_score >= 70 else "MEDIUM" if total_score >= 50 else "LOW"
        return {"total_score": total_score, "confidence": confidence,
                "probability": prob, "passed_layers": passed, "num_passed": len(passed),
                "enough": len(passed) >= self.min_pass_layers}

# =====================================================================
# TELEGRAM PIPELINE (unchanged)
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
# BINANCE WEBSOCKET (with exponential backoff and heartbeat)
# =====================================================================
class BinancePublicStream:
    def __init__(self, on_price_update):
        self.on_price_update = on_price_update
        self.running = False
        self.reconnect_count = 0
        self.delay = Config.WS_RECONNECT_MIN_DELAY
        self.ws = None

    def start(self):
        self.running = True
        threading.Thread(target=self._ws_loop, daemon=True).start()

    def _ws_loop(self):
        import websocket
        while self.running:
            try:
                streams = [f"{a.lower()}@kline_1m" for a in Config.ASSETS]
                url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"
                self.ws = websocket.WebSocketApp(url,
                                                 on_message=self._on_msg,
                                                 on_error=self._on_error,
                                                 on_close=self._on_close,
                                                 on_open=self._on_open)
                self.ws.run_forever(ping_interval=Config.WS_HEARTBEAT_INTERVAL,
                                    ping_timeout=10,
                                    ping_payload="ping")
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
            # Exponential backoff
            if self.running:
                self.reconnect_count += 1
                self.delay = min(Config.WS_RECONNECT_MAX_DELAY, self.delay * 1.5)
                logger.info(f"WebSocket reconnecting in {self.delay}s...")
                time.sleep(self.delay)

    def _on_open(self, ws):
        logger.info("WebSocket connected.")
        self.reconnect_count = 0
        self.delay = Config.WS_RECONNECT_MIN_DELAY

    def _on_error(self, ws, error):
        logger.warning(f"WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"WebSocket closed: {close_status_code} - {close_msg}")

    def _on_msg(self, ws, msg):
        try:
            data = json.loads(msg)["data"]["k"]
            symbol = data["s"]
            if symbol in Config.ASSETS:
                self.on_price_update(symbol, float(data["c"]), float(data["v"]))
        except Exception as e:
            logger.debug(f"WebSocket message error: {e}")

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()

# =====================================================================
# HEALTH SERVER (unchanged)
# =====================================================================
def start_health_server(orchestrator):
    port = int(os.environ.get("PORT", 10000))
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type","application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status":"online",
                "version":"5.2.3-Final",
                "active_trades":len(orchestrator.active_trades)
            }).encode())
    httpd = HTTPServer(("0.0.0.0", port), H)
    logger.info(f"Health server started on port {port}")
    httpd.serve_forever()

# =====================================================================
# LIFECYCLE CONTROLLER (with market structure revalidation)
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

                    # Time decay check
                    if trade_duration > Config.TIME_DECAY_SECONDS and abs(current_price - trade['entry']) / trade['entry'] < Config.TIME_DECAY_THRESHOLD_PCT:
                        self.orch._close_trade(tid, current_price, 0.0, "Time-Decay (Consolidation)")
                        to_remove.append(tid)
                        self.orch.telegram.send_message(f"⏳ <b>Time-Decay Exit:</b> Trade #{tid} closed.")
                        continue

                    # === INTELLIGENT HEALTH SCORE ===
                    health = 100
                    if trade['direction'] == 'BUY':
                        if trade['entry'] != trade['sl']:
                            dd = (trade['entry'] - current_price) / (trade['entry'] - trade['sl'])
                        else:
                            dd = 0
                    else:
                        if trade['sl'] != trade['entry']:
                            dd = (current_price - trade['entry']) / (trade['sl'] - trade['entry'])
                        else:
                            dd = 0
                    dd = max(0, min(1, dd))
                    health -= dd * 40
                    stale = min(1, trade_duration / Config.TIME_DECAY_SECONDS)
                    health -= stale * 15
                    if (trade['direction'] == 'BUY' and htf_trend == 'BULLISH') or (trade['direction'] == 'SELL' and htf_trend == 'BEARISH'):
                        health += 5
                    else:
                        health -= 10
                    entry_atr = trade.get('entry_atr', atr)
                    if entry_atr > 0:
                        vol_change = atr / entry_atr
                        if vol_change > 1.5:
                            health -= 15
                    rsi = self.orch.asset_state[asset]['rsi']
                    if trade['direction'] == 'BUY' and rsi > 70:
                        health -= 10
                    elif trade['direction'] == 'SELL' and rsi < 30:
                        health -= 10
                    health = max(0, min(100, health))
                    trade['health'] = health

                    # === Market structure revalidation ===
                    # If BOS/CHOCH changes against trade direction, reduce health
                    bos_dir = self.orch.topology.bos[asset]["direction"]
                    if trade['direction'] == 'BUY' and bos_dir == 'DOWN':
                        health -= 15
                    elif trade['direction'] == 'SELL' and bos_dir == 'UP':
                        health -= 15
                    if self.orch.topology.choch[asset]:
                        # CHOCH indicates potential reversal
                        health -= 10

                    if health < Config.HEALTH_EMERGENCY_THRESHOLD:
                        pnl = current_price - trade['entry'] if trade['direction'] == 'BUY' else trade['entry'] - current_price
                        self.orch._close_trade(tid, current_price, pnl, f"Emergency (Health {health}%)")
                        to_remove.append(tid)
                        self.orch.telegram.send_message(f"🚨 <b>Emergency Exit:</b> Trade #{tid} cut early.")
                        continue

                    if int(time.time()) % Config.CONFIDENCE_UPDATE_INTERVAL < 60:
                        base_score = trade.get('initial_score', 70)
                        msg = (f"🔄 <b>Lifecycle Update: #{tid} ({asset})</b>\n"
                               f"Direction: {trade['direction']} | PnL: {(current_price - trade['entry']):.2f}\n"
                               f"📊 Confidence: {trade.get('current_score', base_score)}%\n"
                               f"❤️ Health: {health}%")
                        self.orch.telegram.send_message(msg)

                for tid in to_remove:
                    if tid in self.orch.active_trades:
                        del self.orch.active_trades[tid]
                gc.collect()

# =====================================================================
# TRADE JOURNAL AI (unchanged)
# =====================================================================
class TradeJournalAI:
    def __init__(self, db_connection):
        self.conn = db_connection

    def log_and_learn(self):
        with self.conn.cursor() as cur:
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

# =====================================================================
# CORE ORCHESTRATOR (with all patches integrated and thread safety)
# =====================================================================
class AIOrchestrator:
    def __init__(self):
        self.topology = CandleTopologyEngine()
        self.news = CryptoNewsScanner()
        self.scoring = SignalScoringEngine()
        self.liquidity = InstitutionalLiquidityEngine()
        self.ob_engine = OrderBlockEngine()
        self.telegram = TelegramPipeline()
        self.db = TradeDatabase()
        self.mongo = MongoDatabase()
        
        # Thread-safe data structures
        self.active_trades = {}
        self.trade_lock = threading.Lock()
        self.price_queue = queue.Queue(maxsize=1000)
        self.start_time = time.time()
        self.last_signal_time = {a:0 for a in Config.ASSETS}
        self.signal_timestamps = deque(maxlen=100)
        # Shared state with lock
        self.asset_state_lock = threading.Lock()
        self.asset_state = {a: {"trend":"NEUTRAL","htf_trend":"NEUTRAL","volume_ratio":1.0,
                                "rsi":50,"adx":20,"volatility":0.01,"news_sentiment":0,"news_importance":0.5,
                                "volume_spike":False} for a in Config.ASSETS}
        self.accepted = 0
        self.rejected = 0

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

    # --- Strong Trend Check (safe) ---
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

    # --- REAL ADX (Welles Wilder) - Corrected ---
    def _calc_adx(self, highs, lows, closes, period=14):
        """Correct Welles Wilder ADX calculation."""
        if len(closes) < period + 1:
            return 20
        # True Range
        tr = [max(highs[i] - lows[i],
                  abs(highs[i] - closes[i-1]),
                  abs(lows[i] - closes[i-1]))
              for i in range(1, len(closes))]
        # +DM and -DM
        plus_dm = []
        minus_dm = []
        for i in range(1, len(highs)):
            up = highs[i] - highs[i-1]
            down = lows[i-1] - lows[i]
            if up > down and up > 0:
                plus_dm.append(up)
            else:
                plus_dm.append(0)
            if down > up and down > 0:
                minus_dm.append(down)
            else:
                minus_dm.append(0)

        # Smooth using Wilder's method (exponential with smoothing factor 1/period)
        atr = [0] * len(tr)
        atr[0] = tr[0]
        for i in range(1, len(tr)):
            atr[i] = (atr[i-1] * (period-1) + tr[i]) / period

        plus_di = [0] * len(plus_dm)
        minus_di = [0] * len(minus_dm)
        plus_di[0] = 100 * (plus_dm[0] / (atr[0] if atr[0] != 0 else 1))
        minus_di[0] = 100 * (minus_dm[0] / (atr[0] if atr[0] != 0 else 1))
        for i in range(1, len(plus_dm)):
            plus_di[i] = 100 * ((plus_di[i-1] * (period-1) + plus_dm[i]) / (atr[i] * period))
            minus_di[i] = 100 * ((minus_di[i-1] * (period-1) + minus_dm[i]) / (atr[i] * period))

        # Directional Movement Index (DX)
        dx = [0] * len(plus_di)
        for i in range(len(dx)):
            denom = plus_di[i] + minus_di[i]
            dx[i] = 100 * abs(plus_di[i] - minus_di[i]) / denom if denom != 0 else 0

        # ADX is smoothed DX (Wilder's)
        adx = [0] * len(dx)
        adx[0] = dx[0]
        for i in range(1, len(dx)):
            adx[i] = (adx[i-1] * (period-1) + dx[i]) / period
        return adx[-1] if len(adx) > period else 20

    # --- REAL RSI (Wilder) with edge case handling ---
    def _calc_rsi(self, closes, period=14):
        """Wilder's RSI with safe zero handling."""
        if len(closes) < period + 1:
            return 50
        gains = 0.0
        losses = 0.0
        for i in range(1, period + 1):
            diff = closes[i] - closes[i-1]
            if diff > 0:
                gains += diff
            else:
                losses -= diff
        avg_gain = gains / period
        avg_loss = losses / period
        for i in range(period + 1, len(closes)):
            diff = closes[i] - closes[i-1]
            if diff > 0:
                avg_gain = (avg_gain * (period - 1) + diff) / period
                avg_loss = (avg_loss * (period - 1) + 0) / period
            else:
                avg_gain = (avg_gain * (period - 1) + 0) / period
                avg_loss = (avg_loss * (period - 1) - diff) / period
        if avg_loss == 0:
            return 100  # no losses, strong uptrend
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _update_indicators(self, asset, price):
        # 15m trend
        c15 = [c["close"] for c in self.topology.candles[900][asset] if c.get("complete", False)][-30:]
        if len(c15) > 10:
            e9, e21 = self.topology._ema(c15,9), self.topology._ema(c15,21)
            if len(e9)>1 and len(e21)>1: 
                with self.asset_state_lock:
                    self.asset_state[asset]["trend"] = "BULLISH" if e9[-1]>e21[-1] else "BEARISH"
            if len(c15)>=14:
                rsi_val = self._calc_rsi(c15)
                with self.asset_state_lock:
                    self.asset_state[asset]["rsi"] = rsi_val
        # HTF trend
        c1h = [c["close"] for c in self.topology.candles[3600][asset] if c.get("complete", False)][-30:]
        if len(c1h)>10:
            e9,e21 = self.topology._ema(c1h,9), self.topology._ema(c1h,21)
            if len(e9)>1 and len(e21)>1:
                with self.asset_state_lock:
                    self.asset_state[asset]["htf_trend"] = "BULLISH" if e9[-1]>e21[-1] else "BEARISH"
        # Real ADX
        if len(c1h) >= 30:
            highs = [c["high"] for c in self.topology.candles[3600][asset] if c.get("complete", False)][-30:]
            lows  = [c["low"]  for c in self.topology.candles[3600][asset] if c.get("complete", False)][-30:]
            closes = [c["close"] for c in self.topology.candles[3600][asset] if c.get("complete", False)][-30:]
            adx_val = self._calc_adx(highs, lows, closes, period=14)
            with self.asset_state_lock:
                self.asset_state[asset]["adx"] = adx_val
        else:
            with self.asset_state_lock:
                self.asset_state[asset]["adx"] = 20

        # Institutional Volume Analysis
        vols_15m = [c["volume"] for c in self.topology.candles[900][asset] if c.get("complete", False)]
        if len(vols_15m) >= 20:
            avg_vol = sum(vols_15m[-20:]) / 20
            current_vol = vols_15m[-1]
            prev_5_avg = sum(vols_15m[-6:-1]) / 5 if len(vols_15m) >= 6 else avg_vol
            is_spike = (current_vol > avg_vol * 1.5) and (current_vol > prev_5_avg * 1.2)
            vol_score = min(3.0, current_vol / avg_vol)
            with self.asset_state_lock:
                self.asset_state[asset]["volume_ratio"] = vol_score
                self.asset_state[asset]["volume_spike"] = is_spike
        else:
            with self.asset_state_lock:
                self.asset_state[asset]["volume_ratio"] = 1.0
                self.asset_state[asset]["volume_spike"] = False

        atr = self.topology.get_atr(asset)
        if atr:
            with self.asset_state_lock:
                self.asset_state[asset]["volatility"] = atr / price

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

    # ---- MAIN PRICE TICK HANDLER (with all patches) ----
    def _handle_price_tick(self, asset, price, volume):
        self.topology.process_tick(asset, price, volume)
        self._update_active_trades(asset, price)

        # Update FVG and OB statuses
        self.topology.update_fvg_status(asset, price)
        self.ob_engine.update_ob_status(asset, price)

        if self.topology.candle_just_closed[asset]:
            # Save completed 15m candle to MongoDB
            candles_15m = self.topology.candles[900][asset]
            if candles_15m and candles_15m[-1].get("complete", False):
                self.mongo.save_candle(asset, 900, candles_15m[-1])
            for tf in [60, 300, 3600]:
                c_list = self.topology.candles[tf][asset]
                if c_list and c_list[-1].get("complete", False):
                    self.mongo.save_candle(asset, tf, c_list[-1])

            # Detect order blocks with BOS confirmation
            bos_dir = self.topology.bos[asset]["direction"]
            self.ob_engine.detect_order_blocks(asset, candles_15m, bos_dir)

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

        # Get asset state safely
        with self.asset_state_lock:
            asset_state = self.asset_state[asset].copy()
        c1h = [c for c in self.topology.candles[3600][asset] if c.get("complete", False)]
        bsl = max(c['high'] for c in c1h[-20:]) if len(c1h)>=20 else price*1.02
        ssl = min(c['low'] for c in c1h[-20:]) if len(c1h)>=20 else price*0.98
        atr = self.topology.get_atr(asset)
        candle_1m = self.topology.candles[60][asset][-1] if self.topology.candles[60][asset] else None
        volume_ratio = asset_state["volume_ratio"]
        inst = self.liquidity.analyze(c1h, self.topology.candles[300][asset], candle_1m, price, atr, bsl, ssl, volume_ratio) if candle_1m else {"trigger":"WAIT"}
        sweep = self.topology.detect_liquidity_sweep(asset, price)
        hunt = (sweep=="SELL_SWEEP" and self.topology.check_1m_rejection(asset,"BUY")) or (sweep=="BUY_SWEEP" and self.topology.check_1m_rejection(asset,"SELL"))

        patterns = self.topology.detect_candle_patterns(asset)

        # Get FVGs (fresh ones)
        fvgs = self.topology.detect_fvg(asset, tf=300)

        # Get Order Block
        trend = asset_state["trend"]
        ob_level = self.ob_engine.get_ob_zone(asset, price, trend) if trend in ["BULLISH", "BEARISH"] else None

        # Update layer performance and adapt weights
        if self.accepted % 10 == 0 and self.accepted > 0:
            self.scoring.update_layer_performance(self.db)

        score = self.scoring.evaluate(asset, price, patterns, {}, trend,
                                      asset_state["news_sentiment"], asset_state["volume_ratio"],
                                      asset_state["rsi"], asset_state["adx"],
                                      asset_state["volatility"], asset_state["htf_trend"],
                                      self.topology.bos[asset], self.topology.choch[asset], fvgs, ob_level,
                                      sweep, asset_state["news_importance"], hunt, inst["trigger"])
        if not score["enough"] or score["total_score"] < Config.MIN_CONFLUENCE_SCORE:
            self.db.log_rejected(asset, price, score["total_score"], "Low score", asset_state["volatility"], "medium")
            self.rejected+=1
            if self.mongo.db:
                try:
                    self.mongo.db.rejected.insert_one({"asset": asset, "price": price, "score": score["total_score"], "reason": "Low score", "timestamp": int(time.time())})
                except: pass
            return

        if asset_state["htf_trend"] == "BULLISH" and asset_state["trend"] == "BULLISH":
            direction = "BUY"
        elif asset_state["htf_trend"] == "BEARISH" and asset_state["trend"] == "BEARISH":
            direction = "SELL"
        else: return

        regime = self.topology.get_volatility_regime(asset)
        sl_m, tp_m = Config.VOLATILITY_MULTIPLIERS.get(regime, (1.5, 2.5))
        sl = price - sl_m * atr if direction=="BUY" else price + sl_m * atr
        tp = price + tp_m * atr if direction=="BUY" else price - tp_m * atr

        sl_distance = abs(price - sl)
        if sl_distance < atr * Config.MIN_SL_DISTANCE_MULTIPLIER:
            reason = f"SL too tight ({sl_distance:.3f} < {atr * Config.MIN_SL_DISTANCE_MULTIPLIER:.3f})"
            self.db.log_rejected(asset, price, score["total_score"], reason, asset_state["volatility"], regime)
            self.rejected+=1
            return

        rr = abs(tp - price) / sl_distance
        if rr < Config.MIN_RISK_REWARD:
            tp = price + sl_distance * Config.MIN_RISK_REWARD if direction=="BUY" else price - sl_distance * Config.MIN_RISK_REWARD
            rr = abs(tp - price) / sl_distance
            if rr < Config.MIN_RISK_REWARD - 0.01:
                self.db.log_rejected(asset, price, score["total_score"], "RR low", asset_state["volatility"], regime)
                self.rejected+=1
                return

        if time.time() - self.last_signal_time[asset] < Config.SIGNAL_COOLDOWN and not self._is_strong_trend(asset):
            self.db.log_rejected(asset, price, score["total_score"], "Cooldown", asset_state["volatility"], regime)
            self.rejected+=1
            return

        logic_parts = [f"HTF {asset_state['htf_trend']}"]
        bos_dir = self.topology.bos[asset]["direction"]
        if (direction=="BUY" and bos_dir=="UP") or (direction=="SELL" and bos_dir=="DOWN"):
            logic_parts.append(f"BOS {bos_dir}")
        if self.topology.choch[asset]: logic_parts.append("CHOCH")
        if hunt: logic_parts.append("HUNT")
        if inst["trigger"] != "WAIT": logic_parts.append("INST_LIQ")
        if fvgs: logic_parts.append("FVG")
        if ob_level: logic_parts.append("OB")
        logic = "+".join(logic_parts)

        tid = self.db.log_trade(asset, direction, price, sl, tp, score["total_score"], score["confidence"],
                                list(patterns.keys()), logic, asset_state["volatility"],
                                regime, asset_state["htf_trend"], asset_state["news_sentiment"])
        
        if self.mongo.db:
            try:
                trade_doc = {
                    "id": tid, "asset": asset, "direction": direction, "entry": price, "stop_loss": sl, "take_profit": tp,
                    "score": score["total_score"], "confidence": score["confidence"], "logic": logic,
                    "timestamp": int(time.time()), "status": "open", "entry_time": int(time.time()),
                    "volatility": asset_state["volatility"], "regime": regime, 
                    "htf_trend": asset_state["htf_trend"], "entry_atr": atr
                }
                self.mongo.db.trades.insert_one(trade_doc)
            except Exception as e:
                logger.debug(f"Mongo trade backup error: {e}")

        with self.trade_lock:
            self.active_trades[tid] = {"id":tid, "asset":asset, "direction":direction, "entry":price,
                                       "sl":sl, "tp":tp, "entry_time":int(time.time()),
                                       "breakeven_locked":False, "trailing_activated":False,
                                       "hold_sent":False, "initial_score":score["total_score"],
                                       "current_score":score["total_score"], "health":100, "entry_atr":atr}
        self.accepted += 1
        self.last_signal_time[asset] = time.time()
        self.signal_timestamps.append(time.time())

        chart = self.topology.get_visual_topology(asset, price, direction, sl, tp, patterns)
        self.telegram.fire_signal(asset, direction, price, sl, tp, chart, logic,
                                  f"News Sentiment: {asset_state['news_sentiment']:.0f}", score, patterns, tid, datetime.now().strftime("%H:%M"), rr)
        logger.info(f"🔥 SIGNAL: {asset} {direction} @ {price} (Score: {score['total_score']:.0f}, RR: {rr:.2f})")

        if self.accepted % 10 == 0:
            insight = self.journal_ai.log_and_learn()
            self.telegram.send_message(insight)

    # ---- RUN ----
    def run(self):
        # Start health server immediately
        threading.Thread(target=start_health_server, args=(self,), daemon=True).start()

        # Parallel data loading
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
        self.telegram.send_message("🚀 AI v5.2.3 Final - Institutional Grade Patches Applied")
        
        last_news = 0
        while True:
            try:
                time.sleep(10)
                if time.time() - last_news > 60:
                    news = self.news.fetch_latest()
                    if news.get("fresh"):
                        sent = news.get("sentiment", 0)
                        with self.asset_state_lock:
                            for a in Config.ASSETS:
                                self.asset_state[a]["news_sentiment"] = sent
                                self.asset_state[a]["news_importance"] = 0.8
                        if news["articles"]:
                            self.telegram.fire_news_alert(news["articles"][0]["title"], 
                                                          sent, news.get("fear_greed", 50))
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

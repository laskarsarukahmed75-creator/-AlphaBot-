# =====================================================================
# app.py – AlphaBot v7.5 FINAL with Neutral Candle Engine
# =====================================================================
# एकीकृत आर्किटेक्चर: Sniper (प्राथमिक) + Neutral Candle बोनस
# =====================================================================

import math
from typing import List, Dict, Optional, Tuple, Any, Deque
import os
import time
import json
import logging
import threading
import queue
import requests
import sqlite3
import gc
import html
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import deque
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import websocket

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

try:
    from pymongo import MongoClient, ASCENDING, DESCENDING
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AI-Orchestrator-v7.5")

class Config:
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    DISPLAY_NAMES = {"BTCUSDT": "BTC/USDT", "ETHUSDT": "ETH/USDT", "SOLUSDT": "SOL/USDT"}

    MONGO_URI = os.getenv("MONGO_URI", "")
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "crypto_bot_v5")
    RENDER_URL = os.getenv("RENDER_URL", "https://alphabot-76tj.onrender.com")
    DB_PATH = "trades_v7.db"
    MAX_CANDLES = 500
    BINANCE_FUTURES_WS_URL = "wss://fstream.binance.com/ws"

    IST = pytz.timezone('Asia/Kolkata')
    SESSION_WINDOWS = [("ALWAYS", 0, 0, 23, 59)]
    DEAD_ZONES = []

    MIN_SQS = 45
    PENDING_VERIFICATION_CANDLES = 1
    VOLUME_DECAY_THRESHOLD = 0.5
    SIGNAL_COOLDOWN = 1200
    MAX_SIGNALS_PER_DAY = 8
    MAX_HOLD_TIME = 14400
    TIME_DECAY_SECONDS = 3600
    TIME_DECAY_THRESHOLD_PCT = 0.002
    HEALTH_EMERGENCY_THRESHOLD = 55
    TRADE_HEALTH_STALE_MINUTES = 25
    WS_HEALTH_CHECK_TIMEOUT = 1800

    ANCHOR_RETEST_TOLERANCE = 0.004
    ABSORPTION_MIN_SCORE = 50
    ABSORPTION_EXIT_SCORE = 80

# =====================================================================
# DATA VALIDATION LAYER (unchanged)
# =====================================================================
class DataValidator:
    @staticmethod
    def validate_price(price):
        try:
            return isinstance(price, (int, float)) and price > 0 and math.isfinite(price)
        except Exception:
            return False

    @staticmethod
    def validate_volume(volume):
        try:
            return isinstance(volume, (int, float)) and volume >= 0 and math.isfinite(volume)
        except Exception:
            return False

    @staticmethod
    def validate_candle(candle):
        if not candle:
            return False
        try:
            return (DataValidator.validate_price(candle.get("open")) and
                    DataValidator.validate_price(candle.get("high")) and
                    DataValidator.validate_price(candle.get("low")) and
                    DataValidator.validate_price(candle.get("close")) and
                    candle["high"] >= max(candle["open"], candle["close"]) and
                    candle["low"] <= min(candle["open"], candle["close"]) and
                    candle["low"] <= candle["high"] and
                    candle["volume"] >= 0)
        except Exception:
            return False

# =====================================================================
# DATABASE LAYERS (unchanged – thread-safe)
# =====================================================================
class MongoDatabase:
    def __init__(self):
        if not HAS_PYMONGO or not Config.MONGO_URI:
            self.client = None
            self.db = None
            return
        try:
            self.client = MongoClient(Config.MONGO_URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[Config.MONGO_DB_NAME]
            self._create_indexes()
            logger.info("MongoDB connected")
        except Exception as e:
            logger.warning(f"MongoDB connection failed: {e}")
            self.client = None
            self.db = None

    def _create_indexes(self):
        if self.db is None:
            return
        try:
            self.db.candles.create_index([("asset", 1), ("timeframe", 1), ("timestamp", 1)], unique=True)
            self.db.trades.create_index([("id", 1)], unique=True)
            self.db.trades.create_index([("status", 1)])
        except Exception:
            pass

    def save_candle(self, asset, timeframe, candle):
        if self.db is None: return
        try:
            doc = {**candle, "asset": asset, "timeframe": timeframe}
            self.db.candles.update_one(
                {"asset": asset, "timeframe": timeframe, "timestamp": candle["timestamp"]},
                {"$set": doc}, upsert=True
            )
        except Exception:
            pass

    def load_candles(self, asset, timeframe, limit=5000):
        if self.db is None: return []
        try:
            if timeframe in (3600, 14400):
                fetch_limit = 3000  # 1H और 4H का पूरा 3+ महीने का डेटा
            elif timeframe == 900:
                fetch_limit = 5000  # 15m का ~50 दिन का डेटा
            else:
                fetch_limit = 1000  # 1m/5m के लिए सुरक्षित लिमिट
            return list(self.db.candles.find({"asset": asset, "timeframe": timeframe})
                        .sort("timestamp", 1).limit(fetch_limit))
        except Exception:
            return []

    def save_trade_backup(self, trade_data):
        if self.db is None: return
        try:
            self.db.trades.replace_one({"id": trade_data["id"]}, trade_data, upsert=True)
        except Exception:
            pass

    def update_trade_sl(self, trade_id, new_sl):
        if self.db is None: return
        try:
            self.db.trades.update_one({"id": trade_id}, {"$set": {"stop_loss": new_sl}})
        except Exception:
            pass

    def close_trade_mongo(self, trade_id, exit_price, pnl, exit_reason):
        if self.db is None: return
        try:
            self.db.trades.update_one({"id": trade_id}, {"$set": {
                "status": "closed",
                "exit_price": exit_price,
                "pnl": pnl,
                "close_time": int(time.time()),
                "exit_reason": exit_reason
            }})
        except Exception:
            pass

    def get_open_trades(self):
        if self.db is None: return []
        try:
            return list(self.db.trades.find({"status": "open"}))
        except Exception:
            return []

    def save_rejected_backup(self, rejected_data):
        if self.db is None: return
        try:
            self.db.rejected.insert_one(rejected_data)
        except Exception:
            pass

class PersistentMemoryEngine:
    def __init__(self, mongo_db):
        self.db = mongo_db
        self.collection = "global_bot_memory"
        if self.db is not None:
            try:
                if self.collection not in self.db.list_collection_names():
                    self.db.create_collection(self.collection)
            except Exception:
                pass

    def get_or_create_state(self):
        if self.db is None:
            return self._default_state()
        try:
            doc = self.db[self.collection].find_one({"_id": "global_state"})
            if doc:
                return doc
            default = self._default_state()
            default["_id"] = "global_state"
            self.db[self.collection].insert_one(default)
            return default
        except Exception:
            return self._default_state()

    def _default_state(self):
        return {
            "first_launch_timestamp": int(time.time()),
            "total_run_seconds": 0,
            "total_signals_generated": 0,
            "accepted_signals_count": 0,
            "rejected_signals_count": 0,
            "total_trades_closed": 0,
            "total_wins": 0,
            "total_losses": 0,
            "total_pnl": 0.0,
            "last_update": int(time.time())
        }

    def update_state(self, updates: dict):
        if self.db is None:
            return
        try:
            inc_fields = {k: v for k, v in updates.items() if isinstance(v, (int, float)) and k != "total_pnl"}
            set_fields = {k: v for k, v in updates.items() if not isinstance(v, (int, float)) or k == "total_pnl"}
            if inc_fields:
                inc_fields["last_update"] = 1
            else:
                set_fields["last_update"] = int(time.time())
            update_doc = {}
            if inc_fields:
                update_doc["$inc"] = inc_fields
            if set_fields:
                update_doc["$set"] = set_fields
            self.db[self.collection].update_one({"_id": "global_state"}, update_doc, upsert=True)
        except Exception as e:
            logger.error(f"Memory update error: {e}")

class TradeDatabase:
    def __init__(self):
        self.local = threading.local()
        self._create_tables()

    def _get_conn(self):
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(Config.DB_PATH, check_same_thread=False)
            self.local.conn.row_factory = sqlite3.Row
        return self.local.conn

    def _create_tables(self):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute('''CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY,
                asset TEXT, direction TEXT,
                entry REAL, stop_loss REAL, take_profit REAL,
                score INTEGER, confidence TEXT, patterns TEXT, logic TEXT,
                timestamp INTEGER, status TEXT DEFAULT 'open',
                exit_price REAL, pnl REAL, close_time INTEGER,
                volatility REAL, market_regime TEXT, htf_trend TEXT, news_score REAL,
                entry_time INTEGER, exit_reason TEXT,
                session TEXT, sqs_score INTEGER, pattern_name TEXT,
                regime TEXT, dynamic_min_sqs INTEGER,
                signal_type TEXT DEFAULT 'STANDARD',
                signal_token TEXT
            )''')
            cur.execute('''CREATE TABLE IF NOT EXISTS rejected_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset TEXT, price REAL, score INTEGER, reason TEXT,
                timestamp INTEGER, volatility REAL, market_regime TEXT,
                gate_name TEXT, regime TEXT
            )''')
            cur.execute('''CREATE TABLE IF NOT EXISTS performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT, win_rate REAL, profit_factor REAL, sharpe REAL,
                total_trades INTEGER, winning_trades INTEGER, losing_trades INTEGER, total_pnl REAL
            )''')
            cur.execute('''CREATE TABLE IF NOT EXISTS pattern_performance (
                pattern_name TEXT, session TEXT, regime TEXT,
                total_trades INTEGER, wins INTEGER, last_updated INTEGER,
                PRIMARY KEY (pattern_name, session, regime)
            )''')
            cur.execute('''CREATE TABLE IF NOT EXISTS adaptive_params (
                asset TEXT PRIMARY KEY, regime TEXT,
                min_sqs INTEGER, use_sweep INTEGER, mtf_tolerance REAL,
                volume_decay REAL, last_updated INTEGER
            )''')
            try:
                cur.execute("ALTER TABLE trades ADD COLUMN signal_type TEXT DEFAULT 'STANDARD'")
                cur.execute("ALTER TABLE trades ADD COLUMN signal_token TEXT")
            except sqlite3.OperationalError:
                pass
            conn.commit()
        finally:
            cur.close()

    def generate_trade_id(self):
        return int(time.time() * 1000)

    def log_trade(self, trade_id, asset, direction, entry, sl, tp, score, confidence, patterns, logic,
                  volatility, regime, htf_trend, news_score, session, sqs_score, pattern_name,
                  dynamic_min_sqs, signal_type="STANDARD", signal_token=None):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute('''INSERT INTO trades 
                (id, asset, direction, entry, stop_loss, take_profit, score, confidence, patterns, logic,
                 timestamp, volatility, market_regime, htf_trend, news_score, entry_time, status,
                 session, sqs_score, pattern_name, regime, dynamic_min_sqs, signal_type, signal_token)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (trade_id, asset, direction, entry, sl, tp, score, confidence, json.dumps(patterns), logic,
                 int(time.time()), volatility, regime, htf_trend, news_score, int(time.time()), 'open',
                 session, sqs_score, pattern_name, regime, dynamic_min_sqs, signal_type, signal_token))
            conn.commit()
            return trade_id
        finally:
            cur.close()

    def log_rejected(self, asset, price, score, reason, volatility, regime, gate_name="", dynamic_regime=""):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute('''INSERT INTO rejected_signals (asset, price, score, reason, timestamp, volatility, market_regime, gate_name, regime)
                VALUES (?,?,?,?,?,?,?,?,?)''',
                (asset, price, score, reason, int(time.time()), volatility, regime, gate_name, dynamic_regime))
            conn.commit()
        finally:
            cur.close()

    def close_trade(self, trade_id, exit_price, pnl, exit_reason=""):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute('''UPDATE trades SET status='closed', exit_price=?, pnl=?, close_time=?, exit_reason=?
                WHERE id=?''', (exit_price, pnl, int(time.time()), exit_reason, trade_id))
            conn.commit()
        finally:
            cur.close()

    def get_performance_metrics(self):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) FROM trades WHERE status='closed' AND pnl IS NOT NULL")
            total = cur.fetchone()[0] or 0
            cur.execute("SELECT COUNT(*) FROM trades WHERE status='closed' AND pnl > 0")
            wins = cur.fetchone()[0] or 0
            cur.execute("SELECT SUM(pnl) FROM trades WHERE status='closed' AND pnl > 0")
            gross_profit = cur.fetchone()[0] or 0.0
            cur.execute("SELECT SUM(pnl) FROM trades WHERE status='closed' AND pnl < 0")
            gross_loss = abs(cur.fetchone()[0] or 0.0)
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

    def get_recent_signal_timestamps(self, seconds=86400):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cutoff = int(time.time() - seconds)
            cur.execute("SELECT timestamp FROM trades WHERE timestamp >= ?", (cutoff,))
            return [row[0] for row in cur.fetchall()]
        finally:
            cur.close()

# =====================================================================
# NEWS SCANNER (unchanged)
# =====================================================================
class CryptoNewsScanner:
    def __init__(self):
        self.last_news = {}
        self.fear_greed = 50

    def fetch_latest(self):
        try:
            resp = requests.get("https://min-api.cryptocompare.com/data/v2/news/?lang=EN&limit=3", timeout=5)
            articles = []
            if resp.status_code == 200:
                data = resp.json()
                if data.get("Data"):
                    for article in data["Data"][:2]:
                        title = article.get("title", "")
                        sentiment = self._analyze_sentiment(title)
                        articles.append({"title": title, "sentiment": sentiment})
                    if articles:
                        self.last_news = articles[0]
            fg_resp = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5)
            if fg_resp.status_code == 200:
                fg_data = fg_resp.json()
                if fg_data.get("data"):
                    self.fear_greed = int(fg_data["data"][0]["value"])
            return {"articles": articles, "fresh": True, "fear_greed": self.fear_greed}
        except Exception:
            return {"articles": [], "fresh": False, "fear_greed": 50}

    def _analyze_sentiment(self, text):
        bullish = ["bullish", "breakout", "surge", "buy", "accumulate", "rally", "green", "etf"]
        bearish = ["bearish", "crash", "dump", "sell", "liquidation", "drop", "red", "sec"]
        text = text.lower()
        score = sum(1 for w in bullish if w in text) - sum(1 for w in bearish if w in text)
        return max(-100, min(100, score * 20))

# =====================================================================
# WEBSOCKET STREAMS (Futures & Public Fixed – unchanged)
# =====================================================================
class BinanceFuturesStream:
    def __init__(self, on_data=None):
        self.symbols = [s.lower() for s in Config.ASSETS]
        self.ws = None
        self.running = False
        self.data = {'open_interest': {}, 'liquidations': deque(maxlen=200), 'cvd': {}, 'last_trade': {}}
        self.oi_history = {s: deque(maxlen=10) for s in self.symbols}
        self.lock = threading.Lock()
        self.reconnect_count = 0
        self.on_data = on_data
        self.thread = None
        self.last_ping = time.time()
        self.oi_fetch_running = False

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.running = True
        self.oi_fetch_running = True
        self.thread = threading.Thread(target=self._ws_loop, daemon=True)
        self.thread.start()
        threading.Thread(target=self._health_check, daemon=True).start()
        threading.Thread(target=self._fetch_oi_loop, daemon=True).start()

    def stop(self):
        self.running = False
        self.oi_fetch_running = False
        if self.ws:
            try: self.ws.close()
            except Exception: pass

    def _fetch_oi_loop(self):
        while self.oi_fetch_running:
            try:
                for symbol in self.symbols:
                    resp = requests.get(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol.upper()}", timeout=5)
                    if resp.status_code == 200:
                        oi = float(resp.json()['openInterest'])
                        with self.lock:
                            self.data['open_interest'][symbol] = oi
                            self.oi_history[symbol].append(oi)
                time.sleep(30)
            except Exception as e:
                logger.error(f"OI fetch error: {e}")
                time.sleep(60)

    def _ws_loop(self):
        while self.running:
            try:
                streams = []
                for s in self.symbols:
                    streams.extend([f"{s}@aggTrade", f"{s}@forceOrder"])
                ws_url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
                self.ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                    on_ping=self._on_ping,
                    on_pong=self._on_pong
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                logger.error(f"Futures WS Loop Error: {e}")
                self.reconnect_count += 1
                time.sleep(5)

    def _on_open(self, ws):
        self.reconnect_count = 0
        self.last_ping = time.time()
        logger.info("✅ Futures Combined WS connected successfully.")

    def _on_message(self, ws, message):
        self.last_ping = time.time()
        try:
            raw = json.loads(message)
            data = raw.get("data", raw)
            if not isinstance(data, dict) or 'e' not in data:
                return
            e = data['e']
            if e == 'forceOrder':
                order = data.get('o', {})
                symbol = order.get('s')
                if symbol:
                    with self.lock:
                        self.data['liquidations'].append({
                            'symbol': symbol, 'side': order.get('S'),
                            'price': float(order.get('p', 0)), 'qty': float(order.get('q', 0)),
                            'time': time.time()
                        })
                        if self.on_data:
                            self.on_data('liquidation', symbol, {'side': order.get('S'), 'price': float(order.get('p', 0)), 'qty': float(order.get('q', 0))})
            elif e == 'aggTrade':
                symbol = data.get('s')
                if symbol:
                    price = float(data.get('p', 0))
                    qty = float(data.get('q', 0))
                    with self.lock:
                        last = self.data['last_trade'].get(symbol, price)
                        delta = qty if price >= last else -qty
                        self.data['cvd'][symbol] = self.data['cvd'].get(symbol, 0) + delta
                        self.data['last_trade'][symbol] = price
                        if self.on_data:
                            self.on_data('cvd', symbol, self.data['cvd'][symbol])
        except Exception:
            pass

    def _on_ping(self, ws, data):
        self.last_ping = time.time()

    def _on_pong(self, ws, data):
        self.last_ping = time.time()

    def _on_error(self, ws, error):
        logger.error(f"Futures WS error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        self.reconnect_count += 1
        logger.warning("Futures WS closed. Auto-reconnecting...")

    def _health_check(self):
        while self.running:
            time.sleep(30)
            if time.time() - self.last_ping > Config.WS_HEALTH_CHECK_TIMEOUT:
                logger.warning(f"Futures WS no data >{Config.WS_HEALTH_CHECK_TIMEOUT}s, forcing reconnect")
                if self.ws:
                    try: self.ws.close()
                    except Exception: pass
                self.reconnect_count += 1

    def get_open_interest(self, symbol):
        with self.lock:
            return self.data['open_interest'].get(symbol, 0)

    def get_oi_trend(self, symbol):
        with self.lock:
            hist = list(self.oi_history.get(symbol.lower(), []))
            return hist[-1] - hist[0] if len(hist) >= 2 else 0

    def get_cvd(self, symbol):
        with self.lock:
            return self.data['cvd'].get(symbol, 0)

    def get_liquidations(self, symbol, lookback_seconds=60):
        with self.lock:
            now = time.time()
            return [e for e in self.data['liquidations'] if e['symbol'] == symbol and (now - e['time']) <= lookback_seconds]

class BinancePublicStream:
    def __init__(self, on_price_update):
        self.on_price_update = on_price_update
        self.running = False
        self.reconnect_count = 0
        self.last_ping = time.time()
        self.last_tick_time = time.time()
        self.tick_counter = 0
        self.ws = None
        self.lock = threading.Lock()

    def start(self):
        self.running = True
        threading.Thread(target=self._ws_loop, daemon=True).start()
        threading.Thread(target=self._health_check, daemon=True).start()

    def _ws_loop(self):
        while self.running:
            try:
                streams = [f"{a.lower()}@kline_1m" for a in Config.ASSETS]
                ws_url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"
                self.ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=self._on_open,
                    on_message=self._on_msg,
                    on_error=self._on_error,
                    on_close=self._on_close,
                    on_ping=self._on_ping,
                    on_pong=self._on_pong
                )
                self.ws.run_forever(ping_interval=15, ping_timeout=10)
            except Exception as e:
                logger.error(f"Public WS loop exception: {e}")
                self.reconnect_count += 1
                time.sleep(5)

    def _on_open(self, ws):
        with self.lock:
            self.last_ping = time.time()
            self.reconnect_count = 0
        logger.info("✅ Public WebSocket connected.")

    def _on_msg(self, ws, msg):
        with self.lock:
            self.last_ping = time.time()
            self.last_tick_time = time.time()
            self.tick_counter += 1
        try:
            data = json.loads(msg)["data"]["k"]
            symbol = data["s"]
            if symbol in Config.ASSETS:
                self.on_price_update(symbol, float(data["c"]), float(data["v"]))
                if self.tick_counter % 50 == 0:
                    logger.info(f"📊 Public WS ticks: {self.tick_counter} (last: {symbol} @ {float(data['c'])})")
        except Exception as e:
            logger.error(f"Public WS msg parse error: {e}")

    def _on_ping(self, ws, data):
        with self.lock:
            self.last_ping = time.time()

    def _on_pong(self, ws, data):
        with self.lock:
            self.last_ping = time.time()

    def _on_error(self, ws, error):
        logger.error(f"Public WS error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"Public WS closed: {close_status_code} {close_msg}. Reconnecting...")
        self.reconnect_count += 1

    def _health_check(self):
        while self.running:
            time.sleep(30)
            with self.lock:
                age = time.time() - self.last_ping
            if age > 300:
                logger.warning(f"Public WS no ping/pong for {age:.0f}s, forcing reconnect")
                if self.ws:
                    try: self.ws.close()
                    except Exception: pass

# =====================================================================
# CANDLE TOPOLOGY ENGINE (Thread-safe – unchanged)
# =====================================================================
class CandleTopologyEngine:
    def __init__(self):
        self.lock = threading.RLock()
        self.candles = {tf: {asset: [] for asset in Config.ASSETS} for tf in [60, 300, 900, 3600, 14400]}
        self.pivots = {asset: {"high": [], "low": []} for asset in Config.ASSETS}
        self.bos = {asset: {"direction": ""} for asset in Config.ASSETS}
        self.choch = {asset: False for asset in Config.ASSETS}
        self.support_resistance = {asset: {"support": [], "resistance": []} for asset in Config.ASSETS}
        self.last_tick_time = {asset: 0 for asset in Config.ASSETS}
        self.candle_just_closed = {asset: False for asset in Config.ASSETS}
        self.history = {asset: deque(maxlen=200) for asset in Config.ASSETS}
        self.volume_ma = {asset: 0.0 for asset in Config.ASSETS}
        self.high_volume_levels = {asset: [] for asset in Config.ASSETS}

    def process_tick(self, asset, price, volume):
        if not DataValidator.validate_price(price) or not DataValidator.validate_volume(volume):
            return
        with self.lock:
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
                    self._detect_high_volume_anchor(asset, storage[-1])

            for timeframe in [60, 300, 900, 3600, 14400]:
                self._build_candle(asset, price, volume, now, timeframe, self.candles[timeframe][asset])

            self._update_volume_ma(asset)
            self._update_pivots(asset, price)
            self._update_support_resistance(asset, price)
            self._detect_bos_choch(asset)
            self.last_tick_time[asset] = now

    def _build_candle(self, asset, price, volume, ts, tf, storage):
        start = (ts // tf) * tf
        if not storage or storage[-1].get("timestamp") != start:
            if storage and not storage[-1].get("complete", False):
                storage[-1]["complete"] = True
            candle = {
                "timestamp": start, "open": price, "high": price, "low": price,
                "close": price, "volume": volume, "complete": False
            }
            if DataValidator.validate_candle(candle):
                storage.append(candle)
            if len(storage) > Config.MAX_CANDLES:
                storage.pop(0)
        else:
            c = storage[-1]
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["close"] = price
            c["volume"] += volume

    def _detect_high_volume_anchor(self, asset, candle):
        vol_ma = self.volume_ma.get(asset, 0)
        if vol_ma == 0:
            return
        if candle["volume"] > 2.0 * vol_ma:
            levels = []
            levels.append({"price": candle["high"], "type": "high"})
            levels.append({"price": candle["low"], "type": "low"})
            if abs(candle["close"] - candle["open"]) / (candle["high"] - candle["low"] + 0.001) > 0.5:
                levels.append({"price": candle["close"], "type": "close"})
            for lvl in levels:
                if not any(abs(lvl["price"] - existing["price"]) / existing["price"] < 0.002 for existing in self.high_volume_levels[asset]):
                    self.high_volume_levels[asset].append({"price": lvl["price"], "type": lvl["type"], "timestamp": int(time.time())})
            if len(self.high_volume_levels[asset]) > 10:
                self.high_volume_levels[asset] = self.high_volume_levels[asset][-10:]

    def check_anchor_line_retest(self, asset, price, direction=None):
        tolerance = Config.ANCHOR_RETEST_TOLERANCE
        with self.lock:
            for lvl in self.high_volume_levels.get(asset, []):
                level_price = lvl["price"]
                if abs(price - level_price) / level_price <= tolerance:
                    if direction:
                        if direction == "BUY":
                            if lvl["type"] in ("low", "close"):
                                if self.check_1m_rejection(asset, "BUY"):
                                    return True, level_price
                        else:
                            if lvl["type"] in ("high", "close"):
                                if self.check_1m_rejection(asset, "SELL"):
                                    return True, level_price
                    else:
                        return True, level_price
            return False, None

    def _update_volume_ma(self, asset):
        with self.lock:
            completed = self.get_completed(asset, 300)
            if len(completed) >= 20:
                self.volume_ma[asset] = sum(c["volume"] for c in completed[-20:]) / 20
            else:
                self.volume_ma[asset] = 0.0

    def _update_pivots(self, asset, price):
        with self.lock:
            complete = self.get_completed(asset, 900)
            if len(complete) < 10:
                return
            for i in range(2, len(complete)-2):
                if complete[i-2]["high"] < complete[i]["high"] > complete[i+2]["high"] and complete[i-1]["high"] < complete[i]["high"] > complete[i+1]["high"]:
                    if complete[i]["high"] not in self.pivots[asset]["high"]:
                        self.pivots[asset]["high"].append(complete[i]["high"])
                if complete[i-2]["low"] > complete[i]["low"] < complete[i+2]["low"] and complete[i-1]["low"] > complete[i]["low"] < complete[i+1]["low"]:
                    if complete[i]["low"] not in self.pivots[asset]["low"]:
                        self.pivots[asset]["low"].append(complete[i]["low"])
            self.pivots[asset]["high"] = sorted(self.pivots[asset]["high"], reverse=True)[:5]
            self.pivots[asset]["low"] = sorted(self.pivots[asset]["low"])[:5]

    def _detect_bos_choch(self, asset):
        with self.lock:
            h, l = self.pivots[asset]["high"], self.pivots[asset]["low"]
            if len(h) >= 2 and len(l) >= 2:
                if h[0] > h[1]:
                    self.bos[asset]["direction"] = "UP"
                elif l[0] < l[1]:
                    self.bos[asset]["direction"] = "DOWN"
                if len(h) >= 3 and len(l) >= 3:
                    self.choch[asset] = (h[1] < h[2] and l[1] > l[2]) or (h[1] > h[2] and l[1] < l[2])

    def _update_support_resistance(self, asset, price):
        with self.lock:
            all_levels = self.pivots[asset]["high"] + self.pivots[asset]["low"]
            recent = self.get_completed(asset, 900)[-10:]
            for c in recent:
                if c["high"] not in all_levels: all_levels.append(c["high"])
                if c["low"] not in all_levels: all_levels.append(c["low"])
            clusters = []
            for level in sorted(all_levels):
                if not clusters or abs(level - clusters[-1]) / level > 0.005:
                    clusters.append(level)
            self.support_resistance[asset]["support"] = [l for l in clusters if l < price * 0.99]
            self.support_resistance[asset]["resistance"] = [r for r in clusters if r > price * 1.01]

    def get_completed(self, asset, tf):
        with self.lock:
            return [c for c in self.candles[tf][asset] if c.get("complete", False)]

    def detect_candle_patterns(self, asset):
        with self.lock:
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

    def get_atr(self, asset, period=14, tf=3600):
        with self.lock:
            complete = self.get_completed(asset, tf)
            if len(complete) < period: return 0.0
            tr_list = []
            for i in range(1, period+1):
                high, low = complete[i]["high"], complete[i]["low"]
                prev_close = complete[i-1]["close"]
                tr_list.append(max(high-low, abs(high-prev_close), abs(low-prev_close)))
            return sum(tr_list) / period

    def detect_liquidity_sweep(self, asset, price):
        with self.lock:
            h, l = self.pivots[asset]["high"], self.pivots[asset]["low"]
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
        with self.lock:
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
        with self.lock:
            min_price = min(price, sl, tp) * 0.98
            max_price = max(price, sl, tp) * 1.02
            if max_price - min_price < 0.01:
                min_price = price * 0.95; max_price = price * 1.05
            sr = self.support_resistance[asset]
            supports = [s for s in sr["support"] if min_price <= s <= max_price]
            resistances = [r for r in sr["resistance"] if min_price <= r <= max_price]
            rows = 10
            lines = ["┌──────────────────────────────────────┐", "│       📊 LIVE TOPOLOGY CHART (Zoom)     │", "├──────────────────────────────────────┤"]
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
                    if any(abs(level - s) / s < 0.001 for s in supports): marker = "S"
                    elif any(abs(level - r) / r < 0.001 for r in resistances): marker = "R"
                bar = "█" * int((i / rows) * 10) if i > 0 else ""
                lines.append(f"│ {level:>8.2f} │ {marker} {bar:<10} │")
            lines += ["├──────────────────────────────────────┤", "│ ●=Entry ▼=SL ★=TP  S=Support R=Res │", "└──────────────────────────────────────┘"]
            return "\n".join(lines)

    def get_adx(self, asset, tf, period=14):
        with self.lock:
            complete = self.get_completed(asset, tf)
            if len(complete) < period: return 20
            tr_list, dm_plus, dm_minus = [], [], []
            for i in range(1, len(complete)):
                high, low = complete[i]["high"], complete[i]["low"]
                prev_high, prev_low = complete[i-1]["high"], complete[i-1]["low"]
                tr = max(high-low, abs(high-prev_high), abs(low-prev_low))
                tr_list.append(tr)
                up = high - prev_high; down = prev_low - low
                dm_plus.append(max(up,0) if up > down else 0)
                dm_minus.append(max(down,0) if down > up else 0)
            if len(tr_list) < period: return 20
            atr = sum(tr_list[:period]) / period
            dp = sum(dm_plus[:period]) / period
            dm = sum(dm_minus[:period]) / period
            for i in range(period, len(tr_list)):
                atr = (atr*(period-1)+tr_list[i])/period
                dp = (dp*(period-1)+dm_plus[i])/period
                dm = (dm*(period-1)+dm_minus[i])/period
            if atr == 0: return 20
            di_p = (dp/atr)*100
            di_m = (dm/atr)*100
            dx = (abs(di_p-di_m)/(di_p+di_m))*100 if (di_p+di_m)>0 else 0
            return min(100, dx)

    def detect_fvg(self, asset):
        with self.lock:
            complete = self.get_completed(asset, 900)
            if len(complete) < 3: return []
            fvgs = []
            for i in range(2, len(complete)-1):
                c1, c2, c3 = complete[i-2], complete[i-1], complete[i]
                if c1["close"] < c2["open"] and c2["close"] < c3["close"] and c1["high"] > c2["low"]:
                    fvgs.append({"type": "bullish", "upper": c1["high"], "lower": c2["low"]})
                if c1["close"] > c2["open"] and c2["close"] > c3["close"] and c2["high"] > c1["low"]:
                    fvgs.append({"type": "bearish", "upper": c2["high"], "lower": c1["low"]})
            return fvgs[-5:]

    def detect_order_block(self, asset):
        with self.lock:
            if not self.bos[asset]["direction"]: return {}
            complete = self.get_completed(asset, 900)
            if len(complete) < 10: return {}
            atr = self.get_atr(asset)
            if atr == 0: return {}
            for i in range(len(complete)-1, -1, -1):
                c = complete[i]
                if (c["high"] - c["low"]) > 1.5 * atr:
                    return {"type": "bullish" if c["close"] > c["open"] else "bearish", "high": c["high"], "low": c["low"]}
            return {}

    def _calc_rsi(self, closes, period=14):
        if len(closes) < period + 1: return 50
        start = len(closes) - period - 1
        gains = losses = 0.0
        for i in range(start+1, len(closes)):
            diff = closes[i] - closes[i-1]
            if diff > 0: gains += diff
            else: losses -= diff
        avg_gain = gains / period
        avg_loss = losses / period
        if avg_loss == 0: return 100
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

# =====================================================================
# INDICATOR CACHE (unchanged)
# =====================================================================
class IndicatorCache:
    def __init__(self, topology):
        self.topology = topology
        self.cache = {}
        self.lock = threading.Lock()
        self.cache_ttl = 60

    def get(self, asset, tf, price=None, volume=None, force_refresh=False):
        key = (asset, tf)
        now = time.time()
        with self.lock:
            if key in self.cache and not force_refresh:
                if now - self.cache[key]['timestamp'] < self.cache_ttl:
                    return self.cache[key]['data']
            data = self._compute(asset, tf, price, volume)
            self.cache[key] = {'data': data, 'timestamp': now}
            return data

    def _compute(self, asset, tf, price, volume):
        try:
            with self.topology.lock:
                candles = self.topology.candles.get(tf, {}).get(asset, [])
                completed = [c for c in candles if c.get("complete", False)]
                data = {}
                closes = [c['close'] for c in completed]
                if len(closes) >= 20:
                    for p in [9, 20, 21, 50, 200]:
                        ema_list = self.topology._ema(closes, p)
                        if ema_list:
                            data[f'ema_{p}'] = ema_list[-1]
                data['atr'] = self.topology.get_atr(asset, period=14, tf=tf) or 0.0
                data['adx'] = self.topology.get_adx(asset, tf, period=14) or 20
                data['rsi'] = self.topology._calc_rsi(closes) if len(closes) >= 14 else 50
                sr = self.topology.support_resistance.get(asset, {})
                data['support'] = sr.get('support', [])
                data['resistance'] = sr.get('resistance', [])
                data['bos'] = self.topology.bos.get(asset, {}).get('direction', '')
                data['choch'] = self.topology.choch.get(asset, False)
                data['fvg'] = self.topology.detect_fvg(asset)
                data['order_block'] = self.topology.detect_order_block(asset)
                data['volume_ma'] = self.topology.volume_ma.get(asset, 0.0)
                if price and volume and data['volume_ma'] > 0:
                    data['volume_ratio'] = volume / data['volume_ma']
                else:
                    data['volume_ratio'] = 1.0
                return data
        except Exception as e:
            logger.error(f"Indicator cache error: {e}")
            return {}

# =====================================================================
# NEUTRAL CANDLE ENGINE (NEW)
# =====================================================================
class NeutralCandleEngine:
    """
    Detects neutral/indecision candle patterns (Doji, Spinning Top, etc.)
    and returns a bonus score for directionally favorable patterns.
    """
    def __init__(self, topology):
        self.topology = topology

    def detect(self, asset, price, tf=900):
        """
        Analyze the last completed candle of given timeframe (default 15m).
        Returns a dict: {
            'pattern': str,          # e.g. "Doji", "SpinningTop", "LongLegged", "None"
            'direction': str,        # "BUY", "SELL", or "" if neutral
            'score': int,            # bonus score (0-20)
            'reason': str
        }
        """
        with self.topology.lock:
            completed = self.topology.get_completed(asset, tf)
            if len(completed) < 2:
                return {'pattern': 'None', 'direction': '', 'score': 0, 'reason': 'Insufficient data'}

            candle = completed[-1]
            open_ = candle['open']
            close = candle['close']
            high = candle['high']
            low = candle['low']
            body = abs(close - open_)
            range_ = high - low
            if range_ == 0:
                return {'pattern': 'None', 'direction': '', 'score': 0, 'reason': 'Zero range'}

            upper_wick = high - max(open_, close)
            lower_wick = min(open_, close) - low
            body_pct = body / range_

            # Define thresholds
            DOJI_BODY_PCT = 0.05
            SPINNING_TOP_BODY_PCT = 0.30
            WICK_RATIO = 2.0  # wick > 2 * body

            pattern = 'None'
            direction = ''
            score = 0
            reason = ''

            # Identify pattern
            if body_pct <= DOJI_BODY_PCT:
                # Doji or variants
                if upper_wick > 3 * lower_wick and upper_wick > body:
                    pattern = 'GravestoneDoji'
                    direction = 'SELL'   # potential top reversal
                elif lower_wick > 3 * upper_wick and lower_wick > body:
                    pattern = 'DragonflyDoji'
                    direction = 'BUY'
                else:
                    pattern = 'Doji'
                    direction = ''       # neutral
            elif body_pct <= SPINNING_TOP_BODY_PCT:
                # Spinning Top
                if upper_wick > body * WICK_RATIO and lower_wick > body * WICK_RATIO:
                    pattern = 'SpinningTop'
                    direction = ''
                elif upper_wick > lower_wick * 2 and upper_wick > body:
                    pattern = 'ShootingStar'
                    direction = 'SELL'
                elif lower_wick > upper_wick * 2 and lower_wick > body:
                    pattern = 'Hammer'
                    direction = 'BUY'
                else:
                    pattern = 'SpinningTop'
                    direction = ''
            else:
                # Not a neutral pattern
                return {'pattern': 'None', 'direction': '', 'score': 0, 'reason': 'No neutral pattern'}

            # If direction detected, assign a bonus score
            if direction == 'BUY':
                score = 15
                reason = f'{pattern} (bullish)'
            elif direction == 'SELL':
                score = 15
                reason = f'{pattern} (bearish)'
            else:
                # Neutral pattern, still may be useful but no directional bonus
                score = 5  # small bonus for showing indecision
                reason = f'{pattern} (neutral)'

            return {
                'pattern': pattern,
                'direction': direction,
                'score': score,
                'reason': reason
            }

# =====================================================================
# ENGINE: SNIPER (RELAXED – unchanged)
# =====================================================================
class RallyExhaustionFilter:
    def __init__(self, topology, absorption_meter):
        self.topology = topology
        self.absorption_meter = absorption_meter

    def evaluate(self, asset, price):
        with self.topology.lock:
            candles_4h = self.topology.candles[14400][asset]
            complete_4h = [c for c in candles_4h if c.get("complete", False)]
            if len(complete_4h) < 30:
                return None, "Insufficient 4H data"
            closes_4h = [c["close"] for c in complete_4h]
            ema20_4h = self.topology._ema(closes_4h, 20)
            if len(ema20_4h) < 2:
                return None, "EMA20 not ready"
            atr = self.topology.get_atr(asset, period=14, tf=3600)
            if atr == 0:
                return None, "ATR zero"
            above = price - ema20_4h[-1]
            below = ema20_4h[-1] - price
            overbought = above > 1.1 * atr
            oversold = below > 1.1 * atr
            if not overbought and not oversold:
                return None, "No overextension"

            candles_15m = self.topology.candles[900][asset]
            complete_15m = [c for c in candles_15m if c.get("complete", False)]
            if len(complete_15m) < 20:
                return None, "Insufficient 15m"
            last = complete_15m[-1]
            range_ = last["high"] - last["low"]
            if range_ == 0:
                return None, "No range"
            upper_wick = last["high"] - max(last["open"], last["close"])
            lower_wick = min(last["open"], last["close"]) - last["low"]

            if overbought and upper_wick / range_ > 0.4:
                direction = "SELL"
                base_reason = "Overbought+UpperWick"
            elif oversold and lower_wick / range_ > 0.4:
                direction = "BUY"
                base_reason = "Oversold+LowerWick"
            else:
                return None, "No rejection wick"

            meter = self.absorption_meter.get_meter(asset)
            if not meter:
                return None, "Meter not ready"
            meter_score = meter.get('score', 0)
            meter_direction = meter.get('direction', '')
            if meter_score < Config.ABSORPTION_MIN_SCORE:
                return None, f"Absorption score too low ({meter_score})"
            if meter_direction != direction:
                return None, f"Meter direction mismatch: {meter_direction} vs {direction}"

            anchor_ok, anchor_price = self.topology.check_anchor_line_retest(asset, price, direction)
            bonus = 15 if anchor_ok else 0
            anchor_text = f"AnchorRetest@{anchor_price:.2f}" if anchor_ok else "NoAnchor"

            score = 70
            rsi_4h = self.topology._calc_rsi(closes_4h[-15:])
            if direction == "SELL" and rsi_4h > 70: score += 10
            elif direction == "BUY" and rsi_4h < 30: score += 10
            score += bonus
            reason = f"{base_reason} + Absorption{meter_score} + {anchor_text}"
            return {"direction": direction, "score": min(score, 100), "reason": reason}, None

# =====================================================================
# REGIME DETECTOR (unchanged)
# =====================================================================
class RegimeDetector:
    def __init__(self, topology):
        self.topology = topology
        self.current_regime = {}
        self.params = {}

    def detect(self, asset, price, vol_ratio, htf_trend, tf_trend):
        with self.topology.lock:
            adx_15 = self.topology.get_adx(asset, 900)
            adx_1h = self.topology.get_adx(asset, 3600)
            atr = self.topology.get_atr(asset, period=14, tf=3600)
            atr_pct = atr / price if price > 0 else 0.01
            vol_ma = self.topology.volume_ma[asset]
            vol_ratio = vol_ratio if vol_ratio > 0 else 1.0
            trend_aligned = (htf_trend == tf_trend and htf_trend != "NEUTRAL")
            if adx_15 > 35 and vol_ratio > 1.5 and atr_pct > 0.005 and trend_aligned:
                regime = "STRONG_TREND"
                params = {"min_sqs": 70, "use_micro_sweep": True, "mtf_tolerance": 0.015,
                          "volume_decay_threshold": 0.5, "pending_candles": 2,
                          "order_flow_strict": True, "check_4h_ema": True}
            elif adx_15 >= 20 and adx_15 <= 35 and 0.8 <= vol_ratio <= 1.5 and 0.003 <= atr_pct <= 0.005 and trend_aligned:
                regime = "GRADUAL_TREND"
                params = {"min_sqs": 58, "use_micro_sweep": False, "mtf_tolerance": 0.025,
                          "volume_decay_threshold": 0.7, "pending_candles": 1,
                          "order_flow_strict": False, "check_4h_ema": False}
            else:
                regime = "CHOP"
                params = {"min_sqs": 75, "use_micro_sweep": True, "mtf_tolerance": 0.05,
                          "volume_decay_threshold": 0.7, "pending_candles": 2,
                          "order_flow_strict": True, "check_4h_ema": False}
            self.current_regime[asset] = regime
            self.params[asset] = params
            return regime, params

# =====================================================================
# GATES (unchanged)
# =====================================================================
class MarketRegimeFilter:
    def __init__(self, topology):
        self.topology = topology

    def check(self, asset, price, adx_threshold=22):
        with self.topology.lock:
            adx_15 = self.topology.get_adx(asset, 900)
            adx_1h = self.topology.get_adx(asset, 3600)
            if adx_15 < adx_threshold and adx_1h < adx_threshold:
                return False, "Sideways/Chop"
            candles_5m = self.topology.candles[300][asset]
            completed = [c for c in candles_5m if c.get("complete", False)]
            if len(completed) >= 5:
                recent_high = max(c["high"] for c in completed[-5:])
                recent_low = min(c["low"] for c in completed[-5:])
                last = completed[-1]
                vol_ma = self.topology.volume_ma[asset]
                if last["close"] > recent_high and last["volume"] < 1.2 * vol_ma:
                    return False, "Fake Breakout"
                if last["close"] < recent_low and last["volume"] < 1.2 * vol_ma:
                    return False, "Fake Breakdown"
            return True, "Pass"

class MTFConfluenceGate:
    def __init__(self, topology):
        self.topology = topology

    def check(self, asset, direction, tolerance=0.02, check_4h=False):
        with self.topology.lock:
            current_price = self.topology.history[asset][-1]['price'] if self.topology.history[asset] else 0
            if current_price == 0:
                return False, "No price"
            if check_4h:
                complete_4h = [c for c in self.topology.candles[14400][asset] if c.get("complete", False)]
                if len(complete_4h) >= 200:
                    closes_4h = [c["close"] for c in complete_4h]
                    ema50 = self.topology._ema(closes_4h, 50)
                    ema200 = self.topology._ema(closes_4h, 200)
                    if len(ema50) >= 2 and len(ema200) >= 2:
                        if direction == "BUY" and current_price < ema50[-1] and current_price < ema200[-1]:
                            return False, "4H bearish"
                        if direction == "SELL" and current_price > ema50[-1] and current_price > ema200[-1]:
                            return False, "4H bullish"
            pivots_high = self.topology.pivots[asset]["high"]
            pivots_low = self.topology.pivots[asset]["low"]
            if len(pivots_high) >= 2 and len(pivots_low) >= 2:
                if direction == "BUY" and pivots_high[0] < pivots_high[1]:
                    return False, "1H structure down"
                if direction == "SELL" and pivots_low[0] > pivots_low[1]:
                    return False, "1H structure up"
            sr = self.topology.support_resistance[asset]
            if direction == "BUY":
                if sr["support"]:
                    nearest = max(sr["support"])
                    if abs(current_price - nearest) / nearest > tolerance:
                        return False, f"Not near support"
            else:
                if sr["resistance"]:
                    nearest = min(sr["resistance"])
                    if abs(current_price - nearest) / nearest > tolerance:
                        return False, f"Not near resistance"
            return True, "Pass"

class OrderFlowAnalyzer:
    def __init__(self, topology, futures_stream):
        self.topology = topology
        self.futures = futures_stream

    def check(self, asset, direction, price, strict=True):
        symbol = asset.lower()
        with self.topology.lock:
            oi = self.futures.get_open_interest(symbol)
            oi_trend = self.futures.get_oi_trend(symbol)
            cvd = self.futures.get_cvd(symbol)
            if oi == 0:
                return True, "No OI data"
            if strict:
                if direction == "BUY" and oi_trend <= 0:
                    return False, "OI not increasing"
                if direction == "SELL" and oi_trend >= 0:
                    return False, "OI increasing while selling"
                candles = self.topology.candles[300][asset]
                completed = [c for c in candles if c.get("complete", False)]
                if len(completed) >= 2:
                    price_change = price - completed[-2]["close"]
                    if direction == "BUY" and price_change > 0 and cvd < 0:
                        return False, "CVD divergence"
                    if direction == "SELL" and price_change < 0 and cvd > 0:
                        return False, "CVD divergence"
            return True, "Pass"

class SessionTimer:
    def is_trading_time(self):
        return True, "ALWAYS", "00:00-23:59 IST"

# =====================================================================
# 100-POINT UNIFIED INTELLIGENCE ENGINE (Score-Based)
# =====================================================================
class UnifiedIntelligenceEngine:
    def __init__(self, topology, futures_stream, absorption_meter, neutral_engine):
        self.topology = topology
        self.futures = futures_stream
        self.absorption = absorption_meter
        self.neutral = neutral_engine

    def evaluate_setup(self, asset: str, price: float, direction: str):
        total_score = 0
        breakdown = {}

        with self.topology.lock:
            # 1. 4H Macro Overextension (Max 15 Pts)
            c_4h = self.topology.get_completed(asset, 14400)
            if len(c_4h) >= 30:
                closes_4h = [c["close"] for c in c_4h]
                ema20 = self.topology._ema(closes_4h, 20)[-1]
                atr_4h = self.topology.get_atr(asset, period=14, tf=14400) or (price * 0.01)
                dist = abs(price - ema20) / atr_4h
                pts = 15 if dist >= 2.0 else (10 if dist >= 1.5 else (5 if dist >= 1.0 else 0))
            else:
                pts = 0
            breakdown["MacroOverextension"] = pts
            total_score += pts

            # 2. 15m Wick Rejection (Max 15 Pts)
            c_15m = self.topology.get_completed(asset, 900)
            if len(c_15m) >= 2:
                last = c_15m[-1]
                rng = last["high"] - last["low"]
                if rng > 0:
                    upper_wick = (last["high"] - max(last["open"], last["close"])) / rng
                    lower_wick = (min(last["open"], last["close"]) - last["low"]) / rng
                    wick_ratio = lower_wick if direction == "BUY" else upper_wick
                    rej_pts = int(min(15, wick_ratio * 25))
                else:
                    rej_pts = 0
            else:
                rej_pts = 0
            breakdown["15mRejection"] = rej_pts
            total_score += rej_pts

            # 3. High Volume Anchor (Max 10 Pts)
            anchor_ok, _ = self.topology.check_anchor_line_retest(asset, price, direction)
            pts = 10 if anchor_ok else 0
            breakdown["VolumeAnchor"] = pts
            total_score += pts

            # 4. Multi-Timeframe Structure (Max 15 Pts)
            mtf_pts = 0
            p_highs = self.topology.pivots[asset]["high"]
            p_lows = self.topology.pivots[asset]["low"]
            if direction == "BUY" and len(p_highs) >= 2 and p_highs[0] >= p_highs[1]: mtf_pts += 8
            if direction == "SELL" and len(p_lows) >= 2 and p_lows[0] <= p_lows[1]: mtf_pts += 8
            sr = self.topology.support_resistance[asset]
            atr_1h = self.topology.get_atr(asset, period=14, tf=3600) or (price * 0.01)
            if direction == "BUY" and sr["support"] and abs(price - max(sr["support"])) <= 1.2 * atr_1h: mtf_pts += 7
            elif direction == "SELL" and sr["resistance"] and abs(price - min(sr["resistance"])) <= 1.2 * atr_1h: mtf_pts += 7
            breakdown["MTFStructure"] = min(15, mtf_pts)
            total_score += breakdown["MTFStructure"]

        # 5. Order Flow & Absorption (Max 20 Pts)
        symbol = asset.lower()
        oi_trend = self.futures.get_oi_trend(symbol)
        cvd = self.futures.get_cvd(symbol)
        meter = self.absorption.get_meter(asset)
        of_pts = 0
        if meter.get("score", 0) >= 50 and meter.get("direction") == direction: of_pts += 10
        if (direction == "BUY" and cvd > 0) or (direction == "SELL" and cvd < 0): of_pts += 5
        if (direction == "BUY" and oi_trend > 0) or (direction == "SELL" and oi_trend < 0): of_pts += 5
        breakdown["OrderFlow"] = of_pts
        total_score += of_pts

        # 6. Neutral Candle Engine (Max 15 Pts)
        neutral_data = self.neutral.detect(asset, price, tf=900)
        neutral_pts = 0
        if neutral_data.get("direction") == direction:
            pat = neutral_data.get("pattern", "")
            if pat in ("Hammer", "ShootingStar", "DragonflyDoji", "GravestoneDoji"): neutral_pts = 15
            elif pat in ("SpinningTop", "Doji"): neutral_pts = 8
        breakdown["NeutralCandle"] = neutral_pts
        total_score += neutral_pts

        # 7. ADX Exhaustion (Max 10 Pts)
        adx_15 = self.topology.get_adx(asset, 900)
        pts = 10 if (20 <= adx_15 <= 40) else (5 if adx_15 < 20 else 2)
        breakdown["RegimeExhaustion"] = pts
        total_score += pts

        is_valid = total_score >= 65
        reason = " | ".join([f"{k}:{v}" for k, v in breakdown.items() if v > 0])
        return is_valid, total_score, reason

# =====================================================================
# DYNAMIC STOP LOSS (unchanged)
# =====================================================================
class DynamicStopLoss:
    def __init__(self, topology):
        self.topology = topology

    def calculate(self, asset, direction, entry, atr):
        buffer = max(atr * 0.8, entry * 0.005)
        with self.topology.lock:
            sr = self.topology.support_resistance[asset]
            nearest_support = None
            nearest_resistance = None
            if sr["support"]:
                candidates = [s for s in sr["support"] if s < entry and (entry - s) / entry < 0.10]
                if candidates: nearest_support = max(candidates)
            if sr["resistance"]:
                candidates = [r for r in sr["resistance"] if r > entry and (r - entry) / entry < 0.10]
                if candidates: nearest_resistance = min(candidates)
        default_sl = entry + 1.5 * atr if direction == "SELL" else entry - 1.5 * atr
        if direction == "SELL":
            sl = nearest_resistance + 0.5 * atr if nearest_resistance else default_sl
            if sl - entry > 1.5 * atr: sl = default_sl
            sl = max(sl, entry + buffer)
        else:
            sl = nearest_support - 0.5 * atr if nearest_support else default_sl
            if entry - sl > 1.5 * atr: sl = default_sl
            sl = min(sl, entry - buffer)
        risk = abs(entry - sl)
        default_tp = entry - 2 * risk if direction == "SELL" else entry + 2 * risk
        if direction == "SELL":
            tp = nearest_support if nearest_support and (entry - nearest_support) <= 3 * risk else default_tp
            tp = max(tp, entry - 3 * risk, entry * 0.70)
            if tp >= entry: tp = entry - 1.5 * risk
            if entry - tp < 1.5 * risk: tp = entry - 1.5 * risk
        else:
            tp = nearest_resistance if nearest_resistance and (nearest_resistance - entry) <= 3 * risk else default_tp
            tp = min(tp, entry + 3 * risk, entry * 1.30)
            if tp <= entry: tp = entry + 1.5 * risk
            if tp - entry < 1.5 * risk: tp = entry + 1.5 * risk
        return sl, tp

# =====================================================================
# PENDING VERIFICATION QUEUE (THREAD SAFE – unchanged)
# =====================================================================
class PendingVerificationQueue:
    def __init__(self, topology):
        self.topology = topology
        self.pending = {}
        self.lock = threading.Lock()

    def add_signal(self, signal_data):
        with self.lock:
            asset = signal_data['asset']
            completed = [c for c in self.topology.candles[300][asset] if c.get("complete", False)]
            if len(completed) < 2:
                return False
            signal_data['volumes'] = [completed[-1]["volume"]]
            signal_data['candle_count'] = 0
            signal_data['start_price'] = signal_data['entry']
            signal_data['rejected'] = False
            key = f"{asset}_{signal_data['direction']}_{int(time.time())}"
            self.pending[key] = signal_data
            return key

    def check_pending(self, asset):
        to_remove = []
        with self.lock:
            for key, data in list(self.pending.items()):
                if data['asset'] != asset:
                    continue
                completed = [c for c in self.topology.candles[300][asset] if c.get("complete", False)]
                if len(completed) < 2:
                    continue
                limit = data.get('pending_candles', Config.PENDING_VERIFICATION_CANDLES)
                vol_decay = data.get('volume_decay_threshold', Config.VOLUME_DECAY_THRESHOLD)
                new_candles = completed[-limit:] if len(completed) >= limit else completed
                if len(new_candles) > data['candle_count']:
                    for c in new_candles[data['candle_count']:]:
                        data['volumes'].append(c["volume"])
                        data['candle_count'] += 1
                    if len(data['volumes']) >= 2 and data['volumes'][-1] < data['volumes'][0] * (1 - vol_decay):
                        data['rejected'] = True
                        to_remove.append(key)
                        continue
                    first_close = completed[-limit]['close']
                    atr = self.topology.get_atr(asset, period=14, tf=300) or (data['start_price'] * 0.005)
                    max_allowed_adverse = 0.8 * atr

                    if data['direction'] == 'BUY' and (data['start_price'] - first_close) > max_allowed_adverse:
                        data['rejected'] = True
                        to_remove.append(key)
                    elif data['direction'] == 'SELL' and (first_close - data['start_price']) > max_allowed_adverse:
                        data['rejected'] = True
                        to_remove.append(key)

                if data['candle_count'] >= limit:
                    to_remove.append(key)
            for key in to_remove:
                if key in self.pending:
                    del self.pending[key]
            return to_remove

    def get_verified_signals(self):
        ready = []
        to_remove = []
        with self.lock:
            for key, data in list(self.pending.items()):
                limit = data.get('pending_candles', Config.PENDING_VERIFICATION_CANDLES)
                if data['candle_count'] >= limit and not data['rejected']:
                    ready.append(data)
                    to_remove.append(key)
                elif data['candle_count'] >= limit and data['rejected']:
                    to_remove.append(key)
            for key in to_remove:
                if key in self.pending:
                    del self.pending[key]
            return ready

# =====================================================================
# TRADE HEALTH ENGINE (unchanged)
# =====================================================================
class TradeHealthEngine:
    def __init__(self, topology, cache):
        self.topology = topology
        self.cache = cache

    def calculate_health(self, trade):
        try:
            asset = trade['asset']
            direction = trade['direction']
            entry = trade['entry']
            with self.topology.lock:
                current = self.topology.history[asset][-1]['price'] if self.topology.history.get(asset) else entry
                atr = self.topology.get_atr(asset)
            if atr == 0:
                return 100
            unrealized = (current - entry) if direction == "BUY" else (entry - current)
            drawdown_pct = -unrealized / (atr * 2) if unrealized < 0 else 0
            health_dd = max(0, 100 + drawdown_pct * 50)
            duration_min = (time.time() - trade.get('entry_time', time.time())) / 60
            if duration_min > Config.TRADE_HEALTH_STALE_MINUTES:
                health_time = max(0, 100 - (duration_min - Config.TRADE_HEALTH_STALE_MINUTES) * 2)
            else:
                health_time = 100
            ind1h = self.cache.get(asset, 3600, current)
            ema9 = ind1h.get('ema_9', current)
            ema21 = ind1h.get('ema_21', current)
            trend_bull = ema9 > ema21
            if direction == "BUY" and not trend_bull:
                health_trend = 70
            elif direction == "SELL" and trend_bull:
                health_trend = 70
            else:
                health_trend = 100
            vol_ratio = self.topology.volume_ma.get(asset, 0)
            if vol_ratio > 2.0:
                health_vol = 60
            elif vol_ratio > 1.5:
                health_vol = 80
            else:
                health_vol = 100
            health = health_dd * 0.4 + health_time * 0.3 + health_trend * 0.2 + health_vol * 0.1
            return max(0, min(100, int(health)))
        except Exception as e:
            logger.error(f"Health calc error: {e}")
            return 100

# =====================================================================
# MARKET ABSORPTION METER (unchanged)
# =====================================================================
class MarketAbsorptionMeter:
    def __init__(self, futures_stream, topology):
        self.futures = futures_stream
        self.topology = topology
        self.meter_state = {asset: {"score": 0, "direction": "", "last_update": 0} for asset in Config.ASSETS}
        self.lock = threading.Lock()

    def update_meter(self, asset, price):
        symbol = asset.lower()
        oi_trend = self.futures.get_oi_trend(symbol)
        cvd = self.futures.get_cvd(symbol)
        liqs = self.futures.get_liquidations(symbol, 120)

        score = 0
        direction = ""

        if oi_trend > 0 and cvd > 0:
            score += 40
            direction = "BUY"
        elif oi_trend < 0 and cvd < 0:
            score += 40
            direction = "SELL"
        else:
            if cvd > 0:
                direction = "BUY"; score += 20
            elif cvd < 0:
                direction = "SELL"; score += 20

        sell_liqs = sum(1 for l in liqs if l['side'] == 'SELL')
        buy_liqs = sum(1 for l in liqs if l['side'] == 'BUY')
        if direction == "BUY" and sell_liqs > 0:
            score += 20
        elif direction == "SELL" and buy_liqs > 0:
            score += 20

        anchor_ok, _ = self.topology.check_anchor_line_retest(asset, price)
        if anchor_ok:
            score += 20

        score = min(100, max(0, score))
        if score < 30:
            direction = ""

        with self.lock:
            self.meter_state[asset] = {"score": score, "direction": direction, "last_update": int(time.time())}
        return {"score": score, "direction": direction}

    def get_meter(self, asset):
        with self.lock:
            return self.meter_state.get(asset, {"score": 0, "direction": ""})

    def get_strong_opposite(self, asset, current_direction):
        meter = self.get_meter(asset)
        if meter["score"] >= Config.ABSORPTION_EXIT_SCORE:
            if current_direction == "BUY" and meter["direction"] == "SELL":
                return True
            if current_direction == "SELL" and meter["direction"] == "BUY":
                return True
        return False

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
            except Exception:
                pass

    def send_message(self, text):
        self.queue.put(text)

    def fire_signal(self, asset, direction, price, sl, tp, chart, logic, news,
                    score, patterns, trade_id, session, rr, regime, signal_type="SNIPER", signal_token=None):
        header = "🎯 <b>AI SNIPER REVERSAL (SELL 🔴)</b>" if direction == "SELL" else "🎯 <b>AI SNIPER REVERSAL (BUY 🟢)</b>"
        token_line = f"🆔 Token: {signal_token} (DB ID: #{trade_id})" if signal_token else f"🆔 DB ID: #{trade_id}"
        msg = (f"{header}\n━━━━━━━━━━━━━━━━━━━━━━━\n📊 {Config.DISPLAY_NAMES.get(asset, asset)} | {token_line}\n"
               f"⏰ {session} | ⚡ {score['confidence']} ({score['total_score']:.0f}%)\n"
               f"🎯 R:R {rr:.2f}\n💰 Entry: {price:.2f}  🛑 SL: {sl:.2f}  🎯 TP: {tp:.2f}\n"
               f"📈 Regime: {regime}  | Type: {signal_type}\n\n📊 CHART:\n{chart}\n"
               f"🧠 Logic: {logic}\n📰 News: {news}\n📊 Layers Passed: {score['num_passed']}/11\n━━━━━━━━━━━━━━━━━━━━━━━")
        self.queue.put(msg)

    def fire_trade_close(self, trade_id, asset, entry, exit_price, pnl, reason, entry_time):
        hold_min = (time.time() - entry_time) / 60
        pnl_pct = (pnl / entry) * 100 if entry else 0
        status = "🟢 PROFIT" if pnl > 0 else "🔴 LOSS" if pnl < 0 else "⚪ BREAKEVEN"
        msg = (f"<b>🔒 Trade #{trade_id} Closed – {Config.DISPLAY_NAMES.get(asset, asset)}</b>\n"
               f"Status: {status}\n💰 PnL: {pnl:+.2f} ({pnl_pct:+.2f}%)\n"
               f"🕐 Held for: {int(hold_min)} min\n📌 Exit Reason: {reason}")
        self.queue.put(msg)

    def fire_news_alert(self, title, sentiment, fg):
        self.queue.put(f"📰 {html.escape(title)}\n🧠 Sentiment: {sentiment:.0f} | Fear/Greed: {fg}")

# =====================================================================
# ACTIVE TRADE LIFECYCLE (unchanged)
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
                    with self.orch.topology.lock:
                        current_price = self.orch.topology.history[asset][-1]['price'] if self.orch.topology.history.get(asset) else trade['entry']
                    trade_duration = now - trade.get('entry_time', now)

                    health = self.orch.health_engine.calculate_health(trade)
                    trade['health'] = health

                    if self.orch.absorption_meter.get_strong_opposite(asset, trade['direction']):
                        pnl = (current_price - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - current_price)
                        self.orch._close_trade(tid, current_price, pnl, "Reversal-Absorption")
                        to_remove.append(tid)
                        continue

                    if health < Config.HEALTH_EMERGENCY_THRESHOLD:
                        pnl = (current_price - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - current_price)
                        self.orch._close_trade(tid, current_price, pnl, f"EmergencyHealth-{health}%")
                        to_remove.append(tid)
                        continue

                    if trade_duration > Config.MAX_HOLD_TIME:
                        pnl = (current_price - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - current_price)
                        self.orch._close_trade(tid, current_price, pnl, "MaxHold")
                        to_remove.append(tid)
                        continue

                    if trade_duration > Config.TIME_DECAY_SECONDS and abs(current_price - trade['entry']) / trade['entry'] < Config.TIME_DECAY_THRESHOLD_PCT:
                        pnl = (current_price - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - current_price)
                        self.orch._close_trade(tid, current_price, pnl, "TimeDecay-60m")
                        to_remove.append(tid)
                        continue

                    if not trade.get('breakeven_locked', False):
                        target = abs(trade['tp'] - trade['entry'])
                        half = trade['entry'] + 0.5*target if trade['direction'] == 'BUY' else trade['entry'] - 0.5*target
                        if (trade['direction'] == 'BUY' and current_price >= half) or (trade['direction'] == 'SELL' and current_price <= half):
                            if self.orch.topology.check_1m_rejection(asset, trade['direction']):
                                trade['sl'] = trade['entry']
                                trade['breakeven_locked'] = True
                                self.orch.mongo.update_trade_sl(tid, trade['entry'])

                    if not trade.get('trailing_activated', False):
                        target = abs(trade['tp'] - trade['entry'])
                        trigger = trade['entry'] + 0.7*target if trade['direction'] == 'BUY' else trade['entry'] - 0.7*target
                        if (trade['direction'] == 'BUY' and current_price >= trigger) or (trade['direction'] == 'SELL' and current_price <= trigger):
                            new_sl = trade['entry'] + 0.3*target if trade['direction'] == 'BUY' else trade['entry'] - 0.3*target
                            if (trade['direction'] == 'BUY' and new_sl > trade['sl']) or (trade['direction'] == 'SELL' and new_sl < trade['sl']):
                                trade['sl'] = new_sl
                                trade['trailing_activated'] = True
                                self.orch.mongo.update_trade_sl(tid, new_sl)

                for tid in to_remove:
                    if tid in self.orch.active_trades:
                        del self.orch.active_trades[tid]
                gc.collect()

# =====================================================================
# CORE ORCHESTRATOR (with Neutral Candle Engine)
# =====================================================================
class AIOrchestrator:
    def __init__(self):
        self.topology = CandleTopologyEngine()
        self.cache = IndicatorCache(self.topology)
        self.news = CryptoNewsScanner()
        self.db = TradeDatabase()
        self.mongo = MongoDatabase()
        self.memory = PersistentMemoryEngine(self.mongo.db)
        self.telegram = TelegramPipeline()

        self.futures_stream = BinanceFuturesStream()
        self.futures_stream.start()

        self.absorption_meter = MarketAbsorptionMeter(self.futures_stream, self.topology)
        self.sniper_engine = RallyExhaustionFilter(self.topology, self.absorption_meter)

        # New Neutral Candle Engine
        self.neutral_engine = NeutralCandleEngine(self.topology)

        self.regime_detector = RegimeDetector(self.topology)
        self.market_regime = MarketRegimeFilter(self.topology)
        self.mtf_gate = MTFConfluenceGate(self.topology)
        self.orderflow = OrderFlowAnalyzer(self.topology, self.futures_stream)
        self.session_timer = SessionTimer()
        self.intelligence_engine = UnifiedIntelligenceEngine(self.topology, self.futures_stream, self.absorption_meter, self.neutral_engine)
        self.pending_queue = PendingVerificationQueue(self.topology)
        self.dynamic_sl = DynamicStopLoss(self.topology)
        self.health_engine = TradeHealthEngine(self.topology, self.cache)

        self.active_trades = {}
        self.trade_lock = threading.Lock()
        self.price_queue = queue.Queue(maxsize=1000)
        self.start_time = time.time()
        self.last_signal_time = {a: 0 for a in Config.ASSETS}
        self.signal_timestamps = deque(maxlen=100)
        self.asset_state = {a: {"trend": "NEUTRAL", "htf_trend": "NEUTRAL", "volume_ratio": 1.0,
                                "rsi": 50, "adx": 20, "volatility": 0.01,
                                "news_sentiment": 0, "news_importance": 0.5,
                                "neutral_pattern": "None"} for a in Config.ASSETS}
        self.accepted = 0
        self.rejected = 0
        self.stream = None
        self._price_counter = 0

        self._restore_state_from_db()

        self.lifecycle = ActiveTradeLifecycle(self)
        threading.Thread(target=self.lifecycle.monitor_lifecycle, daemon=True).start()
        threading.Thread(target=self._process_queue, daemon=True).start()
        threading.Thread(target=self._memory_sync_loop, daemon=True).start()
        threading.Thread(target=self._meter_update_loop, daemon=True).start()
        threading.Thread(target=self._status_monitor, daemon=True).start()

    def _restore_state_from_db(self):
        open_trades = self.mongo.get_open_trades()
        with self.trade_lock:
            for t in open_trades:
                tid = t.get('id')
                if not tid: continue
                self.active_trades[tid] = {
                    'id': tid,
                    'asset': t.get('asset',''),
                    'direction': t.get('direction',''),
                    'entry': t.get('entry',0),
                    'sl': t.get('stop_loss',0),
                    'tp': t.get('take_profit',0),
                    'entry_time': t.get('entry_time', int(time.time())),
                    'breakeven_locked': False,
                    'trailing_activated': False,
                    'health': 100,
                    'regime': t.get('regime',''),
                    'signal_token': t.get('signal_token')
                }
        timestamps = self.db.get_recent_signal_timestamps(86400)
        self.signal_timestamps = deque(timestamps[-100:], maxlen=100)
        logger.info(f"Restored {len(open_trades)} open trades, {len(timestamps)} recent signals")

    def _memory_sync_loop(self):
        last_sync = 0
        while True:
            time.sleep(60)
            now = int(time.time())
            if now - last_sync >= 300:
                self.memory.update_state({"total_run_seconds": int(time.time() - self.start_time), "last_update": now})
                last_sync = now

    def _meter_update_loop(self):
        while True:
            try:
                for asset in Config.ASSETS:
                    with self.topology.lock:
                        price = self.topology.history[asset][-1]['price'] if self.topology.history.get(asset) else 0
                    if price:
                        self.absorption_meter.update_meter(asset, price)
                time.sleep(30)
            except Exception as e:
                logger.error(f"Meter update error: {e}")

    def _status_monitor(self):
        while True:
            time.sleep(60)
            last_tick = self.stream.last_tick_time if hasattr(self.stream, 'last_tick_time') else 0
            age = time.time() - last_tick
            qsize = self.price_queue.qsize()
            logger.info(f"⏱️ STATUS: last tick {age:.0f}s ago, queue size {qsize}, active trades {len(self.active_trades)}")

    def _check_all_gates(self, asset, price, direction, sniper_score, sniper_reason):
        if not hasattr(self, 'intelligence_engine'):
            self.intelligence_engine = UnifiedIntelligenceEngine(
                self.topology, self.futures_stream, self.absorption_meter, self.neutral_engine
            )

        is_valid, total_score, reason = self.intelligence_engine.evaluate_setup(asset, price, direction)
        if not is_valid:
            return False, f"Score {total_score} < 65 ({reason})"
        return True, f"Passed [{total_score}/100]: {reason}"

    def _handle_price_tick(self, asset, price, volume):
        with self.trade_lock:
            if any(t['asset'] == asset for t in self.active_trades.values()):
                return

        self.topology.process_tick(asset, price, volume)
        self._update_active_trades(asset, price)

        # Update neutral pattern in asset state (every tick but we only care on candle close)
        if self.topology.candle_just_closed.get(asset, False):
            neutral = self.neutral_engine.detect(asset, price, tf=900)
            self.asset_state[asset]['neutral_pattern'] = neutral['pattern']

        if self.topology.candle_just_closed.get(asset, False):
            if self.pending_queue.pending:
                self.pending_queue.check_pending(asset)
                verified = self.pending_queue.get_verified_signals()
                for signal in verified:
                    self._send_final_signal(signal)

            sniper_signal, err = self.sniper_engine.evaluate(asset, price)
            if not sniper_signal:
                return

            direction = sniper_signal["direction"]
            sniper_score = sniper_signal["score"]
            sniper_reason = sniper_signal["reason"]

            gates_ok, gate_reason = self._check_all_gates(asset, price, direction, sniper_score, sniper_reason)
            if not gates_ok:
                self.db.log_rejected(asset, price, sniper_score, gate_reason,
                                     self.asset_state[asset]["volatility"], "SNIPER", "GateFail", "SNIPER")
                self.rejected += 1
                self.memory.update_state({"rejected_signals_count": 1})
                return

            now_ts = time.time()
            if now_ts - self.last_signal_time[asset] < Config.SIGNAL_COOLDOWN:
                self.db.log_rejected(asset, price, sniper_score, "Cooldown",
                                     self.asset_state[asset]["volatility"], "SNIPER", "Cooldown", "SNIPER")
                self.rejected += 1
                self.memory.update_state({"rejected_signals_count": 1})
                return
            if len([t for t in self.signal_timestamps if now_ts - t < 86400]) >= Config.MAX_SIGNALS_PER_DAY:
                self.db.log_rejected(asset, price, sniper_score, "DailyCap",
                                     self.asset_state[asset]["volatility"], "SNIPER", "DailyCap", "SNIPER")
                self.rejected += 1
                self.memory.update_state({"rejected_signals_count": 1})
                return

            atr = self.topology.get_atr(asset) or price*0.01
            sl, tp = self.dynamic_sl.calculate(asset, direction, price, atr)
            risk = abs(price - sl)
            if risk == 0:
                return
            rr = abs(tp - price) / risk
            if rr < 2.5:
                if direction == "BUY":
                    tp = price + 2.5 * risk
                else:
                    tp = price - 2.5 * risk

            trade_id = self.db.generate_trade_id()
            token = f"SNPR-{asset}-{int(time.time()*1000)}"
            signal = {
                'asset': asset,
                'direction': direction,
                'entry': price,
                'sl': sl,
                'tp': tp,
                'sqs': sniper_score,
                'session': "ALWAYS",
                'patterns': {},
                'logic': f"SNIPER: {sniper_reason}",
                'news': self.news.last_news.get('title','')[:100],
                'volatility': self.asset_state[asset]["volatility"],
                'regime': "SNIPER",
                'htf_trend': self.asset_state[asset]["htf_trend"],
                'news_score': self.asset_state[asset]["news_sentiment"],
                'score': 0,
                'confidence': 'HIGH',
                'num_passed': 11,
                'pending_candles': Config.PENDING_VERIFICATION_CANDLES,
                'volume_decay_threshold': Config.VOLUME_DECAY_THRESHOLD,
                'dynamic_min_sqs': Config.MIN_SQS,
                'signal_type': 'SNIPER',
                'signal_token': token,
                'trade_id': trade_id
            }
            self.pending_queue.add_signal(signal)
            self.memory.update_state({"total_signals_generated": 1})
            logger.info(f"⏳ Pending: {asset} {direction} @ {price} SQS:{sniper_score}")

    def _send_final_signal(self, signal):
        try:
            asset = signal['asset']
            direction = signal['direction']
            price = signal['entry']
            sl, tp = signal['sl'], signal['tp']
            sqs = signal['sqs']
            session = signal['session']
            patterns = signal['patterns']
            logic = signal['logic']
            news = signal['news']
            volatility = signal['volatility']
            regime = signal['regime']
            htf_trend = signal['htf_trend']
            news_score = signal['news_score']
            dm = signal.get('dynamic_min_sqs', Config.MIN_SQS)
            st = signal.get('signal_type', 'SNIPER')
            token = signal.get('signal_token')
            trade_id = signal.get('trade_id') or self.db.generate_trade_id()
            pattern_name = list(patterns.keys())[0] if patterns else "unknown"

            self.db.log_trade(trade_id, asset, direction, price, sl, tp, sqs, "HIGH", list(patterns.keys()), logic,
                              volatility, regime, htf_trend, news_score, session, sqs, pattern_name, dm, st, token)
            if self.mongo.db is not None:
                self.mongo.save_trade_backup({
                    'id': trade_id, 'asset': asset, 'direction': direction,
                    'entry': price, 'stop_loss': sl, 'take_profit': tp,
                    'score': sqs, 'status': 'open', 'signal_type': st,
                    'signal_token': token, 'entry_time': int(time.time())
                })
            chart = self.topology.get_visual_topology(asset, price, direction, sl, tp, patterns)
            rr = abs(tp - price) / abs(price - sl) if abs(price - sl) > 0 else 0
            self.telegram.fire_signal(asset=asset, direction=direction, price=price, sl=sl, tp=tp,
                                      chart=chart, logic=logic, news=news,
                                      score={"total_score": sqs, "confidence": "HIGH", "num_passed": 11},
                                      patterns=patterns, trade_id=trade_id,
                                      session=session, rr=rr, regime=regime, signal_type=st, signal_token=token)
            self.accepted += 1
            self.last_signal_time[asset] = time.time()
            self.signal_timestamps.append(time.time())
            with self.trade_lock:
                self.active_trades[trade_id] = {
                    'id': trade_id, 'asset': asset, 'direction': direction,
                    'entry': price, 'sl': sl, 'tp': tp, 'entry_time': int(time.time()),
                    'breakeven_locked': False, 'trailing_activated': False,
                    'health': 100, 'regime': regime, 'signal_token': token
                }
            self.memory.update_state({"accepted_signals_count": 1})
        except Exception as e:
            logger.error(f"Error in _send_final_signal: {e}", exc_info=True)

    def _update_active_trades(self, asset, price):
        with self.trade_lock:
            to_remove = []
            for tid, trade in list(self.active_trades.items()):
                if trade['asset'] != asset:
                    continue
                if not trade.get('breakeven_locked', False):
                    target = abs(trade['tp'] - trade['entry'])
                    half = trade['entry'] + 0.5*target if trade['direction'] == 'BUY' else trade['entry'] - 0.5*target
                    if (trade['direction'] == 'BUY' and price >= half) or (trade['direction'] == 'SELL' and price <= half):
                        if self.topology.check_1m_rejection(asset, trade['direction']):
                            trade['sl'] = trade['entry']
                            trade['breakeven_locked'] = True
                            self.mongo.update_trade_sl(tid, trade['entry'])
                if not trade.get('trailing_activated', False):
                    target = abs(trade['tp'] - trade['entry'])
                    trigger = trade['entry'] + 0.7*target if trade['direction'] == 'BUY' else trade['entry'] - 0.7*target
                    if (trade['direction'] == 'BUY' and price >= trigger) or (trade['direction'] == 'SELL' and price <= trigger):
                        new_sl = trade['entry'] + 0.3*target if trade['direction'] == 'BUY' else trade['entry'] - 0.3*target
                        if (trade['direction'] == 'BUY' and new_sl > trade['sl']) or (trade['direction'] == 'SELL' and new_sl < trade['sl']):
                            trade['sl'] = new_sl
                            trade['trailing_activated'] = True
                            self.mongo.update_trade_sl(tid, new_sl)
                if trade['direction'] == 'BUY':
                    if price <= trade['sl']:
                        pnl = price - trade['entry']
                        self._close_trade(tid, price, pnl, "SL Hit")
                        to_remove.append(tid)
                    elif price >= trade['tp']:
                        pnl = price - trade['entry']
                        self._close_trade(tid, price, pnl, "TP Hit")
                        to_remove.append(tid)
                else:
                    if price >= trade['sl']:
                        pnl = trade['entry'] - price
                        self._close_trade(tid, price, pnl, "SL Hit")
                        to_remove.append(tid)
                    elif price <= trade['tp']:
                        pnl = trade['entry'] - price
                        self._close_trade(tid, price, pnl, "TP Hit")
                        to_remove.append(tid)
            for tid in to_remove:
                if tid in self.active_trades:
                    del self.active_trades[tid]

    def _close_trade(self, tid, price, pnl, reason=""):
        trade = self.active_trades.get(tid)
        entry_time = trade['entry_time'] if trade else int(time.time())
        entry = trade['entry'] if trade else 0
        asset = trade['asset'] if trade else ''
        self.db.close_trade(tid, price, pnl, reason)
        self.mongo.close_trade_mongo(tid, price, pnl, reason)
        self.telegram.fire_trade_close(tid, asset, entry, price, pnl, reason, entry_time)
        logger.info(f"Trade {tid} closed. PnL: {pnl:.2f}, Reason: {reason}")
        if trade:
            updates = {"total_trades_closed": 1, "total_pnl": pnl}
            if pnl > 0:
                updates["total_wins"] = 1
            else:
                updates["total_losses"] = 1
            self.memory.update_state(updates)
        if tid in self.active_trades:
            del self.active_trades[tid]

    def _update_indicators(self, asset, price):
        try:
            with self.topology.lock:
                c15 = [c["close"] for c in self.topology.candles[900][asset] if c.get("complete", False)][-30:]
                if len(c15) > 10:
                    e9, e21 = self.topology._ema(c15, 9), self.topology._ema(c15, 21)
                    if len(e9) > 1 and len(e21) > 1:
                        self.asset_state[asset]["trend"] = "BULLISH" if e9[-1] > e21[-1] else "BEARISH"
                    if len(c15) >= 14:
                        self.asset_state[asset]["rsi"] = self.topology._calc_rsi(c15)
                        self.asset_state[asset]["adx"] = self.topology.get_adx(asset, 900)
                c1h = [c["close"] for c in self.topology.candles[3600][asset] if c.get("complete", False)][-30:]
                if len(c1h) > 10:
                    e9, e21 = self.topology._ema(c1h, 9), self.topology._ema(c1h, 21)
                    if len(e9) > 1 and len(e21) > 1:
                        self.asset_state[asset]["htf_trend"] = "BULLISH" if e9[-1] > e21[-1] else "BEARISH"
                vols = [c["volume"] for c in self.topology.candles[300][asset] if c.get("complete", False)][-10:]
                if len(vols) > 1:
                    avg = sum(vols[:-1]) / max(1, len(vols[:-1]))
                    self.asset_state[asset]["volume_ratio"] = vols[-1] / avg if avg > 0 else 1.0
                atr = self.topology.get_atr(asset)
                if atr:
                    self.asset_state[asset]["volatility"] = atr / price
        except Exception as e:
            logger.error(f"Indicator update error: {e}")

    def _is_strong_trend(self, asset):
        with self.topology.lock:
            c15 = [c["close"] for c in self.topology.candles[900][asset] if c.get("complete", False)][-30:]
            c1h = [c["close"] for c in self.topology.candles[3600][asset] if c.get("complete", False)][-30:]
            if len(c15) < 20 or len(c1h) < 20:
                return False
            e15_9, e15_21 = self.topology._ema(c15, 9), self.topology._ema(c15, 21)
            e1h_9, e1h_21 = self.topology._ema(c1h, 9), self.topology._ema(c1h, 21)
            return (e15_9[-1] > e15_21[-1] and e1h_9[-1] > e1h_21[-1])

    def _process_queue(self):
        while True:
            try:
                item = self.price_queue.get(timeout=1)
                if item:
                    self._handle_price_tick(*item)
            except Exception:
                pass

    def run(self):
        threading.Thread(target=start_health_server, args=(self,), daemon=True).start()
        threading.Thread(target=self._ping_self_loop, daemon=True).start()
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(self._load_and_backfill, asset, tf)
                       for asset in Config.ASSETS for tf in [60, 300, 900, 3600, 14400]]
            for _ in as_completed(futures):
                pass
        self.stream = BinancePublicStream(self._on_price)
        self.stream.start()
        self.telegram.send_message("🚀 AlphaBot v7.5 – Neutral Candle Engine Active")
        last_news = 0
        while True:
            time.sleep(10)
            if time.time() - last_news > 60:
                news = self.news.fetch_latest()
                if news.get("fresh"):
                    for a in Config.ASSETS:
                        self.asset_state[a]["news_sentiment"] = news["articles"][0]["sentiment"] if news["articles"] else 0
                    if news["articles"]:
                        self.telegram.fire_news_alert(news["articles"][0]["title"],
                                                      news["articles"][0]["sentiment"],
                                                      news.get("fear_greed", 50))
                    last_news = time.time()

    def _load_and_backfill(self, asset, tf):
        candles = self.mongo.load_candles(asset, tf)
        if len(candles) >= 100:
            with self.topology.lock:
                self.topology.candles[tf][asset] = candles
            logger.info(f"📂 Loaded {len(candles)} cached candles for {asset} [{tf}s] from MongoDB")
            return

        interval = {60: "1m", 300: "5m", 900: "15m", 3600: "1h", 14400: "4h"}[tf]
        try:
            resp = requests.get("https://api.binance.com/api/v3/klines",
                                params={"symbol": asset, "interval": interval, "limit": 1000}, timeout=15)
            if resp.status_code == 200:
                fetched = []
                for d in resp.json():
                    c = {"timestamp": d[0] // 1000, "open": float(d[1]), "high": float(d[2]),
                         "low": float(d[3]), "close": float(d[4]), "volume": float(d[5]), "complete": True}
                    if DataValidator.validate_candle(c):
                        fetched.append(c)
                        self.mongo.save_candle(asset, tf, c)
                with self.topology.lock:
                    self.topology.candles[tf][asset] = fetched
                logger.info(f"🌐 Backfilled {len(fetched)} candles for {asset} [{interval}] from Binance API")
        except Exception as e:
            logger.error(f"Backfill error for {asset} {tf}: {e}")

    def _on_price(self, asset, price, volume):
        if DataValidator.validate_price(price) and DataValidator.validate_volume(volume):
            try:
                self.price_queue.put_nowait((asset, price, volume))
                self._price_counter += 1
                if self._price_counter % 100 == 0:
                    logger.info(f"📈 Price tick #{self._price_counter}: {asset} @ {price}")
            except queue.Full:
                logger.warning("Price queue full!")

    def _ping_self_loop(self):
        while True:
            try:
                requests.get(Config.RENDER_URL, timeout=10)
            except Exception:
                pass
            time.sleep(300)

# =====================================================================
# HEALTH SERVER (Updated Dashboard with Neutral Pattern)
# =====================================================================
def start_health_server(orchestrator):
    port = int(os.environ.get("PORT", 10000))

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.startswith('/close_trade'):
                try:
                    query = urllib.parse.urlparse(self.path).query
                    params = urllib.parse.parse_qs(query)
                    tid = int(params.get('id', [0])[0])
                    with orchestrator.trade_lock:
                        if tid and tid in orchestrator.active_trades:
                            trade = orchestrator.active_trades[tid]
                            asset = trade['asset']
                            with orchestrator.topology.lock:
                                curr = orchestrator.topology.history[asset][-1]['price'] if orchestrator.topology.history.get(asset) else trade['entry']
                            pnl = (curr - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - curr)
                            orchestrator._close_trade(tid, curr, pnl, "ManualDashboardReject")
                except Exception as e:
                    logger.error(f"Manual close error: {e}")
                self.send_response(302)
                self.send_header('Location', '/')
                self.end_headers()
                return

            if self.path == '/rejections':
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                try:
                    conn = orchestrator.db._get_conn()
                    cur = conn.cursor()
                    cur.execute("SELECT datetime(timestamp, 'unixepoch'), asset, price, reason, gate_name, regime FROM rejected_signals ORDER BY timestamp DESC LIMIT 50")
                    rows = cur.fetchall()
                    data = [{"time": r[0], "asset": r[1], "price": r[2], "reason": r[3], "gate": r[4], "regime": r[5]} for r in rows]
                    self.wfile.write(json.dumps(data, indent=2).encode())
                except Exception as e:
                    self.wfile.write(json.dumps({"error": str(e)}).encode())
                return

            if self.path == '/' or self.path == '/health':
                self.send_response(200)
                self.send_header("Content-type", "text/html; charset=utf-8")
                self.end_headers()
                mem = orchestrator.memory.get_or_create_state()
                first_launch = mem.get("first_launch_timestamp", int(time.time()))
                watch_sec = int(time.time() - first_launch)
                d = watch_sec // 86400
                h = (watch_sec % 86400) // 3600
                m = (watch_sec % 3600) // 60
                age_str = f"{d}d {h}h {m}m"
                perf = orchestrator.db.get_performance_metrics()
                active_list = []
                with orchestrator.trade_lock:
                    for tid, trade in orchestrator.active_trades.items():
                        with orchestrator.topology.lock:
                            curr = orchestrator.topology.history[trade['asset']][-1]['price'] if orchestrator.topology.history.get(trade['asset']) else trade['entry']
                        pnl = (curr - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - curr)
                        active_list.append({"id": tid, "asset": trade['asset'], "dir": trade['direction'],
                                            "entry": trade['entry'], "pnl": round(pnl, 2),
                                            "health": trade.get('health', 100)})

                # Build neutral pattern display
                neutral_html = ""
                for asset in Config.ASSETS:
                    pat = orchestrator.asset_state.get(asset, {}).get('neutral_pattern', 'None')
                    neutral_html += f"<span style='margin:0 15px;'><b>{asset}</b>: {pat}</span>"

                html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><title>AlphaBot v7.5 Dashboard</title>
<meta http-equiv="refresh" content="10"><style>body{{font-family:Arial;background:#111;color:#eee;margin:20px}} table{{border-collapse:collapse;width:100%}} th,td{{border:1px solid #444;padding:6px;text-align:center}} th{{background:#333}} .g{{color:#0f0}} .r{{color:#f00}} .btn{{background:#d9534f;color:#fff;padding:3px 8px;text-decoration:none;border-radius:3px;font-size:12px;font-weight:bold}}</style></head><body>
<h1>🚀 AlphaBot v7.5 – Neutral Candle Active</h1>
<p>🟢 <b>Bot Status: Online</b> | ⏱️ Market Watching Age: {age_str}</p>
<p>🌼 <b>Current Neutral Patterns:</b> {neutral_html}</p>
<h2>All-Time Counters</h2>
<p>📊 Accepted Signals: {mem.get("accepted_signals_count", 0)} | ❌ Rejected: {mem.get("rejected_signals_count", 0)}</p>
<p>💰 Closed Trades: {mem.get("total_trades_closed", 0)} | Wins: {mem.get("total_wins", 0)} | Losses: {mem.get("total_losses", 0)}</p>
<p>Win Rate: {perf.get('win_rate', 0):.2%} | Total PnL: ${mem.get('total_pnl', 0.0):.2f}</p>
<h2>Active Trades</h2><table><tr><th>ID</th><th>Asset</th><th>Dir</th><th>Entry</th><th>PnL</th><th>Health</th><th>Action</th></tr>"""
                for t in active_list:
                    cls = "g" if t["pnl"] >= 0 else "r"
                    btn = f"<a href='/close_trade?id={t['id']}' class='btn'>❌ Reject / Close</a>"
                    html += f"<tr><td>{t['id']}</td><td>{t['asset']}</td><td>{t['dir']}</td><td>{t['entry']:.2f}</td><td class='{cls}'>{t['pnl']:.2f}</td><td>{t['health']}%</td><td>{btn}</td></tr>"
                html += "</table></body></html>"
                self.wfile.write(html.encode())
            else:
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "online", "version": "7.5"}).encode())

        def do_HEAD(self):
            self.send_response(200)
            self.end_headers()

    httpd = HTTPServer(("0.0.0.0", port), H)
    logger.info(f"Health server on port {port}")
    httpd.serve_forever()

if __name__ == "__main__":
    bot = AIOrchestrator()
    bot.run()

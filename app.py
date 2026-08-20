# =====================================================================
# app.py – AlphaBot v7.6 FINAL (All 4 Critical Fixes)
# =====================================================================
# 4-Layer Simple Engine with full observation logging
# EMA window: 150 candles, Pending → Verified → Final Signal
# 5m verification triggered on 5m candle close
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
logger = logging.getLogger("AlphaBot-v7.6-Final")

class Config:
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    DISPLAY_NAMES = {"BTCUSDT": "BTC/USDT", "ETHUSDT": "ETH/USDT", "SOLUSDT": "SOL/USDT"}

    MONGO_URI = os.getenv("MONGO_URI", "")
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "crypto_bot_v7")
    RENDER_URL = os.getenv("RENDER_URL", "https://alphabot-76tj.onrender.com")
    DB_PATH = "trades_v7.db"
    MAX_CANDLES = 500
    BINANCE_FUTURES_WS_URL = "wss://fstream.binance.com/ws"

    IST = pytz.timezone('Asia/Kolkata')
    SESSION_WINDOWS = [("ALWAYS", 0, 0, 23, 59)]
    DEAD_ZONES = []

    SCORE_REJECT = 45
    SCORE_WATCH = 45
    SCORE_VALID = 50
    SCORE_HIGH = 60

    MIN_SCORE_FOR_SIGNAL = 50
    MIN_RR = 2.0
    MAX_SIGNALS_PER_DAY = 4
    ASSET_COOLDOWN_HOURS = 4

    PENDING_VERIFICATION_CANDLES = 1  # 1 candle = 5m
    VOLUME_DECAY_THRESHOLD = 0.5
    MAX_HOLD_TIME = 14400
    TIME_DECAY_SECONDS = 3600
    TIME_DECAY_THRESHOLD_PCT = 0.002
    HEALTH_EMERGENCY_THRESHOLD = 55
    TRADE_HEALTH_STALE_MINUTES = 25
    WS_HEALTH_CHECK_TIMEOUT = 1800

    ANCHOR_RETEST_TOLERANCE = 0.004
    ABSORPTION_MIN_SCORE = 35
    ABSORPTION_EXIT_SCORE = 80
    REST_FALLBACK_INTERVAL = 30

# =====================================================================
# DATA VALIDATION (unchanged)
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
# DATABASE LAYERS (MongoDB + SQLite) – same as app(14).py
# =====================================================================
class MongoDatabase:
    def __init__(self):
        if not HAS_PYMONGO or not Config.MONGO_URI:
            self.client = None
            self.db = None
            logger.warning("MongoDB not available.")
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
            self.db.observations.create_index([("asset", 1), ("timestamp", -1)])
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
                fetch_limit = 3000
            elif timeframe == 900:
                fetch_limit = 5000
            else:
                fetch_limit = 1000
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

    def save_observation(self, obs):
        if self.db is None: return
        try:
            self.db.observations.insert_one(obs)
        except Exception:
            pass

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
                signal_token TEXT,
                score_breakdown TEXT
            )''')
            cur.execute('''CREATE TABLE IF NOT EXISTS rejected_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset TEXT, price REAL, score INTEGER, reason TEXT,
                timestamp INTEGER, volatility REAL, market_regime TEXT,
                gate_name TEXT, regime TEXT,
                score_breakdown TEXT,
                status TEXT,
                direction TEXT,
                candidate_timestamp INTEGER
            )''')
            try:
                cur.execute("ALTER TABLE rejected_signals ADD COLUMN status TEXT")
                cur.execute("ALTER TABLE rejected_signals ADD COLUMN direction TEXT")
                cur.execute("ALTER TABLE rejected_signals ADD COLUMN candidate_timestamp INTEGER")
            except sqlite3.OperationalError:
                pass
            cur.execute('''CREATE TABLE IF NOT EXISTS signal_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset TEXT,
                timestamp INTEGER,
                direction TEXT,
                score INTEGER,
                status TEXT,
                reason TEXT,
                breakdown TEXT,
                price REAL,
                volatility REAL
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
            cur.execute('''CREATE TABLE IF NOT EXISTS global_bot_memory (
                id INTEGER PRIMARY KEY CHECK (id=1),
                first_launch_timestamp INTEGER,
                total_run_seconds INTEGER,
                total_signals_generated INTEGER,
                accepted_signals_count INTEGER,
                rejected_signals_count INTEGER,
                total_trades_closed INTEGER,
                total_wins INTEGER,
                total_losses INTEGER,
                total_pnl REAL,
                restart_count INTEGER DEFAULT 0,
                last_update INTEGER
            )''')
            try:
                cur.execute("ALTER TABLE global_bot_memory ADD COLUMN restart_count INTEGER DEFAULT 0")
            except Exception:
                pass
            cur.execute("INSERT OR IGNORE INTO global_bot_memory (id, first_launch_timestamp, total_run_seconds, total_signals_generated, accepted_signals_count, rejected_signals_count, total_trades_closed, total_wins, total_losses, total_pnl, restart_count, last_update) VALUES (1, strftime('%s','now'), 0, 0, 0, 0, 0, 0, 0, 0.0, 0, strftime('%s','now'))")
            conn.commit()
        finally:
            cur.close()

    def get_memory_state(self):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT * FROM global_bot_memory WHERE id=1")
            row = cur.fetchone()
            if row:
                return dict(row)
            return None
        except Exception:
            return None

    def update_memory_state(self, updates: dict):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            set_clause = ", ".join([f"{k}=?" for k in updates.keys()])
            values = list(updates.values())
            cur.execute(f"UPDATE global_bot_memory SET {set_clause} WHERE id=1", values)
            conn.commit()
        except Exception:
            pass

    def generate_trade_id(self):
        return int(time.time() * 1000)

    def log_trade(self, trade_id, asset, direction, entry, sl, tp, score, confidence, patterns, logic,
                  volatility, regime, htf_trend, news_score, session, sqs_score, pattern_name,
                  dynamic_min_sqs, signal_type="STANDARD", signal_token=None, score_breakdown=""):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute('''INSERT INTO trades 
                (id, asset, direction, entry, stop_loss, take_profit, score, confidence, patterns, logic,
                 timestamp, volatility, market_regime, htf_trend, news_score, entry_time, status,
                 session, sqs_score, pattern_name, regime, dynamic_min_sqs, signal_type, signal_token, score_breakdown)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (trade_id, asset, direction, entry, sl, tp, score, confidence, json.dumps(patterns), logic,
                 int(time.time()), volatility, regime, htf_trend, news_score, int(time.time()), 'open',
                 session, sqs_score, pattern_name, regime, dynamic_min_sqs, signal_type, signal_token, score_breakdown))
            conn.commit()
            return trade_id
        finally:
            cur.close()

    def log_rejected(self, asset, price, score, reason, volatility, regime, gate_name="", dynamic_regime="",
                     score_breakdown="", status="REJECTED", direction="", candidate_ts=None):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute('''INSERT INTO rejected_signals 
                (asset, price, score, reason, timestamp, volatility, market_regime, gate_name, regime, score_breakdown, status, direction, candidate_timestamp)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (asset, price, score, reason, int(time.time()), volatility, regime, gate_name, dynamic_regime,
                 score_breakdown, status, direction, candidate_ts or int(time.time())))
            conn.commit()
        finally:
            cur.close()

    def log_observation(self, asset, price, direction, score, status, reason, breakdown, volatility):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute('''INSERT INTO signal_observations 
                (asset, timestamp, direction, score, status, reason, breakdown, price, volatility)
                VALUES (?,?,?,?,?,?,?,?,?)''',
                (asset, int(time.time()), direction, score, status, reason, json.dumps(breakdown), price, volatility))
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

    def get_recent_observations(self, limit=50):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT asset, datetime(timestamp, 'unixepoch') as time, direction, score, status, reason FROM signal_observations ORDER BY timestamp DESC LIMIT ?", (limit,))
            return cur.fetchall()
        except Exception:
            return []

# =====================================================================
# PERSISTENT MEMORY (unchanged)
# =====================================================================
class PersistentMemoryEngine:
    def __init__(self, mongo_db, sqlite_db):
        self.mongo = mongo_db
        self.sqlite = sqlite_db
        self.use_mongo = mongo_db.db is not None
        self.cache = None
        self.lock = threading.Lock()
        self._load()

    def _load(self):
        with self.lock:
            if self.use_mongo:
                try:
                    doc = self.mongo.db.global_bot_memory.find_one({"_id": "global_state"})
                    if doc:
                        self.cache = doc
                        logger.info("Memory loaded from MongoDB.")
                        return
                except Exception:
                    pass
            state = self.sqlite.get_memory_state()
            if state:
                self.cache = {
                    "_id": "global_state",
                    "first_launch_timestamp": state["first_launch_timestamp"],
                    "total_run_seconds": state["total_run_seconds"],
                    "total_signals_generated": state["total_signals_generated"],
                    "accepted_signals_count": state["accepted_signals_count"],
                    "rejected_signals_count": state["rejected_signals_count"],
                    "total_trades_closed": state["total_trades_closed"],
                    "total_wins": state["total_wins"],
                    "total_losses": state["total_losses"],
                    "total_pnl": state["total_pnl"],
                    "last_update": state["last_update"]
                }
                logger.info("Memory loaded from SQLite fallback.")
                return
            default = {
                "_id": "global_state",
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
            self.cache = default
            self._save_to_sqlite(default)
            logger.info("New memory state created in SQLite.")

    def _save_to_sqlite(self, state):
        valid_cols = {"first_launch_timestamp", "total_run_seconds", "total_signals_generated",
                      "accepted_signals_count", "rejected_signals_count", "total_trades_closed",
                      "total_wins", "total_losses", "total_pnl", "restart_count", "last_update"}
        data = {k: v for k, v in state.items() if k in valid_cols}
        self.sqlite.update_memory_state(data)

    def get_or_create_state(self):
        with self.lock:
            return self.cache

    def update_state(self, updates: dict):
        with self.lock:
            for k, v in updates.items():
                if k in self.cache:
                    if isinstance(v, (int, float)) and k != "total_pnl":
                        self.cache[k] += v
                    else:
                        self.cache[k] = v
                else:
                    self.cache[k] = v
            self.cache["last_update"] = int(time.time())
            if self.use_mongo:
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
                    self.mongo.db.global_bot_memory.update_one({"_id": "global_state"}, update_doc, upsert=True)
                except Exception as e:
                    logger.error(f"MongoDB memory update error: {e}")
            self._save_to_sqlite(self.cache)

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
# WEBSOCKET STREAMS (same as app(14).py)
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
        self.rest_fallback = False
        self.last_rest_fetch = 0

    def start(self):
        self.running = True
        threading.Thread(target=self._ws_loop, daemon=True).start()
        threading.Thread(target=self._health_check, daemon=True).start()
        threading.Thread(target=self._rest_fallback_loop, daemon=True).start()

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
            self.rest_fallback = False
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
            if age > 60 and not self.rest_fallback:
                logger.warning(f"Public WS no ping/pong for {age:.0f}s, switching to REST fallback")
                self.rest_fallback = True
                if self.ws:
                    try: self.ws.close()
                    except Exception: pass

    def _rest_fallback_loop(self):
        while self.running:
            time.sleep(Config.REST_FALLBACK_INTERVAL)
            with self.lock:
                if not self.rest_fallback:
                    continue
                now = time.time()
                if now - self.last_rest_fetch < Config.REST_FALLBACK_INTERVAL:
                    continue
                self.last_rest_fetch = now
            try:
                for symbol in Config.ASSETS:
                    resp = requests.get(f"https://api.binance.com/api/v3/ticker?symbol={symbol}", timeout=5)
                    if resp.status_code == 200:
                        data = resp.json()
                        self.on_price_update(symbol, float(data["lastPrice"]), float(data["volume"]))
                        logger.info(f"🌐 REST fallback: {symbol} @ {data['lastPrice']}")
            except Exception as e:
                logger.error(f"REST fallback error: {e}")

# =====================================================================
# CANDLE TOPOLOGY ENGINE (per-timeframe close flags) – FIXED
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
        # ---- per-timeframe close flags ----
        self.candle_just_closed = {tf: {asset: False for asset in Config.ASSETS} for tf in [60, 300, 900, 3600, 14400]}
        self.history = {asset: deque(maxlen=200) for asset in Config.ASSETS}
        self.volume_ma = {asset: 0.0 for asset in Config.ASSETS}
        self.high_volume_levels = {asset: [] for asset in Config.ASSETS}

    def process_tick(self, asset, price, volume):
        if not DataValidator.validate_price(price) or not DataValidator.validate_volume(volume):
            return
        with self.lock:
            now = int(time.time())
            self.history[asset].append({"price": price, "volume": volume, "time": now})
            # Reset close flags for all timeframes for this asset
            for tf in self.candle_just_closed:
                self.candle_just_closed[tf][asset] = False

            # Process each timeframe
            for timeframe in [60, 300, 900, 3600, 14400]:
                storage = self.candles[timeframe][asset]
                start = (now // timeframe) * timeframe
                if not storage or storage[-1].get("timestamp") != start:
                    if storage and not storage[-1].get("complete", False):
                        storage[-1]["complete"] = True
                        self.candle_just_closed[timeframe][asset] = True
                    # New candle
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

            self._update_volume_ma(asset)
            self._update_pivots(asset, price)
            self._update_support_resistance(asset, price)
            self._detect_bos_choch(asset)
            self.last_tick_time[asset] = now

    # ---- remaining methods unchanged from app(14).py ----
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
                if (min(last["open"], last["close"]) - last["low"]) / total > 0.6:
                    patterns["bullish_rej"] = 1
                if (last["high"] - max(last["open"], last["close"])) / total > 0.6:
                    patterns["bearish_rej"] = 1
            return patterns

    def get_atr(self, asset, period=14, tf=3600):
        with self.lock:
            complete = self.get_completed(asset, tf)
            if len(complete) < period:
                return 0.0
            tr_list = []
            for i in range(1, period+1):
                high, low = complete[i]["high"], complete[i]["low"]
                prev_close = complete[i-1]["close"]
                tr_list.append(max(high-low, abs(high-prev_close), abs(low-prev_close)))
            return sum(tr_list) / period

    def detect_liquidity_sweep(self, asset, price):
        with self.lock:
            h, l = self.pivots[asset]["high"], self.pivots[asset]["low"]
            if h and price > max(h[-2:]):
                return "BUY_SWEEP"
            if l and price < min(l[-2:]):
                return "SELL_SWEEP"
            return ""

    def get_volatility_regime(self, asset):
        atr = self.get_atr(asset)
        if atr == 0:
            return "medium"
        return "low" if atr < 50 else "medium" if atr < 150 else "high" if atr < 300 else "extreme"

    def _ema(self, series, period):
        if len(series) < period:
            return []
        ema = [sum(series[:period]) / period]
        m = 2 / (period + 1)
        for i in range(period, len(series)):
            ema.append((series[i] - ema[-1]) * m + ema[-1])
        return ema

    def check_1m_rejection(self, asset, direction):
        with self.lock:
            candles = self.candles[60][asset]
            if len(candles) < 2:
                return False
            last = next((c for c in reversed(candles) if c.get("complete", False)), None)
            if not last:
                return False
            r = last["high"] - last["low"]
            if r <= 0:
                return False
            if direction == "BUY":
                return (min(last["open"], last["close"]) - last["low"]) / r >= 0.4
            else:
                return (last["high"] - max(last["open"], last["close"])) / r >= 0.4

    def get_visual_topology(self, asset, price, direction, sl, tp, patterns):
        with self.lock:
            min_price = min(price, sl, tp) * 0.98
            max_price = max(price, sl, tp) * 1.02
            if max_price - min_price < 0.01:
                min_price = price * 0.95
                max_price = price * 1.05
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
                    if any(abs(level - s) / s < 0.001 for s in supports):
                        marker = "S"
                    elif any(abs(level - r) / r < 0.001 for r in resistances):
                        marker = "R"
                bar = "█" * int((i / rows) * 10) if i > 0 else ""
                lines.append(f"│ {level:>8.2f} │ {marker} {bar:<10} │")
            lines += ["├──────────────────────────────────────┤", "│ ●=Entry ▼=SL ★=TP  S=Support R=Res │", "└──────────────────────────────────────┘"]
            return "\n".join(lines)

    def get_adx(self, asset, tf, period=14):
        with self.lock:
            complete = self.get_completed(asset, tf)
            if len(complete) < period:
                return 20
            tr_list, dm_plus, dm_minus = [], [], []
            for i in range(1, len(complete)):
                high, low = complete[i]["high"], complete[i]["low"]
                prev_high, prev_low = complete[i-1]["high"], complete[i-1]["low"]
                tr = max(high-low, abs(high-prev_high), abs(low-prev_low))
                tr_list.append(tr)
                up = high - prev_high
                down = prev_low - low
                dm_plus.append(max(up, 0) if up > down else 0)
                dm_minus.append(max(down, 0) if down > up else 0)
            if len(tr_list) < period:
                return 20
            atr = sum(tr_list[:period]) / period
            dp = sum(dm_plus[:period]) / period
            dm = sum(dm_minus[:period]) / period
            for i in range(period, len(tr_list)):
                atr = (atr * (period - 1) + tr_list[i]) / period
                dp = (dp * (period - 1) + dm_plus[i]) / period
                dm = (dm * (period - 1) + dm_minus[i]) / period
            if atr == 0:
                return 20
            di_p = (dp / atr) * 100
            di_m = (dm / atr) * 100
            dx = (abs(di_p - di_m) / (di_p + di_m)) * 100 if (di_p + di_m) > 0 else 0
            return min(100, dx)

    def detect_fvg(self, asset):
        with self.lock:
            complete = self.get_completed(asset, 900)
            if len(complete) < 3:
                return []
            fvgs = []
            for i in range(2, len(complete) - 1):
                c1, c2, c3 = complete[i - 2], complete[i - 1], complete[i]
                if c1["close"] < c2["open"] and c2["close"] < c3["close"] and c1["high"] > c2["low"]:
                    fvgs.append({"type": "bullish", "upper": c1["high"], "lower": c2["low"]})
                if c1["close"] > c2["open"] and c2["close"] > c3["close"] and c2["high"] > c1["low"]:
                    fvgs.append({"type": "bearish", "upper": c2["high"], "lower": c1["low"]})
            return fvgs[-5:]

    def detect_order_block(self, asset):
        with self.lock:
            if not self.bos[asset]["direction"]:
                return {}
            complete = self.get_completed(asset, 900)
            if len(complete) < 10:
                return {}
            atr = self.get_atr(asset)
            if atr == 0:
                return {}
            for i in range(len(complete) - 1, -1, -1):
                c = complete[i]
                if (c["high"] - c["low"]) > 1.5 * atr:
                    return {"type": "bullish" if c["close"] > c["open"] else "bearish", "high": c["high"], "low": c["low"]}
            return {}

    def _calc_rsi(self, closes, period=14):
        if len(closes) < period + 1:
            return 50
        start = len(closes) - period - 1
        gains = losses = 0.0
        for i in range(start + 1, len(closes)):
            diff = closes[i] - closes[i - 1]
            if diff > 0:
                gains += diff
            else:
                losses -= diff
        avg_gain = gains / period
        avg_loss = losses / period
        if avg_loss == 0:
            return 100
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
# NEUTRAL CANDLE ENGINE (unchanged)
# =====================================================================
class NeutralCandleEngine:
    def __init__(self, topology):
        self.topology = topology

    def detect(self, asset, price, tf=900):
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

            DOJI_BODY_PCT = 0.05
            SPINNING_TOP_BODY_PCT = 0.30
            WICK_RATIO = 2.0

            pattern = 'None'
            direction = ''
            score = 0
            reason = ''

            if body_pct <= DOJI_BODY_PCT:
                if upper_wick > 3 * lower_wick and upper_wick > body:
                    pattern = 'GravestoneDoji'
                    direction = 'SELL'
                elif lower_wick > 3 * upper_wick and lower_wick > body:
                    pattern = 'DragonflyDoji'
                    direction = 'BUY'
                else:
                    pattern = 'Doji'
                    direction = ''
            elif body_pct <= SPINNING_TOP_BODY_PCT:
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
                return {'pattern': 'None', 'direction': '', 'score': 0, 'reason': 'No neutral pattern'}

            if direction == 'BUY':
                score = 10
                reason = f'{pattern} (bullish)'
            elif direction == 'SELL':
                score = 10
                reason = f'{pattern} (bearish)'
            else:
                score = 3
                reason = f'{pattern} (neutral)'

            return {
                'pattern': pattern,
                'direction': direction,
                'score': score,
                'reason': reason
            }

# =====================================================================
# SIMPLE SIGNAL ENGINE (FIXED: 150 candles for EMA)
# =====================================================================
class SimpleSignalEngine:
    def __init__(self, topology, futures_stream, absorption_meter, neutral_engine):
        self.topology = topology
        self.futures = futures_stream
        self.absorption = absorption_meter
        self.neutral = neutral_engine

    def evaluate(self, asset: str, price: float, volume: float):
        """
        Returns a dict with status, direction, score, reason, breakdown.
        status: "NO SETUP" | "REJECTED" | "WATCH" | "VALID" | "HIGH"
        """
        breakdown = {}
        with self.topology.lock:
            # ---- LAYER 1: TREND ----
            c15 = self.topology.get_completed(asset, 900)
            if len(c15) < 60:   # Need at least 60 for EMA50
                return {
                    "status": "NO SETUP",
                    "direction": None,
                    "score": 0,
                    "reason": "Insufficient 15m data (need 60)",
                    "breakdown": {"error": "insufficient_data"}
                }
            c1h = self.topology.get_completed(asset, 3600)
            if len(c1h) < 60:
                return {
                    "status": "NO SETUP",
                    "direction": None,
                    "score": 0,
                    "reason": "Insufficient 1h data (need 60)",
                    "breakdown": {"error": "insufficient_data"}
                }

            # ---- FIX: Use last 150 candles for EMA calculation ----
            closes_15 = [c['close'] for c in c15[-150:]]
            closes_1h = [c['close'] for c in c1h[-150:]]

            ema20_15 = self.topology._ema(closes_15, 20)[-1] if len(closes_15) >= 20 else None
            ema50_15 = self.topology._ema(closes_15, 50)[-1] if len(closes_15) >= 50 else None
            ema20_1h = self.topology._ema(closes_1h, 20)[-1] if len(closes_1h) >= 20 else None
            ema50_1h = self.topology._ema(closes_1h, 50)[-1] if len(closes_1h) >= 50 else None
            rsi_15 = self.topology._calc_rsi(closes_15[-15:]) if len(closes_15) >= 15 else 50

            # Now EMA50 should be available
            if None in (ema20_15, ema50_15, ema20_1h, ema50_1h):
                return {
                    "status": "NO SETUP",
                    "direction": None,
                    "score": 0,
                    "reason": "EMA not ready (even with enlarged window)",
                    "breakdown": {"error": "ema_not_ready"}
                }

            trend_15_bull = ema20_15 > ema50_15
            trend_1h_bull = ema20_1h > ema50_1h
            rsi_bull = rsi_15 > 50

            if trend_15_bull and trend_1h_bull and rsi_bull:
                direction = "BUY"
                trend_score = 30
            elif not trend_15_bull and not trend_1h_bull and not rsi_bull:
                direction = "SELL"
                trend_score = 30
            else:
                return {
                    "status": "NO SETUP",
                    "direction": None,
                    "score": 0,
                    "reason": "Trend conflict or unclear",
                    "breakdown": {"trend": "conflict"}
                }

            breakdown["Trend"] = trend_score

            # ---- LAYER 2: LOCATION ----
            loc_score = 0
            loc_reasons = []
            sr = self.topology.support_resistance[asset]
            atr = self.topology.get_atr(asset, period=14, tf=3600) or (price * 0.01)
            nearest_support = max(sr["support"]) if sr["support"] else None
            nearest_resistance = min(sr["resistance"]) if sr["resistance"] else None

            if direction == "BUY" and nearest_support and abs(price - nearest_support) <= 1.5 * atr:
                loc_score += 12
                loc_reasons.append("NearSupport")
            elif direction == "SELL" and nearest_resistance and abs(price - nearest_resistance) <= 1.5 * atr:
                loc_score += 12
                loc_reasons.append("NearResistance")

            anchor_ok, anchor_price = self.topology.check_anchor_line_retest(asset, price, direction)
            if anchor_ok:
                loc_score += 8
                loc_reasons.append(f"Anchor@{anchor_price:.0f}")

            if direction == "BUY" and price <= ema20_15 * 1.005:
                loc_score += 5
                loc_reasons.append("Near15EMA")
            elif direction == "SELL" and price >= ema20_15 * 0.995:
                loc_score += 5
                loc_reasons.append("Near15EMA")

            loc_score = min(loc_score, 25)
            breakdown["Location"] = loc_score
            breakdown["LocationDetails"] = " | ".join(loc_reasons) if loc_reasons else "Poor location"

            # ---- LAYER 3: CONFIRMATION ----
            conf_score = 0
            vol_reasons = []

            vol_ma = self.topology.volume_ma.get(asset, 0)
            vol_ratio = volume / vol_ma if vol_ma > 0 else 1.0
            if (direction == "BUY" and vol_ratio > 1.5) or (direction == "SELL" and vol_ratio > 1.5):
                conf_score += 8
                vol_reasons.append("HighVolume")
            elif vol_ratio > 1.2:
                conf_score += 5
                vol_reasons.append("AboveAvgVolume")

            cvd = self.futures.get_cvd(asset.lower())
            if (direction == "BUY" and cvd > 0) or (direction == "SELL" and cvd < 0):
                conf_score += 6
                vol_reasons.append("CVDsupport")

            oi_trend = self.futures.get_oi_trend(asset.lower())
            if (direction == "BUY" and oi_trend > 0) or (direction == "SELL" and oi_trend < 0):
                conf_score += 6
                vol_reasons.append("OIsupport")

            conf_score = min(conf_score, 20)
            breakdown["VolumeOrderFlow"] = conf_score
            breakdown["VolFlowDetails"] = " | ".join(vol_reasons) if vol_reasons else "Neutral"

            # Candle + Structure
            struct_score = 0
            struct_reasons = []

            if len(c15) >= 2:
                last = c15[-1]
                rng = last["high"] - last["low"]
                if rng > 0:
                    upper_wick = (last["high"] - max(last["open"], last["close"])) / rng
                    lower_wick = (min(last["open"], last["close"]) - last["low"]) / rng
                    if direction == "BUY" and lower_wick > 0.4:
                        struct_score += 6
                        struct_reasons.append("RejectionWick")
                    elif direction == "SELL" and upper_wick > 0.4:
                        struct_score += 6
                        struct_reasons.append("RejectionWick")

            bos = self.topology.bos[asset]["direction"]
            if (direction == "BUY" and bos == "UP") or (direction == "SELL" and bos == "DOWN"):
                struct_score += 5
                struct_reasons.append("BOSconfirm")

            if self.topology.choch[asset]:
                struct_score += 4
                struct_reasons.append("CHoCH")

            neutral = self.neutral.detect(asset, price, tf=900)
            if neutral.get("direction") == direction:
                struct_score += 5
                struct_reasons.append(neutral.get("pattern", "Neutral"))

            struct_score = min(struct_score, 15)
            breakdown["CandleStructure"] = struct_score
            breakdown["StructDetails"] = " | ".join(struct_reasons) if struct_reasons else "Weak"

            # ---- LAYER 4: RISK ----
            risk_score = 0
            risk_reasons = []

            atr_pct = atr / price if price > 0 else 0.01
            if atr_pct < 0.02:
                risk_score += 4
                risk_reasons.append("LowVol")
            elif atr_pct < 0.04:
                risk_score += 2
                risk_reasons.append("MedVol")
            else:
                risk_reasons.append("HighVol")

            dist_pct = abs(price - ema20_15) / price
            if dist_pct < 0.02:
                risk_score += 6
                risk_reasons.append("NearEMA")
            elif dist_pct < 0.04:
                risk_score += 3
                risk_reasons.append("ModerateExt")
            else:
                risk_reasons.append("Extended")

            risk_score = min(risk_score, 10)
            breakdown["Risk"] = risk_score
            breakdown["RiskDetails"] = " | ".join(risk_reasons) if risk_reasons else "Unknown"

            # ---- TOTAL SCORE ----
            total_score = trend_score + loc_score + conf_score + struct_score + risk_score
            breakdown["Total"] = total_score
            breakdown["Direction"] = direction

            # ---- DETERMINE STATUS ----
            if total_score >= 75:
                status = "EXTREME"
            elif total_score >= Config.SCORE_HIGH:
                status = "HIGH"
            elif total_score >= Config.SCORE_VALID:
                status = "VALID"
            elif total_score >= Config.SCORE_WATCH:
                status = "WATCH"
            else:
                status = "REJECTED"

            reason = (f"Trend: {trend_score} | Loc: {loc_score} | V/OF: {conf_score} | C/S: {struct_score} | Risk: {risk_score}")
            return {
                "status": status,
                "direction": direction,
                "score": total_score,
                "reason": reason,
                "breakdown": breakdown
            }

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
                if candidates:
                    nearest_support = max(candidates)
            if sr["resistance"]:
                candidates = [r for r in sr["resistance"] if r > entry and (r - entry) / entry < 0.10]
                if candidates:
                    nearest_resistance = min(candidates)

        default_sl = entry + 1.5 * atr if direction == "SELL" else entry - 1.5 * atr
        if direction == "SELL":
            sl = nearest_resistance + 0.5 * atr if nearest_resistance else default_sl
            if sl - entry > 1.5 * atr:
                sl = default_sl
            sl = max(sl, entry + buffer)
        else:
            sl = nearest_support - 0.5 * atr if nearest_support else default_sl
            if entry - sl > 1.5 * atr:
                sl = default_sl
            sl = min(sl, entry - buffer)

        risk = abs(entry - sl)
        if risk < buffer * 0.5:
            sl = entry - buffer if direction == "BUY" else entry + buffer
            risk = buffer

        # TP based on structure
        targets = []
        if direction == "SELL":
            if nearest_support:
                targets.append(nearest_support)
            targets.append(entry - 2.5 * risk)
            with self.topology.lock:
                lows = self.topology.pivots[asset]["low"]
                if lows:
                    targets.append(min(lows))
            tp = min(targets) if targets else entry - 2.5 * risk
            if entry - tp < 1.5 * risk:
                tp = entry - 1.5 * risk
        else:
            if nearest_resistance:
                targets.append(nearest_resistance)
            targets.append(entry + 2.5 * risk)
            with self.topology.lock:
                highs = self.topology.pivots[asset]["high"]
                if highs:
                    targets.append(max(highs))
            tp = max(targets) if targets else entry + 2.5 * risk
            if tp - entry < 1.5 * risk:
                tp = entry + 1.5 * risk

        rr = abs(tp - entry) / risk if risk > 0 else 0
        if rr < Config.MIN_RR:
            if direction == "BUY":
                tp = entry + Config.MIN_RR * risk
            else:
                tp = entry - Config.MIN_RR * risk
            rr = Config.MIN_RR

        return sl, tp, risk, rr

# =====================================================================
# PENDING VERIFICATION QUEUE (FIXED: store verified signals)
# =====================================================================
class PendingVerificationQueue:
    def __init__(self, topology):
        self.topology = topology
        self.pending = {}
        self.verified = []   # FIX: store verified signals here
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
        """Check pending signals for asset and move verified ones to self.verified"""
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

                    # Directional confirmation (light)
                    if data['direction'] == 'BUY' and first_close <= data['start_price'] * 0.998:
                        data['rejected'] = True
                        to_remove.append(key)
                    elif data['direction'] == 'SELL' and first_close >= data['start_price'] * 1.002:
                        data['rejected'] = True
                        to_remove.append(key)

                    # Adverse move check
                    if data['direction'] == 'BUY' and (data['start_price'] - first_close) > max_allowed_adverse:
                        data['rejected'] = True
                        to_remove.append(key)
                    elif data['direction'] == 'SELL' and (first_close - data['start_price']) > max_allowed_adverse:
                        data['rejected'] = True
                        to_remove.append(key)

                if data['candle_count'] >= limit:
                    if not data['rejected']:
                        # FIX: move to verified list
                        self.verified.append(data)
                    to_remove.append(key)
            for key in to_remove:
                if key in self.pending:
                    del self.pending[key]
            return to_remove

    def get_verified_signals(self):
        """Return all verified signals and clear the list"""
        with self.lock:
            ready = self.verified[:]
            self.verified = []
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
                direction = "BUY"
                score += 20
            elif cvd < 0:
                direction = "SELL"
                score += 20

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
                    score, patterns, trade_id, session, rr, regime, signal_type="SIMPLE",
                    signal_token=None, tier="HIGH"):
        header = "🚀 ALPHABOT SIGNAL\n━━━━━━━━━━━━━━━━━━━━━━━"
        token_line = f"🆔 Token: {signal_token} (DB ID: #{trade_id})" if signal_token else f"🆔 DB ID: #{trade_id}"
        msg = (f"{header}\n"
               f"📊 {Config.DISPLAY_NAMES.get(asset, asset)} | {token_line}\n"
               f"DIRECTION: {direction}\n"
               f"━━━━━━━━━━━━━━━━━━━━━━━\n"
               f"💰 Entry: {price:.2f}\n"
               f"🛑 Stop Loss: {sl:.2f}\n"
               f"🎯 Take Profit: {tp:.2f}\n"
               f"R:R: 1:{rr:.2f}\n"
               f"QUALITY SCORE: {score}/100\n"
               f"TIER: {tier}\n"
               f"━━━━━━━━━━━━━━━━━━━━━━━\n"
               f"🧠 Logic: {logic}\n"
               f"📰 News: {news}\n"
               f"SIGNAL ID: {signal_token or trade_id}")
        self.queue.put(msg)

    def fire_trade_close(self, trade_id, asset, entry, exit_price, pnl, reason, entry_time):
        hold_min = (time.time() - entry_time) / 60
        pnl_pct = (pnl / entry) * 100 if entry else 0
        status = "🟢 PROFIT" if pnl > 0 else "🔴 LOSS" if pnl < 0 else "⚪ BREAKEVEN"
        msg = (f"<b>🔒 Trade #{trade_id} Closed – {Config.DISPLAY_NAMES.get(asset, asset)}</b>\n"
               f"Status: {status}\n"
               f"💰 PnL: {pnl:+.2f} ({pnl_pct:+.2f}%)\n"
               f"🕐 Held for: {int(hold_min)} min\n"
               f"📌 Exit Reason: {reason}")
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
                        half = trade['entry'] + 0.5 * target if trade['direction'] == 'BUY' else trade['entry'] - 0.5 * target
                        if (trade['direction'] == 'BUY' and current_price >= half) or (trade['direction'] == 'SELL' and current_price <= half):
                            if self.orch.topology.check_1m_rejection(asset, trade['direction']):
                                trade['sl'] = trade['entry']
                                trade['breakeven_locked'] = True
                                self.orch.mongo.update_trade_sl(tid, trade['entry'])

                    if not trade.get('trailing_activated', False):
                        target = abs(trade['tp'] - trade['entry'])
                        trigger = trade['entry'] + 0.7 * target if trade['direction'] == 'BUY' else trade['entry'] - 0.7 * target
                        if (trade['direction'] == 'BUY' and current_price >= trigger) or (trade['direction'] == 'SELL' and current_price <= trigger):
                            new_sl = trade['entry'] + 0.3 * target if trade['direction'] == 'BUY' else trade['entry'] - 0.3 * target
                            if (trade['direction'] == 'BUY' and new_sl > trade['sl']) or (trade['direction'] == 'SELL' and new_sl < trade['sl']):
                                trade['sl'] = new_sl
                                trade['trailing_activated'] = True
                                self.orch.mongo.update_trade_sl(tid, new_sl)

                for tid in to_remove:
                    if tid in self.orch.active_trades:
                        del self.orch.active_trades[tid]
                gc.collect()

# =====================================================================
# CORE ORCHESTRATOR – FIXED: separate 5m and 15m triggers
# =====================================================================
class AIOrchestrator:
    def __init__(self):
        self.topology = CandleTopologyEngine()
        self.cache = IndicatorCache(self.topology)
        self.news = CryptoNewsScanner()
        self.db = TradeDatabase()
        self.mongo = MongoDatabase()
        self.memory = PersistentMemoryEngine(self.mongo, self.db)
        self.telegram = TelegramPipeline()

        self.futures_stream = BinanceFuturesStream()
        self.futures_stream.start()

        self.absorption_meter = MarketAbsorptionMeter(self.futures_stream, self.topology)
        self.neutral_engine = NeutralCandleEngine(self.topology)

        self.signal_engine = SimpleSignalEngine(
            self.topology,
            self.futures_stream,
            self.absorption_meter,
            self.neutral_engine
        )

        self.pending_queue = PendingVerificationQueue(self.topology)
        self.dynamic_sl = DynamicStopLoss(self.topology)
        self.health_engine = TradeHealthEngine(self.topology, self.cache)

        self.active_trades = {}
        self.trade_lock = threading.Lock()
        self.price_queue = queue.Queue(maxsize=1000)
        self.start_time = time.time()

        self.signal_timestamps = deque(maxlen=100)
        self.asset_last_signal = {a: 0 for a in Config.ASSETS}

        self.asset_state = {a: {"trend": "NEUTRAL", "htf_trend": "NEUTRAL", "volume_ratio": 1.0,
                                "rsi": 50, "adx": 20, "volatility": 0.01,
                                "news_sentiment": 0, "news_importance": 0.5,
                                "neutral_pattern": "None"} for a in Config.ASSETS}
        self.accepted = 0
        self.rejected = 0
        self.stream = None
        self._price_counter = 0
        self._ready = False

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

    def _signal_limit_ok(self):
        now = time.time()
        count = sum(1 for ts in self.signal_timestamps if now - ts < 86400)
        return count < Config.MAX_SIGNALS_PER_DAY

    def _asset_cooldown_ok(self, asset):
        last = self.asset_last_signal.get(asset, 0)
        return (time.time() - last) >= (Config.ASSET_COOLDOWN_HOURS * 3600)

    def _handle_price_tick(self, asset, price, volume):
        with self.trade_lock:
            if any(t['asset'] == asset for t in self.active_trades.values()):
                return

        self.topology.process_tick(asset, price, volume)
        self._update_active_trades(asset, price)

        # ---- Check 5m candle close for pending verification ----
        if self.topology.candle_just_closed[300].get(asset, False):
            if self.pending_queue.pending:
                self.pending_queue.check_pending(asset)
                verified = self.pending_queue.get_verified_signals()
                for signal in verified:
                    self._send_final_signal(signal)

        # ---- Check 15m candle close for new signal evaluation ----
        if self.topology.candle_just_closed[900].get(asset, False):
            # ---- Evaluate with Simple Engine ----
            result = self.signal_engine.evaluate(asset, price, volume)
            status = result["status"]
            direction = result["direction"]
            score = result["score"]
            reason = result["reason"]
            breakdown = result["breakdown"]

            # ---- LOG OBSERVATION (ALWAYS) ----
            self.db.log_observation(
                asset=asset,
                price=price,
                direction=direction if direction else "NONE",
                score=score,
                status=status,
                reason=reason,
                breakdown=breakdown,
                volatility=self.asset_state[asset]["volatility"]
            )
            logger.info(f"🔍 {asset} | Status: {status} | Score: {score} | Dir: {direction} | Reason: {reason}")

            # ---- Always log to rejected_signals if status is not HIGH/VALID ----
            if status in ("NO SETUP", "REJECTED", "WATCH"):
                self.db.log_rejected(
                    asset=asset,
                    price=price,
                    score=score,
                    reason=f"{status}: {reason}",
                    volatility=self.asset_state[asset]["volatility"],
                    regime="SIMPLE",
                    gate_name="Engine",
                    status=status,
                    direction=direction if direction else "",
                    score_breakdown=json.dumps(breakdown)
                )
                self.rejected += 1
                self.memory.update_state({"rejected_signals_count": 1})
                return

            # ---- Only VALID and HIGH proceed further ----
            if status not in ("VALID", "HIGH"):
                return

            # ---- Global and per-asset limits ----
            if not self._signal_limit_ok():
                self.db.log_rejected(asset, price, score, "Global daily limit reached",
                                     self.asset_state[asset]["volatility"], "SIMPLE", "DailyLimit",
                                     json.dumps(breakdown), status="REJECTED", direction=direction)
                self.rejected += 1
                self.memory.update_state({"rejected_signals_count": 1})
                return

            if not self._asset_cooldown_ok(asset):
                self.db.log_rejected(asset, price, score, f"Asset cooldown {Config.ASSET_COOLDOWN_HOURS}h",
                                     self.asset_state[asset]["volatility"], "SIMPLE", "Cooldown",
                                     json.dumps(breakdown), status="REJECTED", direction=direction)
                self.rejected += 1
                self.memory.update_state({"rejected_signals_count": 1})
                return

            # ---- Calculate SL/TP ----
            atr = self.topology.get_atr(asset, period=14, tf=3600) or (price * 0.01)
            sl, tp, risk, rr = self.dynamic_sl.calculate(asset, direction, price, atr)

            if rr < Config.MIN_RR:
                self.db.log_rejected(asset, price, score, f"R:R {rr:.2f} < {Config.MIN_RR}",
                                     self.asset_state[asset]["volatility"], "SIMPLE", "Risk",
                                     json.dumps(breakdown), status="REJECTED", direction=direction)
                self.rejected += 1
                self.memory.update_state({"rejected_signals_count": 1})
                return

            # ---- Prepare pending signal ----
            trade_id = self.db.generate_trade_id()
            token = f"SMP-{asset}-{int(time.time()*1000)}"
            signal = {
                'asset': asset,
                'direction': direction,
                'entry': price,
                'sl': sl,
                'tp': tp,
                'sqs': score,
                'session': "ALWAYS",
                'patterns': {},
                'logic': reason,
                'news': self.news.last_news.get('title','')[:100],
                'volatility': self.asset_state[asset]["volatility"],
                'regime': "SIMPLE",
                'htf_trend': self.asset_state[asset]["htf_trend"],
                'news_score': self.asset_state[asset]["news_sentiment"],
                'score': score,
                'confidence': status,
                'num_passed': 4,
                'pending_candles': Config.PENDING_VERIFICATION_CANDLES,
                'volume_decay_threshold': Config.VOLUME_DECAY_THRESHOLD,
                'dynamic_min_sqs': 0,
                'signal_type': 'SIMPLE',
                'signal_token': token,
                'trade_id': trade_id,
                'score_breakdown': json.dumps(breakdown)
            }

            self.pending_queue.add_signal(signal)
            self.memory.update_state({"total_signals_generated": 1})
            logger.info(f"⏳ Pending: {asset} {direction} @ {price} Score:{score} ({status})")

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
            dm = signal.get('dynamic_min_sqs', 0)
            st = signal.get('signal_type', 'SIMPLE')
            token = signal.get('signal_token')
            trade_id = signal.get('trade_id') or self.db.generate_trade_id()
            pattern_name = list(patterns.keys())[0] if patterns else "unknown"
            total_score = signal.get('score', 0)
            confidence = signal.get('confidence', 'HIGH')
            breakdown = signal.get('score_breakdown', '')

            self.db.log_trade(trade_id, asset, direction, price, sl, tp, total_score, confidence,
                              list(patterns.keys()), logic, volatility, regime, htf_trend,
                              news_score, session, sqs, pattern_name, dm, st, token, breakdown)
            if self.mongo.db is not None:
                self.mongo.save_trade_backup({
                    'id': trade_id, 'asset': asset, 'direction': direction,
                    'entry': price, 'stop_loss': sl, 'take_profit': tp,
                    'score': total_score, 'status': 'open', 'signal_type': st,
                    'signal_token': token, 'entry_time': int(time.time())
                })

            chart = self.topology.get_visual_topology(asset, price, direction, sl, tp, patterns)
            rr = abs(tp - price) / abs(price - sl) if abs(price - sl) > 0 else 0
            self.telegram.fire_signal(asset=asset, direction=direction, price=price, sl=sl, tp=tp,
                                      chart=chart, logic=logic, news=news,
                                      score=total_score, patterns=patterns, trade_id=trade_id,
                                      session=session, rr=rr, regime=regime, signal_type=st,
                                      signal_token=token, tier=confidence)

            self.accepted += 1
            self.asset_last_signal[asset] = time.time()
            self.signal_timestamps.append(time.time())
            with self.trade_lock:
                self.active_trades[trade_id] = {
                    'id': trade_id, 'asset': asset, 'direction': direction,
                    'entry': price, 'sl': sl, 'tp': tp, 'entry_time': int(time.time()),
                    'breakeven_locked': False, 'trailing_activated': False,
                    'health': 100, 'regime': regime, 'signal_token': token
                }
            self.memory.update_state({"accepted_signals_count": 1})
            logger.info(f"✅ FINAL SIGNAL: {asset} {direction} @ {price} Score:{total_score} {confidence}")
        except Exception as e:
            logger.error(f"Error sending final signal: {e}", exc_info=True)

    def _update_active_trades(self, asset, price):
        # (unchanged)
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
        self._ready = True
        self.stream = BinancePublicStream(self._on_price)
        self.stream.start()
        self.telegram.send_message("🚀 AlphaBot v7.6 FINAL – All Bugs Fixed")
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
# HEALTH SERVER (with Observations table)
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
                    cur.execute("SELECT datetime(timestamp, 'unixepoch'), asset, price, reason, gate_name, regime, status FROM rejected_signals ORDER BY timestamp DESC LIMIT 50")
                    rows = cur.fetchall()
                    data = [{"time": r[0], "asset": r[1], "price": r[2], "reason": r[3], "gate": r[4], "regime": r[5], "status": r[6]} for r in rows]
                    self.wfile.write(json.dumps(data, indent=2).encode())
                except Exception as e:
                    self.wfile.write(json.dumps({"error": str(e)}).encode())
                return

            if self.path.startswith('/observations'):
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                try:
                    limit = 50
                    if '?limit=' in self.path:
                        limit = int(self.path.split('limit=')[1].split('&')[0])
                    conn = orchestrator.db._get_conn()
                    cur = conn.cursor()
                    cur.execute("SELECT asset, datetime(timestamp, 'unixepoch') as ts, direction, score, status, reason FROM signal_observations ORDER BY timestamp DESC LIMIT ?", (limit,))
                    rows = cur.fetchall()
                    data = [{"asset": r[0], "time": r[1], "direction": r[2], "score": r[3], "status": r[4], "reason": r[5]} for r in rows]
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

                observations = orchestrator.db.get_recent_observations(10)
                obs_rows = ""
                for obs in observations:
                    obs_rows += f"<tr><td>{obs['asset']}</td><td>{obs['time']}</td><td>{obs['direction']}</td><td>{obs['score']}</td><td>{obs['status']}</td><td>{obs['reason'][:50]}</td></tr>"

                neutral_html = ""
                for asset in Config.ASSETS:
                    pat = orchestrator.asset_state.get(asset, {}).get('neutral_pattern', 'None')
                    neutral_html += f"<span style='margin:0 15px;'><b>{asset}</b>: {pat}</span>"

                html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><title>AlphaBot v7.6 Final</title>
<meta http-equiv="refresh" content="15"><style>
body{{font-family:Arial;background:#111;color:#eee;margin:20px}}
table{{border-collapse:collapse;width:100%;font-size:14px}}
th,td{{border:1px solid #444;padding:6px;text-align:center}}
th{{background:#333}}
.g{{color:#0f0}} .r{{color:#f00}}
.btn{{background:#d9534f;color:#fff;padding:3px 8px;text-decoration:none;border-radius:3px;font-size:12px;font-weight:bold}}
.status-REJECTED{{color:#f44}}
.status-NO-SETUP{{color:#888}}
.status-WATCH{{color:#ffa500}}
.status-VALID{{color:#0f0}}
.status-HIGH{{color:#0ff}}
</style></head><body>
<h1>🚀 AlphaBot v7.6 FINAL – All Bugs Fixed</h1>
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
                html += "</table>"
                html += f"""
<h2>📋 Latest Observations</h2>
<table><tr><th>Asset</th><th>Time</th><th>Dir</th><th>Score</th><th>Status</th><th>Reason</th></tr>
{obs_rows}
</table>
<p><a href='/observations' target='_blank'>View all observations (JSON)</a></p>
<p><a href='/rejections' target='_blank'>View rejected signals (JSON)</a></p>
</body></html>
"""
                self.wfile.write(html.encode())
            else:
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "online", "version": "7.6"}).encode())

        def do_HEAD(self):
            self.send_response(200)
            self.end_headers()

    httpd = HTTPServer(("0.0.0.0", port), H)
    logger.info(f"Health server on port {port}")
    httpd.serve_forever()

if __name__ == "__main__":
    bot = AIOrchestrator()
    bot.run()

# =====================================================================
# app.py – AlphaBot v8.0 PRO FINAL (All Critical Bugs Fixed + Upgraded UI)
# =====================================================================
# Architecture: Render Web Service + Spot/Futures WS + MongoDB + SQLite
# Signal Flow : 15m Candle Close Evaluation -> 5m Candle Verification -> Telegram
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

# Safe import for MultiExchangeOIFetcher with built-in fallback
try:
    from multi_oi_fetcher import MultiExchangeOIFetcher
    HAS_MULTI_OI = True
except ImportError:
    HAS_MULTI_OI = False

    class MultiExchangeOIFetcher:
        def __init__(self):
            self._cache = {}
            self._last_fetch = {}

        def fetch_composite_oi(self, asset: str) -> dict:
            now = time.time()
            if asset in self._cache and (now - self._last_fetch.get(asset, 0)) < 45:
                return self._cache[asset]
            result = {"binance": None, "bybit": None, "okx": None}
            try:
                r = requests.get(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={asset}", timeout=4)
                if r.status_code == 200:
                    result["binance"] = float(r.json().get("openInterest", 0))
            except Exception:
                pass
            try:
                r2 = requests.get(f"https://api.bybit.com/v5/market/open-interest?category=linear&symbol={asset}&intervalTime=5min&limit=1", timeout=4)
                if r2.status_code == 200:
                    lst = r2.json().get("result", {}).get("list", [])
                    if lst:
                        result["bybit"] = float(lst[0].get("openInterest", 0))
            except Exception:
                pass
            self._cache[asset] = result
            self._last_fetch[asset] = now
            return result

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
logger = logging.getLogger("AlphaBot-v8.0-Pro")


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
    SCORE_VALID = 52
    SCORE_HIGH = 62
    SCORE_EXTREME = 75

    MIN_SCORE_FOR_SIGNAL = 52
    MIN_RR = 2.0
    MAX_SIGNALS_PER_DAY = 20
    ASSET_COOLDOWN_HOURS = 1.0  # 1 hour cooldown per asset to avoid spam

    PENDING_VERIFICATION_CANDLES = 1  # 1 candle = 5m confirmation
    VOLUME_DECAY_THRESHOLD = 0.55
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
    ATR_NOISE_MULTIPLIER = 0.45
    CVD_EXHAUSTION_PENALTY = 15
    VOLUME_EXPANSION_RATIO = 1.4
    PARABOLIC_RSI_THRESHOLD = 68
    OVEREXTENSION_ATR_MULT = 2.2


# =====================================================================
# DATA VALIDATION
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
# DATABASE LAYERS (MongoDB + SQLite)
# =====================================================================
class MongoDatabase:
    def __init__(self):
        if not HAS_PYMONGO or not Config.MONGO_URI:
            self.client = None
            self.db = None
            logger.warning("MongoDB not configured; using SQLite fallback.")
            return
        try:
            self.client = MongoClient(Config.MONGO_URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[Config.MONGO_DB_NAME]
            self._create_indexes()
            logger.info("✅ MongoDB connected")
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
        if self.db is None:
            return
        try:
            doc = {**candle, "asset": asset, "timeframe": timeframe}
            self.db.candles.update_one(
                {"asset": asset, "timeframe": timeframe, "timestamp": candle["timestamp"]},
                {"$set": doc}, upsert=True
            )
        except Exception:
            pass

    def load_candles(self, asset, timeframe, limit=1000):
        if self.db is None:
            return []
        try:
            cursor = (self.db.candles.find({"asset": asset, "timeframe": timeframe}, {"_id": 0})
                      .sort("timestamp", -1).limit(limit))
            candles = list(cursor)
            candles.reverse()
            return candles
        except Exception:
            return []

    def save_trade_backup(self, trade_data):
        if self.db is None:
            return
        try:
            doc = dict(trade_data)
            doc.pop("_id", None)
            self.db.trades.replace_one({"id": doc["id"]}, doc, upsert=True)
        except Exception:
            pass

    def update_trade_sl(self, trade_id, new_sl):
        if self.db is None:
            return
        try:
            self.db.trades.update_one({"id": trade_id}, {"$set": {"stop_loss": new_sl}})
        except Exception:
            pass

    def close_trade_mongo(self, trade_id, exit_price, pnl, exit_reason):
        if self.db is None:
            return
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
        if self.db is None:
            return []
        try:
            return list(self.db.trades.find({"status": "open"}, {"_id": 0}))
        except Exception:
            return []

    def save_observation(self, obs):
        if self.db is None:
            return
        try:
            self.db.observations.insert_one(dict(obs))
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
            for col_sql in [
                "ALTER TABLE rejected_signals ADD COLUMN status TEXT",
                "ALTER TABLE rejected_signals ADD COLUMN direction TEXT",
                "ALTER TABLE rejected_signals ADD COLUMN candidate_timestamp INTEGER"
            ]:
                try:
                    cur.execute(col_sql)
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
            cur.execute('''INSERT OR IGNORE INTO global_bot_memory 
                (id, first_launch_timestamp, total_run_seconds, total_signals_generated,
                 accepted_signals_count, rejected_signals_count, total_trades_closed,
                 total_wins, total_losses, total_pnl, restart_count, last_update)
                VALUES (1, strftime('%s','now'), 0, 0, 0, 0, 0, 0, 0, 0.0, 0, strftime('%s','now'))''')
            conn.commit()
        finally:
            cur.close()

    def get_memory_state(self):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT * FROM global_bot_memory WHERE id=1")
            row = cur.fetchone()
            return dict(row) if row else None
        except Exception:
            return None
        finally:
            cur.close()

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
        finally:
            cur.close()

    def generate_trade_id(self):
        return int(time.time() * 1000)

    def log_trade(self, trade_id, asset, direction, entry, sl, tp, score, confidence, patterns, logic,
                  volatility, regime, htf_trend, news_score, session, sqs_score, pattern_name,
                  dynamic_min_sqs, signal_type="STANDARD", signal_token=None, score_breakdown=""):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute('''INSERT OR REPLACE INTO trades 
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

    def update_trade_sl_sqlite(self, trade_id, new_sl):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("UPDATE trades SET stop_loss=? WHERE id=?", (new_sl, trade_id))
            conn.commit()
        except Exception:
            pass
        finally:
            cur.close()

    def get_open_trades_sqlite(self):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT * FROM trades WHERE status='open'")
            return [dict(r) for r in cur.fetchall()]
        except Exception:
            return []
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

    def get_recent_observations(self, limit=25):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT asset, datetime(timestamp, 'unixepoch') as time, price, direction, score, status, reason FROM signal_observations ORDER BY timestamp DESC LIMIT ?", (limit,))
            return [dict(r) for r in cur.fetchall()]
        except Exception:
            return []
        finally:
            cur.close()


# =====================================================================
# PERSISTENT MEMORY (FIXED: Cumulative PnL & Run Seconds)
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

    def _save_to_sqlite(self, state):
        valid_cols = {"first_launch_timestamp", "total_run_seconds", "total_signals_generated",
                      "accepted_signals_count", "rejected_signals_count", "total_trades_closed",
                      "total_wins", "total_losses", "total_pnl", "restart_count", "last_update"}
        data = {k: v for k, v in state.items() if k in valid_cols}
        self.sqlite.update_memory_state(data)

    def get_or_create_state(self):
        with self.lock:
            return dict(self.cache)

    def update_state(self, updates: dict):
        """Increment counters safely (including total_pnl and total_run_seconds delta)."""
        with self.lock:
            inc_fields = {}
            set_fields = {}
            for k, v in updates.items():
                if k in ("last_update", "first_launch_timestamp"):
                    self.cache[k] = v
                    set_fields[k] = v
                elif isinstance(v, (int, float)):
                    self.cache[k] = self.cache.get(k, 0) + v
                    inc_fields[k] = v
                else:
                    self.cache[k] = v
                    set_fields[k] = v

            now = int(time.time())
            self.cache["last_update"] = now
            set_fields["last_update"] = now

            if self.use_mongo:
                try:
                    update_doc = {}
                    if inc_fields:
                        update_doc["$inc"] = inc_fields
                    if set_fields:
                        update_doc["$set"] = set_fields
                    if update_doc:
                        self.mongo.db.global_bot_memory.update_one({"_id": "global_state"}, update_doc, upsert=True)
                except Exception as e:
                    logger.error(f"MongoDB memory update error: {e}")
            self._save_to_sqlite(self.cache)


# =====================================================================
# NEWS SCANNER
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
            return {"articles": [], "fresh": False, "fear_greed": self.fear_greed}

    def _analyze_sentiment(self, text):
        bullish = ["bullish", "breakout", "surge", "buy", "accumulate", "rally", "green", "etf", "approval", "inflow"]
        bearish = ["bearish", "crash", "dump", "sell", "liquidation", "drop", "red", "sec", "hack", "outflow"]
        text = text.lower()
        score = sum(1 for w in bullish if w in text) - sum(1 for w in bearish if w in text)
        return max(-100, min(100, score * 20))


# =====================================================================
# WEBSOCKET STREAMS (FIXED: Incremental Volume Delta Tracking)
# =====================================================================
class BinanceFuturesStream:
    def __init__(self, on_data=None):
        self.symbols = [s.lower() for s in Config.ASSETS]
        self.ws = None
        self.running = False
        self.data = {'open_interest': {}, 'liquidations': deque(maxlen=200), 'cvd': {}, 'last_trade': {}}
        self.oi_history = {s: deque(maxlen=12) for s in self.symbols}
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
            try:
                self.ws.close()
            except Exception:
                pass

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
            elif e == 'aggTrade':
                symbol = data.get('s')
                if symbol:
                    price = float(data.get('p', 0))
                    qty = float(data.get('q', 0))
                    is_buyer_maker = data.get('m', False)
                    with self.lock:
                        # True taker delta: if buyer is maker (m=True), aggressive order was a SELL (-qty)
                        delta = -qty if is_buyer_maker else qty
                        self.data['cvd'][symbol.lower()] = self.data['cvd'].get(symbol.lower(), 0.0) + delta
                        self.data['last_trade'][symbol.lower()] = price
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
                logger.warning("Futures WS stale, forcing reconnect...")
                if self.ws:
                    try:
                        self.ws.close()
                    except Exception:
                        pass
                self.reconnect_count += 1

    def get_open_interest(self, symbol):
        with self.lock:
            return self.data['open_interest'].get(symbol.lower(), 0)

    def get_oi_trend(self, symbol):
        with self.lock:
            hist = list(self.oi_history.get(symbol.lower(), []))
            return hist[-1] - hist[0] if len(hist) >= 2 else 0

    def get_cvd(self, symbol):
        with self.lock:
            return self.data['cvd'].get(symbol.lower(), 0.0)

    def get_liquidations(self, symbol, lookback_seconds=120):
        with self.lock:
            now = time.time()
            return [e for e in self.data['liquidations']
                    if e['symbol'].upper() == symbol.upper() and (now - e['time']) <= lookback_seconds]


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
        # Track cumulative kline volume per 1m bar to compute accurate incremental delta volume
        self._last_kline_state = {}

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
        logger.info("✅ Public Spot WebSocket connected.")

    def _on_msg(self, ws, msg):
        with self.lock:
            self.last_ping = time.time()
            self.last_tick_time = time.time()
            self.tick_counter += 1
        try:
            k = json.loads(msg)["data"]["k"]
            symbol = k["s"]
            if symbol in Config.ASSETS:
                close_price = float(k["c"])
                cum_vol = float(k["v"])
                kline_start = int(k["t"]) // 1000

                # Compute true incremental volume delta for this tick
                prev_start, prev_vol = self._last_kline_state.get(symbol, (kline_start, 0.0))
                if kline_start != prev_start:
                    delta_vol = cum_vol
                else:
                    delta_vol = max(0.0, cum_vol - prev_vol)
                self._last_kline_state[symbol] = (kline_start, cum_vol)

                self.on_price_update(symbol, close_price, delta_vol)
                if self.tick_counter % 100 == 0:
                    logger.info(f"📊 Public WS ticks: {self.tick_counter} (last: {symbol} @ {close_price})")
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
                logger.warning(f"Public WS no ping/pong for {age:.0f}s, enabling REST fallback")
                self.rest_fallback = True
                if self.ws:
                    try:
                        self.ws.close()
                    except Exception:
                        pass

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
                    resp = requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}", timeout=5)
                    if resp.status_code == 200:
                        data = resp.json()
                        self.on_price_update(symbol, float(data["price"]), 0.0)
            except Exception as e:
                logger.error(f"REST fallback error: {e}")


# =====================================================================
# CANDLE TOPOLOGY ENGINE (FIXED: Latest ATR, Wilder RSI, High-Vol Anchor)
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
            for tf in self.candle_just_closed:
                self.candle_just_closed[tf][asset] = False

            for timeframe in [60, 300, 900, 3600, 14400]:
                storage = self.candles[timeframe][asset]
                start = (now // timeframe) * timeframe
                if not storage or storage[-1].get("timestamp") != start:
                    if storage and not storage[-1].get("complete", False):
                        storage[-1]["complete"] = True
                        self.candle_just_closed[timeframe][asset] = True
                        if timeframe == 300:
                            self._detect_high_volume_anchor(asset, storage[-1])
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

            if any(self.candle_just_closed[tf][asset] for tf in [60, 300, 900]):
                self._update_volume_ma(asset)
                self._update_pivots(asset, price)
                self._update_support_resistance(asset, price)
                self._detect_bos_choch(asset)
            self.last_tick_time[asset] = now

    def initialize_structure(self, asset):
        """Run initial structure computation right after historical backfill."""
        with self.lock:
            c15 = self.get_completed(asset, 900)
            if not c15:
                return
            price = c15[-1]["close"]
            self._update_volume_ma(asset)
            self._update_pivots(asset, price)
            self._update_support_resistance(asset, price)
            self._detect_bos_choch(asset)

    def _detect_high_volume_anchor(self, asset, candle):
        vol_ma = self.volume_ma.get(asset, 0)
        if vol_ma <= 0:
            return
        if candle["volume"] > 2.0 * vol_ma:
            levels = [
                {"price": candle["high"], "type": "high"},
                {"price": candle["low"], "type": "low"}
            ]
            if abs(candle["close"] - candle["open"]) / (candle["high"] - candle["low"] + 1e-6) > 0.5:
                levels.append({"price": candle["close"], "type": "close"})
            for lvl in levels:
                if not any(abs(lvl["price"] - existing["price"]) / existing["price"] < 0.002
                           for existing in self.high_volume_levels[asset]):
                    self.high_volume_levels[asset].append({
                        "price": lvl["price"], "type": lvl["type"], "timestamp": int(time.time())
                    })
            if len(self.high_volume_levels[asset]) > 10:
                self.high_volume_levels[asset] = self.high_volume_levels[asset][-10:]

    def check_anchor_line_retest(self, asset, price, direction=None):
        tolerance = Config.ANCHOR_RETEST_TOLERANCE
        with self.lock:
            for lvl in self.high_volume_levels.get(asset, []):
                level_price = lvl["price"]
                if abs(price - level_price) / level_price <= tolerance:
                    if direction:
                        if direction == "BUY" and lvl["type"] in ("low", "close"):
                            return True, level_price
                        elif direction == "SELL" and lvl["type"] in ("high", "close"):
                            return True, level_price
                    else:
                        return True, level_price
            return False, None

    def _update_volume_ma(self, asset):
        with self.lock:
            completed = self.get_completed(asset, 300)
            if len(completed) >= 20:
                self.volume_ma[asset] = sum(c["volume"] for c in completed[-20:]) / 20.0
            else:
                self.volume_ma[asset] = 0.0

    def _update_pivots(self, asset, price):
        with self.lock:
            complete = self.get_completed(asset, 900)
            if len(complete) < 10:
                return
            recent = complete[-60:]
            highs, lows = [], []
            for i in range(2, len(recent) - 2):
                if (recent[i - 2]["high"] < recent[i]["high"] > recent[i + 2]["high"] and
                        recent[i - 1]["high"] < recent[i]["high"] > recent[i + 1]["high"]):
                    highs.append(recent[i]["high"])
                if (recent[i - 2]["low"] > recent[i]["low"] < recent[i + 2]["low"] and
                        recent[i - 1]["low"] > recent[i]["low"] < recent[i + 1]["low"]):
                    lows.append(recent[i]["low"])
            self.pivots[asset]["high"] = highs[-5:] if highs else []
            self.pivots[asset]["low"] = lows[-5:] if lows else []

    def _detect_bos_choch(self, asset):
        with self.lock:
            h, l = self.pivots[asset]["high"], self.pivots[asset]["low"]
            if len(h) >= 2 and len(l) >= 2:
                if h[-1] > h[-2]:
                    self.bos[asset]["direction"] = "UP"
                elif l[-1] < l[-2]:
                    self.bos[asset]["direction"] = "DOWN"
                if len(h) >= 3 and len(l) >= 3:
                    self.choch[asset] = (h[-1] > h[-2] and l[-2] < l[-3]) or (l[-1] < l[-2] and h[-2] > h[-3])

    def _update_support_resistance(self, asset, price):
        with self.lock:
            all_levels = list(self.pivots[asset]["high"]) + list(self.pivots[asset]["low"])
            recent = self.get_completed(asset, 900)[-15:]
            for c in recent:
                all_levels.append(c["high"])
                all_levels.append(c["low"])
            clusters = []
            for level in sorted(all_levels):
                if not clusters or abs(level - clusters[-1]) / max(level, 1e-6) > 0.004:
                    clusters.append(level)
            self.support_resistance[asset]["support"] = [lvl for lvl in clusters if lvl < price * 0.998]
            self.support_resistance[asset]["resistance"] = [lvl for lvl in clusters if lvl > price * 1.002]

    def get_completed(self, asset, tf):
        with self.lock:
            return [c for c in self.candles[tf][asset] if c.get("complete", False)]

    def detect_candle_patterns(self, asset, tf=900):
        with self.lock:
            completed = self.get_completed(asset, tf)
            if len(completed) < 2:
                return {}
            last = completed[-1]
            prev = completed[-2]
            patterns = {}
            total = last["high"] - last["low"]
            body = abs(last["close"] - last["open"])
            if total > 0:
                lower_wick = min(last["open"], last["close"]) - last["low"]
                upper_wick = last["high"] - max(last["open"], last["close"])
                if lower_wick / total > 0.55:
                    patterns["bullish_pinbar"] = 1
                if upper_wick / total > 0.55:
                    patterns["bearish_pinbar"] = 1
                # Bullish / Bearish Engulfing
                if last["close"] > last["open"] and prev["close"] < prev["open"] and body > abs(prev["close"] - prev["open"]):
                    patterns["bullish_engulfing"] = 1
                if last["close"] < last["open"] and prev["close"] > prev["open"] and body > abs(prev["close"] - prev["open"]):
                    patterns["bearish_engulfing"] = 1
            return patterns

    def get_atr(self, asset, period=14, tf=3600):
        """FIXED: Calculates ATR on the LATEST `period` completed candles."""
        with self.lock:
            complete = self.get_completed(asset, tf)
            if len(complete) < period + 1:
                return 0.0
            recent = complete[-(period + 1):]
            tr_list = []
            for i in range(1, len(recent)):
                high, low = recent[i]["high"], recent[i]["low"]
                prev_close = recent[i - 1]["close"]
                tr_list.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
            return sum(tr_list) / len(tr_list) if tr_list else 0.0

    def detect_liquidity_sweep(self, asset, price):
        with self.lock:
            completed = self.get_completed(asset, 900)
            h, l = self.pivots[asset]["high"], self.pivots[asset]["low"]
            if not completed:
                return ""
            last = completed[-1]
            # Bullish sweep: swept below recent pivot low but closed back above it
            if l and last["low"] < min(l[-2:]) and last["close"] > min(l[-2:]):
                return "BUY_SWEEP"
            # Bearish sweep: swept above recent pivot high but closed back below it
            if h and last["high"] > max(h[-2:]) and last["close"] < max(h[-2:]):
                return "SELL_SWEEP"
            return ""

    def _ema(self, series, period):
        if len(series) < period:
            return []
        ema = [sum(series[:period]) / period]
        m = 2.0 / (period + 1)
        for i in range(period, len(series)):
            ema.append((series[i] - ema[-1]) * m + ema[-1])
        return ema

    def check_1m_rejection(self, asset, direction):
        with self.lock:
            completed = self.get_completed(asset, 60)
            if not completed:
                return False
            last = completed[-1]
            r = last["high"] - last["low"]
            if r <= 0:
                return False
            if direction == "BUY":
                return (min(last["open"], last["close"]) - last["low"]) / r >= 0.4
            else:
                return (last["high"] - max(last["open"], last["close"])) / r >= 0.4

    def get_visual_topology(self, asset, price, direction, sl, tp, patterns):
        with self.lock:
            min_price = min(price, sl, tp) * 0.995
            max_price = max(price, sl, tp) * 1.005
            if max_price - min_price <= 0:
                min_price = price * 0.98
                max_price = price * 1.02
            rows = 8
            lines = [
                "┌──────────────────────────────────────┐",
                "│       📊 LIVE TOPOLOGY MAP           │",
                "├──────────────────────────────────────┤"
            ]
            for i in range(rows, -1, -1):
                level = min_price + (max_price - min_price) * (i / rows)
                marker = " "
                if i == min(range(rows + 1), key=lambda x: abs(min_price + (max_price - min_price) * (x / rows) - price)):
                    marker = "●"
                elif i == min(range(rows + 1), key=lambda x: abs(min_price + (max_price - min_price) * (x / rows) - sl)):
                    marker = "▼"
                elif i == min(range(rows + 1), key=lambda x: abs(min_price + (max_price - min_price) * (x / rows) - tp)):
                    marker = "★"
                bar = "█" * int((i / rows) * 8) if i > 0 else ""
                lines.append(f"│ {level:>9.2f} │ {marker} {bar:<8} │")
            lines += [
                "├──────────────────────────────────────┤",
                "│ ●=Entry  ▼=StopLoss  ★=TakeProfit   │",
                "└──────────────────────────────────────┘"
            ]
            return "\n".join(lines)

    def get_adx(self, asset, tf, period=14):
        with self.lock:
            complete = self.get_completed(asset, tf)
            if len(complete) < period * 2:
                return 20.0
            recent = complete[-(period * 3):]
            tr_list, dm_plus, dm_minus = [], [], []
            for i in range(1, len(recent)):
                high, low = recent[i]["high"], recent[i]["low"]
                prev_high, prev_low = recent[i - 1]["high"], recent[i - 1]["low"]
                prev_close = recent[i - 1]["close"]
                tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                tr_list.append(tr)
                up = high - prev_high
                down = prev_low - low
                dm_plus.append(max(up, 0) if up > down else 0)
                dm_minus.append(max(down, 0) if down > up else 0)
            atr = sum(tr_list[:period]) / period
            dp = sum(dm_plus[:period]) / period
            dm = sum(dm_minus[:period]) / period
            dx_list = []
            for i in range(period, len(tr_list)):
                atr = (atr * (period - 1) + tr_list[i]) / period
                dp = (dp * (period - 1) + dm_plus[i]) / period
                dm = (dm * (period - 1) + dm_minus[i]) / period
                if atr > 0:
                    di_p = (dp / atr) * 100
                    di_m = (dm / atr) * 100
                    dx = (abs(di_p - di_m) / (di_p + di_m)) * 100 if (di_p + di_m) > 0 else 0
                    dx_list.append(dx)
            return round(sum(dx_list[-period:]) / len(dx_list[-period:]), 1) if dx_list else 20.0

    def detect_fvg(self, asset):
        with self.lock:
            complete = self.get_completed(asset, 900)
            if len(complete) < 5:
                return []
            fvgs = []
            recent = complete[-25:]
            for i in range(2, len(recent)):
                c1, c2, c3 = recent[i - 2], recent[i - 1], recent[i]
                # Bullish FVG: Candle 1 High < Candle 3 Low
                if c3["low"] > c1["high"] and c2["close"] > c2["open"]:
                    fvgs.append({"type": "bullish", "upper": c3["low"], "lower": c1["high"]})
                # Bearish FVG: Candle 1 Low > Candle 3 High
                if c3["high"] < c1["low"] and c2["close"] < c2["open"]:
                    fvgs.append({"type": "bearish", "upper": c1["low"], "lower": c3["high"]})
            return fvgs[-5:]

    def detect_order_block(self, asset):
        with self.lock:
            complete = self.get_completed(asset, 900)
            if len(complete) < 15:
                return {}
            atr = self.get_atr(asset, period=14, tf=900)
            if atr <= 0:
                return {}
            recent = complete[-20:]
            for i in range(len(recent) - 2, 1, -1):
                c = recent[i]
                next_c = recent[i + 1]
                # Strong displacement candle preceded by opposite candle = Order Block
                if (next_c["high"] - next_c["low"]) > 1.3 * atr:
                    if next_c["close"] > next_c["open"] and c["close"] < c["open"]:
                        return {"type": "bullish", "high": c["high"], "low": c["low"]}
                    elif next_c["close"] < next_c["open"] and c["close"] > c["open"]:
                        return {"type": "bearish", "high": c["high"], "low": c["low"]}
            return {}

    def _calc_rsi(self, closes, period=14):
        """FIXED: Wilder's Smoothed RSI (matches TradingView)."""
        if len(closes) < period + 1:
            return 50.0
        deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [max(d, 0.0) for d in deltas]
        losses = [abs(min(d, 0.0)) for d in deltas]

        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100.0 - (100.0 / (1.0 + rs)), 2)


# =====================================================================
# INDICATOR CACHE
# =====================================================================
class IndicatorCache:
    def __init__(self, topology):
        self.topology = topology
        self.cache = {}
        self.lock = threading.Lock()
        self.cache_ttl = 30

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
                completed = self.topology.get_completed(asset, tf)
                data = {}
                closes = [c['close'] for c in completed[-150:]]
                if len(closes) >= 20:
                    for p in [9, 20, 21, 50]:
                        if len(closes) >= p:
                            ema_list = self.topology._ema(closes, p)
                            if ema_list:
                                data[f'ema_{p}'] = ema_list[-1]
                data['atr'] = self.topology.get_atr(asset, period=14, tf=tf) or 0.0
                data['adx'] = self.topology.get_adx(asset, tf, period=14) or 20.0
                data['rsi'] = self.topology._calc_rsi(closes) if len(closes) >= 15 else 50.0
                sr = self.topology.support_resistance.get(asset, {})
                data['support'] = sr.get('support', [])
                data['resistance'] = sr.get('resistance', [])
                data['bos'] = self.topology.bos.get(asset, {}).get('direction', '')
                data['choch'] = self.topology.choch.get(asset, False)
                data['fvg'] = self.topology.detect_fvg(asset)
                data['order_block'] = self.topology.detect_order_block(asset)
                data['volume_ma'] = self.topology.volume_ma.get(asset, 0.0)
                return data
        except Exception as e:
            logger.error(f"Indicator cache error: {e}")
            return {}


# =====================================================================
# NEUTRAL CANDLE ENGINE
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
            if range_ <= 0:
                return {'pattern': 'None', 'direction': '', 'score': 0, 'reason': 'Zero range'}

            upper_wick = high - max(open_, close)
            lower_wick = min(open_, close) - low
            body_pct = body / range_

            pattern = 'None'
            direction = ''
            score = 0

            if body_pct <= 0.08:
                if upper_wick > 2.5 * lower_wick and upper_wick > body:
                    pattern = 'GravestoneDoji'
                    direction = 'SELL'
                    score = 8
                elif lower_wick > 2.5 * upper_wick and lower_wick > body:
                    pattern = 'DragonflyDoji'
                    direction = 'BUY'
                    score = 8
                else:
                    pattern = 'Doji'
                    score = 2
            elif body_pct <= 0.35:
                if upper_wick > lower_wick * 2.0 and upper_wick > body * 1.5:
                    pattern = 'ShootingStar'
                    direction = 'SELL'
                    score = 8
                elif lower_wick > upper_wick * 2.0 and lower_wick > body * 1.5:
                    pattern = 'Hammer'
                    direction = 'BUY'
                    score = 8
                else:
                    pattern = 'SpinningTop'
                    score = 2

            return {
                'pattern': pattern,
                'direction': direction,
                'score': score,
                'reason': f"{pattern} ({direction or 'neutral'})"
            }


# =====================================================================
# UPGRADED 4-LAYER SMART SIGNAL ENGINE
# =====================================================================
class SimpleSignalEngine:
    def __init__(self, topology, futures_stream, absorption_meter, neutral_engine):
        self.topology = topology
        self.futures = futures_stream
        self.absorption = absorption_meter
        self.neutral = neutral_engine
        self.multi_oi = MultiExchangeOIFetcher()
        self._prev_cvd = {}

    def _detect_regime(self, asset: str, price: float, last_15m_vol: float, c15: list,
                       rsi_15: float, ema20_15: float, atr_15: float, oi_data: dict) -> dict:
        result = {
            "regime": "RANGE",
            "parabolic": False,
            "overextended": False,
            "vol_expansion": False,
            "oi_expanding": False
        }
        if len(c15) < 20:
            return result

        vol_ma = sum(c['volume'] for c in c15[-20:]) / 20.0
        result["vol_expansion"] = vol_ma > 0 and last_15m_vol > (vol_ma * Config.VOLUME_EXPANSION_RATIO)

        dist_pct = (abs(price - ema20_15) / ema20_15 * 100) if ema20_15 > 0 else 0
        atr_pct = (atr_15 / price * 100) if price > 0 else 1.0

        result["parabolic"] = (
            result["vol_expansion"] and
            (rsi_15 >= Config.PARABOLIC_RSI_THRESHOLD or rsi_15 <= (100 - Config.PARABOLIC_RSI_THRESHOLD)) and
            dist_pct > (0.8 * atr_pct)
        )

        result["overextended"] = (
            dist_pct > (Config.OVEREXTENSION_ATR_MULT * atr_pct) and
            not result["vol_expansion"]
        )

        valid_oi = [v for v in oi_data.values() if v is not None and v > 0]
        oi_trend = self.futures.get_oi_trend(asset.lower())
        result["oi_expanding"] = len(valid_oi) >= 1 and oi_trend >= 0

        if result["parabolic"] and result["oi_expanding"]:
            result["regime"] = "MOMENTUM"
        elif result["overextended"]:
            result["regime"] = "REVERSAL"

        return result

    def evaluate(self, asset: str, price: float, volume: float):
        """
        4-Layer Institutional Evaluation (Non-blocking OI fetch + Full SMC & Order Flow)
        """
        # Fetch OI OUTSIDE topology lock so network calls never freeze WebSocket threads
        oi_data = self.multi_oi.fetch_composite_oi(asset)
        abs_meter = self.absorption.get_meter(asset)
        neutral_info = self.neutral.detect(asset, price, tf=900)

        breakdown = {}
        with self.topology.lock:
            c15 = self.topology.get_completed(asset, 900)
            if len(c15) < 55:
                return {"status": "NO SETUP", "direction": None, "score": 0, "reason": "Insufficient 15m data", "breakdown": {}}
            c1h = self.topology.get_completed(asset, 3600)
            if len(c1h) < 55:
                return {"status": "NO SETUP", "direction": None, "score": 0, "reason": "Insufficient 1h data", "breakdown": {}}

            closes_15 = [c['close'] for c in c15[-150:]]
            closes_1h = [c['close'] for c in c1h[-150:]]
            last_15m_vol = c15[-1]["volume"]

            ema20_15 = self.topology._ema(closes_15, 20)[-1]
            ema50_15 = self.topology._ema(closes_15, 50)[-1]
            ema20_1h = self.topology._ema(closes_1h, 20)[-1]
            ema50_1h = self.topology._ema(closes_1h, 50)[-1]
            rsi_15 = self.topology._calc_rsi(closes_15, 14)
            atr_15 = self.topology.get_atr(asset, period=14, tf=900) or (price * 0.008)

            regime = self._detect_regime(asset, price, last_15m_vol, c15, rsi_15, ema20_15, atr_15, oi_data)
            breakdown["Regime"] = regime["regime"]

            # ATR Chop Filter in RANGE mode
            if regime["regime"] == "RANGE" and abs(price - ema20_15) <= (Config.ATR_NOISE_MULTIPLIER * atr_15):
                return {"status": "NO SETUP", "direction": None, "score": 0, "reason": "Chop Zone: Near EMA20", "breakdown": breakdown}

            trend_15_bull = ema20_15 > ema50_15
            trend_1h_bull = ema20_1h > ema50_1h

            # ---- LAYER 1: TREND & MTF ALIGNMENT (Max 30 pts) ----
            if trend_15_bull and trend_1h_bull and rsi_15 >= 52:
                direction = "BUY"
                trend_score = 30
            elif not trend_15_bull and not trend_1h_bull and rsi_15 <= 48:
                direction = "SELL"
                trend_score = 30
            elif trend_15_bull and rsi_15 >= 62 and price > ema20_15:
                direction = "BUY"
                trend_score = 24
                breakdown["TrendNotice"] = "15m Momentum Breakout"
            elif not trend_15_bull and rsi_15 <= 38 and price < ema20_15:
                direction = "SELL"
                trend_score = 24
                breakdown["TrendNotice"] = "15m Momentum Breakdown"
            elif regime["regime"] == "REVERSAL" and (rsi_15 >= 74 or rsi_15 <= 26):
                direction = "SELL" if rsi_15 >= 74 else "BUY"
                trend_score = 22
                breakdown["TrendNotice"] = "Extreme RSI Reversal"
            else:
                return {"status": "NO SETUP", "direction": None, "score": 0, "reason": "MTF Trend Conflict", "breakdown": breakdown}

            breakdown["Trend"] = trend_score

            # ---- LAYER 2: SMC LOCATION (S/R, Order Block, FVG, Anchor) (Max 25 pts) ----
            loc_score = 0
            loc_reasons = []
            sr = self.topology.support_resistance[asset]
            nearest_support = max(sr["support"]) if sr["support"] else None
            nearest_resistance = min(sr["resistance"]) if sr["resistance"] else None
            dist_from_ema = (abs(price - ema20_15) / ema20_15 * 100) if ema20_15 > 0 else 0
            atr_pct = (atr_15 / price * 100) if price > 0 else 1.0

            if regime["regime"] == "MOMENTUM":
                loc_score += 18 if dist_from_ema > atr_pct else 14
                loc_reasons.append("MomentumZone")
            elif regime["regime"] == "REVERSAL":
                if direction == "SELL" and nearest_resistance and abs(price - nearest_resistance) <= 1.5 * atr_15:
                    loc_score += 18
                    loc_reasons.append("ResistanceReversal")
                elif direction == "BUY" and nearest_support and abs(price - nearest_support) <= 1.5 * atr_15:
                    loc_score += 18
                    loc_reasons.append("SupportReversal")
            else:
                if direction == "BUY" and nearest_support and abs(price - nearest_support) <= 1.8 * atr_15:
                    loc_score += 12
                    loc_reasons.append("NearSupport")
                elif direction == "SELL" and nearest_resistance and abs(price - nearest_resistance) <= 1.8 * atr_15:
                    loc_score += 12
                    loc_reasons.append("NearResistance")

            # Order Block & FVG Confluence
            ob = self.topology.detect_order_block(asset)
            if ob:
                if direction == "BUY" and ob.get("type") == "bullish" and ob["low"] * 0.995 <= price <= ob["high"] * 1.008:
                    loc_score += 8
                    loc_reasons.append("BullishOB")
                elif direction == "SELL" and ob.get("type") == "bearish" and ob["low"] * 0.992 <= price <= ob["high"] * 1.005:
                    loc_score += 8
                    loc_reasons.append("BearishOB")

            fvgs = self.topology.detect_fvg(asset)
            for fvg in fvgs:
                if direction == "BUY" and fvg["type"] == "bullish" and fvg["lower"] <= price <= fvg["upper"] * 1.005:
                    loc_score += 6
                    loc_reasons.append("BullishFVG")
                    break
                elif direction == "SELL" and fvg["type"] == "bearish" and fvg["lower"] * 0.995 <= price <= fvg["upper"]:
                    loc_score += 6
                    loc_reasons.append("BearishFVG")
                    break

            anchor_ok, _ = self.topology.check_anchor_line_retest(asset, price, direction)
            if anchor_ok:
                loc_score += 6
                loc_reasons.append("HighVolAnchor")

            loc_score = min(loc_score, 25)
            breakdown["Location"] = loc_score
            breakdown["LocationDetails"] = " | ".join(loc_reasons) if loc_reasons else "Standard"

            # ---- LAYER 3: ORDER FLOW (CVD, OI, Absorption, Volume) (Max 20 pts) ----
            conf_score = 0
            vol_reasons = []

            if regime["vol_expansion"]:
                conf_score += 7
                vol_reasons.append("VolExpansion")

            current_cvd = self.futures.get_cvd(asset.lower())
            prev_cvd = self._prev_cvd.get(asset, current_cvd)
            prev_price = c15[-2]['close'] if len(c15) >= 2 else price
            self._prev_cvd[asset] = current_cvd

            cvd_delta = current_cvd - prev_cvd
            cvd_exhausted = False
            if direction == "BUY":
                if cvd_delta >= 0 or current_cvd > 0:
                    conf_score += 6
                    vol_reasons.append("PositiveCVD")
                if price > prev_price and cvd_delta < 0 and current_cvd < 0:
                    cvd_exhausted = True
            else:
                if cvd_delta <= 0 or current_cvd < 0:
                    conf_score += 6
                    vol_reasons.append("NegativeCVD")
                if price < prev_price and cvd_delta > 0 and current_cvd > 0:
                    cvd_exhausted = True

            if cvd_exhausted and regime["regime"] != "MOMENTUM":
                conf_score -= Config.CVD_EXHAUSTION_PENALTY
                vol_reasons.append("CVDDivergencePenalty")

            if abs_meter.get("direction") == direction and abs_meter.get("score", 0) >= Config.ABSORPTION_MIN_SCORE:
                conf_score += 5
                vol_reasons.append(f"Absorption({abs_meter['score']})")

            valid_oi_count = sum(1 for v in oi_data.values() if v is not None and v > 0)
            if valid_oi_count >= 2:
                conf_score += 4
                vol_reasons.append(f"MultiOI({valid_oi_count}x)")

            conf_score = max(0, min(conf_score, 20))
            breakdown["VolumeOrderFlow"] = conf_score
            breakdown["VolFlowDetails"] = " | ".join(vol_reasons) if vol_reasons else "Neutral"

            # ---- LAYER 4: STRUCTURE & CANDLE PATTERNS (Max 15 pts) + RISK (Max 10 pts) ----
            struct_score = 0
            struct_reasons = []

            if self.topology.choch.get(asset, False):
                struct_score += 6
                struct_reasons.append("CHoCH")
            bos = self.topology.bos[asset]["direction"]
            if (direction == "BUY" and bos == "UP") or (direction == "SELL" and bos == "DOWN"):
                struct_score += 6
                struct_reasons.append("BOS")

            sweep = self.topology.detect_liquidity_sweep(asset, price)
            if (direction == "BUY" and sweep == "BUY_SWEEP") or (direction == "SELL" and sweep == "SELL_SWEEP"):
                struct_score += 7
                struct_reasons.append(sweep)

            patterns = self.topology.detect_candle_patterns(asset, tf=900)
            if direction == "BUY" and ("bullish_pinbar" in patterns or "bullish_engulfing" in patterns):
                struct_score += 5
                struct_reasons.append("BullishCandle")
            elif direction == "SELL" and ("bearish_pinbar" in patterns or "bearish_engulfing" in patterns):
                struct_score += 5
                struct_reasons.append("BearishCandle")

            if neutral_info.get("direction") == direction:
                struct_score += 4
                struct_reasons.append(neutral_info["pattern"])

            struct_score = min(struct_score, 15)
            breakdown["CandleStructure"] = struct_score
            breakdown["StructureDetails"] = " | ".join(struct_reasons) if struct_reasons else "None"

            risk_score = 10 if dist_from_ema < (2.2 * atr_pct) else 5
            breakdown["Risk"] = risk_score

            # ---- FINAL SCORE & TIER CLASSIFICATION ----
            total_score = trend_score + loc_score + conf_score + struct_score + risk_score
            breakdown["Total"] = total_score
            breakdown["Direction"] = direction

            if regime["regime"] == "MOMENTUM":
                thresh_extreme, thresh_high, thresh_valid = 70, 58, 50
            elif regime["regime"] == "REVERSAL":
                thresh_extreme, thresh_high, thresh_valid = 78, 68, 58
            else:
                thresh_extreme, thresh_high, thresh_valid = Config.SCORE_EXTREME, Config.SCORE_HIGH, Config.SCORE_VALID

            if total_score >= thresh_extreme:
                status = "EXTREME"
            elif total_score >= thresh_high:
                status = "HIGH"
            elif total_score >= thresh_valid:
                status = "VALID"
            elif total_score >= Config.SCORE_WATCH:
                status = "WATCH"
            else:
                status = "REJECTED"

            reason = (f"Regime:{regime['regime']} | Trend:{trend_score} | Loc:{loc_score} "
                      f"| Flow:{conf_score} | Struct:{struct_score} | Risk:{risk_score}")
            return {
                "status": status,
                "direction": direction,
                "score": total_score,
                "reason": reason,
                "breakdown": breakdown,
                "patterns": patterns
            }


# =====================================================================
# DYNAMIC STOP LOSS & STRUCTURE TARGETS
# =====================================================================
class DynamicStopLoss:
    def __init__(self, topology):
        self.topology = topology

    def calculate(self, asset, direction, entry, atr):
        buffer = max(atr * 0.8, entry * 0.004)
        with self.topology.lock:
            sr = self.topology.support_resistance[asset]
            nearest_support = None
            nearest_resistance = None
            if sr["support"]:
                candidates = [s for s in sr["support"] if s < entry and (entry - s) / entry < 0.06]
                if candidates:
                    nearest_support = max(candidates)
            if sr["resistance"]:
                candidates = [r for r in sr["resistance"] if r > entry and (r - entry) / entry < 0.06]
                if candidates:
                    nearest_resistance = min(candidates)

        default_sl = entry + 1.5 * atr if direction == "SELL" else entry - 1.5 * atr
        if direction == "SELL":
            sl = (nearest_resistance + 0.4 * atr) if nearest_resistance else default_sl
            if sl - entry > 1.8 * atr:
                sl = default_sl
            sl = max(sl, entry + buffer)
        else:
            sl = (nearest_support - 0.4 * atr) if nearest_support else default_sl
            if entry - sl > 1.8 * atr:
                sl = default_sl
            sl = min(sl, entry - buffer)

        risk = abs(entry - sl)
        if risk < buffer * 0.5:
            sl = entry - buffer if direction == "BUY" else entry + buffer
            risk = buffer

        if direction == "BUY":
            tp = entry + max(Config.MIN_RR, 2.2) * risk
            if nearest_resistance and (nearest_resistance - entry) >= Config.MIN_RR * risk:
                tp = min(tp * 1.02, nearest_resistance)
        else:
            tp = entry - max(Config.MIN_RR, 2.2) * risk
            if nearest_support and (entry - nearest_support) >= Config.MIN_RR * risk:
                tp = max(tp * 0.98, nearest_support)

        rr = abs(tp - entry) / risk if risk > 0 else Config.MIN_RR
        if rr < Config.MIN_RR:
            tp = entry + Config.MIN_RR * risk if direction == "BUY" else entry - Config.MIN_RR * risk
            rr = Config.MIN_RR

        return sl, tp, risk, rr


# =====================================================================
# PENDING VERIFICATION QUEUE (FIXED: Exact 5m Candle Timestamp Check)
# =====================================================================
class PendingVerificationQueue:
    def __init__(self, topology):
        self.topology = topology
        self.pending = {}
        self.verified = []
        self.rejected_pending = []
        self.lock = threading.Lock()

    def add_signal(self, signal_data):
        with self.lock:
            asset = signal_data['asset']
            completed = self.topology.get_completed(asset, 300)
            if len(completed) < 2:
                return False
            signal_data['base_volume'] = completed[-1]["volume"]
            signal_data['added_5m_ts'] = completed[-1]["timestamp"]
            signal_data['candles_verified'] = 0
            signal_data['start_price'] = signal_data['entry']
            key = f"{asset}_{signal_data['direction']}_{int(time.time())}"
            self.pending[key] = signal_data
            return key

    def check_pending(self, asset):
        to_remove = []
        with self.lock:
            completed = self.topology.get_completed(asset, 300)
            if len(completed) < 2:
                return []
            latest_candle = completed[-1]

            for key, data in list(self.pending.items()):
                if data['asset'] != asset:
                    continue
                if latest_candle["timestamp"] <= data.get("added_5m_ts", 0):
                    continue

                data['candles_verified'] += 1
                data['added_5m_ts'] = latest_candle["timestamp"]
                limit = data.get('pending_candles', Config.PENDING_VERIFICATION_CANDLES)
                vol_decay = data.get('volume_decay_threshold', Config.VOLUME_DECAY_THRESHOLD)

                # 1. Volume decay check
                if data['base_volume'] > 0 and latest_candle["volume"] < data['base_volume'] * (1.0 - vol_decay):
                    data['reject_reason'] = "5m Verification Failed: Severe Volume Decay"
                    self.rejected_pending.append(data)
                    to_remove.append(key)
                    continue

                # 2. Adverse excursion check on 5m close
                close_5m = latest_candle['close']
                atr_5m = self.topology.get_atr(asset, period=14, tf=300) or (data['start_price'] * 0.005)
                max_adverse = 0.85 * atr_5m

                if data['direction'] == 'BUY' and (data['start_price'] - close_5m) > max_adverse:
                    data['reject_reason'] = "5m Verification Failed: Adverse Bearish Breakdown"
                    self.rejected_pending.append(data)
                    to_remove.append(key)
                    continue
                elif data['direction'] == 'SELL' and (close_5m - data['start_price']) > max_adverse:
                    data['reject_reason'] = "5m Verification Failed: Adverse Bullish Breakout"
                    self.rejected_pending.append(data)
                    to_remove.append(key)
                    continue

                if data['candles_verified'] >= limit:
                    # Update entry price to current verified 5m close
                    data['entry'] = close_5m
                    self.verified.append(data)
                    to_remove.append(key)

            for key in to_remove:
                self.pending.pop(key, None)
            return to_remove

    def get_verified_signals(self):
        with self.lock:
            ready = self.verified[:]
            self.verified = []
            return ready

    def get_rejected_pending(self):
        with self.lock:
            rej = self.rejected_pending[:]
            self.rejected_pending = []
            return rej


# =====================================================================
# TRADE HEALTH ENGINE & ABSORPTION METER
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
                atr = self.topology.get_atr(asset, period=14, tf=3600) or (entry * 0.01)

            unrealized = (current - entry) if direction == "BUY" else (entry - current)
            drawdown_pct = -unrealized / (atr * 2.0) if unrealized < 0 else 0
            health_dd = max(0, 100 - drawdown_pct * 50)

            duration_min = (time.time() - trade.get('entry_time', time.time())) / 60.0
            if duration_min > Config.TRADE_HEALTH_STALE_MINUTES and unrealized <= 0:
                health_time = max(0, 100 - (duration_min - Config.TRADE_HEALTH_STALE_MINUTES) * 1.5)
            else:
                health_time = 100

            ind1h = self.cache.get(asset, 3600, current)
            ema9 = ind1h.get('ema_9', current)
            ema21 = ind1h.get('ema_21', current)
            trend_bull = ema9 > ema21
            health_trend = 100 if ((direction == "BUY" and trend_bull) or (direction == "SELL" and not trend_bull)) else 75

            health = health_dd * 0.5 + health_time * 0.3 + health_trend * 0.2
            return max(0, min(100, int(health)))
        except Exception as e:
            logger.error(f"Health calc error: {e}")
            return 100


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
        liqs = self.futures.get_liquidations(asset, 120)

        score = 0
        direction = ""

        if oi_trend > 0 and cvd > 0:
            score += 45
            direction = "BUY"
        elif oi_trend < 0 and cvd < 0:
            score += 45
            direction = "SELL"
        elif cvd > 0:
            direction = "BUY"
            score += 25
        elif cvd < 0:
            direction = "SELL"
            score += 25

        sell_liqs = sum(1 for l in liqs if l['side'] == 'SELL')
        buy_liqs = sum(1 for l in liqs if l['side'] == 'BUY')
        if direction == "BUY" and sell_liqs > 0:
            score += 25
        elif direction == "SELL" and buy_liqs > 0:
            score += 25

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
            return dict(self.meter_state.get(asset, {"score": 0, "direction": ""}))

    def get_strong_opposite(self, asset, current_direction):
        meter = self.get_meter(asset)
        if meter["score"] >= Config.ABSORPTION_EXIT_SCORE:
            if current_direction == "BUY" and meter["direction"] == "SELL":
                return True
            if current_direction == "SELL" and meter["direction"] == "BUY":
                return True
        return False


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
            if not self.token or not self.chat_id:
                continue
            try:
                requests.post(
                    f"https://api.telegram.org/bot{self.token}/sendMessage",
                    data={"chat_id": self.chat_id, "text": msg, "parse_mode": "HTML"},
                    timeout=10
                )
            except Exception as e:
                logger.error(f"Telegram send error: {e}")

    def send_message(self, text):
        self.queue.put(text)

    def fire_signal(self, asset, direction, price, sl, tp, chart, logic, news,
                    score, patterns, trade_id, session, rr, regime, signal_type="SIMPLE",
                    signal_token=None, tier="HIGH"):
        dir_emoji = "🟢 LONG / BUY" if direction == "BUY" else "🔴 SHORT / SELL"
        tier_badge = "🔥 EXTREME" if tier == "EXTREME" else ("⚡ HIGH" if tier == "HIGH" else "✅ VALID")
        msg = (
            f"🚀 <b>ALPHABOT v8.0 PRO SIGNAL</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>Asset:</b> {Config.DISPLAY_NAMES.get(asset, asset)}\n"
            f"📌 <b>Direction:</b> {dir_emoji}\n"
            f"🏆 <b>Tier:</b> {tier_badge} ({score}/100)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Entry Price:</b> <code>{price:.2f}</code>\n"
            f"🛑 <b>Stop Loss:</b> <code>{sl:.2f}</code>\n"
            f"🎯 <b>Take Profit:</b> <code>{tp:.2f}</code>\n"
            f"⚖️ <b>Risk:Reward:</b> 1:{rr:.2f}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🧠 <b>Breakdown:</b> {html.escape(logic)}\n"
            f"📰 <b>News:</b> {html.escape(news or 'Neutral Market')}\n"
            f"🆔 <code>{signal_token or trade_id}</code>"
        )
        self.queue.put(msg)

    def fire_trade_close(self, trade_id, asset, entry, exit_price, pnl, reason, entry_time):
        hold_min = max(1, int((time.time() - entry_time) / 60))
        pnl_pct = (pnl / entry) * 100 if entry else 0
        status = "🟢 TARGET / PROFIT" if pnl > 0 else "🔴 STOP / LOSS" if pnl < 0 else "⚪ BREAKEVEN"
        msg = (
            f"<b>🔒 Trade #{trade_id} Closed – {Config.DISPLAY_NAMES.get(asset, asset)}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Result:</b> {status}\n"
            f"💵 <b>Entry:</b> {entry:.2f} ➔ <b>Exit:</b> {exit_price:.2f}\n"
            f"💰 <b>Net Move:</b> {pnl:+.2f} ({pnl_pct:+.2f}%)\n"
            f"⏱️ <b>Duration:</b> {hold_min} min\n"
            f"📌 <b>Exit Reason:</b> {html.escape(reason)}"
        )
        self.queue.put(msg)

    def fire_news_alert(self, title, sentiment, fg):
        self.queue.put(f"📰 <b>Crypto News:</b> {html.escape(title)}\n🧠 Sentiment: {sentiment:.0f} | Fear/Greed: {fg}")


# =====================================================================
# ACTIVE TRADE LIFECYCLE
# =====================================================================
class ActiveTradeLifecycle:
    def __init__(self, orchestrator):
        self.orch = orchestrator
        self.check_interval = 30

    def monitor_lifecycle(self):
        while True:
            time.sleep(self.check_interval)
            with self.orch.trade_lock:
                if not self.orch.active_trades:
                    continue
                now = int(time.time())
                to_close = []
                for tid, trade in list(self.orch.active_trades.items()):
                    asset = trade['asset']
                    with self.orch.topology.lock:
                        current_price = (self.orch.topology.history[asset][-1]['price']
                                         if self.orch.topology.history.get(asset) else trade['entry'])
                    trade_duration = now - trade.get('entry_time', now)

                    health = self.orch.health_engine.calculate_health(trade)
                    trade['health'] = health

                    if self.orch.absorption_meter.get_strong_opposite(asset, trade['direction']):
                        pnl = (current_price - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - current_price)
                        to_close.append((tid, current_price, pnl, "Reversal-Absorption"))
                        continue

                    if health < Config.HEALTH_EMERGENCY_THRESHOLD:
                        pnl = (current_price - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - current_price)
                        to_close.append((tid, current_price, pnl, f"EmergencyHealth-{health}%"))
                        continue

                    if trade_duration > Config.MAX_HOLD_TIME:
                        pnl = (current_price - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - current_price)
                        to_close.append((tid, current_price, pnl, "MaxHold-4h"))
                        continue

                    if (trade_duration > Config.TIME_DECAY_SECONDS and
                            abs(current_price - trade['entry']) / max(trade['entry'], 1e-6) < Config.TIME_DECAY_THRESHOLD_PCT):
                        pnl = (current_price - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - current_price)
                        to_close.append((tid, current_price, pnl, "TimeDecay-60m"))
                        continue

                for tid, price, pnl, reason in to_close:
                    self.orch._close_trade(tid, price, pnl, reason)
                gc.collect()


# =====================================================================
# CORE ORCHESTRATOR (FIXED: Live Tick Updates + EXTREME Signals Included)
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
        self.price_queue = queue.Queue(maxsize=2000)
        self.start_time = time.time()

        self.signal_timestamps = deque(maxlen=100)
        self.asset_last_signal = {a: 0 for a in Config.ASSETS}

        self.asset_state = {
            a: {
                "price": 0.0,
                "trend": "NEUTRAL",
                "htf_trend": "NEUTRAL",
                "volume_ratio": 1.0,
                "rsi": 50.0,
                "adx": 20.0,
                "volatility": 0.01,
                "news_sentiment": 0,
                "neutral_pattern": "None",
                "absorption_score": 0,
                "absorption_dir": ""
            } for a in Config.ASSETS
        }
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
        if not open_trades:
            open_trades = self.db.get_open_trades_sqlite()
        with self.trade_lock:
            for t in open_trades:
                tid = t.get('id')
                if not tid:
                    continue
                self.active_trades[tid] = {
                    'id': tid,
                    'asset': t.get('asset', ''),
                    'direction': t.get('direction', ''),
                    'entry': float(t.get('entry', 0)),
                    'sl': float(t.get('stop_loss', 0)),
                    'tp': float(t.get('take_profit', 0)),
                    'entry_time': int(t.get('entry_time', int(time.time()))),
                    'breakeven_locked': False,
                    'trailing_activated': False,
                    'health': 100,
                    'regime': t.get('regime', 'SIMPLE'),
                    'signal_token': t.get('signal_token')
                }
        timestamps = self.db.get_recent_signal_timestamps(86400)
        self.signal_timestamps = deque(timestamps[-100:], maxlen=100)
        logger.info(f"Restored {len(self.active_trades)} open trades, {len(timestamps)} recent signals")

    def _memory_sync_loop(self):
        last_sync = int(time.time())
        while True:
            time.sleep(60)
            now = int(time.time())
            elapsed = now - last_sync
            if elapsed >= 300:
                self.memory.update_state({"total_run_seconds": elapsed, "last_update": now})
                last_sync = now

    def _meter_update_loop(self):
        while True:
            try:
                for asset in Config.ASSETS:
                    with self.topology.lock:
                        price = self.topology.history[asset][-1]['price'] if self.topology.history.get(asset) else 0
                    if price > 0:
                        meter = self.absorption_meter.update_meter(asset, price)
                        self.asset_state[asset]["absorption_score"] = meter["score"]
                        self.asset_state[asset]["absorption_dir"] = meter["direction"]
                time.sleep(20)
            except Exception as e:
                logger.error(f"Meter update error: {e}")
                time.sleep(20)

    def _status_monitor(self):
        while True:
            time.sleep(60)
            last_tick = self.stream.last_tick_time if self.stream and hasattr(self.stream, 'last_tick_time') else 0
            age = time.time() - last_tick if last_tick else -1
            qsize = self.price_queue.qsize()
            logger.info(f"⏱️ STATUS: last tick {age:.0f}s ago | queue: {qsize} | active trades: {len(self.active_trades)} | pending: {len(self.pending_queue.pending)}")

    def _signal_limit_ok(self):
        now = time.time()
        count = sum(1 for ts in self.signal_timestamps if now - ts < 86400)
        return count < Config.MAX_SIGNALS_PER_DAY

    def _asset_cooldown_ok(self, asset):
        last = self.asset_last_signal.get(asset, 0)
        return (time.time() - last) >= (Config.ASSET_COOLDOWN_HOURS * 3600)

    def _handle_price_tick(self, asset, price, volume):
        # 1. ALWAYS update topology and active trades FIRST (Fixed Critical Bug #2)
        self.asset_state[asset]["price"] = price
        self.topology.process_tick(asset, price, volume)
        self._update_active_trades(asset, price)

        # Update dashboard indicators when 1m or 5m candle closes
        if self.topology.candle_just_closed[60].get(asset, False) or self.topology.candle_just_closed[300].get(asset, False):
            self._update_indicators(asset, price)

        # 2. Check if an active trade is already running on this asset before scanning new signals
        with self.trade_lock:
            has_open_trade = any(t['asset'] == asset for t in self.active_trades.values())

        # 3. Check 5m candle close for pending signal verification
        if self.topology.candle_just_closed[300].get(asset, False):
            if self.pending_queue.pending:
                self.pending_queue.check_pending(asset)
                # Log any pending signals that failed 5m verification
                for rej in self.pending_queue.get_rejected_pending():
                    self.db.log_rejected(
                        asset=rej['asset'], price=price, score=rej['score'],
                        reason=rej.get('reject_reason', '5m Verification Failed'),
                        volatility=self.asset_state[asset]["volatility"],
                        regime=rej.get('regime', 'SIMPLE'), gate_name="5mVerification",
                        status="REJECTED", direction=rej['direction'],
                        score_breakdown=rej.get('score_breakdown', '')
                    )
                    self.rejected += 1
                    self.memory.update_state({"rejected_signals_count": 1})
                    logger.info(f"❌ Pending Rejected ({asset}): {rej.get('reject_reason')}")

                verified = self.pending_queue.get_verified_signals()
                for signal in verified:
                    if not has_open_trade:
                        self._send_final_signal(signal)

        # 4. Check 15m candle close for new signal evaluation
        if self.topology.candle_just_closed[900].get(asset, False):
            self._update_indicators(asset, price)
            if has_open_trade:
                return

            result = self.signal_engine.evaluate(asset, price, volume)
            status = result["status"]
            direction = result["direction"]
            score = result["score"]
            reason = result["reason"]
            breakdown = result["breakdown"]
            patterns = result.get("patterns", {})

            # Always log observation to SQLite and MongoDB
            self.db.log_observation(
                asset=asset, price=price,
                direction=direction if direction else "NONE",
                score=score, status=status, reason=reason,
                breakdown=breakdown, volatility=self.asset_state[asset]["volatility"]
            )
            self.mongo.save_observation({
                "asset": asset, "timestamp": int(time.time()), "price": price,
                "direction": direction or "NONE", "score": score, "status": status, "reason": reason
            })
            logger.info(f"🔍 15m Eval [{asset}] | Status: {status} | Score: {score} | Dir: {direction} | {reason}")

            # Reject if status is NO SETUP, REJECTED, or WATCH
            if status in ("NO SETUP", "REJECTED", "WATCH"):
                self.db.log_rejected(
                    asset=asset, price=price, score=score,
                    reason=f"{status}: {reason}",
                    volatility=self.asset_state[asset]["volatility"],
                    regime=breakdown.get("Regime", "SIMPLE"),
                    gate_name="Engine", status=status,
                    direction=direction if direction else "",
                    score_breakdown=json.dumps(breakdown)
                )
                self.rejected += 1
                self.memory.update_state({"rejected_signals_count": 1})
                return

            # FIXED Critical Bug #1: Allow EXTREME, HIGH, and VALID!
            if status not in ("VALID", "HIGH", "EXTREME"):
                return

            if not self._signal_limit_ok():
                self.db.log_rejected(asset, price, score, "Global daily limit reached",
                                     self.asset_state[asset]["volatility"], breakdown.get("Regime", "SIMPLE"),
                                     "DailyLimit", json.dumps(breakdown), status="REJECTED", direction=direction)
                self.rejected += 1
                self.memory.update_state({"rejected_signals_count": 1})
                return

            if not self._asset_cooldown_ok(asset):
                self.db.log_rejected(asset, price, score, f"Asset cooldown ({Config.ASSET_COOLDOWN_HOURS}h)",
                                     self.asset_state[asset]["volatility"], breakdown.get("Regime", "SIMPLE"),
                                     "Cooldown", json.dumps(breakdown), status="REJECTED", direction=direction)
                self.rejected += 1
                self.memory.update_state({"rejected_signals_count": 1})
                return

            atr = self.topology.get_atr(asset, period=14, tf=900) or (price * 0.008)
            sl, tp, risk, rr = self.dynamic_sl.calculate(asset, direction, price, atr)

            if rr < Config.MIN_RR:
                self.db.log_rejected(asset, price, score, f"R:R {rr:.2f} < {Config.MIN_RR}",
                                     self.asset_state[asset]["volatility"], breakdown.get("Regime", "SIMPLE"),
                                     "Risk", json.dumps(breakdown), status="REJECTED", direction=direction)
                self.rejected += 1
                self.memory.update_state({"rejected_signals_count": 1})
                return

            trade_id = self.db.generate_trade_id()
            token = f"PRO-{asset}-{int(time.time()*1000)}"
            signal = {
                'asset': asset,
                'direction': direction,
                'entry': price,
                'sl': sl,
                'tp': tp,
                'sqs': score,
                'session': "ALWAYS",
                'patterns': patterns,
                'logic': reason,
                'news': self.news.last_news.get('title', '')[:100],
                'volatility': self.asset_state[asset]["volatility"],
                'regime': breakdown.get("Regime", "SIMPLE"),
                'htf_trend': self.asset_state[asset]["htf_trend"],
                'news_score': self.asset_state[asset]["news_sentiment"],
                'score': score,
                'confidence': status,
                'pending_candles': Config.PENDING_VERIFICATION_CANDLES,
                'volume_decay_threshold': Config.VOLUME_DECAY_THRESHOLD,
                'dynamic_min_sqs': Config.MIN_SCORE_FOR_SIGNAL,
                'signal_type': 'PRO_V8',
                'signal_token': token,
                'trade_id': trade_id,
                'score_breakdown': json.dumps(breakdown)
            }

            self.pending_queue.add_signal(signal)
            self.memory.update_state({"total_signals_generated": 1})
            logger.info(f"⏳ Added to 5m Verification Queue: {asset} {direction} @ {price:.2f} | Score: {score} ({status})")

    def _send_final_signal(self, signal):
        try:
            asset = signal['asset']
            direction = signal['direction']
            price = signal['entry']
            # Recalculate SL/TP around the verified 5m entry price
            atr = self.topology.get_atr(asset, period=14, tf=900) or (price * 0.008)
            sl, tp, risk, rr = self.dynamic_sl.calculate(asset, direction, price, atr)

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
            st = signal.get('signal_type', 'PRO_V8')
            token = signal.get('signal_token')
            trade_id = signal.get('trade_id') or self.db.generate_trade_id()
            pattern_name = list(patterns.keys())[0] if patterns else "smc_flow"
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
                    'regime': regime, 'signal_token': token, 'entry_time': int(time.time())
                })

            chart = self.topology.get_visual_topology(asset, price, direction, sl, tp, patterns)
            self.telegram.fire_signal(
                asset=asset, direction=direction, price=price, sl=sl, tp=tp,
                chart=chart, logic=logic, news=news, score=total_score,
                patterns=patterns, trade_id=trade_id, session=session,
                rr=rr, regime=regime, signal_type=st, signal_token=token, tier=confidence
            )

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
            logger.info(f"✅ FINAL SIGNAL FIRED: {asset} {direction} @ {price:.2f} | Score:{total_score} ({confidence})")
        except Exception as e:
            logger.error(f"Error sending final signal: {e}", exc_info=True)

    def _update_active_trades(self, asset, price):
        with self.trade_lock:
            to_close = []
            for tid, trade in list(self.active_trades.items()):
                if trade['asset'] != asset:
                    continue
                target = abs(trade['tp'] - trade['entry'])
                # 1. Breakeven Lock at 50% of Target
                if not trade.get('breakeven_locked', False) and target > 0:
                    half = trade['entry'] + 0.5 * target if trade['direction'] == 'BUY' else trade['entry'] - 0.5 * target
                    if (trade['direction'] == 'BUY' and price >= half) or (trade['direction'] == 'SELL' and price <= half):
                        trade['sl'] = trade['entry']
                        trade['breakeven_locked'] = True
                        self.mongo.update_trade_sl(tid, trade['entry'])
                        self.db.update_trade_sl_sqlite(tid, trade['entry'])
                        logger.info(f"🛡️ Trade #{tid} ({asset}) SL moved to Breakeven @ {trade['entry']:.2f}")

                # 2. Trailing Stop at 70% of Target (locks in 35% of target)
                if not trade.get('trailing_activated', False) and target > 0:
                    trigger = trade['entry'] + 0.7 * target if trade['direction'] == 'BUY' else trade['entry'] - 0.7 * target
                    if (trade['direction'] == 'BUY' and price >= trigger) or (trade['direction'] == 'SELL' and price <= trigger):
                        new_sl = trade['entry'] + 0.35 * target if trade['direction'] == 'BUY' else trade['entry'] - 0.35 * target
                        if (trade['direction'] == 'BUY' and new_sl > trade['sl']) or (trade['direction'] == 'SELL' and new_sl < trade['sl']):
                            trade['sl'] = new_sl
                            trade['trailing_activated'] = True
                            self.mongo.update_trade_sl(tid, new_sl)
                            self.db.update_trade_sl_sqlite(tid, new_sl)
                            logger.info(f"📈 Trade #{tid} ({asset}) Trailing SL locked @ {new_sl:.2f}")

                # 3. Check SL / TP Hit
                if trade['direction'] == 'BUY':
                    if price <= trade['sl']:
                        pnl = price - trade['entry']
                        reason = "Trailing SL Hit" if trade.get('trailing_activated') else ("Breakeven Exit" if trade.get('breakeven_locked') else "SL Hit")
                        to_close.append((tid, price, pnl, reason))
                    elif price >= trade['tp']:
                        pnl = price - trade['entry']
                        to_close.append((tid, price, pnl, "TP Hit 🎯"))
                else:
                    if price >= trade['sl']:
                        pnl = trade['entry'] - price
                        reason = "Trailing SL Hit" if trade.get('trailing_activated') else ("Breakeven Exit" if trade.get('breakeven_locked') else "SL Hit")
                        to_close.append((tid, price, pnl, reason))
                    elif price <= trade['tp']:
                        pnl = trade['entry'] - price
                        to_close.append((tid, price, pnl, "TP Hit 🎯"))

            for tid, exit_p, pnl, reason in to_close:
                self._close_trade(tid, exit_p, pnl, reason)

    def _close_trade(self, tid, price, pnl, reason=""):
        trade = self.active_trades.pop(tid, None)
        if not trade:
            return
        entry_time = trade.get('entry_time', int(time.time()))
        entry = trade.get('entry', 0)
        asset = trade.get('asset', '')
        self.db.close_trade(tid, price, pnl, reason)
        self.mongo.close_trade_mongo(tid, price, pnl, reason)
        self.telegram.fire_trade_close(tid, asset, entry, price, pnl, reason, entry_time)
        logger.info(f"🔒 Trade #{tid} ({asset}) closed @ {price:.2f} | PnL: {pnl:+.2f} | Reason: {reason}")

        updates = {"total_trades_closed": 1, "total_pnl": float(pnl)}
        if pnl > 0:
            updates["total_wins"] = 1
        elif pnl < 0:
            updates["total_losses"] = 1
        self.memory.update_state(updates)

    def _update_indicators(self, asset, price):
        try:
            with self.topology.lock:
                c15 = [c["close"] for c in self.topology.get_completed(asset, 900)][-150:]
                if len(c15) >= 50:
                    e20, e50 = self.topology._ema(c15, 20), self.topology._ema(c15, 50)
                    if e20 and e50:
                        self.asset_state[asset]["trend"] = "BULLISH" if e20[-1] > e50[-1] else "BEARISH"
                    self.asset_state[asset]["rsi"] = self.topology._calc_rsi(c15, 14)
                    self.asset_state[asset]["adx"] = self.topology.get_adx(asset, 900)
                c1h = [c["close"] for c in self.topology.get_completed(asset, 3600)][-150:]
                if len(c1h) >= 50:
                    e20_1h, e50_1h = self.topology._ema(c1h, 20), self.topology._ema(c1h, 50)
                    if e20_1h and e50_1h:
                        self.asset_state[asset]["htf_trend"] = "BULLISH" if e20_1h[-1] > e50_1h[-1] else "BEARISH"
                vols = [c["volume"] for c in self.topology.get_completed(asset, 300)][-20:]
                if len(vols) > 2:
                    avg = sum(vols[:-1]) / max(1, len(vols) - 1)
                    self.asset_state[asset]["volume_ratio"] = round(vols[-1] / avg, 2) if avg > 0 else 1.0
                atr = self.topology.get_atr(asset, period=14, tf=900)
                if atr and price > 0:
                    self.asset_state[asset]["volatility"] = round((atr / price) * 100, 3)
                neut = self.neutral_engine.detect(asset, price, tf=900)
                self.asset_state[asset]["neutral_pattern"] = neut.get("pattern", "None")
        except Exception as e:
            logger.error(f"Indicator update error: {e}")

    def _process_queue(self):
        while True:
            try:
                item = self.price_queue.get(timeout=1)
                if item:
                    self._handle_price_tick(*item)
            except queue.Empty:
                pass
            except Exception as e:
                logger.error(f"Queue processing error: {e}")

    def run(self):
        threading.Thread(target=start_health_server, args=(self,), daemon=True).start()
        threading.Thread(target=self._ping_self_loop, daemon=True).start()
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self._load_and_backfill, asset, tf)
                       for asset in Config.ASSETS for tf in [60, 300, 900, 3600, 14400]]
            for _ in as_completed(futures):
                pass
        for asset in Config.ASSETS:
            self.topology.initialize_structure(asset)
            c15 = self.topology.get_completed(asset, 900)
            if c15:
                self.asset_state[asset]["price"] = c15[-1]["close"]
                self._update_indicators(asset, c15[-1]["close"])

        self._ready = True
        self.stream = BinancePublicStream(self._on_price)
        self.stream.start()
        self.telegram.send_message("🚀 <b>AlphaBot v8.0 PRO Online</b>\n✅ All Critical Bugs Fixed | SMC + OrderFlow Active")
        last_news = 0
        while True:
            time.sleep(15)
            if time.time() - last_news > 300:
                news = self.news.fetch_latest()
                if news.get("fresh") and news.get("articles"):
                    for a in Config.ASSETS:
                        self.asset_state[a]["news_sentiment"] = news["articles"][0]["sentiment"]
                    last_news = time.time()

    def _load_and_backfill(self, asset, tf):
        interval = {60: "1m", 300: "5m", 900: "15m", 3600: "1h", 14400: "4h"}[tf]
        try:
            resp = requests.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": asset, "interval": interval, "limit": 300},
                timeout=15
            )
            if resp.status_code == 200:
                fetched = []
                raw_klines = resp.json()
                # Exclude the very last kline if it is still forming
                for d in raw_klines[:-1]:
                    c = {
                        "timestamp": d[0] // 1000,
                        "open": float(d[1]), "high": float(d[2]),
                        "low": float(d[3]), "close": float(d[4]),
                        "volume": float(d[5]), "complete": True
                    }
                    if DataValidator.validate_candle(c):
                        fetched.append(c)
                with self.topology.lock:
                    self.topology.candles[tf][asset] = fetched
                logger.info(f"🌐 Backfilled {len(fetched)} fresh candles for {asset} [{interval}]")
                return
        except Exception as e:
            logger.error(f"Binance REST backfill error for {asset} {tf}: {e}")

        # Fallback to MongoDB cached candles if Binance API call fails
        candles = self.mongo.load_candles(asset, tf, limit=300)
        if candles:
            with self.topology.lock:
                self.topology.candles[tf][asset] = candles
            logger.info(f"📂 Loaded {len(candles)} fallback candles for {asset} [{tf}s] from MongoDB")

    def _on_price(self, asset, price, volume):
        if DataValidator.validate_price(price) and DataValidator.validate_volume(volume):
            try:
                self.price_queue.put_nowait((asset, price, volume))
                self._price_counter += 1
            except queue.Full:
                try:
                    self.price_queue.get_nowait()
                    self.price_queue.put_nowait((asset, price, volume))
                except Exception:
                    pass

    def _ping_self_loop(self):
        while True:
            time.sleep(300)
            try:
                requests.get(Config.RENDER_URL, timeout=10)
            except Exception:
                pass


# =====================================================================
# UPGRADED MODERN DASHBOARD & HEALTH SERVER
# =====================================================================
def start_health_server(orchestrator):
    port = int(os.environ.get("PORT", 10000))

    class H(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass  # Keep console logs clean

        def do_GET(self):
            if self.path.startswith('/close_trade'):
                try:
                    query = urllib.parse.urlparse(self.path).query
                    params = urllib.parse.parse_qs(query)
                    tid = int(params.get('id', [0])[0])
                    with orchestrator.trade_lock:
                        trade = orchestrator.active_trades.get(tid)
                        if trade:
                            asset = trade['asset']
                            curr = orchestrator.asset_state.get(asset, {}).get("price") or trade['entry']
                            pnl = (curr - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - curr)
                            orchestrator._close_trade(tid, curr, pnl, "ManualDashboardClose")
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
                    cur.execute("SELECT datetime(timestamp, 'unixepoch'), asset, price, score, reason, gate_name, regime, status FROM rejected_signals ORDER BY timestamp DESC LIMIT 50")
                    rows = cur.fetchall()
                    data = [{"time": r[0], "asset": r[1], "price": r[2], "score": r[3], "reason": r[4], "gate": r[5], "regime": r[6], "status": r[7]} for r in rows]
                    self.wfile.write(json.dumps(data, indent=2).encode())
                except Exception as e:
                    self.wfile.write(json.dumps({"error": str(e)}).encode())
                return

            if self.path.startswith('/observations'):
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                try:
                    obs = orchestrator.db.get_recent_observations(50)
                    self.wfile.write(json.dumps(obs, indent=2).encode())
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
                d, h, m = watch_sec // 86400, (watch_sec % 86400) // 3600, (watch_sec % 3600) // 60
                age_str = f"{d}d {h}h {m}m"
                perf = orchestrator.db.get_performance_metrics()

                # Live Asset Cards
                asset_cards_html = ""
                for a in Config.ASSETS:
                    st = orchestrator.asset_state.get(a, {})
                    cvd_val = orchestrator.futures_stream.get_cvd(a.lower())
                    oi_val = orchestrator.futures_stream.get_open_interest(a.lower())
                    t15_cls = "badge-bull" if st.get("trend") == "BULLISH" else "badge-bear"
                    t1h_cls = "badge-bull" if st.get("htf_trend") == "BULLISH" else "badge-bear"
                    asset_cards_html += f"""
                    <div class="card">
                        <div class="card-title">{Config.DISPLAY_NAMES.get(a, a)} <span class="price">${st.get('price', 0):,.2f}</span></div>
                        <div class="metrics-grid">
                            <div>15m Trend: <span class="{t15_cls}">{st.get('trend')}</span></div>
                            <div>1h Trend: <span class="{t1h_cls}">{st.get('htf_trend')}</span></div>
                            <div>RSI (15m): <b>{st.get('rsi', 50):.1f}</b></div>
                            <div>ADX: <b>{st.get('adx', 20):.1f}</b></div>
                            <div>ATR%: <b>{st.get('volatility', 0):.2f}%</b></div>
                            <div>Candle: <b>{st.get('neutral_pattern', 'None')}</b></div>
                            <div>CVD: <b>{cvd_val:+,.1f}</b></div>
                            <div>Absorption: <b>{st.get('absorption_score', 0)} ({st.get('absorption_dir') or 'N/A'})</b></div>
                        </div>
                    </div>"""

                # Pending Verification Queue Rows
                pending_rows = ""
                with orchestrator.pending_queue.lock:
                    for k, p in orchestrator.pending_queue.pending.items():
                        pending_rows += f"<tr><td><b>{p['asset']}</b></td><td>{p['direction']}</td><td>${p['entry']:,.2f}</td><td>{p['score']}</td><td>{p['confidence']}</td><td>Waiting 5m Close...</td></tr>"
                if not pending_rows:
                    pending_rows = "<tr><td colspan='6' style='color:#777'>No signals waiting in 5m verification queue</td></tr>"

                # Active Trades Rows
                active_rows = ""
                with orchestrator.trade_lock:
                    for tid, trade in orchestrator.active_trades.items():
                        curr = orchestrator.asset_state.get(trade['asset'], {}).get('price') or trade['entry']
                        pnl = (curr - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - curr)
                        pnl_pct = (pnl / trade['entry'] * 100) if trade['entry'] else 0
                        cls = "g" if pnl >= 0 else "r"
                        btn = f"<a href='/close_trade?id={tid}' class='btn'>Close</a>"
                        active_rows += (
                            f"<tr><td>#{tid}</td><td><b>{trade['asset']}</b></td><td>{trade['direction']}</td>"
                            f"<td>${trade['entry']:,.2f}</td><td>${curr:,.2f}</td><td>${trade['sl']:,.2f}</td><td>${trade['tp']:,.2f}</td>"
                            f"<td class='{cls}'>${pnl:+,.2f} ({pnl_pct:+.2f}%)</td><td>{trade.get('health', 100)}%</td><td>{btn}</td></tr>"
                        )
                if not active_rows:
                    active_rows = "<tr><td colspan='10' style='color:#777'>No active trades open right now</td></tr>"

                # Observations Rows
                observations = orchestrator.db.get_recent_observations(15)
                obs_rows = ""
                for obs in observations:
                    st_cls = f"status-{obs['status'].replace(' ', '-')}"
                    obs_rows += (
                        f"<tr><td><b>{obs['asset']}</b></td><td>{obs['time']}</td><td>${(obs.get('price') or 0):,.2f}</td>"
                        f"<td>{obs['direction']}</td><td><b>{obs['score']}</b></td>"
                        f"<td class='{st_cls}'><b>{obs['status']}</b></td><td style='text-align:left'>{html.escape(obs['reason'][:90])}</td></tr>"
                    )
                if not obs_rows:
                    obs_rows = "<tr><td colspan='7' style='color:#777'>Waiting for next 15m candle close evaluation...</td></tr>"

                page = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AlphaBot v8.0 Pro Terminal</title><meta http-equiv="refresh" content="15">
<style>
body{{font-family:'Segoe UI',Roboto,Arial,sans-serif;background:#0b0e14;color:#e6edf3;margin:0;padding:16px}}
.header{{display:flex;justify-content:space-between;align-items:center;background:#161b22;padding:14px 20px;border-radius:10px;border:1px solid #30363d;margin-bottom:16px;flex-wrap:wrap}}
.header h1{{margin:0;font-size:20px;color:#58a6ff}}
.stats-bar{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:16px}}
.stat-box{{background:#161b22;border:1px solid #30363d;padding:12px;border-radius:8px;text-align:center}}
.stat-box .val{{font-size:20px;font-weight:bold;color:#fff;margin-top:4px}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:14px;margin-bottom:20px}}
.card{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px}}
.card-title{{font-size:17px;font-weight:bold;display:flex;justify-content:space-between;border-bottom:1px solid #21262d;padding-bottom:8px;margin-bottom:10px}}
.price{{color:#3fb950}}
.metrics-grid{{display:grid;grid-template-columns:1fr 1fr;gap:6px;font-size:13px;color:#8b949e}}
.metrics-grid b{{color:#e6edf3}}
.badge-bull{{color:#3fb950;font-weight:bold}} .badge-bear{{color:#f85149;font-weight:bold}}
h2{{font-size:16px;color:#58a6ff;margin:18px 0 8px}}
.table-wrap{{overflow-x:auto;background:#161b22;border:1px solid #30363d;border-radius:8px;margin-bottom:16px}}
table{{border-collapse:collapse;width:100%;font-size:13px}}
th,td{{border-bottom:1px solid #21262d;padding:9px 10px;text-align:center}}
th{{background:#21262d;color:#8b949e;font-weight:600}}
.g{{color:#3fb950;font-weight:bold}} .r{{color:#f85149;font-weight:bold}}
.btn{{background:#da3633;color:#fff;padding:4px 10px;text-decoration:none;border-radius:4px;font-size:12px;font-weight:bold}}
.status-REJECTED{{color:#f85149}} .status-NO-SETUP{{color:#8b949e}} .status-WATCH{{color:#d29922}}
.status-VALID{{color:#3fb950}} .status-HIGH{{color:#58a6ff}} .status-EXTREME{{color:#bc8cff}}
.links a{{color:#58a6ff;margin-right:15px;text-decoration:none;font-size:13px}}
</style></head><body>
<div class="header">
  <h1>🚀 AlphaBot v8.0 PRO – SMC + OrderFlow Engine</h1>
  <div>🟢 <b>Online</b> | ⏱️ Uptime Age: <b>{age_str}</b> | 😨 Fear & Greed: <b>{orchestrator.news.fear_greed}</b></div>
</div>
<div class="stats-bar">
  <div class="stat-box"><div>Accepted Signals</div><div class="val g">{mem.get("accepted_signals_count", 0)}</div></div>
  <div class="stat-box"><div>Filtered / Rejected</div><div class="val r">{mem.get("rejected_signals_count", 0)}</div></div>
  <div class="stat-box"><div>Closed Trades (W/L)</div><div class="val">{mem.get("total_trades_closed", 0)} ({mem.get("total_wins", 0)}W / {mem.get("total_losses", 0)}L)</div></div>
  <div class="stat-box"><div>Win Rate</div><div class="val">{perf.get('win_rate', 0):.1%}</div></div>
  <div class="stat-box"><div>Cumulative Move (PnL)</div><div class="val {'g' if mem.get('total_pnl', 0) >= 0 else 'r'}">${mem.get('total_pnl', 0.0):+,.2f}</div></div>
</div>
<div class="cards">{asset_cards_html}</div>
<h2>⏳ 5m Pending Verification Queue</h2>
<div class="table-wrap"><table><tr><th>Asset</th><th>Direction</th><th>Trigger Price</th><th>Score</th><th>Tier</th><th>Status</th></tr>{pending_rows}</table></div>
<h2>⚡ Active Open Trades</h2>
<div class="table-wrap"><table><tr><th>ID</th><th>Asset</th><th>Dir</th><th>Entry</th><th>Current</th><th>SL</th><th>TP</th><th>Unrealized PnL</th><th>Health</th><th>Action</th></tr>{active_rows}</table></div>
<h2>📋 Latest 15m Signal Evaluations</h2>
<div class="table-wrap"><table><tr><th>Asset</th><th>Time (UTC)</th><th>Price</th><th>Dir</th><th>Score</th><th>Status</th><th>Layer Breakdown</th></tr>{obs_rows}</table></div>
<div class="links">
  <a href="/observations" target="_blank">📄 View Observations JSON</a>
  <a href="/rejections" target="_blank">📄 View Rejections JSON</a>
</div>
</body></html>"""
                self.wfile.write(page.encode())
            else:
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "online", "version": "8.0-PRO"}).encode())

        def do_HEAD(self):
            self.send_response(200)
            self.end_headers()

    httpd = HTTPServer(("0.0.0.0", port), H)
    logger.info(f"🌐 Health & Dashboard server listening on port {port}")
    httpd.serve_forever()


if __name__ == "__main__":
    bot = AIOrchestrator()
    bot.run()

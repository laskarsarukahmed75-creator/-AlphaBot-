# =====================================================================
# app.py – AlphaBot v10.0 FINAL INSTITUTIONAL EDITION
# =====================================================================
# MASTER UPGRADES (v9 -> v10):
#   FIXED   : MCB bypass bug in pending queue
#   FIXED   : Funding rate polling 30s -> 10min
#   FIXED   : Correlation shield now softer (blocks only if 2+ same-dir)
#   FIXED   : Structure trailing now uses 5m lows + ATR cap
#   FIXED   : CVD now uses rate-of-change (delta), not absolute value
#   FIXED   : Weighted PnL edge case guarded with bounds
#   FIXED   : Performance metrics fallback honest when no data
#   FIXED   : Dead code removed
#   NEW     : Backtest Engine (offline replay on 1yr historical)
#   NEW     : Position Sizing Engine (0.5% risk/trade)
#   NEW     : Daily/Weekly Loss Circuit Breaker
#   NEW     : CVD Persistence in MongoDB
#   NEW     : Performance Attribution (WR by regime/session/tier)
#   NEW     : Slippage Model (0.10% applied to live + backtest)
#   NEW     : Telegram Inline Buttons (Close/Pause)
#   KEPT    : v9's OI 4-Quadrant, Funding Filter, TP1/TP2/TP3 Scale-Out,
#             OrderBook Imbalance, IST Session Bias, Auto-MCB
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
from collections import deque, defaultdict
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import websocket

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
                r2 = requests.get(
                    f"https://api.bybit.com/v5/market/open-interest?category=linear&symbol={asset}&intervalTime=5min&limit=1",
                    timeout=4
                )
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
logger = logging.getLogger("AlphaBot-v10.0-FINAL")


class Config:
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    DISPLAY_NAMES = {"BTCUSDT": "BTC/USDT", "ETHUSDT": "ETH/USDT", "SOLUSDT": "SOL/USDT"}

    MONGO_URI = os.getenv("MONGO_URI", "")
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "crypto_bot_v10")
    RENDER_URL = os.getenv("RENDER_URL", "https://alphabot-76tj.onrender.com")
    DB_PATH = "trades_v10.db"
    MAX_CANDLES = 500
    BINANCE_FUTURES_WS_URL = "wss://fstream.binance.com/ws"

    IST = pytz.timezone('Asia/Kolkata')

    # Scoring
    SCORE_REJECT = 45
    SCORE_WATCH = 45
    SCORE_VALID = 54
    SCORE_HIGH = 64
    SCORE_EXTREME = 75

    MIN_SCORE_FOR_SIGNAL = 54
    MIN_RR = 2.0
    TP1_RR = 1.0
    TP2_RR = 2.0
    TP3_RR = 3.5

    MAX_SIGNALS_PER_DAY = 20
    ASSET_COOLDOWN_HOURS = 1.0

    # MCB (consecutive loss based)
    MCB_MAX_CONSECUTIVE_LOSSES = 3
    MCB_COOLDOWN_SECONDS = 6 * 3600

    # NEW: Daily/Weekly loss circuit breaker
    DAILY_LOSS_LIMIT_PCT = 2.0    # account % — trip 12h pause
    WEEKLY_LOSS_LIMIT_PCT = 6.0   # account % — trip 48h pause
    DAILY_LOSS_PAUSE_SECONDS = 12 * 3600
    WEEKLY_LOSS_PAUSE_SECONDS = 48 * 3600

    # NEW: Position Sizing
    ACCOUNT_BALANCE_USD = float(os.getenv("ACCOUNT_BALANCE_USD", "1000"))
    RISK_PER_TRADE_PCT = 0.5       # 0.5% per trade
    MAX_PORTFOLIO_HEAT_PCT = 3.0   # total max risk across all open
    MAX_LEVERAGE = 10.0

    # NEW: Slippage model (applied to live PnL + backtest)
    SLIPPAGE_PCT = 0.001           # 0.10%

    # NEW: Backtest config
    BACKTEST_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    BACKTEST_INTERVAL = "15m"
    BACKTEST_LOOKBACK_DAYS = 365
    BACKTEST_INITIAL_BALANCE = 1000.0

    # Funding & OrderBook
    EXTREME_FUNDING_RATE = 0.0005
    OB_BULL_IMBALANCE = 1.25
    OB_BEAR_IMBALANCE = 0.80
    FUNDING_POLL_SECONDS = 600     # FIXED: was 30s, now 10 min

    # Correlation shield (FIXED: softer)
    CORRELATION_BLOCK_MIN_OPEN = 2

    PENDING_VERIFICATION_CANDLES = 1
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
    CVD_EXHAUSTION_PENALTY = 12
    VOLUME_EXPANSION_RATIO = 1.4
    PARABOLIC_RSI_THRESHOLD = 68
    OVEREXTENSION_ATR_MULT = 2.2

    # CVD persistence
    CVD_PERSIST_SECONDS = 300       # save every 5 min


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
            logger.warning("MongoDB not configured; SQLite-only mode.")
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
            self.db.cvd_state.create_index([("symbol", 1)], unique=True)
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

    def load_candles(self, asset, timeframe, limit=400):
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

    def update_trade_fields(self, trade_id, updates: dict):
        if self.db is None:
            return
        try:
            self.db.trades.update_one({"id": trade_id}, {"$set": updates})
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

    # NEW: CVD persistence
    def save_cvd_state(self, symbol: str, cvd_value: float):
        if self.db is None:
            return
        try:
            self.db.cvd_state.update_one(
                {"symbol": symbol},
                {"$set": {"symbol": symbol, "cvd": cvd_value, "ts": int(time.time())}},
                upsert=True
            )
        except Exception:
            pass

    def load_cvd_state(self, symbol: str) -> float:
        if self.db is None:
            return 0.0
        try:
            doc = self.db.cvd_state.find_one({"symbol": symbol})
            if doc:
                return float(doc.get("cvd", 0.0))
        except Exception:
            pass
        return 0.0


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
                tp1 REAL, tp2 REAL, tp3 REAL,
                tp1_hit INTEGER DEFAULT 0, tp2_hit INTEGER DEFAULT 0,
                score INTEGER, confidence TEXT, patterns TEXT, logic TEXT,
                timestamp INTEGER, status TEXT DEFAULT 'open',
                exit_price REAL, pnl REAL, close_time INTEGER,
                volatility REAL, market_regime TEXT, htf_trend TEXT, news_score REAL,
                entry_time INTEGER, exit_reason TEXT,
                session TEXT, sqs_score INTEGER, pattern_name TEXT,
                regime TEXT, dynamic_min_sqs INTEGER,
                signal_type TEXT DEFAULT 'V10_INST',
                signal_token TEXT,
                score_breakdown TEXT,
                quantity REAL DEFAULT 0.0,
                risk_usd REAL DEFAULT 0.0
            )''')
            for col_sql in [
                "ALTER TABLE trades ADD COLUMN tp1 REAL",
                "ALTER TABLE trades ADD COLUMN tp2 REAL",
                "ALTER TABLE trades ADD COLUMN tp3 REAL",
                "ALTER TABLE trades ADD COLUMN tp1_hit INTEGER DEFAULT 0",
                "ALTER TABLE trades ADD COLUMN tp2_hit INTEGER DEFAULT 0",
                "ALTER TABLE trades ADD COLUMN quantity REAL DEFAULT 0.0",
                "ALTER TABLE trades ADD COLUMN risk_usd REAL DEFAULT 0.0"
            ]:
                try:
                    cur.execute(col_sql)
                except sqlite3.OperationalError:
                    pass

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

            cur.execute('''CREATE TABLE IF NOT EXISTS signal_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset TEXT, timestamp INTEGER, direction TEXT,
                score INTEGER, status TEXT, reason TEXT,
                breakdown TEXT, price REAL, volatility REAL
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
                consecutive_losses INTEGER DEFAULT 0,
                mcb_until_ts INTEGER DEFAULT 0,
                restart_count INTEGER DEFAULT 0,
                daily_start_balance REAL DEFAULT 0.0,
                daily_start_ts INTEGER DEFAULT 0,
                weekly_start_balance REAL DEFAULT 0.0,
                weekly_start_ts INTEGER DEFAULT 0,
                daily_cb_until_ts INTEGER DEFAULT 0,
                weekly_cb_until_ts INTEGER DEFAULT 0,
                last_update INTEGER
            )''')
            for col_sql in [
                "ALTER TABLE global_bot_memory ADD COLUMN consecutive_losses INTEGER DEFAULT 0",
                "ALTER TABLE global_bot_memory ADD COLUMN mcb_until_ts INTEGER DEFAULT 0",
                "ALTER TABLE global_bot_memory ADD COLUMN restart_count INTEGER DEFAULT 0",
                "ALTER TABLE global_bot_memory ADD COLUMN daily_start_balance REAL DEFAULT 0.0",
                "ALTER TABLE global_bot_memory ADD COLUMN daily_start_ts INTEGER DEFAULT 0",
                "ALTER TABLE global_bot_memory ADD COLUMN weekly_start_balance REAL DEFAULT 0.0",
                "ALTER TABLE global_bot_memory ADD COLUMN weekly_start_ts INTEGER DEFAULT 0",
                "ALTER TABLE global_bot_memory ADD COLUMN daily_cb_until_ts INTEGER DEFAULT 0",
                "ALTER TABLE global_bot_memory ADD COLUMN weekly_cb_until_ts INTEGER DEFAULT 0"
            ]:
                try:
                    cur.execute(col_sql)
                except sqlite3.OperationalError:
                    pass

            cur.execute('''INSERT OR IGNORE INTO global_bot_memory 
                (id, first_launch_timestamp, total_run_seconds, total_signals_generated,
                 accepted_signals_count, rejected_signals_count, total_trades_closed,
                 total_wins, total_losses, total_pnl, consecutive_losses, mcb_until_ts,
                 restart_count, daily_start_balance, daily_start_ts,
                 weekly_start_balance, weekly_start_ts,
                 daily_cb_until_ts, weekly_cb_until_ts, last_update)
                VALUES (1, strftime('%s','now'), 0, 0, 0, 0, 0, 0, 0, 0.0, 0, 0, 0,
                        0.0, 0, 0.0, 0, 0, 0, strftime('%s','now'))''')
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

    def log_trade(self, trade_id, asset, direction, entry, sl, tp1, tp2, tp3, score, confidence,
                  patterns, logic, volatility, regime, htf_trend, news_score, session, sqs_score,
                  pattern_name, dynamic_min_sqs, quantity=0.0, risk_usd=0.0,
                  signal_type="V10_INST", signal_token=None, score_breakdown=""):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute('''INSERT OR REPLACE INTO trades 
                (id, asset, direction, entry, stop_loss, take_profit, tp1, tp2, tp3, tp1_hit, tp2_hit,
                 score, confidence, patterns, logic, timestamp, volatility, market_regime, htf_trend,
                 news_score, entry_time, status, session, sqs_score, pattern_name, regime,
                 dynamic_min_sqs, signal_type, signal_token, score_breakdown, quantity, risk_usd)
                VALUES (?,?,?,?,?,?,?,?,?,0,0,?,?,?,?,?,?,?,?,?,?,'open',?,?,?,?,?,?,?,?,?,?)''',
                (trade_id, asset, direction, entry, sl, tp3, tp1, tp2, tp3,
                 score, confidence, json.dumps(patterns), logic, int(time.time()), volatility,
                 regime, htf_trend, news_score, int(time.time()), session, sqs_score,
                 pattern_name, regime, dynamic_min_sqs, signal_type, signal_token,
                 score_breakdown, quantity, risk_usd))
            conn.commit()
            return trade_id
        finally:
            cur.close()

    def update_trade_sqlite(self, trade_id, updates: dict):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            set_clause = ", ".join([f"{k}=?" for k in updates.keys()])
            vals = list(updates.values()) + [trade_id]
            cur.execute(f"UPDATE trades SET {set_clause} WHERE id=?", vals)
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

    def get_performance_metrics(self, mem_fallback: dict = None):
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

            if total == 0:
                # FIXED: honest fallback (no fake profit_factor=1.0)
                return {
                    "total_trades": 0, "winning_trades": 0, "losing_trades": 0,
                    "win_rate": 0.0, "profit_factor": 0.0,
                    "total_pnl": float((mem_fallback or {}).get("total_pnl", 0.0)),
                    "note": "No closed trades yet in SQLite; using memory counters only."
                }

            total_pnl = gross_profit - gross_loss
            win_rate = wins / total if total else 0.0
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0.0)
            return {
                "total_trades": total,
                "winning_trades": wins,
                "losing_trades": total - wins,
                "win_rate": win_rate,
                "profit_factor": profit_factor,
                "total_pnl": total_pnl
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

    # NEW: Performance Attribution
    def get_attribution_stats(self):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            result = {"by_regime": {}, "by_session": {}, "by_tier": {}, "by_asset": {}}

            cur.execute("""SELECT regime, COUNT(*), SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END), SUM(pnl)
                           FROM trades WHERE status='closed' AND pnl IS NOT NULL GROUP BY regime""")
            for r in cur.fetchall():
                result["by_regime"][r[0] or "UNKNOWN"] = {
                    "total": r[1], "wins": r[2] or 0,
                    "wr": (r[2] or 0) / r[1] if r[1] else 0.0,
                    "pnl": float(r[3] or 0.0)
                }

            cur.execute("""SELECT session, COUNT(*), SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END), SUM(pnl)
                           FROM trades WHERE status='closed' AND pnl IS NOT NULL GROUP BY session""")
            for r in cur.fetchall():
                result["by_session"][r[0] or "UNKNOWN"] = {
                    "total": r[1], "wins": r[2] or 0,
                    "wr": (r[2] or 0) / r[1] if r[1] else 0.0,
                    "pnl": float(r[3] or 0.0)
                }

            cur.execute("""SELECT confidence, COUNT(*), SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END), SUM(pnl)
                           FROM trades WHERE status='closed' AND pnl IS NOT NULL GROUP BY confidence""")
            for r in cur.fetchall():
                result["by_tier"][r[0] or "UNKNOWN"] = {
                    "total": r[1], "wins": r[2] or 0,
                    "wr": (r[2] or 0) / r[1] if r[1] else 0.0,
                    "pnl": float(r[3] or 0.0)
                }

            cur.execute("""SELECT asset, COUNT(*), SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END), SUM(pnl)
                           FROM trades WHERE status='closed' AND pnl IS NOT NULL GROUP BY asset""")
            for r in cur.fetchall():
                result["by_asset"][r[0] or "UNKNOWN"] = {
                    "total": r[1], "wins": r[2] or 0,
                    "wr": (r[2] or 0) / r[1] if r[1] else 0.0,
                    "pnl": float(r[3] or 0.0)
                }
            return result
        finally:
            cur.close()


# =====================================================================
# PERSISTENT MEMORY (With MCB + Daily/Weekly Loss CB)
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
                        for k, v in self._defaults().items():
                            doc.setdefault(k, v)
                        self.cache = doc
                        logger.info("Memory loaded from MongoDB.")
                        return
                except Exception:
                    pass
            state = self.sqlite.get_memory_state()
            if state:
                self.cache = {"_id": "global_state", **state}
                logger.info("Memory loaded from SQLite fallback.")
                return
            default = self._defaults()
            default["_id"] = "global_state"
            default["first_launch_timestamp"] = int(time.time())
            default["last_update"] = int(time.time())
            self.cache = default
            self._save_to_sqlite(default)

    @staticmethod
    def _defaults():
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
            "consecutive_losses": 0,
            "mcb_until_ts": 0,
            "restart_count": 0,
            "daily_start_balance": 0.0,
            "daily_start_ts": 0,
            "weekly_start_balance": 0.0,
            "weekly_start_ts": 0,
            "daily_cb_until_ts": 0,
            "weekly_cb_until_ts": 0,
            "last_update": int(time.time())
        }

    def _save_to_sqlite(self, state):
        valid = {"first_launch_timestamp", "total_run_seconds", "total_signals_generated",
                 "accepted_signals_count", "rejected_signals_count", "total_trades_closed",
                 "total_wins", "total_losses", "total_pnl", "consecutive_losses",
                 "mcb_until_ts", "restart_count", "daily_start_balance", "daily_start_ts",
                 "weekly_start_balance", "weekly_start_ts", "daily_cb_until_ts",
                 "weekly_cb_until_ts", "last_update"}
        data = {k: v for k, v in state.items() if k in valid}
        self.sqlite.update_memory_state(data)

    def get_or_create_state(self):
        with self.lock:
            return dict(self.cache)

    def update_state(self, updates: dict, absolute_keys: set = None):
        if absolute_keys is None:
            absolute_keys = {"last_update", "first_launch_timestamp", "consecutive_losses",
                             "mcb_until_ts", "daily_start_balance", "daily_start_ts",
                             "weekly_start_balance", "weekly_start_ts",
                             "daily_cb_until_ts", "weekly_cb_until_ts"}
        with self.lock:
            inc_fields, set_fields = {}, {}
            for k, v in updates.items():
                if k in absolute_keys:
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
                    doc = {}
                    if inc_fields:
                        doc["$inc"] = inc_fields
                    if set_fields:
                        doc["$set"] = set_fields
                    if doc:
                        self.mongo.db.global_bot_memory.update_one({"_id": "global_state"}, doc, upsert=True)
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
# WEBSOCKET & INSTITUTIONAL DATA STREAM (With CVD Persistence)
# =====================================================================
class BinanceFuturesStream:
    def __init__(self, mongo_db=None):
        self.symbols = [s.lower() for s in Config.ASSETS]
        self.ws = None
        self.running = False
        self.mongo = mongo_db
        self.data = {
            'open_interest': {}, 'funding_rate': {}, 'ob_imbalance': {},
            'liquidations': deque(maxlen=200), 'cvd': {}, 'last_trade': {}
        }
        self.oi_history = {s: deque(maxlen=12) for s in self.symbols}
        self.lock = threading.Lock()
        self.reconnect_count = 0
        self.thread = None
        self.last_ping = time.time()
        self.running_metrics = False

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.running = True
        self.running_metrics = True
        # NEW: restore CVD from Mongo on boot
        if self.mongo and self.mongo.db is not None:
            for s in self.symbols:
                restored = self.mongo.load_cvd_state(s)
                if restored:
                    self.data['cvd'][s] = restored
                    logger.info(f"🔁 Restored CVD for {s}: {restored:+.2f}")

        self.thread = threading.Thread(target=self._ws_loop, daemon=True)
        self.thread.start()
        threading.Thread(target=self._health_check, daemon=True).start()
        threading.Thread(target=self._fetch_oi_loop, daemon=True).start()
        threading.Thread(target=self._fetch_metrics_loop, daemon=True).start()
        threading.Thread(target=self._cvd_persist_loop, daemon=True).start()

    def stop(self):
        self.running = False
        self.running_metrics = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

    def _fetch_oi_loop(self):
        while self.running_metrics:
            try:
                for symbol in self.symbols:
                    try:
                        r = requests.get(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol.upper()}", timeout=5)
                        if r.status_code == 200:
                            oi = float(r.json().get('openInterest', 0))
                            with self.lock:
                                self.data['open_interest'][symbol] = oi
                                self.oi_history[symbol].append(oi)
                    except Exception:
                        pass
                time.sleep(30)
            except Exception as e:
                logger.error(f"OI fetch error: {e}")
                time.sleep(60)

    def _fetch_metrics_loop(self):
        """FIXED: Funding polled every 10 min instead of 30s to respect rate limits."""
        last_funding_fetch = 0
        last_ob_fetch = 0
        while self.running_metrics:
            try:
                now = time.time()
                for symbol in self.symbols:
                    sym_up = symbol.upper()
                    # Funding rate: every 10 min
                    if now - last_funding_fetch >= Config.FUNDING_POLL_SECONDS:
                        try:
                            r = requests.get(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={sym_up}", timeout=5)
                            if r.status_code == 200:
                                fr = float(r.json().get('lastFundingRate', 0.0))
                                with self.lock:
                                    self.data['funding_rate'][symbol] = fr
                        except Exception:
                            pass
                    # Order book: every 30s
                    if now - last_ob_fetch >= 30:
                        try:
                            r = requests.get(f"https://fapi.binance.com/fapi/v1/depth?symbol={sym_up}&limit=20", timeout=5)
                            if r.status_code == 200:
                                ob = r.json()
                                bids = sum(float(b[1]) for b in ob.get('bids', []))
                                asks = sum(float(a[1]) for a in ob.get('asks', []))
                                ratio = round(bids / max(asks, 1e-6), 2)
                                with self.lock:
                                    self.data['ob_imbalance'][symbol] = ratio
                        except Exception:
                            pass
                if now - last_funding_fetch >= Config.FUNDING_POLL_SECONDS:
                    last_funding_fetch = now
                if now - last_ob_fetch >= 30:
                    last_ob_fetch = now
                time.sleep(15)
            except Exception as e:
                logger.error(f"Metrics fetch error: {e}")
                time.sleep(30)

    def _cvd_persist_loop(self):
        """Persist CVD to Mongo every 5 min."""
        while self.running_metrics:
            time.sleep(Config.CVD_PERSIST_SECONDS)
            if self.mongo and self.mongo.db is not None:
                with self.lock:
                    snapshot = dict(self.data['cvd'])
                for sym, val in snapshot.items():
                    self.mongo.save_cvd_state(sym, val)

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
        logger.info("✅ Futures Combined WS connected.")

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
            return hist[-1] - hist[0] if len(hist) >= 2 else 0.0

    def get_funding_rate(self, symbol):
        with self.lock:
            return self.data['funding_rate'].get(symbol.lower(), 0.0)

    def get_ob_imbalance(self, symbol):
        with self.lock:
            return self.data['ob_imbalance'].get(symbol.lower(), 1.0)

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
                    on_open=self._on_open, on_message=self._on_msg,
                    on_error=self._on_error, on_close=self._on_close,
                    on_ping=self._on_ping, on_pong=self._on_pong
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
        logger.info("✅ Public Spot WS connected.")

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
                prev_start, prev_vol = self._last_kline_state.get(symbol, (kline_start, 0.0))
                delta_vol = cum_vol if kline_start != prev_start else max(0.0, cum_vol - prev_vol)
                self._last_kline_state[symbol] = (kline_start, cum_vol)
                self.on_price_update(symbol, close_price, delta_vol)
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
        self.reconnect_count += 1

    def _health_check(self):
        while self.running:
            time.sleep(30)
            with self.lock:
                age = time.time() - self.last_ping
            if age > 60 and not self.rest_fallback:
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
                        self.on_price_update(symbol, float(resp.json()["price"]), 0.0)
            except Exception as e:
                logger.error(f"REST fallback error: {e}")


# =====================================================================
# CANDLE TOPOLOGY ENGINE
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
                if not any(abs(lvl["price"] - ex["price"]) / ex["price"] < 0.002
                           for ex in self.high_volume_levels[asset]):
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
                if (recent[i-2]["high"] < recent[i]["high"] > recent[i+2]["high"] and
                        recent[i-1]["high"] < recent[i]["high"] > recent[i+1]["high"]):
                    highs.append(recent[i]["high"])
                if (recent[i-2]["low"] > recent[i]["low"] < recent[i+2]["low"] and
                        recent[i-1]["low"] > recent[i]["low"] < recent[i+1]["low"]):
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
            self.support_resistance[asset]["support"] = [l for l in clusters if l < price * 0.998]
            self.support_resistance[asset]["resistance"] = [l for l in clusters if l > price * 1.002]

    def get_completed(self, asset, tf):
        with self.lock:
            return [c for c in self.candles[tf][asset] if c.get("complete", False)]

    def detect_candle_patterns(self, asset, tf=900):
        with self.lock:
            completed = self.get_completed(asset, tf)
            if len(completed) < 2:
                return {}
            last, prev = completed[-1], completed[-2]
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
                if last["close"] > last["open"] and prev["close"] < prev["open"] and body > abs(prev["close"] - prev["open"]):
                    patterns["bullish_engulfing"] = 1
                if last["close"] < last["open"] and prev["close"] > prev["open"] and body > abs(prev["close"] - prev["open"]):
                    patterns["bearish_engulfing"] = 1
            return patterns

    def get_atr(self, asset, period=14, tf=3600):
        with self.lock:
            complete = self.get_completed(asset, tf)
            if len(complete) < period + 1:
                return 0.0
            recent = complete[-(period + 1):]
            tr_list = []
            for i in range(1, len(recent)):
                high, low = recent[i]["high"], recent[i]["low"]
                prev_close = recent[i-1]["close"]
                tr_list.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
            return sum(tr_list) / len(tr_list) if tr_list else 0.0

    def detect_liquidity_sweep(self, asset, price):
        with self.lock:
            completed = self.get_completed(asset, 900)
            h, l = self.pivots[asset]["high"], self.pivots[asset]["low"]
            if not completed:
                return ""
            last = completed[-1]
            if l and last["low"] < min(l[-2:]) and last["close"] > min(l[-2:]):
                return "BUY_SWEEP"
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

    def get_adx(self, asset, tf, period=14):
        with self.lock:
            complete = self.get_completed(asset, tf)
            if len(complete) < period * 2:
                return 20.0
            recent = complete[-(period * 3):]
            tr_list, dm_plus, dm_minus = [], [], []
            for i in range(1, len(recent)):
                high, low = recent[i]["high"], recent[i]["low"]
                ph, pl, pc = recent[i-1]["high"], recent[i-1]["low"], recent[i-1]["close"]
                tr = max(high - low, abs(high - pc), abs(low - pc))
                tr_list.append(tr)
                up = high - ph
                down = pl - low
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
                c1, c2, c3 = recent[i-2], recent[i-1], recent[i]
                if c3["low"] > c1["high"] and c2["close"] > c2["open"]:
                    fvgs.append({"type": "bullish", "upper": c3["low"], "lower": c1["high"]})
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
                c, next_c = recent[i], recent[i+1]
                if (next_c["high"] - next_c["low"]) > 1.3 * atr:
                    if next_c["close"] > next_c["open"] and c["close"] < c["open"]:
                        return {"type": "bullish", "high": c["high"], "low": c["low"]}
                    elif next_c["close"] < next_c["open"] and c["close"] > c["open"]:
                        return {"type": "bearish", "high": c["high"], "low": c["low"]}
            return {}

    def _calc_rsi(self, closes, period=14):
        if len(closes) < period + 1:
            return 50.0
        deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
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
# INDICATOR CACHE & NEUTRAL CANDLE ENGINE
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
            data = self._compute(asset, tf)
            self.cache[key] = {'data': data, 'timestamp': now}
            return data

    def _compute(self, asset, tf):
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
                return data
        except Exception as e:
            logger.error(f"Indicator cache error: {e}")
            return {}


class NeutralCandleEngine:
    def __init__(self, topology):
        self.topology = topology

    def detect(self, asset, price, tf=900):
        with self.topology.lock:
            completed = self.topology.get_completed(asset, tf)
            if len(completed) < 2:
                return {'pattern': 'None', 'direction': '', 'score': 0, 'reason': 'Insufficient data'}
            c = completed[-1]
            o, cl, h, l = c['open'], c['close'], c['high'], c['low']
            body = abs(cl - o)
            range_ = h - l
            if range_ <= 0:
                return {'pattern': 'None', 'direction': '', 'score': 0, 'reason': 'Zero range'}
            uw = h - max(o, cl)
            lw = min(o, cl) - l
            bp = body / range_
            pattern, direction, score = 'None', '', 0
            if bp <= 0.08:
                if uw > 2.5 * lw and uw > body:
                    pattern, direction, score = 'GravestoneDoji', 'SELL', 8
                elif lw > 2.5 * uw and lw > body:
                    pattern, direction, score = 'DragonflyDoji', 'BUY', 8
                else:
                    pattern, score = 'Doji', 2
            elif bp <= 0.35:
                if uw > lw * 2.0 and uw > body * 1.5:
                    pattern, direction, score = 'ShootingStar', 'SELL', 8
                elif lw > uw * 2.0 and lw > body * 1.5:
                    pattern, direction, score = 'Hammer', 'BUY', 8
                else:
                    pattern, score = 'SpinningTop', 2
            return {'pattern': pattern, 'direction': direction, 'score': score,
                    'reason': f"{pattern} ({direction or 'neutral'})"}


# =====================================================================
# POSITION SIZING ENGINE (NEW v10)
# =====================================================================
class PositionSizingEngine:
    def __init__(self, memory_engine):
        self.memory = memory_engine

    def calculate(self, entry: float, sl: float, account_balance: float = None) -> dict:
        balance = account_balance or Config.ACCOUNT_BALANCE_USD
        risk_amount = balance * (Config.RISK_PER_TRADE_PCT / 100.0)
        risk_per_unit = abs(entry - sl)
        if risk_per_unit <= 0:
            return {"quantity": 0.0, "risk_usd": 0.0, "notional": 0.0, "error": "Invalid SL"}
        qty = risk_amount / risk_per_unit
        notional = qty * entry
        max_notional = balance * Config.MAX_LEVERAGE
        if notional > max_notional:
            qty = max_notional / entry
            notional = max_notional
        return {
            "quantity": round(qty, 6),
            "risk_usd": round(risk_amount, 2),
            "notional": round(notional, 2),
            "leverage_used": round(notional / balance, 2)
        }

    def portfolio_heat_ok(self, active_trades: dict) -> Tuple[bool, float]:
        total_risk = 0.0
        balance = Config.ACCOUNT_BALANCE_USD
        for t in active_trades.values():
            r = float(t.get('risk_usd', 0) or 0)
            total_risk += r
        heat_pct = (total_risk / balance * 100.0) if balance > 0 else 0.0
        return heat_pct < Config.MAX_PORTFOLIO_HEAT_PCT, round(heat_pct, 2)


# =====================================================================
# INSTITUTIONAL SIGNAL ENGINE (With Fixed CVD-rate-of-change)
# =====================================================================
class InstitutionalSignalEngine:
    def __init__(self, topology, futures_stream, absorption_meter, neutral_engine):
        self.topology = topology
        self.futures = futures_stream
        self.absorption = absorption_meter
        self.neutral = neutral_engine
        self.multi_oi = MultiExchangeOIFetcher()
        self._cvd_history = {a: deque(maxlen=20) for a in Config.ASSETS}

    @staticmethod
    def get_ist_session() -> Tuple[str, int]:
        now_ist = datetime.now(Config.IST)
        m = now_ist.hour * 60 + now_ist.minute
        if 90 <= m < 390:
            return "DEAD_ZONE", -5
        if m >= 1050 or m < 90:
            return "LONDON_NY_PRIME", 5
        return "ASIA_EUROPE", 0

    def get_oi_quadrant(self, asset: str, price_change: float) -> Tuple[str, str]:
        oi_delta = self.futures.get_oi_trend(asset.lower())
        if price_change >= 0 and oi_delta > 0:
            return "NEW_LONGS", "🟢 New Longs"
        elif price_change >= 0 and oi_delta < 0:
            return "SHORT_COVERING", "⚠️ Short Covering"
        elif price_change < 0 and oi_delta > 0:
            return "NEW_SHORTS", "🔴 New Shorts"
        elif price_change < 0 and oi_delta < 0:
            return "LONG_LIQUIDATION", "⚠️ Long Liq"
        return "NEUTRAL", "⚪ Neutral"

    def _detect_regime(self, asset, price, last_15m_vol, c15, rsi_15, ema20_15, atr_15, oi_data):
        result = {"regime": "RANGE", "parabolic": False, "overextended": False,
                  "vol_expansion": False, "oi_expanding": False}
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

    def evaluate(self, asset: str, price: float, volume: float, btc_state: dict = None):
        oi_data = self.multi_oi.fetch_composite_oi(asset)
        abs_meter = self.absorption.get_meter(asset)
        neutral_info = self.neutral.detect(asset, price, tf=900)
        funding_rate = self.futures.get_funding_rate(asset.lower())
        ob_imbalance = self.futures.get_ob_imbalance(asset.lower())
        session_name, session_mod = self.get_ist_session()

        # FIXED: CVD rate-of-change (delta over last N ticks)
        current_cvd = self.futures.get_cvd(asset.lower())
        self._cvd_history[asset].append(current_cvd)
        if len(self._cvd_history[asset]) >= 5:
            cvd_roc = self._cvd_history[asset][-1] - self._cvd_history[asset][-5]
        else:
            cvd_roc = 0.0

        breakdown = {"Session": session_name, "CVDRoC": round(cvd_roc, 2)}
        with self.topology.lock:
            c15 = self.topology.get_completed(asset, 900)
            if len(c15) < 55:
                return {"status": "NO SETUP", "direction": None, "score": 0,
                        "reason": "Insufficient 15m data", "breakdown": {}}
            c1h = self.topology.get_completed(asset, 3600)
            if len(c1h) < 55:
                return {"status": "NO SETUP", "direction": None, "score": 0,
                        "reason": "Insufficient 1h data", "breakdown": {}}

            closes_15 = [c['close'] for c in c15[-150:]]
            closes_1h = [c['close'] for c in c1h[-150:]]
            last_15m_vol = c15[-1]["volume"]
            price_change_15m = c15[-1]["close"] - c15[-1]["open"]

            ema20_15 = self.topology._ema(closes_15, 20)[-1]
            ema50_15 = self.topology._ema(closes_15, 50)[-1]
            ema20_1h = self.topology._ema(closes_1h, 20)[-1]
            ema50_1h = self.topology._ema(closes_1h, 50)[-1]
            rsi_15 = self.topology._calc_rsi(closes_15, 14)
            atr_15 = self.topology.get_atr(asset, period=14, tf=900) or (price * 0.008)

            regime = self._detect_regime(asset, price, last_15m_vol, c15, rsi_15, ema20_15, atr_15, oi_data)
            breakdown["Regime"] = regime["regime"]

            if regime["regime"] == "RANGE" and abs(price - ema20_15) <= (Config.ATR_NOISE_MULTIPLIER * atr_15):
                return {"status": "NO SETUP", "direction": None, "score": 0,
                        "reason": "Chop Zone: Near EMA20", "breakdown": breakdown}

            trend_15_bull = ema20_15 > ema50_15
            trend_1h_bull = ema20_1h > ema50_1h

            # ---- LAYER 1: MTF TREND + SESSION (Max 25) ----
            if trend_15_bull and trend_1h_bull and rsi_15 >= 52:
                direction, trend_score = "BUY", 22
            elif not trend_15_bull and not trend_1h_bull and rsi_15 <= 48:
                direction, trend_score = "SELL", 22
            elif trend_15_bull and rsi_15 >= 62 and price > ema20_15:
                direction, trend_score = "BUY", 18
                breakdown["TrendNotice"] = "15m Fast Breakout"
            elif not trend_15_bull and rsi_15 <= 38 and price < ema20_15:
                direction, trend_score = "SELL", 18
                breakdown["TrendNotice"] = "15m Fast Breakdown"
            elif regime["regime"] == "REVERSAL" and (rsi_15 >= 74 or rsi_15 <= 26):
                direction = "SELL" if rsi_15 >= 74 else "BUY"
                trend_score = 18
                breakdown["TrendNotice"] = "Extreme RSI Reversal"
            else:
                return {"status": "NO SETUP", "direction": None, "score": 0,
                        "reason": "MTF Trend Conflict", "breakdown": breakdown}

            trend_score = max(5, min(25, trend_score + (3 if session_mod > 0 else session_mod)))
            breakdown["Trend"] = trend_score

            # ---- LAYER 2: SMC LOCATION (Max 25) ----
            loc_score, loc_reasons = 0, []
            sr = self.topology.support_resistance[asset]
            nearest_support = max(sr["support"]) if sr["support"] else None
            nearest_resistance = min(sr["resistance"]) if sr["resistance"] else None
            dist_from_ema = (abs(price - ema20_15) / ema20_15 * 100) if ema20_15 > 0 else 0
            atr_pct = (atr_15 / price * 100) if price > 0 else 1.0

            if regime["regime"] == "MOMENTUM":
                loc_score += 16 if dist_from_ema > atr_pct else 12
                loc_reasons.append("MomentumZone")
            elif regime["regime"] == "REVERSAL":
                if direction == "SELL" and nearest_resistance and abs(price - nearest_resistance) <= 1.5 * atr_15:
                    loc_score += 16
                    loc_reasons.append("ResistanceReversal")
                elif direction == "BUY" and nearest_support and abs(price - nearest_support) <= 1.5 * atr_15:
                    loc_score += 16
                    loc_reasons.append("SupportReversal")
            else:
                if direction == "BUY" and nearest_support and abs(price - nearest_support) <= 1.8 * atr_15:
                    loc_score += 12
                    loc_reasons.append("NearSupport")
                elif direction == "SELL" and nearest_resistance and abs(price - nearest_resistance) <= 1.8 * atr_15:
                    loc_score += 12
                    loc_reasons.append("NearResistance")

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

            # ---- LAYER 3: ORDER FLOW (Max 30) ----
            flow_score, flow_reasons = 0, []
            quad_code, quad_label = self.get_oi_quadrant(asset, price_change_15m)
            breakdown["OIQuadrant"] = quad_code

            if direction == "BUY":
                if quad_code == "NEW_LONGS":
                    flow_score += 10
                    flow_reasons.append("OI:NewLongs")
                elif quad_code == "SHORT_COVERING" and regime["regime"] != "REVERSAL":
                    flow_score -= 6
                    flow_reasons.append("OI:ShortCoverTrap")
            else:
                if quad_code == "NEW_SHORTS":
                    flow_score += 10
                    flow_reasons.append("OI:NewShorts")
                elif quad_code == "LONG_LIQUIDATION" and regime["regime"] != "REVERSAL":
                    flow_score -= 6
                    flow_reasons.append("OI:LongLiqTrap")

            if direction == "BUY":
                if funding_rate > Config.EXTREME_FUNDING_RATE:
                    flow_score -= 8
                    flow_reasons.append("CrowdedLongFunding")
                elif funding_rate < 0:
                    flow_score += 5
                    flow_reasons.append("NegFundingSqueeze")
            else:
                if funding_rate < -Config.EXTREME_FUNDING_RATE:
                    flow_score -= 8
                    flow_reasons.append("CrowdedShortFunding")
                elif funding_rate > Config.EXTREME_FUNDING_RATE:
                    flow_score += 5
                    flow_reasons.append("PosFundingFlush")

            if direction == "BUY" and ob_imbalance >= Config.OB_BULL_IMBALANCE:
                flow_score += 6
                flow_reasons.append(f"OB_BidWall({ob_imbalance}x)")
            elif direction == "SELL" and ob_imbalance <= Config.OB_BEAR_IMBALANCE:
                flow_score += 6
                flow_reasons.append(f"OB_AskWall({ob_imbalance}x)")

            if regime["vol_expansion"]:
                flow_score += 5
                flow_reasons.append("VolExpansion")

            # FIXED: use CVD rate-of-change, not absolute
            if direction == "BUY" and cvd_roc > 0:
                flow_score += 5
                flow_reasons.append(f"CVD_ROC+({cvd_roc:.1f})")
            elif direction == "SELL" and cvd_roc < 0:
                flow_score += 5
                flow_reasons.append(f"CVD_ROC-({cvd_roc:.1f})")

            if abs_meter.get("direction") == direction and abs_meter.get("score", 0) >= Config.ABSORPTION_MIN_SCORE:
                flow_score += 4
                flow_reasons.append(f"Absorption({abs_meter['score']})")

            flow_score = max(0, min(flow_score, 30))
            breakdown["VolumeOrderFlow"] = flow_score
            breakdown["VolFlowDetails"] = " | ".join(flow_reasons) if flow_reasons else "Neutral"

            # ---- LAYER 4: STRUCTURE + BTC CORRELATION (Max 20) ----
            struct_score, struct_reasons = 0, []
            if self.topology.choch.get(asset, False):
                struct_score += 5
                struct_reasons.append("CHoCH")
            bos = self.topology.bos[asset]["direction"]
            if (direction == "BUY" and bos == "UP") or (direction == "SELL" and bos == "DOWN"):
                struct_score += 5
                struct_reasons.append("BOS")

            sweep = self.topology.detect_liquidity_sweep(asset, price)
            if (direction == "BUY" and sweep == "BUY_SWEEP") or (direction == "SELL" and sweep == "SELL_SWEEP"):
                struct_score += 6
                struct_reasons.append(sweep)

            patterns = self.topology.detect_candle_patterns(asset, tf=900)
            if direction == "BUY" and ("bullish_pinbar" in patterns or "bullish_engulfing" in patterns):
                struct_score += 4
                struct_reasons.append("BullishCandle")
            elif direction == "SELL" and ("bearish_pinbar" in patterns or "bearish_engulfing" in patterns):
                struct_score += 4
                struct_reasons.append("BearishCandle")

            if neutral_info.get("direction") == direction:
                struct_score += 3
                struct_reasons.append(neutral_info["pattern"])

            if asset != "BTCUSDT" and btc_state:
                btc_t15 = btc_state.get("trend", "NEUTRAL")
                btc_t1h = btc_state.get("htf_trend", "NEUTRAL")
                if direction == "BUY" and btc_t15 == "BEARISH" and btc_t1h == "BEARISH":
                    struct_score -= 8
                    struct_reasons.append("BTC_BearHeadwind")
                elif direction == "SELL" and btc_t15 == "BULLISH" and btc_t1h == "BULLISH":
                    struct_score -= 8
                    struct_reasons.append("BTC_BullHeadwind")
                elif (direction == "BUY" and btc_t15 == "BULLISH") or (direction == "SELL" and btc_t15 == "BEARISH"):
                    struct_score += 4
                    struct_reasons.append("BTC_KingAligned")

            if dist_from_ema < (2.2 * atr_pct):
                struct_score += 4

            struct_score = max(0, min(struct_score, 20))
            breakdown["CandleStructure"] = struct_score
            breakdown["StructureDetails"] = " | ".join(struct_reasons) if struct_reasons else "None"

            # ---- FINAL ----
            total_score = trend_score + loc_score + flow_score + struct_score
            breakdown["Total"] = total_score
            breakdown["Direction"] = direction

            if regime["regime"] == "MOMENTUM":
                te, th, tv = 72, 60, 52
            elif regime["regime"] == "REVERSAL":
                te, th, tv = 78, 68, 58
            else:
                te, th, tv = Config.SCORE_EXTREME, Config.SCORE_HIGH, Config.SCORE_VALID

            if session_name == "DEAD_ZONE":
                tv += 6
                th += 5

            if total_score >= te:
                status = "EXTREME"
            elif total_score >= th:
                status = "HIGH"
            elif total_score >= tv:
                status = "VALID"
            elif total_score >= Config.SCORE_WATCH:
                status = "WATCH"
            else:
                status = "REJECTED"

            reason = (f"[{session_name}] {quad_code} | T:{trend_score} L:{loc_score} "
                      f"F:{flow_score} S:{struct_score} | CVD-ROC:{cvd_roc:+.1f}")
            return {
                "status": status, "direction": direction, "score": total_score,
                "reason": reason, "breakdown": breakdown, "patterns": patterns,
                "oi_quadrant": quad_label
            }


# =====================================================================
# DYNAMIC STOP LOSS (3-Tier TP + Slippage Aware)
# =====================================================================
class DynamicStopLoss:
    def __init__(self, topology):
        self.topology = topology

    def calculate(self, asset, direction, entry, atr):
        buffer = max(atr * 0.8, entry * 0.004)
        with self.topology.lock:
            sr = self.topology.support_resistance[asset]
            nearest_support, nearest_resistance = None, None
            if sr["support"]:
                c = [s for s in sr["support"] if s < entry and (entry - s) / entry < 0.06]
                if c:
                    nearest_support = max(c)
            if sr["resistance"]:
                c = [r for r in sr["resistance"] if r > entry and (r - entry) / entry < 0.06]
                if c:
                    nearest_resistance = min(c)

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
            tp1 = entry + Config.TP1_RR * risk
            tp2 = entry + Config.TP2_RR * risk
            tp3 = entry + Config.TP3_RR * risk
        else:
            tp1 = entry - Config.TP1_RR * risk
            tp2 = entry - Config.TP2_RR * risk
            tp3 = entry - Config.TP3_RR * risk

        return sl, tp1, tp2, tp3, risk, Config.TP2_RR


# =====================================================================
# 5M PENDING QUEUE (FIXED: MCB re-check + tighter adverse threshold)
# =====================================================================
class PendingVerificationQueue:
    def __init__(self, topology, futures_stream, memory_engine, correlation_fn):
        self.topology = topology
        self.futures = futures_stream
        self.memory = memory_engine
        self.correlation_fn = correlation_fn
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
            latest = completed[-1]

            # FIXED: check MCB before releasing any queued signals
            mem = self.memory.get_or_create_state()
            mcb_until = int(mem.get("mcb_until_ts", 0))
            mcb_active = mcb_until > int(time.time())
            daily_cb_until = int(mem.get("daily_cb_until_ts", 0))
            weekly_cb_until = int(mem.get("weekly_cb_until_ts", 0))
            now_ts = int(time.time())
            daily_cb_active = daily_cb_until > now_ts
            weekly_cb_active = weekly_cb_until > now_ts
            cb_active = mcb_active or daily_cb_active or weekly_cb_active

            for key, data in list(self.pending.items()):
                if data['asset'] != asset:
                    continue
                if latest["timestamp"] <= data.get("added_5m_ts", 0):
                    continue

                data['candles_verified'] += 1
                data['added_5m_ts'] = latest["timestamp"]
                limit = data.get('pending_candles', Config.PENDING_VERIFICATION_CANDLES)
                vol_decay = data.get('volume_decay_threshold', Config.VOLUME_DECAY_THRESHOLD)

                if cb_active:
                    data['reject_reason'] = "MCB/CB activated during pending wait"
                    self.rejected_pending.append(data)
                    to_remove.append(key)
                    continue

                if data['base_volume'] > 0 and latest["volume"] < data['base_volume'] * (1.0 - vol_decay):
                    data['reject_reason'] = "5m Verify Failed: Volume Decay"
                    self.rejected_pending.append(data)
                    to_remove.append(key)
                    continue

                ob_ratio = self.futures.get_ob_imbalance(asset.lower())
                if data['direction'] == 'BUY' and ob_ratio < 0.55:
                    data['reject_reason'] = f"5m Verify Failed: Ask Wall ({ob_ratio}x)"
                    self.rejected_pending.append(data)
                    to_remove.append(key)
                    continue
                elif data['direction'] == 'SELL' and ob_ratio > 1.85:
                    data['reject_reason'] = f"5m Verify Failed: Bid Wall ({ob_ratio}x)"
                    self.rejected_pending.append(data)
                    to_remove.append(key)
                    continue

                close_5m = latest['close']
                atr_5m = self.topology.get_atr(asset, period=14, tf=300) or (data['start_price'] * 0.005)
                max_adverse = 0.85 * atr_5m

                if data['direction'] == 'BUY' and (data['start_price'] - close_5m) > max_adverse:
                    data['reject_reason'] = "5m Verify Failed: Bearish Breakdown"
                    self.rejected_pending.append(data)
                    to_remove.append(key)
                    continue
                elif data['direction'] == 'SELL' and (close_5m - data['start_price']) > max_adverse:
                    data['reject_reason'] = "5m Verify Failed: Bullish Breakout"
                    self.rejected_pending.append(data)
                    to_remove.append(key)
                    continue

                # NEW: signal decay check — if price drifted too far
                drift = abs(close_5m - data['start_price'])
                if drift > 0.35 * atr_5m:
                    data['reject_reason'] = f"5m Verify Failed: Entry Drift {drift:.2f} > 0.35*ATR"
                    self.rejected_pending.append(data)
                    to_remove.append(key)
                    continue

                if data['candles_verified'] >= limit:
                    data['entry'] = close_5m
                    self.verified.append(data)
                    to_remove.append(key)

            for key in to_remove:
                self.pending.pop(key, None)
            return to_remove

    def clear_all(self, reason="Circuit Breaker"):
        with self.lock:
            for key, data in list(self.pending.items()):
                data['reject_reason'] = reason
                self.rejected_pending.append(data)
            self.pending.clear()

    def get_verified_signals(self):
        with self.lock:
            ready = self.verified[:]
            self.verified = []
            return ready

    def get_rejected_pending(self):
        with self.lock:
            r = self.rejected_pending[:]
            self.rejected_pending = []
            return r


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
        self.meter_state = {a: {"score": 0, "direction": "", "last_update": 0} for a in Config.ASSETS}
        self.lock = threading.Lock()

    def update_meter(self, asset, price):
        sym = asset.lower()
        oi_trend = self.futures.get_oi_trend(sym)
        cvd = self.futures.get_cvd(sym)
        liqs = self.futures.get_liquidations(asset, 120)
        score, direction = 0, ""
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
        m = self.get_meter(asset)
        if m["score"] >= Config.ABSORPTION_EXIT_SCORE:
            if current_direction == "BUY" and m["direction"] == "SELL":
                return True
            if current_direction == "SELL" and m["direction"] == "BUY":
                return True
        return False


# =====================================================================
# TELEGRAM PIPELINE (Inline Buttons Support)
# =====================================================================
class TelegramPipeline:
    def __init__(self, orch=None):
        self.token = Config.TELEGRAM_BOT_TOKEN
        self.chat_id = Config.TELEGRAM_CHAT_ID
        self.orch = orch
        self.queue = queue.Queue()
        threading.Thread(target=self._worker, daemon=True).start()

    def _worker(self):
        while True:
            item = self.queue.get()
            if not self.token or not self.chat_id:
                continue
            try:
                if isinstance(item, dict):
                    requests.post(f"https://api.telegram.org/bot{self.token}/sendMessage", data=item, timeout=10)
                else:
                    requests.post(f"https://api.telegram.org/bot{self.token}/sendMessage",
                                  data={"chat_id": self.chat_id, "text": item, "parse_mode": "HTML"}, timeout=10)
            except Exception as e:
                logger.error(f"Telegram send error: {e}")

    def send_message(self, text):
        self.queue.put(text)

    def fire_signal(self, asset, direction, price, sl, tp1, tp2, tp3, logic, news,
                    score, trade_id, session, rr, regime, signal_token=None, tier="HIGH", quantity=0.0):
        dir_emoji = "🟢 LONG / BUY" if direction == "BUY" else "🔴 SHORT / SELL"
        tier_badge = "🔥 EXTREME" if tier == "EXTREME" else ("⚡ HIGH" if tier == "HIGH" else "✅ VALID")
        msg = (
            f"🏛️ <b>ALPHABOT v10.0 FINAL SIGNAL</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{Config.DISPLAY_NAMES.get(asset, asset)}</b> | <b>{regime}</b>\n"
            f"📌 <b>{dir_emoji}</b>\n"
            f"🏆 <b>Quality:</b> {tier_badge} ({score}/100)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Entry:</b> <code>{price:.2f}</code>\n"
            f"🛑 <b>SL:</b> <code>{sl:.2f}</code>\n"
            f"🎯 <b>TP1 (1R - 50%):</b> <code>{tp1:.2f}</code>\n"
            f"🎯 <b>TP2 (2R - 30%):</b> <code>{tp2:.2f}</code>\n"
            f"🚀 <b>TP3 (3.5R - 20%):</b> <code>{tp3:.2f}</code>\n"
            f"📦 <b>Qty:</b> <code>{quantity}</code> | <b>RR:</b> 1:{rr:.2f}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🧠 {html.escape(logic)}\n"
            f"🆔 <code>{signal_token or trade_id}</code>"
        )
        keyboard = {
            "inline_keyboard": [[
                {"text": "❌ Close Trade", "callback_data": f"close_{trade_id}"},
                {"text": "ℹ️ Info", "callback_data": f"info_{trade_id}"}
            ]]
        }
        self.queue.put({
            "chat_id": self.chat_id, "text": msg,
            "parse_mode": "HTML",
            "reply_markup": json.dumps(keyboard)
        })

    def fire_partial_tp(self, trade_id, asset, direction, tp_level, price, new_sl):
        msg = (
            f"🎯 <b>{tp_level} – {Config.DISPLAY_NAMES.get(asset, asset)} (#{trade_id})</b>\n"
            f"✅ Price: <code>{price:.2f}</code>\n"
            f"🛡️ SL: <code>{new_sl:.2f}</code>"
        )
        self.queue.put(msg)

    def fire_trade_close(self, trade_id, asset, entry, exit_price, pnl, reason, entry_time):
        hold_min = max(1, int((time.time() - entry_time) / 60))
        pnl_pct = (pnl / entry) * 100 if entry else 0
        status = "🟢 PROFIT" if pnl > 0 else "🔴 LOSS" if pnl < 0 else "⚪ BE"
        msg = (
            f"<b>🔒 Trade #{trade_id} Closed – {Config.DISPLAY_NAMES.get(asset, asset)}</b>\n"
            f"📊 {status} | Entry {entry:.2f} → Exit {exit_price:.2f}\n"
            f"💰 PnL: {pnl:+.2f} ({pnl_pct:+.2f}%) | ⏱️ {hold_min}min\n"
            f"📌 {html.escape(reason)}"
        )
        self.queue.put(msg)

    def fire_cb_alert(self, cb_type, minutes_left):
        msg = (
            f"⚠️ <b>CIRCUIT BREAKER TRIPPED ({cb_type})</b>\n"
            f"🛡️ Bot paused for <b>{minutes_left} min</b> to protect capital.\n"
            f"Pending signals have been cancelled."
        )
        self.queue.put(msg)


# =====================================================================
# ACTIVE TRADE LIFECYCLE (with Daily/Weekly CB check)
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
                    current_price = self.orch.asset_state.get(asset, {}).get("price") or trade['entry']
                    trade_duration = now - trade.get('entry_time', now)
                    health = self.orch.health_engine.calculate_health(trade)
                    trade['health'] = health

                    if self.orch.absorption_meter.get_strong_opposite(asset, trade['direction']):
                        raw = (current_price - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - current_price)
                        to_close.append((tid, current_price, raw, "Reversal-Absorption"))
                        continue

                    if health < Config.HEALTH_EMERGENCY_THRESHOLD and not trade.get('tp1_hit'):
                        raw = (current_price - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - current_price)
                        to_close.append((tid, current_price, raw, f"EmergencyHealth-{health}%"))
                        continue

                    if trade_duration > Config.MAX_HOLD_TIME:
                        raw = (current_price - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - current_price)
                        to_close.append((tid, current_price, raw, "MaxHold-4h"))
                        continue

                    if (not trade.get('tp1_hit') and trade_duration > Config.TIME_DECAY_SECONDS and
                            abs(current_price - trade['entry']) / max(trade['entry'], 1e-6) < Config.TIME_DECAY_THRESHOLD_PCT):
                        raw = (current_price - trade['entry']) if trade['direction'] == 'BUY' else (trade['entry'] - current_price)
                        to_close.append((tid, current_price, raw, "TimeDecay-60m"))
                        continue

                for tid, price, raw, reason in to_close:
                    self.orch._close_trade(tid, price, raw, reason)
                gc.collect()


# =====================================================================
# BACKTEST ENGINE (NEW v10)
# =====================================================================
class BacktestEngine:
    """Offline replay of signal engine over historical candles."""

    def __init__(self, topology, signal_engine, dynamic_sl, slippage_pct=None):
        self.topology = topology
        self.signal_engine = signal_engine
        self.dynamic_sl = dynamic_sl
        self.slippage = slippage_pct if slippage_pct is not None else Config.SLIPPAGE_PCT

    def fetch_historical(self, symbol: str, interval: str = "15m", days: int = 365) -> List[dict]:
        """Fetch up to `days` of candles from Binance (auto-paginates)."""
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - (days * 24 * 3600 * 1000)
        url = "https://api.binance.com/api/v3/klines"
        all_klines = []
        cursor = start_ms
        while cursor < end_ms:
            try:
                r = requests.get(url, params={
                    "symbol": symbol, "interval": interval,
                    "startTime": cursor, "limit": 1000
                }, timeout=15)
                if r.status_code != 200:
                    break
                batch = r.json()
                if not batch:
                    break
                all_klines.extend(batch)
                cursor = batch[-1][0] + 1
                time.sleep(0.12)
            except Exception as e:
                logger.error(f"Historical fetch error: {e}")
                break

        candles = []
        for d in all_klines:
            c = {
                "timestamp": d[0] // 1000,
                "open": float(d[1]), "high": float(d[2]),
                "low": float(d[3]), "close": float(d[4]),
                "volume": float(d[5]), "complete": True
            }
            if DataValidator.validate_candle(c):
                candles.append(c)
        return candles

    def run(self, symbol: str, days: int = None) -> dict:
        """Simple replay: iterate candles, evaluate signal at each close, simulate TP/SL."""
        days = days or Config.BACKTEST_LOOKBACK_DAYS
        logger.info(f"🧪 Backtest starting for {symbol} ({days} days)...")
        candles = self.fetch_historical(symbol, Config.BACKTEST_INTERVAL, days)
        if len(candles) < 100:
            return {"error": "Not enough candles", "symbol": symbol}

        # Load into topology at 15m only
        with self.topology.lock:
            self.topology.candles[900][symbol] = candles
            c15 = candles
            self.topology.volume_ma[symbol] = sum(c["volume"] for c in c15[-20:]) / 20.0 if len(c15) >= 20 else 0

        trades = []
        balance = Config.BACKTEST_INITIAL_BALANCE
        peak = balance
        max_dd = 0.0

        # Replay every 4th 15m candle (once per hour) to keep it fast
        step = 4
        for i in range(60, len(candles) - 2, step):
            with self.topology.lock:
                # Trim view to candles up to i
                self.topology.candles[900][symbol] = candles[:i+1]
            try:
                price = candles[i]["close"]
                res = self.signal_engine.evaluate(symbol, price, candles[i]["volume"], btc_state=None)
            except Exception:
                continue

            if res.get("status") not in ("VALID", "HIGH", "EXTREME"):
                continue

            direction = res["direction"]
            entry = price
            atr = self.topology.get_atr(symbol, period=14, tf=900) or (price * 0.008)
            sl, tp1, tp2, tp3, risk, rr = self.dynamic_sl.calculate(symbol, direction, entry, atr)

            # Simulate forward on next N candles
            tp1_hit = tp2_hit = False
            exit_price, exit_reason = None, None
            for j in range(i + 1, min(i + 96, len(candles))):  # max 24h hold
                c = candles[j]
                if direction == "BUY":
                    if not tp1_hit and c["high"] >= tp1:
                        tp1_hit = True
                        sl = entry
                    if tp1_hit and not tp2_hit and c["high"] >= tp2:
                        tp2_hit = True
                        sl = tp1
                    if c["low"] <= sl:
                        exit_price, exit_reason = sl * (1 - self.slippage), "SL"
                        break
                    if c["high"] >= tp3:
                        exit_price, exit_reason = tp3 * (1 - self.slippage), "TP3"
                        break
                else:
                    if not tp1_hit and c["low"] <= tp1:
                        tp1_hit = True
                        sl = entry
                    if tp1_hit and not tp2_hit and c["low"] <= tp2:
                        tp2_hit = True
                        sl = tp1
                    if c["high"] >= sl:
                        exit_price, exit_reason = sl * (1 + self.slippage), "SL"
                        break
                    if c["low"] <= tp3:
                        exit_price, exit_reason = tp3 * (1 + self.slippage), "TP3"
                        break

            if exit_price is None:
                exit_price = candles[min(i + 96, len(candles)-1)]["close"]
                exit_reason = "TimeExit"

            # Weighted PnL (matching live logic)
            if tp2_hit:
                move = 0.5 * abs(tp1 - entry) + 0.3 * abs(tp2 - entry) + 0.2 * abs(exit_price - entry)
                if direction == "SELL":
                    move = -move if exit_price > entry else move
            elif tp1_hit:
                move = 0.5 * abs(tp1 - entry) + 0.5 * abs(exit_price - entry)
                if direction == "SELL":
                    move = -move if exit_price > entry else move
            else:
                move = (exit_price - entry) if direction == "BUY" else (entry - exit_price)
                move -= abs(entry) * self.slippage

            # Guard: SELL trades — PnL should be entry - exit when profitable
            if direction == "SELL" and not tp1_hit:
                move = (entry - exit_price) - abs(entry) * self.slippage

            risk_usd = balance * (Config.RISK_PER_TRADE_PCT / 100.0)
            pnl_usd = (move / risk) * risk_usd if risk > 0 else 0
            balance += pnl_usd
            peak = max(peak, balance)
            max_dd = max(max_dd, (peak - balance) / peak * 100 if peak > 0 else 0)

            trades.append({
                "i": i, "direction": direction, "entry": entry, "sl": sl,
                "tp1": tp1, "tp2": tp2, "tp3": tp3,
                "exit": exit_price, "reason": exit_reason,
                "tp1_hit": tp1_hit, "tp2_hit": tp2_hit, "pnl": round(pnl_usd, 4),
                "score": res["score"], "tier": res["status"]
            })

        wins = sum(1 for t in trades if t["pnl"] > 0)
        losses = sum(1 for t in trades if t["pnl"] < 0)
        gross_p = sum(t["pnl"] for t in trades if t["pnl"] > 0)
        gross_l = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
        summary = {
            "symbol": symbol,
            "days": days,
            "candles": len(candles),
            "trades": len(trades),
            "wins": wins, "losses": losses,
            "win_rate": round((wins / len(trades) * 100) if trades else 0, 2),
            "profit_factor": round(gross_p / gross_l, 2) if gross_l > 0 else None,
            "total_pnl": round(sum(t["pnl"] for t in trades), 2),
            "final_balance": round(balance, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "avg_pnl": round(sum(t["pnl"] for t in trades) / len(trades), 4) if trades else 0
        }
        logger.info(f"🧪 Backtest complete {symbol}: {summary}")
        return {"summary": summary, "trades": trades[-50:]}


# =====================================================================
# CORE ORCHESTRATOR v10.0 FINAL
# =====================================================================
class AIOrchestrator:
    def __init__(self):
        self.topology = CandleTopologyEngine()
        self.cache = IndicatorCache(self.topology)
        self.news = CryptoNewsScanner()
        self.db = TradeDatabase()
        self.mongo = MongoDatabase()
        self.memory = PersistentMemoryEngine(self.mongo, self.db)

        self.futures_stream = BinanceFuturesStream(mongo_db=self.mongo)
        self.futures_stream.start()

        self.absorption_meter = MarketAbsorptionMeter(self.futures_stream, self.topology)
        self.neutral_engine = NeutralCandleEngine(self.topology)

        self.signal_engine = InstitutionalSignalEngine(
            self.topology, self.futures_stream, self.absorption_meter, self.neutral_engine
        )
        self.pending_queue = PendingVerificationQueue(
            self.topology, self.futures_stream, self.memory, self._correlation_shield_ok
        )
        self.dynamic_sl = DynamicStopLoss(self.topology)
        self.health_engine = TradeHealthEngine(self.topology, self.cache)
        self.sizing_engine = PositionSizingEngine(self.memory)

        self.telegram = TelegramPipeline(orch=self)

        self.active_trades = {}
        self.trade_lock = threading.Lock()
        self.price_queue = queue.Queue(maxsize=2000)
        self.start_time = time.time()

        self.signal_timestamps = deque(maxlen=100)
        self.asset_last_signal = {a: 0 for a in Config.ASSETS}

        self.asset_state = {
            a: {
                "price": 0.0, "trend": "NEUTRAL", "htf_trend": "NEUTRAL",
                "volume_ratio": 1.0, "rsi": 50.0, "adx": 20.0,
                "volatility": 0.01, "news_sentiment": 0,
                "neutral_pattern": "None", "absorption_score": 0,
                "absorption_dir": "", "oi_quadrant": "⚪ Neutral",
                "funding_rate": 0.0, "ob_imbalance": 1.0
            } for a in Config.ASSETS
        }
        self.stream = None
        self._ready = False

        self._restore_state_from_db()
        self._init_daily_weekly_baselines()

        self.lifecycle = ActiveTradeLifecycle(self)
        threading.Thread(target=self.lifecycle.monitor_lifecycle, daemon=True).start()
        threading.Thread(target=self._process_queue, daemon=True).start()
        threading.Thread(target=self._memory_sync_loop, daemon=True).start()
        threading.Thread(target=self._meter_update_loop, daemon=True).start()
        threading.Thread(target=self._status_monitor, daemon=True).start()
        threading.Thread(target=self._attribution_report_loop, daemon=True).start()

    def _init_daily_weekly_baselines(self):
        mem = self.memory.get_or_create_state()
        now = int(time.time())
        updates = {}
        # Daily rollover (24h)
        daily_ts = int(mem.get("daily_start_ts", 0))
        if now - daily_ts > 86400:
            updates["daily_start_ts"] = now
            updates["daily_start_balance"] = Config.ACCOUNT_BALANCE_USD + float(mem.get("total_pnl", 0.0))
        # Weekly rollover (7d)
        weekly_ts = int(mem.get("weekly_start_ts", 0))
        if now - weekly_ts > 7 * 86400:
            updates["weekly_start_ts"] = now
            updates["weekly_start_balance"] = Config.ACCOUNT_BALANCE_USD + float(mem.get("total_pnl", 0.0))
        if updates:
            self.memory.update_state(updates, absolute_keys=set(updates.keys()))

    def _restore_state_from_db(self):
        open_trades = self.mongo.get_open_trades() or self.db.get_open_trades_sqlite()
        with self.trade_lock:
            for t in open_trades:
                tid = t.get('id')
                if not tid:
                    continue
                entry = float(t.get('entry', 0))
                sl = float(t.get('stop_loss', 0))
                tp = float(t.get('take_profit', 0))
                risk = abs(entry - sl) if abs(entry - sl) > 0 else entry * 0.01
                d = t.get('direction', 'BUY')
                tp1 = float(t.get('tp1') or (entry + risk if d == 'BUY' else entry - risk))
                tp2 = float(t.get('tp2') or (entry + 2 * risk if d == 'BUY' else entry - 2 * risk))
                tp3 = float(t.get('tp3') or tp)
                self.active_trades[tid] = {
                    'id': tid, 'asset': t.get('asset', ''), 'direction': d,
                    'entry': entry, 'sl': sl, 'tp': tp3,
                    'tp1': tp1, 'tp2': tp2, 'tp3': tp3,
                    'tp1_hit': bool(t.get('tp1_hit', False)),
                    'tp2_hit': bool(t.get('tp2_hit', False)),
                    'entry_time': int(t.get('entry_time', int(time.time()))),
                    'health': 100, 'regime': t.get('regime', 'V10_INST'),
                    'signal_token': t.get('signal_token'),
                    'quantity': float(t.get('quantity', 0) or 0),
                    'risk_usd': float(t.get('risk_usd', 0) or 0)
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
                self.memory.update_state({"total_run_seconds": elapsed, "last_update": now},
                                         absolute_keys={"last_update"})
                # rollover check
                self._init_daily_weekly_baselines()
                last_sync = now

    def _attribution_report_loop(self):
        """Every 6h, log performance attribution."""
        while True:
            time.sleep(6 * 3600)
            try:
                stats = self.db.get_attribution_stats()
                logger.info(f"📊 ATTRIBUTION: {json.dumps(stats, indent=2)[:800]}")
            except Exception as e:
                logger.error(f"Attribution error: {e}")

    def _meter_update_loop(self):
        while True:
            try:
                for asset in Config.ASSETS:
                    price = self.asset_state[asset]["price"]
                    if price > 0:
                        m = self.absorption_meter.update_meter(asset, price)
                        self.asset_state[asset]["absorption_score"] = m["score"]
                        self.asset_state[asset]["absorption_dir"] = m["direction"]
                        self.asset_state[asset]["funding_rate"] = self.futures_stream.get_funding_rate(asset.lower())
                        self.asset_state[asset]["ob_imbalance"] = self.futures_stream.get_ob_imbalance(asset.lower())
                time.sleep(20)
            except Exception as e:
                logger.error(f"Meter update error: {e}")
                time.sleep(20)

    def _status_monitor(self):
        while True:
            time.sleep(60)
            last_tick = self.stream.last_tick_time if self.stream and hasattr(self.stream, 'last_tick_time') else 0
            age = time.time() - last_tick if last_tick else -1
            logger.info(f"⏱️ STATUS: last tick {age:.0f}s | active: {len(self.active_trades)} | pending: {len(self.pending_queue.pending)}")

    def is_mcb_tripped(self) -> Tuple[bool, int]:
        mem = self.memory.get_or_create_state()
        until_ts = int(mem.get("mcb_until_ts", 0))
        now = int(time.time())
        if until_ts > now:
            return True, (until_ts - now) // 60
        return False, 0

    def is_daily_cb_active(self) -> Tuple[bool, int]:
        mem = self.memory.get_or_create_state()
        until_ts = int(mem.get("daily_cb_until_ts", 0))
        now = int(time.time())
        if until_ts > now:
            return True, (until_ts - now) // 60
        return False, 0

    def is_weekly_cb_active(self) -> Tuple[bool, int]:
        mem = self.memory.get_or_create_state()
        until_ts = int(mem.get("weekly_cb_until_ts", 0))
        now = int(time.time())
        if until_ts > now:
            return True, (until_ts - now) // 60
        return False, 0

    def reset_mcb(self):
        self.memory.update_state({
            "consecutive_losses": 0, "mcb_until_ts": 0,
            "daily_cb_until_ts": 0, "weekly_cb_until_ts": 0
        }, absolute_keys={"consecutive_losses", "mcb_until_ts", "daily_cb_until_ts", "weekly_cb_until_ts"})
        logger.info("⚡ All Circuit Breakers manually reset.")

    def _signal_limit_ok(self):
        now = time.time()
        return sum(1 for ts in self.signal_timestamps if now - ts < 86400) < Config.MAX_SIGNALS_PER_DAY

    def _asset_cooldown_ok(self, asset):
        last = self.asset_last_signal.get(asset, 0)
        return (time.time() - last) >= (Config.ASSET_COOLDOWN_HOURS * 3600)

    def _correlation_shield_ok(self, asset: str, direction: str) -> Tuple[bool, str]:
        """FIXED: block only if 2+ same-direction trades already open."""
        same_dir_open = 0
        with self.trade_lock:
            for tid, t in self.active_trades.items():
                if t['asset'] != asset and t['direction'] == direction and not t.get('tp1_hit', False):
                    same_dir_open += 1
        if same_dir_open >= Config.CORRELATION_BLOCK_MIN_OPEN:
            return False, f"Correlation Shield: {same_dir_open} same-direction trades already open"
        return True, ""

    def _handle_price_tick(self, asset, price, volume):
        self.asset_state[asset]["price"] = price
        self.topology.process_tick(asset, price, volume)
        self._update_active_trades(asset, price)

        if self.topology.candle_just_closed[60].get(asset, False) or self.topology.candle_just_closed[300].get(asset, False):
            self._update_indicators(asset, price)

        with self.trade_lock:
            has_open_trade = any(t['asset'] == asset for t in self.active_trades.values())

        # 5m candle close -> pending verification
        if self.topology.candle_just_closed[300].get(asset, False):
            if self.pending_queue.pending:
                self.pending_queue.check_pending(asset)
                for rej in self.pending_queue.get_rejected_pending():
                    self.db.log_rejected(
                        asset=rej['asset'], price=price, score=rej['score'],
                        reason=rej.get('reject_reason', '5m Verify Failed'),
                        volatility=self.asset_state[asset]["volatility"],
                        regime=rej.get('regime', 'V10_INST'),
                        gate_name="5mVerification", status="REJECTED",
                        direction=rej['direction'],
                        score_breakdown=rej.get('score_breakdown', '')
                    )
                    self.memory.update_state({"rejected_signals_count": 1})

                verified = self.pending_queue.get_verified_signals()
                for signal in verified:
                    if not has_open_trade:
                        self._send_final_signal(signal)

        # 15m candle close -> new evaluation
        if self.topology.candle_just_closed[900].get(asset, False):
            self._update_indicators(asset, price)
            if has_open_trade:
                return

            mcb_active, mcb_min = self.is_mcb_tripped()
            daily_active, daily_min = self.is_daily_cb_active()
            weekly_active, weekly_min = self.is_weekly_cb_active()

            if mcb_active or daily_active or weekly_active:
                cb_tag = "MCB" if mcb_active else ("DailyCB" if daily_active else "WeeklyCB")
                mins = mcb_min or daily_min or weekly_min
                self.db.log_observation(
                    asset=asset, price=price, direction="NONE", score=0,
                    status="REJECTED", reason=f"{cb_tag} active ({mins}m left)",
                    breakdown={"CB": cb_tag}, volatility=self.asset_state[asset]["volatility"]
                )
                return

            btc_state = self.asset_state.get("BTCUSDT", {})
            result = self.signal_engine.evaluate(asset, price, volume, btc_state=btc_state)
            status = result["status"]
            direction = result["direction"]
            score = result["score"]
            reason = result["reason"]
            breakdown = result["breakdown"]
            patterns = result.get("patterns", {})
            if result.get("oi_quadrant"):
                self.asset_state[asset]["oi_quadrant"] = result["oi_quadrant"]

            self.db.log_observation(
                asset=asset, price=price, direction=direction or "NONE",
                score=score, status=status, reason=reason,
                breakdown=breakdown, volatility=self.asset_state[asset]["volatility"]
            )
            self.mongo.save_observation({
                "asset": asset, "timestamp": int(time.time()), "price": price,
                "direction": direction or "NONE", "score": score,
                "status": status, "reason": reason
            })
            logger.info(f"🔍 15m Eval [{asset}] | {status} | {score} | {direction} | {reason}")

            if status in ("NO SETUP", "REJECTED", "WATCH"):
                self.db.log_rejected(
                    asset=asset, price=price, score=score,
                    reason=f"{status}: {reason}",
                    volatility=self.asset_state[asset]["volatility"],
                    regime=breakdown.get("Regime", "V10_INST"),
                    gate_name="Engine", status=status,
                    direction=direction if direction else "",
                    score_breakdown=json.dumps(breakdown)
                )
                self.memory.update_state({"rejected_signals_count": 1})
                return

            if status not in ("VALID", "HIGH", "EXTREME"):
                return

            # Correlation shield (softened)
            corr_ok, corr_reason = self._correlation_shield_ok(asset, direction)
            if not corr_ok:
                self.db.log_rejected(asset, price, score, corr_reason,
                                     self.asset_state[asset]["volatility"],
                                     breakdown.get("Regime", "V10_INST"),
                                     "CorrelationShield", json.dumps(breakdown),
                                     status="REJECTED", direction=direction)
                self.memory.update_state({"rejected_signals_count": 1})
                return

            # Portfolio heat check
            heat_ok, heat_pct = self.sizing_engine.portfolio_heat_ok(self.active_trades)
            if not heat_ok:
                self.db.log_rejected(asset, price, score, f"Portfolio heat {heat_pct}% > cap {Config.MAX_PORTFOLIO_HEAT_PCT}%",
                                     self.asset_state[asset]["volatility"],
                                     breakdown.get("Regime", "V10_INST"),
                                     "PortfolioHeat", json.dumps(breakdown),
                                     status="REJECTED", direction=direction)
                self.memory.update_state({"rejected_signals_count": 1})
                return

            if not self._signal_limit_ok():
                self.db.log_rejected(asset, price, score, "Global daily limit reached",
                                     self.asset_state[asset]["volatility"],
                                     breakdown.get("Regime", "V10_INST"),
                                     "DailyLimit", json.dumps(breakdown),
                                     status="REJECTED", direction=direction)
                self.memory.update_state({"rejected_signals_count": 1})
                return

            if not self._asset_cooldown_ok(asset):
                self.db.log_rejected(asset, price, score, f"Asset cooldown ({Config.ASSET_COOLDOWN_HOURS}h)",
                                     self.asset_state[asset]["volatility"],
                                     breakdown.get("Regime", "V10_INST"),
                                     "Cooldown", json.dumps(breakdown),
                                     status="REJECTED", direction=direction)
                self.memory.update_state({"re

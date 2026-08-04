# =====================================================================
# app.py – AlphaBot v7.0 Core (Ultra-Low-Latency Production Architecture)
# =====================================================================
import math
from typing import List, Dict, Optional, Tuple, Any, Deque
import os
import sys
import time
import json
import logging
import threading
import queue
import requests
import sqlite3
import gc
import re
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import deque
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import websocket

# ---- Optional Modules ----
try:
    from modules.institutional_analyzer import InstitutionalAnalyzer
except Exception:
    InstitutionalAnalyzer = None

try:
    from modules.oi_fetcher import OIFetcher
except Exception:
    OIFetcher = None

try:
    from modules.websocket_listener import AbsorptionWebSocket
except Exception:
    AbsorptionWebSocket = None

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

try:
    import pymongo
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AI-Orchestrator-v7.0")

# =====================================================================
# CONFIGURATION
# =====================================================================
class Config:
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    DISPLAY_NAMES = {"BTCUSDT": "BTC/USDT", "ETHUSDT": "ETH/USDT", "SOLUSDT": "SOL/USDT"}

    MONGO_URI = os.getenv("MONGO_URI", "")
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "crypto_bot_v5")
    RENDER_URL = os.getenv("RENDER_URL", "https://alphabot-76tj.onrender.com")
    DB_PATH = "trades_v6.db"
    MAX_CANDLES = 500
    BINANCE_FUTURES_WS_URL = "wss://fstream.binance.com/ws"

    IST = pytz.timezone('Asia/Kolkata')
    SESSION_WINDOWS = [("ALWAYS", 0, 0, 23, 59)]
    DEAD_ZONES = []

    MIN_SQS = 65
    PENDING_VERIFICATION_CANDLES = 2
    VOLUME_DECAY_THRESHOLD = 0.6
    SIGNAL_COOLDOWN = 1200
    MAX_SIGNALS_PER_DAY = 8
    MAX_HOLD_TIME = 14400
    TIME_DECAY_SECONDS = 1500
    TIME_DECAY_THRESHOLD_PCT = 0.002
    HEALTH_EMERGENCY_THRESHOLD = 55
    CONFIDENCE_UPDATE_INTERVAL = 300
    ADMIN_SECRET = os.getenv("ADMIN_SECRET", "AlphaSecret123")

# =====================================================================
# 1. DATABASE LAYERS (SQLite + MongoDB Async)
# =====================================================================
class MongoDatabase:
    def __init__(self):
        if not HAS_PYMONGO or not Config.MONGO_URI:
            self.client = None; self.db = None; return
        try:
            from pymongo import MongoClient, ASCENDING, DESCENDING
            self.client = MongoClient(Config.MONGO_URI, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[Config.MONGO_DB_NAME]
            self._create_indexes()
            logger.info("MongoDB connected")
        except Exception as e:
            logger.warning(f"MongoDB connection failed: {e}")
            self.client = None; self.db = None

    def _create_indexes(self):
        if self.db is None: return
        try:
            self.db.candles.create_index([("asset", 1), ("timeframe", 1), ("timestamp", 1)], unique=True)
            self.db.trades.create_index([("asset", 1), ("timestamp", -1)])
        except Exception: pass

    def save_candle(self, asset, tf, candle):
        if self.db is None: return
        try:
            doc = {**candle, "asset": asset, "timeframe": tf}
            self.db.candles.update_one({"asset": asset, "timeframe": tf, "timestamp": candle["timestamp"]}, {"$set": doc}, upsert=True)
        except Exception: pass

    def load_candles(self, asset, tf, limit=500):
        if self.db is None: return []
        try:
            return list(self.db.candles.find({"asset": asset, "timeframe": tf}).sort("timestamp", 1).limit(limit))
        except Exception: return []

    def save_trade_backup(self, trade_data):
        if self.db is None: return
        try:
            self.db.trades.update_one({"id": trade_data["id"]}, {"$set": trade_data}, upsert=True)
        except Exception: pass

    def save_rejected_backup(self, rejected_data):
        if self.db is None: return
        try:
            self.db.rejected.insert_one(rejected_data)
        except Exception: pass

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
            try:
                cur.execute("ALTER TABLE trades ADD COLUMN signal_type TEXT DEFAULT 'STANDARD'")
                cur.execute("ALTER TABLE trades ADD COLUMN signal_token TEXT")
            except sqlite3.OperationalError:
                pass
            self.conn.commit()
        finally:
            cur.close()

    def log_trade(self, asset, direction, entry, sl, tp, score, confidence, patterns, logic,
                  volatility, regime, htf_trend, news_score, session, sqs_score, pattern_name,
                  dynamic_min_sqs, signal_type="STANDARD", signal_token=None):
        cur = self.conn.cursor()
        try:
            cur.execute('''INSERT INTO trades 
                (asset, direction, entry, stop_loss, take_profit, score, confidence, patterns, logic,
                 timestamp, volatility, market_regime, htf_trend, news_score, entry_time, status,
                 session, sqs_score, pattern_name, regime, dynamic_min_sqs, signal_type, signal_token)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (asset, direction, entry, sl, tp, score, confidence, json.dumps(patterns), logic,
                 int(time.time()), volatility, regime, htf_trend, news_score, int(time.time()), 'open',
                 session, sqs_score, pattern_name, regime, dynamic_min_sqs, signal_type, signal_token))
            self.conn.commit()
            return cur.lastrowid
        finally:
            cur.close()

    def log_rejected(self, asset, price, score, reason, volatility, regime, gate_name="", dynamic_regime=""):
        cur = self.conn.cursor()
        try:
            cur.execute('''INSERT INTO rejected_signals (asset, price, score, reason, timestamp, volatility, market_regime, gate_name, regime)
                VALUES (?,?,?,?,?,?,?,?,?)''',
                (asset, price, score, reason, int(time.time()), volatility, regime, gate_name, dynamic_regime))
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
            if not rows: return 0.5
            wins = sum(1 for r in rows if r[0] > 0)
            return wins / len(rows)
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
                "total_trades": total, "winning_trades": wins, "losing_trades": total - wins,
                "win_rate": win_rate, "profit_factor": profit_factor,
                "total_pnl": total_pnl, "avg_pnl": total_pnl / total if total else 0.0
            }
        finally:
            cur.close()

# ==============================================================================
# 2. PERSISTENT MEMORY ENGINE (Optimized for Persistent Signal Tracking)
# ==============================================================================
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
            "total_run_seconds": 0,
            "restart_count": 0,
            "total_signals_generated": 0,
            "accepted_signals_count": 0,  # ✅ ऑल-टाइम पास सिग्नल (Persistent)
            "rejected_signals_count": 0,  # ✅ ऑल-टाइम रिजेक्टेड सिग्नल (Persistent)
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
            inc_fields = {k: v for k, v in updates.items() if isinstance(v, (int, float))}
            set_fields = {k: v for k, v in updates.items() if not isinstance(v, (int, float))}
            
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
        except Exception:
            pass

# =====================================================================
# 3. DYNAMIC SCORE GOVERNOR
# =====================================================================
class DynamicScoreGovernor:
    def __init__(self, memory_engine, lower_floor=58, upper_ceiling=75):
        self.memory = memory_engine
        self.lower_floor = lower_floor
        self.upper_ceiling = upper_ceiling
        self.current_base = Config.MIN_SQS
        self.adjustment_step = 3
        self.last_adjustment_time = 0
        self.cooldown = 3600

    def get_current_sqs_base(self):
        state = self.memory.get_or_create_state()
        total_signals = state.get("total_signals_generated", 0)
        if total_signals >= 140 and total_signals % 140 == 0:
            if time.time() - self.last_adjustment_time > self.cooldown:
                self._apply_auto_recovery(state)
        return self.current_base

    def _apply_auto_recovery(self, state):
        total = state.get("total_trades_closed", 0)
        wins = state.get("total_wins", 0)
        if total == 0:
            return
        win_rate = wins / total
        if win_rate < 0.5 and self.current_base < self.upper_ceiling:
            self.current_base = min(self.current_base + self.adjustment_step, self.upper_ceiling)
            self.last_adjustment_time = time.time()
        elif win_rate > 0.65 and self.current_base > self.lower_floor:
            self.current_base = max(self.current_base - self.adjustment_step, self.lower_floor)
            self.last_adjustment_time = time.time()

# =====================================================================
# 4. TOKEN MANAGER
# =====================================================================
class TokenManager:
    def __init__(self):
        self.counters = {"SNP": 0, "SCL": 0, "BOT": 0, "REJ": 0}
        self.lock = threading.Lock()

    def generate(self, prefix, asset, gate=None):
        with self.lock:
            self.counters[prefix] += 1
            counter = self.counters[prefix]
        if prefix == "REJ" and gate:
            return f"{prefix}-{gate}-{asset}-{counter:04d}"
        return f"{prefix}-{asset}-{counter:04d}"

# =====================================================================
# 5. THINKING MODEL (140-signal self-reflective)
# =====================================================================
class ThinkingOptimizationModel:
    def __init__(self, orchestrator):
        self.orch = orchestrator
        self.last_run = 0

    def trigger(self, total_signals):
        if total_signals % 30 != 0:
            return
        if time.time() - self.last_run < 300:
            return
        self.last_run = time.time()
        self._run_analysis()

    def _run_analysis(self):
        logger.info("🧠 30-signal Thinking Model running...")
        try:
            cur = self.orch.db.conn.cursor()
            cur.execute("""
                SELECT pattern_name, regime, sqs_score, pnl
                FROM trades
                WHERE status='closed' AND pnl IS NOT NULL
                ORDER BY id DESC LIMIT 30
            """)
            rows = cur.fetchall()
            cur.close()
            if len(rows) < 30:
                return
            pattern_stats = {}
            regime_stats = {}
            sqs_bands = {"55-65": 0, "66-75": 0, "76-85": 0, "86-100": 0}
            total_wins = 0
            for pattern, regime, sqs, pnl in rows:
                is_win = 1 if pnl > 0 else 0
                total_wins += is_win
                pattern_stats.setdefault(pattern or "unknown", {"total": 0, "wins": 0})
                pattern_stats[pattern]["total"] += 1
                pattern_stats[pattern]["wins"] += is_win
                regime_stats.setdefault(regime or "unknown", {"total": 0, "wins": 0})
                regime_stats[regime]["total"] += 1
                regime_stats[regime]["wins"] += is_win
                band = "55-65" if sqs <= 65 else "66-75" if sqs <= 75 else "76-85" if sqs <= 85 else "86-100"
                sqs_bands[band] += is_win
            win_rate_30 = total_wins / len(rows)
            worst_pattern = min(pattern_stats.items(), key=lambda x: x[1]["wins"]/x[1]["total"] if x[1]["total"]>=5 else 1)[0] if pattern_stats else "unknown"
            msg = f"🧠 30-Signal Audit:\n━━━━━━━━━━━━━━━━━━━━\nWin Rate: {win_rate_30:.2%}\nWorst Pattern: {worst_pattern}\nSQS Bands: {sqs_bands}"
            self.orch.telegram.send_message(msg)
            gc.collect()
        except Exception as e:
            logger.error(f"Thinking model error: {e}")

# =====================================================================
# 6. INDICATOR CACHE ENGINE (Shared, Thread-safe)
# =====================================================================
class IndicatorCache:
    def __init__(self, topology):
        self.topology = topology
        self.cache = {}  # key: (asset, tf) -> dict of indicators
        self.lock = threading.Lock()
        self.cache_ttl = 60  # seconds (or per candle close)

    def get(self, asset, tf, price=None, volume=None, force_refresh=False):
        key = (asset, tf)
        now = time.time()
        with self.lock:
            if key in self.cache and not force_refresh:
                if now - self.cache[key]['timestamp'] < self.cache_ttl:
                    return self.cache[key]['data']
            # Compute indicators
            data = self._compute(asset, tf, price, volume)
            self.cache[key] = {'data': data, 'timestamp': now}
            return data

    def _compute(self, asset, tf, price, volume):
        candles = self.topology.candles[tf][asset]
        completed = [c for c in candles if c.get("complete", False)]
        data = {}
        # Basic indicators (EMAs)
        if len(completed) >= 20:
            closes = [c['close'] for c in completed]
            data['ema_9'] = self.topology._ema(closes, 9)[-1] if len(closes)>=9 else None
            data['ema_20'] = self.topology._ema(closes, 20)[-1] if len(closes)>=20 else None
            data['ema_21'] = self.topology._ema(closes, 21)[-1] if len(closes)>=21 else None
            data['ema_50'] = self.topology._ema(closes, 50)[-1] if len(closes)>=50 else None
            data['ema_200'] = self.topology._ema(closes, 200)[-1] if len(closes)>=200 else None
        # ATR
        data['atr'] = self.topology.get_atr(asset, period=14, tf=tf)
        # ADX
        data['adx'] = self.topology.get_adx(asset, tf, period=14)
        # RSI (last 14 candles)
        if len(completed) >= 14:
            closes = [c['close'] for c in completed[-14:]]
            data['rsi'] = self.topology._calc_rsi(closes)
        # Support/Resistance from topology
        sr = self.topology.support_resistance[asset]
        data['support'] = sr.get('support', [])
        data['resistance'] = sr.get('resistance', [])
        # Structure
        data['bos'] = self.topology.bos[asset]['direction']
        data['choch'] = self.topology.choch[asset]
        # Order block, FVG
        data['fvg'] = self.topology.detect_fvg(asset)
        data['order_block'] = self.topology.detect_order_block(asset)
        # Volume MA
        data['volume_ma'] = self.topology.volume_ma[asset]
        # Current price and volume
        data['price'] = price
        data['volume'] = volume
        return data

# =====================================================================
# 7. MARKET CYCLE STATE MACHINE (7 stages)
# =====================================================================
class MarketCycleState:
    STATES = ['SEARCHING_BOTTOM', 'ACCUMULATION', 'BREAKOUT', 'TREND', 'DISTRIBUTION', 'REVERSAL', 'SEARCHING_TOP']

    def __init__(self, initial='SEARCHING_BOTTOM'):
        self.state = initial
        self.last_transition = time.time()
        self.lock = threading.Lock()

    def transition(self, new_state):
        with self.lock:
            if new_state in self.STATES and new_state != self.state:
                self.state = new_state
                self.last_transition = time.time()
                return True
        return False

    def get(self):
        with self.lock:
            return self.state

# =====================================================================
# 8. EVENT PIPELINE (Single-threaded sequential processing)
# =====================================================================
class EventPipeline:
    def __init__(self):
        self.handlers = []
        self.queue = queue.Queue()

    def register(self, handler):
        self.handlers.append(handler)

    def process(self, event):
        for handler in self.handlers:
            if not handler(event):
                break

# =====================================================================
# 9. ASYNC DATABASE PIPELINE
# =====================================================================
class DatabasePipeline:
    def __init__(self, db, mongo):
        self.db = db
        self.mongo = mongo
        self.queue = queue.Queue()
        self.running = True
        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()

    def _worker(self):
        while self.running:
            try:
                item = self.queue.get(timeout=1)
                if item is None:
                    continue
                self._process_item(item)
            except Exception:
                pass

    def _process_item(self, item):
        if item.get('type') == 'trade':
            if 'args' in item and item['args']:
                if self.db is not None:
                    self.db.log_trade(*item['args'])
            if self.mongo is not None and getattr(self.mongo, 'db', None) is not None and 'data' in item:
                self.mongo.save_trade_backup(item['data'])
        elif item.get('type') == 'reject':
            if 'args' in item and item['args']:
                if self.db is not None:
                    self.db.log_rejected(*item['args'])
            if self.mongo is not None and getattr(self.mongo, 'db', None) is not None and 'data' in item:
                self.mongo.save_rejected_backup(item['data'])

    def add_trade(self, *args, **kwargs):
        data = kwargs.get('data', args[-1] if args else {})
        self.queue.put({'type': 'trade', 'args': args, 'data': data})

    def add_reject(self, *args, **kwargs):
        data = kwargs.get('data', args[-1] if args else {})
        self.queue.put({'type': 'reject', 'args': args, 'data': data})

    def shutdown(self):
        self.running = False
        self.thread.join(timeout=1)

# =====================================================================
# 10. WEBSOCKET STREAMS (Spot + Futures)
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
        while self.running:
            try:
                streams = [f"{a.lower()}@kline_1m" for a in Config.ASSETS]
                ws = websocket.WebSocketApp(
                    f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}",
                    on_message=self._on_msg,
                    on_error=lambda x, y: None
                )
                ws.run_forever(ping_interval=15, ping_timeout=8)
            except Exception:
                time.sleep(5)

    def _on_msg(self, ws, msg):
        try:
            data = json.loads(msg)["data"]["k"]
            symbol = data["s"]
            if symbol in Config.ASSETS:
                self.on_price_update(symbol, float(data["c"]), float(data["v"]))
        except Exception:
            pass

class BinanceFuturesStream:
    def __init__(self, on_data=None):
        self.ws_url = Config.BINANCE_FUTURES_WS_URL
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

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.running = True
        self.thread = threading.Thread(target=self._ws_loop, daemon=True)
        self.thread.start()

    def _ws_loop(self):
        delay = 1
        while self.running:
            try:
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                self.ws.run_forever(ping_interval=15, ping_timeout=10)
            except Exception:
                self.reconnect_count += 1
                time.sleep(delay)
                delay = min(60, delay * 2)
            else:
                delay = 1

    def _on_open(self, ws):
        streams = []
        for s in self.symbols:
            streams.extend([f"{s}@openInterest", f"{s}@forceOrder", f"{s}@aggTrade"])
        ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))
        self.reconnect_count = 0
        self.last_ping = time.time()

    def _on_message(self, ws, message):
        self.last_ping = time.time()
        try:
            data = json.loads(message)
            if 'result' in data and 'id' in data:
                return
            if 'e' not in data:
                return
            e = data['e']
            if e == 'openInterest':
                symbol = data['s']
                oi = float(data['o'])
                with self.lock:
                    self.data['open_interest'][symbol] = oi
                    self.oi_history[symbol].append(oi)
                    if self.on_data:
                        self.on_data('open_interest', symbol, oi)
            elif e == 'forceOrder':
                order = data['o']
                symbol = order['s']
                with self.lock:
                    self.data['liquidations'].append({'symbol': symbol, 'side': order['S'], 'price': float(order['p']), 'qty': float(order['q']), 'time': time.time()})
                    if self.on_data:
                        self.on_data('liquidation', symbol, {'side': order['S'], 'price': float(order['p']), 'qty': float(order['q'])})
            elif e == 'aggTrade':
                symbol = data['s']
                price = float(data['p'])
                qty = float(data['q'])
                last_price = self.data['last_trade'].get(symbol, price)
                delta = qty if price >= last_price else -qty
                with self.lock:
                    self.data['cvd'][symbol] = self.data['cvd'].get(symbol, 0) + delta
                    self.data['last_trade'][symbol] = price
                    if self.on_data:
                        self.on_data('cvd', symbol, self.data['cvd'][symbol])
        except Exception:
            pass

    def _on_error(self, ws, error):
        logger.error(f"Futures WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
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

# =====================================================================
# 11. NEW: TOPOLOGY ENGINE (Modified to support IndicatorCache)
# =====================================================================
class CandleTopologyEngine:
    def __init__(self):
        self.candles = {tf: {asset: [] for asset in Config.ASSETS} for tf in [60, 300, 900, 3600, 14400]}
        self.pivots = {asset: {"high": [], "low": []} for asset in Config.ASSETS}
        self.bos = {asset: {"direction": ""} for asset in Config.ASSETS}
        self.choch = {asset: False for asset in Config.ASSETS}
        self.support_resistance = {asset: {"support": [], "resistance": []} for asset in Config.ASSETS}
        self.last_tick_time = {asset: 0 for asset in Config.ASSETS}
        self.candle_just_closed = {asset: False for asset in Config.ASSETS}
        self.history = {asset: deque(maxlen=200) for asset in Config.ASSETS}
        self.volume_ma = {asset: 0.0 for asset in Config.ASSETS}
        self._completed_cache = {}  # (asset, tf) -> list of completed candles

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
        for timeframe in [60, 300, 900, 3600, 14400]:
            self._build_candle(asset, price, volume, now, timeframe, self.candles[timeframe][asset])
        self._update_volume_ma(asset)
        self._update_pivots(asset, price)
        self._update_support_resistance(asset, price)
        self._detect_bos_choch(asset)
        self.last_tick_time[asset] = now
        # Invalidate completed cache
        self._completed_cache.clear()

    def _build_candle(self, asset, price, volume, ts, tf, storage):
        start = (ts // tf) * tf
        if not storage or storage[-1].get("timestamp") != start:
            if storage and not storage[-1].get("complete", False):
                storage[-1]["complete"] = True
            storage.append({"timestamp": start, "open": price, "high": price, "low": price, "close": price, "volume": volume, "complete": False})
            if len(storage) > Config.MAX_CANDLES:
                storage.pop(0)
        else:
            c = storage[-1]
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["close"] = price
            c["volume"] += volume

    def _update_volume_ma(self, asset):
        candles = self.candles[300][asset]
        completed = self.get_completed(asset, 300)
        if len(completed) >= 20:
            self.volume_ma[asset] = sum(c["volume"] for c in completed[-20:]) / 20
        else:
            self.volume_ma[asset] = 0.0

    def _update_pivots(self, asset, price):
        candles = self.candles[900][asset]
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
        h = self.pivots[asset]["high"]
        l = self.pivots[asset]["low"]
        if len(h) >= 2 and len(l) >= 2:
            if h[0] > h[1]:
                self.bos[asset]["direction"] = "UP"
            elif l[0] < l[1]:
                self.bos[asset]["direction"] = "DOWN"
            if len(h) >= 3 and len(l) >= 3:
                self.choch[asset] = (h[1] < h[2] and l[1] > l[2]) or (h[1] > h[2] and l[1] < l[2])

    def _update_support_resistance(self, asset, price):
        all_levels = self.pivots[asset]["high"] + self.pivots[asset]["low"]
        candles = self.candles[900][asset]
        recent = self.get_completed(asset, 900)[-10:]
        for c in recent:
            if c["high"] not in all_levels:
                all_levels.append(c["high"])
            if c["low"] not in all_levels:
                all_levels.append(c["low"])
        clusters = []
        for level in sorted(all_levels):
            if not clusters or abs(level - clusters[-1]) / level > 0.005:
                clusters.append(level)
        self.support_resistance[asset]["support"] = [l for l in clusters if l < price * 0.99]
        self.support_resistance[asset]["resistance"] = [r for r in clusters if r > price * 1.01]

    def get_completed(self, asset, tf):
        key = (asset, tf)
        if key in self._completed_cache:
            return self._completed_cache[key]
        completed = [c for c in self.candles[tf][asset] if c.get("complete", False)]
        self._completed_cache[key] = completed
        return completed

    def detect_candle_patterns(self, asset):
        candles = self.candles[300][asset]
        if len(candles) < 2:
            return {}
        last = candles[-1]
        if not last.get("complete", False):
            return {}
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
        candles = self.candles[tf][asset]
        complete = self.get_completed(asset, tf)
        if len(complete) < period:
            return 0.0
        tr_list = []
        for i in range(1, period+1):
            high, low = complete[i]["high"], complete[i]["low"]
            prev_close = complete[i-1]["close"]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_list.append(tr)
        return sum(tr_list) / period

    def detect_liquidity_sweep(self, asset, price):
        h = self.pivots[asset]["high"]
        l = self.pivots[asset]["low"]
        if h and price > max(h[-2:]):
            return "BUY_SWEEP"
        if l and price < min(l[-2:]):
            return "SELL_SWEEP"
        return ""

    def _ema(self, series, period):
        if len(series) < period:
            return []
        ema = [sum(series[:period]) / period]
        m = 2 / (period + 1)
        for i in range(period, len(series)):
            ema.append((series[i] - ema[-1]) * m + ema[-1])
        return ema

    def check_1m_rejection(self, asset, direction):
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
        chart_lines.extend(["├──────────────────────────────────────┤", "│ ●=Entry ▼=SL ★=TP  S=Support R=Res │", "└──────────────────────────────────────┘"])
        return "\n".join(chart_lines)

    def get_adx(self, asset, tf, period=14):
        candles = self.candles[tf][asset]
        complete = self.get_completed(asset, tf)
        if len(complete) < period:
            return 20
        tr_list, dm_plus, dm_minus = [], [], []
        for i in range(1, len(complete)):
            high, low = complete[i]["high"], complete[i]["low"]
            prev_high, prev_low = complete[i-1]["high"], complete[i-1]["low"]
            tr = max(high - low, abs(high - prev_high), abs(low - prev_low))
            tr_list.append(tr)
            up = high - prev_high
            down = prev_low - low
            dm_plus.append(max(up, 0) if up > down else 0)
            dm_minus.append(max(down, 0) if down > up else 0)
        if len(tr_list) < period:
            return 20
        atr = sum(tr_list[:period]) / period
        dm_plus_smooth = sum(dm_plus[:period]) / period
        dm_minus_smooth = sum(dm_minus[:period]) / period
        for i in range(period, len(tr_list)):
            atr = (atr * (period-1) + tr_list[i]) / period
            dm_plus_smooth = (dm_plus_smooth * (period-1) + dm_plus[i]) / period
            dm_minus_smooth = (dm_minus_smooth * (period-1) + dm_minus[i]) / period
        if atr == 0:
            return 20
        di_plus = (dm_plus_smooth / atr) * 100
        di_minus = (dm_minus_smooth / atr) * 100
        dx = (abs(di_plus - di_minus) / (di_plus + di_minus)) * 100 if (di_plus + di_minus) > 0 else 0
        return min(100, dx)

    def detect_fvg(self, asset):
        candles = self.candles[900][asset]
        complete = self.get_completed(asset, 900)
        if len(complete) < 3:
            return []
        fvgs = []
        for i in range(2, len(complete)-1):
            c1, c2, c3 = complete[i-2], complete[i-1], complete[i]
            if c1["close"] < c2["open"] and c2["close"] < c3["close"] and c1["high"] > c2["low"]:
                fvgs.append({"type": "bullish", "upper": c1["high"], "lower": c2["low"]})
            if c1["close"] > c2["open"] and c2["close"] > c3["close"] and c2["high"] > c1["low"]:
                fvgs.append({"type": "bearish", "upper": c2["high"], "lower": c1["low"]})
        return fvgs[-5:]

    def detect_order_block(self, asset):
        if not self.bos[asset]["direction"]:
            return {}
        candles = self.candles[900][asset]
        complete = self.get_completed(asset, 900)
        if len(complete) < 10:
            return {}
        atr = self.get_atr(asset)
        if atr == 0:
            return {}
        for i in range(len(complete)-1, -1, -1):
            c = complete[i]
            if (c["high"] - c["low"]) > 1.5 * atr:
                return {"type": "bullish" if c["close"] > c["open"] else "bearish", "high": c["high"], "low": c["low"]}
        return {}

    def _calc_rsi(self, closes, period=14):
        if len(closes) < period + 1:
            return 50
        start = len(closes) - period - 1
        gains = losses = 0
        for i in range(start + 1, len(closes)):
            diff = closes[i] - closes[i-1]
            if diff > 0:
                gains += diff
            else:
                losses -= diff
        avg_gain = gains / period
        avg_loss = losses / period
        if avg_loss == 0:
            return 100
        return 100 - (100 / (1 + avg_gain / avg_loss))

# =====================================================================
# 12. GATES (Use IndicatorCache)
# =====================================================================
class AdvanceRegimeDetector:
    def __init__(self, cache):
        self.cache = cache
        self.current_regime = {}
        self.params = {}

    def detect(self, asset, price, volume, htf_trend, tf_trend):
        ind_15m = self.cache.get(asset, 900, price, volume)
        ind_1h  = self.cache.get(asset, 3600, price, volume)

        if ind_1h is None:
            ind_1h = ind_15m

        adx_15m = ind_15m.get('adx', 20)
        adx_1h  = ind_1h.get('adx', 20)
        atr_15m = ind_15m.get('atr', price * 0.01)
        vol_ma  = ind_15m.get('volume_ma', 1.0)

        vol_ratio = volume / vol_ma if vol_ma > 0 else 1.0
        composite_adx = (0.6 * adx_15m) + (0.4 * adx_1h)
        atr_pct = (atr_15m / price) if price > 0 else 0.01

        if composite_adx > 32 and vol_ratio > 1.3:
            regime = "STRONG_TREND"
            min_sqs = 70
            atr_multiplier = 0.8
            pending_candles = 2
            order_flow_strict = True
            check_4h_ema = True
            use_micro_sweep = False

        elif composite_adx >= 18:
            regime = "GRADUAL_TREND"
            min_sqs = 60
            atr_multiplier = 1.5
            pending_candles = 1
            order_flow_strict = False
            check_4h_ema = True
            use_micro_sweep = True

        else:
            regime = "CHOP"
            min_sqs = 65
            atr_multiplier = 2.5
            pending_candles = 1
            order_flow_strict = False
            check_4h_ema = False
            use_micro_sweep = True

        dynamic_mtf_tol = round(atr_pct * atr_multiplier, 4)
        clamped_tolerance = max(0.01, min(dynamic_mtf_tol, 0.08))

        params = {
            "min_sqs": min_sqs,
            "use_micro_sweep": use_micro_sweep,
            "mtf_tolerance": clamped_tolerance,
            "volume_decay_threshold": 0.6,
            "pending_candles": pending_candles,
            "order_flow_strict": order_flow_strict,
            "check_4h_ema": check_4h_ema,
            "composite_adx": composite_adx,
            "regime": regime
        }

        self.current_regime[asset] = regime
        self.params[asset] = params

        return regime, params

class MarketRegimeFilter:
    def __init__(self, cache):
        self.cache = cache

    def check(self, asset, price, adx_threshold):
        ind = self.cache.get(asset, 900, price)
        adx_15 = ind['adx']
        adx_1h = self.cache.get(asset, 3600, price)['adx']
        if adx_15 < adx_threshold and adx_1h < adx_threshold:
            return False, f"Sideways/Chop (ADX 15={adx_15:.1f}, 1h={adx_1h:.1f})"
        # VSA fake breakout
        candles = self.cache.topology.candles[300][asset]
        completed = self.cache.topology.get_completed(asset, 300)
        if len(completed) >= 5:
            recent_high = max(c["high"] for c in completed[-5:])
            recent_low = min(c["low"] for c in completed[-5:])
            last = completed[-1]
            vol_ma = ind['volume_ma']
            if last["close"] > recent_high and last["volume"] < 1.2 * vol_ma:
                return False, "Fake Breakout (low volume)"
            if last["close"] < recent_low and last["volume"] < 1.2 * vol_ma:
                return False, "Fake Breakdown (low volume)"
        return True, "Pass"

class MTFConfluenceGate:
    """
    Advanced Score-Based MTF Gate with Confidence Output.
    Returns a confidence score (0-100) and detailed breakdown.
    """
    def __init__(self, cache):
        self.cache = cache

    def check(self, asset: str, direction: str, price: float, params: dict):
        """
        Evaluate MTF confluence and return confidence.
        - asset: trading pair
        - direction: 'BUY' or 'SELL'
        - price: current price (from tick)
        - params: regime params (read-only)
        Returns: (passed: bool, result: dict)
        """
        # 1. Load indicators from cache (real keys only)
        ind_1h = self.cache.get(asset, 3600, price, 0) or {}
        ind_15m = self.cache.get(asset, 900, price, 0) or {}
        ind_5m = self.cache.get(asset, 300, price, 0) or {}

        # Safety: if critical data missing, reject immediately
        if not ind_1h or not ind_15m:
            return False, {
                "confidence": 0,
                "log": "❌ REJECTED: Insufficient indicator data (1H/15M missing)",
                "passed": False,
                "reason": "No Data"
            }

        # 2. Scoring variables
        earned_score = 0
        max_possible_score = 0
        log_parts = []

        # -------- Condition 1: Trend (EMA + ADX) - 35 points --------
        ema_50 = ind_1h.get('ema_50')
        ema_200 = ind_1h.get('ema_200')
        adx = ind_1h.get('adx', 0)

        # EMA must exist
        if ema_50 is not None and ema_200 is not None:
            max_possible_score += 35
            # Direction check: price vs EMA200 and EMA50 vs EMA200
            bullish = (price > ema_200) and (ema_50 > ema_200)
            bearish = (price < ema_200) and (ema_50 < ema_200)

            if (direction == "BUY" and bullish) or (direction == "SELL" and bearish):
                # Bonus for strong trend (ADX > 25)
                if adx > 25:
                    earned_score += 35
                    log_parts.append(f"Trend: ✅ Strong (+35) | ADX={adx:.1f}")
                else:
                    earned_score += 25  # weak trend but direction correct
                    log_parts.append(f"Trend: ✅ Weak (+25) | ADX={adx:.1f}")
            else:
                # Direction mismatch – check if ADX is very low (chop)
                if adx < 20:
                    # In chop, direction less strict – give half points
                    earned_score += 15
                    log_parts.append(f"Trend: ⚠️ Chop (+15) | ADX={adx:.1f}")
                else:
                    log_parts.append(f"Trend: ❌ (+0) | ADX={adx:.1f} | EMA50={ema_50:.0f} EMA200={ema_200:.0f}")
        else:
            log_parts.append("Trend: ⚠️ No EMA data (skipped)")

        # -------- Condition 2: Support/Resistance Proximity - 30 points --------
        supports = ind_15m.get('support', [])
        resistances = ind_15m.get('resistance', [])

        # Only consider if lists are non-empty
        if supports or resistances:
            max_possible_score += 30
            nearest_level = None

            if direction == "BUY" and supports:
                # Find nearest support below price (most recent/valid)
                valid_supports = [s for s in supports if isinstance(s, (int, float)) and s < price]
                if valid_supports:
                    nearest_level = max(valid_supports)  # closest below
            elif direction == "SELL" and resistances:
                valid_resistances = [r for r in resistances if isinstance(r, (int, float)) and r > price]
                if valid_resistances:
                    nearest_level = min(valid_resistances)  # closest above

            if nearest_level:
                dist_pct = abs(price - nearest_level) / price
                if dist_pct <= 0.015:
                    earned_score += 30
                    log_parts.append(f"S/R: ✅ (+30) | Dist={dist_pct:.2%}")
                elif dist_pct <= 0.030:
                    earned_score += 20
                    log_parts.append(f"S/R: ✅ (+20) | Dist={dist_pct:.2%}")
                else:
                    earned_score += 5
                    log_parts.append(f"S/R: ⚠️ Far (+5) | Dist={dist_pct:.2%}")
            else:
                log_parts.append("S/R: ❌ (+0) | No valid level")
        else:
            log_parts.append("S/R: ⚠️ No data (skipped)")

        # -------- Condition 3: Order Block / FVG (with direction) - 35 points --------
        # Real keys: 'order_block' and 'fvg' – each should be a list of dicts with keys: price, type, direction
        obs = ind_15m.get('order_block', [])
        fvgs = ind_15m.get('fvg', [])

        # Collect valid levels with direction info
        level_candidates = []

        if isinstance(obs, list):
            for ob in obs:
                if isinstance(ob, dict):
                    ob_price = ob.get('price') or ob.get('level')
                    ob_type = ob.get('type', '').lower()      # 'bullish' or 'bearish'
                    if ob_price and isinstance(ob_price, (int, float)) and ob_price > 0:
                        level_candidates.append((ob_price, ob_type))
        if isinstance(fvgs, list):
            for fvg in fvgs:
                if isinstance(fvg, dict):
                    fvg_price = fvg.get('price') or fvg.get('level')
                    fvg_type = fvg.get('type', '').lower()
                    if fvg_price and isinstance(fvg_price, (int, float)) and fvg_price > 0:
                        level_candidates.append((fvg_price, fvg_type))

        if level_candidates:
            max_possible_score += 35
            # Find the nearest level and check its direction
            min_dist = float('inf')
            best_match = None

            for lvl_price, lvl_type in level_candidates:
                dist = abs(price - lvl_price) / price
                if dist < min_dist:
                    min_dist = dist
                    best_match = (lvl_price, lvl_type)

            if best_match and min_dist <= 0.020:  # within 2%
                lvl_price, lvl_type = best_match
                # Check direction alignment
                if (direction == "BUY" and lvl_type == 'bullish') or (direction == "SELL" and lvl_type == 'bearish'):
                    earned_score += 35
                    log_parts.append(f"OB/FVG: ✅ (+35) | {lvl_type} at {lvl_price:.2f} | Dist={min_dist:.2%}")
                else:
                    # opposite direction – still give some credit if very close
                    if min_dist <= 0.010:
                        earned_score += 20
                        log_parts.append(f"OB/FVG: ⚠️ (+20) | Direction mismatch but very close")
                    else:
                        log_parts.append(f"OB/FVG: ❌ (+0) | Direction mismatch")
            else:
                log_parts.append(f"OB/FVG: ❌ (+0) | No level within 2%")
        else:
            log_parts.append("OB/FVG: ⚠️ No data (skipped)")

        # 3. Final confidence calculation
        if max_possible_score == 0:
            # No indicators at all – reject
            return False, {
                "confidence": 0,
                "log": "❌ REJECTED: No indicator data available",
                "passed": False,
                "reason": "No Data"
            }

        confidence = (earned_score / max_possible_score) * 100

        # 4. Dynamic threshold from regime (read from params)
        regime = params.get('regime', 'GRADUAL_TREND')
        if regime == "STRONG_TREND":
            threshold = 75
        elif regime == "GRADUAL_TREND":
            threshold = 65
        else:  # CHOP
            threshold = 60

        passed = confidence >= threshold

        # 5. Build final log
        status = "✅ PASSED" if passed else "❌ REJECTED"
        full_log = (f"{status} | Confidence: {confidence:.1f}% "
                    f"(Earned: {earned_score}/{max_possible_score}) | "
                    f"Threshold: {threshold} | " + " | ".join(log_parts))

        return passed, {
            "confidence": round(confidence, 1),
            "earned": earned_score,
            "max_possible": max_possible_score,
            "threshold": threshold,
            "log": full_log,
            "passed": passed,
            "regime": regime
        }

class OrderFlowAnalyzer:
    def __init__(self, futures_stream):
        self.futures = futures_stream

    def check(self, asset, direction, price, strict=True):
        symbol = asset.lower()
        oi = self.futures.get_open_interest(symbol)
        oi_trend = self.futures.get_oi_trend(symbol)
        cvd = self.futures.get_cvd(symbol)
        if oi == 0:
            return True, "Bypassed (no OI data)"
        if strict:
            if direction == "BUY" and oi_trend <= 0:
                return False, "Open Interest not increasing"
            if direction == "SELL" and oi_trend >= 0:
                return False, "Open Interest increasing while selling"
            if cvd < 0 and direction == "BUY":
                return False, "CVD divergence (price up, CVD down)"
            if cvd > 0 and direction == "SELL":
                return False, "CVD divergence (price down, CVD up)"
        return True, "Pass"

class SessionTimer:
    def is_trading_time(self):
        return True, "ALWAYS", "00:00-23:59 IST"

# =====================================================================
# 13. SQS CALCULATOR & DYNAMIC SL/TP
# =====================================================================
class SQS_Calculator:
    def __init__(self, cache):
        self.cache = cache

    def calculate(self, asset, price, direction, session_ok, patterns, bos, choch,
                  liquidity_sweep, ob, fvgs, vol_ratio, htf_trend, use_micro_sweep=True):
        score = 0
        if bos and bos.get("direction"):
            score += 15
        if choch:
            score += 10
        if liquidity_sweep:
            score += 10
        if use_micro_sweep and self.cache.topology.check_1m_rejection(asset, direction):
            score += 10
        if ob and ob.get("type"):
            score += 15
        if vol_ratio > 1.5:
            score += 15
        elif vol_ratio > 1.2:
            score += 10
        if htf_trend == direction:
            score += 15
        if session_ok:
            score += 10
        return score

class DynamicStopLoss:
    def __init__(self, cache):
        self.cache = cache

    def calculate(self, asset, direction, entry, atr):
        sr = self.cache.get(asset, 300, entry)['support'] if direction=="BUY" else self.cache.get(asset, 300, entry)['resistance']
        nearest = None
        if sr:
            nearest = max(sr) if direction=="BUY" else min(sr)
        default_sl = entry + 1.5 * atr if direction == "SELL" else entry - 1.5 * atr
        if direction == "SELL":
            if nearest:
                sl = nearest + 0.5 * atr
                if sl - entry > 1.5 * atr:
                    sl = default_sl
            else:
                sl = default_sl
            sl = max(entry, min(sl, entry * 1.10))
        else:
            if nearest:
                sl = nearest - 0.5 * atr
                if entry - sl > 1.5 * atr:
                    sl = default_sl
            else:
                sl = default_sl
            sl = min(entry, max(sl, entry * 0.90))
        risk = abs(entry - sl)
        default_tp = entry - 2 * risk if direction == "SELL" else entry + 2 * risk
        if direction == "SELL":
            if nearest and (entry - nearest) <= 3 * risk:
                tp = nearest
            else:
                tp = default_tp
            tp = max(tp, entry - 3 * risk)
            tp = max(tp, entry * 0.70)
        else:
            if nearest and (nearest - entry) <= 3 * risk:
                tp = nearest
            else:
                tp = default_tp
            tp = min(tp, entry + 3 * risk)
            tp = min(tp, entry * 1.30)
        if direction == "SELL":
            if tp >= entry:
                tp = entry - 1.5 * risk
            if entry - tp < 1.5 * risk:
                tp = entry - 1.5 * risk
        else:
            if tp <= entry:
                tp = entry + 1.5 * risk
            if tp - entry < 1.5 * risk:
                tp = entry + 1.5 * risk
        return sl, tp

# =====================================================================
# 14. TELEGRAM PIPELINE (PriorityQueue)
# =====================================================================
class TelegramPipeline:
    def __init__(self):
        self.token = Config.TELEGRAM_BOT_TOKEN
        self.chat_id = Config.TELEGRAM_CHAT_ID
        self.queue = queue.PriorityQueue()
        threading.Thread(target=self._worker, daemon=True).start()

    def _worker(self):
        while True:
            try:
                priority, msg = self.queue.get()
                requests.post(f"https://api.telegram.org/bot{self.token}/sendMessage",
                              data={"chat_id": self.chat_id, "text": msg, "parse_mode": "HTML"}, timeout=10)
            except Exception:
                pass

    def send(self, msg, priority=5):
        self.queue.put((priority, msg))

    def fire_signal(self, asset, direction, price, sl, tp, chart, logic, news,
                    score, patterns, trade_id, session, rr, regime, signal_type="STANDARD", signal_token=None):
        priority = 1 if signal_type=="SNIPER" else 2 if signal_type in ("BOTTLING", "MICRO") else 3 if signal_type=="STANDARD" else 5
        if signal_type == "SNIPER":
            header = "🎯 <b>AI SNIPER REVERSAL</b>"
            engine_label = "🎯 [ENGINE A: SNIPER]"
        elif signal_type in ("BOTTLING", "MICRO"):
            header = "🏦 <b>INSTITUTIONAL ENTRY</b>"
            engine_label = "🏦 [ENGINE C: BOTTLING]"
        else:
            header = "🔥 <b>AI SCALP SIGNAL</b>" if direction == "BUY" else "❄️ <b>AI SCALP SIGNAL</b>"
            engine_label = "⚡ [ENGINE B: SCALPER]"
        token_line = f"🆔 Token: {signal_token} (DB ID: #{trade_id})" if signal_token else f"🆔 DB ID: #{trade_id}"
        msg = (f"{header}\n━━━━━━━━━━━━━━━━━━━━\n"
               f"📊 {Config.DISPLAY_NAMES.get(asset, asset)} | {token_line}\n"
               f"⏰ {session} | ⚡ {score['confidence']} ({score['total_score']:.0f}%)\n"
               f"🎯 R:R {rr:.2f}\n"
               f"💰 Entry: {price:.2f}  🛑 SL: {sl:.2f}  🎯 TP: {tp:.2f}\n"
               f"📈 Regime: {regime}  | Type: {signal_type}\n"
               f"📌 Engine: {engine_label}\n"
               f"\n📊 CHART:\n{chart}\n"
               f"🧠 Logic: {logic}\n📰 News: {news}\n"
               f"📊 Layers Passed: {score['num_passed']}/11\n"
               f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        self.send(msg, priority)

    def fire_news_alert(self, title, sentiment, fg):
        self.send(f"📰 {title}\n🧠 Sentiment: {sentiment:.0f} | Fear/Greed: {fg}", priority=4)

# =====================================================================
# 15. PENDING VERIFICATION QUEUE (Optimized)
# =====================================================================
class PendingVerificationQueue:
    def __init__(self, topology):
        self.topology = topology
        self.pending = {}

    def add_signal(self, signal_data):
        asset = signal_data['asset']
        candles = self.topology.candles[300][asset]
        completed = self.topology.get_completed(asset, 300)
        if len(completed) < 2:
            return False
        signal_data['initial_volume'] = completed[-1]["volume"]
        signal_data['latest_volume'] = completed[-1]["volume"]
        signal_data['candle_count'] = 0
        signal_data['start_price'] = signal_data['entry']
        signal_data['rejected'] = False
        key = f"{asset}_{signal_data['direction']}_{int(time.time())}"
        self.pending[key] = signal_data
        return key

    def check_pending(self, asset):
        to_remove = []
        for key, data in self.pending.items():
            if data['asset'] != asset:
                continue
            candles = self.topology.candles[300][asset]
            completed = self.topology.get_completed(asset, 300)
            if len(completed) < 2:
                continue
            limit = data.get('pending_candles', Config.PENDING_VERIFICATION_CANDLES)
            vol_decay = data.get('volume_decay_threshold', Config.VOLUME_DECAY_THRESHOLD)
            new_candles = completed[-limit:] if len(completed) >= limit else completed
            if len(new_candles) > data['candle_count']:
                for c in new_candles[data['candle_count']:]:
                    data['latest_volume'] = c["volume"]
                    data['candle_count'] += 1
                if data['candle_count'] >= 2:
                    if data['latest_volume'] < data['initial_volume'] * (1 - vol_decay):
                        data['rejected'] = True
                        to_remove.append(key)
                        continue
                first_close = completed[-limit]['close']
                if data['direction'] == 'BUY' and first_close < data['start_price'] * 0.995:
                    data['rejected'] = True
                    to_remove.append(key)
                elif data['direction'] == 'SELL' and first_close > data['start_price'] * 1.005:
                    data['rejected'] = True
                    to_remove.append(key)
            if data['candle_count'] >= limit:
                to_remove.append(key)
        return to_remove

    def get_verified_signals(self):
        ready = []
        to_remove = []
        for key, data in self.pending.items():
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
# 16. SNIPER ENGINE (Uses IndicatorCache)
# =====================================================================
class RallyExhaustionFilter:
    def __init__(self, cache):
        self.cache = cache

    def evaluate(self, asset, price):
        ind = self.cache.get(asset, 14400, price)
        ema20 = ind.get('ema_20')
        if ema20 is None:
            return None, "No EMA"
        atr = self.cache.get(asset, 3600, price)['atr']
        if atr == 0:
            return None, "No ATR"
        above = price - ema20
        below = ema20 - price
        if above > 2.5*atr or below > 2.5*atr:
            ind15 = self.cache.get(asset, 900, price)
            candles = self.cache.topology.candles[900][asset]
            complete = self.cache.topology.get_completed(asset, 900)
            if len(complete) < 20:
                return None, "No 15m data"
            last = complete[-1]
            body = abs(last["close"] - last["open"])
            range_ = last["high"] - last["low"]
            if range_ == 0:
                return None, "No range"
            vol_ma = ind15['volume_ma']
            vol_spike = last["volume"] > 1.5 * vol_ma
            upper_wick = last["high"] - max(last["open"], last["close"])
            lower_wick = min(last["open"], last["close"]) - last["low"]
            if above > 2.5*atr and vol_spike and upper_wick/range_ > 0.5:
                return {"direction": "SELL", "score": 85, "reason": "Overbought+Wick"}, None
            elif below > 2.5*atr and vol_spike and lower_wick/range_ > 0.5:
                return {"direction": "BUY", "score": 85, "reason": "Oversold+Wick"}, None
        return None, "No trigger"

# =====================================================================
# 17. ADVANCED SIGNAL ENGINE
# =====================================================================
class AdvancedSignalEngine:
    def __init__(self, cache):
        self.cache = cache

    def evaluate(self, asset, price, direction):
        patterns = self.cache.topology.detect_candle_patterns(asset)
        bos = self.cache.get(asset, 900, price)['bos']
        choch = self.cache.get(asset, 900, price)['choch']
        score = 0
        if patterns:
            if direction == "BUY" and "bullish_rej" in patterns:
                score += 10
            elif direction == "SELL" and "bearish_rej" in patterns:
                score += 10
        if bos == direction:
            score += 10
        if choch:
            score += 5
        return score, patterns, "", ""

# =====================================================================
# 18. HEALTH SERVER WITH SNAPSHOT CACHING
# =====================================================================
class HealthSnapshot:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.snapshot = {}
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()

    def _worker(self):
        while True:
            time.sleep(5)
            self._update()

    def _update(self):
        with self.lock:
            # Compute snapshot
            cpu = psutil.cpu_percent() if HAS_PSUTIL else 0
            mem = psutil.virtual_memory().percent if HAS_PSUTIL else 0
            active_trades = []
            with self.orchestrator.trade_lock:
                for tid, trade in self.orchestrator.active_trades.items():
                    current_price = self.orchestrator.topology.history[trade['asset']][-1]['price'] if self.orchestrator.topology.history[trade['asset']] else trade['entry']
                    pnl = round(current_price - trade['entry'] if trade['direction'] == 'BUY' else trade['entry'] - current_price, 2)
                    active_trades.append({"id": tid, "asset": trade['asset'], "direction": trade['direction'], "entry": trade['entry'], "pnl": pnl})
            perf = self.orchestrator.db.get_performance_metrics()
            mem_state = self.orchestrator.memory.get_or_create_state()
            total_uptime = mem_state.get("total_run_seconds", 0)
            uptime_str = f"{total_uptime//86400}d {(total_uptime%86400)//3600}h {(total_uptime%3600)//60}m"
            rejected = self.orchestrator.rejected
            accepted = self.orchestrator.accepted
            self.snapshot = {
                "status": "online",
                "uptime": uptime_str,
                "cpu": cpu,
                "memory": mem,
                "active_trades_count": len(active_trades),
                "active_trades": active_trades,
                "accepted_signals": accepted,
                "rejected_signals": rejected,
                "performance": perf,
                "dynamic_sqs_base": self.orchestrator.score_governor.current_base,
                "total_signals_all_time": mem_state.get("total_signals_generated", 0)
            }

    def get(self):
        with self.lock:
            return self.snapshot

def start_health_server(orchestrator):
    port = int(os.environ.get("PORT", 10000))
    snapshot = HealthSnapshot(orchestrator)

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            path_parts = self.path.split('?')
            path = path_parts[0]
            params = {}
            if len(path_parts) > 1:
                for pair in path_parts[1].split('&'):
                    if '=' in pair:
                        k, v = pair.split('=', 1)
                        params[k] = v

            if path.startswith('/admin/'):
                if params.get('key') != Config.ADMIN_SECRET:
                    self.send_response(403); self.end_headers(); self.wfile.write(json.dumps({"error": "Unauthorized"}).encode()); return
                if path == '/admin/close_trade':
                    trade_id = params.get('id')
                    token = params.get('token')
                    if not trade_id and not token:
                        self.send_response(400); self.end_headers(); return
                    with orchestrator.trade_lock:
                        for tid, trade in list(orchestrator.active_trades.items()):
                            if (trade_id and str(tid)==str(trade_id)) or (token and trade.get('signal_token')==token):
                                price = orchestrator.topology.history[trade['asset']][-1]['price'] if orchestrator.topology.history[trade['asset']] else trade['entry']
                                pnl = price - trade['entry'] if trade['direction']=='BUY' else trade['entry'] - price
                                orchestrator._close_trade(tid, price, pnl, "Admin close")
                                self.send_response(200); self.end_headers(); return
                        self.send_response(404); self.end_headers(); return
                elif path == '/admin/clear_asset':
                    symbol = params.get('symbol')
                    if symbol not in Config.ASSETS:
                        self.send_response(400); self.end_headers(); return
                    with orchestrator.trade_lock:
                        for tid, trade in list(orchestrator.active_trades.items()):
                            if trade['asset'] == symbol:
                                price = orchestrator.topology.history[symbol][-1]['price'] if orchestrator.topology.history[symbol] else trade['entry']
                                pnl = price - trade['entry'] if trade['direction']=='BUY' else trade['entry'] - price
                                orchestrator._close_trade(tid, price, pnl, "Admin clear asset")
                    self.send_response(200); self.end_headers(); return
                elif path == '/admin/clear_all':
                    with orchestrator.trade_lock:
                        for tid, trade in list(orchestrator.active_trades.items()):
                            price = orchestrator.topology.history[trade['asset']][-1]['price'] if orchestrator.topology.history[trade['asset']] else trade['entry']
                            pnl = price - trade['entry'] if trade['direction']=='BUY' else trade['entry'] - price
                            orchestrator._close_trade(tid, price, pnl, "Admin clear all")
                    self.send_response(200); self.end_headers(); return

            if path == '/rejections':
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                try:
                    cur = orchestrator.db.conn.cursor()
                    cur.execute("SELECT datetime(timestamp, 'unixepoch'), asset, price, reason, gate_name FROM rejected_signals ORDER BY timestamp DESC LIMIT 50")
                    data = [{"time": r[0], "asset": r[1], "price": r[2], "reason": r[3], "gate": r[4]} for r in cur.fetchall()]
                    cur.close()
                    self.wfile.write(json.dumps(data, indent=2).encode())
                except Exception:
                    self.wfile.write(json.dumps({"error": "DB error"}).encode())
                return

            # Health snapshot
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(snapshot.get(), indent=2).encode())

    httpd = HTTPServer(("0.0.0.0", port), H)
    logger.info(f"Health server on port {port}")
    httpd.serve_forever()

# =====================================================================
# 19. LIFECYCLE CONTROLLER
# =====================================================================
class ActiveTradeLifecycle:
    def __init__(self, orchestrator):
        self.orch = orchestrator

    def monitor_lifecycle(self):
        while True:
            time.sleep(60)
            with self.orch.trade_lock:
                to_remove = []
                for tid, trade in list(self.orch.active_trades.items()):
                    asset = trade['asset']
                    current_price = self.orch.topology.history[asset][-1]['price'] if self.orch.topology.history[asset] else trade['entry']
                    atr = self.orch.topology.get_atr(asset)
                    htf_trend = self.orch.asset_state[asset]["htf_trend"]
                    duration = time.time() - trade.get('entry_time', time.time())
                    if duration > Config.MAX_HOLD_TIME:
                        pnl = current_price - trade['entry'] if trade['direction'] == 'BUY' else trade['entry'] - current_price
                        self.orch._close_trade(tid, current_price, pnl, "MaxHold")
                        to_remove.append(tid)
                        continue
                    if duration > Config.TIME_DECAY_SECONDS and abs(current_price - trade['entry']) / trade['entry'] < Config.TIME_DECAY_THRESHOLD_PCT:
                        self.orch._close_trade(tid, current_price, 0.0, "TimeDecay")
                        to_remove.append(tid)
                        continue
                    # BE lock at 50% TP
                    if not trade.get('breakeven_locked', False):
                        target = abs(trade['tp'] - trade['entry'])
                        half = trade['entry'] + 0.5*target if trade['direction']=='BUY' else trade['entry'] - 0.5*target
                        if (trade['direction']=='BUY' and current_price >= half) or (trade['direction']=='SELL' and current_price <= half):
                            if self.orch.topology.check_1m_rejection(asset, trade['direction']):
                                trade['sl'] = trade['entry']
                                trade['breakeven_locked'] = True
                    # Trailing at 70% TP
                    if not trade.get('trailing_activated', False):
                        target = abs(trade['tp'] - trade['entry'])
                        trigger = trade['entry'] + 0.7*target if trade['direction']=='BUY' else trade['entry'] - 0.7*target
                        if (trade['direction']=='BUY' and current_price >= trigger) or (trade['direction']=='SELL' and current_price <= trigger):
                            new_sl = trade['entry'] + 0.3*target if trade['direction']=='BUY' else trade['entry'] - 0.3*target
                            if (trade['direction']=='BUY' and new_sl > trade['sl']) or (trade['direction']=='SELL' and new_sl < trade['sl']):
                                trade['sl'] = new_sl
                                trade['trailing_activated'] = True
                    # SL/TP checks
                    if trade['direction'] == 'BUY':
                        if current_price <= trade['sl']:
                            self.orch._close_trade(tid, current_price, current_price - trade['entry'], "SL")
                            to_remove.append(tid)
                        elif current_price >= trade['tp']:
                            self.orch._close_trade(tid, current_price, current_price - trade['entry'], "TP")
                            to_remove.append(tid)
                    else:
                        if current_price >= trade['sl']:
                            self.orch._close_trade(tid, current_price, trade['entry'] - current_price, "SL")
                            to_remove.append(tid)
                        elif current_price <= trade['tp']:
                            self.orch._close_trade(tid, current_price, trade['entry'] - current_price, "TP")
                            to_remove.append(tid)
                for tid in to_remove:
                    if tid in self.orch.active_trades:
                        del self.orch.active_trades[tid]
                gc.collect()

# =====================================================================
# 20. INSTITUTIONAL BOTTLING ENGINE (Merged)
# =====================================================================
class InstitutionalBottlingEngine:
    def __init__(self, orchestrator):
        self.orch = orchestrator
        self.executor = ThreadPoolExecutor(max_workers=len(Config.ASSETS)*2)

    def start(self):
        threading.Thread(target=self._bottling_loop, daemon=True).start()

    def _bottling_loop(self):
        while True:
            try:
                futures = []
                for asset in Config.ASSETS:
                    futures.append(self.executor.submit(self._analyze_asset, asset))
                for f in futures:
                    f.result()
                time.sleep(1)
            except Exception as e:
                logger.error(f"Bottling loop: {e}")
                time.sleep(5)

    def _analyze_asset(self, asset):
        analyzer = self.orch.institutional_analyzers.get(asset)
        if not analyzer:
            return
        listener = self.orch.absorption_listeners.get(asset)
        if listener:
            state = listener.get_state()
            if state and time.time() - state.get("last_update",0) < 3:
                analyzer.update_absorption_state(state)
                analyzer.update_cvd_state(state)
        price = self.orch.topology.history[asset][-1]['price'] if self.orch.topology.history.get(asset) else 0
        if not price:
            return
        with self.orch.trade_lock:
            if any(t['asset']==asset for t in self.orch.active_trades.values()):
                return
        regime = self.orch.regime_detector.current_regime.get(asset, "CHOP")
        analyzer.set_regime(regime)
        _, score, _ = analyzer.analyze(price)
        state = self.orch.asset_states.get(asset, "SEARCHING_BOTTOM")
        # 7-stage state transitions (simplified for now)
        if state == "SEARCHING_BOTTOM" and score >= 60:
            self.orch._fire_high_reward_signal(asset, "BUY", price, {})
            self.orch._switch_asset_state(asset, "ACCUMULATION")
        elif state == "ACCUMULATION" and score >= 70:
            self.orch._switch_asset_state(asset, "BREAKOUT")
        elif state == "BREAKOUT" and score >= 80:
            self.orch._switch_asset_state(asset, "TREND")
        elif state == "TREND" and score < 50:
            self.orch._switch_asset_state(asset, "DISTRIBUTION")
        elif state == "DISTRIBUTION" and score < 40:
            self.orch._fire_high_reward_signal(asset, "SELL", price, {})
            self.orch._switch_asset_state(asset, "REVERSAL")
        elif state == "REVERSAL" and score < 30:
            self.orch._switch_asset_state(asset, "SEARCHING_TOP")
        elif state == "SEARCHING_TOP" and score >= 40:
            self.orch._switch_asset_state(asset, "SEARCHING_BOTTOM")

# =====================================================================
# 21. CORE ORCHESTRATOR (v7.0)
# =====================================================================
class AIOrchestrator:
    def __init__(self):
        self.topology = CandleTopologyEngine()
        self.cache = IndicatorCache(self.topology)

        self.news = CryptoNewsScanner()
        self.db = TradeDatabase()
        self.mongo = MongoDatabase()
        self.telegram = TelegramPipeline()

        self.futures_stream = BinanceFuturesStream()
        self.futures_stream.start()

        self.regime_detector = AdvanceRegimeDetector(self.cache)
        self.advanced_engine = AdvancedSignalEngine(self.cache)
        self.exhaust_filter = RallyExhaustionFilter(self.cache)

        self.market_regime = MarketRegimeFilter(self.cache)
        self.mtf_gate = MTFConfluenceGate(self.cache)
        self.orderflow = OrderFlowAnalyzer(self.futures_stream)
        self.session_timer = SessionTimer()
        self.sqs_calc = SQS_Calculator(self.cache)
        self.pending_queue = PendingVerificationQueue(self.topology)
        self.dynamic_sl = DynamicStopLoss(self.cache)

        self.active_trades = {}
        self.trade_lock = threading.Lock()
        self.price_queue = queue.Queue(maxsize=1000)
        self.start_time = time.time()
        self.last_signal_time = {a: 0 for a in Config.ASSETS}
        self.signal_timestamps = deque(maxlen=100)
        self.asset_state = {a: {"trend": "NEUTRAL", "htf_trend": "NEUTRAL", "volume_ratio": 1.0,
                                "rsi": 50, "adx": 20, "volatility": 0.01} for a in Config.ASSETS}
        self.accepted = 0
        self.rejected = 0
        self.stream = None

        # Persistent Memory & Governor
        self.memory = PersistentMemoryEngine(self.mongo.db)
        self._sync_initial_metadata()
        self.score_governor = DynamicScoreGovernor(self.memory)
        self.token_manager = TokenManager()
        self.thinking_model = ThinkingOptimizationModel(self)

        # Asset states (7-stage)
        self.asset_states = {a: "SEARCHING_BOTTOM" for a in Config.ASSETS}
        self._load_asset_states()

        # Institutional components
        self.oi_fetchers = {}
        self.institutional_analyzers = {}
        self.absorption_listeners = {}
        self._init_institutional_components()

        # Bottling engine
        self.bottling_engine = InstitutionalBottlingEngine(self)
        self.bottling_engine.start()

        # Database pipeline
        self.db_pipeline = DatabasePipeline(self.db, self.mongo)

        # Lifecycle
        self.lifecycle = ActiveTradeLifecycle(self)
        threading.Thread(target=self.lifecycle.monitor_lifecycle, daemon=True).start()
        threading.Thread(target=self._process_queue, daemon=True).start()
        threading.Thread(target=self._ping_self_loop, daemon=True).start()
        threading.Thread(target=self._memory_sync_loop, daemon=True).start()

        logger.info("🚀 AlphaBot v7.0 Core (Ultra-Low-Latency) started")

    def _sync_initial_metadata(self):
        state = self.memory.get_or_create_state()
        now = int(time.time())
        restart_count = state.get("restart_count", 0) + 1
        self.memory.update_state({"restart_count": 1, "last_restart_at": now})
        self.start_time = now
        self.restart_count = restart_count

    def _memory_sync_loop(self):
        last_sync = 0
        while True:
            time.sleep(60)
            now = int(time.time())
            if now - last_sync >= 300:
                self.memory.update_state({"total_run_seconds": int(time.time() - self.start_time), "last_update": now})
                last_sync = now

    def _load_asset_states(self):
        if self.mongo.db is not None:
            try:
                doc = self.mongo.db.bot_state.find_one({"_id": "asset_states"})
                if doc and "states" in doc:
                    for asset in Config.ASSETS:
                        if asset in doc["states"]:
                            self.asset_states[asset] = doc["states"][asset]
            except Exception: pass

    def _persist_asset_states(self):
        if self.mongo.db is None:
            return
        try:
            self.mongo.db.bot_state.update_one(
                {"_id": "asset_states"},
                {"$set": {"states": self.asset_states, "updated_at": int(time.time())}},
                upsert=True
            )
        except Exception: pass

    def _switch_asset_state(self, asset, new_state):
        if self.asset_states[asset] != new_state:
            self.asset_states[asset] = new_state
            self._persist_asset_states()

    def _init_institutional_components(self):
        for asset in Config.ASSETS:
            if OIFetcher:
                self.oi_fetchers[asset] = OIFetcher(asset)
            if InstitutionalAnalyzer:
                try:
                    analyzer = InstitutionalAnalyzer(asset)
                    if self.oi_fetchers.get(asset):
                        analyzer.set_oi_fetcher(self.oi_fetchers[asset])
                    self.institutional_analyzers[asset] = analyzer
                except Exception: pass
            if AbsorptionWebSocket:
                try:
                    listener = AbsorptionWebSocket(asset)
                    listener.start()
                    self.absorption_listeners[asset] = listener
                except Exception: pass

    def _fire_high_reward_signal(self, asset, direction, price, details):
        atr = self.cache.get(asset, 3600, price)['atr'] or price*0.01
        sl, tp = self.dynamic_sl.calculate(asset, direction, price, atr)
        risk = abs(price-sl)
        rr = abs(tp-price)/risk if risk else 0
        token = self.token_manager.generate("BOT", asset)
        data = {
            'asset': asset, 'direction': direction, 'entry': price,
            'sl': sl, 'tp': tp, 'sqs': 80, 'session': "ALWAYS",
            'patterns': {}, 'logic': f"INSTITUTIONAL_REVERSAL (RR={rr:.1f})",
            'news': self.news.last_news.get('title', 'No news')[:100],
            'volatility': self.asset_state[asset]["volatility"],
            'regime': "BOTTLING", 'htf_trend': self.asset_state[asset]["htf_trend"],
            'news_score': 0, 'score': 0, 'confidence': 'VERY HIGH', 'num_passed': 11,
            'signal_type': 'BOTTLING', 'dynamic_min_sqs': 80,
            'signal_token': token, 'pattern_name': 'bottling'
        }
        self._send_final_signal(data)

    def _handle_price_tick(self, asset, price, volume):
        try:
            self.topology.process_tick(asset, price, volume)
            # Micro-entry check (fast path)
            self._check_intra_candle_signal(asset, price, volume)
            if not self.topology.candle_just_closed.get(asset, False):
                return
            # Pending
            if self.pending_queue.pending:
                self.pending_queue.check_pending(asset)
                for sig in self.pending_queue.get_verified_signals():
                    self._send_final_signal(sig)
            # Sniper
            sniper, _ = self.exhaust_filter.evaluate(asset, price)
            if sniper:
                direction = sniper["direction"]
                score = sniper["score"]
                atr = self.cache.get(asset, 3600, price)['atr'] or price*0.01
                sl, tp = self.dynamic_sl.calculate(asset, direction, price, atr)
                risk = abs(price - sl)
                rr = abs(tp-price)/risk if risk else 0
                if rr < 2.5:
                    tp = price - 2.5*risk if direction=="SELL" else price + 2.5*risk
                token = self.token_manager.generate("SNP", asset)
                data = {
                    'asset': asset, 'direction': direction, 'entry': price,
                    'sl': sl, 'tp': tp, 'sqs': score, 'session': "ALWAYS",
                    'patterns': {}, 'logic': f"SNIPER: {sniper['reason']}",
                    'news': self.news.last_news.get('title', 'No news')[:100],
                    'volatility': self.asset_state[asset]["volatility"],
                    'regime': "SNIPER", 'htf_trend': self.asset_state[asset]["htf_trend"],
                    'news_score': 0, 'score': 0, 'confidence': 'HIGH', 'num_passed': 11,
                    'signal_type': 'SNIPER', 'dynamic_min_sqs': score,
                    'signal_token': token
                }
                self._send_final_signal(data)
                self.memory.update_state({"total_signals_generated": 1})
                self.thinking_model.trigger(self.memory.get_or_create_state().get("total_signals_generated",0))
                return
            # Scalper
            self._update_indicators(asset, price)
            htf_trend = self.asset_state[asset]["htf_trend"]
            tf_trend = self.asset_state[asset]["trend"]
            regime, params = self.regime_detector.detect(asset, price, volume, htf_trend, tf_trend)
            adx_threshold = 18 if regime=="STRONG_TREND" else 20 if regime=="GRADUAL_TREND" else 20
            # Session
            session_ok, session_name, _ = self.session_timer.is_trading_time()
            if not session_ok:
                self.db_pipeline.add_reject(asset, price, 0, "Session", self.asset_state[asset]["volatility"], regime, "Session", regime)
                self.rejected+=1; return
            # Regime
            if not self.market_regime.check(asset, price, adx_threshold)[0]:
                self.db_pipeline.add_reject(asset, price, 0, "Regime", self.asset_state[asset]["volatility"], regime, "Regime", regime)
                self.rejected+=1; return
            # Direction
            if htf_trend=="BULLISH" and tf_trend=="BULLISH":
                direction="BUY"
            elif htf_trend=="BEARISH" and tf_trend=="BEARISH":
                direction="SELL"
            else:
                return
            # Advanced bonus
            adv_score, patterns, _, _ = self.advanced_engine.evaluate(asset, price, direction)
            # MTF (Score-Based Gate)
            mtf_passed, mtf_result = self.mtf_gate.check(
                asset=asset,
                direction=direction,
                price=price,
                params=params
            )
            if not mtf_passed:
                logger.info(mtf_result['log'])
                self.db_pipeline.add_reject(
                    asset, 
                    price, 
                    mtf_result.get('confidence', 0), 
                    "MTF", 
                    self.asset_state[asset]["volatility"], 
                    regime, 
                    "MTF"
                )
                self.rejected += 1
                return
            
            logger.info(mtf_result['log'])

            # Order flow
            of_strict = params.get("order_flow_strict", True)
            if not self.orderflow.check(asset, direction, price, of_strict)[0]:
                self.db_pipeline.add_reject(asset, price, 0, "OrderFlow", self.asset_state[asset]["volatility"], regime, "OrderFlow", regime)
                self.rejected+=1; return
            # SQS
            sr = self.cache.get(asset, 300, price)['support'] if direction=="BUY" else self.cache.get(asset, 300, price)['resistance']
            bos = self.cache.get(asset, 900, price)['bos']
            choch = self.cache.get(asset, 900, price)['choch']
            sweep = self.topology.detect_liquidity_sweep(asset, price) if params.get("use_micro_sweep", True) else ""
            ob = self.cache.get(asset, 900, price)['order_block']
            fvgs = self.cache.get(asset, 900, price)['fvg']
            vol_ratio = self.asset_state[asset]["volume_ratio"]
            base_sqs = self.sqs_calc.calculate(asset, price, direction, session_ok, patterns, bos, choch, sweep, ob, fvgs, vol_ratio, htf_trend, use_micro_sweep=params.get("use_micro_sweep", True))
            total_sqs = base_sqs + adv_score
            min_sqs = self.score_governor.get_current_sqs_base()
            if total_sqs < min_sqs:
                self.db_pipeline.add_reject(asset, price, total_sqs, f"SQS {total_sqs}<{min_sqs}", self.asset_state[asset]["volatility"], regime, "SQS", regime)
                self.rejected+=1; return
            # SL/TP
            atr = self.cache.get(asset, 3600, price)['atr'] or price*0.01
            sl, tp = self.dynamic_sl.calculate(asset, direction, price, atr)
            risk = abs(price-sl)
            rr = abs(tp-price)/risk if risk else 0
            # Cooldown & cap
            if time.time() - self.last_signal_time[asset] < Config.SIGNAL_COOLDOWN and not self._is_strong_trend(asset):
                self.db_pipeline.add_reject(asset, price, total_sqs, "Cooldown", self.asset_state[asset]["volatility"], regime, "Cooldown", regime)
                self.rejected+=1; return
            if len([t for t in self.signal_timestamps if time.time()-t<86400]) >= Config.MAX_SIGNALS_PER_DAY:
                self.db_pipeline.add_reject(asset, price, total_sqs, "DailyCap", self.asset_state[asset]["volatility"], regime, "DailyCap", regime)
                self.rejected+=1; return
            token = self.token_manager.generate("SCL", asset)
            data = {
                'asset': asset, 'direction': direction, 'entry': price,
                'sl': sl, 'tp': tp, 'sqs': total_sqs,
                'session': session_name, 'patterns': patterns,
                'logic': f"HTF {htf_trend} + BOS {bos} + Adv {adv_score}",
                'news': self.news.last_news.get('title', 'No news')[:100],
                'volatility': self.asset_state[asset]["volatility"],
                'regime': regime, 'htf_trend': htf_trend,
                'news_score': 0, 'score': 0, 'confidence': 'HIGH', 'num_passed': 11,
                'pending_candles': params.get('pending_candles',2),
                'volume_decay_threshold': params.get('volume_decay_threshold',0.6),
                'dynamic_min_sqs': min_sqs, 'signal_type': 'STANDARD',
                'signal_token': token, 'pattern_name': list(patterns.keys())[0] if patterns else "unknown"
            }
            self.pending_queue.add_signal(data)
            self.memory.update_state({"total_signals_generated": 1})
            self.thinking_model.trigger(self.memory.get_or_create_state().get("total_signals_generated",0))
            logger.info(f"⏳ Pending: {asset} {direction} @ {price} (SQS:{total_sqs}) Token:{token}")
        except Exception as e:
            logger.error(f"Tick error: {e}")

    def _check_intra_candle_signal(self, asset, price, volume):
        # Fast path: early exit if no absorption
        listener = self.absorption_listeners.get(asset)
        if not listener:
            return
        state = listener.get_state()
        if not state or not state.get("absorption_active_0_5", False):
            return
        if time.time() - state.get("last_update",0) > 3:
            return
        ratio = state.get("imbalance_ratio_0_5", 1.0)
        cvd = state.get("cvd", 0.0)
        with self.trade_lock:
            if any(t['asset']==asset for t in self.active_trades.values()):
                return
        if ratio >= 3.0 and cvd > 0:
            direction = "BUY"
        elif ratio <= 0.33 and cvd < 0:
            direction = "SELL"
        else:
            return
        # Check near S/R
        atr = self.cache.get(asset, 3600, price)['atr'] or price*0.01
        sr = self.cache.get(asset, 300, price)['support'] if direction=="BUY" else self.cache.get(asset, 300, price)['resistance']
        if direction == "BUY" and sr and abs(price - max(sr)) < 0.3*atr:
            pass
        elif direction == "SELL" and sr and abs(price - min(sr)) < 0.3*atr:
            pass
        else:
            return
        if not hasattr(self, '_last_micro_time'):
            self._last_micro_time = {}
        if self._last_micro_time.get(asset, 0) > time.time() - 300:
            return
        self._last_micro_time[asset] = time.time()
        sl, tp = self.dynamic_sl.calculate(asset, direction, price, atr)
        risk = abs(price-sl)
        rr = abs(tp-price)/risk if risk else 0
        token = self.token_manager.generate("BOT", asset)
        data = {
            'asset': asset, 'direction': direction, 'entry': price,
            'sl': sl, 'tp': tp, 'sqs': 75, 'session': "ALWAYS",
            'patterns': {}, 'logic': f"MICRO_STRUCTURE_ENTRY (RR={rr:.1f})",
            'news': self.news.last_news.get('title', 'No news')[:100],
            'volatility': self.asset_state[asset]["volatility"],
            'regime': "MICRO", 'htf_trend': self.asset_state[asset]["htf_trend"],
            'news_score': 0, 'score': 0, 'confidence': 'HIGH', 'num_passed': 11,
            'signal_type': 'MICRO', 'dynamic_min_sqs': 75,
            'signal_token': token, 'pattern_name': 'micro'
        }
        self._send_final_signal(data)

    def _update_indicators(self, asset, price):
        ind15 = self.cache.get(asset, 900, price)
        ind1h = self.cache.get(asset, 3600, price)
        self.asset_state[asset]["adx"] = ind15['adx']
        self.asset_state[asset]["rsi"] = ind15.get('rsi', 50)
        if ind15.get('ema_9') and ind15.get('ema_21'):
            self.asset_state[asset]["trend"] = "BULLISH" if ind15['ema_9'] > ind15['ema_21'] else "BEARISH"
        if ind1h.get('ema_9') and ind1h.get('ema_21'):
            self.asset_state[asset]["htf_trend"] = "BULLISH" if ind1h['ema_9'] > ind1h['ema_21'] else "BEARISH"
        self.asset_state[asset]["volume_ratio"] = ind15.get('volume_ratio', 1.0)
        self.asset_state[asset]["volatility"] = ind15['atr'] / price if ind15['atr'] else 0.01

    def _is_strong_trend(self, asset):
        ind15 = self.cache.get(asset, 900, 0)
        ind1h = self.cache.get(asset, 3600, 0)
        if not ind15.get('ema_9') or not ind15.get('ema_21') or not ind1h.get('ema_9') or not ind1h.get('ema_21'):
            return False
        return (ind15['ema_9'] - ind15['ema_21'] > 0) and (ind1h['ema_9'] - ind1h['ema_21'] > 0)

    def _send_final_signal(self, signal):
        try:
            asset = signal['asset']
            direction = signal['direction']
            price = signal['entry']
            sl, tp = signal['sl'], signal['tp']
            sqs, session = signal['sqs'], signal['session']
            patterns, logic, news = signal['patterns'], signal['logic'], signal['news']
            volatility, regime, htf_trend = signal['volatility'], signal['regime'], signal['htf_trend']
            news_score = signal['news_score']
            dynamic_min_sqs = signal.get('dynamic_min_sqs', Config.MIN_SQS)
            signal_type = signal.get('signal_type', 'STANDARD')
            token = signal.get('signal_token', None)
            pattern_name = signal.get('pattern_name', "unknown")
            chart = self.topology.get_visual_topology(asset, price, direction, sl, tp, patterns)
            rr = abs(tp - price) / abs(price - sl) if abs(price - sl) > 0 else 0

            # Direct synchronous SQLite write (To get valid trade_id)
            trade_id = self.db.log_trade(
                asset, direction, price, sl, tp, sqs, "HIGH", list(patterns.keys()), logic,
                volatility, regime, htf_trend, news_score, session, sqs,
                pattern_name, dynamic_min_sqs, signal_type, token
            )

            # Asynchronous Mongo Backup
            if self.mongo.db:
                backup_data = {
                    'id': trade_id,
                    'asset': asset,
                    'direction': direction,
                    'entry': price,
                    'stop_loss': sl,
                    'take_profit': tp,
                    'score': sqs,
                    'status': 'open',
                    'signal_type': signal_type,
                    'signal_token': token
                }
                self.db_pipeline.queue.put({'type': 'trade', 'args': (), 'data': backup_data})

            self.telegram.fire_signal(
                asset, direction, price, sl, tp, chart, logic, news,
                {"total_score": sqs, "confidence": "HIGH", "num_passed": 11},
                patterns, trade_id, session, rr, regime, signal_type, token
            )
            self.accepted += 1
            self.last_signal_time[asset] = time.time()
            self.signal_timestamps.append(time.time())
            with self.trade_lock:
                self.active_trades[trade_id] = {
                    'id': trade_id, 'asset': asset, 'direction': direction,
                    'entry': price, 'sl': sl, 'tp': tp, 'entry_time': int(time.time()),
                    'breakeven_locked': False, 'trailing_activated': False,
                    'hold_sent': False, 'initial_score': sqs, 'current_score': sqs,
                    'health': 100, 'regime': regime, 'signal_token': token
                }
        except Exception as e:
            logger.error(f"Final signal error: {e}")

    def _close_trade(self, tid, price, pnl, reason=""):
        self.db.close_trade(tid, price, pnl, reason)
        self.telegram.send(f"🔒 Trade #{tid} closed: {pnl:+.2f} | {reason}", priority=5)
        if tid in self.active_trades:
            del self.active_trades[tid]
        self.memory.update_state({"total_trades_closed": 1, "total_pnl": pnl,
                                  "total_wins": 1 if pnl>0 else 0, "total_losses": 1 if pnl<0 else 0})

    def _process_queue(self):
        while True:
            try:
                item = self.price_queue.get(timeout=1)
                if item:
                    self._handle_price_tick(*item)
            except Exception:
                pass

    def _ping_self_loop(self):
        while True:
            try:
                requests.get(Config.RENDER_URL, timeout=10)
            except Exception:
                pass
            time.sleep(300)

    def run(self):
        threading.Thread(target=start_health_server, args=(self,), daemon=True).start()
        # Load historical candles
        with ThreadPoolExecutor(max_workers=4) as executor:
            for asset in Config.ASSETS:
                for tf in [60, 300, 900, 3600, 14400]:
                    executor.submit(self._load_and_backfill, asset, tf)
        self.stream = BinancePublicStream(self._on_price)
        self.stream.start()
        self.telegram.send("🚀 AlphaBot v7.0 Core Online (Ultra-Low-Latency)", priority=5)
        last_news = 0
        while True:
            try:
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
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Main loop: {e}")

    def _load_and_backfill(self, asset, tf):
        candles = self.mongo.load_candles(asset, tf, limit=Config.MAX_CANDLES)
        if len(candles) >= Config.MAX_CANDLES * 0.9:
            self.topology.candles[tf][asset] = candles
            return
        interval = {60:"1m", 300:"5m", 900:"15m", 3600:"1h", 14400:"4h"}[tf]
        url = f"https://api.binance.com/api/v3/klines?symbol={asset}&interval={interval}&limit=1000"
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            fetched = []
            for d in resp.json():
                c = {"timestamp": d[0]//1000, "open": float(d[1]), "high": float(d[2]),
                     "low": float(d[3]), "close": float(d[4]), "volume": float(d[5]), "complete": True}
                fetched.append(c)
                self.mongo.save_candle(asset, tf, c)
            self.topology.candles[tf][asset] = fetched[-Config.MAX_CANDLES:]

    def _on_price(self, asset, price, volume):
        try:
            self.price_queue.put_nowait((asset, price, volume))
        except queue.Full:
            pass

# =====================================================================
# NEWS SCANNER (Lightweight)
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

    def _analyze_sentiment(self, text: str) -> float:
        bullish = ["bullish", "breakout", "surge", "buy", "accumulate", "rally", "green", "etf", "approve"]
        bearish = ["bearish", "crash", "dump", "sell", "liquidation", "drop", "red", "sec", "hack"]
        text = text.lower()
        score = sum(2 for w in bullish if w in text) - sum(2 for w in bearish if w in text)
        return max(-100, min(100, score * 5))

# =====================================================================
# ENTRY
# =====================================================================
if __name__ == "__main__":
    bot = AIOrchestrator()
    bot.run()

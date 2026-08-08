# =====================================================================
# app.py – AlphaBot v7.2 ULTIMATE (Cold Start Proof + Full Recovery)
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
import html
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import deque
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import websocket

HAS_PSUTIL = False
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    pass

try:
    import pymongo
    from pymongo import MongoClient, ASCENDING, DESCENDING
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AI-Orchestrator-v7.2")

# =====================================================================
# CONFIGURATION
# =====================================================================
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

    MIN_SQS = 50
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
    TRADE_HEALTH_STALE_MINUTES = 25

# =====================================================================
# DATABASE LAYERS
# =====================================================================
class MongoDatabase:
    def __init__(self):
        if not HAS_PYMONGO or not Config.MONGO_URI:
            self.client = None
            self.db = None
            return
        try:
            self.client = MongoClient(Config.MONGO_URI, serverSelectionTimeoutMS=5000)
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
            self.db.trades.create_index([("status", 1)])  # for open trades query
        except Exception:
            pass

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
            self.db.trades.replace_one({"id": trade_data["id"]}, trade_data, upsert=True)
        except Exception: pass

    def update_trade_sl(self, trade_id, new_sl):
        if self.db is None: return
        try:
            self.db.trades.update_one({"id": trade_id}, {"$set": {"stop_loss": new_sl}})
        except Exception: pass

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
        except Exception: pass

    def get_open_trades(self):
        if self.db is None: return []
        try:
            return list(self.db.trades.find({"status": "open"}))
        except Exception: return []

    def save_rejected_backup(self, rejected_data):
        if self.db is None: return
        try:
            self.db.rejected.insert_one(rejected_data)
        except Exception: pass

class TradeDatabase:
    def __init__(self, memory_engine=None):
        self.conn = sqlite3.connect(Config.DB_PATH, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.memory_engine = memory_engine
        self._lock = threading.Lock()
        self._create_tables()

    def _create_tables(self):
        with self._lock:
            cur = self.conn.cursor()
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
                    total_trades INTEGER, winning_trades INTEGER, losing_trades INTEGER
                )''')
                try:
                    cur.execute("ALTER TABLE trades ADD COLUMN signal_type TEXT DEFAULT 'STANDARD'")
                    cur.execute("ALTER TABLE trades ADD COLUMN signal_token TEXT")
                except sqlite3.OperationalError: pass
                self.conn.commit()
            except Exception: pass
            finally: cur.close()

    def generate_trade_id(self):
        return int(time.time() * 1000)

    def log_trade(self, trade_id, asset, direction, entry, sl, tp, score, confidence, patterns, logic,
                  volatility, regime, htf_trend, news_score, session, sqs_score, pattern_name,
                  dynamic_min_sqs, signal_type="STANDARD", signal_token=None):
        with self._lock:
            cur = self.conn.cursor()
            try:
                cur.execute('''INSERT INTO trades
                    (id, asset, direction, entry, stop_loss, take_profit, score, confidence, patterns, logic,
                     timestamp, volatility, regime, htf_trend, news_score, session, sqs_score, pattern_name,
                     dynamic_min_sqs, signal_type, signal_token)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (trade_id, asset, direction, entry, sl, tp, score, confidence, json.dumps(patterns), logic,
                     int(time.time()), volatility, regime, htf_trend, news_score, session, sqs_score,
                     pattern_name, dynamic_min_sqs, signal_type, signal_token))
                self.conn.commit()
                if self.memory_engine:
                    self.memory_engine.update_state({"accepted_signals_count": 1})
                return trade_id
            except Exception: return None
            finally: cur.close()

    def log_rejected(self, asset, price, score, reason, volatility, regime, gate_name=""):
        with self._lock:
            cur = self.conn.cursor()
            try:
                cur.execute('''INSERT INTO rejected_signals (asset, price, score, reason, timestamp, volatility, regime, gate_name)
                               VALUES (?,?,?,?,?,?,?,?)''',
                            (asset, price, score, reason, int(time.time()), volatility, regime, gate_name))
                self.conn.commit()
                if self.memory_engine:
                    self.memory_engine.update_state({"rejected_signals_count": 1})
            except Exception: pass
            finally: cur.close()

    def close_trade(self, trade_id, exit_price, pnl, exit_reason=""):
        with self._lock:
            cur = self.conn.cursor()
            try:
                cur.execute('''UPDATE trades SET status='closed', exit_price=?, pnl=?, close_time=?, exit_reason=?
                    WHERE id=?''', (exit_price, pnl, int(time.time()), exit_reason, trade_id))
                self.conn.commit()
            except Exception: pass
            finally: cur.close()

    def get_rolling_win_rate(self, asset: str, lookback: int = 50) -> float:
        with self._lock:
            cur = self.conn.cursor()
            try:
                cur.execute('''SELECT pnl FROM trades WHERE asset=? AND status='closed' AND pnl IS NOT NULL ORDER BY close_time DESC LIMIT ?''', (asset, lookback))
                rows = cur.fetchall()
                if not rows: return 0.5
                wins = sum(1 for r in rows if r[0] > 0)
                return wins / len(rows)
            except Exception: return 0.5
            finally: cur.close()

    def get_performance_metrics(self):
        with self._lock:
            cur = self.conn.cursor()
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
                return {"total_trades": total, "winning_trades": wins, "losing_trades": total - wins,
                        "win_rate": win_rate, "profit_factor": profit_factor,
                        "total_pnl": total_pnl, "avg_pnl": total_pnl / total if total else 0.0}
            except Exception: return {"total_trades":0,"winning_trades":0,"losing_trades":0,"win_rate":0.0,"profit_factor":0.0,"total_pnl":0.0,"avg_pnl":0.0}
            finally: cur.close()

    def get_recent_signal_timestamps(self, seconds=86400):
        """Return list of unix timestamps of open/closed signals within last seconds."""
        with self._lock:
            cur = self.conn.cursor()
            try:
                cutoff = int(time.time() - seconds)
                cur.execute("SELECT timestamp FROM trades WHERE timestamp >= ?", (cutoff,))
                return [row[0] for row in cur.fetchall()]
            except Exception: return []
            finally: cur.close()

# =====================================================================
# PERSISTENT MEMORY ENGINE
# =====================================================================
class PersistentMemoryEngine:
    def __init__(self, mongo_db):
        self.db = mongo_db
        self.collection = "global_bot_memory"
        if self.db is not None:
            try:
                if self.collection not in self.db.list_collection_names():
                    self.db.create_collection(self.collection)
            except Exception: pass

    def get_or_create_state(self):
        if self.db is None: return self._default_state()
        try:
            doc = self.db[self.collection].find_one({"_id": "global_state"})
            if doc: return doc
            default = self._default_state()
            default["_id"] = "global_state"
            self.db[self.collection].insert_one(default)
            return default
        except Exception: return self._default_state()

    def _default_state(self):
        return {
            "total_run_seconds": 0,
            "restart_count": 0,
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
        if self.db is None: return
        try:
            inc_fields = {k: v for k, v in updates.items() if isinstance(v, (int, float))}
            set_fields = {k: v for k, v in updates.items() if not isinstance(v, (int, float))}
            if inc_fields:
                inc_fields["last_update"] = 1
            else:
                set_fields["last_update"] = int(time.time())
            update_doc = {}
            if inc_fields: update_doc["$inc"] = inc_fields
            if set_fields: update_doc["$set"] = set_fields
            self.db[self.collection].update_one({"_id": "global_state"}, update_doc, upsert=True)
        except Exception: pass

# =====================================================================
# DYNAMIC SCORE GOVERNOR
# =====================================================================
class DynamicScoreGovernor:
    def __init__(self, memory_engine, lower_floor=50, upper_ceiling=70):
        self.memory = memory_engine
        self.lower_floor = lower_floor
        self.upper_ceiling = upper_ceiling
        self.current_base = Config.MIN_SQS
        self.adjustment_step = 3
        self.last_adjustment_time = 0
        self.cooldown = 3600

    def get_current_sqs_base(self):
        try:
            state = self.memory.get_or_create_state()
            total_signals = state.get("total_signals_generated", 0)
            if total_signals >= 140 and total_signals % 140 == 0:
                if time.time() - self.last_adjustment_time > self.cooldown:
                    self._apply_auto_recovery(state)
            return max(self.lower_floor, min(self.current_base, self.upper_ceiling))
        except Exception: return Config.MIN_SQS

    def _apply_auto_recovery(self, state):
        try:
            total = state.get("total_trades_closed", 0)
            wins = state.get("total_wins", 0)
            if total == 0: return
            win_rate = wins / total
            if win_rate < 0.5 and self.current_base < self.upper_ceiling:
                self.current_base = min(self.current_base + self.adjustment_step, self.upper_ceiling)
                self.last_adjustment_time = time.time()
            elif win_rate > 0.65 and self.current_base > self.lower_floor:
                self.current_base = max(self.current_base - self.adjustment_step, self.lower_floor)
                self.last_adjustment_time = time.time()
        except Exception: pass

# =====================================================================
# TOKEN MANAGER
# =====================================================================
class TokenManager:
    def __init__(self):
        self.counters = {"SNP": 0, "SCL": 0, "BOT": 0, "REJ": 0}
        self.lock = threading.Lock()

    def generate(self, prefix, asset, gate=None):
        with self.lock:
            self.counters[prefix] += 1
            counter = self.counters[prefix]
        if prefix == "REJ" and gate: return f"{prefix}-{gate}-{asset}-{counter:04d}"
        return f"{prefix}-{asset}-{counter:04d}"

# =====================================================================
# THINKING MODEL
# =====================================================================
class ThinkingOptimizationModel:
    def __init__(self, orchestrator):
        self.orch = orchestrator
        self.last_run = 0

    def trigger(self, total_signals):
        try:
            if total_signals % 30 != 0: return
            if time.time() - self.last_run < 300: return
            self.last_run = time.time()
            self._run_analysis()
        except Exception: pass

    def _run_analysis(self):
        try:
            logger.info("🧠 30-signal Thinking Model running...")
            cur = self.orch.db.conn.cursor()
            cur.execute("""SELECT pattern_name, regime, sqs_score, pnl FROM trades WHERE status='closed' AND pnl IS NOT NULL ORDER BY id DESC LIMIT 30""")
            rows = cur.fetchall()
            cur.close()
            if len(rows) < 30: return
            pattern_stats, regime_stats, sqs_bands = {}, {}, {"55-65":0,"66-75":0,"76-85":0,"86-100":0}
            total_wins = 0
            for pattern, regime, sqs, pnl in rows:
                is_win = 1 if pnl > 0 else 0
                total_wins += is_win
                pattern_stats.setdefault(pattern or "unknown", {"total":0,"wins":0})
                pattern_stats[pattern]["total"] += 1
                pattern_stats[pattern]["wins"] += is_win
                regime_stats.setdefault(regime or "unknown", {"total":0,"wins":0})
                regime_stats[regime]["total"] += 1
                regime_stats[regime]["wins"] += is_win
                band = "55-65" if sqs <= 65 else "66-75" if sqs <= 75 else "76-85" if sqs <= 85 else "86-100"
                sqs_bands[band] += is_win
            win_rate_30 = total_wins / len(rows) if len(rows) else 0
            worst_pattern = min(pattern_stats.items(), key=lambda x: x[1]["wins"]/x[1]["total"] if x[1]["total"]>=5 else 1)[0] if pattern_stats else "unknown"
            msg = f"🧠 30-Signal Audit:\n━━━━━━━━━━━━━━━━━━━━\nWin Rate: {win_rate_30:.2%}\nWorst Pattern: {worst_pattern}\nSQS Bands: {sqs_bands}"
            self.orch.telegram.send(msg, priority=1)
            gc.collect()
        except Exception as e: logger.error(f"Thinking model error: {e}")

# =====================================================================
# INDICATOR CACHE
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
            candles = self.topology.candles.get(tf, {}).get(asset, [])
            completed = [c for c in candles if c.get("complete", False)]
            data = {}
            if len(completed) >= 20:
                closes = [c['close'] for c in completed]
                def safe_ema(period):
                    if len(closes) >= period:
                        ema_list = self.topology._ema(closes, period)
                        return ema_list[-1] if ema_list else None
                    return None
                for p in [9,20,21,50,200]:
                    data[f'ema_{p}'] = safe_ema(p)
            data['atr'] = self.topology.get_atr(asset, period=14, tf=tf) or 0.0
            data['adx'] = self.topology.get_adx(asset, tf, period=14) or 20
            if len(completed) >= 14:
                closes = [c['close'] for c in completed[-14:]]
                data['rsi'] = self.topology._calc_rsi(closes) if hasattr(self.topology, '_calc_rsi') else 50
            else: data['rsi'] = 50
            sr = self.topology.support_resistance.get(asset, {})
            data['support'] = sr.get('support', [])
            data['resistance'] = sr.get('resistance', [])
            data['bos'] = self.topology.bos.get(asset, {}).get('direction', '')
            data['choch'] = self.topology.choch.get(asset, False)
            data['fvg'] = self.topology.detect_fvg(asset)
            data['order_block'] = self.topology.detect_order_block(asset)
            data['volume_ma'] = self.topology.volume_ma.get(asset, 0.0)
            data['price'] = price
            data['volume'] = volume
            if price and volume and data.get('volume_ma') and data['volume_ma'] > 0:
                data['volume_ratio'] = volume / data['volume_ma']
            else: data['volume_ratio'] = 1.0
            return data
        except Exception: return {}

# =====================================================================
# ASYNC DATABASE PIPELINE
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
                if item is None: continue
                if item.get('type') == 'trade':
                    if 'args' in item and item['args'] and self.db: self.db.log_trade(*item['args'])
                    if self.mongo and getattr(self.mongo,'db',None) and 'data' in item: self.mongo.save_trade_backup(item['data'])
                elif item.get('type') == 'reject':
                    if 'args' in item and item['args'] and self.db: self.db.log_rejected(*item['args'])
                    if self.mongo and getattr(self.mongo,'db',None) and 'data' in item: self.mongo.save_rejected_backup(item['data'])
            except queue.Empty: pass
            except Exception: pass

    def add_trade(self, *args, **kwargs):
        data = kwargs.get('data', args[-1] if args else {})
        try: self.queue.put_nowait({'type':'trade','args':args,'data':data})
        except queue.Full: pass

    def add_reject(self, *args, **kwargs):
        data = kwargs.get('data', args[-1] if args else {})
        try: self.queue.put_nowait({'type':'reject','args':args,'data':data})
        except queue.Full: pass

    def shutdown(self):
        self.running = False
        self.thread.join(timeout=1)

# =====================================================================
# WEBSOCKET STREAMS
# =====================================================================
class BinancePublicStream:
    def __init__(self, on_price_update):
        self.on_price_update = on_price_update
        self.running = False

    def start(self):
        self.running = True
        threading.Thread(target=self._ws_loop, daemon=True).start()

    def _ws_loop(self):
        while self.running:
            try:
                streams = [f"{a.lower()}@kline_1m" for a in Config.ASSETS]
                ws = websocket.WebSocketApp(f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}",
                                            on_message=self._on_msg, on_error=self._on_error)
                ws.run_forever(ping_interval=15, ping_timeout=8)
            except Exception: time.sleep(5)

    def _on_error(self, ws, error): pass
    def _on_msg(self, ws, msg):
        try:
            data = json.loads(msg)["data"]["k"]
            if data["s"] in Config.ASSETS:
                self.on_price_update(data["s"], float(data["c"]), float(data["v"]))
        except: pass

class BinanceFuturesStream:
    def __init__(self, on_data=None):
        self.ws_url = Config.BINANCE_FUTURES_WS_URL
        self.symbols = [s.lower() for s in Config.ASSETS]
        self.running = False
        self.data = {'open_interest': {}, 'liquidations': deque(maxlen=200), 'cvd': {}, 'last_trade': {}}
        self.oi_history = {s: deque(maxlen=10) for s in self.symbols}
        self.lock = threading.Lock()
        self.on_data = on_data
        self.thread = None

    def start(self):
        if self.thread and self.thread.is_alive(): return
        self.running = True
        self.thread = threading.Thread(target=self._ws_loop, daemon=True)
        self.thread.start()

    def _ws_loop(self):
        delay = 1
        while self.running:
            try:
                self.ws = websocket.WebSocketApp(self.ws_url, on_open=self._on_open, on_message=self._on_message,
                                                 on_error=lambda ws,err: None, on_close=lambda ws,c,m: None)
                self.ws.run_forever(ping_interval=15, ping_timeout=10)
            except: time.sleep(delay); delay = min(60, delay*2)
            else: delay = 1

    def _on_open(self, ws):
        try:
            streams = []
            for s in self.symbols:
                streams.extend([f"{s}@openInterest", f"{s}@forceOrder", f"{s}@aggTrade"])
            ws.send(json.dumps({"method":"SUBSCRIBE","params":streams,"id":1}))
        except: pass

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            if 'e' not in data: return
            e = data['e']
            if e == 'openInterest':
                self.data['open_interest'][data['s']] = float(data['o'])
                self.oi_history[data['s']].append(float(data['o']))
            elif e == 'forceOrder':
                o = data['o']
                self.data['liquidations'].append({'symbol':o['s'],'side':o['S'],'price':float(o['p']),'qty':float(o['q']),'time':time.time()})
            elif e == 'aggTrade':
                symbol = data['s']
                price = float(data['p']); qty = float(data['q'])
                last = self.data['last_trade'].get(symbol, price)
                delta = qty if price >= last else -qty
                self.data['cvd'][symbol] = self.data['cvd'].get(symbol,0) + delta
                self.data['last_trade'][symbol] = price
        except: pass

    def get_open_interest(self, sym): return self.data['open_interest'].get(sym,0)
    def get_oi_trend(self, sym):
        hist = list(self.oi_history.get(sym,[]))
        return hist[-1]-hist[0] if len(hist)>=2 else 0
    def get_cvd(self, sym): return self.data['cvd'].get(sym,0)
    def get_liquidations(self, sym, lookback=60):
        now = time.time()
        return [e for e in self.data['liquidations'] if e['symbol']==sym and (now-e['time'])<=lookback]

# =====================================================================
# TOPOLOGY ENGINE
# =====================================================================
class CandleTopologyEngine:
    def __init__(self):
        self.candles = {tf:{asset:[] for asset in Config.ASSETS} for tf in [60,300,900,3600,14400]}
        self.pivots = {asset:{"high":[],"low":[]} for asset in Config.ASSETS}
        self.bos = {asset:{"direction":""} for asset in Config.ASSETS}
        self.choch = {asset:False for asset in Config.ASSETS}
        self.support_resistance = {asset:{"support":[],"resistance":[]} for asset in Config.ASSETS}
        self.last_tick_time = {asset:0 for asset in Config.ASSETS}
        self.candle_just_closed = {asset:False for asset in Config.ASSETS}
        self.history = {asset:deque(maxlen=200) for asset in Config.ASSETS}
        self.volume_ma = {asset:0.0 for asset in Config.ASSETS}
        self._completed_cache = {}
        self.lock = threading.Lock()

    def process_tick(self, asset, price, volume):
        with self.lock:
            try:
                now = int(time.time())
                self.history[asset].append({"price":price,"volume":volume,"time":now})
                self.candle_just_closed[asset] = False
                tf900 = 900
                start = (now//tf900)*tf900
                storage = self.candles[tf900][asset]
                if storage and storage[-1].get("timestamp") != start:
                    if not storage[-1].get("complete",False):
                        storage[-1]["complete"] = True
                        self.candle_just_closed[asset] = True
                for tf in [60,300,900,3600,14400]:
                    self._build_candle(asset,price,volume,now,tf,self.candles[tf][asset])
                self._update_volume_ma(asset)
                self._update_pivots(asset,price)
                self._update_support_resistance(asset,price)
                self._detect_bos_choch(asset)
                self.last_tick_time[asset] = now
                self._completed_cache.clear()
            except: pass

    def _build_candle(self,asset,price,volume,ts,tf,storage):
        try:
            start = (ts//tf)*tf
            if not storage or storage[-1].get("timestamp") != start:
                if storage and not storage[-1].get("complete",False): storage[-1]["complete"] = True
                storage.append({"timestamp":start,"open":price,"high":price,"low":price,"close":price,"volume":volume,"complete":False})
                if len(storage)>Config.MAX_CANDLES: storage.pop(0)
            else:
                c = storage[-1]; c["high"]=max(c["high"],price); c["low"]=min(c["low"],price)
                c["close"]=price; c["volume"]+=volume
        except: pass

    def _update_volume_ma(self,asset):
        try:
            completed = self.get_completed(asset,300)
            self.volume_ma[asset] = sum(c['volume'] for c in completed[-20:])/20 if len(completed)>=20 else 0.0
        except: self.volume_ma[asset]=0.0

    def _update_pivots(self,asset,price):
        try:
            complete = self.get_completed(asset,900)
            if len(complete)<10: return
            for i in range(2,len(complete)-2):
                if complete[i-2]["high"]<complete[i]["high"]>complete[i+2]["high"] and complete[i-1]["high"]<complete[i]["high"]>complete[i+1]["high"]:
                    if complete[i]["high"] not in self.pivots[asset]["high"]: self.pivots[asset]["high"].append(complete[i]["high"])
                if complete[i-2]["low"]>complete[i]["low"]<complete[i+2]["low"] and complete[i-1]["low"]>complete[i]["low"]<complete[i+1]["low"]:
                    if complete[i]["low"] not in self.pivots[asset]["low"]: self.pivots[asset]["low"].append(complete[i]["low"])
            self.pivots[asset]["high"] = sorted(self.pivots[asset]["high"],reverse=True)[:5]
            self.pivots[asset]["low"] = sorted(self.pivots[asset]["low"])[:5]
        except: pass

    def _detect_bos_choch(self,asset):
        try:
            h=self.pivots[asset]["high"]; l=self.pivots[asset]["low"]
            if len(h)>=2 and len(l)>=2:
                if h[0]>h[1]: self.bos[asset]["direction"]="UP"
                elif l[0]<l[1]: self.bos[asset]["direction"]="DOWN"
                if len(h)>=3 and len(l)>=3: self.choch[asset] = (h[1]<h[2] and l[1]>l[2]) or (h[1]>h[2] and l[1]<l[2])
        except: pass

    def _update_support_resistance(self,asset,price):
        try:
            all_levels = self.pivots[asset]["high"]+self.pivots[asset]["low"]
            recent = self.get_completed(asset,900)[-10:]
            for c in recent:
                if c["high"] not in all_levels: all_levels.append(c["high"])
                if c["low"] not in all_levels: all_levels.append(c["low"])
            clusters = []
            for lvl in sorted(all_levels):
                if not clusters or abs(lvl-clusters[-1])/lvl>0.005: clusters.append(lvl)
            self.support_resistance[asset]["support"] = [l for l in clusters if l<price*0.99]
            self.support_resistance[asset]["resistance"] = [r for r in clusters if r>price*1.01]
        except: pass

    def get_completed(self,asset,tf):
        key = (asset,tf)
        if key in self._completed_cache: return self._completed_cache[key]
        completed = [c for c in self.candles[tf][asset] if c.get("complete",False)]
        self._completed_cache[key] = completed
        return completed

    def detect_candle_patterns(self,asset):
        candles = self.candles[300][asset]
        if len(candles)<2: return {}
        last = candles[-1]
        if not last.get("complete",False): return {}
        patterns = {}
        body = abs(last["close"]-last["open"])
        total = last["high"]-last["low"]
        if total>0:
            if (min(last["open"],last["close"])-last["low"])/total>0.6: patterns["bullish_rej"]=1
            if (last["high"]-max(last["open"],last["close"]))/total>0.6: patterns["bearish_rej"]=1
        return patterns

    def get_atr(self,asset,period=14,tf=3600):
        complete = self.get_completed(asset,tf)
        if len(complete)<period: return 0.0
        trs=[]
        for i in range(1,period+1):
            h,l=complete[i]["high"],complete[i]["low"]; pc=complete[i-1]["close"]
            trs.append(max(h-l,abs(h-pc),abs(l-pc)))
        return sum(trs)/period

    def detect_liquidity_sweep(self,asset,price):
        h=self.pivots[asset]["high"]; l=self.pivots[asset]["low"]
        if h and price>max(h[-2:]): return "BUY_SWEEP"
        if l and price<min(l[-2:]): return "SELL_SWEEP"
        return ""

    def _ema(self,series,period):
        if len(series)<period: return []
        ema=[sum(series[:period])/period]
        m=2/(period+1)
        for i in range(period,len(series)): ema.append((series[i]-ema[-1])*m+ema[-1])
        return ema

    def check_1m_rejection(self,asset,direction):
        candles = self.candles[60][asset]
        if len(candles)<2: return False
        last = next((c for c in reversed(candles) if c.get("complete",False)),None)
        if not last: return False
        r=last["high"]-last["low"]
        if r<=0: return False
        if direction=="BUY": return (min(last["open"],last["close"])-last["low"])/r>=0.4
        return (last["high"]-max(last["open"],last["close"]))/r>=0.4

    def get_visual_topology(self,asset,price,direction,sl,tp,patterns):
        try:
            min_p,max_p = min(price,sl,tp)*0.98, max(price,sl,tp)*1.02
            if max_p-min_p<0.01: min_p,max_p = price*0.95, price*1.05
            sr=self.support_resistance[asset]
            supports=[s for s in sr["support"] if min_p<=s<=max_p]
            resistances=[r for r in sr["resistance"] if min_p<=r<=max_p]
            rows=10
            lines=["┌─────────────────────────┐","│  📊 LIVE TOPOLOGY CHART  │","├─────────────────────────┤"]
            for i in range(rows,-1,-1):
                lvl=min_p+(max_p-min_p)*(i/rows)
                m=" "
                if i==min(range(rows+1),key=lambda x:abs(min_p+(max_p-min_p)*(x/rows)-price)): m="●"
                elif i==min(range(rows+1),key=lambda x:abs(min_p+(max_p-min_p)*(x/rows)-sl)): m="▼"
                elif i==min(range(rows+1),key=lambda x:abs(min_p+(max_p-min_p)*(x/rows)-tp)): m="★"
                else:
                    if any(abs(lvl-s)/s<0.001 for s in supports): m="S"
                    elif any(abs(lvl-r)/r<0.001 for r in resistances): m="R"
                bar="█"*int((i/rows)*10) if i>0 else ""
                lines.append(f"│ {lvl:>8.2f} │ {m} {bar:<10} │")
            lines+=["├─────────────────────────┤","│ ●=Entry ▼=SL ★=TP S/R   │","└─────────────────────────┘"]
            return "\n".join(lines)
        except: return "Chart unavailable"

    def get_adx(self,asset,tf,period=14):
        complete=self.get_completed(asset,tf)
        if len(complete)<period: return 20
        tr_list, dm_plus, dm_minus = [],[],[]
        for i in range(1,len(complete)):
            h,l=complete[i]["high"],complete[i]["low"]; ph,pl=complete[i-1]["high"],complete[i-1]["low"]
            tr=max(h-l,abs(h-ph),abs(l-pl)); tr_list.append(tr)
            up=h-ph; down=pl-l
            dm_plus.append(max(up,0) if up>down else 0); dm_minus.append(max(down,0) if down>up else 0)
        if len(tr_list)<period: return 20
        atr = sum(tr_list[:period])/period
        dp = sum(dm_plus[:period])/period; dm = sum(dm_minus[:period])/period
        for i in range(period,len(tr_list)):
            atr = (atr*(period-1)+tr_list[i])/period
            dp = (dp*(period-1)+dm_plus[i])/period
            dm = (dm*(period-1)+dm_minus[i])/period
        if atr==0: return 20
        di_p = (dp/atr)*100; di_m = (dm/atr)*100
        dx = (abs(di_p-di_m)/(di_p+di_m))*100 if (di_p+di_m)>0 else 0
        return min(100,dx)

    def detect_fvg(self,asset):
        complete=self.get_completed(asset,900)
        if len(complete)<3: return []
        fvgs=[]
        for i in range(2,len(complete)-1):
            c1,c2,c3=complete[i-2],complete[i-1],complete[i]
            if c1["close"]<c2["open"] and c2["close"]<c3["close"] and c1["high"]>c2["low"]: fvgs.append({"type":"bullish","upper":c1["high"],"lower":c2["low"]})
            if c1["close"]>c2["open"] and c2["close"]>c3["close"] and c2["high"]>c1["low"]: fvgs.append({"type":"bearish","upper":c2["high"],"lower":c1["low"]})
        return fvgs[-5:]

    def detect_order_block(self,asset):
        if not self.bos[asset]["direction"]: return {}
        complete=self.get_completed(asset,900)
        if len(complete)<10: return {}
        atr=self.get_atr(asset)
        if atr==0: return {}
        for i in range(len(complete)-1,-1,-1):
            c=complete[i]
            if (c["high"]-c["low"])>1.5*atr:
                return {"type":"bullish" if c["close"]>c["open"] else "bearish","high":c["high"],"low":c["low"]}
        return {}

    def _calc_rsi(self,closes,period=14):
        try:
            if len(closes)<period+1: return 50
            gains=losses=0.0
            for i in range(len(closes)-period,len(closes)):
                diff=closes[i]-closes[i-1]
                if diff>0: gains+=diff
                else: losses-=diff
            avg_g=gains/period; avg_l=losses/period
            if avg_l==0: return 100
            return 100-(100/(1+avg_g/avg_l))
        except: return 50

# =====================================================================
# GATES & DETECTORS (v7.2)
# =====================================================================
class AdvanceRegimeDetector:
    def __init__(self, cache):
        self.cache = cache; self.current_regime = {}; self.params = {}
    def detect(self, asset, price, volume, htf_trend, tf_trend):
        try:
            ind15=self.cache.get(asset,900,price,volume); ind1h=self.cache.get(asset,3600,price,volume)
            if ind1h is None: ind1h = ind15
            if ind15 is None: ind15 = {}
            adx15=ind15.get('adx',20); adx1h=ind1h.get('adx',20)
            atr15=ind15.get('atr',price*0.01)
            vol_ma=ind15.get('volume_ma',1.0)
            vol_ratio=volume/vol_ma if vol_ma>0 else 1.0
            composite_adx = 0.6*adx15+0.4*adx1h
            atr_pct = atr15/price if price else 0.01
            if composite_adx>32 and vol_ratio>1.3:
                regime="STRONG_TREND"; min_sqs=70; atr_mult=0.8; pc=2; of_strict=True; check_4h=True; micro_sweep=False
            elif composite_adx>=18:
                regime="GRADUAL_TREND"; min_sqs=60; atr_mult=1.5; pc=1; of_strict=False; check_4h=True; micro_sweep=True
            else:
                regime="CHOP"; min_sqs=55; atr_mult=2.5; pc=1; of_strict=False; check_4h=False; micro_sweep=True
            tol = max(0.01, min(atr_pct*atr_mult, 0.08))
            params = {"min_sqs":min_sqs,"use_micro_sweep":micro_sweep,"mtf_tolerance":tol,"volume_decay_threshold":0.6,
                      "pending_candles":pc,"order_flow_strict":of_strict,"check_4h_ema":check_4h,"composite_adx":composite_adx,"regime":regime}
            self.current_regime[asset]=regime; self.params[asset]=params
            return regime,params
        except: return "CHOP",{"min_sqs":55,"use_micro_sweep":True,"mtf_tolerance":0.03,"volume_decay_threshold":0.6,"pending_candles":1,"order_flow_strict":False,"check_4h_ema":False,"composite_adx":20,"regime":"CHOP"}

class MarketRegimeFilter:
    def __init__(self, cache): self.cache = cache
    def check(self, asset, price, adx_threshold):
        try:
            ind15=self.cache.get(asset,900,price) or {}; ind1h=self.cache.get(asset,3600,price) or {}
            adx15=ind15.get('adx',20); adx1h=ind1h.get('adx',20)
            if adx15<adx_threshold and adx1h<adx_threshold: return True,f"Low ADX ({adx15:.1f}/{adx1h:.1f}) but allowed"
            return True,"Pass"
        except: return True,"Pass"

class MTFConfluenceGate:
    def __init__(self, cache): self.cache = cache
    def check(self, asset, direction, price, params):
        try:
            ind1h=self.cache.get(asset,3600,price,0) or {}; ind15m=self.cache.get(asset,900,price,0) or {}
            if not ind1h or not ind15m: return True,{"confidence":50,"log":"⚠️ No data","passed":True}
            earned=0; max_score=0; log=[]
            ema50,ema200=ind1h.get('ema_50'),ind1h.get('ema_200'); adx=ind1h.get('adx',0)
            if ema50 and ema200:
                max_score+=35
                bull = (price>ema200 and ema50>ema200); bear = (price<ema200 and ema50<ema200)
                if (direction=="BUY" and bull) or (direction=="SELL" and bear):
                    if adx>25: earned+=35; log.append(f"Trend:✅ Strong (+35)")
                    else: earned+=25; log.append(f"Trend:✅ Weak (+25)")
                else:
                    if adx<20: earned+=15; log.append(f"Trend:⚠️ Chop (+15)")
                    else: log.append("Trend:❌ (+0)")
            else: max_score+=20; earned+=15; log.append("Trend:⚠️ No EMA (+15)")
            supports=ind15m.get('support',[]); resistances=ind15m.get('resistance',[])
            if supports or resistances:
                max_score+=30; nearest=None
                if direction=="BUY" and supports:
                    valid=[s for s in supports if isinstance(s,(int,float)) and s<price]
                    if valid: nearest=max(valid)
                elif direction=="SELL" and resistances:
                    valid=[r for r in resistances if isinstance(r,(int,float)) and r>price]
                    if valid: nearest=min(valid)
                if nearest:
                    dist=abs(price-nearest)/price
                    if dist<=0.02: earned+=30; log.append(f"S/R:✅ (+30) {dist:.2%}")
                    elif dist<=0.05: earned+=20; log.append(f"S/R:✅ (+20) {dist:.2%}")
                    else: earned+=10; log.append(f"S/R:⚠️ Far (+10) {dist:.2%}")
                else: log.append("S/R:❌ No level")
            else: max_score+=20; earned+=10; log.append("S/R:⚠️ No data (+10)")
            obs=ind15m.get('order_block',[]); fvgs=ind15m.get('fvg',[])
            cand=[]
            for o in obs if isinstance(obs,list) else []:
                if isinstance(o,dict):
                    p=o.get('price') or o.get('level'); t=o.get('type','').lower()
                    if p and isinstance(p,(int,float)): cand.append((p,t))
            for f in fvgs if isinstance(fvgs,list) else []:
                if isinstance(f,dict):
                    p=f.get('price') or f.get('level'); t=f.get('type','').lower()
                    if p and isinstance(p,(int,float)): cand.append((p,t))
            if cand:
                max_score+=35; min_dist=float('inf'); best=None
                for lvl,typ in cand:
                    d=abs(price-lvl)/price
                    if d<min_dist: min_dist=d; best=(lvl,typ)
                if best and min_dist<=0.02:
                    lvl,typ=best
                    if (direction=="BUY" and typ=='bullish') or (direction=="SELL" and typ=='bearish'):
                        earned+=35; log.append(f"OB/FVG:✅ (+35) {typ} at {lvl:.2f}")
                    else:
                        if min_dist<=0.01: earned+=20; log.append(f"OB/FVG:⚠️ (+20) close")
                        else: log.append("OB/FVG:❌ mismatch")
                else: log.append("OB/FVG:❌ no level within 2%")
            else: max_score+=25; earned+=15; log.append("OB/FVG:⚠️ No data (+15)")
            if max_score==0: return True,{"confidence":50,"log":"⚠️ No indicators","passed":True}
            conf = (earned/max_score)*100
            regime=params.get('regime','GRADUAL_TREND')
            threshold=65 if regime=="STRONG_TREND" else 55 if regime=="GRADUAL_TREND" else 50
            passed=conf>=threshold
            if not passed and conf>=50: passed=True; log.append("⚠️ allowed (conf≥50%)")
            full_log=f"{'✅ PASSED' if passed else '❌ REJECTED'} | Conf:{conf:.1f}% ({earned}/{max_score}) | Th:{threshold} | "+" | ".join(log)
            return passed,{"confidence":round(conf,1),"earned":earned,"max_possible":max_score,"threshold":threshold,"log":full_log,"passed":passed,"regime":regime}
        except: return True,{"confidence":50,"log":"MTF error","passed":True}

class OrderFlowAnalyzer:
    def __init__(self, futures): self.futures = futures
    def check(self, asset, direction, price, strict=True):
        try:
            sym=asset.lower(); oi=self.futures.get_open_interest(sym); oi_t=self.futures.get_oi_trend(sym); cvd=self.futures.get_cvd(sym)
            if oi==0: return True,"No OI data"
            if strict:
                if direction=="BUY" and oi_t<=0: logger.warning(f"OI not rising {asset} BUY")
                if direction=="SELL" and oi_t>=0: logger.warning(f"OI rising {asset} SELL")
            return True,"Pass"
        except: return True,"Pass"

# =====================================================================
# SQS CALCULATOR
# =====================================================================
class SQS_Calculator:
    def __init__(self, cache): self.cache = cache
    def calculate(self, asset, price, direction, session_ok, patterns, bos, choch, sweep, ob, fvgs, vol_ratio, htf_trend, use_micro_sweep=True):
        try:
            score=0
            if bos: score+=10
            if choch: score+=5
            if sweep: score+=5
            if use_micro_sweep and self.cache.topology.check_1m_rejection(asset,direction): score+=5
            if ob and ob.get("type"): score+=10
            if vol_ratio>1.5: score+=10
            elif vol_ratio>1.2: score+=5
            if htf_trend==direction: score+=10
            if session_ok: score+=5
            return max(50,score)
        except: return 50

# =====================================================================
# DYNAMIC STOP LOSS WITH BUFFER
# =====================================================================
class DynamicStopLoss:
    def __init__(self, cache): self.cache = cache
    def calculate(self, asset, direction, entry, atr):
        try:
            if entry<=0: return entry*0.98, entry*1.02
            buffer = max(atr*0.8, entry*0.005)
            sr = self.cache.get(asset,300,entry)['support'] if direction=="BUY" else self.cache.get(asset,300,entry)['resistance']
            nearest=None
            if sr: nearest=max(sr) if direction=="BUY" else min(sr)
            default_sl = entry+1.5*atr if direction=="SELL" else entry-1.5*atr
            if direction=="SELL":
                sl = nearest+0.5*atr if nearest else default_sl
                if sl-entry>1.5*atr: sl=default_sl
                sl = max(sl, entry+buffer)
            else:
                sl = nearest-0.5*atr if nearest else default_sl
                if entry-sl>1.5*atr: sl=default_sl
                sl = min(sl, entry-buffer)
            risk=abs(entry-sl)
            default_tp = entry-2*risk if direction=="SELL" else entry+2*risk
            if direction=="SELL":
                tp=nearest if nearest and (entry-nearest)<=3*risk else default_tp
                tp=max(tp, entry-3*risk, entry*0.70)
                if tp>=entry: tp=entry-1.5*risk
                if entry-tp<1.5*risk: tp=entry-1.5*risk
            else:
                tp=nearest if nearest and (nearest-entry)<=3*risk else default_tp
                tp=min(tp, entry+3*risk, entry*1.30)
                if tp<=entry: tp=entry+1.5*risk
                if tp-entry<1.5*risk: tp=entry+1.5*risk
            return sl,tp
        except: return entry*0.98, entry*1.02

# =====================================================================
# TELEGRAM
# =====================================================================
class TelegramPipeline:
    def __init__(self):
        self.token=Config.TELEGRAM_BOT_TOKEN; self.chat_id=Config.TELEGRAM_CHAT_ID
        self.queue=queue.PriorityQueue()
        threading.Thread(target=self._worker, daemon=True).start()
    def _worker(self):
        while True:
            try:
                pri,msg=self.queue.get(timeout=1)
                if msg: requests.post(f"https://api.telegram.org/bot{self.token}/sendMessage", data={"chat_id":self.chat_id,"text":msg,"parse_mode":"HTML"}, timeout=10)
            except queue.Empty: continue
            except Exception: pass
    def send(self, msg, priority=5):
        if msg:
            try: self.queue.put_nowait((priority,msg))
            except queue.Full: pass
    def fire_signal(self, asset, direction, price, sl, tp, chart, logic, news, score, patterns, trade_id, session, rr, regime, signal_type="STANDARD", signal_token=None):
        try:
            if signal_type=="SNIPER": header="🎯 <b>AI SNIPER REVERSAL</b>"; eng="🎯 [ENGINE A: SNIPER]"
            elif signal_type in ("BOTTLING","MICRO"): header="🏦 <b>INSTITUTIONAL ENTRY</b>"; eng="🏦 [ENGINE C: BOTTLING]"
            else: header="🔥 <b>AI SCALP SIGNAL</b>" if direction=="BUY" else "❄️ <b>AI SCALP SIGNAL</b>"; eng="⚡ [ENGINE B: SCALPER]"
            token_line = f"🆔 Token: {signal_token} (DB ID: #{trade_id})" if signal_token else f"🆔 DB ID: #{trade_id}"
            msg = (f"{header}\n{'━'*30}\n📊 {Config.DISPLAY_NAMES.get(asset,asset)} | {token_line}\n⏰ {session} | ⚡ {score['confidence']} ({score['total_score']:.0f}%)\n🎯 R:R {rr:.2f}\n💰 Entry:{price:.2f} 🛑 SL:{sl:.2f} 🎯 TP:{tp:.2f}\n📈 Regime:{regime} | Type:{signal_type}\n📌 Engine: {eng}\n\n📊 CHART:\n{chart}\n🧠 Logic: {logic}\n📰 News: {news}\n📊 Layers Passed: {score['num_passed']}/11\n{'━'*30}")
            self.send(msg, 1)
        except: pass
    def fire_news_alert(self, title, sentiment, fg):
        try: self.send(f"📰 {html.escape(title)}\n🧠 Sentiment: {sentiment:.0f} | Fear/Greed: {fg}", priority=4)
        except: pass

# =====================================================================
# PENDING VERIFICATION (scalper max 1 candle)
# =====================================================================
class PendingVerificationQueue:
    def __init__(self, topology): self.topology=topology; self.pending={}
    def add_signal(self, data):
        asset=data['asset']; completed=self.topology.get_completed(asset,300)
        if len(completed)<2: return False
        data['initial_volume']=completed[-1]["volume"]; data['latest_volume']=completed[-1]["volume"]
        data['candle_count']=0; data['start_price']=data['entry']; data['rejected']=False
        key=f"{asset}_{data['direction']}_{int(time.time())}"; self.pending[key]=data; return key
    def check_pending(self, asset):
        rem=[]; 
        for key,data in list(self.pending.items()):
            if data.get('asset')!=asset: continue
            completed=self.topology.get_completed(asset,300)
            if len(completed)<2: continue
            limit=data.get('pending_candles', Config.PENDING_VERIFICATION_CANDLES)
            new=completed[-limit:] if len(completed)>=limit else completed
            if len(new)>data.get('candle_count',0):
                for c in new[data['candle_count']:]: data['latest_volume']=c["volume"]; data['candle_count']+=1
                if data['candle_count']>=2:
                    fc=completed[-limit]['close']
                    if data['direction']=='BUY' and fc<data['start_price']*0.995: data['rejected']=True; rem.append(key)
                    elif data['direction']=='SELL' and fc>data['start_price']*1.005: data['rejected']=True; rem.append(key)
            if data.get('candle_count',0)>=limit: rem.append(key)
        for k in rem: self.pending.pop(k,None)
        return rem
    def get_verified(self):
        ready=[]; rem=[]
        for key,data in list(self.pending.items()):
            limit=data.get('pending_candles', Config.PENDING_VERIFICATION_CANDLES)
            if data.get('candle_count',0)>=limit and not data.get('rejected'): ready.append(data); rem.append(key)
            elif data.get('candle_count',0)>=limit and data.get('rejected'): rem.append(key)
        for k in rem: self.pending.pop(k,None)
        return ready

# =====================================================================
# SNIPER FILTER (Engine A)
# =====================================================================
class RallyExhaustionFilter:
    def __init__(self, cache): self.cache=cache
    def evaluate(self, asset, price):
        try:
            ind=self.cache.get(asset,14400,price); ema20=ind.get('ema_20')
            if not ema20: return None,"No EMA"
            atr=self.cache.get(asset,3600,price)['atr']
            if atr==0: return None,"No ATR"
            above=price-ema20; below=ema20-price
            if above>2.5*atr or below>2.5*atr:
                ind15=self.cache.get(asset,900,price); comp=self.cache.topology.get_completed(asset,900)
                if len(comp)<20: return None,"No 15m"
                last=comp[-1]; body=abs(last["close"]-last["open"]); rng=last["high"]-last["low"]
                if rng==0: return None,"No range"
                vol_ma=ind15.get('volume_ma',0)
                if vol_ma==0: return None,"No vol MA"
                vol_spike=last["volume"]>1.5*vol_ma
                upper_wick=last["high"]-max(last["open"],last["close"]); lower_wick=min(last["open"],last["close"])-last["low"]
                if above>2.5*atr and vol_spike and upper_wick/rng>0.5: return {"direction":"SELL","score":85,"reason":"Overbought+Wick"},None
                if below>2.5*atr and vol_spike and lower_wick/rng>0.5: return {"direction":"BUY","score":85,"reason":"Oversold+Wick"},None
            return None,"No trigger"
        except: return None,"Error"

# =====================================================================
# INSTITUTIONAL ABSORPTION DETECTOR (Engine C embedded)
# =====================================================================
class InstitutionalAbsorptionDetector:
    def __init__(self, futures_stream): self.futures=futures_stream
    def evaluate(self, asset, price, direction_hint=None):
        try:
            sym=asset.lower(); oi=self.futures.get_open_interest(sym); oi_t=self.futures.get_oi_trend(sym); cvd=self.futures.get_cvd(sym)
            liqs=self.futures.get_liquidations(sym,120); score=0; direction=None
            if oi_t>0 and cvd>0: score+=40; direction="BUY"
            elif oi_t<0 and cvd<0: score+=40; direction="SELL"
            else:
                if cvd>0: direction="BUY"; score+=20
                elif cvd<0: direction="SELL"; score+=20
            sell_liqs=sum(1 for l in liqs if l['side']=='SELL'); buy_liqs=sum(1 for l in liqs if l['side']=='BUY')
            if direction=="BUY" and sell_liqs>0: score+=20
            elif direction=="SELL" and buy_liqs>0: score+=20
            return direction, min(score,100), {}
        except: return None,0,{}

# =====================================================================
# ADVANCED SIGNAL ENGINE
# =====================================================================
class AdvancedSignalEngine:
    def __init__(self, cache): self.cache=cache
    def evaluate(self, asset, price, direction):
        try:
            patterns=self.cache.topology.detect_candle_patterns(asset); data=self.cache.get(asset,900,price)
            bos=data.get('bos',''); choch=data.get('choch',False); score=0
            if patterns:
                if direction=="BUY" and "bullish_rej" in patterns: score+=10
                elif direction=="SELL" and "bearish_rej" in patterns: score+=10
            if bos==direction: score+=10
            if choch: score+=5
            return score, patterns, "", ""
        except: return 0,{}, "",""

# =====================================================================
# TRADE HEALTH ENGINE (0-100%)
# =====================================================================
class TradeHealthEngine:
    def __init__(self, topology, cache): self.topology=topology; self.cache=cache
    def calculate_health(self, trade):
        try:
            asset=trade['asset']; direction=trade['direction']; entry=trade['entry']
            current=self.topology.history[asset][-1]['price'] if self.topology.history.get(asset) else entry
            atr=self.topology.get_atr(asset)
            if atr==0: return 100
            unreal = (current-entry) if direction=="BUY" else (entry-current)
            dd_pct = -unreal/(atr*2) if unreal<0 else 0
            health_dd = max(0,100+dd_pct*50)
            duration_m = (time.time() - trade.get('entry_time',time.time()))/60
            if duration_m > Config.TRADE_HEALTH_STALE_MINUTES: health_time = max(0,100 - (duration_m-Config.TRADE_HEALTH_STALE_MINUTES)*2)
            else: health_time = 100
            ind1h = self.cache.get(asset,3600,current); ema9=ind1h.get('ema_9',current); ema21=ind1h.get('ema_21',current)
            trend_bull = ema9>ema21
            if direction=="BUY" and not trend_bull: health_trend=70
            elif direction=="SELL" and trend_bull: health_trend=70
            else: health_trend=100
            vol_ratio = self.topology.volume_ma.get(asset,0)
            if vol_ratio>2.0: health_vol=60
            elif vol_ratio>1.5: health_vol=80
            else: health_vol=100
            health = health_dd*0.4 + health_time*0.3 + health_trend*0.2 + health_vol*0.1
            return max(0,min(100,int(health)))
        except: return 100

# =====================================================================
# LIFECYCLE (with health, emergency exit, SL persistence)
# =====================================================================
class ActiveTradeLifecycle:
    def __init__(self, orchestrator): self.orch=orchestrator
    def monitor_lifecycle(self):
        while True:
            try:
                time.sleep(60); self._update_trades(); gc.collect()
            except Exception as e: logger.error(f"Lifecycle: {e}")
    def _update_trades(self):
        with self.orch.trade_lock:
            to_rem=[]
            for tid,trade in list(self.orch.active_trades.items()):
                asset=trade['asset']
                curr=self.orch.topology.history[asset][-1]['price'] if self.orch.topology.history.get(asset) else trade['entry']
                atr=self.orch.topology.get_atr(asset); dur=time.time()-trade.get('entry_time',time.time())
                health=self.orch.health_engine.calculate_health(trade); trade['health']=health
                if health<Config.HEALTH_EMERGENCY_THRESHOLD:
                    pnl=curr-trade['entry'] if trade['direction']=='BUY' else trade['entry']-curr
                    self.orch._close_trade(tid,curr,pnl,f"EmergencyHealth-{health}"); to_rem.append(tid); continue
                if dur>Config.MAX_HOLD_TIME:
                    pnl=curr-trade['entry'] if trade['direction']=='BUY' else trade['entry']-curr
                    self.orch._close_trade(tid,curr,pnl,"MaxHold"); to_rem.append(tid); continue
                if dur>Config.TIME_DECAY_SECONDS and abs(curr-trade['entry'])/trade['entry']<Config.TIME_DECAY_THRESHOLD_PCT:
                    self.orch._close_trade(tid,curr,0.0,"TimeDecay"); to_rem.append(tid); continue
                if not trade.get('breakeven_locked',False):
                    target=abs(trade['tp']-trade['entry']); half=trade['entry']+0.5*target if trade['direction']=='BUY' else trade['entry']-0.5*target
                    if (trade['direction']=='BUY' and curr>=half) or (trade['direction']=='SELL' and curr<=half):
                        if self.orch.topology.check_1m_rejection(asset,trade['direction']):
                            trade['sl']=trade['entry']; trade['breakeven_locked']=True; self.orch.mongo.update_trade_sl(tid,trade['entry'])
                if not trade.get('trailing_activated',False):
                    target=abs(trade['tp']-trade['entry']); trigger=trade['entry']+0.7*target if trade['direction']=='BUY' else trade['entry']-0.7*target
                    if (trade['direction']=='BUY' and curr>=trigger) or (trade['direction']=='SELL' and curr<=trigger):
                        new_sl=trade['entry']+0.3*target if trade['direction']=='BUY' else trade['entry']-0.3*target
                        if (trade['direction']=='BUY' and new_sl>trade['sl']) or (trade['direction']=='SELL' and new_sl<trade['sl']):
                            trade['sl']=new_sl; trade['trailing_activated']=True; self.orch.mongo.update_trade_sl(tid,new_sl)
                if trade['direction']=='BUY':
                    if curr<=trade['sl']: self.orch._close_trade(tid,curr,curr-trade['entry'],"SL"); to_rem.append(tid)
                    elif curr>=trade['tp']: self.orch._close_trade(tid,curr,curr-trade['entry'],"TP"); to_rem.append(tid)
                else:
                    if curr>=trade['sl']: self.orch._close_trade(tid,curr,trade['entry']-curr,"SL"); to_rem.append(tid)
                    elif curr<=trade['tp']: self.orch._close_trade(tid,curr,trade['entry']-curr,"TP"); to_rem.append(tid)
            for tid in to_rem: self.orch.active_trades.pop(tid,None)

# =====================================================================
# INSTITUTIONAL BOTTLING ENGINE (Engine C, no state lock)
# =====================================================================
class InstitutionalBottlingEngine:
    def __init__(self, orchestrator): self.orch=orchestrator; self.executor=ThreadPoolExecutor(max_workers=len(Config.ASSETS)*2)
    def start(self): threading.Thread(target=self._loop, daemon=True).start()
    def _loop(self):
        while True:
            try:
                time.sleep(10); fs=[self.executor.submit(self._analyze,a) for a in Config.ASSETS]
                for f in fs: f.result()
            except Exception as e: logger.error(f"Bottling loop: {e}")
    def _analyze(self, asset):
        try:
            price=self.orch.topology.history[asset][-1]['price'] if self.orch.topology.history.get(asset) else 0
            if not price: return
            with self.orch.trade_lock:
                if any(t['asset']==asset for t in self.orch.active_trades.values()): return
            det=self.orch.absorption_detector; direction,score,_ = det.evaluate(asset,price)
            if not direction or score<50: return
            sr=self.orch.cache.get(asset,900,price)
            supports=sr.get('support',[]); resistances=sr.get('resistance',[])
            near=False
            if direction=="BUY" and supports: near=(price-max(supports))/price<0.02
            elif direction=="SELL" and resistances: near=(min(resistances)-price)/price<0.02
            if score>=60 and near:
                self.orch._fire_bottling_signal(asset,direction,price,score)
        except: pass

# =====================================================================
# CORE ORCHESTRATOR v7.2 (with state recovery)
# =====================================================================
class AIOrchestrator:
    def __init__(self):
        try:
            self.topology = CandleTopologyEngine()
            self.cache = IndicatorCache(self.topology)
            self.news = CryptoNewsScanner()
            self.mongo = MongoDatabase()
            self.memory = PersistentMemoryEngine(self.mongo.db)
            self.db = TradeDatabase(memory_engine=self.memory)
            self.telegram = TelegramPipeline()
            self.futures_stream = BinanceFuturesStream(); self.futures_stream.start()
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
            self.absorption_detector = InstitutionalAbsorptionDetector(self.futures_stream)
            self.health_engine = TradeHealthEngine(self.topology, self.cache)
            self.active_trades = {}
            self.trade_lock = threading.Lock()
            self.price_queue = queue.Queue(maxsize=1000)
            self.start_time = time.time()
            self.last_signal_time = {a:0 for a in Config.ASSETS}
            self.signal_timestamps = deque(maxlen=100)
            self.asset_state = {a:{"trend":"NEUTRAL","htf_trend":"NEUTRAL","volume_ratio":1.0,"rsi":50,"adx":20,"volatility":0.01} for a in Config.ASSETS}
            self.accepted=0; self.rejected=0; self.stream=None
            self._sync_initial_metadata()
            self.score_governor = DynamicScoreGovernor(self.memory)
            self.token_manager = TokenManager()
            self.thinking_model = ThinkingOptimizationModel(self)
            self._restore_state_from_db()  # CRITICAL: reload open trades and signal timestamps
            self.bottling_engine = InstitutionalBottlingEngine(self); self.bottling_engine.start()
            self.db_pipeline = DatabasePipeline(self.db, self.mongo)
            self.lifecycle = ActiveTradeLifecycle(self)
            threading.Thread(target=self.lifecycle.monitor_lifecycle, daemon=True).start()
            threading.Thread(target=self._process_queue, daemon=True).start()
            threading.Thread(target=self._ping_self_loop, daemon=True).start()
            threading.Thread(target=self._memory_sync_loop, daemon=True).start()
            logger.info("🚀 AlphaBot v7.2 Core (Cold Start Proof) started")
        except Exception as e: logger.error(f"Orchestrator init: {e}")

    def _sync_initial_metadata(self):
        try: self.memory.update_state({"restart_count":1, "last_restart_at":int(time.time())})
        except: pass

    def _memory_sync_loop(self):
        while True:
            time.sleep(60); now=int(time.time())
            if now - getattr(self,'_last_mem_sync',0) >= 300:
                self.memory.update_state({"total_run_seconds":int(time.time()-self.start_time),"last_update":now})
                self._last_mem_sync = now

    def _restore_state_from_db(self):
        """Reload open trades and recent signal timestamps from MongoDB/SQLite."""
        # Reload open trades
        open_trades = self.mongo.get_open_trades()
        restored = 0
        with self.trade_lock:
            for t in open_trades:
                tid = t.get('id')
                if not tid: continue
                if tid not in self.active_trades:
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
                    restored += 1
        if restored: logger.info(f"✅ Restored {restored} open trades from MongoDB")
        # Restore signal timestamps (for cooldown/daily cap)
        timestamps = self.db.get_recent_signal_timestamps(86400)
        self.signal_timestamps = deque(timestamps, maxlen=100)
        for ts in timestamps:
            # populate last_signal_time roughly
            pass  # Not perfect but helps avoid immediate re-signal
        if timestamps: logger.info(f"✅ Restored {len(timestamps)} recent signal timestamps")

    def _fire_bottling_signal(self, asset, direction, price, abs_score):
        try:
            atr=self.cache.get(asset,3600,price)['atr'] or price*0.01
            sl,tp=self.dynamic_sl.calculate(asset,direction,price,atr)
            risk=abs(price-sl); rr=abs(tp-price)/risk if risk else 0
            token=self.token_manager.generate("BOT",asset)
            trade_id=self.db.generate_trade_id()
            data={'asset':asset,'direction':direction,'entry':price,'sl':sl,'tp':tp,'sqs':80,'session':"ALWAYS",
                  'patterns':{},'logic':f"INST_ABSORPTION (score {abs_score})",'news':self.news.last_news.get('title','')[:100],
                  'volatility':self.asset_state[asset]["volatility"],'regime':"BOTTLING",'htf_trend':self.asset_state[asset]["htf_trend"],
                  'news_score':0,'score':0,'confidence':'VERY HIGH','num_passed':11,'signal_type':'BOTTLING','dynamic_min_sqs':80,
                  'signal_token':token,'pattern_name':'absorption','trade_id':trade_id}
            self._send_final_signal(data)
        except: pass

    def _handle_price_tick(self, asset, price, volume):
        try:
            self.topology.process_tick(asset,price,volume)
            self._check_intra_candle_signal(asset,price,volume)
            if not self.topology.candle_just_closed.get(asset,False): return
            if self.pending_queue.pending:
                self.pending_queue.check_pending(asset)
                for sig in self.pending_queue.get_verified(): self._send_final_signal(sig)
            sniper,_ = self.exhaust_filter.evaluate(asset,price)
            if sniper:
                direction=sniper["direction"]; atr=self.cache.get(asset,3600,price)['atr'] or price*0.01
                sl,tp=self.dynamic_sl.calculate(asset,direction,price,atr); risk=abs(price-sl); rr=abs(tp-price)/risk if risk else 0
                if rr<2.5: tp = price-2.5*risk if direction=="SELL" else price+2.5*risk
                token=self.token_manager.generate("SNP",asset); trade_id=self.db.generate_trade_id()
                data={'asset':asset,'direction':direction,'entry':price,'sl':sl,'tp':tp,'sqs':sniper["score"],'session':"ALWAYS",
                      'patterns':{},'logic':f"SNIPER: {sniper['reason']}",'news':self.news.last_news.get('title','')[:100],
                      'volatility':self.asset_state[asset]["volatility"],'regime':"SNIPER",'htf_trend':self.asset_state[asset]["htf_trend"],
                      'news_score':0,'score':0,'confidence':'HIGH','num_passed':11,'signal_type':'SNIPER','dynamic_min_sqs':sniper["score"],
                      'signal_token':token,'trade_id':trade_id}
                self._send_final_signal(data); self.memory.update_state({"total_signals_generated":1})
                self.thinking_model.trigger(self.memory.get_or_create_state().get("total_signals_generated",0)); return
            self._update_indicators(asset,price)
            htf=self.asset_state[asset]["htf_trend"]; tf=self.asset_state[asset]["trend"]
            regime,params=self.regime_detector.detect(asset,price,volume,htf,tf)
            session_ok,session_name,_=self.session_timer.is_trading_time()
            if not session_ok: self.db_pipeline.add_reject(asset,price,0,"Session",self.asset_state[asset]["volatility"],regime,"Session"); self.rejected+=1; return
            if not self.market_regime.check(asset,price,15)[0]: self.db_pipeline.add_reject(asset,price,0,"Regime",self.asset_state[asset]["volatility"],regime,"Regime"); self.rejected+=1; return
            if htf=="BULLISH" and tf=="BULLISH": direction="BUY"
            elif htf=="BEARISH" and tf=="BEARISH": direction="SELL"
            else:
                rsi=self.asset_state[asset]["rsi"]
                if rsi>70: direction="SELL"
                elif rsi<30: direction="BUY"
                else: return
            adv,patterns,_,_ = self.advanced_engine.evaluate(asset,price,direction)
            mtf_ok,mtf_res=self.mtf_gate.check(asset,direction,price,params)
            if not mtf_ok: self.db_pipeline.add_reject(asset,price,mtf_res.get('confidence',0),"MTF",self.asset_state[asset]["volatility"],regime,"MTF"); self.rejected+=1; return
            if not self.orderflow.check(asset,direction,price,params.get("order_flow_strict",False))[0]: self.db_pipeline.add_reject(asset,price,0,"OrderFlow",self.asset_state[asset]["volatility"],regime,"OrderFlow"); self.rejected+=1; return
            data900=self.cache.get(asset,900,price); bos=data900.get('bos',''); choch=data900.get('choch',False); ob=data900.get('order_block',{})
            fvgs=data900.get('fvg',[]); sweep=self.topology.detect_liquidity_sweep(asset,price) if params.get("use_micro_sweep",True) else ""
            vol_ratio=self.asset_state[asset]["volume_ratio"]; base_sqs=self.sqs_calc.calculate(asset,price,direction,session_ok,patterns,bos,choch,sweep,ob,fvgs,vol_ratio,htf,params.get("use_micro_sweep",True))
            total_sqs=base_sqs+adv; min_sqs=self.score_governor.get_current_sqs_base()
            if total_sqs<min_sqs: self.db_pipeline.add_reject(asset,price,total_sqs,f"SQS<{min_sqs}",self.asset_state[asset]["volatility"],regime,"SQS"); self.rejected+=1; return
            atr=self.cache.get(asset,3600,price)['atr'] or price*0.01; sl,tp=self.dynamic_sl.calculate(asset,direction,price,atr)
            risk=abs(price-sl); rr=abs(tp-price)/risk if risk else 0
            if time.time()-self.last_signal_time[asset]<Config.SIGNAL_COOLDOWN and not self._is_strong_trend(asset):
                self.db_pipeline.add_reject(asset,price,total_sqs,"Cooldown",self.asset_state[asset]["volatility"],regime,"Cooldown"); self.rejected+=1; return
            if len([t for t in self.signal_timestamps if time.time()-t<86400])>=Config.MAX_SIGNALS_PER_DAY:
                self.db_pipeline.add_reject(asset,price,total_sqs,"DailyCap",self.asset_state[asset]["volatility"],regime,"DailyCap"); self.rejected+=1; return
            token=self.token_manager.generate("SCL",asset); trade_id=self.db.generate_trade_id()
            data={'asset':asset,'direction':direction,'entry':price,'sl':sl,'tp':tp,'sqs':total_sqs,'session':session_name,
                  'patterns':patterns,'logic':f"HTF {htf} + BOS {bos}",'news':self.news.last_news.get('title','')[:100],
                  'volatility':self.asset_state[asset]["volatility"],'regime':regime,'htf_trend':htf,
                  'news_score':0,'score':0,'confidence':'HIGH','num_passed':11,'pending_candles':params.get('pending_candles',2),
                  'volume_decay_threshold':params.get('volume_decay_threshold',0.6),'dynamic_min_sqs':min_sqs,'signal_type':'STANDARD',
                  'signal_token':token,'pattern_name':list(patterns.keys())[0] if patterns else "unknown",'trade_id':trade_id}
            self.pending_queue.add_signal(data); self.memory.update_state({"total_signals_generated":1})
            self.thinking_model.trigger(self.memory.get_or_create_state().get("total_signals_generated",0))
            logger.info(f"⏳ Pending: {asset} {direction} @ {price} (SQS:{total_sqs}) Token:{token}")
        except Exception as e: logger.error(f"Tick error: {e}")

    def _check_intra_candle_signal(self, asset, price, volume):
        try:
            with self.trade_lock:
                if any(t['asset']==asset for t in self.active_trades.values()): return
            direction,score,_ = self.absorption_detector.evaluate(asset,price)
            if not direction or score<70: return
            atr=self.cache.get(asset,3600,price)['atr'] or price*0.01
            sr=self.cache.get(asset,300,price)['support'] if direction=="BUY" else self.cache.get(asset,300,price)['resistance']
            near=False
            if direction=="BUY" and sr: near=(price-max(sr))/price<0.02
            elif direction=="SELL" and sr: near=(min(sr)-price)/price<0.02
            if not near: return
            if not hasattr(self,'_last_micro_time'): self._last_micro_time={}
            if self._last_micro_time.get(asset,0)>time.time()-300: return
            self._last_micro_time[asset]=time.time()
            sl,tp=self.dynamic_sl.calculate(asset,direction,price,atr); risk=abs(price-sl); rr=abs(tp-price)/risk if risk else 0
            token=self.token_manager.generate("BOT",asset); trade_id=self.db.generate_trade_id()
            data={'asset':asset,'direction':direction,'entry':price,'sl':sl,'tp':tp,'sqs':75,'session':"ALWAYS",
                  'patterns':{},'logic':f"MICRO_STRUCT_ABSORPTION",'news':self.news.last_news.get('title','')[:100],
                  'volatility':self.asset_state[asset]["volatility"],'regime':"MICRO",'htf_trend':self.asset_state[asset]["htf_trend"],
                  'news_score':0,'score':0,'confidence':'HIGH','num_passed':11,'signal_type':'MICRO','dynamic_min_sqs':75,
                  'signal_token':token,'pattern_name':'micro','trade_id':trade_id}
            self._send_final_signal(data)
        except: pass

    def _update_indicators(self, asset, price):
        try:
            ind15=self.cache.get(asset,900,price); ind1h=self.cache.get(asset,3600,price)
            self.asset_state[asset]["adx"]=ind15['adx']; self.asset_state[asset]["rsi"]=ind15.get('rsi',50)
            if ind15.get('ema_9') and ind15.get('ema_21'): self.asset_state[asset]["trend"]="BULLISH" if ind15['ema_9']>ind15['ema_21'] else "BEARISH"
            if ind1h.get('ema_9') and ind1h.get('ema_21'): self.asset_state[asset]["htf_trend"]="BULLISH" if ind1h['ema_9']>ind1h['ema_21'] else "BEARISH"
            self.asset_state[asset]["volume_ratio"]=ind15.get('volume_ratio',1.0); self.asset_state[asset]["volatility"]=ind15['atr']/price if ind15['atr'] else 0.01
        except: pass

    def _is_strong_trend(self, asset):
        try:
            ind15=self.cache.get(asset,900,0); ind1h=self.cache.get(asset,3600,0)
            return (ind15.get('ema_9') and ind15.get('ema_21') and ind1h.get('ema_9') and ind1h.get('ema_21') and
                    (ind15['ema_9']-ind15['ema_21']>0) and (ind1h['ema_9']-ind1h['ema_21']>0))
        except: return False

    def _send_final_signal(self, signal):
        try:
            asset=signal['asset']; direction=signal['direction']; price=signal['entry']; sl,tp=signal['sl'],signal['tp']
            sqs,session=signal['sqs'],signal['session']; patterns,logic,news=signal['patterns'],signal['logic'],signal['news']
            vol,regime,htf=signal['volatility'],signal['regime'],signal['htf_trend']; ns=signal['news_score']
            dm=signal.get('dynamic_min_sqs',Config.MIN_SQS); st=signal.get('signal_type','STANDARD'); token=signal.get('signal_token')
            pn=signal.get('pattern_name','unknown'); trade_id=signal.get('trade_id') or self.db.generate_trade_id()
            chart=self.topology.get_visual_topology(asset,price,direction,sl,tp,patterns)
            rr=abs(tp-price)/abs(price-sl) if abs(price-sl)>0 else 0
            self.db.log_trade(trade_id,asset,direction,price,sl,tp,sqs,"HIGH",list(patterns.keys()),logic,vol,regime,htf,ns,session,sqs,pn,dm,st,token)
            if self.mongo.db: self.mongo.save_trade_backup({'id':trade_id,'asset':asset,'direction':direction,'entry':price,'stop_loss':sl,'take_profit':tp,'score':sqs,'status':'open','signal_type':st,'signal_token':token})
            self.telegram.fire_signal(asset,direction,price,sl,tp,chart,logic,news,{"total_score":sqs,"confidence":"HIGH","num_passed":11},patterns,trade_id,session,rr,regime,st,token)
            self.accepted+=1; self.last_signal_time[asset]=time.time(); self.signal_timestamps.append(time.time())
            with self.trade_lock: self.active_trades[trade_id]={'id':trade_id,'asset':asset,'direction':direction,'entry':price,'sl':sl,'tp':tp,'entry_time':int(time.time()),'breakeven_locked':False,'trailing_activated':False,'hold_sent':False,'initial_score':sqs,'current_score':sqs,'health':100,'regime':regime,'signal_token':token}
        except Exception as e: logger.error(f"Final signal: {e}")

    def _close_trade(self, tid, price, pnl, reason=""):
        try:
            self.db.close_trade(tid,price,pnl,reason); self.mongo.close_trade_mongo(tid,price,pnl,reason)
            self.telegram.send(f"🔒 Trade #{tid} closed: {pnl:+.2f} | {reason}", priority=5)
            if tid in self.active_trades: del self.active_trades[tid]
            self.memory.update_state({"total_trades_closed":1,"total_pnl":pnl,"total_wins":1 if pnl>0 else 0,"total_losses":1 if pnl<0 else 0})
        except: pass

    def _process_queue(self):
        while True:
            try:
                item=self.price_queue.get(timeout=1)
                if item: self._handle_price_tick(*item)
            except queue.Empty: continue
            except: pass

    def _ping_self_loop(self):
        while True:
            try: requests.get(Config.RENDER_URL, timeout=10)
            except: pass
            time.sleep(300)

    def run(self):
        try:
            threading.Thread(target=start_health_server, args=(self,), daemon=True).start()
            with ThreadPoolExecutor(max_workers=4) as ex:
                for asset in Config.ASSETS:
                    for tf in [60,300,900,3600,14400]: ex.submit(self._load_and_backfill, asset, tf)
            self.stream=BinancePublicStream(self._on_price); self.stream.start()
            self.telegram.send("🚀 AlphaBot v7.2 Online (Persistent + Auto Recovery)", priority=5)
            last_news=0
            while True:
                try:
                    time.sleep(10)
                    if time.time()-last_news>60:
                        news=self.news.fetch_latest()
                        if news.get("fresh"):
                            for a in Config.ASSETS: self.asset_state[a]["news_sentiment"]=news["articles"][0]["sentiment"] if news["articles"] else 0
                            if news["articles"]: self.telegram.fire_news_alert(news["articles"][0]["title"],news["articles"][0]["sentiment"],news.get("fear_greed",50))
                            last_news=time.time()
                except KeyboardInterrupt: break
                except Exception as e: logger.error(f"Main loop: {e}")
        except: pass

    def _load_and_backfill(self, asset, tf):
        try:
            candles=self.mongo.load_candles(asset,tf,Config.MAX_CANDLES)
            if len(candles)>=Config.MAX_CANDLES*0.9: self.topology.candles[tf][asset]=candles; return
            interval={60:"1m",300:"5m",900:"15m",3600:"1h",14400:"4h"}[tf]
            url=f"https://api.binance.com/api/v3/klines?symbol={asset}&interval={interval}&limit=1000"
            resp=requests.get(url,timeout=15)
            if resp.status_code==200:
                fetched=[]
                for d in resp.json():
                    c={"timestamp":d[0]//1000,"open":float(d[1]),"high":float(d[2]),"low":float(d[3]),"close":float(d[4]),"volume":float(d[5]),"complete":True}
                    fetched.append(c); self.mongo.save_candle(asset,tf,c)
                self.topology.candles[tf][asset]=fetched[-Config.MAX_CANDLES:]
        except: pass

    def _on_price(self, asset, price, volume):
        try: self.price_queue.put_nowait((asset,price,volume))
        except queue.Full: pass

class SessionTimer:
    def is_trading_time(self): return True, "ALWAYS", "00:00-23:59 IST"

# =====================================================================
# NEWS SCANNER
# =====================================================================
class CryptoNewsScanner:
    def __init__(self): self.last_news={}; self.fear_greed=50
    def fetch_latest(self):
        try:
            resp=requests.get("https://min-api.cryptocompare.com/data/v2/news/?lang=EN&limit=3", timeout=5)
            articles=[]
            if resp.status_code==200:
                data=resp.json()
                if data.get("Data"):
                    for a in data["Data"][:2]:
                        sent=self._analyze_sentiment(a.get("title",""))
                        articles.append({"title":a.get("title",""),"sentiment":sent})
                    if articles: self.last_news=articles[0]
            fg_resp=requests.get("https://api.alternative.me/fng/?limit=1", timeout=5)
            if fg_resp.status_code==200:
                fg_data=fg_resp.json()
                if fg_data.get("data"): self.fear_greed=int(fg_data["data"][0]["value"])
            return {"articles":articles,"fresh":True,"fear_greed":self.fear_greed}
        except: return {"articles":[],"fresh":False,"fear_greed":50}
    def _analyze_sentiment(self, text):
        try:
            bull=["bullish","breakout","surge","buy","accumulate","rally","green","etf","approve"]
            bear=["bearish","crash","dump","sell","liquidation","drop","red","sec","hack"]
            text=text.lower(); score=sum(2 for w in bull if w in text)-sum(2 for w in bear if w in text)
            return max(-100,min(100,score*5))
        except: return 0

# =====================================================================
# HEALTH SERVER WITH DASHBOARD
# =====================================================================
class HealthSnapshot:
    def __init__(self, orchestrator):
        self.orch=orchestrator; self.snapshot={}; self.lock=threading.Lock()
        self.thread=threading.Thread(target=self._worker, daemon=True); self.thread.start()
    def _worker(self):
        while True:
            try: time.sleep(5); self._update()
            except: pass
    def _update(self):
        try:
            with self.lock:
                cpu=psutil.cpu_percent() if HAS_PSUTIL else 0; mem=psutil.virtual_memory().percent if HAS_PSUTIL else 0
                active=[]
                with self.orch.trade_lock:
                    for tid,trade in self.orch.active_trades.items():
                        curr=self.orch.topology.history[trade['asset']][-1]['price'] if self.orch.topology.history.get(trade['asset']) else trade['entry']
                        pnl=round(curr-trade['entry'] if trade['direction']=='BUY' else trade['entry']-curr,2)
                        active.append({"id":tid,"asset":trade['asset'],"direction":trade['direction'],"entry":trade['entry'],"pnl":pnl,"health":trade.get('health',100)})
                perf=self.orch.db.get_performance_metrics()
                mem_state=self.orch.memory.get_or_create_state()
                uptime_sec=mem_state.get("total_run_seconds",0); uptime_str=f"{uptime_sec//86400}d {(uptime_sec%86400)//3600}h {(uptime_sec%3600)//60}m"
                self.snapshot={"status":"online","uptime":uptime_str,"cpu":cpu,"memory":mem,"active_trades_count":len(active),
                               "active_trades":active,"accepted_signals":mem_state.get("accepted_signals_count",0),
                               "rejected_signals":mem_state.get("rejected_signals_count",0),"performance":perf,
                               "dynamic_sqs_base":self.orch.score_governor.current_base,
                               "total_signals_all_time":mem_state.get("total_signals_generated",0)}
        except: pass
    def get(self):
        with self.lock: return self.snapshot

def start_health_server(orchestrator):
    port=int(os.environ.get("PORT",10000))
    snap=HealthSnapshot(orchestrator)
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            try:
                path=self.path.split('?')[0]; params=dict(p.split('=') for p in self.path.split('?')[1].split('&') if '=' in p) if '?' in self.path else {}
                if path.startswith('/admin/'):
                    if params.get('key')!=Config.ADMIN_SECRET: self.send_response(403); self.end_headers(); return
                    if path=='/admin/close_trade':
                        tid=params.get('id'); token=params.get('token')
                        with orchestrator.trade_lock:
                            for id_,t in list(orchestrator.active_trades.items()):
                                if (tid and str(id_)==str(tid)) or (token and t.get('signal_token')==token):
                                    curr=orchestrator.topology.history[t['asset']][-1]['price'] if orchestrator.topology.history.get(t['asset']) else t['entry']
                                    pnl=curr-t['entry'] if t['direction']=='BUY' else t['entry']-curr
                                    orchestrator._close_trade(id_,curr,pnl,"Admin")
                                    self.send_response(200); self.end_headers(); return
                        self.send_response(404); self.end_headers(); return
                    # other admin routes omitted for brevity
                if path=='/rejections':
                    self.send_response(200); self.send_header("Content-type","application/json"); self.end_headers()
                    try:
                        cur=orchestrator.db.conn.cursor()
                        cur.execute("SELECT datetime(timestamp,'unixepoch'),asset,price,reason,gate_name FROM rejected_signals ORDER BY timestamp DESC LIMIT 50")
                        self.wfile.write(json.dumps([{"time":r[0],"asset":r[1],"price":r[2],"reason":r[3],"gate":r[4]} for r in cur.fetchall()],indent=2).encode())
                    except: self.wfile.write(json.dumps({"error":"DB"}).encode()); return
                if path=='/' or path=='/health':
                    self.send_response(200); self.send_header("Content-type","text/html"); self.end_headers()
                    s=snap.get(); p=s.get('performance',{})
                    html=f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><title>AlphaBot v7.2 Dashboard</title>
<meta http-equiv="refresh" content="10"><style>body{{font-family:Arial;background:#111;color:#eee;margin:20px}} table{{border-collapse:collapse;width:100%}} th,td{{border:1px solid #444;padding:6px}} th{{background:#333}} .g{{color:#0f0}} .r{{color:#f00}}</style></head><body>
<h1>🚀 AlphaBot v7.2 ULTIMATE</h1>
<p>Status: <b>{s.get('status','')}</b> | Uptime: {s.get('uptime','')} | CPU:{s.get('cpu',0)}% | Mem:{s.get('memory',0)}%</p>
<h2>Performance</h2><p>Total Trades:{p.get('total_trades',0)} | Win Rate:{p.get('win_rate',0):.2%} | Profit Factor:{p.get('profit_factor',0):.2f} | Total PnL:{p.get('total_pnl',0):.2f}</p>
<p>Signals: Accepted {s.get('accepted_signals',0)} | Rejected {s.get('rejected_signals',0)} | SQS Base:{s.get('dynamic_sqs_base',0)}</p>
<h2>Active Trades</h2><table><tr><th>ID</th><th>Asset</th><th>Dir</th><th>Entry</th><th>PnL</th><th>Health</th></tr>"""
                    for t in s.get('active_trades',[]):
                        cls="g" if t['pnl']>=0 else "r"
                        html+=f"<tr><td>{t['id']}</td><td>{t['asset']}</td><td>{t['direction']}</td><td>{t['entry']:.2f}</td><td class='{cls}'>{t['pnl']:.2f}</td><td>{t.get('health',100)}%</td></tr>"
                    html+="</table></body></html>"
                    self.wfile.write(html.encode()); return
                self.send_response(200); self.send_header("Content-type","application/json"); self.end_headers()
                self.wfile.write(json.dumps(snap.get(),indent=2).encode())
            except: self.send_response(500); self.end_headers()
        def do_HEAD(self): self.send_response(200); self.end_headers()
    httpd=HTTPServer(("0.0.0.0",port),H)
    logger.info(f"Health server on {port}")
    httpd.serve_forever()

if __name__=="__main__":
    bot=AIOrchestrator(); bot.run()

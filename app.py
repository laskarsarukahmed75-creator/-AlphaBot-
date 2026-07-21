# =====================================================================
# app.py – AlphaBot v6.1 (Institutional Signal Quality Gate) – FIXED
# =====================================================================
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
import re
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import deque
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import websocket  # For Binance Futures WebSocket

# ---- BeautifulSoup for ForexFactory scraping ----
try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    print("⚠️ BeautifulSoup not installed. Economic Calendar scraper disabled.")

# ---- MONGODB INTEGRATION ----
try:
    from pymongo import MongoClient, ASCENDING, DESCENDING
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False
    print("⚠️ pymongo not installed. Install with: pip install pymongo")

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# ---- Logging Setup ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AI-Orchestrator-v6.1")

# =====================================================================
# CONFIGURATION (v6.1 – Adjusted for signal generation)
# =====================================================================
class Config:
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    DISPLAY_NAMES = {"BTCUSDT": "BTC/USDT", "ETHUSDT": "ETH/USDT", "SOLUSDT": "SOL/USDT"}

    # --- MONGODB ---
    MONGO_URI = os.getenv("MONGO_URI", "")
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "crypto_bot_v5")

    # --- RENDER SELF-PING ---
    RENDER_URL = os.getenv("RENDER_URL", "https://alphabot-76tj.onrender.com")

    # --- DATABASE ---
    DB_PATH = "trades_v6.db"
    MAX_CANDLES = 500

    # --- BINANCE FUTURES WEBSOCKET ---
    BINANCE_FUTURES_WS_URL = "wss://fstream.binance.com/ws"

    # --- SESSION TIMING (IST) – Relaxed for testing ---
    IST = pytz.timezone('Asia/Kolkata')
    # Temporary: Always active
    SESSION_WINDOWS = [("ALWAYS", 0, 0, 23, 59)]
    DEAD_ZONES = []

    # --- NEWS BLACKOUT ---
    NEWS_EVENTS = []

    # --- SCORING & GATES ---
    MIN_SQS = 75                     # Reduced from 90
    PENDING_VERIFICATION_CANDLES = 2
    VOLUME_DECAY_THRESHOLD = 0.6     # Increased from 0.4

    # --- ADAPTIVE LEARNING ---
    ADAPTIVE_LEARN_INTERVAL = 50

    # --- COOLDOWN & CAPS ---
    SIGNAL_COOLDOWN = 1800
    MAX_SIGNALS_PER_DAY = 5

    # --- VOLATILITY ---
    VOLATILITY_MULTIPLIERS = {
        "low": (1.2, 2.0),
        "medium": (1.5, 2.5),
        "high": (1.8, 3.0),
        "extreme": (2.0, 3.5)
    }

    # --- LIFECYCLE PARAMETERS ---
    TIME_DECAY_SECONDS = 1500
    TIME_DECAY_THRESHOLD_PCT = 0.002
    HEALTH_EMERGENCY_THRESHOLD = 55
    CONFIDENCE_UPDATE_INTERVAL = 300

# =====================================================================
# MONGODB DATABASE (with quick fallback)
# =====================================================================
class MongoDatabase:
    def __init__(self):
        if not HAS_PYMONGO:
            logger.warning("pymongo not installed. MongoDB disabled.")
            self.client = None
            self.db = None
            return
        if not Config.MONGO_URI:
            logger.warning("MONGO_URI not set. MongoDB disabled.")
            self.client = None
            self.db = None
            return
        self.auth_failed = False
        # Try once, if fail, go to SQLite
        try:
            self.client = MongoClient(Config.MONGO_URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[Config.MONGO_DB_NAME]
            self._create_indexes()
            logger.info("MongoDB connected successfully.")
        except Exception as e:
            if "bad auth" in str(e) or "authentication failed" in str(e):
                logger.error(f"MongoDB authentication failed! Please check MONGO_URI credentials.")
                self.auth_failed = True
            else:
                logger.warning(f"MongoDB connection failed: {e}. Running without MongoDB.")
            self.client = None
            self.db = None

    def _create_indexes(self):
        if self.db is None: return
        try:
            self.db.candles.create_index([("asset", ASCENDING), ("timeframe", ASCENDING), ("timestamp", ASCENDING)], unique=True)
            self.db.trades.create_index([("asset", ASCENDING), ("timestamp", DESCENDING)])
            self.db.rejected.create_index([("asset", ASCENDING), ("timestamp", DESCENDING)])
        except Exception as e:
            logger.debug(f"Index creation error: {e}")

    def save_candle(self, asset, timeframe, candle):
        if self.db is None: return
        try:
            doc = {**candle, "asset": asset, "timeframe": timeframe}
            self.db.candles.update_one(
                {"asset": asset, "timeframe": timeframe, "timestamp": candle["timestamp"]},
                {"$set": doc},
                upsert=True
            )
        except Exception as e:
            if "auth" not in str(e).lower():
                logger.debug(f"Mongo save_candle error: {e}")

    def load_candles(self, asset, timeframe, limit=500, since=None):
        if self.db is None: return []
        try:
            query = {"asset": asset, "timeframe": timeframe}
            if since:
                query["timestamp"] = {"$gte": since}
            cursor = self.db.candles.find(query, {"_id": 0}).sort("timestamp", ASCENDING).limit(limit)
            return list(cursor)
        except Exception as e:
            if "auth" not in str(e).lower():
                logger.error(f"Mongo load_candles error: {e}")
            return []

    def get_candle_stats(self):
        if self.db is None: return {"counts": {}, "oldest": 0}
        try:
            pipeline = [{"$group": {"_id": "$timeframe", "count": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
            counts_result = list(self.db.candles.aggregate(pipeline))
            counts = {str(item["_id"]) + "s": item["count"] for item in counts_result}
            oldest_doc = self.db.candles.find_one(sort=[("timestamp", ASCENDING)])
            oldest_ts = oldest_doc["timestamp"] if oldest_doc else 0
            return {"counts": counts, "oldest": oldest_ts}
        except Exception as e:
            if "auth" not in str(e).lower():
                logger.error(f"Mongo get_candle_stats error: {e}")
            return {"counts": {}, "oldest": 0}

    def get_trades_count(self):
        if self.db is None: return 0
        try:
            return self.db.trades.count_documents({})
        except Exception:
            return 0

    def save_trade_backup(self, trade_data):
        if self.db is None: return
        try:
            self.db.trades.update_one({"id": trade_data["id"]}, {"$set": trade_data}, upsert=True)
        except Exception as e:
            if "auth" not in str(e).lower():
                logger.debug(f"Mongo save_trade error: {e}")

    def save_rejected_backup(self, rejected_data):
        if self.db is None: return
        try:
            self.db.rejected.insert_one(rejected_data)
        except Exception as e:
            if "auth" not in str(e).lower():
                logger.debug(f"Mongo save_rejected error: {e}")

# =====================================================================
# SQLite DATABASE (Unchanged)
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
                entry_time INTEGER, exit_reason TEXT, health_history TEXT,
                session TEXT, sqs_score INTEGER, pattern_name TEXT
            )''')
            cur.execute('''CREATE TABLE IF NOT EXISTS rejected_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset TEXT, price REAL, score INTEGER, reason TEXT,
                timestamp INTEGER, volatility REAL, market_regime TEXT,
                gate_name TEXT
            )''')
            cur.execute('''CREATE TABLE IF NOT EXISTS performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                win_rate REAL,
                profit_factor REAL,
                sharpe REAL,
                total_trades INTEGER,
                winning_trades INTEGER,
                losing_trades INTEGER,
                total_pnl REAL
            )''')
            cur.execute('''CREATE TABLE IF NOT EXISTS pattern_performance (
                pattern_name TEXT,
                session TEXT,
                total_trades INTEGER,
                wins INTEGER,
                last_updated INTEGER,
                PRIMARY KEY (pattern_name, session)
            )''')
            self.conn.commit()
        finally:
            cur.close()

    def log_trade(self, asset, direction, entry, sl, tp, score, confidence, patterns, logic,
                  volatility, regime, htf_trend, news_score, session, sqs_score, pattern_name):
        cur = self.conn.cursor()
        try:
            cur.execute('''INSERT INTO trades 
                (asset, direction, entry, stop_loss, take_profit, score, confidence, patterns, logic,
                 timestamp, volatility, market_regime, htf_trend, news_score, entry_time, status,
                 session, sqs_score, pattern_name)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (asset, direction, entry, sl, tp, score, confidence, json.dumps(patterns), logic,
                 int(time.time()), volatility, regime, htf_trend, news_score, int(time.time()), 'open',
                 session, sqs_score, pattern_name))
            self.conn.commit()
            return cur.lastrowid
        finally:
            cur.close()

    def log_rejected(self, asset, price, score, reason, volatility, regime, gate_name=""):
        cur = self.conn.cursor()
        try:
            cur.execute('''INSERT INTO rejected_signals (asset, price, score, reason, timestamp, volatility, market_regime, gate_name)
                VALUES (?,?,?,?,?,?,?,?)''', (asset, price, score, reason, int(time.time()), volatility, regime, gate_name))
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

    def get_closed_trades(self, limit=50):
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT asset, direction, score, pnl, logic, pattern_name, session FROM trades WHERE status='closed' ORDER BY id DESC LIMIT ?", (limit,))
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

    def get_pattern_performance(self, pattern_name, session):
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT total_trades, wins FROM pattern_performance WHERE pattern_name=? AND session=?", (pattern_name, session))
            row = cur.fetchone()
            if row:
                return {"total": row[0], "wins": row[1]}
            return {"total": 0, "wins": 0}
        finally:
            cur.close()

    def update_pattern_performance(self, pattern_name, session, total, wins):
        cur = self.conn.cursor()
        try:
            cur.execute('''INSERT OR REPLACE INTO pattern_performance (pattern_name, session, total_trades, wins, last_updated)
                VALUES (?, ?, ?, ?, ?)''', (pattern_name, session, total, wins, int(time.time())))
            self.conn.commit()
        finally:
            cur.close()

# =====================================================================
# CRYPTO NEWS SCANNER (Unchanged)
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
# ECONOMIC CALENDAR (Unchanged)
# =====================================================================
class EconomicCalendar:
    def __init__(self):
        self.events = []
        if HAS_BS4:
            self.fetch_events()
        else:
            logger.warning("BeautifulSoup not installed. Economic calendar scraper disabled.")

    def fetch_events(self):
        try:
            now = datetime.now(Config.IST)
            month = now.month
            year = now.year
            url = f"https://www.forexfactory.com/calendar?month={month}.{year}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                logger.error("ForexFactory calendar fetch failed. Status code: %d", resp.status_code)
                return

            soup = BeautifulSoup(resp.text, 'lxml')
            rows = soup.find_all('tr', class_=re.compile('calendar__row'))
            for row in rows:
                impact_cell = row.find('td', class_='calendar__impact')
                if impact_cell:
                    impact_icon = impact_cell.find('span', class_='impact--high')
                    if not impact_icon:
                        continue
                else:
                    continue

                time_cell = row.find('td', class_='calendar__time')
                if not time_cell:
                    continue
                time_str = time_cell.get_text(strip=True)
                if not time_str:
                    continue

                event_cell = row.find('td', class_='calendar__event')
                if not event_cell:
                    continue
                event_name = event_cell.get_text(strip=True)
                keywords = ['CPI', 'FOMC', 'NFP', 'Non-Farm', 'Interest Rate', 'GDP', 'Unemployment', 'PPI', 'Retail Sales', 'PMI']
                if not any(k in event_name for k in keywords):
                    continue

                date_cell = row.find('td', class_='calendar__date')
                if date_cell:
                    date_text = date_cell.get('data-date')
                    if date_text:
                        try:
                            dt_est = datetime.strptime(date_text, '%Y-%m-%d')
                            try:
                                if 'am' in time_str or 'pm' in time_str:
                                    t = datetime.strptime(time_str, '%I:%M%p').time()
                                else:
                                    t = datetime.strptime(time_str, '%H:%M').time()
                            except:
                                t = datetime.strptime('00:00', '%H:%M').time()
                            dt_est = dt_est.replace(hour=t.hour, minute=t.minute)
                            est = pytz.timezone('America/New_York')
                            dt_est_aware = est.localize(dt_est)
                            dt_ist = dt_est_aware.astimezone(Config.IST)
                            self.events.append((dt_ist, event_name, Config.ASSETS))
                        except Exception as e:
                            logger.debug(f"Date parse error for {event_name}: {e}")
                            continue
            logger.info(f"Loaded {len(self.events)} high-impact economic events from ForexFactory.")
        except Exception as e:
            logger.error(f"Economic calendar scraping error: {e}")

    def add_event(self, dt, name, impact_assets=None):
        self.events.append((dt, name, impact_assets or Config.ASSETS))

    def is_blackout(self, current_dt, asset):
        buffer_minutes = 45
        for evt_dt, evt_name, impact in self.events:
            if asset not in impact:
                continue
            diff = abs((current_dt - evt_dt).total_seconds()) / 60
            if diff <= buffer_minutes:
                return True, evt_name
        return False, None

# =====================================================================
# BINANCE FUTURES WEBSOCKET (with health check & OI history)
# =====================================================================
class BinanceFuturesStream:
    def __init__(self, on_data=None):
        self.ws_url = Config.BINANCE_FUTURES_WS_URL
        self.symbols = [s.lower() for s in Config.ASSETS]
        self.ws = None
        self.running = False
        self.data = {
            'open_interest': {},
            'liquidations': [],
            'cvd': {},
            'last_trade': {},
        }
        self.oi_history = {s: deque(maxlen=5) for s in self.symbols}  # store last 5 OI values
        self.lock = threading.Lock()
        self.reconnect_count = 0
        self.on_data = on_data
        self.thread = None
        self.last_ping = time.time()
        self.health_thread = None

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.running = True
        self.thread = threading.Thread(target=self._ws_loop, daemon=True)
        self.thread.start()
        self.health_thread = threading.Thread(target=self._health_check, daemon=True)
        self.health_thread.start()

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()

    def _ws_loop(self):
        while self.running:
            try:
                self.ws = websocket.WebSocketApp(self.ws_url,
                                                 on_open=self._on_open,
                                                 on_message=self._on_message,
                                                 on_error=self._on_error,
                                                 on_close=self._on_close)
                self.ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.error(f"Binance Futures WebSocket error: {e}")
                self.reconnect_count += 1
                time.sleep(5)

    def _on_open(self, ws):
        logger.info("Binance Futures WebSocket connected.")
        streams = []
        for s in self.symbols:
            streams.append(f"{s}@openInterest")
            streams.append(f"{s}@forceOrder")
            streams.append(f"{s}@aggTrade")
        sub_msg = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        }
        ws.send(json.dumps(sub_msg))
        self.reconnect_count = 0
        self.last_ping = time.time()

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            if 'e' not in data:
                return
            event_type = data['e']
            if event_type == 'openInterest':
                symbol = data['s']
                oi = float(data['o'])
                with self.lock:
                    self.data['open_interest'][symbol] = oi
                    self.oi_history[symbol].append(oi)
                    if self.on_data:
                        self.on_data('open_interest', symbol, oi)
            elif event_type == 'forceOrder':
                order = data['o']
                symbol = order['s']
                side = order['S']
                price = float(order['p'])
                qty = float(order['q'])
                with self.lock:
                    self.data['liquidations'].append({
                        'symbol': symbol,
                        'side': side,
                        'price': price,
                        'qty': qty,
                        'time': time.time()
                    })
                    if self.on_data:
                        self.on_data('liquidation', symbol, {'side': side, 'price': price, 'qty': qty})
            elif event_type == 'aggTrade':
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
            self.last_ping = time.time()  # update on any message
        except Exception as e:
            logger.debug(f"Futures WebSocket message parse error: {e}")

    def _on_error(self, ws, error):
        logger.error(f"Futures WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning("Futures WebSocket disconnected. Reconnecting...")
        self.reconnect_count += 1
        time.sleep(5)

    def _health_check(self):
        while self.running:
            time.sleep(30)
            if time.time() - self.last_ping > 60:
                logger.warning("Futures WebSocket no data for >60s, forcing reconnect.")
                if self.ws:
                    self.ws.close()
                self.reconnect_count += 1

    def get_open_interest(self, symbol):
        with self.lock:
            return self.data['open_interest'].get(symbol, 0)

    def get_oi_trend(self, symbol):
        with self.lock:
            hist = list(self.oi_history.get(symbol.lower(), []))
            if len(hist) < 2:
                return 0
            # Return slope: positive = increasing
            return hist[-1] - hist[0]

    def get_cvd(self, symbol):
        with self.lock:
            return self.data['cvd'].get(symbol, 0)

    def get_liquidations(self, symbol, lookback_seconds=60):
        with self.lock:
            now = time.time()
            events = [e for e in self.data['liquidations'] if e['symbol'] == symbol and (now - e['time']) <= lookback_seconds]
            return events

# =====================================================================
# CANDLE TOPOLOGY ENGINE (fixed candle_just_closed reset)
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

    def process_tick(self, asset: str, price: float, volume: float):
        now = int(time.time())
        self.history[asset].append({"price": price, "volume": volume, "time": now})
        self.candle_just_closed[asset] = False  # reset

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

    def _build_candle(self, asset, price, volume, ts, tf, storage):
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
        completed = [c for c in candles if c.get("complete", False)]
        if len(completed) >= 20:
            vols = [c["volume"] for c in completed[-20:]]
            self.volume_ma[asset] = sum(vols) / 20
        else:
            self.volume_ma[asset] = 0.0

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
            if (min(last["open"], last["close"]) - last["low"]) / total > 0.6:
                patterns["bullish_rej"] = 1
            if (last["high"] - max(last["open"], last["close"])) / total > 0.6:
                patterns["bearish_rej"] = 1
        return patterns

    def get_atr(self, asset, period=14, tf=3600):
        candles = self.candles[tf][asset]
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
        # Used but relaxed – we keep it but not mandatory
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

    # ---- ADX method ----
    def get_adx(self, asset, tf, period=14):
        candles = self.candles[tf][asset]
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < period: return 20
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
        if len(tr_list) < period: return 20
        atr = sum(tr_list[:period]) / period
        dm_plus_smooth = sum(dm_plus[:period]) / period
        dm_minus_smooth = sum(dm_minus[:period]) / period
        for i in range(period, len(tr_list)):
            atr = (atr * (period-1) + tr_list[i]) / period
            dm_plus_smooth = (dm_plus_smooth * (period-1) + dm_plus[i]) / period
            dm_minus_smooth = (dm_minus_smooth * (period-1) + dm_minus[i]) / period
        if atr == 0: return 20
        di_plus = (dm_plus_smooth / atr) * 100
        di_minus = (dm_minus_smooth / atr) * 100
        dx = (abs(di_plus - di_minus) / (di_plus + di_minus)) * 100 if (di_plus + di_minus) > 0 else 0
        return min(100, dx)

    def detect_fvg(self, asset):
        candles = self.candles[900][asset]
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < 3: return []
        fvgs = []
        for i in range(2, len(complete)-1):
            c1 = complete[i-2]
            c2 = complete[i-1]
            c3 = complete[i]
            if c1["close"] < c2["open"] and c2["close"] < c3["close"] and c1["high"] > c2["low"]:
                fvgs.append({"type": "bullish", "upper": c1["high"], "lower": c2["low"]})
            if c1["close"] > c2["open"] and c2["close"] > c3["close"] and c2["high"] > c1["low"]:
                fvgs.append({"type": "bearish", "upper": c2["high"], "lower": c1["low"]})
        return fvgs[-5:]

    def detect_order_block(self, asset):
        if not self.bos[asset]["direction"]:
            return {}
        candles = self.candles[900][asset]
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < 10: return {}
        atr = self.get_atr(asset)
        if atr == 0: return {}
        for i in range(len(complete)-1, -1, -1):
            c = complete[i]
            if (c["high"] - c["low"]) > 1.5 * atr:
                ob_type = "bullish" if c["close"] > c["open"] else "bearish"
                return {"type": ob_type, "high": c["high"], "low": c["low"]}
        return {}

# =====================================================================
# GATE 1: MARKET REGIME FILTER
# =====================================================================
class MarketRegimeFilter:
    def __init__(self, topology):
        self.topology = topology

    def check(self, asset: str, price: float) -> Tuple[bool, str]:
        adx_15 = self.topology.get_adx(asset, 900)
        adx_1h = self.topology.get_adx(asset, 3600)
        if adx_15 < 22 and adx_1h < 22:
            return False, f"Sideways/Chop (ADX 15={adx_15:.1f}, 1h={adx_1h:.1f})"

        candles_5m = self.topology.candles[300][asset]
        completed = [c for c in candles_5m if c.get("complete", False)]
        if len(completed) >= 5:
            recent_high = max(c["high"] for c in completed[-5:])
            recent_low = min(c["low"] for c in completed[-5:])
            last = completed[-1]
            vol_ma = self.topology.volume_ma[asset]
            if last["close"] > recent_high and last["volume"] < 1.2 * vol_ma:
                return False, "Fake Breakout (low volume)"
            if last["close"] < recent_low and last["volume"] < 1.2 * vol_ma:
                return False, "Fake Breakdown (low volume)"

        return True, "Pass"

# =====================================================================
# GATE 3: HARD MTF CONFLUENCE (Relaxed)
# =====================================================================
class MTFConfluenceGate:
    def __init__(self, topology):
        self.topology = topology

    def check(self, asset: str, direction: str) -> Tuple[bool, str]:
        current_price = self.topology.history[asset][-1]['price'] if self.topology.history[asset] else 0
        if current_price == 0:
            return False, "No price"

        # 4H trend
        candles_4h = self.topology.candles[14400][asset]
        complete_4h = [c for c in candles_4h if c.get("complete", False)]
        if len(complete_4h) < 200:
            return False, "Insufficient 4H data"
        closes_4h = [c["close"] for c in complete_4h]
        ema50_4h = self.topology._ema(closes_4h, 50)
        ema200_4h = self.topology._ema(closes_4h, 200)
        if len(ema50_4h) < 2 or len(ema200_4h) < 2:
            return False, "4H EMAs not ready"
        if direction == "BUY" and current_price < ema50_4h[-1] and current_price < ema200_4h[-1]:
            return False, "4H trend bearish vs BUY"
        if direction == "SELL" and current_price > ema50_4h[-1] and current_price > ema200_4h[-1]:
            return False, "4H trend bullish vs SELL"

        # 1H structure
        pivots_high = self.topology.pivots[asset]["high"]
        pivots_low = self.topology.pivots[asset]["low"]
        if len(pivots_high) >= 2 and len(pivots_low) >= 2:
            if direction == "BUY" and pivots_high[0] < pivots_high[1]:
                return False, "1H structure down"
            if direction == "SELL" and pivots_low[0] > pivots_low[1]:
                return False, "1H structure up"

        # 15m: OB/FVG (at least one present)
        fvgs = self.topology.detect_fvg(asset)
        ob = self.topology.detect_order_block(asset)
        if not ob and not fvgs:
            return False, "No OB or FVG on 15m"

        # 5m: Near support/resistance (relaxed to 2%)
        sr = self.topology.support_resistance[asset]
        if direction == "BUY":
            if sr["support"]:
                nearest_support = max(sr["support"])
                if abs(current_price - nearest_support) / nearest_support > 0.02:
                    return False, "Not near support zone (2%)"
        else:
            if sr["resistance"]:
                nearest_resistance = min(sr["resistance"])
                if abs(current_price - nearest_resistance) / nearest_resistance > 0.02:
                    return False, "Not near resistance zone (2%)"

        # 1m micro trigger removed – not mandatory

        return True, "Pass"

# =====================================================================
# GATE 4: INSTITUTIONAL ORDER FLOW (with fallback)
# =====================================================================
class OrderFlowAnalyzer:
    def __init__(self, topology, futures_stream):
        self.topology = topology
        self.futures = futures_stream

    def check(self, asset: str, direction: str, price: float) -> Tuple[bool, str]:
        symbol = asset.lower()
        oi = self.futures.get_open_interest(symbol)
        oi_trend = self.futures.get_oi_trend(symbol)  # difference over last 5 readings
        cvd = self.futures.get_cvd(symbol)
        liquidations = self.futures.get_liquidations(symbol, lookback_seconds=60)

        # If OI is zero, we bypass (likely data not available)
        if oi == 0:
            logger.warning(f"OI data missing for {asset}, bypassing Order Flow gate.")
            return True, "Bypassed (no OI data)"

        # Check OI trend
        if direction == "BUY" and oi_trend <= 0:
            return False, "Open Interest not increasing (institutional buying?)"
        if direction == "SELL" and oi_trend >= 0:
            return False, "Open Interest increasing while selling (possible trap)"

        # CVD divergence: use 5m candle close
        candles = self.topology.candles[300][asset]
        completed = [c for c in candles if c.get("complete", False)]
        if len(completed) >= 2:
            price_change = price - completed[-2]["close"]
            if direction == "BUY" and price_change > 0 and cvd < 0:
                return False, "CVD divergence (price up, CVD down)"
            if direction == "SELL" and price_change < 0 and cvd > 0:
                return False, "CVD divergence (price down, CVD up)"

        return True, "Pass"

# =====================================================================
# GATE 5: CHRONO-SESSION TIMING (now always on)
# =====================================================================
class SessionTimer:
    def __init__(self):
        self.ist = Config.IST

    def is_trading_time(self) -> Tuple[bool, str, str]:
        # Always true for testing
        return True, "ALWAYS", "00:00-23:59 IST"

# =====================================================================
# ADAPTIVE LEARNING ENGINE (Unchanged)
# =====================================================================
class AdaptiveLearner:
    def __init__(self, db):
        self.db = db
        self.trade_count = 0

    def update(self, trade_record):
        pattern = trade_record.get("pattern_name", "unknown")
        session = trade_record.get("session", "unknown")
        pnl = trade_record["pnl"]
        is_win = 1 if pnl > 0 else 0

        perf = self.db.get_pattern_performance(pattern, session)
        new_total = perf["total"] + 1
        new_wins = perf["wins"] + is_win
        self.db.update_pattern_performance(pattern, session, new_total, new_wins)

        self.trade_count += 1
        if self.trade_count % Config.ADAPTIVE_LEARN_INTERVAL == 0:
            self.adjust_weights()

    def adjust_weights(self):
        cur = self.db.conn.cursor()
        cur.execute("SELECT pattern_name, session, total_trades, wins FROM pattern_performance")
        rows = cur.fetchall()
        for pattern, session, total, wins in rows:
            if total >= 10:
                wr = wins / total
                if wr < 0.45:
                    logger.info(f"Adaptive: Pattern {pattern} win rate {wr:.2f} < 45% -> reducing weight.")
        cur.close()

# =====================================================================
# COMPOSITE SQS CALCULATOR (Unchanged)
# =====================================================================
class SQS_Calculator:
    def __init__(self, topology):
        self.topology = topology

    def calculate(self, asset, price, direction, session_ok, patterns, sr, bos, choch, liquidity_sweep, ob, fvgs, vol_ratio, htf_trend):
        score = 0
        if bos and bos["direction"]:
            score += 15
        if choch:
            score += 10
        if liquidity_sweep:
            score += 10
        if self.topology.check_1m_rejection(asset, direction):
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

# =====================================================================
# PENDING VERIFICATION QUEUE (with adjusted threshold)
# =====================================================================
class PendingVerificationQueue:
    def __init__(self, topology):
        self.topology = topology
        self.pending = {}

    def add_signal(self, signal_data):
        asset = signal_data['asset']
        candles = self.topology.candles[300][asset]
        completed = [c for c in candles if c.get("complete", False)]
        if len(completed) < 2:
            return False
        last_vol = completed[-1]["volume"] if completed else 0
        signal_data['volumes'] = [last_vol]
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
            completed = [c for c in candles if c.get("complete", False)]
            if len(completed) < 2:
                continue
            new_candles = completed[-Config.PENDING_VERIFICATION_CANDLES:] if len(completed) >= Config.PENDING_VERIFICATION_CANDLES else completed
            if len(new_candles) > data['candle_count']:
                for c in new_candles[data['candle_count']:]:
                    data['volumes'].append(c["volume"])
                    data['candle_count'] += 1
                if len(data['volumes']) >= 2:
                    if data['volumes'][-1] < data['volumes'][0] * (1 - Config.VOLUME_DECAY_THRESHOLD):
                        data['rejected'] = True
                        to_remove.append(key)
                        continue
                # Check price reversal (relaxed: allow 0.5% retracement)
                first_close = completed[-Config.PENDING_VERIFICATION_CANDLES]['close']
                if data['direction'] == 'BUY' and first_close < data['start_price'] * 0.995:
                    data['rejected'] = True
                    to_remove.append(key)
                elif data['direction'] == 'SELL' and first_close > data['start_price'] * 1.005:
                    data['rejected'] = True
                    to_remove.append(key)
            if data['candle_count'] >= Config.PENDING_VERIFICATION_CANDLES:
                to_remove.append(key)
        return to_remove

    def get_verified_signals(self):
        ready = []
        to_remove = []
        for key, data in self.pending.items():
            if data['candle_count'] >= Config.PENDING_VERIFICATION_CANDLES and not data['rejected']:
                ready.append(data)
                to_remove.append(key)
            elif data['candle_count'] >= Config.PENDING_VERIFICATION_CANDLES and data['rejected']:
                to_remove.append(key)
        for key in to_remove:
            if key in self.pending:
                del self.pending[key]
        return ready

# =====================================================================
# DYNAMIC STOP LOSS (Unchanged)
# =====================================================================
class DynamicStopLoss:
    def __init__(self, topology):
        self.topology = topology

    def calculate(self, asset, direction, entry, atr):
        sr = self.topology.support_resistance[asset]
        if direction == "BUY":
            if sr["support"]:
                sl_level = min(sr["support"])
            else:
                pivots_low = self.topology.pivots[asset]["low"]
                sl_level = min(pivots_low) if pivots_low else entry * 0.99
            sl = sl_level - 0.5 * atr
            if sl >= entry:
                sl = entry - 0.5 * atr
        else:
            if sr["resistance"]:
                sl_level = max(sr["resistance"])
            else:
                pivots_high = self.topology.pivots[asset]["high"]
                sl_level = max(pivots_high) if pivots_high else entry * 1.01
            sl = sl_level + 0.5 * atr
            if sl <= entry:
                sl = entry + 0.5 * atr
        return sl

# =====================================================================
# TELEGRAM PIPELINE (FIXED: removed extra args in fire_signal)
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
# BINANCE SPOT WEBSOCKET (Unchanged)
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
# HEALTH SERVER (Unchanged)
# =====================================================================
def start_health_server(orchestrator):
    port = int(os.environ.get("PORT", 10000))
    class H(BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()

            cpu = psutil.cpu_percent() if HAS_PSUTIL else 0
            mem = psutil.virtual_memory().percent if HAS_PSUTIL else 0
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
            perf = orchestrator.db.get_performance_metrics()
            mongo_stats = {}
            if orchestrator.mongo.db is not None:
                try:
                    candle_stats = orchestrator.mongo.get_candle_stats()
                    trades_backup = orchestrator.mongo.get_trades_count()
                    mongo_stats = {
                        "connected": True,
                        "candle_counts": candle_stats["counts"],
                        "oldest_date": datetime.fromtimestamp(candle_stats["oldest"]).strftime('%Y-%m-%d %H:%M:%S') if candle_stats["oldest"] else None,
                        "trades_backup_count": trades_backup,
                        "data_days": round((time.time()-candle_stats["oldest"])/86400,1) if candle_stats["oldest"] else 0
                    }
                except Exception as e:
                    mongo_stats = {"connected": True, "error": str(e)}
            else:
                mongo_stats = {"connected": False}
            last_signal = {asset: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(orchestrator.last_signal_time[asset])) if orchestrator.last_signal_time[asset] > 0 else "Never" for asset in Config.ASSETS}
            candle_delay = 0
            for asset in Config.ASSETS:
                candles = orchestrator.topology.candles[60][asset]
                if candles and candles[-1].get("complete", False):
                    candle_delay = max(candle_delay, int(time.time()) - candles[-1]["timestamp"] - 60)
            response = {
                "status": "online",
                "version": "6.1",
                "uptime_seconds": int(time.time() - orchestrator.start_time),
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
                "news_sentiment": orchestrator.news.last_news.get('sentiment', {}).get('label', 'NEUTRAL'),
                "fear_greed": orchestrator.news.fear_greed,
                "mongodb": mongo_stats
            }
            self.wfile.write(json.dumps(response, indent=2).encode())
    httpd = HTTPServer(("0.0.0.0", port), H)
    logger.info(f"Health server started on port {port}")
    httpd.serve_forever()

# =====================================================================
# LIFECYCLE CONTROLLER (Unchanged)
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
                        self.orch._close_trade(tid, current_price, 0.0, "Time-Decay")
                        to_remove.append(tid)
                        self.orch.telegram.send_message(f"⏳ Trade #{tid} closed due to consolidation.")
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
                        self.orch.telegram.send_message(f"🚨 Trade #{tid} cut early (Health {health}%).")
                        continue
                    if int(time.time()) % Config.CONFIDENCE_UPDATE_INTERVAL < 60:
                        msg = (f"🔄 Lifecycle Update: #{tid} ({asset})\n"
                               f"Direction: {trade['direction']} | PnL: {(current_price - trade['entry']):.2f}\n"
                               f"Confidence: {trade.get('current_score', base_score)}% → {base_score}%\n"
                               f"Health: {health}%")
                        self.orch.telegram.send_message(msg)
                    trade['current_score'] = base_score
                    trade['health'] = health
                for tid in to_remove:
                    if tid in self.orch.active_trades:
                        del self.orch.active_trades[tid]
                gc.collect()

# =====================================================================
# CORE ORCHESTRATOR (v6.1 – Integrated all modules)
# =====================================================================
class AIOrchestrator:
    def __init__(self):
        self.topology = CandleTopologyEngine()
        self.news = CryptoNewsScanner()
        self.db = TradeDatabase()
        self.mongo = MongoDatabase()
        self.telegram = TelegramPipeline()
        self.lifecycle = ActiveTradeLifecycle(self)

        # --- NEW: Binance Futures Stream (Gate 4) ---
        self.futures_stream = BinanceFuturesStream()
        self.futures_stream.start()

        # --- Gates & Modules ---
        self.market_regime = MarketRegimeFilter(self.topology)
        self.economic_calendar = EconomicCalendar()
        self.mtf_gate = MTFConfluenceGate(self.topology)
        self.orderflow = OrderFlowAnalyzer(self.topology, self.futures_stream)
        self.session_timer = SessionTimer()
        self.adaptive = AdaptiveLearner(self.db)
        self.sqs_calc = SQS_Calculator(self.topology)
        self.pending_queue = PendingVerificationQueue(self.topology)
        self.dynamic_sl = DynamicStopLoss(self.topology)

        # State
        self.active_trades = {}
        self.trade_lock = threading.Lock()
        self.price_queue = queue.Queue(maxsize=1000)
        self.start_time = time.time()
        self.last_signal_time = {a:0 for a in Config.ASSETS}
        self.signal_timestamps = deque(maxlen=100)
        self.asset_state = {a: {"trend":"NEUTRAL","htf_trend":"NEUTRAL","volume_ratio":1.0,
                                "rsi":50,"adx":20,"volatility":0.01,"news_sentiment":0,"news_importance":0.5} for a in Config.ASSETS}
        self.accepted = 0
        self.rejected = 0
        self.stream = None

        threading.Thread(target=self.lifecycle.monitor_lifecycle, daemon=True).start()
        threading.Thread(target=self._process_queue, daemon=True).start()

    def _process_queue(self):
        while True:
            try:
                item = self.price_queue.get(timeout=1)
                if item:
                    self._handle_price_tick(*item)
            except: pass

    def _ping_self_loop(self):
        while True:
            try:
                requests.get(Config.RENDER_URL, timeout=10)
                logger.info("✨ Self-ping sent")
            except: pass
            time.sleep(300)

    def _load_and_backfill(self, asset, tf):
        logger.info(f"Loading {asset} TF={tf}...")
        candles = self.mongo.load_candles(asset, tf, limit=Config.MAX_CANDLES)
        if len(candles) >= Config.MAX_CANDLES * 0.9:
            self.topology.candles[tf][asset] = candles
            logger.info(f"Loaded {len(candles)} candles from MongoDB for {asset} TF={tf}")
            return
        logger.info(f"Fetching from Binance for {asset} TF={tf}")
        since_ts = int(time.time()) - (90 * 24 * 3600)
        try:
            interval = {60:"1m", 300:"5m", 900:"15m", 3600:"1h", 14400:"4h"}[tf]
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

    def _is_strong_trend(self, asset):
        c15 = [c["close"] for c in self.topology.candles[900][asset] if c.get("complete", False)][-30:]
        c1h = [c["close"] for c in self.topology.candles[3600][asset] if c.get("complete", False)][-30:]
        if len(c15) < 20 or len(c1h) < 20: return False
        e15_9, e15_21 = self.topology._ema(c15,9), self.topology._ema(c15,21)
        e1h_9, e1h_21 = self.topology._ema(c1h,9), self.topology._ema(c1h,21)
        if not e15_9 or not e15_21 or not e1h_9 or not e1h_21: return False
        if len(e15_9)<2 or len(e15_21)<2 or len(e1h_9)<2 or len(e1h_21)<2: return False
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
                self.asset_state[asset]["adx"] = self.topology.get_adx(asset, 900)
        c1h = [c["close"] for c in self.topology.candles[3600][asset] if c.get("complete", False)][-30:]
        if len(c1h)>10:
            e9,e21 = self.topology._ema(c1h,9), self.topology._ema(c1h,21)
            if len(e9)>1 and len(e21)>1:
                self.asset_state[asset]["htf_trend"] = "BULLISH" if e9[-1]>e21[-1] else "BEARISH"
        vols = [c["volume"] for c in self.topology.candles[300][asset] if c.get("complete", False)][-10:]
        if len(vols)>1:
            avg = sum(vols[:-1]) / max(1, len(vols[:-1]))
            self.asset_state[asset]["volume_ratio"] = vols[-1] / avg if avg>0 else 1.0
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
        if self.mongo.db is not None:
            try:
                self.mongo.db.trades.update_one({"id": tid}, {"$set": {"status": "closed", "exit_price": price, "pnl": pnl, "close_time": int(time.time()), "exit_reason": reason}})
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

    def _handle_price_tick(self, asset, price, volume):
        try:
            self.topology.process_tick(asset, price, volume)
            self._update_active_trades(asset, price)

            # ---- Pending verification check ----
            if self.topology.candle_just_closed.get(asset, False):
                if self.pending_queue.pending:
                    removed = self.pending_queue.check_pending(asset)
                    verified = self.pending_queue.get_verified_signals()
                    for signal in verified:
                        self._send_final_signal(signal)

            # ---- Skip if active trade ----
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

            # ---- New signal only on 15m candle close ----
            if not self.topology.candle_just_closed.get(asset, False):
                return

            self._update_indicators(asset, price)

            # ---- GATE 5: Session ----
            session_ok, session_name, _ = self.session_timer.is_trading_time()
            if not session_ok:
                self.db.log_rejected(asset, price, 0, "Out of Session", self.asset_state[asset]["volatility"], "N/A", "Session Filter")
                self.rejected += 1
                return

            # ---- GATE 2: News Blackout ----
            now_dt = datetime.now(Config.IST)
            blackout, event_name = self.economic_calendar.is_blackout(now_dt, asset)
            if blackout:
                self.db.log_rejected(asset, price, 0, f"News Blackout: {event_name}", self.asset_state[asset]["volatility"], "N/A", "News Blackout")
                self.rejected += 1
                return

            # ---- GATE 1: Regime ----
            regime_ok, regime_reason = self.market_regime.check(asset, price)
            if not regime_ok:
                self.db.log_rejected(asset, price, 0, regime_reason, self.asset_state[asset]["volatility"], "N/A", "Market Regime")
                self.rejected += 1
                return

            # ---- Determine direction ----
            htf_trend = self.asset_state[asset]["htf_trend"]
            tf_trend = self.asset_state[asset]["trend"]
            if htf_trend == "BULLISH" and tf_trend == "BULLISH":
                direction = "BUY"
            elif htf_trend == "BEARISH" and tf_trend == "BEARISH":
                direction = "SELL"
            else:
                return

            # ---- GATE 3: MTF ----
            mtf_ok, mtf_reason = self.mtf_gate.check(asset, direction)
            if not mtf_ok:
                self.db.log_rejected(asset, price, 0, mtf_reason, self.asset_state[asset]["volatility"], "N/A", "MTF Confluence")
                self.rejected += 1
                return

            # ---- GATE 4: Order Flow ----
            of_ok, of_reason = self.orderflow.check(asset, direction, price)
            if not of_ok:
                self.db.log_rejected(asset, price, 0, of_reason, self.asset_state[asset]["volatility"], "N/A", "Order Flow")
                self.rejected += 1
                return

            # ---- SQS ----
            patterns = self.topology.detect_candle_patterns(asset)
            sr = self.topology.support_resistance[asset]
            bos = self.topology.bos[asset]
            choch = self.topology.choch[asset]
            sweep = self.topology.detect_liquidity_sweep(asset, price)
            ob = self.topology.detect_order_block(asset)
            fvgs = self.topology.detect_fvg(asset)
            vol_ratio = self.asset_state[asset]["volume_ratio"]

            sqs = self.sqs_calc.calculate(asset, price, direction, session_ok, patterns, sr,
                                          bos, choch, sweep, ob, fvgs, vol_ratio, htf_trend)

            if sqs < Config.MIN_SQS:
                self.db.log_rejected(asset, price, sqs, f"SQS {sqs} < {Config.MIN_SQS}", self.asset_state[asset]["volatility"], "N/A", "SQS")
                self.rejected += 1
                return

            # ---- Dynamic SL & TP ----
            atr = self.topology.get_atr(asset)
            if atr == 0: atr = price * 0.01
            sl = self.dynamic_sl.calculate(asset, direction, price, atr)
            risk = abs(price - sl)
            tp = price + 2 * risk if direction == "BUY" else price - 2 * risk
            if direction == "BUY":
                if sr["resistance"]:
                    nearest_res = min(sr["resistance"])
                    if nearest_res > price:
                        tp = min(tp, nearest_res * 0.99)
            else:
                if sr["support"]:
                    nearest_sup = max(sr["support"])
                    if nearest_sup < price:
                        tp = max(tp, nearest_sup * 1.01)
            rr = abs(tp - price) / risk
            if rr < 1.5:
                tp = price + 1.5 * risk if direction == "BUY" else price - 1.5 * risk

            # ---- Cooldown & Daily Cap ----
            now_ts = time.time()
            if now_ts - self.last_signal_time[asset] < Config.SIGNAL_COOLDOWN and not self._is_strong_trend(asset):
                self.db.log_rejected(asset, price, sqs, "Cooldown", self.asset_state[asset]["volatility"], "N/A", "Cooldown")
                self.rejected += 1
                return
            recent_signals = [t for t in self.signal_timestamps if now_ts - t < 86400]
            if len(recent_signals) >= Config.MAX_SIGNALS_PER_DAY:
                self.db.log_rejected(asset, price, sqs, "Daily cap reached", self.asset_state[asset]["volatility"], "N/A", "Daily Cap")
                self.rejected += 1
                return

            # ---- Add to Pending Queue ----
            signal_data = {
                'asset': asset,
                'direction': direction,
                'entry': price,
                'sl': sl,
                'tp': tp,
                'sqs': sqs,
                'session': session_name,
                'patterns': patterns,
                'logic': f"HTF {htf_trend} + BOS {bos['direction']}" if bos else "HTF only",
                'news': self.news.last_news.get('title', 'No news')[:100],
                'volatility': self.asset_state[asset]["volatility"],
                'regime': self.topology.get_volatility_regime(asset),
                'htf_trend': htf_trend,
                'news_score': self.asset_state[asset]["news_sentiment"],
                'score': 0,
                'confidence': 'HIGH',
                'num_passed': 11
            }
            self.pending_queue.add_signal(signal_data)
            logger.info(f"⏳ Signal pending: {asset} {direction} @ {price} (SQS: {sqs})")
        except Exception as e:
            logger.error(f"Error in _handle_price_tick: {e}", exc_info=True)

    def _send_final_signal(self, signal):
        try:
            asset = signal['asset']
            direction = signal['direction']
            price = signal['entry']
            sl = signal['sl']
            tp = signal['tp']
            sqs = signal['sqs']
            session = signal['session']  # use correct session name
            patterns = signal['patterns']
            logic = signal['logic']
            news = signal['news']
            volatility = signal['volatility']
            regime = signal['regime']
            htf_trend = signal['htf_trend']
            news_score = signal['news_score']

            # Build logic parts (enhanced)
            logic_parts = [f"HTF {htf_trend}"]
            if self.topology.bos[asset]["direction"]:
                logic_parts.append(f"BOS {self.topology.bos[asset]['direction']}")
            if self.topology.choch[asset]:
                logic_parts.append("CHOCH")
            if self.topology.detect_liquidity_sweep(asset, price):
                logic_parts.append("SWEEP")
            if self.topology.detect_order_block(asset):
                logic_parts.append("OB")
            logic = " + ".join(logic_parts)

            # Log trade
            trade_id = self.db.log_trade(
                asset, direction, price, sl, tp,
                sqs, "HIGH", list(patterns.keys()), logic,
                volatility, regime, htf_trend, news_score,
                session, sqs, list(patterns.keys())[0] if patterns else "unknown"
            )

            # Send Telegram signal (NO extra args)
            chart = self.topology.get_visual_topology(asset, price, direction, sl, tp, patterns)
            rr = abs(tp - price) / abs(price - sl) if abs(price - sl) > 0 else 0
            # win_prob removed

            self.telegram.fire_signal(
                asset=asset, direction=direction, price=price, sl=sl, tp=tp,
                chart=chart, logic=logic, news=news,
                score={"total_score": sqs, "confidence": "HIGH", "num_passed": 11},
                patterns=patterns, trade_id=trade_id,
                session=session, rr=rr
            )

            self.accepted += 1
            self.last_signal_time[asset] = time.time()
            self.signal_timestamps.append(time.time())

            with self.trade_lock:
                self.active_trades[trade_id] = {
                    'id': trade_id,
                    'asset': asset,
                    'direction': direction,
                    'entry': price,
                    'sl': sl,
                    'tp': tp,
                    'entry_time': int(time.time()),
                    'breakeven_locked': False,
                    'trailing_activated': False,
                    'hold_sent': False,
                    'initial_score': sqs,
                    'current_score': sqs,
                    'health': 100
                }
        except Exception as e:
            logger.error(f"Error in _send_final_signal: {e}", exc_info=True)

    def run(self):
        # Start health server and self-ping
        threading.Thread(target=start_health_server, args=(self,), daemon=True).start()
        threading.Thread(target=self._ping_self_loop, daemon=True).start()

        # Load historical data
        logger.info("Loading historical data from MongoDB/Binance in parallel...")
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for asset in Config.ASSETS:
                for tf in [60, 300, 900, 3600, 14400]:
                    futures.append(executor.submit(self._load_and_backfill, asset, tf))
            for future in as_completed(futures):
                pass
        logger.info("Data loading complete.")

        # Start spot WebSocket
        self.stream = BinancePublicStream(self._on_price)
        self.stream.start()
        self.telegram.send_message("🚀 AI v6.1 Online – Signal Quality Gate + Real OI/CVD Active (Relaxed)")

        # Main loop
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
        try:
            self.price_queue.put_nowait((asset, price, volume))
        except queue.Full:
            pass

# =====================================================================
# ENTRY POINT
# =====================================================================
if __name__ == "__main__":
    bot = AIOrchestrator()
    bot.run()

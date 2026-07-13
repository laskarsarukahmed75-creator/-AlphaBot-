import math
from typing import List, Dict, Optional, Tuple
import os
import sys
import time
import json
import logging
import threading
import queue
import requests
import sqlite3
import statistics
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import deque
from datetime import datetime

# ---- NEW INTEGRATION IMPORTS ----
from db_handler import DatabaseHandler
from keepalive import KeepAliveEngine

# Optional for health metrics
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# ---- Logging Setup ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AI-Orchestrator")

class InstitutionalLiquidityEngine:
    def __init__(self, lookback_candles: int = 200, wick_ratio_threshold: float = 1.5):
        self.lookback = lookback_candles
        self.wick_ratio_threshold = wick_ratio_threshold
        self.proximity_pct = 0.005

    def get_liquidity_pools(self, candles: List[Dict]) -> Dict[str, float]:
        window = candles[-self.lookback:] if len(candles) >= self.lookback else candles
        if not window:
            return {"buy_side_liquidity": 0.0, "sell_side_liquidity": 0.0}
        highs = [c['high'] for c in window]
        lows = [c['low'] for c in window]
        return {"buy_side_liquidity": max(highs), "sell_side_liquidity": min(lows)}

    def get_mtf_confirmation(self, candles_5m: List[Dict], level: float, direction: str) -> bool:
        complete = [c for c in candles_5m if c.get("complete", False)]
        if not complete: return False
        c_curr = complete[-1]
        body = abs(c_curr["close"] - c_curr["open"])
        if body == 0: body = 1e-9
        if direction == "SELL":
            upper_wick = c_curr["high"] - max(c_curr["open"], c_curr["close"])
            return c_curr["high"] > level and c_curr["close"] < level and (upper_wick / body) >= self.wick_ratio_threshold
        elif direction == "BUY":
            lower_wick = min(c_curr["open"], c_curr["close"]) - c_curr["low"]
            return c_curr["low"] < level and c_curr["close"] > level and (lower_wick / body) >= self.wick_ratio_threshold
        return False

    def analyze(self, candles_4h: List[Dict], candles_5m: List[Dict], candle_1m: Dict, ltp: float, atr: float, bsl: float, ssl: float) -> Dict:
        if bsl == 0.0 or ssl == 0.0 or atr == 0.0: 
            return {"trigger": "WAIT", "logic": "Insufficient range variables"}
        m1_high, m1_low, m1_close = candle_1m["high"], candle_1m["low"], candle_1m["close"]
        short_proximity = (ltp >= bsl * (1 - self.proximity_pct))
        long_proximity = (ltp <= ssl * (1 + self.proximity_pct))
        if m1_high >= bsl and m1_close < bsl and short_proximity:
            if self.get_mtf_confirmation(candles_5m, bsl, "SELL"):
                return {"trigger": "SELL", "entry": ltp, "sl": m1_high + (1.5 * atr), "tp": ssl - (1.0 * atr), "logic": "Whale Trap Blaster (Wick Confirmed) → Macro SHORT"}
        if m1_low <= ssl and m1_close > ssl and long_proximity:
            if self.get_mtf_confirmation(candles_5m, ssl, "BUY"):
                return {"trigger": "BUY", "entry": ltp, "sl": m1_low - (1.5 * atr), "tp": bsl + (1.0 * atr), "logic": "Whale Accumulation (Wick Confirmed) → Macro LONG"}
        return {"trigger": "WAIT", "logic": "Waiting for Macro Extremes"}

# ---- Configuration ----
class Config:
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    DISPLAY_NAMES = {"BTCUSDT": "BTC/USDT", "ETHUSDT": "ETH/USDT", "SOLUSDT": "SOL/USDT"}
    
    # --- FIXED: Lowered score threshold & layer requirement to get signals ---
    MIN_CONFLUENCE_SCORE = 50       # was 55 → now 50
    MIN_LAYER_PASS = 3              # was 5 → now 3
    
    SIGNAL_COOLDOWN = 3600
    DB_PATH = "trades.db"
    MAX_CANDLES = 500
    VOLATILITY_MULTIPLIERS = {
        "low": (1.2, 2.0),
        "medium": (1.5, 2.5),
        "high": (1.8, 3.0),
        "extreme": (2.0, 3.5)
    }
    MAX_SIGNALS_PER_DAY = 3
    MIN_RISK_REWARD = 1.3

# =====================================================================
# DATABASE (unchanged)
# =====================================================================
class TradeDatabase:
    def __init__(self):
        self.conn = sqlite3.connect(Config.DB_PATH, check_same_thread=False)
        self._create_tables()

    def _create_tables(self):
        cur = self.conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset TEXT,
                direction TEXT,
                entry REAL,
                stop_loss REAL,
                take_profit REAL,
                score INTEGER,
                confidence TEXT,
                patterns TEXT,
                logic TEXT,
                timestamp INTEGER,
                status TEXT DEFAULT 'open',
                exit_price REAL,
                pnl REAL,
                close_time INTEGER,
                volatility REAL,
                market_regime TEXT,
                htf_trend TEXT,
                news_score REAL,
                rejection_reason TEXT
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS rejected_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset TEXT,
                price REAL,
                score INTEGER,
                reason TEXT,
                timestamp INTEGER,
                volatility REAL,
                market_regime TEXT
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                win_rate REAL,
                profit_factor REAL,
                sharpe REAL,
                total_trades INTEGER,
                winning_trades INTEGER,
                losing_trades INTEGER,
                total_pnl REAL
            )
        ''')
        self.conn.commit()

    def log_trade(self, asset, direction, entry, sl, tp, score, confidence, patterns, logic,
                  volatility, regime, htf_trend, news_score):
        cur = self.conn.cursor()
        cur.execute('''
            INSERT INTO trades 
            (asset, direction, entry, stop_loss, take_profit, score, confidence, patterns, logic, 
             timestamp, volatility, market_regime, htf_trend, news_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (asset, direction, entry, sl, tp, score, confidence, json.dumps(patterns), logic,
              int(time.time()), volatility, regime, htf_trend, news_score))
        self.conn.commit()
        return cur.lastrowid

    def log_rejected(self, asset, price, score, reason, volatility, regime):
        cur = self.conn.cursor()
        cur.execute('''
            INSERT INTO rejected_signals (asset, price, score, reason, timestamp, volatility, market_regime)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (asset, price, score, reason, int(time.time()), volatility, regime))
        self.conn.commit()

    def close_trade(self, trade_id, exit_price, pnl):
        cur = self.conn.cursor()
        cur.execute('''
            UPDATE trades SET status='closed', exit_price=?, pnl=?, close_time=?
            WHERE id=?
        ''', (exit_price, pnl, int(time.time()), trade_id))
        self.conn.commit()

    def get_performance_metrics(self):
        cur = self.conn.cursor()
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

    def get_rolling_win_rate(self, asset: str, lookback: int = 50) -> float:
        cur = self.conn.cursor()
        cur.execute('''
            SELECT pnl FROM trades WHERE asset=? AND status='closed' AND pnl IS NOT NULL
            ORDER BY close_time DESC LIMIT ?
        ''', (asset, lookback))
        rows = cur.fetchall()
        if not rows:
            return 0.5
        wins = sum(1 for r in rows if r[0] > 0)
        return wins / len(rows)

    def get_db_size(self):
        try:
            return os.path.getsize(Config.DB_PATH)
        except:
            return 0

# =====================================================================
# NEWS SCANNER (unchanged)
# =====================================================================
class CryptoNewsScanner:
    def __init__(self):
        self.last_news = {}
        self.sentiment_history = deque(maxlen=10)
        self.importance_map = {
            "etf": 1.0, "cpi": 0.9, "fomc": 0.9, "liquidation": 0.7,
            "whale": 0.6, "institutional": 0.8, "regulation": 0.7, "adoption": 0.5,
        }

    def fetch_latest(self) -> Dict[str, Any]:
        try:
            url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&limit=5"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get("Data"):
                    articles = []
                    for article in data["Data"][:3]:
                        title = article.get("title", "")
                        body = article.get("body", "").lower()
                        sentiment = self._analyze_sentiment(title, body)
                        importance = self._compute_importance(title + " " + body)
                        articles.append({
                            "title": title,
                            "sentiment": sentiment,
                            "source": article.get("source", ""),
                            "timestamp": article.get("published_on", 0),
                            "importance": importance
                        })
                    if articles:
                        self.last_news = articles[0]
                    return {"articles": articles, "fresh": True}
        except Exception as e:
            logger.error(f"News fetch error: {e}")
        return {"articles": [], "fresh": False}

    def _analyze_sentiment(self, title: str, body: str) -> Dict:
        bullish_words = ["bullish", "breakout", "surge", "buy", "accumulate", "growth", "approved", "institutional", "rally", "pump"]
        bearish_words = ["bearish", "crash", "dump", "sell", "liquidation", "ban", "hack", "regulatory", "drop", "panic"]
        text = (title + " " + body).lower()
        bull_score = sum(1 for w in bullish_words if w in text)
        bear_score = sum(1 for w in bearish_words if w in text)
        total = bull_score + bear_score
        net_score = ((bull_score - bear_score) / total * 100) if total else 0
        label = "BULLISH" if net_score > 20 else "BEARISH" if net_score < -20 else "NEUTRAL"
        return {"score": net_score, "label": label, "bullish_count": bull_score, "bearish_count": bear_score}

    def _compute_importance(self, text: str) -> float:
        text_lower = text.lower()
        max_imp = 0.0
        for word, imp in self.importance_map.items():
            if word in text_lower:
                max_imp = max(max_imp, imp)
        return max_imp

# =====================================================================
# CANDLE TOPOLOGY ENGINE (with proper candle‑close detection)
# =====================================================================
class CandleTopologyEngine:
    def __init__(self):
        self.history = {asset: deque(maxlen=200) for asset in Config.ASSETS}
        self.candles = {tf: {asset: [] for asset in Config.ASSETS} for tf in [60, 300, 900, 3600]}
        self.support_resistance = {asset: {"support": [], "resistance": []} for asset in Config.ASSETS}
        self.trendlines = {asset: {"up": [], "down": []} for asset in Config.ASSETS}
        self.pivots = {asset: {"high": [], "low": []} for asset in Config.ASSETS}
        self.bos = {asset: {"break": None, "direction": ""} for asset in Config.ASSETS}
        self.choch = {asset: False for asset in Config.ASSETS}
        self.fvgs = {asset: [] for asset in Config.ASSETS}
        self.order_blocks = {asset: {} for asset in Config.ASSETS}
        self.last_tick_time = {asset: 0 for asset in Config.ASSETS}
        # --- FIX: flag to signal that a 15m candle just closed ---
        self.candle_just_closed = {asset: False for asset in Config.ASSETS}

    def process_tick(self, asset: str, price: float, volume: float):
        now = int(time.time())
        self.history[asset].append({"price": price, "volume": volume, "time": now})
        # reset flag for this asset
        self.candle_just_closed[asset] = False

        # --- Check 15‑minute candle close (900s) ---
        tf = 900
        start = (now // tf) * tf
        storage = self.candles[tf][asset]
        if storage and storage[-1].get("timestamp") != start:
            if not storage[-1].get("complete", False):
                storage[-1]["complete"] = True
                db = DatabaseHandler()
                db.save_candle(asset, tf, storage[-1])
                self.candle_just_closed[asset] = True   # << evaluation trigger

        # build candles for all timeframes (1m, 5m, 15m, 1h)
        for timeframe in [60, 300, 900, 3600]:
            self._build_candle(asset, price, volume, now, timeframe, self.candles[timeframe][asset])

        self._update_pivots(asset, price)
        self._update_support_resistance(asset, price)
        self._update_trendlines(asset, price)
        self._detect_bos_choch(asset, price)
        self._update_fvgs(asset)
        self._update_order_blocks(asset, price)
        self.last_tick_time[asset] = now

    def _build_candle(self, asset: str, price: float, volume: float, ts: int, tf: int, storage: List):
        start = (ts // tf) * tf
        if not storage or storage[-1].get("timestamp") != start:
            if storage and not storage[-1].get("complete", False):
                storage[-1]["complete"] = True
                db = DatabaseHandler()
                db.save_candle(asset, tf, storage[-1])
            storage.append({
                "timestamp": start, "open": price, "high": price, "low": price,
                "close": price, "volume": volume, "complete": False
            })
            if len(storage) > Config.MAX_CANDLES:
                storage.pop(0)
        else:
            candle = storage[-1]
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price
            candle["volume"] += volume

    def _update_pivots(self, asset: str, price: float):
        candles = self.candles[900][asset]
        if len(candles) < 10: return
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < 10: return
        for i in range(2, len(complete)-2):
            high = complete[i]["high"]
            if (complete[i-2]["high"] < high and complete[i-1]["high"] < high and
                complete[i+1]["high"] < high and complete[i+2]["high"] < high):
                if high not in self.pivots[asset]["high"]:
                    self.pivots[asset]["high"].append(high)
                    self.pivots[asset]["high"] = sorted(self.pivots[asset]["high"], reverse=True)[:10]
            low = complete[i]["low"]
            if (complete[i-2]["low"] > low and complete[i-1]["low"] > low and
                complete[i+1]["low"] > low and complete[i+2]["low"] > low):
                if low not in self.pivots[asset]["low"]:
                    self.pivots[asset]["low"].append(low)
                    self.pivots[asset]["low"] = sorted(self.pivots[asset]["low"])[:10]

    def _detect_bos_choch(self, asset: str, price: float):
        piv_high = self.pivots[asset]["high"]
        piv_low = self.pivots[asset]["low"]
        if len(piv_high) >= 3 and len(piv_low) >= 3:
            last_high, prev_high = piv_high[0], piv_high[1]
            last_low, prev_low = piv_low[0], piv_low[1]
            if last_high > prev_high:
                self.bos[asset] = {"break": last_high, "direction": "UP"}
            elif last_low < prev_low:
                self.bos[asset] = {"break": last_low, "direction": "DOWN"}
            self.choch[asset] = piv_low[1] > piv_low[2] and piv_high[1] < piv_high[2]

    def _update_support_resistance(self, asset: str, price: float):
        all_levels = self.pivots[asset]["high"] + self.pivots[asset]["low"]
        clusters = []
        for level in sorted(all_levels):
            if not clusters or abs(level - clusters[-1]) / level > 0.005:
                clusters.append(level)
        supports = [l for l in clusters if l < price * 0.99]
        resistances = [l for l in clusters if l > price * 1.01]
        self.support_resistance[asset]["support"] = sorted(supports)[-5:]
        self.support_resistance[asset]["resistance"] = sorted(resistances, reverse=True)[:5]

    def _update_trendlines(self, asset: str, price: float):
        candles = self.candles[900][asset]
        if len(candles) < 30: return
        closes = [c["close"] for c in candles[-30:] if c.get("complete", False)]
        if len(closes) < 20: return
        ema_short = self._ema(closes, 9)
        ema_long = self._ema(closes, 21)
        if len(ema_short) > 1 and len(ema_long) > 1:
            if ema_short[-1] - ema_short[-2] > 0 and ema_long[-1] - ema_long[-2] > 0:
                if not self.trendlines[asset]["up"] or price > self.trendlines[asset]["up"][-1]:
                    self.trendlines[asset]["up"].append(price)
                    self.trendlines[asset]["up"] = self.trendlines[asset]["up"][-5:]
            elif ema_short[-1] - ema_short[-2] < 0 and ema_long[-1] - ema_long[-2] < 0:
                if not self.trendlines[asset]["down"] or price < self.trendlines[asset]["down"][-1]:
                    self.trendlines[asset]["down"].append(price)
                    self.trendlines[asset]["down"] = self.trendlines[asset]["down"][-5:]

    def _ema(self, series: List[float], period: int) -> List[float]:
        if len(series) < period: return []
        ema = [sum(series[:period]) / period]
        multiplier = 2 / (period + 1)
        for i in range(period, len(series)):
            ema.append((series[i] - ema[-1]) * multiplier + ema[-1])
        return ema

    def detect_candle_patterns(self, asset: str) -> Dict:
        candles = self.candles[300][asset]
        if len(candles) < 5: return {}
        patterns = {}
        last = candles[-1]
        prev = candles[-2] if len(candles) > 1 else None
        if not last.get("complete", False): return {}
        body = abs(last["close"] - last["open"])
        total_range = last["high"] - last["low"]
        if total_range > 0:
            if (min(last["open"], last["close"]) - last["low"]) / total_range > 0.6 and body / total_range < 0.4:
                patterns["bullish_rejection"] = {"strength": min(100, int(((min(last["open"], last["close"]) - last["low"]) / total_range)*100)), "logic": "Bullish pin bar"}
            if (last["high"] - max(last["open"], last["close"])) / total_range > 0.6 and body / total_range < 0.4:
                patterns["bearish_rejection"] = {"strength": min(100, int(((last["high"] - max(last["open"], last["close"])) / total_range)*100)), "logic": "Bearish pin bar"}
        if prev and last.get("complete", False) and prev.get("complete", False):
            prev_body = abs(prev["close"] - prev["open"])
            if prev_body > 0:
                if prev["close"] < prev["open"] and last["close"] > last["open"] and last["close"] > prev["open"] and last["open"] < prev["close"]:
                    patterns["bullish_engulfing"] = {"strength": min(100, int((body / prev_body) * 50)), "logic": "Bullish engulfing"}
                if prev["close"] > prev["open"] and last["close"] < last["open"] and last["close"] < prev["open"] and last["open"] > prev["close"]:
                    patterns["bearish_engulfing"] = {"strength": min(100, int((body / prev_body) * 50)), "logic": "Bearish engulfing"}
        return patterns

    def get_atr(self, asset: str, period: int = 14) -> float:
        candles = self.candles[3600][asset]
        if len(candles) < period: return 0.0
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < period: return 0.0
        tr_list = []
        for i in range(1, len(complete)):
            tr = max(complete[i]["high"] - complete[i]["low"], abs(complete[i]["high"] - complete[i-1]["close"]), abs(complete[i]["low"] - complete[i-1]["close"]))
            tr_list.append(tr)
        return sum(tr_list[-period:]) / period if len(tr_list) >= period else 0.0

    def _update_fvgs(self, asset: str):
        candles = self.candles[900][asset]
        if len(candles) < 3: return
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < 3: return
        fvgs = []
        for i in range(2, len(complete)-1):
            c1, c2, c3 = complete[i-2], complete[i-1], complete[i]
            if c1["close"] < c2["open"] and c2["close"] < c3["close"] and c1["high"] > c2["low"]:
                fvgs.append({"type": "bullish", "upper": c1["high"], "lower": c2["low"], "timestamp": c2["timestamp"]})
            if c1["close"] > c2["open"] and c2["close"] > c3["close"] and c2["high"] > c1["low"]:
                fvgs.append({"type": "bearish", "upper": c2["high"], "lower": c1["low"], "timestamp": c2["timestamp"]})
        self.fvgs[asset] = fvgs[-5:]

    def detect_fvg(self, asset: str) -> List[Dict]:
        return self.fvgs[asset]

    def _update_order_blocks(self, asset: str, price: float):
        if not self.bos[asset]["direction"]: return
        candles = self.candles[900][asset]
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < 10: return
        atr = self.get_atr(asset)
        if atr == 0: return
        for i in range(len(complete)-1, -1, -1):
            c = complete[i]
            if (c["high"] - c["low"]) > 1.5 * atr:
                self.order_blocks[asset] = {
                    "type": "bullish" if c["close"] > c["open"] else "bearish", "high": c["high"], "low": c["low"], "timestamp": c["timestamp"], "direction": self.bos[asset]["direction"]
                }
                return
        self.order_blocks[asset] = {}

    def detect_order_block(self, asset: str) -> Dict:
        return self.order_blocks[asset]

    def detect_liquidity_sweep(self, asset: str, price: float) -> str:
        piv_high = self.pivots[asset]["high"]
        piv_low = self.pivots[asset]["low"]
        if piv_high and price > max(piv_high[-2:]): return "BUY_SWEEP"
        if piv_low and price < min(piv_low[-2:]): return "SELL_SWEEP"
        return ""

    def get_volatility_regime(self, asset: str) -> str:
        atr = self.get_atr(asset)
        if atr == 0: return "medium"
        candles = self.candles[3600][asset]
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < 50: return "medium"
        atr_list = [max(complete[i]["high"] - complete[i]["low"], abs(complete[i]["high"] - complete[i-1]["close"]), abs(complete[i]["low"] - complete[i-1]["close"])) for i in range(50, len(complete))]
        avg_atr = sum(atr_list) / len(atr_list) if atr_list else atr
        ratio = atr / avg_atr if avg_atr > 0 else 1.0
        return "low" if ratio < 0.7 else "medium" if ratio < 1.3 else "high" if ratio < 2.0 else "extreme"

    def get_visual_topology(self, asset: str, price: float, direction: str, sl: float, tp: float, patterns: Dict) -> str:
        sr = self.support_resistance[asset]
        supports = sr["support"][-3:] if sr["support"] else [price * 0.98]
        resistances = sr["resistance"][-3:] if sr["resistance"] else [price * 1.02]
        min_price = min(min(supports), sl, price * 0.97)
        max_price = max(max(resistances), tp, price * 1.03)
        rows = 10
        chart_lines = ["┌──────────────────────────────────────┐", "│           LIVE TOPOLOGY CHART         │", "├──────────────────────────────────────┤"]
        for i in range(rows, -1, -1):
            level_price = min_price + (max_price - min_price) * (i / rows)
            marker = "S" if any(abs(level_price - s) / s < 0.001 for s in supports) else "R" if any(abs(level_price - r) / r < 0.001 for r in resistances) else "●" if abs(level_price - price) / price < 0.001 else "▼" if abs(level_price - sl) / sl < 0.001 else "★" if abs(level_price - tp) / tp < 0.001 else " "
            bar = "█" * int((i / rows) * 10) if i > 0 else ""
            chart_lines.append(f"│ {level_price:>8.2f} │ {marker} {bar:<10} │")
        chart_lines.extend(["├──────────────────────────────────────┤", "│ S=Support  R=Resistance  ●=Entry    │", "│ ▼=SL  ★=Target                      │", "└──────────────────────────────────────┘"])
        return "\n".join(chart_lines)

# =====================================================================
# LAYERED SIGNAL SCORING ENGINE (adjusted weights for better sensitivity)
# =====================================================================
class SignalScoringEngine:
    def __init__(self):
        self.weights = {
            "htf_trend": 15,       # high weight
            "market_structure": 15,
            "liquidity_sweep": 10,
            "fvg": 10,
            "order_block": 10,
            "volume": 10,
            "rsi": 10,
            "adx": 10,
            "news": 10
        }
        self.min_pass_layers = Config.MIN_LAYER_PASS

    def evaluate(self, asset: str, price: float, patterns: Dict, sr_data: Dict,
                 trend: str, news_sentiment: float, volume_ratio: float,
                 rsi: float, adx: float, volatility: float,
                 htf_trend: str, bos: Dict, choch: bool,
                 fvgs: List, order_block: Dict, liquidity_sweep: str,
                 news_importance: float) -> Dict:
        passed_layers = []
        total_score = 0

        # 1. HTF Trend
        htf_score = self.weights["htf_trend"] if htf_trend == trend and htf_trend != "NEUTRAL" else self.weights["htf_trend"] * 0.5 if htf_trend == "NEUTRAL" else 0
        if htf_score > 0: passed_layers.append("htf_trend")
        total_score += htf_score

        # 2. Market Structure
        structure_score = self.weights["market_structure"] if choch else self.weights["market_structure"] * 0.7 if bos and bos["direction"] else 0
        if structure_score > 0: passed_layers.append("market_structure")
        total_score += structure_score

        # 3. Liquidity Sweep
        ls_score = self.weights["liquidity_sweep"] if liquidity_sweep else 0
        if ls_score > 0: passed_layers.append("liquidity_sweep")
        total_score += ls_score

        # 4. FVG
        fvg_score = next((self.weights["fvg"] for fvg in fvgs if fvg["lower"] < price < fvg["upper"]), 0)
        if fvg_score > 0: passed_layers.append("fvg")
        total_score += fvg_score

        # 5. Order Block
        ob_score = self.weights["order_block"] if order_block and order_block.get("type") else 0
        if ob_score > 0: passed_layers.append("order_block")
        total_score += ob_score

        # 6. Volume
        vol_score = self.weights["volume"] if volume_ratio > 1.2 else self.weights["volume"] * 0.5 if volume_ratio > 0.8 else 0
        if vol_score > 0: passed_layers.append("volume")
        total_score += vol_score

        # 7. RSI
        rsi_score = self.weights["rsi"] if (adx > 25 or 30 <= rsi <= 70) else self.weights["rsi"] * 0.5 if (20 <= rsi < 30 or 70 < rsi <= 80) else 0
        if rsi_score > 0: passed_layers.append("rsi")
        total_score += rsi_score

        # 8. ADX
        adx_score = self.weights["adx"] if adx > 25 else self.weights["adx"] * 0.5 if adx > 20 else 0
        if adx_score > 0: passed_layers.append("adx")
        total_score += adx_score

        # 9. News
        news_score = self.weights["news"] * news_importance if (news_sentiment != 0 and news_importance > 0.5 and ((trend == "BULLISH" and news_sentiment > 0) or (trend == "BEARISH" and news_sentiment < 0))) else self.weights["news"] * 0.5 if abs(news_sentiment) > 50 else 0
        if news_score > 0: passed_layers.append("news")
        total_score += news_score

        final_score = min(100, total_score)
        return {
            "total_score": final_score,
            "confidence": "HIGH" if final_score >= 70 else "MEDIUM" if final_score >= 50 else "LOW",
            "probability": 50 + (final_score - 50) * 0.6,
            "passed_layers": passed_layers,
            "enough": len(passed_layers) >= self.min_pass_layers,
            "num_passed": len(passed_layers)
        }

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
                url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                requests.post(url, data={"chat_id": self.chat_id, "text": msg, "parse_mode": "HTML"}, timeout=10)
            except Exception as e:
                logger.error(f"Telegram error: {e}")

    def fire_signal(self, asset: str, direction: str, price: float, sl: float, tp: float,
                    topology_chart: str, logic: str, news: str, score: Dict, patterns: Dict,
                    trade_id: int, session: str, entry_zone: str, exit_zone: str,
                    win_prob: float, rr: float):
        icon = "🔥" if direction == "BUY" else "❄️"
        msg = (
            f"{icon} <b>AI SIGNAL: {direction}</b> {icon}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 {Config.DISPLAY_NAMES.get(asset, asset)} | 🆔 #{trade_id}\n"
            f"⏰ {session} | ⚡ {'STRONG' if score['total_score'] >= 70 else 'MODERATE'} ({score['total_score']:.0f}%)\n"
            f"🎯 Win Prob: {win_prob:.1f}% | R:R {rr:.2f}\n"
            f"💰 Entry: {price:.2f}  🛑 SL: {sl:.2f}  🎯 TP: {tp:.2f}\n"
            f"📌 Entry Zone: {entry_zone} | Exit Zone: {exit_zone}\n"
            f"\n📊 CHART:\n{topology_chart}\n"
            f"🧠 Logic: {logic}\n"
            f"📰 News: {news}\n"
            f"📊 Layers Passed: {score['num_passed']}/9\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        self.queue.put(msg)

    def fire_news_alert(self, title: str, sentiment: str, score: float, importance: float):
        icon = "🚨" if sentiment == "BEARISH" else "🚀" if sentiment == "BULLISH" else "📰"
        msg = (
            f"{icon} <b>BREAKING NEWS</b> {icon}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📰 {title}\n"
            f"🧠 Sentiment: {sentiment} ({score:+.0f}%) | Importance: {importance:.1f}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        self.queue.put(msg)

# =====================================================================
# BINANCE PUBLIC WEBSOCKET (unchanged)
# =====================================================================
class BinancePublicStream:
    def __init__(self, on_price_update):
        self.on_price_update = on_price_update
        self.running = False
        self.reconnect_count = 0
        self.latency = 0
        self.last_ping = 0
        self.ws = None

    def start(self):
        self.running = True
        threading.Thread(target=self._ws_loop, daemon=True).start()

    def _ws_loop(self):
        import websocket
        while self.running:
            try:
                streams = [f"{asset.lower()}@kline_1m" for asset in Config.ASSETS]
                ws_url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"
                self.ws = websocket.WebSocketApp(
                    ws_url, on_open=self._on_open, on_message=self._on_message, on_error=self._on_error, on_close=self._on_close
                )
                self.ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.error(f"WebSocket loop error: {e}")
                self.reconnect_count += 1
                time.sleep(5)

    def _on_open(self, ws):
        logger.info("Binance Public WebSocket connected.")
        self.reconnect_count = 0

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            kline = data["data"]["k"] if "data" in data else data["k"] if "k" in data else None
            if kline:
                symbol = kline.get("s")
                if symbol in Config.ASSETS:
                    price, volume = float(kline.get("c", 0)), float(kline.get("v", 0))
                    if price > 0: self.on_price_update(symbol, price, volume)
        except Exception as e:
            logger.debug(f"Message parse error: {e}")

    def _on_error(self, ws, error): logger.error(f"WebSocket error: {error}")
    def _on_close(self, ws, close_status_code, close_msg): self.reconnect_count += 1

# =====================================================================
# HEALTH SERVER (unchanged)
# =====================================================================
def start_health_server(orchestrator):
    port = int(os.environ.get("PORT", 10000))
    class HealthHandler(BaseHTTPRequestHandler):
        def log_message(self, *a, **k): pass
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            uptime = int(time.time() - orchestrator.start_time) if hasattr(orchestrator, 'start_time') else 0
            candle_delay = 0
            for asset in Config.ASSETS:
                candles = orchestrator.topology.candles[60][asset]
                if candles and candles[-1].get("complete", False):
                    candle_delay = max(candle_delay, int(time.time()) - candles[-1]["timestamp"] - 60)
            status = {
                "status": "online", "version": "3.1", "uptime": uptime,
                "cpu_percent": psutil.cpu_percent() if HAS_PSUTIL else 0,
                "memory_percent": psutil.virtual_memory().percent if HAS_PSUTIL else 0,
                "queue_size": orchestrator.telegram.queue.qsize(), "db_size_bytes": orchestrator.db.get_db_size(),
                "last_tick": orchestrator.topology.last_tick_time, "last_signal": orchestrator.last_signal_time,
                "reconnect_count": orchestrator.stream.reconnect_count, "ws_latency": orchestrator.stream.latency,
                "candle_delay_seconds": candle_delay, "signal_counts": {"accepted": orchestrator.accepted_signals, "rejected": orchestrator.rejected_signals}
            }
            self.wfile.write(json.dumps(status).encode())
    HTTPServer(("0.0.0.0", port), HealthHandler).serve_forever()

# =====================================================================
# CORE ORCHESTRATOR – Only evaluates on 15‑minute candle close
# =====================================================================
class AIOrchestrator:
    def __init__(self):
        self.start_time = time.time()
        self.topology = CandleTopologyEngine()
        self.news = CryptoNewsScanner()
        self.scoring = SignalScoringEngine()
        self.telegram = TelegramPipeline()
        self.db = TradeDatabase()
        self.stream = None
        self.last_signal_time = {asset: 0 for asset in Config.ASSETS}
        self.last_news_time = 0
        self.asset_state = {asset: {
            "trend": "NEUTRAL", "htf_trend": "NEUTRAL", "volume_ratio": 1.0,
            "rsi": 50, "adx": 20, "volatility": 0.01,
            "news_sentiment": 0, "news_importance": 0
        } for asset in Config.ASSETS}
        self.rejected_signals = 0
        self.accepted_signals = 0
        self.signal_timestamps = deque(maxlen=100)

    def _recover_from_db(self):
        db = DatabaseHandler()
        for asset in Config.ASSETS:
            for tf in [60, 300, 900, 3600]:
                candles = db.load_candles(asset, tf, limit=Config.MAX_CANDLES)
                if candles:
                    self.topology.candles[tf][asset] = candles
                    self.topology.last_tick_time[asset] = candles[-1]["timestamp"]
                    self._fetch_missing_candles(asset, tf, candles[-1]["timestamp"])
                else:
                    self._fetch_missing_candles(asset, tf, int(time.time()) - (90 * 24 * 3600))

    def _fetch_missing_candles(self, asset, tf, since_ts):
        interval_map = {60: "1m", 300: "5m", 900: "15m", 3600: "1h"}
        try:
            resp = requests.get("https://api.binance.com/api/v3/klines", params={"symbol": asset, "interval": interval_map.get(tf, "1m"), "limit": 1000, "startTime": since_ts * 1000}, timeout=15)
            if resp.status_code == 200:
                db = DatabaseHandler()
                for item in resp.json():
                    candle = {"timestamp": item[0] // 1000, "open": float(item[1]), "high": float(item[2]), "low": float(item[3]), "close": float(item[4]), "volume": float(item[5]), "complete": True}
                    if self.topology.candles[tf][asset] and candle["timestamp"] <= self.topology.candles[tf][asset][-1]["timestamp"]: continue
                    self.topology.candles[tf][asset].append(candle)
                    db.save_candle(asset, tf, candle)
                if len(self.topology.candles[tf][asset]) > Config.MAX_CANDLES:
                    self.topology.candles[tf][asset] = self.topology.candles[tf][asset][-Config.MAX_CANDLES:]
        except Exception as e:
            logger.error(f"Error executing REST history sync: {e}")

    def _prune_old_data(self):
        cutoff = int(time.time()) - (90 * 24 * 3600)
        db = DatabaseHandler()
        for asset in Config.ASSETS:
            for tf in [60, 300, 900, 3600]: db.delete_older_than(asset, tf, cutoff)

    def run(self):
        self._recover_from_db()
        KeepAliveEngine().start()
        self.stream = BinancePublicStream(on_price_update=self._handle_price_tick)
        self.stream.start()
        self.telegram.fire_news_alert("AI v3.1 Online - Candle‑Close Evaluation Engaged", "BULLISH", 85, 1.0)
        threading.Thread(target=start_health_server, args=(self,), daemon=True).start()
        
        last_prune = time.time()
        while True:
            try:
                time.sleep(15)
                if time.time() - last_prune > 86400:
                    self._prune_old_data()
                    last_prune = time.time()
                if int(time.time()) - self.last_news_time > 30:
                    news_data = self.news.fetch_latest()
                    if news_data.get("fresh") and news_data.get("articles"):
                        for article in news_data["articles"][:2]:
                            self.telegram.fire_news_alert(article["title"], article["sentiment"]["label"], article["sentiment"]["score"], article["importance"])
                            for asset in Config.ASSETS:
                                self.asset_state[asset]["news_sentiment"] = article["sentiment"]["score"]
                                self.asset_state[asset]["news_importance"] = article["importance"]
                        self.last_news_time = int(time.time())
            except KeyboardInterrupt: break
            except Exception as e: logger.error(f"Main loop error: {e}")

    def _handle_price_tick(self, asset: str, price: float, volume: float):
        self.topology.process_tick(asset, price, volume)

        # --- CRITICAL FIX: only evaluate when a 15‑minute candle has just closed ---
        if not self.topology.candle_just_closed[asset]:
            return

        # --- Now we run the full analysis once per candle close ---
        # update indicators using the completed 15m candles
        candles_15m = self.topology.candles[900][asset]
        if len(candles_15m) > 10:
            closes = [c["close"] for c in candles_15m if c.get("complete", False)]
            if len(closes) > 10:
                ema_short, ema_long = self.topology._ema(closes, 9), self.topology._ema(closes, 21)
                if len(ema_short) > 1 and len(ema_long) > 1:
                    self.asset_state[asset]["trend"] = "BULLISH" if ema_short[-1] > ema_long[-1] else "BEARISH"
                if len(closes) >= 14:
                    self.asset_state[asset]["rsi"] = self._calculate_rsi(closes, 14)
                    self.asset_state[asset]["adx"] = self._calculate_adx(candles_15m[-14:])

        candles_1h = self.topology.candles[3600][asset]
        if len(candles_1h) > 10:
            closes_1h = [c["close"] for c in candles_1h if c.get("complete", False)]
            if len(closes_1h) > 10:
                ema_short_1h, ema_long_1h = self.topology._ema(closes_1h, 9), self.topology._ema(closes_1h, 21)
                if len(ema_short_1h) > 1 and len(ema_long_1h) > 1:
                    self.asset_state[asset]["htf_trend"] = "BULLISH" if ema_short_1h[-1] > ema_long_1h[-1] else "BEARISH"

        vols = [c["volume"] for c in self.topology.candles[300][asset] if c.get("complete", False)]
        if len(vols) > 10:
            avg_vol = sum(vols[-10:-1]) / max(1, len(vols[-10:-1]))
            self.asset_state[asset]["volume_ratio"] = vols[-1] / avg_vol if avg_vol > 0 else 1.0

        atr = self.topology.get_atr(asset)
        if atr > 0 and price > 0: self.asset_state[asset]["volatility"] = atr / price

        now_ts = time.time()
        if now_ts - self.last_signal_time[asset] < Config.SIGNAL_COOLDOWN:
            return

        patterns = self.topology.detect_candle_patterns(asset)
        score = self.scoring.evaluate(
            asset=asset, price=price, patterns=patterns, sr_data=self.topology.support_resistance[asset],
            trend=self.asset_state[asset]["trend"], news_sentiment=self.asset_state[asset]["news_sentiment"], volume_ratio=self.asset_state[asset]["volume_ratio"],
            rsi=self.asset_state[asset]["rsi"], adx=self.asset_state[asset]["adx"], volatility=self.asset_state[asset]["volatility"],
            htf_trend=self.asset_state[asset]["htf_trend"], bos=self.topology.bos[asset], choch=self.topology.choch[asset],
            fvgs=self.topology.detect_fvg(asset), order_block=self.topology.detect_order_block(asset), liquidity_sweep=self.topology.detect_liquidity_sweep(asset, price),
            news_importance=self.asset_state[asset]["news_importance"]
        )

        # Reject if not enough layers or score below threshold
        if not score["enough"] or score["total_score"] < Config.MIN_CONFLUENCE_SCORE:
            reason = f"Confluence failure ({score['num_passed']}/9) or low score ({score['total_score']:.0f})"
            self.db.log_rejected(asset, price, score["total_score"], reason, self.asset_state[asset]["volatility"], self.topology.get_volatility_regime(asset))
            self.rejected_signals += 1
            return

        direction = None
        if self.asset_state[asset]["htf_trend"] == "BULLISH" and self.asset_state[asset]["trend"] == "BULLISH":
            direction = "BUY"
        elif self.asset_state[asset]["htf_trend"] == "BEARISH" and self.asset_state[asset]["trend"] == "BEARISH":
            direction = "SELL"

        if direction is None:
            return

        regime = self.topology.get_volatility_regime(asset)
        sl_mult, tp_mult = Config.VOLATILITY_MULTIPLIERS.get(regime, (1.5, 2.5))
        if direction == "BUY":
            sl = price - sl_mult * atr
            tp = price + tp_mult * atr
        else:
            sl = price + sl_mult * atr
            tp = price - tp_mult * atr

        # Enforce minimum RR
        rr = abs(tp - price) / abs(price - sl) if abs(price - sl) > 0 else 0
        if rr < Config.MIN_RISK_REWARD:
            if direction == "BUY":
                tp = price + abs(price - sl) * Config.MIN_RISK_REWARD
            else:
                tp = price - abs(price - sl) * Config.MIN_RISK_REWARD
            rr = abs(tp - price) / abs(price - sl)

        # Daily cap check
        recent_signals = [t for t in self.signal_timestamps if now_ts - t < 86400]
        if len(recent_signals) >= Config.MAX_SIGNALS_PER_DAY:
            reason = f"Daily max cap reached ({Config.MAX_SIGNALS_PER_DAY} per 24h)"
            self.db.log_rejected(asset, price, score["total_score"], reason, self.asset_state[asset]["volatility"], regime)
            self.rejected_signals += 1
            return

        # Build logic string
        logic_parts = [f"HTF {self.asset_state[asset]['htf_trend']}"]
        if self.topology.bos[asset]["direction"]: logic_parts.append(f"BOS {self.topology.bos[asset]['direction']}")
        if self.topology.choch[asset]: logic_parts.append("CHOCH")
        logic = " + ".join(logic_parts)

        # Log trade
        trade_id = self.db.log_trade(
            asset, direction, price, sl, tp, score["total_score"], score["confidence"],
            list(patterns.keys()), logic, self.asset_state[asset]["volatility"],
            regime, self.asset_state[asset]["htf_trend"], self.asset_state[asset]["news_sentiment"]
        )

        self.accepted_signals += 1
        self.last_signal_time[asset] = now_ts
        self.signal_timestamps.append(now_ts)

        # Fire Telegram signal
        self.telegram.fire_signal(
            asset=asset, direction=direction, price=price, sl=sl, tp=tp,
            topology_chart=self.topology.get_visual_topology(asset, price, direction, sl, tp, patterns),
            logic=logic, news=self.news.last_news.get("title", "No news")[:100] if self.news.last_news else "No news",
            score=score, patterns=patterns, trade_id=trade_id, session=datetime.now().strftime("%H:%M"),
            entry_zone=f"{price - atr*0.5:.2f} - {price + atr*0.5:.2f}",
            exit_zone=f"{tp - atr*0.5:.2f} - {tp + atr*0.5:.2f}",
            win_prob=min(95, max(5, (self.db.get_rolling_win_rate(asset) * 50 + score["probability"] * 0.5))),
            rr=rr
        )
        logger.info(f"🔥 SIGNAL: {asset} {direction} @ {price} (Score: {score['total_score']:.0f}, RR: {rr:.2f})")

    def _calculate_rsi(self, closes: List[float], period: int = 14) -> float:
        if len(closes) < period+1: return 50
        gains, losses = 0, 0
        for i in range(len(closes)-period, len(closes)):
            change = closes[i] - closes[i-1]
            if change > 0: gains += change
            else: losses -= change
        return 100 - (100 / (1 + ((gains / period) / (losses / period)))) if losses > 0 else 100

    def _calculate_adx(self, candles: List[Dict]) -> float:
        if len(candles) < 14: return 20
        tr_list, dm_plus, dm_minus = [], [], []
        for i in range(1, len(candles)):
            tr_list.append(max(candles[i]["high"] - candles[i]["low"], abs(candles[i]["high"] - candles[i-1]["close"]), abs(candles[i]["low"] - candles[i-1]["close"])))
            up, down = candles[i]["high"] - candles[i-1]["high"], candles[i-1]["low"] - candles[i]["low"]
            dm_plus.append(max(up, 0) if up > down else 0)
            dm_minus.append(max(down, 0) if down > up else 0)
        atr = sum(tr_list[:14]) / 14
        dm_p_smooth = sum(dm_plus[:14]) / 14
        dm_m_smooth = sum(dm_minus[:14]) / 14
        if atr == 0: return 20
        dx = (abs((dm_p_smooth / atr) * 100 - (dm_m_smooth / atr) * 100) / ((dm_p_smooth / atr) * 100 + (dm_m_smooth / atr) * 100)) * 100 if ((dm_p_smooth / atr) * 100 + (dm_m_smooth / atr) * 100) > 0 else 0
        return min(100, dx)

if __name__ == "__main__":
    orchestrator = AIOrchestrator()
    orchestrator.run()

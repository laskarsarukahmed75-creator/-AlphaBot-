import math
from typing import List, Dict, Optional, Tuple

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



#!/usr/bin/env python3
"""
app.py – Institutional‑Grade Crypto AI Analyst v3.1 (Persistent DB + Anti‑Sleep)
Data Source: Binance Public WebSocket (No API Keys)
Target Win‑Rate: 75‑85% | Horizon: 1 Hour
Features: Layered Filtering, Multi‑Timeframe, Smart Money Concepts, Dynamic Risk, Trade Journal, Health API
"""

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
from typing import Dict, Any, List, Optional, Tuple
from collections import deque
from datetime import datetime
import math

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

# ---- Configuration ----
class Config:
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    DISPLAY_NAMES = {"BTCUSDT": "BTC/USDT", "ETHUSDT": "ETH/USDT", "SOLUSDT": "SOL/USDT"}
    MIN_CONFLUENCE_SCORE = 70
    MIN_LAYER_PASS = 5
    SIGNAL_COOLDOWN = 3600
    DB_PATH = "trades.db"
    MAX_CANDLES = 500
    VOLATILITY_MULTIPLIERS = {
        "low": (1.2, 2.0),
        "medium": (1.5, 2.5),
        "high": (1.8, 3.0),
        "extreme": (2.0, 3.5)
    }

# =====================================================================
# DATABASE (Trade Journal + Rejected Logs + Performance)
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

        # Gross profit and loss
        cur.execute("SELECT SUM(pnl) FROM trades WHERE status='closed' AND pnl > 0")
        gross_profit = cur.fetchone()[0] or 0.0
        cur.execute("SELECT SUM(pnl) FROM trades WHERE status='closed' AND pnl < 0")
        gross_loss = cur.fetchone()[0] or 0.0
        gross_loss = abs(gross_loss)

        total_pnl = gross_profit - gross_loss
        avg_pnl = total_pnl / total if total else 0.0
        win_rate = wins / total if total else 0.0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0.0)

        return {
            "total_trades": total,
            "winning_trades": wins,
            "losing_trades": total - wins,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "total_pnl": total_pnl,
            "avg_pnl": avg_pnl
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
# NEWS SCANNER (Enhanced with importance weighting)
# =====================================================================
class CryptoNewsScanner:
    def __init__(self):
        self.last_news = {}
        self.sentiment_history = deque(maxlen=10)
        self.importance_map = {
            "etf": 1.0,
            "cpi": 0.9,
            "fomc": 0.9,
            "liquidation": 0.7,
            "whale": 0.6,
            "institutional": 0.8,
            "regulation": 0.7,
            "adoption": 0.5,
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
        bullish_words = [
            "bullish", "breakout", "surge", "buy", "accumulate", "growth",
            "approved", "institutional", "inflows", "rally", "recovery",
            "adoption", "partnership", "upgrade", "positive", "moon", "pump"
        ]
        bearish_words = [
            "bearish", "crash", "dump", "sell", "liquidation", "ban",
            "hack", "regulatory", "outflows", "decline", "drop",
            "rejection", "warning", "negative", "concern", "fud", "panic"
        ]
        text = (title + " " + body).lower()
        bull_score = sum(1 for w in bullish_words if w in text)
        bear_score = sum(1 for w in bearish_words if w in text)
        total = bull_score + bear_score
        net_score = ((bull_score - bear_score) / total * 100) if total else 0
        if net_score > 20:
            label = "BULLISH"
        elif net_score < -20:
            label = "BEARISH"
        else:
            label = "NEUTRAL"
        return {"score": net_score, "label": label, "bullish_count": bull_score, "bearish_count": bear_score}

    def _compute_importance(self, text: str) -> float:
        text_lower = text.lower()
        max_imp = 0.0
        for word, imp in self.importance_map.items():
            if word in text_lower:
                max_imp = max(max_imp, imp)
        return max_imp

# =====================================================================
# CANDLE TOPOLOGY ENGINE (Enhanced with Market Structure, FVG, OB)
# =====================================================================
class CandleTopologyEngine:
    def __init__(self):
        self.history = {asset: deque(maxlen=200) for asset in Config.ASSETS}
        self.candles = {
            tf: {asset: [] for asset in Config.ASSETS}
            for tf in [60, 300, 900, 3600]
        }
        self.support_resistance = {asset: {"support": [], "resistance": []} for asset in Config.ASSETS}
        self.trendlines = {asset: {"up": [], "down": []} for asset in Config.ASSETS}
        self.pivots = {asset: {"high": [], "low": []} for asset in Config.ASSETS}
        self.bos = {asset: {"break": None, "direction": ""} for asset in Config.ASSETS}
        self.choch = {asset: False for asset in Config.ASSETS}
        self.fvgs = {asset: [] for asset in Config.ASSETS}
        self.order_blocks = {asset: {} for asset in Config.ASSETS}
        self.last_tick_time = {asset: 0 for asset in Config.ASSETS}

    def process_tick(self, asset: str, price: float, volume: float):
        now = int(time.time())
        self.history[asset].append({"price": price, "volume": volume, "time": now})
        for tf in [60, 300, 900, 3600]:
            self._build_candle(asset, price, volume, now, tf, self.candles[tf][asset])
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
                # NEW: Save finalized candle to local persistent DB
                db = DatabaseHandler()
                db.save_candle(asset, tf, storage[-1])
                
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
            candle = storage[-1]
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price
            candle["volume"] += volume

    def _update_pivots(self, asset: str, price: float):
        candles = self.candles[900][asset]
        if len(candles) < 10:
            return
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < 10:
            return
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
            last_high = piv_high[0] if piv_high else None
            last_low = piv_low[0] if piv_low else None
            prev_high = piv_high[1] if len(piv_high) > 1 else None
            prev_low = piv_low[1] if len(piv_low) > 1 else None
            if last_high and prev_high and last_high > prev_high:
                self.bos[asset] = {"break": last_high, "direction": "UP"}
            elif last_low and prev_low and last_low < prev_low:
                self.bos[asset] = {"break": last_low, "direction": "DOWN"}
            if len(piv_low) >= 3 and len(piv_high) >= 3:
                if piv_low[1] > piv_low[2] and piv_high[1] < piv_high[2]:
                    self.choch[asset] = True
                else:
                    self.choch[asset] = False

    def _update_support_resistance(self, asset: str, price: float):
        piv_high = self.pivots[asset]["high"]
        piv_low = self.pivots[asset]["low"]
        all_levels = piv_high + piv_low
        clusters = []
        for level in sorted(all_levels):
            if not clusters or abs(level - clusters[-1]) / level > 0.005:
                clusters.append(level)
        supports = [l for l in clusters if l < price * 0.99]
        resistances = [l for l in clusters if l > price * 1.01]
        self.support_resistance[asset]["support"] = sorted(supports)[-5:] if supports else []
        self.support_resistance[asset]["resistance"] = sorted(resistances, reverse=True)[:5] if resistances else []

    def _update_trendlines(self, asset: str, price: float):
        candles = self.candles[900][asset]
        if len(candles) < 30:
            return
        closes = [c["close"] for c in candles[-30:] if c.get("complete", False)]
        if len(closes) < 20:
            return
        ema_short = self._ema(closes, 9)
        ema_long = self._ema(closes, 21)
        if len(ema_short) > 1 and len(ema_long) > 1:
            slope_short = ema_short[-1] - ema_short[-2]
            slope_long = ema_long[-1] - ema_long[-2]
            if slope_short > 0 and slope_long > 0:
                if not self.trendlines[asset]["up"] or price > self.trendlines[asset]["up"][-1]:
                    self.trendlines[asset]["up"].append(price)
                    self.trendlines[asset]["up"] = self.trendlines[asset]["up"][-5:]
            elif slope_short < 0 and slope_long < 0:
                if not self.trendlines[asset]["down"] or price < self.trendlines[asset]["down"][-1]:
                    self.trendlines[asset]["down"].append(price)
                    self.trendlines[asset]["down"] = self.trendlines[asset]["down"][-5:]

    def _ema(self, series: List[float], period: int) -> List[float]:
        if len(series) < period:
            return []
        ema = [sum(series[:period]) / period]
        multiplier = 2 / (period + 1)
        for i in range(period, len(series)):
            ema.append((series[i] - ema[-1]) * multiplier + ema[-1])
        return ema

    def detect_candle_patterns(self, asset: str) -> Dict:
        candles = self.candles[300][asset]
        if len(candles) < 5:
            return {}
        patterns = {}
        last = candles[-1]
        prev = candles[-2] if len(candles) > 1 else None
        if not last.get("complete", False):
            return {}
        body = abs(last["close"] - last["open"])
        upper_wick = last["high"] - max(last["open"], last["close"])
        lower_wick = min(last["open"], last["close"]) - last["low"]
        total_range = last["high"] - last["low"]
        if total_range > 0:
            upper_ratio = upper_wick / total_range
            lower_ratio = lower_wick / total_range
            body_ratio = body / total_range
            if lower_ratio > 0.6 and body_ratio < 0.4:
                patterns["bullish_rejection"] = {"strength": min(100, int(lower_ratio*100)), "logic": "Bullish pin bar"}
            if upper_ratio > 0.6 and body_ratio < 0.4:
                patterns["bearish_rejection"] = {"strength": min(100, int(upper_ratio*100)), "logic": "Bearish pin bar"}
            if body_ratio < 0.15:
                patterns["doji"] = {"strength": 70, "logic": "Doji – indecision"}
            if upper_ratio < 0.05 and lower_ratio < 0.05 and body_ratio > 0.8:
                if last["close"] > last["open"]:
                    patterns["bullish_marubozu"] = {"strength": 85, "logic": "Strong bullish momentum"}
                else:
                    patterns["bearish_marubozu"] = {"strength": 85, "logic": "Strong bearish momentum"}
        if prev and last.get("complete", False) and prev.get("complete", False):
            prev_body = abs(prev["close"] - prev["open"])
            if prev_body > 0:
                if (prev["close"] < prev["open"] and 
                    last["close"] > last["open"] and
                    last["close"] > prev["open"] and
                    last["open"] < prev["close"]):
                    patterns["bullish_engulfing"] = {"strength": min(100, int((body / prev_body) * 50)), "logic": "Bullish engulfing"}
                if (prev["close"] > prev["open"] and 
                    last["close"] < last["open"] and
                    last["close"] < prev["open"] and
                    last["open"] > prev["close"]):
                    patterns["bearish_engulfing"] = {"strength": min(100, int((body / prev_body) * 50)), "logic": "Bearish engulfing"}
        if len(candles) >= 3:
            c1 = candles[-3]
            c2 = candles[-2]
            c3 = candles[-1]
            if (c1.get("complete", False) and c2.get("complete", False) and c3.get("complete", False)):
                if (c1["close"] < c1["open"] and abs(c2["close"]-c2["open"]) < (c2["high"]-c2["low"])*0.2 and
                    c3["close"] > c3["open"] and c3["close"] > (c1["open"]+c1["close"])/2):
                    patterns["morning_star"] = {"strength": 80, "logic": "Morning star reversal"}
                if (c1["close"] > c1["open"] and abs(c2["close"]-c2["open"]) < (c2["high"]-c2["low"])*0.2 and
                    c3["close"] < c3["open"] and c3["close"] < (c1["open"]+c1["close"])/2):
                    patterns["evening_star"] = {"strength": 80, "logic": "Evening star reversal"}
        return patterns

    def get_atr(self, asset: str, period: int = 14) -> float:
        candles = self.candles[3600][asset]
        if len(candles) < period:
            return 0.0
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < period:
            return 0.0
        tr_list = []
        for i in range(1, len(complete)):
            high = complete[i]["high"]
            low = complete[i]["low"]
            prev_close = complete[i-1]["close"]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_list.append(tr)
        if len(tr_list) < period:
            return 0.0
        return sum(tr_list[-period:]) / period

    def _update_fvgs(self, asset: str):
        candles = self.candles[900][asset]
        if len(candles) < 3:
            return
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < 3:
            return
        fvgs = []
        for i in range(2, len(complete)-1):
            c1 = complete[i-2]
            c2 = complete[i-1]
            c3 = complete[i]
            if c1["close"] < c2["open"] and c2["close"] < c3["close"]:
                upper = c1["high"]
                lower = c2["low"]
                if upper > lower:
                    mitigated = False
                    for j in range(i+1, len(complete)):
                        if complete[j]["low"] <= upper and complete[j]["high"] >= lower:
                            mitigated = True
                            break
                    if not mitigated:
                        fvgs.append({"type": "bullish", "upper": upper, "lower": lower, "timestamp": c2["timestamp"]})
            if c1["close"] > c2["open"] and c2["close"] > c3["close"]:
                upper = c2["high"]
                lower = c1["low"]
                if upper > lower:
                    mitigated = False
                    for j in range(i+1, len(complete)):
                        if complete[j]["low"] <= upper and complete[j]["high"] >= lower:
                            mitigated = True
                            break
                    if not mitigated:
                        fvgs.append({"type": "bearish", "upper": upper, "lower": lower, "timestamp": c2["timestamp"]})
        self.fvgs[asset] = fvgs[-5:]

    def detect_fvg(self, asset: str) -> List[Dict]:
        return self.fvgs[asset]

    def _update_order_blocks(self, asset: str, price: float):
        if not self.bos[asset]["direction"]:
            return
        candles = self.candles[900][asset]
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < 10:
            return
        atr = self.get_atr(asset)
        if atr == 0:
            return
        for i in range(len(complete)-1, -1, -1):
            c = complete[i]
            if (c["high"] - c["low"]) > 1.5 * atr:
                ob_type = "bullish" if c["close"] > c["open"] else "bearish"
                mitigated = False
                for j in range(i+1, len(complete)):
                    if complete[j]["low"] <= c["high"] and complete[j]["high"] >= c["low"]:
                        mitigated = True
                        break
                if not mitigated:
                    self.order_blocks[asset] = {
                        "type": ob_type,
                        "high": c["high"],
                        "low": c["low"],
                        "timestamp": c["timestamp"],
                        "direction": self.bos[asset]["direction"]
                    }
                    return
        self.order_blocks[asset] = {}

    def detect_order_block(self, asset: str) -> Dict:
        return self.order_blocks[asset]

    def detect_liquidity_sweep(self, asset: str, price: float) -> str:
        piv_high = self.pivots[asset]["high"]
        piv_low = self.pivots[asset]["low"]
        if piv_high and price > max(piv_high[-2:]):
            return "BUY_SWEEP"
        if piv_low and price < min(piv_low[-2:]):
            return "SELL_SWEEP"
        return ""

    def get_volatility_regime(self, asset: str) -> str:
        atr = self.get_atr(asset)
        if atr == 0:
            return "medium"
        candles = self.candles[3600][asset]
        complete = [c for c in candles if c.get("complete", False)]
        if len(complete) < 50:
            return "medium"
        atr_list = []
        for i in range(50, len(complete)):
            tr = max(complete[i]["high"] - complete[i]["low"],
                     abs(complete[i]["high"] - complete[i-1]["close"]),
                     abs(complete[i]["low"] - complete[i-1]["close"]))
            atr_list.append(tr)
        avg_atr = sum(atr_list) / len(atr_list) if atr_list else atr
        ratio = atr / avg_atr if avg_atr > 0 else 1.0
        if ratio < 0.7:
            return "low"
        elif ratio < 1.3:
            return "medium"
        elif ratio < 2.0:
            return "high"
        else:
            return "extreme"

    def get_visual_topology(self, asset: str, price: float, direction: str,
                            sl: float, tp: float, patterns: Dict) -> str:
        sr = self.support_resistance[asset]
        supports = sr["support"][-3:] if sr["support"] else [price * 0.98]
        resistances = sr["resistance"][-3:] if sr["resistance"] else [price * 1.02]
        min_price = min(min(supports) if supports else price * 0.97, sl, price * 0.97)
        max_price = max(max(resistances) if resistances else price * 1.03, tp, price * 1.03)
        rows = 10
        chart_lines = []
        chart_lines.append("┌──────────────────────────────────────┐")
        chart_lines.append("│           LIVE TOPOLOGY CHART         │")
        chart_lines.append("├──────────────────────────────────────┤")
        for i in range(rows, -1, -1):
            level_price = min_price + (max_price - min_price) * (i / rows)
            level_str = f"{level_price:>8.2f}"
            marker = " "
            if any(abs(level_price - s) / s < 0.001 for s in supports):
                marker = "S"
            if any(abs(level_price - r) / r < 0.001 for r in resistances):
                marker = "R"
            if abs(level_price - price) / price < 0.001:
                marker = "●" if direction == "BUY" else "○"
            if abs(level_price - sl) / sl < 0.001:
                marker = "▼" if direction == "BUY" else "▲"
            if abs(level_price - tp) / tp < 0.001:
                marker = "★"
            bar = "█" * int((i / rows) * 10) if i > 0 else ""
            chart_lines.append(f"│ {level_str} │ {marker} {bar:<10} │")
        chart_lines.append("├──────────────────────────────────────┤")
        chart_lines.append("│ S=Support  R=Resistance  ●=Entry    │")
        chart_lines.append("│ ▼=SL  ▲=SL  ★=Target              │")
        chart_lines.append("└──────────────────────────────────────┘")
        if patterns:
            chart_lines.append("")
            chart_lines.append("📊 <b>Detected Patterns:</b>")
            for name, info in patterns.items():
                chart_lines.append(f"  • {name.replace('_', ' ').title()}: {info['logic']}")
                chart_lines.append(f"    Strength: {info['strength']}%")
        return "\n".join(chart_lines)

# =====================================================================
# LAYERED SIGNAL SCORING ENGINE (Dynamic Filters)
# =====================================================================
class SignalScoringEngine:
    def __init__(self):
        self.weights = {
            "htf_trend": 15,
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
        reasons = []
        breakdown = {}
        total_score = 0

        # 1. HTF Trend
        htf_score = 0
        if htf_trend == "BULLISH" and trend == "BULLISH":
            htf_score = self.weights["htf_trend"]
            passed_layers.append("htf_trend")
        elif htf_trend == "BEARISH" and trend == "BEARISH":
            htf_score = self.weights["htf_trend"]
            passed_layers.append("htf_trend")
        elif htf_trend == "NEUTRAL":
            htf_score = self.weights["htf_trend"] * 0.5
        breakdown["htf_trend"] = {"score": htf_score, "weight": self.weights["htf_trend"], "pass": htf_score>0}
        total_score += htf_score

        # 2. Market Structure
        structure_score = 0
        if bos and bos["direction"]:
            structure_score = self.weights["market_structure"] * 0.7
            if choch:
                structure_score = self.weights["market_structure"]
            passed_layers.append("market_structure")
        breakdown["market_structure"] = {"score": structure_score, "weight": self.weights["market_structure"], "pass": structure_score>0}
        total_score += structure_score

        # 3. Liquidity Sweep
        ls_score = 0
        if liquidity_sweep:
            ls_score = self.weights["liquidity_sweep"]
            passed_layers.append("liquidity_sweep")
        breakdown["liquidity_sweep"] = {"score": ls_score, "weight": self.weights["liquidity_sweep"], "pass": ls_score>0}
        total_score += ls_score

        # 4. FVG
        fvg_score = 0
        for fvg in fvgs:
            if fvg["lower"] < price < fvg["upper"]:
                fvg_score = self.weights["fvg"]
                passed_layers.append("fvg")
                break
        breakdown["fvg"] = {"score": fvg_score, "weight": self.weights["fvg"], "pass": fvg_score>0}
        total_score += fvg_score

        # 5. Order Block
        ob_score = 0
        if order_block and order_block.get("type"):
            ob_score = self.weights["order_block"]
            passed_layers.append("order_block")
        breakdown["order_block"] = {"score": ob_score, "weight": self.weights["order_block"], "pass": ob_score>0}
        total_score += ob_score

        # 6. Volume
        vol_score = 0
        if volume_ratio > 1.2:
            vol_score = self.weights["volume"]
            passed_layers.append("volume")
        elif volume_ratio > 0.8:
            vol_score = self.weights["volume"] * 0.5
        breakdown["volume"] = {"score": vol_score, "weight": self.weights["volume"], "pass": vol_score>0}
        total_score += vol_score

        # 7. RSI (context-aware)
        rsi_score = 0
        if adx > 25:
            rsi_score = self.weights["rsi"]
            passed_layers.append("rsi")
        else:
            if 30 <= rsi <= 70:
                rsi_score = self.weights["rsi"]
                passed_layers.append("rsi")
            elif 20 <= rsi < 30 or 70 < rsi <= 80:
                rsi_score = self.weights["rsi"] * 0.5
            else:
                rsi_score = 0
        breakdown["rsi"] = {"score": rsi_score, "weight": self.weights["rsi"], "pass": rsi_score>0}
        total_score += rsi_score

        # 8. ADX
        adx_score = 0
        if adx > 25:
            adx_score = self.weights["adx"]
            passed_layers.append("adx")
        elif adx > 20:
            adx_score = self.weights["adx"] * 0.5
        breakdown["adx"] = {"score": adx_score, "weight": self.weights["adx"], "pass": adx_score>0}
        total_score += adx_score

        # 9. News
        news_score = 0
        if news_sentiment != 0 and news_importance > 0.5:
            if (trend == "BULLISH" and news_sentiment > 0) or (trend == "BEARISH" and news_sentiment < 0):
                news_score = self.weights["news"] * news_importance
                passed_layers.append("news")
            elif abs(news_sentiment) > 50:
                news_score = self.weights["news"] * 0.5
        breakdown["news"] = {"score": news_score, "weight": self.weights["news"], "pass": news_score>0}
        total_score += news_score

        num_passed = len(passed_layers)
        enough = num_passed >= self.min_pass_layers
        final_score = min(100, total_score)
        confidence = "HIGH" if final_score >= 70 else "MEDIUM" if final_score >= 50 else "LOW"
        probability = 50 + (final_score - 50) * 0.6

        return {
            "total_score": final_score,
            "breakdown": breakdown,
            "confidence": confidence,
            "probability": probability,
            "passed_layers": passed_layers,
            "enough": enough,
            "num_passed": num_passed,
            "reasons": reasons
        }

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
                url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                payload = {"chat_id": self.chat_id, "text": msg, "parse_mode": "HTML"}
                requests.post(url, data=payload, timeout=10)
            except Exception as e:
                logger.error(f"Telegram error: {e}")

    def fire_signal(self, asset: str, direction: str, price: float, sl: float, tp: float,
                    topology_chart: str, logic: str, news: str, score: Dict, patterns: Dict,
                    trade_id: int, session: str, entry_zone: str, exit_zone: str,
                    win_prob: float, rr: float):
        icon = "🔥" if direction == "BUY" else "❄️"
        strength = "STRONG" if score["total_score"] >= 70 else "MODERATE"
        display = Config.DISPLAY_NAMES.get(asset, asset)
        msg = (
            f"{icon} <b>AI SIGNAL: {direction}</b> {icon}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 {display} | 🆔 #{trade_id}\n"
            f"⏰ {session} | ⚡ {strength} ({score['total_score']:.0f}%)\n"
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

    def fire_diagnostic(self, asset: str, score: float, trend: str, patterns: List, vol_ratio: float, avg_score: float):
        msg = (
            f"🔍 DIAG - {asset}\n"
            f"Score: {score:.0f} | Avg: {avg_score:.1f}\n"
            f"Trend: {trend} | Patterns: {', '.join(patterns) if patterns else 'None'}\n"
            f"Vol Ratio: {vol_ratio:.2f}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        self.queue.put(msg)

# =====================================================================
# BINANCE PUBLIC WEBSOCKET (with heartbeat & reconnect)
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
                stream_str = "/".join(streams)
                ws_url = f"wss://stream.binance.com:9443/stream?streams={stream_str}"
                self.ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                    on_pong=self._on_pong
                )
                self.ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.error(f"WebSocket loop error: {e}")
                self.reconnect_count += 1
                time.sleep(5)

    def _on_pong(self, ws, data):
        self.latency = time.time() - self.last_ping if self.last_ping else 0

    def _on_open(self, ws):
        logger.info("Binance Public WebSocket connected.")
        self.reconnect_count = 0

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            if "data" in data and "k" in data["data"]:
                kline = data["data"]["k"]
                symbol = kline.get("s")
                if symbol not in Config.ASSETS:
                    return
                price = float(kline.get("c", 0))
                volume = float(kline.get("v", 0))
                if price > 0:
                    self.on_price_update(symbol, price, volume)
            elif "k" in data:
                kline = data["k"]
                symbol = kline.get("s")
                if symbol not in Config.ASSETS:
                    return
                price = float(kline.get("c", 0))
                volume = float(kline.get("v", 0))
                if price > 0:
                    self.on_price_update(symbol, price, volume)
        except Exception as e:
            logger.debug(f"Message parse error: {e}")

    def _on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning("Binance WebSocket disconnected")
        self.reconnect_count += 1

# =====================================================================
# HEALTH SERVER (Enhanced with more metrics)
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
            cpu = psutil.cpu_percent() if HAS_PSUTIL else 0
            mem = psutil.virtual_memory().percent if HAS_PSUTIL else 0
            qsize = orchestrator.telegram.queue.qsize() if orchestrator else 0
            last_tick = orchestrator.topology.last_tick_time if orchestrator else {}
            last_signal = orchestrator.last_signal_time if orchestrator else {}
            db_size = orchestrator.db.get_db_size() if orchestrator else 0
            candle_delay = 0
            if orchestrator:
                for asset in Config.ASSETS:
                    candles = orchestrator.topology.candles[60][asset]
                    if candles and candles[-1].get("complete", False):
                        candle_delay = max(candle_delay, int(time.time()) - candles[-1]["timestamp"] - 60)
            status = {
                "status": "online",
                "version": "3.1",
                "uptime": uptime,
                "cpu_percent": cpu,
                "memory_percent": mem,
                "queue_size": qsize,
                "db_size_bytes": db_size,
                "last_tick": last_tick,
                "last_signal": last_signal,
                "reconnect_count": orchestrator.stream.reconnect_count if orchestrator and orchestrator.stream else 0,
                "ws_latency": orchestrator.stream.latency if orchestrator and orchestrator.stream else 0,
                "candle_delay_seconds": candle_delay,
                "signal_counts": {
                    "accepted": orchestrator.accepted_signals if orchestrator else 0,
                    "rejected": orchestrator.rejected_signals if orchestrator else 0
                }
            }
            self.wfile.write(json.dumps(status).encode())
    HTTPServer(("0.0.0.0", port), HealthHandler).serve_forever()

# =====================================================================
# CORE ORCHESTRATOR (Enhanced with Multi-Timeframe, Layered Scoring)
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
        self._last_score_log = 0
        self.asset_state = {asset: {
            "trend": "NEUTRAL",
            "htf_trend": "NEUTRAL",
            "volume_ratio": 1.0,
            "rsi": 50,
            "adx": 20,
            "volatility": 0.01,
            "score_history": deque(maxlen=12),
            "news_sentiment": 0,
            "news_importance": 0
        } for asset in Config.ASSETS}
        self.signal_count = 0
        self.rejected_signals = 0
        self.accepted_signals = 0

    # ---- NEW INTEGRATION: RECOVER FROM DATABASE ON BOOT ----
    def _recover_from_db(self):
        logger.info("📂 Checking local persistent database for cached historical candles...")
        db = DatabaseHandler()
        for asset in Config.ASSETS:
            for tf in [60, 300, 900, 3600]:
                candles = db.load_candles(asset, tf, limit=Config.MAX_CANDLES)
                if candles:
                    self.topology.candles[tf][asset] = candles
                    self.topology.last_tick_time[asset] = candles[-1]["timestamp"]
                    logger.info(f"✅ Loaded {len(candles)} cached candles for {asset} ({tf}s) from local DB.")
                    self._fetch_missing_candles(asset, tf, candles[-1]["timestamp"])
                else:
                    logger.info(f"📥 Empty database for {asset} ({tf}s). Cold seeding 3 months history...")
                    start_ts = int(time.time()) - (90 * 24 * 3600)
                    self._fetch_missing_candles(asset, tf, start_ts)

    # ---- NEW INTEGRATION: SYNC MISSING CANDLES/GAPS FROM BINANCE REST ----
    def _fetch_missing_candles(self, asset, tf, since_ts):
        limit = 1000
        url = "https://api.binance.com/api/v3/klines"
        interval_map = {60: "1m", 300: "5m", 900: "15m", 3600: "1h"}
        interval = interval_map.get(tf, "1m")
        params = {
            "symbol": asset,
            "interval": interval,
            "limit": limit,
            "startTime": since_ts * 1000 if since_ts else None
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                db = DatabaseHandler()
                added_count = 0
                for item in data:
                    candle = {
                        "timestamp": item[0] // 1000,
                        "open": float(item[1]),
                        "high": float(item[2]),
                        "low": float(item[3]),
                        "close": float(item[4]),
                        "volume": float(item[5]),
                        "complete": True
                    }
                    if self.topology.candles[tf][asset] and candle["timestamp"] <= self.topology.candles[tf][asset][-1]["timestamp"]:
                        continue
                    self.topology.candles[tf][asset].append(candle)
                    db.save_candle(asset, tf, candle)
                    added_count += 1
                logger.info(f"🧱 Synced {added_count} missing/historical data gap for {asset} ({interval})")
                
                if len(self.topology.candles[tf][asset]) > Config.MAX_CANDLES:
                    self.topology.candles[tf][asset] = self.topology.candles[tf][asset][-Config.MAX_CANDLES:]
        except Exception as e:
            logger.error(f"Error executing REST history sync for {asset}: {e}")

    # ---- NEW INTEGRATION: DAILY FIFO PRUNING ENGINE ----
    def _prune_old_data(self):
        cutoff = int(time.time()) - (90 * 24 * 3600)
        db = DatabaseHandler()
        for asset in Config.ASSETS:
            for tf in [60, 300, 900, 3600]:
                db.delete_older_than(asset, tf, cutoff)
        logger.info("✂️ Daily Pruning Completed: Removed historical items older than 3 months.")

    def run(self):
        logger.info("🚀 Institutional AI Analyst v3.1 (Persistent DB) Starting...")
        
        # 1. NEW: Cold Boot Memory Restoration Layer
        self._recover_from_db()
        
        # 2. NEW: Initialize Internal Anti-Sleep Engine Thread
        ka = KeepAliveEngine()
        ka.start()

        self.stream = BinancePublicStream(on_price_update=self._handle_price_tick)
        self.stream.start()
        
        self.telegram.fire_news_alert(
            "AI v3.1 Online - Layered Filtering + Smart Money Concepts (Persistent DB Layer)",
            "BULLISH", 85, 1.0
        )
        threading.Thread(target=start_health_server, args=(self,), daemon=True).start()
        
        last_prune = time.time()
        while True:
            try:
                time.sleep(15)
                
                # NEW: Trigger internal data management pruning engine once per day
                if time.time() - last_prune > 86400:
                    self._prune_old_data()
                    last_prune = time.time()
                    
                if int(time.time()) - self.last_news_time > 30:
                    news_data = self.news.fetch_latest()
                    if news_data.get("fresh") and news_data.get("articles"):
                        for article in news_data["articles"][:2]:
                            self.telegram.fire_news_alert(
                                article["title"],
                                article["sentiment"]["label"],
                                article["sentiment"]["score"],
                                article["importance"]
                            )
                            for asset in Config.ASSETS:
                                self.asset_state[asset]["news_sentiment"] = article["sentiment"]["score"]
                                self.asset_state[asset]["news_importance"] = article["importance"]
                            self.last_news_time = int(time.time())

                now = time.time()
                if now - self._last_score_log > 300:
                    self._log_scores()
                    self._last_score_log = now
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}")

    def _log_scores(self):
        for asset in Config.ASSETS:
            candles = self.topology.candles[900][asset]
            if len(candles) < 10:
                continue
            price = candles[-1]["close"] if candles else 0
            patterns = self.topology.detect_candle_patterns(asset)
            sr_data = self.topology.support_resistance[asset]
            trend = self.asset_state[asset]["trend"]
            vol_ratio = self.asset_state[asset]["volume_ratio"]
            rsi = self.asset_state[asset]["rsi"]
            adx = self.asset_state[asset]["adx"]
            vol = self.asset_state[asset]["volatility"]
            htf_trend = self.asset_state[asset]["htf_trend"]
            bos = self.topology.bos[asset]
            choch = self.topology.choch[asset]
            fvgs = self.topology.detect_fvg(asset)
            ob = self.topology.detect_order_block(asset)
            liquidity_sweep = self.topology.detect_liquidity_sweep(asset, price)
            news_sent = self.asset_state[asset]["news_sentiment"]
            news_imp = self.asset_state[asset]["news_importance"]

            score = self.scoring.evaluate(
                asset=asset, price=price, patterns=patterns, sr_data=sr_data,
                trend=trend, news_sentiment=news_sent, volume_ratio=vol_ratio,
                rsi=rsi, adx=adx, volatility=vol,
                htf_trend=htf_trend, bos=bos, choch=choch,
                fvgs=fvgs, order_block=ob, liquidity_sweep=liquidity_sweep,
                news_importance=news_imp
            )
            self.asset_state[asset]["score_history"].append(score["total_score"])
            avg_score = sum(self.asset_state[asset]["score_history"]) / max(1, len(self.asset_state[asset]["score_history"]))
            pattern_names = list(patterns.keys())
            self.telegram.fire_diagnostic(
                asset, score["total_score"], trend, pattern_names, vol_ratio, avg_score
            )
            logger.info(f"🔍 {asset} DIAG -> Score: {score['total_score']:.0f} | Avg: {avg_score:.1f} | Passed: {score['num_passed']}/9")

    def _handle_price_tick(self, asset: str, price: float, volume: float):
        self.topology.process_tick(asset, price, volume)

        candles_15m = self.topology.candles[900][asset]
        if len(candles_15m) > 10:
            closes = [c["close"] for c in candles_15m if c.get("complete", False)]
            if len(closes) > 10:
                ema_short = self.topology._ema(closes, 9)
                ema_long = self.topology._ema(closes, 21)
                if len(ema_short) > 1 and len(ema_long) > 1:
                    if ema_short[-1] > ema_long[-1]:
                        self.asset_state[asset]["trend"] = "BULLISH"
                    elif ema_short[-1] < ema_long[-1]:
                        self.asset_state[asset]["trend"] = "BEARISH"
                if len(closes) >= 14:
                    self.asset_state[asset]["rsi"] = self._calculate_rsi(closes, 14)
                if len(closes) >= 14:
                    self.asset_state[asset]["adx"] = self._calculate_adx(candles_15m[-14:])

        candles_1h = self.topology.candles[3600][asset]
        if len(candles_1h) > 10:
            closes_1h = [c["close"] for c in candles_1h if c.get("complete", False)]
            if len(closes_1h) > 10:
                ema_short_1h = self.topology._ema(closes_1h, 9)
                ema_long_1h = self.topology._ema(closes_1h, 21)
                if len(ema_short_1h) > 1 and len(ema_long_1h) > 1:
                    if ema_short_1h[-1] > ema_long_1h[-1]:
                        self.asset_state[asset]["htf_trend"] = "BULLISH"
                    elif ema_short_1h[-1] < ema_long_1h[-1]:
                        self.asset_state[asset]["htf_trend"] = "BEARISH"
                    else:
                        self.asset_state[asset]["htf_trend"] = "NEUTRAL"

        vols = [c["volume"] for c in self.topology.candles[300][asset] if c.get("complete", False)]
        if len(vols) > 10:
            avg_vol = sum(vols[-10:-1]) / max(1, len(vols[-10:-1]))
            if avg_vol > 0:
                self.asset_state[asset]["volume_ratio"] = vols[-1] / avg_vol if vols else 1.0

        atr = self.topology.get_atr(asset)
        if atr > 0 and price > 0:
            self.asset_state[asset]["volatility"] = atr / price

        now = time.time()
        if now - self.last_signal_time[asset] < Config.SIGNAL_COOLDOWN:
            return

        patterns = self.topology.detect_candle_patterns(asset)
        sr_data = self.topology.support_resistance[asset]
        rsi = self.asset_state[asset]["rsi"]
        adx = self.asset_state[asset]["adx"]
        vol = self.asset_state[asset]["volatility"]
        vol_ratio = self.asset_state[asset]["volume_ratio"]
        trend = self.asset_state[asset]["trend"]
        htf_trend = self.asset_state[asset]["htf_trend"]
        bos = self.topology.bos[asset]
        choch = self.topology.choch[asset]
        fvgs = self.topology.detect_fvg(asset)
        ob = self.topology.detect_order_block(asset)
        liquidity_sweep = self.topology.detect_liquidity_sweep(asset, price)
        news_sent = self.asset_state[asset]["news_sentiment"]
        news_imp = self.asset_state[asset]["news_importance"]

        score = self.scoring.evaluate(
            asset=asset, price=price, patterns=patterns, sr_data=sr_data,
            trend=trend, news_sentiment=news_sent, volume_ratio=vol_ratio,
            rsi=rsi, adx=adx, volatility=vol,
            htf_trend=htf_trend, bos=bos, choch=choch,
            fvgs=fvgs, order_block=ob, liquidity_sweep=liquidity_sweep,
            news_importance=news_imp
        )

        if not score["enough"] or score["total_score"] < Config.MIN_CONFLUENCE_SCORE:
            reason = f"Not enough confluences ({score['num_passed']}/9) or low score ({score['total_score']:.0f})"
            self.db.log_rejected(asset, price, score["total_score"], reason, vol, self.topology.get_volatility_regime(asset))
            self.rejected_signals += 1
            logger.debug(f"{asset} Rejected: {reason}")
            return

        if htf_trend == "BULLISH" and trend == "BULLISH":
            direction = "BUY"
        elif htf_trend == "BEARISH" and trend == "BEARISH":
            direction = "SELL"
        else:
            bullish_score = sum(info.get("strength",0) for name,info in patterns.items() if "bullish" in name)
            bearish_score = sum(info.get("strength",0) for name,info in patterns.items() if "bearish" in name)
            if trend == "BULLISH" and bullish_score >= bearish_score:
                direction = "BUY"
            elif trend == "BEARISH" and bearish_score >= bullish_score:
                direction = "SELL"
            else:
                direction = "BUY" if bullish_score > bearish_score else "SELL" if bearish_score > bullish_score else None
        if direction is None:
            return

        atr = self.topology.get_atr(asset)
        if atr == 0:
            atr = price * 0.01
        regime = self.topology.get_volatility_regime(asset)
        sl_mult, tp_mult = Config.VOLATILITY_MULTIPLIERS.get(regime, (1.5, 2.5))
        if direction == "BUY":
            sl = price - sl_mult * atr
            tp = price + tp_mult * atr
            if sr_data["support"]:
                nearest_support = max(sr_data["support"])
                if nearest_support > sl:
                    sl = nearest_support * 0.99
            if sr_data["resistance"]:
                nearest_resistance = min(sr_data["resistance"])
                if nearest_resistance > price:
                    tp = min(tp, nearest_resistance * 0.99)
        else:
            sl = price + sl_mult * atr
            tp = price - tp_mult * atr
            if sr_data["resistance"]:
                nearest_resistance = min(sr_data["resistance"])
                if nearest_resistance < sl:
                    sl = nearest_resistance * 1.01
            if sr_data["support"]:
                nearest_support = max(sr_data["support"])
                if nearest_support < price:
                    tp = max(tp, nearest_support * 1.01)
        if direction == "BUY" and sl >= price:
            sl = price * 0.98
            tp = price * 1.04
        if direction == "SELL" and sl <= price:
            sl = price * 1.02
            tp = price * 0.96

        logic_parts = []
        if htf_trend != "NEUTRAL":
            logic_parts.append(f"HTF {htf_trend} + 15m {trend}")
        if bos and bos["direction"]:
            logic_parts.append(f"BOS {bos['direction']}")
        if choch:
            logic_parts.append("CHOCH")
        if liquidity_sweep:
            logic_parts.append(f"Liquidity Sweep {liquidity_sweep}")
        if fvgs:
            fvg_types = [f["type"] for f in fvgs]
            logic_parts.append(f"FVG ({', '.join(fvg_types)})")
        if ob:
            logic_parts.append(f"Order Block {ob['type']}")
        if vol_ratio > 1.2:
            logic_parts.append("High Volume")
        if adx > 25:
            logic_parts.append(f"Trend Strength ADX {adx:.0f}")
        if news_sent != 0 and news_imp > 0.5:
            logic_parts.append(f"News {news_sent:+.0f}%")
        logic = " + ".join(logic_parts) if logic_parts else "Confluence"

        news_text = "No significant news"
        if self.news.last_news:
            news_text = self.news.last_news.get("title", "No news")[:100]

        chart = self.topology.get_visual_topology(asset, price, direction, sl, tp, patterns)
        entry_zone = f"{price - atr*0.5:.2f} - {price + atr*0.5:.2f}"
        exit_zone = f"{tp - atr*0.5:.2f} - {tp + atr*0.5:.2f}"
        rr = abs(tp - price) / abs(price - sl) if abs(price - sl) > 0 else 0

        rolling_win = self.db.get_rolling_win_rate(asset, lookback=50)
        prob = (rolling_win * 100 * 0.5 + (50 + (score["total_score"] - 50) * 0.6) * 0.5)
        prob = min(95, max(5, prob))

        trade_id = self.db.log_trade(
            asset, direction, price, sl, tp,
            score["total_score"], score["confidence"],
            list(patterns.keys()), logic,
            vol, regime, htf_trend, news_sent
        )
        self.signal_count += 1
        self.accepted_signals += 1
        self.last_signal_time[asset] = now

        self.telegram.fire_signal(
            asset=asset,
            direction=direction,
            price=price,
            sl=sl,
            tp=tp,
            topology_chart=chart,
            logic=logic,
            news=news_text,
            score=score,
            patterns=patterns,
            trade_id=trade_id,
            session=datetime.now().strftime("%H:%M"),
            entry_zone=entry_zone,
            exit_zone=exit_zone,
            win_prob=prob,
            rr=rr
        )
        logger.info(f"🔥 SIGNAL: {asset} {direction} @ {price} (Score: {score['total_score']:.0f}, Prob: {prob:.1f}%)")

    def _calculate_rsi(self, closes: List[float], period: int = 14) -> float:
        if len(closes) < period+1:
            return 50
        gains, losses = 0,0
        for i in range(len(closes)-period, len(closes)):
            change = closes[i] - closes[i-1]
            if change > 0:
                gains += change
            else:
                losses -= change
        avg_gain = gains / period
        avg_loss = losses / period
        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _calculate_adx(self, candles: List[Dict]) -> float:
        if len(candles) < 14:
            return 20
        tr_list, dm_plus, dm_minus = [], [], []
        for i in range(1, len(candles)):
            high, low = candles[i]["high"], candles[i]["low"]
            prev_high, prev_low = candles[i-1]["high"], candles[i-1]["low"]
            tr = max(high - low, abs(high - prev_high), abs(low - prev_low))
            tr_list.append(tr)
            up = high - prev_high
            down = prev_low - low
            dm_plus.append(max(up, 0) if up > down else 0)
            dm_minus.append(max(down, 0) if down > up else 0)
        if len(tr_list) < 14:
            return 20
        atr = sum(tr_list[:14]) / 14
        dm_plus_smooth = sum(dm_plus[:14]) / 14
        dm_minus_smooth = sum(dm_minus[:14]) / 14
        for i in range(14, len(tr_list)):
            atr = (atr * 13 + tr_list[i]) / 14
            dm_plus_smooth = (dm_plus_smooth * 13 + dm_plus[i]) / 14
            dm_minus_smooth = (dm_minus_smooth * 13 + dm_minus[i]) / 14
        if atr == 0:
            return 20
        di_plus = (dm_plus_smooth / atr) * 100
        di_minus = (dm_minus_smooth / atr) * 100
        dx = (abs(di_plus - di_minus) / (di_plus + di_minus)) * 100 if (di_plus + di_minus) > 0 else 0
        return min(100, dx)

# =====================================================================
# ENTRY POINT
# =====================================================================
if __name__ == "__main__":
    orchestrator = AIOrchestrator()
    orchestrator.run()

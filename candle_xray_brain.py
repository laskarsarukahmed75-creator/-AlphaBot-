# =====================================================================
# candle_xray_brain.py – Advanced Candle X-Ray & Wick Pattern Brain v2.2
# =====================================================================
# Applies every single exact fix requested in the second audit:
#   - Fix 1 (Bug C): fail_closed safe_default (allow_buy=False, allow_sell=False) + Telegram alert
#   - Fix 2 (Bug A): Pattern 4 checks x15_intent and divergence before SELL
#   - Fix 3 (Bug B): Pattern 1 blocks BUY and blocks SELL on BULLISH_DIV
#   - Fix 4 (Bug B): Pattern 2 blocks BUY and blocks SELL on BULLISH_DIV
#   - Fix 5 (Bug D): Pattern 6 SELL sets pattern3_ready[asset] = True + timestamp
#   - Fix 6 (Bug D): Pattern 3 SELL sets pattern3_ready[asset] = True + timestamp
#   - Fix 7 (Issue 2): Backfill uses strict > ts so future[0] is the true next 1h candle
#   - Bonus A: pattern3_ready_ts 900s (15 min) staleness guard
#   - Bonus B: _backfill_running thread lock
# =====================================================================

import time
import math
import json
import sqlite3
import hashlib
import logging
import threading
from collections import defaultdict
from datetime import datetime
import pytz

logger = logging.getLogger("CandleXRayBrain")


class PatternAttributionStore:
    """
    Stores fired pattern events in SQLite (and optional MongoDB) with deduplication
    and backfills 1h / 4h / 12h outcomes, MFE, and MAE objectively.
    """
    def __init__(self, db_path: str = "trades_v7.db", mongo_db=None):
        self.db_path = db_path
        self.mongo = mongo_db
        self.local = threading.local()
        self._recent_event_keys = {}
        self._lock = threading.Lock()
        self._init_tables()

    def _get_conn(self):
        if not hasattr(self.local, "conn"):
            self.local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.local.conn.row_factory = sqlite3.Row
        return self.local.conn

    def _init_tables(self):
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pattern_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset TEXT,
                    pattern TEXT,
                    direction TEXT,
                    xray_intent TEXT,
                    conviction INTEGER,
                    price REAL,
                    timestamp INTEGER,
                    fingerprint TEXT,
                    outcome_1h REAL,
                    outcome_4h REAL,
                    outcome_12h REAL,
                    mfe REAL,
                    mae REAL,
                    backfilled INTEGER DEFAULT 0
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pattern_ts ON pattern_events(asset, timestamp)")
            conn.commit()
            cur.close()
        except Exception as e:
            logger.error(f"PatternAttributionStore init error: {e}")

    def log_event(self, asset: str, pattern: str, direction: str, xray_intent: str,
                  conviction: int, price: float, fingerprint: str = ""):
        now = int(time.time())
        dedup_key = f"{asset}:{pattern}:{fingerprint}"
        with self._lock:
            last_logged = self._recent_event_keys.get(dedup_key, 0)
            if now - last_logged < 900:
                return
            self._recent_event_keys[dedup_key] = now
            if len(self._recent_event_keys) > 500:
                cutoff = now - 3600
                self._recent_event_keys = {k: v for k, v in self._recent_event_keys.items() if v >= cutoff}

        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO pattern_events
                (asset, pattern, direction, xray_intent, conviction, price, timestamp, fingerprint, backfilled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            """, (asset, pattern, direction, xray_intent, int(conviction), float(price), now, fingerprint))
            conn.commit()
            cur.close()
        except Exception as e:
            logger.error(f"log_event SQLite error: {e}")

        if self.mongo is not None and getattr(self.mongo, "db", None) is not None:
            try:
                self.mongo.db.pattern_state.update_one(
                    {"asset": asset},
                    {"$set": {
                        "asset": asset,
                        "last_pattern": pattern,
                        "last_direction": direction,
                        "last_ts": now,
                        "last_conviction": int(conviction),
                        "last_price": float(price),
                        "fingerprint": fingerprint
                    }},
                    upsert=True
                )
            except Exception:
                pass

    def load_persisted_state(self) -> dict:
        state = {}
        if self.mongo is not None and getattr(self.mongo, "db", None) is not None:
            try:
                for doc in self.mongo.db.pattern_state.find({}):
                    a = doc.get("asset")
                    if a:
                        state[a] = doc
                return state
            except Exception:
                pass
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute("""
                SELECT asset, pattern, direction, timestamp, conviction, price, fingerprint
                FROM pattern_events
                WHERE id IN (SELECT MAX(id) FROM pattern_events GROUP BY asset)
            """)
            for r in cur.fetchall():
                state[r["asset"]] = {
                    "last_pattern": r["pattern"],
                    "last_direction": r["direction"],
                    "last_ts": r["timestamp"],
                    "last_conviction": r["conviction"],
                    "last_price": r["price"],
                    "fingerprint": r["fingerprint"]
                }
            cur.close()
        except Exception:
            pass
        return state

    def backfill_outcomes(self, topology):
        """
        Fix 7: Uses strict > ts so future[0] is the next 1h candle after event ts,
        preventing intra-candle overlap.
        """
        cutoff = int(time.time()) - 43200
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute("""
                SELECT id, asset, direction, price, timestamp
                FROM pattern_events
                WHERE backfilled = 0 AND timestamp <= ?
                LIMIT 50
            """, (cutoff,))
            rows = cur.fetchall()
            if not rows:
                cur.close()
                return

            for r in rows:
                asset = r["asset"]
                ts = int(r["timestamp"])
                entry = float(r["price"])
                if entry <= 0:
                    continue
                direction = r["direction"] or "BUY"
                with topology.lock:
                    c1h = list(topology.get_completed(asset, 3600))
                future = [c for c in c1h if int(c["timestamp"]) > ts][:12]
                if len(future) < 4:
                    continue

                def pct_move(c_close):
                    raw = ((float(c_close) - entry) / entry) * 100.0
                    return raw if direction == "BUY" else -raw

                out_1h = pct_move(future[0]["close"])
                out_4h = pct_move(future[min(3, len(future) - 1)]["close"])
                out_12h = pct_move(future[-1]["close"])

                if direction == "BUY":
                    max_fav = max((float(x["high"]) - entry) / entry * 100.0 for x in future)
                    max_adv = max((entry - float(x["low"])) / entry * 100.0 for x in future)
                else:
                    max_fav = max((entry - float(x["low"])) / entry * 100.0 for x in future)
                    max_adv = max((float(x["high"]) - entry) / entry * 100.0 for x in future)

                cur.execute("""
                    UPDATE pattern_events
                    SET outcome_1h=?, outcome_4h=?, outcome_12h=?, mfe=?, mae=?, backfilled=1
                    WHERE id=?
                """, (round(out_1h, 3), round(out_4h, 3), round(out_12h, 3),
                      round(max_fav, 3), round(max_adv, 3), r["id"]))
            conn.commit()
            cur.close()
        except Exception as e:
            logger.error(f"backfill_outcomes error: {e}")


class AdvancedCandleXRay:
    """
    Deep Candle X-Ray:
      - Candle-close aligned CVD snapshot delta (seeded at startup)
      - Volume Profile HVN / LVN confluence (with safe low-price bucket step)
      - Price vs CVD & RSI divergence detection
      - Nearest S/R proximity with combined ATR and percentage check
      - 60s TTL result cache keyed on (asset, tf, last_candle_ts)
    """
    def __init__(self, topology, futures_stream=None, multi_oi=None):
        self.topology = topology
        self.futures = futures_stream
        self.multi_oi = multi_oi
        self._candle_cvd_snap = {}
        self._cache = {}
        self._cache_ttl = 60
        self._lock = threading.Lock()
        self.seed_cvd_baselines()

    def _read_raw_cvd(self, asset: str) -> float:
        if not self.futures:
            return 0.0
        for key in (asset.upper(), asset.lower()):
            try:
                val = self.futures.get_cvd(key)
                if val is not None and val != 0:
                    return float(val)
            except Exception:
                pass
        return 0.0

    def seed_cvd_baselines(self):
        if not self.futures:
            return
        assets = list(getattr(self.topology, "candles", {}).get(900, {}).keys())
        for asset in assets:
            val = self._read_raw_cvd(asset)
            for tf in (300, 900, 3600, 14400):
                self._candle_cvd_snap[(asset, tf)] = {"ts": 0, "cvd": val}

    def _get_candle_cvd_delta(self, asset: str, tf: int, candle_ts: int) -> float:
        curr_cvd = self._read_raw_cvd(asset)
        key = (asset, tf)
        with self._lock:
            snap = self._candle_cvd_snap.get(key)
            if snap is None:
                self._candle_cvd_snap[key] = {"ts": candle_ts, "cvd": curr_cvd}
                return 0.0
            if snap["ts"] != candle_ts:
                delta = curr_cvd - snap["cvd"]
                self._candle_cvd_snap[key] = {"ts": candle_ts, "cvd": curr_cvd}
                return delta
            return curr_cvd - snap["cvd"]

    def _get_oi_info(self, asset: str) -> tuple:
        oi_trend = 0.0
        if self.futures:
            for key in (asset.upper(), asset.lower()):
                try:
                    val = self.futures.get_oi_trend(key)
                    if val is not None and val != 0:
                        oi_trend = float(val)
                        break
                except Exception:
                    pass
        valid_ex = 0
        if self.multi_oi:
            try:
                comp = self.multi_oi.fetch_composite_oi(asset)
                valid_ex = sum(1 for v in comp.values() if v is not None)
            except Exception:
                pass
        return oi_trend, valid_ex

    def _volume_profile_strength(self, candles: list, price: float, lookback: int = 96) -> float:
        subset = candles[-lookback:]
        if len(subset) < 10 or price <= 0:
            return 1.0
        buckets = defaultdict(float)
        step = max(price * 0.002, price * 0.0001, 1e-8)
        for x in subset:
            mid = (float(x["high"]) + float(x["low"])) / 2.0
            if mid <= 0:
                continue
            b = int(round(mid / step))
            buckets[b] += float(x["volume"])
        if not buckets:
            return 1.0
        avg_vol = sum(buckets.values()) / len(buckets)
        curr_b = int(round(price / step))
        local_vol = buckets.get(curr_b, 0.0) + 0.5 * (buckets.get(curr_b - 1, 0.0) + buckets.get(curr_b + 1, 0.0))
        return local_vol / (avg_vol + 1e-9)

    def _nearest_sr(self, supports: list, resistances: list, price: float) -> tuple:
        valid_s = [float(s) for s in supports if 0 < float(s) <= price * 1.005]
        valid_r = [float(r) for r in resistances if float(r) >= price * 0.995]
        nearest_s = max(valid_s) if valid_s else None
        nearest_r = min(valid_r) if valid_r else None
        return nearest_s, nearest_r

    def _detect_divergence(self, candles: list, cvd_delta: float) -> str:
        if len(candles) < 16:
            return "NONE"
        closes = [float(x["close"]) for x in candles[-16:]]
        highs = [float(x["high"]) for x in candles[-6:]]
        lows = [float(x["low"]) for x in candles[-6:]]

        rsi_now = self.topology._calc_rsi(closes[-15:])
        rsi_prev = self.topology._calc_rsi(closes[-16:-1])

        if highs[-1] >= max(highs[:-1]) and rsi_now < (rsi_prev - 2.0) and cvd_delta < 0:
            return "BEARISH_DIV"
        if lows[-1] <= min(lows[:-1]) and rsi_now > (rsi_prev + 2.0) and cvd_delta > 0:
            return "BULLISH_DIV"
        return "NONE"

    def examine(self, asset: str, tf: int = 900, ema20: float = None, ema50: float = None) -> dict:
        default = {
            "intent": "NEUTRAL_CHOP",
            "side": "NONE",
            "conviction": 0,
            "is_trap": False,
            "lower_wick_pct": 0.0,
            "upper_wick_pct": 0.0,
            "body_pct": 0.0,
            "vol_ratio": 1.0,
            "vp_strength": 1.0,
            "divergence": "NONE",
            "cvd_delta": 0.0,
            "adx": 20.0,
            "nearest_support": None,
            "nearest_resistance": None,
            "reason": "Insufficient candles"
        }
        try:
            with self.topology.lock:
                candles = list(self.topology.get_completed(asset, tf))
                if not candles or len(candles) < 6:
                    return default
                c0 = candles[-1]
                c1 = candles[-2]
                sr = self.topology.support_resistance.get(asset, {})
                supports = list(sr.get("support", []))
                resistances = list(sr.get("resistance", []))
                atr = self.topology.get_atr(asset, period=14, tf=tf) or (float(c0["close"]) * 0.008)
                adx_val = float(self.topology.get_adx(asset, tf, period=14) or 20.0)

            candle_ts = int(c0.get("timestamp", 0))
            cache_key = (asset, tf, candle_ts)
            now = time.time()
            with self._lock:
                cached = self._cache.get(cache_key)
                if cached and (now - cached["ts"]) < self._cache_ttl:
                    return cached["data"]

            o, h, l, c, v = float(c0["open"]), float(c0["high"]), float(c0["low"]), float(c0["close"]), float(c0["volume"])
            rng = h - l
            if rng <= 0 or c <= 0:
                return default

            body = abs(c - o)
            upper_wick = h - max(o, c)
            lower_wick = min(o, c) - l

            body_pct = body / rng
            upper_wick_pct = upper_wick / rng
            lower_wick_pct = lower_wick / rng
            close_pos = (c - l) / rng

            recent_vols = [float(x["volume"]) for x in candles[-20:]]
            vol_ma = (sum(recent_vols) / len(recent_vols)) if recent_vols else 0.0
            vol_ratio = (v / vol_ma) if vol_ma > 0 else 1.0

            cvd_delta = self._get_candle_cvd_delta(asset, tf, candle_ts)
            oi_trend, valid_ex = self._get_oi_info(asset)
            vp_strength = self._volume_profile_strength(candles, c, lookback=96)
            div = self._detect_divergence(candles, cvd_delta)
            nearest_s, nearest_r = self._nearest_sr(supports, resistances, c)

            max_sr_dist = min(1.2 * atr, c * 0.015)
            max_sr_dist = max(max_sr_dist, c * 0.002)
            near_support = (nearest_s is not None) and (abs(c - nearest_s) <= max_sr_dist)
            near_resistance = (nearest_r is not None) and (abs(c - nearest_r) <= max_sr_dist)

            intent = "NEUTRAL_CHOP"
            side = "NONE"
            conviction = 0
            is_trap = False
            reasons = []

            if c > o and (upper_wick_pct >= 0.40 or div == "BEARISH_DIV" or (cvd_delta < 0 and vol_ratio >= 1.25)):
                intent = "BUYER_TRAP"
                side = "SELL"
                is_trap = True
                conviction = 35
                reasons.append("BullTrapOrBearDiv")

            elif c < o and ((lower_wick_pct >= 0.42 and cvd_delta > 0) or div == "BULLISH_DIV"):
                intent = "SELLER_TRAP_ABSORPTION"
                side = "BUY"
                is_trap = True
                conviction = 40
                reasons.append("LowerWickAbsorptionOrBullDiv")

            elif c > o and body_pct >= 0.48 and close_pos >= 0.68 and upper_wick_pct <= 0.24:
                if near_resistance and c < float(c1["high"]) and vp_strength < 1.2:
                    intent = "WEAK_BUY_AT_RES"
                    side = "BUY"
                    conviction = 32
                    reasons.append("WeakAtNearestRes")
                else:
                    intent = "REAL_BUYER"
                    side = "BUY"
                    conviction = 50
                    reasons.append("CleanBullClose")
                    if vol_ratio >= 1.20:
                        conviction += 12
                        reasons.append("VolExpansion")
                    if cvd_delta > 0:
                        conviction += 12
                        reasons.append("CVDAligned")
                    if vp_strength >= 1.3:
                        conviction += 10
                        reasons.append("HVNConfluence")
                    if oi_trend > 0 or valid_ex >= 2:
                        conviction += 8
                        reasons.append("OIConfirm")
                    if ema20 and c > ema20 and float(c1["close"]) <= ema20:
                        conviction += 8
                        reasons.append("EMA20Reclaim")

            elif c < o and body_pct >= 0.48 and close_pos <= 0.32 and lower_wick_pct <= 0.24:
                if near_support and c > float(c1["low"]) and vp_strength < 1.2:
                    intent = "WEAK_SELL_AT_SUP"
                    side = "SELL"
                    conviction = 32
                    reasons.append("WeakAtNearestSup")
                else:
                    intent = "REAL_SELLER"
                    side = "SELL"
                    conviction = 50
                    reasons.append("CleanBearClose")
                    if vol_ratio >= 1.20:
                        conviction += 12
                        reasons.append("VolExpansion")
                    if cvd_delta < 0:
                        conviction += 12
                        reasons.append("CVDAligned")
                    if vp_strength >= 1.3:
                        conviction += 10
                        reasons.append("HVNConfluence")
                    if oi_trend > 0 or valid_ex >= 2:
                        conviction += 8
                        reasons.append("OIConfirm")
                    if ema20 and c < ema20 and float(c1["close"]) >= ema20:
                        conviction += 8
                        reasons.append("EMA20Breakdown")

            elif lower_wick_pct >= 0.45:
                intent = "LOWER_WICK_PROBE"
                side = "BUY"
                conviction = 30 + (10 if vp_strength >= 1.3 else 0)
                reasons.append("LowerWickProbe")
            elif upper_wick_pct >= 0.45:
                intent = "UPPER_WICK_PROBE"
                side = "SELL"
                conviction = 30 + (10 if vp_strength >= 1.3 else 0)
                reasons.append("UpperWickProbe")

            res = {
                "intent": intent,
                "side": side,
                "conviction": max(0, min(100, conviction)),
                "is_trap": is_trap,
                "lower_wick_pct": round(lower_wick_pct, 2),
                "upper_wick_pct": round(upper_wick_pct, 2),
                "body_pct": round(body_pct, 2),
                "vol_ratio": round(vol_ratio, 2),
                "vp_strength": round(vp_strength, 2),
                "divergence": div,
                "cvd_delta": round(cvd_delta, 2),
                "adx": round(adx_val, 1),
                "nearest_support": nearest_s,
                "nearest_resistance": nearest_r,
                "reason": " | ".join(reasons) if reasons else "MixedFlow"
            }

            with self._lock:
                if len(self._cache) > 100:
                    self._cache.clear()
                self._cache[cache_key] = {"ts": now, "data": res}
            return res
        except Exception as e:
            logger.error(f"AdvancedCandleXRay.examine error for {asset}: {e}")
            return default


class WickPatternBrain:
    """
    Audit v2.2 Pattern Brain with Fixes 1–7 + Bonus A & B applied.
    """
    IST = pytz.timezone("Asia/Kolkata")

    def __init__(self, topology, xray: AdvancedCandleXRay, telegram=None, db_path: str = "trades_v7.db", mongo_db=None):
        self.topology = topology
        self.xray = xray
        self.telegram = telegram
        self.store = PatternAttributionStore(db_path=db_path, mongo_db=mongo_db)
        self.state = self.store.load_persisted_state()
        self.last_pattern_alert = {}
        self.last_asset_alert = {}
        self.last_fingerprint = {}
        self.pattern3_ready = {}
        self.pattern3_ready_ts = {}
        self._last_backfill_ts = 0
        self._backfill_running = False
        self._last_error_alert_ts = 0
        self._lock = threading.Lock()

    def _set_pattern3_ready(self, asset: str, ready: bool):
        with self._lock:
            self.pattern3_ready[asset] = bool(ready)
            if ready:
                self.pattern3_ready_ts[asset] = time.time()

    def _get_session_and_adaptive_params(self, atr_pct: float) -> tuple:
        now_ist = datetime.now(self.IST)
        hour = now_ist.hour
        if 2 <= hour <= 7:
            session = "DEAD_ZONE"
            min_conv = 72
        elif 13 <= hour <= 23:
            session = "LONDON_NY_PRIME"
            min_conv = 60
        else:
            session = "ASIA_TRANSITION"
            min_conv = 64

        if atr_pct >= 1.5:
            cooldown = 900
        elif atr_pct <= 0.4:
            cooldown = 5400
        else:
            cooldown = 2700
        return session, min_conv, cooldown

    def _compute_fingerprint(self, candles: list, session: str) -> str:
        raw = f"{session}|" + "|".join(
            f"{round(float(x['high']),2)}:{round(float(x['low']),2)}:{round(float(x['close']),2)}"
            for x in candles[-3:]
        )
        return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]

    def _notify_telegram(self, asset: str, pattern_code: str, title: str, detail: str,
                         support: float, resistance: float, trigger: float, inv: float,
                         cooldown_sec: int, fingerprint: str):
        if not self.telegram:
            return
        now = time.time()
        pkey = f"{asset}_{pattern_code}"
        if self.last_fingerprint.get(pkey) == fingerprint:
            return
        if now - self.last_asset_alert.get(asset, 0) < 900:
            return
        if now - self.last_pattern_alert.get(pkey, 0) < cooldown_sec:
            return

        self.last_asset_alert[asset] = now
        self.last_pattern_alert[pkey] = now
        self.last_fingerprint[pkey] = fingerprint

        sup_str = f"{support:.2f}" if support else "N/A"
        res_str = f"{resistance:.2f}" if resistance else "N/A"
        chart_url = f"https://www.tradingview.com/chart/?symbol=BINANCE:{asset}"

        msg = (
            f"🧠 <b>MARKET PATTERN UPDATE ({asset})</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 <b>{title}</b>\n"
            f"🔎 {detail}\n"
            f"🔑 <b>Support:</b> {sup_str} | <b>Resistance:</b> {res_str}\n"
            f"🎯 <b>Trigger:</b> {trigger:.2f} | <b>Invalidation:</b> {inv:.2f}\n"
            f"📊 <a href='{chart_url}'>TradingView Chart Kholein</a>"
        )
        try:
            self.telegram.send_message(msg)
        except Exception as e:
            if "429" in str(e):
                logger.warning("Telegram 429 rate limit hit; backing off pattern alerts.")
                self.last_asset_alert[asset] = now + 120

    def _alert_brain_error(self, asset: str, err_msg: str):
        now = time.time()
        if now - self._last_error_alert_ts < 1800:
            return
        self._last_error_alert_ts = now
        if self.telegram:
            try:
                self.telegram.send_message(
                    f"⚠️ <b>PATTERN BRAIN FAIL-CLOSED ({asset})</b>\n"
                    f"Error: <code>{str(err_msg)[:120]}</code>\n"
                    f"Safety: BUY/SELL blocked until recovery."
                )
            except Exception:
                pass

    def _get_4h_trend(self, asset: str) -> str:
        try:
            with self.topology.lock:
                c4h = list(self.topology.get_completed(asset, 14400))
            if len(c4h) < 50:
                return "UNKNOWN"
            closes = [float(x["close"]) for x in c4h[-100:]]
            e20 = self.topology._ema(closes, 20)[-1]
            e50 = self.topology._ema(closes, 50)[-1]
            return "BULL" if e20 > e50 else "BEAR"
        except Exception:
            return "UNKNOWN"

    def evaluate_pattern(self, asset: str, price: float, ema20_15: float, ema50_15: float,
                         ema20_1h: float, ema50_1h: float) -> dict:
        # Fix 1 (Bug C): safe_default fail-closed (False / False)
        safe_default = {
            "pattern": "BRAIN_ERROR",
            "allow_buy": False,
            "allow_sell": False,
            "bonus": 0,
            "xray_15": {"intent": "NONE", "conviction": 0, "is_trap": False},
            "xray_1h": {"intent": "NONE", "conviction": 0, "is_trap": False},
            "note": "Fallback: brain error, blocking for safety"
        }
        try:
            now = time.time()
            # Bonus A: pattern3_ready timestamp staleness guard (900s = 15m)
            if self.pattern3_ready.get(asset) and (now - self.pattern3_ready_ts.get(asset, 0) > 900):
                self.pattern3_ready[asset] = False

            # Bonus B: Thread-safe backfill with _backfill_running lock
            if (now - self._last_backfill_ts > 1800) and (not self._backfill_running):
                self._last_backfill_ts = now
                self._backfill_running = True

                def _run():
                    try:
                        self.store.backfill_outcomes(self.topology)
                    finally:
                        self._backfill_running = False

                threading.Thread(target=_run, daemon=True).start()

            with self.topology.lock:
                c15 = list(self.topology.get_completed(asset, 900))
                c1h = list(self.topology.get_completed(asset, 3600))
                atr_15 = self.topology.get_atr(asset, period=14, tf=900) or (price * 0.008)

            if len(c15) < 22 or len(c1h) < 6:
                return {**safe_default, "pattern": "WARMING_UP", "note": "Insufficient candle history"}

            xray_15 = self.xray.examine(asset, tf=900, ema20=ema20_15, ema50=ema50_15)
            xray_1h = self.xray.examine(asset, tf=3600, ema20=ema20_1h, ema50=ema50_1h)

            x15_conv = xray_15["conviction"]
            x15_intent = xray_15["intent"] if (x15_conv >= 45 or xray_15["is_trap"]) else "NEUTRAL_CHOP"
            x15_div = xray_15.get("divergence", "NONE")
            adx_15 = xray_15.get("adx", 20.0)

            atr_pct = (atr_15 / price * 100.0) if price > 0 else 0.8
            session, min_conv_p3, cooldown_sec = self._get_session_and_adaptive_params(atr_pct)
            htf_4h = self._get_4h_trend(asset)
            htf_1h_bull = ema20_1h > ema50_1h
            htf_1h_bear = ema20_1h < ema50_1h

            last4 = c15[-4:]
            fp = self._compute_fingerprint(last4, session)
            sup = xray_15.get("nearest_support")
            res = xray_15.get("nearest_resistance")

            wick_candle_low = None
            wick_candle_high = None
            double_lower_wick = 0
            double_upper_wick = 0

            for candle in last4[:-1]:
                o_i, h_i, l_i, c_i = float(candle["open"]), float(candle["high"]), float(candle["low"]), float(candle["close"])
                r_i = h_i - l_i
                if r_i <= 0:
                    continue
                lw_i = (min(o_i, c_i) - l_i) / r_i
                uw_i = (h_i - max(o_i, c_i)) / r_i
                if lw_i >= 0.42:
                    wick_candle_low = l_i
                    double_lower_wick += 1
                if uw_i >= 0.42:
                    wick_candle_high = h_i
                    double_upper_wick += 1

            c_curr = last4[-1]
            c_prev = last4[-2]
            curr_close = float(c_curr["close"])
            curr_high = float(c_curr["high"])
            curr_low = float(c_curr["low"])
            prev_body_top = max(float(c_prev["open"]), float(c_prev["close"]))
            prev_body_bot = min(float(c_prev["open"]), float(c_prev["close"]))

            ranges_20 = [float(x["high"]) - float(x["low"]) for x in c15[-22:-2]]
            avg_rng_20 = (sum(ranges_20) / len(ranges_20)) if ranges_20 else atr_15
            recent_avg_rng = sum(float(x["high"]) - float(x["low"]) for x in c15[-4:-1]) / 3.0
            was_compressed = (avg_rng_20 > 0) and (recent_avg_rng < 0.55 * avg_rng_20)

            # HARD TRAP BLOCK
            if xray_15["is_trap"] and xray_15["intent"] == "BUYER_TRAP":
                self._set_pattern3_ready(asset, False)
                return {
                    "pattern": "TRAP_BUYER_BLOCK",
                    "allow_buy": False,
                    "allow_sell": (xray_15["divergence"] != "BULLISH_DIV" and htf_4h == "BEAR"),
                    "bonus": 5 if htf_4h == "BEAR" else 0,
                    "xray_15": xray_15,
                    "xray_1h": xray_1h,
                    "note": f"BuyerTrapBlocked ({xray_15['reason']})"
                }

            # Fix 3 (Bug B): PATTERN 1 blocks BUY and blocks SELL on BULLISH_DIV
            if (wick_candle_low is not None and
                    curr_close < wick_candle_low and
                    x15_intent in ("REAL_SELLER", "NEUTRAL_CHOP") and
                    xray_15["divergence"] != "BULLISH_DIV"):
                bonus = 10 + (10 if htf_4h == "BEAR" else 0)
                self.store.log_event(asset, "PATTERN_1_FAKE_WICK", "SELL", x15_intent, x15_conv, price, fp)
                self._notify_telegram(
                    asset, "PATTERN_1",
                    "Pattern 1: Fake Wick -> Bear Continuation",
                    f"Jis candle mein niche wick thi ({wick_candle_low:.2f}), price ne uska low tod diya hai.",
                    sup, res, trigger=curr_close, inv=float(c_curr["high"]),
                    cooldown_sec=cooldown_sec, fingerprint=fp
                )
                return {
                    "pattern": "PATTERN_1_FAKE_WICK",
                    "allow_buy": False,
                    "allow_sell": (htf_4h != "BULL"),
                    "bonus": bonus,
                    "xray_15": xray_15,
                    "xray_1h": xray_1h,
                    "note": "Pattern 1: Exact wick low broken, downtrend continues"
                }

            # Fix 2 (Bug A): PATTERN 4 checks x15_intent and BULLISH_DIV before SELL
            prior_swing_high = max(float(x["high"]) for x in c15[-10:-2])
            if (float(c_prev["high"]) > prior_swing_high and
                    curr_close < prior_swing_high and
                    xray_15["vol_ratio"] < 1.3 and
                    x15_intent not in ("REAL_BUYER", "SELLER_TRAP_ABSORPTION") and
                    xray_15["divergence"] != "BULLISH_DIV"):
                self._set_pattern3_ready(asset, False)
                self.store.log_event(asset, "PATTERN_4_FAILED_BREAKOUT", "SELL", x15_intent, x15_conv, price, fp)
                self._notify_telegram(
                    asset, "PATTERN_4",
                    "Pattern 4: Failed Breakout (Bull Trap)",
                    f"Price ne {prior_swing_high:.2f} toda par weak volume par wapas range ke andar close de diya.",
                    sup, res, trigger=curr_low, inv=float(c_prev["high"]),
                    cooldown_sec=cooldown_sec, fingerprint=fp
                )
                return {
                    "pattern": "PATTERN_4_FAILED_BREAKOUT",
                    "allow_buy": False,
                    "allow_sell": True,
                    "bonus": 12,
                    "xray_15": xray_15,
                    "xray_1h": xray_1h,
                    "note": "Pattern 4: Failed breakout verified against buyer absorption/div"
                }

            # Fix 4 (Bug B): PATTERN 2 blocks BUY and blocks SELL on BULLISH_DIV
            if wick_candle_low is not None and htf_1h_bear and curr_close > wick_candle_low and price < ema20_1h:
                if ((x15_intent in ("BUYER_TRAP", "WEAK_BUY_AT_RES", "NEUTRAL_CHOP", "UPPER_WICK_PROBE")
                     or xray_15["divergence"] == "BEARISH_DIV") and
                        xray_15["divergence"] != "BULLISH_DIV"):
                    allow_sell_p2 = (x15_intent == "BUYER_TRAP")
                    self.store.log_event(asset, "PATTERN_2_WEAK_BOUNCE", "SELL", x15_intent, x15_conv, price, fp)
                    self._notify_telegram(
                        asset, "PATTERN_2",
                        "Pattern 2: Weak Relief Bounce (Fake BUY Risk)",
                        f"Wick ke baad halka bounce hai par 1h EMA20 ({ema20_1h:.2f}) ke neeche kamzori dikh rahi hai.",
                        sup, res, trigger=curr_low, inv=ema20_1h,
                        cooldown_sec=cooldown_sec, fingerprint=fp
                    )
                    return {
                        "pattern": "PATTERN_2_WEAK_BOUNCE",
                        "allow_buy": False,
                        "allow_sell": allow_sell_p2,
                        "bonus": 0,
                        "xray_15": xray_15,
                        "xray_1h": xray_1h,
                        "note": "Pattern 2: Weak bounce below 1h EMA20"
                    }

            # PATTERN 5: Double Wick Rejection at S/R
            if double_lower_wick >= 2 and x15_intent in ("REAL_BUYER", "SELLER_TRAP_ABSORPTION") and x15_div != "BEARISH_DIV":
                self._set_pattern3_ready(asset, True)
                self.store.log_event(asset, "PATTERN_5_DOUBLE_WICK_BUY", "BUY", x15_intent, x15_conv, price, fp)
                self._notify_telegram(
                    asset, "PATTERN_5_BUY",
                    "Pattern 5: Double Lower-Wick Support Hold",
                    f"Do candles ne ek hi zone se buyers ka strong defense dikhaya hai ({price:.2f}).",
                    sup, res, trigger=curr_high, inv=min(float(c_curr["low"]), float(c_prev["low"])),
                    cooldown_sec=cooldown_sec, fingerprint=fp
                )
                return {
                    "pattern": "PATTERN_5_DOUBLE_WICK_BUY",
                    "allow_buy": True,
                    "allow_sell": False,
                    "bonus": 12 + (8 if htf_4h == "BULL" else 0),
                    "xray_15": xray_15,
                    "xray_1h": xray_1h,
                    "note": "Pattern 5: Double wick support absorption"
                }

            # Fix 5 (Bug D): PATTERN 6 Compression Spring (sets pattern3_ready for both BUY & SELL)
            if was_compressed and xray_15["vol_ratio"] >= 1.4 and x15_conv >= min_conv_p3:
                if x15_intent == "REAL_BUYER" and curr_close > prev_body_top and x15_div != "BEARISH_DIV":
                    self._set_pattern3_ready(asset, True)
                    self.store.log_event(asset, "PATTERN_6_COMPRESSION_BUY", "BUY", x15_intent, x15_conv, price, fp)
                    self._notify_telegram(
                        asset, "PATTERN_6_BUY",
                        "Pattern 6: Compression Spring Breakout UP",
                        f"Tight range ke baad volume explosion ke saath Real Buyer breakout hua hai ({price:.2f}).",
                        sup, res, trigger=curr_close, inv=curr_low,
                        cooldown_sec=cooldown_sec, fingerprint=fp
                    )
                    return {
                        "pattern": "PATTERN_6_COMPRESSION_BUY",
                        "allow_buy": True,
                        "allow_sell": False,
                        "bonus": 15 + (10 if htf_4h == "BULL" else 0),
                        "xray_15": xray_15,
                        "xray_1h": xray_1h,
                        "note": "Pattern 6: Volatility compression spring upward"
                    }
                elif x15_intent == "REAL_SELLER" and curr_close < prev_body_bot and xray_15["divergence"] != "BULLISH_DIV":
                    self._set_pattern3_ready(asset, True)
                    self.store.log_event(asset, "PATTERN_6_COMPRESSION_SELL", "SELL", x15_intent, x15_conv, price, fp)
                    self._notify_telegram(
                        asset, "PATTERN_6_SELL",
                        "Pattern 6: Compression Spring Breakdown DOWN",
                        f"Tight range ke baad volume explosion ke saath Real Seller breakdown hua hai ({price:.2f}).",
                        sup, res, trigger=curr_close, inv=curr_high,
                        cooldown_sec=cooldown_sec, fingerprint=fp
                    )
                    return {
                        "pattern": "PATTERN_6_COMPRESSION_SELL",
                        "allow_buy": False,
                        "allow_sell": True,
                        "bonus": 15 + (10 if htf_4h == "BEAR" else 0),
                        "xray_15": xray_15,
                        "xray_1h": xray_1h,
                        "note": "Pattern 6: Volatility compression spring downward"
                    }

            # PATTERN 3 BUY (3A Spring vs 3B Trend Continuation)
            higher_low_ok = curr_low > float(c_prev["low"]) and (
                curr_close > prev_body_top or (curr_close > ema20_15 and float(c_prev["close"]) <= ema20_15)
            )
            if higher_low_ok and x15_intent == "REAL_BUYER" and x15_conv >= min_conv_p3:
                if x15_div != "BEARISH_DIV" and (xray_1h["intent"] in ("REAL_BUYER", "SELLER_TRAP_ABSORPTION") or price > ema20_1h):
                    self._set_pattern3_ready(asset, True)
                    if wick_candle_low is not None:
                        p_name = "PATTERN_3A_SPRING_BUY"
                        bonus = 18 + (10 if htf_4h == "BULL" else 0)
                        title = "Pattern 3A: TRUE SPRING BULLISH EXPANSION"
                    else:
                        p_name = "PATTERN_3B_TREND_CONT_BUY"
                        bonus = 12 + (8 if htf_4h == "BULL" else 0)
                        title = "Pattern 3B: Bullish Trend Continuation"
                    self.store.log_event(asset, p_name, "BUY", x15_intent, x15_conv, price, fp)
                    self._notify_telegram(
                        asset, p_name, title,
                        f"Higher Low + Real Buyer ({x15_conv}%) + ADX:{adx_15:.0f} + {session} ({price:.2f}).",
                        sup, res, trigger=curr_close, inv=curr_low,
                        cooldown_sec=cooldown_sec, fingerprint=fp
                    )
                    return {
                        "pattern": p_name,
                        "allow_buy": True,
                        "allow_sell": False,
                        "bonus": bonus,
                        "xray_15": xray_15,
                        "xray_1h": xray_1h,
                        "note": f"{p_name} ({session}, 4h:{htf_4h}, ADX:{adx_15:.0f})"
                    }

            # Fix 6 (Bug D): PATTERN 3 SELL (sets pattern3_ready[asset] = True + timestamp)
            lower_high_ok = curr_high < float(c_prev["high"]) and (
                curr_close < prev_body_bot or (curr_close < ema20_15 and float(c_prev["close"]) >= ema20_15)
            )
            if lower_high_ok and x15_intent == "REAL_SELLER" and x15_conv >= min_conv_p3:
                if xray_15["divergence"] != "BULLISH_DIV" and (xray_1h["intent"] in ("REAL_SELLER", "BUYER_TRAP") or price < ema20_1h):
                    self._set_pattern3_ready(asset, True)
                    if wick_candle_high is not None:
                        p_name = "PATTERN_3A_SPRING_SELL"
                        bonus = 15 + (10 if htf_4h == "BEAR" else 0)
                        title = "Pattern 3A: UPPER REJECTION BEARISH EXPANSION"
                    else:
                        p_name = "PATTERN_3B_TREND_CONT_SELL"
                        bonus = 12 + (8 if htf_4h == "BEAR" else 0)
                        title = "Pattern 3B: Bearish Trend Continuation"
                    self.store.log_event(asset, p_name, "SELL", x15_intent, x15_conv, price, fp)
                    self._notify_telegram(
                        asset, p_name, title,
                        f"Lower High + Real Seller ({x15_conv}%) + ADX:{adx_15:.0f} + {session} ({price:.2f}).",
                        sup, res, trigger=curr_close, inv=curr_high,
                        cooldown_sec=cooldown_sec, fingerprint=fp
                    )
                    return {
                        "pattern": p_name,
                        "allow_buy": False,
                        "allow_sell": True,
                        "bonus": bonus,
                        "xray_15": xray_15,
                        "xray_1h": xray_1h,
                        "note": f"{p_name} ({session}, 4h:{htf_4h}, ADX:{adx_15:.0f})"
                    }

            # NORMAL SCAN
            self._set_pattern3_ready(asset, False)
            allow_buy = (
                x15_intent == "REAL_BUYER"
                and x15_conv >= min_conv_p3
                and x15_div != "BEARISH_DIV"
                and htf_4h == "BULL"
            )
            allow_sell = (
                x15_intent == "REAL_SELLER"
                and x15_conv >= min_conv_p3
                and xray_15["divergence"] != "BULLISH_DIV"
                and htf_4h == "BEAR"
            )

            return {
                "pattern": "NORMAL_SCAN",
                "allow_buy": allow_buy,
                "allow_sell": allow_sell,
                "bonus": 0,
                "xray_15": xray_15,
                "xray_1h": xray_1h,
                "note": f"XRay15:{x15_intent}({x15_conv}) | 4h:{htf_4h} | ADX:{adx_15:.0f} | Div:{x15_div}"
            }
        except Exception as e:
            logger.error(f"WickPatternBrain.evaluate_pattern error for {asset}: {e}")
            self._alert_brain_error(asset, str(e))
            return safe_default

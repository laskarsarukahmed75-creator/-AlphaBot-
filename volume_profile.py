# =====================================================================
# volume_profile.py – High Performance Pure-Python Volume Profile Engine
# (Upgraded with Volume Dry-Up & Fakeout Opposite-Breakout Detection)
# =====================================================================
import logging
from typing import Dict, List, Any, Tuple

logger = logging.getLogger("VolumeProfile")


class VolumeProfileEngine:
    @staticmethod
    def _extract_ohlcv(candle: Any) -> Tuple[float, float, float, float, float]:
        """Safely extract (open, high, low, close, volume) from dict or object."""
        if isinstance(candle, dict):
            o = float(candle.get("open", candle.get("o", candle.get("close", candle.get("c", 0.0)))) or 0.0)
            h = float(candle.get("high", candle.get("h", 0.0)) or 0.0)
            l = float(candle.get("low", candle.get("l", 0.0)) or 0.0)
            c = float(candle.get("close", candle.get("c", 0.0)) or 0.0)
            v = float(candle.get("volume", candle.get("v", 0.0)) or 0.0)
            return o, h, l, c, v

        o = float(getattr(candle, "o", getattr(candle, "open", getattr(candle, "c", getattr(candle, "close", 0.0)))) or 0.0)
        h = float(getattr(candle, "h", getattr(candle, "high", 0.0)) or 0.0)
        l = float(getattr(candle, "l", getattr(candle, "low", 0.0)) or 0.0)
        c = float(getattr(candle, "c", getattr(candle, "close", 0.0)) or 0.0)
        v = float(getattr(candle, "v", getattr(candle, "volume", 0.0)) or 0.0)
        return o, h, l, c, v

    @staticmethod
    def _normalize_direction(direction: str) -> str:
        """Normalize BUY/LONG -> BULLISH and SELL/SHORT -> BEARISH."""
        d = str(direction or "").strip().upper()
        if d in ("BUY", "LONG", "BULL", "BULLISH"):
            return "BULLISH"
        if d in ("SELL", "SHORT", "BEAR", "BEARISH"):
            return "BEARISH"
        return d

    @staticmethod
    def calculate_profile(
        candles: List[Any],
        bins: int = 40,
        value_area_pct: float = 0.70,
    ) -> Dict[str, float]:
        if not candles or len(candles) < 15:
            return {"poc": 0.0, "vah": 0.0, "val": 0.0}
        try:
            extracted = [VolumeProfileEngine._extract_ohlcv(c) for c in candles]
            lows = [x[2] for x in extracted]
            highs = [x[1] for x in extracted]
            min_price = min(lows)
            max_price = max(highs)

            if max_price <= min_price:
                return {"poc": round(min_price, 4), "vah": round(max_price, 4), "val": round(min_price, 4)}

            bin_size = (max_price - min_price) / bins
            profile = [0.0] * bins

            for _, h, l, c, vol in extracted:
                typical_price = (h + l + c) / 3.0
                bin_idx = int((typical_price - min_price) / bin_size)
                bin_idx = min(max(bin_idx, 0), bins - 1)
                profile[bin_idx] += vol

            poc_idx = profile.index(max(profile))
            poc = min_price + (poc_idx + 0.5) * bin_size

            total_vol = sum(profile)
            if total_vol == 0.0:
                return {"poc": round(poc, 4), "vah": round(max_price, 4), "val": round(min_price, 4)}

            target_va_vol = total_vol * value_area_pct
            current_va_vol = profile[poc_idx]
            upper_idx = poc_idx
            lower_idx = poc_idx

            while current_va_vol < target_va_vol:
                next_upper_vol = profile[upper_idx + 1] if upper_idx + 1 < bins else 0.0
                next_lower_vol = profile[lower_idx - 1] if lower_idx - 1 >= 0 else 0.0

                if next_upper_vol == 0.0 and next_lower_vol == 0.0:
                    break

                if next_upper_vol >= next_lower_vol:
                    upper_idx += 1
                    current_va_vol += next_upper_vol
                else:
                    lower_idx -= 1
                    current_va_vol += next_lower_vol

            vah = min_price + (upper_idx + 1) * bin_size
            val = min_price + lower_idx * bin_size

            return {
                "poc": round(poc, 4),
                "vah": round(vah, 4),
                "val": round(val, 4),
            }
        except Exception as e:
            logger.error(f"Volume Profile error: {e}")
            return {"poc": 0.0, "vah": 0.0, "val": 0.0}

    @staticmethod
    def score_retest(
        price: float,
        vp: Dict[str, float],
        direction: str,
        tolerance: float = 0.003,
    ) -> Tuple[int, str]:
        poc = float(vp.get("poc", 0.0) or 0.0)
        vah = float(vp.get("vah", 0.0) or 0.0)
        val = float(vp.get("val", 0.0) or 0.0)
        if poc == 0.0:
            return 0, "NO_VP_DATA"

        side = VolumeProfileEngine._normalize_direction(direction)
        if abs(price - poc) / poc <= tolerance:
            return 8, "POC_Retest"
        if side == "BEARISH" and vah > 0.0 and abs(price - vah) / vah <= tolerance:
            return 6, "VAH_Rejection"
        if side == "BULLISH" and val > 0.0 and abs(price - val) / val <= tolerance:
            return 6, "VAL_Bounce"

        return 0, "NO_VP_LEVEL"

    @staticmethod
    def detect_fakeout_breakout(
        candles: List[Any],
        direction: str,
        lookback: int = 8,
        dryup_ratio_max: float = 0.80,
        breakout_vol_min: float = 1.25,
    ) -> Dict[str, Any]:
        """
        Detect:
          1. Volume dry-up (gradual volume contraction before breakout)
          2. Fake breakout / sweep on the opposite side
          3. Opposite-direction breakout on the latest candle (`candles[-1]`)
        """
        side = VolumeProfileEngine._normalize_direction(direction)
        empty_result = {
            "valid_setup": False,
            "score": 0,
            "tag": "NO_FAKEOUT_BREAKOUT",
            "volume_dryup": False,
            "dryup_ratio": 1.0,
            "fakeout_swept": False,
            "opposite_breakout": False,
            "vol_expansion": False,
            "breakout_vol_mult": 0.0,
        }

        if not candles or len(candles) < (lookback + 3):
            return empty_result

        try:
            rows = [VolumeProfileEngine._extract_ohlcv(c) for c in candles]
            _, c_h, c_l, c_c, c_v = rows[-1]

            # Compression window right before current candle
            comp_window = rows[-(lookback + 1):-1]
            # Baseline volume before compression window (up to 20 candles)
            base_start = max(0, len(rows) - (lookback + 21))
            base_end = len(rows) - (lookback + 1)
            base_window = rows[base_start:base_end] if base_end > base_start else rows[:-1]

            base_vol = sum(x[4] for x in base_window) / max(len(base_window), 1)
            base_vol = max(base_vol, 1e-9)

            # 1. Volume dry-up: compare second half of compression vs first half & baseline
            mid = max(len(comp_window) // 2, 1)
            first_half_vol = sum(x[4] for x in comp_window[:mid]) / max(len(comp_window[:mid]), 1)
            second_half_vol = sum(x[4] for x in comp_window[mid:]) / max(len(comp_window[mid:]), 1)

            dryup_ratio = second_half_vol / base_vol
            trending_down_vol = second_half_vol <= max(first_half_vol, 1e-9) * 0.95
            volume_dryup = (dryup_ratio <= dryup_ratio_max) or (dryup_ratio < 0.95 and trending_down_vol)

            # Range of the compression window
            comp_high = max(x[1] for x in comp_window)
            comp_low = min(x[2] for x in comp_window)

            # Last 2-3 pre-breakout candles for liquidity sweep / fake breakout
            sweep_window = comp_window[-3:] if len(comp_window) >= 3 else comp_window
            breakout_vol_mult = c_v / base_vol
            vol_expansion = breakout_vol_mult >= breakout_vol_min

            fakeout_swept = False
            opposite_breakout = False
            tag = "NO_FAKEOUT_BREAKOUT"
            score = 0

            if side == "BULLISH":
                # Fake breakdown below comp_low with reclaim back inside range
                fakeout_swept = any(x[2] < comp_low and x[3] >= comp_low for x in sweep_window)
                # Then strong bullish breakout above comp_high
                opposite_breakout = (c_c > comp_high) and (c_c > c_l)

                if fakeout_swept and opposite_breakout:
                    score += 8
                    tag = "BULL_FAKEOUT_REVERSAL_BREAKOUT"
                elif opposite_breakout:
                    score += 3
                    tag = "BULL_RANGE_BREAKOUT"

            elif side == "BEARISH":
                # Fake breakout above comp_high with pullback back inside range
                fakeout_swept = any(x[1] > comp_high and x[3] <= comp_high for x in sweep_window)
                # Then strong bearish breakdown below comp_low
                opposite_breakout = (c_c < comp_low) and (c_c < c_h)

                if fakeout_swept and opposite_breakout:
                    score += 8
                    tag = "BEAR_FAKEOUT_REVERSAL_BREAKOUT"
                elif opposite_breakout:
                    score += 3
                    tag = "BEAR_RANGE_BREAKOUT"

            if volume_dryup and opposite_breakout:
                score += 5
            if vol_expansion and opposite_breakout:
                score += 4

            valid_setup = bool(fakeout_swept and opposite_breakout and (volume_dryup or vol_expansion))

            return {
                "valid_setup": valid_setup,
                "score": score,
                "tag": tag,
                "volume_dryup": volume_dryup,
                "dryup_ratio": round(dryup_ratio, 4),
                "fakeout_swept": fakeout_swept,
                "opposite_breakout": opposite_breakout,
                "vol_expansion": vol_expansion,
                "breakout_vol_mult": round(breakout_vol_mult, 2),
            }
        except Exception as e:
            logger.error(f"Fakeout breakout detection error: {e}")
            return empty_result

    @staticmethod
    def evaluate_setup(
        candles: List[Any],
        price: float,
        direction: str,
        bins: int = 40,
        value_area_pct: float = 0.70,
        tolerance: float = 0.003,
    ) -> Dict[str, Any]:
        """Combine Volume Profile levels with volume dry-up & fakeout breakout analysis."""
        vp = VolumeProfileEngine.calculate_profile(candles, bins=bins, value_area_pct=value_area_pct)
        retest_score, retest_tag = VolumeProfileEngine.score_retest(price, vp, direction, tolerance=tolerance)
        fb = VolumeProfileEngine.detect_fakeout_breakout(candles, direction)

        total_score = retest_score + int(fb.get("score", 0))
        tags = []
        if retest_score > 0:
            tags.append(retest_tag)
        if int(fb.get("score", 0)) > 0:
            tags.append(str(fb.get("tag")))
        if fb.get("volume_dryup"):
            tags.append(f"VOL_DRYUP({fb.get('dryup_ratio')})")
        if fb.get("vol_expansion"):
            tags.append(f"VOL_SURGE({fb.get('breakout_vol_mult')}x)")

        return {
            "vp": vp,
            "retest_score": retest_score,
            "retest_tag": retest_tag,
            "fakeout": fb,
            "total_score": total_score,
            "tags": tags,
        }

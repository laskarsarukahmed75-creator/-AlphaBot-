# =====================================================================
# volume_profile.py – High Performance Pure-Python Volume Profile Engine
# =====================================================================
import logging
from typing import Dict, List, Any, Tuple

logger = logging.getLogger("VolumeProfile")

class VolumeProfileEngine:
    @staticmethod
    def calculate_profile(candles: List[Dict[str, Any]], bins: int = 40, value_area_pct: float = 0.70) -> Dict[str, float]:
        if not candles or len(candles) < 15:
            return {"poc": 0.0, "vah": 0.0, "val": 0.0}
        try:
            lows = [c["low"] for c in candles]
            highs = [c["high"] for c in candles]
            min_price = min(lows)
            max_price = max(highs)

            if max_price <= min_price:
                return {"poc": min_price, "vah": max_price, "val": min_price}

            bin_size = (max_price - min_price) / bins
            profile = [0.0] * bins

            for c in candles:
                typical_price = (c["high"] + c["low"] + c["close"]) / 3.0
                vol = c.get("volume", 0.0)
                bin_idx = int((typical_price - min_price) / bin_size)
                bin_idx = min(max(bin_idx, 0), bins - 1)
                profile[bin_idx] += vol

            poc_idx = profile.index(max(profile))
            poc = min_price + (poc_idx + 0.5) * bin_size

            total_vol = sum(profile)
            if total_vol == 0:
                return {"poc": poc, "vah": max_price, "val": min_price}

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
                "val": round(val, 4)
            }
        except Exception as e:
            logger.error(f"Volume Profile error: {e}")
            return {"poc": 0.0, "vah": 0.0, "val": 0.0}

    @staticmethod
    def score_retest(price: float, vp: Dict[str, float], direction: str, tolerance: float = 0.003) -> Tuple[int, str]:
        poc, vah, val = vp.get("poc", 0.0), vp.get("vah", 0.0), vp.get("val", 0.0)
        if poc == 0.0:
            return 0, "NO_VP_DATA"

        if abs(price - poc) / poc <= tolerance:
            return 8, "POC_Retest"
        if direction == "SELL" and vah > 0 and abs(price - vah) / vah <= tolerance:
            return 6, "VAH_Rejection"
        if direction == "BUY" and val > 0 and abs(price - val) / val <= tolerance:
            return 6, "VAL_Bounce"

        return 0, "NO_VP_LEVEL"

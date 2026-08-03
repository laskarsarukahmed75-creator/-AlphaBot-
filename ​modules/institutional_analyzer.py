# modules/institutional_analyzer.py
import time
import threading
import logging
from collections import deque
from typing import Dict, Any, Tuple

logger = logging.getLogger("InstitutionalAnalyzer")

class InstitutionalAnalyzer:
    """
    Triple Confluence Decision Engine with:
      - Continuous proportional scoring (Both Bullish & Bearish).
      - Market regime adaptive weights.
      - CVD slope exhaustion detection.
      - OI percentage change and velocity.
      - Normalized weighted scoring (0-100 scale).
    """
    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol
        self.absorption_state = None
        self.cvd_state = None
        self.oi_fetcher = None
        self.oi_history = deque(maxlen=20)
        self.oi_timestamps = deque(maxlen=20)

        self.lock = threading.Lock()
        self.current_regime = "CHOP"

    def set_oi_fetcher(self, fetcher):
        """Inject OI fetcher instance."""
        self.oi_fetcher = fetcher

    def update_absorption_state(self, state: Dict[str, Any]):
        with self.lock:
            self.absorption_state = state

    def update_cvd_state(self, state: Dict[str, Any]):
        with self.lock:
            self.cvd_state = state

    def set_regime(self, regime: str):
        """Update market regime from app.py."""
        self.current_regime = regime

    def analyze(self, price: float) -> Tuple[bool, float, Dict]:
        """
        Returns:
          - reversal_confirmed (bool)
          - bottling_score (0-100)
          - details dict with sub-scores
        """
        with self.lock:
            abs_state = self.absorption_state
            cvd_state = self.cvd_state

        # ---- 1. Order Book Absorption Score (0-40) [Bullish & Bearish Fix] ----
        ratio = abs_state.get("imbalance_ratio_0_5", 1.0) if abs_state else 1.0
        
        # Support both Buy Imbalance (ratio >= 1.5) and Sell Imbalance (ratio <= 0.67)
        if ratio >= 1.5:
            score_abs = min(40.0, (ratio / 3.0) * 40.0)
        elif ratio <= 0.67 and ratio > 0:
            inv_ratio = 1.0 / ratio  # e.g., 0.33 becomes 3.0
            score_abs = min(40.0, (inv_ratio / 3.0) * 40.0)
        else:
            score_abs = 0.0

        # ---- 2. CVD Exhaustion Score (0-30) ----
        if cvd_state:
            slope = cvd_state.get("cvd_slope", 0.0)
            exhaustion = cvd_state.get("cvd_exhaustion", False)
            if exhaustion:
                score_cvd = 30.0
            else:
                score_cvd = max(0.0, min(30.0, (1.0 - abs(slope)) * 20.0))
        else:
            score_cvd = 0.0

        # ---- 3. Open Interest Score (0-30) [Duplicate/Zero Fix] ----
        if self.oi_fetcher:
            oi = self.oi_fetcher.fetch()
            if oi is not None and oi > 0:
                # Add ONLY if value has changed (prevents same cached value duplication)
                if not self.oi_history or self.oi_history[-1] != oi:
                    self.oi_history.append(oi)
                    self.oi_timestamps.append(time.time())

        score_oi = 0.0
        if len(self.oi_history) >= 2 and self.oi_history[-2] > 0:
            oi_pct = abs((self.oi_history[-1] - self.oi_history[-2]) / self.oi_history[-2]) * 100
            if oi_pct >= 2.0:
                score_oi = 30.0
            elif oi_pct >= 1.0:
                score_oi = 20.0
            else:
                score_oi = max(0.0, oi_pct * 10.0)

        # Check velocity from fetcher as well
        if self.oi_fetcher:
            velocity = abs(self.oi_fetcher.get_oi_velocity())
            if velocity >= 2.0:
                score_oi = max(score_oi, 30.0)

        # ---- 4. Adaptive Weights based on Market Regime ----
        regime = self.current_regime
        if regime == "CHOP":
            w_abs, w_cvd, w_oi = 0.50, 0.25, 0.25
        elif regime in ["STRONG_TREND", "GRADUAL_TREND"]:
            w_abs, w_cvd, w_oi = 0.20, 0.40, 0.40
        else:
            w_abs, w_cvd, w_oi = 0.33, 0.33, 0.34

        max_abs, max_cvd, max_oi = 40.0, 30.0, 30.0
        max_weighted = w_abs * max_abs + w_cvd * max_cvd + w_oi * max_oi

        weighted_sum = w_abs * score_abs + w_cvd * score_cvd + w_oi * score_oi

        total_score = (weighted_sum / max_weighted) * 100.0 if max_weighted > 0 else 0.0
        total_score = min(100.0, max(0.0, total_score))

        # ---- 5. Decision Logic ----
        confirmed_layers = 0
        if score_abs >= 20: confirmed_layers += 1
        if score_cvd >= 20: confirmed_layers += 1
        if score_oi >= 20: confirmed_layers += 1

        # Confirmation threshold tuned to practical 80.0
        reversal_confirmed = (total_score >= 80.0) and (confirmed_layers >= 2)

        details = {
            "absorption_score": round(score_abs, 2),
            "cvd_score": round(score_cvd, 2),
            "oi_score": round(score_oi, 2),
            "total_score": round(total_score, 2),
            "confirmed_layers": confirmed_layers,
            "regime": regime,
            "cvd_exhaustion": cvd_state.get("cvd_exhaustion", False) if cvd_state else False,
            "oi_velocity": self.oi_fetcher.get_oi_velocity() if self.oi_fetcher else 0.0
        }

        return reversal_confirmed, total_score, details

    def get_bottling_metrics(self) -> Dict:
        """For JSON output."""
        with self.lock:
            abs_state = self.absorption_state
            cvd_state = self.cvd_state
        return {
            "absorption_ratio": abs_state.get("imbalance_ratio_0_5", 1.0) if abs_state else 1.0,
            "absorption_active": abs_state.get("absorption_active_0_5", False) if abs_state else False,
            "cvd": cvd_state.get("cvd", 0.0) if cvd_state else 0.0,
            "cvd_slope": cvd_state.get("cvd_slope", 0.0) if cvd_state else 0.0,
            "oi_latest": self.oi_history[-1] if self.oi_history else None,
            "oi_velocity": self.oi_fetcher.get_oi_velocity() if self.oi_fetcher else 0.0
        }

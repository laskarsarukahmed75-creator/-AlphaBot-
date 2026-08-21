# =====================================================================
# backtester.py – Lightweight Dual-Mode Backtesting Engine
# =====================================================================
import logging
from typing import Dict, List, Optional

logger = logging.getLogger("Backtester")

class BacktestingEngine:
    """Lightweight backtester for AlphaBot dual-mode strategy."""
    
    def __init__(self, signal_engine, db_handler, initial_capital: float = 10000.0):
        self.engine = signal_engine
        self.db = db_handler
        self.initial_capital = initial_capital
        self.trades: List[Dict] = []

    def run_backtest(
        self,
        asset: str,
        tf: int = 900,
        score_threshold: int = 50,
        take_profit_pct: float = 0.015,
        stop_loss_pct: float = 0.007,
        lookback_buffer: int = 50
    ) -> Dict:
        """Run backtest on historical candles."""
        candles = self.db.load_candles(asset, tf, limit=2000)
        if not candles or len(candles) < lookback_buffer:
            return {"error": "Insufficient candle data"}

        in_position = False
        entry_price = 0.0
        entry_direction = ""
        self.trades = []

        for i in range(lookback_buffer, len(candles)):
            current_price = float(candles[i]["close"])
            current_vol = float(candles[i].get("volume", 0))

            # Entry Logic
            if not in_position:
                try:
                    result = self.engine.evaluate(asset, current_price, current_vol)
                    signal = result.get("direction")
                    score = result.get("score", 0)
                except Exception as e:
                    logger.debug(f"[BT] Eval error: {e}")
                    continue

                if score >= score_threshold and signal in ("BUY", "SELL"):
                    in_position = True
                    entry_price = current_price
                    entry_direction = signal
                    entry_idx = i

            # Exit Logic
            elif in_position:
                if entry_direction == "BUY":
                    pnl_pct = (current_price - entry_price) / entry_price
                else:
                    pnl_pct = (entry_price - current_price) / entry_price

                # Take Profit / Stop Loss Exit
                if pnl_pct >= take_profit_pct or pnl_pct <= -stop_loss_pct:
                    self.trades.append({
                        "asset": asset,
                        "direction": entry_direction,
                        "entry": round(entry_price, 4),
                        "exit": round(current_price, 4),
                        "pnl_pct": round(pnl_pct, 4),
                        "pnl_usd": round(pnl_pct * self.initial_capital, 2),
                        "win": pnl_pct > 0,
                        "exit_reason": "TP/SL"
                    })
                    in_position = False

                # Time-based Exit (Max 50 candles hold)
                elif (i - entry_idx) > 50:
                    self.trades.append({
                        "asset": asset,
                        "direction": entry_direction,
                        "entry": round(entry_price, 4),
                        "exit": round(current_price, 4),
                        "pnl_pct": round(pnl_pct, 4),
                        "pnl_usd": round(pnl_pct * self.initial_capital, 2),
                        "win": pnl_pct > 0,
                        "exit_reason": "timeout"
                    })
                    in_position = False

        # Performance Summary
        total = len(self.trades)
        wins = sum(1 for t in self.trades if t["win"])
        gross_profit = sum(t["pnl_pct"] for t in self.trades if t["pnl_pct"] > 0)
        gross_loss = abs(sum(t["pnl_pct"] for t in self.trades if t["pnl_pct"] < 0))
        total_pnl = sum(t["pnl_usd"] for t in self.trades)

        return {
            "asset": asset,
            "timeframe": tf,
            "total_trades": total,
            "wins": wins,
            "losses": total - wins,
            "win_rate": round((wins / total) * 100, 2) if total > 0 else 0,
            "profit_factor": round(gross_profit / (gross_loss or 0.001), 2),
            "total_pnl_usd": round(total_pnl, 2),
            "trades": self.trades
        }

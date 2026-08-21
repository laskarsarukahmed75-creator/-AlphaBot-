# =====================================================================
# mongo_db.py – MongoDB Persistence for Lifetime Uptime & Paper Trading
# =====================================================================
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime
from pymongo import MongoClient

logger = logging.getLogger("MongoDBHandler")

class MongoDatabaseHandler:
    def __init__(self, uri: str, db_name: str = "crypto_bot_v7"):
        if not uri:
            self.client = None
            self.db = None
            return
        try:
            self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            self.db = self.client[db_name]
            self.metrics_col = self.db["paper_metrics"]
            self.trades_col = self.db["trade_history"]
            self.system_col = self.db["system_state"]
            self._start_time = time.time()
            logger.info("✅ MongoDB Handler Connected.")
        except Exception as e:
            logger.error(f"MongoDB connection error: {e}")
            self.client = None
            self.db = None

    def store_uptime(self) -> None:
        """Persist cumulative uptime."""
        if self.db is None: return
        try:
            elapsed = int(time.time() - self._start_time)
            self.system_col.update_one(
                {"_id": "uptime"},
                {"$inc": {"lifetime_seconds": elapsed}, "$set": {"last_tick": datetime.utcnow()}},
                upsert=True
            )
            self._start_time = time.time()
        except Exception as e:
            logger.error(f"Uptime store error: {e}")

    def get_uptime(self) -> Dict:
        """Returns formatted lifetime uptime."""
        if self.db is None: return {"formatted": "0d 0h 0m", "lifetime_seconds": 0}
        try:
            doc = self.system_col.find_one({"_id": "uptime"})
            secs = doc["lifetime_seconds"] if doc else 0
            days, rem = divmod(secs, 86400)
            hours, rem = divmod(rem, 3600)
            mins, _ = divmod(rem, 60)
            return {
                "lifetime_seconds": secs,
                "formatted": f"{days}d {hours}h {mins}m",
                "days": days,
                "hours": hours,
                "minutes": mins
            }
        except Exception:
            return {"formatted": "0d 0h 0m", "lifetime_seconds": 0}

    def store_paper_metrics(self, total_pnl: float, win_rate: float, closed_trades: int, gross_profit: float, gross_loss: float) -> None:
        """Store permanent paper trading metrics."""
        if self.db is None: return
        try:
            self.metrics_col.update_one(
                {"_id": "paper_trading"},
                {"$set": {
                    "total_pnl": round(total_pnl, 2),
                    "win_rate": round(win_rate, 2),
                    "closed_trades": closed_trades,
                    "gross_profit": round(gross_profit, 2),
                    "gross_loss": round(gross_loss, 2),
                    "updated_at": datetime.utcnow()
                }},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Paper metrics store error: {e}")

    def get_paper_metrics(self) -> Dict:
        """Get permanent paper trading metrics."""
        if self.db is None: return {"total_pnl": 0.0, "win_rate": 0.0, "closed_trades": 0}
        try:
            doc = self.metrics_col.find_one({"_id": "paper_trading"})
            return doc or {"total_pnl": 0.0, "win_rate": 0.0, "closed_trades": 0, "gross_profit": 0.0, "gross_loss": 0.0}
        except Exception:
            return {"total_pnl": 0.0, "win_rate": 0.0, "closed_trades": 0}

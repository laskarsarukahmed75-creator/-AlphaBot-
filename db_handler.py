import sqlite3
import time
from typing import List, Dict

class DatabaseHandler:
    def __init__(self):
        # लोकल SQLite डेटाबेस फाइल
        self.conn = sqlite3.connect("candles.db", check_same_thread=False)
        self._init_sqlite()

    def _init_sqlite(self):
        cur = self.conn.conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS candles (
                asset TEXT, tf INTEGER, timestamp INTEGER,
                open REAL, high REAL, low REAL, close REAL, volume REAL, complete BOOLEAN,
                PRIMARY KEY (asset, tf, timestamp)
            )
        ''')
        self.conn.commit()

    def save_candle(self, asset: str, tf: int, candle: Dict):
        cur = self.conn.cursor()
        cur.execute('''
            INSERT OR REPLACE INTO candles
            (asset, tf, timestamp, open, high, low, close, volume, complete)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (asset, tf, candle["timestamp"], candle["open"], candle["high"],
              candle["low"], candle["close"], candle["volume"], int(candle.get("complete", False))))
        self.conn.commit()

    def load_candles(self, asset: str, tf: int, limit: int = 500) -> List[Dict]:
        cur = self.conn.cursor()
        query = "SELECT timestamp, open, high, low, close, volume, complete FROM candles WHERE asset=? AND tf=? ORDER BY timestamp ASC"
        if limit: query += f" LIMIT {limit}"
        cur.execute(query, (asset, tf))
        rows = cur.fetchall()
        return [{"timestamp": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5], "complete": bool(r[6])} for r in rows]

    def delete_older_than(self, asset: str, tf: int, timestamp: int):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM candles WHERE asset=? AND tf=? AND timestamp < ?", (asset, tf, timestamp))
        self.conn.commit()

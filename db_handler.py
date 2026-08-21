import sqlite3
import threading
import logging
from typing import List, Dict

class DatabaseHandler:
    def __init__(self):
        self.db_path = "candles.db"
        self.local = threading.local()
        self._init_sqlite()

    def _get_conn(self):
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(self.db_path)
            self.local.conn.row_factory = sqlite3.Row
        return self.local.conn

    def _init_sqlite(self):
        try:
            conn = self._get_conn()
            conn.execute('''
                CREATE TABLE IF NOT EXISTS candles (
                    asset TEXT,
                    tf INTEGER,
                    timestamp INTEGER,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    complete BOOLEAN,
                    PRIMARY KEY (asset, tf, timestamp)
                )''')
            conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB Init Error: {e}")

    def save_candle(self, asset: str, tf: int, candle: Dict):
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute('''
                INSERT OR REPLACE INTO candles (asset, tf, timestamp, open, high, low, close, volume, complete)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (asset, tf, candle["timestamp"], candle["open"], candle["high"], candle["low"], candle["close"], candle["volume"], int(candle.get("complete", False))))
            conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB Save Error: {e}")
        finally:
            if 'cur' in locals():
                cur.close()

    def load_candles(self, asset: str, tf: int, limit: int = 500) -> List[Dict]:
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            # FIX: Parameterized limit prevents SQL injection
            query = "SELECT * FROM candles WHERE asset=? AND tf=? ORDER BY timestamp ASC LIMIT ?"
            cur.execute(query, (asset, tf, int(limit)))
            return [dict(row) for row in cur.fetchall()]
        except sqlite3.Error as e:
            logging.error(f"DB Load Error: {e}")
            return []
        finally:
            if 'cur' in locals():
                cur.close()

    def delete_older_than(self, asset: str, tf: int, timestamp: int):
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute("DELETE FROM candles WHERE asset=? AND tf=? AND timestamp < ?", (asset, tf, timestamp))
            conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB Delete Error: {e}")
        finally:
            if 'cur' in locals():
                cur.close()

# modules/oi_fetcher.py
import time
import requests
import logging
import threading
from collections import deque
from typing import Optional, Dict

logger = logging.getLogger("OIFetcher")

class OIFetcher:
    """
    Fetch Open Interest from multiple exchanges (Binance, Bybit, OKX) with:
      - 30‑second memory cache per exchange.
      - Percentage change and velocity (3‑minute spike) detection.
      - Automatic fallback if primary fails.
      - Background thread updates to avoid blocking main thread.
    """
    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol
        self.cache: Dict[str, Optional[float]] = {}
        self.last_fetch_time: Dict[str, float] = {}
        self.cache_ttl = 30  # seconds
        self.history: Dict[str, deque] = {}
        self._lock = threading.Lock()

        # Exchange endpoints with different symbols mapping
        self.exchanges = {
            "binance": {
                "url": "https://fapi.binance.com/fapi/v1/openInterest",
                "symbol": symbol,
                "field": "openInterest"
            },
            "bybit": {
                "url": "https://api.bybit.com/v5/market/tickers",
                "symbol": symbol,
                "field": "openInterest"
            },
            "okx": {
                "url": "https://www.okx.com/api/v5/public/open-interest",
                "symbol": symbol,
                "field": "oi"
            }
        }
        self.symbol_map = {
            "BTCUSDT": {"bybit": "BTCUSDT", "okx": "BTC-USD-SWAP"},
            "ETHUSDT": {"bybit": "ETHUSDT", "okx": "ETH-USD-SWAP"},
            "SOLUSDT": {"bybit": "SOLUSDT", "okx": "SOL-USD-SWAP"}
        }

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json"
        }

        # Initialize history for each exchange
        for ex in self.exchanges:
            self.history[ex] = deque(maxlen=10)

        # Start background updater
        self._running = True
        self._thread = threading.Thread(target=self._background_update, daemon=True)
        self._thread.start()

    def _background_update(self):
        """Periodically fetch OI from all exchanges in background."""
        while self._running:
            # Try Binance first, then others if needed
            for exchange in ["binance", "bybit", "okx"]:
                self._fetch_exchange(exchange)
            time.sleep(30)  # update every 30 seconds

    def _fetch_exchange(self, exchange: str) -> Optional[float]:
        """Fetch OI from a specific exchange (thread-safe)."""
        if exchange not in self.exchanges:
            return None
        config = self.exchanges[exchange]
        url = config["url"]
        mapped_symbol = self.symbol_map.get(self.symbol, {}).get(exchange, self.symbol)
        params = {"symbol": mapped_symbol}
        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                oi = None
                if exchange == "binance":
                    oi = float(data["openInterest"])
                elif exchange == "bybit":
                    if "result" in data and "list" in data["result"] and data["result"]["list"]:
                        oi = float(data["result"]["list"][0]["openInterest"])
                elif exchange == "okx":
                    if "data" in data and data["data"]:
                        oi = float(data["data"][0]["oi"])
                if oi is not None:
                    with self._lock:
                        self.cache[exchange] = oi
                        self.last_fetch_time[exchange] = time.time()
                        self.history[exchange].append((time.time(), oi))
                    return oi
            elif resp.status_code == 429:
                logger.warning(f"Rate limit on {exchange}, waiting 30s")
                time.sleep(30)
            else:
                logger.debug(f"{exchange} returned {resp.status_code}")
        except Exception as e:
            logger.error(f"Error fetching OI from {exchange}: {e}")
        return None

    def fetch(self) -> Optional[float]:
        """Return the latest cached OI (from Binance if available, else fallback)."""
        now = time.time()
        # Try to get from cache (Binance preferred)
        with self._lock:
            if "binance" in self.cache and self.cache["binance"] is not None:
                if (now - self.last_fetch_time.get("binance", 0)) < self.cache_ttl:
                    return self.cache["binance"]
            # If Binance cache stale, try Bybit or OKX
            for ex in ["bybit", "okx"]:
                if ex in self.cache and self.cache[ex] is not None:
                    if (now - self.last_fetch_time.get(ex, 0)) < self.cache_ttl:
                        return self.cache[ex]
        # If all stale, trigger immediate fetch (but this may block)
        # We'll do a quick fetch on Binance only
        return self._fetch_exchange("binance")

    def get_oi_velocity(self) -> float:
        """
        Calculate the percentage change in OI over the last 3 minutes.
        Uses Binance history if available, else fallback.
        """
        with self._lock:
            hist = self.history.get("binance", deque())
            if len(hist) < 2:
                # Try to use Bybit or OKX
                for ex in ["bybit", "okx"]:
                    if len(self.history.get(ex, deque())) >= 2:
                        hist = self.history[ex]
                        break
            if len(hist) < 2:
                return 0.0
        now = time.time()
        cutoff = now - 180  # 3 minutes
        past_oi = None
        for t, oi in hist:
            if t <= cutoff:
                past_oi = oi
                break
        if past_oi is None:
            # if no data within 3 min, use earliest
            past_oi = hist[0][1]
        current_oi = hist[-1][1]
        if past_oi == 0:
            return 0.0
        return ((current_oi - past_oi) / past_oi) * 100

    def get_oi_spike_signal(self) -> bool:
        """Returns True if OI velocity > 2% (institutional injection)."""
        velocity = self.get_oi_velocity()
        return velocity >= 2.0

    def shutdown(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=1)

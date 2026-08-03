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
        for ex in ["binance", "bybit", "okx"]:
            self.history[ex] = deque(maxlen=20)

        # Start background updater
        self._running = True
        self._thread = threading.Thread(target=self._background_update, daemon=True)
        self._thread.start()

    def _background_update(self):
        """Periodically fetch OI from all exchanges in background."""
        while self._running:
            for exchange in ["binance", "bybit", "okx"]:
                self._fetch_exchange(exchange)
            time.sleep(30)  # update every 30 seconds

    def _fetch_exchange(self, exchange: str) -> Optional[float]:
        """Fetch OI from a specific exchange (thread-safe and accurate API params)."""
        mapped_symbol = self.symbol_map.get(self.symbol, {}).get(exchange, self.symbol)
        
        # API URLs and Params according to Exchange Documentation
        if exchange == "binance":
            url = "https://fapi.binance.com/fapi/v1/openInterest"
            params = {"symbol": self.symbol}
        elif exchange == "bybit":
            url = "https://api.bybit.com/v5/market/tickers"
            params = {"category": "linear", "symbol": mapped_symbol}
        elif exchange == "okx":
            url = "https://www.okx.com/api/v5/public/open-interest"
            params = {"instId": mapped_symbol}
        else:
            return None

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
        except Exception as e:
            logger.error(f"Error fetching OI from {exchange}: {e}")
        return None

    def fetch(self) -> Optional[float]:
        """Return the latest cached OI (from Binance if available, else fallback)."""
        now = time.time()
        with self._lock:
            # Check Binance first
            if "binance" in self.cache and self.cache["binance"] is not None:
                if (now - self.last_fetch_time.get("binance", 0)) < self.cache_ttl:
                    return self.cache["binance"]
            # Fallback to Bybit or OKX
            for ex in ["bybit", "okx"]:
                if ex in self.cache and self.cache[ex] is not None:
                    if (now - self.last_fetch_time.get(ex, 0)) < self.cache_ttl:
                        return self.cache[ex]
        return self._fetch_exchange("binance")

    def get_oi_velocity(self) -> float:
        """
        Calculate the percentage change in OI over the last 3 minutes (180s).
        """
        with self._lock:
            hist = self.history.get("binance", deque())
            if len(hist) < 2:
                for ex in ["bybit", "okx"]:
                    if len(self.history.get(ex, deque())) >= 2:
                        hist = self.history[ex]
                        break
            if len(hist) < 2:
                return 0.0
            
            # Reverse check to find closest entry near 3 minutes ago
            now = time.time()
            cutoff = now - 180  # 3 minutes
            past_oi = hist[0][1] # fallback to oldest
            for t, oi in reversed(hist):
                if t <= cutoff:
                    past_oi = oi
                    break
            current_oi = hist[-1][1]

        if past_oi == 0:
            return 0.0
        return ((current_oi - past_oi) / past_oi) * 100

    def get_oi_spike_signal(self) -> bool:
        """Returns True if OI velocity >= 2.0% (institutional injection)."""
        return self.get_oi_velocity() >= 2.0

    def shutdown(self):
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1)

# modules/websocket_listener.py

import json
import threading
import time
import logging
from collections import deque
import websocket
from typing import Dict, Any

logger = logging.getLogger("WebSocketListener")

class AbsorptionWebSocket:
    """
    Real-time Order Book (Depth) + CVD (Aggregate Trades) via Binance WebSocket.
    Features:
    - Dynamic imbalance ratio for 0.5% and 1% spread levels (Bullish & Bearish).
    - Auto-reconnect with exponential backoff.
    - Ping-pong keep-alive.
    """

    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol.lower()
        self.ws_url = (
            f"wss://fstream.binance.com/stream?streams="
            f"{self.symbol}@depth20@100ms/{self.symbol}@aggTrade"
        )
        self.ws = None
        self.running = False
        self.thread = None
        self.reconnect_delay = 1

        # Shared state (updated by WebSocket)
        self.state = {
            "imbalance_ratio_0_5": 1.0,
            "imbalance_ratio_1_0": 1.0,
            "absorption_active_0_5": False,
            "absorption_active_1_0": False,
            "cvd": 0.0,
            "cvd_slope": 0.0,
            "cvd_exhaustion": False,
            "last_update": time.time(),
            "last_cvd_update": time.time()
        }
        self.lock = threading.Lock()
        self._cvd_history = deque(maxlen=20)

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        logger.info(f"AbsorptionWebSocket started for {self.symbol}")

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()

    def _run(self):
        while self.running:
            try:
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                    on_open=self._on_open
                )
                self.ws.run_forever(ping_interval=15, ping_timeout=8)
                time.sleep(self.reconnect_delay)
                self.reconnect_delay = min(30, self.reconnect_delay * 1.5)
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                time.sleep(5)

    def _on_open(self, ws):
        logger.info(f"Absorption WebSocket connected for {self.symbol}")
        self.reconnect_delay = 1

    def _on_message(self, ws, message):
        try:
            raw_msg = json.loads(message)
            data = raw_msg.get("data", raw_msg)
            
            with self.lock:
                now = time.time()

                # ---- Depth (Order Book) ----
                if "bids" in data and "asks" in data:
                    bids = data["bids"]
                    asks = data["asks"]

                    if not bids or not asks:
                        return

                    best_bid = float(bids[0][0]) if bids else 0.0
                    best_ask = float(asks[0][0]) if asks else 0.0
                    mid = (best_bid + best_ask) / 2.0 if best_bid and best_ask else 0.0

                    if mid == 0.0:
                        return

                    def vol_within_spread(spread_pct):
                        upper = mid * (1.0 + spread_pct)
                        lower = mid * (1.0 - spread_pct)
                        
                        bid_vol = 0.0
                        ask_vol = 0.0
                        for b in bids:
                            price = float(b[0])
                            qty = float(b[1])
                            if price >= lower:
                                bid_vol += qty
                        for a in asks:
                            price = float(a[0])
                            qty = float(a[1])
                            if price <= upper:
                                ask_vol += qty
                        return bid_vol, ask_vol

                    # 0.5% spread (Bullish & Bearish Both)
                    bid_vol_05, ask_vol_05 = vol_within_spread(0.005)
                    ratio_05 = bid_vol_05 / (ask_vol_05 + 1e-8)
                    self.state["imbalance_ratio_0_5"] = ratio_05
                    self.state["absorption_active_0_5"] = (ratio_05 >= 3.0 or ratio_05 <= 0.33)

                    # 1.0% spread (Bullish & Bearish Both)
                    bid_vol_10, ask_vol_10 = vol_within_spread(0.01)
                    ratio_10 = bid_vol_10 / (ask_vol_10 + 1e-8)
                    self.state["imbalance_ratio_1_0"] = ratio_10
                    self.state["absorption_active_1_0"] = (ratio_10 >= 2.5 or ratio_10 <= 0.40)
                    self.state["last_update"] = now

                # ---- Aggregate Trades (CVD) ----
                if data.get("e") == "aggTrade":
                    price = float(data["p"])
                    qty = float(data["q"])
                    is_buyer_maker = data["m"]
                    delta = qty if not is_buyer_maker else -qty

                    self.state["cvd"] += delta
                    self.state["last_cvd_update"] = now

                    self._cvd_history.append((now, self.state["cvd"]))

                    if len(self._cvd_history) >= 2:
                        t0, cvd0 = self._cvd_history[-2]
                        t1, cvd1 = self._cvd_history[-1]
                        dt = t1 - t0
                        if dt > 1e-6:
                            self.state["cvd_slope"] = (cvd1 - cvd0) / dt

                    # Safe Indexing & Accurate Exhaustion Detection
                    if len(self._cvd_history) >= 6:
                        slopes = []
                        for i in range(1, 5):
                            t0, c0 = self._cvd_history[-i-1]
                            t1, c1 = self._cvd_history[-i]
                            dt = t1 - t0
                            if dt > 1e-6:
                                slopes.append((c1 - c0) / dt)
                        if slopes:
                            avg_slope = sum(slopes) / len(slopes)
                            self.state["cvd_exhaustion"] = abs(avg_slope) < 0.1
                    else:
                        self.state["cvd_exhaustion"] = False

        except Exception as e:
            logger.debug(f"Absorption WebSocket message error: {e}")

    def _on_error(self, ws, error):
        logger.error(f"Absorption WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning("Absorption WebSocket closed. Reconnecting...")

    def get_state(self) -> Dict[str, Any]:
        with self.lock:
            return self.state.copy()

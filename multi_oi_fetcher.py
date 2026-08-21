import requests
import logging
from concurrent.futures import ThreadPoolExecutor

class MultiExchangeOIFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'AlphaBot-Engine'})

    def get_binance_oi(self, symbol: str) -> float:
        try:
            url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}"
            res = self.session.get(url, timeout=3)
            return float(res.json()['openInterest'])
        except Exception as e:
            logging.warning(f"Binance OI error for {symbol}: {e}")
            return None

    def get_bybit_oi(self, symbol: str) -> float:
        try:
            url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}"
            res = self.session.get(url, timeout=3)
            return float(res.json()['result']['list'][0]['openInterest'])
        except Exception as e:
            logging.warning(f"Bybit OI error for {symbol}: {e}")
            return None

    def get_okx_oi(self, symbol: str) -> float:
        try:
            # Normalize BTCUSDT to BTC-USDT-SWAP
            base = symbol.replace("USDT", "")
            okx_sym = f"{base}-USDT-SWAP"
            url = f"https://www.okx.com/api/v5/public/open-interest?instId={okx_sym}"
            res = self.session.get(url, timeout=3)
            return float(res.json()['data'][0]['oi'])
        except Exception as e:
            logging.warning(f"OKX OI error for {symbol}: {e}")
            return None

    def fetch_composite_oi(self, symbol: str) -> dict:
        """Fetches OI in parallel and returns exchange-specific values."""
        with ThreadPoolExecutor(max_workers=3) as executor:
            fut_binance = executor.submit(self.get_binance_oi, symbol)
            fut_bybit = executor.submit(self.get_bybit_oi, symbol)
            fut_okx = executor.submit(self.get_okx_oi, symbol)
            return {
                "Binance": fut_binance.result(),
                "Bybit": fut_bybit.result(),
                "OKX": fut_okx.result()
            }

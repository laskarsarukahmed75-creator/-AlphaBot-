import threading
import time
import requests
import os

class KeepAliveEngine:
    def __init__(self, interval=300):
        self.interval = interval
        self.running = True

    def start(self):
        # बैकग्राउंड में अलार्म की तरह चलेगा
        threading.Thread(target=self._ping_loop, daemon=True).start()

    def _ping_loop(self):
        # रेंडर का बाहरी यूआरएल उठाएगा, नहीं तो लोकलहोस्ट
        base_url = os.getenv("RENDER_EXTERNAL_URL", "http://localhost:10000")
        health_url = f"{base_url}/"
        time.sleep(10) # बोट को पूरी तरह बूट होने का समय देगा
        while self.running:
            try:
                requests.get(health_url, timeout=5)
                print(f"📡 Keep-alive: Bot pinged successfully at {health_url}")
            except Exception as e:
                print(f"⚠️ Keep-alive ping error: {e}")
            time.sleep(self.interval)

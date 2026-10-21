"""Verify Demo Trading API Key + find WS endpoint"""
import hashlib, hmac, time, sys, os
from urllib.parse import urlencode
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import httpx

API_KEY = "a953nMmUdvQPaSW9wLlrYZgsm8QH4Vm07ODmFquPZnKQJBLho6tcHirkHmWzH9US"
API_SECRET = "kwOaNjqKNa5C4SMHJMKQlEskLx1t700eYF1a6TTU3CdY7G1DjhEWHrvUJKkaOJKr"
BASE = "https://demo-api.binance.com"
PROXY = "http://127.0.0.1:7897"

# 1. Signed request - account info
params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
query = urlencode(params)
sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
url = f"{BASE}/api/v3/account?{query}&signature={sig}"
headers = {"X-MBX-APIKEY": API_KEY}

r = httpx.get(url, headers=headers, proxy=PROXY, timeout=10)
print(f"[1] /api/v3/account => {r.status_code}")
if r.status_code == 200:
    data = r.json()
    print(f"    accountType={data.get('accountType')}, canTrade={data.get('canTrade')}")
    balances = [b for b in data.get("balances", []) if float(b["free"]) > 0]
    for b in balances[:10]:
        print(f"    {b['asset']}: free={b['free']}")
else:
    print(f"    Error: {r.text[:200]}")

# 2. Test ticker
r = httpx.get(f"{BASE}/api/v3/ticker/price", params={"symbol": "BTCUSDT"}, proxy=PROXY, timeout=10)
print(f"[2] BTCUSDT price => {r.status_code} {r.json()}")

# 3. Test klines
r = httpx.get(f"{BASE}/api/v3/klines", params={"symbol": "BTCUSDT", "interval": "1m", "limit": 3}, proxy=PROXY, timeout=10)
print(f"[3] klines => {r.status_code}, count={len(r.json())}")

# 4. Test Demo WebSocket
import asyncio, websockets, json as j

async def test_ws():
    urls = [
        "wss://demo-stream.binance.com/ws/btcusdt@kline_1m",
        "wss://stream.binance.com:9443/ws/btcusdt@kline_1m",
    ]
    for url in urls:
        try:
            async with websockets.connect(url, proxy=PROXY, open_timeout=8) as ws:
                msg = await asyncio.wait_for(ws.recv(), timeout=8)
                d = j.loads(msg)
                print(f"[4] WS OK: {url} => close={d.get('k',{}).get('c','?')}")
                return
        except Exception as e:
            print(f"[4] WS FAIL: {url} => {type(e).__name__}: {e}")

asyncio.run(test_ws())

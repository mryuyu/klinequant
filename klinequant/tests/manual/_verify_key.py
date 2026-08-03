"""Verify Testnet API Key with signed request"""
import hashlib, hmac, time, sys, os
from urllib.parse import urlencode
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import httpx

API_KEY = "a953nMmUdvQPaSW9wLlrYZgsm8QH4Vm07ODmFquPZnKQJBLho6tcHirkHmWzH9US"
API_SECRET = "kwOaNjqKNa5C4SMHJMKQlEskLx1t700eYF1a6TTU3CdY7G1DjhEWHrvUJKkaOJKr"
BASE = "https://testnet.binance.vision"
PROXY = "http://127.0.0.1:7897"

params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
query = urlencode(params)
sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
url = f"{BASE}/api/v3/account?{query}&signature={sig}"
headers = {"X-MBX-APIKEY": API_KEY}

r = httpx.get(url, headers=headers, proxy=PROXY, timeout=10)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    data = r.json()
    print(f"accountType={data.get('accountType')}, canTrade={data.get('canTrade')}")
    balances = [b for b in data.get("balances", []) if float(b["free"]) > 0]
    for b in balances[:10]:
        print(f"  {b['asset']}: free={b['free']}, locked={b['locked']}")
else:
    print(f"Error: {r.text[:300]}")

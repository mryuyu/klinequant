"""验证 Binance Testnet API Key 连通性"""
import hashlib
import hmac
import time
from urllib.parse import urlencode

import httpx

API_KEY = "Tqlz4QvrY6PIsZ3EcMspi60rH4z9j1J8rkxT5zczXC2YhtxJNSADhXC4Rwfip2kJ"
API_SECRET = "Qy6z80Zev5kv8xrYCzn109mX56rMQS3JslYbUzFjHvzRPBWWL2PBYHYdpiqnuUSV"
BASE = "https://api.binance.com"
PROXY = "http://127.0.0.1:7897"


def signed_request(method: str, path: str, params: dict = None):
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000
    query = urlencode(params)
    sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    url = f"{BASE}{path}?{query}&signature={sig}"
    headers = {"X-MBX-APIKEY": API_KEY}
    if method == "GET":
        return httpx.get(url, headers=headers, proxy=PROXY, timeout=10)
    return httpx.post(url, headers=headers, proxy=PROXY, timeout=10)


def main():
    # 1. 测试连通性
    r = httpx.get(f"{BASE}/api/v3/time", proxy=PROXY, timeout=10)
    print(f"[1] /api/v3/time => {r.status_code} {r.json()}")

    # 2. 测试签名 - 查询账户
    r = signed_request("GET", "/api/v3/account")
    print(f"[2] /api/v3/account => {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"    accountType={data.get('accountType')}, canTrade={data.get('canTrade')}")
        balances = [b for b in data.get("balances", []) if float(b["free"]) > 0]
        for b in balances[:8]:
            print(f"    {b['asset']}: free={b['free']}, locked={b['locked']}")
    else:
        print(f"    ERROR: {r.text[:300]}")

    # 3. 查询 BTCUSDT 价格
    r = httpx.get(f"{BASE}/api/v3/ticker/price", params={"symbol": "BTCUSDT"}, proxy=PROXY, timeout=10)
    print(f"[3] BTCUSDT price => {r.status_code} {r.json()}")


if __name__ == "__main__":
    main()

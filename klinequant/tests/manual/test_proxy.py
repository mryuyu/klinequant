"""测试币安 API 代理连通性"""
import httpx
import json

PROXY = "http://127.0.0.1:7897"

with httpx.Client(proxy=PROXY, timeout=15) as c:
    # 1. K线数据
    r = c.get("https://api.binance.com/api/v3/klines", params={
        "symbol": "BTCUSDT",
        "interval": "1m",
        "limit": 3,
    })
    data = r.json()
    print("=== K线接口 ===")
    print(f"Status: {r.status_code}")
    print(f"返回 {len(data)} 根K线")
    k = data[0]
    print(f"第1根: open_time={k[0]}, O={k[1]}, H={k[2]}, L={k[3]}, C={k[4]}, V={k[5]}")
    print(f"字段数: {len(k)}")

    # 2. 服务器时间
    r2 = c.get("https://api.binance.com/api/v3/time")
    t = r2.json()
    print(f"\n=== 服务器时间 ===")
    print(f"serverTime: {t['serverTime']}")

    # 3. 最新价格
    r3 = c.get("https://api.binance.com/api/v3/ticker/price", params={"symbol": "BTCUSDT"})
    p = r3.json()
    print(f"\n=== 实时价格 ===")
    print(f"BTCUSDT: {p['price']}")

    # 4. exchangeInfo (交易对信息)
    r4 = c.get("https://api.binance.com/api/v3/exchangeInfo", params={"symbol": "BTCUSDT"})
    info = r4.json()
    print(f"\n=== ExchangeInfo ===")
    print(f"timezone: {info['timezone']}")
    print(f"serverTime: {info['serverTime']}")
    if info["symbols"]:
        s = info["symbols"][0]
        print(f"symbol: {s['symbol']}, status: {s['status']}, baseAsset: {s['baseAsset']}, quoteAsset: {s['quoteAsset']}")

    print("\n=== 全部测试通过! 代理可用 ===")

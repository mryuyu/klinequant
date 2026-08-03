"""Quick test: Testnet WS connection"""
import asyncio, json, sys, os
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import websockets

async def t():
    url = "wss://stream.testnet.binance.vision:9443/ws/btcusdt@kline_1m"
    print(f"Connecting to {url} ...")
    async with websockets.connect(url, proxy="http://127.0.0.1:7897") as ws:
        msg = await asyncio.wait_for(ws.recv(), timeout=10)
        d = json.loads(msg)
        print(f"[OK] Testnet WS works! close={d['k']['c']}")

asyncio.run(t())

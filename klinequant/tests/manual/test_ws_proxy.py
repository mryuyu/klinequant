"""快速测试 WebSocket 代理连接"""
import asyncio
import sys
import os
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import websockets
import json

PROXY = "http://127.0.0.1:7897"
WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"


async def test():
    print(f"连接 {WS_URL} via proxy {PROXY} ...")
    try:
        async with websockets.connect(WS_URL, proxy=PROXY, ping_interval=20) as ws:
            print("[OK] WebSocket connected! Waiting for messages...")
            for i in range(3):
                msg = await asyncio.wait_for(ws.recv(), timeout=10)
                data = json.loads(msg)
                k = data.get("k", {})
                print(f"  [{i+1}] close={k.get('c')} high={k.get('h')} low={k.get('l')} closed={k.get('x')}")
            print("[OK] Received 3 messages, WS proxy works!")
    except Exception as e:
        print(f"[FAIL] {type(e).__name__}: {e}")


asyncio.run(test())

"""Debug: verify WS kline events are received and dispatched"""
import asyncio
import sys
import os
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.market_engine.adapters.binance import BinanceAdapter
from protocol.types import Kline

PROXY = "http://127.0.0.1:7897"
count = 0


async def on_kline(kline: Kline):
    global count
    count += 1
    print(f"  [#{count}] ts={kline.timestamp} close={kline.close} is_closed={kline.is_closed}")
    if count >= 5:
        print("[DONE] Received 5 kline events")
        asyncio.get_event_loop().stop()


async def main():
    adapter = BinanceAdapter(config={"proxy": PROXY})
    await adapter.connect()
    print("[OK] Connected")

    await adapter.subscribe_kline("BTCUSDT", "1m", on_kline)
    await adapter.start_ws()
    print("[OK] WS started, waiting for events...")

    # Wait up to 90 seconds
    await asyncio.sleep(90)
    print(f"[TIMEOUT] Only received {count} events in 90s")
    await adapter.disconnect()


try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("\nInterrupted")

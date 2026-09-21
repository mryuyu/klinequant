"""BinanceDataFeed 单元测试

用假 adapter（REST 预热 + 捕获 WS 回调）验证：
  - start() 预热历史 bars（与 Mt5DataFeed._row_to_bar 同形 dict）
  - WS 回调：新 bar append / 未收盘 bar 更新
  - latest_bars 切片、wait_update 唤醒、is_changing 版本比对、stop 唤醒
真实 event loop 线程跑 async 回调，主线程验证同步 DataFeedProtocol 接口。
"""
import asyncio
import threading
import time
from decimal import Decimal

import pytest

from protocol.types import Kline
from strategy.sdk.binance_feed import BinanceDataFeed


def _kline(ts, o, h, l, c, v, symbol="BTCUSDT", tf="1m", closed=True):
    return Kline(
        symbol=symbol, exchange="binance_futures", timeframe=tf, timestamp=ts,
        open=Decimal(str(o)), high=Decimal(str(h)), low=Decimal(str(l)),
        close=Decimal(str(c)), volume=Decimal(str(v)),
        quote_volume=Decimal("0"), trade_count=0, is_closed=closed,
    )


class _FakeAdapter:
    def __init__(self, history):
        self._history = history
        self.callbacks = []
        self.subscribed = []
        self.ws_started = False

    async def fetch_klines(self, symbol, interval, start_time=None,
                           end_time=None, limit=1000):
        return list(self._history)

    async def subscribe_kline(self, symbol, interval, callback):
        self.subscribed.append((symbol, interval))
        self.callbacks.append(callback)

    async def start_ws(self):
        self.ws_started = True


@pytest.fixture
def loop():
    lp = asyncio.new_event_loop()
    t = threading.Thread(target=lp.run_forever, daemon=True)
    t.start()
    yield lp
    lp.call_soon_threadsafe(lp.stop)
    t.join(timeout=5)
    lp.close()


def _feed(loop, adapter, bar_count=300):
    return BinanceDataFeed(
        loop=loop, adapter=adapter, symbols=["BTCUSDT"], periods=["1m"],
        bar_count=bar_count,
    )


def _fire(loop, adapter, kline):
    """在 loop 线程触发已注册的 WS 回调"""
    cb = adapter.callbacks[0]
    asyncio.run_coroutine_threadsafe(cb(kline), loop).result(timeout=5)


def test_start_preheats_bars_and_subscribes(loop):
    history = [
        _kline(1000, 10, 12, 9, 11, 1),
        _kline(2000, 11, 13, 10, 12, 2),
        _kline(3000, 12, 14, 11, 13, 3),
    ]
    adapter = _FakeAdapter(history)
    feed = _feed(loop, adapter)
    feed.start()

    assert adapter.ws_started is True
    assert adapter.subscribed == [("BTCUSDT", "1m")]

    bars = feed.latest_bars("BTCUSDT", "1m", 10)
    assert len(bars) == 3
    b = bars[-1]
    # 与 Mt5DataFeed._row_to_bar 同形
    assert set(b) == {"symbol", "period", "timestamp", "open", "high", "low", "close", "volume"}
    assert b["symbol"] == "BTCUSDT"
    assert b["period"] == "1m"
    assert b["timestamp"] == 3000
    assert b["close"] == 13.0
    assert b["volume"] == 3.0


def test_ws_callback_appends_new_bar(loop):
    adapter = _FakeAdapter([_kline(1000, 10, 12, 9, 11, 1)])
    feed = _feed(loop, adapter)
    feed.start()
    assert len(feed.latest_bars("BTCUSDT", "1m", 10)) == 1

    _fire(loop, adapter, _kline(2000, 11, 13, 10, 12, 5))

    bars = feed.latest_bars("BTCUSDT", "1m", 10)
    assert len(bars) == 2
    assert bars[-1]["timestamp"] == 2000
    assert bars[-1]["close"] == 12.0
    # 合成 tick
    tick = feed.latest_tick("BTCUSDT")
    assert tick is not None
    assert tick.last_price == Decimal("12")


def test_ws_callback_updates_forming_bar(loop):
    adapter = _FakeAdapter([_kline(1000, 10, 12, 9, 11, 1)])
    feed = _feed(loop, adapter)
    feed.start()

    # 同一 timestamp、close 变化 → 更新未收盘 bar，不新增
    _fire(loop, adapter, _kline(1000, 10, 15, 9, 14, 9, closed=False))

    bars = feed.latest_bars("BTCUSDT", "1m", 10)
    assert len(bars) == 1
    assert bars[-1]["close"] == 14.0
    assert bars[-1]["high"] == 15.0


def test_latest_bars_slice(loop):
    history = [_kline(i * 1000, 10, 12, 9, 11, 1) for i in range(1, 6)]
    adapter = _FakeAdapter(history)
    feed = _feed(loop, adapter)
    feed.start()

    sliced = feed.latest_bars("BTCUSDT", "1m", 2)
    assert len(sliced) == 2
    assert sliced[0]["timestamp"] == 4000
    assert sliced[-1]["timestamp"] == 5000


def test_bar_count_trims_cache(loop):
    history = [_kline(i * 1000, 10, 12, 9, 11, 1) for i in range(1, 4)]
    adapter = _FakeAdapter(history)
    feed = _feed(loop, adapter, bar_count=3)
    feed.start()

    _fire(loop, adapter, _kline(9000, 11, 13, 10, 12, 2))

    bars = feed.latest_bars("BTCUSDT", "1m", 10)
    assert len(bars) == 3          # 上限裁剪
    assert bars[-1]["timestamp"] == 9000
    assert bars[0]["timestamp"] == 2000   # 最旧被挤出


def test_wait_update_and_is_changing(loop):
    adapter = _FakeAdapter([_kline(1000, 10, 12, 9, 11, 1)])
    feed = _feed(loop, adapter)
    feed.start()

    result = {}

    def waiter():
        result["got"] = feed.wait_update(3)

    t = threading.Thread(target=waiter, daemon=True)
    t.start()
    time.sleep(0.3)   # 确保已快照版本并阻塞在 Event

    _fire(loop, adapter, _kline(2000, 11, 13, 10, 12, 5))
    t.join(timeout=5)

    assert result["got"] is True
    # is_changing 比对当前版本 vs wait_update 起始快照
    assert feed.is_changing(feed.latest_bars("BTCUSDT", "1m")) is True
    assert feed.is_changing(feed.latest_tick("BTCUSDT")) is True


def test_stop_wakes_wait_update(loop):
    adapter = _FakeAdapter([_kline(1000, 10, 12, 9, 11, 1)])
    feed = _feed(loop, adapter)
    feed.start()

    result = {}

    def waiter():
        result["got"] = feed.wait_update(5)

    t = threading.Thread(target=waiter, daemon=True)
    t.start()
    time.sleep(0.3)
    feed.stop()
    t.join(timeout=6)

    assert result["got"] is False
    # stop 后 wait_update 立即 False
    assert feed.wait_update(0.1) is False

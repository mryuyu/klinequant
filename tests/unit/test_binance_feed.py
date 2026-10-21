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

from klinequant.kqsdk.binance_feed import BinanceDataFeed
from klinequant.protocol.types import Kline


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


class _RangeAdapter(_FakeAdapter):
    """G1：fetch_klines 支持 start/end 范围切片（模拟 REST 范围补拉）"""

    def __init__(self, initial, full, fail_first_range=False):
        super().__init__(initial)
        self._full = full
        self.range_calls = 0
        self._fail_first_range = fail_first_range

    async def fetch_klines(self, symbol, interval, start_time=None,
                           end_time=None, limit=1000):
        if start_time is None:
            return list(self._history)
        self.range_calls += 1
        if self._fail_first_range and self.range_calls == 1:
            raise RuntimeError("REST down")
        return [k for k in self._full if start_time <= k.timestamp <= end_time]


def test_ws_gap_backfills_disconnect_window(loop):
    """G1：推送时间戳跳变⇒断线窗中段缺 bar，按 REST 范围补拉合并（zigzag 结构依赖）"""
    full = [_kline(60_000 * i, 10 + i, 11 + i, 9 + i, 10.5 + i, i)
            for i in range(1, 7)]
    adapter = _RangeAdapter(full[:2], full)
    feed = _feed(loop, adapter)
    feed.start()
    _fire(loop, adapter, full[5])   # 断线后首推 t=360s（洞 120→360）
    ts = [b["timestamp"] for b in feed.latest_bars("BTCUSDT", "1m", 10)]
    assert ts == [60_000 * i for i in range(1, 7)]
    assert adapter.range_calls == 1


def test_ws_gap_legit_hole_not_retried(loop):
    """G1：源也缺（休市等合法洞）记 attempted 不重试，不捏造 bar"""
    full = [_kline(60_000, 1, 1, 1, 1, 1), _kline(120_000, 2, 2, 2, 2, 2),
            _kline(600_000, 3, 3, 3, 3, 3)]
    adapter = _RangeAdapter(full[:2], full)   # 开区间切片为空⇒源缺
    feed = _feed(loop, adapter)
    feed.start()
    _fire(loop, adapter, full[2])
    _fire(loop, adapter, full[2])   # 同 bar 重推（update 分支）不再补拉
    assert adapter.range_calls == 1
    ts = [b["timestamp"] for b in feed.latest_bars("BTCUSDT", "1m", 10)]
    assert ts == [60_000, 120_000, 600_000]
    assert feed._gap_attempted["BTCUSDT/1m"] == (120_000, 600_000)


def test_ws_gap_pending_retry_after_rest_failure(loop):
    """G1：补拉瞬断失败记 pending，节流窗过后随后续推送重试直至补回"""
    full = [_kline(60_000 * i, 10 + i, 11 + i, 9 + i, 10.5 + i, i)
            for i in range(1, 8)]
    adapter = _RangeAdapter(full[:2], full, fail_first_range=True)
    feed = _feed(loop, adapter)
    feed.start()
    _fire(loop, adapter, full[5])   # 首次补拉抛异常→pending，live bar 仍 append
    ts = [b["timestamp"] for b in feed.latest_bars("BTCUSDT", "1m", 10)]
    assert ts == [60_000, 120_000, 360_000]
    assert "BTCUSDT/1m" in feed._gap_pending
    # 节流窗内不重试
    _fire(loop, adapter, _kline(420_000, 1, 1, 1, 1, 1))
    assert adapter.range_calls == 1
    # 人为老化 pending 超节流窗→下一推送触发重试补回
    lo, hi, _ = feed._gap_pending["BTCUSDT/1m"]
    feed._gap_pending["BTCUSDT/1m"] = (lo, hi, time.monotonic() - 31)
    _fire(loop, adapter, _kline(480_000, 1, 1, 1, 1, 1))
    ts = [b["timestamp"] for b in feed.latest_bars("BTCUSDT", "1m", 10)]
    assert ts == [60_000 * i for i in range(1, 9)]
    assert "BTCUSDT/1m" not in feed._gap_pending



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

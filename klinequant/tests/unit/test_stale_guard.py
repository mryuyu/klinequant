"""Phase R5 断线闸门（stale guard）单元测试。

覆盖《SDK 阶段实施规划 v1.3》R5 验收点：
  - feed 心跳龄超阈（degraded）→ send_order 拒 OPEN、放 CLOSE（只减风险不加风险）
  - 心跳以「驱动响应」计（连上就刷新，与行情是否变动无关）→ 区分断线 vs 休市/清淡
  - stale_threshold=None → 闸门不启用，且从不查询 feed（后向兼容缺方法的旧 feed）
  - 各 feed 的 seconds_since_update：MT5 驱动响应刷新 / 币安 WS 推送刷新 / 回测恒 0.0
"""
import asyncio
import time
from decimal import Decimal

from core.trade_engine.executors.mt5_executor import SubmitResult
from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import UnifiedResolver
from protocol.types import Kline, Offset, OrderSide, SymbolInfo
from strategy.sdk.api import KqApi
from strategy.sdk.backtest_feed import BacktestDataFeed
from strategy.sdk.binance_feed import BinanceDataFeed
from strategy.sdk.data_feed import Mt5DataFeed

# ─── 脚手架 ───


def _fx_spec(symbol="EURUSD"):
    return SymbolInfo(
        symbol=symbol, market_type="FX",
        pip_size=Decimal("0.0001"), tick_size=Decimal("0.00001"),
        qty_step=Decimal("0.01"), min_qty=Decimal("0.01"), qty_max=Decimal("200"),
        can_short=True,
    )


class _StaleFeed:
    """可控心跳龄的 feed 桩：seconds_since_update 返回预设值，并记录是否被查询。"""

    def __init__(self, age=0.0):
        self.age = age
        self.ssu_called = False

    def seconds_since_update(self):
        self.ssu_called = True
        return self.age

    def now_ms(self):
        return 0

    def latest_tick(self, symbol):
        return None

    def latest_bars(self, symbol, period, count):
        return []

    def wait_update(self, deadline=None):
        return False

    def is_changing(self, obj, field=None):
        return False


class _ResultExecutor:
    """submit 返回预设 SubmitResult；记录收到的 spec（证明是否越过闸门到达执行器）。"""

    def __init__(self, result=None):
        self._result = result
        self.submitted = []

    def submit(self, spec):
        self.submitted.append(spec)
        return self._result

    def cancel(self, order_ticket, symbol, magic=None):
        return True

    def query_positions(self, symbol="", magic=None):
        return []

    def query_account(self):
        return None

    def query_orders(self, symbol="", magic=None):
        return []


def _filled():
    return SubmitResult(
        success=True, status="FILLED", order_ticket=1001,
        filled_qty=Decimal("0.10"), filled_price=Decimal("1.1000"), comment="ok",
    )


def _make_api(feed, executor, *, stale_threshold=None):
    return KqApi(
        symbol="EURUSD", period="1h", tag="macd:1h",
        specs={"EURUSD": _fx_spec()}, ledger=ExposureLedger(),
        resolver=UnifiedResolver(), executor=executor, feed=feed,
        account_name="acct", stale_threshold=stale_threshold,
    )


def _seed_long(api, qty=Decimal("0.10")):
    """给账本播一个已成交多头（供 CLOSE 测试；镜像 send_order FILLED 的记账）。"""
    api._ledger.on_order_accepted(
        "EURUSD", "macd:1h", "seed", OrderSide.BUY, Offset.OPEN, qty)
    api._ledger.on_order_filled(
        "EURUSD", "macd:1h", "seed", OrderSide.BUY, Offset.OPEN, qty,
        fill_price=Decimal("1.1000"), fill_qty=qty)


# ═══════════════════════════════════════════
# 闸门语义：拒 OPEN、放 CLOSE、None 禁用
# ═══════════════════════════════════════════

def test_stale_open_rejected():
    """心跳龄超阈 + OPEN → 拒单，reason 含 stale guard，且未到达执行器"""
    feed = _StaleFeed(age=999.0)
    ex = _ResultExecutor(result=_filled())
    api = _make_api(feed, ex, stale_threshold=60.0)
    res = api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"))
    assert res.ok is False
    assert "stale guard" in res.reason
    assert ex.submitted == []          # 闸门在 submit 之前拦截
    assert feed.ssu_called is True     # 仅 OPEN 才查询心跳


def test_stale_open_rejected_reason_mentions_age_and_threshold():
    """拒单原因回显心跳龄与阈值（便于运维定位）"""
    feed = _StaleFeed(age=999.0)
    api = _make_api(feed, _ResultExecutor(result=_filled()), stale_threshold=60.0)
    res = api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"))
    assert "999s" in res.reason and "60s" in res.reason


def test_stale_close_allowed():
    """心跳龄超阈 + CLOSE → 闸门放行（不查心跳、直达执行器成交）：只减风险不加风险"""
    feed = _StaleFeed(age=999.0)
    ex = _ResultExecutor(result=_filled())
    api = _make_api(feed, ex, stale_threshold=60.0)
    _seed_long(api)
    res = api.send_order(OrderSide.SELL, Offset.CLOSE, Decimal("0.10"))
    assert feed.ssu_called is False        # CLOSE 不触发闸门查询
    assert "stale guard" not in (res.reason or "")
    assert len(ex.submitted) == 1          # 越过闸门，到达执行器
    assert res.ok is True


def test_fresh_open_allowed():
    """心跳新鲜（龄 ≤ 阈）+ OPEN → 放行成交"""
    feed = _StaleFeed(age=1.0)
    ex = _ResultExecutor(result=_filled())
    api = _make_api(feed, ex, stale_threshold=60.0)
    res = api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"))
    assert feed.ssu_called is True
    assert res.ok is True
    assert len(ex.submitted) == 1


def test_gate_boundary_exactly_at_threshold_allowed():
    """龄 == 阈（未严格超过）→ 放行（> 才拒，边界不误伤）"""
    feed = _StaleFeed(age=60.0)
    ex = _ResultExecutor(result=_filled())
    api = _make_api(feed, ex, stale_threshold=60.0)
    res = api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"))
    assert res.ok is True
    assert len(ex.submitted) == 1


def test_threshold_none_disables_gate():
    """stale_threshold=None → 闸门不启用：OPEN 照常，且从不查询 feed 心跳（后向兼容）"""
    feed = _StaleFeed(age=1e9)         # 极大龄，若被查询必然拒单
    ex = _ResultExecutor(result=_filled())
    api = _make_api(feed, ex, stale_threshold=None)
    res = api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"))
    assert feed.ssu_called is False    # None → 根本不查心跳
    assert res.ok is True
    assert len(ex.submitted) == 1


# ═══════════════════════════════════════════
# feed 心跳：seconds_since_update 各实现
# ═══════════════════════════════════════════

def test_backtest_feed_never_stale():
    """回测 feed 恒返 0.0（无真实断线，闸门永不误伤回测）"""
    bars = {"EURUSD": [
        {"timestamp": 1_700_000_000_000 + i * 60_000, "open": 1.1,
         "high": 1.11, "low": 1.09, "close": 1.10, "volume": 100}
        for i in range(5)
    ]}
    feed = BacktestDataFeed(bars, period="1m")
    assert feed.seconds_since_update() == 0.0
    feed.wait_update()                 # 推进一根 bar 后仍 0.0
    assert feed.seconds_since_update() == 0.0


class _FakeMt5Driver:
    """MT5 驱动桩：symbol_info_tick / copy_rates_from_pos 可切换返回。"""

    def __init__(self, tick=None, rows=None):
        self._tick = tick
        self._rows = rows

    def symbol_select(self, symbol, flag):
        return True

    def symbol_info_tick(self, symbol):
        return self._tick

    def copy_rates_from_pos(self, symbol, tf, start, count):
        return self._rows


def test_mt5_feed_heartbeat_refreshes_on_tick_response():
    """驱动响应（返回 tick）即刷新心跳，与价格是否变动无关（区分断线 vs 休市/清淡）"""
    drv = _FakeMt5Driver(tick={"bid": 1.1, "ask": 1.1001, "last": 0, "time": 1_700_000_000})
    feed = Mt5DataFeed(driver=drv, symbols=["EURUSD"], periods=["1m"])
    feed._last_data_mono = time.monotonic() - 999.0    # 人为老化
    assert feed.seconds_since_update() > 900.0
    feed._poll_tick("EURUSD")                          # 驱动响应
    assert feed.seconds_since_update() < 1.0           # 心跳刷新


def test_mt5_feed_heartbeat_not_refreshed_when_driver_returns_none():
    """驱动断开（symbol_info_tick 返 None）→ 心跳不刷新，龄持续增长（触发 degraded）"""
    drv = _FakeMt5Driver(tick=None)
    feed = Mt5DataFeed(driver=drv, symbols=["EURUSD"], periods=["1m"])
    feed._last_data_mono = time.monotonic() - 999.0
    assert feed._poll_tick("EURUSD") is False
    assert feed.seconds_since_update() > 900.0         # 未刷新


def test_mt5_feed_heartbeat_refreshes_on_bars_response():
    """copy_rates_from_pos 返回非空 rows 即刷新心跳"""
    rows = [
        {"time": 1_700_000_000, "open": 1.1, "high": 1.11, "low": 1.09,
         "close": 1.10, "tick_volume": 10},
        {"time": 1_700_000_060, "open": 1.10, "high": 1.12, "low": 1.10,
         "close": 1.11, "tick_volume": 12},
    ]
    drv = _FakeMt5Driver(rows=rows)
    feed = Mt5DataFeed(driver=drv, symbols=["EURUSD"], periods=["1m"])
    feed._last_data_mono = time.monotonic() - 999.0
    feed._poll_bars("EURUSD", "1m")
    assert feed.seconds_since_update() < 1.0


def test_mt5_feed_heartbeat_not_refreshed_on_empty_bars():
    """copy_rates_from_pos 返 []（无数据）不算驱动响应 → 心跳不刷新（tick 轮询才是存活主探针）"""
    drv = _FakeMt5Driver(rows=[])
    feed = Mt5DataFeed(driver=drv, symbols=["EURUSD"], periods=["1m"])
    feed._last_data_mono = time.monotonic() - 999.0
    assert feed._poll_bars("EURUSD", "1m") is False
    assert feed.seconds_since_update() > 900.0


def _btc_kline():
    return Kline(
        symbol="BTCUSDT", exchange="binance", timeframe="1m",
        timestamp=1_700_000_000_000, open=Decimal("100"), high=Decimal("101"),
        low=Decimal("99"), close=Decimal("100.5"), volume=Decimal("10"),
        quote_volume=Decimal("1000"), trade_count=5, is_closed=False,
    )


def test_binance_feed_heartbeat_refreshes_on_ws_push():
    """收到 WS K 线推送即刷新心跳（即使 bar/tick 值未变）"""
    feed = BinanceDataFeed(loop=None, adapter=None, symbols=["BTCUSDT"], periods=["1m"])
    feed._running = True
    feed._last_data_mono = time.monotonic() - 999.0
    asyncio.run(feed._on_kline(_btc_kline()))
    assert feed.seconds_since_update() < 1.0


def test_binance_feed_heartbeat_ignores_push_when_stopped():
    """停止后（_running=False）的回调不刷新心跳"""
    feed = BinanceDataFeed(loop=None, adapter=None, symbols=["BTCUSDT"], periods=["1m"])
    feed._running = False
    feed._last_data_mono = time.monotonic() - 999.0
    asyncio.run(feed._on_kline(_btc_kline()))
    assert feed.seconds_since_update() > 900.0

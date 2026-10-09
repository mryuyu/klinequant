"""Phase M 验收：回测多周期同构（拓扑 Z，统一时间轴派发）

验证《SDK 阶段实施规划 v1.3》L252：同一份 strategy(api: KqApi) 在 BacktestRunner
跑通，多周期事件按时间轴派发，**同一时刻按周期从大到小**（1h 先于 15m），保证小
周期读到大周期最新结论；per-period 反 look-ahead（未揭示的未来 bar 不可见）；
BacktestRunner 端到端嵌套注入。

回测组件用注入的 bars/specs，不依赖 MT5。
"""
from decimal import Decimal

from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import UnifiedResolver
from core.trade_engine.spec_loader import load_spec_from_mt5_dict
from protocol.types import Offset, OrderKind, OrderSide
from strategy.sdk.api import KqApi
from strategy.sdk.backtest_executor import BacktestExecutor
from strategy.sdk.backtest_feed import BacktestDataFeed
from strategy.sdk.backtest_runner import BacktestRunner

BASE_TS = 1_600_000_000_000     # 15m/1h 对齐边界
M15 = 900_000
H1 = 3_600_000
_INFO = {
    "digits": 5, "point": 0.00001, "trade_contract_size": 100000,
    "volume_step": 0.01, "volume_min": 0.01, "volume_max": 200,
    "trade_mode": 4, "spread": 0,
    "currency_base": "EUR", "currency_profit": "USD", "margin_currency": "USD",
}


def _spec(symbol: str = "TESTUSD"):
    return load_spec_from_mt5_dict(_INFO, symbol)


def _series(period: str, step_ms: int, n: int, base: float = 100.0):
    """构造 n 根时间戳对齐 bar（价格随序号递增，便于区分周期与断言趋势）。"""
    return [
        {
            "symbol": "TESTUSD", "period": period,
            "timestamp": BASE_TS + i * step_ms,
            "open": base + i, "high": base + i + 0.5,
            "low": base + i - 0.5, "close": base + i, "volume": 1000.0,
        }
        for i in range(n)
    ]


def _nested():
    """{symbol: {period: [bars]}}：3 根 1h + 9 根 15m → 时间轴共 12 事件。

    对齐点（1h 与 15m 同刻 open）：t0、t0+1h、t0+2h。
    """
    return {
        "TESTUSD": {
            "1h": _series("1h", H1, 3),
            "15m": _series("15m", M15, 9),
        }
    }


def _wire_multi(nested, periods=("15m", "1h"), main="15m"):
    """组装多周期 feed + executor + api（滑点/手续费置零）。"""
    spec = _spec()
    feed = BacktestDataFeed(nested, periods=list(periods))
    ex = BacktestExecutor(
        feed=feed, specs={"TESTUSD": spec}, initial_capital=Decimal("10000"),
        slippage_model="fixed", slippage_params={"ticks": Decimal("0")},
        fee_model="fixed", fee_params={"fee_per_trade": Decimal("0")},
    )
    api = KqApi(
        symbol="TESTUSD", period=main, periods=list(periods), tag=main,
        specs={"TESTUSD": spec}, ledger=ExposureLedger(),
        resolver=UnifiedResolver(), executor=ex, feed=feed,
    )
    return feed, ex, api


# ─── 派发顺序：同一时刻大周期先（1h 先于 15m） ───


def test_same_timestamp_large_period_dispatched_first():
    """时间轴派发序列：三个对齐点上 1h 事件紧邻在 15m 事件之前。"""
    feed = BacktestDataFeed(_nested(), periods=["15m", "1h"])
    seq = []
    while feed.wait_update():
        if feed.is_changing("TESTUSD/1h", "timestamp"):
            seq.append("1h")
        elif feed.is_changing("TESTUSD/15m", "timestamp"):
            seq.append("15m")
    assert seq == [
        "1h", "15m", "15m", "15m", "15m",
        "1h", "15m", "15m", "15m", "15m",
        "1h", "15m",
    ]


def test_is_changing_distinguishes_period():
    """每步恰有一个周期在收盘（is_changing 天然区分哪个周期推进）。"""
    feed = BacktestDataFeed(_nested(), periods=["15m", "1h"])
    steps = 0
    h_events = 0
    while feed.wait_update():
        steps += 1
        h = feed.is_changing("TESTUSD/1h", "timestamp")
        m = feed.is_changing("TESTUSD/15m", "timestamp")
        assert h != m                 # 恰一个周期收盘事件
        if h:
            h_events += 1
    assert steps == 12                # 3 + 9
    assert h_events == 3              # 3 根 1h


# ─── 小周期读到大周期最新结论 + per-period 反 look-ahead ───


def test_no_future_bars_and_large_period_visible_at_alignment():
    """任一步可见 bar 的 timestamp ≤ 当前时间轴（无 look-ahead）；且对齐点的
    15m 步能看到同刻 1h bar（大周期先派发 → 小周期读到最新结论）。"""
    feed, ex, api = _wire_multi(_nested())
    aligned_seen = []
    while api.wait_update():
        now = api.now()
        for per in ("15m", "1h"):
            for b in api.klines(per, count=999):
                assert b["timestamp"] <= now, (
                    f"look-ahead: {per} bar {b['timestamp']} > now {now}"
                )
        if api.is_changing("TESTUSD/15m", "timestamp"):
            ts15 = api.klines("15m", count=1)[-1]["timestamp"]
            h_ts = {b["timestamp"] for b in api.klines("1h", count=999)}
            if ts15 in h_ts:                       # 对齐点
                aligned_seen.append(ts15)
    assert aligned_seen == [BASE_TS, BASE_TS + H1, BASE_TS + 2 * H1]


def test_per_period_pointer_no_lookahead():
    """每周期独立指针：早期步看不到该周期未来 bar（数量 = 已揭示根数）。"""
    feed = BacktestDataFeed(_nested(), periods=["15m", "1h"])
    h_seen = []
    m_seen = []
    while feed.wait_update():
        h_seen.append(len(feed.latest_bars("TESTUSD", "1h", 999)))
        m_seen.append(len(feed.latest_bars("TESTUSD", "15m", 999)))
    # 末步：1h 全部 3 根、15m 全部 9 根揭示
    assert h_seen[-1] == 3
    assert m_seen[-1] == 9
    # 首步（1h@t0）：1h 已见 1 根，15m 尚未揭示
    assert h_seen[0] == 1
    assert m_seen[0] == 0


# ─── BacktestRunner 端到端（嵌套注入） ───


def _runner_strat(a):
    """多周期策略：15m 步、当 1h 收盘走高时开多一次（读大周期结论）。"""
    opened = False
    while a.wait_update():
        if opened or not a.is_changing("TESTUSD/15m", "timestamp"):
            continue
        h = a.klines("1h", count=2)
        if len(h) >= 2 and h[-1]["close"] > h[-2]["close"]:
            a.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.01"),
                         kind=OrderKind.MARKET, period="15m")
            opened = True


def test_runner_multiperiod_end_to_end():
    """BacktestRunner 端到端：嵌套 bars + periods 跑通，出报告，n_bars=主周期根数。"""
    runner = BacktestRunner(
        symbols=["TESTUSD"], period="15m", periods=["15m", "1h"],
        strategy_fn=_runner_strat, initial_capital=Decimal("10000"),
        slippage_model="fixed", slippage_params={"ticks": Decimal("0")},
        fee_model="fixed", fee_params={"fee_per_trade": Decimal("0")},
        bars_by_symbol=_nested(), specs={"TESTUSD": _spec()},
    )
    report = runner.run()
    res = report.results["TESTUSD"]
    assert res.n_bars == 9                    # 主周期 15m 根数
    assert res.report.total_trades >= 1       # 策略确实在 1h 走高时开多并末尾清仓
    assert len(res.equity_curve) > 0


def test_runner_single_period_flat_injection_still_works():
    """向后兼容：单周期扁平 bars 注入（无 periods）仍跑通，逐根回放。"""
    bars = _series("1m", 60_000, 8)
    runner = BacktestRunner(
        symbols=["TESTUSD"], period="1m", strategy_fn=_runner_strat_1m,
        initial_capital=Decimal("10000"),
        slippage_model="fixed", slippage_params={"ticks": Decimal("0")},
        fee_model="fixed", fee_params={"fee_per_trade": Decimal("0")},
        bars_by_symbol={"TESTUSD": bars}, specs={"TESTUSD": _spec()},
    )
    res = runner.run().results["TESTUSD"]
    assert res.n_bars == 8


def _runner_strat_1m(a):
    """单周期占位策略：仅消费 bar，不下单。"""
    n = 0
    while a.wait_update():
        n += 1
        a.klines(count=999)

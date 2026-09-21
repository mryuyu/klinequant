"""回测/实盘同构单测

验证同一份 strategy(api: KqApi) 在回测中运行：
  - 反 look-ahead：klines 只见已回放 bar，市价单成交在当前 bar 开盘价
  - 净持仓记账 + 已实现盈亏（按 contract_multiplier 换算）
  - 数据耗尽后 flatten 清仓
  - BacktestRunner 端到端 + 确定性（同输入同结果）

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

BASE_TS = 1_600_000_000_000
_INFO = {
    "digits": 5, "point": 0.00001, "trade_contract_size": 100000,
    "volume_step": 0.01, "volume_min": 0.01, "volume_max": 200,
    "trade_mode": 4, "spread": 0,
    "currency_base": "EUR", "currency_profit": "USD", "margin_currency": "USD",
}


def _spec(symbol: str = "TESTUSD"):
    return load_spec_from_mt5_dict(_INFO, symbol)


def _mk(opens, closes=None, symbol="TESTUSD"):
    """构造 bar dict 序列（与 Mt5DataFeed 同形）"""
    closes = closes or opens
    return [
        {
            "symbol": symbol, "period": "1m", "timestamp": BASE_TS + i * 60000,
            "open": float(opens[i]), "high": float(max(opens[i], closes[i])),
            "low": float(min(opens[i], closes[i])), "close": float(closes[i]),
            "volume": 1000.0,
        }
        for i in range(len(opens))
    ]


def _wire(bars, capital="10000"):
    """组装 feed + executor + api（滑点/手续费置零，成交价=开盘价，便于精确断言）"""
    spec = _spec()
    feed = BacktestDataFeed({"TESTUSD": bars}, "1m")
    ex = BacktestExecutor(
        feed=feed, specs={"TESTUSD": spec}, initial_capital=Decimal(capital),
        slippage_model="fixed", slippage_params={"ticks": Decimal("0")},
        fee_model="fixed", fee_params={"fee_per_trade": Decimal("0")},
    )
    api = KqApi(
        symbol="TESTUSD", period="1m", tag="1m", specs={"TESTUSD": spec},
        ledger=ExposureLedger(), resolver=UnifiedResolver(), executor=ex, feed=feed,
    )
    return feed, ex, api


# ─── 反 look-ahead ───


def test_fill_uses_current_bar_open_not_close():
    """市价单成交在当前 bar 开盘价（非收盘价），证明未偷看当根收盘。"""
    opens = [1.00, 1.00, 1.10, 1.00, 1.00, 1.00]
    closes = [1.00, 1.00, 1.20, 1.00, 1.00, 1.00]  # index2 收盘 1.20 ≠ 开盘 1.10
    feed, ex, api = _wire(_mk(opens, closes))

    def strat(a):
        n = 0
        while a.wait_update():
            n += 1
            if n == 3:  # index=2，成交应落在 open[2]=1.10
                r = a.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.01"),
                                 kind=OrderKind.MARKET)
                assert r.ok
                assert r.filled_price == Decimal("1.1")

    strat(api)
    pos = ex.query_positions("TESTUSD")
    assert len(pos) == 1
    assert abs(pos[0]["price_open"] - 1.10) < 1e-9


def test_no_future_bars_visible():
    """每次 wait_update 后 klines 只返回已回放的 bar（数量 = index+1）。"""
    bars = _mk([1.0] * 6)
    feed, ex, api = _wire(bars)
    seen = {}

    def strat(a):
        n = 0
        while a.wait_update():
            n += 1
            seen[n] = len(a.klines(count=999))

    strat(api)
    assert seen == {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6}


# ─── 净持仓记账 + 盈亏 ───


def test_netting_open_close_realizes_pnl():
    """开多→平多：已实现盈亏 = 价差 × 手数 × 合约乘数，现金正确入账。"""
    bars = _mk([1.00, 1.00, 1.10, 1.00, 1.30, 1.00])  # 开@open[2]=1.10 平@open[4]=1.30
    feed, ex, api = _wire(bars)

    def strat(a):
        n = 0
        while a.wait_update():
            n += 1
            if n == 3:
                a.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.01"),
                             kind=OrderKind.MARKET)
            elif n == 5:
                assert a.position().volume == Decimal("0.01")
                a.send_order(OrderSide.SELL, Offset.CLOSE, Decimal("0.01"),
                             kind=OrderKind.MARKET)

    strat(api)
    assert len(ex.trades) == 1
    assert float(ex.trades[0].pnl) == 200.0   # (1.30-1.10)*0.01*100000
    assert ex.cash == Decimal("10200")
    assert ex.query_positions("TESTUSD") == []  # 已平，无残留


def test_short_position_profit_when_price_falls():
    """开空→平空：价格下跌盈利。"""
    bars = _mk([1.00, 1.00, 1.30, 1.00, 1.10, 1.00])  # 开空@1.30 平@1.10
    feed, ex, api = _wire(bars)

    def strat(a):
        n = 0
        while a.wait_update():
            n += 1
            if n == 3:
                a.send_order(OrderSide.SELL, Offset.OPEN, Decimal("0.01"),
                             kind=OrderKind.MARKET)
            elif n == 5:
                a.send_order(OrderSide.BUY, Offset.CLOSE, Decimal("0.01"),
                             kind=OrderKind.MARKET)

    strat(api)
    assert len(ex.trades) == 1
    assert ex.trades[0].side == "SHORT"
    assert float(ex.trades[0].pnl) == 200.0   # (1.30-1.10)*0.01*100000
    assert ex.cash == Decimal("10200")


def test_equity_curve_marks_to_market_each_bar():
    """资金曲线逐 bar 采样，持仓期间反映浮动盈亏。"""
    bars = _mk([1.00, 1.00, 1.10, 1.20, 1.00, 1.00])  # index2 开多@1.10
    feed, ex, api = _wire(bars)

    def strat(a):
        n = 0
        while a.wait_update():
            n += 1
            if n == 3:
                a.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.01"),
                             kind=OrderKind.MARKET)

    strat(api)
    # index3 mark（on_bar 先于下单，持仓在 index2 建立）：close[3]=1.20 → 浮盈 100
    assert len(ex.equity_curve) == 6
    assert ex.equity_curve[3] == 10100.0   # (1.20-1.10)*0.01*100000 = 100


# ─── 清仓 ───


def test_flatten_closes_residual_position():
    """数据耗尽后 flatten 平掉残留持仓，成交在最后一根开盘价。"""
    bars = _mk([1.00, 1.00, 1.10, 1.10, 1.10, 1.10])
    feed, ex, api = _wire(bars)

    def strat(a):
        n = 0
        while a.wait_update():
            n += 1
            if n == 3:
                a.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.01"),
                             kind=OrderKind.MARKET)

    strat(api)
    assert len(ex.query_positions("TESTUSD")) == 1  # 仍有持仓
    api.flatten(symbol="TESTUSD")
    assert ex.query_positions("TESTUSD") == []       # 已清
    assert len(ex.trades) == 1


# ─── BacktestRunner 端到端 + 确定性 ───


def _rev_strat(a):
    """开多 → 反手开空（显式平再开），产生 2 笔交易。"""
    state = None
    n = 0
    while a.wait_update():
        n += 1
        if n == 3 and state is None:
            a.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.01"),
                         kind=OrderKind.MARKET)
            state = "long"
        elif n == 6 and state == "long":
            a.send_order(OrderSide.SELL, Offset.CLOSE, Decimal("0.01"),
                         kind=OrderKind.MARKET)
            a.send_order(OrderSide.SELL, Offset.OPEN, Decimal("0.01"),
                         kind=OrderKind.MARKET)
            state = "short"


def _run_runner(bars):
    runner = BacktestRunner(
        symbols=["TESTUSD"], period="1m", strategy_fn=_rev_strat,
        initial_capital=Decimal("10000"),
        slippage_model="fixed", slippage_params={"ticks": Decimal("0")},
        fee_model="fixed", fee_params={"fee_per_trade": Decimal("0")},
        bars_by_symbol={"TESTUSD": bars}, specs={"TESTUSD": _spec()},
    )
    return runner.run()


def test_runner_produces_report_and_trades():
    """BacktestRunner 端到端：出绩效报告，交易被记录，末尾自动清仓。"""
    bars = _mk([1.00, 1.00, 1.10, 1.15, 1.20, 1.25, 1.30, 1.35])
    report = _run_runner(bars)
    res = report.results["TESTUSD"]
    assert res.n_bars == 8
    assert res.report.total_trades >= 1
    assert len(res.equity_curve) > 0
    assert res.report.initial_capital == 10000.0


def test_runner_deterministic():
    """同输入两次回测结果一致（确定性）。"""
    bars = _mk([1.00, 1.00, 1.10, 1.15, 1.20, 1.25, 1.30, 1.35])
    r1 = _run_runner(bars).results["TESTUSD"].report
    r2 = _run_runner(bars).results["TESTUSD"].report
    assert r1.total_trades == r2.total_trades
    assert r1.final_equity == r2.final_equity
    assert r1.total_return == r2.total_return


# ─── 跨币种盈亏换算 ───


def test_usd_base_pair_pnl_converted_to_account_currency():
    """USDJPY（base=USD, quote=JPY）盈亏 ÷价格 换算到账户币 USD，避免被放大 ~157 倍。"""
    info = dict(_INFO)
    info.update({"currency_base": "USD", "currency_profit": "JPY",
                 "digits": 3, "point": 0.001})
    spec = load_spec_from_mt5_dict(info, "USDJPY")
    # 开空@open[2]=157.00，平@open[4]=156.00 → gross=1.00×0.01×100000=1000 JPY
    bars = _mk([157.0, 157.0, 157.00, 157.0, 156.00, 157.0], symbol="USDJPY")
    feed = BacktestDataFeed({"USDJPY": bars}, "1m")
    ex = BacktestExecutor(
        feed=feed, specs={"USDJPY": spec}, initial_capital=Decimal("10000"),
        slippage_model="fixed", slippage_params={"ticks": Decimal("0")},
        fee_model="fixed", fee_params={"fee_per_trade": Decimal("0")},
    )
    api = KqApi(
        symbol="USDJPY", period="1m", tag="1m", specs={"USDJPY": spec},
        ledger=ExposureLedger(), resolver=UnifiedResolver(), executor=ex, feed=feed,
    )

    def strat(a):
        n = 0
        while a.wait_update():
            n += 1
            if n == 3:
                a.send_order(OrderSide.SELL, Offset.OPEN, Decimal("0.01"),
                             kind=OrderKind.MARKET)
            elif n == 5:
                a.send_order(OrderSide.BUY, Offset.CLOSE, Decimal("0.01"),
                             kind=OrderKind.MARKET)

    strat(api)
    # 1000 JPY ÷ 156.00 ≈ 6.41 USD
    assert len(ex.trades) == 1
    assert 6.0 < float(ex.trades[0].pnl) < 7.0
    assert 10006.0 < float(ex.cash) < 10007.0


def test_cross_pair_uses_conv_rate_from_mt5():
    """交叉盘 EURJPY（base/quote 均非 USD）用 conv_rates 系数换算到账户币。"""
    info = dict(_INFO)
    info.update({"currency_base": "EUR", "currency_profit": "JPY",
                 "digits": 3, "point": 0.001})
    spec = load_spec_from_mt5_dict(info, "EURJPY")
    # 开空@open[2]=157.00，平@open[4]=156.00 → gross=1.00×0.01×100000=1000 JPY
    bars = _mk([157.0, 157.0, 157.00, 157.0, 156.00, 157.0], symbol="EURJPY")
    feed = BacktestDataFeed({"EURJPY": bars}, "1m")
    ex = BacktestExecutor(
        feed=feed, specs={"EURJPY": spec}, initial_capital=Decimal("10000"),
        slippage_model="fixed", slippage_params={"ticks": Decimal("0")},
        fee_model="fixed", fee_params={"fee_per_trade": Decimal("0")},
        conv_rates={"EURJPY": Decimal("0.0064")},  # ≈1/156，runner 经 MT5 推导
    )
    api = KqApi(
        symbol="EURJPY", period="1m", tag="1m", specs={"EURJPY": spec},
        ledger=ExposureLedger(), resolver=UnifiedResolver(), executor=ex, feed=feed,
    )

    def strat(a):
        n = 0
        while a.wait_update():
            n += 1
            if n == 3:
                a.send_order(OrderSide.SELL, Offset.OPEN, Decimal("0.01"),
                             kind=OrderKind.MARKET)
            elif n == 5:
                a.send_order(OrderSide.BUY, Offset.CLOSE, Decimal("0.01"),
                             kind=OrderKind.MARKET)

    strat(api)
    # 1000 JPY × 0.0064 = 6.4 USD（不再原样当 1000 USD）
    assert len(ex.trades) == 1
    assert abs(float(ex.trades[0].pnl) - 6.4) < 1e-6
    assert abs(float(ex.cash) - 10006.4) < 1e-6

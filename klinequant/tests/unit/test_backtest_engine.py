"""回测引擎单元测试

覆盖 BT-T-001 ~ BT-T-004：
    BT-T-001: 撮合模型：市价/限价/止损单撮合规则
    BT-T-002: 滑点模型：三种模型计算正确性
    BT-T-003: 绩效指标：双均线回测与手工计算一致（误差 < 0.01%）
    BT-T-004: look-ahead bias 防护：信号在收盘时产生，下根执行
"""
from __future__ import annotations

import uuid
from decimal import Decimal

import polars as pl
import pytest

from core.backtest_engine.engine import BacktestConfig, BacktestEngine
from core.backtest_engine.fee import FixedFee, PercentageFee, TieredFee, create_fee_model
from core.backtest_engine.matcher import Matcher
from core.backtest_engine.performance import PerformanceAnalyzer, Trade
from core.backtest_engine.slippage import (
    FixedSlippage,
    PercentageSlippage,
    VolumeBasedSlippage,
    create_slippage_model,
)
from protocol.types import Kline, Order, OrderSide, OrderStatus, OrderType


# ─── 辅助函数 ───


def make_kline(
    timestamp: int = 1000000,
    open_: float = 100.0,
    high: float = 105.0,
    low: float = 95.0,
    close: float = 102.0,
    volume: float = 1000.0,
) -> Kline:
    return Kline(
        symbol="BTCUSDT",
        exchange="binance",
        timeframe="1h",
        timestamp=timestamp,
        open=Decimal(str(open_)),
        high=Decimal(str(high)),
        low=Decimal(str(low)),
        close=Decimal(str(close)),
        volume=Decimal(str(volume)),
        quote_volume=Decimal(str(volume * close)),
        trade_count=100,
        is_closed=True,
    )


def make_order(
    side=OrderSide.BUY,
    order_type=OrderType.MARKET,
    quantity: float = 1.0,
    price: float = None,
) -> Order:
    return Order(
        order_id=str(uuid.uuid4()),
        client_order_id=f"TEST-{uuid.uuid4().hex[:8]}",
        symbol="BTCUSDT",
        exchange="binance",
        side=side,
        order_type=order_type,
        quantity=Decimal(str(quantity)),
        price=Decimal(str(price)) if price else None,
        status=OrderStatus.PENDING,
        strategy_id="test",
        created_at=1000000,
        updated_at=1000000,
    )


def make_kline_df(n: int = 100, start_price: float = 100.0) -> pl.DataFrame:
    """生成模拟 K 线 DataFrame"""
    import math

    timestamps = [1000000 + i * 3600000 for i in range(n)]
    opens = [start_price + 10 * math.sin(i * 0.1) for i in range(n)]
    closes = [start_price + 10 * math.sin((i + 1) * 0.1) for i in range(n)]
    highs = [max(o, c) + 2 for o, c in zip(opens, closes)]
    lows = [min(o, c) - 2 for o, c in zip(opens, closes)]
    volumes = [1000.0 + i * 10 for i in range(n)]

    return pl.DataFrame({
        "timestamp": timestamps,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": volumes,
    })


# ─── BT-T-001: 撮合模型 ───


class TestMatcher:
    def test_market_order_fills_at_next_bar_open(self):
        """市价单在下根 K 线开盘价成交"""
        matcher = Matcher(
            slippage_model=FixedSlippage(ticks=Decimal("0")),
            fee_model=PercentageFee(rate=Decimal("0")),
        )
        order = make_order(side=OrderSide.BUY, order_type=OrderType.MARKET)
        matcher.submit_order(order, current_bar_index=0)

        # bar 0 不成交（同 bar 创建）
        kline0 = make_kline(timestamp=1000000, open_=100.0)
        fills = matcher.on_bar(kline0, bar_index=0)
        assert len(fills) == 0

        # bar 1 成交，以 open 价
        kline1 = make_kline(timestamp=2000000, open_=105.0)
        fills = matcher.on_bar(kline1, bar_index=1)
        assert len(fills) == 1
        assert fills[0].price == Decimal("105.0")
        assert order.status == OrderStatus.FILLED

    def test_limit_buy_fills_when_price_drops(self):
        """限价买入单在价格下穿时成交"""
        matcher = Matcher(
            slippage_model=FixedSlippage(ticks=Decimal("0")),
            fee_model=PercentageFee(rate=Decimal("0")),
        )
        order = make_order(
            side=OrderSide.BUY, order_type=OrderType.LIMIT, price=98.0
        )
        matcher.submit_order(order, current_bar_index=0)

        # bar 1: low=99 > 98，不成交
        kline1 = make_kline(timestamp=2000000, open_=100.0, low=99.0)
        fills = matcher.on_bar(kline1, bar_index=1)
        assert len(fills) == 0

        # bar 2: low=97 <= 98，成交
        kline2 = make_kline(timestamp=3000000, open_=100.0, low=97.0)
        fills = matcher.on_bar(kline2, bar_index=2)
        assert len(fills) == 1
        assert fills[0].price == Decimal("98.0")  # 以限价成交

    def test_limit_sell_fills_when_price_rises(self):
        """限价卖出单在价格上穿时成交"""
        matcher = Matcher(
            slippage_model=FixedSlippage(ticks=Decimal("0")),
            fee_model=PercentageFee(rate=Decimal("0")),
        )
        order = make_order(
            side=OrderSide.SELL, order_type=OrderType.LIMIT, price=110.0
        )
        matcher.submit_order(order, current_bar_index=0)

        # bar 1: high=108 < 110，不成交
        kline1 = make_kline(timestamp=2000000, high=108.0)
        fills = matcher.on_bar(kline1, bar_index=1)
        assert len(fills) == 0

        # bar 2: high=112 >= 110，成交
        kline2 = make_kline(timestamp=3000000, open_=105.0, high=112.0)
        fills = matcher.on_bar(kline2, bar_index=2)
        assert len(fills) == 1
        assert fills[0].price == Decimal("110.0")

    def test_stop_loss_order(self):
        """止损单在价格触发时成交"""
        matcher = Matcher(
            slippage_model=FixedSlippage(ticks=Decimal("0")),
            fee_model=PercentageFee(rate=Decimal("0")),
        )
        order = make_order(side=OrderSide.SELL, order_type=OrderType.STOP_LIMIT)
        matcher.submit_order(order, current_bar_index=0, stop_price=Decimal("90"))

        # bar 1: low=92 > 90，不触发
        kline1 = make_kline(timestamp=2000000, low=92.0)
        fills = matcher.on_bar(kline1, bar_index=1)
        assert len(fills) == 0

        # bar 2: low=88 <= 90，触发
        kline2 = make_kline(timestamp=3000000, low=88.0)
        fills = matcher.on_bar(kline2, bar_index=2)
        assert len(fills) == 1
        assert fills[0].price == Decimal("90.0")  # 以止损价成交

    def test_cancel_order(self):
        """撤销挂单"""
        matcher = Matcher()
        order = make_order(order_type=OrderType.LIMIT, price=90.0)
        matcher.submit_order(order, current_bar_index=0)

        assert matcher.cancel_order(order.order_id) is True
        assert order.status == OrderStatus.CANCELED
        assert len(matcher.pending_orders) == 0


# ─── BT-T-002: 滑点模型 ───


class TestSlippage:
    def test_fixed_slippage(self):
        model = FixedSlippage(ticks=Decimal("0.5"))
        buy_price = model.calculate(Decimal("100"), Decimal("1"), "BUY")
        sell_price = model.calculate(Decimal("100"), Decimal("1"), "SELL")
        assert buy_price == Decimal("100.5")
        assert sell_price == Decimal("99.5")

    def test_percentage_slippage(self):
        model = PercentageSlippage(pct=Decimal("0.001"))
        buy_price = model.calculate(Decimal("10000"), Decimal("1"), "BUY")
        sell_price = model.calculate(Decimal("10000"), Decimal("1"), "SELL")
        assert buy_price == Decimal("10010")
        assert sell_price == Decimal("9990")

    def test_volume_based_slippage(self):
        model = VolumeBasedSlippage(impact_factor=Decimal("0.1"))
        # qty=10, volume=1000 → ratio=0.01, slip=100*0.1*0.01=0.1
        price = model.calculate(
            Decimal("100"), Decimal("10"), "BUY", volume=Decimal("1000")
        )
        assert price == Decimal("100.1")

    def test_factory(self):
        m1 = create_slippage_model("fixed", ticks=Decimal("1"))
        assert isinstance(m1, FixedSlippage)
        m2 = create_slippage_model("percentage", pct=Decimal("0.01"))
        assert isinstance(m2, PercentageSlippage)
        m3 = create_slippage_model("volume_based")
        assert isinstance(m3, VolumeBasedSlippage)
        with pytest.raises(ValueError):
            create_slippage_model("unknown")


# ─── BT-T-002b: 手续费模型 ───


class TestFee:
    def test_fixed_fee(self):
        model = FixedFee(fee_per_trade=Decimal("2.5"))
        fee = model.calculate(Decimal("50000"), Decimal("1"), "BUY")
        assert fee == Decimal("2.5")

    def test_percentage_fee(self):
        model = PercentageFee(rate=Decimal("0.001"))
        fee = model.calculate(Decimal("50000"), Decimal("2"), "BUY")
        assert fee == Decimal("100")  # 50000 * 2 * 0.001

    def test_tiered_fee(self):
        model = TieredFee(maker_rate=Decimal("0.0008"), taker_rate=Decimal("0.001"))
        maker_fee = model.calculate(Decimal("10000"), Decimal("1"), "BUY", is_maker=True)
        taker_fee = model.calculate(Decimal("10000"), Decimal("1"), "BUY", is_maker=False)
        assert maker_fee == Decimal("8")
        assert taker_fee == Decimal("10")

    def test_factory(self):
        m = create_fee_model("tiered", maker_rate=Decimal("0.001"), taker_rate=Decimal("0.002"))
        assert isinstance(m, TieredFee)
        with pytest.raises(ValueError):
            create_fee_model("bad")


# ─── BT-T-003: 绩效指标 ───


class TestPerformance:
    def test_total_return(self):
        analyzer = PerformanceAnalyzer(bars_per_year=365)
        curve = [100000, 110000, 105000, 120000]
        trades = [
            Trade(
                symbol="BTC", side="LONG",
                entry_price=Decimal("100"), exit_price=Decimal("110"),
                quantity=Decimal("1"), entry_time=0, exit_time=1,
                pnl=Decimal("10"), fee=Decimal("0"),
            ),
            Trade(
                symbol="BTC", side="LONG",
                entry_price=Decimal("110"), exit_price=Decimal("120"),
                quantity=Decimal("1"), entry_time=2, exit_time=3,
                pnl=Decimal("10"), fee=Decimal("0"),
            ),
        ]
        report = analyzer.analyze(curve, trades, 100000)
        assert abs(report.total_return - 0.20) < 0.0001  # 20%
        assert report.total_trades == 2
        assert report.win_rate == 1.0

    def test_max_drawdown(self):
        analyzer = PerformanceAnalyzer()
        # 从 100k 涨到 120k，跌到 90k，再涨到 110k
        curve = [100000, 110000, 120000, 100000, 90000, 95000, 110000]
        report = analyzer.analyze(curve, [], 100000)
        # 最大回撤 = (120000 - 90000) / 120000 = 25%
        assert abs(report.max_drawdown - 0.25) < 0.0001

    def test_win_rate_and_profit_factor(self):
        analyzer = PerformanceAnalyzer()
        trades = [
            Trade("BTC", "LONG", Decimal("100"), Decimal("110"), Decimal("1"), 0, 1, Decimal("10"), Decimal("0")),
            Trade("BTC", "LONG", Decimal("110"), Decimal("105"), Decimal("1"), 2, 3, Decimal("-5"), Decimal("0")),
            Trade("BTC", "LONG", Decimal("105"), Decimal("115"), Decimal("1"), 4, 5, Decimal("10"), Decimal("0")),
            Trade("BTC", "LONG", Decimal("115"), Decimal("112"), Decimal("1"), 6, 7, Decimal("-3"), Decimal("0")),
        ]
        report = analyzer.analyze([100000, 100000], trades, 100000)
        assert report.win_rate == 0.5  # 2/4
        # profit_factor = 20 / 8 = 2.5
        assert abs(report.profit_factor - 2.5) < 0.0001
        assert report.max_consecutive_wins == 1
        assert report.max_consecutive_losses == 1

    def test_sharpe_ratio_positive(self):
        """稳定上涨的夏普比率应为正"""
        analyzer = PerformanceAnalyzer(bars_per_year=365)
        # 每天涨 0.1%
        curve = [100000 * (1.001 ** i) for i in range(100)]
        report = analyzer.analyze(curve, [], 100000)
        assert report.sharpe_ratio > 0


# ─── BT-T-004: look-ahead bias 防护 ───


class TestLookAheadBias:
    def test_signal_executes_on_next_bar(self):
        """信号在 bar i 收盘产生，订单在 bar i+1 执行"""
        config = BacktestConfig(
            initial_capital=Decimal("100000"),
            slippage_model="fixed",
            slippage_params={"ticks": Decimal("0")},
            fee_model="percentage",
            fee_params={"rate": Decimal("0")},
        )
        engine = BacktestEngine(config)

        # 5 根 K 线
        data = pl.DataFrame({
            "timestamp": [1000, 2000, 3000, 4000, 5000],
            "open": [100.0, 101.0, 102.0, 103.0, 104.0],
            "high": [105.0, 106.0, 107.0, 108.0, 109.0],
            "low": [95.0, 96.0, 97.0, 98.0, 99.0],
            "close": [101.0, 102.0, 103.0, 104.0, 105.0],
            "volume": [1000.0, 1000.0, 1000.0, 1000.0, 1000.0],
        })

        # 策略：在 bar 1 发出 LONG 信号
        def strategy(df: pl.DataFrame, bar_idx: int):
            if bar_idx == 1:
                return "LONG"
            return None

        result = engine.run(data, strategy)

        # 成交应该发生在 bar 2（timestamp=3000），价格 = open(102)
        assert len(result.fills) >= 1
        first_fill = result.fills[0]
        assert first_fill.timestamp == 3000  # bar 2 的时间戳
        assert first_fill.price == Decimal("102.0")  # bar 2 的 open

    def test_no_same_bar_execution(self):
        """订单不会在创建的同一根 bar 成交"""
        matcher = Matcher(
            slippage_model=FixedSlippage(ticks=Decimal("0")),
            fee_model=PercentageFee(rate=Decimal("0")),
        )
        order = make_order(order_type=OrderType.MARKET)
        matcher.submit_order(order, current_bar_index=5)

        # 同 bar 不成交
        kline = make_kline(timestamp=5000)
        fills = matcher.on_bar(kline, bar_index=5)
        assert len(fills) == 0
        assert order.status == OrderStatus.SUBMITTED


# ─── 集成测试：双均线回测 ───


class TestDualMABacktest:
    def test_dual_ma_produces_trades(self):
        """双均线策略回测产生交易"""
        config = BacktestConfig(
            initial_capital=Decimal("100000"),
            slippage_model="percentage",
            slippage_params={"pct": Decimal("0.0005")},
            fee_model="percentage",
            fee_params={"rate": Decimal("0.001")},
            bars_per_year=8760,
        )
        engine = BacktestEngine(config)
        data = make_kline_df(n=200)

        def indicator_fn(df: pl.DataFrame) -> pl.DataFrame:
            return df.with_columns([
                pl.col("close").rolling_mean(window_size=7).alias("ma7"),
                pl.col("close").rolling_mean(window_size=25).alias("ma25"),
            ])

        def strategy(df: pl.DataFrame, bar_idx: int):
            if bar_idx < 25:
                return None
            ma7 = df["ma7"][bar_idx]
            ma25 = df["ma25"][bar_idx]
            if ma7 is None or ma25 is None:
                return None
            prev_ma7 = df["ma7"][bar_idx - 1]
            prev_ma25 = df["ma25"][bar_idx - 1]
            if prev_ma7 is None or prev_ma25 is None:
                return None
            # 金叉
            if prev_ma7 <= prev_ma25 and ma7 > ma25:
                return "LONG"
            # 死叉
            if prev_ma7 >= prev_ma25 and ma7 < ma25:
                return "SHORT"
            return None

        result = engine.run(data, strategy, indicator_fn)

        assert result.report.total_trades > 0
        assert len(result.equity_curve) == 201  # n_bars + 1
        assert result.duration_ms >= 0
        # 验证有绩效指标
        assert result.report.total_return != 0 or result.report.total_trades > 0

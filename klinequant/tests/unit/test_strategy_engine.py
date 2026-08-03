"""策略框架单元测试

覆盖 STR-T-001 ~ STR-T-004：
    STR-T-001: SDK TradeClient：下单/撤单/查询（mock）
    STR-T-002: SDK MarketClient：K线/指标查询（mock）
    STR-T-003: 策略沙箱：崩溃隔离（策略异常不影响主进程）
    STR-T-004: 双均线策略：金叉买入 + 死叉卖出（回测验证）
"""
from __future__ import annotations

import asyncio
import math
import time
from decimal import Decimal
from typing import Optional

import polars as pl
import pytest

from core.strategy_engine.base import StrategyBase
from core.strategy_engine.clients import MarketClient, TradeClient
from core.strategy_engine.context import StrategyContext, StrategyInfo
from core.strategy_engine.manager import StrategyManager, StrategyStatus
from core.strategy_engine.sandbox import SandboxStatus, StrategySandbox
from protocol.types import OrderSide, OrderType
from strategies.dual_ma import DualMAStrategy


# ─── 辅助 ───


def make_info(strategy_id="test-001", **kwargs) -> StrategyInfo:
    return StrategyInfo(
        strategy_id=strategy_id,
        name=kwargs.get("name", "Test Strategy"),
        parameters=kwargs.get("parameters", {}),
    )


def make_context(strategy_id="test-001", **kwargs) -> StrategyContext:
    return StrategyContext(make_info(strategy_id, **kwargs))


def make_kline_df(n: int = 100, start_price: float = 100.0) -> pl.DataFrame:
    timestamps = [1000000 + i * 3600000 for i in range(n)]
    opens = [start_price + 10 * math.sin(i * 0.1) for i in range(n)]
    closes = [start_price + 10 * math.sin((i + 1) * 0.1) for i in range(n)]
    highs = [max(o, c) + 2 for o, c in zip(opens, closes)]
    lows = [min(o, c) - 2 for o, c in zip(opens, closes)]
    volumes = [1000.0] * n
    return pl.DataFrame({
        "timestamp": timestamps,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": volumes,
    })


# ─── STR-T-001: TradeClient ───


class TestTradeClient:
    @pytest.mark.asyncio
    async def test_submit_order_with_callback(self):
        """TradeClient 通过回调下单"""
        submitted = []

        async def mock_submit(**kwargs):
            submitted.append(kwargs)
            return {"order_id": "ORD-001", "status": "SUBMITTED"}

        client = TradeClient("strat-1")
        client.set_callbacks(submit_cb=mock_submit)

        result = await client.submit_order(
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.1"),
        )
        assert result["order_id"] == "ORD-001"
        assert len(submitted) == 1
        assert submitted[0]["symbol"] == "BTCUSDT"
        assert submitted[0]["strategy_id"] == "strat-1"

    @pytest.mark.asyncio
    async def test_cancel_order_with_callback(self):
        """TradeClient 通过回调撤单"""
        async def mock_cancel(order_id):
            return True

        client = TradeClient("strat-1")
        client.set_callbacks(cancel_cb=mock_cancel)

        result = await client.cancel_order("ORD-001")
        assert result is True

    @pytest.mark.asyncio
    async def test_query_positions_with_callback(self):
        """TradeClient 查询持仓"""
        async def mock_positions(symbol=None):
            return {"BTCUSDT": {"qty": 1.0, "side": "LONG"}}

        client = TradeClient("strat-1")
        client.set_callbacks(query_positions_cb=mock_positions)

        positions = await client.get_positions("BTCUSDT")
        assert "BTCUSDT" in positions

    @pytest.mark.asyncio
    async def test_no_callback_returns_none(self):
        """无回调时返回 None"""
        client = TradeClient("strat-1")
        result = await client.submit_order(
            symbol="BTC", side=OrderSide.BUY,
            order_type=OrderType.MARKET, quantity=Decimal("1"),
        )
        assert result is None


# ─── STR-T-002: MarketClient ───


class TestMarketClient:
    @pytest.mark.asyncio
    async def test_get_klines_with_callback(self):
        """MarketClient 通过回调获取 K 线"""
        async def mock_klines(symbol, timeframe, limit):
            return [{"close": 100 + i} for i in range(limit)]

        client = MarketClient("strat-1")
        client.set_callbacks(get_klines_cb=mock_klines)

        klines = await client.get_klines("BTCUSDT", "1h", 10)
        assert len(klines) == 10
        assert klines[0]["close"] == 100

    @pytest.mark.asyncio
    async def test_get_indicators_with_callback(self):
        """MarketClient 获取指标值"""
        async def mock_indicators(symbol, name, timeframe):
            return {"ma_7": 105.5, "ma_25": 102.3}

        client = MarketClient("strat-1")
        client.set_callbacks(get_indicators_cb=mock_indicators)

        values = await client.get_indicators("BTCUSDT", "ma_7", "1h")
        assert values["ma_7"] == 105.5

    @pytest.mark.asyncio
    async def test_no_callback_returns_empty(self):
        """无回调时返回空"""
        client = MarketClient("strat-1")
        klines = await client.get_klines("BTC", "1h", 10)
        assert klines == []


# ─── STR-T-003: 策略沙箱 ───


def _crash_fn():
    """会崩溃的策略函数"""
    raise RuntimeError("Strategy crashed!")


def _normal_fn():
    """正常完成的策略函数"""
    return 42


def _long_running_fn():
    """长时间运行的函数"""
    import time
    time.sleep(60)


class TestSandbox:
    def test_crash_isolation(self):
        """策略崩溃不影响主进程"""
        sandbox = StrategySandbox("crash-test")
        sandbox.start(_crash_fn)

        # 等待进程结束
        time.sleep(1.0)
        result = sandbox.poll(timeout=2.0)

        assert result is not None
        assert result.status == SandboxStatus.CRASHED
        assert "RuntimeError" in result.error
        assert "Strategy crashed!" in result.error
        # 主进程仍然正常
        assert True

    def test_normal_execution(self):
        """正常策略执行完成"""
        sandbox = StrategySandbox("normal-test")
        sandbox.start(_normal_fn)

        time.sleep(1.0)
        result = sandbox.poll(timeout=2.0)

        assert result is not None
        assert result.status == SandboxStatus.STOPPED
        assert result.data["result"] == 42

    def test_stop_running_process(self):
        """停止正在运行的进程"""
        sandbox = StrategySandbox("long-test")
        sandbox.start(_long_running_fn)
        assert sandbox.is_alive

        result = sandbox.stop(timeout=2.0)
        assert result.status == SandboxStatus.STOPPED
        assert not sandbox.is_alive


# ─── STR-T-004: 双均线策略 ───


class TestDualMAStrategy:
    def test_golden_cross_signal(self):
        """金叉产生 LONG 信号"""
        ctx = make_context(parameters={"fast_period": 3, "slow_period": 7})
        strategy = DualMAStrategy(
            context=ctx,
            trade_client=TradeClient("test"),
            market_client=MarketClient("test"),
        )
        strategy.on_init()

        # 构造一个明确的金叉场景：先跌后涨
        # 前 7 根下跌，后 7 根上涨
        prices = [100, 99, 98, 97, 96, 95, 94, 93, 94, 96, 99, 103, 108, 114, 121]
        df = pl.DataFrame({"close": prices})

        signals = []
        for i in range(len(prices)):
            sig = strategy.on_bar(df, i)
            if sig:
                signals.append((i, sig))

        # 应该产生至少一个 LONG 信号
        long_signals = [s for s in signals if s[1] == "LONG"]
        assert len(long_signals) > 0

    def test_death_cross_signal(self):
        """死叉产生 SHORT 信号"""
        ctx = make_context(parameters={"fast_period": 3, "slow_period": 7})
        strategy = DualMAStrategy(
            context=ctx,
            trade_client=TradeClient("test"),
            market_client=MarketClient("test"),
        )
        strategy.on_init()

        # 先涨后跌
        prices = [100, 102, 104, 106, 108, 110, 112, 114, 112, 109, 105, 100, 94, 87, 79]
        df = pl.DataFrame({"close": prices})

        signals = []
        for i in range(len(prices)):
            sig = strategy.on_bar(df, i)
            if sig:
                signals.append((i, sig))

        short_signals = [s for s in signals if s[1] == "SHORT"]
        assert len(short_signals) > 0

    def test_no_signal_insufficient_data(self):
        """数据不足时不产生信号"""
        ctx = make_context(parameters={"fast_period": 7, "slow_period": 25})
        strategy = DualMAStrategy(
            context=ctx,
            trade_client=TradeClient("test"),
            market_client=MarketClient("test"),
        )
        strategy.on_init()

        df = pl.DataFrame({"close": [100.0] * 20})
        for i in range(20):
            assert strategy.on_bar(df, i) is None

    def test_backtest_integration(self):
        """双均线策略 + 回测引擎集成"""
        from core.backtest_engine.engine import BacktestConfig, BacktestEngine

        config = BacktestConfig(
            initial_capital=Decimal("100000"),
            slippage_model="percentage",
            slippage_params={"pct": Decimal("0.0005")},
            fee_model="percentage",
            fee_params={"rate": Decimal("0.001")},
        )
        engine = BacktestEngine(config)
        data = make_kline_df(n=200)

        ctx = make_context(parameters={"fast_period": 7, "slow_period": 25})
        strategy = DualMAStrategy(
            context=ctx,
            trade_client=TradeClient("bt"),
            market_client=MarketClient("bt"),
        )
        strategy.on_init()

        def strategy_fn(df: pl.DataFrame, bar_idx: int) -> Optional[str]:
            return strategy.on_bar(df, bar_idx)

        result = engine.run(data, strategy_fn)
        assert result.report.total_trades > 0
        assert len(result.equity_curve) == 201


# ─── 策略生命周期管理 ───


class TestStrategyManager:
    def test_full_lifecycle(self):
        """策略完整生命周期：加载→初始化→启动→停止→卸载"""
        manager = StrategyManager()
        info = make_info("lc-001", name="Lifecycle Test")

        managed = manager.load_strategy(
            "lc-001", DualMAStrategy, info
        )
        assert managed.status == StrategyStatus.LOADED

        manager.init_strategy("lc-001")
        assert managed.status == StrategyStatus.INITIALIZED

        manager.start_strategy("lc-001")
        assert managed.status == StrategyStatus.RUNNING
        assert manager.running_count == 1

        manager.pause_strategy("lc-001")
        assert managed.status == StrategyStatus.PAUSED

        manager.resume_strategy("lc-001")
        assert managed.status == StrategyStatus.RUNNING

        manager.stop_strategy("lc-001")
        assert managed.status == StrategyStatus.STOPPED

        manager.unload_strategy("lc-001")
        assert "lc-001" not in manager.strategies

    def test_params_hot_update(self):
        """策略参数热更新"""
        manager = StrategyManager()
        info = make_info("hu-001", parameters={"fast_period": 7})
        manager.load_strategy("hu-001", DualMAStrategy, info)
        manager.init_strategy("hu-001")

        manager.update_params("hu-001", {"fast_period": 10, "slow_period": 30})
        managed = manager.strategies["hu-001"]
        assert managed.context.get_param("fast_period") == 10
        assert managed.context.get_param("slow_period") == 30

    def test_context_state(self):
        """策略上下文状态存储"""
        ctx = make_context("state-001")
        ctx.set_state("position", "LONG")
        ctx.set_state("entry_price", 50000)

        assert ctx.get_state("position") == "LONG"
        assert ctx.get_state("entry_price") == 50000
        assert ctx.get_state("nonexist", "default") == "default"

        # 状态导出/导入
        state = ctx.get_all_state()
        ctx2 = make_context("state-002")
        ctx2.load_state(state)
        assert ctx2.get_state("position") == "LONG"

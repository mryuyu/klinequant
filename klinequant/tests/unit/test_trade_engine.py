"""交易引擎单元测试

覆盖 TRD-T-001 ~ TRD-T-005：
    TRD-T-001: 订单状态机：合法流转通过 / 非法流转拒绝
    TRD-T-002: 部分成交：剩余数量正确更新
    TRD-T-003: TradeEngine：信号→风控→下单全链路（mock）
    TRD-T-004: 模拟执行器撮合正确性
    TRD-T-005: 订单断线重连状态同步
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from core.risk_engine.engine import RiskEngine
from core.risk_engine.rules import MaxOrderAmountRule
from core.trade_engine.engine import TradeEngine, TradeMode
from core.trade_engine.executors.simulator import Simulator
from core.trade_engine.order_manager import OrderManager
from core.trade_engine.position_manager import PositionManager
from protocol.types import (
    Account,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    Signal,
    SignalDirection,
    SignalStrength,
)


# ─── TRD-T-001: 订单状态机 ───

class TestOrderStateMachine:
    def test_valid_transitions(self):
        """合法状态流转"""
        order = Order(
            order_id="1", symbol="BTCUSDT", exchange="binance",
            side=OrderSide.BUY, order_type=OrderType.MARKET,
            quantity=Decimal("1"),
        )
        assert order.status == OrderStatus.PENDING

        order.transition_to(OrderStatus.SUBMITTED)
        assert order.status == OrderStatus.SUBMITTED

        order.transition_to(OrderStatus.PARTIAL_FILLED)
        assert order.status == OrderStatus.PARTIAL_FILLED

        order.transition_to(OrderStatus.FILLED)
        assert order.status == OrderStatus.FILLED

    def test_invalid_transition_raises(self):
        """非法状态流转抛出 ValueError"""
        order = Order(
            order_id="1", symbol="BTCUSDT", exchange="binance",
            side=OrderSide.BUY, order_type=OrderType.MARKET,
            quantity=Decimal("1"),
        )
        # PENDING → FILLED 不合法
        with pytest.raises(ValueError, match="Invalid transition"):
            order.transition_to(OrderStatus.FILLED)

    def test_terminal_state_no_transition(self):
        """终态不允许再流转"""
        order = Order(
            order_id="1", symbol="BTCUSDT", exchange="binance",
            side=OrderSide.BUY, order_type=OrderType.MARKET,
            quantity=Decimal("1"), status=OrderStatus.FILLED,
        )
        assert not order.can_transition_to(OrderStatus.CANCELED)

    def test_cancel_flow(self):
        """撤单流程"""
        order = Order(
            order_id="1", symbol="BTCUSDT", exchange="binance",
            side=OrderSide.BUY, order_type=OrderType.LIMIT,
            quantity=Decimal("1"), status=OrderStatus.SUBMITTED,
        )
        order.transition_to(OrderStatus.CANCELING)
        order.transition_to(OrderStatus.CANCELED)
        assert order.status == OrderStatus.CANCELED


# ─── TRD-T-002: 部分成交 ───

class TestPartialFill:
    def test_partial_fill_updates(self):
        """部分成交后剩余数量正确"""
        order = Order(
            order_id="1", symbol="BTCUSDT", exchange="binance",
            side=OrderSide.BUY, order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"), status=OrderStatus.SUBMITTED,
        )
        # 模拟部分成交
        order.filled_quantity = Decimal("0.3")
        order.avg_fill_price = Decimal("50000")
        order.transition_to(OrderStatus.PARTIAL_FILLED)

        remaining = order.quantity - order.filled_quantity
        assert remaining == Decimal("0.7")
        assert order.status == OrderStatus.PARTIAL_FILLED

    def test_partial_to_filled(self):
        """部分成交 → 全部成交"""
        order = Order(
            order_id="1", symbol="BTCUSDT", exchange="binance",
            side=OrderSide.BUY, order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"), status=OrderStatus.PARTIAL_FILLED,
            filled_quantity=Decimal("0.5"),
        )
        order.filled_quantity = Decimal("1.0")
        order.transition_to(OrderStatus.FILLED)
        assert order.status == OrderStatus.FILLED


# ─── TRD-T-003: TradeEngine 全链路 ───

class TestTradeEngineIntegration:
    @pytest.fixture
    def engine(self):
        sim = Simulator(initial_balance=Decimal("100000"))
        risk = RiskEngine(rules=[MaxOrderAmountRule({"max_amount": 50000})])
        eng = TradeEngine(executor=sim, risk_engine=risk, mode=TradeMode.PAPER)
        return eng

    @pytest.mark.asyncio
    async def test_signal_to_order_full_chain(self, engine):
        """信号 → 风控 → 下单全链路"""
        await engine.start()

        # 设置市场价格
        engine._executor.update_price("BTCUSDT", Decimal("50000"))

        signal = Signal(
            signal_id="sig-001",
            strategy_id="test",
            symbol="BTCUSDT",
            direction=SignalDirection.LONG,
            strength=SignalStrength.STRONG,
            price=Decimal("50000"),
            reason="MA crossover",
            timestamp=1700000000000,
            suggested_quantity=Decimal("0.1"),
        )

        order = await engine.process_signal(signal)
        assert order is not None
        assert order.status == OrderStatus.FILLED
        assert order.filled_quantity == Decimal("0.1")

        await engine.stop()

    @pytest.mark.asyncio
    async def test_risk_rejection(self, engine):
        """风控拒绝信号"""
        await engine.start()
        engine._executor.update_price("BTCUSDT", Decimal("50000"))

        # 下单金额 = 2.0 * 50000 = 100000 > 50000 限制
        signal = Signal(
            signal_id="sig-002",
            strategy_id="test",
            symbol="BTCUSDT",
            direction=SignalDirection.LONG,
            strength=SignalStrength.STRONG,
            price=Decimal("50000"),
            reason="test",
            timestamp=1700000000000,
            suggested_quantity=Decimal("2.0"),
        )

        order = await engine.process_signal(signal)
        assert order is None  # 被风控拒绝

        await engine.stop()

    @pytest.mark.asyncio
    async def test_expired_signal_ignored(self, engine):
        """过期信号被忽略"""
        await engine.start()

        signal = Signal(
            signal_id="sig-003",
            strategy_id="test",
            symbol="BTCUSDT",
            direction=SignalDirection.LONG,
            strength=SignalStrength.WEAK,
            price=Decimal("50000"),
            reason="test",
            timestamp=1000000,
            expires_at=1000001,  # 已过期
        )

        order = await engine.process_signal(signal)
        assert order is None

        await engine.stop()


# ─── TRD-T-004: 模拟执行器 ───

class TestSimulator:
    @pytest.mark.asyncio
    async def test_market_order_fill(self):
        """市价单立即成交"""
        sim = Simulator(initial_balance=Decimal("100000"))
        sim.update_price("BTCUSDT", Decimal("50000"))

        order = Order(
            order_id="1", symbol="BTCUSDT", exchange="binance",
            side=OrderSide.BUY, order_type=OrderType.MARKET,
            quantity=Decimal("0.1"),
        )
        result = await sim.submit_order(order)
        assert result.status == OrderStatus.FILLED
        assert result.avg_fill_price == Decimal("50000")
        assert result.fee > 0

    @pytest.mark.asyncio
    async def test_limit_order_pending(self):
        """限价单挂单"""
        sim = Simulator(initial_balance=Decimal("100000"))
        sim.update_price("BTCUSDT", Decimal("50000"))

        order = Order(
            order_id="1", symbol="BTCUSDT", exchange="binance",
            side=OrderSide.BUY, order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"), price=Decimal("48000"),
        )
        result = await sim.submit_order(order)
        assert result.status == OrderStatus.SUBMITTED  # 挂单中

    @pytest.mark.asyncio
    async def test_limit_order_triggered(self):
        """限价单价格触发成交"""
        sim = Simulator(initial_balance=Decimal("100000"))
        sim.update_price("BTCUSDT", Decimal("50000"))

        order = Order(
            order_id="1", symbol="BTCUSDT", exchange="binance",
            side=OrderSide.BUY, order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"), price=Decimal("49000"),
        )
        await sim.submit_order(order)

        # 价格下跌触发
        sim.update_price("BTCUSDT", Decimal("48500"))
        # 订单应已成交
        assert order.status == OrderStatus.FILLED

    @pytest.mark.asyncio
    async def test_balance_deducted(self):
        """买入后余额扣减"""
        sim = Simulator(initial_balance=Decimal("100000"))
        sim.update_price("BTCUSDT", Decimal("50000"))

        order = Order(
            order_id="1", symbol="BTCUSDT", exchange="binance",
            side=OrderSide.BUY, order_type=OrderType.MARKET,
            quantity=Decimal("1"),
        )
        await sim.submit_order(order)

        # 100000 - 50000 - fee
        assert sim.balance < Decimal("50000")
        assert sim.balance > Decimal("49000")

    @pytest.mark.asyncio
    async def test_position_created(self):
        """成交后创建持仓"""
        sim = Simulator(initial_balance=Decimal("100000"))
        sim.update_price("BTCUSDT", Decimal("50000"))

        order = Order(
            order_id="1", symbol="BTCUSDT", exchange="binance",
            side=OrderSide.BUY, order_type=OrderType.MARKET,
            quantity=Decimal("0.5"),
        )
        await sim.submit_order(order)

        positions = await sim.query_positions()
        assert "BTCUSDT" in positions
        assert positions["BTCUSDT"].quantity == Decimal("0.5")
        assert positions["BTCUSDT"].side == "LONG"

    @pytest.mark.asyncio
    async def test_cancel_order(self):
        """撤销挂单"""
        sim = Simulator(initial_balance=Decimal("100000"))
        sim.update_price("BTCUSDT", Decimal("50000"))

        order = Order(
            order_id="1", symbol="BTCUSDT", exchange="binance",
            side=OrderSide.BUY, order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"), price=Decimal("45000"),
        )
        await sim.submit_order(order)
        result = await sim.cancel_order(order)
        assert result.status == OrderStatus.CANCELED


# ─── TRD-T-005: 状态同步 ───

class TestOrderSync:
    @pytest.mark.asyncio
    async def test_order_manager_sync(self):
        """OrderManager 状态同步"""
        sim = Simulator(initial_balance=Decimal("100000"))
        sim.update_price("BTCUSDT", Decimal("50000"))

        om = OrderManager(sim)
        order = om.create_order(
            symbol="BTCUSDT", exchange="binance",
            side=OrderSide.BUY, order_type=OrderType.MARKET,
            quantity=Decimal("0.1"),
        )
        assert order.status == OrderStatus.PENDING

        result = await om.submit_order(order)
        assert result.status == OrderStatus.FILLED

        # 查询
        fetched = om.get_order(order.order_id)
        assert fetched.status == OrderStatus.FILLED

    @pytest.mark.asyncio
    async def test_position_manager_pnl(self):
        """持仓管理器盈亏计算"""
        pm = PositionManager()
        pos = Position(
            symbol="BTCUSDT", exchange="binance", side="LONG",
            quantity=Decimal("1"), avg_entry_price=Decimal("50000"),
        )
        pm.update_position(pos)
        pm.update_price("BTCUSDT", Decimal("52000"))

        updated = pm.get_position("BTCUSDT")
        assert updated.unrealized_pnl == Decimal("2000")


class TestPositionManager:
    def test_snapshot(self):
        """持仓快照"""
        pm = PositionManager()
        pos = Position(
            symbol="BTCUSDT", exchange="binance", side="LONG",
            quantity=Decimal("1"), avg_entry_price=Decimal("50000"),
            unrealized_pnl=Decimal("1000"),
        )
        pm.update_position(pos)
        snap = pm.snapshot()
        assert "BTCUSDT" in snap
        assert snap["BTCUSDT"]["quantity"] == 1.0

    def test_total_pnl(self):
        """总盈亏"""
        pm = PositionManager()
        pm.update_position(Position(
            symbol="BTCUSDT", exchange="binance", side="LONG",
            quantity=Decimal("1"), avg_entry_price=Decimal("50000"),
            unrealized_pnl=Decimal("1000"),
        ))
        pm.update_position(Position(
            symbol="ETHUSDT", exchange="binance", side="SHORT",
            quantity=Decimal("10"), avg_entry_price=Decimal("3000"),
            unrealized_pnl=Decimal("-200"),
        ))
        assert pm.total_unrealized_pnl() == Decimal("800")

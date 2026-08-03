"""风控引擎单元测试

覆盖 RISK-T-001 ~ RISK-T-004：
    RISK-T-001: 12 条规则逐条验证（通过/拒绝边界值）
    RISK-T-002: fail-closed：风控不可用时拒绝所有订单
    RISK-T-003: 风控检查延迟 < 1ms
    RISK-T-004: 风控日志写入完整性
"""
from __future__ import annotations

import time
from decimal import Decimal

import pytest

from core.risk_engine.engine import RiskEngine
from core.risk_engine.rules import (
    AvailableBalanceRule,
    ConsecutiveLossRule,
    MaxDailyLossRule,
    MaxOrderAmountRule,
    MaxPositionPerSymbolRule,
    MaxStrategyLossRule,
    MaxTotalPositionRule,
    MinOrderQuantityRule,
    NewSymbolRule,
    NightTradingRule,
    OrderFrequencyRule,
    PriceDeviationRule,
    RiskCheckResult,
    RiskContext,
    create_default_rules,
)
from protocol.types import Account, Order, OrderSide, OrderStatus, OrderType, Position


def make_order(
    symbol="BTCUSDT",
    side=OrderSide.BUY,
    quantity=Decimal("0.1"),
    price=Decimal("50000"),
    strategy_id="test_strategy",
) -> Order:
    return Order(
        order_id="test-order-001",
        symbol=symbol,
        exchange="binance",
        side=side,
        order_type=OrderType.LIMIT,
        quantity=quantity,
        price=price,
        strategy_id=strategy_id,
    )


def make_ctx(order=None, **kwargs) -> RiskContext:
    ts = kwargs.pop("timestamp", int(time.time() * 1000))
    return RiskContext(
        order=order or make_order(),
        timestamp=ts,
        **kwargs,
    )


# ─── RISK-T-001: 12 条规则逐条验证 ───

class TestRiskRules:
    def test_max_order_amount_pass(self):
        """单笔金额在限制内 → 通过"""
        rule = MaxOrderAmountRule({"max_amount": 10000})
        ctx = make_ctx(make_order(quantity=Decimal("0.1"), price=Decimal("50000")))
        # notional = 0.1 * 50000 = 5000 < 10000
        result = rule.check(ctx)
        assert result.passed

    def test_max_order_amount_reject(self):
        """单笔金额超限 → 拒绝"""
        rule = MaxOrderAmountRule({"max_amount": 10000})
        ctx = make_ctx(make_order(quantity=Decimal("1.0"), price=Decimal("50000")))
        # notional = 50000 > 10000
        result = rule.check(ctx)
        assert not result.passed
        assert result.level == "CRITICAL"

    def test_max_position_per_symbol_pass(self):
        """持仓在限制内"""
        rule = MaxPositionPerSymbolRule({"max_quantity": 10})
        ctx = make_ctx(make_order(quantity=Decimal("1")))
        result = rule.check(ctx)
        assert result.passed

    def test_max_position_per_symbol_reject(self):
        """持仓超限"""
        rule = MaxPositionPerSymbolRule({"max_quantity": 5})
        pos = Position(
            symbol="BTCUSDT", exchange="binance", side="LONG",
            quantity=Decimal("4.5"), avg_entry_price=Decimal("50000"),
        )
        ctx = make_ctx(make_order(quantity=Decimal("1")), positions={"BTCUSDT": pos})
        result = rule.check(ctx)
        assert not result.passed

    def test_max_total_position(self):
        """总持仓超限"""
        rule = MaxTotalPositionRule({"max_total_notional": 100000})
        pos = Position(
            symbol="ETHUSDT", exchange="binance", side="LONG",
            quantity=Decimal("30"), avg_entry_price=Decimal("3000"),
        )
        # existing = 90000, new = 0.5 * 50000 = 25000, total = 115000 > 100000
        ctx = make_ctx(
            make_order(quantity=Decimal("0.5"), price=Decimal("50000")),
            positions={"ETHUSDT": pos},
        )
        result = rule.check(ctx)
        assert not result.passed

    def test_max_daily_loss(self):
        """单日亏损超限"""
        rule = MaxDailyLossRule({"max_loss": 5000})
        ctx = make_ctx(daily_pnl=Decimal("-6000"))
        result = rule.check(ctx)
        assert not result.passed

    def test_max_daily_loss_pass(self):
        """单日亏损在限制内"""
        rule = MaxDailyLossRule({"max_loss": 5000})
        ctx = make_ctx(daily_pnl=Decimal("-3000"))
        result = rule.check(ctx)
        assert result.passed

    def test_max_strategy_loss(self):
        """单策略亏损超限"""
        rule = MaxStrategyLossRule({"max_loss": 2000})
        ctx = make_ctx(
            make_order(strategy_id="s1"),
            strategy_pnl={"s1": Decimal("-2500")},
        )
        result = rule.check(ctx)
        assert not result.passed

    def test_order_frequency(self):
        """下单频率超限"""
        rule = OrderFrequencyRule({"max_orders": 3, "window_seconds": 60})
        now = int(time.time() * 1000)

        for i in range(3):
            ctx = make_ctx(timestamp=now + i * 1000)
            result = rule.check(ctx)
            assert result.passed

        # 第 4 次超限
        ctx = make_ctx(timestamp=now + 3000)
        result = rule.check(ctx)
        assert not result.passed

    def test_price_deviation(self):
        """价格偏离超限"""
        rule = PriceDeviationRule({"max_deviation_pct": 5.0})
        ctx = make_ctx(
            make_order(price=Decimal("55000")),
            extra={"last_price": 50000.0},
        )
        # deviation = |55000-50000|/50000 * 100 = 10% > 5%
        result = rule.check(ctx)
        assert not result.passed

    def test_price_deviation_pass(self):
        """价格偏离在限制内"""
        rule = PriceDeviationRule({"max_deviation_pct": 5.0})
        ctx = make_ctx(
            make_order(price=Decimal("51000")),
            extra={"last_price": 50000.0},
        )
        result = rule.check(ctx)
        assert result.passed

    def test_min_order_quantity(self):
        """下单量低于最小值"""
        rule = MinOrderQuantityRule({"min_quantity": 0.01})
        ctx = make_ctx(make_order(quantity=Decimal("0.001")))
        result = rule.check(ctx)
        assert not result.passed

    def test_available_balance(self):
        """可用资金不足"""
        rule = AvailableBalanceRule()
        account = Account(
            exchange="binance", account_type="SPOT",
            total_balance=Decimal("1000"),
            available_balance=Decimal("500"),
        )
        ctx = make_ctx(
            make_order(quantity=Decimal("0.1"), price=Decimal("50000")),
            account=account,
        )
        # cost = 5000 > 500
        result = rule.check(ctx)
        assert not result.passed

    def test_consecutive_loss(self):
        """连续亏损超限"""
        rule = ConsecutiveLossRule({"max_consecutive_losses": 3})
        ctx = make_ctx(
            make_order(strategy_id="s1"),
            strategy_consecutive_losses={"s1": 5},
        )
        result = rule.check(ctx)
        assert not result.passed

    def test_night_trading(self):
        """夜间交易限制"""
        rule = NightTradingRule({"enabled": True, "start_hour": 22, "end_hour": 6})
        # 23:00 UTC → 在限制内
        ts_23 = 23 * 3600000  # 简化时间戳
        ctx = make_ctx(timestamp=ts_23)
        result = rule.check(ctx)
        assert not result.passed

    def test_night_trading_disabled(self):
        """夜间交易限制关闭"""
        rule = NightTradingRule({"enabled": False})
        ctx = make_ctx()
        result = rule.check(ctx)
        assert result.passed

    def test_new_symbol_whitelist(self):
        """新品种白名单"""
        rule = NewSymbolRule({"whitelist": ["BTCUSDT", "ETHUSDT"]})
        ctx = make_ctx(make_order(symbol="DOGEUSDT"))
        result = rule.check(ctx)
        assert not result.passed

    def test_new_symbol_in_whitelist(self):
        """白名单内品种通过"""
        rule = NewSymbolRule({"whitelist": ["BTCUSDT", "ETHUSDT"]})
        ctx = make_ctx(make_order(symbol="BTCUSDT"))
        result = rule.check(ctx)
        assert result.passed


# ─── RISK-T-002: fail-closed ───

class TestFailClosed:
    def test_engine_not_running_rejects(self):
        """引擎未运行时拒绝所有订单"""
        engine = RiskEngine(fail_closed=True)
        # 不 start()
        ctx = make_ctx()
        result = engine.check_order(ctx)
        assert not result.passed
        assert "not running" in result.reason

    def test_rule_exception_rejects(self):
        """规则异常时拒绝（fail-closed）"""
        class BrokenRule(MaxOrderAmountRule):
            def check(self, ctx):
                raise RuntimeError("boom")

        engine = RiskEngine(rules=[BrokenRule()], fail_closed=True)
        engine.start()
        ctx = make_ctx()
        result = engine.check_order(ctx)
        assert not result.passed
        assert "exception" in result.reason

    def test_fail_open_allows(self):
        """fail-open 模式下规则异常放行"""
        class BrokenRule(MaxOrderAmountRule):
            def check(self, ctx):
                raise RuntimeError("boom")

        engine = RiskEngine(rules=[BrokenRule()], fail_closed=False)
        engine.start()
        ctx = make_ctx()
        result = engine.check_order(ctx)
        assert result.passed


# ─── RISK-T-003: 性能 < 1ms ───

class TestPerformance:
    def test_check_latency_under_1ms(self):
        """风控检查延迟 < 1ms"""
        engine = RiskEngine()
        engine.start()
        ctx = make_ctx()

        # 预热
        for _ in range(10):
            engine.check_order(ctx)

        # 测量
        start = time.perf_counter()
        iterations = 1000
        for _ in range(iterations):
            engine.check_order(ctx)
        elapsed_ms = (time.perf_counter() - start) * 1000

        avg_ms = elapsed_ms / iterations
        assert avg_ms < 1.0, f"Avg check time {avg_ms:.4f}ms exceeds 1ms"


# ─── RISK-T-004: 风控日志 ───

class TestRiskLogging:
    def test_log_callback_on_reject(self):
        """拒绝时触发日志回调"""
        logs = []
        engine = RiskEngine(rules=[MaxOrderAmountRule({"max_amount": 100})])
        engine.add_log_callback(lambda entry: logs.append(entry))
        engine.start()

        ctx = make_ctx(make_order(quantity=Decimal("1"), price=Decimal("50000")))
        engine.check_order(ctx)

        assert len(logs) == 1
        assert logs[0]["rule_name"] == "max_order_amount"
        assert logs[0]["level"] == "CRITICAL"

    def test_no_log_on_pass(self):
        """通过时不触发日志"""
        logs = []
        engine = RiskEngine(rules=[MaxOrderAmountRule({"max_amount": 100000})])
        engine.add_log_callback(lambda entry: logs.append(entry))
        engine.start()

        ctx = make_ctx()
        engine.check_order(ctx)
        assert len(logs) == 0


class TestHotReload:
    def test_update_rule_params(self):
        """热更新规则参数"""
        engine = RiskEngine(rules=[MaxOrderAmountRule({"max_amount": 100})])
        engine.start()

        # 初始拒绝
        ctx = make_ctx(make_order(quantity=Decimal("0.01"), price=Decimal("50000")))
        assert not engine.check_order(ctx).passed

        # 热更新放大限制
        engine.update_rule_params("max_order_amount", {"max_amount": 1000000})
        assert engine.check_order(ctx).passed

    def test_enable_disable_rule(self):
        """启用/禁用规则"""
        engine = RiskEngine(rules=[MaxOrderAmountRule({"max_amount": 100})])
        engine.start()

        ctx = make_ctx(make_order(quantity=Decimal("0.01"), price=Decimal("50000")))
        assert not engine.check_order(ctx).passed

        engine.enable_rule("max_order_amount", False)
        assert engine.check_order(ctx).passed

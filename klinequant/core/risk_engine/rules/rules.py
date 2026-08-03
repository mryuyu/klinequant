"""12 条风控规则实现

RISK-001: 单笔最大金额
RISK-002: 单品种最大持仓
RISK-003: 总持仓上限
RISK-004: 单日最大亏损
RISK-005: 单策略最大亏损
RISK-006: 下单频率限制
RISK-007: 价格偏离保护
RISK-008: 最小下单量
RISK-009: 可用资金检查
RISK-010: 连续亏损限制
RISK-011: 夜间交易限制
RISK-012: 新品种限制
"""
from __future__ import annotations

import time
from collections import defaultdict, deque
from decimal import Decimal
from typing import Any, Deque, Dict, Optional

from core.risk_engine.rules.base import RiskCheckResult, RiskContext, RiskRule


class MaxOrderAmountRule(RiskRule):
    """RISK-001: 单笔最大金额限制"""

    @property
    def name(self) -> str:
        return "max_order_amount"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        max_amount = Decimal(str(self._params.get("max_amount", 10000)))
        price = ctx.order.price
        if price is None or price == 0:
            # 市价单：使用上下文中的最新价格
            last = ctx.extra.get("last_price")
            price = Decimal(str(last)) if last else Decimal("0")
        notional = ctx.order.quantity * price

        if notional > max_amount:
            return RiskCheckResult.reject(
                self.name,
                f"Order notional {notional} exceeds max {max_amount}",
                "CRITICAL",
            )
        return RiskCheckResult.ok(self.name)


class MaxPositionPerSymbolRule(RiskRule):
    """RISK-002: 单品种最大持仓"""

    @property
    def name(self) -> str:
        return "max_position_per_symbol"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        max_qty = Decimal(str(self._params.get("max_quantity", 100)))
        symbol = ctx.order.symbol
        pos = ctx.positions.get(symbol)
        current_qty = pos.quantity if pos else Decimal("0")

        # 买入增加持仓
        if ctx.order.side.value == "BUY":
            new_qty = current_qty + ctx.order.quantity
        else:
            new_qty = current_qty - ctx.order.quantity

        if abs(new_qty) > max_qty:
            return RiskCheckResult.reject(
                self.name,
                f"Position for {symbol} would be {new_qty}, exceeds max {max_qty}",
            )
        return RiskCheckResult.ok(self.name)


class MaxTotalPositionRule(RiskRule):
    """RISK-003: 总持仓上限（按名义价值）"""

    @property
    def name(self) -> str:
        return "max_total_position"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        max_total = Decimal(str(self._params.get("max_total_notional", 100000)))

        total = Decimal("0")
        for pos in ctx.positions.values():
            total += pos.quantity * pos.avg_entry_price

        # 加上新订单
        price = ctx.order.price or Decimal("0")
        total += ctx.order.quantity * price

        if total > max_total:
            return RiskCheckResult.reject(
                self.name,
                f"Total position notional {total} exceeds max {max_total}",
            )
        return RiskCheckResult.ok(self.name)


class MaxDailyLossRule(RiskRule):
    """RISK-004: 单日最大亏损"""

    @property
    def name(self) -> str:
        return "max_daily_loss"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        max_loss = Decimal(str(self._params.get("max_loss", 5000)))

        if ctx.daily_pnl < -max_loss:
            return RiskCheckResult.reject(
                self.name,
                f"Daily PnL {ctx.daily_pnl} exceeds max loss {-max_loss}",
                "CRITICAL",
            )
        return RiskCheckResult.ok(self.name)


class MaxStrategyLossRule(RiskRule):
    """RISK-005: 单策略最大亏损"""

    @property
    def name(self) -> str:
        return "max_strategy_loss"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        max_loss = Decimal(str(self._params.get("max_loss", 2000)))
        strategy_id = ctx.order.strategy_id
        pnl = ctx.strategy_pnl.get(strategy_id, Decimal("0"))

        if pnl < -max_loss:
            return RiskCheckResult.reject(
                self.name,
                f"Strategy {strategy_id} PnL {pnl} exceeds max loss {-max_loss}",
            )
        return RiskCheckResult.ok(self.name)


class OrderFrequencyRule(RiskRule):
    """RISK-006: 下单频率限制"""

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        super().__init__(params)
        self._order_timestamps: Deque[int] = deque()

    @property
    def name(self) -> str:
        return "order_frequency"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        max_orders = self._params.get("max_orders", 10)
        window_seconds = self._params.get("window_seconds", 60)
        now_ms = ctx.timestamp or int(time.time() * 1000)
        window_ms = window_seconds * 1000

        # 清除过期记录
        while self._order_timestamps and (now_ms - self._order_timestamps[0]) > window_ms:
            self._order_timestamps.popleft()

        if len(self._order_timestamps) >= max_orders:
            return RiskCheckResult.reject(
                self.name,
                f"Order frequency {len(self._order_timestamps)}/{window_seconds}s "
                f"exceeds max {max_orders}",
            )

        # 记录本次
        self._order_timestamps.append(now_ms)
        return RiskCheckResult.ok(self.name)


class PriceDeviationRule(RiskRule):
    """RISK-007: 价格偏离保护"""

    @property
    def name(self) -> str:
        return "price_deviation"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        max_deviation = Decimal(str(self._params.get("max_deviation_pct", 5.0)))
        order_price = ctx.order.price

        if order_price is None:
            return RiskCheckResult.ok(self.name)  # 市价单跳过

        # 从上下文获取参考价（最新价格）
        ref_price = ctx.extra.get("last_price")
        if ref_price is None:
            return RiskCheckResult.ok(self.name)

        ref_price = Decimal(str(ref_price))
        if ref_price == 0:
            return RiskCheckResult.ok(self.name)

        deviation_pct = abs(order_price - ref_price) / ref_price * 100

        if deviation_pct > max_deviation:
            return RiskCheckResult.reject(
                self.name,
                f"Price deviation {deviation_pct:.2f}% exceeds max {max_deviation}%",
            )
        return RiskCheckResult.ok(self.name)


class MinOrderQuantityRule(RiskRule):
    """RISK-008: 最小下单量"""

    @property
    def name(self) -> str:
        return "min_order_quantity"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        min_qty = Decimal(str(self._params.get("min_quantity", 0.001)))

        if ctx.order.quantity < min_qty:
            return RiskCheckResult.reject(
                self.name,
                f"Order quantity {ctx.order.quantity} below min {min_qty}",
            )
        return RiskCheckResult.ok(self.name)


class AvailableBalanceRule(RiskRule):
    """RISK-009: 可用资金检查"""

    @property
    def name(self) -> str:
        return "available_balance"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        if ctx.account is None:
            return RiskCheckResult.ok(self.name)

        # 市价单 price=None 时使用 last_price 估算
        price = ctx.order.price
        if price is None or price == Decimal("0"):
            last_price = ctx.extra.get("last_price", 0) if ctx.extra else 0
            price = Decimal(str(last_price)) if last_price else Decimal("0")

        order_cost = ctx.order.quantity * price
        available = ctx.account.available_balance

        if order_cost > available:
            return RiskCheckResult.reject(
                self.name,
                f"Order cost {order_cost} exceeds available balance {available}",
                "CRITICAL",
            )
        return RiskCheckResult.ok(self.name)


class ConsecutiveLossRule(RiskRule):
    """RISK-010: 连续亏损限制"""

    @property
    def name(self) -> str:
        return "consecutive_loss"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        max_losses = self._params.get("max_consecutive_losses", 5)
        strategy_id = ctx.order.strategy_id
        losses = ctx.strategy_consecutive_losses.get(strategy_id, 0)

        if losses >= max_losses:
            return RiskCheckResult.reject(
                self.name,
                f"Strategy {strategy_id} has {losses} consecutive losses "
                f"(max {max_losses})",
            )
        return RiskCheckResult.ok(self.name)


class NightTradingRule(RiskRule):
    """RISK-011: 夜间交易限制"""

    @property
    def name(self) -> str:
        return "night_trading"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        if not self._params.get("enabled", False):
            return RiskCheckResult.ok(self.name)

        start_hour = self._params.get("start_hour", 22)
        end_hour = self._params.get("end_hour", 6)

        # 使用 UTC 时间
        ts_ms = ctx.timestamp or int(time.time() * 1000)
        hour = (ts_ms // 3600000) % 24

        in_night = (hour >= start_hour or hour < end_hour) if start_hour > end_hour else (start_hour <= hour < end_hour)

        if in_night:
            return RiskCheckResult.reject(
                self.name,
                f"Night trading restricted ({start_hour}:00 - {end_hour}:00 UTC)",
            )
        return RiskCheckResult.ok(self.name)


class NewSymbolRule(RiskRule):
    """RISK-012: 新品种限制（白名单）"""

    @property
    def name(self) -> str:
        return "new_symbol"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        whitelist = self._params.get("whitelist")
        if whitelist is None:
            return RiskCheckResult.ok(self.name)  # 无白名单不限制

        if ctx.order.symbol not in whitelist:
            return RiskCheckResult.reject(
                self.name,
                f"Symbol {ctx.order.symbol} not in whitelist",
            )
        return RiskCheckResult.ok(self.name)


# ─── 工厂函数 ───

def create_default_rules(params: Optional[Dict[str, Any]] = None) -> list:
    """创建默认 12 条风控规则"""
    p = params or {}
    return [
        MaxOrderAmountRule(p.get("max_order_amount")),
        MaxPositionPerSymbolRule(p.get("max_position_per_symbol")),
        MaxTotalPositionRule(p.get("max_total_position")),
        MaxDailyLossRule(p.get("max_daily_loss")),
        MaxStrategyLossRule(p.get("max_strategy_loss")),
        OrderFrequencyRule(p.get("order_frequency")),
        PriceDeviationRule(p.get("price_deviation")),
        MinOrderQuantityRule(p.get("min_order_quantity")),
        AvailableBalanceRule(p.get("available_balance")),
        ConsecutiveLossRule(p.get("consecutive_loss")),
        NightTradingRule(p.get("night_trading")),
        NewSymbolRule(p.get("new_symbol")),
    ]

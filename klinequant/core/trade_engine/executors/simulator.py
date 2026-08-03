"""Simulator — 模拟执行器（Paper Trading）

在本地模拟订单撮合：
    - 市价单：立即以当前价格全额成交
    - 限价单：记录挂单，当价格触及时成交
    - 维护虚拟账户余额和持仓

遵循需求文档 §4.4 TRD-012, TRD-013。
"""
from __future__ import annotations

import logging
import time
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Optional

from core.trade_engine.executors.base import Executor
from protocol.types import (
    Account,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
)

logger = logging.getLogger(__name__)


class Simulator(Executor):
    """模拟执行器

    本地模拟撮合，不连接真实交易所。
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        initial_balance: Decimal = Decimal("100000"),
        fee_rate: Decimal = Decimal("0.001"),
    ):
        super().__init__(name="simulator", config=config)
        self._balance = initial_balance
        self._initial_balance = initial_balance
        self._fee_rate = fee_rate

        # 持仓: symbol -> Position
        self._positions: Dict[str, Position] = {}

        # 挂单: order_id -> Order
        self._pending_orders: Dict[str, Order] = {}

        # 最新价格: symbol -> Decimal
        self._last_prices: Dict[str, Decimal] = {}

        # 成交记录
        self._fills: List[Dict[str, Any]] = []

    @property
    def balance(self) -> Decimal:
        return self._balance

    @property
    def positions(self) -> Dict[str, Position]:
        return dict(self._positions)

    def update_price(self, symbol: str, price: Decimal) -> None:
        """更新最新价格（由外部行情驱动）"""
        self._last_prices[symbol] = price
        # 检查限价单是否可以成交
        self._check_pending_orders(symbol, price)

    async def submit_order(self, order: Order) -> Order:
        """提交订单"""
        order.exchange_order_id = f"SIM-{uuid.uuid4().hex[:12]}"
        order.status = OrderStatus.SUBMITTED
        order.created_at = int(time.time() * 1000)
        order.updated_at = order.created_at

        if order.order_type == OrderType.MARKET:
            # 市价单立即成交
            price = self._last_prices.get(order.symbol)
            if price is None:
                order.status = OrderStatus.REJECTED
                order.cancel_reason = "No market price available"
                return order
            self._fill_order(order, price, order.quantity)
        else:
            # 限价单加入挂单队列
            self._pending_orders[order.order_id] = order
            # 检查是否可以立即成交
            price = self._last_prices.get(order.symbol)
            if price and self._can_fill_limit(order, price):
                self._pending_orders.pop(order.order_id, None)
                self._fill_order(order, order.price, order.quantity)

        return order

    async def cancel_order(self, order: Order) -> Order:
        """撤销订单"""
        if order.order_id in self._pending_orders:
            self._pending_orders.pop(order.order_id)
            if order.can_transition_to(OrderStatus.CANCELING):
                order.transition_to(OrderStatus.CANCELING)
            if order.can_transition_to(OrderStatus.CANCELED):
                order.transition_to(OrderStatus.CANCELED)
            else:
                order.status = OrderStatus.CANCELED
            order.updated_at = int(time.time() * 1000)
            order.cancel_reason = "User canceled"
        else:
            # 已成交或不存在
            if order.status not in (OrderStatus.FILLED, OrderStatus.CANCELED):
                order.status = OrderStatus.CANCELED
        return order

    async def query_order(self, order: Order) -> Order:
        """查询订单状态"""
        if order.order_id in self._pending_orders:
            return self._pending_orders[order.order_id]
        return order

    async def query_positions(self, symbols: Optional[List[str]] = None) -> Dict[str, Position]:
        """查询持仓"""
        if symbols:
            return {s: p for s, p in self._positions.items() if s in symbols}
        return dict(self._positions)

    async def query_account(self) -> Account:
        """查询账户"""
        unrealized = Decimal("0")
        for symbol, pos in self._positions.items():
            last = self._last_prices.get(symbol, pos.avg_entry_price)
            if pos.side == "LONG":
                unrealized += (last - pos.avg_entry_price) * pos.quantity
            elif pos.side == "SHORT":
                unrealized += (pos.avg_entry_price - last) * pos.quantity

        return Account(
            exchange="simulator",
            account_type="SPOT",
            total_balance=self._balance + unrealized,
            available_balance=self._balance,
            unrealized_pnl=unrealized,
            positions=list(self._positions.values()),
            updated_at=int(time.time() * 1000),
        )

    # ─── 内部方法 ───

    def _fill_order(self, order: Order, price: Decimal, quantity: Decimal) -> None:
        """模拟成交"""
        fee = price * quantity * self._fee_rate

        # 更新余额
        if order.side == OrderSide.BUY:
            self._balance -= (price * quantity + fee)
        else:
            self._balance += (price * quantity - fee)

        # 更新持仓
        self._update_position(order.symbol, order.side, price, quantity)

        # 更新订单状态
        order.filled_quantity = quantity
        order.avg_fill_price = price
        order.fee = fee
        order.fee_currency = "USDT"
        order.status = OrderStatus.FILLED
        order.filled_at = int(time.time() * 1000)
        order.updated_at = order.filled_at

        # 记录成交
        self._fills.append({
            "fill_id": str(uuid.uuid4()),
            "order_id": order.order_id,
            "symbol": order.symbol,
            "side": order.side.value,
            "price": float(price),
            "quantity": float(quantity),
            "fee": float(fee),
            "timestamp": order.filled_at,
        })

    def _update_position(
        self, symbol: str, side: OrderSide, price: Decimal, quantity: Decimal
    ) -> None:
        """更新持仓"""
        pos = self._positions.get(symbol)

        if pos is None or pos.quantity == 0:
            # 新建持仓
            self._positions[symbol] = Position(
                symbol=symbol,
                exchange="simulator",
                side="LONG" if side == OrderSide.BUY else "SHORT",
                quantity=quantity,
                avg_entry_price=price,
                updated_at=int(time.time() * 1000),
            )
        else:
            if side == OrderSide.BUY:
                # 加仓或平空
                if pos.side == "SHORT":
                    # 平空
                    pos.realized_pnl += (pos.avg_entry_price - price) * min(quantity, pos.quantity)
                    pos.quantity -= quantity
                    if pos.quantity <= 0:
                        pos.quantity = abs(pos.quantity)
                        pos.side = "LONG"
                        pos.avg_entry_price = price
                else:
                    # 加多仓
                    total_cost = pos.avg_entry_price * pos.quantity + price * quantity
                    pos.quantity += quantity
                    pos.avg_entry_price = total_cost / pos.quantity
            else:
                # 减仓或平多
                if pos.side == "LONG":
                    pos.realized_pnl += (price - pos.avg_entry_price) * min(quantity, pos.quantity)
                    pos.quantity -= quantity
                    if pos.quantity <= 0:
                        pos.quantity = abs(pos.quantity)
                        pos.side = "SHORT"
                        pos.avg_entry_price = price
                else:
                    total_cost = pos.avg_entry_price * pos.quantity + price * quantity
                    pos.quantity += quantity
                    pos.avg_entry_price = total_cost / pos.quantity

            pos.updated_at = int(time.time() * 1000)

            # 清除零持仓
            if pos.quantity == 0:
                self._positions.pop(symbol, None)

    def _can_fill_limit(self, order: Order, market_price: Decimal) -> bool:
        """检查限价单是否可以成交"""
        if order.price is None:
            return False
        if order.side == OrderSide.BUY:
            return market_price <= order.price
        else:
            return market_price >= order.price

    def _check_pending_orders(self, symbol: str, price: Decimal) -> None:
        """检查挂单是否可以成交"""
        to_fill = []
        for oid, order in self._pending_orders.items():
            if order.symbol == symbol and self._can_fill_limit(order, price):
                to_fill.append(oid)

        for oid in to_fill:
            order = self._pending_orders.pop(oid)
            self._fill_order(order, order.price, order.quantity)
            logger.info(f"Simulator: limit order {oid} filled at {order.price}")

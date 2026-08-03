"""OrderManager — 订单生命周期管理

功能：
    - 创建订单（生成 UUID + client_order_id）
    - 提交订单（调用 Executor）
    - 状态流转管理
    - 订单查询/过滤
    - 持久化到 DuckDB

遵循需求文档 §4.4 TRD-004~TRD-005, TRD-008~TRD-010。
"""
from __future__ import annotations

import logging
import time
import uuid
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

from core.trade_engine.executors.base import Executor
from protocol.types import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
)

logger = logging.getLogger(__name__)


class OrderManager:
    """订单生命周期管理器"""

    def __init__(self, executor: Executor):
        self._executor = executor
        # 活跃订单: order_id -> Order
        self._orders: Dict[str, Order] = {}
        # 状态变更回调
        self._on_status_change: List[Callable[[Order, OrderStatus], None]] = []
        # 持久化回调
        self._on_persist: Optional[Callable[[Order], Any]] = None

    @property
    def executor(self) -> Executor:
        return self._executor

    @property
    def active_orders(self) -> List[Order]:
        """获取所有活跃订单（非终态）"""
        terminal = {
            OrderStatus.FILLED, OrderStatus.CANCELED,
            OrderStatus.REJECTED, OrderStatus.EXPIRED, OrderStatus.FAILED,
        }
        return [o for o in self._orders.values() if o.status not in terminal]

    @property
    def all_orders(self) -> List[Order]:
        return list(self._orders.values())

    def on_status_change(self, callback: Callable[[Order, OrderStatus], None]) -> None:
        """注册状态变更回调"""
        self._on_status_change.append(callback)

    def set_persist_callback(self, callback: Callable[[Order], Any]) -> None:
        """设置持久化回调"""
        self._on_persist = callback

    def create_order(
        self,
        symbol: str,
        exchange: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        strategy_id: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Order:
        """创建订单（不提交）"""
        now = int(time.time() * 1000)
        order = Order(
            order_id=str(uuid.uuid4()),
            symbol=symbol,
            exchange=exchange,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            strategy_id=strategy_id,
            client_order_id=f"KQ-{uuid.uuid4().hex[:16]}",
            created_at=now,
            updated_at=now,
            metadata=metadata or {},
        )
        self._orders[order.order_id] = order
        return order

    async def submit_order(self, order: Order) -> Order:
        """提交订单到执行器"""
        old_status = order.status
        try:
            result = await self._executor.submit_order(order)
            # 更新本地引用
            self._orders[result.order_id] = result
            if result.status != old_status:
                self._notify_status_change(result, old_status)
            return result
        except Exception as e:
            logger.error(f"Submit order failed: {e}")
            order.status = OrderStatus.FAILED
            order.cancel_reason = str(e)
            order.updated_at = int(time.time() * 1000)
            self._notify_status_change(order, old_status)
            return order

    async def cancel_order(self, order: Order) -> Order:
        """撤销订单"""
        old_status = order.status
        try:
            if order.can_transition_to(OrderStatus.CANCELING):
                order.transition_to(OrderStatus.CANCELING)
            result = await self._executor.cancel_order(order)
            self._orders[result.order_id] = result
            if result.status != old_status:
                self._notify_status_change(result, old_status)
            return result
        except Exception as e:
            logger.error(f"Cancel order failed: {e}")
            return order

    async def sync_order(self, order: Order) -> Order:
        """从交易所同步订单状态"""
        try:
            result = await self._executor.query_order(order)
            old_status = order.status
            self._orders[result.order_id] = result
            if result.status != old_status:
                self._notify_status_change(result, old_status)
            return result
        except Exception as e:
            logger.error(f"Sync order failed: {e}")
            return order

    def get_order(self, order_id: str) -> Optional[Order]:
        return self._orders.get(order_id)

    def get_orders_by_symbol(self, symbol: str) -> List[Order]:
        return [o for o in self._orders.values() if o.symbol == symbol]

    def get_orders_by_strategy(self, strategy_id: str) -> List[Order]:
        return [o for o in self._orders.values() if o.strategy_id == strategy_id]

    def _notify_status_change(self, order: Order, old_status: OrderStatus) -> None:
        """通知状态变更"""
        for cb in self._on_status_change:
            try:
                cb(order, old_status)
            except Exception as e:
                logger.error(f"Status change callback error: {e}")

        # 持久化
        if self._on_persist:
            try:
                self._on_persist(order)
            except Exception as e:
                logger.error(f"Persist callback error: {e}")

"""Matcher — 回测撮合模拟器

模拟交易所撮合逻辑：
    - 市价单：以下根 K 线开盘价成交（避免 look-ahead bias）
    - 限价单：当 K 线价格触及时成交
    - 止损单：当 K 线价格触发止损线时以市价成交
    - 支持滑点模型和手续费模型

遵循需求文档 §4.5 BT-001, BT-004（look-ahead bias 防护）。
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional

from core.backtest_engine.fee import FeeModel, PercentageFee
from core.backtest_engine.slippage import PercentageSlippage, SlippageModel
from protocol.types import Kline, Order, OrderSide, OrderStatus, OrderType

logger = logging.getLogger(__name__)


@dataclass
class BacktestFill:
    """回测成交记录"""

    fill_id: str
    order_id: str
    symbol: str
    side: str
    price: Decimal
    quantity: Decimal
    fee: Decimal
    slippage: Decimal
    timestamp: int
    is_maker: bool = False


@dataclass
class PendingOrder:
    """挂单（等待撮合）"""

    order: Order
    created_bar_index: int  # 创建时的 K 线索引
    stop_price: Optional[Decimal] = None  # 止损触发价


class Matcher:
    """回测撮合模拟器

    核心原则：
        - 信号在 K 线收盘时产生，订单在下根 K 线执行（防止 look-ahead bias）
        - 市价单：以下根 K 线 open 价 + 滑点成交
        - 限价单：当 K 线 low <= buy_price 或 high >= sell_price 时成交
        - 止损单：当价格触及止损线时转为市价单执行
    """

    def __init__(
        self,
        slippage_model: Optional[SlippageModel] = None,
        fee_model: Optional[FeeModel] = None,
    ):
        self._slippage = slippage_model or PercentageSlippage()
        self._fee = fee_model or PercentageFee()
        self._pending_orders: List[PendingOrder] = []
        self._fills: List[BacktestFill] = []
        self._bar_index: int = 0

    @property
    def fills(self) -> List[BacktestFill]:
        return list(self._fills)

    @property
    def pending_orders(self) -> List[PendingOrder]:
        return list(self._pending_orders)

    def submit_order(
        self,
        order: Order,
        current_bar_index: int,
        stop_price: Optional[Decimal] = None,
    ) -> Order:
        """提交订单到撮合引擎

        订单不会立即成交，而是等待下根 K 线到来时撮合。
        """
        order.status = OrderStatus.SUBMITTED
        order.exchange_order_id = f"BT-{uuid.uuid4().hex[:12]}"
        self._pending_orders.append(
            PendingOrder(
                order=order,
                created_bar_index=current_bar_index,
                stop_price=stop_price,
            )
        )
        return order

    def cancel_order(self, order_id: str) -> bool:
        """撤销挂单"""
        for i, po in enumerate(self._pending_orders):
            if po.order.order_id == order_id:
                po.order.status = OrderStatus.CANCELED
                self._pending_orders.pop(i)
                return True
        return False

    def on_bar(self, kline: Kline, bar_index: int) -> List[BacktestFill]:
        """新 K 线到来时撮合所有挂单

        只撮合 created_bar_index < bar_index 的订单（防止 look-ahead bias）。

        Args:
            kline: 当前 K 线
            bar_index: 当前 K 线索引

        Returns:
            本根 K 线产生的成交列表
        """
        self._bar_index = bar_index
        bar_fills: List[BacktestFill] = []

        remaining: List[PendingOrder] = []
        for po in self._pending_orders:
            # 防止 look-ahead bias：只处理之前创建的订单
            if po.created_bar_index >= bar_index:
                remaining.append(po)
                continue

            fill = self._try_match(po, kline)
            if fill:
                bar_fills.append(fill)
                self._fills.append(fill)
            else:
                remaining.append(po)

        self._pending_orders = remaining
        return bar_fills

    def _try_match(self, po: PendingOrder, kline: Kline) -> Optional[BacktestFill]:
        """尝试撮合单个挂单"""
        order = po.order
        side_str = "BUY" if order.side == OrderSide.BUY else "SELL"

        if order.order_type == OrderType.MARKET:
            # 市价单：以开盘价 + 滑点成交
            exec_price = self._slippage.calculate(
                kline.open, order.quantity, side_str, kline.volume
            )
            return self._create_fill(order, exec_price, kline, is_maker=False)

        elif order.order_type == OrderType.LIMIT:
            # 限价单：检查价格是否触及
            if order.price is None:
                return None
            if order.side == OrderSide.BUY:
                # 买入限价单：K 线最低价 <= 限价
                if kline.low <= order.price:
                    exec_price = min(order.price, kline.open)
                    exec_price = self._slippage.calculate(
                        exec_price, order.quantity, side_str, kline.volume
                    )
                    return self._create_fill(order, exec_price, kline, is_maker=True)
            else:
                # 卖出限价单：K 线最高价 >= 限价
                if kline.high >= order.price:
                    exec_price = max(order.price, kline.open)
                    exec_price = self._slippage.calculate(
                        exec_price, order.quantity, side_str, kline.volume
                    )
                    return self._create_fill(order, exec_price, kline, is_maker=True)

        elif order.order_type == OrderType.STOP_LIMIT:
            # 止损单：触发后以市价成交
            if po.stop_price is None:
                return None
            if order.side == OrderSide.SELL:
                # 卖出止损：价格跌破止损线
                if kline.low <= po.stop_price:
                    exec_price = self._slippage.calculate(
                        po.stop_price, order.quantity, side_str, kline.volume
                    )
                    return self._create_fill(order, exec_price, kline, is_maker=False)
            else:
                # 买入止损：价格突破止损线
                if kline.high >= po.stop_price:
                    exec_price = self._slippage.calculate(
                        po.stop_price, order.quantity, side_str, kline.volume
                    )
                    return self._create_fill(order, exec_price, kline, is_maker=False)

        return None

    def _create_fill(
        self,
        order: Order,
        exec_price: Decimal,
        kline: Kline,
        is_maker: bool,
    ) -> BacktestFill:
        """创建成交记录"""
        side_str = "BUY" if order.side == OrderSide.BUY else "SELL"
        fee = self._fee.calculate(exec_price, order.quantity, side_str, is_maker)

        # 计算滑点量
        base_price = kline.open if order.order_type == OrderType.MARKET else (order.price or kline.open)
        slippage_amount = abs(exec_price - base_price) * order.quantity

        fill = BacktestFill(
            fill_id=str(uuid.uuid4()),
            order_id=order.order_id,
            symbol=order.symbol,
            side=side_str,
            price=exec_price,
            quantity=order.quantity,
            fee=fee,
            slippage=slippage_amount,
            timestamp=kline.timestamp,
            is_maker=is_maker,
        )

        # 更新订单状态
        order.status = OrderStatus.FILLED
        order.filled_quantity = order.quantity
        order.avg_fill_price = exec_price
        order.fee = fee
        order.fee_currency = "USDT"
        order.filled_at = kline.timestamp
        order.updated_at = kline.timestamp

        return fill

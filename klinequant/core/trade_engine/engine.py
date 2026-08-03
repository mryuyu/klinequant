"""TradeEngine — 交易引擎主循环

整合所有组件：
    - Executor（订单执行：live/paper/backtest）
    - OrderManager（订单生命周期）
    - PositionManager（持仓管理）
    - RiskEngine（风控检查）
    - Signal 接收 → 风控 → 下单

遵循需求文档 §4.4 TRD-007, TRD-012~TRD-014。
"""
from __future__ import annotations

import logging
import time
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from core.risk_engine.engine import RiskEngine
from core.risk_engine.rules.base import RiskContext
from core.trade_engine.executors.base import Executor
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
)

logger = logging.getLogger(__name__)


class TradeMode(str, Enum):
    """交易模式"""
    LIVE = "LIVE"        # 实盘
    PAPER = "PAPER"      # 模拟盘
    BACKTEST = "BACKTEST"  # 回测


class TradeEngine:
    """交易引擎

    信号 → 风控 → 下单 → 持仓管理
    """

    def __init__(
        self,
        executor: Executor,
        risk_engine: Optional[RiskEngine] = None,
        mode: TradeMode = TradeMode.PAPER,
    ):
        self._executor = executor
        self._risk_engine = risk_engine or RiskEngine()
        self._mode = mode

        self._order_manager = OrderManager(executor)
        self._position_manager = PositionManager()

        # 账户缓存
        self._account: Optional[Account] = None

        # 信号处理回调
        self._on_order_filled: List[Callable[[Order], None]] = []

        self._running = False

    @property
    def mode(self) -> TradeMode:
        return self._mode

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def order_manager(self) -> OrderManager:
        return self._order_manager

    @property
    def position_manager(self) -> PositionManager:
        return self._position_manager

    @property
    def risk_engine(self) -> RiskEngine:
        return self._risk_engine

    @property
    def account(self) -> Optional[Account]:
        return self._account

    def on_order_filled(self, callback: Callable[[Order], None]) -> None:
        """注册订单成交回调"""
        self._on_order_filled.append(callback)

    async def process_signal(self, signal: Signal) -> Optional[Order]:
        """处理交易信号

        流程：信号 → 风控检查 → 创建订单 → 提交执行

        Args:
            signal: 交易信号

        Returns:
            提交的订单（风控拒绝时返回 None）
        """
        if not self._running:
            logger.warning("TradeEngine not running, ignoring signal")
            return None

        # 信号过期检查
        now_ms = int(time.time() * 1000)
        if signal.is_expired(now_ms):
            logger.info(f"Signal expired: {signal.signal_id}")
            return None

        # 确定订单方向和数量
        side = self._signal_to_side(signal)
        if side is None:
            return None

        quantity = signal.suggested_quantity or Decimal("0.001")
        price = signal.price

        # 创建订单
        order = self._order_manager.create_order(
            symbol=signal.symbol,
            exchange="binance",
            side=side,
            order_type=OrderType.MARKET,
            quantity=quantity,
            strategy_id=signal.strategy_id,
            metadata={"signal_id": signal.signal_id, "reason": signal.reason},
        )

        # 风控检查
        ctx = RiskContext(
            order=order,
            account=self._account,
            positions=self._position_manager.positions,
            open_orders=self._order_manager.active_orders,
            timestamp=now_ms,
            extra={"last_price": float(price)},
        )

        risk_result = self._risk_engine.check_order(ctx)
        if not risk_result.passed:
            logger.warning(
                f"Order rejected by risk: {risk_result.rule_name} - {risk_result.reason}"
            )
            order.status = OrderStatus.REJECTED
            order.cancel_reason = f"Risk: {risk_result.reason}"
            return None

        # 提交订单
        result = await self._order_manager.submit_order(order)

        # 如果成交，更新持仓
        if result.status == OrderStatus.FILLED:
            await self._sync_positions()
            for cb in self._on_order_filled:
                try:
                    cb(result)
                except Exception as e:
                    logger.error(f"Order filled callback error: {e}")

        return result

    async def submit_manual_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        strategy_id: str = "manual",
    ) -> Order:
        """手动下单（含风控检查）"""
        order = self._order_manager.create_order(
            symbol=symbol,
            exchange="binance",
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            strategy_id=strategy_id,
        )

        # 风控检查
        now_ms = int(time.time() * 1000)
        ctx = RiskContext(
            order=order,
            account=self._account,
            positions=self._position_manager.positions,
            open_orders=self._order_manager.active_orders,
            timestamp=now_ms,
            extra={"last_price": float(price) if price else 0},
        )

        risk_result = self._risk_engine.check_order(ctx)
        if not risk_result.passed:
            order.status = OrderStatus.REJECTED
            order.cancel_reason = f"Risk: {risk_result.reason}"
            return order

        result = await self._order_manager.submit_order(order)
        if result.status == OrderStatus.FILLED:
            await self._sync_positions()
        return result

    async def cancel_order(self, order_id: str) -> Optional[Order]:
        """撤单"""
        order = self._order_manager.get_order(order_id)
        if order is None:
            return None
        return await self._order_manager.cancel_order(order)

    async def sync_account(self) -> Account:
        """同步账户信息"""
        self._account = await self._executor.query_account()
        return self._account

    async def _sync_positions(self) -> None:
        """同步持仓"""
        positions = await self._executor.query_positions()
        self._position_manager.set_positions(positions)

    def _signal_to_side(self, signal: Signal) -> Optional[OrderSide]:
        """信号方向 → 订单方向"""
        if signal.direction == SignalDirection.LONG:
            return OrderSide.BUY
        elif signal.direction == SignalDirection.SHORT:
            return OrderSide.SELL
        elif signal.direction == SignalDirection.CLOSE:
            # 平仓：根据当前持仓方向决定
            pos = self._position_manager.get_position(signal.symbol)
            if pos:
                return OrderSide.SELL if pos.side == "LONG" else OrderSide.BUY
            return None
        return None

    async def start(self) -> None:
        """启动交易引擎"""
        self._running = True
        self._risk_engine.start()
        await self._executor.connect()
        await self.sync_account()
        await self._sync_positions()
        logger.info(f"TradeEngine started (mode={self._mode.value})")

    async def stop(self) -> None:
        """停止交易引擎"""
        self._running = False
        self._risk_engine.stop()
        await self._executor.disconnect()
        logger.info("TradeEngine stopped")

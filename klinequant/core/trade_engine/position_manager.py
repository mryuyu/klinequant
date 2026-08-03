"""PositionManager — 持仓管理

功能：
    - 维护各品种实时持仓
    - 计算未实现盈亏
    - 持仓快照写入 Redis
    - 持仓变化通知

遵循需求文档 §4.4 TRD-006, TRD-011。
"""
from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

from protocol.types import Position

logger = logging.getLogger(__name__)


class PositionManager:
    """持仓管理器"""

    def __init__(self):
        # symbol -> Position
        self._positions: Dict[str, Position] = {}
        # 最新价格: symbol -> Decimal
        self._last_prices: Dict[str, Decimal] = {}
        # 变化回调
        self._on_change: List[Callable[[Position], None]] = []

    @property
    def positions(self) -> Dict[str, Position]:
        return dict(self._positions)

    def get_position(self, symbol: str) -> Optional[Position]:
        return self._positions.get(symbol)

    def get_all_positions(self) -> List[Position]:
        return list(self._positions.values())

    def update_price(self, symbol: str, price: Decimal) -> None:
        """更新最新价格并重算未实现盈亏"""
        self._last_prices[symbol] = price
        pos = self._positions.get(symbol)
        if pos and pos.quantity > 0:
            if pos.side == "LONG":
                pos.unrealized_pnl = (price - pos.avg_entry_price) * pos.quantity
            elif pos.side == "SHORT":
                pos.unrealized_pnl = (pos.avg_entry_price - price) * pos.quantity

    def update_position(self, position: Position) -> None:
        """更新持仓（从 Executor 同步）"""
        old = self._positions.get(position.symbol)
        self._positions[position.symbol] = position

        # 重算未实现盈亏
        price = self._last_prices.get(position.symbol)
        if price:
            self.update_price(position.symbol, price)

        # 清除零持仓
        if position.quantity == 0:
            self._positions.pop(position.symbol, None)

        # 通知
        for cb in self._on_change:
            try:
                cb(position)
            except Exception as e:
                logger.error(f"Position change callback error: {e}")

    def set_positions(self, positions: Dict[str, Position]) -> None:
        """批量设置持仓（启动时同步）"""
        self._positions = dict(positions)

    def on_change(self, callback: Callable[[Position], None]) -> None:
        """注册持仓变化回调"""
        self._on_change.append(callback)

    def total_unrealized_pnl(self) -> Decimal:
        """总未实现盈亏"""
        return sum(
            (p.unrealized_pnl for p in self._positions.values()),
            Decimal("0"),
        )

    def total_realized_pnl(self) -> Decimal:
        """总已实现盈亏"""
        return sum(
            (p.realized_pnl for p in self._positions.values()),
            Decimal("0"),
        )

    def snapshot(self) -> Dict[str, Any]:
        """生成持仓快照（用于 Redis 缓存）"""
        return {
            symbol: {
                "side": pos.side,
                "quantity": float(pos.quantity),
                "avg_entry_price": float(pos.avg_entry_price),
                "unrealized_pnl": float(pos.unrealized_pnl),
                "realized_pnl": float(pos.realized_pnl),
                "updated_at": pos.updated_at,
            }
            for symbol, pos in self._positions.items()
        }

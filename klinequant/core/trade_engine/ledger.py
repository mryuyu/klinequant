"""ExposureLedger — 敞口账本

核心不变量：effective_position = filled_position + in_flight_exposure

按 (symbol, tag) 记账。一期单 tag，数据结构预留多周期/多策略扩展。
订单生命周期驱动账本：
  - accepted → in_flight += signed_qty
  - filled   → in_flight -= signed_qty, volume += signed_qty
  - dead     → in_flight -= signed_qty（释放）
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from protocol.types import Offset, OrderSide, Position

logger = logging.getLogger(__name__)


@dataclass
class TagPosition:
    """单个 (symbol, tag) 的持仓状态"""
    symbol: str
    tag: str
    volume: Decimal = Decimal("0")          # 已成交净持仓（signed）
    in_flight: Decimal = Decimal("0")       # 在途净敞口（signed）
    avg_entry_price: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    # 在途订单明细（防重 + 对账用）
    pending_orders: Dict[str, "_PendingEntry"] = field(default_factory=dict)

    @property
    def effective(self) -> Decimal:
        """有效持仓 = 已成交 + 在途"""
        return self.volume + self.in_flight

    @property
    def available_to_close(self) -> Decimal:
        """可平量（T+0 市场 = |volume|；T+1 需扣除今仓，一期简化为 |volume|）"""
        return abs(self.volume)


@dataclass
class _PendingEntry:
    """在途订单记录"""
    order_id: str
    side: OrderSide
    offset: Offset
    qty: Decimal            # 正数
    signed_qty: Decimal     # 带符号（对 in_flight 的贡献）


def _signed_qty(side: OrderSide, offset: Offset, qty: Decimal) -> Decimal:
    """计算订单对持仓的带符号贡献。

    BUY+OPEN  → +qty（开多）
    SELL+OPEN → -qty（开空）
    SELL+CLOSE → -qty（平多）
    BUY+CLOSE  → +qty（平空）
    """
    if side == OrderSide.BUY:
        return qty if offset == Offset.OPEN else qty
    else:
        return -qty if offset == Offset.OPEN else -qty


class ExposureLedger:
    """敞口账本（线程安全）"""

    def __init__(self):
        self._lock = threading.Lock()
        # (symbol, tag) → TagPosition
        self._positions: Dict[Tuple[str, str], TagPosition] = {}

    def _get_or_create(self, symbol: str, tag: str) -> TagPosition:
        key = (symbol, tag)
        if key not in self._positions:
            self._positions[key] = TagPosition(symbol=symbol, tag=tag)
        return self._positions[key]

    # ─── 账本动作 ───

    def on_order_accepted(
        self, symbol: str, tag: str, order_id: str,
        side: OrderSide, offset: Offset, qty: Decimal,
    ) -> None:
        """订单被受理（提交到 venue 前）：in_flight 增加"""
        sq = _signed_qty(side, offset, qty)
        with self._lock:
            tp = self._get_or_create(symbol, tag)
            tp.in_flight += sq
            tp.pending_orders[order_id] = _PendingEntry(
                order_id=order_id, side=side, offset=offset,
                qty=qty, signed_qty=sq,
            )

    def on_order_filled(
        self, symbol: str, tag: str, order_id: str,
        side: OrderSide, offset: Offset, qty: Decimal,
        fill_price: Decimal = Decimal("0"),
        fill_qty: Optional[Decimal] = None,
    ) -> None:
        """订单成交：in_flight 释放，volume 增加"""
        actual_qty = fill_qty if fill_qty is not None else qty
        sq = _signed_qty(side, offset, actual_qty)
        with self._lock:
            tp = self._get_or_create(symbol, tag)
            # 释放在途（按原始记录）
            entry = tp.pending_orders.pop(order_id, None)
            if entry:
                tp.in_flight -= entry.signed_qty
            else:
                # 对账恢复场景：无在途记录，直接减
                tp.in_flight -= sq
            # 更新已成交持仓
            old_vol = tp.volume
            tp.volume += sq
            # 更新均价（仅开仓时）
            if offset == Offset.OPEN and fill_price > 0:
                if old_vol == 0:
                    tp.avg_entry_price = fill_price
                elif (old_vol > 0 and sq > 0) or (old_vol < 0 and sq < 0):
                    # 同向加仓：加权平均
                    total = abs(old_vol) + abs(sq)
                    if total > 0:
                        tp.avg_entry_price = (
                            tp.avg_entry_price * abs(old_vol) + fill_price * abs(sq)
                        ) / total
            # 平仓实现盈亏
            if offset == Offset.CLOSE and fill_price > 0 and tp.avg_entry_price > 0:
                pnl = (fill_price - tp.avg_entry_price) * sq
                if old_vol > 0:  # 原来持多，sq < 0
                    pnl = (fill_price - tp.avg_entry_price) * abs(sq)
                else:  # 原来持空，sq > 0
                    pnl = (tp.avg_entry_price - fill_price) * abs(sq)
                tp.realized_pnl += pnl
            # 清零时重置均价
            if tp.volume == 0:
                tp.avg_entry_price = Decimal("0")

    def on_order_dead(
        self, symbol: str, tag: str, order_id: str,
        side: OrderSide, offset: Offset, qty: Decimal,
    ) -> None:
        """订单死亡（撤单/拒绝/过期）：释放在途敞口"""
        with self._lock:
            tp = self._get_or_create(symbol, tag)
            entry = tp.pending_orders.pop(order_id, None)
            if entry:
                tp.in_flight -= entry.signed_qty
            else:
                sq = _signed_qty(side, offset, qty)
                tp.in_flight -= sq

    # ─── 查询 ───

    def position(self, symbol: str, tag: str = "") -> Position:
        """合成 Position 视图（策略 SDK 消费）"""
        with self._lock:
            tp = self._positions.get((symbol, tag))
            if tp is None:
                return Position(symbol=symbol, tag=tag)
            return Position(
                symbol=symbol,
                tag=tag,
                volume=tp.volume,
                in_flight=tp.in_flight,
                effective=tp.effective,
                available_to_close=tp.available_to_close,
                avg_entry_price=tp.avg_entry_price,
                realized_pnl=tp.realized_pnl,
            )

    def net_position(self, symbol: str) -> Decimal:
        """交易所净持仓（所有 tag 之和）"""
        with self._lock:
            total = Decimal("0")
            for (sym, _tag), tp in self._positions.items():
                if sym == symbol:
                    total += tp.effective
            return total

    def has_in_flight_open(self, symbol: str, tag: str, side: OrderSide) -> bool:
        """防重：同 (symbol, tag, direction) 是否有在途 OPEN 订单"""
        with self._lock:
            tp = self._positions.get((symbol, tag))
            if tp is None:
                return False
            for entry in tp.pending_orders.values():
                if entry.offset == Offset.OPEN and entry.side == side:
                    return True
            return False

    def all_positions(self) -> List[Position]:
        """所有持仓快照"""
        with self._lock:
            return [
                Position(
                    symbol=tp.symbol, tag=tp.tag,
                    volume=tp.volume, in_flight=tp.in_flight,
                    effective=tp.effective,
                    available_to_close=tp.available_to_close,
                    avg_entry_price=tp.avg_entry_price,
                    realized_pnl=tp.realized_pnl,
                )
                for tp in self._positions.values()
                if tp.effective != 0 or tp.in_flight != 0
            ]

    def sync_from_venue(self, symbol: str, tag: str, venue_volume: Decimal,
                        venue_avg_price: Decimal) -> None:
        """对账：用 venue 实际持仓修正账本（重启恢复用）"""
        with self._lock:
            tp = self._get_or_create(symbol, tag)
            if tp.volume != venue_volume:
                logger.warning(
                    f"Ledger reconcile {symbol}/{tag}: "
                    f"local={tp.volume} venue={venue_volume}, correcting"
                )
                tp.volume = venue_volume
                tp.avg_entry_price = venue_avg_price

    def clear_in_flight(self, symbol: str, tag: str) -> None:
        """清空在途敞口与挂单明细（批量撤单后收尾用）。

        撤单是 venue 侧动作，账本不会自动释放在途；清仓/退出前调用本方法
        把 in_flight 归零，使 effective 回到纯已成交持仓，保证后续平仓校验准确。
        """
        with self._lock:
            tp = self._positions.get((symbol, tag))
            if tp:
                tp.in_flight = Decimal("0")
                tp.pending_orders.clear()

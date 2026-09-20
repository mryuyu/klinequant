"""UnifiedResolver — 订单意图解析器

主流程：验证 → 量化 → 构造 VenueOrderSpec
市场差异收敛到 ClosePolicy 策略模式（一期仅 NettingClosePolicy）。

设计原则：
  - Resolver 是纯函数（无副作用），不修改账本
  - 验证失败返回 Rejection，不抛异常
  - 量化对齐到 spec 的 step/tick
  - 输出 VenueOrderSpec 可直接交给 Executor 执行
"""
from __future__ import annotations

import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple

from protocol.types import (
    Offset,
    OrderKind,
    OrderSide,
    SymbolInfo,
    Tif,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# 数据结构
# ─────────────────────────────────────────────

@dataclass
class OrderRequest:
    """策略意图（SDK 层构造，Resolver 消费）"""
    symbol: str
    tag: str
    side: OrderSide
    offset: Offset
    qty: Decimal                  # 正数
    kind: OrderKind = OrderKind.MARKET
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None
    tif: Optional[Tif] = None     # None = 用市场默认
    client_order_id: str = ""


@dataclass
class VenueOrderSpec:
    """Resolver 输出：可直接交给 Executor 的 venue 原生订单规格"""
    symbol: str
    side: OrderSide
    offset: Offset
    qty: Decimal                  # 量化后，正数
    kind: OrderKind
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None
    tif: Tif = Tif.GTC
    client_order_id: str = ""
    tag: str = ""
    # venue 特有字段（预留）
    reduce_only: bool = False
    position_side: str = ""       # "BOTH" / "LONG" / "SHORT"
    close_ticket: int = 0         # FX Hedging 平仓 ticket
    deviation: int = 20           # MT5 滑点容忍（points）
    magic: int = 202609           # MT5 EA magic number


@dataclass
class Rejection:
    """验证失败原因"""
    code: str
    message: str


@dataclass
class Resolution:
    """Resolver 输出"""
    specs: List[VenueOrderSpec] = field(default_factory=list)
    rejection: Optional[Rejection] = None

    @property
    def ok(self) -> bool:
        return self.rejection is None and len(self.specs) > 0


# ─────────────────────────────────────────────
# ClosePolicy 策略接口
# ─────────────────────────────────────────────

class ClosePolicy(ABC):
    """平仓选桶策略。不同市场实现不同 Policy。"""

    @abstractmethod
    def build_close_legs(
        self, req: OrderRequest, spec: SymbolInfo,
        qty: Decimal, price: Optional[Decimal],
        position_volume: Decimal,
    ) -> List[VenueOrderSpec]:
        """构造平仓 leg(s)。

        Args:
            req: 原始请求
            spec: 品种规格
            qty: 量化后的平仓数量（正数）
            price: 量化后的价格
            position_volume: 当前已成交持仓（signed）

        Returns:
            一个或多个 VenueOrderSpec
        """
        ...


class NettingClosePolicy(ClosePolicy):
    """净轧平仓（FX Netting / 加密现货）。

    Netting 账户不区分多空持仓桶，平仓就是发反向单。
    直接构造一个 leg，无需选桶。
    """

    def build_close_legs(
        self, req: OrderRequest, spec: SymbolInfo,
        qty: Decimal, price: Optional[Decimal],
        position_volume: Decimal,
    ) -> List[VenueOrderSpec]:
        return [VenueOrderSpec(
            symbol=req.symbol,
            side=req.side,
            offset=Offset.CLOSE,
            qty=qty,
            kind=req.kind,
            price=price,
            stop_price=req.stop_price,
            tif=req.tif or Tif.GTC,
            client_order_id=req.client_order_id,
            tag=req.tag,
        )]


class FifoClosePolicy(ClosePolicy):
    """FIFO 平仓（加密永续双向 / 国内期货）。

    一期不实现逻辑，仅占位。未来按开仓时间排序选桶。
    """

    def build_close_legs(
        self, req: OrderRequest, spec: SymbolInfo,
        qty: Decimal, price: Optional[Decimal],
        position_volume: Decimal,
    ) -> List[VenueOrderSpec]:
        # 一期 fallback 到 netting 行为
        return [VenueOrderSpec(
            symbol=req.symbol,
            side=req.side,
            offset=Offset.CLOSE,
            qty=qty,
            kind=req.kind,
            price=price,
            stop_price=req.stop_price,
            tif=req.tif or Tif.GTC,
            client_order_id=req.client_order_id,
            tag=req.tag,
        )]


# ClosePolicy 注册表
CLOSE_POLICY_MAP: Dict[str, ClosePolicy] = {
    "net": NettingClosePolicy(),
    "fifo": FifoClosePolicy(),
}


# ─────────────────────────────────────────────
# UnifiedResolver
# ─────────────────────────────────────────────

class UnifiedResolver:
    """统一订单解析器。

    主流程：
      1. 验证（合规性）
      2. 量化（对齐 step/tick）
      3. 构造 legs（OPEN 直接构造，CLOSE 委托 ClosePolicy）
    """

    def __init__(self, rounding=ROUND_HALF_UP):
        self._rounding = rounding

    def resolve(
        self,
        req: OrderRequest,
        spec: SymbolInfo,
        position_volume: Decimal = Decimal("0"),
        available_to_close: Decimal = Decimal("0"),
        has_in_flight_open: bool = False,
    ) -> Resolution:
        """解析订单意图。

        Args:
            req: 策略意图
            spec: 品种规格
            position_volume: 当前已成交持仓（signed）
            available_to_close: 可平量
            has_in_flight_open: 同方向是否有在途 OPEN（防重）

        Returns:
            Resolution（specs 或 rejection）
        """
        # ① 验证
        rej = self._validate(req, spec, position_volume, available_to_close, has_in_flight_open)
        if rej:
            return Resolution(rejection=rej)

        # ② 量化
        qty = self._quantize(req.qty, spec.qty_step)
        price = self._quantize(req.price, spec.tick_size) if req.price else None
        stop_price = self._quantize(req.stop_price, spec.tick_size) if req.stop_price else None

        # ③ 量化后二次校验
        rej = self._post_validate(qty, price, spec, req)
        if rej:
            return Resolution(rejection=rej)

        # ④ 生成 client_order_id
        if not req.client_order_id:
            req.client_order_id = f"KQ-{uuid.uuid4().hex[:16]}"

        # ⑤ 构造 legs
        if req.offset == Offset.CLOSE:
            policy = CLOSE_POLICY_MAP.get(spec.close_priority, CLOSE_POLICY_MAP["net"])
            legs = policy.build_close_legs(req, spec, qty, price, position_volume)
            # 补充 stop_price 和 tif
            for leg in legs:
                if stop_price and not leg.stop_price:
                    leg.stop_price = stop_price
        else:
            legs = [VenueOrderSpec(
                symbol=req.symbol,
                side=req.side,
                offset=Offset.OPEN,
                qty=qty,
                kind=req.kind,
                price=price,
                stop_price=stop_price,
                tif=req.tif or Tif.GTC,
                client_order_id=req.client_order_id,
                tag=req.tag,
            )]

        return Resolution(specs=legs)

    # ─── 验证 ───

    def _validate(
        self, req: OrderRequest, spec: SymbolInfo,
        position_volume: Decimal, available_to_close: Decimal,
        has_in_flight_open: bool,
    ) -> Optional[Rejection]:
        # qty 必须为正
        if req.qty <= 0:
            return Rejection("INVALID_QTY", f"qty must > 0, got {req.qty}")

        # 最小/最大下单量
        if spec.qty_step > 0 and req.qty < spec.min_qty:
            return Rejection("BELOW_MIN", f"qty {req.qty} < min {spec.min_qty}")
        if spec.qty_max > 0 and req.qty > spec.qty_max:
            return Rejection("ABOVE_MAX", f"qty {req.qty} > max {spec.qty_max}")

        # 做空权限
        if req.side == OrderSide.SELL and req.offset == Offset.OPEN and not spec.can_short:
            return Rejection("NO_SHORT", f"{spec.symbol} does not allow shorting")

        # 平仓可平量
        if req.offset == Offset.CLOSE:
            if available_to_close <= 0:
                return Rejection("NOTHING_TO_CLOSE", "no available position to close")
            if req.qty > available_to_close:
                return Rejection(
                    "EXCEED_CLOSEABLE",
                    f"close qty {req.qty} > available {available_to_close}",
                )

        # 防重：同方向有在途 OPEN
        if req.offset == Offset.OPEN and has_in_flight_open:
            return Rejection(
                "DUPLICATE_OPEN",
                f"already has in-flight OPEN order for {req.symbol}/{req.tag}/{req.side.value}",
            )

        # 价格类型校验
        if req.kind in (OrderKind.LIMIT, OrderKind.STOP_LIMIT) and not req.price:
            return Rejection("PRICE_REQUIRED", f"{req.kind.value} order requires price")
        if req.kind in (OrderKind.STOP_MARKET, OrderKind.STOP_LIMIT) and not req.stop_price:
            return Rejection("STOP_PRICE_REQUIRED", f"{req.kind.value} order requires stop_price")

        return None

    def _post_validate(
        self, qty: Decimal, price: Optional[Decimal],
        spec: SymbolInfo, req: OrderRequest,
    ) -> Optional[Rejection]:
        """量化后二次校验"""
        # 量化后可能变成 0（如 qty=0.001 但 step=0.01）
        if qty <= 0:
            return Rejection("QTY_ROUNDED_ZERO", f"qty rounded to 0 (step={spec.qty_step})")
        # 量化后仍超范围
        if spec.min_qty > 0 and qty < spec.min_qty:
            return Rejection("BELOW_MIN_AFTER_ROUND", f"rounded qty {qty} < min {spec.min_qty}")
        return None

    # ─── 量化 ───

    def _quantize(self, value: Optional[Decimal], step: Optional[Decimal]) -> Optional[Decimal]:
        """对齐到 step（四舍五入）"""
        if value is None or step is None or step <= 0:
            return value
        return (value / step).quantize(Decimal("1"), rounding=self._rounding) * step

"""kqsdk —— KlineQuant 策略 SDK 的公开入口（扁平门面）。

外部 ``from klinequant import kqsdk`` 之后，策略作者只需从本包一次性拿到全部公开符号::

    from klinequant.kqsdk import strategy, KqApi, StrategyContext, OrderSide, Offset, OrderKind

    @strategy("dualma-eur")           # 策略名自定义（≠ 文件名）
    def dual_ma(api: KqApi, ctx: StrategyContext) -> None:
        ...                            # 回测 / 实盘同一套代码

无需感知内部模块结构（``kqsdk.api`` / ``kqsdk.registry`` / ``kqsdk.indic`` /
``kqsdk.risk_adapter`` …），也无需直接 ``import protocol``——交易枚举已由本门面转发。

公开面（见 ``__all__``）：
  策略编写    strategy / StrategyContext / KqApi
  装载辅助    resolve_strategy_fn / invoke_strategy / registered_strategies /
              clear_registry / StrategyLoadError
  迭代容错    resilient
  指标视图    IndicApi / IndicatorView / FieldView
  ZigZag      ZigZagView / Pivot / CrossEvent / zigzag_high / zigzag_low
  交易类型    OrderSide / Offset / OrderKind / OrderType / Position / SymbolInfo
              （转发自 protocol.types，策略侧不必依赖 protocol 包）
"""
from __future__ import annotations

from klinequant.kqsdk.api import KqApi
from klinequant.kqsdk.indic import (
    CrossEvent,
    FieldView,
    IndicApi,
    IndicatorView,
    Pivot,
    ZigZagView,
    zigzag_high,
    zigzag_low,
)
from klinequant.kqsdk.registry import (
    StrategyContext,
    StrategyLoadError,
    clear_registry,
    invoke_strategy,
    registered_strategies,
    resilient,
    resolve_strategy_fn,
    strategy,
)
from klinequant.protocol.types import (
    Offset,
    OrderKind,
    OrderSide,
    OrderType,
    Position,
    SymbolInfo,
)

__all__ = [
    # ── 策略编写 ──
    "strategy",
    "StrategyContext",
    "KqApi",
    # ── 装载辅助 ──
    "resolve_strategy_fn",
    "invoke_strategy",
    "registered_strategies",
    "clear_registry",
    "StrategyLoadError",
    # ── 迭代级容错 ──
    "resilient",
    # ── 指标视图 ──
    "IndicApi",
    "IndicatorView",
    "FieldView",
    # ── 条件驱动 ZigZag ──
    "ZigZagView",
    "Pivot",
    "CrossEvent",
    "zigzag_high",
    "zigzag_low",
    # ── 交易类型（转发 protocol.types）──
    "OrderSide",
    "Offset",
    "OrderKind",
    "OrderType",
    "Position",
    "SymbolInfo",
]

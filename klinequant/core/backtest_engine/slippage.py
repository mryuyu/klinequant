"""滑点模型

三种滑点模型：
    - FixedSlippage: 固定滑点（绝对值）
    - PercentageSlippage: 百分比滑点
    - VolumeBasedSlippage: 基于成交量的滑点（量越小滑点越大）

遵循需求文档 §4.5 BT-002。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional


class SlippageModel(ABC):
    """滑点模型抽象基类"""

    @abstractmethod
    def calculate(
        self,
        price: Decimal,
        quantity: Decimal,
        side: str,
        volume: Optional[Decimal] = None,
    ) -> Decimal:
        """计算滑点后的实际成交价格

        Args:
            price: 信号触发价格
            quantity: 订单数量
            side: BUY / SELL
            volume: 当前 K 线成交量（VolumeBased 模型需要）

        Returns:
            滑点后的成交价格
        """
        ...


class FixedSlippage(SlippageModel):
    """固定滑点：价格 ± 固定值

    买入时价格上移，卖出时价格下移。
    """

    def __init__(self, ticks: Decimal = Decimal("0.01")):
        self._ticks = ticks

    @property
    def ticks(self) -> Decimal:
        return self._ticks

    def calculate(
        self,
        price: Decimal,
        quantity: Decimal,
        side: str,
        volume: Optional[Decimal] = None,
    ) -> Decimal:
        if side == "BUY":
            return price + self._ticks
        return price - self._ticks


class PercentageSlippage(SlippageModel):
    """百分比滑点：价格 ± price * pct"""

    def __init__(self, pct: Decimal = Decimal("0.0005")):
        self._pct = pct

    @property
    def pct(self) -> Decimal:
        return self._pct

    def calculate(
        self,
        price: Decimal,
        quantity: Decimal,
        side: str,
        volume: Optional[Decimal] = None,
    ) -> Decimal:
        slip = price * self._pct
        if side == "BUY":
            return price + slip
        return price - slip


class VolumeBasedSlippage(SlippageModel):
    """基于成交量的滑点

    滑点 = price * impact_factor * (quantity / volume)
    量越小或订单越大，滑点越大。
    """

    def __init__(self, impact_factor: Decimal = Decimal("0.1")):
        self._impact_factor = impact_factor

    @property
    def impact_factor(self) -> Decimal:
        return self._impact_factor

    def calculate(
        self,
        price: Decimal,
        quantity: Decimal,
        side: str,
        volume: Optional[Decimal] = None,
    ) -> Decimal:
        if volume is None or volume <= 0:
            # 无成交量数据时退化为 0.1% 固定百分比
            volume = quantity * 1000
        ratio = quantity / volume
        slip = price * self._impact_factor * ratio
        if side == "BUY":
            return price + slip
        return price - slip


def create_slippage_model(
    model_type: str = "percentage", **kwargs
) -> SlippageModel:
    """工厂函数：创建滑点模型"""
    models = {
        "fixed": FixedSlippage,
        "percentage": PercentageSlippage,
        "volume_based": VolumeBasedSlippage,
    }
    cls = models.get(model_type)
    if cls is None:
        raise ValueError(f"Unknown slippage model: {model_type}, available: {list(models.keys())}")
    return cls(**kwargs)

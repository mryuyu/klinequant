"""手续费模型

三种手续费模型：
    - FixedFee: 固定手续费（每笔固定金额）
    - PercentageFee: 百分比手续费（按成交金额比例）
    - TieredFee: 阶梯手续费（Maker/Taker 不同费率）

遵循需求文档 §4.5 BT-003。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal


class FeeModel(ABC):
    """手续费模型抽象基类"""

    @abstractmethod
    def calculate(
        self,
        price: Decimal,
        quantity: Decimal,
        side: str,
        is_maker: bool = False,
    ) -> Decimal:
        """计算手续费

        Args:
            price: 成交价格
            quantity: 成交数量
            side: BUY / SELL
            is_maker: 是否为 Maker（挂单方）

        Returns:
            手续费金额（正数）
        """
        ...


class FixedFee(FeeModel):
    """固定手续费：每笔交易固定金额"""

    def __init__(self, fee_per_trade: Decimal = Decimal("1.0")):
        self._fee = fee_per_trade

    @property
    def fee_per_trade(self) -> Decimal:
        return self._fee

    def calculate(
        self,
        price: Decimal,
        quantity: Decimal,
        side: str,
        is_maker: bool = False,
    ) -> Decimal:
        return self._fee


class PercentageFee(FeeModel):
    """百分比手续费：成交金额 × 费率"""

    def __init__(self, rate: Decimal = Decimal("0.001")):
        self._rate = rate

    @property
    def rate(self) -> Decimal:
        return self._rate

    def calculate(
        self,
        price: Decimal,
        quantity: Decimal,
        side: str,
        is_maker: bool = False,
    ) -> Decimal:
        return price * quantity * self._rate


class TieredFee(FeeModel):
    """阶梯手续费：Maker/Taker 不同费率

    Binance 默认：Maker 0.1%, Taker 0.1%
    使用 BNB 折扣后：Maker 0.075%, Taker 0.075%
    """

    def __init__(
        self,
        maker_rate: Decimal = Decimal("0.001"),
        taker_rate: Decimal = Decimal("0.001"),
    ):
        self._maker_rate = maker_rate
        self._taker_rate = taker_rate

    @property
    def maker_rate(self) -> Decimal:
        return self._maker_rate

    @property
    def taker_rate(self) -> Decimal:
        return self._taker_rate

    def calculate(
        self,
        price: Decimal,
        quantity: Decimal,
        side: str,
        is_maker: bool = False,
    ) -> Decimal:
        rate = self._maker_rate if is_maker else self._taker_rate
        return price * quantity * rate


def _to_decimal(v):
    """数值参数归一为 Decimal（兼容 yaml/JSON 传入的 float/int/str；Decimal 原样返回）。

    手续费模型内部以 Decimal 与价格/数量相乘，float 参数会触发
    ``Decimal * float`` TypeError，故在工厂入口统一转换。
    """
    if isinstance(v, Decimal):
        return v
    if isinstance(v, (int, float, str)):
        return Decimal(str(v))
    return v


def create_fee_model(model_type: str = "percentage", **kwargs) -> FeeModel:
    """工厂函数：创建手续费模型（数值参数统一转 Decimal，兼容 yaml/JSON 的 float/int）。"""
    models = {
        "fixed": FixedFee,
        "percentage": PercentageFee,
        "tiered": TieredFee,
    }
    cls = models.get(model_type)
    if cls is None:
        raise ValueError(f"Unknown fee model: {model_type}, available: {list(models.keys())}")
    return cls(**{k: _to_decimal(v) for k, v in kwargs.items()})

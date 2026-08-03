"""RuleBase — 信号规则抽象基类

提供基于指标值的条件规则：
    - CrossoverRule：交叉检测（金叉/死叉）
    - ThresholdRule：阈值检测（超买/超卖）
    - ComparisonRule：两个指标值比较

遵循需求文档 §4.3 SIG-001。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from protocol.types import SignalDirection


class RuleBase(ABC):
    """规则抽象基类"""

    @property
    @abstractmethod
    def name(self) -> str:
        """规则名称"""
        ...

    @abstractmethod
    def evaluate(
        self,
        current: Dict[str, Any],
        previous: Optional[Dict[str, Any]] = None,
    ) -> Optional[RuleResult]:
        """评估规则

        Args:
            current: 当前指标值字典
            previous: 前一根 K 线的指标值字典

        Returns:
            RuleResult（触发时）或 None（未触发）
        """
        ...


class RuleResult:
    """规则评估结果"""

    def __init__(
        self,
        rule_name: str,
        direction: SignalDirection,
        strength: float = 0.5,
        reason: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.rule_name = rule_name
        self.direction = direction
        self.strength = min(1.0, max(0.0, strength))
        self.reason = reason
        self.metadata = metadata or {}

    def __repr__(self) -> str:
        return (
            f"RuleResult(rule={self.rule_name}, dir={self.direction.value}, "
            f"strength={self.strength:.2f}, reason={self.reason!r})"
        )


class CrossoverRule(RuleBase):
    """交叉规则 — 检测两条线的金叉/死叉

    金叉（LONG）：fast 从下方穿越 slow
    死叉（SHORT）：fast 从上方穿越 slow

    Args:
        fast_key: 快线在指标值字典中的 key
        slow_key: 慢线在指标值字典中的 key
    """

    def __init__(self, fast_key: str, slow_key: str):
        self._fast_key = fast_key
        self._slow_key = slow_key

    @property
    def name(self) -> str:
        return f"Crossover({self._fast_key} x {self._slow_key})"

    def evaluate(
        self,
        current: Dict[str, Any],
        previous: Optional[Dict[str, Any]] = None,
    ) -> Optional[RuleResult]:
        if previous is None:
            return None

        fast_curr = current.get(self._fast_key)
        slow_curr = current.get(self._slow_key)
        fast_prev = previous.get(self._fast_key)
        slow_prev = previous.get(self._slow_key)

        if any(v is None for v in [fast_curr, slow_curr, fast_prev, slow_prev]):
            return None

        # 金叉：前一根 fast < slow，当前 fast >= slow
        if fast_prev < slow_prev and fast_curr >= slow_curr:
            return RuleResult(
                rule_name=self.name,
                direction=SignalDirection.LONG,
                strength=0.7,
                reason=f"Golden cross: {self._fast_key}({fast_curr:.4f}) crossed above {self._slow_key}({slow_curr:.4f})",
            )

        # 死叉：前一根 fast > slow，当前 fast <= slow
        if fast_prev > slow_prev and fast_curr <= slow_curr:
            return RuleResult(
                rule_name=self.name,
                direction=SignalDirection.SHORT,
                strength=0.7,
                reason=f"Death cross: {self._fast_key}({fast_curr:.4f}) crossed below {self._slow_key}({slow_curr:.4f})",
            )

        return None


class ThresholdRule(RuleBase):
    """阈值规则 — 检测指标值穿越阈值

    上穿阈值（SHORT）：value 从下方穿越 upper
    下穿阈值（LONG）：value 从上方穿越 lower

    Args:
        value_key: 指标值字典中的 key
        upper: 上界阈值（超买）
        lower: 下界阈值（超卖）
    """

    def __init__(
        self,
        value_key: str,
        upper: Optional[float] = None,
        lower: Optional[float] = None,
    ):
        self._value_key = value_key
        self._upper = upper
        self._lower = lower

    @property
    def name(self) -> str:
        return f"Threshold({self._value_key}, upper={self._upper}, lower={self._lower})"

    def evaluate(
        self,
        current: Dict[str, Any],
        previous: Optional[Dict[str, Any]] = None,
    ) -> Optional[RuleResult]:
        if previous is None:
            return None

        val_curr = current.get(self._value_key)
        val_prev = previous.get(self._value_key)

        if val_curr is None or val_prev is None:
            return None

        # 上穿 upper → 超买信号（SHORT）
        if self._upper is not None:
            if val_prev < self._upper and val_curr >= self._upper:
                return RuleResult(
                    rule_name=self.name,
                    direction=SignalDirection.SHORT,
                    strength=0.6,
                    reason=f"{self._value_key}({val_curr:.4f}) crossed above upper threshold {self._upper}",
                )

        # 下穿 lower → 超卖信号（LONG）
        if self._lower is not None:
            if val_prev > self._lower and val_curr <= self._lower:
                return RuleResult(
                    rule_name=self.name,
                    direction=SignalDirection.LONG,
                    strength=0.6,
                    reason=f"{self._value_key}({val_curr:.4f}) crossed below lower threshold {self._lower}",
                )

        return None


class ComparisonRule(RuleBase):
    """比较规则 — 两个指标值的比较

    Args:
        left_key: 左侧指标值 key
        right_key: 右侧指标值 key
        operator: 比较运算符 ('>', '<', '>=', '<=', '==')
        direction: 触发时的信号方向
    """

    _OPERATORS = {
        ">": lambda a, b: a > b,
        "<": lambda a, b: a < b,
        ">=": lambda a, b: a >= b,
        "<=": lambda a, b: a <= b,
        "==": lambda a, b: abs(a - b) < 1e-10,
    }

    def __init__(
        self,
        left_key: str,
        right_key: str,
        operator: str = ">",
        direction: SignalDirection = SignalDirection.LONG,
    ):
        if operator not in self._OPERATORS:
            raise ValueError(f"Invalid operator: {operator}")
        self._left_key = left_key
        self._right_key = right_key
        self._operator = operator
        self._direction = direction

    @property
    def name(self) -> str:
        return f"Compare({self._left_key} {self._operator} {self._right_key})"

    def evaluate(
        self,
        current: Dict[str, Any],
        previous: Optional[Dict[str, Any]] = None,
    ) -> Optional[RuleResult]:
        left = current.get(self._left_key)
        right = current.get(self._right_key)

        if left is None or right is None:
            return None

        if self._OPERATORS[self._operator](left, right):
            return RuleResult(
                rule_name=self.name,
                direction=self._direction,
                strength=0.5,
                reason=f"{self._left_key}({left:.4f}) {self._operator} {self._right_key}({right:.4f})",
            )

        return None

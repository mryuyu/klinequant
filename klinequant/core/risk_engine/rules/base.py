"""RiskRule — 风控规则抽象基类

所有风控规则的统一接口：
    - name: 规则名称
    - check(context): 风控检查
    - 返回 RiskCheckResult（通过/拒绝+原因）

遵循需求文档 §4.4 RISK-001~RISK-012。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional

from protocol.types import Account, Order, Position


@dataclass
class RiskContext:
    """风控检查上下文 — 包含当前订单和系统状态"""
    order: Order
    account: Optional[Account] = None
    positions: Dict[str, Position] = field(default_factory=dict)
    open_orders: List[Order] = field(default_factory=list)
    # 当日已实现盈亏
    daily_pnl: Decimal = Decimal("0")
    # 策略当日盈亏
    strategy_pnl: Dict[str, Decimal] = field(default_factory=dict)
    # 策略连续亏损次数
    strategy_consecutive_losses: Dict[str, int] = field(default_factory=dict)
    # 当前时间戳 (ms)
    timestamp: int = 0
    # 额外上下文
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RiskCheckResult:
    """风控检查结果"""
    passed: bool
    rule_name: str
    reason: str = ""
    level: str = "INFO"  # INFO / WARNING / CRITICAL

    @staticmethod
    def ok(rule_name: str) -> "RiskCheckResult":
        return RiskCheckResult(passed=True, rule_name=rule_name)

    @staticmethod
    def reject(rule_name: str, reason: str, level: str = "WARNING") -> "RiskCheckResult":
        return RiskCheckResult(passed=False, rule_name=rule_name, reason=reason, level=level)


class RiskRule(ABC):
    """风控规则抽象基类"""

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        self._params = params or {}
        self._enabled = True

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool) -> None:
        self._enabled = value

    @property
    def params(self) -> Dict[str, Any]:
        return dict(self._params)

    def update_params(self, params: Dict[str, Any]) -> None:
        """热更新参数"""
        self._params.update(params)

    @abstractmethod
    def check(self, ctx: RiskContext) -> RiskCheckResult:
        """执行风控检查"""
        ...

    def __repr__(self) -> str:
        return f"{self.name}(enabled={self._enabled})"

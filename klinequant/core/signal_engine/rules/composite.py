"""CompositeRules — 组合条件规则

支持 AND / OR / NOT 逻辑组合：
    - AndRule：所有子规则都触发才触发
    - OrRule：任一子规则触发即触发
    - NotRule：反转规则方向

遵循需求文档 §4.3 SIG-002。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from protocol.types import SignalDirection

from core.signal_engine.rules.base import RuleBase, RuleResult


class AndRule(RuleBase):
    """AND 组合规则 — 所有子规则必须同时触发

    触发条件：所有子规则都返回非 None 结果
    信号方向：取第一个子规则的方向
    信号强度：所有子规则强度的平均值
    """

    def __init__(self, rules: List[RuleBase]):
        self._rules = rules

    @property
    def name(self) -> str:
        inner = " AND ".join(r.name for r in self._rules)
        return f"AND({inner})"

    def evaluate(
        self,
        current: Dict[str, Any],
        previous: Optional[Dict[str, Any]] = None,
    ) -> Optional[RuleResult]:
        results: List[RuleResult] = []

        for rule in self._rules:
            result = rule.evaluate(current, previous)
            if result is None:
                return None  # AND 要求所有规则都触发
            results.append(result)

        if not results:
            return None

        # 检查方向一致性
        directions = {r.direction for r in results}
        if len(directions) > 1:
            return None  # 方向冲突不触发

        avg_strength = sum(r.strength for r in results) / len(results)
        reasons = " + ".join(r.reason for r in results)

        return RuleResult(
            rule_name=self.name,
            direction=results[0].direction,
            strength=avg_strength,
            reason=reasons,
            metadata={"sub_results": [r.rule_name for r in results]},
        )


class OrRule(RuleBase):
    """OR 组合规则 — 任一子规则触发即触发

    触发条件：至少一个子规则返回非 None 结果
    信号方向：取第一个触发规则的方向
    信号强度：取触发规则中最高的强度
    """

    def __init__(self, rules: List[RuleBase]):
        self._rules = rules

    @property
    def name(self) -> str:
        inner = " OR ".join(r.name for r in self._rules)
        return f"OR({inner})"

    def evaluate(
        self,
        current: Dict[str, Any],
        previous: Optional[Dict[str, Any]] = None,
    ) -> Optional[RuleResult]:
        triggered: List[RuleResult] = []

        for rule in self._rules:
            result = rule.evaluate(current, previous)
            if result is not None:
                triggered.append(result)

        if not triggered:
            return None

        # 取强度最高的
        best = max(triggered, key=lambda r: r.strength)
        reasons = " | ".join(r.reason for r in triggered)

        return RuleResult(
            rule_name=self.name,
            direction=best.direction,
            strength=best.strength,
            reason=reasons,
            metadata={
                "triggered_count": len(triggered),
                "sub_results": [r.rule_name for r in triggered],
            },
        )


class NotRule(RuleBase):
    """NOT 规则 — 反转子规则方向

    子规则触发 LONG → NOT 输出 SHORT
    子规则触发 SHORT → NOT 输出 LONG
    """

    def __init__(self, rule: RuleBase):
        self._rule = rule

    @property
    def name(self) -> str:
        return f"NOT({self._rule.name})"

    def evaluate(
        self,
        current: Dict[str, Any],
        previous: Optional[Dict[str, Any]] = None,
    ) -> Optional[RuleResult]:
        result = self._rule.evaluate(current, previous)
        if result is None:
            return None

        # 反转方向
        reversed_direction = (
            SignalDirection.SHORT
            if result.direction == SignalDirection.LONG
            else SignalDirection.LONG
            if result.direction == SignalDirection.SHORT
            else result.direction
        )

        return RuleResult(
            rule_name=self.name,
            direction=reversed_direction,
            strength=result.strength,
            reason=f"Reversed: {result.reason}",
            metadata={"original_direction": result.direction.value},
        )

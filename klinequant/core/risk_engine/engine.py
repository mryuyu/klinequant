"""RiskEngine — 风控引擎主循环

核心原则：fail-closed（风控不可用时拒绝所有订单）

功能：
    - 管理风控规则集
    - 订单预检查（所有规则通过才放行）
    - 风控日志写入（不可删除）
    - 规则热更新（运行时修改参数/启停）
    - 性能目标：单次检查 < 1ms

遵循需求文档 §4.4 RISK-009~RISK-011。
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, List, Optional

from core.risk_engine.rules.base import RiskCheckResult, RiskContext, RiskRule
from core.risk_engine.rules.rules import create_default_rules

logger = logging.getLogger(__name__)


class RiskEngine:
    """风控引擎 — fail-closed 设计

    任何异常情况（规则执行出错、引擎未启动）都拒绝订单。
    """

    def __init__(
        self,
        rules: Optional[List[RiskRule]] = None,
        fail_closed: bool = True,
    ):
        """
        Args:
            rules: 风控规则列表（None 使用默认 12 条）
            fail_closed: True=异常时拒绝（默认），False=异常时放行
        """
        self._rules: List[RiskRule] = rules if rules is not None else create_default_rules()
        self._fail_closed = fail_closed
        self._running = False

        # 风控日志回调
        self._log_callbacks: List[Callable] = []

        # 统计
        self._total_checks = 0
        self._total_passed = 0
        self._total_rejected = 0

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def fail_closed(self) -> bool:
        return self._fail_closed

    @property
    def rules(self) -> List[RiskRule]:
        return list(self._rules)

    @property
    def stats(self) -> Dict[str, int]:
        return {
            "total_checks": self._total_checks,
            "total_passed": self._total_passed,
            "total_rejected": self._total_rejected,
        }

    def add_rule(self, rule: RiskRule) -> None:
        """添加风控规则"""
        self._rules.append(rule)
        logger.info(f"Added risk rule: {rule.name}")

    def remove_rule(self, rule_name: str) -> bool:
        """移除风控规则"""
        for i, r in enumerate(self._rules):
            if r.name == rule_name:
                self._rules.pop(i)
                logger.info(f"Removed risk rule: {rule_name}")
                return True
        return False

    def get_rule(self, rule_name: str) -> Optional[RiskRule]:
        """获取规则实例"""
        for r in self._rules:
            if r.name == rule_name:
                return r
        return None

    def update_rule_params(self, rule_name: str, params: Dict[str, Any]) -> bool:
        """热更新规则参数"""
        rule = self.get_rule(rule_name)
        if rule is None:
            return False
        rule.update_params(params)
        logger.info(f"Updated risk rule params: {rule_name} -> {params}")
        return True

    def enable_rule(self, rule_name: str, enabled: bool = True) -> bool:
        """启用/禁用规则"""
        rule = self.get_rule(rule_name)
        if rule is None:
            return False
        rule.enabled = enabled
        logger.info(f"Risk rule {rule_name} {'enabled' if enabled else 'disabled'}")
        return True

    def add_log_callback(self, callback: Callable) -> None:
        """添加风控日志回调"""
        self._log_callbacks.append(callback)

    def check_order(self, ctx: RiskContext) -> RiskCheckResult:
        """执行风控检查（同步，< 1ms）

        所有启用的规则都必须通过，任一拒绝则整体拒绝。
        fail-closed: 规则执行异常时拒绝订单。

        Returns:
            RiskCheckResult: passed=True 表示通过
        """
        # fail-closed: 引擎未运行时拒绝
        if not self._running:
            return RiskCheckResult.reject(
                "risk_engine",
                "RiskEngine not running (fail-closed)",
                "CRITICAL",
            )

        self._total_checks += 1

        for rule in self._rules:
            if not rule.enabled:
                continue

            try:
                result = rule.check(ctx)
            except Exception as e:
                # fail-closed: 规则异常时拒绝
                if self._fail_closed:
                    self._total_rejected += 1
                    reason = f"Rule {rule.name} exception: {e}"
                    logger.error(reason)
                    self._emit_log(ctx, rule.name, "CRITICAL", reason)
                    return RiskCheckResult.reject(rule.name, reason, "CRITICAL")
                else:
                    logger.warning(f"Rule {rule.name} exception (fail-open): {e}")
                    continue

            if not result.passed:
                self._total_rejected += 1
                logger.warning(
                    f"Risk REJECTED: {result.rule_name} - {result.reason}"
                )
                self._emit_log(ctx, result.rule_name, result.level, result.reason)
                return result

        self._total_passed += 1
        return RiskCheckResult.ok("all_rules")

    def start(self) -> None:
        """启动风控引擎"""
        self._running = True
        logger.info(
            f"RiskEngine started ({len(self._rules)} rules, "
            f"fail_closed={self._fail_closed})"
        )

    def stop(self) -> None:
        """停止风控引擎"""
        self._running = False
        logger.info("RiskEngine stopped")

    def _emit_log(
        self, ctx: RiskContext, rule_name: str, level: str, message: str
    ) -> None:
        """触发风控日志回调"""
        log_entry = {
            "strategy_id": ctx.order.strategy_id,
            "rule_name": rule_name,
            "level": level,
            "message": message,
            "symbol": ctx.order.symbol,
            "timestamp": ctx.timestamp or int(time.time() * 1000),
        }
        for cb in self._log_callbacks:
            try:
                cb(log_entry)
            except Exception as e:
                logger.error(f"Risk log callback error: {e}")

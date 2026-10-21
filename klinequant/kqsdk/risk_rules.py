"""SDK 轨新增风控规则（RiskEngine 规则体系扩展，注册进同一 rules 列表）

RiskEngine 既有 12 条规则（RISK-001~012）零改动；本模块补三条 SDK 轨规则：
  - DailyLossPctRule        日亏损百分比限额（既有 RISK-004 只有绝对金额，
                            与金额规则「先到先拒」由同列表顺序执行天然达成）
  - MaxTotalPositionsRule   全局持仓笔数上限（跨策略汇总；既有 RISK-003 按名义
                            价值，FX 保证金语义不适用）
  - MaxPositionsPerStrategyRule 单策略持仓笔数上限

数据一律从 RiskContext.extra 读（由 RiskDataProvider 供给）：
  daily_pnl_pct / total_positions / strategy_positions / offset。

开平语义：笔数类规则只拦 OPEN（加仓），CLOSE（减仓）永远放行——只减风险
不加风险，与 R5 断线闸门同一原则。
"""
from __future__ import annotations

from decimal import Decimal
from typing import Dict

from core.risk_engine.rules.base import RiskCheckResult, RiskContext, RiskRule

__all__ = [
    "DailyLossPctRule",
    "MaxTotalPositionsRule",
    "MaxPositionsPerStrategyRule",
    "create_sdk_rules",
]


class DailyLossPctRule(RiskRule):
    """SDK-R1: 单日最大亏损（占日起始权益百分比）"""

    @property
    def name(self) -> str:
        return "daily_loss_pct"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        max_pct = Decimal(str(self._params.get("max_pct", 0)))
        if max_pct <= 0:
            return RiskCheckResult.ok(self.name)
        pnl_pct = Decimal(str(ctx.extra.get("daily_pnl_pct", 0) or 0))
        if pnl_pct < -max_pct:
            return RiskCheckResult.reject(
                self.name,
                f"Daily PnL {pnl_pct:.2f}% exceeds max daily loss -{max_pct}%",
                "CRITICAL",
            )
        return RiskCheckResult.ok(self.name)


class MaxTotalPositionsRule(RiskRule):
    """SDK-R2: 全局持仓笔数上限（跨策略汇总，按 (symbol,tag) 有效持仓计）"""

    @property
    def name(self) -> str:
        return "max_total_positions"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        max_pos = int(self._params.get("max_positions", 0) or 0)
        if max_pos <= 0:
            return RiskCheckResult.ok(self.name)
        if str(ctx.extra.get("offset", "")) != "OPEN":
            return RiskCheckResult.ok(self.name)   # CLOSE 放行（只减风险）
        total = int(ctx.extra.get("total_positions", 0) or 0)
        if total >= max_pos:
            return RiskCheckResult.reject(
                self.name,
                f"Total open positions {total} >= max {max_pos} (global)",
            )
        return RiskCheckResult.ok(self.name)


class MaxPositionsPerStrategyRule(RiskRule):
    """SDK-R3: 单策略持仓笔数上限（按 tag 前缀归属策略）"""

    @property
    def name(self) -> str:
        return "max_positions_per_strategy"

    def check(self, ctx: RiskContext) -> RiskCheckResult:
        max_pos = int(self._params.get("max_positions", 0) or 0)
        if max_pos <= 0:
            return RiskCheckResult.ok(self.name)
        if str(ctx.extra.get("offset", "")) != "OPEN":
            return RiskCheckResult.ok(self.name)   # CLOSE 放行
        strategy_id = ctx.order.strategy_id
        per_strategy: Dict[str, int] = ctx.extra.get("strategy_positions", {}) or {}
        count = int(per_strategy.get(_base_tag(strategy_id), 0))
        if count >= max_pos:
            return RiskCheckResult.reject(
                self.name,
                f"Strategy {strategy_id} open positions {count} >= max {max_pos}",
            )
        return RiskCheckResult.ok(self.name)


def _base_tag(tag: str) -> str:
    """tag(``{strategy}`` 或 ``{strategy}:{period}``) → 策略基名。"""
    return tag.split(":", 1)[0] if tag else tag


def create_sdk_rules(
    *,
    max_daily_loss_pct: float = 0.0,
    max_total_positions: int = 0,
    max_positions_per_strategy: int = 0,
) -> list:
    """按配置构造 SDK 轨规则（参数 <=0 的规则仍创建但恒放行，便于热更新启用）。"""
    return [
        DailyLossPctRule({"max_pct": max_daily_loss_pct}),
        MaxTotalPositionsRule({"max_positions": max_total_positions}),
        MaxPositionsPerStrategyRule({"max_positions": max_positions_per_strategy}),
    ]

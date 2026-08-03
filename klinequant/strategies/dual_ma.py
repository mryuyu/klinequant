"""双均线示例策略（Dual Moving Average Crossover）

经典趋势跟踪策略：
    - 短期均线上穿长期均线（金叉）→ 做多
    - 短期均线下穿长期均线（死叉）→ 做空/平仓

参数：
    - fast_period: 短期均线周期（默认 7）
    - slow_period: 长期均线周期（默认 25）

遵循需求文档 §4.6 STR-005。
"""
from __future__ import annotations

from typing import Optional

import polars as pl

from core.strategy_engine.base import StrategyBase


class DualMAStrategy(StrategyBase):
    """双均线交叉策略"""

    def on_init(self) -> None:
        """初始化：设置默认参数"""
        if self.ctx.get_param("fast_period") is None:
            self.ctx.set_param("fast_period", 7)
        if self.ctx.get_param("slow_period") is None:
            self.ctx.set_param("slow_period", 25)

        self.logger.info(
            f"DualMA initialized: fast={self.ctx.get_param('fast_period')}, "
            f"slow={self.ctx.get_param('slow_period')}"
        )

    def on_bar(self, df: pl.DataFrame, bar_index: int) -> Optional[str]:
        """每根 K 线：检测均线交叉

        Args:
            df: 含 close 列的 DataFrame
            bar_index: 当前 bar 索引

        Returns:
            "LONG" / "SHORT" / None
        """
        fast = self.ctx.get_param("fast_period", 7)
        slow = self.ctx.get_param("slow_period", 25)

        # 数据不足
        if bar_index < slow:
            return None

        # 计算均线
        closes = df["close"]
        if len(closes) < slow + 1:
            return None

        # 当前和前一 bar 的均线值
        fast_ma = closes.slice(bar_index - fast + 1, fast).mean()
        slow_ma = closes.slice(bar_index - slow + 1, slow).mean()
        prev_fast_ma = closes.slice(bar_index - fast, fast).mean()
        prev_slow_ma = closes.slice(bar_index - slow, slow).mean()

        if None in (fast_ma, slow_ma, prev_fast_ma, prev_slow_ma):
            return None

        # 金叉：短均线从下方穿越长均线
        if prev_fast_ma <= prev_slow_ma and fast_ma > slow_ma:
            self.logger.info(
                f"Golden cross at bar {bar_index}: "
                f"fast_ma={fast_ma:.2f} > slow_ma={slow_ma:.2f}"
            )
            self.ctx.set_state("last_signal_bar", bar_index)
            return "LONG"

        # 死叉：短均线从上方穿越长均线
        if prev_fast_ma >= prev_slow_ma and fast_ma < slow_ma:
            self.logger.info(
                f"Death cross at bar {bar_index}: "
                f"fast_ma={fast_ma:.2f} < slow_ma={slow_ma:.2f}"
            )
            self.ctx.set_state("last_signal_bar", bar_index)
            return "SHORT"

        return None

    def on_stop(self) -> None:
        """停止时保存状态"""
        self.logger.info("DualMA strategy stopped")

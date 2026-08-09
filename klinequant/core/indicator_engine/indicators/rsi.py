"""RSI — 相对强弱指标 (Relative Strength Index)

基于 polars 实现 Wilder 平滑法的 RSI 计算。
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import polars as pl

from core.indicator_engine.base import IndicatorBase
from core.indicator_engine.registry import register_indicator


@register_indicator
class RSI(IndicatorBase):
    """相对强弱指标

    参数：
        period: 周期数（默认 14）
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        super().__init__(params)
        self._period = self._params.get("period", 14)

    @property
    def name(self) -> str:
        return "RSI"

    @property
    def min_periods(self) -> int:
        return self._period + 1  # 需要 period+1 个数据点计算变化

    def _calculate(self, df: pl.DataFrame) -> pl.DataFrame:
        period = self._period
        col_name = f"RSI_{period}"

        # 计算价格变化
        delta = pl.col("close").diff()

        # 分离涨幅和跌幅
        gain = delta.clip(lower_bound=0)
        loss = (-delta).clip(lower_bound=0)

        # Wilder 平滑（等价于 alpha=1/period 的 EMA）
        alpha = 1.0 / period
        avg_gain = gain.ewm_mean(alpha=alpha, adjust=False)
        avg_loss = loss.ewm_mean(alpha=alpha, adjust=False)

        # RS = avg_gain / avg_loss, RSI = 100 - 100/(1+RS)
        rsi = (
            pl.when(avg_loss == 0)
            .then(pl.lit(100.0))
            .otherwise(
                100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
            )
        )

        return df.with_columns(rsi.alias(col_name))

"""EMA — 指数移动平均线 (Exponential Moving Average)

基于 polars ewm_mean 实现，支持自定义周期。
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import polars as pl

from core.indicator_engine.base import IndicatorBase
from core.indicator_engine.registry import register_indicator


@register_indicator
class EMA(IndicatorBase):
    """指数移动平均线

    参数：
        period: 周期数（默认 20）
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        super().__init__(params)
        self._period = self._params.get("period", 20)

    @property
    def name(self) -> str:
        return "EMA"

    @property
    def min_periods(self) -> int:
        return self._period

    def _calculate(self, df: pl.DataFrame) -> pl.DataFrame:
        col_name = f"EMA_{self._period}"
        alpha = 2.0 / (self._period + 1)
        return df.with_columns(
            pl.col("close")
            .ewm_mean(alpha=alpha, adjust=False)
            .alias(col_name)
        )

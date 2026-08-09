"""MA — 移动平均线 (Simple Moving Average)

基于 polars rolling_mean 实现，支持自定义周期。
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import polars as pl

from core.indicator_engine.base import IndicatorBase
from core.indicator_engine.registry import register_indicator


@register_indicator
class MA(IndicatorBase):
    """简单移动平均线

    参数：
        period: 周期数（默认 20）
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        super().__init__(params)
        self._period = self._params.get("period", 20)

    @property
    def name(self) -> str:
        return "MA"

    @property
    def min_periods(self) -> int:
        return self._period

    def _calculate(self, df: pl.DataFrame) -> pl.DataFrame:
        col_name = f"MA_{self._period}"
        return df.with_columns(
            pl.col("close")
            .rolling_mean(window_size=self._period)
            .alias(col_name)
        )

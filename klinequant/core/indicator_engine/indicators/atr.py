"""ATR — 平均真实波幅 (Average True Range)

基于 polars 实现 Wilder 平滑的 ATR 计算。
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import polars as pl

from core.indicator_engine.base import IndicatorBase
from core.indicator_engine.registry import register_indicator


@register_indicator
class ATR(IndicatorBase):
    """平均真实波幅

    参数：
        period: 周期数（默认 14）
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        super().__init__(params)
        self._period = self._params.get("period", 14)

    @property
    def name(self) -> str:
        return "ATR"

    @property
    def min_periods(self) -> int:
        return self._period + 1  # 需要前一根收盘价

    def _calculate(self, df: pl.DataFrame) -> pl.DataFrame:
        period = self._period
        col_name = f"ATR_{period}"

        prev_close = pl.col("close").shift(1)

        # True Range = max(high-low, |high-prev_close|, |low-prev_close|)
        tr = pl.max_horizontal([
            pl.col("high") - pl.col("low"),
            (pl.col("high") - prev_close).abs(),
            (pl.col("low") - prev_close).abs(),
        ])

        # Wilder 平滑（alpha = 1/period）
        atr = tr.ewm_mean(alpha=1.0 / period, adjust=False)

        return df.with_columns(atr.alias(col_name))

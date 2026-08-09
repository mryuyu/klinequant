"""BOLL — 布林带 (Bollinger Bands)

基于 polars rolling_mean + rolling_std 实现。
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import polars as pl

from core.indicator_engine.base import IndicatorBase
from core.indicator_engine.registry import register_indicator


@register_indicator
class BOLL(IndicatorBase):
    """布林带指标

    参数：
        period: 周期数（默认 20）
        std_dev: 标准差倍数（默认 2.0）
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        super().__init__(params)
        self._period = self._params.get("period", 20)
        self._std_dev = self._params.get("std_dev", 2.0)

    @property
    def name(self) -> str:
        return "BOLL"

    @property
    def min_periods(self) -> int:
        return self._period

    def _calculate(self, df: pl.DataFrame) -> pl.DataFrame:
        period = self._period
        std_dev = self._std_dev
        prefix = f"BOLL_{period}_{std_dev}"

        mid = pl.col("close").rolling_mean(window_size=period)
        std = pl.col("close").rolling_std(window_size=period)

        upper = mid + std_dev * std
        lower = mid - std_dev * std

        return df.with_columns([
            upper.alias(f"{prefix}_UPPER"),
            mid.alias(f"{prefix}_MID"),
            lower.alias(f"{prefix}_LOWER"),
        ])

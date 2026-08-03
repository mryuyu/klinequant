"""VWAP — 成交量加权平均价 (Volume Weighted Average Price)

基于 polars 实现日内/滚动 VWAP 计算。
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import polars as pl

from core.indicator_engine.base import IndicatorBase


class VWAP(IndicatorBase):
    """成交量加权平均价

    参数：
        period: 滚动窗口周期数（默认 20，0 表示从起始累计）
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        super().__init__(params)
        self._period = self._params.get("period", 20)

    @property
    def name(self) -> str:
        return "VWAP"

    @property
    def min_periods(self) -> int:
        return max(1, self._period)

    def _calculate(self, df: pl.DataFrame) -> pl.DataFrame:
        col_name = f"VWAP_{self._period}"

        # 典型价格 = (H + L + C) / 3
        tp = (pl.col("high") + pl.col("low") + pl.col("close")) / 3.0
        tp_vol = tp * pl.col("volume")

        if self._period == 0:
            # 累计 VWAP
            cum_vol = pl.col("volume").cum_sum()
            cum_tp_vol = tp_vol.cum_sum()
            vwap = cum_tp_vol / cum_vol
        else:
            # 滚动 VWAP
            rolling_vol = pl.col("volume").rolling_sum(window_size=self._period)
            rolling_tp_vol = tp_vol.rolling_sum(window_size=self._period)
            vwap = rolling_tp_vol / rolling_vol

        return df.with_columns(vwap.alias(col_name))

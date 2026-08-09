"""KDJ — 随机指标 (Stochastic Oscillator)

基于 polars 实现 K/D/J 三线计算。
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import polars as pl

from core.indicator_engine.base import IndicatorBase
from core.indicator_engine.registry import register_indicator


@register_indicator
class KDJ(IndicatorBase):
    """KDJ 随机指标

    参数：
        k_period: K 线周期（默认 9）
        d_period: D 线平滑周期（默认 3）
        j_period: J 线平滑周期（默认 3）
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        super().__init__(params)
        self._k_period = self._params.get("k_period", 9)
        self._d_period = self._params.get("d_period", 3)
        self._j_period = self._params.get("j_period", 3)

    @property
    def name(self) -> str:
        return "KDJ"

    @property
    def min_periods(self) -> int:
        return self._k_period

    def _calculate(self, df: pl.DataFrame) -> pl.DataFrame:
        k_period = self._k_period
        prefix = f"KDJ_{k_period}_{self._d_period}_{self._j_period}"

        # 计算 N 周期内最高价和最低价
        highest = pl.col("high").rolling_max(window_size=k_period)
        lowest = pl.col("low").rolling_min(window_size=k_period)

        # RSV = (close - lowest) / (highest - lowest) * 100
        rsv = (
            pl.when((highest - lowest) == 0)
            .then(pl.lit(50.0))
            .otherwise(
                (pl.col("close") - lowest) / (highest - lowest) * 100.0
            )
        )

        # K = SMA(RSV, d_period) — 用 ewm 近似
        k_alpha = 1.0 / self._d_period
        k = rsv.ewm_mean(alpha=k_alpha, adjust=False)

        # D = SMA(K, j_period)
        d_alpha = 1.0 / self._j_period
        d = k.ewm_mean(alpha=d_alpha, adjust=False)

        # J = 3*K - 2*D
        j = 3.0 * k - 2.0 * d

        return df.with_columns([
            k.alias(f"{prefix}_K"),
            d.alias(f"{prefix}_D"),
            j.alias(f"{prefix}_J"),
        ])

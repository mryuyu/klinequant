"""MACD — 指数平滑异同移动平均线

基于 polars 实现 DIF/DEA/HIST 三线计算。
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import polars as pl

from core.indicator_engine.base import IndicatorBase


class MACD(IndicatorBase):
    """MACD 指标

    参数：
        fast_period: 快线周期（默认 12）
        slow_period: 慢线周期（默认 26）
        signal_period: 信号线周期（默认 9）
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        super().__init__(params)
        self._fast = self._params.get("fast_period", 12)
        self._slow = self._params.get("slow_period", 26)
        self._signal = self._params.get("signal_period", 9)

    @property
    def name(self) -> str:
        return "MACD"

    @property
    def min_periods(self) -> int:
        return self._slow + self._signal - 1

    def _calculate(self, df: pl.DataFrame) -> pl.DataFrame:
        fast_alpha = 2.0 / (self._fast + 1)
        slow_alpha = 2.0 / (self._slow + 1)
        signal_alpha = 2.0 / (self._signal + 1)

        ema_fast = pl.col("close").ewm_mean(alpha=fast_alpha, adjust=False)
        ema_slow = pl.col("close").ewm_mean(alpha=slow_alpha, adjust=False)

        # DIF = EMA(fast) - EMA(slow)
        dif = ema_fast - ema_slow

        prefix = f"MACD_{self._fast}_{self._slow}_{self._signal}"

        # DEA = EMA(DIF, signal)
        dea = dif.ewm_mean(alpha=signal_alpha, adjust=False)

        # HIST = 2 * (DIF - DEA)
        hist = 2.0 * (dif - dea)

        return df.with_columns([
            dif.alias(f"{prefix}_DIF"),
            dea.alias(f"{prefix}_DEA"),
            hist.alias(f"{prefix}_HIST"),
        ])

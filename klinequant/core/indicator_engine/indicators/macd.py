"""MACD — 指数平滑异同移动平均线

基于 polars 实现 DIF/DEA/HIST 三线计算；另提供 O(1) 增量递推
（IND-101，快照法处理未收盘 bar），与全量 ewm_mean(adjust=False) 结果一致。
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import polars as pl

from core.indicator_engine.base import IndicatorBase
from core.indicator_engine.registry import register_indicator


@register_indicator
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
        self._alpha_fast = 2.0 / (self._fast + 1)
        self._alpha_slow = 2.0 / (self._slow + 1)
        self._alpha_signal = 2.0 / (self._signal + 1)
        self._reset_state()

    @property
    def name(self) -> str:
        return "MACD"

    @property
    def min_periods(self) -> int:
        return self._slow + self._signal - 1

    @property
    def default_params(self) -> Dict[str, Any]:
        return {"fast_period": 12, "slow_period": 26, "signal_period": 9}

    @property
    def supports_incremental(self) -> bool:
        return True

    @property
    def display_meta(self) -> Dict[str, Any]:
        return {"fields": ["DIF", "DEA", "HIST"], "range": "zero_symmetric"}

    def _calculate(self, df: pl.DataFrame) -> pl.DataFrame:
        ema_fast = pl.col("close").ewm_mean(alpha=self._alpha_fast, adjust=False)
        ema_slow = pl.col("close").ewm_mean(alpha=self._alpha_slow, adjust=False)

        # DIF = EMA(fast) - EMA(slow)
        dif = ema_fast - ema_slow

        prefix = f"MACD_{self._fast}_{self._slow}_{self._signal}"

        # DEA = EMA(DIF, signal)
        dea = dif.ewm_mean(alpha=self._alpha_signal, adjust=False)

        # HIST = 2 * (DIF - DEA)
        hist = 2.0 * (dif - dea)

        return df.with_columns([
            dif.alias(f"{prefix}_DIF"),
            dea.alias(f"{prefix}_DEA"),
            hist.alias(f"{prefix}_HIST"),
        ])

    # ─── 增量递推（O(1)/bar，快照法） ───

    def _reset_state(self) -> None:
        self._ema_fast: Optional[float] = None
        self._ema_slow: Optional[float] = None
        self._dea: Optional[float] = None
        self._count = 0
        self._last_ts: Optional[int] = None
        # 快照：最近一根已确认 bar 后的状态 (ema_fast, ema_slow, dea, count)
        self._snap: Optional[Tuple[Optional[float], Optional[float], Optional[float], int]] = None

    def reset(self) -> None:
        super().reset()
        self._reset_state()

    def _apply(self, close: float) -> None:
        """应用一根 bar 的 close（初值对齐 polars ewm_mean(adjust=False)：首值=首个数据点）"""
        if self._ema_fast is None:
            self._ema_fast = close
            self._ema_slow = close
            self._dea = 0.0  # 首根 DIF = 0（快慢 EMA 同种子）→ DEA 首值 = DIF
        else:
            self._ema_fast = self._alpha_fast * close + (1 - self._alpha_fast) * self._ema_fast
            self._ema_slow = self._alpha_slow * close + (1 - self._alpha_slow) * self._ema_slow
            dif = self._ema_fast - self._ema_slow
            self._dea = self._alpha_signal * dif + (1 - self._alpha_signal) * self._dea
        self._count += 1

    def update_bar(
        self, bar: Dict[str, Any], is_closed: bool
    ) -> Optional[Dict[str, Any]]:
        ts = bar["timestamp"]
        close = float(bar["close"])
        if self._last_ts is not None and ts < self._last_ts:
            return None  # 乱序历史 bar：增量路径不处理（走全量重算/重新预热）

        # 快照 = 当前 bar 之前的状态：同 ts 重复推送（未收盘）恢复后重新应用，
        # 新 ts 到达即隐式确认上一根（不依赖 is_closed 标记，源侧漏标也安全）
        if ts != self._last_ts:
            self._snap = (self._ema_fast, self._ema_slow, self._dea, self._count)
        elif self._snap is not None:
            self._ema_fast, self._ema_slow, self._dea, self._count = self._snap

        self._apply(close)
        self._last_ts = ts

        if self._count >= self.min_periods:
            self._warmed_up = True
        if not self._warmed_up:
            return None

        dif = self._ema_fast - self._ema_slow
        values = {"DIF": dif, "DEA": self._dea, "HIST": 2.0 * (dif - self._dea)}
        self._last_values = values
        return values

"""MACD 倍数族策略（M2.5 端到端验收样例）

base(2,5,3) × 倍数(1/4/16/64) 声明 4 组 MACD 实例，验证指标引擎
多实例隔离与 IND-106 声明消费链路（require_indicators → 引擎预热 → df 注入）。

信号逻辑（家族共识）：
    - ≥ threshold 组 HIST > 0 → LONG
    - ≥ threshold 组 HIST < 0 → SHORT
    - 其余 → 持仓不动（None）

参数：
    - fast_period / slow_period / signal_period: 1x 基准参数（默认 2/5/3）
    - mults: 倍数列表（默认 [1, 4, 16, 64]）
    - threshold: 共识组数阈值（默认 3）
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import polars as pl

from core.strategy_engine.base import StrategyBase
from core.strategy_engine.wiring import field_col

DEFAULT_MULTS = [1, 4, 16, 64]


class MACDFamilyStrategy(StrategyBase):
    """MACD 倍数族共识策略"""

    def _base_params(self) -> Dict[str, int]:
        return {
            "fast_period": self.ctx.get_param("fast_period", 2),
            "slow_period": self.ctx.get_param("slow_period", 5),
            "signal_period": self.ctx.get_param("signal_period", 3),
        }

    def _mults(self) -> List[int]:
        return self.ctx.get_param("mults", DEFAULT_MULTS)

    def family_params(self, mult: int) -> Dict[str, int]:
        """倍数展开：基准三参数同乘"""
        base = self._base_params()
        return {k: v * mult for k, v in base.items()}

    def hist_col(self, mult: int) -> str:
        """该倍数实例 HIST 注入列名"""
        return field_col("MACD", self.family_params(mult), "HIST")

    def on_init(self) -> None:
        """声明 4 组 MACD 实例（IND-106）"""
        symbol = self.ctx.info.symbols[0] if self.ctx.info.symbols else "BTCUSDT"
        timeframe = (
            self.ctx.info.timeframes[0] if self.ctx.info.timeframes else "1m"
        )
        indicators: List[Tuple[str, Dict[str, Any]]] = [
            ("MACD", self.family_params(m)) for m in self._mults()
        ]
        self.require_indicators(symbol, timeframe, indicators)
        self.logger.info(
            f"MACDFamily initialized: base={self._base_params()} "
            f"mults={self._mults()} threshold={self.ctx.get_param('threshold', 3)}"
        )

    def on_bar(self, df: pl.DataFrame, bar_index: int) -> Optional[str]:
        """家族共识：统计当前 bar 各组 HIST 正负"""
        threshold = self.ctx.get_param("threshold", 3)
        row = df.row(bar_index, named=True)

        pos = neg = 0
        for mult in self._mults():
            v = row.get(self.hist_col(mult))
            if v is None:
                continue  # 该倍数预热未完成（慢周期组早期 bar）
            if v > 0:
                pos += 1
            elif v < 0:
                neg += 1

        if pos >= threshold:
            return "LONG"
        if neg >= threshold:
            return "SHORT"
        return None

    def on_stop(self) -> None:
        self.logger.info("MACDFamily strategy stopped")

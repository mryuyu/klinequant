"""MACD 多倍数趋势触发策略（MACD_MULTI def 式指标接入样例）

MACD_MULTI 单实例同时输出 1X 柱 + 1X/4X/16X/64X 的 DIF/DEA，
天然适合"大周期定方向、小周期找入场点"：
    - 16X 趋势过滤：柱值 = 2*(DIF_16X - DEA_16X)（指标仅 1X 出柱，
      大周期柱在策略层由 DIF/DEA 还原，与 Pine 同公式）：>0 只做多、<0 只做空
    - 1X DIF 上穿/下穿 DEA 作触发信号

指标声明走 IND-106 标准链路：require_indicators → 引擎预热 →
wiring.inject_indicators 注入 df → on_bar 按 field_col 读列。
"""
from __future__ import annotations

from typing import Dict, Optional

import polars as pl

from core.strategy_engine.base import StrategyBase
from core.strategy_engine.wiring import field_col

INDICATOR = "MACD_MULTI"


class MACDMultiStrategy(StrategyBase):
    """16X 趋势过滤 + 1X 金叉/死叉触发"""

    def _ind_params(self) -> Dict[str, int]:
        return {
            "s": self.ctx.get_param("s", 12),
            "p": self.ctx.get_param("p", 20),
            "m": self.ctx.get_param("m", 9),
        }

    def _col(self, field: str) -> str:
        """该实例字段注入列名（不硬编码列名，与 wiring 同源）"""
        return field_col(INDICATOR, self._ind_params(), field)

    def on_init(self) -> None:
        symbol = self.ctx.info.symbols[0] if self.ctx.info.symbols else "BTCUSDT"
        timeframe = (
            self.ctx.info.timeframes[0] if self.ctx.info.timeframes else "1m"
        )
        self.require_indicators(symbol, timeframe, [(INDICATOR, self._ind_params())])
        self.logger.info(f"MACDMulti initialized: params={self._ind_params()}")

    def on_bar(self, df: pl.DataFrame, bar_index: int) -> Optional[str]:
        """16X 定向 + 1X 交叉触发"""
        if bar_index < 1:
            return None
        row = df.row(bar_index, named=True)
        prev = df.row(bar_index - 1, named=True)

        dif16, dea16 = row.get(self._col("DIF_16X")), row.get(self._col("DEA_16X"))
        dif1, dea1 = row.get(self._col("DIF_1X")), row.get(self._col("DEA_1X"))
        pdif1, pdea1 = (
            prev.get(self._col("DIF_1X")), prev.get(self._col("DEA_1X")),
        )
        if None in (dif16, dea16, dif1, dea1, pdif1, pdea1):
            return None   # 预热未完成（换参/早期 bar）

        mcd16 = 2.0 * (dif16 - dea16)   # 16X 柱：指标仅 1X 出柱，此处同公式还原

        if mcd16 > 0 and pdif1 <= pdea1 and dif1 > dea1:
            return "LONG"    # 大周期多头 + 1X 金叉
        if mcd16 < 0 and pdif1 >= pdea1 and dif1 < dea1:
            return "SHORT"   # 大周期空头 + 1X 死叉
        return None

    def on_stop(self) -> None:
        self.logger.info("MACDMulti strategy stopped")

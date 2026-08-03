"""IndicatorBase — 指标抽象基类

所有技术指标的统一接口：
    - name / params：指标标识
    - min_periods：预热所需最小 K 线数
    - calculate(df)：全量计算（polars DataFrame）
    - update(df)：增量计算（仅新增 K 线）

遵循需求文档 §4.2 IND-001~IND-007。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import polars as pl

from protocol.types import IndicatorValue


class IndicatorBase(ABC):
    """指标抽象基类

    子类必须实现：
        - name 属性
        - min_periods 属性
        - _calculate(df) 方法
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        self._params = params or {}
        self._warmed_up = False
        self._last_values: Optional[Dict[str, Any]] = None

    @property
    @abstractmethod
    def name(self) -> str:
        """指标名称，如 'MA', 'RSI', 'MACD'"""
        ...

    @property
    @abstractmethod
    def min_periods(self) -> int:
        """预热所需最小 K 线数量"""
        ...

    @property
    def params(self) -> Dict[str, Any]:
        return dict(self._params)

    @property
    def is_warmed_up(self) -> bool:
        return self._warmed_up

    @property
    def last_values(self) -> Optional[Dict[str, Any]]:
        return self._last_values

    def calculate(self, df: pl.DataFrame) -> pl.DataFrame:
        """全量计算指标

        Args:
            df: polars DataFrame，至少包含 ['timestamp', 'open', 'high', 'low', 'close', 'volume'] 列

        Returns:
            附加指标列的 DataFrame
        """
        if len(df) < self.min_periods:
            self._warmed_up = False
            return df
        result = self._calculate(df)
        self._warmed_up = True
        return result

    @abstractmethod
    def _calculate(self, df: pl.DataFrame) -> pl.DataFrame:
        """子类实现具体指标计算"""
        ...

    def to_indicator_value(
        self,
        symbol: str,
        timeframe: str,
        timestamp: int,
        values: Dict[str, Any],
    ) -> IndicatorValue:
        """构建 IndicatorValue"""
        return IndicatorValue(
            indicator_name=self.name,
            symbol=symbol,
            timeframe=timeframe,
            timestamp=timestamp,
            values=values,
            params=self.params,
        )

    def __repr__(self) -> str:
        param_str = ", ".join(f"{k}={v}" for k, v in self._params.items())
        return f"{self.name}({param_str})"

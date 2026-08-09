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
    def default_params(self) -> Dict[str, Any]:
        """默认参数（meta 端点展示用），子类按需覆盖"""
        return {}

    @property
    def supports_incremental(self) -> bool:
        """是否支持 O(1) 增量递推（IND-101），默认 False 走全量重算"""
        return False

    @property
    def display_meta(self) -> Dict[str, Any]:
        """展示元数据（IND-102/IND-109 契约）

        fields: 输出字段列表；range: 值域类型
            unbounded（无界）/ bounded_0_100（0-100）/ zero_symmetric（零轴对称）
        同窗格叠加兼容性判断依据。
        """
        return {"fields": [], "range": "unbounded"}

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

    def reset(self) -> None:
        """重置内部状态（预热重放前调用）"""
        self._warmed_up = False
        self._last_values = None

    def update_bar(
        self, bar: Dict[str, Any], is_closed: bool
    ) -> Optional[Dict[str, Any]]:
        """单根 K 线增量计算（O(1) 递推），支持增量的子类必须实现

        快照法约定（IND-101）：内部状态必须可回滚到「最近一根已确认 bar」，
        未收盘 bar 重复推送（同 timestamp）时先恢复快照再应用，保证幂等。

        Args:
            bar: 原始 K 线 dict，至少含 timestamp(ms)/close
            is_closed: 该 K 线是否已收盘

        Returns:
            当根指标值 dict；预热未完成返回 None
        """
        raise NotImplementedError(f"{self.name} 不支持增量计算")

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

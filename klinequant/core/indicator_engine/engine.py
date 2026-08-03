"""IndicatorEngine — 指标引擎主循环

整合所有组件：
    - IndicatorRegistry（指标注册表）
    - polars DataFrame（K 线数据缓存）
    - 增量计算（新 K 线仅增量更新）
    - 多周期指标独立计算
    - 指标预热（加载历史数据初始化）
    - ZMQ SUB（接收行情） + ZMQ PUB（发布指标值）

遵循需求文档 §4.2 IND-001~IND-013。
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import polars as pl

from core.indicator_engine.base import IndicatorBase
from core.indicator_engine.registry import IndicatorRegistry, get_registry
from protocol.types import IndicatorValue, Kline

logger = logging.getLogger(__name__)

# K 线数据 key: (symbol, exchange, timeframe)
KlineKey = Tuple[str, str, str]


class IndicatorEngine:
    """指标引擎

    功能：
        1. 管理指标实例（按 symbol + timeframe 分组）
        2. 接收 K 线数据，增量更新指标
        3. 预热：从历史数据初始化指标
        4. 回调通知：指标值变化时触发订阅者
    """

    def __init__(
        self,
        registry: Optional[IndicatorRegistry] = None,
        max_cache_size: int = 10000,
    ):
        self._registry = registry or get_registry()
        self._max_cache_size = max_cache_size

        # K 线数据缓存: KlineKey -> pl.DataFrame
        self._kline_cache: Dict[KlineKey, pl.DataFrame] = {}

        # 指标实例: KlineKey -> List[IndicatorBase]
        self._indicators: Dict[KlineKey, List[IndicatorBase]] = defaultdict(list)

        # 订阅者回调: indicator_name -> List[callback]
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)

        # 全局回调（所有指标）
        self._global_subscribers: List[Callable] = []

        self._running = False

    @property
    def registry(self) -> IndicatorRegistry:
        return self._registry

    @property
    def is_running(self) -> bool:
        return self._running

    def add_indicator(
        self,
        indicator: IndicatorBase,
        symbol: str,
        exchange: str,
        timeframe: str,
    ) -> None:
        """添加指标实例到指定品种/周期

        Args:
            indicator: 指标实例
            symbol: 交易对
            exchange: 交易所
            timeframe: 周期
        """
        key = (symbol, exchange, timeframe)
        self._indicators[key].append(indicator)
        logger.info(f"Added indicator {indicator} for {key}")

    def create_indicator(
        self,
        name: str,
        params: Optional[Dict[str, Any]],
        symbol: str,
        exchange: str,
        timeframe: str,
    ) -> IndicatorBase:
        """通过注册表创建并添加指标

        Returns:
            创建的指标实例
        """
        indicator = self._registry.create(name, params)
        self.add_indicator(indicator, symbol, exchange, timeframe)
        return indicator

    def subscribe(
        self,
        indicator_name: str,
        callback: Callable[[IndicatorValue], None],
    ) -> None:
        """订阅特定指标的更新

        Args:
            indicator_name: 指标名称（如 'MA', 'RSI'）
            callback: 回调函数
        """
        self._subscribers[indicator_name].append(callback)

    def subscribe_all(
        self, callback: Callable[[IndicatorValue], None]
    ) -> None:
        """订阅所有指标的更新"""
        self._global_subscribers.append(callback)

    def warmup(
        self,
        symbol: str,
        exchange: str,
        timeframe: str,
        historical_df: pl.DataFrame,
    ) -> Dict[str, Dict[str, Any]]:
        """使用历史数据预热指标

        Args:
            symbol: 交易对
            exchange: 交易所
            timeframe: 周期
            historical_df: 历史 K 线 DataFrame

        Returns:
            各指标最新值字典
        """
        key = (symbol, exchange, timeframe)

        # 缓存历史数据
        self._kline_cache[key] = historical_df.clone()

        results = {}
        for indicator in self._indicators.get(key, []):
            result_df = indicator.calculate(historical_df)
            if indicator.is_warmed_up:
                # 提取最后一行的指标值
                values = self._extract_last_values(indicator, result_df)
                indicator._last_values = values
                results[indicator.name] = values
                logger.info(
                    f"Warmed up {indicator} for {key}: {len(historical_df)} bars"
                )
            else:
                logger.warning(
                    f"Insufficient data to warmup {indicator} for {key}: "
                    f"need {indicator.min_periods}, got {len(historical_df)}"
                )
        return results

    def update_kline(self, kline: Kline) -> List[IndicatorValue]:
        """接收新 K 线，增量更新指标

        Args:
            kline: 新到达的 K 线

        Returns:
            更新的 IndicatorValue 列表
        """
        key = (kline.symbol, kline.exchange, kline.timeframe)
        updated_values: List[IndicatorValue] = []

        # 更新 K 线缓存
        new_row = pl.DataFrame({
            "timestamp": [kline.timestamp],
            "open": [float(kline.open)],
            "high": [float(kline.high)],
            "low": [float(kline.low)],
            "close": [float(kline.close)],
            "volume": [float(kline.volume)],
            "quote_volume": [float(kline.quote_volume)],
            "trade_count": [kline.trade_count],
            "is_closed": [kline.is_closed],
        })

        if key in self._kline_cache:
            cache = self._kline_cache[key]

            # 检查是否为更新（相同 timestamp）还是新增
            if len(cache) > 0 and cache["timestamp"][-1] == kline.timestamp:
                # 更新最后一行（未收盘 K 线）
                self._kline_cache[key] = pl.concat([
                    cache.slice(0, len(cache) - 1),
                    new_row,
                ])
            else:
                # 新增 K 线
                self._kline_cache[key] = pl.concat([cache, new_row])

                # 限制缓存大小
                if len(self._kline_cache[key]) > self._max_cache_size:
                    self._kline_cache[key] = self._kline_cache[key].slice(
                        -self._max_cache_size
                    )
        else:
            self._kline_cache[key] = new_row

        # 增量计算所有关联指标
        df = self._kline_cache[key]
        for indicator in self._indicators.get(key, []):
            result_df = indicator.calculate(df)
            if indicator.is_warmed_up:
                values = self._extract_last_values(indicator, result_df)
                indicator._last_values = values

                iv = indicator.to_indicator_value(
                    symbol=kline.symbol,
                    timeframe=kline.timeframe,
                    timestamp=kline.timestamp,
                    values=values,
                )
                updated_values.append(iv)

                # 通知订阅者
                self._notify_subscribers(indicator.name, iv)

        return updated_values

    def get_indicator_value(
        self,
        indicator_name: str,
        symbol: str,
        exchange: str,
        timeframe: str,
    ) -> Optional[Dict[str, Any]]:
        """获取指定指标的当前值"""
        key = (symbol, exchange, timeframe)
        for indicator in self._indicators.get(key, []):
            if indicator.name == indicator_name:
                return indicator.last_values
        return None

    def get_all_values(
        self, symbol: str, exchange: str, timeframe: str
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """获取指定品种/周期的所有指标当前值"""
        key = (symbol, exchange, timeframe)
        result = {}
        for indicator in self._indicators.get(key, []):
            result[indicator.name] = indicator.last_values
        return result

    def get_kline_cache(
        self, symbol: str, exchange: str, timeframe: str
    ) -> Optional[pl.DataFrame]:
        """获取 K 线缓存数据"""
        key = (symbol, exchange, timeframe)
        return self._kline_cache.get(key)

    def list_indicators(
        self, symbol: str, exchange: str, timeframe: str
    ) -> List[str]:
        """列出指定品种/周期的所有指标"""
        key = (symbol, exchange, timeframe)
        return [ind.name for ind in self._indicators.get(key, [])]

    def warmup_keys(self) -> Set[KlineKey]:
        """返回所有已预热的 key 集合"""
        warmed = set()
        for key, indicators in self._indicators.items():
            if all(ind.is_warmed_up for ind in indicators) and indicators:
                warmed.add(key)
        return warmed

    # ─── 内部方法 ───

    def _extract_last_values(
        self, indicator: IndicatorBase, df: pl.DataFrame
    ) -> Dict[str, Any]:
        """从计算结果 DataFrame 提取最后一行的指标值"""
        # 找到该指标产生的列
        prefix_map = self._get_column_prefix(indicator)
        values = {}

        for col_name in df.columns:
            for prefix in prefix_map:
                if col_name.startswith(prefix):
                    val = df[col_name][-1]
                    # 提取子名称（如 MACD_12_26_9_DIF → DIF）
                    suffix = col_name[len(prefix):]
                    if suffix.startswith("_"):
                        suffix = suffix[1:]
                    if suffix:
                        values[suffix] = float(val) if val is not None else None
                    else:
                        values[indicator.name] = float(val) if val is not None else None

        return values

    def _get_column_prefix(self, indicator: IndicatorBase) -> List[str]:
        """获取指标产生的列名前缀列表"""
        name = indicator.name
        params = indicator.params

        if name == "MA":
            return [f"MA_{params.get('period', 20)}"]
        elif name == "EMA":
            return [f"EMA_{params.get('period', 20)}"]
        elif name == "RSI":
            return [f"RSI_{params.get('period', 14)}"]
        elif name == "MACD":
            f = params.get("fast_period", 12)
            s = params.get("slow_period", 26)
            sig = params.get("signal_period", 9)
            return [f"MACD_{f}_{s}_{sig}"]
        elif name == "BOLL":
            p = params.get("period", 20)
            sd = params.get("std_dev", 2.0)
            return [f"BOLL_{p}_{sd}"]
        elif name == "ATR":
            return [f"ATR_{params.get('period', 14)}"]
        elif name == "KDJ":
            k = params.get("k_period", 9)
            d = params.get("d_period", 3)
            j = params.get("j_period", 3)
            return [f"KDJ_{k}_{d}_{j}"]
        elif name == "VWAP":
            return [f"VWAP_{params.get('period', 20)}"]
        return []

    def _notify_subscribers(self, indicator_name: str, value: IndicatorValue) -> None:
        """通知订阅者"""
        for cb in self._subscribers.get(indicator_name, []):
            try:
                cb(value)
            except Exception as e:
                logger.error(f"Subscriber error for {indicator_name}: {e}")

        for cb in self._global_subscribers:
            try:
                cb(value)
            except Exception as e:
                logger.error(f"Global subscriber error: {e}")

    def start(self) -> None:
        """启动指标引擎"""
        self._running = True
        logger.info("IndicatorEngine started")

    def stop(self) -> None:
        """停止指标引擎"""
        self._running = False
        logger.info("IndicatorEngine stopped")

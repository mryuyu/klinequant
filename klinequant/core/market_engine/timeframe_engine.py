"""K 线周期重采样引擎

基于 1m K 线实时合成更大周期 K 线（5m/15m/1h/4h/1d 等）：
    - 接收 1m K 线，按周期边界聚合
    - 实时更新未收盘的大周期 K 线
    - 大周期收盘时触发 is_closed=True

遵循需求文档 §4.1 MKT-004。
"""
from __future__ import annotations

from decimal import Decimal
from typing import Callable, Dict, List, Optional, Tuple

from core.market_engine.normalizer import TIMEFRAME_MS, align_timestamp, timeframe_to_ms
from protocol.types import Kline


class TimeframeEngine:
    """K 线周期重采样引擎

    用法：
        engine = TimeframeEngine(base_timeframe="1m")
        engine.add_target("5m")
        engine.add_target("1h")

        for kline_1m in stream:
            results = engine.feed(kline_1m)
            # results = {"5m": Kline(...), "1h": Kline(...)}
    """

    def __init__(self, symbol: str, exchange: str, base_timeframe: str = "1m"):
        """
        Args:
            symbol: 交易对
            exchange: 交易所
            base_timeframe: 基础周期（通常为 1m）
        """
        self._symbol = symbol
        self._exchange = exchange
        self._base_tf = base_timeframe
        self._base_ms = timeframe_to_ms(base_timeframe)

        # 目标周期 → 当前聚合状态
        self._targets: Dict[str, _AggregationState] = {}

    def add_target(self, timeframe: str) -> None:
        """添加目标周期。"""
        if timeframe == self._base_tf:
            return
        if timeframe not in TIMEFRAME_MS:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        self._targets[timeframe] = _AggregationState(
            symbol=self._symbol,
            exchange=self._exchange,
            timeframe=timeframe,
            target_ms=timeframe_to_ms(timeframe),
        )

    def remove_target(self, timeframe: str) -> None:
        """移除目标周期。"""
        self._targets.pop(timeframe, None)

    @property
    def target_timeframes(self) -> List[str]:
        """当前所有目标周期。"""
        return list(self._targets.keys())

    def feed(self, kline: Kline) -> Dict[str, Kline]:
        """输入一根基础周期 K 线，返回各目标周期的聚合结果。

        Args:
            kline: 基础周期 K 线（如 1m）

        Returns:
            {target_timeframe: Kline} 字典
        """
        if kline.timeframe != self._base_tf:
            raise ValueError(
                f"Expected base timeframe {self._base_tf}, got {kline.timeframe}"
            )

        results: Dict[str, Kline] = {}
        for tf, state in self._targets.items():
            result = state.update(kline)
            if result is not None:
                results[tf] = result

        return results

    def init_from_history(self, timeframe: str, klines: List[Kline]) -> None:
        """从历史 K 线初始化某个目标周期的状态。

        Args:
            timeframe: 目标周期
            klines: 该周期的历史 K 线列表
        """
        if timeframe not in self._targets:
            self.add_target(timeframe)

        state = self._targets[timeframe]
        if klines:
            # 用最后一根 K 线初始化状态
            last = klines[-1]
            state._current_bar = _BarState(
                open_time=last.timestamp,
                open=last.open,
                high=last.high,
                low=last.low,
                close=last.close,
                volume=last.volume,
                quote_volume=last.quote_volume,
                trade_count=last.trade_count,
            )
            state._bar_start = last.timestamp


class _AggregationState:
    """单个目标周期的聚合状态。"""

    def __init__(
        self,
        symbol: str,
        exchange: str,
        timeframe: str,
        target_ms: int,
    ):
        self._symbol = symbol
        self._exchange = exchange
        self._timeframe = timeframe
        self._target_ms = target_ms
        self._current_bar: Optional[_BarState] = None
        self._bar_start: int = 0

    def update(self, kline: Kline) -> Optional[Kline]:
        """更新聚合状态，返回当前大周期 K 线。"""
        ts = kline.timestamp

        # 判断是否需要开始新的大周期 bar
        aligned = (ts // self._target_ms) * self._target_ms

        if self._current_bar is None or aligned != self._bar_start:
            # 开始新 bar
            self._bar_start = aligned
            self._current_bar = _BarState(
                open_time=aligned,
                open=kline.open,
                high=kline.high,
                low=kline.low,
                close=kline.close,
                volume=kline.volume,
                quote_volume=kline.quote_volume,
                trade_count=kline.trade_count,
            )
        else:
            # 更新当前 bar
            bar = self._current_bar
            if kline.high > bar.high:
                bar.high = kline.high
            if kline.low < bar.low:
                bar.low = kline.low
            bar.close = kline.close
            bar.volume += kline.volume
            bar.quote_volume += kline.quote_volume
            bar.trade_count += kline.trade_count

        # 判断当前大周期 bar 是否已收盘
        bar_end = self._bar_start + self._target_ms
        is_closed = kline.timestamp + timeframe_to_ms(kline.timeframe) >= bar_end

        return Kline(
            symbol=self._symbol,
            exchange=self._exchange,
            timeframe=self._timeframe,
            timestamp=self._bar_start,
            open=self._current_bar.open,
            high=self._current_bar.high,
            low=self._current_bar.low,
            close=self._current_bar.close,
            volume=self._current_bar.volume,
            quote_volume=self._current_bar.quote_volume,
            trade_count=self._current_bar.trade_count,
            is_closed=is_closed,
        )


class _BarState:
    """单根 K 线的聚合中间状态。"""

    __slots__ = (
        "open_time", "open", "high", "low", "close",
        "volume", "quote_volume", "trade_count",
    )

    def __init__(
        self,
        open_time: int,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: Decimal,
        quote_volume: Decimal,
        trade_count: int,
    ):
        self.open_time = open_time
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.quote_volume = quote_volume
        self.trade_count = trade_count

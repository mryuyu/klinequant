"""StrategyBase — 策略抽象基类

所有策略必须继承此类并实现：
    - on_init: 初始化（加载指标、设置参数）
    - on_bar: 每根 K 线收盘时调用
    - on_stop: 策略停止时清理

遵循需求文档 §4.6 STR-001~STR-002。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

import polars as pl

from core.strategy_engine.clients import MarketClient, TradeClient
from core.strategy_engine.context import StrategyContext


class StrategyBase(ABC):
    """策略抽象基类

    生命周期：
        1. __init__(context, trade_client, market_client)
        2. on_init() — 初始化
        3. on_bar(df, bar_index) — 每根 K 线（循环调用）
        4. on_stop() — 停止
    """

    def __init__(
        self,
        context: StrategyContext,
        trade_client: TradeClient,
        market_client: MarketClient,
    ):
        self._ctx = context
        self._trade = trade_client
        self._market = market_client
        # IND-106：指标需求声明 [{symbol, timeframe, indicator, params}]
        self._indicator_requirements: List[Dict[str, Any]] = []

    @property
    def ctx(self) -> StrategyContext:
        return self._ctx

    @property
    def trade(self) -> TradeClient:
        return self._trade

    @property
    def market(self) -> MarketClient:
        return self._market

    @property
    def params(self) -> Dict[str, Any]:
        return self._ctx.params

    @property
    def logger(self):
        return self._ctx.logger

    # ─── 指标需求声明（IND-106） ───

    def require_indicators(
        self,
        symbol: str,
        timeframe: str,
        indicators: List[Tuple[str, Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        """声明策略依赖的指标（on_init 中调用）

        计算契约 key = (指标名, 参数组合)：同指标多参数实例可重复声明（去重）。
        声明由引擎侧统一预热维护，策略不自行实现公式（IND-106）。

        Args:
            indicators: [("MACD", {"fast_period": 12, ...}), ...]

        Returns:
            当前累计的声明列表
        """
        for name, params in indicators:
            req = {
                "symbol": symbol,
                "timeframe": timeframe,
                "indicator": name,
                "params": dict(params or {}),
            }
            if req not in self._indicator_requirements:
                self._indicator_requirements.append(req)
                self.logger.info(
                    f"Indicator required: {name}{req['params']} {symbol}@{timeframe}"
                )
        return list(self._indicator_requirements)

    @property
    def indicator_requirements(self) -> List[Dict[str, Any]]:
        """策略声明的指标需求列表（供管理器/引擎接线消费）"""
        return list(self._indicator_requirements)

    # ─── 生命周期方法 ───

    @abstractmethod
    def on_init(self) -> None:
        """策略初始化

        在此方法中：
            - 设置需要的指标
            - 初始化状态
            - 加载历史数据
        """
        ...

    @abstractmethod
    def on_bar(self, df: pl.DataFrame, bar_index: int) -> Optional[str]:
        """每根 K 线收盘时调用

        Args:
            df: 包含 OHLCV + 指标列的 DataFrame（到当前 bar）
            bar_index: 当前 bar 索引

        Returns:
            信号: "LONG" / "SHORT" / "CLOSE" / None
        """
        ...

    def on_stop(self) -> None:
        """策略停止时调用（可选覆盖）

        在此方法中：
            - 保存状态
            - 清理资源
        """
        pass

    def on_order_filled(self, order_info: Dict[str, Any]) -> None:
        """订单成交回调（可选覆盖）"""
        pass

    def on_signal(self, signal_info: Dict[str, Any]) -> None:
        """信号生成回调（可选覆盖）"""
        pass

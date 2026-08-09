"""Strategy SDK — TradeClient + MarketClient

策略进程通过 SDK 与主引擎通信：
    - TradeClient: 下单/撤单/查询订单/查询持仓
    - MarketClient: 查询 K 线/查询指标值

通信方式：ZMQ REQ/REP（策略进程 → 主引擎）

遵循需求文档 §4.6 STR-001。
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional

from protocol.types import Kline, Order, OrderSide, OrderType, Position

logger = logging.getLogger(__name__)


class TradeClient:
    """交易客户端 — 策略下单接口

    在沙箱模式下通过 ZMQ REQ/REP 与主引擎通信。
    在回测模式下直接调用 BacktestEngine 接口。
    """

    def __init__(self, strategy_id: str, transport=None):
        self._strategy_id = strategy_id
        self._transport = transport
        # 本地模式（回测/模拟）的回调
        self._submit_callback = None
        self._cancel_callback = None
        self._query_positions_callback = None

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def set_callbacks(
        self,
        submit_cb=None,
        cancel_cb=None,
        query_positions_cb=None,
    ):
        """设置本地模式回调（回测/模拟时使用）"""
        self._submit_callback = submit_cb
        self._cancel_callback = cancel_cb
        self._query_positions_callback = query_positions_cb

    async def submit_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        **kwargs,
    ) -> Optional[Order]:
        """提交订单

        Args:
            symbol: 交易对
            side: 买/卖方向
            order_type: 市价/限价
            quantity: 数量
            price: 限价单价格

        Returns:
            Order 对象（含 order_id）
        """
        if self._submit_callback:
            return await self._submit_callback(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                strategy_id=self._strategy_id,
                **kwargs,
            )

        if self._transport:
            msg = {
                "action": "submit_order",
                "strategy_id": self._strategy_id,
                "symbol": symbol,
                "side": side.value,
                "order_type": order_type.value,
                "quantity": str(quantity),
                "price": str(price) if price else None,
            }
            resp = await self._transport.request("trade_service", msg)
            return resp

        logger.warning("TradeClient: no transport or callback configured")
        return None

    async def cancel_order(self, order_id: str) -> bool:
        """撤销订单"""
        if self._cancel_callback:
            return await self._cancel_callback(order_id)

        if self._transport:
            msg = {
                "action": "cancel_order",
                "strategy_id": self._strategy_id,
                "order_id": order_id,
            }
            resp = await self._transport.request("trade_service", msg)
            return resp.get("success", False)

        return False

    async def get_positions(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """查询持仓"""
        if self._query_positions_callback:
            return await self._query_positions_callback(symbol)

        if self._transport:
            msg = {
                "action": "query_positions",
                "strategy_id": self._strategy_id,
                "symbol": symbol,
            }
            return await self._transport.request("trade_service", msg)

        return {}


class MarketClient:
    """行情客户端 — 策略查询行情/指标

    在沙箱模式下通过 ZMQ REQ/REP 与主引擎通信。
    在回测模式下直接访问 DataFrame。
    """

    def __init__(self, strategy_id: str, transport=None):
        self._strategy_id = strategy_id
        self._transport = transport
        # 本地模式回调
        self._get_klines_callback = None
        self._get_indicators_callback = None
        self._get_indicator_history_callback = None

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def set_callbacks(
        self,
        get_klines_cb=None,
        get_indicators_cb=None,
        get_indicator_history_cb=None,
    ):
        """设置本地模式回调"""
        self._get_klines_callback = get_klines_cb
        self._get_indicators_callback = get_indicators_cb
        self._get_indicator_history_callback = get_indicator_history_cb

    async def get_klines(
        self,
        symbol: str,
        timeframe: str = "1h",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """获取 K 线数据

        Args:
            symbol: 交易对
            timeframe: 周期
            limit: 数量

        Returns:
            K 线列表（dict 格式）
        """
        if self._get_klines_callback:
            return await self._get_klines_callback(symbol, timeframe, limit)

        if self._transport:
            msg = {
                "action": "get_klines",
                "strategy_id": self._strategy_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "limit": limit,
            }
            return await self._transport.request("market_service", msg)

        return []

    async def get_indicators(
        self,
        symbol: str,
        indicator_name: str,
        timeframe: str = "1h",
    ) -> Optional[Dict[str, Any]]:
        """获取最新指标值

        Args:
            symbol: 交易对
            indicator_name: 指标名称（如 ma_7）
            timeframe: 周期

        Returns:
            指标值字典
        """
        if self._get_indicators_callback:
            return await self._get_indicators_callback(symbol, indicator_name, timeframe)

        if self._transport:
            msg = {
                "action": "get_indicators",
                "strategy_id": self._strategy_id,
                "symbol": symbol,
                "indicator_name": indicator_name,
                "timeframe": timeframe,
            }
            return await self._transport.request("market_service", msg)

        return None

    async def get_indicator_history(
        self,
        symbol: str,
        timeframe: str,
        indicator_name: str,
        params: Optional[Dict[str, Any]] = None,
        limit: int = 300,
    ) -> Optional[List[Dict[str, Any]]]:
        """获取指标历史序列（IND-106：后端引擎统一计算的有效序列）

        Args:
            symbol: 交易对
            timeframe: 周期
            indicator_name: 指标名（如 'MACD'）
            params: 参数组合（计算契约 key 的一部分）
            limit: 根数

        Returns:
            [{"timestamp": ms, "values": {...}}, ...]；未接通时 None
        """
        if self._get_indicator_history_callback:
            return await self._get_indicator_history_callback(
                symbol, timeframe, indicator_name, params, limit
            )

        if self._transport:
            msg = {
                "action": "get_indicator_history",
                "strategy_id": self._strategy_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "indicator_name": indicator_name,
                "params": params or {},
                "limit": limit,
            }
            return await self._transport.request("market_service", msg)

        return None

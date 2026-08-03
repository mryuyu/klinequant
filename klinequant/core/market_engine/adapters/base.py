"""ExchangeAdapter 抽象基类

所有交易所适配器的统一接口：
    - connect / disconnect：连接管理
    - subscribe_kline / subscribe_tick：订阅行情
    - fetch_klines：REST 历史 K 线
    - on_kline / on_tick：数据回调

遵循需求文档 §4.1 MKT-001。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable, Dict, List, Optional

from protocol.types import Kline, Tick


# 数据回调类型
KlineCallback = Callable[[Kline], Awaitable[None]]
TickCallback = Callable[[Tick], Awaitable[None]]


class ExchangeAdapter(ABC):
    """交易所适配器抽象基类"""

    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Args:
            name: 交易所名称，如 "binance"
            config: 交易所配置字典
        """
        self._name = name
        self._config = config
        self._connected = False
        self._kline_callbacks: List[KlineCallback] = []
        self._tick_callbacks: List[TickCallback] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ─── 连接管理 ───

    @abstractmethod
    async def connect(self) -> None:
        """建立与交易所的连接"""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """断开连接"""
        ...

    # ─── 订阅 ───

    @abstractmethod
    async def subscribe_kline(
        self,
        symbol: str,
        interval: str,
        callback: KlineCallback,
    ) -> None:
        """订阅 K 线数据

        Args:
            symbol: 交易对，如 "BTCUSDT"
            interval: K 线周期，如 "1m"
            callback: K 线数据回调
        """
        ...

    @abstractmethod
    async def subscribe_tick(
        self,
        symbol: str,
        callback: TickCallback,
    ) -> None:
        """订阅 Tick 数据

        Args:
            symbol: 交易对
            callback: Tick 数据回调
        """
        ...

    @abstractmethod
    async def unsubscribe(self, symbol: str, stream: str) -> None:
        """取消订阅

        Args:
            symbol: 交易对
            stream: 流类型，如 "kline_1m"
        """
        ...

    # ─── REST API ───

    @abstractmethod
    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Kline]:
        """REST API 获取历史 K 线

        Args:
            symbol: 交易对
            interval: K 线周期
            start_time: 开始时间（Unix ms）
            end_time: 结束时间（Unix ms）
            limit: 单次请求上限

        Returns:
            Kline 列表，按时间升序
        """
        ...

    @abstractmethod
    async def fetch_server_time(self) -> int:
        """获取交易所服务器时间（Unix ms）"""
        ...

    # ─── 内部方法 ───

    def register_kline_callback(self, callback: KlineCallback) -> None:
        """注册 K 线回调"""
        self._kline_callbacks.append(callback)

    def register_tick_callback(self, callback: TickCallback) -> None:
        """注册 Tick 回调"""
        self._tick_callbacks.append(callback)

    async def _dispatch_kline(self, kline: Kline) -> None:
        """分发 K 线到所有回调"""
        for cb in self._kline_callbacks:
            await cb(kline)

    async def _dispatch_tick(self, tick: Tick) -> None:
        """分发 Tick 到所有回调"""
        for cb in self._tick_callbacks:
            await cb(tick)

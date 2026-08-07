"""市场数据源插件统一接口

新增市场/交易所（外汇、股票、加密等）一律实现 MarketSource 抽象基类，
由 manager 注册路由，网关不硬编码具体交易所逻辑。

标准 bar dict 字段：
    timestamp / open / high / low / close / volume / event_ms
    （exchange 字段由 manager 广播时统一附加）
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class MarketSource(ABC):
    """市场数据源插件基类"""

    #: 交易所标识（小写），作为 REST/WS 的 exchange 维度值
    name: str = ""
    #: 展示标签（前端状态栏/交易所选择器用），如 "Binance Spot"
    label: str = ""
    #: 支持的 K 线周期集合（前端据此禁用不支持的周期按钮）
    supported_timeframes: set[str] = set()
    #: 是否提供成交量（外汇等 OTC 市场为 False，前端据此隐藏 VOL）
    supports_volume: bool = True
    #: 默认品种列表 [{symbol, name}]（前端品种列表初始化）
    default_symbols: list[dict[str, str]] = []
    #: 无订阅者时的默认监控集 [(symbol, timeframe)]，保持旧行为
    watched_targets: list[tuple[str, str]] = []

    @abstractmethod
    async def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 200,
        end_time: int | None = None,
    ) -> list[dict[str, Any]]:
        """历史 K 线（标准 bar dict，按时间升序）"""
        ...

    @abstractmethod
    async def stream_loop(self) -> None:
        """实时流主循环（永不主动返回）：自行处理建连/重连/降级，
        通过 manager.active_targets(self.name) 获取当前订阅集，
        产出 bar 后调 manager.publish_bar() 广播"""
        ...

    async def fetch_ticker(self, symbol: str) -> dict[str, Any] | None:
        """最新行情摘要（可选实现；默认返回 None 由前端兜底）"""
        return None

    def meta(self) -> dict[str, Any]:
        """插件元数据（/api/market/sources 下发给前端）"""
        return {
            "exchange": self.name,
            "label": self.label,
            "supports_volume": self.supports_volume,
            "timeframes": sorted(self.supported_timeframes),
            "default_symbols": self.default_symbols,
        }

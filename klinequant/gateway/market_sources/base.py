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


def price_decimals(values, cap: int = 8) -> int:
    """从订阅到的原始价格推导显示小数位（前端只渲染不推导）

    字符串价格去除尾部零后计小数位（交易所按 tick 补齐，去零后即时值）；
    数值型取十进制表示。返回批次内最大位数（上限 cap），无可推导值时返 0。
    """
    max_d = 0
    for v in values:
        s = v if isinstance(v, str) else repr(v) if isinstance(v, (int, float)) else None
        if not s:
            continue
        s = s.rstrip("0").rstrip(".")
        i = s.find(".")
        if i >= 0:
            max_d = max(max_d, min(cap, len(s) - i - 1))
    return max_d


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

    def _track_prec(self, symbol: str, values) -> None:
        """从订阅到的原始价格更新品种精度缓存（只增不减：新批次可能碰巧整数价）"""
        d = price_decimals(values)
        if not hasattr(self, "_price_prec"):
            self._price_prec = {}
        if d > self._price_prec.get(symbol.upper(), 0):
            self._price_prec[symbol.upper()] = d

    def price_precision(self, symbol: str) -> int:
        """品种价格显示精度（已订阅数据推导；未知时 0，前端自行保底）"""
        return getattr(self, "_price_prec", {}).get(symbol.upper(), 0)

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

    async def list_symbols(self) -> list[dict[str, str]]:
        """全量可交易品种目录 [{symbol, name, type}]（可选实现；默认返回 default_symbols）

        type 为资产类别（forex/metal/index/commodity/crypto/stock/bond），
        供前端品种搜索弹窗的资产分类筛选；插件从数据源读取并归类。
        """
        return [
            {"symbol": s["symbol"], "name": s.get("name", s["symbol"]), "type": s.get("type", "")}
            for s in self.default_symbols
        ]

    def meta(self) -> dict[str, Any]:
        """插件元数据（/api/market/sources 下发给前端）"""
        return {
            "exchange": self.name,
            "label": self.label,
            "supports_volume": self.supports_volume,
            "timeframes": sorted(self.supported_timeframes),
            "default_symbols": self.default_symbols,
        }

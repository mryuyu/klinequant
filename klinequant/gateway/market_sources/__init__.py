"""市场数据源插件包

统一接口（MarketSource）+ 注册路由（manager），交易所插件即插即用：
    - binance_source: 币安现货（WS 主链路 + REST 降级）
    - mt5_source: 本地 MetaTrader 5 终端（全量历史 + 终端轮询实时）
    - ths_source: 同花顺 A 股（thsdk 协议 SDK，快照轮询 + 本地 m1 聚合）

网关启动时调 manager.bootstrap_sources() 按 KQ_MARKET_SOURCES 注册启用插件。
"""
from gateway.market_sources.base import MarketSource
from gateway.market_sources.manager import bootstrap_sources, market_manager

__all__ = ["MarketSource", "market_manager", "bootstrap_sources"]

"""[已迁移] WebSocket K线实时推送

原币安 WS 主链路 + REST 降级逻辑已迁入插件框架：
    gateway/market_sources/binance_source.py（BinanceSource）
启动入口改为 gateway/app.py on_startup → market_sources.manager.start()

本模块仅保留 start_kline_broadcaster 空壳，防止历史导入报错；无实际作用。
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


async def start_kline_broadcaster() -> None:
    """兼容入口（空壳）：K 线广播已由 market_sources.manager 接管"""
    logger.warning(
        "ws_kline.start_kline_broadcaster is deprecated; "
        "kline broadcasting is handled by gateway.market_sources.manager"
    )

"""指标供给服务（IND-102）— 市场源插件 ↔ 指标引擎桥接

职责：
    - ensure_warmed：按计算契约 key=(指标名, 参数组合) 注册指标到引擎，
      并从市场源拉取历史 K 线预热（拉取深度 = 显示需求 + 预热根数）
    - on_bar：实时 bar 广播后驱动引擎增量计算，推送 WS 主题
      indicators.{exchange}.{symbol}.{tf}

历史深度规则（迭代计划 v2.1）：历史拉取量 = max(显示需求, warmup_max)，
REST 序列只含预热完成后的有效值（引擎侧剔除预热段）。
前端 K 线懒加载翻页后显示需求增大，本模块向后分页加深预热深度；
拉取统一走进程级 K 线缓存（kline_cache），与 REST 行情共享存量。
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Dict, Optional

import polars as pl

from gateway.market_sources.kline_cache import cached_klines
from gateway.state import state
from gateway.ws import ws_manager
from protocol.types import Kline

logger = logging.getLogger(__name__)

# 加深阈值（一页 1000 根：差距不足一页时不值得重拉）
_PAGE_FETCH_LIMIT = 1000
# 预热总深度上限（对齐前端 PRELOAD_HARD_CAP：默认全量加载后 K 线可达 3 万根）
_MAX_WARMUP_TOTAL = 30000
# 已预热但显示需求继续增长时的加深阈值：差距不足一页时不重拉（预热完成态不回退，
# 避免每次翻页都差几根触发全量重拉）；差距够大才按新目标重拉至数据源尽头/上限


def _bars_to_df(bars: list[dict]) -> pl.DataFrame:
    """市场源 bar dict 列表 → 引擎预热用 DataFrame"""
    return pl.DataFrame({
        "timestamp": [int(b["timestamp"]) for b in bars],
        "open": [float(b["open"]) for b in bars],
        "high": [float(b["high"]) for b in bars],
        "low": [float(b["low"]) for b in bars],
        "close": [float(b["close"]) for b in bars],
        "volume": [float(b.get("volume", 0.0)) for b in bars],
        "quote_volume": [0.0] * len(bars),
        "trade_count": [0] * len(bars),
        "is_closed": [bool(b.get("is_closed", True)) for b in bars],
    })



async def ensure_warmed(
    exchange: str,
    symbol: str,
    timeframe: str,
    name: str,
    params: Optional[Dict[str, Any]],
    need: int,
):
    """确保指标已注册并预热到显示需求深度，返回指标实例

    Args:
        need: 显示需求根数（前端 limit，随懒加载翻页增大）
    """
    engine = state.indicator_engine
    indicator = engine.ensure_indicator(name, params, symbol, exchange, timeframe)
    series_len = len(engine.get_series(name, params, symbol, exchange, timeframe))
    if series_len >= need:
        return indicator
    if indicator.is_warmed_up and need - series_len < _PAGE_FETCH_LIMIT:
        return indicator

    from gateway.market_sources.manager import market_manager
    source = market_manager.get(exchange)
    if source is None:
        logger.warning(f"Indicator warmup skipped: unknown market source {exchange}")
        return indicator

    target = min(_MAX_WARMUP_TOTAL, need + indicator.min_periods)
    try:
        # 走进程级 K 线缓存：与 REST 行情共享存量，同品种同周期重复预热零拉取；
        # 缺口由缓存层自动向前补页（派生周期同样适用）
        bars = await cached_klines(source, symbol, timeframe, target)
    except Exception as e:
        logger.error(f"Indicator warmup fetch failed [{exchange}] {symbol}/{timeframe}: {e}")
        return indicator
    if not bars:
        return indicator

    engine.warmup(symbol, exchange, timeframe, _bars_to_df(bars))
    return indicator


async def on_bar(exchange: str, symbol: str, timeframe: str, bar: dict) -> None:
    """实时 bar → 引擎增量计算 → WS 推送（publish_bar 内调用）

    仅当该 (symbol, exchange, timeframe) 存在已注册指标时才计算，
    无订阅指标的品种零开销跳过。
    """
    engine = state.indicator_engine
    if not engine.has_indicators(symbol, exchange, timeframe):
        return
    try:
        kline = Kline(
            symbol=symbol,
            exchange=exchange,
            timeframe=timeframe,
            timestamp=int(bar["timestamp"]),
            open=Decimal(str(bar["open"])),
            high=Decimal(str(bar["high"])),
            low=Decimal(str(bar["low"])),
            close=Decimal(str(bar["close"])),
            volume=Decimal(str(bar.get("volume", 0))),
            quote_volume=Decimal("0"),
            trade_count=0,
            is_closed=bool(bar.get("is_closed", False)),
        )
        updated = engine.update_kline(kline)
    except Exception as e:
        logger.debug(f"Indicator compute error [{exchange}] {symbol}/{timeframe}: {e}")
        return
    if not updated:
        return

    payload = [
        {
            "indicator": iv.indicator_name,
            "params": iv.params,
            "timestamp": iv.timestamp,
            "values": iv.values,
        }
        for iv in updated
    ]
    await ws_manager.publish(f"indicators.{exchange}.{symbol}.{timeframe}", payload)

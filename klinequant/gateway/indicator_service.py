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

from klinequant.gateway.market_sources import kline_cache
from klinequant.gateway.market_sources.kline_cache import cached_klines
from klinequant.gateway.state import state
from klinequant.gateway.ws import ws_manager
from klinequant.protocol.types import Kline

logger = logging.getLogger(__name__)

# 预热总深度上限（对齐前端 PRELOAD_HARD_CAP：默认全量加载后 K 线可达 3 万根）
_MAX_WARMUP_TOTAL = 30000


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



async def _missed_bars(engine, exchange: str, symbol: str, timeframe: str, tail: int = 2000) -> bool:
    """网关 K 线缓存已回填、但引擎增量缓存缺失的 bar（推送中断窗口漏 bar）⇒ 需强制重建

    漏 bar 永不回放进增量状态：窗口型字段（长周期 SMA/滚动 std）带洞长期偏离全量
    （2026-10-21 实证：MR_Y_MAIN 主图慢字段冻平「未计算」，副图递推型指数自收敛看似正常）；
    序列 ts 连续（逐 push upsert）、深度短路亦不可见，故按 ts 集合对拍检测。
    合法休市洞两缓存皆无，不计入。
    """
    eng_ts = engine.kline_cache_ts(symbol, exchange, timeframe, tail)
    if not eng_ts:
        return False
    from klinequant.gateway.market_sources.manager import market_manager
    source = market_manager.get(exchange)
    if source is None:
        return False
    # 只读 peek 网关内存缓存（零源请求：短路路径不能走 cached_klines，其尾刷/补拉即 fetch）
    gw_ts = kline_cache.peek_timestamps(source, symbol, timeframe, tail)
    if not gw_ts:
        return False
    eng_set = set(eng_ts)
    first, last = eng_ts[0], eng_ts[-1]
    # 只比对引擎缓存覆盖区间 [first, last)：左侧比网关浅属预热深度差（正常），
    # 区间内缺失才是中断窗口漏 bar；合法休市洞两缓存皆无不计入
    return any(first <= ts < last and ts not in eng_set for ts in gw_ts)


async def ensure_warmed(
    exchange: str,
    symbol: str,
    timeframe: str,
    name: str,
    params: Optional[Dict[str, Any]],
    need: int,
    force: bool = False,
):
    """确保指标已注册并预热到显示需求深度，返回指标实例

    Args:
        need: 显示需求根数（前端 limit，随懒加载翻页增大）
        force: 绕过深度短路强制重新预热重建序列。用于序列中段空洞自愈：
            序列曾按带洞 K 线缓存重建后空洞焊死（深度已够短路永不重建），
            前端检测到空洞且缺失 ts 已在 K 线中时带 refresh=1 请求重建。
    """
    engine = state.indicator_engine
    indicator = engine.ensure_indicator(name, params, symbol, exchange, timeframe)
    series_len = len(engine.get_series(name, params, symbol, exchange, timeframe))

    # 本次预热目标深度（受 _MAX_WARMUP_TOTAL 钳制）与由此可及的有效序列深度上限：
    # 重放 target 根后，窗口型指标剔除预热段的有效值约 target - min_periods + 1 根。
    # 未触顶时 target = need + min_periods → achievable = need + 1（>= need，短路判断与
    # 旧版 series_len >= need 完全等价）；但当 need 逼近 _MAX_WARMUP_TOTAL（前端满深度
    # 3 万根）时 target 被钳到上限，achievable 恒 < need，若仍按 series_len >= need 判断
    # 则短路永不命中、每次历史请求都对满深度全量重放（深历史卡顿）。故按“本次预热可及
    # 深度”封顶判断：序列已达可及深度即免重算（再拉同样深度，幂等无增益）。
    target = min(_MAX_WARMUP_TOTAL, need + indicator.min_periods)
    achievable = max(0, target - indicator.min_periods + 1)
    if not force and series_len >= min(need, achievable):
        # 漏 bar 自愈：深度短路前对拍网关/引擎缓存 ts 集合，中断窗口漏 bar 已回填网关但
        # 未回放进增量状态时强制重建（否则长窗口字段偏离焊死，无任何触发器自愈）
        force = await _missed_bars(engine, exchange, symbol, timeframe)
        if not force:
            return indicator   # 有效序列已覆盖显示需求（或已达预热上限可及深度），免重算
    # series_len < 可及深度：左侧存在未覆盖的历史缺口，必须加深预热补齐。此前这里有一条
    # `is_warmed_up and need - series_len < _PAGE_FETCH_LIMIT(1000)` 的迟滞直接跳过，会把
    # 一次瞬时短拉（补页异常 break / 短页）留下的 <1000 根缺口永久锁死——阶梯线等
    # min_periods 较大的指标历史段再也不绘制（2026-09-23 实证：刷新后前端 klines=6000
    # 而 series 恒为 5258 = 缓存 6026 − 预热 768，diff 742<1000 每次请求都被跳过）。
    # cached_klines 命中存量时开销极小、源确已到头时 warmup 幂等无副作用，故只要还有
    # 缺口就按 need+min_periods 重拉，不再设迟滞（实时 bar 走 on_bar 增量，不经此路径）。

    from klinequant.gateway.market_sources.manager import market_manager
    source = market_manager.get(exchange)
    if source is None:
        logger.warning(f"Indicator warmup skipped: unknown market source {exchange}")
        return indicator

    try:
        # 走进程级 K 线缓存：与 REST 行情共享存量，同品种同周期重复预热零拉取；
        # 缺口由缓存层自动向前补页（派生周期同样适用）
        bars = await cached_klines(source, symbol, timeframe, target)
    except Exception as e:
        logger.error(f"Indicator warmup fetch failed [{exchange}] {symbol}/{timeframe}: {e}")
        return indicator
    if not bars:
        return indicator

    # 源深度不足（返回根数 < 请求 target）→ 强制逐字段部分输出：浅源（A 股 60m 等）
    # 若走非 partial 门控，预热段会吃掉几乎全部显示窗口，指标只显示最新一小段
    force_partial = len(bars) < target
    engine.warmup(
        symbol, exchange, timeframe, _bars_to_df(bars),
        only_key=engine.ind_key(name, params),   # 定向预热：仅重放本次请求的指标，
        # 避免 M 个订阅请求 × M 个已注册指标 = O(M²) 全量重放（切品种/周期预热极慢）
        force_partial=force_partial,
    )
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

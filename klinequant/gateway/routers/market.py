"""行情路由

API：
    GET /api/market/klines  — K 线数据（按 exchange 路由到市场源插件）
    GET /api/market/symbols — 全量品种目录（数据源/资产类别维度，品种搜索用）
    GET /api/market/ticker  — 最新行情
    GET /api/market/depth   — 盘口深度（仅 binance 支持）
    GET /api/market/sources — 已注册市场源插件元数据（前端交易所选择器）

exchange 缺省时路由到默认市场源（binance），保持旧前端兼容。
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx
from fastapi import APIRouter, Query

from gateway.market_sources.binance_source import BINANCE_REST_BASE, HTTP_PROXY
from gateway.market_sources.derived import is_derived
from gateway.market_sources.kline_cache import cached_klines
from gateway.market_sources.manager import market_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/market", tags=["market"])


def _resolve_source(exchange: Optional[str]):
    """exchange 参数 → 市场源插件；未指定时用默认所"""
    ex = (exchange or "").strip().lower()
    source = market_manager.get(ex) if ex else market_manager.get(market_manager.default_exchange())
    return ex or market_manager.default_exchange(), source


@router.get("/sources")
async def get_sources():
    """已注册市场源插件元数据（前端渲染交易所选择器/默认品种/周期支持）"""
    return {"sources": [s.meta() for s in market_manager.list_sources()]}


@router.get("/klines")
async def get_klines(
    symbol: str = Query("BTCUSDT", description="交易对，如 BTCUSDT / EURUSD"),
    timeframe: str = Query("1h", description="K线周期"),
    limit: int = Query(200, ge=1, le=30000, description="数量（上限对齐前端 PRELOAD_HARD_CAP；各源插件内部分页支撑大页请求）"),
    start_time: Optional[int] = Query(None, description="起始时间戳(ms)"),
    end_time: Optional[int] = Query(None, description="结束时间戳(ms)"),
    exchange: Optional[str] = Query(None, description="市场源（binance/ig），缺省默认所"),
):
    """获取 K 线数据（路由到对应市场源插件）"""
    ex, source = _resolve_source(exchange)
    if source is None:
        logger.error(f"Unknown market source: {exchange}")
        return {"symbol": symbol, "timeframe": timeframe, "exchange": ex, "count": 0, "data": []}
    # 派生档位（1M/1Q/1Y/自定义倍率）由网关从日 K 聚合，不走源原生支持表；
    # 1w 各源原生直供。翻页（end_time）语义不变，前端懒加载/预载零改动。
    derived = is_derived(timeframe)
    if not derived and timeframe not in source.supported_timeframes:
        logger.warning(f"[{ex}] unsupported timeframe: {timeframe}")
        return {"symbol": symbol, "timeframe": timeframe, "exchange": ex, "count": 0, "data": []}

    try:
        # 进程级缓存：同品种同周期来回切换免重复拉取/聚合（原生与派生同层，含尾部未收盘 bar 刷新）
        data = await cached_klines(
            source, symbol, timeframe, limit=limit, end_time=end_time or None
        )
        # start_time 过滤（插件接口统一用 end_time 翻页，起始过滤在网关侧做）
        if start_time:
            data = [k for k in data if k["timestamp"] >= start_time]
    except Exception as e:
        logger.error(f"Failed to fetch klines from {ex}: {e}")
        return {"symbol": symbol, "timeframe": timeframe, "exchange": ex, "count": 0, "data": []}

    # 剔除插件内部字段，保持前端契约
    out = [
        {
            "timestamp": k["timestamp"], "open": k["open"], "high": k["high"],
            "low": k["low"], "close": k["close"], "volume": k["volume"],
        }
        for k in data
    ]
    return {
        "symbol": symbol.upper(),
        "timeframe": timeframe,
        "exchange": ex,
        "count": len(out),
        "data": out,
        # 品种价格显示精度（市场源从订阅到的价格推导，前端只渲染不推导）
        "price_precision": source.price_precision(symbol),
    }


@router.get("/symbols")
async def get_symbols(
    exchange: Optional[str] = Query(None, description="市场源，缺省返回全部已注册所"),
):
    """全量可交易品种目录（带数据源/资产类别维度，前端品种搜索弹窗用）

    每行含 exchange（内部路由标识）/ source（数据源展示名）/ type（资产类别）/ region（国内/国外）/
    code（面向用户的展示码，A 股为 6 位纯数字，缺省同 symbol），
    同名品种来自不同数据源时为多行（如 EURUSD 同时存在于 IC Markets 与 IG）。
    拉取失败回退到插件默认品种，保证弹窗永远有结果。
    """
    sources = (
        [s for s in [market_manager.get(exchange)] if s]
        if exchange else market_manager.list_sources()
    )
    symbols = []
    for s in sources:
        try:
            rows = await market_manager.list_symbols(s.name) or []
        except Exception as e:
            logger.error(f"Failed to list symbols from {s.name}: {e}")
            rows = []
        if not rows:
            rows = [
                {
                    "symbol": i["symbol"], "name": i.get("name", i["symbol"]),
                    "type": i.get("type", ""), "code": i.get("code", i["symbol"]),
                }
                for i in s.default_symbols
            ]
        for item in rows:
            symbols.append({
                "exchange": s.name,
                "source": s.label,
                "region": s.region,
                "symbol": item["symbol"],
                "name": item.get("name", item["symbol"]),
                "type": item.get("type", ""),
                "code": item.get("code", item["symbol"]),
            })
    return {"symbols": symbols}


@router.get("/ticker")
async def get_ticker(
    symbol: str = Query("BTCUSDT", description="交易对"),
    exchange: Optional[str] = Query(None, description="市场源，缺省默认所"),
):
    """获取最新行情（路由到对应市场源插件）"""
    ex, source = _resolve_source(exchange)
    if source is None:
        return {"symbol": symbol, "last_price": 0, "bid": 0, "ask": 0, "volume_24h": 0}
    try:
        t = await source.fetch_ticker(symbol)
        if t:
            t["exchange"] = ex
            t["price_precision"] = source.price_precision(symbol)
            return t
    except Exception as e:
        logger.error(f"Failed to fetch ticker from {ex}: {e}")
    return {"symbol": symbol, "exchange": ex, "last_price": 0, "bid": 0, "ask": 0, "volume_24h": 0}


@router.get("/depth")
async def get_depth(
    symbol: str = Query("BTCUSDT", description="交易对"),
    limit: int = Query(5, ge=5, le=20, description="档数"),
):
    """获取盘口深度（仅 binance 支持，其余市场源返回空档）"""
    source = market_manager.get("binance")
    if source is None:
        return {"bids": [], "asks": []}

    try:
        async with httpx.AsyncClient(proxy=HTTP_PROXY or None, timeout=10.0) as client:
            resp = await client.get(
                f"{BINANCE_REST_BASE}/api/v3/depth",
                params={"symbol": symbol.upper(), "limit": limit},
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch depth: {e}")
        return {"bids": [], "asks": []}

    return {
        "symbol": symbol.upper(),
        "bids": [{"price": float(p), "qty": float(q)} for p, q in data.get("bids", [])],
        "asks": [{"price": float(p), "qty": float(q)} for p, q in data.get("asks", [])],
    }

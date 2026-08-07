"""行情路由

API：
    GET /api/market/klines  — K 线数据（按 exchange 路由到市场源插件）
    GET /api/market/symbols — 交易对列表（含 exchange 维度）
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
    limit: int = Query(200, ge=1, le=1000, description="数量"),
    start_time: Optional[int] = Query(None, description="起始时间戳(ms)"),
    end_time: Optional[int] = Query(None, description="结束时间戳(ms)"),
    exchange: Optional[str] = Query(None, description="市场源（binance/ig），缺省默认所"),
):
    """获取 K 线数据（路由到对应市场源插件）"""
    ex, source = _resolve_source(exchange)
    if source is None:
        logger.error(f"Unknown market source: {exchange}")
        return {"symbol": symbol, "timeframe": timeframe, "exchange": ex, "count": 0, "data": []}
    if timeframe not in source.supported_timeframes:
        logger.warning(f"[{ex}] unsupported timeframe: {timeframe}")
        return {"symbol": symbol, "timeframe": timeframe, "exchange": ex, "count": 0, "data": []}

    try:
        data = await source.fetch_klines(
            symbol, timeframe, limit=limit, end_time=end_time or None
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
    }


@router.get("/symbols")
async def get_symbols(
    exchange: Optional[str] = Query(None, description="市场源，缺省返回全部已注册所"),
):
    """获取可用交易对列表（带 exchange 维度）"""
    sources = (
        [s for s in [market_manager.get(exchange)] if s]
        if exchange else market_manager.list_sources()
    )
    symbols = []
    for s in sources:
        for item in s.default_symbols:
            symbols.append({
                "exchange": s.name,
                "symbol": item["symbol"],
                "name": item.get("name", item["symbol"]),
                "base": item["symbol"][:3],
                "quote": item["symbol"][3:] if len(item["symbol"]) > 3 else "",
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

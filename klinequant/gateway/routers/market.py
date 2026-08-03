"""行情路由

API：
    GET /api/market/klines — 获取 K 线数据（实时从币安拉取）
    GET /api/market/symbols — 获取交易对列表
    GET /api/market/ticker — 获取最新行情

遵循需求文档 §4.7 GW-003。
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import httpx
from fastapi import APIRouter, Query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/market", tags=["market"])

# 币安 REST API 基址（公开行情无需认证）
BINANCE_REST_BASE = os.getenv("BINANCE_REST_BASE", "https://api.binance.com")
HTTP_PROXY = os.getenv("HTTP_PROXY", "http://127.0.0.1:7897")

# 周期映射
TIMEFRAME_MAP = {
    "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h", "12h": "12h",
    "1d": "1d", "3d": "3d", "1w": "1w",
}


def _get_client() -> httpx.AsyncClient:
    """创建带代理的 HTTP 客户端"""
    return httpx.AsyncClient(
        proxy=HTTP_PROXY if HTTP_PROXY else None,
        timeout=10.0,
    )


@router.get("/klines")
async def get_klines(
    symbol: str = Query("BTCUSDT", description="交易对，如 BTCUSDT"),
    timeframe: str = Query("1h", description="K线周期"),
    limit: int = Query(200, ge=1, le=1000, description="数量"),
    start_time: Optional[int] = Query(None, description="起始时间戳(ms)"),
    end_time: Optional[int] = Query(None, description="结束时间戳(ms)"),
):
    """获取 K 线数据（实时从币安拉取）"""
    interval = TIMEFRAME_MAP.get(timeframe, "1h")
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": limit,
    }
    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time

    try:
        async with _get_client() as client:
            resp = await client.get(f"{BINANCE_REST_BASE}/api/v3/klines", params=params)
            resp.raise_for_status()
            raw = resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch klines from Binance: {e}")
        return {"symbol": symbol, "timeframe": timeframe, "count": 0, "data": []}

    # 转换为前端格式
    data = []
    for k in raw:
        data.append({
            "timestamp": int(k[0]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
        })

    return {
        "symbol": symbol.upper(),
        "timeframe": timeframe,
        "count": len(data),
        "data": data,
    }


@router.get("/symbols")
async def get_symbols():
    """获取可用交易对列表"""
    return {
        "symbols": [
            {"symbol": "BTCUSDT", "base": "BTC", "quote": "USDT"},
            {"symbol": "ETHUSDT", "base": "ETH", "quote": "USDT"},
            {"symbol": "BNBUSDT", "base": "BNB", "quote": "USDT"},
            {"symbol": "SOLUSDT", "base": "SOL", "quote": "USDT"},
            {"symbol": "XRPUSDT", "base": "XRP", "quote": "USDT"},
            {"symbol": "DOGEUSDT", "base": "DOGE", "quote": "USDT"},
        ]
    }


@router.get("/ticker")
async def get_ticker(
    symbol: str = Query("BTCUSDT", description="交易对"),
):
    """获取最新行情"""
    try:
        async with _get_client() as client:
            resp = await client.get(
                f"{BINANCE_REST_BASE}/api/v3/ticker/24hr",
                params={"symbol": symbol.upper()},
            )
            resp.raise_for_status()
            t = resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch ticker: {e}")
        return {"symbol": symbol, "last_price": 0, "bid": 0, "ask": 0, "volume_24h": 0}

    return {
        "symbol": symbol.upper(),
        "last_price": float(t["lastPrice"]),
        "bid": float(t["bidPrice"]),
        "ask": float(t["askPrice"]),
        "volume_24h": float(t["volume"]),
        "price_change_pct": float(t["priceChangePercent"]),
        "high_24h": float(t["highPrice"]),
        "low_24h": float(t["lowPrice"]),
    }


@router.get("/depth")
async def get_depth(
    symbol: str = Query("BTCUSDT", description="交易对"),
    limit: int = Query(5, ge=5, le=20, description="档数"),
):
    """获取盘口深度（买卖 N 档）"""
    try:
        async with _get_client() as client:
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

"""WebSocket K线实时推送

后台任务：定时从币安 REST API 拉取最新 K 线，
通过 ws_manager 广播给所有订阅了 klines.{SYMBOL}.{TIMEFRAME} 主题的客户端。

轮询间隔 500ms，多交易对并发请求。
降级策略：如果币安 WS 不可用，使用 REST 轮询。
"""
from __future__ import annotations

import asyncio
import logging
import os

import httpx

from gateway.ws import ws_manager

logger = logging.getLogger(__name__)

BINANCE_REST_BASE = os.getenv("BINANCE_REST_BASE", "https://api.binance.com")
HTTP_PROXY = os.getenv("HTTP_PROXY", "http://127.0.0.1:7897")

# 默认监控的交易对
WATCHED_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
DEFAULT_TIMEFRAME = "1h"
POLL_INTERVAL = 0.5  # 秒（500ms）

# 有效周期集合
VALID_TIMEFRAMES = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "3d", "1w"}


def _parse_topic(topic: str) -> tuple[str, str] | None:
    """解析主题 klines.BTCUSDT.1h -> (BTCUSDT, 1h)"""
    parts = topic.split(".")
    if len(parts) == 3 and parts[0] == "klines":
        symbol, tf = parts[1], parts[2]
        if tf in VALID_TIMEFRAMES:
            return symbol, tf
    # 兼容旧格式 klines.BTCUSDT（默认 1m）
    if len(parts) == 2 and parts[0] == "klines":
        return parts[1], "1m"
    return None


async def _fetch_and_publish(
    client: httpx.AsyncClient,
    symbol: str,
    timeframe: str,
    last_bar: dict[str, str],
) -> None:
    """拉取单个交易对的最新 K 线并推送（供并发调用）"""
    try:
        resp = await client.get(
            f"{BINANCE_REST_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": timeframe, "limit": 1},
        )
        if resp.status_code != 200:
            return
        raw = resp.json()
        if not raw:
            return

        k = raw[0]
        ts = int(k[0])
        bar = {
            "timestamp": ts,
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
        }

        # 去重：timestamp + close + high + low 均相同则跳过
        cache_key = f"{symbol}_{timeframe}"
        bar_sig = f"{ts}_{k[2]}_{k[3]}_{k[4]}"
        if last_bar.get(cache_key) == bar_sig:
            return
        last_bar[cache_key] = bar_sig

        # 推送到精确主题
        await ws_manager.publish(f"klines.{symbol}.{timeframe}", bar)
        # 兼容旧格式订阅
        await ws_manager.publish(f"klines.{symbol}", bar)

    except Exception as e:
        logger.debug(f"Kline fetch error for {symbol}/{timeframe}: {e}")


async def start_kline_broadcaster():
    """启动 K 线广播后台任务（500ms 轮询，并发请求）"""
    logger.info("Kline broadcaster started (REST polling mode, interval=500ms)")
    # 缓存上次推送的 bar 签名，避免重复推送
    last_bar: dict[str, str] = {}

    async with httpx.AsyncClient(
        proxy=HTTP_PROXY if HTTP_PROXY else None,
        timeout=5.0,
    ) as client:
        while True:
            try:
                # 获取当前有订阅者的主题
                active_topics = set(ws_manager._subscriptions.keys())
                kline_topics = [t for t in active_topics if t.startswith("klines.")]

                # 解析需要拉取的 (symbol, timeframe) 对
                fetch_targets: set[tuple[str, str]] = set()
                for topic in kline_topics:
                    parsed = _parse_topic(topic)
                    if parsed:
                        fetch_targets.add(parsed)

                # 如果没有订阅者，拉取默认交易对
                if not fetch_targets:
                    for sym in WATCHED_SYMBOLS:
                        fetch_targets.add((sym, DEFAULT_TIMEFRAME))

                # 并发请求所有交易对
                tasks = [
                    _fetch_and_publish(client, symbol, timeframe, last_bar)
                    for symbol, timeframe in fetch_targets
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

            except Exception as e:
                logger.error(f"Kline broadcaster error: {e}")

            await asyncio.sleep(POLL_INTERVAL)

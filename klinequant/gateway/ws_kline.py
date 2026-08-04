"""WebSocket K线实时推送

主链路：订阅币安原生 WebSocket kline 流（~250ms 级实时更新），
通过 ws_manager 广播给所有订阅了 klines.{SYMBOL}.{TIMEFRAME} 主题的客户端。
降级策略：币安 WS 断开/静默时自动回退 REST 轮询，重连成功后恢复 WS 主链路。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time

import httpx

from gateway.ws import ws_manager

logger = logging.getLogger(__name__)
# 诊断用：gateway 主日志未配置 stdlib logging handler，额外写一份文件日志
if not logger.handlers:
    try:
        _fh = logging.FileHandler(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "ws_kline.log"),
            encoding="utf-8",
        )
        _fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(_fh)
        logger.setLevel(logging.INFO)
    except Exception:
        pass

BINANCE_REST_BASE = os.getenv("BINANCE_REST_BASE", "https://api.binance.com")
# 币安 WS 流基址：必须使用公共行情流（支持 /stream?streams= 组合路径）。
# 注意：demo-stream.binance.com 是模拟交易私有流，无组合行情路径（会 404），
# 因此此处默认不读环境变量；确需覆盖时用 KQ_MARKET_WS_BASE。
BINANCE_WS_BASE = os.getenv("KQ_MARKET_WS_BASE", "wss://stream.binance.com:9443")
HTTP_PROXY = os.getenv("HTTP_PROXY", "http://127.0.0.1:7897")

# 降级 REST 轮询参数
WATCHED_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
DEFAULT_TIMEFRAME = "1h"
POLL_INTERVAL = 2.0  # 秒（降级模式，避免 REST 限频）

# WS 链路健康判定：超过该秒数未收到任何币安消息 → 视为断流，触发重连
WS_SILENT_TIMEOUT = 15.0
WS_CONNECT_TIMEOUT = 10.0   # 建连超时（代理丢弃 SYN 时 connect 会永久挂起，必须兜底）
WS_RECONNECT_BASE = 1.0
WS_RECONNECT_MAX = 30.0

# 有效周期集合
VALID_TIMEFRAMES = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "3d", "1w"}

try:
    import websockets
    _HAS_WEBSOCKETS = True
except ImportError:  # pragma: no cover
    _HAS_WEBSOCKETS = False


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


def _active_fetch_targets() -> set[tuple[str, str]]:
    """当前有订阅者的 (symbol, timeframe) 集合；无订阅者时用默认监控集"""
    fetch_targets: set[tuple[str, str]] = set()
    for topic in list(ws_manager._subscriptions.keys()):
        if topic.startswith("klines."):
            parsed = _parse_topic(topic)
            if parsed:
                fetch_targets.add(parsed)
    if not fetch_targets:
        for sym in WATCHED_SYMBOLS:
            fetch_targets.add((sym, DEFAULT_TIMEFRAME))
    return fetch_targets


async def _publish_bar(
    symbol: str, timeframe: str, ts: int,
    open_: float, high: float, low: float, close: float, volume: float,
    sig: str, last_bar: dict[str, str], event_ms: int = 0,
) -> bool:
    """去重后广播一根 bar（WS 主链路与 REST 降级共用），返回是否实际广播

    event_ms: 行情事件时间（币安 kline 事件 E 字段，REST 降级用本机时间），
    供前端计算端到端实时延迟。
    """
    cache_key = f"{symbol}_{timeframe}"
    if last_bar.get(cache_key) == sig:
        return False
    last_bar[cache_key] = sig
    bar = {
        "timestamp": ts,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "event_ms": event_ms,
    }
    await ws_manager.publish(f"klines.{symbol}.{timeframe}", bar)
    # 兼容旧格式订阅
    await ws_manager.publish(f"klines.{symbol}", bar)
    return True


# ─── 币安 WS 主链路 ───

# 币安 WS 主链路健康状态（最近收到消息的单调时间）
_ws_last_event_at: float = 0.0


def _ws_healthy() -> bool:
    """主链路在 WS_SILENT_TIMEOUT 内有过消息 → 健康（REST 降级让路）"""
    return _HAS_WEBSOCKETS and (time.monotonic() - _ws_last_event_at) < WS_SILENT_TIMEOUT


async def _ws_loop(last_bar: dict[str, str]) -> None:
    """币安原生 WS 接收主循环：按当前订阅动态拼流地址，静默超时/断线自动重连"""
    global _ws_last_event_at
    backoff = WS_RECONNECT_BASE
    while True:
        targets = _active_fetch_targets()
        streams = "/".join(f"{s.lower()}@kline_{tf}" for s, tf in sorted(targets))
        # 单个流用直连路径 /ws/<stream>，多个流用组合路径 /stream?streams=a/b
        if len(targets) == 1:
            url = f"{BINANCE_WS_BASE}/ws/{streams}"
        else:
            url = f"{BINANCE_WS_BASE}/stream?streams={streams}"
        logger.info(f"Binance WS connecting: {url}")
        connect_kwargs = {"ping_interval": 20, "ping_timeout": 10, "close_timeout": 5}
        if HTTP_PROXY:
            connect_kwargs["proxy"] = HTTP_PROXY

        try:
            # 建连超时保护：代理异常时 connect 可能永久挂起，超时后走重连退避
            conn = await asyncio.wait_for(
                websockets.connect(url, **connect_kwargs), timeout=WS_CONNECT_TIMEOUT
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Binance WS connect failed: {e}, retrying...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WS_RECONNECT_MAX)
            continue

        try:
            async with conn:
                logger.info(f"Binance WS stream connected: {len(targets)} streams")
                backoff = WS_RECONNECT_BASE
                connected_targets = targets
                _ws_last_event_at = time.monotonic()   # 静默判定的宽限起点，避免首条消息前误判断流
                _rx_count = 0          # 本轮连接收到的原始消息数（诊断）
                _pub_count = 0         # 本轮连接实际广播的 bar 数（诊断）
                _last_stat_at = time.monotonic()
                while True:
                    try:
                        raw = await asyncio.wait_for(conn.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        raw = None

                    # 周期性诊断/控制面检查（无论是否有消息都执行）
                    now = time.monotonic()
                    if now - _last_stat_at >= 10:
                        logger.info(f"Binance WS stats: rx={_rx_count} pub={_pub_count} targets={len(connected_targets)}")
                        _rx_count = 0; _pub_count = 0; _last_stat_at = now
                    # 静默超时 → 断流重连；订阅集合变化 → 重连以纳入新主题
                    if now - _ws_last_event_at > WS_SILENT_TIMEOUT:
                        logger.warning("Binance WS silent timeout, reconnecting...")
                        break
                    if _active_fetch_targets() != connected_targets:
                        logger.info("Subscription set changed, reconnecting WS streams...")
                        break
                    if raw is None:
                        continue

                    _rx_count += 1
                    _ws_last_event_at = now
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    # 组合流消息带 data 包装，单流直连无包装，两种都兼容
                    payload = msg.get("data") or msg
                    k = payload.get("k") if isinstance(payload, dict) else None
                    if not k:
                        continue
                    symbol = k.get("s", "")
                    tf = k.get("i", "")
                    if not symbol or tf not in VALID_TIMEFRAMES:
                        continue
                    ts = int(k.get("t", 0))
                    sig = f"{ts}_{k.get('h')}_{k.get('l')}_{k.get('c')}"
                    published = await _publish_bar(
                        symbol, tf, ts,
                        float(k.get("o", 0)), float(k.get("h", 0)),
                        float(k.get("l", 0)), float(k.get("c", 0)),
                        float(k.get("v", 0)), sig, last_bar,
                        event_ms=int(payload.get("E", 0)),
                    )
                    if published:
                        _pub_count += 1
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Binance WS error: {e}, fallback to REST polling")

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, WS_RECONNECT_MAX)


# ─── REST 降级轮询 ───

async def _fetch_and_publish(
    client: httpx.AsyncClient,
    symbol: str,
    timeframe: str,
    last_bar: dict[str, str],
) -> None:
    """拉取单个交易对的最新 K 线并推送（降级模式）"""
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
        sig = f"{ts}_{k[2]}_{k[3]}_{k[4]}"
        await _publish_bar(
            symbol, timeframe, ts,
            float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]),
            sig, last_bar,
            event_ms=int(time.time() * 1000),
        )
    except Exception as e:
        logger.debug(f"Kline fetch error for {symbol}/{timeframe}: {e}")


async def _rest_poll_loop(last_bar: dict[str, str]) -> None:
    """REST 轮询协程：仅在币安 WS 主循环未存活期间实际推送（通过健康标记门控）"""
    async with httpx.AsyncClient(
        proxy=HTTP_PROXY if HTTP_PROXY else None,
        timeout=5.0,
    ) as client:
        while True:
            try:
                if not _ws_healthy():
                    tasks = [
                        _fetch_and_publish(client, symbol, timeframe, last_bar)
                        for symbol, timeframe in _active_fetch_targets()
                    ]
                    await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                logger.error(f"Kline REST poll error: {e}")
            await asyncio.sleep(POLL_INTERVAL)


async def start_kline_broadcaster():
    """启动 K 线广播后台任务：币安 WS 主链路 + REST 轮询降级"""
    last_bar: dict[str, str] = {}
    if _HAS_WEBSOCKETS:
        logger.info("Kline broadcaster started (Binance WS primary, REST fallback)")
        asyncio.create_task(_ws_loop(last_bar))
    else:
        logger.warning("websockets 库不可用，K 线广播降级为纯 REST 轮询")
    await _rest_poll_loop(last_bar)

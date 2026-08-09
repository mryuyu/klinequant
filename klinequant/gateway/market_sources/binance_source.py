"""Binance 现货市场源插件

由 gateway/ws_kline.py 迁入插件框架（行为保持不变）：
    - 主链路：币安原生 WS kline 流（~250ms 级实时更新）
    - 降级：WS 断开/静默时 REST 轮询，重连成功后恢复主链路
    - 订阅集合变化检测/静默判定起点重置等修复随迁
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time

import httpx

from gateway.market_sources.base import MarketSource
from gateway.market_sources.manager import market_manager

logger = logging.getLogger(__name__)
# 诊断用：gateway 主日志未配置 stdlib logging handler，额外写一份文件日志
if not logger.handlers:
    try:
        _fh = logging.FileHandler(
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "logs", "ws_kline.log"),
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

POLL_INTERVAL = 2.0  # 秒（降级模式，避免 REST 限频）

# WS 链路健康判定：超过该秒数未收到任何币安消息 → 视为断流，触发重连
WS_SILENT_TIMEOUT = 15.0
WS_CONNECT_TIMEOUT = 10.0   # 建连超时（代理丢弃 SYN 时 connect 会永久挂起，必须兜底）
WS_RECONNECT_BASE = 1.0
WS_RECONNECT_MAX = 30.0

VALID_TIMEFRAMES = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "3d", "1w"}

try:
    import websockets
    _HAS_WEBSOCKETS = True
except ImportError:  # pragma: no cover
    _HAS_WEBSOCKETS = False


class BinanceSource(MarketSource):
    """币安现货：WS 主链路 + REST 降级"""

    name = "binance"
    label = "Binance Spot"
    supported_timeframes = set(VALID_TIMEFRAMES)
    supports_volume = True
    default_symbols = [
        {"symbol": "BTCUSDT", "name": "BTC/USDT"},
        {"symbol": "ETHUSDT", "name": "ETH/USDT"},
        {"symbol": "BNBUSDT", "name": "BNB/USDT"},
        {"symbol": "SOLUSDT", "name": "SOL/USDT"},
        {"symbol": "XRPUSDT", "name": "XRP/USDT"},
        {"symbol": "DOGEUSDT", "name": "DOGE/USDT"},
    ]
    watched_targets = [("BTCUSDT", "1h"), ("ETHUSDT", "1h")]

    # ─── REST 历史 K 线 ───

    async def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 200,
        end_time: int | None = None,
    ) -> list[dict]:
        params: dict = {"symbol": symbol.upper(), "interval": timeframe, "limit": limit}
        if end_time:
            params["endTime"] = end_time
        async with httpx.AsyncClient(proxy=HTTP_PROXY or None, timeout=10.0) as client:
            resp = await client.get(f"{BINANCE_REST_BASE}/api/v3/klines", params=params)
            resp.raise_for_status()
            raw = resp.json()
        # 从订阅到的原始字符串价格推导品种显示精度（随 klines 响应下发，前端只渲染）
        self._track_prec(
            symbol,
            [p for k in raw for p in (k[1], k[2], k[3], k[4])],
        )
        return [
            {
                "timestamp": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "event_ms": 0,
            }
            for k in raw
        ]

    async def fetch_ticker(self, symbol: str) -> dict | None:
        async with httpx.AsyncClient(proxy=HTTP_PROXY or None, timeout=10.0) as client:
            resp = await client.get(
                f"{BINANCE_REST_BASE}/api/v3/ticker/24hr",
                params={"symbol": symbol.upper()},
            )
            resp.raise_for_status()
            t = resp.json()
        self._track_prec(
            symbol.upper(),
            [t["lastPrice"], t["bidPrice"], t["askPrice"], t["highPrice"], t["lowPrice"]],
        )
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

    # ─── 实时流主循环（WS 主链路 + REST 降级并行） ───

    async def stream_loop(self) -> None:
        last_event_at = time.monotonic()  # 静默判定的宽限起点，避免首条消息前误判断流

        async def ws_healthy() -> bool:
            return _HAS_WEBSOCKETS and (time.monotonic() - last_event_at) < WS_SILENT_TIMEOUT

        async def ws_main() -> None:
            """币安原生 WS 接收主循环：按当前订阅动态拼流地址，静默超时/断线自动重连"""
            nonlocal last_event_at
            backoff = WS_RECONNECT_BASE
            while True:
                targets = market_manager.active_targets(self.name)
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
                        last_event_at = time.monotonic()
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
                            # 静默超时 → 断流重连；订阅集合变化 → 交由 manager 重启本循环
                            if now - last_event_at > WS_SILENT_TIMEOUT:
                                logger.warning("Binance WS silent timeout, reconnecting...")
                                break
                            if market_manager.active_targets(self.name) != connected_targets:
                                logger.info("Subscription set changed, reconnecting WS streams...")
                                break
                            if raw is None:
                                continue

                            _rx_count += 1
                            last_event_at = now
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
                            # 实时 bar 同步累积品种精度（WS 主链路）
                            self._track_prec(symbol, [k.get("o"), k.get("h"), k.get("l"), k.get("c")])
                            published = await market_manager.publish_bar(
                                self.name, symbol, tf,
                                {
                                    "timestamp": ts,
                                    "open": float(k.get("o", 0)),
                                    "high": float(k.get("h", 0)),
                                    "low": float(k.get("l", 0)),
                                    "close": float(k.get("c", 0)),
                                    "volume": float(k.get("v", 0)),
                                    "event_ms": int(payload.get("E", 0)),
                                    "is_closed": bool(k.get("x", False)),
                                },
                            )
                            if published:
                                _pub_count += 1
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(f"Binance WS error: {e}, fallback to REST polling")

                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, WS_RECONNECT_MAX)

        async def rest_poll() -> None:
            """REST 轮询协程：仅在 WS 主循环未存活期间实际推送（健康标记门控）"""
            async with httpx.AsyncClient(proxy=HTTP_PROXY or None, timeout=5.0) as client:
                while True:
                    try:
                        if not await ws_healthy():
                            tasks = [
                                self._fetch_and_publish(client, symbol, timeframe)
                                for symbol, timeframe in market_manager.active_targets(self.name)
                            ]
                            await asyncio.gather(*tasks, return_exceptions=True)
                    except Exception as e:
                        logger.error(f"Kline REST poll error: {e}")
                    await asyncio.sleep(POLL_INTERVAL)

        if _HAS_WEBSOCKETS:
            logger.info("Binance source stream started (WS primary, REST fallback)")
            ws_task = asyncio.create_task(ws_main())
            try:
                await rest_poll()
            finally:
                ws_task.cancel()
        else:
            logger.warning("websockets 库不可用，Binance 源降级为纯 REST 轮询")
            await rest_poll()

    async def _fetch_and_publish(self, client: httpx.AsyncClient, symbol: str, timeframe: str) -> None:
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
            self._track_prec(symbol, [k[1], k[2], k[3], k[4]])   # REST 降级链路同步累积精度
            await market_manager.publish_bar(
                self.name, symbol, timeframe,
                {
                    "timestamp": int(k[0]),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                    "event_ms": int(time.time() * 1000),
                },
            )
        except Exception as e:
            logger.debug(f"Kline fetch error for {symbol}/{timeframe}: {e}")

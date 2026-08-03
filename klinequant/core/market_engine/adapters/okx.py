"""OKXAdapter — OKX 交易所适配器

完整实现：
    - WebSocket 实时 K 线 + Tick 数据（OKX API v5）
    - REST API 历史 K 线拉取
    - 断线自动重连（指数退避）
    - 多品种订阅
    - OKX 特有的 ping/pong 心跳机制

OKX API v5 文档: https://www.okx.com/docs-v5/
遵循需求文档 §4.1 MKT-001。
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set

import httpx
import websockets
import websockets.exceptions

from core.market_engine.adapters.base import (
    ExchangeAdapter,
    KlineCallback,
    TickCallback,
)
from core.market_engine.okx_normalizer import (
    TIMEFRAME_TO_OKX,
    normalize_okx_kline,
    normalize_okx_klines,
    normalize_okx_trade,
    normalize_okx_ticker,
    normalize_symbol,
    denormalize_symbol,
    timeframe_to_okx_bar,
    okx_bar_to_timeframe,
)
from protocol.types import Kline, Tick

logger = logging.getLogger(__name__)


class OKXAdapter(ExchangeAdapter):
    """OKX 交易所适配器（API v5）"""

    # 默认配置
    _DEFAULT_REST_BASE = "https://www.okx.com"
    _DEFAULT_WS_BASE = "wss://ws.okx.com:8443/ws/v5/public"
    _RECONNECT_BASE_DELAY = 1.0
    _RECONNECT_MAX_DELAY = 60.0
    _REST_TIMEOUT = 15.0
    _WS_PING_INTERVAL = 25  # OKX 要求 30s 内发送 ping
    _MAX_CANDLES_LIMIT = 300  # OKX 单次最多返回 300 条

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        config = config or {}
        super().__init__(name="okx", config=config)

        # 网络配置
        self._rest_base = config.get("rest_base", self._DEFAULT_REST_BASE)
        self._ws_base = config.get("ws_base", self._DEFAULT_WS_BASE)
        self._proxy = config.get("proxy")

        # WebSocket 状态
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._running = False
        self._reconnect_count = 0

        # 订阅管理
        # 格式: {"channel": "candle1m", "instId": "BTC-USDT"}
        self._sub_args: List[Dict[str, str]] = []
        self._kline_subs: Dict[str, KlineCallback] = {}  # "instId_bar" → callback
        self._tick_subs: Set[str] = set()  # instId set

        # 最新 K 线缓存
        self._latest_kline: Dict[str, Kline] = {}

        # HTTP 客户端
        self._http: Optional[httpx.AsyncClient] = None

    # ─── 连接管理 ───

    async def connect(self) -> None:
        """建立连接"""
        if self._connected:
            return

        transport = None
        if self._proxy:
            transport = httpx.AsyncHTTPTransport(proxy=self._proxy)
        self._http = httpx.AsyncClient(
            base_url=self._rest_base,
            timeout=self._REST_TIMEOUT,
            transport=transport,
        )

        self._connected = True
        self._running = True
        self._reconnect_count = 0
        logger.info(f"OKXAdapter connected: {self._rest_base}")

    async def disconnect(self) -> None:
        """断开连接"""
        self._running = False
        self._connected = False

        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass

        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass

        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        if self._http:
            await self._http.aclose()
            self._http = None

        logger.info("OKXAdapter disconnected")

    # ─── 订阅 ───

    async def subscribe_kline(
        self,
        symbol: str,
        interval: str,
        callback: KlineCallback,
    ) -> None:
        """订阅 K 线数据

        Args:
            symbol: 交易对（支持 "BTCUSDT" 或 "BTC-USDT"）
            interval: K 线周期（如 "1m", "1h"）
            callback: K 线回调
        """
        inst_id = normalize_symbol(symbol)
        bar = timeframe_to_okx_bar(interval)
        channel = f"candle{bar}"

        sub_key = f"{inst_id}_{bar}"
        self._kline_subs[sub_key] = callback
        self.register_kline_callback(callback)

        arg = {"channel": channel, "instId": inst_id}
        if arg not in self._sub_args:
            self._sub_args.append(arg)

        if self._ws:
            await self._send_subscribe([arg])

        logger.info(f"OKX subscribed kline: {inst_id} {bar}")

    async def subscribe_tick(
        self,
        symbol: str,
        callback: TickCallback,
    ) -> None:
        """订阅逐笔成交"""
        inst_id = normalize_symbol(symbol)
        self._tick_subs.add(inst_id)
        self.register_tick_callback(callback)

        arg = {"channel": "trades", "instId": inst_id}
        if arg not in self._sub_args:
            self._sub_args.append(arg)

        if self._ws:
            await self._send_subscribe([arg])

        logger.info(f"OKX subscribed trades: {inst_id}")

    async def unsubscribe(self, symbol: str, stream: str) -> None:
        """取消订阅"""
        inst_id = normalize_symbol(symbol)

        # 确定要取消的 arg
        to_remove = []
        for arg in self._sub_args:
            if arg["instId"] == inst_id:
                if stream == "kline" and arg["channel"].startswith("candle"):
                    to_remove.append(arg)
                elif stream == "trades" and arg["channel"] == "trades":
                    to_remove.append(arg)
                elif stream and arg["channel"] == stream:
                    to_remove.append(arg)

        for arg in to_remove:
            self._sub_args.remove(arg)

        if self._ws and to_remove:
            await self._send_unsubscribe(to_remove)

    # ─── WebSocket 连接与重连 ───

    async def start_ws(self) -> None:
        """启动 WebSocket 接收循环"""
        if not self._sub_args:
            logger.warning("No subscriptions, skipping WS start")
            return
        self._ws_task = asyncio.create_task(self._ws_loop())

    async def _ws_loop(self) -> None:
        """WebSocket 主循环，含断线重连"""
        while self._running:
            try:
                await self._ws_connect()
                self._reconnect_count = 0
                await self._ws_receive_loop()
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running:
                    break
                self._reconnect_count += 1
                delay = min(
                    self._RECONNECT_BASE_DELAY * (2 ** (self._reconnect_count - 1)),
                    self._RECONNECT_MAX_DELAY,
                )
                logger.warning(
                    f"OKX WS disconnected: {e}, reconnecting in {delay:.1f}s "
                    f"(attempt {self._reconnect_count})"
                )
                await asyncio.sleep(delay)

    async def _ws_connect(self) -> None:
        """建立 WebSocket 连接并订阅"""
        connect_kwargs: Dict[str, Any] = {
            "ping_interval": None,  # OKX 使用自定义 ping
            "ping_timeout": None,
            "close_timeout": 5,
        }
        if self._proxy:
            connect_kwargs["proxy"] = self._proxy

        self._ws = await websockets.connect(self._ws_base, **connect_kwargs)
        self._connected = True

        # 发送订阅
        if self._sub_args:
            await self._send_subscribe(self._sub_args)

        # 启动 ping 心跳
        self._ping_task = asyncio.create_task(self._ping_loop())

        logger.info(f"OKX WS connected: {len(self._sub_args)} subscriptions")

    async def _ping_loop(self) -> None:
        """OKX 心跳：每 25s 发送 'ping' 文本"""
        try:
            while self._running and self._ws:
                await asyncio.sleep(self._WS_PING_INTERVAL)
                if self._ws:
                    await self._ws.send("ping")
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def _ws_receive_loop(self) -> None:
        """WebSocket 消息接收循环"""
        if not self._ws:
            return

        async for message in self._ws:
            if not self._running:
                break

            # OKX pong 响应
            if message == "pong":
                continue

            try:
                data = json.loads(message)
                await self._handle_ws_message(data)
            except json.JSONDecodeError:
                logger.debug(f"OKX WS non-JSON: {message[:50]}")
            except Exception as e:
                logger.error(f"OKX WS message error: {e}")

    async def _handle_ws_message(self, data: Dict[str, Any]) -> None:
        """处理 WebSocket 消息"""
        # 订阅确认
        if "event" in data:
            event = data["event"]
            if event == "subscribe":
                logger.debug(f"OKX subscribe confirmed: {data.get('arg')}")
            elif event == "unsubscribe":
                logger.debug(f"OKX unsubscribe confirmed: {data.get('arg')}")
            elif event == "error":
                logger.error(f"OKX WS error: {data.get('code')} {data.get('msg')}")
            return

        # 数据推送
        arg = data.get("arg", {})
        channel = arg.get("channel", "")
        action = data.get("action", "")
        items = data.get("data", [])

        if channel.startswith("candle"):
            await self._handle_candle_data(arg, items)
        elif channel == "trades":
            await self._handle_trades_data(arg, items)
        elif channel == "tickers":
            await self._handle_ticker_data(arg, items)

    async def _handle_candle_data(
        self, arg: Dict[str, Any], items: List[Any]
    ) -> None:
        """处理 K 线数据"""
        inst_id = arg.get("instId", "")
        channel = arg.get("channel", "")

        # 从 channel 解析 bar: "candle1m" → "1m"
        bar = channel.replace("candle", "")
        try:
            timeframe = okx_bar_to_timeframe(bar)
        except ValueError:
            timeframe = bar  # fallback

        symbol = denormalize_symbol(inst_id)

        for raw in items:
            try:
                kline = normalize_okx_kline(raw, symbol, timeframe, self._name)
                cache_key = f"{inst_id}_{bar}"
                self._latest_kline[cache_key] = kline
                await self._dispatch_kline(kline)
            except Exception as e:
                logger.error(f"OKX kline parse error: {e}")

    async def _handle_trades_data(
        self, arg: Dict[str, Any], items: List[Any]
    ) -> None:
        """处理逐笔成交"""
        for item in items:
            try:
                tick = normalize_okx_trade(item, self._name)
                await self._dispatch_tick(tick)
            except Exception as e:
                logger.error(f"OKX trade parse error: {e}")

    async def _handle_ticker_data(
        self, arg: Dict[str, Any], items: List[Any]
    ) -> None:
        """处理 ticker 数据"""
        for item in items:
            try:
                tick = normalize_okx_ticker(item, self._name)
                await self._dispatch_tick(tick)
            except Exception as e:
                logger.error(f"OKX ticker parse error: {e}")

    async def _send_subscribe(self, args: List[Dict[str, str]]) -> None:
        """发送订阅消息"""
        if not self._ws:
            return
        msg = {"op": "subscribe", "args": args}
        await self._ws.send(json.dumps(msg))

    async def _send_unsubscribe(self, args: List[Dict[str, str]]) -> None:
        """发送取消订阅消息"""
        if not self._ws:
            return
        msg = {"op": "unsubscribe", "args": args}
        await self._ws.send(json.dumps(msg))

    # ─── REST API ───

    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 300,
    ) -> List[Kline]:
        """REST 获取历史 K 线

        OKX: GET /api/v5/market/candles
        """
        if not self._http:
            raise RuntimeError("Adapter not connected")

        inst_id = normalize_symbol(symbol)
        bar = timeframe_to_okx_bar(interval)

        params: Dict[str, Any] = {
            "instId": inst_id,
            "bar": bar,
            "limit": str(min(limit, self._MAX_CANDLES_LIMIT)),
        }
        if start_time is not None:
            params["before"] = str(start_time)  # OKX: before = 请求此时间之前的数据
        if end_time is not None:
            params["after"] = str(end_time)  # OKX: after = 请求此时间之后的数据

        resp = await self._http.get("/api/v5/market/candles", params=params)
        resp.raise_for_status()
        body = resp.json()

        if body.get("code") != "0":
            raise RuntimeError(f"OKX API error: {body.get('code')} {body.get('msg')}")

        raw_list = body.get("data", [])
        return normalize_okx_klines(raw_list, symbol, interval, self._name)

    async def fetch_server_time(self) -> int:
        """获取 OKX 服务器时间"""
        if not self._http:
            raise RuntimeError("Adapter not connected")

        resp = await self._http.get("/api/v5/public/time")
        resp.raise_for_status()
        body = resp.json()

        if body.get("code") != "0":
            raise RuntimeError(f"OKX API error: {body.get('code')} {body.get('msg')}")

        data = body.get("data", [{}])
        return int(data[0].get("ts", 0))

    # ─── K 线缺失检测 ───

    async def detect_and_fill_gaps(
        self,
        symbol: str,
        interval: str,
    ) -> List[Kline]:
        """检测 K 线缺失并通过 REST 补全"""
        inst_id = normalize_symbol(symbol)
        bar = timeframe_to_okx_bar(interval)
        cache_key = f"{inst_id}_{bar}"
        latest = self._latest_kline.get(cache_key)

        if latest is None:
            klines = await self.fetch_klines(symbol, interval, limit=1)
            if klines:
                self._latest_kline[cache_key] = klines[-1]
            return klines

        from core.market_engine.normalizer import timeframe_to_ms
        expected_interval = timeframe_to_ms(interval)
        now_ms = int(time.time() * 1000)
        missing_start = latest.timestamp + expected_interval

        if missing_start >= now_ms:
            return []

        klines = await self.fetch_klines(
            symbol, interval,
            start_time=missing_start,
            end_time=now_ms,
            limit=300,
        )
        if klines:
            self._latest_kline[cache_key] = klines[-1]
        return klines

    # ─── 批量订阅 ───

    async def subscribe_multiple_symbols(
        self,
        symbols: List[str],
        intervals: List[str],
        callback: KlineCallback,
    ) -> None:
        """批量订阅多个交易对的多个周期"""
        for symbol in symbols:
            for interval in intervals:
                await self.subscribe_kline(symbol, interval, callback)

        logger.info(
            f"OKX subscribed {len(symbols)} symbols x {len(intervals)} intervals "
            f"= {len(symbols) * len(intervals)} streams"
        )

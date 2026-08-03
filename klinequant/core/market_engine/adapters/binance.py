"""BinanceAdapter — 币安交易所适配器

完整实现：
    - WebSocket 实时 K 线 + Tick 数据
    - REST API 历史 K 线拉取
    - 断线自动重连（指数退避）
    - 多品种订阅（≥ 50 交易对）
    - 重连后 K 线缺失检测

遵循需求文档 §4.1 MKT-001~MKT-009。
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
from core.market_engine.normalizer import (
    normalize_binance_kline,
    normalize_binance_klines,
)
from protocol.types import Kline, Tick

logger = logging.getLogger(__name__)


class BinanceAdapter(ExchangeAdapter):
    """币安交易所适配器"""

    # 默认配置
    _DEFAULT_REST_BASE = "https://api.binance.com"
    _DEFAULT_WS_BASE = "wss://stream.binance.com:9443/ws"
    _RECONNECT_BASE_DELAY = 1.0  # 秒
    _RECONNECT_MAX_DELAY = 60.0  # 最大重连延迟
    _REST_TIMEOUT = 15.0  # REST 请求超时
    _WS_PING_INTERVAL = 20  # WebSocket ping 间隔（秒）
    _REST_RATE_LIMIT = 1200  # 请求/分钟

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        config = config or {}
        super().__init__(name="binance", config=config)

        # 网络配置
        self._rest_base = config.get("rest_base", self._DEFAULT_REST_BASE)
        self._ws_base = config.get("ws_base", self._DEFAULT_WS_BASE)
        self._proxy = config.get("proxy")  # HTTP 代理地址

        # 测试网支持
        testnet = config.get("testnet", {})
        if testnet.get("enabled", False):
            self._rest_base = testnet.get("rest_base", self._rest_base)
            self._ws_base = testnet.get("ws_base", self._ws_base)

        # WebSocket 状态
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._running = False
        self._reconnect_count = 0

        # 订阅管理
        self._subscriptions: Set[str] = set()  # 已订阅的 stream 名称
        self._symbol_intervals: Dict[str, Set[str]] = {}  # symbol → {interval}
        self._tick_symbols: Set[str] = set()

        # 最新 K 线缓存（用于缺失检测）
        self._latest_kline: Dict[str, Kline] = {}  # "symbol_interval" → Kline

        # HTTP 客户端
        self._http: Optional[httpx.AsyncClient] = None

    # ─── 连接管理 ───

    async def connect(self) -> None:
        """建立连接"""
        if self._connected:
            return

        # 创建 HTTP 客户端
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
        logger.info(f"BinanceAdapter connected: {self._rest_base}")

    async def disconnect(self) -> None:
        """断开连接"""
        self._running = False
        self._connected = False

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

        logger.info("BinanceAdapter disconnected")

    # ─── 订阅 ───

    async def subscribe_kline(
        self,
        symbol: str,
        interval: str,
        callback: KlineCallback,
    ) -> None:
        """订阅 K 线数据"""
        stream = f"{symbol.lower()}@kline_{interval}"
        self._subscriptions.add(stream)
        self._symbol_intervals.setdefault(symbol.upper(), set()).add(interval)
        self.register_kline_callback(callback)

        if self._ws and self._ws.open:
            await self._send_subscribe([stream])

        logger.info(f"Subscribed kline: {symbol} {interval}")

    async def subscribe_tick(
        self,
        symbol: str,
        callback: TickCallback,
    ) -> None:
        """订阅逐笔成交"""
        stream = f"{symbol.lower()}@trade"
        self._subscriptions.add(stream)
        self._tick_symbols.add(symbol.upper())
        self.register_tick_callback(callback)

        if self._ws and self._ws.open:
            await self._send_subscribe([stream])

        logger.info(f"Subscribed tick: {symbol}")

    async def unsubscribe(self, symbol: str, stream: str) -> None:
        """取消订阅"""
        stream_name = f"{symbol.lower()}@{stream}"
        self._subscriptions.discard(stream_name)

        if self._ws and self._ws.open:
            await self._send_unsubscribe([stream_name])

    # ─── WebSocket 连接与重连 ───

    async def start_ws(self) -> None:
        """启动 WebSocket 接收循环"""
        if not self._subscriptions:
            logger.warning("No subscriptions, skipping WS start")
            return

        self._ws_task = asyncio.create_task(self._ws_loop())

    async def _ws_loop(self) -> None:
        """WebSocket 主循环，含断线重连"""
        while self._running:
            try:
                await self._ws_connect()
                self._reconnect_count = 0  # 重置重连计数
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
                    f"WS disconnected: {e}, reconnecting in {delay:.1f}s "
                    f"(attempt {self._reconnect_count})"
                )
                await asyncio.sleep(delay)

    async def _ws_connect(self) -> None:
        """建立 WebSocket 连接"""
        # 构建组合流 URL
        streams = "/".join(sorted(self._subscriptions))
        url = f"{self._ws_base}/{streams}" if self._subscriptions else self._ws_base

        extra_headers = {}
        connect_kwargs: Dict[str, Any] = {
            "ping_interval": self._WS_PING_INTERVAL,
            "ping_timeout": 10,
            "close_timeout": 5,
        }

        # 代理支持（websockets >= 14 支持 proxy 参数）
        if self._proxy:
            connect_kwargs["proxy"] = self._proxy

        self._ws = await websockets.connect(url, **connect_kwargs)
        self._connected = True
        logger.info(f"WS connected: {len(self._subscriptions)} streams")

    async def _ws_receive_loop(self) -> None:
        """WebSocket 消息接收循环"""
        if not self._ws:
            return

        async for message in self._ws:
            if not self._running:
                break
            try:
                data = json.loads(message)
                await self._handle_ws_message(data)
            except json.JSONDecodeError as e:
                logger.warning(f"WS JSON decode error: {e}")
            except Exception as e:
                logger.error(f"WS message handling error: {e}")

    async def _handle_ws_message(self, data: Dict[str, Any]) -> None:
        """处理 WebSocket 消息"""
        event = data.get("e")

        if event == "kline":
            await self._handle_kline_event(data)
        elif event == "trade":
            await self._handle_trade_event(data)
        elif event == "error":
            logger.error(f"WS error: {data}")

    async def _handle_kline_event(self, data: Dict[str, Any]) -> None:
        """处理 K 线事件"""
        k = data.get("k", {})
        symbol = k.get("s", "")
        interval = k.get("i", "")
        is_closed = k.get("x", False)  # Binance 直接标记是否收盘

        try:
            # 构建 Binance 格式的 raw 数组
            raw = [
                k.get("t"),  # open_time
                k.get("o"),  # open
                k.get("h"),  # high
                k.get("l"),  # low
                k.get("c"),  # close
                k.get("v"),  # volume
                k.get("T"),  # close_time
                k.get("q"),  # quote_volume
                k.get("n"),  # trade_count
                k.get("V"),  # taker_buy_base
                k.get("Q"),  # taker_buy_quote
                "0",         # ignore
            ]
            kline = normalize_binance_kline(raw, symbol, interval, self._name)

            # 使用 WebSocket 事件的 x 字段覆盖 is_closed
            if is_closed:
                kline = Kline(
                    symbol=kline.symbol,
                    exchange=kline.exchange,
                    timeframe=kline.timeframe,
                    timestamp=kline.timestamp,
                    open=kline.open,
                    high=kline.high,
                    low=kline.low,
                    close=kline.close,
                    volume=kline.volume,
                    quote_volume=kline.quote_volume,
                    trade_count=kline.trade_count,
                    is_closed=True,
                )

            # 更新缓存
            cache_key = f"{symbol}_{interval}"
            self._latest_kline[cache_key] = kline

            # 分发
            await self._dispatch_kline(kline)

        except Exception as e:
            logger.error(f"Kline parse error: {e}")

    async def _handle_trade_event(self, data: Dict[str, Any]) -> None:
        """处理逐笔成交事件"""
        try:
            price = Decimal(str(data.get("p", "0")))
            qty = Decimal(str(data.get("q", "0")))
            tick = Tick(
                symbol=data.get("s", ""),
                exchange=self._name,
                timestamp=int(data.get("T", 0)),
                last_price=price,
                bid_price=price,  # trade 事件没有 bid/ask
                bid_qty=Decimal("0"),
                ask_price=price,
                ask_qty=Decimal("0"),
                volume_24h=qty,
            )
            await self._dispatch_tick(tick)
        except Exception as e:
            logger.error(f"Trade parse error: {e}")

    async def _send_subscribe(self, streams: List[str]) -> None:
        """动态订阅"""
        if not self._ws:
            return
        msg = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": int(time.time() * 1000),
        }
        await self._ws.send(json.dumps(msg))

    async def _send_unsubscribe(self, streams: List[str]) -> None:
        """动态取消订阅"""
        if not self._ws:
            return
        msg = {
            "method": "UNSUBSCRIBE",
            "params": streams,
            "id": int(time.time() * 1000),
        }
        await self._ws.send(json.dumps(msg))

    # ─── REST API ───

    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Kline]:
        """REST 获取历史 K 线"""
        if not self._http:
            raise RuntimeError("Adapter not connected")

        params: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": min(limit, 1000),
        }
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time

        resp = await self._http.get("/api/v3/klines", params=params)
        resp.raise_for_status()
        raw_list = resp.json()

        return normalize_binance_klines(raw_list, symbol, interval, self._name)

    async def fetch_server_time(self) -> int:
        """获取币安服务器时间"""
        if not self._http:
            raise RuntimeError("Adapter not connected")

        resp = await self._http.get("/api/v3/time")
        resp.raise_for_status()
        data = resp.json()
        return int(data["serverTime"])

    # ─── K 线缺失检测 ───

    async def detect_and_fill_gaps(
        self,
        symbol: str,
        interval: str,
        kline_repo: Optional[Any] = None,
    ) -> List[Kline]:
        """检测 K 线缺失并通过 REST 补全。

        Args:
            symbol: 交易对
            interval: K 线周期
            kline_repo: KlineRepository 实例（可选，用于查询本地数据）

        Returns:
            补全的 K 线列表
        """
        cache_key = f"{symbol}_{interval}"
        latest = self._latest_kline.get(cache_key)

        if latest is None:
            # 没有缓存，拉取最新
            klines = await self.fetch_klines(symbol, interval, limit=1)
            if klines:
                self._latest_kline[cache_key] = klines[-1]
            return klines

        # 检测时间跳跃
        from core.market_engine.normalizer import timeframe_to_ms
        expected_interval = timeframe_to_ms(interval)
        now_ms = int(time.time() * 1000)
        missing_start = latest.timestamp + expected_interval

        if missing_start >= now_ms:
            return []  # 没有缺失

        klines = await self.fetch_klines(
            symbol, interval,
            start_time=missing_start,
            end_time=now_ms,
            limit=1000,
        )

        if klines:
            self._latest_kline[cache_key] = klines[-1]

        return klines

    # ─── 批量订阅辅助 ───

    async def subscribe_multiple_symbols(
        self,
        symbols: List[str],
        intervals: List[str],
        callback: KlineCallback,
    ) -> None:
        """批量订阅多个交易对的多个周期。

        Args:
            symbols: 交易对列表
            intervals: K 线周期列表
            callback: K 线回调
        """
        for symbol in symbols:
            for interval in intervals:
                await self.subscribe_kline(symbol, interval, callback)

        logger.info(
            f"Subscribed {len(symbols)} symbols × {len(intervals)} intervals "
            f"= {len(symbols) * len(intervals)} streams"
        )

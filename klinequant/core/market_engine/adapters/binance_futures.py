"""BinanceFuturesAdapter — 币安合约适配器

基于 BinanceAdapter 扩展，支持 USDT-M 合约：
    - fapi REST API（K线/资金费率/标记价格）
    - fstream WebSocket（实时行情 + 资金费率推送）
    - 杠杆设置
    - 做空支持
    - 资金费率监控

Binance Futures API: https://binance-docs.github.io/apidocs/futures/
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlencode

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
from protocol.types import Kline, Tick, FundingRate, FuturesPosition, MarginMode

logger = logging.getLogger(__name__)


class BinanceFuturesAdapter(ExchangeAdapter):
    """币安 USDT-M 合约适配器"""

    _DEFAULT_REST_BASE = "https://fapi.binance.com"
    _DEFAULT_WS_BASE = "wss://fstream.binance.com/ws"
    _RECONNECT_BASE_DELAY = 1.0
    _RECONNECT_MAX_DELAY = 60.0
    _REST_TIMEOUT = 15.0
    _WS_PING_INTERVAL = 20

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        config = config or {}
        super().__init__(name="binance_futures", config=config)

        self._rest_base = config.get("rest_base", self._DEFAULT_REST_BASE)
        self._ws_base = config.get("ws_base", self._DEFAULT_WS_BASE)
        self._proxy = config.get("proxy")
        self._api_key = config.get("api_key", "")
        self._api_secret = config.get("api_secret", "")

        # WebSocket 状态
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._running = False
        self._reconnect_count = 0

        # 订阅管理
        self._subscriptions: Set[str] = set()
        self._stream_callbacks: Dict[str, List[KlineCallback]] = {}
        self._funding_callbacks: List[Any] = []

        # 缓存
        self._latest_kline: Dict[str, Kline] = {}
        self._funding_rates: Dict[str, FundingRate] = {}

        # HTTP
        self._http: Optional[httpx.AsyncClient] = None

    # ─── 连接管理 ───

    async def connect(self) -> None:
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
        logger.info(f"BinanceFuturesAdapter connected: {self._rest_base}")

    async def disconnect(self) -> None:
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

        logger.info("BinanceFuturesAdapter disconnected")

    # ─── 订阅 ───

    async def subscribe_kline(
        self, symbol: str, interval: str, callback: KlineCallback
    ) -> None:
        stream = f"{symbol.lower()}@kline_{interval}"
        self._subscriptions.add(stream)
        self._stream_callbacks.setdefault(stream, []).append(callback)
        self.register_kline_callback(callback)

        if self._ws:
            await self._send_subscribe([stream])
        logger.info(f"Futures subscribed kline: {symbol} {interval}")

    async def subscribe_tick(self, symbol: str, callback: TickCallback) -> None:
        stream = f"{symbol.lower()}@trade"
        self._subscriptions.add(stream)
        self.register_tick_callback(callback)

        if self._ws:
            await self._send_subscribe([stream])

    async def subscribe_funding_rate(self, symbol: str, callback=None) -> None:
        """订阅资金费率推送"""
        stream = f"{symbol.lower()}@markPrice@1s"
        self._subscriptions.add(stream)
        if callback:
            self._funding_callbacks.append(callback)

        if self._ws:
            await self._send_subscribe([stream])
        logger.info(f"Futures subscribed funding rate: {symbol}")

    async def unsubscribe(self, symbol: str, stream: str) -> None:
        stream_name = f"{symbol.lower()}@{stream}"
        self._subscriptions.discard(stream_name)
        if self._ws:
            await self._send_unsubscribe([stream_name])

    # ─── WebSocket ───

    async def start_ws(self) -> None:
        if not self._subscriptions:
            logger.warning("No subscriptions, skipping WS start")
            return
        self._ws_task = asyncio.create_task(self._ws_loop())

    async def _ws_loop(self) -> None:
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
                logger.warning(f"Futures WS error: {e}, reconnect in {delay:.1f}s")
                await asyncio.sleep(delay)

    async def _ws_connect(self) -> None:
        streams = "/".join(sorted(self._subscriptions))
        url = f"{self._ws_base}/{streams}" if streams else self._ws_base

        connect_kwargs: Dict[str, Any] = {
            "ping_interval": self._WS_PING_INTERVAL,
            "ping_timeout": 10,
            "close_timeout": 5,
        }
        if self._proxy:
            connect_kwargs["proxy"] = self._proxy

        self._ws = await websockets.connect(url, **connect_kwargs)
        self._connected = True
        logger.info(f"Futures WS connected: {len(self._subscriptions)} streams")

    async def _ws_receive_loop(self) -> None:
        if not self._ws:
            return
        async for message in self._ws:
            if not self._running:
                break
            try:
                data = json.loads(message)
                await self._handle_ws_message(data)
            except json.JSONDecodeError:
                pass
            except Exception as e:
                logger.error(f"Futures WS error: {e}")

    async def _handle_ws_message(self, data: Dict[str, Any]) -> None:
        event = data.get("e")

        if event == "kline":
            await self._handle_kline_event(data)
        elif event == "trade":
            await self._handle_trade_event(data)
        elif event == "markPriceUpdate":
            await self._handle_mark_price_event(data)

    async def _handle_kline_event(self, data: Dict[str, Any]) -> None:
        k = data.get("k", {})
        symbol = k.get("s", "")
        interval = k.get("i", "")
        is_closed = k.get("x", False)

        try:
            raw = [
                k.get("t"), k.get("o"), k.get("h"), k.get("l"),
                k.get("c"), k.get("v"), k.get("T"), k.get("q"),
                k.get("n"), k.get("V"), k.get("Q"), "0",
            ]
            kline = normalize_binance_kline(raw, symbol, interval, self._name)

            if is_closed:
                kline = Kline(
                    symbol=kline.symbol, exchange=kline.exchange,
                    timeframe=kline.timeframe, timestamp=kline.timestamp,
                    open=kline.open, high=kline.high, low=kline.low,
                    close=kline.close, volume=kline.volume,
                    quote_volume=kline.quote_volume,
                    trade_count=kline.trade_count, is_closed=True,
                )

            cache_key = f"{symbol}_{interval}"
            self._latest_kline[cache_key] = kline
            await self._dispatch_kline(kline)
        except Exception as e:
            logger.error(f"Futures kline error: {e}")

    async def _handle_trade_event(self, data: Dict[str, Any]) -> None:
        try:
            price = Decimal(str(data.get("p", "0")))
            qty = Decimal(str(data.get("q", "0")))
            tick = Tick(
                symbol=data.get("s", ""),
                exchange=self._name,
                timestamp=int(data.get("T", 0)),
                last_price=price,
                bid_price=price,
                bid_qty=Decimal("0"),
                ask_price=price,
                ask_qty=Decimal("0"),
                volume_24h=qty,
            )
            await self._dispatch_tick(tick)
        except Exception as e:
            logger.error(f"Futures trade error: {e}")

    async def _handle_mark_price_event(self, data: Dict[str, Any]) -> None:
        """处理标记价格/资金费率推送"""
        try:
            symbol = data.get("s", "")
            funding_rate = FundingRate(
                symbol=symbol,
                exchange=self._name,
                funding_rate=Decimal(str(data.get("r", "0"))),
                next_funding_time=int(data.get("T", 0)),
                mark_price=Decimal(str(data.get("p", "0"))),
                index_price=Decimal(str(data.get("i", "0"))),
                timestamp=int(data.get("E", 0)),
            )
            self._funding_rates[symbol] = funding_rate

            for cb in self._funding_callbacks:
                if asyncio.iscoroutinefunction(cb):
                    await cb(funding_rate)
                else:
                    cb(funding_rate)
        except Exception as e:
            logger.error(f"Futures mark price error: {e}")

    async def _send_subscribe(self, streams: List[str]) -> None:
        if not self._ws:
            return
        msg = {"method": "SUBSCRIBE", "params": streams, "id": int(time.time() * 1000)}
        await self._ws.send(json.dumps(msg))

    async def _send_unsubscribe(self, streams: List[str]) -> None:
        if not self._ws:
            return
        msg = {"method": "UNSUBSCRIBE", "params": streams, "id": int(time.time() * 1000)}
        await self._ws.send(json.dumps(msg))

    # ─── REST API ───

    async def fetch_klines(
        self, symbol: str, interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Kline]:
        if not self._http:
            raise RuntimeError("Adapter not connected")

        params: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": min(limit, 1500),
        }
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time

        resp = await self._http.get("/fapi/v1/klines", params=params)
        resp.raise_for_status()
        return normalize_binance_klines(resp.json(), symbol, interval, self._name)

    async def fetch_server_time(self) -> int:
        if not self._http:
            raise RuntimeError("Adapter not connected")
        resp = await self._http.get("/fapi/v1/time")
        resp.raise_for_status()
        return int(resp.json()["serverTime"])

    async def fetch_funding_rate(self, symbol: str) -> FundingRate:
        """获取当前资金费率

        GET /fapi/v1/premiumIndex
        """
        if not self._http:
            raise RuntimeError("Adapter not connected")

        resp = await self._http.get(
            "/fapi/v1/premiumIndex", params={"symbol": symbol.upper()}
        )
        resp.raise_for_status()
        data = resp.json()

        return FundingRate(
            symbol=data.get("symbol", symbol),
            exchange=self._name,
            funding_rate=Decimal(str(data.get("lastFundingRate", "0"))),
            next_funding_time=int(data.get("nextFundingTime", 0)),
            mark_price=Decimal(str(data.get("markPrice", "0"))),
            index_price=Decimal(str(data.get("indexPrice", "0"))),
            timestamp=int(data.get("time", 0)),
        )

    async def fetch_funding_rate_history(
        self, symbol: str, limit: int = 100
    ) -> List[FundingRate]:
        """获取历史资金费率

        GET /fapi/v1/fundingRate
        """
        if not self._http:
            raise RuntimeError("Adapter not connected")

        resp = await self._http.get(
            "/fapi/v1/fundingRate",
            params={"symbol": symbol.upper(), "limit": min(limit, 1000)},
        )
        resp.raise_for_status()

        result = []
        for item in resp.json():
            result.append(FundingRate(
                symbol=item.get("symbol", symbol),
                exchange=self._name,
                funding_rate=Decimal(str(item.get("fundingRate", "0"))),
                next_funding_time=0,
                mark_price=Decimal("0"),
                index_price=Decimal("0"),
                timestamp=int(item.get("fundingTime", 0)),
            ))
        return result

    # ─── 签名请求 ───

    def _sign_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """为私有接口添加签名"""
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = 5000
        query = urlencode(params)
        signature = hmac.new(
            self._api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        params["signature"] = signature
        return params

    def _auth_headers(self) -> Dict[str, str]:
        return {"X-MBX-APIKEY": self._api_key}

    async def set_leverage(self, symbol: str, leverage: int) -> int:
        """设置杠杆倍数

        POST /fapi/v1/leverage
        """
        if not self._http:
            raise RuntimeError("Adapter not connected")

        params = self._sign_params({
            "symbol": symbol.upper(),
            "leverage": leverage,
        })
        resp = await self._http.post(
            "/fapi/v1/leverage",
            params=params,
            headers=self._auth_headers(),
        )
        resp.raise_for_status()
        data = resp.json()
        return int(data.get("leverage", leverage))

    async def set_margin_mode(self, symbol: str, mode: MarginMode) -> bool:
        """设置保证金模式

        POST /fapi/v1/marginType
        """
        if not self._http:
            raise RuntimeError("Adapter not connected")

        params = self._sign_params({
            "symbol": symbol.upper(),
            "marginType": mode.value,
        })
        resp = await self._http.post(
            "/fapi/v1/marginType",
            params=params,
            headers=self._auth_headers(),
        )
        # -4046 = "No need to change margin type" 也算成功
        if resp.status_code == 200:
            data = resp.json()
            code = data.get("code", 0)
            return code == 200 or code == -4046
        return False

    async def fetch_positions(self) -> List[FuturesPosition]:
        """查询合约持仓

        GET /fapi/v2/positionRisk
        """
        if not self._http:
            raise RuntimeError("Adapter not connected")

        params = self._sign_params({})
        resp = await self._http.get(
            "/fapi/v2/positionRisk",
            params=params,
            headers=self._auth_headers(),
        )
        resp.raise_for_status()

        positions = []
        for item in resp.json():
            qty = Decimal(str(item.get("positionAmt", "0")))
            if qty == Decimal("0"):
                continue

            side = "LONG" if qty > 0 else "SHORT"
            positions.append(FuturesPosition(
                symbol=item.get("symbol", ""),
                exchange=self._name,
                side=side,
                quantity=abs(qty),
                avg_entry_price=Decimal(str(item.get("entryPrice", "0"))),
                mark_price=Decimal(str(item.get("markPrice", "0"))),
                liquidation_price=Decimal(str(item.get("liquidationPrice", "0"))),
                unrealized_pnl=Decimal(str(item.get("unRealizedProfit", "0"))),
                margin=Decimal(str(item.get("isolatedMargin", "0") or "0")),
                leverage=int(float(item.get("leverage", "1"))),
                margin_mode=MarginMode.ISOLATED if item.get("isolated") == "true" else MarginMode.CROSS,
                updated_at=int(time.time() * 1000),
            ))
        return positions

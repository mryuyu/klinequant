"""BinanceFuturesExecutor — 币安合约交易执行器

实现 Binance USDT-M 合约交易：
    - 下单（市价/限价，支持做多/做空）
    - 撤单
    - 查询订单/持仓/账户
    - 设置杠杆/保证金模式
    - HMAC SHA256 签名

Binance Futures API: https://binance-docs.github.io/apidocs/futures/
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import httpx

from core.trade_engine.executors.base import Executor
from protocol.types import (
    Account,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    FuturesPosition,
    MarginMode,
)

logger = logging.getLogger(__name__)


# Binance 订单状态 → 内部状态
_STATUS_MAP: Dict[str, OrderStatus] = {
    "NEW": OrderStatus.SUBMITTED,
    "PARTIALLY_FILLED": OrderStatus.PARTIAL_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELED,
    "PENDING_CANCEL": OrderStatus.CANCELING,
    "REJECTED": OrderStatus.REJECTED,
    "EXPIRED": OrderStatus.EXPIRED,
}


class BinanceFuturesExecutor(Executor):
    """币安合约交易执行器"""

    _DEFAULT_REST_BASE = "https://fapi.binance.com"

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        api_key: str = "",
        api_secret: str = "",
    ):
        super().__init__(name="binance_futures_executor", config=config)
        self._api_key = api_key or self._config.get("api_key", "")
        self._api_secret = api_secret or self._config.get("api_secret", "")
        self._rest_base = self._config.get("rest_base", self._DEFAULT_REST_BASE)
        self._proxy = self._config.get("proxy")
        self._http: Optional[httpx.AsyncClient] = None

    async def connect(self) -> None:
        transport = None
        if self._proxy:
            transport = httpx.AsyncHTTPTransport(proxy=self._proxy)
        self._http = httpx.AsyncClient(
            base_url=self._rest_base,
            timeout=15.0,
            transport=transport,
        )
        logger.info(f"BinanceFuturesExecutor connected: {self._rest_base}")

    async def disconnect(self) -> None:
        if self._http:
            await self._http.aclose()
            self._http = None

    # ─── 签名 ───

    def _sign_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = 5000
        query = urlencode(params)
        sig = hmac.new(
            self._api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        params["signature"] = sig
        return params

    def _headers(self) -> Dict[str, str]:
        return {"X-MBX-APIKEY": self._api_key}

    # ─── 交易操作 ───

    async def submit_order(self, order: Order) -> Order:
        """提交合约订单

        POST /fapi/v1/order
        支持做多(BUY)和做空(SELL)
        """
        if not self._http:
            raise RuntimeError("Not connected")

        params: Dict[str, Any] = {
            "symbol": order.symbol.upper(),
            "side": order.side.value,  # BUY / SELL
            "quantity": str(order.quantity),
            "newClientOrderId": order.client_order_id or f"kq_{order.order_id[:16]}",
        }

        if order.order_type == OrderType.MARKET:
            params["type"] = "MARKET"
        elif order.order_type == OrderType.LIMIT:
            params["type"] = "LIMIT"
            params["price"] = str(order.price)
            params["timeInForce"] = "GTC"
        elif order.order_type == OrderType.STOP_LIMIT:
            params["type"] = "STOP"
            params["price"] = str(order.price)
            params["stopPrice"] = str(order.price)
            params["timeInForce"] = "GTC"

        # 可选: reduceOnly 用于平仓
        if hasattr(order, "reduce_only") and order.reduce_only:
            params["reduceOnly"] = "true"

        signed = self._sign_params(params)
        resp = await self._http.post(
            "/fapi/v1/order", params=signed, headers=self._headers()
        )

        if resp.status_code != 200:
            error = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            msg = error.get("msg", f"HTTP {resp.status_code}")
            logger.error(f"Futures order rejected: {msg}")
            return Order(
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                symbol=order.symbol,
                exchange="binance_futures",
                side=order.side,
                order_type=order.order_type,
                quantity=order.quantity,
                price=order.price,
                status=OrderStatus.REJECTED,
                strategy_id=order.strategy_id,
                created_at=order.created_at,
                updated_at=int(time.time() * 1000),
                cancel_reason=msg,
            )

        data = resp.json()
        return Order(
            order_id=order.order_id,
            client_order_id=data.get("clientOrderId", order.client_order_id),
            exchange_order_id=str(data.get("orderId", "")),
            symbol=order.symbol,
            exchange="binance_futures",
            side=order.side,
            order_type=order.order_type,
            quantity=Decimal(str(data.get("origQty", order.quantity))),
            price=order.price,
            status=_STATUS_MAP.get(data.get("status", "NEW"), OrderStatus.SUBMITTED),
            strategy_id=order.strategy_id,
            created_at=order.created_at,
            updated_at=int(time.time() * 1000),
        )

    async def cancel_order(self, order: Order) -> Order:
        """撤单

        DELETE /fapi/v1/order
        """
        if not self._http:
            raise RuntimeError("Not connected")

        params = self._sign_params({
            "symbol": order.symbol.upper(),
            "orderId": order.exchange_order_id,
        })
        resp = await self._http.delete(
            "/fapi/v1/order", params=params, headers=self._headers()
        )

        if resp.status_code == 200:
            return Order(
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
                symbol=order.symbol,
                exchange="binance_futures",
                side=order.side,
                order_type=order.order_type,
                quantity=order.quantity,
                price=order.price,
                status=OrderStatus.CANCELED,
                strategy_id=order.strategy_id,
                created_at=order.created_at,
                updated_at=int(time.time() * 1000),
            )
        logger.error(f"Futures cancel failed: {resp.text}")
        return order

    async def query_order(self, order: Order) -> Order:
        """查询订单

        GET /fapi/v1/order
        """
        if not self._http:
            raise RuntimeError("Not connected")

        params = self._sign_params({
            "symbol": order.symbol.upper(),
            "orderId": order.exchange_order_id,
        })
        resp = await self._http.get(
            "/fapi/v1/order", params=params, headers=self._headers()
        )

        if resp.status_code != 200:
            return order

        data = resp.json()
        return Order(
            order_id=order.order_id,
            client_order_id=data.get("clientOrderId", order.client_order_id),
            exchange_order_id=str(data.get("orderId", "")),
            symbol=order.symbol,
            exchange="binance_futures",
            side=OrderSide(data.get("side", "BUY")),
            order_type=order.order_type,
            quantity=Decimal(str(data.get("origQty", "0"))),
            filled_quantity=Decimal(str(data.get("executedQty", "0"))),
            avg_fill_price=Decimal(str(data.get("avgPrice", "0") or "0")),
            price=order.price,
            status=_STATUS_MAP.get(data.get("status", "NEW"), OrderStatus.SUBMITTED),
            strategy_id=order.strategy_id,
            created_at=order.created_at,
            updated_at=int(time.time() * 1000),
        )

    async def query_positions(
        self, symbols: Optional[List[str]] = None
    ) -> Dict[str, Position]:
        """查询持仓

        GET /fapi/v2/positionRisk
        """
        if not self._http:
            raise RuntimeError("Not connected")

        params = self._sign_params({})
        resp = await self._http.get(
            "/fapi/v2/positionRisk", params=params, headers=self._headers()
        )

        positions: Dict[str, Position] = {}
        if resp.status_code != 200:
            return positions

        for item in resp.json():
            symbol = item.get("symbol", "")
            if symbols and symbol not in [s.upper() for s in symbols]:
                continue

            qty = Decimal(str(item.get("positionAmt", "0")))
            if qty == Decimal("0"):
                continue

            side = "LONG" if qty > 0 else "SHORT"
            positions[symbol] = Position(
                symbol=symbol,
                exchange="binance_futures",
                side=side,
                quantity=abs(qty),
                avg_entry_price=Decimal(str(item.get("entryPrice", "0"))),
                unrealized_pnl=Decimal(str(item.get("unRealizedProfit", "0"))),
                margin=Decimal(str(item.get("isolatedMargin", "0") or "0")),
                leverage=int(float(item.get("leverage", "1"))),
                updated_at=int(time.time() * 1000),
            )
        return positions

    async def query_account(self) -> Account:
        """查询账户

        GET /fapi/v2/balance
        """
        if not self._http:
            raise RuntimeError("Not connected")

        params = self._sign_params({})
        resp = await self._http.get(
            "/fapi/v2/balance", params=params, headers=self._headers()
        )

        if resp.status_code != 200:
            raise RuntimeError(f"Futures balance error: {resp.text}")

        total = Decimal("0")
        available = Decimal("0")
        frozen = Decimal("0")
        upl = Decimal("0")

        for item in resp.json():
            if item.get("asset") == "USDT":
                total = Decimal(str(item.get("balance", "0")))
                available = Decimal(str(item.get("availableBalance", "0")))
                frozen = Decimal(str(item.get("crossUnPnl", "0")))
                upl = Decimal(str(item.get("crossUnPnl", "0")))
                break

        return Account(
            exchange="binance_futures",
            account_type="FUTURES",
            total_balance=total,
            available_balance=available,
            frozen_balance=frozen,
            unrealized_pnl=upl,
        )

    # ─── 合约专用 ───

    async def set_leverage(self, symbol: str, leverage: int) -> int:
        """设置杠杆"""
        if not self._http:
            raise RuntimeError("Not connected")

        params = self._sign_params({
            "symbol": symbol.upper(),
            "leverage": leverage,
        })
        resp = await self._http.post(
            "/fapi/v1/leverage", params=params, headers=self._headers()
        )
        if resp.status_code == 200:
            return int(resp.json().get("leverage", leverage))
        logger.error(f"Set leverage failed: {resp.text}")
        return leverage

    async def set_margin_mode(self, symbol: str, mode: MarginMode) -> bool:
        """设置保证金模式"""
        if not self._http:
            raise RuntimeError("Not connected")

        params = self._sign_params({
            "symbol": symbol.upper(),
            "marginType": mode.value,
        })
        resp = await self._http.post(
            "/fapi/v1/marginType", params=params, headers=self._headers()
        )
        if resp.status_code == 200:
            code = resp.json().get("code", 0)
            return code == 200 or code == -4046
        return False

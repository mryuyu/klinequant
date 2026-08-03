"""OKXExecutor — OKX 交易执行器

实现 OKX API v5 的交易操作：
    - 下单（市价/限价）
    - 撤单
    - 查询订单/持仓/账户
    - HMAC SHA256 签名认证

OKX API v5: https://www.okx.com/docs-v5/
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import httpx

from core.trade_engine.executors.base import Executor
from core.market_engine.okx_normalizer import normalize_symbol, denormalize_symbol
from protocol.types import (
    Account,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
)

logger = logging.getLogger(__name__)


# OKX 订单状态 → 内部状态映射
_OKX_STATUS_MAP: Dict[str, OrderStatus] = {
    "live": OrderStatus.SUBMITTED,       # 等待成交
    "partially_filled": OrderStatus.PARTIAL_FILLED,
    "filled": OrderStatus.FILLED,
    "canceled": OrderStatus.CANCELED,
    "cancelled": OrderStatus.CANCELED,
    "mmp_canceled": OrderStatus.CANCELED,
}

# 内部订单类型 → OKX ordType
_ORDER_TYPE_MAP: Dict[OrderType, str] = {
    OrderType.MARKET: "market",
    OrderType.LIMIT: "limit",
}


class OKXExecutor(Executor):
    """OKX 交易执行器

    支持现货和合约交易（通过 td_mode 区分）。
    """

    _DEFAULT_REST_BASE = "https://www.okx.com"

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        api_key: str = "",
        api_secret: str = "",
        passphrase: str = "",
        td_mode: str = "cash",  # "cash"=现货, "cross"=全仓, "isolated"=逐仓
    ):
        super().__init__(name="okx_executor", config=config)
        self._api_key = api_key or (self._config.get("api_key", ""))
        self._api_secret = api_secret or (self._config.get("api_secret", ""))
        self._passphrase = passphrase or (self._config.get("passphrase", ""))
        self._td_mode = td_mode
        self._rest_base = self._config.get("rest_base", self._DEFAULT_REST_BASE)
        self._proxy = self._config.get("proxy")
        self._http: Optional[httpx.AsyncClient] = None

    # ─── 连接管理 ───

    async def connect(self) -> None:
        """创建 HTTP 客户端"""
        transport = None
        if self._proxy:
            transport = httpx.AsyncHTTPTransport(proxy=self._proxy)
        self._http = httpx.AsyncClient(
            base_url=self._rest_base,
            timeout=15.0,
            transport=transport,
        )
        logger.info(f"OKXExecutor connected: {self._rest_base}")

    async def disconnect(self) -> None:
        """关闭 HTTP 客户端"""
        if self._http:
            await self._http.aclose()
            self._http = None

    # ─── 签名 ───

    def _sign(self, timestamp: str, method: str, path: str, body: str = "") -> str:
        """HMAC SHA256 签名

        Sign = Base64(HMAC_SHA256(secret, timestamp + method + requestPath + body))
        """
        message = f"{timestamp}{method}{path}{body}"
        mac = hmac.new(
            self._api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode("utf-8")

    def _headers(self, method: str, path: str, body: str = "") -> Dict[str, str]:
        """构建认证请求头"""
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        return {
            "OK-ACCESS-KEY": self._api_key,
            "OK-ACCESS-SIGN": self._sign(timestamp, method, path, body),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type": "application/json",
        }

    # ─── 交易操作 ───

    async def submit_order(self, order: Order) -> Order:
        """提交订单

        OKX: POST /api/v5/trade/order
        """
        if not self._http:
            raise RuntimeError("OKXExecutor not connected")

        inst_id = normalize_symbol(order.symbol)
        ord_type = _ORDER_TYPE_MAP.get(order.order_type, "market")
        side = "buy" if order.side == OrderSide.BUY else "sell"

        body_dict: Dict[str, Any] = {
            "instId": inst_id,
            "tdMode": self._td_mode,
            "side": side,
            "ordType": ord_type,
            "sz": str(order.quantity),
            "clOrdId": order.client_order_id or f"kq_{uuid.uuid4().hex[:16]}",
        }
        if order.order_type == OrderType.LIMIT and order.price:
            body_dict["px"] = str(order.price)

        body = json.dumps(body_dict)
        path = "/api/v5/trade/order"
        headers = self._headers("POST", path, body)

        resp = await self._http.post(path, content=body, headers=headers)
        resp.raise_for_status()
        result = resp.json()

        if result.get("code") != "0":
            error_msg = result.get("msg", "")
            data = result.get("data", [{}])
            if data:
                error_msg = data[0].get("sMsg", error_msg)
            logger.error(f"OKX order rejected: {error_msg}")
            return Order(
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                symbol=order.symbol,
                exchange="okx",
                side=order.side,
                order_type=order.order_type,
                quantity=order.quantity,
                price=order.price,
                status=OrderStatus.REJECTED,
                strategy_id=order.strategy_id,
                created_at=order.created_at,
                updated_at=int(time.time() * 1000),
                cancel_reason=error_msg,
            )

        data = result.get("data", [{}])[0]
        exchange_order_id = data.get("ordId", "")

        return Order(
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            exchange_order_id=exchange_order_id,
            symbol=order.symbol,
            exchange="okx",
            side=order.side,
            order_type=order.order_type,
            quantity=order.quantity,
            price=order.price,
            status=OrderStatus.SUBMITTED,
            strategy_id=order.strategy_id,
            created_at=order.created_at,
            updated_at=int(time.time() * 1000),
        )

    async def cancel_order(self, order: Order) -> Order:
        """撤销订单

        OKX: POST /api/v5/trade/cancel-order
        """
        if not self._http:
            raise RuntimeError("OKXExecutor not connected")

        inst_id = normalize_symbol(order.symbol)
        body_dict = {"instId": inst_id, "ordId": order.exchange_order_id}
        body = json.dumps(body_dict)
        path = "/api/v5/trade/cancel-order"
        headers = self._headers("POST", path, body)

        resp = await self._http.post(path, content=body, headers=headers)
        resp.raise_for_status()
        result = resp.json()

        if result.get("code") == "0":
            return Order(
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
                symbol=order.symbol,
                exchange="okx",
                side=order.side,
                order_type=order.order_type,
                quantity=order.quantity,
                price=order.price,
                status=OrderStatus.CANCELED,
                strategy_id=order.strategy_id,
                created_at=order.created_at,
                updated_at=int(time.time() * 1000),
            )
        else:
            logger.error(f"OKX cancel failed: {result}")
            return order

    async def query_order(self, order: Order) -> Order:
        """查询订单状态

        OKX: GET /api/v5/trade/order?instId=...&ordId=...
        """
        if not self._http:
            raise RuntimeError("OKXExecutor not connected")

        inst_id = normalize_symbol(order.symbol)
        path = f"/api/v5/trade/order?instId={inst_id}&ordId={order.exchange_order_id}"
        headers = self._headers("GET", path)

        resp = await self._http.get(path, headers=headers)
        resp.raise_for_status()
        result = resp.json()

        if result.get("code") != "0":
            return order

        data = result.get("data", [{}])[0]
        state = data.get("state", "live")
        status = _OKX_STATUS_MAP.get(state, OrderStatus.SUBMITTED)

        return Order(
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
            symbol=order.symbol,
            exchange="okx",
            side=order.side,
            order_type=order.order_type,
            quantity=Decimal(str(data.get("sz", order.quantity))),
            filled_quantity=Decimal(str(data.get("accFillSz", "0"))),
            avg_fill_price=Decimal(str(data.get("avgPx", "0") or "0")),
            price=order.price,
            status=status,
            strategy_id=order.strategy_id,
            created_at=order.created_at,
            updated_at=int(time.time() * 1000),
            fee=Decimal(str(data.get("fee", "0") or "0")),
        )

    async def query_positions(
        self, symbols: Optional[List[str]] = None
    ) -> Dict[str, Position]:
        """查询持仓

        OKX: GET /api/v5/account/positions
        """
        if not self._http:
            raise RuntimeError("OKXExecutor not connected")

        path = "/api/v5/account/positions"
        if symbols:
            inst_ids = ",".join(normalize_symbol(s) for s in symbols)
            path += f"?instId={inst_ids}"

        headers = self._headers("GET", path)
        resp = await self._http.get(path, headers=headers)
        resp.raise_for_status()
        result = resp.json()

        positions: Dict[str, Position] = {}
        if result.get("code") != "0":
            return positions

        for item in result.get("data", []):
            inst_id = item.get("instId", "")
            symbol = denormalize_symbol(inst_id)
            pos_side = item.get("posSide", "net")
            qty = Decimal(str(item.get("pos", "0")))

            # OKX: pos > 0 = long, pos < 0 = short
            if qty > 0:
                side = "LONG"
            elif qty < 0:
                side = "SHORT"
                qty = abs(qty)
            else:
                continue  # 无持仓

            positions[symbol] = Position(
                symbol=symbol,
                exchange="okx",
                side=side,
                quantity=qty,
                avg_entry_price=Decimal(str(item.get("avgPx", "0") or "0")),
                unrealized_pnl=Decimal(str(item.get("upl", "0") or "0")),
                realized_pnl=Decimal(str(item.get("realizedPnl", "0") or "0")),
                margin=Decimal(str(item.get("margin", "0") or "0")),
                leverage=int(float(item.get("lever", "1"))),
                updated_at=int(time.time() * 1000),
            )

        return positions

    async def query_account(self) -> Account:
        """查询账户余额

        OKX: GET /api/v5/account/balance
        """
        if not self._http:
            raise RuntimeError("OKXExecutor not connected")

        path = "/api/v5/account/balance"
        headers = self._headers("GET", path)
        resp = await self._http.get(path, headers=headers)
        resp.raise_for_status()
        result = resp.json()

        if result.get("code") != "0":
            raise RuntimeError(f"OKX account error: {result.get('msg')}")

        data = result.get("data", [{}])[0]
        total_eq = Decimal(str(data.get("totalEq", "0") or "0"))

        # 计算可用和冻结
        available = Decimal("0")
        frozen = Decimal("0")
        for detail in data.get("details", []):
            avail = Decimal(str(detail.get("availBal", "0") or "0"))
            frz = Decimal(str(detail.get("frozenBal", "0") or "0"))
            available += avail
            frozen += frz

        return Account(
            exchange="okx",
            account_type="SPOT" if self._td_mode == "cash" else "FUTURES",
            total_balance=total_eq,
            available_balance=available,
            frozen_balance=frozen,
        )

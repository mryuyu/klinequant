"""交易路由

API：
    GET  /api/trade/orders — 订单列表
    POST /api/trade/orders — 手动下单
    DELETE /api/trade/orders/{id} — 撤单
    GET  /api/trade/positions — 持仓列表
    GET  /api/trade/fills — 成交记录
    GET  /api/trade/account — 账户信息

遵循需求文档 §4.7 GW-005。
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import time
from typing import Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Query
from pydantic import BaseModel

from gateway.audit import audit_logger
from gateway.state import BINANCE_API_KEY, BINANCE_API_SECRET, BINANCE_REST_BASE, state

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/trade", tags=["trade"])


def _sign(params: dict) -> dict:
    """币安 API 签名"""
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000
    query = urlencode(params)
    signature = hmac.new(
        BINANCE_API_SECRET.encode(), query.encode(), hashlib.sha256
    ).hexdigest()
    params["signature"] = signature
    return params


def _headers() -> dict:
    return {"X-MBX-APIKEY": BINANCE_API_KEY}


class OrderCreate(BaseModel):
    symbol: str
    side: str  # BUY / SELL
    order_type: str = "MARKET"  # MARKET / LIMIT
    quantity: float
    price: Optional[float] = None


@router.get("/orders")
async def list_orders(
    symbol: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
):
    """获取订单列表"""
    try:
        client = state.get_http_client()
        params = _sign({"symbol": symbol or "BTCUSDT", "limit": limit})
        resp = await client.get(
            f"{BINANCE_REST_BASE}/api/v3/openOrders",
            params=params,
            headers=_headers(),
        )
        if resp.status_code == 200:
            orders = resp.json()
            return {
                "orders": [
                    {
                        "order_id": str(o["orderId"]),
                        "symbol": o["symbol"],
                        "side": o["side"],
                        "order_type": o["type"],
                        "quantity": float(o["origQty"]),
                        "price": float(o["price"]) if o["price"] != "0" else None,
                        "status": o["status"],
                        "time": o["time"],
                    }
                    for o in orders
                ],
                "total": len(orders),
            }
    except Exception as e:
        logger.error(f"Failed to fetch orders: {e}")
    return {"orders": [], "total": 0}


@router.post("/orders")
async def create_order(body: OrderCreate):
    """手动下单"""
    try:
        client = state.get_http_client()
        params = {
            "symbol": body.symbol.upper(),
            "side": body.side.upper(),
            "type": body.order_type.upper(),
            "quantity": body.quantity,
        }
        if body.order_type.upper() == "LIMIT":
            params["timeInForce"] = "GTC"
            params["price"] = body.price
        signed = _sign(params)
        resp = await client.post(
            f"{BINANCE_REST_BASE}/api/v3/order",
            params=signed,
            headers=_headers(),
        )
        data = resp.json()
        if resp.status_code == 200:
            audit_logger.log(
                action="ORDER_CREATE", operator="user", resource="order",
                resource_id=str(data["orderId"]),
                detail=f"下单 {body.side.upper()} {body.symbol} qty={body.quantity}",
            )
            return {
                "order_id": str(data["orderId"]),
                "status": data["status"],
                "symbol": data["symbol"],
                "side": data["side"],
            }
        audit_logger.log(
            action="ORDER_CREATE", operator="user", resource="order",
            resource_id="unknown",
            detail=f"下单被拒: {data.get('msg', 'Order failed')}",
            result="FAILED",
        )
        return {"error": data.get("msg", "Order failed"), "code": data.get("code")}
    except Exception as e:
        logger.error(f"Failed to create order: {e}")
        return {"error": str(e)}


@router.delete("/orders/{order_id}")
async def cancel_order(order_id: str, symbol: str = Query("BTCUSDT")):
    """撤单"""
    try:
        client = state.get_http_client()
        params = _sign({"symbol": symbol.upper(), "orderId": order_id})
        resp = await client.delete(
            f"{BINANCE_REST_BASE}/api/v3/order",
            params=params,
            headers=_headers(),
        )
        data = resp.json()
        if resp.status_code == 200:
            audit_logger.log(
                action="ORDER_CANCEL", operator="user", resource="order",
                resource_id=order_id, detail=f"撤单 {order_id} ({symbol})",
            )
            return {"order_id": order_id, "status": "CANCELED"}
        return {"error": data.get("msg", "Cancel failed")}
    except Exception as e:
        logger.error(f"Failed to cancel order: {e}")
        return {"error": str(e)}


@router.get("/positions")
async def list_positions():
    """获取持仓列表（通过余额推算）"""
    try:
        client = state.get_http_client()
        params = _sign({})
        resp = await client.get(
            f"{BINANCE_REST_BASE}/api/v3/account",
            params=params,
            headers=_headers(),
        )
        if resp.status_code == 200:
            data = resp.json()
            positions = []
            for b in data.get("balances", []):
                free = float(b["free"])
                locked = float(b["locked"])
                total = free + locked
                if total > 0 and b["asset"] not in ("USDT", "BUSD", "USD"):
                    positions.append({
                        "symbol": f"{b['asset']}USDT",
                        "asset": b["asset"],
                        "side": "LONG",
                        "quantity": total,
                        "free": free,
                        "locked": locked,
                        "pnl": 0,
                    })
            return {"positions": positions}
    except Exception as e:
        logger.error(f"Failed to fetch positions: {e}")
    return {"positions": []}


@router.get("/fills")
async def list_fills(
    symbol: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
):
    """获取成交记录"""
    try:
        client = state.get_http_client()
        params = _sign({"symbol": symbol or "BTCUSDT", "limit": limit})
        resp = await client.get(
            f"{BINANCE_REST_BASE}/api/v3/myTrades",
            params=params,
            headers=_headers(),
        )
        if resp.status_code == 200:
            trades = resp.json()
            return {
                "fills": [
                    {
                        "trade_id": str(t["id"]),
                        "symbol": t["symbol"],
                        "price": float(t["price"]),
                        "quantity": float(t["qty"]),
                        "commission": float(t["commission"]),
                        "time": t["time"],
                        "is_buyer": t["isBuyer"],
                    }
                    for t in trades
                ],
                "total": len(trades),
            }
    except Exception as e:
        logger.error(f"Failed to fetch fills: {e}")
    return {"fills": [], "total": 0}


@router.get("/account")
async def get_account():
    """获取账户信息"""
    try:
        client = state.get_http_client()
        params = _sign({})
        resp = await client.get(
            f"{BINANCE_REST_BASE}/api/v3/account",
            params=params,
            headers=_headers(),
        )
        if resp.status_code == 200:
            data = resp.json()
            balances = data.get("balances", [])
            # 计算 USDT 余额
            usdt_free = 0.0
            usdt_locked = 0.0
            assets = []
            for b in balances:
                free = float(b["free"])
                locked = float(b["locked"])
                if free + locked > 0:
                    assets.append({"asset": b["asset"], "free": free, "locked": locked})
                if b["asset"] == "USDT":
                    usdt_free = free
                    usdt_locked = locked
            return {
                "total_balance": usdt_free + usdt_locked,
                "available_balance": usdt_free,
                "unrealized_pnl": 0,
                "assets": assets[:20],  # 前20个有余额的资产
            }
    except Exception as e:
        logger.error(f"Failed to fetch account: {e}")
    return {
        "total_balance": 0,
        "available_balance": 0,
        "unrealized_pnl": 0,
        "assets": [],
    }

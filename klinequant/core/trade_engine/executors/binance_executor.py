"""BinanceExecutor — 币安 USDT-M Futures 执行器（新架构 ExecutorProtocol）

与 Mt5Executor / BacktestExecutor 暴露同一 submit/cancel/query 契约（返回同一
SubmitResult、同形持仓/账户/挂单 dict），使 KqApi / ExposureLedger /
UnifiedResolver 在加密实盘模拟中零改动复用（同构）。

关键约束：
  - ExecutorProtocol 是**同步**契约，而币安 REST 是 async（httpx.AsyncClient）。
    本类内部用 `asyncio.run_coroutine_threadsafe(coro, loop).result(timeout)`
    把 async 调用桥接为同步；loop 由 BinanceBackend 持有的独立 event loop 线程提供。
  - One-way 净持仓模式：所有单 `positionSide=BOTH`；平仓（offset==CLOSE）带
    `reduceOnly=true`（由本类补，NettingClosePolicy 不设 reduce_only）。
  - query_* 必须返回 **MT5 同形 dict**（type 0=BUY/1=SELL、volume、price_open、
    ticket、balance/margin_free/margin/profit 等），KqApi.flatten/account/
    pending_orders 依赖这些字段解析（BacktestExecutor 已验证此模式可行）。

Binance Futures API: https://binance-docs.github.io/apidocs/futures/
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
import zlib
from decimal import Decimal
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import httpx

from core.trade_engine.executors.mt5_executor import SubmitResult
from core.trade_engine.resolver import VenueOrderSpec
from protocol.types import DeadReason, Offset, OrderKind, OrderSide

logger = logging.getLogger(__name__)


class BinanceExecutor:
    """币安合约执行器（实现 ExecutorProtocol，同步桥接 async REST）。

    Args:
        loop: BinanceBackend 持有的 event loop（独立 daemon 线程 run_forever）
        client: 已配置 base_url（demo-fapi）+ proxy 的 httpx.AsyncClient
        api_key / api_secret: 币安 API 凭证（demo）
        magic: 记账标记（写入 MT5 同形 dict 的 magic 字段，仅对齐形状）
        timeout: 同步桥接等待 async 结果的超时（秒）
    """

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        client: httpx.AsyncClient,
        api_key: str = "",
        api_secret: str = "",
        magic: int = 202609,
        timeout: float = 15.0,
    ):
        self._loop = loop
        self._client = client
        self._api_key = api_key
        self._api_secret = api_secret
        self._magic = magic
        self._timeout = timeout

    # ─── async→sync 桥接 ───

    def _run(self, coro) -> Any:
        """在 backend 的 event loop 上跑协程并同步等待结果。"""
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=self._timeout)

    # ─── 签名（与 binance_futures_executor 同算法：HMAC SHA256）───

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

    @staticmethod
    def _num(d: Any) -> str:
        """Decimal/数值 → 定点字符串（避免科学计数法被 venue 拒）。"""
        if isinstance(d, Decimal):
            return format(d, "f")
        return str(d)

    # ─── ExecutorProtocol 实现 ───

    def submit(self, spec: VenueOrderSpec) -> SubmitResult:
        """提交订单（同步桥接 async REST）。"""
        return self._run(self._submit_async(spec))

    def cancel(self, order_ticket: int, symbol: str, magic: Optional[int] = None) -> bool:
        # 币安撤单按 orderId，无 magic 概念（参数仅为对齐 ExecutorProtocol，R1）
        return self._run(self._cancel_async(order_ticket, symbol))

    def query_positions(self, symbol: str = "", magic: Optional[int] = None) -> List[Dict]:
        # 币安无 magic 概念（隔离靠 API key/账户）；magic 仅为对齐 ExecutorProtocol，忽略
        return self._run(self._query_positions_async(symbol))

    def query_account(self) -> Optional[Dict]:
        return self._run(self._query_account_async())

    def query_orders(self, symbol: str = "", magic: Optional[int] = None) -> List[Dict]:
        # 同 query_positions：币安无 magic，忽略（隔离靠 API key）
        return self._run(self._query_orders_async(symbol))

    def query_order_outcome(self, client_order_id: str, symbol: str = "") -> Optional[Dict]:
        """R3 恢复：凭 origClientOrderId 向币安反查单笔订单真实结局（权威）。"""
        return self._run(self._query_order_outcome_async(client_order_id, symbol))

    # ─── async 实现 ───

    async def _submit_async(self, spec: VenueOrderSpec) -> SubmitResult:
        params: Dict[str, Any] = {
            "symbol": spec.symbol.upper(),
            "side": spec.side.value,           # BUY / SELL
            "quantity": self._num(spec.qty),
            "positionSide": "BOTH",            # One-way 净持仓
        }

        if spec.kind == OrderKind.MARKET:
            params["type"] = "MARKET"
        elif spec.kind == OrderKind.LIMIT:
            params["type"] = "LIMIT"
            params["price"] = self._num(spec.price)
            params["timeInForce"] = "GTC"
        else:
            return SubmitResult(
                success=False, status="DEAD",
                dead_reason=DeadReason.REJECTED.value,
                comment=f"binance executor supports MARKET/LIMIT, got {spec.kind.value}",
            )

        # 平仓：One-way 用 reduceOnly 表达（不发裸反向单）
        if spec.offset == Offset.CLOSE:
            params["reduceOnly"] = "true"

        if spec.client_order_id:
            params["newClientOrderId"] = spec.client_order_id

        signed = self._sign_params(params)
        logger.info(
            f"Binance order: {spec.symbol} {spec.side.value}/{spec.offset.value} "
            f"qty={spec.qty} kind={spec.kind.value} reduceOnly={params.get('reduceOnly')}"
        )
        resp = await self._client.post(
            "/fapi/v1/order", params=signed, headers=self._headers()
        )

        if resp.status_code != 200:
            msg = self._error_msg(resp)
            logger.error(f"Binance order rejected: {msg}")
            return SubmitResult(
                success=False, status="DEAD",
                dead_reason=DeadReason.REJECTED.value,
                comment=msg,
            )

        data = resp.json()
        order_id = int(data.get("orderId", 0) or 0)
        status = data.get("status", "NEW")
        executed = Decimal(str(data.get("executedQty", "0") or "0"))
        avg_price = Decimal(str(data.get("avgPrice", "0") or "0"))

        # M5 灾难保险丝：OPEN 成功后挂 STOP_MARKET/TAKE_PROFIT_MARKET + reduceOnly
        #   （本地平仓后 reduceOnly 保证孤儿单不反向开仓，仍需 cancel_all 收尾）
        if spec.offset == Offset.OPEN and status in ("FILLED", "NEW", "PARTIALLY_FILLED"):
            await self._place_fuse_async(spec)

        if status == "FILLED":
            return SubmitResult(
                success=True, status="FILLED",
                order_ticket=order_id,
                filled_qty=executed if executed > 0 else spec.qty,
                filled_price=avg_price,
                comment=f"binance {status}",
            )
        if status in ("NEW", "PARTIALLY_FILLED"):
            return SubmitResult(
                success=True, status="IN_FLIGHT",
                order_ticket=order_id,
                filled_qty=executed,
                filled_price=avg_price,
                comment=f"binance {status}",
            )
        # EXPIRED / REJECTED / CANCELED 等
        return SubmitResult(
            success=False, status="DEAD",
            dead_reason=DeadReason.REJECTED.value,
            order_ticket=order_id,
            comment=f"binance {status}",
        )

    async def _place_fuse_async(self, spec: VenueOrderSpec) -> None:
        """M5：为已开仓位挂灾难保险丝（SL=STOP_MARKET / TP=TAKE_PROFIT_MARKET，均 reduceOnly）。

        方向与开仓相反（多头用 SELL 平、空头用 BUY 平）；best-effort——失败仅告警，
        不影响主开仓结果（仓位已开，保险丝是进程死亡期间的兜底保护）。
        币安无 MT5 position 属性，故用独立 reduceOnly 触发单表达（平仓后成孤儿单，
        不反向开仓，由 cancel_all 收尾）。
        """
        close_side = "SELL" if spec.side == OrderSide.BUY else "BUY"
        if spec.sl:
            await self._post_fuse_order(spec, "STOP_MARKET", close_side, spec.sl)
        if spec.tp:
            await self._post_fuse_order(spec, "TAKE_PROFIT_MARKET", close_side, spec.tp)

    async def _post_fuse_order(
        self, spec: VenueOrderSpec, order_type: str, side: str, stop_price: Decimal,
    ) -> None:
        """发一笔 reduceOnly 触发单（保险丝）；失败仅告警不抛（best-effort）。"""
        params: dict[str, Any] = {
            "symbol": spec.symbol.upper(),
            "side": side,
            "type": order_type,
            "quantity": self._num(spec.qty),
            "stopPrice": self._num(stop_price),
            "positionSide": "BOTH",
            "reduceOnly": "true",
        }
        signed = self._sign_params(params)
        resp = await self._client.post(
            "/fapi/v1/order", params=signed, headers=self._headers()
        )
        if resp.status_code != 200:
            logger.warning(
                f"Binance fuse {order_type} rejected: {self._error_msg(resp)} "
                f"[{spec.symbol} {side} @ {stop_price}]"
            )
        else:
            logger.info(
                f"Binance fuse placed: {spec.symbol} {order_type} {side} "
                f"qty={spec.qty} @ {stop_price} (reduceOnly)"
            )

    async def _cancel_async(self, order_ticket: int, symbol: str) -> bool:
        params = self._sign_params({
            "symbol": symbol.upper(),
            "orderId": int(order_ticket),
        })
        resp = await self._client.delete(
            "/fapi/v1/order", params=params, headers=self._headers()
        )
        if resp.status_code == 200:
            return True
        logger.error(f"Binance cancel failed: {self._error_msg(resp)}")
        return False

    async def _query_positions_async(self, symbol: str = "") -> List[Dict]:
        raw: Dict[str, Any] = {}
        if symbol:
            raw["symbol"] = symbol.upper()
        params = self._sign_params(raw)
        resp = await self._client.get(
            "/fapi/v2/positionRisk", params=params, headers=self._headers()
        )
        if resp.status_code != 200:
            logger.error(f"Binance positionRisk failed: {self._error_msg(resp)}")
            return []

        out: List[Dict] = []
        for item in resp.json():
            amt = Decimal(str(item.get("positionAmt", "0") or "0"))
            if amt == 0:
                continue
            sym = item.get("symbol", "")
            if symbol and sym != symbol.upper():
                continue
            out.append({
                # 稳定 ticket（positionRisk 无 order id，用 symbol crc32）
                "ticket": zlib.crc32(sym.encode()) & 0x7FFFFFFF,
                "symbol": sym,
                "type": 0 if amt > 0 else 1,   # 0=BUY(多) 1=SELL(空)
                "volume": float(abs(amt)),
                "price_open": float(item.get("entryPrice", "0") or "0"),
                "price_current": float(item.get("markPrice", "0") or "0"),
                "profit": float(item.get("unRealizedProfit", "0") or "0"),
                "magic": self._magic,
                "time": int(float(item.get("updateTime", 0) or 0) // 1000),
                "comment": "binance_futures",
            })
        return out

    async def _query_account_async(self) -> Optional[Dict]:
        params = self._sign_params({})
        resp = await self._client.get(
            "/fapi/v2/balance", params=params, headers=self._headers()
        )
        if resp.status_code != 200:
            logger.error(f"Binance balance failed: {self._error_msg(resp)}")
            return None

        balance = Decimal("0")
        available = Decimal("0")
        upnl = Decimal("0")
        for item in resp.json():
            if item.get("asset") == "USDT":
                balance = Decimal(str(item.get("balance", "0") or "0"))
                available = Decimal(str(item.get("availableBalance", "0") or "0"))
                upnl = Decimal(str(item.get("crossUnPnl", "0") or "0"))
                break
        equity = balance + upnl
        margin = balance - available
        return {
            "balance": float(balance),
            "equity": float(equity),
            "margin": float(margin),
            "margin_free": float(available),
            "profit": float(upnl),
            "currency": "USDT",
        }

    async def _query_orders_async(self, symbol: str = "") -> List[Dict]:
        raw: Dict[str, Any] = {}
        if symbol:
            raw["symbol"] = symbol.upper()
        params = self._sign_params(raw)
        resp = await self._client.get(
            "/fapi/v1/openOrders", params=params, headers=self._headers()
        )
        if resp.status_code != 200:
            logger.error(f"Binance openOrders failed: {self._error_msg(resp)}")
            return []

        out: List[Dict] = []
        for item in resp.json():
            out.append({
                "ticket": int(item.get("orderId", 0) or 0),
                "symbol": item.get("symbol", ""),
                "type": 0 if item.get("side") == "BUY" else 1,
                "volume_current": float(item.get("origQty", "0") or "0"),
                "price_open": float(item.get("price", "0") or "0"),
                "time_setup": int(float(item.get("time", 0) or 0) // 1000),
                # R3 reconcile 缺口2：凭 client_order_id 恢复挂单 in_flight（与 MT5 comment 同义）
                "client_order_id": item.get("clientOrderId", ""),
                "magic": self._magic,
            })
        return out

    async def _query_order_outcome_async(
        self, client_order_id: str, symbol: str = ""
    ) -> Optional[Dict]:
        """GET /fapi/v1/order?origClientOrderId=… 按状态映射 OPEN/FILLED/DEAD/None。"""
        raw: Dict[str, Any] = {"origClientOrderId": client_order_id}
        if symbol:
            raw["symbol"] = symbol.upper()
        params = self._sign_params(raw)
        resp = await self._client.get(
            "/fapi/v1/order", params=params, headers=self._headers()
        )
        if resp.status_code == 400:
            # -2013 = Order does not exist → 查无此单
            return None
        if resp.status_code != 200:
            # 其他错误（限频/服务不可用）→ 抛出，调用方判定 venue 异常
            raise RuntimeError(f"binance queryOrder failed: {self._error_msg(resp)}")
        data = resp.json()
        status = data.get("status", "")
        ticket = int(data.get("orderId", 0) or 0)
        if status in ("NEW", "PARTIALLY_FILLED"):
            return {"state": "OPEN", "ticket": ticket}
        if status == "FILLED":
            return {
                "state": "FILLED", "ticket": ticket,
                "filled_qty": Decimal(str(data.get("executedQty", "0") or "0")),
                "filled_price": Decimal(str(data.get("avgPrice", "0") or "0")),
            }
        # CANCELED / EXPIRED / REJECTED
        return {"state": "DEAD", "ticket": ticket, "reason": f"binance {status}"}

    # ─── 辅助 ───

    @staticmethod
    def _error_msg(resp: httpx.Response) -> str:
        ctype = resp.headers.get("content-type", "")
        if ctype.startswith("application/json"):
            try:
                err = resp.json()
                return f"HTTP {resp.status_code} {err.get('msg', '')} (code={err.get('code')})"
            except Exception:
                pass
        return f"HTTP {resp.status_code}: {resp.text[:200]}"

"""MT5 交易执行器 — VenueOrderSpec → MT5 order_send

职责：
  - 将 Resolver 输出的 VenueOrderSpec 翻译为 MT5 TradeRequest dict
  - 调用 Mt5Api.order_send() 执行
  - 按 retcode 分类结果（FILLED / IN_FLIGHT / DEAD）
  - 提供持仓/账户/挂单查询

MT5 order_send 是同步调用（子进程内执行），无需 async。
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from core.trade_engine.resolver import VenueOrderSpec
from gateway.market_sources.mt5_driver import (
    Mt5Api,
    ORDER_FILLING_FOK,
    ORDER_FILLING_IOC,
    ORDER_FILLING_RETURN,
    ORDER_TIME_DAY,
    ORDER_TIME_GTC,
    ORDER_TIME_SPECIFIED,
    ORDER_TYPE_BUY,
    ORDER_TYPE_BUY_LIMIT,
    ORDER_TYPE_BUY_STOP,
    ORDER_TYPE_SELL,
    ORDER_TYPE_SELL_LIMIT,
    ORDER_TYPE_SELL_STOP,
    TRADE_ACTION_DEAL,
    TRADE_ACTION_PENDING,
    TRADE_ACTION_REMOVE,
    TRADE_RETCODE_CANCEL,
    TRADE_RETCODE_CONNECTION,
    TRADE_RETCODE_DONE,
    TRADE_RETCODE_DONE_PARTIAL,
    TRADE_RETCODE_INVALID_PRICE,
    TRADE_RETCODE_INVALID_STOP,
    TRADE_RETCODE_INVALID_VOLUME,
    TRADE_RETCODE_MARKET_CLOSED,
    TRADE_RETCODE_NO_MONEY,
    TRADE_RETCODE_PLACED,
    TRADE_RETCODE_REJECT,
    TRADE_RETCODE_REQUOTE,
    TRADE_RETCODE_TIMEOUT,
)
from protocol.types import (
    Account,
    DeadReason,
    Offset,
    OrderKind,
    OrderSide,
    Position,
    Tif,
)

logger = logging.getLogger(__name__)


@dataclass
class SubmitResult:
    """order_send 结果"""
    success: bool
    status: str = ""            # "FILLED" / "IN_FLIGHT" / "DEAD"
    dead_reason: str = ""       # DeadReason value（status=DEAD 时）
    order_ticket: int = 0       # MT5 order ticket
    deal_ticket: int = 0        # MT5 deal ticket（成交时）
    filled_qty: Decimal = Decimal("0")
    filled_price: Decimal = Decimal("0")
    retcode: int = 0
    comment: str = ""


# TIF → MT5 type_time / type_filling 映射
_TIF_TO_TIME = {
    Tif.GTC: ORDER_TIME_GTC,
    Tif.DAY: ORDER_TIME_DAY,
    Tif.IOC: ORDER_TIME_GTC,   # IOC 由 filling mode 控制
    Tif.FOK: ORDER_TIME_GTC,   # FOK 由 filling mode 控制
    Tif.GTD: ORDER_TIME_SPECIFIED,
}

_TIF_TO_FILLING = {
    Tif.GTC: ORDER_FILLING_RETURN,   # 限价挂单用 RETURN（部分成交后剩余继续挂）
    Tif.DAY: ORDER_FILLING_RETURN,
    Tif.IOC: ORDER_FILLING_IOC,
    Tif.FOK: ORDER_FILLING_FOK,
    Tif.GTD: ORDER_FILLING_RETURN,
}

# retcode → DeadReason 映射
_RETCODE_TO_DEAD = {
    TRADE_RETCODE_REJECT: DeadReason.REJECTED,
    TRADE_RETCODE_CANCEL: DeadReason.CANCELED,
    TRADE_RETCODE_INVALID_VOLUME: DeadReason.REJECTED,
    TRADE_RETCODE_INVALID_PRICE: DeadReason.REJECTED,
    TRADE_RETCODE_INVALID_STOP: DeadReason.REJECTED,
    TRADE_RETCODE_NO_MONEY: DeadReason.REJECTED,
    TRADE_RETCODE_MARKET_CLOSED: DeadReason.REJECTED,
    TRADE_RETCODE_TIMEOUT: DeadReason.TIMEOUT,
    TRADE_RETCODE_CONNECTION: DeadReason.FAILED,
}


class Mt5Executor:
    """MT5 交易执行器

    与 Mt5Source 共享同一 Mt5Api 实例（单终端连接）。
    """

    def __init__(self, driver: Mt5Api, magic: int = 202609, default_deviation: int = 20):
        self._driver = driver
        self._magic = magic
        self._default_deviation = default_deviation

    @property
    def driver(self) -> Mt5Api:
        return self._driver

    # ─── 下单 ───

    def submit(self, spec: VenueOrderSpec) -> SubmitResult:
        """提交订单到 MT5。

        根据 kind + offset 决定 action 和 order type：
          - MARKET + CLOSE → 按 position ticket 精确平仓（Hedging 安全，见 _submit_close）
          - MARKET + OPEN → TRADE_ACTION_DEAL（即时成交）
          - LIMIT → TRADE_ACTION_PENDING（挂单）
          - STOP_MARKET / STOP_LIMIT → TRADE_ACTION_PENDING（触发挂单）
        """
        # 市价平仓：走 ticket 精确平仓路径（兼容 Netting/Hedging）
        if spec.offset == Offset.CLOSE and spec.kind == OrderKind.MARKET:
            return self._submit_close(spec)

        request = self._build_request(spec)
        if request is None:
            return SubmitResult(
                success=False, status="DEAD",
                dead_reason=DeadReason.FAILED.value,
                comment="failed to build MT5 request",
            )

        logger.info(
            f"MT5 order_send: {spec.symbol} {spec.side.value}/{spec.offset.value} "
            f"qty={spec.qty} kind={spec.kind.value} price={spec.price}"
        )

        result = self._driver.order_send(request)
        if result is None:
            return SubmitResult(
                success=False, status="DEAD",
                dead_reason=DeadReason.FAILED.value,
                comment="order_send returned None (connection lost or timeout)",
            )

        return self._parse_result(result, spec)

    def _submit_close(self, spec: VenueOrderSpec) -> SubmitResult:
        """市价平仓：按 position ticket 精确平指定持仓（兼容 Netting/Hedging）。

        - Netting 账户：positions_get 返回单一净持仓（带 ticket），按 ticket 平即可；
        - Hedging 账户：同品种多笔持仓，按开仓时间 FIFO 选与平仓方向相反的持仓逐笔平
          （BUY 平平仓 type=1 的空单，SELL 平 type=0 的多单）。

        关键安全：找不到匹配持仓时返回 DEAD，绝不发裸反向单
        （否则 Hedging 账户会误开新仓，2026-09-21 实盘实证）。
        """
        # 目标持仓类型：0=BUY(多) 1=SELL(空)；BUY 平空、SELL 平多
        target_type = 1 if spec.side == OrderSide.BUY else 0

        # 选定平仓 legs：(position_ticket, volume)
        legs: List[Tuple[int, Decimal]] = []
        if spec.close_ticket:
            legs = [(spec.close_ticket, spec.qty)]
        else:
            positions = self._driver.positions_get(spec.symbol) or []
            matched = [
                p for p in positions
                if int(p.get("type", -1)) == target_type
                and Decimal(str(p.get("volume", 0))) > 0
            ]
            matched.sort(key=lambda p: int(p.get("time", 0)))  # FIFO
            remaining = spec.qty
            for p in matched:
                if remaining <= 0:
                    break
                vol = Decimal(str(p.get("volume", 0)))
                take = min(vol, remaining)
                legs.append((int(p.get("ticket", 0)), take))
                remaining -= take

        if not legs:
            logger.error(
                f"MT5 close: no matching position for {spec.symbol} "
                f"side={spec.side.value} qty={spec.qty} target_type={target_type}"
            )
            return SubmitResult(
                success=False, status="DEAD",
                dead_reason=DeadReason.REJECTED.value,
                comment="no matching position to close (hedging-safe reject)",
            )

        order_type = ORDER_TYPE_BUY if spec.side == OrderSide.BUY else ORDER_TYPE_SELL
        total_qty = Decimal("0")
        total_value = Decimal("0")   # 成交量×价格，用于加权均价
        last_ticket = 0
        last_retcode = 0
        last_comment = ""
        for ticket, volume in legs:
            price = self._get_current_price(spec.symbol, spec.side)
            if price is None:
                last_comment = f"cannot get current price for {spec.symbol}"
                logger.error(last_comment)
                break
            request = {
                "action": TRADE_ACTION_DEAL,
                "symbol": spec.symbol,
                "volume": float(volume),
                "type": order_type,
                "price": price,
                "position": ticket,   # Hedging 平仓关键字段
                "deviation": spec.deviation or self._default_deviation,
                "magic": spec.magic or self._magic,
                "comment": spec.client_order_id or f"KQ-{int(time.time())}",
                "type_time": ORDER_TIME_GTC,
                "type_filling": ORDER_FILLING_IOC,
            }
            logger.info(
                f"MT5 order_send CLOSE: {spec.symbol} {spec.side.value} "
                f"qty={volume} position={ticket} kind=MARKET price={price}"
            )
            result = self._driver.order_send(request)
            if result is None:
                last_comment = "order_send returned None (connection lost or timeout)"
                break
            parsed = self._parse_result(result, spec)
            last_retcode = parsed.retcode
            last_comment = parsed.comment
            if parsed.status == "FILLED":
                total_qty += parsed.filled_qty
                total_value += parsed.filled_price * parsed.filled_qty
                last_ticket = parsed.order_ticket or ticket
            else:
                break   # 某笔失败，停止后续 leg（已成交部分如实回报）

        if total_qty > 0:
            avg_price = total_value / total_qty
            return SubmitResult(
                success=True, status="FILLED",
                order_ticket=last_ticket,
                filled_qty=total_qty,
                filled_price=avg_price,
                retcode=last_retcode,
                comment=last_comment,
            )
        return SubmitResult(
            success=False, status="DEAD",
            dead_reason=DeadReason.REJECTED.value,
            order_ticket=last_ticket,
            retcode=last_retcode,
            comment=last_comment or "close failed",
        )

    def cancel(self, order_ticket: int, symbol: str) -> bool:
        """撤销挂单"""
        request = {
            "action": TRADE_ACTION_REMOVE,
            "order": order_ticket,
            "symbol": symbol,
            "magic": self._magic,
        }
        result = self._driver.order_send(request)
        if result is None:
            return False
        retcode = int(result.get("retcode", 0))
        return retcode == TRADE_RETCODE_DONE

    # ─── 查询 ───

    def query_positions(self, symbol: str = "") -> List[Dict]:
        """查询 MT5 持仓"""
        return self._driver.positions_get(symbol)

    def query_account(self) -> Optional[Dict]:
        """查询 MT5 账户信息"""
        return self._driver.account_info()

    def query_orders(self, symbol: str = "") -> List[Dict]:
        """查询 MT5 挂单"""
        return self._driver.orders_get(symbol)

    # ─── 内部方法 ───

    def _build_request(self, spec: VenueOrderSpec) -> Optional[dict]:
        """构造 MT5 TradeRequest dict"""
        # 确定 action 和 order type
        if spec.kind == OrderKind.MARKET:
            action = TRADE_ACTION_DEAL
            if spec.side == OrderSide.BUY:
                order_type = ORDER_TYPE_BUY
            else:
                order_type = ORDER_TYPE_SELL
            # 市价单需要当前价格（MT5 要求填写）
            price = self._get_current_price(spec.symbol, spec.side)
            if price is None:
                logger.error(f"Cannot get current price for {spec.symbol}")
                return None
        elif spec.kind == OrderKind.LIMIT:
            action = TRADE_ACTION_PENDING
            if spec.side == OrderSide.BUY:
                order_type = ORDER_TYPE_BUY_LIMIT
            else:
                order_type = ORDER_TYPE_SELL_LIMIT
            price = float(spec.price) if spec.price else 0
        elif spec.kind == OrderKind.STOP_MARKET:
            action = TRADE_ACTION_PENDING
            if spec.side == OrderSide.BUY:
                order_type = ORDER_TYPE_BUY_STOP
            else:
                order_type = ORDER_TYPE_SELL_STOP
            price = float(spec.stop_price) if spec.stop_price else 0
        elif spec.kind == OrderKind.STOP_LIMIT:
            action = TRADE_ACTION_PENDING
            if spec.side == OrderSide.BUY:
                order_type = ORDER_TYPE_BUY_LIMIT
            else:
                order_type = ORDER_TYPE_SELL_LIMIT
            price = float(spec.price) if spec.price else 0
        else:
            return None

        # TIF / filling mode 映射
        # 市价单(DEAL)：即时成交，用 IOC filling。
        #   RETURN 仅适用于挂单；市价单用 RETURN 会被 MT5 拒 "Invalid filling mode"。
        # 挂单(PENDING)：按 tif 映射（GTC/DAY→RETURN, IOC→IOC, FOK→FOK）。
        tif = spec.tif or Tif.GTC
        if action == TRADE_ACTION_DEAL:
            type_time = ORDER_TIME_GTC
            type_filling = ORDER_FILLING_IOC
        else:
            type_time = _TIF_TO_TIME.get(tif, ORDER_TIME_GTC)
            type_filling = _TIF_TO_FILLING.get(tif, ORDER_FILLING_RETURN)

        request = {
            "action": action,
            "symbol": spec.symbol,
            "volume": float(spec.qty),
            "type": order_type,
            "price": price,
            "deviation": spec.deviation or self._default_deviation,
            "magic": spec.magic or self._magic,
            "comment": spec.client_order_id or f"KQ-{int(time.time())}",
            "type_time": type_time,
            "type_filling": type_filling,
        }

        # 止损/止盈（STOP_LIMIT 的 stop_price 作为触发价）
        if spec.kind == OrderKind.STOP_LIMIT and spec.stop_price:
            request["stoplimit"] = float(spec.stop_price)

        # 注：市价平仓不走本方法（submit 已路由到 _submit_close，按 position ticket
        # 精确平仓，兼容 Netting/Hedging）。本方法仅处理 OPEN 与挂单类请求。

        return request

    def _get_current_price(self, symbol: str, side: OrderSide) -> Optional[float]:
        """获取当前价格（市价单用）：BUY 取 ask，SELL 取 bid"""
        tick = self._driver.symbol_info_tick(symbol)
        if tick is None:
            return None
        if side == OrderSide.BUY:
            price = tick.get("ask", 0)
        else:
            price = tick.get("bid", 0)
        return float(price) if price else None

    def _parse_result(self, result: dict, spec: VenueOrderSpec) -> SubmitResult:
        """解析 MT5 TradeResult"""
        retcode = int(result.get("retcode", 0))
        order_ticket = int(result.get("order", 0))
        deal_ticket = int(result.get("deal", 0))
        volume = Decimal(str(result.get("volume", 0)))
        price = Decimal(str(result.get("price", 0)))
        comment = result.get("comment", "")

        # 成功成交
        if retcode in (TRADE_RETCODE_DONE, TRADE_RETCODE_DONE_PARTIAL):
            return SubmitResult(
                success=True,
                status="FILLED",
                order_ticket=order_ticket,
                deal_ticket=deal_ticket,
                filled_qty=volume,
                filled_price=price,
                retcode=retcode,
                comment=comment,
            )

        # 挂单已放置（LIMIT/STOP 等待触发）
        if retcode == TRADE_RETCODE_PLACED:
            return SubmitResult(
                success=True,
                status="IN_FLIGHT",
                order_ticket=order_ticket,
                retcode=retcode,
                comment=comment,
            )

        # Requote：MT5 返回新价格，一期不自动重试，视为失败
        if retcode == TRADE_RETCODE_REQUOTE:
            return SubmitResult(
                success=False,
                status="DEAD",
                dead_reason=DeadReason.REJECTED.value,
                order_ticket=order_ticket,
                retcode=retcode,
                comment=f"Requote: {comment}",
            )

        # 其他失败
        dead_reason = _RETCODE_TO_DEAD.get(retcode, DeadReason.FAILED)
        return SubmitResult(
            success=False,
            status="DEAD",
            dead_reason=dead_reason.value,
            order_ticket=order_ticket,
            retcode=retcode,
            comment=comment or f"retcode={retcode}",
        )

"""KqApi — 策略唯一交互面

每个 (symbol, period) 驱动上下文一个实例。
symbol 默认绑定当前驱动上下文，跨品种策略可显式覆盖。

设计原则：
  - 框架只提供数据 + 执行订单 + 管理生命周期
  - 策略是所有交易决策的唯一主人
  - 框架不猜意图、不算差额、不自动跨零拆单、不自动补单
"""
from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Protocol

from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import OrderRequest, Resolution, UnifiedResolver, VenueOrderSpec
from protocol.types import (
    Account,
    Offset,
    OrderKind,
    OrderResult,
    OrderSide,
    PendingOrder,
    Position,
    SymbolInfo,
    Tick,
    Tif,
)

logger = logging.getLogger(__name__)


class DataFeedProtocol(Protocol):
    """数据源协议（Live / Backtest 各实现一套）"""

    def latest_tick(self, symbol: str) -> Optional[Tick]: ...
    def latest_bars(self, symbol: str, period: str, count: int) -> list: ...
    def wait_update(self, deadline: Optional[float] = None) -> bool: ...
    def is_changing(self, obj: Any, field: Optional[str] = None) -> bool: ...
    def now_ms(self) -> int: ...


class ExecutorProtocol(Protocol):
    """执行器协议（MT5 / Simulator 各实现一套）"""

    def submit(self, spec: VenueOrderSpec) -> Any: ...
    def cancel(self, order_ticket: int, symbol: str) -> bool: ...
    def query_positions(self, symbol: str = "") -> list: ...
    def query_account(self) -> Optional[dict]: ...
    def query_orders(self, symbol: str = "") -> list: ...


class KqApi:
    """策略 SDK 主类。

    Args:
        symbol: 主驱动品种（默认上下文；多品种时作为 symbol=None 的回落）
        period: 驱动周期
        tag: 策略/周期标识（敞口账本按 tag 隔离）
        specs: 品种规格表 {symbol: SymbolInfo}，多品种共享一个上下文
        ledger: 敞口账本
        resolver: 订单解析器
        executor: 交易执行器
        feed: 数据源
    """

    def __init__(
        self,
        symbol: str,
        period: str,
        tag: str,
        specs: Dict[str, SymbolInfo],
        ledger: ExposureLedger,
        resolver: UnifiedResolver,
        executor: ExecutorProtocol,
        feed: DataFeedProtocol,
    ):
        self._symbol = symbol
        self._period = period
        self._tag = tag
        self._specs = specs
        self._ledger = ledger
        self._resolver = resolver
        self._executor = executor
        self._feed = feed
        self._state: Dict[str, Any] = {}
        # 订单 ticket 跟踪（cancel 用）
        self._order_tickets: Dict[str, int] = {}  # client_order_id → MT5 ticket
        # 运行截止时间（Unix 秒）：到点后 wait_update 返回 False，策略优雅退出
        self._run_until: Optional[float] = None

    # ═══════════ 流程控制 ═══════════

    def wait_update(self, deadline: float = None) -> bool:
        """阻塞等待下一次数据/事件更新。

        设置了运行截止时间（set_run_until）时：中途无数据的超时会自动重试
        （周末休市/盘中静默不会误退出），直到有数据返回 True，或到点返回 False。
        未设截止时间时，超时返回 False（旧语义）。

        Returns: True=有更新, False=到达运行截止时间（或无时限模式下超时）
        """
        while True:
            if self._run_until is not None:
                remaining = self._run_until - time.time()
                if remaining <= 0:
                    return False  # 到达运行时限 → 策略退出
                timeout = remaining if deadline is None else min(deadline, remaining)
            else:
                timeout = deadline

            if self._feed.wait_update(timeout):
                return True

            # 本轮无数据：有时限则继续等到点（休市/静默不误退出），无时限返回 False
            if self._run_until is None:
                return False

    def set_run_until(self, unix_ts: Optional[float]) -> None:
        """设置运行截止时间（Unix 秒）。None=不限时（运行到 Ctrl+C）。"""
        self._run_until = unix_ts

    def is_changing(self, obj, field: str = None) -> bool:
        """检查某对象自上次 wait_update 后是否有变化。"""
        return self._feed.is_changing(obj, field)

    # ═══════════ 行情数据 ═══════════

    def ticks(self, symbol: str = None) -> Optional[Tick]:
        """即时价格（最新 tick）。"""
        sym = symbol or self._symbol
        return self._feed.latest_tick(sym)

    def klines(self, period: str = None, count: int = 200,
               symbol: str = None) -> list:
        """K线序列。"""
        sym = symbol or self._symbol
        per = period or self._period
        return self._feed.latest_bars(sym, per, count)

    # ═══════════ 品种与持仓 ═══════════

    def symbols(self) -> List[str]:
        """本上下文订阅的全部品种（主品种在首位）。"""
        return list(self._specs.keys())

    def _spec_for(self, symbol: str) -> SymbolInfo:
        """按品种取规格（多品种各自 pip/step/min 不同，不可混用）。"""
        spec = self._specs.get(symbol) or self._specs.get(symbol.upper())
        if spec is None:
            raise KeyError(
                f"No SymbolInfo for {symbol!r}; loaded={list(self._specs.keys())}"
            )
        return spec

    def symbol_info(self, symbol: str = None) -> SymbolInfo:
        """品种规格（默认主品种）。"""
        return self._spec_for(symbol or self._symbol)

    def position(self, symbol: str = None) -> Position:
        """本策略的持仓视图 = 已成交 + 在途。"""
        sym = symbol or self._symbol
        return self._ledger.position(sym, self._tag)

    def net_position(self, symbol: str = None) -> Decimal:
        """交易所净持仓（所有策略/所有 tag 之和）。"""
        sym = symbol or self._symbol
        return self._ledger.net_position(sym)

    def pending_orders(self, symbol: str = None) -> List[PendingOrder]:
        """本策略当前未终结的挂单列表。"""
        sym = symbol or self._symbol
        raw_orders = self._executor.query_orders(sym)
        result = []
        for o in raw_orders:
            result.append(PendingOrder(
                order_id=str(o.get("ticket", "")),
                symbol=o.get("symbol", sym),
                side=OrderSide.BUY if int(o.get("type", 0)) % 2 == 0 else OrderSide.SELL,
                offset=Offset.OPEN,  # MT5 挂单不区分开平，简化处理
                qty=Decimal(str(o.get("volume_current", 0))),
                price=Decimal(str(o.get("price_open", 0))) if o.get("price_open") else None,
                kind=OrderKind.LIMIT,  # 简化
                created_at=int(o.get("time_setup", 0)) * 1000,
            ))
        return result

    def account(self) -> Account:
        """账户资金信息。"""
        info = self._executor.query_account()
        if info is None:
            return Account(exchange="mt5", account_type="FX",
                           total_balance=Decimal("0"), available_balance=Decimal("0"))
        return Account(
            exchange="mt5",
            account_type="FX",
            total_balance=Decimal(str(info.get("balance", 0))),
            available_balance=Decimal(str(info.get("margin_free", 0))),
            frozen_balance=Decimal(str(info.get("margin", 0))),
            unrealized_pnl=Decimal(str(info.get("profit", 0))),
            updated_at=int(time.time() * 1000),
        )

    # ═══════════ 交易动作 ═══════════

    def send_order(
        self,
        side: OrderSide,
        offset: Offset,
        qty: Decimal,
        *,
        kind: OrderKind = OrderKind.MARKET,
        price: Decimal = None,
        stop_price: Decimal = None,
        tif: Tif = None,
        symbol: str = None,
    ) -> OrderResult:
        """下单。唯一交易入口。

        流程：构造 OrderRequest → Resolver 验证/量化 → Ledger 记账 → Executor 执行 → 更新 Ledger
        """
        sym = symbol or self._symbol

        # ① 构造 OrderRequest
        req = OrderRequest(
            symbol=sym,
            tag=self._tag,
            side=side,
            offset=offset,
            qty=qty,
            kind=kind,
            price=price,
            stop_price=stop_price,
            tif=tif,
        )

        # ② Resolver 验证 + 量化
        pos = self._ledger.position(sym, self._tag)
        has_dup = self._ledger.has_in_flight_open(sym, self._tag, side)
        resolution = self._resolver.resolve(
            req, self._spec_for(sym),
            position_volume=pos.volume,
            available_to_close=pos.available_to_close,
            has_in_flight_open=has_dup,
        )

        if not resolution.ok:
            rej = resolution.rejection
            reason = f"{rej.code}: {rej.message}" if rej else "unknown rejection"
            logger.warning(f"Order rejected: {reason}")
            return OrderResult(ok=False, reason=reason)

        # ③ 执行每个 leg（一期只有一个 leg）
        last_result = None
        for venue_spec in resolution.specs:
            # 记账：accepted
            self._ledger.on_order_accepted(
                sym, self._tag, venue_spec.client_order_id,
                venue_spec.side, venue_spec.offset, venue_spec.qty,
            )

            # 提交到 venue
            submit_res = self._executor.submit(venue_spec)
            last_result = submit_res

            # ④ 按结果更新 ledger
            if submit_res.status == "FILLED":
                self._ledger.on_order_filled(
                    sym, self._tag, venue_spec.client_order_id,
                    venue_spec.side, venue_spec.offset, venue_spec.qty,
                    fill_price=submit_res.filled_price,
                    fill_qty=submit_res.filled_qty,
                )
                # 记录 ticket（cancel 用）
                if submit_res.order_ticket:
                    self._order_tickets[venue_spec.client_order_id] = submit_res.order_ticket
            elif submit_res.status == "IN_FLIGHT":
                # 挂单成功，保持 in_flight（等后续 poll 更新）
                if submit_res.order_ticket:
                    self._order_tickets[venue_spec.client_order_id] = submit_res.order_ticket
            else:
                # DEAD：释放敞口
                self._ledger.on_order_dead(
                    sym, self._tag, venue_spec.client_order_id,
                    venue_spec.side, venue_spec.offset, venue_spec.qty,
                )

        # ⑤ 构造返回
        if last_result is None:
            return OrderResult(ok=False, reason="no legs executed")

        if last_result.status == "FILLED":
            return OrderResult(
                ok=True,
                order_id=last_result.comment or "",
                filled_qty=last_result.filled_qty,
                filled_price=last_result.filled_price,
            )
        elif last_result.status == "IN_FLIGHT":
            return OrderResult(
                ok=True,
                order_id=str(last_result.order_ticket),
            )
        else:
            return OrderResult(
                ok=False,
                reason=f"[{last_result.retcode}] {last_result.comment}",
            )

    def cancel(self, order_id: str) -> bool:
        """撤销指定挂单。"""
        ticket = self._order_tickets.get(order_id)
        if ticket is None:
            # 尝试直接作为 ticket 解析
            try:
                ticket = int(order_id)
            except (ValueError, TypeError):
                logger.warning(f"cancel: unknown order_id {order_id}")
                return False
        return self._executor.cancel(ticket, self._symbol)

    def cancel_all(self, symbol: str = None) -> int:
        """撤销本策略该品种所有挂单。"""
        sym = symbol or self._symbol
        orders = self._executor.query_orders(sym)
        count = 0
        for o in orders:
            ticket = int(o.get("ticket", 0))
            if ticket and self._executor.cancel(ticket, sym):
                count += 1
        return count

    def flatten(self, symbol: str = None) -> dict:
        """一键清仓：撤销所有未成交挂单 + 平掉净持仓（市价）。

        运行到时/退出收尾用。以 venue 真实持仓为准（不依赖本地账本），
        确保孤儿仓被平掉。

        Returns:
            {"canceled": int, "net_vol": Decimal, "close": OrderResult|None}
        """
        sym = symbol or self._symbol
        result = {"canceled": 0, "net_vol": Decimal("0"), "close": None}

        # ① 撤销所有未成交挂单，并释放账本在途
        result["canceled"] = self.cancel_all(sym)
        self._ledger.clear_in_flight(sym, self._tag)

        # ② 以 venue 真实持仓为准计算净敞口
        positions = self._executor.query_positions(sym)
        net_vol = Decimal("0")
        ref_price = Decimal("0")
        for p in positions:
            vol = Decimal(str(p.get("volume", 0)))
            ptype = int(p.get("type", 0))  # 0=BUY, 1=SELL
            net_vol += vol if ptype == 0 else -vol
            ref_price = Decimal(str(p.get("price_open", 0))) or ref_price
        result["net_vol"] = net_vol

        if net_vol == 0:
            logger.info(f"[FLATTEN] {sym}: no net position, nothing to close")
            return result

        # ③ 对齐本地账本到 venue（避免 volume 不一致导致 CLOSE 被拒）
        self._ledger.sync_from_venue(sym, self._tag, net_vol, ref_price)

        # ④ 发反向市价单平掉
        if net_vol > 0:
            r = self.send_order(OrderSide.SELL, Offset.CLOSE, net_vol,
                                kind=OrderKind.MARKET, symbol=sym)
        else:
            r = self.send_order(OrderSide.BUY, Offset.CLOSE, abs(net_vol),
                                kind=OrderKind.MARKET, symbol=sym)
        result["close"] = r
        logger.info(
            f"[FLATTEN] {sym}: closed net_vol={net_vol} ok={r.ok} "
            f"filled={r.filled_qty}@{r.filled_price} reason={r.reason}"
        )
        return result

    # ═══════════ 辅助 ═══════════

    def log(self, msg: str, level: str = "INFO") -> None:
        """写策略日志（多品种共享日志时按 symbol 区分）。"""
        getattr(logger, level.lower(), logger.info)(f"[STRATEGY:{self._symbol}] {msg}")

    def now(self) -> int:
        """当前时间戳（Unix ms）。"""
        return self._feed.now_ms()

    @property
    def state(self) -> dict:
        """策略全局共享状态（跨周期通信用）。"""
        return self._state

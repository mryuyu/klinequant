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
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol

from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import (
    OrderRequest,
    Resolution,
    UnifiedResolver,
    VenueOrderSpec,
    derive_magic,
)
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
from strategy.sdk.state_store import StateStore

if TYPE_CHECKING:  # 仅类型标注，避免运行期耦合 order_journal / indicator_engine
    from core.indicator_engine.engine import IndicatorEngine
    from strategy.sdk.indic import IndicApi
    from strategy.sdk.order_journal import Journal

logger = logging.getLogger(__name__)


class DataFeedProtocol(Protocol):
    """数据源协议（Live / Backtest 各实现一套）"""

    def latest_tick(self, symbol: str) -> Optional[Tick]: ...
    def latest_bars(self, symbol: str, period: str, count: int) -> list: ...
    def wait_update(self, deadline: Optional[float] = None) -> bool: ...
    def is_changing(self, obj: Any, field: Optional[str] = None) -> bool: ...
    def now_ms(self) -> int: ...
    def seconds_since_update(self) -> float:
        """R5 断线闸门：距上次从 venue 收到数据的秒数（心跳年龄）。

        实盘 feed 以「驱动响应」为心跳（连上就每轮刷新，与行情是否变动无关），
        故能区分「断线」与「休市/清淡」；回测 feed 恒返 0.0（永不 stale）。
        """
        ...


class ExecutorProtocol(Protocol):
    """执行器协议（MT5 / Binance / Simulator 各实现一套）"""

    def submit(self, spec: VenueOrderSpec) -> Any: ...
    def cancel(self, order_ticket: int, symbol: str, magic: Optional[int] = None) -> bool: ...
    def query_positions(self, symbol: str = "", magic: Optional[int] = None) -> list: ...
    def query_account(self) -> Optional[dict]: ...
    def query_orders(self, symbol: str = "", magic: Optional[int] = None) -> list: ...
    def query_order_outcome(self, client_order_id: str, symbol: str = "") -> Optional[dict]:
        """R3 恢复：凭 client_order_id 向 venue 反查单笔订单真实结局。

        Returns:
            None                                  → 查无此单（调用方标 DEAD 释放）
            {"state": "OPEN",   "ticket": int}    → 挂单仍在（补 in_flight + ticket）
            {"state": "FILLED", "ticket": int,
             "filled_qty": Decimal, "filled_price": Decimal}  → 已成交（补 ledger 持仓）
            {"state": "DEAD",   "ticket": int, "reason": str} → 已终结（标 DEAD）
        Raises:
            连接异常向上抛（调用方据此判定 venue 不可达 → 重试+告警，不进策略循环）。
        """
        ...


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
        account_name: 绑定账户名（R1 身份化：magic 派生 + client_order_id 结构化）
        magic: 账户级显式 magic 覆盖（None=按 (account_name, tag) 派生）
        journal: 订单意图 WAL（R2，None=不落盘，回测/单测默认）
        state: 策略语义状态存储（R4，策略级共享一份；None=本实例独占内存态，回测/单测默认）
        stale_threshold: R5 断线闸门阈值（秒）；feed 心跳龄超此值时拒 OPEN、放 CLOSE；
                         None=不启用（回测/单测默认）
        engine: Phase 1 进程内 IndicatorEngine（runner 注入；None=api.INDIC() 不可用）
        exchange: 指标引擎 KlineKey 的 exchange 维度（默认 mt5；runner 按 backend 注入）
        engine_lock: 多品种线程共享同一 engine 时的串行化锁（拓扑 Z；None=本实例独占）
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
        account_name: str = "",
        magic: Optional[int] = None,
        journal: Optional["Journal"] = None,
        state: Optional[StateStore] = None,
        stale_threshold: Optional[float] = None,
        engine: Optional["IndicatorEngine"] = None,
        exchange: str = "mt5",
        engine_lock: Optional[Any] = None,
    ):
        self._symbol = symbol
        self._period = period
        self._tag = tag
        self._specs = specs
        self._ledger = ledger
        self._resolver = resolver
        self._executor = executor
        self._feed = feed
        # R1 身份化 magic：显式覆盖 > 按 (account, tag) 派生 > None（回落执行器实例级）
        self._account_name = account_name
        if magic is not None:
            self._magic: Optional[int] = magic
        elif account_name:
            self._magic = derive_magic(account_name, tag)
        else:
            self._magic = None
        # R2 订单意图 WAL（None=不落盘）
        self._journal = journal
        # R4 策略语义状态（None=本实例独占内存态；LiveRunner 注入策略级共享 StateStore）
        self._state: StateStore = state if state is not None else StateStore()
        # R5 断线闸门阈值（秒；None=不启用）
        self._stale_threshold = stale_threshold
        # Phase 1 声明式指标：进程内 IndicatorEngine（runner 注入；None=api.INDIC() 不可用）
        self._engine = engine
        self._exchange = exchange
        self._engine_lock = engine_lock
        self._indic: Optional["IndicApi"] = None
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
                self._advance_indicators()
                return True

            # 本轮无数据：有时限则继续等到点（休市/静默不误退出），无时限返回 False
            if self._run_until is None:
                return False

    def set_run_until(self, unix_ts: Optional[float]) -> None:
        """设置运行截止时间（Unix 秒）。None=不限时（运行到 Ctrl+C）。"""
        self._run_until = unix_ts

    def is_changing(self, obj, field: str = None) -> bool:
        """检查某对象自上次 wait_update 后是否有变化。

        obj 可为 tick / bars / 字符串 key，或 Phase 1 的 IndicatorView/FieldView
        （后者经 ``_kq_bar_key`` 映射到底层 bar 版本 key：bar 变即指标可能变）。
        """
        bar_key = getattr(obj, "_kq_bar_key", None)
        if bar_key is not None:
            return self._feed.is_changing(bar_key, field)
        return self._feed.is_changing(obj, field)

    # ═══════════ 声明式指标（Phase 1） ═══════════

    def INDIC(self) -> "IndicApi":  # noqa: N802  (规格定名：大写 api.INDIC())
        """声明式指标接口（计算全部下沉进程内 IndicatorEngine，四端同源同参）。

        返回的 IndicApi 惰性构造并缓存；``indic.macd(fast, slow, m)`` 等声明指标，
        返回 IndicatorView 活视图（``view.dif[-1]`` 标量比较、可迭代为序列、
        ``is_changing(view)`` 可用）。runner 未注入 engine 时调用抛 RuntimeError。
        """
        if self._indic is None:
            if self._engine is None:
                raise RuntimeError(
                    "api.INDIC() 需要 runner 注入 IndicatorEngine（当前未注入）"
                )
            from strategy.sdk.indic import IndicApi
            self._indic = IndicApi(
                self._engine, self._feed, self._symbol, self._exchange,
                self._period, lock=self._engine_lock,
            )
        return self._indic

    def _advance_indicators(self) -> None:
        """wait_update 桥接：本轮数据到达后推进已声明指标的引擎增量（Live/Backtest 同构）。"""
        if self._indic is not None:
            self._indic.advance()

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

        # R5 断线闸门：feed 心跳龄超阈（degraded）→ 拒新开仓、放行平仓。
        #   只减风险不加风险：断线时基于陈旧行情开新仓不可控，但平仓（flatten/CLOSE）永远放行。
        if self._stale_threshold is not None and offset == Offset.OPEN:
            age = self._feed.seconds_since_update()
            if age > self._stale_threshold:
                reason = (
                    f"stale guard: no market data for {age:.0f}s "
                    f"(> {self._stale_threshold:.0f}s threshold), OPEN rejected (feed degraded)"
                )
                logger.warning(f"[STALE-GUARD] {reason} [{sym}]")
                return OrderResult(ok=False, reason=reason)

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
            account=self._account_name,
            magic=self._magic,
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
            coid = venue_spec.client_order_id
            # 记账：accepted
            self._ledger.on_order_accepted(
                sym, self._tag, coid,
                venue_spec.side, venue_spec.offset, venue_spec.qty,
            )

            # WAL：先写后发（submit 之前落盘意图，崩溃后 R3 凭 client_order_id 反查）
            if self._journal is not None:
                self._journal.begin(
                    client_order_id=coid, account=self._account_name, tag=self._tag,
                    symbol=sym, side=venue_spec.side.value, offset=venue_spec.offset.value,
                    qty=venue_spec.qty, price=venue_spec.price,
                )

            # 提交到 venue（异常 → 结局未知，写 UNKNOWN，绝不静默；保守保持在途不释放）
            try:
                submit_res = self._executor.submit(venue_spec)
            except Exception as e:
                logger.error(f"submit exception for {coid}: {e}", exc_info=True)
                if self._journal is not None:
                    self._journal.finish(coid, "UNKNOWN", reason=f"submit exception: {e}")
                return OrderResult(ok=False, reason=f"submit exception: {e}")
            last_result = submit_res

            # WAL：落终态/在途态
            if self._journal is not None:
                self._journal.finish(
                    coid, submit_res.status,
                    ticket=submit_res.order_ticket or None,
                    filled_qty=submit_res.filled_qty,
                    filled_price=submit_res.filled_price,
                    reason=(
                        "" if submit_res.status in ("FILLED", "IN_FLIGHT")
                        else f"[{submit_res.retcode}] {submit_res.comment}"
                    ),
                )

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
        return self._executor.cancel(ticket, self._symbol, magic=self._magic)

    def cancel_all(self, symbol: str = None) -> int:
        """撤销本策略该品种所有挂单。"""
        sym = symbol or self._symbol
        orders = self._executor.query_orders(sym)
        count = 0
        for o in orders:
            ticket = int(o.get("ticket", 0))
            if ticket and self._executor.cancel(ticket, sym, magic=self._magic):
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

    def is_resumed(self) -> bool:
        """本次运行是否从上一轮持久化状态恢复（R4，True=崩溃/重启恢复）。

        策略据此决定恢复后是继续持有还是先 ``flatten()``（避免盲目加仓）。
        冷启动 / 回测 / 无状态后端时恒 False。
        """
        return self._state.is_resumed()

    @property
    def state(self) -> StateStore:
        """策略语义状态（跨周期/跨重启共享；dict 子类，变更自动 debounce 落盘）。

        红线：只存**不可从 venue 重算**的语义量（加仓计数、上次信号 bar 时间、
        跨周期趋势结论等）；禁止存持仓/在途/订单号（那些走 R1~R3 venue 对账）。
        runner 自动 load/save（安全网），亦可显式 ``api.state.save()`` 强制落盘。
        """
        return self._state

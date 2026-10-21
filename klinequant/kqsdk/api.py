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
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Protocol

from klinequant.core.risk_engine.rules.base import RiskCheckResult
from klinequant.core.trade_engine.ledger import ExposureLedger
from klinequant.core.trade_engine.resolver import (
    OrderRequest,
    UnifiedResolver,
    VenueOrderSpec,
    derive_magic,
)
from klinequant.kqsdk.state_store import StateStore
from klinequant.protocol.types import (
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

if TYPE_CHECKING:  # 仅类型标注，避免运行期耦合 order_journal / indicator_engine
    from klinequant.core.indicator_engine.engine import IndicatorEngine
    from klinequant.kqsdk.broadcaster import SignalBroadcaster
    from klinequant.kqsdk.indic import IndicApi
    from klinequant.kqsdk.order_journal import Journal

logger = logging.getLogger(__name__)


class DataFeedProtocol(Protocol):
    """数据源协议（Live / Backtest 各实现一套）"""

    def latest_tick(self, symbol: str) -> Optional[Tick]: ...
    def latest_bars(self, symbol: str, period: str, count: int) -> list: ...
    def wait_update(self, deadline: Optional[float] = None) -> bool: ...
    def wait_and_snapshot(
        self, deadline: float | None = None, symbol: str | None = None,
    ) -> tuple[bool, dict[str, int]]:
        """等待新数据并返回 (是否有更新, 等待起始的版本快照)。

        M3：clear→snapshot→wait 顺序避免吞掉并发 set。M4-a：快照返回调用方（每个
        KqApi 独立持有），消除多线程 wait_update 互相覆盖基线。M4-b：symbol 指定
        时等待品种级事件（只被自己品种唤醒）；None=全局事件。
        """
        ...
    def resolve_key(self, obj: Any, field: str | None = None) -> str | None:
        """对象 → 版本 key（含 M1 field 语义；None=无法解析）。"""
        ...
    def current_version(self, key: str) -> int:
        """某版本 key 的当前版本号（KqApi.is_changing 与 per-api 快照比对）。"""
        ...
    def is_changing(self, obj: Any, field: Optional[str] = None) -> bool: ...
    def now_ms(self) -> int: ...
    def seconds_since_update(self, symbol: str | None = None) -> float:
        """R5 断线闸门：距上次从 venue 收到数据的秒数（心跳年龄）。

        实盘 feed 以「驱动响应」为心跳（连上就每轮刷新，与行情是否变动无关），
        故能区分「断线」与「休市/清淡」；回测 feed 恒返 0.0（永不 stale）。

        M6：symbol 给定时返回**该品种**的心跳龄（品种级看门狗，一个品种停摆只冻结
        自己）；None 或未知品种回落全局心跳龄（后向兼容）。
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
        periods: list[str] | None = None,
        broadcaster: Optional["SignalBroadcaster"] = None,
        strategy_name: str = "",
        risk_adapter: Optional[Any] = None,
        profit_calc: Optional[Callable[[str, Decimal, Decimal], Optional[Decimal]]] = None,
    ):
        self._symbol = symbol
        self._period = period
        # 多策略框架：策略名（≠文件名）；日志前缀 / risk_check strategy_id 用它。
        #   未注入（旧构造路径）时回落 tag（单策略场景 tag 即身份）。
        self._strategy_name = strategy_name or tag
        # 风控内核（SdkRiskAdapter，runner 注入；None=risk_check 降级本地检查，
        #   回测/单测零破坏）
        self._risk_adapter = risk_adapter
        # 交叉盘换算钩子 profit_calc(symbol, price_delta, volume)→账户币盈亏
        #   （实盘 = backend.profit_calc 走 MT5 order_calc_profit；None=按 spec 名义回落）
        self._profit_calc = profit_calc
        # M4 多周期：本 KqApi（绑定一个 symbol）可访问该 symbol 的多个 period；
        #   self._period 为主/默认周期（period=None 时的回落）。
        self._periods: list[str] = list(periods) if periods else [period]
        self._tag = tag
        self._specs = specs
        self._ledger = ledger
        self._resolver = resolver
        self._executor = executor
        self._feed = feed
        self._account_name = account_name
        # R1 身份化 magic：显式覆盖 > 按 (account, tag) 派生 > None（回落执行器实例级）。
        #   M4：多周期同账户必然多 magic（tag 含 period）——显式 period 时按
        #   (account, `{tag}:{period}`) 派生（见 _magic_for）；self._magic 为默认(period=None)
        #   口径，保留作 cancel 等的实例级回落。
        self._magic_override: int | None = magic
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
        # M4 多周期：每 period 一个 IndicApi（惰性构造缓存）；advance 时全部推进
        self._indics: dict[str, IndicApi] = {}
        # 订单 ticket 跟踪（cancel 用）
        self._order_tickets: Dict[str, int] = {}  # client_order_id → MT5 ticket
        # 运行截止时间（Unix 秒）：到点后 wait_update 返回 False，策略优雅退出
        self._run_until: Optional[float] = None
        # M4-a：is_changing 基线快照 per-api 独立持有（拓扑 Z 多线程各自 wait_update
        #   互不覆盖；旧版存 feed 级单一字段会在多品种实盘互相污染基线）
        self._snapshot: dict[str, int] = {}
        # Phase 3 信号分发：lead 侧广播器（None=不分发，standalone/follower 默认）。
        #   send_order/cancel_all 逐条广播白名单 intent；flatten 广播单条 FLATTEN 并
        #   抑制内部 per-leg 广播（_suppress_broadcast>0 时 send_order/cancel_all 不再单独发）。
        self._broadcaster = broadcaster
        self._suppress_broadcast = 0

    # ══════════ 前端 web 栈开关（态③）══════════

    def web(
        self,
        run: bool = True,
        *,
        host: str = "127.0.0.1",
        port: int = 8000,
        open_browser: bool = True,
        open_path: str = "/app/",
    ) -> None:
        """策略运行时是否拉起前端 web 栈（态③ = 策略 + GUI；态② = headless）。

        用法（策略函数内）::

            def strategy(api: KqApi):
                api.web(run=True)     # 跑策略同时后台起 gateway + 前端
                while api.wait_update():
                    ...

        - run=True：后台线程幂等拉起 gateway（复用 webstack，进程级单例，
          多策略/多 KqApi 共享同一栈，不会重复占端口）；策略主循环继续自驱动。
        - run=False：若已拉起则优雅关停，否则无操作（headless）。
        - 惰性导入 webstack：run=False 的 headless 策略不会拉入 uvicorn/fastapi。

        信号约束：后台 web 栈不注册信号处理器（_NoSignalServer），策略主线程的
        SIGINT/SIGTERM 仍由 LiveRunner 统一管理。
        """
        from klinequant import webstack

        if run:
            webstack.ensure_background(
                host, port, open_browser=open_browser, open_path=open_path
            )
        else:
            webstack.stop_background()

    # ═════════ M4 多周期记账：tag / magic 派生 ═════════

    def _tag_for(self, period: str | None = None) -> str:
        """记账 tag（订单系统 v2 §9.2：各 (symbol,period) 独立开平、互不干扰）。

        M4：显式 period → ``{base}:{period}``（多周期各自独立 tag/magic）；
        period=None → base tag（后向兼容单周期与既有口径）。
        """
        return f"{self._tag}:{period}" if period is not None else self._tag

    def _magic_for(self, period: str | None = None) -> int | None:
        """按 period 派生 magic（R1）。

        period=None → self._magic（默认口径，实例级回落）；显式 period →
        显式覆盖优先，否则按 (account, `{tag}:{period}`) 派生（account 空则 None）。
        """
        if period is None:
            return self._magic
        if self._magic_override is not None:
            return self._magic_override
        if self._account_name:
            return derive_magic(self._account_name, self._tag_for(period))
        return None

    # ═══════════ 流程控制 ═══════════

    def wait_update(self, deadline: float = None) -> bool:
        """阻塞等待下一次数据/事件更新。

        设置了运行截止时间（set_run_until）时：中途无数据的超时会自动重试
        （周末休市/盘中静默不会误退出），直到有数据返回 True，或到点返回 False。
        未设截止时间时，超时返回 False（旧语义）。

        拓扑 Z 循环纪律（M6 队头阻塞缓解，策略作者硬约束）：
          ① 本循环体内**禁阻塞调用与重计算**（同步 IO/sleep/全市场扫描）——每品种
             一工作线程 OS 级并行，违反后果收窄为「只坑本品种」（不再拖垮全策略），
             但仍会延迟本品种 S/L 及时反应，故必守；
          ② 指标计算全部下沉 IndicatorEngine 后台增量推进（api.INDIC()），策略只读结果；
          ③ 全市场扫描类重计算在框架侧批处理，策略只消费；
          ④ 看门狗已回到品种级：send_order 的 stale 闸门按本品种心跳判定
             （seconds_since_update(symbol)），一个品种停摆只冻结自己（禁 OPEN、放 CLOSE）。

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

            got, snap = self._feed.wait_and_snapshot(timeout, self._symbol)
            self._snapshot = snap          # M4-a：per-api 基线（is_changing 比对）
            if got:
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
        target = getattr(obj, "_kq_bar_key", None) or obj
        key = self._feed.resolve_key(target, field)
        if key is None:
            return False
        # M4-a：与 per-api 快照比对（不再依赖 feed 级共享 snapshot_versions）
        return self._feed.current_version(key) != self._snapshot.get(key, 0)

    # ═══════════ 声明式指标（Phase 1） ═══════════

    def INDIC(self, period: str = None) -> "IndicApi":  # noqa: N802  (规格定名：大写 api.INDIC())
        """声明式指标接口（计算全部下沉进程内 IndicatorEngine，四端同源同参）。

        返回的 IndicApi 惰性构造并**按 period 缓存**（M4 多周期：``api.INDIC("1h")``
        与 ``api.INDIC("15m")`` 各一个实例，默认 period=主周期）；``indic.macd(...)``
        等声明指标返回 IndicatorView 活视图（``view.dif[-1]`` 标量比较、可迭代为序列、
        ``is_changing(view)`` 可用）。runner 未注入 engine 时调用抛 RuntimeError。
        """
        per = period or self._period
        indic = self._indics.get(per)
        if indic is None:
            if self._engine is None:
                raise RuntimeError(
                    "api.INDIC() 需要 runner 注入 IndicatorEngine（当前未注入）"
                )
            from klinequant.kqsdk.indic import IndicApi
            indic = IndicApi(
                self._engine, self._feed, self._symbol, self._exchange,
                per, lock=self._engine_lock,
            )
            self._indics[per] = indic
        return indic

    def _advance_indicators(self) -> None:
        """wait_update 桥接：本轮数据到达后推进各周期已声明指标的引擎增量（Live/Backtest 同构）。"""
        for indic in self._indics.values():
            indic.advance()

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

    def position(self, symbol: str = None, period: str = None) -> Position:
        """本策略的持仓视图 = 已成交 + 在途。

        M4：period 指定 → 只返回该周期 tag(``{base}:{period}``) 的仓（各周期独立记账）；
        period=None → base tag（后向兼容）。交易所净持仓见 net_position（各 tag 之和）。
        """
        sym = symbol or self._symbol
        return self._ledger.position(sym, self._tag_for(period))

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
        sl: Decimal = None,
        tp: Decimal = None,
        tif: Tif = None,
        symbol: str = None,
        period: str = None,
    ) -> OrderResult:
        """下单。唯一交易入口。

        流程：构造 OrderRequest → Resolver 验证/量化 → Ledger 记账 → Executor 执行 → 更新 Ledger

        M4：period 指定 → 按 (symbol,period) 独立 tag(``{base}:{period}``) + 派生 magic
        记账/下单（各周期各自开平、venue 侧按 magic 隔离）；period=None → base 口径（兼容）。

        M5：sl/tp 为策略给定的灾难保险丝价位（仅 OPEN 生效，开仓时设一次不改）：
        MT5 走 position 属性（随持仓生存亡、本地平仓后自动失效）；币安走
        STOP_MARKET/TAKE_PROFIT_MARKET + reduceOnly。正常行情下策略本地 trailing SL
        先触发，保险丝只在进程死亡期间兜底。None=不设保险丝。
        """
        sym = symbol or self._symbol
        tag = self._tag_for(period)        # M4：per-(symbol,period) 记账 tag
        magic = self._magic_for(period)    # M4：per-period magic（venue 侧按 magic 隔离）

        # R5 断线闸门：feed 心跳龄超阈（degraded）→ 拒新开仓、放行平仓。
        #   只减风险不加风险：断线时基于陈旧行情开新仓不可控，但平仓（flatten/CLOSE）永远放行。
        if self._stale_threshold is not None and offset == Offset.OPEN:
            age = self._feed.seconds_since_update(sym)   # M6：品种级心跳（只冻结本品种）
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
            tag=tag,
            side=side,
            offset=offset,
            qty=qty,
            kind=kind,
            price=price,
            stop_price=stop_price,
            sl=sl,
            tp=tp,
            tif=tif,
            account=self._account_name,
            magic=magic,
        )

        # ② Resolver 验证 + 量化
        pos = self._ledger.position(sym, tag)
        has_dup = self._ledger.has_in_flight_open(sym, tag, side)
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
                sym, tag, coid,
                venue_spec.side, venue_spec.offset, venue_spec.qty,
            )

            # WAL：先写后发（submit 之前落盘意图，崩溃后 R3 凭 client_order_id 反查）
            if self._journal is not None:
                self._journal.begin(
                    client_order_id=coid, account=self._account_name, tag=tag,
                    symbol=sym, side=venue_spec.side.value, offset=venue_spec.offset.value,
                    qty=venue_spec.qty, price=venue_spec.price,
                )

            # Phase 3：向 followers 广播白名单下单意图（fire-and-forget，绝不阻断本地成交）。
            #   flatten 内部调用的 CLOSE leg 由外层单条 FLATTEN 覆盖，故此处抑制。
            if self._broadcaster is not None and self._suppress_broadcast == 0:
                try:
                    self._broadcaster.broadcast_order(
                        client_order_id=coid,
                        tag=venue_spec.tag or tag,
                        symbol=sym,
                        side=venue_spec.side.value,
                        offset=venue_spec.offset.value,
                        qty=venue_spec.qty,
                        kind=venue_spec.kind.value,
                        price=venue_spec.price,
                        stop_price=venue_spec.stop_price,
                        sl=venue_spec.sl,
                        tp=venue_spec.tp,
                        tif=venue_spec.tif.value,
                    )
                except Exception as e:  # 分发失败仅告警，不影响 lead 本地下单
                    logger.warning(f"[DIST] broadcast_order failed for {coid}: {e}")

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
                    sym, tag, venue_spec.client_order_id,
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
                    sym, tag, venue_spec.client_order_id,
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

    def cancel_all(self, symbol: str = None, period: str = None) -> int:
        """撤销本策略该品种挂单。

        M4：period=None → 撤本品种全部挂单（孤儿清理安全网，query 不按 magic 过滤、
        cancel 用实例级 self._magic，沿用旧口径）；period 指定 → 只撤该周期 magic 的挂单。
        """
        sym = symbol or self._symbol
        qmagic = None if period is None else self._magic_for(period)
        cmagic = self._magic_for(period)
        # Phase 3：广播撤挂单意图（flatten 内部调用时由外层 FLATTEN 覆盖，故抑制）。
        if self._broadcaster is not None and self._suppress_broadcast == 0:
            try:
                self._broadcaster.broadcast_cancel(
                    client_order_id=f"cancel-{sym}-{int(time.time() * 1000)}",
                    tag=self._tag_for(period), symbol=sym,
                )
            except Exception as e:  # 分发失败仅告警
                logger.warning(f"[DIST] broadcast_cancel failed for {sym}: {e}")
        orders = self._executor.query_orders(sym, qmagic)
        count = 0
        for o in orders:
            ticket = int(o.get("ticket", 0))
            if ticket and self._executor.cancel(ticket, sym, magic=cmagic):
                count += 1
        return count

    def flatten(self, symbol: str = None, period: str = None) -> dict:
        """一键清仓：撤销所有未成交挂单 + 平掉净持仓（市价）。

        运行到时/退出收尾用。以 venue 真实持仓为准（不依赖本地账本），
        确保孤儿仓被平掉。

        M4：period=None → 平掉本品种全部 venue 净持仓（query 不按 magic 过滤，孤儿清理
        安全网，多周期退出时一次平净，沿用旧口径）；period 指定 → 只平该周期 magic 的仓。

        Returns:
            {"canceled": int, "net_vol": Decimal, "close": OrderResult|None}
        """
        sym = symbol or self._symbol
        tag = self._tag_for(period)
        pmagic = None if period is None else self._magic_for(period)
        result = {"canceled": 0, "net_vol": Decimal("0"), "close": None}

        # Phase 3：广播单条 FLATTEN 意图（follower 收到后独立平自己的净持仓，
        #   而非复制 lead 的具体 CLOSE 量，因 follower 仓位已按 scale 缩放）；同时
        #   抑制内部 cancel_all/send_order 的 per-leg 广播，避免与 FLATTEN 语义冲突。
        if self._broadcaster is not None:
            try:
                self._broadcaster.broadcast_flatten(
                    client_order_id=f"flatten-{sym}-{int(time.time() * 1000)}",
                    tag=tag, symbol=sym,
                )
            except Exception as e:  # 分发失败仅告警
                logger.warning(f"[DIST] broadcast_flatten failed for {sym}: {e}")
        self._suppress_broadcast += 1
        try:
            # ① 撤销所有未成交挂单，并释放账本在途
            result["canceled"] = self.cancel_all(sym, period)
            self._ledger.clear_in_flight(sym, tag)

            # ② 以 venue 真实持仓为准计算净敞口
            positions = self._executor.query_positions(sym, pmagic)
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
            self._ledger.sync_from_venue(sym, tag, net_vol, ref_price)

            # ④ 发反向市价单平掉
            if net_vol > 0:
                r = self.send_order(OrderSide.SELL, Offset.CLOSE, net_vol,
                                    kind=OrderKind.MARKET, symbol=sym, period=period)
            else:
                r = self.send_order(OrderSide.BUY, Offset.CLOSE, abs(net_vol),
                                    kind=OrderKind.MARKET, symbol=sym, period=period)
            result["close"] = r
            logger.info(
                f"[FLATTEN] {sym}: closed net_vol={net_vol} ok={r.ok} "
                f"filled={r.filled_qty}@{r.filled_price} reason={r.reason}"
            )
            return result
        finally:
            self._suppress_broadcast -= 1

    # ═══════════ 资金管理 + 风控助手（策略侧，不强制拦截 send_order）═══════════

    def _account_equity(self) -> Decimal:
        """当前账户权益（账户币）= balance + 浮动盈亏。"""
        acc = self.account()
        return acc.total_balance + acc.unrealized_pnl

    def _loss_per_unit(self, sym: str, stop_distance: Decimal) -> Decimal:
        """每单位量（1 手 / 1 币）在 stop_distance 价格变动下的账户币损失（正数）。

        优先用注入的 profit_calc（实盘 MT5 order_calc_profit 精确含交叉盘换算）；
        无钩子或钩子失败时回落 spec 名义计算（stop_distance × contract_multiplier，
        假设 quote≈账户币，回测/单测常见路径）。
        """
        if self._profit_calc is not None:
            try:
                profit = self._profit_calc(sym, stop_distance, Decimal("1"))
                if profit is not None:
                    val = abs(Decimal(str(profit)))
                    if val > 0:
                        return val
            except Exception as e:  # 换算失败仅告警，回落名义计算
                logger.warning(f"[CALC_QTY] profit_calc({sym}) failed, fallback: {e}")
        spec = self._spec_for(sym)
        mult = spec.contract_multiplier or Decimal("1")
        return stop_distance * mult

    def calc_qty(
        self,
        symbol: str = None,
        *,
        risk_amount: Decimal | float | str | None = None,
        risk_pct: Decimal | float | str | None = None,
        stop_distance: Decimal | float | str,
    ) -> Decimal:
        """按止损风险反推下单量（需求 5：单笔最大固定金额 / 百分比）。

        风险额二选一（都给时 risk_amount 优先）：
          - ``risk_amount``：固定金额（账户币）；
          - ``risk_pct``：账户权益的百分比（如 1.0 = 1%）。

        qty = 风险额 / 每单位止损损失，再按 ``qty_step`` 向下取整、钳制到
        ``[min_qty, qty_max]``；不足 ``min_qty`` 返回 0（拒开，仓位太小无意义）。

        Returns:
            Decimal 下单量（0 = 风险额过小 / 参数非法，应拒开）。
        """
        sym = symbol or self._symbol
        spec = self._spec_for(sym)
        stop = Decimal(str(stop_distance))
        if stop <= 0:
            logger.warning(f"[CALC_QTY] {sym}: stop_distance<=0 ({stop})，返回 0")
            return Decimal("0")

        if risk_amount is not None:
            risk = Decimal(str(risk_amount))
        elif risk_pct is not None:
            risk = self._account_equity() * Decimal(str(risk_pct)) / Decimal("100")
        else:
            raise ValueError("calc_qty 需要 risk_amount 或 risk_pct 之一")
        if risk <= 0:
            logger.warning(f"[CALC_QTY] {sym}: 风险额<=0 ({risk})，返回 0")
            return Decimal("0")

        loss_per_unit = self._loss_per_unit(sym, stop)
        if loss_per_unit <= 0:
            logger.warning(f"[CALC_QTY] {sym}: 每单位损失<=0，返回 0")
            return Decimal("0")

        raw = risk / loss_per_unit
        step = spec.qty_step or Decimal("0")
        if step > 0:
            qty = (raw / step).to_integral_value(rounding=ROUND_DOWN) * step
        else:
            qty = raw

        min_qty = spec.min_qty or Decimal("0")
        if qty < min_qty:
            logger.info(
                f"[CALC_QTY] {sym}: qty {qty} < min_qty {min_qty}，拒开（返回 0）"
            )
            return Decimal("0")
        if spec.qty_max and spec.qty_max > 0 and qty > spec.qty_max:
            qty = spec.qty_max
        return qty

    def risk_check(
        self,
        symbol: str,
        side: OrderSide,
        offset: Offset,
        qty: Decimal,
        *,
        kind: OrderKind = OrderKind.MARKET,
        price: Decimal = None,
        period: str = None,
    ) -> RiskCheckResult:
        """下单前风控预检（需求 7：全局风控汇总；策略侧助手，不强制拦截）。

        内核 = runner 注入的共享 SdkRiskAdapter（RiskEngine + 全局状态供给）；
        未注入时（回测/单测）降级为本地检查（qty>0 且 >= min_qty）。

        策略约定：``risk_check(...).passed`` 为 True 才 ``send_order(...)``。
        """
        sym = symbol or self._symbol
        tag = self._tag_for(period)
        if self._risk_adapter is None:
            return self._local_risk_check(sym, qty)
        last_price = None
        if price is None:
            tick = self._feed.latest_tick(sym)
            last_price = tick.last_price if tick is not None else None
        try:
            return self._risk_adapter.check(
                symbol=sym, tag=tag, side=side, offset=offset, qty=qty,
                kind=kind, price=price, last_price=last_price,
                account=self.account(),
            )
        except Exception as e:  # 风控内核异常 → fail-closed（拒绝，只影响本次决策）
            logger.error(f"[RISK] risk_check({sym}) 异常，fail-closed 拒绝: {e}", exc_info=True)
            return RiskCheckResult.reject("risk_adapter", f"exception: {e}", "CRITICAL")

    def _local_risk_check(self, sym: str, qty: Decimal) -> RiskCheckResult:
        """无 RiskEngine 注入时的本地降级检查（回测/单测零破坏）。"""
        if qty is None or qty <= 0:
            return RiskCheckResult.reject("local", f"invalid qty {qty}")
        try:
            spec = self._spec_for(sym)
        except KeyError:
            return RiskCheckResult.ok("local")
        min_qty = spec.min_qty or Decimal("0")
        if min_qty > 0 and qty < min_qty:
            return RiskCheckResult.reject("local", f"qty {qty} < min_qty {min_qty}")
        return RiskCheckResult.ok("local")

    # ═══════════ 辅助 ═══════════

    def log(self, msg: str, level: str = "INFO") -> None:
        """写策略日志（带策略名 + 品种前缀，多策略/多品种共享日志时可区分）。"""
        getattr(logger, level.lower(), logger.info)(
            f"[STRATEGY:{self._strategy_name}:{self._symbol}] {msg}"
        )

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

"""LiveRunner — 实盘策略运行器（市场无关，单进程多策略）

职责（市场相关逻辑全部委托注入的 MarketBackend）：
  1. backend.connect()（建立市场连接）
  2. backend.load_specs()（加载全策略品种并集的 SymbolInfo）
  3. backend.make_executor() + 本地 ExposureLedger / UnifiedResolver
  4. assemble_risk()（按 YAML risk 段装配共享 RiskEngine，需求 7 全局风控汇总）
  5. backend.reconcile_positions()（逐策略启动对账）
  6. backend.make_feed()（订阅 = 全策略 symbols × periods 并集）
  7. 每 (策略, 品种) 构造 KqApi
  8. 运行策略函数（策略内 while api.wait_update() 循环）
  9. 优雅退出（Ctrl+C / 异常 / 到时），逐策略逐品种清仓，backend.shutdown()

两种装配方式：
    # ① 单策略（旧构造参数，行为完全等价；内部归一为单元素策略列表）
    runner = LiveRunner(Mt5Backend(), symbols="EURUSD", period="1m",
                        strategy_fn=my_strategy)
    # ② 多策略（配置驱动，需求 6/7/11）：共享账本/执行器/风控/连接，
    #    每策略独立 tag/StateStore/KqApi/工作线程；策略名可自定义（≠文件名）
    cfg = load_strategies_config("config/strategies.yaml")
    runner = LiveRunner.from_config(cfg, Mt5Backend(), account_name="fx-demo")
    runner.run()

多策略共享层（全局统一账户/持仓/订单视图，需求 7）：
    一个 backend/连接、一个 executor、一个 ExposureLedger（tag=策略名隔离）、
    一个 IndicatorEngine+锁、一个 SqliteJournal、一个 feed、一个 RiskEngine。
每策略独立：StateStore（key=``{account}:{strategy_name}``）、每 (strategy, symbol)
    一个 KqApi + 一个工作线程（沿用拓扑 Z，异常只影响本线程）。

崩溃恢复与进程守护（Phase R）：
  Runner 自身不含看门狗/自动重启——那属进程外守护层，与代码解耦。重启后由
  _initialize → _recover()（R3）自动收敛：凭 R2 WAL 的 client_order_id 逐条
  向 venue 反查在途意图，补 ledger/ticket，再 reconcile_positions 对账净持仓；
  R4 StateStore 自动 load 语义快照（api.is_resumed() 供策略辨冷启动/恢复）。
  多策略恢复按 tag 分组（journal.pending 已按 tag 过滤，各策略天然隔离）。

  Windows 托管本进程三选一（崩溃/重启即自动拉起，拉起即跑上述恢复流程）：
    1. NSSM（推荐，服务化）：install 服务指向 venv 的 python.exe + 启动脚本，
       设 AppDirectory=klinequant、AppExit Default Restart、AppRestartDelay 5000。
    2. 计划任务：schtasks /SC ONSTART /RL HIGHEST /TR "<python> <脚本>"，
       并在任务设置里勾选「失败后按频率重新启动」。
    3. 守护父进程：while 循环 supervisor 反复 spawn 本脚本，子进程退出即重启。

  停电/硬杀（-9）：WAL 未及 fsync 最坏丢 ~1 秒意图，但 R1 身份化 + 重启全量
  对账兜底——venue 才是持仓/订单真相源，本地 WAL 只加速收敛，缺一条也能补齐。
"""
from __future__ import annotations

import inspect
import logging
import signal
import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Union

from core.indicator_engine.engine import IndicatorEngine
from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import UnifiedResolver, derive_magic
from kqsdk.api import DataFeedProtocol, ExecutorProtocol, KqApi
from kqsdk.backend import MarketBackend
from kqsdk.order_id import OrderIdFactory
from kqsdk.order_journal import STATE_DEAD, STATE_FILLED, STATE_IN_FLIGHT
from kqsdk.registry import (
    StrategyContext,
    resilient,
    resolve_strategy_fn,
)
from kqsdk.risk_adapter import SdkRiskAdapter, assemble_risk
from kqsdk.state_store import StateBackend, StateStore
from protocol.types import Offset, OrderSide, SymbolInfo

if TYPE_CHECKING:  # 仅类型标注，避免运行期耦合 order_journal / broadcaster / config
    from config.strategy_config import RiskConfig, StrategiesConfig
    from kqsdk.broadcaster import SignalBroadcaster
    from kqsdk.order_journal import Journal

logger = logging.getLogger(__name__)


def _fn_takes_ctx(fn: Callable) -> bool:
    """策略函数是否接收第二参 ctx（``strategy(api, ctx)`` 双参形态）。"""
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    return len(sig.parameters) >= 2


def _make_runner(
    fn: Callable,
    ctx: Optional[StrategyContext],
    *,
    resilient_on: bool,
    max_errors: int,
) -> Callable[[KqApi], None]:
    """把策略函数绑定为 ``(api) -> None`` 工作线程体。

    - 双参形态 ``fn(api, ctx)``（ctx 非 None 且签名收 2 参）→ 传上下文；
      单参形态 ``fn(api)``（旧写法）→ 只传 api。
    - ``resilient_on`` → 套迭代级容错包装（需求 8/10：单次迭代异常打日志续跑，
      连续超 ``max_errors`` 才退出本策略线程，不拖垮其他策略/品种）。
    """
    takes_ctx = ctx is not None and _fn_takes_ctx(fn)

    def call(api: KqApi) -> None:
        if takes_ctx:
            fn(api, ctx)
        else:
            fn(api)

    if resilient_on:
        call = resilient(max_consecutive=max_errors, logger_obj=logger)(call)
    return call


@dataclass
class _StrategyInstance:
    """单个策略实例的运行态（多策略：每实例一份 tag/state/apis/线程）。"""

    name: str                                   # 策略名（=敞口 tag 基名 / StateStore key）
    symbols: List[str]
    period: str
    periods: List[str]
    runner: Callable[[KqApi], None]             # 绑定后的 (api)->None 工作线程体
    ctx: Optional[StrategyContext] = None
    max_errors: int = 20
    state_store: Optional[StateStore] = None
    apis: Dict[str, KqApi] = field(default_factory=dict)


class LiveRunner:
    """实盘策略运行器（单进程多策略，市场无关）"""

    def __init__(
        self,
        backend: MarketBackend,
        symbols: Union[str, List[str]],
        period: str,
        strategy_fn: Callable[[KqApi], None],
        *,
        tag: str = "",
        periods: list[str] | None = None,
        poll_interval: float = 0.5,
        bar_count: int = 300,
        duration: Optional[float] = None,
        account_name: str = "",
        magic: Optional[int] = None,
        journal: Optional["Journal"] = None,
        recover_retries: int = 3,
        recover_backoff: float = 2.0,
        state_backend: Optional[StateBackend] = None,
        state_debounce: float = 1.0,
        stale_threshold: Optional[float] = None,
        broadcaster: Optional["SignalBroadcaster"] = None,
    ):
        """
        Args:
            backend: 市场后端（MarketBackend），封装连接/规格/执行器/数据源/对账/关闭；
                     FX 用 Mt5Backend，加密用 BinanceBackend（同构复用本 Runner）
            symbols: 交易品种，单个 str 或列表（如 "EURUSD" 或 ["EURUSD","GBPUSD"]）；
                     多品种共享一个进程/一个连接/一个数据源，每品种一个工作线程，
                     列表首个为主品种（KqApi symbol=None 的回落）
            period: 驱动周期（如 "1m"）
            strategy_fn: 策略函数，签名 (api: KqApi) -> None；每品种各跑一份
            tag: 策略标识（敞口账本隔离用，默认=period）
            poll_interval: 数据轮询间隔（秒）
            bar_count: 加载的 K 线数量
            duration: 运行时长（秒），到点自动清仓退出；None/0=不限时
            account_name: 绑定账户名（R1 身份化）；空=从 backend.account_name 读取
            magic: 账户级显式 magic 覆盖；None=从 backend.account_magic 读取
            journal: 订单意图 WAL（R2，None=不落盘；传入后 seq 与 id 生成器同源）
            recover_retries: R3 恢复期 venue 可达性探测重试次数（超限仍不可达则拒起）
            recover_backoff: R3 可达性探测重试间隔（秒）
            state_backend: R4 策略语义状态持久化后端（None=仅内存不落盘，回测/单测默认）
            state_debounce: R4 状态变更 debounce 落盘节流窗（秒，最坏丢此时长）
            stale_threshold: R5 断线闸门阈值（秒）；feed 心跳龄超此值拒 OPEN、放 CLOSE；
                             None=不启用（回测/单测默认；实盘脚本经 --stale-guard 注入）
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        syms: List[str] = [s.upper() for s in symbols]
        if not syms:
            raise ValueError("LiveRunner requires at least one symbol")
        base_tag = tag or period
        # M4 多周期订阅：periods 给出全部订阅周期（含主 period）；单周期时=[period]。
        if periods:
            strat_periods: list[str] = list(periods)
            if period not in strat_periods:
                strat_periods.insert(0, period)
        else:
            strat_periods = [period]

        self._backend = backend
        self._poll_interval = poll_interval
        self._bar_count = bar_count
        self._duration = duration
        # R1 身份化：账户名/magic 优先取显式参数，否则从 backend 账户配置读取
        self._account_name = account_name or getattr(backend, "account_name", "") or ""
        self._magic = magic if magic is not None else getattr(backend, "account_magic", None)
        self._journal = journal  # R2 订单意图 WAL（None=不落盘）
        self._recover_retries = max(1, recover_retries)
        self._recover_backoff = recover_backoff
        # R4 策略语义状态（策略级共享一份，backend=None 则仅内存）
        self._state_backend = state_backend
        self._state_debounce = state_debounce
        # R5 断线闸门阈值（秒；None=不启用）——透传给每个 KqApi，feed degraded 时拒 OPEN
        self._stale_threshold = stale_threshold
        # Phase 3 信号分发：lead 侧广播器（None=不分发）
        self._broadcaster = broadcaster
        # 多策略风控：单策略旧路径不装配 RiskEngine（risk_check 降级本地检查）
        self._risk_cfg: Optional["RiskConfig"] = None
        self._force_derive_magic = False   # from_config 多策略时强制按 tag 派生 magic（隔离）

        # 单策略旧路径：归一为单元素策略列表（不套 resilient，行为完全等价旧实现）
        self._strategies: List[_StrategyInstance] = [
            _StrategyInstance(
                name=base_tag,
                symbols=syms,
                period=period,
                periods=strat_periods,
                runner=_make_runner(strategy_fn, None, resilient_on=False, max_errors=0),
                ctx=None,
            )
        ]

        # 兼容属性（旧脚本/日志口径；单策略路径下等价原字段）
        self._symbols: List[str] = syms
        self._symbol: str = syms[0]
        self._period: str = period
        self._tag: str = base_tag
        self._periods: list[str] = strat_periods
        self._strategy_fn = strategy_fn

        # 组件（run 时初始化）
        self._specs: Dict[str, SymbolInfo] = {}
        self._executor: Optional[ExecutorProtocol] = None
        self._ledger: Optional[ExposureLedger] = None
        self._resolver: Optional[UnifiedResolver] = None
        self._id_factory: Optional[OrderIdFactory] = None
        self._feed: Optional[DataFeedProtocol] = None
        # Phase 1 进程内指标引擎（拓扑 Z：全策略全品种共享一个 engine + 一把串行化锁）
        self._engine: Optional[IndicatorEngine] = None
        self._engine_lock = threading.Lock()
        self._exchange = getattr(backend, "exchange", "mt5")
        self._risk_adapter: Optional[SdkRiskAdapter] = None
        self._running = False

    # ─── 多策略装配入口（配置驱动，需求 6/7/11）───

    @classmethod
    def from_config(
        cls,
        cfg: "StrategiesConfig",
        backend: MarketBackend,
        *,
        account_name: str = "",
        journal: Optional["Journal"] = None,
        state_backend: Optional[StateBackend] = None,
        state_debounce: float = 1.0,
        only: Optional[List[str]] = None,
        recover_retries: int = 3,
        recover_backoff: float = 2.0,
        broadcaster: Optional["SignalBroadcaster"] = None,
    ) -> "LiveRunner":
        """按 StrategiesConfig 构造多策略运行器（共享账本/执行器/风控/连接）。

        Args:
            cfg: 已加载校验的策略配置（load_strategies_config）
            backend: 市场后端（调用方按 account.market 构造 Mt5/Binance）
            account_name: 绑定账户名（R1 身份化 + StateStore key + magic 派生）；
                          空则从 backend.account_name 读取
            journal / state_backend: R2 WAL / R4 状态后端（跨策略共享一份）
            only: 只运行这些策略名（None=全部 enabled 策略）
        """
        d = cfg.defaults
        enabled = cfg.enabled_strategies()
        if only:
            wanted = set(only)
            enabled = [s for s in enabled if s.name in wanted]
        if not enabled:
            raise ValueError("from_config: 没有可运行的策略（检查 enabled / --only）")

        acct = account_name or getattr(backend, "account_name", "") or ""
        # 品种/周期并集（feed 订阅 + load_specs；KqApi 各自绑定自己的 symbol/periods）
        all_symbols: List[str] = []
        all_periods: List[str] = []
        for s in enabled:
            for sym in s.symbols:
                if sym not in all_symbols:
                    all_symbols.append(sym)
            for p in s.periods:
                if p not in all_periods:
                    all_periods.append(p)

        runner = cls(
            backend,
            symbols=all_symbols,
            period=enabled[0].period,
            strategy_fn=lambda api: None,   # 占位；多策略实际用各实例 runner
            tag=enabled[0].name,
            periods=all_periods,
            poll_interval=d.poll_interval,
            bar_count=d.bar_count,
            duration=d.duration if d.duration and d.duration > 0 else None,
            account_name=acct,
            magic=None,                     # 多策略强制按 (account, tag) 派生 magic（隔离）
            journal=journal,
            recover_retries=recover_retries,
            recover_backoff=recover_backoff,
            state_backend=state_backend,
            state_debounce=state_debounce,
            stale_threshold=d.stale_guard if d.stale_guard and d.stale_guard > 0 else None,
            broadcaster=broadcaster,
        )
        # 覆盖单策略占位：按配置逐策略构造实例（tag=策略名，套 resilient 容错）
        runner._force_derive_magic = True
        runner._magic = None
        runner._risk_cfg = cfg.risk
        strategies: List[_StrategyInstance] = []
        for s in enabled:
            fn = resolve_strategy_fn(s.module, s.name)
            ctx = StrategyContext(
                name=s.name,
                params=dict(s.params),
                risk=cfg.risk,
                symbols=list(s.symbols),
                period=s.period,
                periods=list(s.periods),
                account=acct,
            )
            bound = _make_runner(
                fn, ctx, resilient_on=True, max_errors=s.max_consecutive_errors,
            )
            strategies.append(_StrategyInstance(
                name=s.name,
                symbols=[x.upper() for x in s.symbols],
                period=s.period,
                periods=list(s.periods),
                runner=bound,
                ctx=ctx,
                max_errors=s.max_consecutive_errors,
            ))
        runner._strategies = strategies
        runner._symbols = all_symbols
        runner._symbol = all_symbols[0]
        runner._periods = all_periods
        logger.info(
            f"[MULTI] LiveRunner.from_config: {len(strategies)} strategy(ies) "
            f"[{', '.join(s.name for s in strategies)}] on {len(all_symbols)} symbol(s)"
        )
        return runner

    def run(self) -> None:
        """启动运行器（阻塞，直到策略退出或 Ctrl+C）"""
        logger.info(
            f"=== LiveRunner starting: {','.join(self._symbols)} "
            f"({len(self._strategies)} strategy(ies), {len(self._symbols)} symbol(s)) ==="
        )

        try:
            self._initialize()
            # Phase 3：初始化完成后启动信号分发（起心跳 + 绑定 intent/report 通道）。
            if self._broadcaster is not None:
                try:
                    self._broadcaster.start()
                except Exception as e:  # 分发启动失败不阻断 lead 本地运行
                    logger.error(f"[DIST] broadcaster start failed: {e}", exc_info=True)
            # 设置运行时限：到点后 api.wait_update 返回 False，各策略优雅退出 → 触发清仓
            if self._duration and self._duration > 0:
                end_ts = time.time() + self._duration
                for api in self._all_apis():
                    api.set_run_until(end_ts)
                logger.info(
                    f"Run deadline set: {self._duration:.0f}s "
                    f"(auto-flatten at {time.strftime('%H:%M:%S', time.localtime(end_ts))})"
                )
            self._run_strategy()
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received, shutting down...")
        except Exception as e:
            logger.error(f"LiveRunner fatal error: {e}", exc_info=True)
        finally:
            self._shutdown()

    def stop(self) -> None:
        """外部触发停止"""
        self._running = False
        if self._feed:
            self._feed.stop()

    def _all_apis(self) -> List[KqApi]:
        """全部 (策略, 品种) 的 KqApi（扁平）。"""
        out: List[KqApi] = []
        for strat in self._strategies:
            out.extend(strat.apis.values())
        return out

    # ─── 初始化 ───

    def _profit_calc_hook(self) -> Optional[Callable]:
        """backend.profit_calc（calc_qty 交叉盘换算）；backend 无此方法则 None。"""
        return getattr(self._backend, "profit_calc", None)

    def _equity_fn(self) -> Callable[[], Decimal]:
        """账户权益（账户币）= balance + 浮动盈亏（RiskDataProvider 日盈亏基线用）。"""
        def _fn() -> Decimal:
            if self._executor is None:
                return Decimal("0")
            info = self._executor.query_account()
            if not info:
                return Decimal("0")
            return Decimal(str(info.get("balance", 0))) + Decimal(str(info.get("profit", 0)))
        return _fn

    def _initialize(self) -> None:
        """初始化所有组件（市场相关逻辑委托 backend）"""
        # 1. 连接市场
        self._backend.connect()

        # 2. 品种规格（全策略品种并集，逐品种加载，各自 pip/step/min 不同，绝不混用）
        self._specs = self._backend.load_specs(self._symbols)

        # 3. 执行器
        self._executor = self._backend.make_executor()

        # 4. 敞口账本（全策略共享，tag=策略名隔离）
        self._ledger = ExposureLedger()

        # 5. Resolver（注入结构化 client_order_id 生成器，R1 身份化）
        #    journal 存在时 seq 复用 WAL 序列（R2：id seq 与分发 seq 同源、重启续号）
        self._id_factory = OrderIdFactory(
            seq_provider=self._journal.next_seq if self._journal is not None else None
        )
        self._resolver = UnifiedResolver(order_id_gen=self._id_factory.make)

        # 6. 数据源（一个 feed 订阅全策略 symbols × periods 并集；先构造不启动，
        #    待恢复/对账完成后再 start，避免轮询线程与恢复期 venue 查询争用共享 driver 锁）
        self._feed = self._backend.make_feed(
            symbols=self._symbols,
            periods=self._periods,
            poll_interval=self._poll_interval,
            bar_count=self._bar_count,
        )

        # 7. 全局风控装配（需求 7）：多策略共享一个 RiskEngine（tag 隔离归因）。
        #    单策略旧路径 _risk_cfg=None → 不装配，api.risk_check 降级本地检查。
        if self._risk_cfg is not None and self._ledger is not None:
            self._risk_adapter = assemble_risk(
                self._risk_cfg, self._ledger, self._equity_fn(), self._symbols,
            )

        # 8. 进程内指标引擎（全策略共享一个 engine + 一把串行化锁）
        self._engine = IndicatorEngine()
        self._engine.start()

        # 9. 逐策略：独立 StateStore（R4，key={account}:{name}）+ 每品种一个 KqApi
        profit_calc = self._profit_calc_hook()
        for strat in self._strategies:
            strat.state_store = StateStore(
                backend=self._state_backend,
                key=self._state_key(strat),
                debounce=self._state_debounce,
            )
            strat.state_store.load()
            if strat.state_store.is_resumed():
                logger.info(
                    f"[STATE:{strat.name}] resumed from snapshot "
                    f"key={strat.state_store.key} ({len(strat.state_store)} field(s)) — "
                    f"strategy should check api.is_resumed() before adding positions"
                )
            else:
                logger.info(f"[STATE:{strat.name}] cold start (key={strat.state_store.key})")

            for sym in strat.symbols:
                strat.apis[sym] = KqApi(
                    symbol=sym,
                    period=strat.period,
                    tag=strat.name,
                    specs=self._specs,
                    ledger=self._ledger,
                    resolver=self._resolver,
                    executor=self._executor,
                    feed=self._feed,
                    account_name=self._account_name,
                    magic=self._magic,
                    journal=self._journal,
                    state=strat.state_store,
                    stale_threshold=self._stale_threshold,
                    engine=self._engine,
                    exchange=self._exchange,
                    engine_lock=self._engine_lock,
                    periods=strat.periods,
                    broadcaster=self._broadcaster,
                    strategy_name=strat.name,
                    risk_adapter=self._risk_adapter,
                    profit_calc=profit_calc,
                )

        # 10. R3 启动恢复（journal 驱动，reconcile 之前）：凭 client_order_id 逐条向 venue
        #     收敛在途意图，补 ledger/ticket + 落 journal 终态；venue 不可达则抛（不进策略循环）
        self._recover()

        # 11. 对账：逐策略逐 (tag, magic) 从 venue 恢复净持仓 + 挂单在途（按 magic 隔离）
        #     M4 多周期：逐 period-tag 对账（各周期独立 magic，互不干扰）
        for strat in self._strategies:
            for rtag, rmagic in self._reconcile_targets(strat):
                self._backend.reconcile_positions(
                    self._executor, self._ledger, strat.symbols, rtag, rmagic
                )

        # 12. 恢复/对账完成后才启动行情轮询（此后才进策略循环接受新意图）
        self._feed.start()

        logger.info(
            f"All components initialized, ready to run "
            f"({len(self._strategies)} strategy(ies), "
            f"{sum(len(s.symbols) for s in self._strategies)} worker thread(s)) "
            f"[account={self._account_name or '<none>'}]"
        )

    # ─── R3 启动恢复 ───

    def _state_key(self, strat: _StrategyInstance) -> str:
        """R4 状态存储 key（策略级一份：``{account}:{strategy_name}``，同策略全品种共享）。

        注：Phase M 多周期已收敛为 ``{account}:{strategy}``（tag=base，不含 period），
        使同一策略各周期共享一份快照、崩溃时整体恢复（避免「1h 恢复了、4h 没恢复」半死态）。
        """
        return f"{self._account_name or 'default'}:{strat.name}"

    def _reconcile_targets(
        self, strat: _StrategyInstance,
    ) -> list[tuple[str, int | None]]:
        """M4：返回某策略需对账/恢复的 (tag, magic) 目标列表。

        单周期 → ``[(name, magic)]``（完全等价旧口径，后向兼容）；
        多周期 → 逐 period 返回 ``(``{name}:{period}``, 派生 magic)``（各周期独立
        magic，互不干扰；显式 magic 覆盖时沿用覆盖值，account 空则 magic=None）。
        """
        multi_period = len(strat.periods) > 1
        if not multi_period:
            # 旧单策略口径：原样 self._magic（可能 None = 不按 magic 隔离，孤儿清理安全网）；
            #   多策略（_force_derive_magic）则按 (account, name) 派生 magic 保证隔离。
            if self._force_derive_magic:
                return [(strat.name, self._magic_for(strat, None))]
            return [(strat.name, self._magic)]
        out: list[tuple[str, int | None]] = []
        for p in strat.periods:
            out.append((f"{strat.name}:{p}", self._magic_for(strat, p)))
        return out

    def _magic_for(self, strat: _StrategyInstance, period: Optional[str]) -> Optional[int]:
        """某策略某周期的对账 magic（与 KqApi._magic_for 口径一致）。"""
        tag = strat.name if period is None else f"{strat.name}:{period}"
        if self._magic is not None:
            return self._magic
        if self._account_name:
            return derive_magic(self._account_name, tag)
        return None

    def _recover(self) -> None:
        """R3 启动恢复：journal.pending() 逐条凭 client_order_id 向 venue 收敛。

        分支（对齐规划 R3）：
          - 已成交(FILLED) → 补 ledger 持仓（accepted+filled 净 in_flight→0、volume+=signed）
          - 仍挂着(OPEN)   → 补 in_flight（accepted）+ api._order_tickets + journal IN_FLIGHT
          - 查无此单(None/DEAD) → journal DEAD 释放（重启后 ledger 本空，无在途可释）
        全部收敛后打印逐条恢复报告，才允许接受新意图。
        venue 不可达：重试 + 告警，仍不可达则抛 RuntimeError（宁可不起，不可带病起）。
        journal 为 None（回测/--no-journal）：无意图可恢复，直接返回（零破坏）。
        多策略：逐策略按其 (tag, magic) 目标汇总待恢复意图（tag 隔离，天然分组）。
        """
        if self._journal is None:
            return
        # 逐策略汇总待恢复意图（单策略单周期仅 base tag，行为不变）
        rows: list[tuple[_StrategyInstance, object]] = []
        for strat in self._strategies:
            for rtag, _rmagic in self._reconcile_targets(strat):
                for row in self._journal.pending(
                    account=self._account_name or None, tag=rtag
                ):
                    rows.append((strat, row))
        if not rows:
            logger.info("[RECOVER] no pending order intents, clean start")
            return

        # venue 可达性闸门：有待恢复意图才探测（避免无谓探测）；不可达则抛，不进策略循环
        self._await_venue_reachable()

        logger.info(
            f"[RECOVER] {len(rows)} pending order intent(s) to reconcile against venue "
            f"[account={self._account_name or '<none>'}]"
        )
        report = []   # 逐条恢复明细 (sym, coid, state, detail)
        for strat, row in rows:
            coid = row.client_order_id
            sym = row.symbol
            # M4：意图归属的周期 tag（单周期 row.tag==strat.name，行为不变）
            tag = row.tag
            side = OrderSide.SELL if row.side.lower().startswith("s") else OrderSide.BUY
            offset = Offset.CLOSE if row.offset.lower().startswith("c") else Offset.OPEN
            try:
                outcome = self._executor.query_order_outcome(coid, sym)
            except Exception as e:
                # venue 恢复期掉线：不可信「查无此单」，抛出中止启动（不带病起）
                logger.error(
                    f"[RECOVER] venue query failed for {coid}: {e}", exc_info=True
                )
                raise RuntimeError(
                    f"venue unreachable during recovery ({coid}): {e}"
                ) from e

            api = strat.apis.get(sym)
            state = (outcome or {}).get("state")
            if outcome is None or state == "DEAD":
                reason = (outcome or {}).get("reason", "not found at venue")
                self._journal.finish(coid, STATE_DEAD, reason=f"recovered: {reason}")
                report.append((sym, coid, "DEAD", reason))
            elif state == "OPEN":
                ticket = int(outcome.get("ticket", 0) or 0)
                # 补回在途（重启后 ledger 本空）+ 记录 ticket（cancel 用）
                self._ledger.on_order_accepted(sym, tag, coid, side, offset, row.qty)
                if api is not None and ticket:
                    api._order_tickets[coid] = ticket
                self._journal.finish(coid, STATE_IN_FLIGHT, ticket=ticket or None)
                report.append((sym, coid, "IN_FLIGHT", f"ticket={ticket} qty={row.qty}"))
            else:  # FILLED
                ticket = int(outcome.get("ticket", 0) or 0)
                fq = outcome.get("filled_qty") or row.qty
                fp = outcome.get("filled_price") or Decimal("0")
                # accepted→filled：无 pending 记录时先建再销，净 in_flight→0、volume+=signed
                self._ledger.on_order_accepted(sym, tag, coid, side, offset, row.qty)
                self._ledger.on_order_filled(
                    sym, tag, coid, side, offset, row.qty,
                    fill_price=fp, fill_qty=fq,
                )
                if api is not None and ticket:
                    api._order_tickets[coid] = ticket
                self._journal.finish(
                    coid, STATE_FILLED, ticket=ticket or None,
                    filled_qty=fq, filled_price=fp,
                )
                report.append((sym, coid, "FILLED", f"{fq}@{fp} ticket={ticket}"))

        # 恢复报告（逐条明细）→ 全部收敛后才允许接受新意图
        logger.info("[RECOVER] === recovery report ===")
        for sym, coid, state, detail in report:
            logger.info(f"[RECOVER]   {sym} {coid} -> {state} ({detail})")
        logger.info(
            f"[RECOVER] === {len(report)} intent(s) converged, now accepting new orders ==="
        )

    def _await_venue_reachable(self) -> None:
        """恢复期 venue 可达性闸门：探测 query_account，不可达则重试+告警；
        超过重试上限仍不可达 → 抛 RuntimeError（中止启动，不进策略循环）。"""
        attempt = 0
        while True:
            attempt += 1
            try:
                if self._executor.query_account() is not None:
                    return
                logger.warning(
                    f"[RECOVER] venue unreachable (query_account=None), "
                    f"attempt {attempt}/{self._recover_retries}"
                )
            except Exception as e:
                logger.warning(
                    f"[RECOVER] venue probe error: {e}, "
                    f"attempt {attempt}/{self._recover_retries}"
                )
            if attempt >= self._recover_retries:
                raise RuntimeError(
                    f"venue unreachable during recovery after {attempt} attempt(s); "
                    f"refusing to start (宁可不起，不可带病起)"
                )
            time.sleep(self._recover_backoff)

    # ─── 策略运行 ───

    def _run_strategy(self) -> None:
        """每 (策略, 品种) 一个工作线程并行运行（拓扑 Z：并行、内部串行，避免队头阻塞）。

        单策略异常只影响本线程（线程体 catch-all + resilient 迭代级容错），
        不拖垮其他策略/品种，更不干掉整个进程事件循环（需求 10）。
        """
        self._running = True

        # 注册信号处理（优雅退出）——必须在主线程
        original_sigint = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, lambda *_: self.stop())

        threads: List[threading.Thread] = []
        try:
            for strat in self._strategies:
                for sym in strat.symbols:
                    api = strat.apis.get(sym)
                    if api is None:
                        continue
                    t = threading.Thread(
                        target=self._run_symbol_strategy,
                        args=(strat, sym, api),
                        name=f"strat-{strat.name}-{sym}",
                        daemon=True,
                    )
                    threads.append(t)
                    t.start()
            # 主线程等待所有工作线程退出（到点/Ctrl+C/异常）
            for t in threads:
                t.join()
        finally:
            signal.signal(signal.SIGINT, original_sigint)
            self._running = False
            logger.info("All strategy workers exited")

    def _run_symbol_strategy(
        self, strat: _StrategyInstance, sym: str, api: KqApi,
    ) -> None:
        """单 (策略, 品种) 工作线程体：异常只影响本线程，不拖垮其他策略/品种。"""
        logger.info(f"Strategy starting: {strat.name} [{sym}]")
        try:
            strat.runner(api)
        except Exception as e:
            logger.error(f"Strategy error [{strat.name}:{sym}]: {e}", exc_info=True)

    # ─── 关闭 ───

    def _shutdown(self) -> None:
        """优雅关闭所有组件（逐策略逐品种清仓，绝不留孤儿仓）"""
        logger.info("Shutting down...")

        # ① 停数据轮询
        if self._feed:
            self._feed.stop()

        # ①b 停指标引擎（Phase 1）
        if self._engine is not None:
            self._engine.stop()

        # ② 清仓：逐策略逐品种撤所有挂单 + 平掉净持仓（趁连接还活着）
        #    无论到时/Ctrl+C/异常退出都执行，实盘绝不留孤儿仓
        if self._executor:
            for strat in self._strategies:
                for sym in strat.symbols:
                    api = strat.apis.get(sym)
                    if api is None:
                        continue
                    try:
                        info = api.flatten(symbol=sym)
                        close = info.get("close")
                        close_desc = (
                            f"ok={close.ok} filled={close.filled_qty}@{close.filled_price}"
                            if close else "no net position"
                        )
                        logger.info(
                            f"Flatten [{strat.name}] {sym} done: canceled={info['canceled']} "
                            f"net_vol={info['net_vol']} close=[{close_desc}]"
                        )
                    except Exception as e:
                        logger.error(
                            f"Flatten [{strat.name}] {sym} failed: {e}", exc_info=True
                        )

        # ③ 打印最终持仓 + 关闭市场连接
        if self._ledger:
            for strat in self._strategies:
                for sym in strat.symbols:
                    pos = self._ledger.position(sym, strat.name)
                    logger.info(
                        f"Final position [{strat.name}] {sym}: volume={pos.volume} "
                        f"in_flight={pos.in_flight} effective={pos.effective} "
                        f"realized_pnl={pos.realized_pnl}"
                    )
        self._backend.shutdown()

        # ④ R4 策略语义状态落盘（安全网：不靠策略作者自觉 save，逐策略）
        for strat in self._strategies:
            if strat.state_store is not None:
                try:
                    strat.state_store.save()
                    logger.info(
                        f"[STATE:{strat.name}] snapshot saved (key={strat.state_store.key})"
                    )
                except Exception as e:
                    logger.error(f"State save failed [{strat.name}]: {e}", exc_info=True)
        if self._state_backend is not None:
            self._state_backend.close()

        # ④b Phase 3：清仓后、关 journal 前停信号分发（best-effort，异常不阻断退出）。
        if self._broadcaster is not None:
            try:
                self._broadcaster.stop()
            except Exception as e:
                logger.warning(f"[DIST] broadcaster stop failed: {e}")

        # ⑤ 关闭订单意图 WAL（在清仓写单之后，确保收尾意图落盘）
        if self._journal is not None:
            self._journal.close()

        logger.info("=== LiveRunner stopped ===")

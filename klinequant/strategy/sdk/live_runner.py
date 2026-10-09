"""LiveRunner — 实盘策略运行器（市场无关）

职责（市场相关逻辑全部委托注入的 MarketBackend）：
  1. backend.connect()（建立市场连接）
  2. backend.load_specs()（加载 SymbolInfo）
  3. backend.make_executor() + 本地 ExposureLedger / UnifiedResolver
  4. backend.reconcile_positions()（启动对账）
  5. backend.make_feed()（轮询/推送 ticks + bars）
  6. 每品种构造 KqApi
  7. 运行策略函数（策略内 while api.wait_update() 循环）
  8. 优雅退出（Ctrl+C / 异常 / 到时），backend.shutdown()

使用方式：
    # 单品种（FX）
    runner = LiveRunner(Mt5Backend(), symbols="EURUSD", period="1m",
                        strategy_fn=my_strategy)
    # 多品种：单进程 + 单连接 + 每品种一个工作线程
    #（品种间并行、品种内串行，规避单循环队头阻塞）
    runner = LiveRunner(Mt5Backend(), symbols=["EURUSD", "GBPUSD"], period="1m",
                        strategy_fn=my_strategy)
    # 加密（同构复用，仅换 backend）
    runner = LiveRunner(BinanceBackend(...), symbols="BTCUSDT", period="1m",
                        strategy_fn=my_strategy)
    runner.run()

崩溃恢复与进程守护（Phase R）：
  Runner 自身不含看门狗/自动重启——那属进程外守护层，与代码解耦。重启后由
  _initialize → _recover()（R3）自动收敛：凭 R2 WAL 的 client_order_id 逐条
  向 venue 反查在途意图，补 ledger/ticket，再 reconcile_positions 对账净持仓；
  R4 StateStore 自动 load 语义快照（api.is_resumed() 供策略辨冷启动/恢复）。

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

import logging
import signal
import threading
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Union

from core.indicator_engine.engine import IndicatorEngine
from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import UnifiedResolver
from protocol.types import Offset, OrderSide, SymbolInfo
from strategy.sdk.api import DataFeedProtocol, ExecutorProtocol, KqApi
from strategy.sdk.backend import MarketBackend
from strategy.sdk.order_id import OrderIdFactory
from strategy.sdk.order_journal import STATE_DEAD, STATE_FILLED, STATE_IN_FLIGHT
from strategy.sdk.state_store import StateBackend, StateStore

if TYPE_CHECKING:  # 仅类型标注，避免运行期耦合 order_journal
    from strategy.sdk.order_journal import Journal

logger = logging.getLogger(__name__)


class LiveRunner:
    """实盘策略运行器"""

    def __init__(
        self,
        backend: MarketBackend,
        symbols: Union[str, List[str]],
        period: str,
        strategy_fn: Callable[[KqApi], None],
        *,
        tag: str = "",
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
        self._backend = backend
        self._symbols: List[str] = [s.upper() for s in symbols]
        if not self._symbols:
            raise ValueError("LiveRunner requires at least one symbol")
        self._symbol = self._symbols[0]  # 主品种
        self._period = period
        self._strategy_fn = strategy_fn
        self._tag = tag or period
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
        self._state_store: Optional[StateStore] = None
        # R5 断线闸门阈值（秒；None=不启用）——透传给每个 KqApi，feed degraded 时拒 OPEN
        self._stale_threshold = stale_threshold

        # 组件（run 时初始化）
        self._specs: Dict[str, SymbolInfo] = {}
        self._executor: Optional[ExecutorProtocol] = None
        self._ledger: Optional[ExposureLedger] = None
        self._resolver: Optional[UnifiedResolver] = None
        self._id_factory: Optional[OrderIdFactory] = None
        self._feed: Optional[DataFeedProtocol] = None
        # Phase 1 进程内指标引擎（拓扑 Z：全品种共享一个 engine + 一把串行化锁）
        self._engine: Optional[IndicatorEngine] = None
        self._engine_lock = threading.Lock()
        self._exchange = getattr(backend, "exchange", "mt5")
        self._apis: Dict[str, KqApi] = {}   # 每品种一个 KqApi（绑定各自 symbol）
        self._running = False

    def run(self) -> None:
        """启动运行器（阻塞，直到策略退出或 Ctrl+C）"""
        logger.info(
            f"=== LiveRunner starting: {','.join(self._symbols)}/{self._period} "
            f"tag={self._tag} ({len(self._symbols)} symbol(s)) ==="
        )

        try:
            self._initialize()
            # 设置运行时限：到点后 api.wait_update 返回 False，各品种策略优雅退出 → 触发清仓
            if self._duration and self._duration > 0:
                end_ts = time.time() + self._duration
                for api in self._apis.values():
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

    # ─── 初始化 ───

    def _initialize(self) -> None:
        """初始化所有组件（市场相关逻辑委托 backend）"""
        # 1. 连接市场
        self._backend.connect()

        # 2. 品种规格（逐品种加载，各自 pip/step/min 不同，绝不混用）
        self._specs = self._backend.load_specs(self._symbols)

        # 3. 执行器
        self._executor = self._backend.make_executor()

        # 4. 敞口账本
        self._ledger = ExposureLedger()

        # 5. Resolver（注入结构化 client_order_id 生成器，R1 身份化）
        #    journal 存在时 seq 复用 WAL 序列（R2：id seq 与分发 seq 同源、重启续号）
        self._id_factory = OrderIdFactory(
            seq_provider=self._journal.next_seq if self._journal is not None else None
        )
        self._resolver = UnifiedResolver(order_id_gen=self._id_factory.make)

        # 6. 数据源（一个 feed 订阅全部品种；先构造不启动，待恢复/对账完成后再 start，
        #    避免轮询线程与恢复期 venue 查询争用共享 driver 锁）
        self._feed = self._backend.make_feed(
            symbols=self._symbols,
            periods=[self._period],
            poll_interval=self._poll_interval,
            bar_count=self._bar_count,
        )

        # 7. R4 策略语义状态存储（策略级共享一份；backend 存在则自动 load 预填充）
        #    安全网在 runner：不靠策略作者自觉 load/save；api.is_resumed() 供策略辨冷启动/恢复
        self._state_store = StateStore(
            backend=self._state_backend,
            key=self._state_key(),
            debounce=self._state_debounce,
        )
        self._state_store.load()
        if self._state_store.is_resumed():
            logger.info(
                f"[STATE] resumed from snapshot key={self._state_store.key} "
                f"({len(self._state_store)} field(s)) — strategy should check "
                f"api.is_resumed() before adding positions"
            )
        else:
            logger.info(f"[STATE] cold start (key={self._state_store.key})")

        # 8. 每品种一个 KqApi（绑定各自 symbol，共享 feed/ledger/executor/resolver/specs/state）
        #    Phase 1：共享进程内 IndicatorEngine（+ 一把串行化锁），api.INDIC() 即用
        self._engine = IndicatorEngine()
        self._engine.start()
        for sym in self._symbols:
            self._apis[sym] = KqApi(
                symbol=sym,
                period=self._period,
                tag=self._tag,
                specs=self._specs,
                ledger=self._ledger,
                resolver=self._resolver,
                executor=self._executor,
                feed=self._feed,
                account_name=self._account_name,
                magic=self._magic,
                journal=self._journal,
                state=self._state_store,
                stale_threshold=self._stale_threshold,
                engine=self._engine,
                exchange=self._exchange,
                engine_lock=self._engine_lock,
            )

        # 9. R3 启动恢复（journal 驱动，reconcile 之前）：凭 client_order_id 逐条向 venue
        #    收敛在途意图，补 ledger/ticket + 落 journal 终态；venue 不可达则抛（不进策略循环）
        self._recover()

        # 10. 对账：逐品种从 venue 恢复净持仓 + 挂单在途（按 magic 隔离，只恢复本策略）
        self._backend.reconcile_positions(
            self._executor, self._ledger, self._symbols, self._tag, self._magic
        )

        # 11. 恢复/对账完成后才启动行情轮询（此后才进策略循环接受新意图）
        self._feed.start()

        logger.info(
            f"All components initialized, ready to run strategy "
            f"({len(self._symbols)} symbol worker thread(s)) "
            f"[account={self._account_name or '<none>'} tag={self._tag}]"
        )

    # ─── R3 启动恢复 ───

    def _state_key(self) -> str:
        """R4 状态存储 key（策略级一份：``{account}:{tag}``，同一 runner 内全品种共享）。

        注：Phase M 多周期落地后，key 将收敛为 ``{account}:{strategy}``（不含 period），
        使同一策略各周期共享一份快照、崩溃时整体恢复（避免「1h 恢复了、4h 没恢复」半死态）。
        """
        return f"{self._account_name or 'default'}:{self._tag}"

    def _recover(self) -> None:
        """R3 启动恢复：journal.pending() 逐条凭 client_order_id 向 venue 收敛。

        分支（对齐规划 R3）：
          - 已成交(FILLED) → 补 ledger 持仓（accepted+filled 净 in_flight→0、volume+=signed）
          - 仍挂着(OPEN)   → 补 in_flight（accepted）+ api._order_tickets + journal IN_FLIGHT
          - 查无此单(None/DEAD) → journal DEAD 释放（重启后 ledger 本空，无在途可释）
        全部收敛后打印逐条恢复报告，才允许接受新意图。
        venue 不可达：重试 + 告警，仍不可达则抛 RuntimeError（宁可不起，不可带病起）。
        journal 为 None（回测/--no-journal）：无意图可恢复，直接返回（零破坏）。
        """
        if self._journal is None:
            return
        rows = self._journal.pending(
            account=self._account_name or None, tag=self._tag
        )
        if not rows:
            logger.info("[RECOVER] no pending order intents, clean start")
            return

        # venue 可达性闸门：有待恢复意图才探测（避免无谓探测）；不可达则抛，不进策略循环
        self._await_venue_reachable()

        logger.info(
            f"[RECOVER] {len(rows)} pending order intent(s) to reconcile against venue "
            f"[account={self._account_name or '<none>'} tag={self._tag}]"
        )
        report = []   # 逐条恢复明细 (sym, coid, state, detail)
        for row in rows:
            coid = row.client_order_id
            sym = row.symbol
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

            api = self._apis.get(sym)
            state = (outcome or {}).get("state")
            if outcome is None or state == "DEAD":
                reason = (outcome or {}).get("reason", "not found at venue")
                self._journal.finish(coid, STATE_DEAD, reason=f"recovered: {reason}")
                report.append((sym, coid, "DEAD", reason))
            elif state == "OPEN":
                ticket = int(outcome.get("ticket", 0) or 0)
                # 补回在途（重启后 ledger 本空）+ 记录 ticket（cancel 用）
                self._ledger.on_order_accepted(sym, self._tag, coid, side, offset, row.qty)
                if api is not None and ticket:
                    api._order_tickets[coid] = ticket
                self._journal.finish(coid, STATE_IN_FLIGHT, ticket=ticket or None)
                report.append((sym, coid, "IN_FLIGHT", f"ticket={ticket} qty={row.qty}"))
            else:  # FILLED
                ticket = int(outcome.get("ticket", 0) or 0)
                fq = outcome.get("filled_qty") or row.qty
                fp = outcome.get("filled_price") or Decimal("0")
                # accepted→filled：无 pending 记录时先建再销，净 in_flight→0、volume+=signed
                self._ledger.on_order_accepted(sym, self._tag, coid, side, offset, row.qty)
                self._ledger.on_order_filled(
                    sym, self._tag, coid, side, offset, row.qty,
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
        """每品种一个工作线程并行运行策略（品种间并行、品种内串行，避免队头阻塞）"""
        self._running = True

        # 注册信号处理（优雅退出）——必须在主线程
        original_sigint = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, lambda *_: self.stop())

        threads: List[threading.Thread] = []
        try:
            for sym in self._symbols:
                t = threading.Thread(
                    target=self._run_symbol_strategy,
                    args=(sym, self._apis[sym]),
                    name=f"strat-{sym}",
                    daemon=True,
                )
                threads.append(t)
                t.start()
            # 主线程等待所有品种策略退出（到点/Ctrl+C/异常）
            for t in threads:
                t.join()
        finally:
            signal.signal(signal.SIGINT, original_sigint)
            self._running = False
            logger.info("All symbol strategies exited")

    def _run_symbol_strategy(self, sym: str, api: KqApi) -> None:
        """单品种策略工作线程体：异常只影响本品种，不拖垮其他品种"""
        logger.info(f"Strategy starting: {self._strategy_fn.__name__} [{sym}]")
        try:
            self._strategy_fn(api)
        except Exception as e:
            logger.error(f"Strategy error [{sym}]: {e}", exc_info=True)

    # ─── 关闭 ───

    def _shutdown(self) -> None:
        """优雅关闭所有组件"""
        logger.info("Shutting down...")

        # ① 停数据轮询
        if self._feed:
            self._feed.stop()

        # ①b 停指标引擎（Phase 1）
        if self._engine is not None:
            self._engine.stop()

        # ② 清仓：逐品种撤所有挂单 + 平掉净持仓（趁连接还活着）
        #    无论到时/Ctrl+C/异常退出都执行，实盘绝不留孤儿仓
        if self._apis and self._executor:
            flat_api = self._apis[self._symbol]
            for sym in self._symbols:
                try:
                    info = flat_api.flatten(symbol=sym)
                    close = info.get("close")
                    close_desc = (
                        f"ok={close.ok} filled={close.filled_qty}@{close.filled_price}"
                        if close else "no net position"
                    )
                    logger.info(
                        f"Flatten {sym} done: canceled={info['canceled']} "
                        f"net_vol={info['net_vol']} close=[{close_desc}]"
                    )
                except Exception as e:
                    logger.error(f"Flatten {sym} failed: {e}", exc_info=True)

        # ③ 打印最终持仓 + 关闭市场连接
        if self._ledger:
            for sym in self._symbols:
                pos = self._ledger.position(sym, self._tag)
                logger.info(
                    f"Final position {sym}: volume={pos.volume} "
                    f"in_flight={pos.in_flight} effective={pos.effective} "
                    f"realized_pnl={pos.realized_pnl}"
                )
        self._backend.shutdown()

        # ④ R4 策略语义状态落盘（安全网：不靠策略作者自觉 save）+ 关闭后端
        if self._state_store is not None:
            try:
                self._state_store.save()
                logger.info(f"[STATE] snapshot saved (key={self._state_store.key})")
            except Exception as e:
                logger.error(f"State save failed: {e}", exc_info=True)
        if self._state_backend is not None:
            self._state_backend.close()

        # ⑤ 关闭订单意图 WAL（在清仓写单之后，确保收尾意图落盘）
        if self._journal is not None:
            self._journal.close()

        logger.info("=== LiveRunner stopped ===")

"""BacktestRunner — 回测运行器（与 LiveRunner 同构）

用 MT5 历史数据回放，运行与实盘**完全相同**的 strategy(api: KqApi) 函数：
KqApi / ExposureLedger / UnifiedResolver 零改动，仅把数据源与执行器换成回测版
（BacktestDataFeed + BacktestExecutor），即「回测/实盘同构」。

与实盘的对应关系：
    LiveRunner                     BacktestRunner
    Mt5DataFeed（实时轮询）    →   BacktestDataFeed（历史回放）
    Mt5Executor（真实下单）    →   BacktestExecutor（模拟撮合）
    每品种一工作线程并行        →   每品种顺序独立回放（确定性、无并发）
    duration 到点 flatten      →   数据耗尽 flatten

品种间相互独立（各自 KqApi 绑定自身 symbol，账本按 (symbol,tag) 隔离），故顺序
回放与并行回放对单品种结果无影响；一期不做多品种组合资金曲线。

使用方式：
    runner = BacktestRunner(symbols=["EURUSD"], period="1m",
                            strategy_fn=strategy, history_bars=3000)
    report = runner.run()
    print(report.summary())
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from core.backtest_engine.performance import (
    PerformanceAnalyzer,
    PerformanceReport,
    Trade,
)
from core.indicator_engine.engine import IndicatorEngine
from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import UnifiedResolver
from core.trade_engine.spec_loader import load_spec_from_mt5
from gateway.market_sources.mt5_driver import TF_SECONDS, TIMEFRAME_MAP, Mt5Api
from kqsdk.api import KqApi
from kqsdk.backtest_executor import BacktestExecutor
from kqsdk.backtest_feed import BacktestDataFeed
from kqsdk.backtest_pump import Gate, GatedFeed, run_lockstep
from kqsdk.live_runner import _make_runner
from kqsdk.order_id import OrderIdFactory
from kqsdk.registry import StrategyContext, resolve_strategy_fn
from kqsdk.risk_adapter import assemble_risk
from protocol.types import SymbolInfo

if TYPE_CHECKING:  # 仅类型标注，避免运行期耦合 config
    from config.strategy_config import RiskConfig, StrategiesConfig

logger = logging.getLogger(__name__)


@dataclass
class SymbolBacktestResult:
    """单品种回测结果"""
    symbol: str
    report: PerformanceReport
    trades: List[Trade] = field(default_factory=list)
    equity_curve: List[float] = field(default_factory=list)
    n_bars: int = 0


@dataclass
class BacktestReport:
    """多品种回测汇总"""
    results: Dict[str, SymbolBacktestResult] = field(default_factory=dict)
    period: str = "1m"
    initial_capital: float = 0.0

    def summary(self) -> str:
        lines = [
            "=" * 60,
            f"  回测汇总  period={self.period}  初始资金={self.initial_capital}",
            "=" * 60,
        ]
        for sym, r in self.results.items():
            rep = r.report
            lines.append(
                f"[{sym}] bars={r.n_bars} trades={rep.total_trades} "
                f"return={rep.total_return * 100:.2f}% "
                f"annual={rep.annual_return * 100:.2f}% "
                f"sharpe={rep.sharpe_ratio:.2f} "
                f"maxDD={rep.max_drawdown * 100:.2f}% "
                f"win={rep.win_rate * 100:.1f}% "
                f"PF={rep.profit_factor:.2f} "
                f"fees={rep.total_fees:.2f} "
                f"final={rep.final_equity:.2f}"
            )
        lines.append("=" * 60)
        return "\n".join(lines)


@dataclass
class _PortfolioStrategy:
    """组合回测中的单个策略实例（共享账户/时间轴，tag=策略名隔离）。"""
    name: str
    symbols: List[str]
    period: str
    periods: List[str]
    runner: Callable[[KqApi], None]


@dataclass
class StrategyBacktestResult:
    """分策略回测子报表（共享账户下按 trade.strategy 归属拆分）。"""
    name: str
    report: PerformanceReport
    trades: List[Trade] = field(default_factory=list)
    symbols: List[str] = field(default_factory=list)


@dataclass
class PortfolioReport:
    """多策略组合回测报表（单一模拟账户：组合权益曲线 + 分策略子报表）。

    ``combined`` 为全策略共享账户的组合绩效（组合权益曲线/回撤/年化）；
    ``strategies`` 按策略名拆分 trades 与子报表（子权益曲线 = 初始资金 +
    该策略已实现盈亏累计，仅供横向对比，不等于独立账户）。
    """
    combined: PerformanceReport
    combined_equity_curve: List[float] = field(default_factory=list)
    strategies: Dict[str, StrategyBacktestResult] = field(default_factory=dict)
    initial_capital: float = 0.0
    n_steps: int = 0

    def summary(self) -> str:
        c = self.combined
        lines = [
            "=" * 64,
            f"  组合回测  初始资金={self.initial_capital:.2f}  时间轴步数={self.n_steps}",
            "=" * 64,
            (
                f"[PORTFOLIO] trades={c.total_trades} "
                f"return={c.total_return * 100:.2f}% "
                f"annual={c.annual_return * 100:.2f}% "
                f"sharpe={c.sharpe_ratio:.2f} "
                f"maxDD={c.max_drawdown * 100:.2f}% "
                f"win={c.win_rate * 100:.1f}% "
                f"PF={c.profit_factor:.2f} "
                f"fees={c.total_fees:.2f} "
                f"final={c.final_equity:.2f}"
            ),
            "-" * 64,
        ]
        for name, r in self.strategies.items():
            rep = r.report
            lines.append(
                f"[{name}] trades={rep.total_trades} "
                f"pnl={sum(float(t.pnl) for t in r.trades):.2f} "
                f"win={rep.win_rate * 100:.1f}% "
                f"PF={rep.profit_factor:.2f} "
                f"fees={rep.total_fees:.2f} "
                f"symbols={','.join(r.symbols)}"
            )
        lines.append("=" * 64)
        return "\n".join(lines)


class BacktestRunner:
    """回测运行器"""

    def __init__(
        self,
        symbols,
        period: str,
        strategy_fn: Callable[[KqApi], None],
        *,
        periods: Optional[List[str]] = None,
        tag: str = "",
        history_bars: int = 2000,
        initial_capital: Decimal = Decimal("10000"),
        slippage_model: str = "percentage",
        slippage_params: Optional[dict] = None,
        fee_model: str = "fixed",
        fee_params: Optional[dict] = None,
        magic: int = 202609,
        account_currency: str = "USD",
        bars_by_symbol: Optional[Dict[str, Any]] = None,
        specs: Optional[Dict[str, SymbolInfo]] = None,
        mt5_kwargs: Optional[dict] = None,
    ):
        """
        Args:
            symbols: 品种，str 或 list（逐个独立回测）
            period: 主/驱动周期（如 "1m"）；多周期时为策略主交易周期
            periods: 多周期回放列表（如 ["15m","1h"]）。给定时按统一时间轴派发多周期
                bar 收盘事件（同一时刻大周期先于小周期，1h 先于 15m）；None=单周期 [period]。
            strategy_fn: 策略函数 (api: KqApi) -> None，与实盘同一份
            history_bars: 从 MT5 拉取的历史 bar 数（bars_by_symbol 提供时忽略）
            initial_capital: 每品种初始资金（计价货币）
            slippage_model/slippage_params: 滑点模型（默认 percentage）
            fee_model/fee_params: 手续费模型（默认 fixed，每笔 0）
            magic: 魔术号
            account_currency: 账户币（交叉盘换算目标，默认 USD）
            bars_by_symbol: 直接注入历史 bar（单测/离线数据用，跳过 MT5）。单周期为
                {symbol: [bars]}；多周期为 {symbol: {period: [bars]}}。
            specs: 直接注入品种规格（配合 bars_by_symbol 用）
            mt5_kwargs: MT5 初始化参数
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        self._symbols: List[str] = [s.upper() for s in symbols]
        if not self._symbols:
            raise ValueError("BacktestRunner requires at least one symbol")
        self._period = period
        # 多周期同构：periods 给定则多周期回放（主周期=period，用于报表/年化/tag）
        self._periods: List[str] = list(periods) if periods else [period]
        self._strategy_fn = strategy_fn
        self._tag = tag or period
        self._history_bars = history_bars
        self._initial_capital = Decimal(str(initial_capital))
        self._slippage_model = slippage_model
        self._slippage_params = slippage_params or {"pct": Decimal("0.0001")}
        self._fee_model = fee_model
        self._fee_params = fee_params or {"fee_per_trade": Decimal("0")}
        self._magic = magic
        self._account_currency = account_currency
        self._mt5_kwargs = mt5_kwargs

        self._bars: Dict[str, Any] = bars_by_symbol or {}   # 单周期扁平 / 多周期嵌套
        self._specs: Dict[str, SymbolInfo] = specs or {}
        self._conv_rates: Dict[str, Decimal] = {}
        # 多策略组合回测（from_config 装配）：单策略旧路径为空，risk_cfg=None 不装风控
        self._risk_cfg: Optional["RiskConfig"] = None
        self._portfolio: List[_PortfolioStrategy] = []

    # ─── 主流程 ───

    def run(self) -> BacktestReport:
        """执行回测，返回多品种汇总报告"""
        if not self._bars:
            self._load_from_mt5()

        bars_per_year = self._bars_per_year()
        analyzer = PerformanceAnalyzer(bars_per_year=bars_per_year)
        report = BacktestReport(
            period=self._period,
            initial_capital=float(self._initial_capital),
        )

        logger.info(
            f"=== BacktestRunner: {','.join(self._symbols)}/{self._period} "
            f"({len(self._symbols)} symbol(s)) ==="
        )
        for sym in self._symbols:
            bars = self._bars.get(sym)
            spec = self._specs.get(sym)
            if not bars or spec is None:
                logger.error(f"[{sym}] missing bars/spec, skipped")
                continue
            result = self._run_symbol(sym, spec, self._as_nested(bars), analyzer)
            report.results[sym] = result
            logger.info(
                f"[{sym}] done: bars={result.n_bars} trades={result.report.total_trades} "
                f"return={result.report.total_return * 100:.2f}% "
                f"final={result.report.final_equity:.2f}"
            )
        return report

    # ─── 多策略组合回测（需求：回测也要多策略；确定性 lockstep 回放）───

    @classmethod
    def from_config(
        cls,
        cfg: "StrategiesConfig",
        *,
        bars_by_symbol: Optional[Dict[str, Any]] = None,
        specs: Optional[Dict[str, SymbolInfo]] = None,
        only: Optional[List[str]] = None,
        mt5_kwargs: Optional[dict] = None,
    ) -> "BacktestRunner":
        """按 StrategiesConfig 装配多策略组合回测器（共享账户/时间轴）。

        Args:
            cfg: 已加载校验的策略配置（load_strategies_config）
            bars_by_symbol: 直接注入历史 bar（单测/离线数据，跳过 MT5）；形态同
                __init__（单周期 {sym:[bars]} / 多周期 {sym:{period:[bars]}}），
                需含全策略 symbols×periods 并集
            specs: 直接注入品种规格（配合 bars_by_symbol）
            only: 只回测这些策略名（None=全部 enabled 策略）
            mt5_kwargs: MT5 初始化参数（bars_by_symbol 缺省时拉历史用）
        """
        bt = cfg.backtest
        enabled = cfg.enabled_strategies()
        if only:
            wanted = set(only)
            enabled = [s for s in enabled if s.name in wanted]
        if not enabled:
            raise ValueError("from_config: 没有可回测的策略（检查 enabled / --only）")

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
            symbols=all_symbols,
            period=enabled[0].period,
            strategy_fn=lambda api: None,   # 占位；组合回测实际用各实例 runner
            periods=all_periods,
            history_bars=bt.history_bars,
            initial_capital=Decimal(str(bt.initial_capital)),
            slippage_model=bt.slippage.model,
            slippage_params=dict(bt.slippage.params),
            fee_model=bt.fee.model,
            fee_params=dict(bt.fee.params),
            account_currency=bt.account_currency,
            bars_by_symbol=bars_by_symbol,
            specs=specs,
            mt5_kwargs=mt5_kwargs,
        )
        runner._risk_cfg = cfg.risk
        portfolio: List[_PortfolioStrategy] = []
        for s in enabled:
            fn = resolve_strategy_fn(s.module, s.name)
            ctx = StrategyContext(
                name=s.name,
                params=dict(s.params),
                risk=cfg.risk,
                symbols=list(s.symbols),
                period=s.period,
                periods=list(s.periods),
                account="",
            )
            bound = _make_runner(
                fn, ctx, resilient_on=True, max_errors=s.max_consecutive_errors,
            )
            portfolio.append(_PortfolioStrategy(
                name=s.name,
                symbols=[x.upper() for x in s.symbols],
                period=s.period,
                periods=list(s.periods),
                runner=bound,
            ))
        runner._portfolio = portfolio
        logger.info(
            f"[MULTI] BacktestRunner.from_config: {len(portfolio)} strategy(ies) "
            f"[{', '.join(p.name for p in portfolio)}] on {len(all_symbols)} symbol(s)"
        )
        return runner

    def run_portfolio(self) -> PortfolioReport:
        """确定性 lockstep 组合回放：单一模拟账户 + 单一时间轴，多策略并发。

        共享层（全局统一账户/持仓视图，与实盘同构）：一个 BacktestDataFeed
        （全策略 symbols×periods 并集的统一时间轴）+ 一个 BacktestExecutor（单一
        模拟账户 = initial_capital，组合资金曲线）+ 一个 ExposureLedger + 一个
        IndicatorEngine+锁 + 可选共享 RiskEngine。每 (策略,品种) 独立 KqApi
        （tag=策略名）+ GatedFeed + gate 工作线程，由 backtest_pump.run_lockstep 驱动。
        journal/state=None（回测不持久化）。
        """
        strategies = self._portfolio
        if not strategies:
            raise ValueError("run_portfolio 需经 from_config 装配多策略（单策略请用 run()）")

        if not self._bars:
            self._load_from_mt5()
        nested: Dict[str, Dict[str, List[dict]]] = {}
        for sym in self._symbols:
            bars = self._bars.get(sym)
            if not bars:
                logger.error(f"[{sym}] missing bars, skipped in portfolio")
                continue
            if self._specs.get(sym) is None:
                logger.error(f"[{sym}] missing spec, skipped in portfolio")
                continue
            nested[sym] = self._as_nested(bars)
        if not nested:
            raise ValueError("run_portfolio: 无可用历史数据（bars/spec 缺失）")

        # 共享单一时间轴 feed（on_bar 由 executor 绑定 mark-to-market）
        real_feed = BacktestDataFeed(nested, periods=self._periods)
        executor = BacktestExecutor(
            feed=real_feed,
            specs=self._specs,
            initial_capital=self._initial_capital,
            slippage_model=self._slippage_model,
            slippage_params=self._slippage_params,
            fee_model=self._fee_model,
            fee_params=self._fee_params,
            magic=self._magic,
            account_currency=self._account_currency,
            conv_rates=self._conv_rates,
        )
        ledger = ExposureLedger()
        resolver = UnifiedResolver(order_id_gen=OrderIdFactory().make)
        engine = IndicatorEngine()
        engine.start()
        engine_lock = threading.Lock()

        # 全局风控（需求 7）：多策略共享一个 RiskEngine（tag 隔离归因）；未配置则降级本地检查
        risk_adapter = None
        if self._risk_cfg is not None:
            risk_adapter = assemble_risk(
                self._risk_cfg, ledger, self._portfolio_equity_fn(executor), self._symbols,
            )

        gates: List[Gate] = []
        workers: List[Any] = []
        apis_by_strategy: Dict[str, List[Any]] = {}
        for strat in strategies:
            for sym in strat.symbols:
                if self._specs.get(sym) is None or sym not in nested:
                    continue
                gate = Gate(name=f"{strat.name}:{sym}")
                gfeed = GatedFeed(real_feed, gate)
                api = KqApi(
                    symbol=sym,
                    period=strat.period,
                    periods=strat.periods,
                    tag=strat.name,
                    specs=self._specs,
                    ledger=ledger,
                    resolver=resolver,
                    executor=executor,
                    feed=gfeed,
                    engine=engine,
                    engine_lock=engine_lock,
                    exchange="mt5",
                    strategy_name=strat.name,
                    risk_adapter=risk_adapter,
                    journal=None,
                    state=None,
                )
                gates.append(gate)
                workers.append((self._portfolio_worker(strat, api, gate),
                                f"bt-{strat.name}-{sym}"))
                apis_by_strategy.setdefault(strat.name, []).append((sym, api))

        logger.info(
            f"=== Portfolio backtest: {len(workers)} worker(s), "
            f"timeline={real_feed.n_bars} event(s) ==="
        )
        try:
            run_lockstep(real_feed, gates, workers)
        finally:
            # 数据耗尽后逐策略逐品种清仓（实现残留浮盈），再补一个最终权益点
            for name, lst in apis_by_strategy.items():
                for sym, api in lst:
                    try:
                        api.flatten(symbol=sym)
                    except Exception as e:
                        logger.error(f"[BT:{name}] flatten {sym} error: {e}", exc_info=True)
            executor.mark_equity()
            engine.stop()

        analyzer = PerformanceAnalyzer(bars_per_year=self._bars_per_year())
        init_cap = float(self._initial_capital)
        combined = analyzer.analyze(executor.equity_curve, executor.trades, init_cap)

        strat_results: Dict[str, StrategyBacktestResult] = {}
        for strat in strategies:
            trades = [t for t in executor.trades if t.strategy == strat.name]
            curve = [init_cap]
            acc = init_cap
            for t in trades:
                acc += float(t.pnl)
                curve.append(acc)
            sub = analyzer.analyze(curve, trades, init_cap)
            strat_results[strat.name] = StrategyBacktestResult(
                name=strat.name, report=sub, trades=trades, symbols=list(strat.symbols),
            )

        report = PortfolioReport(
            combined=combined,
            combined_equity_curve=list(executor.equity_curve),
            strategies=strat_results,
            initial_capital=init_cap,
            n_steps=real_feed.n_bars,
        )
        logger.info(
            f"[PORTFOLIO] done: trades={combined.total_trades} "
            f"return={combined.total_return * 100:.2f}% final={combined.final_equity:.2f}"
        )
        return report

    @staticmethod
    def _portfolio_equity_fn(executor: BacktestExecutor) -> Callable[[], Decimal]:
        """组合账户权益（账户币）= balance + 浮动盈亏（RiskDataProvider 日盈亏基线用）。"""
        def _fn() -> Decimal:
            info = executor.query_account()
            if not info:
                return Decimal("0")
            return Decimal(str(info.get("balance", 0))) + Decimal(str(info.get("profit", 0)))
        return _fn

    @staticmethod
    def _portfolio_worker(
        strat: _PortfolioStrategy, api: KqApi, gate: Gate,
    ) -> Callable[[], None]:
        """lockstep 工作线程体：跑策略 → 异常兜底 → drain 门（避免泵 wait_ack 死锁）。

        策略提前退出（break / resilient 放弃 / 异常）后仍须继续消费 gate 事件并 ack，
        否则主线程泵会永久阻塞在该 gate 的 wait_ack（见 Gate.drain 幂等终止语义）。
        """
        def worker() -> None:
            try:
                strat.runner(api)
            except Exception as e:
                logger.error(f"[BT:{strat.name}] strategy error: {e}", exc_info=True)
            finally:
                gate.drain()
        return worker

    def _run_symbol(
        self, sym: str, spec: SymbolInfo, bars_by_period: Dict[str, List[dict]],
        analyzer: PerformanceAnalyzer,
    ) -> SymbolBacktestResult:
        """单品种独立回放：新建 feed/executor/ledger/api，跑同一策略函数"""
        feed = BacktestDataFeed({sym: bars_by_period}, periods=self._periods)
        executor = BacktestExecutor(
            feed=feed,
            specs={sym: spec},
            initial_capital=self._initial_capital,
            slippage_model=self._slippage_model,
            slippage_params=self._slippage_params,
            fee_model=self._fee_model,
            fee_params=self._fee_params,
            magic=self._magic,
            account_currency=self._account_currency,
            conv_rates=(
                {sym: self._conv_rates[sym]} if sym in self._conv_rates else {}
            ),
        )
        ledger = ExposureLedger()
        resolver = UnifiedResolver(order_id_gen=OrderIdFactory().make)
        # Phase 1：进程内指标引擎（与实盘同构，回放循环逐 bar 经 wait_update 桥接推进）
        engine = IndicatorEngine()
        engine.start()
        api = KqApi(
            symbol=sym,
            period=self._period,
            periods=self._periods,
            tag=self._tag,
            specs={sym: spec},
            ledger=ledger,
            resolver=resolver,
            executor=executor,
            feed=feed,
            engine=engine,
            exchange="mt5",
        )

        main_bars = bars_by_period.get(self._period, [])
        logger.info(
            f"[{sym}] strategy starting "
            f"({len(main_bars)} {self._period} bars, periods={self._periods})"
        )
        try:
            self._strategy_fn(api)
        except Exception as e:
            logger.error(f"[{sym}] strategy error: {e}", exc_info=True)

        # 数据耗尽后清仓（实现残留浮盈），再补一个最终权益点
        try:
            api.flatten(symbol=sym)
        except Exception as e:
            logger.error(f"[{sym}] flatten error: {e}", exc_info=True)
        executor.mark_equity()

        perf = analyzer.analyze(
            executor.equity_curve, executor.trades, float(self._initial_capital)
        )
        return SymbolBacktestResult(
            symbol=sym,
            report=perf,
            trades=list(executor.trades),
            equity_curve=list(executor.equity_curve),
            n_bars=len(main_bars),
        )

    def _as_nested(self, val: Any) -> Dict[str, List[dict]]:
        """单品种 bars 归一化为 {period: [bars]}。

        兼容两种注入形态：多周期已是 {period: [bars]} 直接返回；单周期扁平
        [bars]（旧接口/单测）包装为 {self._period: [bars]}。
        """
        if isinstance(val, dict):
            return val
        return {self._period: val}

    # ─── 数据加载 ───

    def _load_from_mt5(self) -> None:
        """从 MT5 终端拉取历史 K 线 + 品种规格（逐周期，多周期嵌套存储）"""
        driver = Mt5Api()
        kwargs = self._mt5_kwargs or Mt5Api.init_kwargs()
        if not driver.initialize(**kwargs):
            raise RuntimeError(
                "MT5 initialize failed（回测取历史数据）。请确认 MT5 终端已启动并登录。"
            )
        tfs = {per: TIMEFRAME_MAP.get(per) for per in self._periods}
        missing = [per for per, tf in tfs.items() if not tf]
        if missing:
            driver.shutdown()
            raise ValueError(f"Unknown period(s): {','.join(missing)}")
        try:
            for sym in self._symbols:
                driver.symbol_select(sym, True)
                spec = load_spec_from_mt5(driver, sym)
                if spec is None:
                    raise RuntimeError(f"Failed to load SymbolInfo for {sym}")
                per_bars: Dict[str, List[dict]] = {}
                for per in self._periods:
                    # 多取 1 根并丢弃最后一根（仍在形成的当前 bar）
                    rows = driver.copy_rates_from_pos(
                        sym, tfs[per], 0, self._history_bars + 1
                    )
                    if not rows or len(rows) < 2:
                        raise RuntimeError(
                            f"Insufficient history for {sym}/{per}: "
                            f"got {len(rows) if rows else 0} bars"
                        )
                    per_bars[per] = [
                        self._row_to_bar(r, sym, per) for r in rows[:-1]
                    ]
                self._bars[sym] = per_bars
                self._specs[sym] = spec
                rate = self._load_conv_rate(driver, sym, spec)
                if rate is not None:
                    self._conv_rates[sym] = rate
                    logger.info(f"[{sym}] cross-rate →{self._account_currency} = {rate}")
                logger.info(
                    f"[{sym}] loaded "
                    + ", ".join(f"{len(per_bars[p])} {p}" for p in self._periods)
                    + f" bars (pip={spec.pip_size} mult={spec.contract_multiplier})"
                )
        finally:
            driver.shutdown()

    def _load_conv_rate(
        self, driver: Mt5Api, sym: str, spec: SymbolInfo,
    ) -> Optional[Decimal]:
        """交叉盘 quote→账户币 换算系数（直盘返回 None）。

        用 MT5 order_calc_profit（返回**账户币**利润，内部走交叉汇率）推导：
        取价差 1.0、volume 1.0，则 gross_quote = 1×1×mult（计价货币），
        rate = profit(账户币) / gross_quote = 每单位计价货币的账户币价值。
        直盘（base/quote 有一端=账户币）无需系数，_conv 用回测时点价更精确。
        """
        q = (spec.quote_currency or "").upper()
        b = (spec.base_currency or "").upper()
        acct = self._account_currency.upper()
        if q == acct or b == acct:
            return None
        tick = driver.symbol_info_tick(sym)
        p = float(tick.get("bid") or 0) if tick else 0.0
        mult = float(spec.contract_multiplier or 0)
        if p <= 0 or mult <= 0:
            return None
        profit = driver.order_calc_profit(0, sym, 1.0, p, p + 1.0)  # ORDER_TYPE_BUY
        if profit is None:
            return None
        return Decimal(str(profit)) / Decimal(str(mult))

    def _row_to_bar(self, row: dict, symbol: str, period: str) -> dict:
        """MT5 rate row → 标准 bar dict（与 Mt5DataFeed 同形）"""
        vol = row.get("real_volume") or row.get("tick_volume") or 0
        return {
            "symbol": symbol,
            "period": period,
            "timestamp": int(row["time"]) * 1000,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(vol),
        }

    def _bars_per_year(self) -> int:
        """年化换算（365 天约定，与现有回测路由一致）。

        多周期取**最细周期**（bar 最密）作年化基准，与逐事件回放的采样密度对齐。
        """
        per = min(self._periods, key=lambda p: TF_SECONDS.get(p, 60))
        sec = TF_SECONDS.get(per, 60)
        return max(1, int(365 * 86400 / sec))

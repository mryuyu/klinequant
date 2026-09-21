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
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Callable, Dict, List, Optional

from core.backtest_engine.performance import (
    PerformanceAnalyzer,
    PerformanceReport,
    Trade,
)
from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import UnifiedResolver
from core.trade_engine.spec_loader import load_spec_from_mt5
from gateway.market_sources.mt5_driver import TF_SECONDS, TIMEFRAME_MAP, Mt5Api
from protocol.types import SymbolInfo
from strategy.sdk.api import KqApi
from strategy.sdk.backtest_executor import BacktestExecutor
from strategy.sdk.backtest_feed import BacktestDataFeed

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


class BacktestRunner:
    """回测运行器"""

    def __init__(
        self,
        symbols,
        period: str,
        strategy_fn: Callable[[KqApi], None],
        *,
        tag: str = "",
        history_bars: int = 2000,
        initial_capital: Decimal = Decimal("10000"),
        slippage_model: str = "percentage",
        slippage_params: Optional[dict] = None,
        fee_model: str = "fixed",
        fee_params: Optional[dict] = None,
        magic: int = 202609,
        bars_by_symbol: Optional[Dict[str, List[dict]]] = None,
        specs: Optional[Dict[str, SymbolInfo]] = None,
        mt5_kwargs: Optional[dict] = None,
    ):
        """
        Args:
            symbols: 品种，str 或 list（逐个独立回测）
            period: 驱动周期（如 "1m"）
            strategy_fn: 策略函数 (api: KqApi) -> None，与实盘同一份
            history_bars: 从 MT5 拉取的历史 bar 数（bars_by_symbol 提供时忽略）
            initial_capital: 每品种初始资金（计价货币）
            slippage_model/slippage_params: 滑点模型（默认 percentage）
            fee_model/fee_params: 手续费模型（默认 fixed，每笔 0）
            bars_by_symbol: 直接注入历史 bar（单测/离线数据用，跳过 MT5）
            specs: 直接注入品种规格（配合 bars_by_symbol 用）
            mt5_kwargs: MT5 初始化参数
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        self._symbols: List[str] = [s.upper() for s in symbols]
        if not self._symbols:
            raise ValueError("BacktestRunner requires at least one symbol")
        self._period = period
        self._strategy_fn = strategy_fn
        self._tag = tag or period
        self._history_bars = history_bars
        self._initial_capital = Decimal(str(initial_capital))
        self._slippage_model = slippage_model
        self._slippage_params = slippage_params or {"pct": Decimal("0.0001")}
        self._fee_model = fee_model
        self._fee_params = fee_params or {"fee_per_trade": Decimal("0")}
        self._magic = magic
        self._mt5_kwargs = mt5_kwargs

        self._bars: Dict[str, List[dict]] = bars_by_symbol or {}
        self._specs: Dict[str, SymbolInfo] = specs or {}

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
            result = self._run_symbol(sym, spec, bars, analyzer)
            report.results[sym] = result
            logger.info(
                f"[{sym}] done: bars={result.n_bars} trades={result.report.total_trades} "
                f"return={result.report.total_return * 100:.2f}% "
                f"final={result.report.final_equity:.2f}"
            )
        return report

    def _run_symbol(
        self, sym: str, spec: SymbolInfo, bars: List[dict],
        analyzer: PerformanceAnalyzer,
    ) -> SymbolBacktestResult:
        """单品种独立回放：新建 feed/executor/ledger/api，跑同一策略函数"""
        feed = BacktestDataFeed({sym: bars}, self._period)
        executor = BacktestExecutor(
            feed=feed,
            specs={sym: spec},
            initial_capital=self._initial_capital,
            slippage_model=self._slippage_model,
            slippage_params=self._slippage_params,
            fee_model=self._fee_model,
            fee_params=self._fee_params,
            magic=self._magic,
        )
        ledger = ExposureLedger()
        resolver = UnifiedResolver()
        api = KqApi(
            symbol=sym,
            period=self._period,
            tag=self._tag,
            specs={sym: spec},
            ledger=ledger,
            resolver=resolver,
            executor=executor,
            feed=feed,
        )

        logger.info(f"[{sym}] strategy starting ({len(bars)} bars)")
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
            n_bars=len(bars),
        )

    # ─── 数据加载 ───

    def _load_from_mt5(self) -> None:
        """从 MT5 终端拉取历史 K 线 + 品种规格"""
        driver = Mt5Api()
        kwargs = self._mt5_kwargs or Mt5Api.init_kwargs()
        if not driver.initialize(**kwargs):
            raise RuntimeError(
                "MT5 initialize failed（回测取历史数据）。请确认 MT5 终端已启动并登录。"
            )
        tf = TIMEFRAME_MAP.get(self._period)
        if not tf:
            driver.shutdown()
            raise ValueError(f"Unknown period: {self._period}")
        try:
            for sym in self._symbols:
                driver.symbol_select(sym, True)
                spec = load_spec_from_mt5(driver, sym)
                if spec is None:
                    raise RuntimeError(f"Failed to load SymbolInfo for {sym}")
                # 多取 1 根并丢弃最后一根（仍在形成的当前 bar）
                rows = driver.copy_rates_from_pos(sym, tf, 0, self._history_bars + 1)
                if not rows or len(rows) < 2:
                    raise RuntimeError(
                        f"Insufficient history for {sym}: got {len(rows) if rows else 0} bars"
                    )
                completed = rows[:-1]
                self._bars[sym] = [self._row_to_bar(r, sym) for r in completed]
                self._specs[sym] = spec
                logger.info(
                    f"[{sym}] loaded {len(self._bars[sym])} historical bars "
                    f"(pip={spec.pip_size} mult={spec.contract_multiplier})"
                )
        finally:
            driver.shutdown()

    def _row_to_bar(self, row: dict, symbol: str) -> dict:
        """MT5 rate row → 标准 bar dict（与 Mt5DataFeed 同形）"""
        vol = row.get("real_volume") or row.get("tick_volume") or 0
        return {
            "symbol": symbol,
            "period": self._period,
            "timestamp": int(row["time"]) * 1000,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(vol),
        }

    def _bars_per_year(self) -> int:
        """年化换算（365 天约定，与现有回测路由一致）"""
        sec = TF_SECONDS.get(self._period, 60)
        return max(1, int(365 * 86400 / sec))

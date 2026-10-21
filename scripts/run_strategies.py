"""KlineQuant 多策略统一入口（配置驱动，实盘/回测同一份策略代码）

一份 ``config/strategies.yaml`` 描述多策略运行（标的/周期/风控/策略参数/回测参数），
本脚本按 ``--mode`` 装配对应运行器：

  live      → LiveRunner.from_config（多策略共享账本/执行器/风控/连接，
              每策略独立 tag/StateStore/KqApi/工作线程）
  backtest  → BacktestRunner.from_config + run_portfolio（确定性 lockstep 组合回放，
              单一模拟账户，组合权益曲线 + 分策略子报表）

账户仍走 ``config/accounts.yaml`` + ``resolve_account``（一进程一账户，凭证 ``${ENV_VAR}``
插值），backend 按 ``account.market`` 选择（fx=Mt5Backend / crypto=BinanceBackend）。

日志：每次运行独立文件 ``logs/strategies_{mode}_{YYYYmmdd_HHMMSS}.log`` + 控制台，
全部框架日志带策略名前缀（KqApi.log 已按 ``[STRATEGY:{name}:{symbol}]`` 前缀）。

运行：
    cd klinequant
    python scripts/run_strategies.py --config config/strategies.yaml --mode live --account fx-demo
    python scripts/run_strategies.py --mode backtest --only dualma-eur
    # 旧式单策略兼容（不经 yaml）：
    python scripts/run_strategies.py --mode live --strategy fx_cross_ema --symbols EURUSD
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 确保项目根目录在 sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from klinequant.config.accounts import (  # noqa: E402
    AccountConfig,
    AccountConfigError,
    resolve_account,
)
from klinequant.config.strategy_config import (  # noqa: E402
    StrategiesConfig,
    StrategyConfigError,
    load_strategies_config,
)
from klinequant.gateway.env import load_env  # noqa: E402

load_env()  # 凭证从仓库根 .env 加载（已有环境变量不覆盖）

from klinequant.kqsdk.backend import BinanceBackend, MarketBackend, Mt5Backend  # noqa: E402
from klinequant.kqsdk.broadcaster import build_broadcaster_from_account  # noqa: E402
from klinequant.kqsdk.live_runner import LiveRunner  # noqa: E402
from klinequant.kqsdk.order_journal import SqliteJournal  # noqa: E402
from klinequant.kqsdk.state_store import SqliteStateBackend  # noqa: E402

logger = logging.getLogger("run_strategies")


def setup_logging(mode: str, verbose: bool = False) -> Path:
    """配置日志：每次运行独立文件 + 控制台（需求 10：完整日志）。"""
    level = logging.DEBUG if verbose else logging.INFO
    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"strategies_{mode}_{time.strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    for noisy in ("urllib3", "asyncio", "httpx", "httpcore", "websockets"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return log_file


def _union_symbols(cfg: StrategiesConfig, only: list[str] | None) -> list[str]:
    """全策略（--only 过滤后）品种并集（backend/feed 订阅用）。"""
    out: list[str] = []
    for s in cfg.enabled_strategies():
        if only and s.name not in only:
            continue
        for sym in s.symbols:
            if sym not in out:
                out.append(sym)
    return out


def _build_backend(
    market: str,
    account: AccountConfig | None,
    symbols: list[str],
    cfg: StrategiesConfig,
) -> MarketBackend:
    """按 market 构造后端（fx=Mt5Backend / crypto=BinanceBackend）。"""
    d = cfg.defaults
    if market == "crypto":
        if account is not None and account.binance is not None:
            b = account.binance
            rest_base = b.rest_base or os.getenv(
                "BINANCE_FUTURES_REST_BASE", "https://demo-fapi.binance.com")
            ws_base = b.ws_base or os.getenv(
                "BINANCE_FUTURES_WS_BASE", "wss://demo-fstream.binance.com/ws")
            api_key, api_secret = b.api_key, b.api_secret
            proxy = b.proxy or None
        else:
            rest_base = os.getenv("BINANCE_FUTURES_REST_BASE", "https://demo-fapi.binance.com")
            ws_base = os.getenv("BINANCE_FUTURES_WS_BASE", "wss://demo-fstream.binance.com/ws")
            api_key = os.getenv("BINANCE_FUTURES_API_KEY", "")
            api_secret = os.getenv("BINANCE_FUTURES_API_SECRET", "")
            proxy = os.getenv("HTTP_PROXY", "http://127.0.0.1:7897") or None
        if not api_key or not api_secret:
            raise SystemExit(
                "ERROR: 缺少币安 Futures 凭证：请在仓库根 .env 配置 "
                "BINANCE_FUTURES_API_KEY / BINANCE_FUTURES_API_SECRET"
            )
        return BinanceBackend(
            symbols,
            rest_base=rest_base,
            ws_base=ws_base,
            api_key=api_key,
            api_secret=api_secret,
            proxy=proxy,
            poll_interval=d.poll_interval,
            bar_count=d.bar_count,
            account=account,
        )
    # fx（默认）：magic/deviation 取 defaults；mt5_kwargs 取账户
    mt5_kwargs = account.mt5.init_kwargs() if (account and account.mt5) else None
    return Mt5Backend(
        magic=d.magic, deviation=d.deviation, mt5_kwargs=mt5_kwargs, account=account,
    )


def _run_live(cfg: StrategiesConfig, args: argparse.Namespace, only: list[str] | None) -> None:
    """实盘：resolve_account → backend → LiveRunner.from_config → run()。"""
    symbols = _union_symbols(cfg, only)
    if not symbols:
        raise SystemExit("ERROR: 没有可运行的策略/品种（检查 enabled / --only）")
    account = _resolve(args)
    market = account.market if account is not None else args.market
    backend = _build_backend(market, account, symbols, cfg)

    acct_key = account.name if account else "default"
    journal = None
    if not args.no_journal:
        journal = SqliteJournal(ROOT / "data" / "journal" / f"{acct_key}.db")
    state_backend = None
    if not args.no_state:
        state_backend = SqliteStateBackend(ROOT / "data" / "state" / f"{acct_key}.db")
    broadcaster = build_broadcaster_from_account(account)

    _banner_live(cfg, args, only, account, market, symbols, broadcaster is not None)

    runner = LiveRunner.from_config(
        cfg,
        backend,
        account_name=acct_key if account else "",
        journal=journal,
        state_backend=state_backend,
        only=only,
        broadcaster=broadcaster,
    )
    runner.run()


def _run_backtest(cfg: StrategiesConfig, args: argparse.Namespace, only: list[str] | None) -> None:
    """回测：BacktestRunner.from_config → run_portfolio（组合 + 分策略报表）。

    数据源：MT5 历史（FX）。加密回测需注入 bars（本脚本暂不拉币安历史，见
    BacktestRunner.from_config 的 bars_by_symbol 参数）。
    """
    from klinequant.kqsdk.backtest_runner import BacktestRunner

    account = _resolve(args, market="fx")
    mt5_kwargs = account.mt5.init_kwargs() if (account and account.mt5) else None

    _banner_backtest(cfg, args, only, account)

    runner = BacktestRunner.from_config(cfg, only=only, mt5_kwargs=mt5_kwargs)
    report = runner.run_portfolio()
    print()
    print(report.summary())


def _resolve(args: argparse.Namespace, market: str | None = None) -> AccountConfig | None:
    """解析账户（CLI --account > env KQ_ACCOUNT > main）。market 用于回落筛选。"""
    m = market or (args.market if args.market else None)
    try:
        return resolve_account(args.account or None, market=m)
    except AccountConfigError as e:
        raise SystemExit(f"ERROR: {e}")


def _banner_live(cfg, args, only, account, market, symbols, is_lead) -> None:
    names = [s.name for s in cfg.enabled_strategies() if not only or s.name in only]
    acct_desc = (
        f"{account.name} (market={account.market}, role={account.role})"
        if account else "<env default> (no account bound)"
    )
    print("=" * 64)
    print("  KlineQuant Multi-Strategy LIVE Runner")
    print(f"  Config:   {args.config}")
    print(f"  Account:  {acct_desc}")
    print(f"  Market:   {market}   Symbols: {','.join(symbols)}")
    print(f"  Strategies ({len(names)}): {', '.join(names)}")
    if is_lead:
        print("  Distribution: LEAD (broadcast intents to followers)")
    print("=" * 64)
    print("  Ctrl+C to stop gracefully (auto-flatten on exit)")
    print()


def _banner_backtest(cfg, args, only, account) -> None:
    names = [s.name for s in cfg.enabled_strategies() if not only or s.name in only]
    acct_desc = account.name if account else "<env default>"
    print("=" * 64)
    print("  KlineQuant Multi-Strategy PORTFOLIO BACKTEST")
    print(f"  Config:   {args.config}")
    print(f"  Account:  {acct_desc} (MT5 history source)")
    print(f"  Strategies ({len(names)}): {', '.join(names)}")
    print(f"  Capital:  {cfg.backtest.initial_capital}  History: {cfg.backtest.history_bars} bars")
    print("=" * 64)
    print()


def _run_legacy(args: argparse.Namespace) -> None:
    """旧式单策略兼容路径（--strategy 模块名，不经 yaml）。

    等价 run_fx_live.py / run_fx_backtest.py 的单策略用法，便于从统一入口跑既有策略。
    """
    import importlib
    from decimal import Decimal

    from klinequant.kqsdk.backtest_runner import BacktestRunner

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        raise SystemExit("ERROR: --strategy 兼容路径需 --symbols 指定至少一个品种")
    try:
        mod = importlib.import_module(f"strategies.{args.strategy}")
    except ModuleNotFoundError:
        raise SystemExit(f"ERROR: 策略模块 strategies/{args.strategy}.py 不存在")
    strategy_fn = getattr(mod, "strategy", None)
    if strategy_fn is None:
        raise SystemExit(f"ERROR: strategies/{args.strategy}.py 中没有 strategy 函数")

    account = _resolve(args)
    market = account.market if account is not None else args.market

    if args.mode == "backtest":
        mt5_kwargs = account.mt5.init_kwargs() if (account and account.mt5) else None
        runner = BacktestRunner(
            symbols=symbols, period=args.period, strategy_fn=strategy_fn,
            history_bars=args.bars, initial_capital=Decimal(str(args.capital)),
            mt5_kwargs=mt5_kwargs,
        )
        print(runner.run().summary())
        return

    cfg = StrategiesConfig()  # 仅取 defaults（magic/deviation/poll/bars）
    backend = _build_backend(market, account, symbols, cfg)
    acct_key = account.name if account else "default"
    journal = None if args.no_journal else SqliteJournal(
        ROOT / "data" / "journal" / f"{acct_key}.db")
    state_backend = None if args.no_state else SqliteStateBackend(
        ROOT / "data" / "state" / f"{acct_key}.db")
    LiveRunner(
        backend, symbols=symbols, period=args.period, strategy_fn=strategy_fn,
        poll_interval=cfg.defaults.poll_interval, bar_count=args.bars,
        duration=args.duration if args.duration > 0 else None,
        account_name=acct_key if account else "",
        journal=journal, state_backend=state_backend,
        stale_threshold=args.stale_guard if args.stale_guard > 0 else None,
        broadcaster=build_broadcaster_from_account(account),
    ).run()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="KlineQuant 多策略统一入口（配置驱动，实盘/回测同构）"
    )
    parser.add_argument("--config", default="config/strategies.yaml",
                        help="策略配置文件 (default: config/strategies.yaml)")
    parser.add_argument("--mode", choices=["live", "backtest"], default="live",
                        help="运行模式 (default: live)")
    parser.add_argument("--account", default="",
                        help="账户名（config/accounts.yaml，default: 空=回落 main/env）")
    parser.add_argument("--market", default="fx", choices=["fx", "crypto"],
                        help="市场（未绑定账户时用于选择 backend，default: fx）")
    parser.add_argument("--only", default="",
                        help="只运行这些策略名，逗号分隔 (default: 全部 enabled)")
    parser.add_argument("--strategy", default="",
                        help="旧式单策略模块名（兼容，不经 yaml；需配 --symbols/--period）")
    parser.add_argument("--symbols", "--symbol", dest="symbols", default="EURUSD",
                        help="旧式单策略路径品种，逗号分隔 (default: EURUSD)")
    parser.add_argument("--period", default="1m", help="旧式单策略路径周期 (default: 1m)")
    parser.add_argument("--bars", type=int, default=2000,
                        help="旧式单策略路径历史/预热 bar 数 (default: 2000)")
    parser.add_argument("--capital", type=float, default=10000.0,
                        help="旧式单策略回测初始资金 (default: 10000)")
    parser.add_argument("--duration", type=float, default=0.0,
                        help="旧式单策略实盘运行时长秒 (default: 0=不限时)")
    parser.add_argument("--stale-guard", type=float, default=120.0,
                        help="R5 断线闸门阈值秒 (default: 120, 0=禁用)")
    parser.add_argument("--no-journal", action="store_true", help="禁用订单意图 WAL")
    parser.add_argument("--no-state", action="store_true", help="禁用策略语义状态持久化")
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG 日志")
    args = parser.parse_args()

    log_file = setup_logging(args.mode, args.verbose)
    only = [s.strip() for s in args.only.split(",") if s.strip()] or None
    logger.info(f"日志文件: {log_file}")

    # 旧式单策略兼容路径（--strategy 给定则不经 yaml）
    if args.strategy:
        _run_legacy(args)
        return

    try:
        cfg = load_strategies_config(args.config)
    except StrategyConfigError as e:
        raise SystemExit(f"ERROR: {e}")

    if args.mode == "backtest":
        _run_backtest(cfg, args, only)
    else:
        _run_live(cfg, args, only)


if __name__ == "__main__":
    main()

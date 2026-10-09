"""FX 实盘策略启动脚本

前提条件：
  1. MT5 终端已启动并登录 Demo 账户（IC Markets）
  2. MetaTrader5 Python 包已安装：pip install MetaTrader5
  3. 环境变量（可选，缺省连接本机已运行终端）：
     - MT5_TERMINAL_PATH: MT5 终端路径
     - MT5_LOGIN: 账户号
     - MT5_PASSWORD: 密码
     - MT5_SERVER: 服务器名

运行：
    cd klinequant
    python scripts/run_fx_live.py
    python scripts/run_fx_live.py --symbols EURUSD,GBPUSD,USDJPY --period 5m
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# 确保项目根目录在 sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from strategy.sdk.backend import Mt5Backend  # noqa: E402
from strategy.sdk.live_runner import LiveRunner  # noqa: E402
from strategy.sdk.order_journal import SqliteJournal  # noqa: E402
from strategy.sdk.state_store import SqliteStateBackend  # noqa: E402
from config.accounts import AccountConfigError, resolve_account  # noqa: E402


def setup_logging(verbose: bool = False) -> None:
    """配置日志"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(ROOT / "logs" / "fx_live.log", encoding="utf-8"),
        ],
    )
    # 降低第三方噪音
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser(description="KlineQuant FX Live Strategy Runner")
    parser.add_argument("--symbols", "--symbol", dest="symbols", default="EURUSD",
                        help="交易品种，逗号分隔 (default: EURUSD)，如 EURUSD,GBPUSD,USDJPY")
    parser.add_argument("--period", default="1m", help="驱动周期 (default: 1m)")
    parser.add_argument("--tag", default="", help="策略标签 (default: =period)")
    parser.add_argument("--poll", type=float, default=0.5, help="轮询间隔秒 (default: 0.5)")
    parser.add_argument("--bars", type=int, default=300, help="K线加载数量 (default: 300)")
    parser.add_argument("--magic", type=int, default=202609, help="MT5 magic number")
    parser.add_argument("--deviation", type=int, default=20, help="滑点容忍 points (default: 20)")
    parser.add_argument("--duration", type=float, default=3600.0,
                        help="运行时长秒，到点自动清仓退出 (default: 3600=1小时, 0=不限时)")
    parser.add_argument("--strategy", default="fx_simple_test",
                        help="策略模块名 (default: fx_simple_test)，"
                             "如 fx_cross_ema / fx_macd_demo，"
                             "对应 strategies/<name>.py 中的 strategy 函数")
    parser.add_argument("--account", default="",
                        help="账户名（config/accounts.yaml，default: 空=回落 main/env）")
    parser.add_argument("--no-journal", action="store_true",
                        help="禁用订单意图 WAL（默认启用，data/journal/{account}.db）")
    parser.add_argument("--no-state", action="store_true",
                        help="禁用策略语义状态持久化（默认启用，data/state/{account}.db）")
    parser.add_argument("--stale-guard", type=float, default=120.0,
                        help="R5 断线闸门阈值秒，feed 心跳龄超此值拒新开仓/放平仓 "
                             "(default: 120=2分钟, 0=禁用)")
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG 日志")
    args = parser.parse_args()

    setup_logging(args.verbose)

    # 确保日志目录存在
    (ROOT / "logs").mkdir(exist_ok=True)

    # 动态导入策略模块
    import importlib
    strategy_name = args.strategy
    try:
        mod = importlib.import_module(f"strategies.{strategy_name}")
    except ModuleNotFoundError:
        print(f"ERROR: 策略模块 strategies/{strategy_name}.py 不存在")
        sys.exit(1)
    if not hasattr(mod, "strategy"):
        print(f"ERROR: strategies/{strategy_name}.py 中没有 strategy 函数")
        sys.exit(1)
    strategy = mod.strategy

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        parser.error("--symbols 解析为空，请至少指定一个品种")

    # 解析账户（CLI --account > env KQ_ACCOUNT > main:true；无则 None=回落 env 行为）
    try:
        account = resolve_account(args.account or None, market="fx")
    except AccountConfigError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    dur_desc = f"{args.duration:.0f}s (auto-flatten at timeout)" if args.duration > 0 else "unlimited"
    stale_desc = (
        f"{args.stale_guard:.0f}s (reject OPEN if feed degraded)"
        if args.stale_guard > 0 else "disabled"
    )
    acct_desc = (
        f"{account.name} (market={account.market}"
        f"{', main' if account.main else ''}, role={account.role})"
        if account else "<env default> (no account bound)"
    )
    print("=" * 60)
    print("  KlineQuant FX Live Runner")
    print(f"  Account: {acct_desc}")
    print(f"  Symbols: {','.join(symbols)}  Period: {args.period}")
    print(f"  Strategy: {args.strategy}")
    print(f"  Mode: MT5 Demo (real MARKET orders)")
    print(f"  Duration: {dur_desc}")
    print(f"  Stale guard: {stale_desc}")
    print("=" * 60)
    print()
    print("  Ctrl+C to stop gracefully (auto-flatten on exit)")
    print()

    backend = Mt5Backend(magic=args.magic, deviation=args.deviation, account=account)

    # R2 订单意图 WAL（崩溃恢复账本级真相源，先写后发；--no-journal 关闭）
    journal = None
    if not args.no_journal:
        acct_key = account.name if account else "default"
        journal = SqliteJournal(ROOT / "data" / "journal" / f"{acct_key}.db")

    # R4 策略语义状态持久化（崩溃恢复语义级快照，自动 load/save；--no-state 关闭）
    state_backend = None
    if not args.no_state:
        acct_key = account.name if account else "default"
        state_backend = SqliteStateBackend(ROOT / "data" / "state" / f"{acct_key}.db")

    runner = LiveRunner(
        backend,
        symbols=symbols,
        period=args.period,
        strategy_fn=strategy,
        tag=args.tag,
        poll_interval=args.poll,
        bar_count=args.bars,
        duration=args.duration if args.duration > 0 else None,
        journal=journal,
        state_backend=state_backend,
        stale_threshold=args.stale_guard if args.stale_guard > 0 else None,
    )
    runner.run()


if __name__ == "__main__":
    main()

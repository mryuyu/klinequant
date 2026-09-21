"""FX 回测启动脚本（回测/实盘同构）

用 MT5 历史数据回放，运行与实盘**完全相同**的 strategy(api: KqApi) 函数。
KqApi / Ledger / Resolver 不变，仅数据源与执行器换成回测版。

前提条件：
  1. MT5 终端已启动并登录（用于拉取历史 K 线与品种规格）
  2. MetaTrader5 Python 包已安装：pip install MetaTrader5

运行：
    cd klinequant
    python scripts/run_fx_backtest.py --symbols EURUSD --bars 3000
    python scripts/run_fx_backtest.py --symbols EURUSD,GBPUSD,AUDUSD,USDJPY --period 1m
    python scripts/run_fx_backtest.py --strategy fx_simple_test --capital 10000
"""
from __future__ import annotations

import argparse
import importlib
import logging
import sys
from decimal import Decimal
from pathlib import Path

# 确保项目根目录在 sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from strategy.sdk.backtest_runner import BacktestRunner  # noqa: E402


def setup_logging(verbose: bool = False) -> None:
    """配置日志"""
    level = logging.DEBUG if verbose else logging.INFO
    (ROOT / "logs").mkdir(exist_ok=True)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(ROOT / "logs" / "fx_backtest.log", encoding="utf-8"),
        ],
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def _load_strategy(name: str):
    """按模块名从 strategies 包动态导入 strategy 函数"""
    module = importlib.import_module(f"strategies.{name}")
    fn = getattr(module, "strategy", None)
    if fn is None:
        raise AttributeError(f"strategies.{name} 未定义 strategy(api) 函数")
    return fn


def main():
    parser = argparse.ArgumentParser(description="KlineQuant FX Backtest Runner (isomorphic)")
    parser.add_argument("--symbols", "--symbol", dest="symbols", default="EURUSD",
                        help="回测品种，逗号分隔 (default: EURUSD)")
    parser.add_argument("--period", default="1m", help="驱动周期 (default: 1m)")
    parser.add_argument("--strategy", default="fx_simple_test",
                        help="策略模块名（strategies 包下，default: fx_simple_test）")
    parser.add_argument("--bars", type=int, default=2000,
                        help="历史 K 线数量 (default: 2000)")
    parser.add_argument("--capital", type=float, default=10000.0,
                        help="每品种初始资金 (default: 10000)")
    parser.add_argument("--tag", default="", help="策略标签 (default: =period)")
    parser.add_argument("--slippage", default="percentage",
                        help="滑点模型 percentage/fixed/volume_based (default: percentage)")
    parser.add_argument("--slippage-value", type=float, default=0.0001,
                        help="滑点参数值（percentage=pct, fixed=ticks）(default: 0.0001)")
    parser.add_argument("--fee", default="fixed",
                        help="手续费模型 fixed/percentage/tiered (default: fixed)")
    parser.add_argument("--fee-value", type=float, default=0.0,
                        help="手续费参数值（fixed=每笔金额, percentage=费率）(default: 0)")
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG 日志")
    args = parser.parse_args()

    setup_logging(args.verbose)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        parser.error("--symbols 解析为空，请至少指定一个品种")
    if args.bars <= 0:
        parser.error("--bars 必须为正数")

    strategy_fn = _load_strategy(args.strategy)

    # 按模型类型组装参数字典（与引擎工厂函数对齐）
    slippage_params = _slippage_params(args.slippage, args.slippage_value)
    fee_params = _fee_params(args.fee, args.fee_value)

    print("=" * 60)
    print("  KlineQuant FX Backtest Runner (回测/实盘同构)")
    print(f"  Symbols: {','.join(symbols)}  Period: {args.period}")
    print(f"  Strategy: {args.strategy}  History: {args.bars} bars")
    print(f"  Capital/symbol: {args.capital}  "
          f"Slippage: {args.slippage}={args.slippage_value}  "
          f"Fee: {args.fee}={args.fee_value}")
    print(f"  Data: MT5 historical bars (real spec: pip/step/mult)")
    print("=" * 60)
    print()

    runner = BacktestRunner(
        symbols=symbols,
        period=args.period,
        strategy_fn=strategy_fn,
        tag=args.tag,
        history_bars=args.bars,
        initial_capital=Decimal(str(args.capital)),
        slippage_model=args.slippage,
        slippage_params=slippage_params,
        fee_model=args.fee,
        fee_params=fee_params,
    )
    report = runner.run()
    print()
    print(report.summary())


def _slippage_params(model: str, value: float) -> dict:
    if model == "percentage":
        return {"pct": Decimal(str(value))}
    if model == "fixed":
        return {"ticks": Decimal(str(value))}
    if model == "volume_based":
        return {"impact_factor": Decimal(str(value))}
    return {}


def _fee_params(model: str, value: float) -> dict:
    if model == "percentage":
        return {"rate": Decimal(str(value))}
    if model == "fixed":
        return {"fee_per_trade": Decimal(str(value))}
    if model == "tiered":
        return {"maker_rate": Decimal(str(value)), "taker_rate": Decimal(str(value))}
    return {}


if __name__ == "__main__":
    main()

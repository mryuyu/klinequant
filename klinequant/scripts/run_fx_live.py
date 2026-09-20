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
    python scripts/run_fx_live.py --symbol GBPUSD --period 5m
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# 确保项目根目录在 sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from strategy.sdk.live_runner import LiveRunner  # noqa: E402


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
    parser.add_argument("--symbol", default="EURUSD", help="交易品种 (default: EURUSD)")
    parser.add_argument("--period", default="1m", help="驱动周期 (default: 1m)")
    parser.add_argument("--tag", default="", help="策略标签 (default: =period)")
    parser.add_argument("--poll", type=float, default=0.5, help="轮询间隔秒 (default: 0.5)")
    parser.add_argument("--bars", type=int, default=300, help="K线加载数量 (default: 300)")
    parser.add_argument("--magic", type=int, default=202609, help="MT5 magic number")
    parser.add_argument("--deviation", type=int, default=20, help="滑点容忍 points (default: 20)")
    parser.add_argument("--duration", type=float, default=3600.0,
                        help="运行时长秒，到点自动清仓退出 (default: 3600=1小时, 0=不限时)")
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG 日志")
    args = parser.parse_args()

    setup_logging(args.verbose)

    # 确保日志目录存在
    (ROOT / "logs").mkdir(exist_ok=True)

    # 导入策略
    from strategies.fx_simple_test import strategy

    dur_desc = f"{args.duration:.0f}s (auto-flatten at timeout)" if args.duration > 0 else "unlimited"
    print("=" * 60)
    print("  KlineQuant FX Live Runner")
    print(f"  Symbol: {args.symbol}  Period: {args.period}")
    print(f"  Strategy: fx_simple_test (MACD>0 + close>EMA10 = long, else short)")
    print(f"  Mode: MT5 Demo (real MARKET orders)")
    print(f"  Duration: {dur_desc}")
    print("=" * 60)
    print()
    print("  Ctrl+C to stop gracefully (auto-flatten on exit)")
    print()

    runner = LiveRunner(
        symbol=args.symbol,
        period=args.period,
        strategy_fn=strategy,
        tag=args.tag,
        poll_interval=args.poll,
        bar_count=args.bars,
        magic=args.magic,
        deviation=args.deviation,
        duration=args.duration if args.duration > 0 else None,
    )
    runner.run()


if __name__ == "__main__":
    main()

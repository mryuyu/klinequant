"""加密（币安 Futures Demo）同构实盘模拟启动脚本

与 run_fx_live.py 同构：同一份 strategy(api: KqApi) + 同一个 LiveRunner，
仅把注入的 MarketBackend 从 Mt5Backend 换成 BinanceBackend（币安 USDT-M
Futures Demo，One-way 净持仓）。真实下单（Demo 资金），非回测。

前提条件：
  1. klinequant/.env 配置币安 Futures Demo 凭证（见 .env.example）：
     - BINANCE_FUTURES_API_KEY / BINANCE_FUTURES_API_SECRET
     - BINANCE_FUTURES_REST_BASE（默认 https://demo-fapi.binance.com）
     - BINANCE_FUTURES_WS_BASE（默认 wss://demo-fstream.binance.com/ws）
     - HTTP_PROXY（REST + WS 均需代理，默认 http://127.0.0.1:7897）
  2. 依赖已装：pip install httpx websockets

运行：
    cd klinequant
    python scripts/run_crypto_live.py
    python scripts/run_crypto_live.py --symbols BTCUSDT --period 1m --duration 3600
"""
from __future__ import annotations

import argparse
import importlib
import logging
import os
import sys
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 确保项目根目录在 sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from gateway.env import load_env  # noqa: E402

load_env()  # 凭证从 klinequant/.env 加载（已有环境变量不覆盖）

from strategy.sdk.backend import BinanceBackend  # noqa: E402
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
            logging.FileHandler(ROOT / "logs" / "crypto_live.log", encoding="utf-8"),
        ],
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def _load_strategy(name: str):
    """动态加载 strategies.{name}.strategy"""
    module = importlib.import_module(f"strategies.{name}")
    strategy_fn = getattr(module, "strategy", None)
    if strategy_fn is None:
        raise AttributeError(f"strategies.{name} 缺少 strategy(api) 函数")
    return strategy_fn


def main():
    parser = argparse.ArgumentParser(
        description="KlineQuant Crypto Live (Binance Futures Demo) Runner"
    )
    parser.add_argument("--symbols", "--symbol", dest="symbols", default="BTCUSDT",
                        help="交易对，逗号分隔 (default: BTCUSDT)，如 BTCUSDT,ETHUSDT")
    parser.add_argument("--period", default="1m", help="驱动周期/币安 interval (default: 1m)")
    parser.add_argument("--strategy", default="fx_simple_test",
                        help="策略模块名 strategies.{name}.strategy (default: fx_simple_test)")
    parser.add_argument("--tag", default="", help="策略标签 (default: =period)")
    parser.add_argument("--poll", type=float, default=0.5, help="兼容参数 (default: 0.5)")
    parser.add_argument("--bars", type=int, default=300, help="K线预热数量 (default: 300)")
    parser.add_argument("--leverage", type=int, default=1, help="杠杆倍数 (default: 1)")
    parser.add_argument("--duration", type=float, default=3600.0,
                        help="运行时长秒，到点自动清仓退出 (default: 3600=1小时, 0=不限时)")
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG 日志")
    args = parser.parse_args()

    setup_logging(args.verbose)
    (ROOT / "logs").mkdir(exist_ok=True)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        parser.error("--symbols 解析为空，请至少指定一个交易对")

    # 币安 Futures Demo 配置（.env / 环境变量）
    rest_base = os.getenv("BINANCE_FUTURES_REST_BASE", "https://demo-fapi.binance.com")
    ws_base = os.getenv("BINANCE_FUTURES_WS_BASE", "wss://demo-fstream.binance.com/ws")
    api_key = os.getenv("BINANCE_FUTURES_API_KEY", "")
    api_secret = os.getenv("BINANCE_FUTURES_API_SECRET", "")
    proxy = os.getenv("HTTP_PROXY", "http://127.0.0.1:7897") or None

    if not api_key or not api_secret:
        parser.error(
            "缺少币安 Futures Demo 凭证：请在 klinequant/.env 配置 "
            "BINANCE_FUTURES_API_KEY / BINANCE_FUTURES_API_SECRET"
        )

    strategy_fn = _load_strategy(args.strategy)

    dur_desc = (
        f"{args.duration:.0f}s (auto-flatten at timeout)"
        if args.duration > 0 else "unlimited"
    )
    print("=" * 60)
    print("  KlineQuant Crypto Live Runner")
    print(f"  Symbols: {','.join(symbols)}  Period: {args.period}")
    print(f"  Strategy: {args.strategy} (KqApi isomorphic, same as FX)")
    print(f"  Mode: Binance Futures Demo (real MARKET orders, One-way)")
    print(f"  REST: {rest_base}")
    print(f"  Leverage: x{args.leverage}   Duration: {dur_desc}")
    print("=" * 60)
    print()
    print("  Ctrl+C to stop gracefully (auto-flatten on exit)")
    print()

    backend = BinanceBackend(
        symbols,
        rest_base=rest_base,
        ws_base=ws_base,
        api_key=api_key,
        api_secret=api_secret,
        proxy=proxy,
        leverage=args.leverage,
        poll_interval=args.poll,
        bar_count=args.bars,
    )
    runner = LiveRunner(
        backend,
        symbols=symbols,
        period=args.period,
        strategy_fn=strategy_fn,
        tag=args.tag,
        poll_interval=args.poll,
        bar_count=args.bars,
        duration=args.duration if args.duration > 0 else None,
    )
    runner.run()


if __name__ == "__main__":
    main()

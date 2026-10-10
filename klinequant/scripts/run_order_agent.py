"""Phase 3 follower 侧 OrderAgent 启动脚本（市场无关：fx / crypto 同构）。

从 ``config/accounts.yaml`` 取 ``role==follower`` 账户，按其 ``market`` 构造
Mt5Backend / BinanceBackend，再按账户 ``extra``（scale / scale_mode / symbol_map /
lead_intent_endpoint / report_endpoint / token / lead_timeout）构造 OrderAgent 并运行。

OrderAgent **不含任何策略代码**：只订阅 lead 广播的白名单指令（下单/撤单/清仓/心跳），
经幂等去重 → 缩放 → 品种映射 → 本地执行 → 回报回流。信任边界见
``strategy/sdk/dist_protocol.py``（非白名单 / token 不符 → 丢弃 + 告警，绝不执行）。

前提：
  1. lead 侧已运行（run_crypto_live.py / run_fx_live.py，账户 role=lead）。
  2. .env 配置 KQ_LEAD_TOKEN（与 lead 一致）+ follower 自己的 venue 凭证。
  3. 加密 follower 需 httpx/websockets + 代理；外汇 follower 需 MetaTrader5 + 已登录终端。

运行：
    cd klinequant
    python scripts/run_order_agent.py --account crypto-follower-1 --market crypto
    python scripts/run_order_agent.py --account fx-follower-1 --market fx --symbols EURUSD
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 确保项目根目录在 sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from gateway.env import load_env  # noqa: E402

load_env()  # 凭证从 klinequant/.env 加载（已有环境变量不覆盖）

from config.accounts import AccountConfigError, resolve_account  # noqa: E402
from strategy.sdk.dist_protocol import (  # noqa: E402
    DEFAULT_INTENT_ENDPOINT,
    DEFAULT_REPORT_ENDPOINT,
)
from strategy.sdk.order_agent import OrderAgent  # noqa: E402


def setup_logging(verbose: bool = False) -> None:
    """配置日志"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(ROOT / "logs" / "order_agent.log", encoding="utf-8"),
        ],
    )
    for noisy in ("urllib3", "httpx", "httpcore", "websockets", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def _build_backend(account, symbols: list[str], args):
    """按账户 market 构造 Mt5Backend / BinanceBackend。"""
    if account.market == "crypto":
        from strategy.sdk.backend import BinanceBackend

        b = account.binance
        if b is None or not b.api_key or not b.api_secret:
            raise SystemExit(
                "ERROR: crypto follower 账户缺少 binance 凭证（api_key/api_secret）"
            )
        rest_base = b.rest_base or os.getenv(
            "BINANCE_FUTURES_REST_BASE", "https://demo-fapi.binance.com")
        ws_base = b.ws_base or os.getenv(
            "BINANCE_FUTURES_WS_BASE", "wss://demo-fstream.binance.com/ws")
        return BinanceBackend(
            symbols,
            rest_base=rest_base,
            ws_base=ws_base,
            api_key=b.api_key,
            api_secret=b.api_secret,
            proxy=b.proxy or None,
            leverage=args.leverage,
            poll_interval=args.poll,
            bar_count=args.bars,
            account=account,
        )
    # fx
    from strategy.sdk.backend import Mt5Backend

    return Mt5Backend(magic=args.magic, deviation=args.deviation, account=account)


def main():
    parser = argparse.ArgumentParser(
        description="KlineQuant OrderAgent (Phase 3 follower, market-agnostic)"
    )
    parser.add_argument("--account", required=True,
                        help="follower 账户名（config/accounts.yaml，role 必须为 follower）")
    parser.add_argument("--symbols", "--symbol", dest="symbols", default="",
                        help="可交易品种，逗号分隔（缺省取 symbol_map 值域）")
    parser.add_argument("--period", default="1m", help="记账周期口径 (default: 1m)")
    parser.add_argument("--tag", default="", help="follower 记账 tag (default: =period)")
    parser.add_argument("--market", choices=["fx", "crypto"], default="",
                        help="市场（缺省按账户 market 推断）")
    parser.add_argument("--leverage", type=int, default=1, help="加密杠杆倍数 (default: 1)")
    parser.add_argument("--poll", type=float, default=0.5, help="轮询间隔秒 (default: 0.5)")
    parser.add_argument("--bars", type=int, default=300, help="K线预热数量 (default: 300)")
    parser.add_argument("--magic", type=int, default=202609, help="MT5 magic number")
    parser.add_argument("--deviation", type=int, default=20, help="MT5 滑点容忍 points")
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG 日志")
    args = parser.parse_args()

    setup_logging(args.verbose)
    (ROOT / "logs").mkdir(exist_ok=True)

    try:
        account = resolve_account(args.account, market=args.market or None)
    except AccountConfigError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    if account is None:
        print(f"ERROR: 未找到账户 {args.account!r}")
        sys.exit(1)
    if account.role != "follower":
        print(f"ERROR: 账户 {account.name} 的 role={account.role!r}，OrderAgent 需要 role=follower")
        sys.exit(1)
    if args.market and account.market != args.market:
        print(f"ERROR: --market={args.market} 与账户 market={account.market} 不匹配")
        sys.exit(1)

    extra = account.extra or {}
    token = extra.get("token") or os.getenv("KQ_LEAD_TOKEN", "")
    if not token:
        print("ERROR: 缺少 token（account.extra.token 或 env KQ_LEAD_TOKEN）")
        sys.exit(1)

    symbol_map = extra.get("symbol_map") or {}
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols and symbol_map:
        symbols = list(dict.fromkeys(str(v).upper() for v in symbol_map.values()))

    try:
        scale = Decimal(str(extra.get("scale", 1)))
    except (InvalidOperation, ValueError):
        print(f"ERROR: scale 非法：{extra.get('scale')!r}")
        sys.exit(1)

    print("=" * 60)
    print("  KlineQuant OrderAgent (Phase 3 follower)")
    print(f"  Account: {account.name} (market={account.market}, role=follower)")
    print(f"  Symbols: {','.join(symbols) or '<from symbol_map>'}  Period: {args.period}")
    print(f"  Scale: {scale} ({extra.get('scale_mode', 'fixed')})")
    print(f"  Symbol map: {symbol_map or '<identity>'}")
    print(f"  Lead intent: {extra.get('lead_intent_endpoint', DEFAULT_INTENT_ENDPOINT)}")
    print(f"  Report endpoint: {extra.get('report_endpoint', DEFAULT_REPORT_ENDPOINT)}")
    print("=" * 60)
    print()
    print("  Ctrl+C to stop gracefully")
    print()

    backend = _build_backend(account, symbols, args)
    agent = OrderAgent(
        token=token,
        follower_account=account.name,
        backend=backend,
        scale=scale,
        scale_mode=str(extra.get("scale_mode", "fixed")),
        symbol_map={str(k): str(v) for k, v in symbol_map.items()},
        lead_intent_endpoint=extra.get("lead_intent_endpoint", DEFAULT_INTENT_ENDPOINT),
        report_endpoint=extra.get("report_endpoint", DEFAULT_REPORT_ENDPOINT),
        lead_timeout=float(extra.get("lead_timeout", 10.0)),
        symbols=symbols or None,
        period=args.period,
        tag=args.tag,
        dedup_path=ROOT / "data" / "agent_dedup" / f"{account.name}.db",
    )
    agent.run()


if __name__ == "__main__":
    main()

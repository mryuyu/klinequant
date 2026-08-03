"""KlineQuant 双均线策略模拟盘运行器

全链路：Binance WS 实时行情 → MA7/MA25 指标 → 金叉/死叉信号 → 风控 → 模拟下单
使用真实行情数据，Simulator 模拟执行（Paper Mode）。

用法：
    python scripts/run_paper.py [--symbol BTCUSDT] [--interval 1m] [--capital 10000]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
from collections import deque
from decimal import Decimal
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 添加项目根目录到 path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.market_engine.adapters.binance import BinanceAdapter
from core.risk_engine.engine import RiskEngine
from core.risk_engine.rules import create_default_rules
from core.trade_engine.engine import TradeEngine, TradeMode
from core.trade_engine.executors.simulator import Simulator
from protocol.types import Kline, Signal, SignalDirection, SignalStrength

# ─── 配置 ───

PROXY = "http://127.0.0.1:7897"
REST_BASE = "https://demo-api.binance.com"
WS_BASE = "wss://demo-stream.binance.com/ws"
DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_INTERVAL = "1m"
DEFAULT_CAPITAL = "10000"
FAST_PERIOD = 7
SLOW_PERIOD = 25

# ─── 日志 ───

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(name)-18s │ %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("PaperTrader")


# ─── 指标计算 ───


def calc_ma(closes: deque, period: int) -> float | None:
    """计算简单移动平均"""
    if len(closes) < period:
        return None
    values = list(closes)[-period:]
    return sum(values) / period


# ─── 策略核心 ───


class DualMAPaperTrader:
    """双均线策略模拟盘交易器"""

    def __init__(self, symbol: str, interval: str, capital: Decimal):
        self.symbol = symbol
        self.interval = interval
        self.closes: deque = deque(maxlen=200)
        self.prev_fast: float | None = None
        self.prev_slow: float | None = None
        self.bar_count = 0
        self.signal_count = 0

        # 交易引擎（Paper 模式）
        self.sim = Simulator(initial_balance=capital)
        risk_engine = RiskEngine(rules=create_default_rules())
        self.trade_engine = TradeEngine(
            executor=self.sim,
            risk_engine=risk_engine,
            mode=TradeMode.PAPER,
        )

        # 行情适配器（Testnet/DEMO）
        self.adapter = BinanceAdapter(config={
            "proxy": PROXY,
            "rest_base": REST_BASE,
            "ws_base": WS_BASE,
        })

        self._running = False

    async def start(self):
        """启动"""
        self._running = True

        # 1. 连接行情
        await self.adapter.connect()
        server_time = await self.adapter.fetch_server_time()
        logger.info(f"已连接 Binance，服务器时间: {server_time}")

        # 2. 启动交易引擎
        await self.trade_engine.start()
        self.sim.update_price(self.symbol, await self._get_price())
        logger.info(f"交易引擎启动 (Paper Mode, 初始资金: {self.sim._balance})")

        # 3. 预热：拉取历史 K 线
        logger.info(f"预热中：拉取 {SLOW_PERIOD + 10} 根历史 K 线...")
        history = await self.adapter.fetch_klines(
            self.symbol, self.interval, limit=SLOW_PERIOD + 10
        )
        for k in history:
            self.closes.append(float(k.close))
        logger.info(f"预热完成，已有 {len(self.closes)} 根 K 线")

        # 4. 订阅实时 K 线
        await self.adapter.subscribe_kline(
            self.symbol, self.interval, self._on_kline
        )
        await self.adapter.start_ws()
        logger.info("=== Paper Trader Started ===")
        logger.info(f"  Symbol: {self.symbol} | Interval: {self.interval}")
        logger.info(f"  Params: MA{FAST_PERIOD} / MA{SLOW_PERIOD}")
        logger.info(f"  Proxy: {PROXY}")
        logger.info(f"=============================")

    async def stop(self):
        """停止"""
        self._running = False
        await self.adapter.disconnect()
        await self.trade_engine.stop()
        self._print_summary()

    async def _get_price(self) -> Decimal:
        """获取当前价格"""
        klines = await self.adapter.fetch_klines(self.symbol, self.interval, limit=1)
        if klines:
            return klines[-1].close
        return Decimal("0")

    async def _on_kline(self, kline: Kline):
        """K 线回调：价格实时更新（每个 tick），指标/信号收盘驱动"""
        if not self._running:
            return

        # 实时价格层：未收盘/收盘事件都刷新最新价（持仓盈亏实时，未来盘中止损挂载点）
        self.sim.update_price(self.symbol, kline.close)

        # 指标/信号层：只在收盘后计算（避免未收盘 bar 反复变化导致信号重算）
        if not kline.is_closed:
            return

        self.bar_count += 1
        close = float(kline.close)
        self.closes.append(close)

        # 计算均线
        fast_ma = calc_ma(self.closes, FAST_PERIOD)
        slow_ma = calc_ma(self.closes, SLOW_PERIOD)

        if fast_ma is None or slow_ma is None:
            return

        # 检测交叉
        signal_direction = self._detect_crossover(fast_ma, slow_ma)
        self.prev_fast = fast_ma
        self.prev_slow = slow_ma

        # 状态输出（每根收盘 bar 打印一条，便于观察实际更新频率）
        pos = self._get_position_info()
        logger.info(
            f"Bar#{self.bar_count} | {self.symbol} close={close:.2f} | "
            f"MA{FAST_PERIOD}={fast_ma:.2f} MA{SLOW_PERIOD}={slow_ma:.2f} | "
            f"{pos}"
        )

        # 产生信号 → 下单
        if signal_direction:
            self.signal_count += 1
            await self._execute_signal(signal_direction, kline.close)

    def _detect_crossover(self, fast: float, slow: float) -> SignalDirection | None:
        """检测金叉/死叉"""
        if self.prev_fast is None or self.prev_slow is None:
            return None

        # 金叉：快线从下方穿越慢线
        if self.prev_fast <= self.prev_slow and fast > slow:
            return SignalDirection.LONG
        # 死叉：快线从上方穿越慢线
        if self.prev_fast >= self.prev_slow and fast < slow:
            return SignalDirection.SHORT
        return None

    async def _execute_signal(self, direction: SignalDirection, price: Decimal):
        """执行交易信号"""
        sig = Signal(
            signal_id=f"PAPER-{int(time.time()*1000)}",
            strategy_id="dual_ma_paper",
            symbol=self.symbol,
            direction=direction,
            strength=SignalStrength.STRONG,
            price=price,
            reason=f"MA{FAST_PERIOD}/MA{SLOW_PERIOD} {'Golden' if direction == SignalDirection.LONG else 'Death'} Cross",
            timestamp=int(time.time() * 1000),
            expires_at=int(time.time() * 1000) + 60000,
        )

        action = "BUY" if direction == SignalDirection.LONG else "SELL"
        logger.info(f"--- Signal #{self.signal_count}: {action} {self.symbol} @ {price:.2f} ---")

        result = await self.trade_engine.process_signal(sig)

        if result:
            logger.info(
                f"  [FILLED] {result.side.value} {result.filled_quantity} @ "
                f"{result.avg_fill_price:.2f} | fee: {result.fee}"
            )
        else:
            logger.info(f"  [REJECTED] Order blocked by risk engine")

    def _get_position_info(self) -> str:
        """获取持仓信息"""
        positions = self.trade_engine.position_manager.positions
        if not positions:
            return "空仓"
        parts = []
        for sym, pos in positions.items():
            pnl = (self.sim._last_prices.get(sym, pos.avg_entry_price) - pos.avg_entry_price) * pos.quantity
            if pos.side == "SHORT":
                pnl = -pnl
            parts.append(f"{pos.side} {pos.quantity} @ {pos.avg_entry_price:.2f} (PnL:{pnl:+.2f})")
        return " | ".join(parts)

    def _print_summary(self):
        """打印运行总结"""
        account = self.sim._balance
        logger.info("=== Summary ===")
        logger.info(f"  Bars processed: {self.bar_count}")
        logger.info(f"  Signals generated: {self.signal_count}")
        logger.info(f"  Final balance: {account}")
        logger.info(f"===============")


# ─── 入口 ───


async def main():
    parser = argparse.ArgumentParser(description="KlineQuant 双均线模拟盘")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="交易对")
    parser.add_argument("--interval", default=DEFAULT_INTERVAL, help="K线周期")
    parser.add_argument("--capital", default=DEFAULT_CAPITAL, help="初始资金")
    args = parser.parse_args()

    trader = DualMAPaperTrader(
        symbol=args.symbol,
        interval=args.interval,
        capital=Decimal(args.capital),
    )

    # 优雅退出
    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("收到退出信号，正在停止...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows 不支持 add_signal_handler
            pass

    await trader.start()

    # 等待退出
    try:
        await stop_event.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await trader.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n已退出")

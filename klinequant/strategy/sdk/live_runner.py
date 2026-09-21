"""LiveRunner — 实盘策略运行器

职责：
  1. 初始化 Mt5Api（共享实例）
  2. 加载 SymbolInfo（spec_loader）
  3. 初始化 Mt5Executor、ExposureLedger、UnifiedResolver
  4. 初始化 Mt5DataFeed（轮询 ticks + bars）
  5. 构造 KqApi
  6. 运行策略函数（策略内 while api.wait_update() 循环）
  7. 优雅退出（Ctrl+C / 异常）

使用方式：
    # 单品种
    runner = LiveRunner(symbols="EURUSD", period="1m", strategy_fn=my_strategy)
    # 多品种：单进程 + 单 MT5 连接 + 每品种一个工作线程
    #（品种间并行、品种内串行，规避单循环队头阻塞）
    runner = LiveRunner(symbols=["EURUSD", "GBPUSD", "USDJPY"], period="1m",
                        strategy_fn=my_strategy)
    runner.run()
"""
from __future__ import annotations

import logging
import signal
import threading
import time
from decimal import Decimal
from typing import Callable, Dict, List, Optional, Union

from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import UnifiedResolver
from core.trade_engine.spec_loader import load_spec_from_mt5
from core.trade_engine.executors.mt5_executor import Mt5Executor
from gateway.market_sources.mt5_driver import Mt5Api
from protocol.types import SymbolInfo
from strategy.sdk.api import KqApi
from strategy.sdk.data_feed import Mt5DataFeed

logger = logging.getLogger(__name__)


class LiveRunner:
    """实盘策略运行器"""

    def __init__(
        self,
        symbols: Union[str, List[str]],
        period: str,
        strategy_fn: Callable[[KqApi], None],
        *,
        tag: str = "",
        poll_interval: float = 0.5,
        bar_count: int = 300,
        magic: int = 202609,
        deviation: int = 20,
        duration: Optional[float] = None,
        mt5_kwargs: Optional[dict] = None,
    ):
        """
        Args:
            symbols: 交易品种，单个 str 或列表（如 "EURUSD" 或 ["EURUSD","GBPUSD"]）；
                     多品种共享一个进程/一个 MT5 连接/一个数据源，每品种一个工作线程，
                     列表首个为主品种（KqApi symbol=None 的回落）
            period: 驱动周期（如 "1m"）
            strategy_fn: 策略函数，签名 (api: KqApi) -> None；每品种各跑一份
            tag: 策略标识（敞口账本隔离用，默认=period）
            poll_interval: 数据轮询间隔（秒）
            bar_count: 加载的 K 线数量
            magic: MT5 EA magic number
            deviation: 滑点容忍（points）
            duration: 运行时长（秒），到点自动清仓退出；None/0=不限时
            mt5_kwargs: MT5 初始化参数（覆盖环境变量）
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        self._symbols: List[str] = [s.upper() for s in symbols]
        if not self._symbols:
            raise ValueError("LiveRunner requires at least one symbol")
        self._symbol = self._symbols[0]  # 主品种
        self._period = period
        self._strategy_fn = strategy_fn
        self._tag = tag or period
        self._poll_interval = poll_interval
        self._bar_count = bar_count
        self._magic = magic
        self._deviation = deviation
        self._duration = duration
        self._mt5_kwargs = mt5_kwargs

        # 组件（run 时初始化）
        self._driver: Optional[Mt5Api] = None
        self._specs: Dict[str, SymbolInfo] = {}
        self._executor: Optional[Mt5Executor] = None
        self._ledger: Optional[ExposureLedger] = None
        self._resolver: Optional[UnifiedResolver] = None
        self._feed: Optional[Mt5DataFeed] = None
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
        """初始化所有组件"""
        # 1. MT5 驱动
        self._driver = Mt5Api()
        kwargs = self._mt5_kwargs or Mt5Api.init_kwargs()
        if not self._driver.initialize(**kwargs):
            raise RuntimeError(
                "MT5 initialize failed. 请确认：\n"
                "  1. MT5 终端已启动并登录 Demo 账户\n"
                "  2. MetaTrader5 Python 包已安装 (pip install MetaTrader5)\n"
                "  3. 环境变量 MT5_TERMINAL_PATH / MT5_LOGIN 正确（如需）"
            )
        logger.info("MT5 driver initialized")

        # 2. 品种规格（逐品种加载，各自 pip/step/min/digits 不同，绝不混用）
        for sym in self._symbols:
            spec = load_spec_from_mt5(self._driver, sym)
            if spec is None:
                raise RuntimeError(f"Failed to load SymbolInfo for {sym}")
            self._specs[sym] = spec
            logger.info(
                f"SymbolInfo loaded: {sym} "
                f"step={spec.qty_step} min={spec.min_qty} max={spec.qty_max} "
                f"pip={spec.pip_size} mult={spec.contract_multiplier}"
            )

        # 3. 执行器
        self._executor = Mt5Executor(
            self._driver, magic=self._magic, default_deviation=self._deviation
        )

        # 4. 敞口账本
        self._ledger = ExposureLedger()

        # 5. 对账：逐品种从 MT5 恢复当前持仓
        self._reconcile_positions()

        # 6. Resolver
        self._resolver = UnifiedResolver()

        # 7. 数据源（一个 feed 订阅全部品种，单 MT5 连接轮询）
        self._feed = Mt5DataFeed(
            driver=self._driver,
            symbols=self._symbols,
            periods=[self._period],
            poll_interval=self._poll_interval,
            bar_count=self._bar_count,
        )
        self._feed.start()

        # 8. 每品种一个 KqApi（绑定各自 symbol，共享 feed/ledger/executor/resolver/specs）
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
            )

        logger.info(
            f"All components initialized, ready to run strategy "
            f"({len(self._symbols)} symbol worker thread(s))"
        )

    def _reconcile_positions(self) -> None:
        """启动时逐品种从 MT5 对账恢复持仓（净敞口 = 该品种所有持仓带符号求和）"""
        any_pos = False
        for sym in self._symbols:
            positions = self._executor.query_positions(sym)
            if not positions:
                continue
            any_pos = True
            total_volume = Decimal("0")
            total_price = Decimal("0")
            for p in positions:
                vol = Decimal(str(p.get("volume", 0)))
                ptype = int(p.get("type", 0))  # 0=BUY, 1=SELL
                price = Decimal(str(p.get("price_open", 0)))
                total_volume += vol if ptype == 0 else -vol
                total_price = price

            if total_volume != 0:
                self._ledger.sync_from_venue(
                    sym, self._tag, total_volume, total_price
                )
                logger.info(
                    f"Reconciled {sym} from MT5: volume={total_volume} "
                    f"avg_price={total_price}"
                )
        if not any_pos:
            logger.info("No existing MT5 positions, ledger starts clean")

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

        # ② 清仓：逐品种撤所有挂单 + 平掉净持仓（趁 driver 还活着）
        #    无论到时/Ctrl+C/异常退出都执行，实盘绝不留孤儿仓
        if self._apis and self._driver:
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

        # ③ 打印最终持仓 + 关闭 driver
        if self._driver:
            if self._ledger:
                for sym in self._symbols:
                    pos = self._ledger.position(sym, self._tag)
                    logger.info(
                        f"Final position {sym}: volume={pos.volume} "
                        f"in_flight={pos.in_flight} effective={pos.effective} "
                        f"realized_pnl={pos.realized_pnl}"
                    )
            self._driver.shutdown()

        logger.info("=== LiveRunner stopped ===")

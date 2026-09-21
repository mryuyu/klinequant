"""MarketBackend — 市场后端抽象（LiveRunner 注入点）

把「连接哪个市场、如何加载品种规格、用什么执行器/数据源、如何对账/关闭」
收敛到 backend，使 LiveRunner 与市场无关。新增市场（币安/国内期货）只需实现
一个 MarketBackend，策略 strategy(api: KqApi) 与 LiveRunner 零改动复用（同构）。

已实现：
  - Mt5Backend：外汇（MT5 Demo），封装原 LiveRunner 的 MT5 初始化逻辑，行为等价
  - BinanceBackend：加密（币安 Futures Demo），见本文件下方
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import threading
import time
from decimal import Decimal
from typing import Dict, List, Optional, Protocol, runtime_checkable
from urllib.parse import urlencode

from core.trade_engine.executors.mt5_executor import Mt5Executor
from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.spec_loader import load_spec_from_mt5
from gateway.market_sources.mt5_driver import Mt5Api
from protocol.types import SymbolInfo
from strategy.sdk.api import DataFeedProtocol, ExecutorProtocol
from strategy.sdk.data_feed import Mt5DataFeed

logger = logging.getLogger(__name__)


def _reconcile_net_positions(
    executor: ExecutorProtocol, ledger: ExposureLedger,
    symbols: List[str], tag: str, venue: str = "venue",
) -> None:
    """逐品种从 venue 对账恢复净持仓到 ledger（MT5 同形 dict）。

    净敞口 = 该品种所有持仓带符号求和（type 0=BUY/多，1=SELL/空）。
    Mt5Backend / BinanceBackend 共用（两者 query_positions 均返回 MT5 同形 dict）。
    """
    any_pos = False
    for sym in symbols:
        positions = executor.query_positions(sym)
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
            ledger.sync_from_venue(sym, tag, total_volume, total_price)
            logger.info(
                f"Reconciled {sym} from {venue}: volume={total_volume} "
                f"avg_price={total_price}"
            )
    if not any_pos:
        logger.info(f"No existing {venue} positions, ledger starts clean")


@runtime_checkable
class MarketBackend(Protocol):
    """市场后端协议（LiveRunner 注入点）。

    LiveRunner 只依赖本协议，不感知具体市场。一个 backend 封装某市场的：
    连接、品种规格加载、执行器/数据源构造、启动对账、关闭清理。
    """

    def connect(self) -> None:
        """建立市场连接（驱动/会话）。失败抛 RuntimeError。"""
        ...

    def load_specs(self, symbols: List[str]) -> Dict[str, SymbolInfo]:
        """逐品种加载规格（pip/step/min/multiplier 等，绝不混用）。"""
        ...

    def make_executor(self) -> ExecutorProtocol:
        """构造交易执行器（实现 ExecutorProtocol，返回 SubmitResult）。"""
        ...

    def make_feed(
        self, symbols: List[str], periods: List[str],
        poll_interval: float, bar_count: int,
    ) -> DataFeedProtocol:
        """构造数据源（实现 DataFeedProtocol）。"""
        ...

    def reconcile_positions(
        self, executor: ExecutorProtocol, ledger: ExposureLedger,
        symbols: List[str], tag: str,
    ) -> None:
        """启动时从 venue 对账恢复持仓到 ledger。"""
        ...

    def shutdown(self) -> None:
        """关闭市场连接（驱动/会话/事件循环）。"""
        ...


class Mt5Backend:
    """外汇后端（MT5 Demo）。

    封装原 LiveRunner._initialize 的 MT5 全套逻辑（驱动/规格/执行器/数据源/
    对账/关闭），逐行等价搬迁，使 FX 实盘行为零退化。
    """

    def __init__(
        self, *, magic: int = 202609, deviation: int = 20,
        mt5_kwargs: Optional[dict] = None,
    ):
        self._magic = magic
        self._deviation = deviation
        self._mt5_kwargs = mt5_kwargs
        self._driver: Optional[Mt5Api] = None

    @property
    def driver(self) -> Optional[Mt5Api]:
        return self._driver

    def connect(self) -> None:
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

    def load_specs(self, symbols: List[str]) -> Dict[str, SymbolInfo]:
        specs: Dict[str, SymbolInfo] = {}
        for sym in symbols:
            spec = load_spec_from_mt5(self._driver, sym)
            if spec is None:
                raise RuntimeError(f"Failed to load SymbolInfo for {sym}")
            specs[sym] = spec
            logger.info(
                f"SymbolInfo loaded: {sym} "
                f"step={spec.qty_step} min={spec.min_qty} max={spec.qty_max} "
                f"pip={spec.pip_size} mult={spec.contract_multiplier}"
            )
        return specs

    def make_executor(self) -> ExecutorProtocol:
        return Mt5Executor(
            self._driver, magic=self._magic, default_deviation=self._deviation
        )

    def make_feed(
        self, symbols: List[str], periods: List[str],
        poll_interval: float, bar_count: int,
    ) -> DataFeedProtocol:
        return Mt5DataFeed(
            driver=self._driver, symbols=symbols, periods=periods,
            poll_interval=poll_interval, bar_count=bar_count,
        )

    def reconcile_positions(
        self, executor: ExecutorProtocol, ledger: ExposureLedger,
        symbols: List[str], tag: str,
    ) -> None:
        """逐品种从 MT5 对账恢复持仓（净敞口 = 该品种所有持仓带符号求和）"""
        _reconcile_net_positions(executor, ledger, symbols, tag, venue="MT5")

    def shutdown(self) -> None:
        if self._driver:
            self._driver.shutdown()


class BinanceBackend:
    """加密后端（币安 USDT-M Futures Demo，One-way 净持仓）。

    同构复用 LiveRunner / KqApi / strategy，仅把市场相关逻辑换成币安：
    行情走 BinanceFuturesAdapter（fstream WS + fapi REST 预热），下单走
    BinanceExecutor。币安 API 均为 async，而 ExecutorProtocol/DataFeedProtocol
    为同步，故 backend 持一个独立 daemon 线程跑 asyncio event loop，所有 async
    调用经 run_coroutine_threadsafe(coro, loop).result(timeout) 桥接为同步。
    """

    def __init__(
        self,
        symbols: List[str],
        *,
        rest_base: str = "https://demo-fapi.binance.com",
        ws_base: str = "wss://demo-fstream.binance.com/ws",
        api_key: str = "",
        api_secret: str = "",
        proxy: Optional[str] = None,
        leverage: int = 1,
        magic: int = 202609,
        poll_interval: float = 0.5,
        bar_count: int = 300,
        timeout: float = 15.0,
    ):
        if isinstance(symbols, str):
            symbols = [symbols]
        self._symbols = [s.upper() for s in symbols]
        self._rest_base = rest_base
        self._ws_base = ws_base
        self._api_key = api_key
        self._api_secret = api_secret
        self._proxy = proxy
        self._leverage = leverage
        self._magic = magic
        self._poll_interval = poll_interval
        self._bar_count = bar_count
        self._timeout = timeout

        # event loop 线程（connect 时懒创建，保持 __init__ 无副作用）
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._adapter = None            # BinanceFuturesAdapter
        self._client = None             # httpx.AsyncClient（下单/查询）

    # ─── event loop 线程 ───

    def _ensure_loop(self) -> None:
        if self._loop is None:
            self._loop = asyncio.new_event_loop()
            self._thread = threading.Thread(
                target=self._loop_runner, daemon=True, name="binance-backend-loop"
            )
            self._thread.start()

    def _loop_runner(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _run(self, coro, timeout: Optional[float] = None):
        """在 loop 线程上跑协程并同步等待结果。"""
        self._ensure_loop()
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=timeout or self._timeout)

    # ─── 签名（positionSide/dual 等私有调用）───

    def _sign_params(self, params: Dict[str, object]) -> Dict[str, object]:
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = 5000
        query = urlencode(params)
        params["signature"] = hmac.new(
            self._api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        return params

    def _headers(self) -> Dict[str, str]:
        return {"X-MBX-APIKEY": self._api_key}

    # ─── MarketBackend 实现 ───

    def connect(self) -> None:
        self._run(self._async_connect(), timeout=30.0)

    async def _async_connect(self) -> None:
        import httpx
        from core.market_engine.adapters.binance_futures import BinanceFuturesAdapter

        self._adapter = BinanceFuturesAdapter({
            "rest_base": self._rest_base,
            "ws_base": self._ws_base,
            "proxy": self._proxy,
            "api_key": self._api_key,
            "api_secret": self._api_secret,
        })
        await self._adapter.connect()

        transport = (
            httpx.AsyncHTTPTransport(proxy=self._proxy) if self._proxy else None
        )
        self._client = httpx.AsyncClient(
            base_url=self._rest_base, timeout=self._timeout, transport=transport
        )

        # One-way 模式（positionSide=BOTH）；-4059 = 无需变更也算成功
        await self._set_one_way()

        # 逐品种设杠杆
        for sym in self._symbols:
            try:
                await self._adapter.set_leverage(sym, self._leverage)
                logger.info(f"Leverage set: {sym} x{self._leverage}")
            except Exception as e:
                logger.warning(f"set_leverage {sym} failed (continue): {e}")

        logger.info(f"BinanceBackend connected: {self._rest_base}")

    async def _set_one_way(self) -> None:
        params = self._sign_params({"dualSidePosition": "false"})
        try:
            resp = await self._client.post(
                "/fapi/v1/positionSide/dual", params=params, headers=self._headers()
            )
            if resp.status_code == 200:
                logger.info("One-way position mode ensured (dualSidePosition=false)")
            else:
                logger.warning(
                    f"Set one-way mode: HTTP {resp.status_code} {resp.text[:120]}"
                )
        except Exception as e:
            logger.warning(f"Set one-way mode failed (continue): {e}")

    def load_specs(self, symbols: List[str]) -> Dict[str, SymbolInfo]:
        return self._run(self._load_specs_async(symbols), timeout=30.0)

    async def _load_specs_async(self, symbols: List[str]) -> Dict[str, SymbolInfo]:
        from core.trade_engine.spec_loader import load_spec_from_binance

        resp = await self._client.get("/fapi/v1/exchangeInfo")
        resp.raise_for_status()
        data = resp.json()
        by_symbol = {s.get("symbol"): s for s in data.get("symbols", [])}

        specs: Dict[str, SymbolInfo] = {}
        for sym in symbols:
            info = by_symbol.get(sym.upper())
            if info is None:
                raise RuntimeError(f"exchangeInfo has no symbol {sym}")
            spec = load_spec_from_binance(info, sym.upper())
            specs[sym] = spec
            logger.info(
                f"SymbolInfo loaded: {sym} step={spec.qty_step} min={spec.min_qty} "
                f"tick={spec.tick_size} min_notional={spec.min_notional}"
            )
        return specs

    def make_executor(self) -> ExecutorProtocol:
        from core.trade_engine.executors.binance_executor import BinanceExecutor

        return BinanceExecutor(
            loop=self._loop, client=self._client,
            api_key=self._api_key, api_secret=self._api_secret,
            magic=self._magic, timeout=self._timeout,
        )

    def make_feed(
        self, symbols: List[str], periods: List[str],
        poll_interval: float, bar_count: int,
    ) -> DataFeedProtocol:
        from strategy.sdk.binance_feed import BinanceDataFeed

        return BinanceDataFeed(
            loop=self._loop, adapter=self._adapter, symbols=symbols,
            periods=periods, bar_count=bar_count, poll_interval=poll_interval,
        )

    def reconcile_positions(
        self, executor: ExecutorProtocol, ledger: ExposureLedger,
        symbols: List[str], tag: str,
    ) -> None:
        """逐品种从币安对账恢复净持仓（One-way，同 MT5）"""
        _reconcile_net_positions(executor, ledger, symbols, tag, venue="Binance")

    def shutdown(self) -> None:
        if self._loop and self._loop.is_running():
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._async_shutdown(), self._loop
                )
                future.result(timeout=10.0)
            except Exception as e:
                logger.warning(f"BinanceBackend async shutdown error: {e}")
            self._loop.call_soon_threadsafe(self._loop.stop)
            if self._thread:
                self._thread.join(timeout=5)
        if self._loop:
            try:
                self._loop.close()
            except Exception:
                pass
            self._loop = None
        logger.info("BinanceBackend shut down")

    async def _async_shutdown(self) -> None:
        if self._adapter:
            try:
                await self._adapter.disconnect()
            except Exception as e:
                logger.warning(f"adapter disconnect error: {e}")
        if self._client:
            try:
                await self._client.aclose()
            except Exception as e:
                logger.warning(f"client aclose error: {e}")

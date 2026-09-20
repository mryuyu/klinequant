"""本地 MetaTrader 5 市场源插件

接入方式：官方 MetaTrader5 Python 包连接本机 MT5 终端（需终端已登录运行，
或配置 MT5_TERMINAL_PATH/MT5_LOGIN 等由其拉起）。历史深度远超 IG demo
（本地终端全量历史，支持多年 M1），实时链路为终端轮询（包无推送接口）。

精度铁律落地：MT5 symbol_info().digits 是订阅到的市场元数据（tick 小数位），
直接作为 price_precision 下发，不做任何推导。

驱动层已提取到 mt5_driver.py（行情与交易共享 Mt5Api 子进程连接）。
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from gateway.market_sources.base import MarketSource
from gateway.market_sources.manager import market_manager
from gateway.market_sources.mt5_driver import (
    Mt5Api,
    TIMEFRAME_MAP,
    TF_SECONDS,
    _HAS_MT5,
    _tf_const,
)

logger = logging.getLogger(__name__)

POLL_INTERVAL = float(os.getenv("MT5_POLL_INTERVAL", "0.5"))
TICKER_CACHE_TTL = 5.0
RECONNECT_COOLDOWN = 30.0

# 品种目录：完全可交易模式常量 + path 顶层目录 → 资产类别
_TRADE_MODE_FULL = _tf_const("SYMBOL_TRADE_MODE_FULL", 4)
_ASSET_TYPE_BY_PATH = {
    "Forex": "forex",
    "Indices": "index",
    "Crypto": "crypto",
    "Bonds CFDs": "bond",
    "Stock CFD's": "stock",
}


class Mt5Source(MarketSource):
    """本地 MT5 终端：历史 K 线（copy_rates）+ 终端轮询实时流"""

    name = "mt5"
    label = "IC Markets"
    supported_timeframes = set(TIMEFRAME_MAP.keys())
    supports_volume = True   # tick_volume 为真实订阅数据（外汇无 real_volume 时用 tick）
    default_symbols = [
        {"symbol": "EURUSD", "name": "EUR/USD", "type": "forex"},
        {"symbol": "GBPUSD", "name": "GBP/USD", "type": "forex"},
        {"symbol": "USDJPY", "name": "USD/JPY", "type": "forex"},
        {"symbol": "AUDUSD", "name": "AUD/USD", "type": "forex"},
        {"symbol": "USDCHF", "name": "USD/CHF", "type": "forex"},
        {"symbol": "XAUUSD", "name": "XAU/USD", "type": "metal"},
    ]
    watched_targets: list[tuple[str, str]] = []

    def __init__(self, driver: Mt5Api | None = None):
        self._driver = driver or Mt5Api()
        self.available = self._driver.initialize(**self._init_kwargs())
        #: symbol -> digits（订阅到的 tick 小数位，精度唯一权威来源）
        self._digits: dict[str, int] = {}
        #: 已加入市场报价的品种（symbol_select 幂等缓存）
        self._selected: set[str] = set()
        #: symbol -> (ts, ticker)
        self._ticker_cache: dict[str, tuple[float, dict | None]] = {}
        self._last_reconnect_at = 0.0
        if self.available:
            self._probe_symbols()

    @staticmethod
    def _init_kwargs() -> dict:
        """终端连接参数（全部可选：缺省时连接已运行的本机终端）"""
        kw: dict = {}
        path = os.getenv("MT5_TERMINAL_PATH", "")
        if path:
            kw["path"] = path
        login = os.getenv("MT5_LOGIN", "")
        if login:
            kw["login"] = int(login)
            kw["password"] = os.getenv("MT5_PASSWORD", "")
            server = os.getenv("MT5_SERVER", "")
            if server:
                kw["server"] = server
        return kw

    def _probe_symbols(self) -> None:
        """过滤默认品种为终端实际存在的（经纪商命名可能有后缀），顺带缓存 digits"""
        kept = []
        for item in self.default_symbols:
            info = self._driver.symbol_info(item["symbol"])
            if info is not None:
                self._digits[item["symbol"].upper()] = int(info["digits"])
                kept.append(item)
        if kept:
            self.default_symbols = kept
        else:
            logger.warning("MT5 none of default symbols found in terminal, keeping list as-is")

    def _ensure_selected(self, symbol: str) -> None:
        """品种须在终端市场报价中才可取数（惰性 symbol_select）"""
        if symbol not in self._selected:
            self._driver.symbol_select(symbol, True)
            self._selected.add(symbol)

    def _ensure_digits(self, symbol: str) -> None:
        if symbol not in self._digits:
            info = self._driver.symbol_info(symbol)
            if info is not None:
                self._digits[symbol] = int(info["digits"])

    # ─── 精度：digits 优先（订阅到的市场元数据），价格推导兜底 ───

    def price_precision(self, symbol: str) -> int:
        d = self._digits.get(symbol.upper())
        if d:
            return d
        return super().price_precision(symbol)

    # ─── 全量品种目录（终端 symbols_get，按 path 资产分类） ───

    async def list_symbols(self) -> list[dict]:
        """终端全量可交易品种：trade_mode=FULL 过滤，path 顶层目录归资产类别"""
        rows = await asyncio.to_thread(self._driver.symbols_get)
        if not rows:
            return await super().list_symbols()
        out = []
        for s in rows:
            if int(s["trade_mode"]) != _TRADE_MODE_FULL:
                continue
            parts = (s["path"] or "").split("\\")
            top = parts[0]
            if top == "Commodities":
                atype = "metal" if len(parts) > 1 and parts[1] == "Metals" else "commodity"
            else:
                atype = _ASSET_TYPE_BY_PATH.get(top, "")
            out.append({"symbol": s["name"], "name": s["description"] or s["name"], "type": atype})
        return out

    # ─── REST 历史 K 线 / 行情摘要 ───

    def _to_bar(self, row: dict) -> dict:
        # real_volume 为 0 时（外汇/CFD 常态）用 tick_volume
        vol = row.get("real_volume") or row.get("tick_volume") or 0
        return {
            "timestamp": int(row["time"]) * 1000,
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": float(vol),
            "event_ms": int(time.time() * 1000),
        }

    def _prepare_sync(self, sym: str) -> None:
        """惰性品种选中 + digits 缓存：同步 MT5 调用，只能在 to_thread 内执行，
        不得占用事件循环（终端挂死时会堵死全部 HTTP 请求）"""
        self._ensure_selected(sym)
        self._ensure_digits(sym)

    def _klines_sync(self, sym, tf_const, timeframe, limit, dt_from, dt_to):
        self._prepare_sync(sym)
        if dt_to is not None:
            return self._driver.copy_rates_range(sym, tf_const, dt_from, dt_to)
        return self._driver.copy_rates_from_pos(sym, tf_const, 0, limit)

    def _tick_sync(self, sym):
        self._prepare_sync(sym)
        return self._driver.symbol_info_tick(sym)

    async def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 200,
        end_time: int | None = None,
    ) -> list[dict]:
        tf_const = TIMEFRAME_MAP.get(timeframe)
        if not tf_const:
            raise ValueError(f"MT5 unsupported timeframe: {timeframe}")
        sym = symbol.upper()
        if end_time:
            # 翻页加深：按 end_time（含）向前回溯 limit 根（窗口留 2 倍冗余防缺口）
            dt_to = datetime.fromtimestamp(end_time / 1000, tz=timezone.utc)
            dt_from = dt_to - timedelta(seconds=TF_SECONDS[timeframe] * limit * 2)
            rows = await asyncio.to_thread(
                self._klines_sync, sym, tf_const, timeframe, limit, dt_from, dt_to
            )
        else:
            rows = await asyncio.to_thread(
                self._klines_sync, sym, tf_const, timeframe, limit, None, None
            )
        if rows is None:
            raise RuntimeError(f"MT5 copy_rates failed for {sym}/{timeframe}")
        bars = [self._to_bar(r) for r in rows]
        # 价格精度兜底累积（digits 缺失时才生效；digits 存在时 price_precision 优先返回）
        for b in bars[-20:]:
            self._track_prec(sym, [b["open"], b["high"], b["low"], b["close"]])
        if end_time:
            bars = [b for b in bars if b["timestamp"] <= end_time]
        return bars[-limit:]

    async def fetch_ticker(self, symbol: str) -> dict | None:
        cached = self._ticker_cache.get(symbol)
        if cached and time.monotonic() - cached[0] < TICKER_CACHE_TTL:
            return cached[1]
        sym = symbol.upper()
        tick = await asyncio.to_thread(self._tick_sync, sym)
        bid = tick.get("bid") if tick is not None else None
        ask = tick.get("ask") if tick is not None else None
        if not bid and not ask:
            # 无报价（休市/终端断连）：用最新 K 线构造 ticker
            result = await self._ticker_from_candles(sym)
            self._ticker_cache[symbol] = (time.monotonic(), result)
            return result
        bid_n, ask_n = float(bid or 0), float(ask or 0)
        self._track_prec(sym, [v for v in (bid_n, ask_n) if v])
        # 中点用 Decimal 均值避免浮点噪声污染展示价
        last = ask_n if not bid_n else (
            bid_n if not ask_n else float((Decimal(str(bid_n)) + Decimal(str(ask_n))) / 2)
        )
        high, low, pct = last, last, 0.0
        try:
            # 24h 高低：近 25 根 H1；涨跌幅：对上一根日 K 收盘价
            h1 = await asyncio.to_thread(
                self._driver.copy_rates_from_pos, sym, TIMEFRAME_MAP["1h"], 0, 25
            )
            if h1:
                highs = [r["high"] for r in h1 if r["high"] > 0]
                lows = [r["low"] for r in h1 if r["low"] > 0]
                if highs:
                    high = max(highs)
                if lows:
                    low = min(lows)
            d1 = await asyncio.to_thread(
                self._driver.copy_rates_from_pos, sym, TIMEFRAME_MAP["1d"], 0, 2
            )
            if d1 and len(d1) >= 2 and d1[-2]["close"] > 0:
                pct = (last / d1[-2]["close"] - 1) * 100
        except Exception as e:
            logger.debug(f"MT5 ticker 24h stats fallback failed {sym}: {e}")
        result = {
            "symbol": sym,
            "last_price": last,
            "bid": bid_n,
            "ask": ask_n,
            "volume_24h": 0.0,
            "price_change_pct": pct,
            "high_24h": high,
            "low_24h": low,
        }
        self._ticker_cache[symbol] = (time.monotonic(), result)
        return result

    async def _ticker_from_candles(self, sym: str) -> dict | None:
        """无 tick 报价时的 ticker 构造：最新分钟 K 收盘价 + 近 25 根小时 K 统计"""
        rows = await asyncio.to_thread(
            self._driver.copy_rates_from_pos, sym, TIMEFRAME_MAP["1m"], 0, 1
        )
        if not rows:
            return None
        close = rows[-1]["close"]
        if close <= 0:
            return None
        high, low, pct = close, close, 0.0
        try:
            h1 = await asyncio.to_thread(
                self._driver.copy_rates_from_pos, sym, TIMEFRAME_MAP["1h"], 0, 25
            )
            if h1:
                highs = [r["high"] for r in h1 if r["high"] > 0]
                lows = [r["low"] for r in h1 if r["low"] > 0]
                if highs:
                    high = max(highs)
                if lows:
                    low = min(lows)
        except Exception as e:
            logger.debug(f"MT5 ticker candle fallback failed {sym}: {e}")
        self._track_prec(sym, [close, high, low])
        return {
            "symbol": sym,
            "last_price": close,
            "bid": close,
            "ask": 0.0,
            "volume_24h": 0.0,
            "price_change_pct": pct,
            "high_24h": high,
            "low_24h": low,
        }

    # ─── 实时流：终端轮询（MetaTrader5 包无推送接口） ───

    async def stream_loop(self) -> None:
        logger.info(f"MT5 source stream started (mode=terminal-poll, interval={POLL_INTERVAL}s)")
        while True:
            if not self.available:
                # 不可用不再永久 idle（2026-09-01 实证：旧逻辑睡 60s 死循环不再触发重连，
                # 源掉线后只能靠用户请求碰驱动冷却重建，页面报「未返回 K 线数据」久不自愈）；
                # 重连含子进程拉起 + 8s poll，必须工作线程执行——同步直调曾把事件循环
                # 堵死 10s（全线程栈实证），期间全部 HTTP/WS 停摆
                logger.warning("MT5 source unavailable: terminal not connected, retrying every 30s")
                while not self.available:
                    await asyncio.to_thread(self._try_reconnect)
                    if not self.available:
                        await asyncio.sleep(RECONNECT_COOLDOWN)
            targets = sorted(market_manager.active_targets(self.name))
            if not targets:
                await asyncio.sleep(2)
                continue
            for symbol, tf in targets:
                try:
                    sym = symbol.upper()
                    # 取最新一根（含未收盘）：_klines_sync 内含品种选中/ digits 准备，
                    # 全程 to_thread 不占事件循环；OHLC 变化由 manager 去重签名识别后广播
                    rows = await asyncio.to_thread(
                        self._klines_sync, sym, TIMEFRAME_MAP[tf], tf, 1, None, None
                    )
                    if rows is None:
                        await asyncio.to_thread(self._try_reconnect)
                        break
                    if rows:
                        bar = self._to_bar(rows[-1])
                        await market_manager.publish_bar(self.name, symbol, tf, bar)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.debug(f"MT5 poll error {symbol}/{tf}: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    def _try_reconnect(self) -> None:
        """终端断连（调用返回 None）时重连（冷却内不重复尝试）"""
        now = time.monotonic()
        if now - self._last_reconnect_at < RECONNECT_COOLDOWN:
            return
        self._last_reconnect_at = now
        logger.warning("MT5 terminal connection lost, attempting reconnect...")
        self.available = False
        try:
            self._driver.shutdown()
        except Exception:
            pass
        self.available = self._driver.initialize(**self._init_kwargs())
        if self.available:
            # 后台补注册/断线重连成功后刷新默认品种过滤与 digits 缓存
            #（deferred 路径下 __init__ 未就绪时未曾探查，首次上线需补上）
            self._probe_symbols()
            logger.info("MT5 terminal reconnected")
        else:
            logger.warning("MT5 terminal reconnect failed, will retry after cooldown")

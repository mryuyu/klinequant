"""本地 MetaTrader 5 市场源插件

接入方式：官方 MetaTrader5 Python 包连接本机 MT5 终端（需终端已登录运行，
或配置 MT5_TERMINAL_PATH/MT5_LOGIN 等由其拉起）。历史深度远超 IG demo
（本地终端全量历史，支持多年 M1），实时链路为终端轮询（包无推送接口）。

精度铁律落地：MT5 symbol_info().digits 是订阅到的市场元数据（tick 小数位），
直接作为 price_precision 下发，不做任何推导。

阻塞调用统一 asyncio.to_thread（MetaTrader5 包为同步实现），驱动层可注入
fake 以便单测。

进程隔离：MT5 C 调用可能永久挂起且不释放 GIL（2026-08-31 py-spy 实证：
挂起线程独占 GIL 冻结网关全进程，线程级超时护栏无法生效），故全部
MetaTrader5 包调用在独立子进程执行，超时强杀子进程重建。
"""
from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from gateway.market_sources.base import MarketSource
from gateway.market_sources.manager import market_manager

try:
    import MetaTrader5 as _mt5  # type: ignore
    _HAS_MT5 = True
except ImportError:  # pragma: no cover
    _mt5 = None
    _HAS_MT5 = False

logger = logging.getLogger(__name__)

POLL_INTERVAL = float(os.getenv("MT5_POLL_INTERVAL", "0.5"))   # 终端轮询间隔（秒，与币安 K 线 500ms 更新节奏对齐；本地调用开销低）
TICKER_CACHE_TTL = 5.0   # ticker 缓存（秒）：前端多品种并发轮询时合并重复请求，
                         # 降低对单连接 MT5 包的并发压力（本地终端数据无时效损失容忍）
RECONNECT_COOLDOWN = 30.0   # 重连冷却（秒）：终端长时间未启动时避免每轮都 shutdown/initialize


def _tf_const(name: str, fallback: int) -> int:
    """从 MetaTrader5 包取 TIMEFRAME 常量（包缺失时用官方协议值兜底，便于单测）"""
    return getattr(_mt5, name, fallback) if _HAS_MT5 else fallback


# 前端周期 → MT5 TIMEFRAME 常量（fallback 为 MT5 协议定义值：分钟级=分钟数，
# 小时级=16384+小时数，D1=16408，W1=32769）
TIMEFRAME_MAP = {
    "1m": _tf_const("TIMEFRAME_M1", 1),
    "3m": _tf_const("TIMEFRAME_M3", 3),
    "5m": _tf_const("TIMEFRAME_M5", 5),
    "15m": _tf_const("TIMEFRAME_M15", 15),
    "30m": _tf_const("TIMEFRAME_M30", 30),
    "1h": _tf_const("TIMEFRAME_H1", 16385),
    "2h": _tf_const("TIMEFRAME_H2", 16386),
    "4h": _tf_const("TIMEFRAME_H4", 16388),
    "6h": _tf_const("TIMEFRAME_H6", 16390),
    "12h": _tf_const("TIMEFRAME_H12", 16396),
    "1d": _tf_const("TIMEFRAME_D1", 16408),
    "1w": _tf_const("TIMEFRAME_W1", 32769),
}
# 周期秒数（copy_rates_range 回溯窗口估算用）
TF_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "12h": 43200,
    "1d": 86400, "1w": 604800,
}

# 品种目录：完全可交易模式常量 + path 顶层目录 → 资产类别
# （Commodities 按二级细分：Metals=贵金属，Energies/Softs=商品）
_TRADE_MODE_FULL = _tf_const("SYMBOL_TRADE_MODE_FULL", 4)
_ASSET_TYPE_BY_PATH = {
    "Forex": "forex",
    "Indices": "index",
    "Crypto": "crypto",
    "Bonds CFDs": "bond",
    "Stock CFD's": "stock",
}


def _pipe_safe(val):
    """结果转可 pickle 形式后再经管道回传（2026-09-01 实证）

    MetaTrader5 包的 symbol_info/symbol_info_tick/symbols_get 返回 C 层构造的
    匿名 namedtuple，其 __module__ 为 builtins，pickle 序列化直接失败，导致父进程
    收 None：digits 真值丢失→精度退回价格推导被浮点尾差污染到 8 位（外汇价格多显 000）。
    统一转普通 dict（消费方均属性/键访问兼容：_probe_symbols/_ensure_digits/list_symbols）。
    """
    if val is None or isinstance(val, (bool, int, float, str, bytes)):
        return val
    if hasattr(val, "_asdict"):
        return val._asdict()
    if isinstance(val, (list, tuple)):
        return [v._asdict() if hasattr(v, "_asdict") else v for v in val]
    return val   # numpy 结构数组等本身可 pickle（copy_rates 路径）


def _mt5_worker(conn) -> None:
    """MT5 子进程工作循环：经管道接收 (op, payload)，回发 ("ok", 结果)/("err", 描述)

    模块级函数（Windows spawn 模式子进程需可导入）。挂死被父进程 terminate 强杀，
    不影响网关主进程。MetaTrader5 包仅在此子进程内实际调用。
    """
    try:
        import MetaTrader5 as _mt5w
    except ImportError:
        try:
            conn.send(("err", "MetaTrader5 package not installed"))
        except Exception:
            pass
        return
    while True:
        try:
            op, payload = conn.recv()
        except (EOFError, OSError):
            return   # 父进程关闭管道：静默退出（不打日志，避免与主进程 stderr 交织）
        try:
            if op == "initialize":
                conn.send(("ok", bool(_mt5w.initialize(allow_none=True, **payload))))
            elif op == "call":
                name, args = payload
                conn.send(("ok", _pipe_safe(getattr(_mt5w, name)(*args))))
            elif op == "shutdown":
                _mt5w.shutdown()
                conn.send(("ok", None))
                return
            else:
                conn.send(("err", f"unknown op: {op}"))
        except Exception as e:
            try:
                conn.send(("err", repr(e)))
            except Exception:
                return


class Mt5Api:
    """真实驱动：包装 MetaTrader5 包的同步调用（numpy 结构数组 → 标准 dict）

    MetaTrader5 包单连接且非线程安全：全部调用经全局锁串行。
    C 调用可能永久挂起且持 GIL 不放（线程级超时无法生效，会冻结整个网关），
    故全部包调用放入独立子进程执行：超时即强杀子进程重建，网关进程永不冻结。
    挂死线程在 DLL 内部未返回时重连同样无意义，重连冷却期内快速返回 None。

    返回值约定：错误/连接丢失/超时返回 None；成功但无数据返回 []/空对象。
    """

    _CALL_TIMEOUT = 8.0       # 单次调用超时（秒）：正常 <1s，挂死即强杀子进程重建防冻结网关
    _RECONNECT_COOLDOWN = 30.0

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_init_at = 0.0
        self._kwargs: dict = {}
        self._proc: multiprocessing.Process | None = None
        self._conn = None

    # ─── 子进程生命周期（均须在 _lock 内调用） ───

    def _spawn_locked(self) -> bool:
        """拉起子进程并初始化终端；失败保留空状态由上层兜底返回 None/False"""
        parent_conn, child_conn = multiprocessing.Pipe()
        proc = multiprocessing.Process(
            target=_mt5_worker, args=(child_conn,), daemon=True, name="mt5-worker"
        )
        proc.start()
        child_conn.close()   # 父进程只持父端，子进程退出时父端 recv 得 EOF 可感知
        self._proc, self._conn = proc, parent_conn
        try:
            parent_conn.send(("initialize", self._kwargs))
            if parent_conn.poll(self._CALL_TIMEOUT):
                kind, val = parent_conn.recv()
                if kind == "ok" and val:
                    return True
        except (EOFError, OSError):
            pass
        except Exception:
            logger.warning("MT5 worker initialize failed", exc_info=True)
        self._kill_locked()
        return False

    def _kill_locked(self) -> None:
        """强杀子进程（挂死线程随进程消亡，GIL/DLL 状态一并释放）"""
        if self._proc is not None:
            try:
                self._proc.kill()
                self._proc.join(timeout=2)
            except Exception:
                pass
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        self._proc, self._conn = None, None

    def _ensure_worker_locked(self) -> bool:
        """确认可用子进程；重连受冷却门控，冷却期内直接判不可用"""
        if self._proc is not None and self._proc.is_alive():
            return True
        self._kill_locked()   # 清理已退出的残留句柄
        now = time.monotonic()
        if now - self._last_init_at < self._RECONNECT_COOLDOWN:
            return False
        self._last_init_at = now
        return self._spawn_locked()

    # ─── 统一调用入口（锁内）：子进程执行 + 超时强杀重建 ───

    def _call(self, fn_name: str, *args):
        with self._lock:
            if not _HAS_MT5 or not self._ensure_worker_locked():
                return None
            try:
                self._conn.send(("call", (fn_name, args)))
                if self._conn.poll(self._CALL_TIMEOUT):
                    kind, val = self._conn.recv()
                    if kind == "ok":
                        return val
                    logger.debug("MT5 call %s failed: %s", fn_name, val)
                    return None
            except (EOFError, OSError):
                pass   # 子进程已退出：走下方重建路径（本次返回 None）
            except Exception:
                logger.warning("MT5 call %s error", fn_name, exc_info=True)
                return None
            # 超时/管道断裂：子进程挂死，强杀重建（重建受冷却门控，失败时下次调用再试）
            logger.warning("MT5 call %s timeout >%.0fs, killing worker", fn_name, self._CALL_TIMEOUT)
            self._kill_locked()
            self._ensure_worker_locked()
            return None

    def initialize(self, **kwargs) -> bool:
        if not _HAS_MT5:
            return False
        self._kwargs = kwargs
        with self._lock:
            self._last_init_at = time.monotonic()
            if self._proc is not None and self._proc.is_alive():
                return True   # 子进程存活即终端已初始化（同包单连接）
            self._kill_locked()
            return self._spawn_locked()

    def connect(self) -> bool:
        """外部触发重连（带冷却）"""
        with self._lock:
            now = time.monotonic()
            if now - self._last_init_at < self._RECONNECT_COOLDOWN:
                return False
            self._last_init_at = now
            self._kill_locked()
            return self._spawn_locked()

    def shutdown(self) -> None:
        with self._lock:
            if self._conn is not None and self._proc is not None and self._proc.is_alive():
                try:   # 优雅退出优先，挂死则直接强杀（_mt5w.shutdown 也可能挂）
                    self._conn.send(("shutdown", None))
                    self._proc.join(timeout=2)
                except Exception:
                    pass
            self._kill_locked()

    def symbol_select(self, symbol: str, enable: bool = True) -> bool:
        return bool(self._call("symbol_select", symbol, enable))

    def symbol_info(self, symbol: str):
        return self._call("symbol_info", symbol)

    def symbol_info_tick(self, symbol: str):
        return self._call("symbol_info_tick", symbol)

    def symbols_get(self):
        """终端全量品种元数据（含 path 分类/可交易模式）"""
        if not _HAS_MT5:
            return None
        return self._call("symbols_get")

    def copy_rates_from_pos(self, symbol: str, timeframe: int, start_pos: int, count: int):
        rows = self._call("copy_rates_from_pos", symbol, timeframe, start_pos, count)
        return self._to_rows(rows) if rows is not None else None

    def copy_rates_range(self, symbol: str, timeframe: int, date_from, date_to):
        rows = self._call("copy_rates_range", symbol, timeframe, date_from, date_to)
        return self._to_rows(rows) if rows is not None else None

    @staticmethod
    def _to_rows(rates) -> list[dict] | None:
        """numpy 结构数组 → list[dict]；time 为 UTC epoch 秒（MT5 内部已转 UTC）

        向量化批量转换：逐行 numpy 标量提取纯 Python 循环长时间持 GIL，
        深分页大数组（上万根）会饿死事件循环线程（2026-08-31 全线程栈实证，
        两次 40s+ 停摆均抓到本函数在逐行转换）；列级 astype/tolist 由 numpy 批量完成。
        """
        if rates is None:
            return None
        times = rates["time"].astype("int64").tolist()      # datetime64[s] → epoch 秒
        opens = rates["open"].tolist()
        highs = rates["high"].tolist()
        lows = rates["low"].tolist()
        closes = rates["close"].tolist()
        tick_vols = rates["tick_volume"].tolist()
        real_vols = rates["real_volume"].tolist()
        return [
            {
                "time": t, "open": o, "high": h, "low": l, "close": c,
                "tick_volume": tv, "real_volume": rv,
            }
            for t, o, h, l, c, tv, rv in zip(
                times, opens, highs, lows, closes, tick_vols, real_vols
            )
        ]


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
            logger.info("MT5 terminal reconnected")
        else:
            logger.warning("MT5 terminal reconnect failed, will retry after cooldown")

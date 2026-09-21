"""MT5 子进程驱动 — 行情与交易共用的终端连接层

进程隔离：MT5 C 调用可能永久挂起且不释放 GIL（2026-08-31 py-spy 实证），
故全部 MetaTrader5 包调用在独立子进程执行，超时强杀子进程重建。

本模块从 mt5_source.py 提取，供 Mt5Source（行情）和 Mt5Executor（交易）共享。
MetaTrader5 包单连接且非线程安全：全部调用经全局锁串行。
"""
from __future__ import annotations

import logging
import multiprocessing
import os
import threading
import time
from typing import Any, Optional

try:
    import MetaTrader5 as _mt5  # type: ignore
    _HAS_MT5 = True
except ImportError:  # pragma: no cover
    _mt5 = None
    _HAS_MT5 = False

logger = logging.getLogger(__name__)

# ─── MT5 常量（包缺失时用官方协议值兜底，便于单测）───


def _tf_const(name: str, fallback: int) -> int:
    """从 MetaTrader5 包取常量"""
    return getattr(_mt5, name, fallback) if _HAS_MT5 else fallback


# 前端周期 → MT5 TIMEFRAME 常量
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

# 交易常量
TRADE_ACTION_DEAL = _tf_const("TRADE_ACTION_DEAL", 1)
TRADE_ACTION_PENDING = _tf_const("TRADE_ACTION_PENDING", 5)
TRADE_ACTION_SLTP = _tf_const("TRADE_ACTION_SLTP", 6)
TRADE_ACTION_REMOVE = _tf_const("TRADE_ACTION_REMOVE", 8)

ORDER_TYPE_BUY = _tf_const("ORDER_TYPE_BUY", 0)
ORDER_TYPE_SELL = _tf_const("ORDER_TYPE_SELL", 1)
ORDER_TYPE_BUY_LIMIT = _tf_const("ORDER_TYPE_BUY_LIMIT", 2)
ORDER_TYPE_SELL_LIMIT = _tf_const("ORDER_TYPE_SELL_LIMIT", 3)
ORDER_TYPE_BUY_STOP = _tf_const("ORDER_TYPE_BUY_STOP", 4)
ORDER_TYPE_SELL_STOP = _tf_const("ORDER_TYPE_SELL_STOP", 5)

ORDER_TIME_GTC = _tf_const("ORDER_TIME_GTC", 0)
ORDER_TIME_DAY = _tf_const("ORDER_TIME_DAY", 1)
ORDER_TIME_SPECIFIED = _tf_const("ORDER_TIME_SPECIFIED", 2)

ORDER_FILLING_FOK = _tf_const("ORDER_FILLING_FOK", 0)
ORDER_FILLING_IOC = _tf_const("ORDER_FILLING_IOC", 1)
ORDER_FILLING_RETURN = _tf_const("ORDER_FILLING_RETURN", 2)

TRADE_RETCODE_DONE = 10009
TRADE_RETCODE_DONE_PARTIAL = 10010
TRADE_RETCODE_PLACED = 10008
TRADE_RETCODE_REQUOTE = 10004
TRADE_RETCODE_REJECT = 10006
TRADE_RETCODE_CANCEL = 10007
TRADE_RETCODE_INVALID_STOP = 10016
TRADE_RETCODE_INVALID_VOLUME = 10014
TRADE_RETCODE_INVALID_PRICE = 10015
TRADE_RETCODE_NO_MONEY = 10019
TRADE_RETCODE_MARKET_CLOSED = 10018
TRADE_RETCODE_TIMEOUT = 10012
TRADE_RETCODE_CONNECTION = 10021


# ─── 子进程通信 ───

def _pipe_safe(val):
    """结果转可 pickle 形式后再经管道回传（2026-09-01 实证）

    MetaTrader5 包返回 C 层构造的匿名 namedtuple，pickle 序列化直接失败。
    统一递归转普通 dict/list：OrderSendResult 等结果内嵌 TradeRequest 等
    C 层 namedtuple 字段（2026-09-21 实证 PicklingError），必须逐层展开，
    否则子进程 conn.send 序列化失败 → order_send 恒返回 None。
    """
    if val is None or isinstance(val, (bool, int, float, str, bytes)):
        return val
    if hasattr(val, "_asdict"):
        return {k: _pipe_safe(v) for k, v in val._asdict().items()}
    if isinstance(val, dict):
        return {k: _pipe_safe(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_pipe_safe(v) for v in val]
    return val   # numpy 结构数组等本身可 pickle（copy_rates 路径）


def _mt5_worker(conn) -> None:
    """MT5 子进程工作循环：经管道接收 (op, payload)，回发 ("ok", 结果)/("err", 描述)

    模块级函数（Windows spawn 模式子进程需可导入）。
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
            return
        try:
            if op == "initialize":
                conn.send(("ok", bool(_mt5w.initialize(allow_none=True, **payload))))
            elif op == "call":
                name, args = payload
                conn.send(("ok", _pipe_safe(getattr(_mt5w, name)(*args))))
            elif op == "call_kw":
                name, args, kwargs = payload
                conn.send(("ok", _pipe_safe(getattr(_mt5w, name)(*args, **kwargs))))
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


# ─── 驱动类 ───

class Mt5Api:
    """MT5 终端驱动：子进程隔离 + 全局锁串行 + 超时强杀重建

    行情（Mt5Source）和交易（Mt5Executor）共享同一实例。
    返回值约定：错误/连接丢失/超时返回 None；成功但无数据返回 []/空对象。
    """

    _CALL_TIMEOUT = 8.0
    _RECONNECT_COOLDOWN = 30.0

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_init_at = 0.0
        self._kwargs: dict = {}
        self._proc: multiprocessing.Process | None = None
        self._conn = None

    @property
    def available(self) -> bool:
        """子进程是否存活"""
        return self._proc is not None and self._proc.is_alive()

    # ─── 子进程生命周期 ───

    def _spawn_locked(self) -> bool:
        parent_conn, child_conn = multiprocessing.Pipe()
        proc = multiprocessing.Process(
            target=_mt5_worker, args=(child_conn,), daemon=True, name="mt5-worker"
        )
        proc.start()
        child_conn.close()
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
        if self._proc is not None and self._proc.is_alive():
            return True
        self._kill_locked()
        now = time.monotonic()
        if now - self._last_init_at < self._RECONNECT_COOLDOWN:
            return False
        self._last_init_at = now
        return self._spawn_locked()

    # ─── 调用入口 ───

    def _call(self, fn_name: str, *args):
        """位置参数调用"""
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
                pass
            except Exception:
                logger.warning("MT5 call %s error", fn_name, exc_info=True)
                return None
            logger.warning("MT5 call %s timeout >%.0fs, killing worker", fn_name, self._CALL_TIMEOUT)
            self._kill_locked()
            self._ensure_worker_locked()
            return None

    def _call_kw(self, fn_name: str, *args, **kwargs):
        """带关键字参数调用（positions_get(symbol=...) 等）"""
        with self._lock:
            if not _HAS_MT5 or not self._ensure_worker_locked():
                return None
            try:
                self._conn.send(("call_kw", (fn_name, args, kwargs)))
                if self._conn.poll(self._CALL_TIMEOUT):
                    kind, val = self._conn.recv()
                    if kind == "ok":
                        return val
                    logger.debug("MT5 call_kw %s failed: %s", fn_name, val)
                    return None
            except (EOFError, OSError):
                pass
            except Exception:
                logger.warning("MT5 call_kw %s error", fn_name, exc_info=True)
                return None
            logger.warning("MT5 call_kw %s timeout, killing worker", fn_name)
            self._kill_locked()
            self._ensure_worker_locked()
            return None

    # ─── 生命周期 ───

    def initialize(self, **kwargs) -> bool:
        if not _HAS_MT5:
            return False
        self._kwargs = kwargs
        with self._lock:
            self._last_init_at = time.monotonic()
            if self._proc is not None and self._proc.is_alive():
                return True
            self._kill_locked()
            return self._spawn_locked()

    def connect(self) -> bool:
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
                try:
                    self._conn.send(("shutdown", None))
                    self._proc.join(timeout=2)
                except Exception:
                    pass
            self._kill_locked()

    # ─── 行情方法 ───

    def symbol_select(self, symbol: str, enable: bool = True) -> bool:
        return bool(self._call("symbol_select", symbol, enable))

    def symbol_info(self, symbol: str) -> Optional[dict]:
        return self._call("symbol_info", symbol)

    def symbol_info_tick(self, symbol: str) -> Optional[dict]:
        return self._call("symbol_info_tick", symbol)

    def symbols_get(self):
        if not _HAS_MT5:
            return None
        return self._call("symbols_get")

    def copy_rates_from_pos(self, symbol: str, timeframe: int, start_pos: int, count: int):
        rows = self._call("copy_rates_from_pos", symbol, timeframe, start_pos, count)
        return self._to_rows(rows) if rows is not None else None

    def copy_rates_range(self, symbol: str, timeframe: int, date_from, date_to):
        rows = self._call("copy_rates_range", symbol, timeframe, date_from, date_to)
        return self._to_rows(rows) if rows is not None else None

    # ─── 交易方法 ───

    def order_send(self, request: dict) -> Optional[dict]:
        """发送交易请求。request 为 MT5 TradeRequest dict。"""
        return self._call("order_send", request)

    def positions_get(self, symbol: str = "") -> list:
        """查询持仓。symbol 为空返回全部。"""
        if symbol:
            result = self._call_kw("positions_get", symbol=symbol)
        else:
            result = self._call("positions_get")
        return result if result else []

    def orders_get(self, symbol: str = "") -> list:
        """查询挂单。symbol 为空返回全部。"""
        if symbol:
            result = self._call_kw("orders_get", symbol=symbol)
        else:
            result = self._call("orders_get")
        return result if result else []

    def account_info(self) -> Optional[dict]:
        """查询账户信息。"""
        return self._call("account_info")

    def history_deals_get(self, position: int = 0) -> list:
        """查询历史成交。"""
        if position:
            result = self._call_kw("history_deals_get", position=position)
        else:
            result = self._call("history_deals_get")
        return result if result else []

    # ─── 工具 ───

    @staticmethod
    def _to_rows(rates) -> list[dict] | None:
        """numpy 结构数组 → list[dict]（向量化批量转换）"""
        if rates is None:
            return None
        times = rates["time"].astype("int64").tolist()
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

    @staticmethod
    def init_kwargs() -> dict:
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

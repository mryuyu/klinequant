"""策略语义状态持久化（Phase R4：StateStore + 可插拔 StateBackend）。

设计约束（对齐《SDK 阶段实施规划 v1.3》R4 + 真相源分层红线）：
  - ``api.state`` 属**语义级**（L-C），只存**不可从 venue 重算**的策略语义量
    （加仓计数、上次信号 bar 时间、跨周期趋势结论等）；
  - 红线：``api.state`` 里**禁止**存持仓/在途/订单号——那些一律走 R1~R3 venue 对账，
    否则出现「state 说持有 0.02、venue 说没有」的双真相源，恢复逻辑无法写；
  - 持久化强度：语义级接受「最坏丢 1 秒」（变更 debounce 落盘 + runner 自动 load/save 安全网），
    故 SQLite 用 ``WAL + synchronous=NORMAL``（进程崩溃不丢、停电靠对账兜底），
    与账本级 journal（``synchronous=FULL``）区分强度；
  - 后端可插拔：一期 ``SqliteStateBackend``（单文件 ``data/state/{account}.db``），Redis 候选；
  - 存储 key = **策略级一份**（``{account}:{tag}``，同一 runner 内全品种共享一个 StateStore）；
    内部字段由策略自行按 ``{symbol}:{period}:{name}`` 命名空间分槽
    （Phase M 多周期共振：小周期读大周期结论走 ``state['EURUSD:1h:trend']``）。

自动 load/save 为主干（安全网在 runner，不靠策略作者自觉）：
  - ``LiveRunner._initialize`` 自动 ``load()``（重启预填充），``_shutdown`` 自动 ``save()``；
  - 显式 ``api.state.save()`` / ``load()`` 保留，供策略作者在关键节点强制落盘；
  - ``api.is_resumed()``：区分冷启动 vs 崩溃恢复，让策略自行决定继续持有还是先 ``flatten()``。

线程安全：拓扑 Z 下多品种线程并发读写同一 StateStore，互斥锁串行化 + debounce 节流落盘。
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

__all__ = [
    "StateBackend",
    "SqliteStateBackend",
    "InMemoryStateBackend",
    "StateStore",
]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS strategy_state (
    state_key  TEXT PRIMARY KEY,
    payload    TEXT NOT NULL DEFAULT '{}',
    updated_at REAL NOT NULL DEFAULT 0
);
"""


@runtime_checkable
class StateBackend(Protocol):
    """策略状态持久化后端协议（可插拔：Sqlite / InMemory / Redis 候选）。"""

    def load(self, key: str) -> dict | None:
        """读取 key 的状态快照（无则 None）。"""
        ...

    def save(self, key: str, payload: dict) -> None:
        """写入 key 的状态快照（整体覆盖）。"""
        ...

    def close(self) -> None:
        """关闭后端（flush + 释放连接）。"""
        ...


class SqliteStateBackend:
    """SQLite 状态后端（``WAL + synchronous=NORMAL``）。

    语义级强度：进程崩溃（``taskkill /F``）不丢已 commit 的快照，停电/OS 崩溃最坏丢
    最近一次 commit（debounce ≤1 秒），由 R1~R3 venue 对账兜底，故无需 FULL 的每写 fsync。
    """

    def __init__(self, db_path: str | Path, *, synchronous: str = "NORMAL"):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.execute(f"PRAGMA synchronous={synchronous}")
        self._conn.execute("PRAGMA journal_mode=WAL")
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()
        logger.info(f"StateBackend ready: {self._db_path} (synchronous={synchronous})")

    def load(self, key: str) -> dict | None:
        with self._lock:
            cur = self._conn.execute(
                "SELECT payload FROM strategy_state WHERE state_key=?", (key,)
            )
            row = cur.fetchone()
        if row is None:
            return None
        try:
            data = json.loads(row[0])
        except (ValueError, TypeError) as e:
            logger.warning(f"StateBackend.load: bad payload for key={key}: {e}")
            return None
        return data if isinstance(data, dict) else None

    def save(self, key: str, payload: dict) -> None:
        blob = json.dumps(payload, default=str, ensure_ascii=False)
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO strategy_state(state_key, payload, updated_at)"
                " VALUES(?,?,?)",
                (key, blob, time.time()),
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.commit()
                self._conn.close()
            except Exception as e:  # pragma: no cover - 关闭异常不阻断退出
                logger.warning(f"SqliteStateBackend close error: {e}")


class InMemoryStateBackend:
    """内存状态后端（回测/单测用，无落盘）。接口与 SqliteStateBackend 同构。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[str, dict] = {}

    def load(self, key: str) -> dict | None:
        with self._lock:
            snap = self._data.get(key)
            return dict(snap) if snap is not None else None

    def save(self, key: str, payload: dict) -> None:
        with self._lock:
            self._data[key] = dict(payload)

    def close(self) -> None:
        pass


class StateStore(dict):
    """策略语义状态存储（``dict`` 子类，带脏标记 + debounce 落盘 + 恢复标记）。

    用法（策略内）::

        api.state['EURUSD:1h:trend'] = 'up'   # 变更自动标脏 + debounce 落盘
        n = api.state.get('add_count', 0)      # 读操作不拦截、不触发落盘
        if api.is_resumed():                   # 崩溃恢复：保守处理，避免盲目加仓
            api.flatten()

    红线：只存不可从 venue 重算的语义量；禁止存持仓/在途/订单号（走 R1~R3 对账）。
    序列化：整表 JSON。int/float/str/bool/list/dict/None 精确往返；tuple 往返降级为
    list；异类值经 ``default=str`` 容错（不保证往返等价，故 state 只应放简单语义量）。
    """

    def __init__(
        self, *, backend: StateBackend | None = None, key: str = "",
        debounce: float = 1.0,
    ):
        super().__init__()
        self._backend = backend
        self._key = key
        self._debounce = max(0.0, debounce)
        self._dirty = False
        self._resumed = False
        self._lock = threading.Lock()
        self._last_flush = 0.0

    @property
    def key(self) -> str:
        """持久化 key（策略级一份，``{account}:{tag}``）。"""
        return self._key

    def is_resumed(self) -> bool:
        """本次运行是否从上一轮持久化快照恢复（True=崩溃/重启恢复，False=冷启动）。"""
        return self._resumed

    # ─── 持久化 ───

    def load(self) -> StateStore:
        """从后端加载快照（重启预填充）。backend=None 时 no-op（回测/单测）。"""
        if self._backend is None:
            return self
        data = self._backend.load(self._key)
        with self._lock:
            super().clear()
            if data:
                super().update(data)
                self._resumed = True
            self._dirty = False
            self._last_flush = 0.0
        if self._resumed:
            logger.info(
                f"[STATE] loaded snapshot key={self._key} ({len(data or {})} field(s))"
            )
        return self

    def save(self) -> None:
        """立即落盘（仅当脏）。runner 退出安全网 + 策略显式调用。"""
        with self._lock:
            if self._backend is None or not self._dirty:
                return
            payload = dict(self)
            self._backend.save(self._key, payload)
            self._dirty = False
            self._last_flush = time.monotonic()

    def flush_if_dirty(self) -> None:
        """debounce 到点才落盘（变更节流，最坏丢 debounce 秒）。

        ``_last_flush==0`` 表示本轮尚未落过盘 → 首次变更立即落盘（不 defer），
        避免「设一次就idle然后崩溃」丢掉唯一一次变更。
        """
        with self._lock:
            if self._backend is None or not self._dirty:
                return
            if self._last_flush and (time.monotonic() - self._last_flush) < self._debounce:
                return
        self.save()  # save 内部再取锁 + 复查 dirty（幂等）

    def _mark_dirty(self) -> None:
        self._dirty = True
        self.flush_if_dirty()

    # ─── dict 变更钩子（标脏 + debounce 落盘；读操作不拦截）───

    def __setitem__(self, key: Any, value: Any) -> None:
        with self._lock:
            super().__setitem__(key, value)
        self._mark_dirty()

    def __delitem__(self, key: Any) -> None:
        with self._lock:
            super().__delitem__(key)
        self._mark_dirty()

    def update(self, *args: Any, **kwargs: Any) -> None:
        with self._lock:
            super().update(*args, **kwargs)
        self._mark_dirty()

    def setdefault(self, key: Any, default: Any = None) -> Any:
        with self._lock:
            existed = super().__contains__(key)
            val = super().setdefault(key, default)
        if not existed:
            self._mark_dirty()
        return val

    def pop(self, key: Any, *args: Any) -> Any:
        with self._lock:
            val = super().pop(key, *args)
        self._mark_dirty()
        return val

    def popitem(self) -> Any:
        with self._lock:
            val = super().popitem()
        self._mark_dirty()
        return val

    def clear(self) -> None:
        with self._lock:
            super().clear()
        self._mark_dirty()

"""Mt5DataFeed — MT5 实时数据轮询 + Event 通知

后台线程轮询 MT5 终端（symbol_info_tick + copy_rates_from_pos），
新数据到达时 set Event，策略 wait_update() 阻塞在 Event 上。

设计：
  - 轮询间隔可配（默认 0.5s，与 MT5 终端刷新节奏对齐）
  - tick 每次轮询都更新（价格变动即通知）
  - bars 仅在最新 bar 的 close/time 变化时更新（去重）
  - is_changing() 通过版本号比对实现
"""
from __future__ import annotations

import logging
import threading
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

from gateway.market_sources.mt5_driver import Mt5Api, TIMEFRAME_MAP
from protocol.types import Tick

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 0.5  # 秒

# M1：收盘事件专用版本 key 后缀 + 触发字段名（对齐 tqsdk is_changing(k, "datetime")）。
#   基础 key `{symbol}/{period}` 表任意变化（含盘中 forming bar）；`{...}#bar` 仅收盘 bump。
_BAR_KEY_SUFFIX = "#bar"
_BAR_CLOSE_FIELDS = frozenset({"timestamp", "time", "datetime"})


class Mt5DataFeed:
    """MT5 数据源（实现 DataFeedProtocol）"""

    def __init__(
        self,
        driver: Mt5Api,
        symbols: List[str],
        periods: List[str],
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        bar_count: int = 300,
        warmup_timeout: float = 30.0,
        warm_retry_interval: float = 5.0,
    ):
        """
        Args:
            driver: Mt5Api 共享实例
            symbols: 订阅品种列表
            periods: 订阅周期列表（如 ["1m", "1h"]）
            poll_interval: 轮询间隔（秒）
            bar_count: 每次拉取的 bar 数量
            warmup_timeout: M7 单品种历史预热的 patient 超时（秒）。冷门品种首次
                copy_rates_from_pos 可能远超默认 8s，用更长超时避免误触发 worker
                强杀 + 冷却连锁（超时降级）。
            warm_retry_interval: M7 degraded 品种后台节流重试间隔（秒）。预热解耦：
                未就绪品种不阻塞其余品种轮询，按此间隔重试，就绪后自动接入。
        """
        self._driver = driver
        self._symbols = [s.upper() for s in symbols]
        self._periods = periods
        self._poll_interval = poll_interval
        self._bar_count = bar_count
        self._warmup_timeout = warmup_timeout
        self._warm_retry_interval = warm_retry_interval
        # M7 预热解耦：per-symbol 就绪标记 + 上次预热尝试时刻（节流重试用）。
        #   未就绪品种在轮询循环内懒预热，不阻塞已就绪品种的实时轮询。
        self._ready: dict[str, bool] = {s: False for s in self._symbols}
        self._last_warm_attempt: dict[str, float] = {}

        # 数据存储
        self._ticks: Dict[str, Tick] = {}           # symbol → latest tick
        self._bars: Dict[str, List[dict]] = {}      # "SYMBOL/period" → bars
        self._lock = threading.Lock()

        # 版本号（is_changing 用）
        self._versions: Dict[str, int] = {}         # key → version
        # M4-a：is_changing 基线快照迁至每个 KqApi 独立持有（api._snapshot）；
        #   _snapshot_versions 仅服务 feed 级 wait_update/is_changing（单线程直调，如单测）。
        self._snapshot_versions: Dict[str, int] = {}

        # Event 通知（M4-b：per-symbol，避免任一品种更新惊群唤醒全部品种线程）
        #   _event=全局事件（feed 级 wait_update(symbol=None) 用，如单测）；
        #   _events[symbol]=品种级事件（拓扑 Z 下每个 KqApi 只被自己品种唤醒）。
        self._event = threading.Event()
        self._events: dict[str, threading.Event] = {
            s: threading.Event() for s in self._symbols
        }
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # R5 断线闸门心跳：上次「驱动响应」的单调时刻（连上就每轮刷新，与行情是否变动无关）。
        #   初值=构造时刻，使刚建好的 feed 不会立即被判 stale。
        self._last_data_mono: float = time.monotonic()
        # M6 品种级看门狗：每品种上次驱动响应的单调时刻（一个品种停摆只冻结自己，
        #   不影响其余品种）。全局 _last_data_mono 仍保留供无参回落。
        self._last_symbol_mono: dict[str, float] = {
            s: self._last_data_mono for s in self._symbols
        }

    # ─── 生命周期 ───

    def start(self) -> None:
        """启动后台轮询线程"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True, name="mt5-feed")
        self._thread.start()
        logger.info(f"Mt5DataFeed started: symbols={self._symbols} periods={self._periods}")

    def stop(self) -> None:
        """停止轮询"""
        self._running = False
        self._event.set()  # 唤醒 feed 级 wait_update
        for ev in self._events.values():   # M4-b：唤醒所有品种线程
            ev.set()
        if self._thread:
            self._thread.join(timeout=3)
        logger.info("Mt5DataFeed stopped")

    # ─── DataFeedProtocol 实现 ───

    def latest_tick(self, symbol: str) -> Optional[Tick]:
        """最新 tick"""
        with self._lock:
            return self._ticks.get(symbol.upper())

    def latest_bars(self, symbol: str, period: str, count: int = 200) -> List[dict]:
        """最新 K 线序列"""
        key = f"{symbol.upper()}/{period}"
        with self._lock:
            bars = self._bars.get(key, [])
            return bars[-count:] if len(bars) > count else list(bars)

    def wait_update(self, deadline: float | None = None) -> bool:
        """阻塞等待新数据（feed 级，symbol=None → 全局事件；单线程直调如单测）。

        多品种实盘请经 KqApi.wait_update（per-api 快照 + per-symbol 事件，M4-a/M4-b）。

        Args:
            deadline: 超时秒数（None=无限等待）

        Returns: True=有更新, False=超时
        """
        got, snap = self.wait_and_snapshot(deadline, symbol=None)
        with self._lock:
            self._snapshot_versions = snap
        return got

    def wait_and_snapshot(
        self, deadline: float | None = None, symbol: str | None = None,
    ) -> tuple[bool, dict[str, int]]:
        """等待新数据并返回 (是否有更新, 等待起始的版本快照)。

        M3：顺序为 clear → snapshot → wait（clear 先行，避免 snapshot 与 clear 之间的
        bump+set 被 clear 吞掉而延迟一整轮）。M4-a：快照返回给调用方（KqApi 各自持有），
        不再写 feed 级共享字段，消除多线程互相覆盖基线。M4-b：等待品种级事件。
        """
        if not self._running:
            return False, {}
        ev = self._event_for(symbol)
        ev.clear()                                   # M3：先 clear
        with self._lock:
            snap = dict(self._versions)              # M4-a：clear 后取快照，返回给调用方
        got = ev.wait(timeout=deadline)
        return (got and self._running), snap

    def current_version(self, key: str) -> int:
        """某版本 key 的当前版本号（KqApi.is_changing 比对用，M4-a）。"""
        with self._lock:
            return self._versions.get(key, 0)

    def resolve_key(self, obj: Any, field: str | None = None) -> str | None:
        """对象 → 版本 key（公开包装，KqApi.is_changing 用；含 M1 field 语义）。"""
        return self._resolve_key(obj, field)

    def _event_for(self, symbol: str | None) -> threading.Event:
        """品种级事件（M4-b）；symbol=None 或未知品种回落全局事件（feed 级 wait_update 用）。"""
        if symbol is None:
            return self._event
        return self._events.get(symbol.upper(), self._event)

    def is_changing(self, obj: Any, field: Optional[str] = None) -> bool:
        """检查对象自上次 wait_update 后是否有变化。

        obj 可以是：
          - Tick 实例 → 按 symbol 查版本
          - str（如 "EURUSD/1m"）→ 按 key 查版本
          - list（bars）→ 按第一个元素的 key 查
        """
        key = self._resolve_key(obj, field)
        if key is None:
            return False
        with self._lock:
            current = self._versions.get(key, 0)
            snapshot = self._snapshot_versions.get(key, 0)
            return current != snapshot

    def now_ms(self) -> int:
        """当前时间戳（Unix ms）"""
        return int(time.time() * 1000)

    def seconds_since_update(self, symbol: str | None = None) -> float:
        """R5：距上次驱动成功响应的秒数（心跳龄）。断线时持续增长，超阈即 degraded。

        M6：symbol 给定→该品种心跳龄（品种级看门狗）；None/未知品种→全局心跳龄。
        """
        if symbol is not None:
            last = self._last_symbol_mono.get(symbol.upper())
            if last is not None:
                return time.monotonic() - last
        return time.monotonic() - self._last_data_mono

    # ─── 内部轮询 ───

    def _poll_loop(self) -> None:
        """后台轮询主循环（M7 预热解耦）。

        旧版启动时同步 _load_initial_bars 拉全部品种历史——任一冷门品种 >8s 会
        阻塞整条 feed 数十秒并触发 worker 强杀连锁。现改为循环内 per-symbol 懒
        预热：未就绪品种先 _try_warm（节流重试），就绪后才轮询 tick/bars；degraded
        品种后台重试，不阻塞已就绪品种的实时轮询，实现分批就绪。
        """
        while self._running:
            try:
                updated_symbols: set[str] = set()

                for symbol in self._symbols:
                    # M7：未就绪品种先尝试预热（节流重试），就绪即唤醒其线程接入
                    if not self._ready.get(symbol):
                        if self._try_warm(symbol):
                            updated_symbols.add(symbol)
                        continue

                    # 轮询 tick（价格变动即通知）
                    if self._poll_tick(symbol):
                        updated_symbols.add(symbol)
                    # 轮询 bars（仅最新一根）
                    for period in self._periods:
                        if self._poll_bars(symbol, period):
                            updated_symbols.add(symbol)

                # M4-b：只唤醒有更新的品种线程（+ 全局事件供 feed 级 wait_update）
                for symbol in updated_symbols:
                    self._event_for(symbol).set()
                if updated_symbols:
                    self._event.set()

            except Exception as e:
                logger.error(f"DataFeed poll error: {e}", exc_info=True)

            time.sleep(self._poll_interval)

    def _warm_symbol(self, symbol: str) -> bool:
        """预热单个品种的历史 bars——所有订阅 period 都拿到数据才算就绪。

        M7 超时降级：copy_rates_from_pos 用 warmup_timeout（patient）而非默认 8s，
        避免冷门品种首次拉取 >8s 误触发 worker 强杀 + 冷却连锁。任一 period 超时/
        无数据 → 返回 False（整品种 degraded，稍后节流重试）。
        """
        self._driver.symbol_select(symbol, True)
        loaded: dict[str, list[dict]] = {}
        for period in self._periods:
            tf_const = TIMEFRAME_MAP.get(period)
            if not tf_const:
                logger.warning(f"Unknown period: {period}")
                continue
            rows = self._driver.copy_rates_from_pos(
                symbol, tf_const, 0, self._bar_count, timeout=self._warmup_timeout
            )
            if not rows:
                return False   # 任一 period 未就绪 → 整品种 degraded
            key = f"{symbol}/{period}"
            loaded[key] = [self._row_to_bar(r, symbol, period) for r in rows]
        if not loaded:
            return False
        with self._lock:
            for key, bars in loaded.items():
                self._bars[key] = bars
                self._bump_version(key)
        logger.info(f"Warmed {symbol}: {', '.join(loaded)}")
        return True

    def _try_warm(self, symbol: str) -> bool:
        """节流重试预热（M7 预热解耦）：成功→标就绪 + 刷新心跳；失败→标 degraded。

        按 warm_retry_interval 节流，避免每轮 poll 都对冷门品种发起昂贵的历史拉取。
        就绪即刷新品种级 + 全局心跳，使预热耗时不预先老化心跳（沿用旧版冷启动重置意图）。
        """
        now = time.monotonic()
        last = self._last_warm_attempt.get(symbol)
        if last is not None and (now - last) < self._warm_retry_interval:
            return False   # 节流窗内不重试
        self._last_warm_attempt[symbol] = now
        if self._warm_symbol(symbol):
            self._ready[symbol] = True
            mono = time.monotonic()
            self._last_symbol_mono[symbol] = mono   # M6 品种级心跳
            self._last_data_mono = mono             # 全局心跳
            return True
        logger.warning(
            f"Symbol {symbol} warmup degraded, retry in {self._warm_retry_interval:.0f}s"
        )
        return False

    def is_ready(self, symbol: str) -> bool:
        """M7：品种历史预热是否就绪（就绪后才轮询实时数据；degraded 品种返 False）。"""
        return self._ready.get(symbol.upper(), False)

    def _poll_tick(self, symbol: str) -> bool:
        """轮询单个品种 tick，返回是否有更新"""
        raw = self._driver.symbol_info_tick(symbol)
        if raw is None:
            return False
        # 驱动响应即视为连接存活（即使价格未变）→ 刷新 R5 心跳，与「休市/清淡」区分
        self._last_data_mono = time.monotonic()
        self._last_symbol_mono[symbol.upper()] = self._last_data_mono   # M6：品种级

        bid = Decimal(str(raw.get("bid", 0)))
        ask = Decimal(str(raw.get("ask", 0)))
        if bid == 0 and ask == 0:
            return False

        last = Decimal(str(raw.get("last", 0)))
        if last == 0:
            last = (bid + ask) / 2

        ts = int(raw.get("time", 0))
        if ts > 0 and ts < 1e12:
            ts *= 1000  # 秒 → 毫秒

        tick = Tick(
            symbol=symbol,
            exchange="mt5",
            timestamp=ts or int(time.time() * 1000),
            last_price=last,
            bid_price=bid,
            bid_qty=Decimal(str(raw.get("bid_size", 0))),
            ask_price=ask,
            ask_qty=Decimal(str(raw.get("ask_size", 0))),
            volume_24h=Decimal("0"),  # FX 无成交量
        )

        with self._lock:
            old = self._ticks.get(symbol)
            self._ticks[symbol] = tick
            # 价格变动才 bump 版本
            if old is None or old.bid_price != bid or old.ask_price != ask:
                self._bump_version(f"tick/{symbol}")
                return True
        return False

    def _poll_bars(self, symbol: str, period: str) -> bool:
        """轮询最新 bar（仅取最近 2 根判断是否有新 bar 或 close 变化）"""
        tf_const = TIMEFRAME_MAP.get(period)
        if not tf_const:
            return False

        rows = self._driver.copy_rates_from_pos(symbol, tf_const, 0, 2)
        if not rows:
            return False
        self._last_data_mono = time.monotonic()  # 驱动响应→刷新 R5 心跳
        self._last_symbol_mono[symbol.upper()] = self._last_data_mono   # M6：品种级

        key = f"{symbol}/{period}"
        latest_row = rows[-1]
        new_bar = self._row_to_bar(latest_row, symbol, period)

        with self._lock:
            bars = self._bars.get(key, [])
            if not bars:
                self._bars[key] = [new_bar]
                self._bump_version(key)
                return True

            last_bar = bars[-1]
            # 新 bar（time 不同）或 close 变化
            if new_bar["timestamp"] != last_bar["timestamp"]:
                # M2：新 bar 出现时，rows[0] 是刚收盘那根的终端终值，而本地
                # bars[-1] 只是最后一次轮询（最多一个 poll_interval 前）的快照，
                # 缺失收盘前最后 ≤poll_interval 的 H/L 极值。先用 rows[0] 回填
                # bars[-1]（数据已取到，零额外 IPC），再 append 新 bar。
                if len(rows) >= 2:
                    closed_bar = self._row_to_bar(rows[0], symbol, period)
                    if closed_bar["timestamp"] == last_bar["timestamp"]:
                        bars[-1] = closed_bar
                bars.append(new_bar)
                # 保持长度限制
                if len(bars) > self._bar_count:
                    bars[:] = bars[-self._bar_count:]
                # M1：新 bar 出现 = 上一根收盘 → 基础 key 与 #bar 收盘 key 同时 bump
                self._bump_bar_close(key)
                return True
            elif new_bar["close"] != last_bar["close"] or new_bar["high"] != last_bar["high"]:
                bars[-1] = new_bar
                self._bump_version(key)
                return True

        return False

    # ─── 工具 ───

    def _row_to_bar(self, row: dict, symbol: str, period: str) -> dict:
        """MT5 rate row → 标准 bar dict"""
        vol = row.get("real_volume") or row.get("tick_volume") or 0
        return {
            "symbol": symbol,
            "period": period,
            "timestamp": int(row["time"]) * 1000,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(vol),
        }

    def _bump_version(self, key: str) -> None:
        """版本号 +1（必须在 _lock 内调用）"""
        self._versions[key] = self._versions.get(key, 0) + 1

    def _bump_bar_close(self, key: str) -> None:
        """新 bar 收盘：同时 bump 基础 key 与收盘专用 `#bar` key（M1）。

        必须在 _lock 内调用。策略据此区分盘中变动（仅基础 key）与收盘事件（#bar）。
        """
        self._bump_version(key)
        self._bump_version(key + _BAR_KEY_SUFFIX)

    def _resolve_key(self, obj: Any, field: str | None = None) -> str | None:
        """将对象解析为版本 key。

        M1：``field ∈ ("timestamp","time","datetime")`` 时映射到收盘专用 key
        ``{base}#bar``（仅新 bar 收盘时 bump），使策略能区分「盘中变动」与「收盘
        事件」——对齐 tqsdk ``is_changing(klines[-1], "datetime")`` 语义。tick 类
        key 无收盘概念，field 忽略。
        """
        base = self._base_key(obj)
        if base is None:
            return None
        if field in _BAR_CLOSE_FIELDS and not base.startswith("tick/"):
            return base + _BAR_KEY_SUFFIX
        return base

    def _base_key(self, obj: Any) -> str | None:
        """对象 → 基础版本 key（不含 field 语义）。"""
        if isinstance(obj, Tick):
            return f"tick/{obj.symbol}"
        if isinstance(obj, str):
            # 直接用字符串作 key（如 "EURUSD/1m"）
            return obj
        if isinstance(obj, list) and obj and isinstance(obj[0], dict):
            # bars 列表：用第一个元素的 symbol/period 构造 key
            bar = obj[0]
            if "symbol" in bar and "period" in bar:
                return f"{bar['symbol']}/{bar['period']}"
        if isinstance(obj, dict) and "symbol" in obj and "period" in obj:
            return f"{obj['symbol']}/{obj['period']}"
        return None

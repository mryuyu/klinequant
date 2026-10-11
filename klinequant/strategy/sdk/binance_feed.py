"""BinanceDataFeed — 币安 Futures WS 推送数据源（实现 DataFeedProtocol）

复用 BinanceFuturesAdapter（fstream WS 推送 + fapi REST 预热），把 async WS
K 线回调收敛为与 Mt5DataFeed **同机制**的同步 DataFeedProtocol：
  - start()：REST fetch_klines 预热历史 bars → subscribe_kline（WS 回调）→ start_ws
  - WS K 线回调（async，跑在 backend 的 event loop 线程）：更新 bars 缓存
    （新 bar append / 未收盘 bar 更新 close/high/low）+ bump version + Event.set
  - latest_bars/latest_tick/wait_update/is_changing/now_ms：threading.Lock +
    Event 电平触发 + 版本快照，与 Mt5DataFeed 一致
  - bars dict 格式与 Mt5DataFeed._row_to_bar 一致（symbol/period/timestamp/
    open/high/low/close/volume），使策略 api.klines() 零改动消费（同构）

线程模型：WS 回调在 loop 线程写缓存 + set Event；策略线程 wait_update 阻塞在
Event 上被唤醒读缓存。threading.Lock 保护跨线程读写。
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

from gateway.market_sources.mt5_driver import TF_SECONDS
from protocol.types import Kline, Tick

logger = logging.getLogger(__name__)

# M1：收盘事件专用版本 key 后缀 + 触发字段名（与 data_feed/backtest_feed 同构）。
_BAR_KEY_SUFFIX = "#bar"
_BAR_CLOSE_FIELDS = frozenset({"timestamp", "time", "datetime"})


class BinanceDataFeed:
    """币安 Futures 数据源（WS 推送 → DataFeedProtocol）"""

    # G1：补洞失败（REST 瞬断）后的节流重试间隔（秒）；源返空（合法洞）不重试
    _GAP_RETRY_SEC = 30.0

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        adapter,
        symbols: List[str],
        periods: List[str],
        bar_count: int = 300,
        poll_interval: float = 0.5,
        start_timeout: float = 30.0,
    ):
        """
        Args:
            loop: BinanceBackend 持有的 event loop（独立 daemon 线程）
            adapter: BinanceFuturesAdapter 实例（需已 connect）
            symbols: 订阅品种列表（如 ["BTCUSDT"]）
            periods: 订阅周期列表（币安 interval，如 ["1m"]）
            bar_count: 预热历史 bar 数量（同时作为缓存长度上限）
            poll_interval: 兼容 DataFeedProtocol 构造签名（推送模式不轮询，忽略）
            start_timeout: start() 阻塞等待预热+订阅+WS 启动的超时（秒）
        """
        self._loop = loop
        self._adapter = adapter
        self._symbols = [s.upper() for s in symbols]
        self._periods = periods
        self._bar_count = bar_count
        self._poll_interval = poll_interval
        self._start_timeout = start_timeout

        # 数据存储
        self._ticks: Dict[str, Tick] = {}           # symbol → latest tick
        self._bars: Dict[str, List[dict]] = {}      # "SYMBOL/period" → bars
        self._lock = threading.Lock()

        # 版本号（is_changing 用）
        self._versions: Dict[str, int] = {}
        self._snapshot_versions: Dict[str, int] = {}

        # Event 通知（M4-b：per-symbol，避免任一品种更新惊群唤醒全部品种线程）
        #   _event=全局事件（feed 级 wait_update(symbol=None) 用，如单测）；
        #   _events[symbol]=品种级事件（拓扑 Z 下每个 KqApi 只被自己品种唤醒）。
        self._event = threading.Event()
        self._events: dict[str, threading.Event] = {
            s: threading.Event() for s in self._symbols
        }
        self._running = False

        # R5 断线闸门心跳：上次「驱动响应」的单调时刻（收到 WS 推送即刷新，与行情是否变动无关）。
        #   初值=构造时刻，使刚建好的 feed 不会立即被判 stale。
        self._last_data_mono: float = time.monotonic()
        # M6 品种级看门狗：每品种上次 WS 推送的单调时刻（一个品种停摆只冻结自己）。
        self._last_symbol_mono: dict[str, float] = {
            s: self._last_data_mono for s in self._symbols
        }
        # G1 断线窗回填：同 (last_ts, new_ts) 洞每 key 只试一次（休市等合法洞防刷 REST）
        self._gap_attempted: dict[str, tuple] = {}
        # G1：补拉瞬断失败的挂起洞 (lo, hi, mono)，节流重试直至成功/源返空
        self._gap_pending: dict[str, tuple] = {}

    # ─── 生命周期 ───

    def start(self) -> None:
        """预热历史 bars + 订阅 WS + 启动 WS（全部在 loop 线程，阻塞至就绪）。"""
        if self._running:
            return
        self._running = True
        future = asyncio.run_coroutine_threadsafe(self._async_start(), self._loop)
        future.result(timeout=self._start_timeout)
        logger.info(
            f"BinanceDataFeed started: symbols={self._symbols} periods={self._periods}"
        )

    def stop(self) -> None:
        """停止接收（唤醒 wait_update 让策略退出）；adapter 关闭由 backend.shutdown 负责。"""
        self._running = False
        self._event.set()
        for ev in self._events.values():   # M4-b：唤醒所有品种线程
            ev.set()
        logger.info("BinanceDataFeed stopped")

    async def _async_start(self) -> None:
        # ① REST 预热历史 bars
        for symbol in self._symbols:
            for period in self._periods:
                klines = await self._adapter.fetch_klines(
                    symbol, period, limit=self._bar_count
                )
                bars = [self._kline_to_bar(k) for k in klines]
                key = f"{symbol}/{period}"
                with self._lock:
                    self._bars[key] = bars
                    self._bump_version(key)
                logger.info(f"Loaded {len(bars)} bars for {key}")

        # ② 订阅 WS K 线（回调在 loop 线程触发）
        for symbol in self._symbols:
            for period in self._periods:
                await self._adapter.subscribe_kline(symbol, period, self._on_kline)

        # ③ 启动 WS
        await self._adapter.start_ws()
        # 预热 + 订阅 + WS 启动全部完成 → 重置 R5 心跳（避免冷启动 REST 下载时长预先老化）
        self._last_data_mono = time.monotonic()
        for s in self._last_symbol_mono:   # M6：品种级心跳同步重置
            self._last_symbol_mono[s] = self._last_data_mono

    # ─── WS 回调（async，loop 线程）───

    async def _on_kline(self, kline: Kline) -> None:
        if not self._running:
            return
        # 收到 WS 推送即视为连接存活（即使 bar/tick 值未变）→ 刷新 R5 心跳，与「休市/清淡」区分
        self._last_data_mono = time.monotonic()
        symbol = kline.symbol.upper()
        self._last_symbol_mono[symbol] = self._last_data_mono   # M6：品种级
        period = kline.timeframe
        key = f"{symbol}/{period}"
        bar = self._kline_to_bar(kline)
        # G1 断线窗回填：新 bar 时间戳跳变时先按 REST 范围补拉中段缺失再合并（见 _fill_gap）
        await self._fill_gap(key, symbol, period, bar)
        updated = False

        with self._lock:
            bars = self._bars.get(key)
            if not bars:
                self._bars[key] = [bar]
                self._bump_version(key)
                updated = True
            else:
                last = bars[-1]
                if bar["timestamp"] != last["timestamp"]:
                    bars.append(bar)
                    if len(bars) > self._bar_count:
                        bars[:] = bars[-self._bar_count:]
                    # M1：新 bar 出现 = 上一根收盘 → 基础 key 与 #bar 收盘 key 同时 bump
                    self._bump_bar_close(key)
                    updated = True
                elif (
                    bar["close"] != last["close"]
                    or bar["high"] != last["high"]
                    or bar["low"] != last["low"]
                ):
                    bars[-1] = bar
                    self._bump_version(key)
                    updated = True

            # 合成 tick（供 api.ticks()/latest_tick 消费；价格变动才 bump）
            price = kline.close
            old = self._ticks.get(symbol)
            self._ticks[symbol] = Tick(
                symbol=symbol,
                exchange=kline.exchange,
                timestamp=int(kline.timestamp),
                last_price=price,
                bid_price=price,
                bid_qty=Decimal("0"),
                ask_price=price,
                ask_qty=Decimal("0"),
                volume_24h=kline.volume,
            )
            if old is None or old.last_price != price:
                self._bump_version(f"tick/{symbol}")
                updated = True

        if updated:
            # M4-b：只唤醒本品种线程（+ 全局事件供 feed 级 wait_update）
            self._event_for(symbol).set()
            self._event.set()

    async def _fill_gap(self, key: str, symbol: str, period: str, bar: dict) -> None:
        """G1 断线窗回填：新 bar 与本地末根跳变（>1 周期）⇒ 断线窗中段缺 bar。

        按 REST 范围补拉开区间并合并：zigzag 等结构型指标依赖窗内 K 线根数与
        极值点，缺 bar 是结构错误而非仅指标偏差。约束：
          - 同一 (lo, hi) 洞源返空（休市合法洞）记 attempted 不重试；
          - 补拉抛异常（REST 瞬断）记 pending，节流 _GAP_RETRY_SEC 后随后续推送重试；
          - REST await 在锁外，不阻塞策略线程读缓存；
          - 仅 bump 基础 key：数组消费方看到补回；不补发历史收盘事件（#bar），
            防收盘驱动策略在恢复后对已收盘旧 bar 补发陈旧信号。
        """
        pms = TF_SECONDS.get(period, 60) * 1000
        with self._lock:
            bars = self._bars.get(key) or []
            last_ts = bars[-1]["timestamp"] if bars else 0
        lo = hi = 0
        pend = self._gap_pending.get(key)
        if pend is not None and time.monotonic() - pend[2] >= self._GAP_RETRY_SEC:
            lo, hi, _ = self._gap_pending.pop(key)   # 上次补拉瞬断失败的洞：节流重试
        elif last_ts and bar["timestamp"] > last_ts + pms:
            if self._gap_attempted.get(key) == (last_ts, bar["timestamp"]):
                return   # 同一洞已试过且源也缺：不刷 REST
            self._gap_attempted[key] = (last_ts, bar["timestamp"])
            lo, hi = last_ts, bar["timestamp"]
        else:
            return   # 连续或未收盘 bar 更新：无洞
        try:
            miss = await self._adapter.fetch_klines(
                symbol, period, start_time=lo + pms, end_time=hi - pms,
            )
        except Exception as e:
            logger.warning(f"G1 gap fill failed {key}: {e}")
            self._gap_pending[key] = (lo, hi, time.monotonic())
            return
        missing = sorted(
            (self._kline_to_bar(k) for k in (miss or [])),
            key=lambda b: b["timestamp"],
        )
        missing = [b for b in missing if lo < b["timestamp"] < hi]
        if not missing:
            self._gap_attempted[key] = (lo, hi)   # 源也缺（休市等合法洞）：放弃重试
            return
        with self._lock:
            bars = self._bars.get(key)
            if not bars or bars[0]["timestamp"] > lo:
                return   # 洞已滑出缓存窗口（裁剪）：无需合并
            have = {b["timestamp"] for b in bars}
            add = [b for b in missing if b["timestamp"] not in have]
            if not add:
                return
            bars.extend(add)
            bars.sort(key=lambda b: b["timestamp"])
            if len(bars) > self._bar_count:
                bars[:] = bars[-self._bar_count:]
            self._bump_version(key)
        logger.info(f"G1 gap filled {key}: +{len(add)} bars ({lo}~{hi})")

    # ─── DataFeedProtocol 实现 ───

    def latest_tick(self, symbol: str) -> Optional[Tick]:
        with self._lock:
            return self._ticks.get(symbol.upper())

    def latest_bars(self, symbol: str, period: str, count: int = 200) -> List[dict]:
        key = f"{symbol.upper()}/{period}"
        with self._lock:
            bars = self._bars.get(key, [])
            return bars[-count:] if len(bars) > count else list(bars)

    def wait_update(self, deadline: Optional[float] = None) -> bool:
        """阻塞等待新数据（feed 级，symbol=None → 全局事件；单线程直调如单测）。

        多品种实盘请经 KqApi.wait_update（per-api 快照 + per-symbol 事件，M4-a/M4-b）。
        deadline=超时秒数（None=无限）。True=有更新, False=超时/停止。
        """
        got, snap = self.wait_and_snapshot(deadline, symbol=None)
        with self._lock:
            self._snapshot_versions = snap
        return got

    def wait_and_snapshot(
        self, deadline: float | None = None, symbol: str | None = None,
    ) -> tuple[bool, dict[str, int]]:
        """等待新数据并返回 (是否有更新, 等待起始的版本快照)。

        M3：clear → snapshot → wait（clear 先行，避免 snapshot 与 clear 之间的
        bump+set 被 clear 吞掉而延迟一整轮）。M4-a：快照返回调用方（KqApi 各自持有）。
        M4-b：等待品种级事件。
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
        key = self._resolve_key(obj, field)
        if key is None:
            return False
        with self._lock:
            current = self._versions.get(key, 0)
            snapshot = self._snapshot_versions.get(key, 0)
            return current != snapshot

    def now_ms(self) -> int:
        return int(time.time() * 1000)

    def seconds_since_update(self, symbol: str | None = None) -> float:
        """R5：距上次收到 WS 推送的秒数（心跳龄）。断线时持续增长，超阈即 degraded。

        M6：symbol 给定→该品种心跳龄（品种级看门狗）；None/未知品种→全局心跳龄。
        """
        if symbol is not None:
            last = self._last_symbol_mono.get(symbol.upper())
            if last is not None:
                return time.monotonic() - last
        return time.monotonic() - self._last_data_mono

    # ─── 工具 ───

    def _kline_to_bar(self, k: Kline) -> dict:
        """Kline → 标准 bar dict（与 Mt5DataFeed._row_to_bar 同形）"""
        return {
            "symbol": k.symbol.upper(),
            "period": k.timeframe,
            "timestamp": int(k.timestamp),
            "open": float(k.open),
            "high": float(k.high),
            "low": float(k.low),
            "close": float(k.close),
            "volume": float(k.volume),
        }

    def _bump_version(self, key: str) -> None:
        """版本号 +1（必须在 _lock 内调用）"""
        self._versions[key] = self._versions.get(key, 0) + 1

    def _bump_bar_close(self, key: str) -> None:
        """新 bar 收盘：同时 bump 基础 key 与收盘专用 `#bar` key（M1）。必须在 _lock 内调用。"""
        self._bump_version(key)
        self._bump_version(key + _BAR_KEY_SUFFIX)

    def _resolve_key(self, obj: Any, field: str | None = None) -> str | None:
        """对象 → 版本 key。M1：field ∈ 收盘字段时映射到 `{base}#bar`（与实盘同构）。"""
        base = self._base_key(obj)
        if base is None:
            return None
        if field in _BAR_CLOSE_FIELDS and not base.startswith("tick/"):
            return base + _BAR_KEY_SUFFIX
        return base

    def _base_key(self, obj: Any) -> str | None:
        if isinstance(obj, Tick):
            return f"tick/{obj.symbol}"
        if isinstance(obj, str):
            return obj
        if isinstance(obj, list) and obj and isinstance(obj[0], dict):
            bar = obj[0]
            if "symbol" in bar and "period" in bar:
                return f"{bar['symbol']}/{bar['period']}"
        if isinstance(obj, dict) and "symbol" in obj and "period" in obj:
            return f"{obj['symbol']}/{obj['period']}"
        return None

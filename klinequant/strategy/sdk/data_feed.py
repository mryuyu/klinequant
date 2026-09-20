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


class Mt5DataFeed:
    """MT5 数据源（实现 DataFeedProtocol）"""

    def __init__(
        self,
        driver: Mt5Api,
        symbols: List[str],
        periods: List[str],
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        bar_count: int = 300,
    ):
        """
        Args:
            driver: Mt5Api 共享实例
            symbols: 订阅品种列表
            periods: 订阅周期列表（如 ["1m", "1h"]）
            poll_interval: 轮询间隔（秒）
            bar_count: 每次拉取的 bar 数量
        """
        self._driver = driver
        self._symbols = [s.upper() for s in symbols]
        self._periods = periods
        self._poll_interval = poll_interval
        self._bar_count = bar_count

        # 数据存储
        self._ticks: Dict[str, Tick] = {}           # symbol → latest tick
        self._bars: Dict[str, List[dict]] = {}      # "SYMBOL/period" → bars
        self._lock = threading.Lock()

        # 版本号（is_changing 用）
        self._versions: Dict[str, int] = {}         # key → version
        self._snapshot_versions: Dict[str, int] = {}  # 上次 wait_update 时的快照

        # Event 通知
        self._event = threading.Event()
        self._running = False
        self._thread: Optional[threading.Thread] = None

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
        self._event.set()  # 唤醒 wait_update
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

    def wait_update(self, deadline: Optional[float] = None) -> bool:
        """阻塞等待新数据。

        Args:
            deadline: 超时秒数（None=无限等待）

        Returns: True=有更新, False=超时
        """
        if not self._running:
            return False

        # 快照当前版本号（is_changing 的比对基线 = 本轮等待开始时的版本）
        with self._lock:
            self._snapshot_versions = dict(self._versions)

        self._event.clear()
        got = self._event.wait(timeout=deadline)

        # 注意：此处不再覆盖 snapshot。
        # is_changing 需比对“等待期间是否 bump” = 当前 versions vs 等待前 snapshot。
        # 若等待后再覆盖 snapshot，is_changing 将永远 False（策略收不到变化，不下单）。

        return got and self._running

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

    # ─── 内部轮询 ───

    def _poll_loop(self) -> None:
        """后台轮询主循环"""
        # 初始加载 bars
        self._load_initial_bars()

        while self._running:
            try:
                updated = False

                # 轮询 ticks
                for symbol in self._symbols:
                    tick = self._poll_tick(symbol)
                    if tick:
                        updated = True

                # 轮询 bars（仅最新一根）
                for symbol in self._symbols:
                    for period in self._periods:
                        if self._poll_bars(symbol, period):
                            updated = True

                if updated:
                    self._event.set()

            except Exception as e:
                logger.error(f"DataFeed poll error: {e}", exc_info=True)

            time.sleep(self._poll_interval)

    def _load_initial_bars(self) -> None:
        """启动时加载历史 bars"""
        for symbol in self._symbols:
            # 确保品种已选中
            self._driver.symbol_select(symbol, True)
            for period in self._periods:
                tf_const = TIMEFRAME_MAP.get(period)
                if not tf_const:
                    logger.warning(f"Unknown period: {period}")
                    continue
                rows = self._driver.copy_rates_from_pos(symbol, tf_const, 0, self._bar_count)
                if rows:
                    key = f"{symbol}/{period}"
                    bars = [self._row_to_bar(r, symbol, period) for r in rows]
                    with self._lock:
                        self._bars[key] = bars
                        self._bump_version(key)
                    logger.info(f"Loaded {len(bars)} bars for {key}")

    def _poll_tick(self, symbol: str) -> bool:
        """轮询单个品种 tick，返回是否有更新"""
        raw = self._driver.symbol_info_tick(symbol)
        if raw is None:
            return False

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
                bars.append(new_bar)
                # 保持长度限制
                if len(bars) > self._bar_count:
                    bars[:] = bars[-self._bar_count:]
                self._bump_version(key)
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

    def _resolve_key(self, obj: Any, field: Optional[str] = None) -> Optional[str]:
        """将对象解析为版本 key"""
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

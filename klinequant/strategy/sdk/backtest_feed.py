"""BacktestDataFeed — 回测数据源（实现 DataFeedProtocol）

回放历史 K 线，与实盘 Mt5DataFeed 暴露同一组接口，使同一份 strategy(api)
无需修改即可在回测中运行（回测/实盘同构）。

关键契约：
  - wait_update()：每调用一次推进一根 bar（同步、瞬时、无真实等待）；
    数据耗尽返回 False（策略循环自然退出）。
  - latest_bars()：只返回到「当前 index」为止的 bar，绝不暴露未来数据
    （反 look-ahead）。
  - now_ms()：随回放推进（= 当前 bar 时间戳），而非真实墙钟时间。

多周期同构：所有 (symbol,period) 的 bar 收盘事件汇成一条统一时间轴，wait_update
每步派发一个事件（timestamp 升序，同一时刻大周期先于小周期，1h 先于 15m）；单周期
单品种时退化为逐根回放（与旧行为完全一致）。
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

from gateway.market_sources.mt5_driver import TF_SECONDS
from protocol.types import Tick

logger = logging.getLogger(__name__)

# M1：收盘事件专用版本 key 后缀 + 触发字段名（与 data_feed/binance_feed 同构）。
_BAR_KEY_SUFFIX = "#bar"
_BAR_CLOSE_FIELDS = frozenset({"timestamp", "time", "datetime"})


class BacktestDataFeed:
    """回测数据源（历史 bar 回放）"""

    def __init__(
        self,
        bars: Dict[str, Any],
        period: Optional[str] = None,
        on_bar: Optional[Callable[[int], None]] = None,
        *,
        periods: Optional[List[str]] = None,
    ):
        """
        Args:
            bars: 单周期 {symbol: [bar dict, ...]}；或多周期（periods 给定时）
                  {symbol: {period: [bar dict, ...]}}。bar dict 需含
                  timestamp/open/high/low/close/volume（与 Mt5DataFeed 同形）。
            period: 单周期驱动周期（如 "1m"）。periods 给定时可省（取 periods[0] 为主）。
            on_bar: 每推进一根 bar 后的回调（BacktestExecutor 用于 mark-to-market）。
            periods: 多周期回放（拓扑 Z 同构）。给定时 bars 须为嵌套结构，回测按
                **统一时间轴**逐 bar 收盘事件派发；同一 timestamp 下**周期从大到小**
                （1h 先于 15m），保证小周期读到大周期最新结论（见 wait_and_snapshot）。
        """
        if periods is not None:
            self._periods: List[str] = list(periods)
            if not self._periods:
                raise ValueError("BacktestDataFeed: periods 不能为空")
            # 多周期：{symbol: {period: [bars]}}（缺某周期则该品种跳过该周期）
            self._bars: Dict[str, Dict[str, List[dict]]] = {
                s.upper(): {p: list(bars[s][p]) for p in self._periods if p in bars[s]}
                for s in bars
            }
        else:
            if period is None:
                raise ValueError("BacktestDataFeed 需要 period（单周期）或 periods（多周期）")
            self._periods = [period]
            self._bars = {s.upper(): {period: list(b)} for s, b in bars.items()}
        self._period = period or self._periods[0]
        self._symbols = list(self._bars.keys())
        if not self._symbols:
            raise ValueError("BacktestDataFeed requires at least one symbol")
        self._main = self._symbols[0]
        self._on_bar = on_bar

        # 统一时间轴：每个 (symbol, period, bar) 收盘事件一项，排序键
        #   (timestamp ASC, -period_seconds, symbol)——同一时刻大周期先于小周期。
        #   单周期单品种时退化为「按 timestamp 逐根」，与旧行为完全一致。
        self._timeline: List[Tuple[int, int, str, str, int]] = []
        for sym, per_bars in self._bars.items():
            for per, blist in per_bars.items():
                psec = TF_SECONDS.get(per, 60)
                for i, b in enumerate(blist):
                    self._timeline.append((int(b["timestamp"]), -psec, sym, per, i))
        self._timeline.sort()
        self._n = len(self._timeline)
        # 每 (symbol,period) 已揭示到的 bar index（-1=未揭示）；latest_bars 据此反 look-ahead
        self._ptr: Dict[Tuple[str, str], int] = {
            (sym, per): -1 for sym, pb in self._bars.items() for per in pb
        }
        # 每 symbol 最近揭示的 bar（open/close/now_ms 取价基准；耗尽后保留末根供清仓）
        self._cur: Dict[str, dict] = {}
        self._step = -1

        # 版本号（is_changing 用，与实盘 feed 同机制）
        self._versions: Dict[str, int] = {}
        self._snapshots: Dict[str, int] = {}

    # ─── 回放控制 ───

    def reset(self) -> None:
        """回到起点（多次回测复用同一数据时用）"""
        self._step = -1
        for k in self._ptr:
            self._ptr[k] = -1
        self._cur.clear()
        self._versions.clear()
        self._snapshots.clear()

    def set_on_bar(self, cb: Optional[Callable[[int], None]]) -> None:
        """设置逐 bar 回调（BacktestExecutor 注册 mark-to-market）"""
        self._on_bar = cb

    @property
    def index(self) -> int:
        """当前时间轴步（单周期单品种时 = 已揭示 bar index，与旧行为一致）。"""
        return self._step

    @property
    def n_bars(self) -> int:
        """时间轴总事件数（单周期单品种 = bar 数；多周期 = 各 series bar 数之和）。"""
        return self._n

    @property
    def symbols(self) -> List[str]:
        return list(self._symbols)

    # ─── DataFeedProtocol 实现 ───

    def wait_update(self, deadline: Optional[float] = None) -> bool:
        """推进一根 bar（feed 级，单线程直调如单测）。数据耗尽返回 False。

        多品种实盘请经 KqApi.wait_update（per-api 快照，M4-a）；回测单线程顺序
        回放，wait_update 委托 wait_and_snapshot 后存 feed 级快照供直调 is_changing。
        """
        got, snap = self.wait_and_snapshot(deadline, symbol=None)
        if got:
            self._snapshots = snap
        return got

    def wait_and_snapshot(
        self, deadline: float | None = None, symbol: str | None = None,
    ) -> tuple[bool, dict[str, int]]:
        """推进一根 bar 并返回 (是否推进, 推进前的版本快照)。

        回测同步推进：快照基线须在 bump 之前（与实盘 wait_and_snapshot 的
        clear→snapshot→wait 语义一致——等待期间发生的变化才算 changing）。
        symbol 参数忽略（回测单线程顺序回放，所有品种同刻推进）。
        """
        if self._step + 1 >= self._n:
            return False, {}
        snap = dict(self._versions)
        self._step += 1
        _ts, _negpsec, sym, per, bar_idx = self._timeline[self._step]
        self._ptr[(sym, per)] = bar_idx
        self._cur[sym] = self._bars[sym][per][bar_idx]
        # M1：回测每根皆收盘 bar → 基础 key 与 #bar 收盘 key 同时 bump（仅本事件周期）
        self._bump(f"{sym}/{per}")
        self._bump(f"{sym}/{per}{_BAR_KEY_SUFFIX}")
        self._bump(f"tick/{sym}")
        if self._on_bar is not None:
            self._on_bar(self._step)
        return True, snap

    def resolve_key(self, obj: Any, field: str | None = None) -> str | None:
        """对象 → 版本 key（公开包装，KqApi.is_changing 用；含 M1 field 语义）。"""
        return self._resolve_key(obj, field)

    def current_version(self, key: str) -> int:
        """某版本 key 的当前版本号（KqApi.is_changing 比对用，M4-a）。"""
        return self._versions.get(key, 0)

    def latest_bars(self, symbol: str, period: str, count: int = 200) -> List[dict]:
        """返回该 (symbol,period) 截至当前已揭示的 bar 序列（含当前，无未来数据）。

        多周期：各 period 独立按自身指针截取（未揭示的未来 bar 不可见，反 look-ahead）。
        """
        blist = self._bars.get(symbol.upper(), {}).get(period, [])
        end = self._ptr.get((symbol.upper(), period), -1) + 1
        if end <= 0:
            return []
        start = max(0, end - count)
        return list(blist[start:end])

    def latest_tick(self, symbol: str) -> Optional[Tick]:
        """当前 bar 的收盘价合成 tick（回测无逐笔 tick）"""
        if self._step < 0:
            return None
        b = self._cur.get(symbol.upper())
        if not b:
            return None
        px = Decimal(str(b["close"]))
        return Tick(
            symbol=symbol.upper(),
            exchange="mt5",
            timestamp=int(b["timestamp"]),
            last_price=px,
            bid_price=px,
            bid_qty=Decimal("0"),
            ask_price=px,
            ask_qty=Decimal("0"),
            volume_24h=Decimal("0"),
        )

    def is_changing(self, obj: Any, field: Optional[str] = None) -> bool:
        """自上次 wait_update 以来是否变化（回测每根 bar 都 bump → 恒 True）"""
        key = self._resolve_key(obj, field)
        if key is None:
            return False
        return self._versions.get(key, 0) != self._snapshots.get(key, 0)

    def now_ms(self) -> int:
        """当前回放时间（主品种最近揭示 bar 的 timestamp，ms；随时间轴单调不减）"""
        if self._step < 0:
            return 0
        b = self._cur.get(self._main)
        return int(b["timestamp"]) if b else 0

    def seconds_since_update(self, symbol: str | None = None) -> float:
        """R5：回测无真实断线（数据由回放器同步逐根推进），恒返 0.0（永不 stale）。

        M6：symbol 参仅为对齐 DataFeedProtocol（品种级看门狗），回测无断线故忽略。
        """
        return 0.0

    # ─── 执行器取价接口 ───

    def open_price(self, symbol: str) -> Optional[float]:
        """当前 bar 开盘价（市价单成交价基准 = 信号 bar 收盘后的下一根开盘）"""
        if self._step < 0:
            return None
        b = self._cur.get(symbol.upper())
        return float(b["open"]) if b else None

    def close_price(self, symbol: str) -> Optional[float]:
        """当前 bar 收盘价（mark-to-market 用）"""
        if self._step < 0:
            return None
        b = self._cur.get(symbol.upper())
        return float(b["close"]) if b else None

    # ─── 工具 ───

    def _bump(self, key: str) -> None:
        self._versions[key] = self._versions.get(key, 0) + 1

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

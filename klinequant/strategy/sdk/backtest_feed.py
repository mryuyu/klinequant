"""BacktestDataFeed — 回测数据源（实现 DataFeedProtocol）

回放历史 K 线，与实盘 Mt5DataFeed 暴露同一组接口，使同一份 strategy(api)
无需修改即可在回测中运行（回测/实盘同构）。

关键契约：
  - wait_update()：每调用一次推进一根 bar（同步、瞬时、无真实等待）；
    数据耗尽返回 False（策略循环自然退出）。
  - latest_bars()：只返回到「当前 index」为止的 bar，绝不暴露未来数据
    （反 look-ahead）。
  - now_ms()：随回放推进（= 当前 bar 时间戳），而非真实墙钟时间。

时间线长度取所有品种 bar 数的最小值，保证多品种对齐回放。
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

from protocol.types import Tick

logger = logging.getLogger(__name__)


class BacktestDataFeed:
    """回测数据源（历史 bar 回放）"""

    def __init__(
        self,
        bars: Dict[str, List[dict]],
        period: str,
        on_bar: Optional[Callable[[int], None]] = None,
    ):
        """
        Args:
            bars: {symbol: [bar dict, ...]}，bar dict 需含
                  timestamp/open/high/low/close/volume（与 Mt5DataFeed 同形）
            period: 驱动周期（如 "1m"）
            on_bar: 每推进一根 bar 后的回调（BacktestExecutor 用于 mark-to-market）
        """
        self._bars: Dict[str, List[dict]] = {s.upper(): list(b) for s, b in bars.items()}
        self._period = period
        self._symbols = list(self._bars.keys())
        if not self._symbols:
            raise ValueError("BacktestDataFeed requires at least one symbol")
        self._main = self._symbols[0]
        # 时间线长度 = 各品种 bar 数的最小值（对齐回放）
        self._n = min(len(b) for b in self._bars.values())
        self._index = -1
        self._on_bar = on_bar

        # 版本号（is_changing 用，与实盘 feed 同机制）
        self._versions: Dict[str, int] = {}
        self._snapshots: Dict[str, int] = {}

    # ─── 回放控制 ───

    def reset(self) -> None:
        """回到起点（多次回测复用同一数据时用）"""
        self._index = -1
        self._versions.clear()
        self._snapshots.clear()

    def set_on_bar(self, cb: Optional[Callable[[int], None]]) -> None:
        """设置逐 bar 回调（BacktestExecutor 注册 mark-to-market）"""
        self._on_bar = cb

    @property
    def index(self) -> int:
        return self._index

    @property
    def n_bars(self) -> int:
        return self._n

    @property
    def symbols(self) -> List[str]:
        return list(self._symbols)

    def _clamp_index(self) -> int:
        """价格查询用的安全 index（清仓发生在数据耗尽后，需回落到最后一根）"""
        return max(0, min(self._index, self._n - 1))

    # ─── DataFeedProtocol 实现 ───

    def wait_update(self, deadline: Optional[float] = None) -> bool:
        """推进一根 bar。数据耗尽返回 False（忽略 deadline，回测无真实等待）。"""
        if self._index + 1 >= self._n:
            return False
        # 快照基线须在 bump 之前（与实盘语义一致：等待期间发生的变化才算 changing）
        self._snapshots = dict(self._versions)
        self._index += 1
        for s in self._symbols:
            self._bump(f"{s}/{self._period}")
            self._bump(f"tick/{s}")
        if self._on_bar is not None:
            self._on_bar(self._index)
        return True

    def latest_bars(self, symbol: str, period: str, count: int = 200) -> List[dict]:
        """返回截至当前 index 的 bar 序列（含当前 bar，无未来数据）"""
        bars = self._bars.get(symbol.upper(), [])
        end = self._index + 1
        if end <= 0:
            return []
        start = max(0, end - count)
        return list(bars[start:end])

    def latest_tick(self, symbol: str) -> Optional[Tick]:
        """当前 bar 的收盘价合成 tick（回测无逐笔 tick）"""
        if self._index < 0:
            return None
        bars = self._bars.get(symbol.upper())
        if not bars:
            return None
        b = bars[self._clamp_index()]
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
        """当前回放时间（主品种当前 bar 时间戳，ms）"""
        if self._index < 0:
            return 0
        bars = self._bars.get(self._main, [])
        if not bars:
            return 0
        return int(bars[self._clamp_index()]["timestamp"])

    # ─── 执行器取价接口 ───

    def open_price(self, symbol: str) -> Optional[float]:
        """当前 bar 开盘价（市价单成交价基准 = 信号 bar 收盘后的下一根开盘）"""
        if self._index < 0:
            return None
        bars = self._bars.get(symbol.upper())
        if not bars:
            return None
        return float(bars[self._clamp_index()]["open"])

    def close_price(self, symbol: str) -> Optional[float]:
        """当前 bar 收盘价（mark-to-market 用）"""
        if self._index < 0:
            return None
        bars = self._bars.get(symbol.upper())
        if not bars:
            return None
        return float(bars[self._clamp_index()]["close"])

    # ─── 工具 ───

    def _bump(self, key: str) -> None:
        self._versions[key] = self._versions.get(key, 0) + 1

    def _resolve_key(self, obj: Any, field: Optional[str] = None) -> Optional[str]:
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

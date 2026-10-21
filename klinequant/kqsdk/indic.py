"""SDK 声明式指标接口（Phase 1：api.INDIC() 接入 IndicatorEngine）

``api.INDIC()`` → :class:`IndicApi`；``indic.macd(fast, slow, m)`` 等声明指标，
计算全部下沉进程内 :class:`IndicatorEngine`——与 gateway / 回测四端**同源同参**
（共用同一全局 ``IndicatorRegistry``，同 (指标名, 参数组合) 计算结果完全一致）。
返回 :class:`IndicatorView` **活视图**：每次访问现读引擎有效序列，随 runner 桥接的
``engine.update_kline`` 快照法增量自动推进，策略无需重取。

设计约束（对齐《SDK 阶段实施规划 v1.3》Phase 1）：
  - 活视图引用：``view.dif[-1]`` / 解包 ``dif, dea, hist = view`` 取标量（信号比较），
    ``FieldView`` 整体可迭代为序列（后续 api.plot 直接消费），``is_changing(view)`` 可用
    （KqApi 经 ``_kq_bar_key`` 映射到底层 bar 版本 key）；
  - 复用既有引擎：``ensure_indicator``（幂等注册）+ ``warmup``（bars→df 预热）+
    ``update_kline``（快照法增量，新 ts 隐式提交 / 同 ts 幂等，forming bar 盘中自动重算）；
  - 多品种线程安全：拓扑 Z 下每品种一线程共享同一 engine，所有 engine 调用经 runner
    注入的同一把锁串行化（品种间并行、engine 串行推进，量级小可接受）；
  - SDK 不耦合 gateway：bars→DataFrame / Kline 桥接在本模块内实现（与
    ``gateway.indicator_service._bars_to_df`` 同形），策略轨与网关轨各自独立装配。
"""
from __future__ import annotations

import threading
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import polars as pl

# 导入即注册全部内置 + def 式自定义指标到全局注册表（IND-110），
# 确保 ensure_indicator/create 前注册表已就绪（与 gateway.state 同源）。
import core.indicator_engine.indicators  # noqa: F401
import custom_indicators  # noqa: F401
from core.indicator_engine.engine import IndicatorEngine
from protocol.types import Kline

__all__ = ["IndicApi", "IndicatorView", "FieldView"]


# ─── bars → 引擎数据结构桥接（SDK 侧，不依赖 gateway） ───


def bars_to_df(bars: list[dict]) -> pl.DataFrame:
    """feed bar dict 列表 → 引擎预热用 DataFrame（与 indicator_service._bars_to_df 同形）。"""
    return pl.DataFrame({
        "timestamp": [int(b["timestamp"]) for b in bars],
        "open": [float(b["open"]) for b in bars],
        "high": [float(b["high"]) for b in bars],
        "low": [float(b["low"]) for b in bars],
        "close": [float(b["close"]) for b in bars],
        "volume": [float(b.get("volume", 0.0)) for b in bars],
        "quote_volume": [0.0] * len(bars),
        "trade_count": [0] * len(bars),
        "is_closed": [bool(b.get("is_closed", True)) for b in bars],
    })


def bar_to_kline(bar: dict, symbol: str, exchange: str, timeframe: str) -> Kline:
    """单根 feed bar dict → 引擎增量推进用 Kline。"""
    return Kline(
        symbol=symbol,
        exchange=exchange,
        timeframe=timeframe,
        timestamp=int(bar["timestamp"]),
        open=Decimal(str(bar["open"])),
        high=Decimal(str(bar["high"])),
        low=Decimal(str(bar["low"])),
        close=Decimal(str(bar["close"])),
        volume=Decimal(str(bar.get("volume", 0))),
        quote_volume=Decimal("0"),
        trade_count=0,
        is_closed=bool(bar.get("is_closed", True)),
    )


def _scalar(x: Any) -> Any:
    """把参与比较的对象归约为标量：FieldView → 其最新值，其余原样。"""
    if isinstance(x, FieldView):
        return x.last
    return x


class FieldView:
    """指标单字段活视图：标量比较（信号）+ 序列迭代（绘图）。

    ``view[-1]`` 取最新值、``view[-2]`` 取上一根；``> < >= <= == !=`` 及 ``float()``
    均作用于**最新值**（预热未完成时最新值为 None，有序比较一律 False，视作无信号）。
    每次访问现读引擎序列，随增量推进自动更新（活视图，不缓存陈旧值）。
    """

    __slots__ = (
        "_engine", "_name", "_params", "_symbol", "_exchange", "_timeframe", "_field",
    )

    def __init__(
        self, engine: IndicatorEngine, name: str, params: dict[str, Any],
        symbol: str, exchange: str, timeframe: str, field: str,
    ):
        self._engine = engine
        self._name = name
        self._params = params
        self._symbol = symbol
        self._exchange = exchange
        self._timeframe = timeframe
        self._field = field

    # ─── 视图元信息 ───

    @property
    def name(self) -> str:
        return self._name

    @property
    def field(self) -> str:
        return self._field

    @property
    def params(self) -> dict[str, Any]:
        return dict(self._params)

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def timeframe(self) -> str:
        return self._timeframe

    @property
    def _kq_bar_key(self) -> str:
        """KqApi.is_changing 映射用：本视图跟随的底层 bar 版本 key。"""
        return f"{self._symbol}/{self._timeframe}"

    # ─── 活数据 ───

    def _series(self) -> list[dict[str, Any]]:
        return self._engine.get_series(
            self._name, self._params, self._symbol, self._exchange, self._timeframe
        )

    @property
    def values(self) -> list[Any]:
        """该字段的整段序列（预热完成后有效值；api.plot 序列通道消费）。"""
        return [item["values"].get(self._field) for item in self._series()]

    @property
    def timestamps(self) -> list[int]:
        return [item["timestamp"] for item in self._series()]

    @property
    def last(self) -> Any:
        """最新值（序列末位）；无有效值时 None。"""
        vals = self.values
        return vals[-1] if vals else None

    def __len__(self) -> int:
        return len(self._series())

    def __getitem__(self, idx):
        return self.values[idx]

    def __iter__(self) -> Iterator[Any]:
        return iter(self.values)

    def __float__(self) -> float:
        v = self.last
        if v is None:
            raise ValueError(f"{self._name}.{self._field} 尚无有效值（预热未完成）")
        return float(v)

    def __bool__(self) -> bool:
        return self.last is not None

    # ─── 标量比较（作用于最新值；None 视作无信号 → 有序比较 False） ───

    def __gt__(self, other):
        a, b = self.last, _scalar(other)
        return a is not None and b is not None and a > b

    def __lt__(self, other):
        a, b = self.last, _scalar(other)
        return a is not None and b is not None and a < b

    def __ge__(self, other):
        a, b = self.last, _scalar(other)
        return a is not None and b is not None and a >= b

    def __le__(self, other):
        a, b = self.last, _scalar(other)
        return a is not None and b is not None and a <= b

    def __eq__(self, other):
        a, b = self.last, _scalar(other)
        if a is None or b is None:
            return a is None and b is None
        return a == b

    def __ne__(self, other):
        return not self.__eq__(other)

    __hash__ = object.__hash__

    def __repr__(self) -> str:
        return f"FieldView({self._name}.{self._field}={self.last})"


class IndicatorView:
    """指标级活视图：字段访问 + 迭代解包 + 整段序列。

    - ``view.dif`` / ``view["DIF"]`` / ``view.field("DIF")`` → :class:`FieldView`；
    - ``dif, dea, hist = view`` → 按字段序解包为多个 FieldView（字段序取引擎有效序列
      的键序，如 MACD 为 DIF/DEA/HIST）；
    - ``view.series`` → ``[{"timestamp", "values"}, ...]``（整体传 api.plot 的序列源）；
    - ``is_changing(view)`` → KqApi 经 ``_kq_bar_key`` 映射到底层 bar 变化。
    """

    __slots__ = (
        "_engine", "_name", "_params", "_symbol", "_exchange", "_timeframe",
    )

    def __init__(
        self, engine: IndicatorEngine, name: str, params: dict[str, Any],
        symbol: str, exchange: str, timeframe: str,
    ):
        self._engine = engine
        self._name = name
        self._params = params
        self._symbol = symbol
        self._exchange = exchange
        self._timeframe = timeframe

    # ─── 视图元信息 ───

    @property
    def name(self) -> str:
        return self._name

    @property
    def params(self) -> dict[str, Any]:
        return dict(self._params)

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def exchange(self) -> str:
        return self._exchange

    @property
    def timeframe(self) -> str:
        return self._timeframe

    @property
    def ind_key(self) -> str:
        """计算契约 key=(指标名|参数)，与引擎/网关口径一致。"""
        return self._engine.ind_key(self._name, self._params)

    @property
    def _kq_bar_key(self) -> str:
        return f"{self._symbol}/{self._timeframe}"

    # ─── 活数据 ───

    @property
    def series(self) -> list[dict[str, Any]]:
        """整段有效序列（api.plot 序列通道消费）。"""
        return self._engine.get_series(
            self._name, self._params, self._symbol, self._exchange, self._timeframe
        )

    @property
    def fields(self) -> list[str]:
        """字段序（取引擎有效序列末位键序；无有效值时回落指标声明的 display_meta）。"""
        s = self.series
        if s:
            return list(s[-1]["values"].keys())
        for ind in self._engine.indicators_for(self._symbol, self._exchange, self._timeframe):
            if self._engine.ind_key(ind.name, ind.params) == self.ind_key:
                return list(ind.display_meta.get("fields", []))
        return []

    def field(self, name: str) -> FieldView:
        """按字段名取活视图（大小写不敏感）。"""
        target = name.upper()
        for f in self.fields:
            if f.upper() == target:
                return FieldView(
                    self._engine, self._name, self._params,
                    self._symbol, self._exchange, self._timeframe, f,
                )
        # 未匹配到已知字段仍返回视图（预热未完成时 fields 可能暂空），值取 None
        return FieldView(
            self._engine, self._name, self._params,
            self._symbol, self._exchange, self._timeframe, target,
        )

    def __getitem__(self, key: str) -> FieldView:
        if not isinstance(key, str):
            raise TypeError(
                "IndicatorView 仅支持按字段名索引（如 view['DIF']）；"
                "按位置取标量请用字段视图 view.dif[-1]"
            )
        return self.field(key)

    def __getattr__(self, name: str) -> FieldView:
        # 仅在常规属性查找失败时触发；保护 dunder/私有名，避免 hasattr 恒真
        if name.startswith("_"):
            raise AttributeError(name)
        fields = object.__getattribute__(self, "fields")
        if any(f.upper() == name.upper() for f in fields):
            return object.__getattribute__(self, "field")(name)
        raise AttributeError(
            f"{self._name} 无字段 {name!r}（可用：{fields}）"
        )

    def __iter__(self) -> Iterator[FieldView]:
        """按字段序迭代 FieldView（支持 ``a, b, c = view`` 解包）。"""
        return iter([self.field(f) for f in self.fields])

    def __len__(self) -> int:
        return len(self.series)

    def __repr__(self) -> str:
        return f"IndicatorView({self._name}{self._params} @{self._symbol}/{self._timeframe})"


class IndicApi:
    """声明式指标接口（``api.INDIC()`` 返回）。

    每个 KqApi（绑定一个 symbol + 一个驱动 period）持一个 IndicApi；声明的指标
    注册进共享 IndicatorEngine，计算全在后端，策略只读活视图结果。

    常用别名方法覆盖内置指标（macd/ma/ema/rsi/atr/boll/kdj/vwap），
    任意注册表指标（含自定义）走通用 :meth:`get`。
    """

    def __init__(
        self, engine: IndicatorEngine, feed: Any, symbol: str, exchange: str,
        timeframe: str, *, warmup_bars: int = 1000,
        lock: threading.Lock | None = None,
    ):
        self._engine = engine
        self._feed = feed
        self._symbol = symbol
        self._exchange = exchange
        self._timeframe = timeframe
        self._warmup_bars = warmup_bars
        # 共享锁：拓扑 Z 下多品种线程共用同一 engine，串行化所有 engine 调用
        self._lock = lock if lock is not None else threading.Lock()
        # ind_key → (name, params)：本 api 声明的全部指标（advance 增量推进依据）
        self._declared: dict[str, tuple[str, dict[str, Any]]] = {}
        # 已成功预热（拿到 bars）的 ind_key；冷启动无 bars 时留待 advance 重试
        self._warmed: set = set()

    # ─── 通用 + 别名声明 ───

    def get(self, name: str, **params: Any) -> IndicatorView:
        """声明任意注册表指标（含自定义），返回活视图。"""
        return self._declare(name, params)

    def macd(self, fast: int = 12, slow: int = 26, m: int = 9) -> IndicatorView:
        """MACD（字段序 DIF/DEA/HIST；``m`` 为信号线周期）。"""
        return self._declare(
            "MACD", {"fast_period": fast, "slow_period": slow, "signal_period": m}
        )

    def ma(self, period: int = 20) -> IndicatorView:
        return self._declare("MA", {"period": period})

    def ema(self, period: int = 20) -> IndicatorView:
        return self._declare("EMA", {"period": period})

    def rsi(self, period: int = 14) -> IndicatorView:
        return self._declare("RSI", {"period": period})

    def atr(self, period: int = 14) -> IndicatorView:
        return self._declare("ATR", {"period": period})

    def boll(self, period: int = 20, std_dev: float = 2.0) -> IndicatorView:
        return self._declare("BOLL", {"period": period, "std_dev": std_dev})

    def kdj(self, k_period: int = 9, d_period: int = 3, j_period: int = 3) -> IndicatorView:
        return self._declare(
            "KDJ", {"k_period": k_period, "d_period": d_period, "j_period": j_period}
        )

    def vwap(self, period: int = 20) -> IndicatorView:
        return self._declare("VWAP", {"period": period})

    # ─── 内部：声明 / 预热 / 增量推进 ───

    def _declare(self, name: str, params: dict[str, Any]) -> IndicatorView:
        ik = self._engine.ind_key(name, params)
        with self._lock:
            # 幂等注册：同 (name, params) 复用已有实例（四端同源同参）
            self._engine.ensure_indicator(
                name, params, self._symbol, self._exchange, self._timeframe
            )
            self._declared[ik] = (name, params)
            if ik not in self._warmed:
                self._warmup_locked(ik, name, params)
        return IndicatorView(
            self._engine, name, params, self._symbol, self._exchange, self._timeframe
        )

    def _warmup_locked(self, ik: str, name: str, params: dict[str, Any]) -> None:
        """用 feed 已加载 bars 预热单个指标（调用方须持锁）。无 bars 则跳过（advance 重试）。"""
        bars = self._feed.latest_bars(self._symbol, self._timeframe, self._warmup_bars)
        if not bars:
            return
        self._engine.warmup(
            self._symbol, self._exchange, self._timeframe, bars_to_df(bars), only_key=ik
        )
        self._warmed.add(ik)

    def advance(self) -> None:
        """runner 桥接：feed 最新 bar 有变化时推进 engine 增量（快照法，同 ts 幂等）。

        由 KqApi.wait_update 在每轮数据到达后调用。值变门控（比对引擎缓存末根
        ts/H/L/C）而非版本号，规避多线程共享 snapshot 竞争，且天然覆盖 forming bar
        盘中变动（close/high/low 变即重算）。冷启动未预热的指标在此补预热。
        """
        if not self._declared:
            return
        with self._lock:
            # ① 补预热：冷启动时声明早于 feed 初始 bars 就绪的指标
            pending = [ik for ik in self._declared if ik not in self._warmed]
            for ik in pending:
                name, params = self._declared[ik]
                self._warmup_locked(ik, name, params)

            # ② 增量推进：取最新 bar，值变才 update_kline（避免 tick-only 无谓重算）
            tail = self._feed.latest_bars(self._symbol, self._timeframe, 1)
            if not tail:
                return
            bar = tail[-1]
            cache = self._engine.get_kline_cache(
                self._symbol, self._exchange, self._timeframe
            )
            if cache is not None and len(cache) > 0 and self._same_last(cache, bar):
                return
            self._engine.update_kline(
                bar_to_kline(bar, self._symbol, self._exchange, self._timeframe)
            )

    @staticmethod
    def _same_last(cache: pl.DataFrame, bar: dict) -> bool:
        """引擎缓存末根与最新 bar 的 ts/H/L/C 是否完全一致（一致=无变化，跳过重算）。"""
        try:
            return (
                int(cache["timestamp"][-1]) == int(bar["timestamp"])
                and float(cache["high"][-1]) == float(bar["high"])
                and float(cache["low"][-1]) == float(bar["low"])
                and float(cache["close"][-1]) == float(bar["close"])
            )
        except (KeyError, IndexError):
            return False

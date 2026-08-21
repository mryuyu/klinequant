"""def 式指标 DSL 与图适配层（IND-110）

作者只写一个普通 Python 函数：

    from core.indicator_engine.graph import pyindicator, ema, shift

    @pyindicator(name="TRIX", pane="sub", range="zero_symmetric")
    def trix(close, period=12):
        e3 = ema(ema(ema(close, period), period), period)
        prev = shift(e3, 1)
        return {"TRIX": (e3 - prev) / prev * 100.0}

约定：
    - 形参名为 open/high/low/close/volume 的自动映射为行情输入
    - 其余带默认值的形参成为可调参数（default_params / 前端参数弹窗）
    - 返回单个 Node 或以字段名为键的 {str: Node} 字典
    - 函数体须为「直代码」：分支/循环可依赖参数，不可依赖行情数据；
      数据依赖的选择用 where(cond, a, b)

增量语义由原语节点内部解决（快照法处理未收盘 bar），作者零感知；
图指标一律 supports_incremental=True，引擎走 update_bar 增量路径。
"""
from __future__ import annotations

import inspect
import logging
from typing import Any, Callable, Dict, Optional, Type

import polars as pl

from core.indicator_engine.base import IndicatorBase
from core.indicator_engine.registry import get_registry

from .nodes import (
    AbsNode,
    CumSumNode,
    EmaNode,
    Graph,
    InputNode,
    MaximumNode,
    MinimumNode,
    Node,
    RollingMaxNode,
    RollingMinNode,
    RollingStdNode,
    ShiftNode,
    SmaNode,
    WhereNode,
    begin_trace,
    end_trace,
)

logger = logging.getLogger(__name__)

INPUT_COLUMNS = ("open", "high", "low", "close", "volume")


# ─── 原语函数（作者面） ───

def _node(x: Any) -> Node:
    return x if isinstance(x, Node) else _const(float(x))


def _const(v: float) -> Node:
    from .nodes import ConstNode
    return ConstNode(v)


def ema(x: Any, period: int) -> Node:
    """指数移动平均（种子=首个有效值，对齐 polars ewm_mean(adjust=False)）"""
    return EmaNode(_node(x), int(period))


def sma(x: Any, period: int) -> Node:
    """简单移动平均（对齐 polars rolling_mean）"""
    return SmaNode(_node(x), int(period))


def rolling_max(x: Any, period: int) -> Node:
    """滚动最高（对齐 polars rolling_max）"""
    return RollingMaxNode(_node(x), int(period))


def rolling_min(x: Any, period: int) -> Node:
    """滚动最低（对齐 polars rolling_min）"""
    return RollingMinNode(_node(x), int(period))


def rolling_std(x: Any, period: int) -> Node:
    """滚动标准差（对齐 polars rolling_std，ddof=1）"""
    return RollingStdNode(_node(x), int(period))


def shift(x: Any, n: int = 1) -> Node:
    """向前引用：n 根之前的值（对齐 polars shift）"""
    return ShiftNode(_node(x), int(n))


def cum_sum(x: Any) -> Node:
    """累计求和（null 按 0 处理，对齐 fill_null(0).cum_sum()）"""
    return CumSumNode(_node(x))


def maximum(a: Any, b: Any) -> Node:
    """逐点取大（任一输入 null → null）"""
    return MaximumNode(_node(a), _node(b))


def minimum(a: Any, b: Any) -> Node:
    """逐点取小（任一输入 null → null）"""
    return MinimumNode(_node(a), _node(b))


def where(cond: Any, a: Any, b: Any) -> Node:
    """三元选择：cond 为真取 a，否则取 b（cond 为 null 走 b，对齐 pl.when）"""
    return WhereNode(_node(cond), _node(a), _node(b))


def abs_(x: Any) -> Node:
    """绝对值"""
    return AbsNode(_node(x))


# ─── 图定义与指标适配 ───

class GraphDef:
    """def 式指标的静态定义：签名解析 + 追踪构图 + 元数据"""

    def __init__(
        self,
        fn: Callable,
        name: str,
        pane: str,
        range_: str,
        desc: str,
        min_periods_override: Optional[int],
    ):
        self.fn = fn
        self.name = name
        self.pane = pane
        self.range = range_
        self.desc = desc
        self.min_periods_override = min_periods_override

        sig = inspect.signature(fn)
        self.input_names = [
            p.name for p in sig.parameters.values() if p.name in INPUT_COLUMNS
        ]
        if not self.input_names:
            raise ValueError(
                f"指标函数 {fn.__name__} 必须至少接收一个行情输入形参"
                f"（{'/'.join(INPUT_COLUMNS)}）"
            )
        self.param_defaults: Dict[str, Any] = {}
        for p in sig.parameters.values():
            if p.name in INPUT_COLUMNS:
                continue
            if p.default is inspect.Parameter.empty:
                raise ValueError(
                    f"指标函数 {fn.__name__} 的参数 {p.name} 必须有默认值"
                    f"（非行情输入的形参即指标参数）"
                )
            self.param_defaults[p.name] = p.default

        # 定义期用默认参数试构图：推导输出字段与结构性 min_periods
        sample = self.build_graph(dict(self.param_defaults))
        self.fields = list(sample.outputs.keys())
        self.struct_min_periods = sample.min_periods

    def build_graph(self, params: Dict[str, Any]) -> Graph:
        """trace 一次函数调用构建计算图（params 覆盖默认参数）"""
        graph = Graph()
        begin_trace(graph)
        try:
            kwargs: Dict[str, Any] = {n: InputNode(n) for n in self.input_names}
            for k, default in self.param_defaults.items():
                kwargs[k] = params.get(k, default)
            out = self.fn(**kwargs)
        finally:
            end_trace()
        if isinstance(out, Node):
            graph.outputs = {self.name: out}
        elif isinstance(out, dict):
            for field, node in out.items():
                if not isinstance(node, Node):
                    raise ValueError(
                        f"指标 {self.name} 返回的字段 {field} 不是图节点"
                    )
                graph.outputs[str(field)] = node
        else:
            raise ValueError(
                f"指标 {self.name} 必须返回 Node 或 {{字段名: Node}} 字典"
            )
        return graph

    @property
    def min_periods(self) -> int:
        return self.min_periods_override or self.struct_min_periods

    @property
    def display_meta(self) -> Dict[str, Any]:
        return {
            "fields": self.fields,
            "range": self.range,
            "pane": self.pane,
            "desc": self.desc,
        }


class GraphIndicator(IndicatorBase):
    """计算图指标实例：一个 (指标, 参数组合) 契约对应一张图"""

    _gdef: GraphDef   # 由 make_class 注入

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        super().__init__(params)
        # 有效参数 = 函数默认值 + 请求参数（内部计算用；params 属性保持请求原值）
        self._eff_params: Dict[str, Any] = {
            **self._gdef.param_defaults, **(params or {})
        }
        self._graph = self._gdef.build_graph(self._eff_params)
        self._reset_runtime()

    def _reset_runtime(self) -> None:
        self._count = 0
        self._last_ts: Optional[int] = None
        self._snap: Optional[Any] = None

    @property
    def name(self) -> str:
        return self._gdef.name

    @property
    def min_periods(self) -> int:
        return self._gdef.min_periods

    @property
    def default_params(self) -> Dict[str, Any]:
        return dict(self._gdef.param_defaults)

    @property
    def supports_incremental(self) -> bool:
        return True

    @property
    def display_meta(self) -> Dict[str, Any]:
        return self._gdef.display_meta

    # ─── 批量路径（一致性核对/回测用；引擎实时路径走 update_bar） ───

    def _calculate(self, df: pl.DataFrame) -> pl.DataFrame:
        cols = [
            node.batch().alias(f"{self.name}_{field}")
            for field, node in self._graph.outputs.items()
        ]
        return df.with_columns(cols)

    # ─── 增量路径（快照法，IND-101 协议） ───

    def reset(self) -> None:
        super().reset()
        self._graph.reset()
        self._reset_runtime()

    def update_bar(
        self, bar: Dict[str, Any], is_closed: bool
    ) -> Optional[Dict[str, Any]]:
        ts = bar["timestamp"]
        if self._last_ts is not None and ts < self._last_ts:
            return None  # 乱序历史 bar：增量路径不处理
        # 快照 = 当前 bar 之前的状态：同 ts 重复推送恢复后重新应用（幂等）
        if ts != self._last_ts:
            self._snap = (self._graph.snapshot(), self._count)
        elif self._snap is not None:
            self._graph.restore(self._snap[0])
            self._count = self._snap[1]

        env = {c: bar.get(c) for c in INPUT_COLUMNS}
        values = self._graph.propagate(env)
        self._count += 1
        self._last_ts = ts

        if self._count >= self.min_periods:
            self._warmed_up = True
        if not self._warmed_up:
            return None
        if any(v is None for v in values.values()):
            return None  # 结构性预热未完成（或除零等奇异点）：不输出失真数据

        out = {k: float(v) for k, v in values.items()}
        self._last_values = out
        return out


def make_indicator_cls(gdef: GraphDef) -> Type[GraphIndicator]:
    """为图定义生成可注册的指标类（注册表按 name 管理）"""

    cls = type(
        f"Graph_{gdef.name}",
        (GraphIndicator,),
        {"_gdef": gdef},
    )
    return cls


def pyindicator(
    name: Optional[str] = None,
    pane: str = "sub",
    range: str = "unbounded",
    min_periods: Optional[int] = None,
    desc: str = "",
):
    """装饰器：把 def 函数注册为计算图指标

    Args:
        name: 注册名（缺省用函数名大写），须全局唯一
        pane: 'sub'（独立副图）/ 'main'（主图叠加）
        range: 值域类型 unbounded / bounded_0_100 / zero_symmetric / price
        min_periods: 预热根数覆盖（缺省按图结构自动推导）
        desc: 指标说明（前端选择面板展示）
    """

    def wrapper(fn: Callable) -> Callable:
        doc_first = (fn.__doc__ or "").strip().split("\n")[0]
        gdef = GraphDef(
            fn=fn,
            name=(name or fn.__name__).upper(),
            pane=pane,
            range_=range,
            desc=desc or doc_first,
            min_periods_override=min_periods,
        )
        get_registry().register(make_indicator_cls(gdef))
        logger.info(f"Registered graph indicator {gdef.name} from {fn.__name__}")
        return fn

    return wrapper

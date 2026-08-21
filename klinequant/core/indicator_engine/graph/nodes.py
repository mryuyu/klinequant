"""计算图节点库（IND-110）— 双语义原语

每个原语节点同时具备两套语义：
    - batch(): polars Expr（预热全量 / 回测 / 一致性核对路径）
    - incr():  O(1) 标量递推（实时路径），结果存于 _value

快照协议（与 IND-101 快照法同源）：snapshot()/restore() 保存/回滚节点内部
状态；未收盘 bar 同 timestamp 重复推送时先恢复快照再重新应用，保证幂等。

null 语义约定（与 polars 批量路径对齐）：
    - 输入为 null → 递推类节点（EMA）不推进状态、输出 null
    - 窗口类节点窗口内含 null → 输出 null（对齐 polars rolling 行为）
    - where(cond, a, b)：cond 为 null 走 otherwise 分支（对齐 pl.when）

节点必须在 trace 上下文内创建（dsl 层负责）；创建顺序即拓扑序
（依赖节点总是先于依赖它的节点被创建）。
"""
from __future__ import annotations

import math
from collections import deque
from typing import Any, Dict, List, Optional

import polars as pl

# ─── trace 上下文 ───

_TRACE_STACK: List["Graph"] = []


def begin_trace(graph: "Graph") -> None:
    _TRACE_STACK.append(graph)


def end_trace() -> "Graph":
    return _TRACE_STACK.pop()


def _current_graph() -> "Graph":
    if not _TRACE_STACK:
        raise RuntimeError(
            "图原语只能在 trace 上下文内调用（指标函数由框架驱动执行）"
        )
    return _TRACE_STACK[-1]


# ─── 计算图 ───

class Graph:
    """计算图：节点列表（创建序=拓扑序）+ 命名输出"""

    def __init__(self) -> None:
        self.nodes: List[Node] = []
        self.outputs: Dict[str, Node] = {}

    @property
    def min_periods(self) -> int:
        """结构性预热根数：所有输出路径 min_periods 的最大值"""
        if not self.outputs:
            return 1
        return max(max(n.min_periods for n in self.outputs.values()), 1)

    def propagate(self, env: Dict[str, Any]) -> Dict[str, Optional[float]]:
        """按拓扑序推进一根 bar，返回各输出字段的当根值"""
        for node in self.nodes:
            node.incr(env)
        return {k: node._value for k, node in self.outputs.items()}

    def snapshot(self) -> List[Any]:
        return [node.snapshot() for node in self.nodes]

    def restore(self, snaps: List[Any]) -> None:
        for node, snap in zip(self.nodes, snaps):
            node.restore(snap)

    def reset(self) -> None:
        for node in self.nodes:
            node.reset_state()


# ─── 节点基类 ───

class Node:
    """图节点基类

    子类须实现 batch()/incr()；有内部状态的还须实现
    snapshot()/restore()/reset_state()。
    """

    def __init__(self, inputs: List["Node"]):
        self.inputs = inputs
        self._value: Optional[float] = None
        _current_graph().nodes.append(self)

    # ── 批量语义 ──
    def batch(self) -> pl.Expr:
        raise NotImplementedError

    # ── 增量语义 ──
    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        raise NotImplementedError

    # ── 快照协议 ──
    def snapshot(self) -> Any:
        return None

    def restore(self, snap: Any) -> None:
        pass

    def reset_state(self) -> None:
        self._value = None

    @property
    def min_periods(self) -> int:
        return max([n.min_periods for n in self.inputs], default=1)

    # ── 运算符（构图语法糖） ──
    def __add__(self, other): return BinaryNode(self, _as_node(other), "+")
    def __radd__(self, other): return BinaryNode(_as_node(other), self, "+")
    def __sub__(self, other): return BinaryNode(self, _as_node(other), "-")
    def __rsub__(self, other): return BinaryNode(_as_node(other), self, "-")
    def __mul__(self, other): return BinaryNode(self, _as_node(other), "*")
    def __rmul__(self, other): return BinaryNode(_as_node(other), self, "*")
    def __truediv__(self, other): return BinaryNode(self, _as_node(other), "/")
    def __rtruediv__(self, other): return BinaryNode(_as_node(other), self, "/")
    def __neg__(self): return NegNode(self)
    def __abs__(self): return AbsNode(self)
    def __gt__(self, other): return CompareNode(self, _as_node(other), ">")
    def __ge__(self, other): return CompareNode(self, _as_node(other), ">=")
    def __lt__(self, other): return CompareNode(self, _as_node(other), "<")
    def __le__(self, other): return CompareNode(self, _as_node(other), "<=")

    # 比较运算符返回 BoolNode 后仍需保持节点可作为 dict key/判等
    __hash__ = object.__hash__

    def __eq__(self, other):  # type: ignore[override]
        return self is other


def _as_node(x: Any) -> Node:
    """标量自动包装为常量节点"""
    return x if isinstance(x, Node) else ConstNode(float(x))


# ─── 输入/常量 ───

class InputNode(Node):
    """行情输入（open/high/low/close/volume）"""

    def __init__(self, column: str):
        super().__init__([])
        self.column = column

    def batch(self) -> pl.Expr:
        return pl.col(self.column)

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        v = env.get(self.column)
        self._value = float(v) if v is not None else None
        return self._value

    @property
    def min_periods(self) -> int:
        return 1


class ConstNode(Node):
    """常量"""

    def __init__(self, value: float):
        super().__init__([])
        self.value = value

    def batch(self) -> pl.Expr:
        return pl.lit(self.value)

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        self._value = self.value
        return self._value

    @property
    def min_periods(self) -> int:
        return 0


# ─── 递推类原语 ───

class EmaNode(Node):
    """指数移动平均（对齐 polars ewm_mean(adjust=False, ignore_nulls=True)）

    首值=首个有效输入（种子）；输入 null 不推进状态、输出 null。
    """

    def __init__(self, x: Node, period: int):
        super().__init__([x])
        self.x = x
        self.period = period
        self.alpha = 2.0 / (period + 1)
        self._ema: Optional[float] = None

    def batch(self) -> pl.Expr:
        return self.x.batch().ewm_mean(
            alpha=self.alpha, adjust=False, ignore_nulls=True
        )

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        v = self.x._value
        if v is None:
            self._value = None
            return None
        if self._ema is None:
            self._ema = v
        else:
            self._ema = self.alpha * v + (1 - self.alpha) * self._ema
        self._value = self._ema
        return self._value

    def snapshot(self) -> Any:
        return self._ema

    def restore(self, snap: Any) -> None:
        self._ema = snap

    def reset_state(self) -> None:
        super().reset_state()
        self._ema = None

    @property
    def min_periods(self) -> int:
        return self.x.min_periods


class CumSumNode(Node):
    """累计求和（对齐 fill_null(0).cum_sum()；null 按 0 处理）"""

    def __init__(self, x: Node):
        super().__init__([x])
        self.x = x
        self._total = 0.0
        self._seen = False

    def batch(self) -> pl.Expr:
        return self.x.batch().fill_null(0.0).cum_sum()

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        v = self.x._value
        self._total += v if v is not None else 0.0
        self._seen = True
        self._value = self._total
        return self._value

    def snapshot(self) -> Any:
        return (self._total, self._seen)

    def restore(self, snap: Any) -> None:
        self._total, self._seen = snap

    def reset_state(self) -> None:
        super().reset_state()
        self._total = 0.0
        self._seen = False


# ─── 窗口类原语 ───

class _WindowNode(Node):
    """滚动窗口基类：维护定长窗口 + 窗口内 null 计数"""

    def __init__(self, x: Node, period: int):
        super().__init__([x])
        self.x = x
        self.period = period
        self._win: deque = deque()
        self._none_count = 0

    def _push(self, env: Dict[str, Any]) -> Optional[float]:
        v = self.x._value
        if len(self._win) == self.period:
            popped = self._win.popleft()
            if popped is None:
                self._none_count -= 1
        self._win.append(v)
        if v is None:
            self._none_count += 1
        return v

    def _ready(self) -> bool:
        return len(self._win) == self.period and self._none_count == 0

    def snapshot(self) -> Any:
        return (list(self._win), self._none_count)

    def restore(self, snap: Any) -> None:
        win, none_count = snap
        self._win = deque(win)
        self._none_count = none_count

    def reset_state(self) -> None:
        super().reset_state()
        self._win = deque()
        self._none_count = 0

    @property
    def min_periods(self) -> int:
        return self.x.min_periods + self.period - 1


class SmaNode(_WindowNode):
    """简单移动平均（对齐 polars rolling_mean，窗口内含 null 输出 null）"""

    def __init__(self, x: Node, period: int):
        super().__init__(x, period)
        self._sum = 0.0

    def batch(self) -> pl.Expr:
        return self.x.batch().rolling_mean(window_size=self.period)

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        was_clean = self._ready()
        popped = self._win[0] if len(self._win) == self.period else None
        v = self._push(env)
        if not self._ready():
            self._value = None
            return None
        if was_clean and popped is not None and v is not None:
            self._sum += v - popped   # 稳态 O(1) 增量维护
        else:
            self._sum = sum(self._win)   # 首次满窗/刚离开含 null 状态：重建一次
        self._value = self._sum / self.period
        return self._value

    def snapshot(self) -> Any:
        return (super().snapshot(), self._sum)

    def restore(self, snap: Any) -> None:
        base, s = snap
        super().restore(base)
        self._sum = s

    def reset_state(self) -> None:
        super().reset_state()
        self._sum = 0.0


class _RollingExtremumNode(_WindowNode):
    """滚动极值（单调队列，摊还 O(1)）"""

    _is_max = True

    def __init__(self, x: Node, period: int):
        super().__init__(x, period)
        self._mono: deque = deque()   # (位置, 值)
        self._pos = 0

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        v = self._push(env)
        if v is not None:
            if self._is_max:
                while self._mono and self._mono[-1][1] <= v:
                    self._mono.pop()
            else:
                while self._mono and self._mono[-1][1] >= v:
                    self._mono.pop()
            self._mono.append((self._pos, v))
        stale = self._pos - self.period   # 位置 <= stale 的已滑出窗口
        while self._mono and self._mono[0][0] <= stale:
            self._mono.popleft()
        self._pos += 1
        self._value = self._mono[0][1] if self._ready() else None
        return self._value

    def snapshot(self) -> Any:
        return (super().snapshot(), list(self._mono), self._pos)

    def restore(self, snap: Any) -> None:
        base, mono, pos = snap
        super().restore(base)
        self._mono = deque(mono)
        self._pos = pos

    def reset_state(self) -> None:
        super().reset_state()
        self._mono = deque()
        self._pos = 0


class RollingMaxNode(_RollingExtremumNode):
    """滚动最高（对齐 polars rolling_max）"""

    _is_max = True

    def batch(self) -> pl.Expr:
        return self.x.batch().rolling_max(window_size=self.period)


class RollingMinNode(_RollingExtremumNode):
    """滚动最低（对齐 polars rolling_min）"""

    _is_max = False

    def batch(self) -> pl.Expr:
        return self.x.batch().rolling_min(window_size=self.period)


class RollingStdNode(_WindowNode):
    """滚动标准差（对齐 polars rolling_std，ddof=1）"""

    def batch(self) -> pl.Expr:
        return self.x.batch().rolling_std(window_size=self.period)

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        self._push(env)
        if not self._ready():
            self._value = None
            return None
        n = self.period
        s = sum(self._win)
        sq = sum(v * v for v in self._win)
        var = max(0.0, (sq - s * s / n) / (n - 1))
        self._value = math.sqrt(var)
        return self._value


class ShiftNode(Node):
    """向前引用 shift(n)：输出 n 根之前的值（对齐 polars shift）"""

    def __init__(self, x: Node, n: int):
        super().__init__([x])
        self.x = x
        self.n = n
        self._buf: deque = deque(maxlen=n + 1)

    def batch(self) -> pl.Expr:
        return self.x.batch().shift(self.n)

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        self._buf.append(self.x._value)
        self._value = self._buf[0] if len(self._buf) == self.n + 1 else None
        return self._value

    def snapshot(self) -> Any:
        return list(self._buf)

    def restore(self, snap: Any) -> None:
        self._buf = deque(snap, maxlen=self.n + 1)

    def reset_state(self) -> None:
        super().reset_state()
        self._buf = deque(maxlen=self.n + 1)

    @property
    def min_periods(self) -> int:
        return self.x.min_periods + self.n


# ─── 组合原语 ───

_BINARY_OPS = {
    "+": lambda a, b: a + b,
    "-": lambda a, b: a - b,
    "*": lambda a, b: a * b,
    "/": lambda a, b: a / b,
}


class BinaryNode(Node):
    """二元四则运算（null 传播；除零输出 null）"""

    def __init__(self, a: Node, b: Node, op: str):
        super().__init__([a, b])
        self.a = a
        self.b = b
        self.op = op

    def batch(self) -> pl.Expr:
        ea, eb = self.a.batch(), self.b.batch()
        if self.op == "+":
            return ea + eb
        if self.op == "-":
            return ea - eb
        if self.op == "*":
            return ea * eb
        return pl.when(eb == 0).then(None).otherwise(ea / eb)

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        a, b = self.a._value, self.b._value
        if a is None or b is None or (self.op == "/" and b == 0):
            self._value = None
            return None
        self._value = _BINARY_OPS[self.op](a, b)
        return self._value


class CompareNode(Node):
    """比较运算（产出布尔，null 传播）"""

    _CMP = {
        ">": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
        "<": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
    }

    def __init__(self, a: Node, b: Node, op: str):
        super().__init__([a, b])
        self.a = a
        self.b = b
        self.op = op

    def batch(self) -> pl.Expr:
        ea, eb = self.a.batch(), self.b.batch()
        if self.op == ">":
            return ea > eb
        if self.op == ">=":
            return ea >= eb
        if self.op == "<":
            return ea < eb
        return ea <= eb

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        a, b = self.a._value, self.b._value
        if a is None or b is None:
            self._value = None
            return None
        self._value = self._CMP[self.op](a, b)
        return self._value


class WhereNode(Node):
    """三元选择 where(cond, a, b)（cond 为 null 走 b，对齐 pl.when）"""

    def __init__(self, cond: Node, a: Node, b: Node):
        super().__init__([cond, a, b])
        self.cond = cond
        self.a = a
        self.b = b

    def batch(self) -> pl.Expr:
        return pl.when(self.cond.batch()).then(self.a.batch()).otherwise(self.b.batch())

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        c = self.cond._value
        self._value = self.b._value if not c else self.a._value
        return self._value


class MaximumNode(Node):
    """逐点取大（任一输入 null → null）"""

    def __init__(self, a: Node, b: Node):
        super().__init__([a, b])
        self.a = a
        self.b = b

    def batch(self) -> pl.Expr:
        ea, eb = self.a.batch(), self.b.batch()
        return (
            pl.when(ea.is_null() | eb.is_null())
            .then(pl.lit(None, dtype=pl.Float64))
            .otherwise(pl.max_horizontal(ea, eb))
        )

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        a, b = self.a._value, self.b._value
        self._value = None if (a is None or b is None) else max(a, b)
        return self._value


class MinimumNode(Node):
    """逐点取小（任一输入 null → null）"""

    def __init__(self, a: Node, b: Node):
        super().__init__([a, b])
        self.a = a
        self.b = b

    def batch(self) -> pl.Expr:
        ea, eb = self.a.batch(), self.b.batch()
        return (
            pl.when(ea.is_null() | eb.is_null())
            .then(pl.lit(None, dtype=pl.Float64))
            .otherwise(pl.min_horizontal(ea, eb))
        )

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        a, b = self.a._value, self.b._value
        self._value = None if (a is None or b is None) else min(a, b)
        return self._value


class NegNode(Node):
    """取负"""

    def __init__(self, x: Node):
        super().__init__([x])
        self.x = x

    def batch(self) -> pl.Expr:
        return -self.x.batch()

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        v = self.x._value
        self._value = None if v is None else -v
        return self._value


class AbsNode(Node):
    """绝对值"""

    def __init__(self, x: Node):
        super().__init__([x])
        self.x = x

    def batch(self) -> pl.Expr:
        return self.x.batch().abs()

    def incr(self, env: Dict[str, Any]) -> Optional[float]:
        v = self.x._value
        self._value = None if v is None else abs(v)
        return self._value

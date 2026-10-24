"""ZigZagCore — 条件驱动 ZigZag 核心算法（IND-107 扩展形态）

非经典 ZigZag：不使用 percent/points/ATR 偏离阈值，转折由**指标条件状态**驱动——
多头态持续期间跟踪区间最高 High 为 pending H1（随新 bar 重绘），条件翻转为空头态
时 pending H1 确认（confirmed），同时开启 pending L1 跟踪（区间最低 Low），反之亦
然；H/L 严格交替。已确认编号不再改变，新确认使旧 H1→H2 平移。

条件表达式（受限 Python 文法，ast 解析 + 节点白名单，无 eval）：
    - 序列原子：open/high/low/close/volume、ma(x,n)、sma(x,n)、ema(x,n)、
      macd_dif(f,s,m)、macd_dea(f,s,m)、boll_upper/lower/mid(p,sd)、rsi(n)、
      atr(n)、indic("NAME", field="X", **params)（任意注册表指标，含自定义）；
    - 比较/逻辑：> < >= <= == != and or not、数值常量、四则 + - * /；
    - 交叉谓词：cross_up(a,b) / cross_down(a,b) / cross(a,b) / golden(a,b) /
      death(a,b)，带 tolerance 容差抑制浮点抖动；
    - 单序列方向谓词（单条均线场景）：rising(x) / falling(x)（新值 vs 前值）、
      ref(x,n)（n 根前的值，可自由组合比较）。

翻转判定 = 严格条件态（bull_cond → 多头 / bear_cond → 空头）**OR** 时间窗修正：
严格条件未定时（False/预热 None），若 time_window 根内发生过 cross_type 类型的
交叉事件（如均线金叉），即认定翻转成立（用户示例：close>MA and close>EMA 未严格
成立，但窗内有交叉 → 条件成立）。

增量语义对齐 IND-101 快照法：同 ts 重复推送（forming bar）先恢复快照再应用，
幂等可重放；回测批量路径与实盘增量路径同一实现。
forming bar（is_closed=False）与收盘 bar 同口径评估条件——与条件线 dirline 的
forming 段变色时刻逐 tick 同步（v9.145 回退 v9.144 仅收盘口径：该口径与 forming
段变色脱节，致「条件成立（已变色）却不绘段」）；盘内瞬翻瞬回由同 ts 快照法 +
v9.142 修补链路同步回退，不留鬼腿。
"""
from __future__ import annotations

import ast
import json
import logging
import math
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

__all__ = [
    "Pivot",
    "CrossEvent",
    "ZigZagCore",
    "SeriesManager",
    "compile_cond",
    "CROSS_TYPES",
]

# 交叉类型：cross_up/golden（上穿，等价）、cross_down/death（下穿，等价）、
# cross（任意交叉）、None/""（跟随条件表达式内出现的交叉谓词对）
CROSS_TYPES = ("cross_up", "cross_down", "cross", "golden", "death")

# 别名归一：golden≡cross_up（金叉）、death≡cross_down（死叉）
_CROSS_ALIAS = {"golden": "cross_up", "death": "cross_down"}


# ─── 数据结构 ───


@dataclass
class Pivot:
    """ZigZag 极值点（转折点）。

    index 为原始 K 线索引（0 起）；direction +1=高点(H) / -1=低点(L)；
    confirmed=True 后不再改变；pending=True 表示当前未确认、允许重绘。
    """

    index: int
    timestamp: int
    price: float
    direction: int
    confirmed: bool = False
    pending: bool = False
    cross_index: Optional[int] = None   # 时间窗修正锚定的交叉事件 bar 索引
    label: Optional[str] = None         # 最近一次访问时的编号快照（H1/L2…）


@dataclass
class CrossEvent:
    """两条线型序列的交叉事件。

    type: cross_up / cross_down；index/timestamp: 交叉检测所在 bar；
    a/b: 序列原子 key；a_value/b_value: 交叉 bar 上两序列值；
    cross_price: 两线交点插值价格（近似坐标）；
    pivot_label: 该事件参与确认的极值点编号（时间窗修正触发时回填）；
    triggered_at_index: 实际触发状态翻转的 bar 索引（None=未被消费）。
    """

    type: str
    index: int
    timestamp: int
    a: str
    b: str
    a_value: Optional[float]
    b_value: Optional[float]
    cross_price: Optional[float]
    pivot_label: Optional[str] = None
    triggered_at_index: Optional[int] = None


# ─── 表达式编译（受限 AST） ───

_CMP_OPS: Dict[type, Callable[[float, float], bool]] = {
    ast.Gt: lambda a, b: a > b,
    ast.Lt: lambda a, b: a < b,
    ast.GtE: lambda a, b: a >= b,
    ast.LtE: lambda a, b: a <= b,
    ast.Eq: lambda a, b: a == b,
    ast.NotEq: lambda a, b: a != b,
}

_BIN_OPS: Dict[type, Callable[[float, float], float]] = {
    ast.Add: lambda a, b: a + b,
    ast.Sub: lambda a, b: a - b,
    ast.Mult: lambda a, b: a * b,
    ast.Div: lambda a, b: a / b if b != 0 else float("nan"),
}

RAW_FIELDS = ("open", "high", "low", "close", "volume")

# 内置序列函数签名：fn_name → (是否带显式源序列, 数值参数个数, 是否全为正整数)
#   带源（ma/sma/ema）：fn(序列, n…)；无源（macd/boll/rsi/atr）：fn(n…) 隐式取 close
_BUILTIN_SIGS: Dict[str, Tuple[bool, int, bool]] = {
    "ma": (True, 1, True), "sma": (True, 1, True), "ema": (True, 1, True),
    "macd_dif": (False, 3, True), "macd_dea": (False, 3, True),
    "boll_upper": (False, 2, False), "boll_lower": (False, 2, False), "boll_mid": (False, 2, False),
    "rsi": (False, 1, True), "atr": (False, 1, True),
}

# 谓词函数（返回布尔/序列，不产出新序列原子之外的东西）
_PREDICATES = ("cross_up", "cross_down", "cross", "golden", "death", "rising", "falling")


class EvalNode:
    """求值树基类：evaluate(sm, tol) → float | bool | None（三值逻辑）"""

    __slots__ = ()

    def evaluate(self, sm: "SeriesManager", tol: float) -> Any:
        raise NotImplementedError


class EConst(EvalNode):
    __slots__ = ("v",)

    def __init__(self, v: float):
        self.v = float(v)

    def evaluate(self, sm, tol):
        return self.v


class EAtom(EvalNode):
    """序列原子引用：key + shift（ref(x,n) → shift=n）"""

    __slots__ = ("key", "shift")

    def __init__(self, key: str, shift: int = 0):
        self.key = key
        self.shift = shift

    def evaluate(self, sm, tol):
        return sm.value_at(self.key, self.shift)


class EBin(EvalNode):
    __slots__ = ("op", "l", "r")

    def __init__(self, op: Callable[[float, float], float], left: EvalNode, right: EvalNode):
        self.op = op
        self.l = left
        self.r = right

    def evaluate(self, sm, tol):
        a = _as_num(self.l.evaluate(sm, tol))
        b = _as_num(self.r.evaluate(sm, tol))
        if a is None or b is None or (isinstance(a, bool) or isinstance(b, bool)):
            return None
        try:
            v = self.op(a, b)
        except (ValueError, OverflowError):
            return None
        return None if (isinstance(v, float) and math.isnan(v)) else float(v)


class ENot(EvalNode):
    __slots__ = ("x",)

    def __init__(self, x: EvalNode):
        self.x = x

    def evaluate(self, sm, tol):
        v = self.x.evaluate(sm, tol)
        return None if v is None else (not _truthy(v))


class EAnd(EvalNode):
    __slots__ = ("xs",)

    def __init__(self, xs: List[EvalNode]):
        self.xs = xs

    def evaluate(self, sm, tol):
        has_none = False
        for x in self.xs:
            v = x.evaluate(sm, tol)
            if v is None:
                has_none = True
            elif not _truthy(v):
                return False
        return None if has_none else True


class EOr(EvalNode):
    __slots__ = ("xs",)

    def __init__(self, xs: List[EvalNode]):
        self.xs = xs

    def evaluate(self, sm, tol):
        has_none = False
        for x in self.xs:
            v = x.evaluate(sm, tol)
            if v is None:
                has_none = True
            elif _truthy(v):
                return True
        return None if has_none else False


class ECmp(EvalNode):
    __slots__ = ("op", "l", "r")

    def __init__(self, op: Callable[[float, float], bool], left: EvalNode, right: EvalNode):
        self.op = op
        self.l = left
        self.r = right

    def evaluate(self, sm, tol):
        a = _as_num(self.l.evaluate(sm, tol))
        b = _as_num(self.r.evaluate(sm, tol))
        if a is None or b is None:
            return None
        return self.op(a, b)


def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    return bool(v)


def _as_num(v: Any) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    return None


@dataclass
class AtomSpec:
    """序列原子规格（编译期产物；SeriesManager 按此建增量计算器）"""

    key: str
    kind: str                       # raw / builtin / registry
    fn: str = ""                    # builtin 函数名 / raw 字段名
    src: Any = None                 # builtin: 序列节点（如 close）；registry: (指标名, 字段)
    args: Tuple[Any, ...] = ()      # builtin 数值参数
    params: Dict[str, Any] = field(default_factory=dict)   # registry 指标参数
    max_shift: int = 0              # 表达式中用到的最大 ref 位移（历史深度）


@dataclass
class CrossPair:
    """条件表达式中出现的交叉谓词序列对（时间窗事件扫描范围）"""

    a_key: str
    b_key: str
    types: Tuple[str, ...]          # 该对被哪些谓词引用（cross_up/cross_down/cross）


class _Collector:
    """编译期原子/交叉对收集器"""

    def __init__(self) -> None:
        self.specs: Dict[str, AtomSpec] = {}
        self.pairs: Dict[Tuple[str, str], List[str]] = {}

    def atom(self, spec: AtomSpec, shift: int = 0) -> EAtom:
        cur = self.specs.get(spec.key)
        if cur is None:
            self.specs[spec.key] = spec
            spec.max_shift = max(spec.max_shift, shift)
        else:
            cur.max_shift = max(cur.max_shift, shift)
        return EAtom(spec.key, shift)

    def pair(self, a_key: str, b_key: str, ptype: str) -> None:
        self.pairs.setdefault((a_key, b_key), [])
        if ptype not in self.pairs[(a_key, b_key)]:
            self.pairs[(a_key, b_key)].append(ptype)


def compile_cond(
    expr: str, tolerance: float
) -> Tuple[EvalNode, Dict[str, AtomSpec], List[CrossPair]]:
    """编译条件表达式 → (求值树, 原子规格表, 交叉对列表)。非法文法抛 ValueError。"""
    try:
        tree = ast.parse(expr.strip(), mode="eval")
    except SyntaxError as e:
        raise ValueError(f"条件表达式语法错误：{expr!r}（{e}）") from e
    col = _Collector()
    root = _lower(tree.body, col, tolerance, expr)
    pairs = [CrossPair(a, b, tuple(ts)) for (a, b), ts in col.pairs.items()]
    return root, col.specs, pairs


def _const_of(node: ast.AST) -> Optional[float]:
    """AST 常量节点 → 数值（支持负号）；非常量返回 None"""
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)) \
            and not isinstance(node.value, bool):
        return float(node.value)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        v = _const_of(node.operand)
        return None if v is None else -v
    return None


def _lower(node: ast.AST, col: _Collector, tol: float, src: str) -> EvalNode:
    if isinstance(node, ast.Constant):
        if isinstance(node.value, bool):
            return EConst(1.0 if node.value else 0.0)
        if isinstance(node.value, (int, float)):
            return EConst(float(node.value))
        raise ValueError(f"条件表达式仅支持数值常量：{src!r}")
    if isinstance(node, ast.Name):
        if node.id not in RAW_FIELDS:
            raise ValueError(
                f"未知变量 {node.id!r}（可用序列：{'/'.join(RAW_FIELDS)} 及指标函数）：{src!r}"
            )
        return col.atom(AtomSpec(key=node.id, kind="raw", fn=node.id))
    if isinstance(node, ast.UnaryOp):
        if isinstance(node.op, ast.USub):
            v = _const_of(node)
            if v is None:
                raise ValueError(f"一元负号仅支持数值常量：{src!r}")
            return EConst(v)
        if isinstance(node.op, ast.Not):
            return ENot(_lower(node.operand, col, tol, src))
        raise ValueError(f"不支持的一元运算 {type(node.op).__name__}：{src!r}")
    if isinstance(node, ast.BinOp):
        op = _BIN_OPS.get(type(node.op))
        if op is None:
            raise ValueError(f"不支持的算术运算 {type(node.op).__name__}：{src!r}")
        return EBin(op, _lower(node.left, col, tol, src), _lower(node.right, col, tol, src))
    if isinstance(node, ast.BoolOp):
        xs = [_lower(v, col, tol, src) for v in node.values]
        return EAnd(xs) if isinstance(node.op, ast.And) else EOr(xs)
    if isinstance(node, ast.Compare):
        if len(node.ops) != 1:
            raise ValueError(f"仅支持单比较符（a > b，不支持链式 a > b > c）：{src!r}")
        op = _CMP_OPS.get(type(node.ops[0]))
        if op is None:
            raise ValueError(f"不支持的比较运算 {type(node.ops[0]).__name__}：{src!r}")
        return ECmp(
            op, _lower(node.left, col, tol, src), _lower(node.comparators[0], col, tol, src)
        )
    if isinstance(node, ast.Call):
        return _lower_call(node, col, tol, src)
    raise ValueError(f"不支持的表达式节点 {type(node).__name__}：{src!r}")


def _lower_call(node: ast.Call, col: _Collector, tol: float, src: str) -> EvalNode:
    if not isinstance(node.func, ast.Name):
        raise ValueError(f"仅支持直接函数调用：{src!r}")
    fname = node.func.id

    # ── ref(x, n)：n 根前的序列值 ──
    if fname == "ref":
        if len(node.args) != 2 or node.keywords:
            raise ValueError(f"ref 需要 (序列, n) 两个参数：{src!r}")
        n = _const_of(node.args[1])
        if n is None or int(n) < 1 or float(int(n)) != n:
            raise ValueError(f"ref 的 n 必须为正整数常量：{src!r}")
        spec = _atom_spec_of(node.args[0], col, tol, src)
        return col.atom(spec, int(n))

    # ── rising(x) / falling(x)：单序列方向谓词（新值 vs 前值，带容差） ──
    if fname in ("rising", "falling"):
        if len(node.args) != 1 or node.keywords:
            raise ValueError(f"{fname} 需要 (序列) 一个参数：{src!r}")
        spec = _atom_spec_of(node.args[0], col, tol, src)
        cur = col.atom(spec, 0)
        prev = col.atom(spec, 1)
        diff = EBin(_BIN_OPS[ast.Sub], cur, prev)
        op = _CMP_OPS[ast.Gt] if fname == "rising" else _CMP_OPS[ast.Lt]
        cmp_node = ECmp(op, diff, EConst(tol))
        if fname == "rising":
            return cmp_node
        # falling: x - ref(x,1) < -tol
        return ECmp(_CMP_OPS[ast.Lt], diff, EConst(-tol))

    # ── 交叉谓词：cross_up/cross_down/cross/golden/death(a, b) ──
    if fname in _PREDICATES:
        if len(node.args) != 2 or node.keywords:
            raise ValueError(f"{fname} 需要 (序列a, 序列b) 两个参数：{src!r}")
        spec_a = _atom_spec_of(node.args[0], col, tol, src)
        spec_b = _atom_spec_of(node.args[1], col, tol, src)
        a0, a1 = col.atom(spec_a, 0), col.atom(spec_a, 1)
        b0, b1 = col.atom(spec_b, 0), col.atom(spec_b, 1)
        eff = _CROSS_ALIAS.get(fname, fname)
        col.pair(spec_a.key, spec_b.key, eff)
        if eff == "cross_up":
            # prev_a <= prev_b + tol and a > b + tol
            return EAnd([
                ECmp(_CMP_OPS[ast.LtE], a1, EBin(_BIN_OPS[ast.Add], b1, EConst(tol))),
                ECmp(_CMP_OPS[ast.Gt], a0, EBin(_BIN_OPS[ast.Add], b0, EConst(tol))),
            ])
        if eff == "cross_down":
            return EAnd([
                ECmp(_CMP_OPS[ast.GtE], a1, EBin(_BIN_OPS[ast.Sub], b1, EConst(tol))),
                ECmp(_CMP_OPS[ast.Lt], a0, EBin(_BIN_OPS[ast.Sub], b0, EConst(tol))),
            ])
        # cross：任意交叉（上穿 or 下穿）
        up = EAnd([
            ECmp(_CMP_OPS[ast.LtE], a1, EBin(_BIN_OPS[ast.Add], b1, EConst(tol))),
            ECmp(_CMP_OPS[ast.Gt], a0, EBin(_BIN_OPS[ast.Add], b0, EConst(tol))),
        ])
        down = EAnd([
            ECmp(_CMP_OPS[ast.GtE], a1, EBin(_BIN_OPS[ast.Sub], b1, EConst(tol))),
            ECmp(_CMP_OPS[ast.Lt], a0, EBin(_BIN_OPS[ast.Sub], b0, EConst(tol))),
        ])
        return EOr([up, down])

    # ── indic("NAME", field="X", **params)：任意注册表指标字段 ──
    if fname == "indic":
        if not node.args or len(node.args) > 2:
            raise ValueError(f"indic 需要 (指标名[, 字段名], **params)：{src!r}")
        name_node = node.args[0]
        if not (isinstance(name_node, ast.Constant) and isinstance(name_node.value, str)):
            raise ValueError(f"indic 指标名必须为字符串常量：{src!r}")
        ind_name = name_node.value
        ind_field: Optional[str] = None
        if len(node.args) == 2:
            f_node = node.args[1]
            if not (isinstance(f_node, ast.Constant) and isinstance(f_node.value, str)):
                raise ValueError(f"indic 字段名必须为字符串常量：{src!r}")
            ind_field = f_node.value
        params: Dict[str, Any] = {}
        for kw in node.keywords:
            if kw.arg is None:
                raise ValueError(f"indic 不支持 ** 展开：{src!r}")
            if kw.arg == "field":
                if not (isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str)):
                    raise ValueError(f"indic field 必须为字符串常量：{src!r}")
                ind_field = kw.value.value
                continue
            pv = kw.value
            if not (isinstance(pv, ast.Constant) and isinstance(pv.value, (int, float, str))
                    and not isinstance(pv.value, bool)):
                raise ValueError(f"indic 参数 {kw.arg} 必须为字面常量：{src!r}")
            params[kw.arg] = pv.value
        return col.atom(_registry_spec(ind_name, ind_field, params, src))

    # ── 内置序列函数：ma/sma/ema（带源）、macd_dif/boll_*/rsi/atr（无源，隐式 close） ──
    if fname in _BUILTIN_SIGS:
        sourced, n_num, all_int = _BUILTIN_SIGS[fname]
        if sourced:
            if len(node.args) != n_num + 1 or node.keywords:
                raise ValueError(f"{fname} 需要 (序列, {n_num} 个数值参数)：{src!r}")
            base = _lower(node.args[0], col, tol, src)
            if not isinstance(base, EAtom) or base.shift != 0:
                raise ValueError(f"{fname} 的第一个参数必须为序列原子：{src!r}")
            src_key = base.key
            num_nodes = node.args[1:]
        else:
            if len(node.args) != n_num or node.keywords:
                raise ValueError(f"{fname} 需要 {n_num} 个数值参数：{src!r}")
            src_key = "close"
            num_nodes = node.args
        nums = [_const_of(a) for a in num_nodes]
        if any(v is None for v in nums):
            raise ValueError(f"{fname} 的参数必须为数值常量：{src!r}")
        if all_int and any(float(int(v)) != v or int(v) < 1 for v in nums):
            raise ValueError(f"{fname} 的周期参数必须为正整数：{src!r}")
        if any(v < 1 for v in nums):
            raise ValueError(f"{fname} 的参数必须 >= 1：{src!r}")
        args_t = tuple(int(v) if all_int else float(v) for v in nums)
        key = f"{fname}({src_key},{','.join(str(a) for a in args_t)})"
        return col.atom(AtomSpec(key=key, kind="builtin", fn=fname, src=src_key, args=args_t))

    raise ValueError(f"未知函数 {fname!r}：{src!r}")


def _atom_spec_of(node: ast.AST, col: _Collector, tol: float, src: str) -> AtomSpec:
    """交叉/方向谓词的参数必须落回单个序列原子（编译出临时 EAtom 后取其 spec）"""
    ev = _lower(node, col, tol, src)
    if not isinstance(ev, EAtom) or ev.shift != 0:
        raise ValueError(f"该谓词参数必须为单条线型序列（如 ma(close,5)）：{src!r}")
    return col.specs[ev.key]


def _registry_spec(
    ind_name: str, ind_field: Optional[str], params: Dict[str, Any], src: str
) -> AtomSpec:
    """任意注册表指标 → AtomSpec（延迟导入注册表，避免环依赖）"""
    from klinequant.core.indicator_engine.registry import get_registry

    reg = get_registry()
    if not reg.has(ind_name):
        raise ValueError(f"未注册指标 {ind_name!r}：{src!r}")
    proto = reg.create(ind_name, params)
    fields = list(proto.display_meta.get("fields", []))
    if ind_field is None:
        if len(fields) != 1:
            raise ValueError(
                f"指标 {ind_name!r} 输出多字段 {fields}，indic() 必须显式指定 field：{src!r}"
            )
        ind_field = fields[0]
    elif ind_field not in fields:
        raise ValueError(f"指标 {ind_name!r} 无字段 {ind_field!r}（可用：{fields}）：{src!r}")
    pj = json.dumps(params, sort_keys=True, separators=(",", ":"))
    return AtomSpec(
        key=f"indic({ind_name}|{pj}|{ind_field})", kind="registry",
        src=(ind_name, ind_field), params=dict(params),
    )


# ─── 序列原子增量计算器 ───


class _Comp:
    """序列原子增量计算器基类：apply(bar) 推进一根，value 为当前值（未预热 None）"""

    min_periods = 1

    def apply(self, bar: Dict[str, Any]) -> None:
        raise NotImplementedError

    def snapshot(self) -> Any:
        raise NotImplementedError

    def restore(self, snap: Any) -> None:
        raise NotImplementedError

    @property
    def value(self) -> Optional[float]:
        raise NotImplementedError


class _RawComp(_Comp):
    """原始行情字段（open/high/low/close/volume）"""

    __slots__ = ("_fld", "_v")

    def __init__(self, fld: str):
        self._fld = fld
        self._v: Optional[float] = None

    def apply(self, bar):
        v = bar.get(self._fld)
        self._v = None if v is None else float(v)

    @property
    def value(self):
        return self._v

    def snapshot(self):
        return self._v

    def restore(self, snap):
        self._v = snap


class _WindowComp(_Comp):
    """滚动窗口基类（deque + 满窗才出值）：ma/sma/boll"""

    def __init__(self, src: str, period: int):
        self._src = src
        self._period = int(period)
        self._win: Deque[float] = deque(maxlen=self._period)

    @property
    def min_periods(self) -> int:
        return self._period

    def _push(self, bar: Dict[str, Any]) -> None:
        v = bar.get(self._src)
        if v is not None:
            self._win.append(float(v))

    def snapshot(self):
        return list(self._win)

    def restore(self, snap):
        self._win = deque(snap, maxlen=self._period)


class _MaComp(_WindowComp):
    @property
    def value(self):
        if len(self._win) < self._period:
            return None
        return math.fsum(self._win) / self._period

    def apply(self, bar):
        self._push(bar)


class _BollBase(_WindowComp):
    """BOLL(p, sd) 滚动均值/标准差（ddof=1，对齐 polars rolling_std）"""

    def __init__(self, src: str, period: int, sd: float):
        super().__init__(src, period)
        self._sd = float(sd)

    def _mid_std(self) -> Optional[Tuple[float, float]]:
        if len(self._win) < self._period:
            return None
        mid = math.fsum(self._win) / self._period
        var = math.fsum((x - mid) ** 2 for x in self._win) / (self._period - 1) \
            if self._period > 1 else 0.0
        return mid, math.sqrt(var)


class _BollUpperComp(_BollBase):
    def apply(self, bar):
        self._push(bar)

    @property
    def value(self):
        ms = self._mid_std()
        return None if ms is None else ms[0] + self._sd * ms[1]


class _BollLowerComp(_BollBase):
    def apply(self, bar):
        self._push(bar)

    @property
    def value(self):
        ms = self._mid_std()
        return None if ms is None else ms[0] - self._sd * ms[1]


class _BollMidComp(_BollBase):
    def apply(self, bar):
        self._push(bar)

    @property
    def value(self):
        ms = self._mid_std()
        return None if ms is None else ms[0]


class _EmaComp(_Comp):
    """EMA 递推（alpha=2/(n+1)，种子=首个有效值，对齐 polars ewm_mean(adjust=False)）"""

    __slots__ = ("_src", "_period", "_alpha", "_ema", "_count")

    def __init__(self, src: str, period: int):
        self._src = src
        self._period = int(period)
        self._alpha = 2.0 / (self._period + 1)
        self._ema: Optional[float] = None
        self._count = 0

    @property
    def min_periods(self):
        return self._period

    def apply(self, bar):
        v = bar.get(self._src)
        x = None if v is None else float(v)
        if x is not None:
            self._ema = x if self._ema is None else self._alpha * x + (1 - self._alpha) * self._ema
            self._count += 1

    @property
    def value(self):
        return self._ema if self._count >= self._period else None

    def snapshot(self):
        return (self._ema, self._count)

    def restore(self, snap):
        self._ema, self._count = snap


class _MacdComp(_Comp):
    """MACD DIF/DEA 递推（与 indicators/macd.py 增量路径同式）"""

    __slots__ = ("_src", "_af", "_as", "_asig", "_field", "_ef", "_es", "_dea", "_count", "_minp")

    def __init__(self, src: str, fast: int, slow: int, sig: int, fld: str):
        self._src = src
        self._af = 2.0 / (int(fast) + 1)
        self._as = 2.0 / (int(slow) + 1)
        self._asig = 2.0 / (int(sig) + 1)
        self._field = fld
        self._minp = int(slow) + int(sig) - 1
        self._ef: Optional[float] = None
        self._es: Optional[float] = None
        self._dea = 0.0
        self._count = 0

    @property
    def min_periods(self):
        return self._minp

    def apply(self, bar):
        v = bar.get(self._src)
        if v is None:
            return
        c = float(v)
        if self._ef is None:
            self._ef = c
            self._es = c
            self._dea = 0.0
        else:
            self._ef = self._af * c + (1 - self._af) * self._ef
            self._es = self._as * c + (1 - self._as) * self._es
            dif = self._ef - self._es
            self._dea = self._asig * dif + (1 - self._asig) * self._dea
        self._count += 1

    @property
    def value(self):
        if self._count < self._minp or self._ef is None or self._es is None:
            return None
        dif = self._ef - self._es
        return dif if self._field == "DIF" else self._dea

    def snapshot(self):
        return (self._ef, self._es, self._dea, self._count)

    def restore(self, snap):
        self._ef, self._es, self._dea, self._count = snap


class _RsiComp(_Comp):
    """RSI Wilder 平滑递推（alpha=1/n，对齐 polars ewm_mean(adjust=False)）"""

    __slots__ = ("_src", "_period", "_ag", "_al", "_prev", "_count")

    def __init__(self, src: str, period: int):
        self._src = src
        self._period = int(period)
        self._ag: Optional[float] = None
        self._al: Optional[float] = None
        self._prev: Optional[float] = None
        self._count = 0

    @property
    def min_periods(self):
        return self._period + 1

    def apply(self, bar):
        v = bar.get(self._src)
        if v is None:
            return
        c = float(v)
        a = 1.0 / self._period
        if self._prev is not None:
            d = c - self._prev
            g = max(d, 0.0)
            loss = max(-d, 0.0)
            self._ag = g if self._ag is None else a * g + (1 - a) * self._ag
            self._al = loss if self._al is None else a * loss + (1 - a) * self._al
        self._prev = c
        self._count += 1

    @property
    def value(self):
        if self._count < self.min_periods or self._ag is None or self._al is None:
            return None
        if self._al == 0:
            return 100.0
        return 100.0 - 100.0 / (1.0 + self._ag / self._al)

    def snapshot(self):
        return (self._ag, self._al, self._prev, self._count)

    def restore(self, snap):
        self._ag, self._al, self._prev, self._count = snap


class _AtrComp(_Comp):
    """ATR Wilder 平滑递推（TR → ewm alpha=1/n，对齐 indicators/atr.py 批量式）"""

    __slots__ = ("_period", "_atr", "_prev_close", "_count")

    def __init__(self, period: int):
        self._period = int(period)
        self._atr: Optional[float] = None
        self._prev_close: Optional[float] = None
        self._count = 0

    @property
    def min_periods(self):
        return self._period + 1

    def apply(self, bar):
        hi, lo, cl = bar.get("high"), bar.get("low"), bar.get("close")
        if hi is None or lo is None or cl is None:
            return
        hi, lo, cl = float(hi), float(lo), float(cl)
        if self._prev_close is not None:
            tr = max(hi - lo, abs(hi - self._prev_close), abs(lo - self._prev_close))
            a = 1.0 / self._period
            self._atr = tr if self._atr is None else a * tr + (1 - a) * self._atr
            self._count += 1
        self._prev_close = cl

    @property
    def value(self):
        return self._atr if self._count >= self._period else None

    def snapshot(self):
        return (self._atr, self._prev_close, self._count)

    def restore(self, snap):
        self._atr, self._prev_close, self._count = snap


class _RegistryComp(_Comp):
    """任意注册表指标字段（indic() 原子）。

    supports_incremental 指标走子实例 update_bar（自带快照幂等）；
    非增量指标每 bar 全量重算（保留完整 bar 缓存，O(n)/bar——自定义指标请优先
    实现增量接口）。同一 (指标,参数) 的多个字段原子共享一个实例，每 bar 只算一次。
    """

    def __init__(self, ind_name: str, ind_field: str, params: Dict[str, Any]):
        from klinequant.core.indicator_engine.registry import get_registry

        self._name = ind_name
        self._field = ind_field
        self._inst = get_registry().create(ind_name, dict(params))
        self._incremental = bool(self._inst.supports_incremental)
        self._bars: List[Dict[str, Any]] = []
        self._val: Optional[float] = None
        self._dirty = False

    @property
    def min_periods(self):
        return self._inst.min_periods

    def mark_dirty(self) -> None:
        self._dirty = True

    def apply(self, bar):
        if self._incremental:
            vals = self._inst.update_bar(dict(bar), True)
            self._val = None if not vals else vals.get(self._field)
            return
        self._bars.append(dict(bar))
        if self._dirty:
            self._recompute()
        self._dirty = False

    def _recompute(self) -> None:
        import polars as pl

        if len(self._bars) < self._inst.min_periods:
            self._val = None
            return
        df = pl.DataFrame({
            "timestamp": [int(b["timestamp"]) for b in self._bars],
            "open": [float(b.get("open", 0.0)) for b in self._bars],
            "high": [float(b["high"]) for b in self._bars],
            "low": [float(b["low"]) for b in self._bars],
            "close": [float(b["close"]) for b in self._bars],
            "volume": [float(b.get("volume", 0.0)) for b in self._bars],
            "quote_volume": [0.0] * len(self._bars),
            "trade_count": [0] * len(self._bars),
            "is_closed": [True] * len(self._bars),
        })
        out = self._inst.calculate(df)
        col = next((c for c in out.columns if c.endswith(f"_{self._field}")), None)
        self._val = None if col is None else out[col][-1]

    @property
    def value(self):
        return self._val

    def snapshot(self):
        # 第三元素仅保留元组形态兼容：父层快照时刻（bar 起始）比子实例自身
        # 快照时刻（其见本 bar 首 tick）早一拍，捕获得的值不可用于回滚
        if self._incremental:
            return (None, len(self._bars), getattr(self._inst, "_snap", None))
        return (None, len(self._bars), None)

    def restore(self, snap):
        _inst_snap, n, _sub_snap = snap
        del self._bars[n:]
        # v9.146：不再回写子实例 _snap——增量子实例（dsl/macd 等）已靠自身
        # 「bar 起始快照」同 ts 幂等；父层 bar 起始时刻捕获的旧快照比子实例
        # 当前快照早一根（缺上一 bar 收盘贡献），回写会把子实例内部状态多回
        # 退一根且逐 bar 链式放大污染：条件源序列与图上显示的同名指标实例
        # 慢性分叉，翻转与条件线变色双向脱节（2026-10-25 实证：「向下段条件
        # 没成立却绘制」/「未变色却翻转」）。子实例同 ts 重放用其自身 _snap，
        # 与显示实例逐 tick 同口径


def _make_comp(spec: AtomSpec) -> _Comp:
    if spec.kind == "raw":
        return _RawComp(spec.fn)
    if spec.kind == "registry":
        ind_name, ind_field = spec.src
        return _RegistryComp(ind_name, ind_field, spec.params)
    fn, args, src = spec.fn, spec.args, spec.src
    if fn in ("ma", "sma"):
        return _MaComp(src, args[0])
    if fn == "ema":
        return _EmaComp(src, args[0])
    if fn == "macd_dif":
        return _MacdComp(src, *args, fld="DIF")
    if fn == "macd_dea":
        return _MacdComp(src, *args, fld="DEA")
    if fn == "boll_upper":
        return _BollUpperComp(src, args[0], args[1])
    if fn == "boll_lower":
        return _BollLowerComp(src, args[0], args[1])
    if fn == "boll_mid":
        return _BollMidComp(src, args[0], args[1])
    if fn == "rsi":
        return _RsiComp(src, args[0])
    if fn == "atr":
        return _AtrComp(args[0])
    raise ValueError(f"未知内置序列函数 {fn!r}")


class _AtomState:
    """单原子运行态：计算器 + 值历史（ref 位移寻址）"""

    __slots__ = ("comp", "hist")

    def __init__(self, comp: _Comp, histlen: int):
        self.comp = comp
        self.hist: Deque[Optional[float]] = deque(maxlen=histlen)


class SeriesManager:
    """序列原子管理：增量推进 / shift 寻址 / 交叉事件扫描 / 快照恢复"""

    def __init__(self, specs: Dict[str, AtomSpec], pairs: List[CrossPair]):
        self._atoms: Dict[str, _AtomState] = {}
        for key, spec in specs.items():
            self._atoms[key] = _AtomState(_make_comp(spec), max(16, spec.max_shift + 2))
        self._specs = specs
        self._pairs = list(pairs)
        self._bar_index = -1

    # ─── 推进 / 取值 ───

    def advance(self, bar: Dict[str, Any]) -> None:
        self._bar_index += 1
        # registry 全量重算实例：同实例多字段每 bar 只重算一次
        marked: set = set()
        for st in self._atoms.values():
            if isinstance(st.comp, _RegistryComp) and id(st.comp) not in marked:
                st.comp.mark_dirty()
                marked.add(id(st.comp))
        for st in self._atoms.values():
            st.comp.apply(bar)
            st.hist.append(st.comp.value)

    def value_at(self, key: str, shift: int = 0) -> Optional[float]:
        st = self._atoms.get(key)
        if st is None:
            return None
        h = st.hist
        idx = len(h) - 1 - shift
        if idx < 0:
            return None
        return h[idx]

    @property
    def bar_index(self) -> int:
        return self._bar_index

    def ready(self) -> bool:
        """全部原子均已出有效值"""
        return all(st.comp.value is not None for st in self._atoms.values())

    def min_periods(self) -> int:
        return max((st.comp.min_periods for st in self._atoms.values()), default=1)

    # ─── 交叉事件扫描 ───

    def detect_crosses(self, ts: int, tolerance: float) -> List[CrossEvent]:
        """扫描配置的交叉对，返回本 bar 新发生的交叉事件（未配置的 pair 不扫描）"""
        out: List[CrossEvent] = []
        for pr in self._pairs:
            a1 = self.value_at(pr.a_key, 1)
            b1 = self.value_at(pr.b_key, 1)
            a0 = self.value_at(pr.a_key, 0)
            b0 = self.value_at(pr.b_key, 0)
            if a1 is None or b1 is None or a0 is None or b0 is None:
                continue
            up = a1 <= b1 + tolerance and a0 > b0 + tolerance
            down = a1 >= b1 - tolerance and a0 < b0 - tolerance
            if not up and not down:
                continue
            if up and ("cross_up" in pr.types or "cross" in pr.types):
                etype = "cross_up"
            elif down and ("cross_down" in pr.types or "cross" in pr.types):
                etype = "cross_down"
            else:
                continue
            # 交点插值价格：d1=a1-b1, d2=a0-b0 → x = |d1|/(|d1|+|d2|)
            d1, d2 = a1 - b1, a0 - b0
            denom = abs(d1) + abs(d2)
            x = abs(d1) / denom if denom > 0 else 0.5
            out.append(CrossEvent(
                type=etype, index=self._bar_index, timestamp=ts,
                a=pr.a_key, b=pr.b_key,
                a_value=a0, b_value=b0,
                cross_price=a0 + (b0 - a0) * x if denom > 0 else a0,
            ))
        return out

    def scan_pairs(self, pairs: List[CrossPair]) -> None:
        """追加交叉扫描对（ZigZagCore 按 cross_type 配置注入）"""
        exist = {(p.a_key, p.b_key) for p in self._pairs}
        for p in pairs:
            if (p.a_key, p.b_key) not in exist:
                self._pairs.append(p)
                exist.add((p.a_key, p.b_key))

    @property
    def atom_keys(self) -> List[str]:
        return list(self._atoms.keys())

    @property
    def pairs(self) -> List[CrossPair]:
        return list(self._pairs)

    # ─── 快照 / 恢复（IND-101 幂等） ───

    def snapshot(self) -> Any:
        return (
            {k: (st.comp.snapshot(), list(st.hist)) for k, st in self._atoms.items()},
            self._bar_index,
        )

    def restore(self, snap: Any) -> None:
        atoms_snap, idx = snap
        for k, (cs, hist) in atoms_snap.items():
            st = self._atoms[k]
            st.comp.restore(cs)
            st.hist = deque(hist, maxlen=st.hist.maxlen)
        self._bar_index = idx

    def reset(self) -> None:
        for key, spec in self._specs.items():
            comp = _make_comp(spec)
            self._atoms[key] = _AtomState(comp, max(16, spec.max_shift + 2))
        self._bar_index = -1


# ─── ZigZag 状态机 ───


class ZigZagCore:
    """条件驱动 ZigZag 状态机（策略无关，可独立使用）。

    用法::

        zz = ZigZagCore(bull_cond="cross_up(ma(close,5), ema(close,5))",
                        tolerance=1e-9, time_window=3)
        for bar in bars:
            zz.update_bar(bar, bar.get("is_closed", True))
        zz.high(1)   # H1（最新确认/pending 高点）
        zz.low(1)    # L1
    """

    def __init__(
        self,
        bull_cond: str,
        bear_cond: Optional[str] = None,
        cross_type: Optional[str] = None,
        tolerance: float = 0.0,
        time_window: int = 0,
        max_events: int = 200,
    ):
        if cross_type is not None and cross_type != "" and cross_type not in CROSS_TYPES:
            raise ValueError(f"cross_type 必须为 {CROSS_TYPES} 之一或 None，得到 {cross_type!r}")
        self.bull_src = str(bull_cond)
        self.bear_src = str(bear_cond) if bear_cond else ""
        self.cross_type = cross_type or None
        self.tolerance = float(tolerance)
        self.time_window = int(time_window)
        if self.tolerance < 0:
            raise ValueError("tolerance 不能为负")
        if self.time_window < 0:
            raise ValueError("time_window 不能为负")

        self._bull_tree, specs, pairs = compile_cond(self.bull_src, self.tolerance)
        self._bear_tree: Optional[EvalNode]
        if self.bear_src:
            self._bear_tree, b_specs, b_pairs = compile_cond(
                self.bear_src, self.tolerance
            )
            for k, sp in b_specs.items():
                if k in specs:
                    specs[k].max_shift = max(specs[k].max_shift, sp.max_shift)
                else:
                    specs[k] = sp
            pairs = list(pairs) + [p for p in b_pairs
                                   if (p.a_key, p.b_key) not in {(q.a_key, q.b_key) for q in pairs}]
        else:
            self._bear_tree = ENot(self._bull_tree)
        self._series = SeriesManager(specs, pairs)
        # cross_type 显式配置时确定事件扫描对：① 条件内交叉谓词对；
        # ② 无谓词对时从条件的比较节点推导隐含对（至少一方为指标序列，
        # 排除纯行情字段对如 close×open 的无意义扫描）
        if self.cross_type is not None:
            eff = _CROSS_ALIAS.get(self.cross_type, self.cross_type)
            scan_keys = {(p.a_key, p.b_key) for p in pairs}
            if not scan_keys:
                inferred: List[Tuple[str, str]] = []
                _collect_inferred_pairs(self._bull_tree, inferred)
                if self._bear_tree is not None:
                    _collect_inferred_pairs(self._bear_tree, inferred)
                for a_key, b_key in inferred:
                    if (a_key, b_key) in scan_keys or (b_key, a_key) in scan_keys:
                        continue
                    a_raw = specs[a_key].kind == "raw"
                    b_raw = specs[b_key].kind == "raw"
                    if a_raw and b_raw:
                        continue
                    scan_keys.add((a_key, b_key))
            self._series.scan_pairs([
                CrossPair(a, b, (eff,) if eff != "cross" else ("cross",))
                for a, b in scan_keys
            ])

        self._pivots: List[Pivot] = []
        self._bullish: Optional[bool] = None
        self._events: Deque[CrossEvent] = deque(maxlen=int(max_events))
        self._bar_count = 0
        self._last_ts: Optional[int] = None
        self._snap: Optional[Any] = None

    # ─── 配置元信息 ───

    @property
    def min_periods(self) -> int:
        return self._series.min_periods() + 1

    @property
    def series(self) -> SeriesManager:
        return self._series

    # ─── 增量推进 ───

    def update_bar(self, bar: Dict[str, Any], is_closed: bool = True) -> None:
        ts = int(bar["timestamp"])
        if self._last_ts is not None and ts < self._last_ts:
            return  # 乱序历史 bar：增量路径不处理
        if ts != self._last_ts:
            self._snap = self._snapshot()
        elif self._snap is not None:
            self._restore(self._snap)

        self._series.advance(bar)
        self._bar_count += 1
        self._last_ts = ts
        idx = self._bar_count - 1

        # forming bar 与收盘 bar 同口径评估：与条件线 dirline forming 段变色逐 tick 同步
        # （v9.145：v9.144 仅收盘口径致「ZLINE 已变色却不绘段」）；盘内瞬翻瞬回由
        # 同 ts 快照法回退 + v9.142 修补链路达前端同步回退，不留鬼腿
        tol = self.tolerance
        bull = self._bull_tree.evaluate(self._series, tol)
        bear = self._bear_tree.evaluate(self._series, tol) if self._bear_tree is not None else None

        # 交叉事件入队（时间窗修正的证据链）
        for ev in self._series.detect_crosses(ts, tol):
            self._events.append(ev)

        state = self._decide(bull, bear, idx)
        high = bar.get("high")
        low = bar.get("low")
        if state is None:
            return  # 预热未定 / 无翻转：状态保持
        if self._bullish is None:
            self._set_initial(state, idx, ts, high, low)
        elif state != self._bullish:
            self._flip(state, idx, ts, high, low)
        else:
            self._track(idx, ts, high, low)

    def _decide(self, bull: Optional[bool], bear: Optional[bool], idx: int) -> Optional[bool]:
        """目标状态：严格条件优先，未定时走时间窗交叉修正；仍无法判定返回 None"""
        if bull is True and bear is not True:
            return True
        if bear is True and bull is not True:
            return False
        if bull is True and bear is True:
            return True  # 双真冲突：bull 优先（文档化行为）
        if self.time_window > 0:
            ev = self._window_event(idx)
            if ev is not None:
                return ev.type == "cross_up"
        return None

    def _window_event(self, idx: int) -> Optional[CrossEvent]:
        """time_window 根内最近一个符合 cross_type 且未被消费的交叉事件"""
        eff = _CROSS_ALIAS.get(self.cross_type or "", self.cross_type)
        for ev in reversed(self._events):
            if idx - ev.index >= self.time_window:
                break
            if ev.triggered_at_index is not None:
                continue  # 已被消费，不重复触发
            if eff is None or eff == "cross" or ev.type == eff:
                return ev
        return None

    # ─── 状态机动作 ───

    def _set_initial(self, state: bool, idx: int, ts: int, high: Any, low: Any) -> None:
        self._bullish = state
        # 多头 leg 跟踪待确认高点 H，空头 leg 跟踪待确认低点 L
        if state and high is not None:
            self._pivots.append(Pivot(idx, ts, float(high), 1, pending=True))
        elif not state and low is not None:
            self._pivots.append(Pivot(idx, ts, float(low), -1, pending=True))

    def _flip(self, state: bool, idx: int, ts: int, high: Any, low: Any) -> None:
        pend = self._pivots[-1] if self._pivots else None
        if pend is not None and pend.pending:
            # 翻转 bar 本身可能更极端：先用本 bar 更新 pending 再确认
            # （pending 方向恒为旧 leg 方向：多头 leg 的 H 用 high 更新，反之用 low）
            if pend.direction == 1 and high is not None and float(high) > pend.price:
                pend.price = float(high)
                pend.index = idx
                pend.timestamp = ts
            elif pend.direction == -1 and low is not None and float(low) < pend.price:
                pend.price = float(low)
                pend.index = idx
                pend.timestamp = ts
            pend.confirmed = True
            pend.pending = False
            self._attach_event(pend)
        self._bullish = state
        # 新 leg 从翻转 bar 起跟踪本方向极值（多头 → 待确认 H，空头 → 待确认 L）；
        # 退化 leg（1 根 bar 即再翻转）允许与刚确认点同 idx，形成垂直段，
        # 两极值均为真实价格；同向不可能相邻（翻转必先确认反向 pending）
        if state and high is not None:
            self._pivots.append(Pivot(idx, ts, float(high), 1, pending=True))
        elif not state and low is not None:
            self._pivots.append(Pivot(idx, ts, float(low), -1, pending=True))

    def _track(self, idx: int, ts: int, high: Any, low: Any) -> None:
        if not self._pivots:
            return
        pend = self._pivots[-1]
        if pend.direction == 1 and high is not None and float(high) > pend.price:
            pend.price = float(high)
            pend.index = idx
            pend.timestamp = ts
        elif pend.direction == -1 and low is not None and float(low) < pend.price:
            pend.price = float(low)
            pend.index = idx
            pend.timestamp = ts

    def _attach_event(self, pivot: Pivot) -> None:
        """时间窗修正锚定：把触发本次确认的交叉事件回填 pivot_label/triggered_at_index。

        方向映射：翻转为多头（金叉 cross_up）确认的是前一段的**低点** L；
        翻转为空头（死叉 cross_down）确认的是前一段的**高点** H。
        """
        if self.time_window <= 0:
            return
        idx = pivot.index
        want = "cross_down" if pivot.direction == 1 else "cross_up"
        for ev in reversed(self._events):
            if idx - ev.index >= self.time_window:
                break
            if ev.triggered_at_index is None and ev.type == want:
                ev.triggered_at_index = idx
                ev.pivot_label = _pivot_label(pivot, self._pivots)
                if pivot.cross_index is None:
                    pivot.cross_index = ev.index
                break

    # ─── 查询接口 ───

    @property
    def pivots(self) -> List[Pivot]:
        """全部极值点（旧→新；末位为 pending 点，若存在；含编号 label 快照）"""
        self._refresh_labels()
        return list(self._pivots)

    @property
    def raw_pivots(self) -> List[Pivot]:
        """内部热路径专用：直接返回枢轴列表引用（**不刷新 label、不拷贝**）。

        仅供增量输出计算（ZigZagIndicator._compute_outputs / _recompute_tail）读取
        index/direction/price——这些路径不消费 label，走 ``pivots`` 属性会白白触发
        一次全量 label 刷新 + list 拷贝；预热逐 bar 累积即 O(n³) 卡顿主因。外部/
        展示 / SDK 仍需编号快照时用 ``pivots``。调用方**不得**就地修改返回列表。
        """
        return self._pivots

    @property
    def confirmed_pivots(self) -> List[Pivot]:
        self._refresh_labels()
        return [p for p in self._pivots if p.confirmed]

    @property
    def pending(self) -> Optional[Pivot]:
        self._refresh_labels()
        return self._pivots[-1] if self._pivots and self._pivots[-1].pending else None

    @property
    def bullish(self) -> Optional[bool]:
        """当前条件态（True 多头 / False 空头 / None 未定）"""
        return self._bullish

    @property
    def bar_count(self) -> int:
        return self._bar_count

    def highs(self, count: Optional[int] = None) -> List[Pivot]:
        """高点列表（新→旧：H1, H2, H3…；含 pending H1）"""
        self._refresh_labels()
        out = [p for p in reversed(self._pivots) if p.direction == 1]
        return out[:count] if count else out

    def lows(self, count: Optional[int] = None) -> List[Pivot]:
        """低点列表（新→旧：L1, L2, L3…；含 pending L1）"""
        self._refresh_labels()
        out = [p for p in reversed(self._pivots) if p.direction == -1]
        return out[:count] if count else out

    def high(self, n: int = 1) -> Optional[Pivot]:
        """Hn（n=1 最新高点，含 pending）"""
        hs = self.highs()
        return hs[n - 1] if 1 <= n <= len(hs) else None

    def low(self, n: int = 1) -> Optional[Pivot]:
        """Ln（n=1 最新低点，含 pending）"""
        ls = self.lows()
        return ls[n - 1] if 1 <= n <= len(ls) else None

    @property
    def cross_events(self) -> List[CrossEvent]:
        """交叉事件（旧→新）"""
        return list(self._events)

    @property
    def last_cross(self) -> Optional[CrossEvent]:
        return self._events[-1] if self._events else None

    @property
    def line_series(self) -> List[Dict[str, Any]]:
        """ZigZag 折线点列（旧→新，含 pending 尾点）"""
        return [{"index": p.index, "timestamp": p.timestamp, "price": p.price}
                for p in self._pivots]

    def _refresh_labels(self) -> None:
        # 单次反向遍历按方向累加计数，等价于逐点 _pivot_label（同方向从新→旧编号、
        # pending 恒为最新 → 1 号），但把 O(p²) 回溯降为 O(p) 整体扫描：pivots 属性
        # 逐 bar 被访问，旧写法在深历史预热时被放大成 O(n³) 卡顿（EVENT LOOP BLOCKED 根因）。
        h = lo = 0
        for p in reversed(self._pivots):
            if p.direction == 1:
                h += 1
                p.label = f"H{h}"
            else:
                lo += 1
                p.label = f"L{lo}"

    # ─── 快照 / 恢复 / 重置 ───

    def _snapshot(self) -> Any:
        # 枢轴仅末位（pending）会被 _track/_flip 就地改写、新确认只在其后追加；故只需
        # 备份末位枢轴 + 记录个数即可精确恢复，避免逐 bar 全量 replace 拷贝把 O(p) 快照
        # 放大成预热期 O(n²)（深历史 ZigZag 预热卡顿根因之一）。events 浅拷贝、series
        # 快照均为常量级（deque maxlen 有界），保持原语义不变。
        n = len(self._pivots)
        last = replace(self._pivots[-1]) if n else None
        return (
            n,
            last,
            self._bullish,
            self._bar_count,
            list(self._events),
            self._series.snapshot(),
        )

    def _restore(self, snap: Any) -> None:
        n, last, bullish, count, events, ssnap = snap
        del self._pivots[n:]                       # 丢弃本次确认/追加的枢轴
        if n and last is not None:
            # 还原被就地改写的末位（replace 拷贝：同一快照可被多次 restore）
            self._pivots[n - 1] = replace(last)
        self._bullish = bullish
        self._bar_count = count
        self._events = deque(events, maxlen=self._events.maxlen)
        self._series.restore(ssnap)

    def reset(self) -> None:
        self._pivots = []
        self._bullish = None
        self._events = deque(maxlen=self._events.maxlen)
        self._bar_count = 0
        self._last_ts = None
        self._snap = None
        self._series.reset()


def _pivot_label(p: Pivot, pivots: List[Pivot]) -> str:
    """动态编号：同方向从新→旧计数（pending 点恒为 1 号）"""
    n = 1
    for q in reversed(pivots):
        if q is p:
            break
        if q.direction == p.direction:
            n += 1
    return f"{'H' if p.direction == 1 else 'L'}{n}"


def _collect_inferred_pairs(tree: EvalNode, out: List[Tuple[str, str]]) -> None:
    """从比较节点推导可交叉序列对（ECmp 两侧均落回单原子时）。

    cross_type 显式配置但条件表达式内无交叉谓词时（如纯状态条件
    close > ma(close,5) and close > ema(close,5)），时间窗修正的事件扫描
    范围取自条件中的隐含比较对（close×ma、close×ema）。
    """
    if isinstance(tree, ECmp):
        if isinstance(tree.l, EAtom) and isinstance(tree.r, EAtom) \
                and tree.l.key != tree.r.key:
            out.append((tree.l.key, tree.r.key))
        return
    for child in getattr(tree, "xs", ()):
        _collect_inferred_pairs(child, out)
    for attr in ("l", "r", "x"):
        child = getattr(tree, attr, None)
        if isinstance(child, EvalNode):
            _collect_inferred_pairs(child, out)

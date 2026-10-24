# ruff: noqa: E501
"""ZIGZAG — 条件驱动 ZigZag 转折指标（IND-107 扩展形态）

非经典 ZigZag：不用 percent/points/ATR 偏离阈值，转折由**指标条件状态**驱动——
多头态期间跟踪区间最高 High 为 pending H1（随新 bar 重绘），条件翻转为空头态时
H1 确认并开启 pending L1（区间最低 Low），反之亦然；H/L 严格交替，已确认编号
不再改变（新确认使旧 H1→H2 平移）。核心状态机见
``klinequant.core.indicator_engine.zigzag``（策略 SDK 与图表共用同一实现）。

条件表达式（受限文法，见 zigzag.compile_cond）::

    bull_cond="cross_up(ma(close,5), ema(close,20))"   # 双线交叉
    bull_cond="rising(ma(close,20))"                   # 单条均线方向（新值>前值）
    bull_cond='close > ma(close,5) and close > ema(close,5)'  # 纯状态条件
    bull_cond='indic("MR_Y_MAIN", field="M_4X") > indic("MR_Y_MAIN", field="E_4X")'

参数（全部 JSON 安全标量，ind_key 序列化稳定）：
    bull_cond    多头态条件表达式（成立 → 多头 leg，跟踪待确认高点 H）
    bear_cond    空头态条件表达式（空串 = not bull_cond；注意交叉型 bull 条件的
                 逻辑反在每个非交叉 bar 恒成立，会把多头 leg 压缩为 1 根，
                 故默认配对显式 cross_down；状态型条件空串取反即自然空头态）
    cross_type   时间窗修正扫描的交叉类型 cross_up/cross_down/cross/golden/death
                 （空 = 仅扫描条件表达式内出现的交叉谓词对）
    tolerance    交叉/方向判定容差（抑制浮点抖动误判）
    time_window  时间窗修正根数（0 = 关闭）：严格条件未定时，窗内发生过
                 cross_type 交叉事件即认定翻转成立

输出字段（sparse 稀疏语义，逐字段 null 透传）：
    ZZ_HIGH  极值高点（H 转折点所在 bar 输出价格，其余 null；plot:'scatter' 前端不建线）
    ZZ_LOW   极值低点（L 转折点所在 bar 输出价格，其余 null；plot:'scatter' 前端不建线）
    ZZ_LINE  ZigZag 折线（相邻极值点间线性插值；pending 尾段随重绘更新，
             经 engine.patch_series 修补历史序列点，契约见该方法 docstring；
             plot:'dirline' 按 leg 方向分段变色——上升段绿 / 下降段红）
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

import polars as pl

from klinequant.core.indicator_engine.base import IndicatorBase
from klinequant.core.indicator_engine.registry import register_indicator
from klinequant.core.indicator_engine.zigzag import Pivot, ZigZagCore

logger = logging.getLogger(__name__)

_FIELDS = ("ZZ_HIGH", "ZZ_LOW", "ZZ_LINE")


@register_indicator
class ZigZagIndicator(IndicatorBase):
    """条件驱动 ZigZag（增量指标，快照法幂等；重绘经 series_patcher 修补序列）"""

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        super().__init__(params)
        p = self._params
        self._core = ZigZagCore(
            bull_cond=str(p.get("bull_cond") or "cross_up(ma(close,5), ema(close,5))"),
            bear_cond=str(p.get("bear_cond") or "") or None,
            cross_type=str(p.get("cross_type") or "") or None,
            tolerance=float(p.get("tolerance") or 0.0),
            time_window=int(p.get("time_window") or 0),
        )
        # 引擎注入的历史序列修补回调（重绘型指标契约，见 engine.patch_series）
        self.series_patcher: Optional[Callable[[Dict[int, Dict[str, Any]]], None]] = None
        self._reset_runtime()

    # ─── 元信息 ───

    @property
    def name(self) -> str:
        return "ZIGZAG"

    @property
    def min_periods(self) -> int:
        return self._core.min_periods

    @property
    def default_params(self) -> Dict[str, Any]:
        return {
            "bull_cond": "cross_up(ma(close,5), ema(close,5))",
            "bear_cond": "cross_down(ma(close,5), ema(close,5))",
            "cross_type": "",
            "tolerance": 0.0,
            "time_window": 0,
        }

    @property
    def supports_incremental(self) -> bool:
        return True

    # 降级场景（源深度 < 全局预热）逐字段部分输出；sparse 语义由本类自管
    supports_partial = True

    @property
    def display_meta(self) -> Dict[str, Any]:
        return {
            "fields": list(_FIELDS),
            "range": "price",
            "pane": "main",
            "desc": "条件驱动 ZigZag（指标交叉/状态转折，H1/L1 动态编号，尾段可重绘）",
            "style": [
                # ZZ_HIGH/ZZ_LOW 为 scatter 语义（仅转折 bar 有值）：plot:'scatter' 声明前端不建可见连线
                # （本 lwc 构建丢弃 whitespace 跨空档直连，喂值会把散点画成贯穿包络线），值仅供图例末值
                {"color": "#26a69a", "line_width": 2, "plot": "scatter"},   # ZZ_HIGH
                {"color": "#ef5350", "line_width": 2, "plot": "scatter"},   # ZZ_LOW
                # ZZ_LINE 变色线：前端 DirLinePrimitive 按「值 vs 前值」方向逐段着色（上升腿递增→绿、下降腿递减→红）
                {"plot": "dirline", "dir_colors": ["#26a69a", "#ef5350"], "line_width": 2},   # ZZ_LINE
            ],
        }

    # ─── 运行态 ───

    def _reset_runtime(self) -> None:
        self._rows: List[Dict[str, Any]] = []        # bar 缓存（timestamp/high/low）
        self._vals: List[Dict[str, Optional[float]]] = []   # 每根输出值缓存
        self._patches: Dict[int, Dict[str, Any]] = {}       # 本 bar 产生的历史修补
        self._last_ts: Optional[int] = None
        self._snap: Optional[Any] = None

    def reset(self) -> None:
        super().reset()
        self._core.reset()
        self._reset_runtime()

    @property
    def core(self) -> ZigZagCore:
        """核心状态机（策略 SDK 经 engine.find_indicator 取用：极值点/交叉事件）"""
        return self._core

    # ─── 增量路径（IND-101 快照法） ───

    def update_bar(
        self, bar: Dict[str, Any], is_closed: bool, partial: bool = False
    ) -> Optional[Dict[str, Any]]:
        ts = int(bar["timestamp"])
        if self._last_ts is not None and ts < self._last_ts:
            return None  # 乱序历史 bar：增量路径不处理
        self._patches = {}
        if ts != self._last_ts:
            self._snap = self._snapshot()
        elif self._snap is not None:
            self._restore(self._snap)

        self._core.update_bar(bar, is_closed)
        self._rows.append({"timestamp": ts, "high": bar.get("high"), "low": bar.get("low")})
        idx = len(self._rows) - 1
        out = self._compute_outputs(idx)
        self._vals.append(dict(out))
        self._recompute_tail(idx)

        self._last_ts = ts
        if len(self._rows) >= self.min_periods:
            self._warmed_up = True

        if not partial and not self._warmed_up:
            return None
        values = {k: (None if v is None else float(v)) for k, v in out.items()}
        # sparse 语义：逐字段合并末值（供图例/last_values 显示）
        merged = dict(self._last_values or {})
        merged.update({k: v for k, v in values.items() if v is not None})
        self._last_values = merged or None
        self._emit_patches()
        return values

    # ─── 批量路径（回测/一致性核对：与增量同源重放） ───

    def _calculate(self, df: pl.DataFrame) -> pl.DataFrame:
        self.reset()
        n = len(df)
        cols: Dict[str, List[Optional[float]]] = {f: [None] * n for f in _FIELDS}
        ts_index: Dict[int, int] = {}
        for i, row in enumerate(df.iter_rows(named=True)):
            values = self.update_bar(row, True, partial=True)
            ts_index[int(row["timestamp"])] = i
            if values:
                for f in _FIELDS:
                    cols[f][i] = values.get(f)
            # 重绘修补回写历史行（重放结束后该列即最终一致状态）
            for pts, fields in self._patches.items():
                j = ts_index.get(int(pts))
                if j is not None:
                    for f, v in fields.items():
                        cols[f][j] = v
        return df.with_columns([
            pl.Series(f"{self.name}_{f}", cols[f], dtype=pl.Float64) for f in _FIELDS
        ])

    # ─── 输出计算 ───

    def _compute_outputs(self, idx: int) -> Dict[str, Optional[float]]:
        pivots = self._core.raw_pivots  # 旧→新，含 pending；热路径只读 index/dir/price，不需 label
        out: Dict[str, Optional[float]] = {"ZZ_HIGH": None, "ZZ_LOW": None, "ZZ_LINE": None}
        # 退化 leg：同 idx 可能同时有确认点与新 pending 点（H+L），两标记都输出
        for p in pivots[-4:]:
            if p.index != idx:
                continue
            if p.direction == 1:
                out["ZZ_HIGH"] = p.price if out["ZZ_HIGH"] is None \
                    else max(out["ZZ_HIGH"], p.price)
            else:
                out["ZZ_LOW"] = p.price if out["ZZ_LOW"] is None \
                    else min(out["ZZ_LOW"], p.price)
        out["ZZ_LINE"] = self._line_value_at(idx, pivots)
        return out

    @staticmethod
    def _line_value_at(idx: int, pivots: List[Pivot]) -> Optional[float]:
        """ZZ_LINE：仅末段（最后两枢轴之间，含 pending 尾点）线性插值，其余 null。

        历史段一经确认即稳定，其插值值在确认当时已写入序列并被后续修补固定；
        每 bar 只需重算末段（pending 重绘 / 新确认延长的影响范围）。
        """
        if len(pivots) < 2:
            return None
        p0, p1 = pivots[-2], pivots[-1]
        if idx < p0.index or idx > p1.index:
            return None
        span = p1.index - p0.index
        if span <= 0:
            return p0.price
        return p0.price + (p1.price - p0.price) * (idx - p0.index) / span

    def _recompute_tail(self, cur_idx: int) -> None:
        """重算受影响历史 bar 的输出（pending 重绘移动/新确认延长末段），
        差异写入 self._patches（timestamp → 字段增量）供序列修补。"""
        pivots = self._core.raw_pivots
        if len(pivots) < 2:
            return
        start = max(0, pivots[-2].index)
        # 重算区间内的 H/L 标记映射（index → 价格；含同 idx 退化叠加）。
        # 枢轴按 index 升序，仅末段 [start, cur_idx] 可能命中：反向扫描遇 index<start 即停，
        # 避免逐 bar 全量遍历枢轴把预热放大成 O(n·p)（max/min 聚合与遍历顺序无关，结果一致）。
        h_map: Dict[int, float] = {}
        l_map: Dict[int, float] = {}
        for p in reversed(pivots):
            if p.index < start:
                break
            if p.index > cur_idx:
                continue
            if p.direction == 1:
                h_map[p.index] = max(h_map.get(p.index, p.price), p.price)
            else:
                l_map[p.index] = min(l_map.get(p.index, p.price), p.price)
        for i in range(start, cur_idx):
            new_vals: Dict[str, Optional[float]] = {
                "ZZ_HIGH": h_map.get(i),
                "ZZ_LOW": l_map.get(i),
                "ZZ_LINE": self._line_value_at(i, pivots),
            }
            stored = self._vals[i]
            diff: Dict[str, Any] = {}
            for f in _FIELDS:
                if (stored.get(f) or None) != (new_vals[f] or None):
                    diff[f] = new_vals[f]
            if diff:
                stored.update(diff)
                self._patches[self._rows[i]["timestamp"]] = diff

    def _emit_patches(self) -> None:
        if self._patches and self.series_patcher is not None:
            try:
                self.series_patcher(self._patches)
            except Exception as e:   # 修补失败不影响增量输出（下次重绘自愈）
                logger.warning(f"ZIGZAG series patch failed: {e}")

    # ─── 快照 / 恢复（同 ts forming bar 幂等重放） ───

    def _snapshot(self) -> Any:
        # _vals 仅末段（当前 leg [pivots[-2].index, len)）会被 _recompute_tail 就地改写、
        # 每 bar 只追加 1 条；故只备份末段 + 记录长度即可精确恢复，避免逐 bar 全量拷贝把
        # O(n) 快照放大成预热期 O(n²)（深历史 ZigZag 预热卡顿根因之一）。备份下界取快照时
        # pivots[-2].index：本 bar 应用后 _recompute_tail 的 start（更新后 pivots[-2].index）
        # 恒 >= 此值（track 取 P_{m-1}、flip 取 P_m，均不早于快照时的 P_{m-1}），覆盖全部可改写位。
        pivots = self._core._pivots
        n = len(self._rows)
        k = max(0, min(pivots[-2].index if len(pivots) >= 2 else 0, n))
        tail = [dict(self._vals[i]) for i in range(k, n)]
        return (self._core._snapshot(), n, k, tail, self._last_ts)

    def _restore(self, snap: Any) -> None:
        core_snap, n, k, tail, last_ts = snap
        self._core._restore(core_snap)
        del self._rows[n:]
        del self._vals[n:]
        for j, v in enumerate(tail):
            self._vals[k + j] = dict(v)   # 拷贝防串改：同一快照可被多次 restore（forming bar 反复推送）
        self._last_ts = last_ts

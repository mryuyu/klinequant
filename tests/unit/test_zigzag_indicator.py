"""条件驱动 ZigZag 指标单测（IND-107 扩展形态）

覆盖《条件驱动 ZigZag 指标实现计划》验收项：
  1. 极值点 H/L 严格交替、confirmed 后 (index/ts/price) 稳定、新确认使旧 H1→H2 平移；
  2. pending 点随新 bar 重绘（价格/index 更新，不产生新编号）；
  3. tolerance 抑制浮点抖动误判（临界差值 < tol 不判交叉）；
  3b. 单序列方向条件 rising/falling(ma) 驱动翻转、走平（差值≤tol）不翻转；
  4. time_window 修正（严格条件不成立但窗内有交叉 → 翻转成立；窗为 0 不修正）；
  5. 交叉事件字段完整（类型/位置/序列/触发 pivot 编号）；
  6. 快照幂等（同 ts forming bar 重复推送结果一致）；
  7. ZIGZAG 注册指标经 IndicatorEngine.warmup + update_kline 全链路（sparse
     输出、ZZ_LINE 插值、批量 _calculate 与增量一致）；
  8. SDK：indic.zigzag() 声明后 zigzag_high()/zigzag_low()/last_cross 可用。

全部用合成 bars 驱动（正弦 + 趋势 / 手写段落），不依赖 MT5 / 网络。
"""
from __future__ import annotations

import math
from decimal import Decimal

import pytest

import klinequant.custom_indicators  # noqa: F401  确保 ZIGZAG 注册进全局注册表
from klinequant.core.indicator_engine.engine import IndicatorEngine
from klinequant.core.indicator_engine.zigzag import (
    CROSS_TYPES,
    CrossEvent,
    Pivot,
    ZigZagCore,
    compile_cond,
)
from klinequant.custom_indicators.zigzag import ZigZagIndicator
from klinequant.kqsdk.backtest_feed import BacktestDataFeed
from klinequant.kqsdk.indic import (
    IndicApi,
    ZigZagView,
    bars_to_df,
    zigzag_high,
    zigzag_low,
)
from klinequant.protocol.types import Kline

BASE_TS = 1_600_000_000_000
SYM, EXCH, TF = "TESTUSD", "mt5", "1m"

# 双线交叉（默认，MA/EMA 金叉死叉）与持续状态条件（干净的多 bar leg）
COND_CROSS = "cross_up(ma(close,5), ema(close,5))"
COND_STATE = "ma(close,3) > ma(close,8)"


# ─── 合成 bar 脚手架 ───


def _mk_bars(n: int, amp: float = 0.02, period: float = 18.0, trend: float = 0.0004):
    """正弦 + 线性趋势合成 bar（多次穿越均线 → 状态机反复翻转）。"""
    bars = []
    prev_close = 1.10
    for i in range(n):
        close = 1.10 + amp * math.sin(2 * math.pi * i / period) + i * trend
        open_ = prev_close
        high = max(open_, close) + 0.0008
        low = min(open_, close) - 0.0008
        bars.append({
            "symbol": SYM, "period": TF, "timestamp": BASE_TS + i * 60000,
            "open": open_, "high": high, "low": low, "close": close, "volume": 1000.0,
        })
        prev_close = close
    return bars


def _mk_bars_from_closes(closes: list[float], pad: float = 0.5):
    """按给定收盘价序列构造 bar（high/low = close ± pad，open = 前收）。"""
    bars = []
    prev = closes[0]
    for i, c in enumerate(closes):
        bars.append({
            "symbol": SYM, "period": TF, "timestamp": BASE_TS + i * 60000,
            "open": prev, "high": max(prev, c) + pad, "low": min(prev, c) - pad,
            "close": c, "volume": 1000.0,
        })
        prev = c
    return bars


def _bar_to_kline(bar: dict, is_closed: bool = True) -> Kline:
    return Kline(
        symbol=SYM, exchange=EXCH, timeframe=TF, timestamp=int(bar["timestamp"]),
        open=Decimal(str(bar["open"])), high=Decimal(str(bar["high"])),
        low=Decimal(str(bar["low"])), close=Decimal(str(bar["close"])),
        volume=Decimal(str(bar["volume"])), quote_volume=Decimal("0"),
        trade_count=0, is_closed=is_closed,
    )


def _run(core: ZigZagCore, bars: list[dict]) -> None:
    for b in bars:
        core.update_bar(b, True)


def _directions(pivots: list[Pivot]) -> list[int]:
    return [p.direction for p in pivots]


# ═══════════════════════════════════════════════════════════
# 表达式编译器（受限文法）
# ═══════════════════════════════════════════════════════════


def test_compile_cond_rejects_illegal_grammar():
    """非法文法抛 ValueError（无 eval，节点白名单）。"""
    with pytest.raises(ValueError):
        compile_cond("__import__('os').system('x')", 0.0)   # 非白名单调用
    with pytest.raises(ValueError):
        compile_cond("unknownvar > 1", 0.0)                 # 未知变量
    with pytest.raises(ValueError):
        compile_cond("close > ", 0.0)                       # 语法错误


def test_compile_cond_accepts_supported_atoms_and_predicates():
    """受支持的序列原子/谓词均可编译（含 indic 任意注册表指标）。"""
    for expr in (
        "close > ma(close,5)",
        "cross_up(ma(close,5), ema(close,5))",
        "golden(macd_dif(12,26,9), macd_dea(12,26,9))",
        "rising(ma(close,20))",
        "ref(close,2) < close",
        "boll_upper(20,2.0) > close and rsi(14) < 70",
        'indic("DEMA", field="DEMA", period=10) > close',   # def 式自定义指标
        'indic("MACD", field="DIF", fast_period=2, slow_period=5, signal_period=2) > 0',
    ):
        tree, specs, _pairs = compile_cond(expr, 0.0)
        assert tree is not None
        assert specs  # 至少收集到一个序列原子


def test_cross_type_validation():
    """cross_type 必须为 CROSS_TYPES 之一或空。"""
    with pytest.raises(ValueError):
        ZigZagCore(bull_cond=COND_CROSS, cross_type="bogus")
    with pytest.raises(ValueError):
        ZigZagCore(bull_cond=COND_CROSS, tolerance=-1.0)
    with pytest.raises(ValueError):
        ZigZagCore(bull_cond=COND_CROSS, time_window=-1)
    assert "cross_up" in CROSS_TYPES


# ═══════════════════════════════════════════════════════════
# 验收 1：H/L 严格交替 + confirmed 稳定 + 编号平移
# ═══════════════════════════════════════════════════════════


def test_pivots_strictly_alternate():
    """极值点 H/L 严格交替（相邻方向必相反）。"""
    core = ZigZagCore(bull_cond=COND_STATE)
    _run(core, _mk_bars(200))
    pivots = core.pivots
    assert len(pivots) >= 3
    for a, b in zip(pivots, pivots[1:]):
        assert a.direction != b.direction, f"相邻极值点未交替：{a.label}->{b.label}"


def test_confirmed_pivot_data_stable_across_bars():
    """confirmed 极值点一经确认，其 (index/timestamp/price/direction) 不再改变。"""
    core = ZigZagCore(bull_cond=COND_STATE)
    seen: dict[int, tuple] = {}       # index → 首次确认时的不可变数据
    for b in _mk_bars(200):
        core.update_bar(b, True)
        for p in core.confirmed_pivots:
            snap = (p.timestamp, round(p.price, 12), p.direction)
            if p.index in seen:
                assert seen[p.index] == snap, f"已确认点 {p.index} 数据发生变化"
            else:
                seen[p.index] = snap
    assert len(seen) >= 2


def test_labels_numbered_new_to_old():
    """highs()/lows() 编号新→旧 H1/H2/H3…、L1/L2/L3…（新确认使旧 H1→H2 平移）。"""
    core = ZigZagCore(bull_cond=COND_STATE)
    _run(core, _mk_bars(300))
    hs, ls = core.highs(), core.lows()
    assert len(hs) >= 2 and len(ls) >= 2
    assert [p.label for p in hs] == [f"H{i+1}" for i in range(len(hs))]
    assert [p.label for p in ls] == [f"L{i+1}" for i in range(len(ls))]
    # high(n)/low(n) 与列表一致
    assert core.high(1) is hs[0] and core.high(2) is hs[1]
    assert core.low(1) is ls[0] and core.low(2) is ls[1]
    # 新→旧时间递减
    assert all(x.timestamp >= y.timestamp for x, y in zip(hs, hs[1:]))


def test_high1_shifts_to_high2_on_new_confirmation():
    """新确认高点使原 H1 平移为 H2（编号动态迁移）。"""
    core = ZigZagCore(bull_cond=COND_STATE)
    bars = _mk_bars(300)
    first_high_idx = None
    for b in bars:
        core.update_bar(b, True)
        if first_high_idx is None and len(core.highs()) >= 1:
            h1 = core.highs()[0]
            if h1.confirmed:
                first_high_idx = h1.index
        # 一旦出现更新的确认高点，最早那个应已平移到 H2 及以后
        if first_high_idx is not None and len(core.highs()) >= 2:
            older = [p for p in core.highs() if p.index == first_high_idx]
            if older and older[0] is not core.highs()[0]:
                assert older[0].label in ("H2", "H3", "H4")
                break
    assert first_high_idx is not None


# ═══════════════════════════════════════════════════════════
# 验收 2：pending 点重绘
# ═══════════════════════════════════════════════════════════


def test_pending_redraws_without_new_label():
    """多头 leg 内价格创新高 → pending 点价格/index 更新，但极值点总数不增、仍 pending。"""
    core = ZigZagCore(bull_cond=COND_STATE)
    # 先喂到进入多头态
    bars = _mk_bars(60)
    for b in bars:
        core.update_bar(b, True)
        if core.bullish is True and core.pending is not None:
            break
    assert core.bullish is True and core.pending is not None
    n_before = len(core.pivots)
    # 构造持续创新高的 bar（同一 leg 内），pending 应被重绘而非新增点
    last = bars[core.bar_count - 1]
    rising = []
    price = last["close"]
    for i in range(5):
        price += 0.01
        rising.append({
            "symbol": SYM, "period": TF,
            "timestamp": BASE_TS + (core.bar_count + i) * 60000,
            "open": price - 0.005, "high": price + 0.002, "low": price - 0.008,
            "close": price, "volume": 1000.0,
        })
    prev_price = core.pending.price
    for rb in rising:
        # 只要仍处多头态，pending 高点应随新高上移且不新增极值点
        if core.bullish is not True:
            break
        core.update_bar(rb, True)
        pend = core.pending
        if pend is not None and pend.direction == 1:
            assert pend.price >= prev_price
            prev_price = pend.price
            assert len(core.pivots) == n_before   # 未新增编号
            assert pend.pending and not pend.confirmed


# ═══════════════════════════════════════════════════════════
# 验收 3：tolerance 抑制抖动
# ═══════════════════════════════════════════════════════════


def test_tolerance_suppresses_cross():
    """tolerance 足够大时交叉判定被抑制（临界差值 < tol 不判交叉）。"""
    bars = _mk_bars(150)
    tight = ZigZagCore(bull_cond=COND_CROSS, tolerance=0.0)
    loose = ZigZagCore(bull_cond=COND_CROSS, tolerance=1.0)  # >> 价格尺度(~1.1)
    _run(tight, bars)
    _run(loose, bars)
    # 容差为 0：cross_up 可达 → 出现多头态与多个极值点
    assert len(tight.pivots) >= 2
    # 容差过大：a > b + tol 永不成立 → 从不进入多头态，极值点极少
    assert len(loose.pivots) < len(tight.pivots)
    assert all(p.direction == -1 for p in loose.pivots)   # 恒空头 leg（仅跟踪 L）


def test_tolerance_flat_no_flip():
    """缓慢上升但差值 ≤ tol：rising 判定被抑制（走平不翻转）。"""
    closes = [10.0 + 0.3 * i for i in range(20)]   # ma 每根 +0.3
    bars = _mk_bars_from_closes(closes)
    zero = ZigZagCore(bull_cond="rising(ma(close,3))", bear_cond="falling(ma(close,3))",
                      tolerance=0.0)
    wide = ZigZagCore(bull_cond="rising(ma(close,3))", bear_cond="falling(ma(close,3))",
                      tolerance=0.5)
    _run(zero, bars)
    _run(wide, bars)
    assert zero.bullish is True      # 0.3 > 0 → rising 成立
    assert wide.bullish is None      # 0.3 > 0.5 假，且非 falling → 状态未定


# ═══════════════════════════════════════════════════════════
# 验收 3b：单序列方向条件 rising/falling
# ═══════════════════════════════════════════════════════════


def test_single_series_direction_flips():
    """单条均线方向：rising→多头、走平→保持、falling→空头。"""
    closes = (
        [10, 10, 10, 10] +          # 预热 + 走平
        [11, 12, 13, 14, 15] +      # 上升段
        [15, 15, 15] +              # 走平段（差值 0 → 不翻转）
        [14, 13, 12, 11, 10]        # 下降段
    )
    bars = _mk_bars_from_closes([float(c) for c in closes])
    core = ZigZagCore(bull_cond="rising(ma(close,3))", bear_cond="falling(ma(close,3))")
    states: list[bool | None] = []
    for b in bars:
        core.update_bar(b, True)
        states.append(core.bullish)

    up_states = states[4:12]         # 上升 + ma 走平段（idx 4~11）
    assert True in up_states         # 上升段进入多头态
    assert states[4] is True         # rising 首次成立
    assert states[11] is True        # ma 走平（差值 0）仍保持多头，未翻转
    assert states[12] is False       # 转跌 → falling → 空头
    assert all(s is False for s in states[12:])   # 下降段稳定空头


# ═══════════════════════════════════════════════════════════
# 验收 4：time_window 修正
# ═══════════════════════════════════════════════════════════


def test_time_window_correction_enables_flips():
    """严格条件恒不成立（bull/bear 恒 False）时：窗为 0 永不定向、
    无极值点；窗 >0 时靠窗内交叉事件定向（翻转成立）。"""
    bars = _mk_bars(200)
    # 矛盾条件：close 不可能同时 > 和 < 同一均线 → 严格态恒 False（永不定向）
    bull = "close > ma(close,5) and close < ma(close,5)"
    bear = "close > ema(close,5) and close < ema(close,5)"
    off = ZigZagCore(bull_cond=bull, bear_cond=bear, cross_type="cross", time_window=0)
    on = ZigZagCore(bull_cond=bull, bear_cond=bear, cross_type="cross", time_window=4)
    _run(off, bars)
    _run(on, bars)
    # 窗为 0：严格条件从不成立 → 未定向、无极值点
    assert off.bullish is None
    assert off.pivots == []
    assert all(e.triggered_at_index is None for e in off.cross_events)
    # 窗 >0：窗内交叉事件驱动翻转 → 定向并生成极值点（交叉确实被检测到）
    assert off.cross_events, "应检测到交叉事件作为修正证据"
    assert on.bullish in (True, False)
    assert len(on.pivots) > 0
    assert len(on.pivots) >= len(off.pivots)
    # 修正锚定：至少一个交叉事件被消费（triggered_at_index / pivot_label 回填）
    consumed = [e for e in on.cross_events if e.triggered_at_index is not None]
    assert consumed, "时间窗修正应至少消费一个交叉事件"
    assert any(e.pivot_label for e in consumed)


def test_window_zero_no_correction():
    """time_window=0 时严格条件未定即返回 None（不因交叉事件修正）。"""
    bars = _mk_bars(120)
    bull = "close > ma(close,5) and close > ema(close,5)"
    bear = "close < ma(close,5) and close < ema(close,5)"
    core = ZigZagCore(bull_cond=bull, bear_cond=bear, cross_type="cross", time_window=0)
    _run(core, bars)
    # 未开启修正：任何交叉事件都不应被标记为触发翻转
    assert all(e.triggered_at_index is None for e in core.cross_events)


# ═══════════════════════════════════════════════════════════
# 验收 5：交叉事件字段完整
# ═══════════════════════════════════════════════════════════


def test_cross_event_fields_complete():
    """交叉事件含类型/位置/序列/两序列值/插值价，last_cross 与列表尾一致。"""
    core = ZigZagCore(bull_cond="cross_up(ma(close,5), ema(close,5))",
                      bear_cond="cross_down(ma(close,5), ema(close,5))",
                      cross_type="cross")
    _run(core, _mk_bars(200))
    events = core.cross_events
    assert events, "应检测到交叉事件"
    for e in events:
        assert isinstance(e, CrossEvent)
        assert e.type in ("cross_up", "cross_down")
        assert e.index >= 0 and e.timestamp == BASE_TS + e.index * 60000
        assert e.a and e.b and e.a != e.b
        assert e.a_value is not None and e.b_value is not None
        assert e.cross_price is not None
    assert core.last_cross is events[-1]


# ═══════════════════════════════════════════════════════════
# 验收 6：快照幂等（同 ts forming bar 重复推送）
# ═══════════════════════════════════════════════════════════


def test_snapshot_idempotent_same_ts():
    """同 ts 重复推送（forming bar）：结果与单次推送一致（快照 restore 幂等）。"""
    bars = _mk_bars(80)
    once = ZigZagCore(bull_cond=COND_STATE)
    multi = ZigZagCore(bull_cond=COND_STATE)
    for b in bars[:-1]:
        once.update_bar(b, True)
        multi.update_bar(b, True)
    last = dict(bars[-1])
    once.update_bar(last, True)
    # 同一根 forming bar 反复推送（值不变）→ 应幂等
    for _ in range(5):
        multi.update_bar(last, False)
    multi.update_bar(last, True)

    def sig(core: ZigZagCore):
        return [(p.index, p.timestamp, round(p.price, 12), p.direction, p.confirmed)
                for p in core.pivots]

    assert sig(once) == sig(multi)
    assert once.bullish == multi.bullish
    assert once.bar_count == multi.bar_count


def test_forming_bar_extreme_updates_pending():
    """forming bar 盘中变动（更极端 high）→ pending 点随之更新，重复推送仍幂等。"""
    core = ZigZagCore(bull_cond=COND_STATE)
    bars = _mk_bars(60)
    for b in bars:
        core.update_bar(b, True)
        if core.bullish is True and core.pending is not None:
            break
    ts = BASE_TS + core.bar_count * 60000
    forming = {"symbol": SYM, "period": TF, "timestamp": ts,
               "open": 1.1, "high": 1.5, "low": 1.0, "close": 1.4, "volume": 10.0}
    core.update_bar(forming, False)
    snap1 = [(p.index, round(p.price, 12)) for p in core.pivots]
    core.update_bar(forming, False)   # 重复同值
    snap2 = [(p.index, round(p.price, 12)) for p in core.pivots]
    assert snap1 == snap2


def test_registry_sub_instance_aligned_with_display_on_forming():
    """v9.146：条件源子实例（indic() 原子）与同名独立显示实例 forming 重复推送逐 tick 同值同翻转。

    回归：_RegistryComp.restore 曾把父层 bar 起始捕获的旧 _snap 回写子实例，
    子实例状态多回退一根（缺上一 bar 收盘贡献），rising/falling 与显示实例
    变色口径慢性分叉（「条件没成立却绘制」/「未变色却翻转」双向脱节）。
    """
    from klinequant.core.indicator_engine.registry import get_registry

    cond = 'indic("ZLINE", field="ZLINE")'
    core = ZigZagCore(bull_cond=f"rising({cond})", bear_cond=f"falling({cond})")
    ref = get_registry().create("ZLINE", {})
    bars = _mk_bars(80, amp=0.05, period=24.0)
    prev_z = None
    for b in bars[:-1]:
        core.update_bar(b, True)
        vals = ref.update_bar(dict(b), True)
        if vals:
            prev_z = vals["ZLINE"]
    assert prev_z is not None   # 预热完成，后续断言有效
    last = dict(bars[-1])
    for tick in (0.0, 0.03, -0.04, 0.02):
        last["close"] = bars[-1]["close"] + tick
        last["high"] = max(last["high"], last["close"])
        last["low"] = min(last["low"], last["close"])
        core.update_bar(last, False)
        vals = ref.update_bar(dict(last), False)
        z = vals["ZLINE"] if vals else None
        if z is None:
            continue
        # 显示口径：forming 段变色 = 新值 vs 前收盘；zigzag 状态须逐 tick 同步
        expect = core.bullish if z == prev_z else (z > prev_z)
        assert core.bullish == expect
    # 收盘推送：状态与收盘口径一致
    closed = dict(bars[-1])
    core.update_bar(closed, True)
    vals = ref.update_bar(dict(closed), True)
    z = vals["ZLINE"] if vals else None
    if z is not None and z != prev_z:
        assert core.bullish == (z > prev_z)


# ═══════════════════════════════════════════════════════════
# 验收 7：ZIGZAG 注册指标 + 引擎全链路
# ═══════════════════════════════════════════════════════════

_ZZ_PARAMS = {
    "bull_cond": COND_STATE, "bear_cond": "", "cross_type": "",
    "tolerance": 0.0, "time_window": 0,
}


def test_zigzag_indicator_batch_calculate_sparse():
    """_calculate 输出三列 sparse：ZZ_HIGH/ZZ_LOW 稀疏、ZZ_LINE 覆盖折线段。"""
    bars = _mk_bars(200)
    df = bars_to_df(bars)
    ind = ZigZagIndicator(dict(_ZZ_PARAMS))
    out = ind._calculate(df)
    assert "ZIGZAG_ZZ_HIGH" in out.columns
    assert "ZIGZAG_ZZ_LOW" in out.columns
    assert "ZIGZAG_ZZ_LINE" in out.columns
    highs = out["ZIGZAG_ZZ_HIGH"].to_list()
    lows = out["ZIGZAG_ZZ_LOW"].to_list()
    line = out["ZIGZAG_ZZ_LINE"].to_list()
    # sparse：高点/低点只在转折 bar 有值（远少于总根数）
    assert 0 < sum(v is not None for v in highs) < len(bars)
    assert 0 < sum(v is not None for v in lows) < len(bars)
    # ZZ_LINE 连续覆盖折线段（非空数量 >= 极值点跨度）
    assert sum(v is not None for v in line) >= sum(v is not None for v in highs)


def test_zigzag_line_interpolates_between_pivots():
    """ZZ_LINE 在极值点 index 处等于该点价格（线性插值端点）。"""
    ind = ZigZagIndicator(dict(_ZZ_PARAMS))
    bars = _mk_bars(200)
    out = ind._calculate(bars_to_df(bars))
    line = out["ZIGZAG_ZZ_LINE"].to_list()
    ts_col = out["timestamp"].to_list()
    ts_to_i = {int(ts): i for i, ts in enumerate(ts_col)}
    pivots = ind.core.pivots
    # 退化 leg 可能同 index 多个极值点（垂直段），ZZ_LINE 只保留一个端点，跳过
    idx_count: dict[int, int] = {}
    for p in pivots:
        idx_count[p.index] = idx_count.get(p.index, 0) + 1
    checked = 0
    for p in pivots[1:-1]:        # 跳过首尾（可能落在折线端点外）
        if idx_count.get(p.index, 0) > 1:
            continue
        i = ts_to_i.get(int(p.timestamp))
        if i is None or line[i] is None:
            continue
        assert line[i] == pytest.approx(p.price, rel=1e-9, abs=1e-9)
        checked += 1
    assert checked >= 2


def test_batch_matches_incremental():
    """批量 _calculate 与逐 bar 增量输出逐字段一致（回测/实盘同源）。"""
    bars = _mk_bars(200)
    batch = ZigZagIndicator(dict(_ZZ_PARAMS))
    out = batch._calculate(bars_to_df(bars))
    ts_col = out["timestamp"].to_list()
    batch_map = {
        int(ts_col[i]): (out["ZIGZAG_ZZ_HIGH"][i], out["ZIGZAG_ZZ_LOW"][i],
                         out["ZIGZAG_ZZ_LINE"][i])
        for i in range(len(bars))
    }

    inc = ZigZagIndicator(dict(_ZZ_PARAMS))
    inc_map: dict[int, tuple] = {}
    for b in bars:
        vals = inc.update_bar(b, True, partial=True)
        # 增量输出叠加历史修补（与 _calculate 回写同法）
        for pts, fields in inc._patches.items():
            key = int(pts)
            cur = list(inc_map.get(key, (None, None, None)))
            for fname, v in fields.items():
                cur[{"ZZ_HIGH": 0, "ZZ_LOW": 1, "ZZ_LINE": 2}[fname]] = v
            inc_map[key] = tuple(cur)
        if vals:
            inc_map[int(b["timestamp"])] = (
                vals.get("ZZ_HIGH"), vals.get("ZZ_LOW"), vals.get("ZZ_LINE"))

    for ts, (bh, bl, bln) in batch_map.items():
        ih, il, iln = inc_map.get(ts, (None, None, None))
        assert (bh is None) == (ih is None)
        assert (bl is None) == (il is None)
        assert (bln is None) == (iln is None)
        if bh is not None:
            assert bh == pytest.approx(ih, rel=1e-9, abs=1e-9)
        if bl is not None:
            assert bl == pytest.approx(il, rel=1e-9, abs=1e-9)
        if bln is not None:
            assert bln == pytest.approx(iln, rel=1e-9, abs=1e-9)


def test_engine_full_chain_warmup_and_update():
    """引擎全链路：ensure + warmup → sparse 序列；find_indicator 取 core；update_kline 增量。"""
    eng = IndicatorEngine()
    eng.start()
    try:
        bars = _mk_bars(120)
        eng.ensure_indicator("ZIGZAG", dict(_ZZ_PARAMS), SYM, EXCH, TF)
        ik = eng.ind_key("ZIGZAG", _ZZ_PARAMS)
        eng.warmup(SYM, EXCH, TF, bars_to_df(bars), only_key=ik)

        series = eng.get_series("ZIGZAG", _ZZ_PARAMS, SYM, EXCH, TF)
        assert series, "预热后应有有效序列"
        # sparse：至少一个字段非空
        assert any(v is not None for it in series for v in it["values"].values())

        ind = eng.find_indicator("ZIGZAG", _ZZ_PARAMS, SYM, EXCH, TF)
        assert ind is not None and isinstance(ind, ZigZagIndicator)
        assert len(ind.core.pivots) >= 2

        # 增量推进：追加新 bar（含 forming bar 重复推送）
        n0 = len(series)
        extra = _mk_bars(140)[120:]
        for b in extra:
            eng.update_kline(_bar_to_kline(b, is_closed=False))
            eng.update_kline(_bar_to_kline(b, is_closed=True))
        series2 = eng.get_series("ZIGZAG", _ZZ_PARAMS, SYM, EXCH, TF)
        assert len(series2) > n0
        assert series2[-1]["timestamp"] == extra[-1]["timestamp"]
        assert ind.core.bar_count == 140
    finally:
        eng.stop()


def test_engine_patch_series_updates_history():
    """重绘修补：pending 尾段移动后，历史序列点经 patch_series 被更新（H1 处有值）。"""
    eng = IndicatorEngine()
    eng.start()
    try:
        bars = _mk_bars(150)
        eng.ensure_indicator("ZIGZAG", dict(_ZZ_PARAMS), SYM, EXCH, TF)
        ik = eng.ind_key("ZIGZAG", _ZZ_PARAMS)
        eng.warmup(SYM, EXCH, TF, bars_to_df(bars), only_key=ik)
        ind = eng.find_indicator("ZIGZAG", _ZZ_PARAMS, SYM, EXCH, TF)
        series = eng.get_series("ZIGZAG", _ZZ_PARAMS, SYM, EXCH, TF)
        ts_to_vals = {int(it["timestamp"]): it["values"] for it in series}
        # 每个确认高点所在 ts，序列里 ZZ_HIGH 应等于其价格（修补已回写）
        checked = 0
        for p in ind.core.confirmed_pivots:
            if p.direction == 1:
                vals = ts_to_vals.get(int(p.timestamp))
                if vals and vals.get("ZZ_HIGH") is not None:
                    assert vals["ZZ_HIGH"] == pytest.approx(p.price, rel=1e-9, abs=1e-9)
                    checked += 1
        assert checked >= 1
    finally:
        eng.stop()


# ═══════════════════════════════════════════════════════════
# 验收 8：SDK indic.zigzag() 活视图
# ═══════════════════════════════════════════════════════════


def _feed(bars) -> BacktestDataFeed:
    return BacktestDataFeed({SYM: bars}, TF)


def _replay(feed: BacktestDataFeed, n: int) -> None:
    for _ in range(n):
        if not feed.wait_update():
            break


def test_sdk_zigzag_view_interfaces():
    """indic.zigzag() 声明 → ZigZagView 活视图：极值点/pending/last_cross/bullish 可用。"""
    bars = _mk_bars(200)
    feed = _feed(bars)
    _replay(feed, 200)
    eng = IndicatorEngine()
    eng.start()
    try:
        indic = IndicApi(eng, feed, SYM, EXCH, TF)
        zz = indic.zigzag(bull=COND_STATE)
        assert isinstance(zz, ZigZagView)
        assert isinstance(zz.pivots, list)
        assert len(zz.pivots) >= 2
        assert zz.bullish in (True, False, None)

        h1 = zz.zigzag_high()
        l1 = zz.zigzag_low()
        assert h1 is None or isinstance(h1, Pivot)
        assert l1 is None or isinstance(l1, Pivot)
        # 函数形态与视图方法等价
        assert zigzag_high(zz) is h1
        assert zigzag_low(zz) is l1
        # highs/lows 列表 + 折线 + 确认点
        assert all(p.direction == 1 for p in zz.highs)
        assert all(p.direction == -1 for p in zz.lows)
        assert isinstance(zz.line, list)
        assert isinstance(zz.confirmed_pivots, list)
        # last_cross：状态条件无交叉谓词时可能为 None，但访问不应报错
        assert zz.last_cross is None or isinstance(zz.last_cross, CrossEvent)
    finally:
        eng.stop()


def test_sdk_zigzag_with_cross_has_events():
    """SDK 声明双线交叉条件 → 交叉事件/最近交叉可读。"""
    bars = _mk_bars(200)
    feed = _feed(bars)
    _replay(feed, 200)
    eng = IndicatorEngine()
    eng.start()
    try:
        indic = IndicApi(eng, feed, SYM, EXCH, TF)
        zz = indic.zigzag(
            bull="cross_up(ma(close,5), ema(close,5))",
            bear="cross_down(ma(close,5), ema(close,5))",
            cross_type="cross", tolerance=0.0, time_window=3,
        )
        assert isinstance(zz.crosses, list)
        if zz.crosses:
            assert isinstance(zz.last_cross, CrossEvent)
        assert isinstance(zz.pending, (Pivot, type(None)))
    finally:
        eng.stop()


def test_sdk_zigzag_advances_with_feed():
    """冷启动声明（index=-1）→ 随 feed 推进 advance() 后活视图出极值点。"""
    bars = _mk_bars(120)
    feed = _feed(bars)
    eng = IndicatorEngine()
    eng.start()
    try:
        indic = IndicApi(eng, feed, SYM, EXCH, TF)
        zz = indic.zigzag(bull=COND_STATE)
        assert zz.pivots == [] or isinstance(zz.pivots, list)
        while feed.wait_update():
            indic.advance()
        assert len(zz.pivots) >= 2
        assert zz.bullish in (True, False)
    finally:
        eng.stop()


# ═══════════════════════════════════════════════════
# 极值搜索方式 search_mode：cond=条件搜索（默认，现行行为）/ range=范围搜索
# ═══════════════════════════════════════════════════

# 确定性场景：纯状态条件 close>1.1 / close<1.1，翻转 bar 完全可控。
# bar3 为底部 L(0.70)；bar4 反弹尖峰 high=1.05 但仍在空头态（条件未成立）；
# bar6 翻多（条件成立）、bar11 翻空确认 H：
#   cond → H=argmax high[6..11]=bar7(1.04)；range → H=argmax high[3..11]=bar4(1.05)，
#   极值下标早于条件成立 bar(6)、不早于上一个 zz 点(3)。
_SM_TS = BASE_TS
_SM_SPEC = [
    # (close, high, low)
    (0.90, 0.95, 0.85),   # 0
    (0.88, 0.93, 0.80),   # 1
    (0.87, 0.92, 0.76),   # 2
    (0.86, 0.90, 0.70),   # 3 ← L 底部
    (0.95, 1.05, 0.85),   # 4 ← 反弹尖峰（range 模式的 H）
    (1.00, 1.00, 0.86),   # 5
    (1.12, 1.02, 0.90),   # 6 ← bull 条件成立 bar
    (1.15, 1.04, 0.91),   # 7 ← cond 模式的 H
    (1.14, 1.01, 0.90),   # 8
    (1.13, 1.02, 0.89),   # 9
    (1.12, 1.03, 0.88),   # 10
    (1.08, 1.04, 0.82),   # 11 ← bear 翻转 bar（两模式 pending L 均 @11/0.82）
]


def _sm_bars(spec=None):
    spec = _SM_SPEC if spec is None else spec
    bars = []
    prev = spec[0][0]
    for i, (c, h, lo) in enumerate(spec):
        bars.append({
            "symbol": SYM, "period": TF, "timestamp": _SM_TS + i * 60000,
            "open": prev, "high": h, "low": lo, "close": c, "volume": 1000.0,
        })
        prev = c
    return bars


_SM_COND = {"bull_cond": "close > 1.1", "bear_cond": "close < 1.1"}


def test_search_mode_validation():
    """search_mode 非法值核心层抛 ValueError（指标层另行归一，见下）。"""
    with pytest.raises(ValueError):
        ZigZagCore(bull_cond="close > 1.1", search_mode="bogus")
    # 合法值不抛
    ZigZagCore(bull_cond="close > 1.1", search_mode="cond")
    ZigZagCore(bull_cond="close > 1.1", search_mode="range")


def test_search_mode_cond_extremum_from_condition_bar():
    """条件搜索（默认）：H 取 leg 内（自条件成立 bar 起）最高 High，无视前段反弹尖峰。"""
    core = ZigZagCore(**_SM_COND)   # 缺省 search_mode=cond
    _run(core, _sm_bars())
    piv = core.pivots
    assert [(p.direction, p.index) for p in piv] == [(-1, 3), (1, 7), (-1, 11)]
    assert piv[0].price == pytest.approx(0.70)
    assert piv[0].confirmed and not piv[0].pending
    assert piv[1].price == pytest.approx(1.04) and piv[1].confirmed
    assert piv[2].pending and piv[2].price == pytest.approx(0.82)


def test_search_mode_range_backtracks_to_prev_pivot():
    """范围搜索：H 自上一个 zz 点回搜，命中条件成立前的反弹尖峰 bar4。"""
    core = ZigZagCore(**_SM_COND, search_mode="range")
    _run(core, _sm_bars())
    piv = core.pivots
    assert [(p.direction, p.index) for p in piv] == [(-1, 3), (1, 4), (-1, 11)]
    assert piv[1].price == pytest.approx(1.05) and piv[1].confirmed
    # 极值下标可早于条件成立 bar(6)、不早于上一个 zz 点(3)
    assert piv[0].index <= piv[1].index < 6
    # L 确认与 pending 尾与 cond 模式一致（本场景低极值均在 leg 内）
    assert piv[0].price == pytest.approx(0.70)
    assert piv[2].pending and piv[2].price == pytest.approx(0.82)


# 固定回搜窗回归（v9.155 用户定稿算法）：bear leg 内先大振幅底部 bar2（low=0.70 且
# high=1.30 尖峰同 bar）后反弹尖峰 bar3(1.25, 条件仍未翻转)，bar9 翻多；
# bull leg 高仅至 1.20（低于两尖峰）。
# 窗恒为 [B, A]：bar9 翻多 A=9、B=L@2 → 窗 [2,9] 捕捉同 bar 尖峰高 1.30
# 为垂直腿 H@2（用户接受偶发垂直）；bar15 翻空 A=15、B=H@2 已承载垂直腿 →
# 防往返守卫下界取 3 → 窗 [3,15] 命中 bar8 而非堆叠回 bar2（v9.150 无守卫时
# 同 bar 往返堆叠三连针）。range_lookback 参数已全链路删除。
_LB_SPEC = [
    (1.00, 1.02, 0.98),   # 0  bear
    (0.99, 1.01, 0.90),   # 1
    (0.95, 1.30, 0.70),   # 2  ← L 底部 + 同 bar 尖峰高（针状腿源）
    (0.96, 1.25, 0.92),   # 3  ← 反弹尖峰（仍 bear 态）
    (0.94, 0.98, 0.90),   # 4
    (0.93, 0.97, 0.89),   # 5
    (0.92, 0.96, 0.88),   # 6
    (0.91, 0.95, 0.87),   # 7
    (0.90, 0.94, 0.86),   # 8
    (1.12, 1.14, 1.08),   # 9  ← bull 条件成立 bar
    (1.14, 1.16, 1.10),   # 10
    (1.15, 1.17, 1.11),   # 11
    (1.16, 1.18, 1.12),   # 12
    (1.17, 1.19, 1.13),   # 13
    (1.18, 1.20, 1.14),   # 14 ← bull leg 真高
    (0.95, 1.00, 0.90),   # 15 ← bear 翻转确认
]


def test_search_mode_range_fixed_window_backsearch():
    """范围搜索固定窗 [B, A]（v9.155 用户定稿，无范围参数）：窗内本方向极值播种，
    含 B 本 bar 捕捉同 bar 垂直尖峰；B 已有垂直腿时守卫取次 bar 防往返堆叠；
    range_lookback 参数已删除（传入即 TypeError）。"""
    with pytest.raises(TypeError):
        ZigZagCore(bull_cond="close > 1.1", range_lookback=5)

    def _no_round_trip(piv):
        # 往返堆叠不变量：不得出现同 bar 三连针（L-H-L / H-L-H 同 index）
        return all(
            not (piv[i].index == piv[i + 1].index == piv[i + 2].index)
            for i in range(len(piv) - 2)
        )

    bars = _sm_bars(_LB_SPEC)
    core = ZigZagCore(**_SM_COND, search_mode="range")
    _run(core, bars)
    piv = core.pivots
    # 窗 [2,9] 捕捉同 bar 尖峰高 H@2(1.30) 垂直腿；守卫使 bar15 窗 [3,15] 命中 L@8
    assert [(p.direction, p.index) for p in piv] == [(-1, 2), (1, 2), (-1, 8)]
    assert piv[1].price == pytest.approx(1.30)
    assert piv[1].confirmed and piv[2].pending
    assert _no_round_trip(piv)


# 范围搜索定稿回归（v9.172 移除 v9.171 守卫）：bear leg 内 bar3 为大上影动量 bar
# （high 1.60 低于前一确认 H@0 的 1.80），bar6 翻多确认 L@3 后回搜窗 [A=L@3, B=6]
# 最高 High 落 bar3 本 bar → 直接播种 H@3，与 L@3 形成同 bar 垂直腿（用户明确
# 接受：极值允许落在上一极值点 A 同一根 bar）；bar8 翻空回搜窗 [A=H@3, B=8]，
# H@3 已承载垂直腿（pivots[-2]=L@3 同 bar）→ 往返守卫下界推 4，最低 Low 命中 bar4(0.88)。
_ND_SPEC = [
    (1.20, 1.80, 1.15),   # 0  bull 初始 pending H@0(1.80)
    (1.05, 1.70, 1.00),   # 1  bear 翻转：确认 H@0，播种 L@1
    (0.95, 1.20, 0.90),   # 2  跟踪 L@2
    (0.90, 1.60, 0.85),   # 3  大上影动量 bar：跟踪 L@3(0.85)；high 1.60 < 前 H 1.80
    (0.95, 1.20, 0.88),   # 4
    (1.00, 1.30, 0.95),   # 5
    (1.20, 1.25, 1.05),   # 6  bull 翻转确认 L@3，回搜 [3,6] 最高 High 落 bar3 → H@3
    (1.25, 1.28, 1.10),   # 7
    (1.05, 1.20, 1.00),   # 8  bear 翻转确认 H@3，往返守卫下界推 4 → 播种 L@4
]


def test_search_mode_range_extremum_may_land_on_prev_pivot_bar():
    """范围搜索 [A,B] 闭区间：回搜极值可落在上一极值点 A 同一根 bar，形成合法的
    同 bar 垂直腿（v9.172 移除 v9.171 精准守卫后的定稿行为：极值允许 ≤ A 时间）。"""
    bars = _sm_bars(_ND_SPEC)
    core = ZigZagCore(**_SM_COND, search_mode="range")
    _run(core, bars)
    piv = core.pivots
    assert [(p.direction, p.index) for p in piv] == [(1, 0), (-1, 3), (1, 3), (-1, 4)]
    assert piv[1].price == pytest.approx(0.85)                      # L@3
    assert piv[2].price == pytest.approx(1.60) and piv[2].confirmed  # H@3 同 bar 垂直腿
    assert piv[3].pending and piv[3].price == pytest.approx(0.88)   # L@4
    # 允许同 bar 垂直对，但不得三连针（往返守卫保障）
    assert all(
        not (piv[i].index == piv[i + 1].index == piv[i + 2].index)
        for i in range(len(piv) - 2)
    )


# 范围搜索同 bar 垂直腿用例：bar6 为真 pin bar（下影 0.60 突破前一确认 L@4 的 0.70），
# bar9 翻空回搜窗 [A=H@6, B=9] 最低 Low 落 bar6 本 bar → 播种 L@6，与 H@6 成同 bar
# 垂直腿（[A,B] 闭区间含 A，极值可落 A 同 bar）。
_PIN_SPEC = [
    (1.20, 1.22, 0.98),   # 0  bull 初始 pending H@0
    (1.05, 1.10, 0.96),   # 1  bear 翻转：确认 H@0，播种 L@1
    (0.90, 0.94, 0.80),   # 2  跟踪 L@2
    (0.80, 0.86, 0.72),   # 3  跟踪 L@3
    (0.75, 0.82, 0.70),   # 4  跟踪 L@4(0.70)
    (1.20, 1.30, 0.90),   # 5  bull 翻转：确认 L@4，播种 H@5
    (1.18, 1.60, 0.60),   # 6  真 pin bar：跟踪 H@6(1.60)；low 0.60 突破 L@4
    (1.15, 1.30, 0.90),   # 7
    (1.12, 1.26, 0.92),   # 8
    (1.05, 1.20, 0.94),   # 9  bear 翻转：确认 H@6，回搜含 B 播种 L@6 垂直腿
]


def test_search_mode_range_pin_spike_beyond_structure_kept():
    """范围搜索 [A,B] 闭区间：回搜极值落在 A 本 bar（真 pin bar 尖峰）→ 保留，
    与 A 形成同 bar 垂直腿（定稿语义）。"""
    bars = _sm_bars(_PIN_SPEC)
    core = ZigZagCore(**_SM_COND, search_mode="range")
    _run(core, bars)
    piv = core.pivots
    assert [(p.direction, p.index) for p in piv] == [(1, 0), (-1, 4), (1, 6), (-1, 6)]
    assert piv[2].price == pytest.approx(1.60) and piv[2].confirmed
    assert piv[3].pending and piv[3].price == pytest.approx(0.60)


def test_zz_line_vertical_bar_keeps_incoming_end():
    """v9.156 同 bar 垂直腿守卫：垂直对 (L@2,H@2) 之后末段前移为 (H@2,L@8)，
    ZZ_LINE@2 仍取入侧端点 L@2=0.70（真低触线），不被新末段插值起点 H@2=1.30
    覆盖；bar3 起按新末段插值，尾段止于 pending L@8。"""
    bars = _sm_bars(_LB_SPEC)
    ind = ZigZagIndicator({**_SM_COND, "search_mode": "range"})
    out = ind._calculate(bars_to_df(bars))
    line = out["ZIGZAG_ZZ_LINE"].to_list()
    assert line[2] == pytest.approx(0.70)
    assert line[3] == pytest.approx(1.30 + (0.86 - 1.30) * (3 - 2) / (8 - 2))
    assert line[8] == pytest.approx(0.86)


def test_search_mode_range_snapshot_idempotent():
    """范围搜索窗随快照回退：forming bar 同 ts 重复/渐进推送结果与一次性收盘一致。"""
    bars = _sm_bars()
    ref = ZigZagCore(**_SM_COND, search_mode="range")
    _run(ref, bars)

    # forming 渐进：bar11 分三次同 ts 推送（high/low 逐步扩展），最后收盘
    inc = ZigZagCore(**_SM_COND, search_mode="range")
    _run(inc, bars[:-1])
    spec11 = _SM_SPEC[11]
    for h, lo in ((0.99, 0.90), (1.02, 0.86), spec11[1:]):
        fb = dict(bars[-1], high=h, low=lo)
        inc.update_bar(fb, False)
    inc.update_bar(bars[-1], True)
    got = [(p.direction, p.index, p.price, p.confirmed, p.pending) for p in inc.pivots]
    want = [(p.direction, p.index, p.price, p.confirmed, p.pending) for p in ref.pivots]
    assert got == want


def test_search_mode_indicator_param_and_normalization():
    """指标层：search_mode=range 输出随核心变；非法值归一 cond 不抛。"""
    bars = _sm_bars()
    ind = ZigZagIndicator({**_SM_COND, "search_mode": "range"})
    out = ind._calculate(bars_to_df(bars))
    highs = out["ZIGZAG_ZZ_HIGH"].to_list()
    line = out["ZIGZAG_ZZ_LINE"].to_list()
    assert highs[4] == pytest.approx(1.05)
    assert highs[7] is None
    # ZZ_LINE 末段＝最后确认枢轴→pending（v9.154）：固定窗 [4,11] 最小低=bar11
    # 0.82 → L@11 播种，尾段 H@4→L@11 插值致密（水平腿修复后的既有不变量）
    assert line[3] == pytest.approx(0.70) and line[4] == pytest.approx(1.05)
    assert all(v is not None for v in line[3:12])

    bad = ZigZagIndicator({**_SM_COND, "search_mode": "bogus"})   # 归一 cond，不抛
    assert bad.core.search_mode == "cond"
    cond = ZigZagIndicator(dict(_SM_COND))                        # 缺省也是 cond
    out_bad = bad._calculate(bars_to_df(bars))
    out_cond = cond._calculate(bars_to_df(bars))
    assert out_bad["ZIGZAG_ZZ_HIGH"].to_list() == out_cond["ZIGZAG_ZZ_HIGH"].to_list()
    assert out_cond["ZIGZAG_ZZ_HIGH"].to_list()[7] == pytest.approx(1.04)


def test_search_mode_range_matches_incremental():
    """范围搜索：批量 _calculate 与逐 bar 增量输出逐字段一致（回测/实盘同源）。"""
    bars = _mk_bars(200)
    params = {**_ZZ_PARAMS, "search_mode": "range"}
    batch = ZigZagIndicator(dict(params))
    out = batch._calculate(bars_to_df(bars))
    ts_col = out["timestamp"].to_list()
    batch_map = {
        int(ts_col[i]): (out["ZIGZAG_ZZ_HIGH"][i], out["ZIGZAG_ZZ_LOW"][i],
                         out["ZIGZAG_ZZ_LINE"][i])
        for i in range(len(bars))
    }
    inc = ZigZagIndicator(dict(params))
    inc_map: dict[int, tuple] = {}
    for b in bars:
        vals = inc.update_bar(b, True, partial=True)
        # 增量输出叠加历史修补（与 _calculate 回写同法）
        for pts, fields in inc._patches.items():
            key = int(pts)
            cur = list(inc_map.get(key, (None, None, None)))
            for fname, v in fields.items():
                cur[{"ZZ_HIGH": 0, "ZZ_LOW": 1, "ZZ_LINE": 2}[fname]] = v
            inc_map[key] = tuple(cur)
        if vals:
            inc_map[int(b["timestamp"])] = (
                vals.get("ZZ_HIGH"), vals.get("ZZ_LOW"), vals.get("ZZ_LINE"))

    for ts, (bh, bl, bln) in batch_map.items():
        ih, il, iln = inc_map.get(ts, (None, None, None))
        assert (bh is None) == (ih is None)
        assert (bl is None) == (il is None)
        assert (bln is None) == (iln is None)
        if bh is not None:
            assert bh == pytest.approx(ih, rel=1e-9, abs=1e-9)
        if bl is not None:
            assert bl == pytest.approx(il, rel=1e-9, abs=1e-9)
        if bln is not None:
            assert bln == pytest.approx(iln, rel=1e-9, abs=1e-9)


# ─── forming tick 残值清扫回归 ───


def test_forming_tick_residual_absolute_sweep():
    """收盘绝对清扫消除 forming tick 残值：中间 tick 瞬移 pending 改写历史 bar，
    收盘 tick pending 回原位（diff 为空），engine 序列持有残值。
    绝对清扫应对 [_tail_floor, cur_idx) 全量发射正确终值覆盖残值。

    场景：_SM_SPEC bars 0-10 收盘后 pivots=[L@3, H@4(pending,1.05)]；
    bar11 tick1 high=1.20 瞬移 pending→H@11(1.20)，改写 bars[3,11) 插值；
    bar11 close high=1.04/close=1.08 翻空：pending 不移动（H@4 确认），
    v9.172 移除守卫后纯 [A=H@4, B=11] 回搜最低 Low=bar4 的 0.85 → 播种 L@4，
    与 H@4 成同 bar 垂直腿（用户接受）——bars[5,11) 尾段外无值（None），
    tick1 插值残值应被清扫/回退清除；bar4 的 ZZ_LINE/ZZ_HIGH 残值（tick1 瞬态
    插值）应被绝对清扫覆盖回终值。
    """
    bars = _sm_bars()
    params = {**_SM_COND, "search_mode": "range"}
    ind = ZigZagIndicator(dict(params))

    # 建立状态：bars 0-10 逐根收盘
    for b in bars[:-1]:
        ind.update_bar(b, True, partial=True)

    bar11 = bars[-1]

    # 模拟 engine 序列（ts → {field: value}），只累积 patches 不回退
    engine: Dict[int, Dict[str, Any]] = {}

    def apply_patches():
        for ts, fields in ind._patches.items():
            engine.setdefault(int(ts), {}).update(fields)

    # Tick 1: forming，high=1.20 > 1.05 → _track 瞬移 pending H 到 bar11
    tick1 = dict(bar11, high=1.20, low=0.90, close=1.15)
    ind.update_bar(tick1, False, partial=True)
    apply_patches()
    tick1_keys = set(int(k) for k in ind._patches.keys())
    # 确认 tick1 改写了 bar4（原 ZZ_HIGH=1.05 → None，因 H 移到 11）
    ts4 = int(bars[4]["timestamp"])
    assert ts4 in tick1_keys, "tick1 应改写 bar4（pending 瞬移后 H 标记消失）"

    # Tick 2 (CLOSE): high=1.04 < 1.05 → pending 不移动，H 留在 bar4
    tick2 = dict(bar11, high=1.04, low=0.88, close=1.08)
    ind.update_bar(tick2, True, partial=True)
    apply_patches()

    # 收盘后 _tail_floor 应已重置
    assert ind._tail_floor is None

    # 验证：engine 中 bar4 的 ZZ_HIGH 恢复为 1.05（绝对清扫覆盖了 tick1 的 None 残值）
    assert engine[ts4]["ZZ_HIGH"] == pytest.approx(1.05), \
        f"bar4 ZZ_HIGH 残值未清扫: {engine[ts4]}"

    # 验证：engine 中 bars 5-10 的 ZZ_LINE 全为 None（同 bar 垂直腿 H@4→L@4 止于 bar4）；
    # tick1 瞬态插值残值不在这些 bar 冻结
    for i in range(5, 11):
        ts_i = int(bars[i]["timestamp"])
        assert engine[ts_i].get("ZZ_LINE") is None, \
            f"bar{i} ZZ_LINE 残值未清扫: {engine[ts_i]}"

    # 验证：确认腿 L@3→H@4 端点 + 同 bar 垂直腿 H@4/L@4 落地（bar4 ZZ_LINE 被绝对
    # 清扫覆盖回 1.05，清除 tick1 的 0.7625 残值；bar4 同时持有 H@4=1.05 与 L@4=0.85）
    assert engine[int(bars[3]["timestamp"])]["ZZ_LINE"] == pytest.approx(0.70)
    assert engine[ts4]["ZZ_LINE"] == pytest.approx(1.05)
    assert engine[ts4]["ZZ_LOW"] == pytest.approx(0.85)
    assert engine[ts4]["ZZ_HIGH"] == pytest.approx(1.05)

    # 全量对比：engine 终态 vs 批量参考（bar11 用 tick2 值收盘）
    ref_bars = bars[:-1] + [tick2]
    ref = ZigZagIndicator(dict(params))
    ref_out = ref._calculate(bars_to_df(ref_bars))
    ts_col = ref_out["timestamp"].to_list()
    for i in range(len(ref_bars)):
        ts_i = int(ts_col[i])
        if ts_i not in engine:
            continue
        for fname, col in (("ZZ_HIGH", "ZIGZAG_ZZ_HIGH"), ("ZZ_LOW", "ZIGZAG_ZZ_LOW"),
                           ("ZZ_LINE", "ZIGZAG_ZZ_LINE")):
            expected = ref_out[col][i]
            actual = engine[ts_i].get(fname)
            if expected is None:
                assert actual is None, f"bar{i} {fname}: engine={actual}, ref=None"
            else:
                assert actual is not None, f"bar{i} {fname}: engine=None, ref={expected}"
                assert actual == pytest.approx(expected, rel=1e-9, abs=1e-9), \
                    f"bar{i} {fname}: engine={actual}, ref={expected}"


# ════════════════════════════════════════════
# v9.172：条件判定口径（仅收盘/近收盘）+ 坐标模式中间带
# ════════════════════════════════════════════


def test_condition_judged_only_on_closed_bar():
    """v9.172：翻转条件仅在收盘 bar 判定——未收盘 bar 延续既有方向只跟踪极值、不翻转。"""
    bars = _sm_bars()   # bars0-5 空头（close<1.1），bar6 close=1.12 多头
    core = ZigZagCore(**_SM_COND)   # cond 模式
    for b in bars[:6]:
        core.update_bar(b, True)
    # 建立空头态：pending L@3(0.70)
    assert len(core.pivots) == 1 and core.pivots[-1].direction == -1
    assert core.pivots[-1].index == 3 and core.pivots[-1].price == pytest.approx(0.70)

    # bar6 以 forming 推送（未收盘）：条件不判定 → 不翻转，仍空头 pending
    core.update_bar(bars[6], False)
    assert len(core.pivots) == 1 and core.pivots[-1].direction == -1, \
        "未收盘 bar 不应翻转"

    # bar6 收盘：条件成立（close 1.12>1.1）→ 翻多，确认 L@3 + 播种 pending H
    core.update_bar(bars[6], True)
    assert len(core.pivots) == 2
    assert core.pivots[0].direction == -1 and core.pivots[0].confirmed
    assert core.pivots[1].direction == 1 and core.pivots[1].pending


def test_near_close_forming_bar_evaluates_condition():
    """v9.172：实时近收盘（near_close=True）的 forming bar 视同收盘评估条件 → 翻转生效。"""
    bars = _sm_bars()
    core = ZigZagCore(**_SM_COND)
    for b in bars[:6]:
        core.update_bar(b, True)
    assert len(core.pivots) == 1   # 空头 pending L@3
    # bar6 forming 但 near_close=True → 评估条件 → 翻多
    core.update_bar(bars[6], False, near_close=True)
    assert len(core.pivots) == 2
    assert core.pivots[1].direction == 1 and core.pivots[1].pending


# 坐标模式（bounded_0_100 指标）在后端等价于“带迟滞中间带的状态条件”：
# bull=值≥hi、bear=值≤lo、中间带两者皆 False → _decide 返回 None → v9.173 用户定稿
# 延续既有方向继续跟踪 pending 极值（ZLINE 同理，不翻转不冻结）。此处用价格尺度模拟。
_BAND_COND = {"bull_cond": "close >= 1.20", "bear_cond": "close <= 0.80"}
_BAND_SPEC = [
    (1.30, 1.35, 1.25),   # 0 bull → 初始 pending H@0(1.35)
    (1.25, 1.28, 1.20),   # 1 bull(≥1.20) → 跟踪，high 1.28<1.35 不更新
    (1.00, 1.05, 0.95),   # 2 中间带 → 延续 bull 跟踪，high 1.05<1.35 不更新
    (0.90, 0.95, 0.85),   # 3 中间带 → 延续 bull 跟踪，high 0.95<1.35 不更新
    (1.10, 1.50, 1.05),   # 4 中间带（high 1.50 更高）→ 延续 bull 跟踪：pending 上移 H@4(1.50)
    (0.75, 0.78, 0.70),   # 5 bear(≤0.80) → 翻转：确认 H@4(1.50)，播种 pending L@5(0.70)
]


def test_coordinate_band_continues_direction_tracks_extremum():
    """v9.173：坐标中间带（lo<值<hi 两条件皆 False）延续既有方向继续跟踪 pending 极值
    （ZLINE 同理，不翻转不冻结）——bar4 中间带 high 1.50 更高 → pending 上移 H@4，
    bar5 空头成立翻转确认该 H@4(1.50)。（反转 v9.172 的中间带冻结语义。）"""
    bars = _sm_bars(_BAND_SPEC)
    core = ZigZagCore(**_BAND_COND)
    _run(core, bars)
    piv = core.pivots
    assert [(p.direction, p.index) for p in piv] == [(1, 4), (-1, 5)]
    assert piv[0].price == pytest.approx(1.50) and piv[0].confirmed  # 中间带延续多头 pending 上移
    assert piv[1].pending and piv[1].price == pytest.approx(0.70)

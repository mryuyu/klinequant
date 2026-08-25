"""def 式指标计算图单元测试（IND-110）

覆盖：
    - 原语批量(polars) vs 增量(O(1)递推) 逐点一致性：EMA/SMA/RollingMax/RollingStd/Shift
    - 前导 null 对齐：shift+EMA 组合（ewm ignore_nulls 语义）
    - 快照法：未收盘 bar 同 ts 重复推送幂等
    - 注册/meta：样例指标 TRIX/DEMA 自动发现，参数默认值来自函数签名
    - 引擎集成：ensure_indicator + warmup + update_kline → IndicatorValue
"""
from __future__ import annotations

import math
import random
from decimal import Decimal

import polars as pl
import pytest

from core.indicator_engine.engine import IndicatorEngine
from core.indicator_engine.graph import ema, rolling_max, rolling_std, shift, sma
from core.indicator_engine.graph.dsl import GraphDef, make_indicator_cls
from core.indicator_engine.registry import get_registry
from protocol.types import Kline


# ─── 测试数据 ───

def _gen_df(n: int = 300, seed: int = 7) -> pl.DataFrame:
    rng = random.Random(seed)
    close = 100.0
    ts, opens, highs, lows, closes, vols = [], [], [], [], [], []
    t = 1_700_000_000_000
    for i in range(n):
        o = close
        close = max(1.0, close * (1 + rng.gauss(0, 0.004)))
        hi = max(o, close) * (1 + abs(rng.gauss(0, 0.001)))
        lo = min(o, close) * (1 - abs(rng.gauss(0, 0.001)))
        ts.append(t + i * 60_000)
        opens.append(o); highs.append(hi); lows.append(lo)
        closes.append(close); vols.append(rng.uniform(10, 1000))
    return pl.DataFrame({
        "timestamp": ts, "open": opens, "high": highs, "low": lows,
        "close": closes, "volume": vols, "quote_volume": [0.0] * n,
        "trade_count": [0] * n, "is_closed": [True] * n,
    })


def _make(fn, params=None, name="T"):
    gdef = GraphDef(fn=fn, name=name, pane="sub", range_="unbounded",
                    desc="", min_periods_override=None)
    return make_indicator_cls(gdef)(params=params)


def _replay(ind, df) -> list:
    """增量路径逐根重放，返回 (ts, values-or-None) 列表"""
    ind.reset()
    out = []
    for row in df.iter_rows(named=True):
        out.append((row["timestamp"], ind.update_bar(row, True)))
    return out


def _batch_col(ind, df, field) -> list:
    """批量路径取指定字段列"""
    result = ind.calculate(df)
    return result[f"{ind.name}_{field}"].to_list()


# ─── 原语一致性：批量 vs 增量 ───

def test_ema_batch_vs_incremental():
    df = _gen_df()
    ind = _make(lambda close: {"V": ema(close, 10)})
    inc = [None if v is None else v["V"] for _, v in _replay(ind, df)]
    batch = _batch_col(ind, df, "V")
    assert all(v is not None for v in inc)   # EMA 首值即种子，无预热段
    for a, b in zip(batch, inc):
        assert a == pytest.approx(b, rel=1e-12)


def test_sma_batch_vs_incremental():
    df = _gen_df()
    ind = _make(lambda close: {"V": sma(close, 20)})
    inc = [None if v is None else v["V"] for _, v in _replay(ind, df)]
    batch = _batch_col(ind, df, "V")
    assert all(v is None for v in inc[:19])   # 窗口未满输出 null
    for a, b in zip(batch[19:], inc[19:]):
        assert a == pytest.approx(b, rel=1e-9)


def test_rolling_max_std_batch_vs_incremental():
    df = _gen_df()
    ind = _make(lambda high, close: {
        "MX": rolling_max(high, 9),
        "SD": rolling_std(close, 14),
    })
    inc = _replay(ind, df)
    mx_b, sd_b = _batch_col(ind, df, "MX"), _batch_col(ind, df, "SD")
    warm = max(9, 14)   # 图级预热门控：任一输出未就绪则整体 None
    for i, (_, v) in enumerate(inc):
        if i < warm - 1:
            assert v is None
            continue
        assert v["MX"] == pytest.approx(mx_b[i], rel=1e-12)
        assert v["SD"] == pytest.approx(sd_b[i], rel=1e-8)


def test_shift_ema_null_alignment():
    """shift 产生前导 null：增量须等首个有效输入才种子，批量 ignore_nulls 对齐"""
    df = _gen_df()
    ind = _make(lambda close: {"V": ema(shift(close, 2), 5)})
    inc = [None if v is None else v["V"] for _, v in _replay(ind, df)]
    batch = _batch_col(ind, df, "V")
    assert inc[0] is None and inc[1] is None
    assert batch[0] is None and batch[1] is None
    for a, b in zip(batch[2:], inc[2:]):
        assert b is not None
        assert a == pytest.approx(b, rel=1e-12)


# ─── display_meta.style 契约校验 ───

def test_style_contract_validation():
    """style 声明校验：#RRGGBB / 线型枚举；未声明返回 None"""
    from core.indicator_engine.graph.dsl import _validate_style

    assert _validate_style("X", None) is None
    assert _validate_style("X", [{"color": "#f0b90b", "line_style": 2}]) == [
        {"color": "#f0b90b", "line_style": 2}
    ]
    assert _validate_style("X", [{}]) == [{}]   # 空项合法（该字段仅占位）
    with pytest.raises(ValueError):
        _validate_style("X", [{"color": "red"}])
    with pytest.raises(ValueError):
        _validate_style("X", [{"line_style": 9}])


def test_price_lines_contract_validation():
    """price_lines 声明校验：price 必填数值；color/line_style 可选校验；未声明返回 None"""
    from core.indicator_engine.graph.dsl import _validate_price_lines

    assert _validate_price_lines("X", None) is None
    assert _validate_price_lines("X", [{"price": 80}]) == [{"price": 80}]
    assert _validate_price_lines("X", [{"price": 0, "color": "#787b86", "line_style": 2}]) == [
        {"price": 0, "color": "#787b86", "line_style": 2}
    ]
    with pytest.raises(ValueError):
        _validate_price_lines("X", [{}])   # price 缺失
    with pytest.raises(ValueError):
        _validate_price_lines("X", [{"price": True}])   # bool 非合法数值
    with pytest.raises(ValueError):
        _validate_price_lines("X", [{"price": 80, "color": "gray"}])
    with pytest.raises(ValueError):
        _validate_price_lines("X", [{"price": 80, "line_style": 7}])


# ─── 样例指标：TRIX 全量重放 vs 批量逐点一致 ───

def test_sample_trix_registered_and_consistent():
    import custom_indicators  # noqa: F401  触发样例注册

    reg = get_registry()
    assert "TRIX" in reg and "DEMA" in reg

    ind = reg.create("TRIX", None)
    assert ind.default_params == {"period": 12}
    meta = ind.display_meta
    assert meta["fields"] == ["TRIX"] and meta["pane"] == "sub"
    assert meta["style"] == [{"color": "#ba68c8"}]   # 字段级默认样式契约（display_meta.style）
    assert "price_lines" not in meta   # 未声明参考线时不输出该键

    df = _gen_df(400)
    inc = [None if v is None else v["TRIX"] for _, v in _replay(ind, df)]
    batch = _batch_col(ind, df, "TRIX")
    valid = [(a, b) for a, b in zip(batch, inc) if b is not None]
    assert len(valid) > 350   # 结构性预热极短（shift 1 根）
    for a, b in valid:
        assert a == pytest.approx(b, rel=1e-9)


def test_sample_dema_overlay_meta():
    import custom_indicators  # noqa: F401

    ind = get_registry().create("DEMA", None)
    assert ind.display_meta["pane"] == "main"
    assert ind.display_meta["fields"] == ["DEMA"]
    assert "style" not in ind.display_meta   # 未声明样式时不输出 style 键（前端走默认色槽）
    df = _gen_df()
    inc = [v for _, v in _replay(ind, df)]
    batch = _batch_col(ind, df, "DEMA")
    for a, b in zip(batch, inc):
        assert a == pytest.approx(b["DEMA"], rel=1e-12)


def test_sample_macd_multi_price_lines():
    """MACD_MULTI 声明零轴参考线，随 display_meta 下发"""
    import custom_indicators  # noqa: F401

    ind = get_registry().create("MACD_MULTI", None)
    assert ind.display_meta["price_lines"] == [{"price": 0}]


# ─── 快照法：未收盘 bar 同 ts 重复推送幂等 ───

def test_snapshot_replay_idempotent():
    import custom_indicators  # noqa: F401

    df = _gen_df(120)
    rows = list(df.iter_rows(named=True))
    base_rows, extra = rows[:-1], rows[-1]

    # 参照：干净重放到末根
    ref = get_registry().create("TRIX", None)
    for r in base_rows:
        ref.update_bar(r, True)
    expected = ref.update_bar(extra, True)

    # 目标实例：末根连续推送 3 次（close 各不相同，模拟盘中 tick）
    ind = get_registry().create("TRIX", None)
    for r in base_rows:
        ind.update_bar(r, True)
    for close in (extra["close"] * 1.01, extra["close"] * 0.99, extra["close"]):
        got = ind.update_bar({**extra, "close": close}, False)
        assert got is not None
    assert got["TRIX"] == pytest.approx(expected["TRIX"], rel=1e-12)

    # 再来一根新 bar：上一根被隐式确认，状态继续前进
    next_row = {**extra, "timestamp": extra["timestamp"] + 60_000,
                "close": extra["close"] * 1.005}
    assert ind.update_bar(next_row, True) is not None


# ─── 引擎集成：warmup + update_kline → IndicatorValue ───

def test_engine_integration_graph_indicator():
    import custom_indicators  # noqa: F401

    engine = IndicatorEngine()
    df = _gen_df(200)
    engine.ensure_indicator("TRIX", {}, "BTCUSDT", "binance", "1m")
    engine.warmup("BTCUSDT", "binance", "1m", df)

    series = engine.get_series("TRIX", {}, "BTCUSDT", "binance", "1m")
    assert len(series) > 150   # 剔除预热段后的有效序列

    last = df.row(-1, named=True)
    kline = Kline(
        symbol="BTCUSDT", exchange="binance", timeframe="1m",
        timestamp=last["timestamp"] + 60_000,
        open=Decimal(str(last["close"])), high=Decimal(str(last["close"] * 1.002)),
        low=Decimal(str(last["close"] * 0.998)), close=Decimal(str(last["close"] * 1.001)),
        volume=Decimal("100"), quote_volume=Decimal("0"), trade_count=0, is_closed=True,
    )
    updated = engine.update_kline(kline)
    assert len(updated) == 1
    iv = updated[0]
    assert iv.indicator_name == "TRIX" and "TRIX" in iv.values
    assert math.isfinite(iv.values["TRIX"])
    assert len(engine.get_series("TRIX", {}, "BTCUSDT", "binance", "1m")) == len(series) + 1

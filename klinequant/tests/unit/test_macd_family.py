"""MACD 倍数族实质测试 — 指标引擎多实例正确性 + 策略端到端

验证目标（M2.5 端到端验收样例）：
    1. 引擎同时承载 4 组 MACD（base 2/5/3 × mults 1/4/16/64），
       各自序列与 polars 全量参考实现逐点对拍
    2. 多实例状态隔离：预热/增量互不串扰，warmup 结果按 ind_key 区分
    3. IND-106 消费链路：策略 require_indicators → 引擎预热 → df 列注入 → on_bar 信号
"""
from __future__ import annotations

import math
import random
from decimal import Decimal
from typing import Dict, List

import polars as pl
import pytest

from core.indicator_engine.engine import IndicatorEngine
from core.indicator_engine.indicators import MACD  # noqa: F401 导入触发注册表登记
from core.strategy_engine.context import StrategyInfo
from core.strategy_engine.manager import StrategyManager
from core.strategy_engine.wiring import (
    col_slug,
    field_col,
    inject_indicators,
    warmup_from_df,
)
from protocol.types import Kline
from strategies.macd_family import DEFAULT_MULTS, MACDFamilyStrategy

SYMBOL = "BTCUSDT"
EXCHANGE = "binance"
TIMEFRAME = "1m"
BASE_TS = 1700000000000
TF_MS = 60_000
N_BARS = 800  # 64x 组 min_periods = 320+192-1 = 511，800 根足够预热并留尾部

BASE_PARAMS = {"fast_period": 2, "slow_period": 5, "signal_period": 3}


def family_params(mult: int) -> Dict[str, int]:
    return {k: v * mult for k, v in BASE_PARAMS.items()}


def min_periods(mult: int) -> int:
    p = family_params(mult)
    return p["slow_period"] + p["signal_period"] - 1


# ─── 测试数据 ───


def _gen_closes(n: int = N_BARS, seed: int = 42) -> List[float]:
    """带趋势与振荡的随机游走（保证 MACD 多次穿越零轴）"""
    rng = random.Random(seed)
    closes, price = [], 100.0
    for i in range(n):
        price *= 1 + rng.gauss(0, 0.004) + 0.0006 * math.sin(i / 25)
        closes.append(price)
    return closes


def make_kline_df(closes: List[float]) -> pl.DataFrame:
    n = len(closes)
    return pl.DataFrame({
        "timestamp": [BASE_TS + i * TF_MS for i in range(n)],
        "open": closes,
        "high": [c * 1.001 for c in closes],
        "low": [c * 0.999 for c in closes],
        "close": closes,
        "volume": [1.0] * n,
        "quote_volume": [0.0] * n,
        "trade_count": [0] * n,
        "is_closed": [True] * n,
    })


def mk_kline(close: float, ts: int) -> Kline:
    d = Decimal(str(close))
    return Kline(
        symbol=SYMBOL, exchange=EXCHANGE, timeframe=TIMEFRAME, timestamp=ts,
        open=d, high=d, low=d, close=d, volume=Decimal("1"),
        quote_volume=Decimal("0"), trade_count=0, is_closed=True,
    )


def ref_macd(closes: List[float], mult: int):
    """polars 全量参考实现（与引擎增量路径对拍的基准）"""
    p = family_params(mult)
    s = pl.Series(closes)
    dif = s.ewm_mean(alpha=2 / (p["fast_period"] + 1), adjust=False) - s.ewm_mean(
        alpha=2 / (p["slow_period"] + 1), adjust=False
    )
    dea = dif.ewm_mean(alpha=2 / (p["signal_period"] + 1), adjust=False)
    hist = 2.0 * (dif - dea)
    return dif, dea, hist


def build_family_engine(closes: List[float]) -> IndicatorEngine:
    engine = IndicatorEngine()
    for mult in DEFAULT_MULTS:
        engine.ensure_indicator("MACD", family_params(mult), SYMBOL, EXCHANGE, TIMEFRAME)
    engine.warmup(SYMBOL, EXCHANGE, TIMEFRAME, make_kline_df(closes))
    return engine


# ─── 引擎多实例正确性 ───


class TestEngineFamily:
    def test_family_warmup_series_match_polars(self):
        """4 组实例各自序列与 polars 参考逐点对拍，长度 = 总根数 - min_periods + 1"""
        closes = _gen_closes()
        engine = build_family_engine(closes)

        for mult in DEFAULT_MULTS:
            params = family_params(mult)
            series = engine.get_series("MACD", params, SYMBOL, EXCHANGE, TIMEFRAME)
            mp = min_periods(mult)
            assert len(series) == N_BARS - mp + 1, f"mult={mult} 序列长度异常"

            dif, dea, hist = ref_macd(closes, mult)
            for offset in (0, 37, (len(series) - 1) // 2, len(series) - 1):
                idx = mp - 1 + offset
                vals = series[offset]["values"]
                assert series[offset]["timestamp"] == BASE_TS + idx * TF_MS
                assert vals["DIF"] == pytest.approx(dif[idx], rel=1e-9)
                assert vals["DEA"] == pytest.approx(dea[idx], rel=1e-9)
                assert vals["HIST"] == pytest.approx(hist[idx], rel=1e-9)

    def test_warmup_results_keyed_by_ind_key(self):
        """多实例 warmup 结果：4 个 ind_key 并存不覆盖（指标名键保留兼容）"""
        closes = _gen_closes()
        engine = IndicatorEngine()
        for mult in DEFAULT_MULTS:
            engine.ensure_indicator("MACD", family_params(mult), SYMBOL, EXCHANGE, TIMEFRAME)
        results = engine.warmup(SYMBOL, EXCHANGE, TIMEFRAME, make_kline_df(closes))

        assert "MACD" in results
        for mult in DEFAULT_MULTS:
            ik = engine.ind_key("MACD", family_params(mult))
            assert ik in results
        # 各组最新值互不相同（未串扰）
        keys = [engine.ind_key("MACD", family_params(m)) for m in DEFAULT_MULTS]
        difs = [results[k]["DIF"] for k in keys]
        assert len(set(difs)) == 4

    def test_multi_instance_value_lookup_by_params(self):
        """get_indicator_value 按 params 精确定位实例"""
        closes = _gen_closes()
        engine = build_family_engine(closes)

        for mult in DEFAULT_MULTS:
            params = family_params(mult)
            values = engine.get_indicator_value("MACD", SYMBOL, EXCHANGE, TIMEFRAME, params)
            ref_dif = ref_macd(closes, mult)[0][-1]
            assert values["DIF"] == pytest.approx(ref_dif, rel=1e-9)

    def test_incremental_update_no_cross_contamination(self):
        """增量推送后 4 组各自尾部仍与全量参考一致（状态隔离）"""
        closes = _gen_closes()
        engine = build_family_engine(closes)

        extra = [105.0, 106.5, 104.0, 107.2, 108.8]
        for i, close in enumerate(extra):
            ivs = engine.update_kline(mk_kline(close, BASE_TS + (N_BARS + i) * TF_MS))
            assert len(ivs) == 4  # 4 组全部产出

        extended = closes + extra
        for mult in DEFAULT_MULTS:
            series = engine.get_series(
                "MACD", family_params(mult), SYMBOL, EXCHANGE, TIMEFRAME
            )
            dif, dea, hist = ref_macd(extended, mult)
            tail = series[-1]["values"]
            assert tail["DIF"] == pytest.approx(dif[-1], rel=1e-9)
            assert tail["DEA"] == pytest.approx(dea[-1], rel=1e-9)
            assert tail["HIST"] == pytest.approx(hist[-1], rel=1e-9)


# ─── 策略端到端（IND-106 消费链路） ───


def _load_strategy() -> MACDFamilyStrategy:
    manager = StrategyManager()
    info = StrategyInfo(
        strategy_id="macd_family_1",
        name="MACDFamily",
        symbols=[SYMBOL],
        timeframes=[TIMEFRAME],
    )
    managed = manager.load_strategy("macd_family_1", MACDFamilyStrategy, info)
    manager.init_strategy("macd_family_1")
    return managed.strategy


class TestStrategyEndToEnd:
    def test_on_init_declares_four_instances(self):
        """on_init 声明 4 组实例且幂等注册到引擎"""
        strategy = _load_strategy()
        reqs = strategy.indicator_requirements
        assert len(reqs) == 4
        assert [r["params"] for r in reqs] == [family_params(m) for m in DEFAULT_MULTS]

        engine = IndicatorEngine()
        from core.strategy_engine.wiring import consume_requirements

        groups = consume_requirements(engine, strategy, EXCHANGE)
        assert groups == [(SYMBOL, TIMEFRAME)]
        assert len(engine.indicators_for(SYMBOL, EXCHANGE, TIMEFRAME)) == 4
        # 重复消费幂等
        consume_requirements(engine, strategy, EXCHANGE)
        assert len(engine.indicators_for(SYMBOL, EXCHANGE, TIMEFRAME)) == 4

    def test_column_naming_slug(self):
        """注入列命名：macd_f{fast}_g{signal}_s{slow}_{field}"""
        strategy = _load_strategy()
        assert strategy.hist_col(64) == "macd_f128_g192_s320_hist"
        assert col_slug("MACD", family_params(1)) == "macd_f2_g3_s5"
        assert field_col("MACD", family_params(4), "DIF") == "macd_f8_g12_s20_dif"

    def test_slow_family_null_before_warmup(self):
        """早期 bar：快组已出值、慢组（64x）预热未完成列为 null"""
        strategy = _load_strategy()
        closes = _gen_closes()
        engine = IndicatorEngine()
        warmup_from_df(engine, strategy, EXCHANGE, make_kline_df(closes))

        df = inject_indicators(
            engine, make_kline_df(closes), SYMBOL, EXCHANGE, TIMEFRAME
        )
        row100 = df.row(100, named=True)
        assert row100[strategy.hist_col(1)] is not None
        assert row100[strategy.hist_col(64)] is None  # 64x 需 511 根预热
        row700 = df.row(700, named=True)
        assert row700[strategy.hist_col(64)] is not None

    def test_on_bar_signals_match_reference(self):
        """on_bar 全循环：信号与 polars 参考逐 bar 对拍，LONG/SHORT 均出现"""
        strategy = _load_strategy()
        closes = _gen_closes()
        engine = IndicatorEngine()
        warmup_from_df(engine, strategy, EXCHANGE, make_kline_df(closes))
        df = inject_indicators(
            engine, make_kline_df(closes), SYMBOL, EXCHANGE, TIMEFRAME
        )

        refs = {m: ref_macd(closes, m) for m in DEFAULT_MULTS}
        produced: List[str] = []
        start = max(min_periods(m) for m in DEFAULT_MULTS) - 1  # 全部组就绪
        for i in range(start, N_BARS):
            signal = strategy.on_bar(df, i)
            produced.extend([signal] if signal else [])

            # 参考共识
            pos = neg = 0
            for mult in DEFAULT_MULTS:
                h = refs[mult][2][i]
                if h > 0:
                    pos += 1
                elif h < 0:
                    neg += 1
            expect = "LONG" if pos >= 3 else ("SHORT" if neg >= 3 else None)
            assert signal == expect, f"bar {i}: 策略={signal} 参考={expect}"

        assert "LONG" in produced and "SHORT" in produced
        assert len(produced) >= 20  # 随机游走数据应产生足量信号

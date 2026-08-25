"""MACDMultiStrategy 端到端 — MACD_MULTI def 式指标接入策略侧（IND-106 链路）

验证目标：
    1. on_init 声明单实例（多倍数由指标内部展开），幂等注册到引擎
    2. 引擎预热 + inject_indicators 列注入后，on_bar 按 field_col 读列出信号
"""
from __future__ import annotations

import random
from typing import List, Tuple

import polars as pl

import custom_indicators  # noqa: F401  触发 def 式指标注册
from core.indicator_engine.engine import IndicatorEngine
from core.strategy_engine.context import StrategyInfo
from core.strategy_engine.manager import StrategyManager
from core.strategy_engine.wiring import (
    consume_requirements,
    field_col,
    inject_indicators,
    warmup_from_df,
)
from strategies.macd_multi import MACDMultiStrategy

SYMBOL = "BTCUSDT"
EXCHANGE = "binance"
TIMEFRAME = "1m"
N_BARS = 800


def _make_df(n: int = N_BARS, seed: int = 11) -> pl.DataFrame:
    """随机游走行情（带趋势段，保证多倍数交叉场景充分）"""
    rng = random.Random(seed)
    close = 100.0
    drift = 0.0
    ts, opens, highs, lows, closes, vols = [], [], [], [], [], []
    t = 1_700_000_000_000
    for i in range(n):
        if i % 100 == 0:
            drift = rng.choice([-0.002, 0.0, 0.002])   # 每 100 根切换趋势
        o = close
        close = max(1.0, close * (1 + drift + rng.gauss(0, 0.004)))
        ts.append(t + i * 60_000)
        opens.append(o)
        highs.append(max(o, close) * 1.001)
        lows.append(min(o, close) * 0.999)
        closes.append(close)
        vols.append(rng.uniform(10, 1000))
    return pl.DataFrame({
        "timestamp": ts, "open": opens, "high": highs, "low": lows,
        "close": closes, "volume": vols,
    })


def _load_strategy() -> MACDMultiStrategy:
    manager = StrategyManager()
    info = StrategyInfo(
        strategy_id="macd_multi_1",
        name="MACDMulti",
        symbols=[SYMBOL],
        timeframes=[TIMEFRAME],
    )
    managed = manager.load_strategy("macd_multi_1", MACDMultiStrategy, info)
    manager.init_strategy("macd_multi_1")
    return managed.strategy


def test_on_init_declares_single_instance():
    """on_init 只声明一个实例（1X/4X/16X/64X 由指标内部展开）"""
    strategy = _load_strategy()
    reqs = strategy.indicator_requirements
    assert len(reqs) == 1
    assert reqs[0]["indicator"] == "MACD_MULTI"
    assert reqs[0]["params"] == {"s": 12, "p": 20, "m": 9}


def test_end_to_end_signals():
    """声明 → 预热 → 注入 → on_bar 逐根出信号"""
    strategy = _load_strategy()
    engine = IndicatorEngine()
    df = _make_df()

    consume_requirements(engine, strategy, EXCHANGE)
    warmup_from_df(engine, strategy, EXCHANGE, df)
    df = inject_indicators(engine, df, SYMBOL, EXCHANGE, TIMEFRAME)

    # 注入列存在（列名由 field_col 同源生成）
    params = {"s": 12, "p": 20, "m": 9}
    for field in ("DIF_16X", "DEA_16X", "DIF_1X", "DEA_1X"):
        assert field_col("MACD_MULTI", params, field) in df.columns

    signals: List[Tuple[int, str]] = []
    for i in range(len(df)):
        sig = strategy.on_bar(df, i)
        if sig:
            signals.append((i, sig))

    assert signals, "趋势段行情下应产生至少一个信号"
    assert all(s in ("LONG", "SHORT") for _, s in signals)

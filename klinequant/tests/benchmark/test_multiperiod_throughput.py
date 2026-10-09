"""Phase M 基准：拓扑 Z 多周期 engine 推进 + 策略回调吞吐（IND-108 约束）

规划 L253 验收：10 品种 × 3 周期（每品种一线程、线程内多周期单循环），engine
推进 + 策略回调总耗时 < 50ms/s。

测量口径（保守上界）：一次「全量派发轮」= 所有 10×3=30 个 (symbol,period) series
各推进一根新 bar（engine.update_kline，串行——拓扑 Z 下 engine 经共享锁串行化，
故串行总耗时即最坏上界）+ 每品种一次策略回调。真实运行中 15m/5m bar 不会每秒收盘，
最细 1m 亦每分钟每品种仅 1 根，故「全量对齐同时推进」是绝对最坏瞬时突发；即便此
突发 < 50ms，稳态每秒开销必然远低于阈值。
"""
from __future__ import annotations

import time
from decimal import Decimal

import polars as pl
import pytest

import core.indicator_engine.indicators  # noqa: F401  (导入即注册内置指标)
import custom_indicators  # noqa: F401  (注册自定义指标)
from core.indicator_engine.engine import IndicatorEngine
from protocol.types import Kline

SYMBOLS = [f"SYM{i}" for i in range(10)]
PERIODS = ["1m", "5m", "15m"]
PERIOD_MS = {"1m": 60_000, "5m": 300_000, "15m": 900_000}
WARMUP = 300
ROUNDS = 100
BASE_TS = 1_600_000_000_000
_MACD = {"fast_period": 12, "slow_period": 26, "signal_period": 9}


def _bars_df(n: int, step_ms: int, start_price: float = 100.0) -> pl.DataFrame:
    """构造预热用 DataFrame（与 indic.bars_to_df 同 schema）。"""
    ts = [BASE_TS + i * step_ms for i in range(n)]
    px = [start_price + (i % 17) * 0.1 for i in range(n)]
    return pl.DataFrame({
        "timestamp": ts,
        "open": px,
        "high": [p * 1.001 for p in px],
        "low": [p * 0.999 for p in px],
        "close": [p * 1.0005 for p in px],
        "volume": [1000.0] * n,
        "quote_volume": [0.0] * n,
        "trade_count": [0] * n,
        "is_closed": [True] * n,
    })


def _kline(sym: str, per: str, ts: int, px: float) -> Kline:
    d = Decimal(str(px))
    return Kline(
        symbol=sym, exchange="mt5", timeframe=per, timestamp=ts,
        open=d, high=d * Decimal("1.001"), low=d * Decimal("0.999"),
        close=d, volume=Decimal("1000"), quote_volume=Decimal("0"),
        trade_count=0, is_closed=True,
    )


def _build_engine() -> IndicatorEngine:
    """注册 10 品种 × 3 周期 × (MA20 + MACD) 并预热（update_kline 同步，无需 start）。"""
    eng = IndicatorEngine()
    for sym in SYMBOLS:
        for per in PERIODS:
            eng.ensure_indicator("MA", {"period": 20}, sym, "mt5", per)
            eng.ensure_indicator("MACD", _MACD, sym, "mt5", per)
            eng.warmup(sym, "mt5", per, _bars_df(WARMUP, PERIOD_MS[per]))
    return eng


def _strategy_callback(sym: str, engine: IndicatorEngine) -> None:
    """代表性策略回调：读两个指标最新值做一次比较（策略只消费，不重算）。"""
    for per in PERIODS:
        ma = engine.get_series("MA", {"period": 20}, sym, "mt5", per)
        macd = engine.get_series("MACD", _MACD, sym, "mt5", per)
        if ma and macd:
            _ = ma[-1]["values"].get("MA")
            _ = macd[-1]["values"].get("DIF")


def test_multiperiod_dispatch_round_under_50ms():
    """10 品种 × 3 周期全量派发轮（engine 推进 + 策略回调）均摊 < 50ms/轮。"""
    eng = _build_engine()
    # 各 series 预热后的下一根 bar 时间戳游标
    cursor = {
        (sym, per): BASE_TS + WARMUP * PERIOD_MS[per]
        for sym in SYMBOLS for per in PERIODS
    }

    # 预热计时器（JIT/缓存冷启动不计入）
    for _ in range(3):
        for sym in SYMBOLS:
            for per in PERIODS:
                cursor[(sym, per)] += PERIOD_MS[per]
                eng.update_kline(_kline(sym, per, cursor[(sym, per)], 100.0))
        for sym in SYMBOLS:
            _strategy_callback(sym, eng)

    start = time.perf_counter()
    for _ in range(ROUNDS):
        # ① engine 推进：全量 30 series 各推进一根（串行 = 最坏上界）
        for sym in SYMBOLS:
            for per in PERIODS:
                cursor[(sym, per)] += PERIOD_MS[per]
                eng.update_kline(_kline(sym, per, cursor[(sym, per)], 100.0))
        # ② 策略回调：每品种一次
        for sym in SYMBOLS:
            _strategy_callback(sym, eng)
    elapsed_ms = (time.perf_counter() - start) * 1000
    per_round_ms = elapsed_ms / ROUNDS

    assert per_round_ms < 50.0, (
        f"10 sym × 3 period 全量派发轮 {per_round_ms:.2f}ms/轮 (limit 50ms)"
    )


def test_single_series_update_is_o1_cheap():
    """单 series 增量推进 < 1ms（快照法 O(1) 递推，非全量重放）。"""
    eng = IndicatorEngine()
    eng.ensure_indicator("MACD", _MACD, "SYM0", "mt5", "1m")
    eng.warmup("SYM0", "mt5", "1m", _bars_df(WARMUP, PERIOD_MS["1m"]))
    ts = BASE_TS + WARMUP * PERIOD_MS["1m"]

    for _ in range(5):     # 预热计时器
        ts += PERIOD_MS["1m"]
        eng.update_kline(_kline("SYM0", "1m", ts, 100.0))

    start = time.perf_counter()
    n = 500
    for _ in range(n):
        ts += PERIOD_MS["1m"]
        eng.update_kline(_kline("SYM0", "1m", ts, 100.0))
    per_update_ms = (time.perf_counter() - start) * 1000 / n

    assert per_update_ms < 1.0, f"单 series update_kline {per_update_ms:.3f}ms (limit 1ms)"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

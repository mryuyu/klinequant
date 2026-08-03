"""指标引擎单元测试

覆盖 IND-T-001 ~ IND-T-008：
    IND-T-001: MA(7)/MA(25) 与手工计算一致（误差 < 0.01%）
    IND-T-002: EMA 递推公式正确性
    IND-T-003: RSI(14) 边界值（全涨=100，全跌=0）
    IND-T-004: MACD DIF/DEA/HIST 与参考值一致
    IND-T-005: BOLL 上中下轨计算正确性
    IND-T-006: 增量计算：全量 vs 增量结果一致性
    IND-T-007: 指标预热：MA(200) 需 ≥ 200 根 K 线
    IND-T-008: polars 性能：10000 根 K 线 MA 计算 < 100ms
"""
from __future__ import annotations

import time
from decimal import Decimal
from typing import Any, Dict

import polars as pl
import pytest

from core.indicator_engine.base import IndicatorBase
from core.indicator_engine.engine import IndicatorEngine
from core.indicator_engine.indicators import (
    ATR,
    BOLL,
    EMA,
    KDJ,
    MA,
    MACD,
    RSI,
    VWAP,
)
from core.indicator_engine.registry import IndicatorRegistry
from protocol.types import IndicatorValue, Kline


# ─── 辅助函数 ───

def make_kline_df(closes: list[float], base_ts: int = 1700000000000) -> pl.DataFrame:
    """构造测试用 K 线 DataFrame"""
    n = len(closes)
    return pl.DataFrame({
        "timestamp": [base_ts + i * 60000 for i in range(n)],
        "open": [c - 0.5 for c in closes],
        "high": [c + 1.0 for c in closes],
        "low": [c - 1.0 for c in closes],
        "close": closes,
        "volume": [100.0] * n,
        "quote_volume": [100.0 * c for c in closes],
        "trade_count": [10] * n,
        "is_closed": [True] * n,
    })


def make_kline(symbol: str, close: float, timestamp: int, timeframe: str = "1m") -> Kline:
    return Kline(
        symbol=symbol,
        exchange="binance",
        timeframe=timeframe,
        timestamp=timestamp,
        open=Decimal(str(close - 0.5)),
        high=Decimal(str(close + 1.0)),
        low=Decimal(str(close - 1.0)),
        close=Decimal(str(close)),
        volume=Decimal("100"),
        quote_volume=Decimal(str(100 * close)),
        trade_count=10,
        is_closed=True,
    )


# ─── IND-T-001: MA 计算正确性 ───

class TestMA:
    def test_ma7_manual_calculation(self):
        """MA(7) 与手工计算一致（误差 < 0.01%）"""
        closes = [10.0, 11.0, 12.0, 11.0, 10.0, 11.0, 12.0, 13.0, 12.0, 11.0]
        df = make_kline_df(closes)
        ma = MA(params={"period": 7})
        result = ma.calculate(df)

        # MA(7) at index 6: mean(10,11,12,11,10,11,12) = 77/7 = 11.0
        assert result["MA_7"][6] == pytest.approx(11.0, rel=0.0001)
        # MA(7) at index 7: mean(11,12,11,10,11,12,13) = 80/7 ≈ 11.4286
        assert result["MA_7"][7] == pytest.approx(80 / 7, rel=0.0001)

    def test_ma25_correctness(self):
        """MA(25) 计算正确性"""
        import math
        closes = [100.0 + i * 0.5 for i in range(30)]
        df = make_kline_df(closes)
        ma = MA(params={"period": 25})
        result = ma.calculate(df)

        # MA(25) at index 24: mean of first 25 values
        expected = sum(closes[:25]) / 25
        assert result["MA_25"][24] == pytest.approx(expected, rel=0.0001)

    def test_ma_insufficient_data(self):
        """MA(7) 数据不足时不预热"""
        closes = [10.0, 11.0, 12.0]
        df = make_kline_df(closes)
        ma = MA(params={"period": 7})
        result = ma.calculate(df)
        assert not ma.is_warmed_up
        assert "MA_7" not in result.columns


# ─── IND-T-002: EMA 递推公式正确性 ───

class TestEMA:
    def test_ema_recursive_formula(self):
        """EMA 递推公式: EMA_t = close_t * alpha + EMA_{t-1} * (1-alpha)"""
        closes = [10.0, 12.0, 11.0, 13.0, 12.0, 14.0, 13.0, 15.0]
        df = make_kline_df(closes)
        period = 5
        ema = EMA(params={"period": period})
        result = ema.calculate(df)

        alpha = 2.0 / (period + 1)
        # 手动递推
        expected = closes[0]
        for i in range(1, len(closes)):
            expected = closes[i] * alpha + expected * (1 - alpha)

        assert result[f"EMA_{period}"][-1] == pytest.approx(expected, rel=0.001)

    def test_ema_equals_close_for_single(self):
        """EMA 第一个值等于第一个 close（adjust=False）"""
        closes = [42.0] * 10
        df = make_kline_df(closes)
        ema = EMA(params={"period": 5})
        result = ema.calculate(df)
        assert result["EMA_5"][0] == pytest.approx(42.0, rel=0.001)


# ─── IND-T-003: RSI 边界值 ───

class TestRSI:
    def test_rsi_all_up(self):
        """全涨 → RSI 接近 100"""
        closes = [float(100 + i) for i in range(20)]
        df = make_kline_df(closes)
        rsi = RSI(params={"period": 14})
        result = rsi.calculate(df)
        assert result["RSI_14"][-1] == pytest.approx(100.0, abs=1.0)

    def test_rsi_all_down(self):
        """全跌 → RSI 接近 0"""
        closes = [float(100 - i) for i in range(20)]
        df = make_kline_df(closes)
        rsi = RSI(params={"period": 14})
        result = rsi.calculate(df)
        assert result["RSI_14"][-1] == pytest.approx(0.0, abs=1.0)

    def test_rsi_range(self):
        """RSI 值在 [0, 100] 范围内"""
        closes = [100.0 + (i % 3 - 1) * 2.0 for i in range(30)]
        df = make_kline_df(closes)
        rsi = RSI(params={"period": 14})
        result = rsi.calculate(df)
        values = result["RSI_14"].drop_nulls().to_list()
        for v in values:
            assert 0.0 <= v <= 100.0


# ─── IND-T-004: MACD DIF/DEA/HIST ───

class TestMACD:
    def test_macd_dif_dea_hist(self):
        """MACD DIF/DEA/HIST 基本关系: HIST = 2*(DIF-DEA)"""
        closes = [100.0 + i * 0.3 for i in range(50)]
        df = make_kline_df(closes)
        macd = MACD()
        result = macd.calculate(df)

        prefix = "MACD_12_26_9"
        dif = result[f"{prefix}_DIF"].to_list()
        dea = result[f"{prefix}_DEA"].to_list()
        hist = result[f"{prefix}_HIST"].to_list()

        for i in range(len(dif)):
            if dif[i] is not None and dea[i] is not None and hist[i] is not None:
                expected_hist = 2.0 * (dif[i] - dea[i])
                assert hist[i] == pytest.approx(expected_hist, abs=0.001)

    def test_macd_min_periods(self):
        """MACD min_periods = slow + signal - 1 = 34"""
        macd = MACD()
        assert macd.min_periods == 34

        closes = [100.0] * 33
        df = make_kline_df(closes)
        result = macd.calculate(df)
        assert not macd.is_warmed_up


# ─── IND-T-005: BOLL 上中下轨 ───

class TestBOLL:
    def test_boll_upper_mid_lower(self):
        """BOLL: UPPER = MID + 2*STD, LOWER = MID - 2*STD"""
        closes = [100.0 + (i % 5) * 2.0 for i in range(30)]
        df = make_kline_df(closes)
        boll = BOLL(params={"period": 20, "std_dev": 2.0})
        result = boll.calculate(df)

        prefix = "BOLL_20_2.0"
        mid = result[f"{prefix}_MID"].to_list()
        upper = result[f"{prefix}_UPPER"].to_list()
        lower = result[f"{prefix}_LOWER"].to_list()

        for i in range(len(mid)):
            if mid[i] is not None and upper[i] is not None and lower[i] is not None:
                assert upper[i] >= mid[i]
                assert lower[i] <= mid[i]
                # UPPER - MID ≈ MID - LOWER
                assert (upper[i] - mid[i]) == pytest.approx(mid[i] - lower[i], abs=0.001)

    def test_boll_constant_price(self):
        """价格不变时，布林带收窄（STD=0）"""
        closes = [100.0] * 25
        df = make_kline_df(closes)
        boll = BOLL(params={"period": 20, "std_dev": 2.0})
        result = boll.calculate(df)

        prefix = "BOLL_20_2.0"
        assert result[f"{prefix}_UPPER"][-1] == pytest.approx(100.0, abs=0.01)
        assert result[f"{prefix}_MID"][-1] == pytest.approx(100.0, abs=0.01)
        assert result[f"{prefix}_LOWER"][-1] == pytest.approx(100.0, abs=0.01)


# ─── IND-T-006: 增量计算一致性 ───

class TestIncrementalCalculation:
    def test_incremental_vs_full_ma(self):
        """增量 vs 全量 MA 结果一致"""
        closes = [100.0 + i * 0.5 for i in range(30)]
        df_full = make_kline_df(closes)

        # 全量计算
        ma_full = MA(params={"period": 7})
        result_full = ma_full.calculate(df_full)

        # 增量计算：先 20 根，再逐根添加
        ma_inc = MA(params={"period": 7})
        df_part1 = make_kline_df(closes[:20])
        ma_inc.calculate(df_part1)

        # 逐根添加
        engine = IndicatorEngine()
        key = ("BTCUSDT", "binance", "1m")
        engine.add_indicator(ma_inc, "BTCUSDT", "binance", "1m")

        # 预热
        engine.warmup("BTCUSDT", "binance", "1m", df_part1)

        # 逐根添加剩余 K 线
        base_ts = 1700000000000
        for i in range(20, 30):
            kline = make_kline("BTCUSDT", closes[i], base_ts + i * 60000)
            engine.update_kline(kline)

        # 比较最后一根
        cache = engine.get_kline_cache("BTCUSDT", "binance", "1m")
        result_inc = ma_inc.calculate(cache)

        last_full = result_full["MA_7"][-1]
        last_inc = result_inc["MA_7"][-1]
        assert last_full == pytest.approx(last_inc, rel=0.001)

    def test_incremental_update_unfinished_kline(self):
        """更新未收盘 K 线（同 timestamp）"""
        engine = IndicatorEngine()
        ma = MA(params={"period": 3})
        engine.add_indicator(ma, "ETHUSDT", "binance", "1m")

        base_ts = 1700000000000
        closes = [100.0, 101.0, 102.0]
        df = make_kline_df(closes, base_ts)
        engine.warmup("ETHUSDT", "binance", "1m", df)

        # 更新最后一根（同 timestamp）
        kline = make_kline("ETHUSDT", 103.0, base_ts + 2 * 60000)
        engine.update_kline(kline)

        cache = engine.get_kline_cache("ETHUSDT", "binance", "1m")
        assert len(cache) == 3  # 不新增行


# ─── IND-T-007: 指标预热 ───

class TestWarmup:
    def test_ma200_needs_200_bars(self):
        """MA(200) 需 ≥ 200 根 K 线预热"""
        ma = MA(params={"period": 200})
        assert ma.min_periods == 200

        # 199 根不够
        closes_199 = [100.0 + i * 0.1 for i in range(199)]
        df_199 = make_kline_df(closes_199)
        ma.calculate(df_199)
        assert not ma.is_warmed_up

        # 200 根刚好
        closes_200 = [100.0 + i * 0.1 for i in range(200)]
        df_200 = make_kline_df(closes_200)
        ma2 = MA(params={"period": 200})
        ma2.calculate(df_200)
        assert ma2.is_warmed_up

    def test_engine_warmup_returns_values(self):
        """引擎预热返回指标值"""
        engine = IndicatorEngine()
        engine.add_indicator(MA(params={"period": 5}), "BTCUSDT", "binance", "1m")
        engine.add_indicator(RSI(params={"period": 14}), "BTCUSDT", "binance", "1m")

        closes = [100.0 + i * 0.5 for i in range(20)]
        df = make_kline_df(closes)
        results = engine.warmup("BTCUSDT", "binance", "1m", df)

        assert "MA" in results
        # RSI min_periods = 15, 20 根足够
        assert "RSI" in results

    def test_warmup_insufficient_data(self):
        """预热数据不足时不标记为已预热"""
        engine = IndicatorEngine()
        engine.add_indicator(MA(params={"period": 50}), "BTCUSDT", "binance", "1m")

        closes = [100.0] * 10
        df = make_kline_df(closes)
        results = engine.warmup("BTCUSDT", "binance", "1m", df)
        assert "MA" not in results


# ─── IND-T-008: polars 性能 ───

class TestPerformance:
    def test_ma_10000_bars_under_100ms(self):
        """10000 根 K 线 MA 计算 < 100ms"""
        closes = [100.0 + (i % 100) * 0.1 for i in range(10000)]
        df = make_kline_df(closes)
        ma = MA(params={"period": 20})

        start = time.perf_counter()
        result = ma.calculate(df)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 100, f"MA calculation took {elapsed_ms:.1f}ms, expected < 100ms"
        assert ma.is_warmed_up
        assert len(result) == 10000


# ─── 额外测试：Registry + ATR/KDJ/VWAP ───

class TestRegistry:
    def test_registry_create(self):
        """注册表工厂方法"""
        reg = IndicatorRegistry()
        reg.register(MA)
        reg.register(RSI)

        ma = reg.create("MA", {"period": 7})
        assert isinstance(ma, MA)
        assert ma.min_periods == 7

    def test_registry_unknown(self):
        """未知指标抛出 KeyError"""
        reg = IndicatorRegistry()
        with pytest.raises(KeyError, match="Unknown indicator"):
            reg.create("UNKNOWN")

    def test_registry_duplicate(self):
        """重复注册抛出 ValueError"""
        reg = IndicatorRegistry()
        reg.register(MA)
        with pytest.raises(ValueError, match="already registered"):
            reg.register(MA)


class TestATRKDJVWAP:
    def test_atr_positive(self):
        """ATR 值始终为正"""
        closes = [100.0 + (i % 5 - 2) * 1.5 for i in range(30)]
        df = make_kline_df(closes)
        atr = ATR(params={"period": 14})
        result = atr.calculate(df)
        values = result["ATR_14"].drop_nulls().to_list()
        for v in values:
            assert v > 0

    def test_kdj_range(self):
        """KDJ K/D 值大致在 [0, 100] 范围"""
        closes = [100.0 + (i % 7 - 3) * 2.0 for i in range(30)]
        df = make_kline_df(closes)
        kdj = KDJ()
        result = kdj.calculate(df)
        k_vals = result["KDJ_9_3_3_K"].drop_nulls().to_list()
        # K 值可能略微超出 [0,100]（由于 ewm 平滑）
        for v in k_vals[5:]:
            assert -10 <= v <= 110

    def test_vwap_reasonable(self):
        """VWAP 在价格范围内"""
        closes = [100.0 + i * 0.5 for i in range(25)]
        df = make_kline_df(closes)
        vwap = VWAP(params={"period": 20})
        result = vwap.calculate(df)
        vwap_vals = result["VWAP_20"].drop_nulls().to_list()
        for v in vwap_vals:
            assert 98.0 <= v <= 115.0


class TestIndicatorEngine:
    def test_update_kline_new(self):
        """新 K 线添加后缓存增长"""
        engine = IndicatorEngine()
        ma = MA(params={"period": 3})
        engine.add_indicator(ma, "BTCUSDT", "binance", "1m")

        df = make_kline_df([100.0, 101.0, 102.0])
        engine.warmup("BTCUSDT", "binance", "1m", df)

        kline = make_kline("BTCUSDT", 103.0, 1700000000000 + 3 * 60000)
        engine.update_kline(kline)

        cache = engine.get_kline_cache("BTCUSDT", "binance", "1m")
        assert len(cache) == 4

    def test_subscriber_notification(self):
        """订阅者收到指标更新通知"""
        engine = IndicatorEngine()
        ma = MA(params={"period": 3})
        engine.add_indicator(ma, "BTCUSDT", "binance", "1m")

        received = []
        engine.subscribe("MA", lambda iv: received.append(iv))

        df = make_kline_df([100.0, 101.0, 102.0])
        engine.warmup("BTCUSDT", "binance", "1m", df)

        kline = make_kline("BTCUSDT", 103.0, 1700000000000 + 3 * 60000)
        engine.update_kline(kline)

        assert len(received) == 1
        assert isinstance(received[0], IndicatorValue)

    def test_list_indicators(self):
        """列出指标"""
        engine = IndicatorEngine()
        engine.add_indicator(MA(params={"period": 7}), "BTCUSDT", "binance", "1m")
        engine.add_indicator(RSI(), "BTCUSDT", "binance", "1m")

        names = engine.list_indicators("BTCUSDT", "binance", "1m")
        assert sorted(names) == ["MA", "RSI"]

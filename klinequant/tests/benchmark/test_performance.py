"""性能压测套件

覆盖指标：
    1. 指标计算性能：MA/RSI/MACD 在 10K/100K 根 K 线下的计算时间
    2. 回测引擎吞吐：完整回测流程耗时
    3. 数据标准化性能：Kline 创建和验证吞吐
    4. 内存占用：大规模数据下的内存使用
    5. 并发性能：多策略并行回测

运行方式：
    pytest tests/benchmark/ -v --tb=short
"""
from __future__ import annotations

import gc
import sys
import time
from decimal import Decimal
from typing import Optional

import polars as pl
import pytest

# ─── 辅助函数 ───


def make_kline_df(n: int, start_price: float = 50000.0) -> pl.DataFrame:
    """生成模拟 K 线数据"""
    import random
    random.seed(42)

    timestamps = [1000000 + i * 60000 for i in range(n)]
    prices = [start_price]
    for i in range(1, n):
        change = random.uniform(-0.02, 0.02)
        prices.append(prices[-1] * (1 + change))

    return pl.DataFrame({
        "timestamp": timestamps,
        "open": prices,
        "high": [p * 1.005 for p in prices],
        "low": [p * 0.995 for p in prices],
        "close": [p * (1 + random.uniform(-0.005, 0.005)) for p in prices],
        "volume": [random.uniform(100, 10000) for _ in range(n)],
    })


def get_memory_mb() -> float:
    """获取当前进程内存占用 (MB)"""
    try:
        import psutil
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    except ImportError:
        # fallback: 使用 sys.getsizeof 估算
        gc.collect()
        return 0.0


# ─── 指标计算性能 ───


class TestIndicatorPerformance:
    """指标计算性能测试"""

    def test_ma_10k_bars(self):
        """MA 计算 10K 根 K 线 < 50ms"""
        from core.indicator_engine.indicators import MA

        df = make_kline_df(10000)
        ma = MA(params={"period": 20})

        start = time.perf_counter()
        result = ma.calculate(df)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 50, f"MA 10K bars: {elapsed_ms:.1f}ms (limit: 50ms)"
        assert len(result) == 10000

    def test_ma_100k_bars(self):
        """MA 计算 100K 根 K 线 < 500ms"""
        from core.indicator_engine.indicators import MA

        df = make_kline_df(100000)
        ma = MA(params={"period": 20})

        start = time.perf_counter()
        result = ma.calculate(df)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 500, f"MA 100K bars: {elapsed_ms:.1f}ms (limit: 500ms)"
        assert len(result) == 100000

    def test_rsi_10k_bars(self):
        """RSI 计算 10K 根 K 线 < 100ms"""
        from core.indicator_engine.indicators import RSI

        df = make_kline_df(10000)
        rsi = RSI(params={"period": 14})

        start = time.perf_counter()
        result = rsi.calculate(df)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 100, f"RSI 10K bars: {elapsed_ms:.1f}ms (limit: 100ms)"

    def test_macd_10k_bars(self):
        """MACD 计算 10K 根 K 线 < 100ms"""
        from core.indicator_engine.indicators import MACD

        df = make_kline_df(10000)
        macd = MACD(params={"fast": 12, "slow": 26, "signal": 9})

        start = time.perf_counter()
        result = macd.calculate(df)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 100, f"MACD 10K bars: {elapsed_ms:.1f}ms (limit: 100ms)"

    def test_bollinger_10k_bars(self):
        """布林带计算 10K 根 K 线 < 100ms"""
        from core.indicator_engine.indicators import BOLL

        df = make_kline_df(10000)
        bb = BOLL(params={"period": 20, "std_dev": 2.0})

        start = time.perf_counter()
        result = bb.calculate(df)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 100, f"Bollinger 10K bars: {elapsed_ms:.1f}ms (limit: 100ms)"

    def test_multiple_indicators_10k(self):
        """多指标联合计算 10K 根 < 300ms"""
        from core.indicator_engine.indicators import MA, RSI, MACD, BOLL

        df = make_kline_df(10000)
        indicators = [
            MA(params={"period": 7}),
            MA(params={"period": 25}),
            MA(params={"period": 99}),
            RSI(params={"period": 14}),
            MACD(params={"fast": 12, "slow": 26, "signal": 9}),
            BOLL(params={"period": 20, "std_dev": 2.0}),
        ]

        start = time.perf_counter()
        for ind in indicators:
            ind.calculate(df)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 300, f"6 indicators 10K bars: {elapsed_ms:.1f}ms (limit: 300ms)"


# ─── 回测引擎性能 ───


class TestBacktestPerformance:
    """回测引擎性能测试"""

    def test_backtest_1k_bars(self):
        """回测 1K 根 K 线 < 2s"""
        from core.backtest_engine.engine import BacktestConfig, BacktestEngine

        config = BacktestConfig(initial_capital=Decimal("100000"))
        engine = BacktestEngine(config)
        data = make_kline_df(1000)

        def strategy(df: pl.DataFrame, bar_idx: int) -> Optional[str]:
            if bar_idx < 25:
                return None
            closes = df["close"]
            ma7 = closes.slice(bar_idx - 6, 7).mean()
            ma25 = closes.slice(bar_idx - 24, 25).mean()
            prev_ma7 = closes.slice(bar_idx - 7, 7).mean()
            prev_ma25 = closes.slice(bar_idx - 25, 25).mean()
            if prev_ma7 <= prev_ma25 and ma7 > ma25:
                return "LONG"
            if prev_ma7 >= prev_ma25 and ma7 < ma25:
                return "SHORT"
            return None

        start = time.perf_counter()
        result = engine.run(data, strategy)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 2000, f"Backtest 1K bars: {elapsed_ms:.1f}ms (limit: 2000ms)"
        assert len(result.equity_curve) == 1001

    def test_backtest_5k_bars(self):
        """回测 5K 根 K 线 < 10s"""
        from core.backtest_engine.engine import BacktestConfig, BacktestEngine

        config = BacktestConfig(initial_capital=Decimal("100000"))
        engine = BacktestEngine(config)
        data = make_kline_df(5000)

        def strategy(df: pl.DataFrame, bar_idx: int) -> Optional[str]:
            if bar_idx % 100 == 0:
                return "LONG"
            if bar_idx % 100 == 50:
                return "CLOSE"
            return None

        start = time.perf_counter()
        result = engine.run(data, strategy)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 10000, f"Backtest 5K bars: {elapsed_ms:.1f}ms (limit: 10000ms)"


# ─── 数据标准化性能 ───


class TestDataNormalization:
    """数据标准化性能测试"""

    def test_kline_creation_10k(self):
        """创建 10K 个 Kline 对象 < 500ms"""
        from protocol.types import Kline

        start = time.perf_counter()
        klines = []
        for i in range(10000):
            k = Kline(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1m",
                timestamp=1000000 + i * 60000,
                open=Decimal("50000"),
                high=Decimal("50100"),
                low=Decimal("49900"),
                close=Decimal("50050"),
                volume=Decimal("100"),
                quote_volume=Decimal("5000000"),
                trade_count=50,
                is_closed=True,
            )
            klines.append(k)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 500, f"10K Kline creation: {elapsed_ms:.1f}ms (limit: 500ms)"
        assert len(klines) == 10000

    def test_okx_normalization_10k(self):
        """OKX 数据标准化 10K 条 < 200ms"""
        from core.market_engine.okx_normalizer import normalize_okx_klines

        raw_data = [
            [str(1000000 + i * 60000), "50000", "50100", "49900", "50050", "100", "5000000", "5000000", "1"]
            for i in range(10000)
        ]

        start = time.perf_counter()
        klines = normalize_okx_klines(raw_data, "BTCUSDT", "1m")
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 200, f"OKX normalize 10K: {elapsed_ms:.1f}ms (limit: 200ms)"
        assert len(klines) == 10000


# ─── 内存性能 ───


class TestMemoryPerformance:
    """内存占用测试"""

    def test_dataframe_100k_memory(self):
        """100K 行 DataFrame 内存 < 50MB"""
        df = make_kline_df(100000)

        # polars DataFrame 内存估算
        mem_bytes = df.estimated_size("mb")
        assert mem_bytes < 50, f"100K DataFrame: {mem_bytes:.1f}MB (limit: 50MB)"

    def test_kline_objects_memory(self):
        """10K 个 Kline 对象内存合理"""
        from protocol.types import Kline

        klines = []
        for i in range(10000):
            k = Kline(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1m",
                timestamp=1000000 + i * 60000,
                open=Decimal("50000"),
                high=Decimal("50100"),
                low=Decimal("49900"),
                close=Decimal("50050"),
                volume=Decimal("100"),
                quote_volume=Decimal("5000000"),
                trade_count=50,
                is_closed=True,
            )
            klines.append(k)

        # 估算内存（每个对象约 500 bytes）
        total_size = sys.getsizeof(klines) + sum(sys.getsizeof(k) for k in klines[:100]) * 100
        total_mb = total_size / 1024 / 1024

        # 宽松限制：10K 对象 < 100MB
        assert total_mb < 100, f"10K Klines: {total_mb:.1f}MB (limit: 100MB)"


# ─── 参数优化性能 ───


class TestOptimizerPerformance:
    """参数优化器性能测试"""

    def test_grid_search_small(self):
        """网格搜索 25 组合 < 30s"""
        from core.backtest_engine.optimizer import (
            OptimizationConfig,
            ParameterOptimizer,
            ParamRange,
        )

        config = OptimizationConfig(
            method="grid",
            max_workers=1,
            sort_by="sharpe_ratio",
            top_n=5,
        )
        optimizer = ParameterOptimizer(config)
        data = make_kline_df(500)

        def strategy_fn(df: pl.DataFrame, bar_idx: int, params: dict) -> Optional[str]:
            fast = params.get("fast", 7)
            slow = params.get("slow", 25)
            if bar_idx < slow:
                return None
            closes = df["close"]
            ma_fast = closes.slice(bar_idx - fast + 1, fast).mean()
            ma_slow = closes.slice(bar_idx - slow + 1, slow).mean()
            prev_fast = closes.slice(bar_idx - fast, fast).mean()
            prev_slow = closes.slice(bar_idx - slow, slow).mean()
            if prev_fast <= prev_slow and ma_fast > ma_slow:
                return "LONG"
            if prev_fast >= prev_slow and ma_fast < ma_slow:
                return "SHORT"
            return None

        param_ranges = [
            ParamRange.range_int("fast", 5, 15, 5),  # 3 values
            ParamRange.range_int("slow", 20, 40, 10),  # 3 values
        ]

        start = time.perf_counter()
        report = optimizer.optimize(data, strategy_fn, param_ranges)
        elapsed_s = time.perf_counter() - start

        assert elapsed_s < 30, f"Grid search 9 combos: {elapsed_s:.1f}s (limit: 30s)"
        assert report.completed == 9
        assert len(report.results) <= 5

    def test_random_search_sampling(self):
        """随机搜索采样正确性"""
        from core.backtest_engine.optimizer import (
            OptimizationConfig,
            ParameterOptimizer,
            ParamRange,
        )

        config = OptimizationConfig(
            method="random",
            n_samples=20,
            max_workers=1,
        )
        optimizer = ParameterOptimizer(config)
        data = make_kline_df(200)

        def strategy_fn(df: pl.DataFrame, bar_idx: int, params: dict) -> Optional[str]:
            if bar_idx == 50:
                return "LONG"
            if bar_idx == 100:
                return "CLOSE"
            return None

        param_ranges = [
            ParamRange.range_int("p1", 1, 100),
            ParamRange.range_int("p2", 1, 100),
        ]

        report = optimizer.optimize(data, strategy_fn, param_ranges)

        # 随机搜索应该采样 20 组（或更少如果空间小）
        assert report.completed <= 20
        assert report.method == "random"


# ─── 综合压力测试 ───


class TestStressTest:
    """综合压力测试"""

    def test_rapid_kline_processing(self):
        """快速连续处理 1000 根 K 线 < 5s"""
        from core.indicator_engine.engine import IndicatorEngine
        from core.indicator_engine.indicators import MA, RSI
        from protocol.types import Kline

        engine = IndicatorEngine()
        engine.add_indicator(MA(params={"period": 20}), "BTCUSDT", "binance", "1m")
        engine.add_indicator(RSI(params={"period": 14}), "BTCUSDT", "binance", "1m")

        df = make_kline_df(1000)

        start = time.perf_counter()
        for i in range(100, 1000):
            # 模拟实时处理：构造单根 Kline 更新
            row = df.slice(i, 1)
            kline = Kline(
                symbol="BTCUSDT", exchange="binance", timeframe="1m",
                timestamp=int(row["timestamp"][0]),
                open=Decimal(str(row["open"][0])),
                high=Decimal(str(row["high"][0])),
                low=Decimal(str(row["low"][0])),
                close=Decimal(str(row["close"][0])),
                volume=Decimal(str(row["volume"][0])),
                quote_volume=Decimal(str(row["volume"][0])) * Decimal(str(row["close"][0])),
                trade_count=10, is_closed=True,
            )
            engine.update_kline(kline)
        elapsed_s = time.perf_counter() - start

        assert elapsed_s < 5, f"900 kline updates: {elapsed_s:.1f}s (limit: 5s)"

    def test_concurrent_backtest(self):
        """并行回测 4 个策略 < 20s"""
        from concurrent.futures import ThreadPoolExecutor
        from core.backtest_engine.engine import BacktestConfig, BacktestEngine

        data = make_kline_df(1000)

        def run_backtest(strategy_id: int):
            config = BacktestConfig(initial_capital=Decimal("100000"))
            engine = BacktestEngine(config)

            def strategy(df: pl.DataFrame, bar_idx: int) -> Optional[str]:
                if bar_idx % (50 + strategy_id * 10) == 0:
                    return "LONG"
                if bar_idx % (50 + strategy_id * 10) == 25:
                    return "CLOSE"
                return None

            return engine.run(data, strategy)

        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(run_backtest, i) for i in range(4)]
            results = [f.result() for f in futures]
        elapsed_s = time.perf_counter() - start

        assert elapsed_s < 20, f"4 parallel backtests: {elapsed_s:.1f}s (limit: 20s)"
        assert all(len(r.equity_curve) == 1001 for r in results)

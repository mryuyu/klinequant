"""ParameterOptimizer — 参数优化回测

支持网格搜索和随机搜索两种模式：
    - 网格搜索：穷举参数空间所有组合
    - 随机搜索：随机采样 N 组参数组合
    - 并行执行：多进程并发回测
    - 结果排名：按指定指标排序
    - 过拟合检测：样本内/样本外对比

遵循需求文档 §4.5 BT-004。
"""
from __future__ import annotations

import itertools
import logging
import random
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl

from core.backtest_engine.engine import BacktestConfig, BacktestEngine, BacktestResult

logger = logging.getLogger(__name__)


# ─── 参数空间定义 ───


@dataclass
class ParamRange:
    """参数范围定义"""
    name: str
    values: List[Any]  # 离散值列表

    @staticmethod
    def linspace(name: str, start: float, end: float, steps: int) -> "ParamRange":
        """等间距数值范围"""
        if steps <= 1:
            return ParamRange(name=name, values=[start])
        step = (end - start) / (steps - 1)
        values = [round(start + i * step, 10) for i in range(steps)]
        return ParamRange(name=name, values=values)

    @staticmethod
    def range_int(name: str, start: int, end: int, step: int = 1) -> "ParamRange":
        """整数范围"""
        return ParamRange(name=name, values=list(range(start, end + 1, step)))

    @staticmethod
    def choices(name: str, values: List[Any]) -> "ParamRange":
        """离散选项"""
        return ParamRange(name=name, values=values)

    @property
    def size(self) -> int:
        return len(self.values)


@dataclass
class OptimizationConfig:
    """优化配置"""
    method: str = "grid"  # "grid" / "random"
    n_samples: int = 100  # 随机搜索采样数
    max_workers: int = 4  # 并行进程数
    sort_by: str = "sharpe_ratio"  # 排序指标
    ascending: bool = False  # 降序（越大越好）
    top_n: int = 10  # 返回前 N 个结果
    # 样本外验证
    enable_walk_forward: bool = False
    train_ratio: float = 0.7  # 训练集比例


# ─── 优化结果 ───


@dataclass
class OptimizationResult:
    """单次优化结果"""
    params: Dict[str, Any]
    total_return: float
    annual_return: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    profit_factor: float
    total_trades: int
    duration_ms: int
    # 样本外结果（walk-forward）
    oos_total_return: Optional[float] = None
    oos_sharpe_ratio: Optional[float] = None
    oos_max_drawdown: Optional[float] = None

    def score(self, metric: str = "sharpe_ratio") -> float:
        """获取指定指标分数"""
        return getattr(self, metric, 0.0) or 0.0


@dataclass
class OptimizationReport:
    """优化报告"""
    optimization_id: str
    method: str
    total_combinations: int
    completed: int
    failed: int
    results: List[OptimizationResult]
    best_params: Dict[str, Any]
    best_score: float
    sort_metric: str
    duration_ms: int
    created_at: int

    def summary(self) -> str:
        return (
            f"=== Optimization {self.optimization_id[:8]} ===\n"
            f"Method: {self.method} | Combinations: {self.total_combinations}\n"
            f"Completed: {self.completed} | Failed: {self.failed}\n"
            f"Best {self.sort_metric}: {self.best_score:.4f}\n"
            f"Best Params: {self.best_params}\n"
            f"Duration: {self.duration_ms}ms\n"
        )


# ─── 优化器 ───


class ParameterOptimizer:
    """参数优化器

    使用方式：
        optimizer = ParameterOptimizer(config)
        report = optimizer.optimize(
            data=klines_df,
            strategy_fn=strategy_callback,
            param_ranges=[
                ParamRange.range_int("fast_period", 5, 20, 5),
                ParamRange.range_int("slow_period", 20, 60, 10),
            ],
            indicator_fn=indicator_callback,
        )
        print(report.summary())
    """

    def __init__(self, config: Optional[OptimizationConfig] = None):
        self._config = config or OptimizationConfig()

    @property
    def config(self) -> OptimizationConfig:
        return self._config

    def optimize(
        self,
        data: pl.DataFrame,
        strategy_fn: Callable[[pl.DataFrame, int, Dict[str, Any]], Optional[str]],
        param_ranges: List[ParamRange],
        indicator_fn: Optional[Callable[[pl.DataFrame], pl.DataFrame]] = None,
        backtest_config: Optional[BacktestConfig] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> OptimizationReport:
        """执行参数优化

        Args:
            data: K 线 DataFrame
            strategy_fn: 策略函数，签名为 (df, bar_idx, params) -> signal
            param_ranges: 参数范围列表
            indicator_fn: 指标计算函数
            backtest_config: 回测配置
            progress_callback: 进度回调 (completed, total)

        Returns:
            OptimizationReport 优化报告
        """
        start_time = int(time.time() * 1000)

        # 生成参数组合
        combinations = self._generate_combinations(param_ranges)
        total = len(combinations)
        logger.info(f"Optimization started: {self._config.method} search, {total} combinations")

        # 样本内/外分割
        train_data = data
        test_data = None
        if self._config.enable_walk_forward:
            split_idx = int(len(data) * self._config.train_ratio)
            train_data = data.slice(0, split_idx)
            test_data = data.slice(split_idx)
            logger.info(f"Walk-forward: train={split_idx} bars, test={len(data) - split_idx} bars")

        # 执行优化
        results: List[OptimizationResult] = []
        failed = 0

        if self._config.max_workers > 1 and total > 1:
            results, failed = self._run_parallel(
                combinations, train_data, test_data,
                strategy_fn, indicator_fn, backtest_config,
                progress_callback, total,
            )
        else:
            results, failed = self._run_sequential(
                combinations, train_data, test_data,
                strategy_fn, indicator_fn, backtest_config,
                progress_callback, total,
            )

        # 排序
        results.sort(
            key=lambda r: r.score(self._config.sort_by),
            reverse=not self._config.ascending,
        )

        # 取 top N
        top_results = results[: self._config.top_n]

        end_time = int(time.time() * 1000)

        best = top_results[0] if top_results else None
        report = OptimizationReport(
            optimization_id=str(uuid.uuid4()),
            method=self._config.method,
            total_combinations=total,
            completed=len(results),
            failed=failed,
            results=top_results,
            best_params=best.params if best else {},
            best_score=best.score(self._config.sort_by) if best else 0.0,
            sort_metric=self._config.sort_by,
            duration_ms=end_time - start_time,
            created_at=end_time,
        )

        logger.info(f"Optimization completed: {len(results)}/{total} in {report.duration_ms}ms")
        return report

    def _generate_combinations(self, param_ranges: List[ParamRange]) -> List[Dict[str, Any]]:
        """生成参数组合"""
        if self._config.method == "grid":
            return self._grid_search(param_ranges)
        elif self._config.method == "random":
            return self._random_search(param_ranges)
        else:
            raise ValueError(f"Unknown method: {self._config.method}")

    def _grid_search(self, param_ranges: List[ParamRange]) -> List[Dict[str, Any]]:
        """网格搜索：穷举所有组合"""
        names = [p.name for p in param_ranges]
        value_lists = [p.values for p in param_ranges]

        combinations = []
        for values in itertools.product(*value_lists):
            combinations.append(dict(zip(names, values)))

        return combinations

    def _random_search(self, param_ranges: List[ParamRange]) -> List[Dict[str, Any]]:
        """随机搜索：随机采样 N 组"""
        # 计算总空间大小
        total_space = 1
        for p in param_ranges:
            total_space *= p.size

        n_samples = min(self._config.n_samples, total_space)

        # 如果采样数 >= 总空间，退化为网格搜索
        if n_samples >= total_space:
            return self._grid_search(param_ranges)

        # 随机采样（去重）
        seen = set()
        combinations = []
        max_attempts = n_samples * 10

        for _ in range(max_attempts):
            if len(combinations) >= n_samples:
                break
            params = {}
            for p in param_ranges:
                params[p.name] = random.choice(p.values)
            key = tuple(sorted(params.items()))
            if key not in seen:
                seen.add(key)
                combinations.append(params)

        return combinations

    def _run_sequential(
        self,
        combinations: List[Dict[str, Any]],
        train_data: pl.DataFrame,
        test_data: Optional[pl.DataFrame],
        strategy_fn: Callable,
        indicator_fn: Optional[Callable],
        backtest_config: Optional[BacktestConfig],
        progress_callback: Optional[Callable],
        total: int,
    ) -> Tuple[List[OptimizationResult], int]:
        """顺序执行"""
        results = []
        failed = 0

        for i, params in enumerate(combinations):
            try:
                result = self._run_single(
                    params, train_data, test_data,
                    strategy_fn, indicator_fn, backtest_config,
                )
                results.append(result)
            except Exception as e:
                failed += 1
                logger.debug(f"Optimization failed for {params}: {e}")

            if progress_callback:
                progress_callback(i + 1, total)

        return results, failed

    def _run_parallel(
        self,
        combinations: List[Dict[str, Any]],
        train_data: pl.DataFrame,
        test_data: Optional[pl.DataFrame],
        strategy_fn: Callable,
        indicator_fn: Optional[Callable],
        backtest_config: Optional[BacktestConfig],
        progress_callback: Optional[Callable],
        total: int,
    ) -> Tuple[List[OptimizationResult], int]:
        """并行执行（使用线程池，因策略函数可能不可 pickle）"""
        from concurrent.futures import ThreadPoolExecutor

        results = []
        failed = 0
        completed = 0

        with ThreadPoolExecutor(max_workers=self._config.max_workers) as executor:
            futures = {}
            for params in combinations:
                future = executor.submit(
                    self._run_single,
                    params, train_data, test_data,
                    strategy_fn, indicator_fn, backtest_config,
                )
                futures[future] = params

            for future in as_completed(futures):
                completed += 1
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    failed += 1
                    logger.debug(f"Parallel optimization failed: {e}")

                if progress_callback:
                    progress_callback(completed, total)

        return results, failed

    def _run_single(
        self,
        params: Dict[str, Any],
        train_data: pl.DataFrame,
        test_data: Optional[pl.DataFrame],
        strategy_fn: Callable,
        indicator_fn: Optional[Callable],
        backtest_config: Optional[BacktestConfig],
    ) -> OptimizationResult:
        """执行单次回测"""
        config = backtest_config or BacktestConfig()
        engine = BacktestEngine(config)

        # 包装策略函数，注入参数
        def wrapped_strategy(df: pl.DataFrame, bar_idx: int) -> Optional[str]:
            return strategy_fn(df, bar_idx, params)

        # 样本内回测
        bt_result = engine.run(train_data, wrapped_strategy, indicator_fn)
        report = bt_result.report

        result = OptimizationResult(
            params=params,
            total_return=report.total_return,
            annual_return=report.annual_return,
            sharpe_ratio=report.sharpe_ratio,
            max_drawdown=report.max_drawdown,
            win_rate=report.win_rate,
            profit_factor=report.profit_factor,
            total_trades=report.total_trades,
            duration_ms=bt_result.duration_ms,
        )

        # 样本外验证
        if test_data is not None and len(test_data) > 0:
            try:
                oos_engine = BacktestEngine(config)
                oos_result = oos_engine.run(test_data, wrapped_strategy, indicator_fn)
                result.oos_total_return = oos_result.report.total_return
                result.oos_sharpe_ratio = oos_result.report.sharpe_ratio
                result.oos_max_drawdown = oos_result.report.max_drawdown
            except Exception:
                pass

        return result

    # ─── 便捷方法 ───

    @staticmethod
    def suggest_param_ranges(
        strategy_name: str,
    ) -> List[ParamRange]:
        """根据策略名称推荐参数范围"""
        presets: Dict[str, List[ParamRange]] = {
            "dual_ma": [
                ParamRange.range_int("fast_period", 3, 20, 1),
                ParamRange.range_int("slow_period", 20, 100, 5),
            ],
            "rsi": [
                ParamRange.range_int("period", 7, 28, 7),
                ParamRange.range_int("oversold", 20, 40, 5),
                ParamRange.range_int("overbought", 60, 80, 5),
            ],
            "bollinger": [
                ParamRange.range_int("period", 10, 30, 5),
                ParamRange.linspace("std_dev", 1.5, 3.0, 4),
            ],
        }
        return presets.get(strategy_name, [])

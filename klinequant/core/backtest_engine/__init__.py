"""backtest_engine 包 — 回测引擎"""
from klinequant.core.backtest_engine.engine import BacktestConfig, BacktestEngine, BacktestResult
from klinequant.core.backtest_engine.fee import (
    FeeModel,
    FixedFee,
    PercentageFee,
    TieredFee,
    create_fee_model,
)
from klinequant.core.backtest_engine.matcher import BacktestFill, Matcher
from klinequant.core.backtest_engine.optimizer import (
    OptimizationConfig,
    OptimizationReport,
    OptimizationResult,
    ParameterOptimizer,
    ParamRange,
)
from klinequant.core.backtest_engine.performance import (
    PerformanceAnalyzer,
    PerformanceReport,
    Trade,
)
from klinequant.core.backtest_engine.slippage import (
    FixedSlippage,
    PercentageSlippage,
    SlippageModel,
    VolumeBasedSlippage,
    create_slippage_model,
)

__all__ = [
    "BacktestEngine",
    "BacktestConfig",
    "BacktestResult",
    "Matcher",
    "BacktestFill",
    "PerformanceAnalyzer",
    "PerformanceReport",
    "Trade",
    "SlippageModel",
    "FixedSlippage",
    "PercentageSlippage",
    "VolumeBasedSlippage",
    "create_slippage_model",
    "FeeModel",
    "FixedFee",
    "PercentageFee",
    "TieredFee",
    "create_fee_model",
    "ParameterOptimizer",
    "OptimizationConfig",
    "OptimizationReport",
    "OptimizationResult",
    "ParamRange",
]

"""策略指标接线（IND-106 消费链路）

消费策略 on_init 中的 require_indicators 声明：
    1. consume_requirements — 按计算契约 key=(指标名, 参数组合) 幂等注册到引擎
    2. warmup_from_df — 按声明的 (symbol, timeframe) 分组用历史 K 线预热
    3. inject_indicators — 把引擎有效序列按 timestamp 对齐注入 df 列，供 on_bar 消费

列命名规则（迭代计划 v2.1 决策⑥）：同名多实例用参数 slug 命名，
如 MACD base(2,5,3)×64 → macd_f128_g192_s320_dif（参数按键名排序，字段小写）。
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

import polars as pl

from core.indicator_engine.engine import IndicatorEngine
import core.indicator_engine.indicators  # noqa: F401 导入即注册全部内置指标
from core.strategy_engine.base import StrategyBase

# 参数名 → slug 缩写（避免 signal/slow 首字母冲突：g 取 siGnal）
_PARAM_ALIASES = {
    "fast_period": "f",
    "slow_period": "s",
    "signal_period": "g",
    "period": "p",
    "std_dev": "d",
    "k_period": "k",
    "d_period": "d",
    "j_period": "j",
}


def col_slug(name: str, params: Dict[str, Any]) -> str:
    """指标实例列前缀：指标名小写 + 参数缩写（按键名排序保证确定性）

    例：MACD{fast_period:2, slow_period:5, signal_period:3} → macd_f2_g3_s5
    """
    parts = []
    for key in sorted(params or {}):
        short = _PARAM_ALIASES.get(key, key[0])
        parts.append(f"{short}{params[key]}")
    return "_".join([name.lower()] + parts)


def field_col(name: str, params: Dict[str, Any], field: str) -> str:
    """指标字段列名：slug + 字段小写（如 macd_f2_g3_s5_dif）"""
    return f"{col_slug(name, params)}_{field.lower()}"


def consume_requirements(
    engine: IndicatorEngine,
    strategy: StrategyBase,
    exchange: str,
) -> List[Tuple[str, str]]:
    """按声明幂等注册指标实例，返回去重后的 (symbol, timeframe) 分组列表"""
    groups: List[Tuple[str, str]] = []
    for req in strategy.indicator_requirements:
        engine.ensure_indicator(
            req["indicator"], req["params"], req["symbol"], exchange, req["timeframe"]
        )
        pair = (req["symbol"], req["timeframe"])
        if pair not in groups:
            groups.append(pair)
    return groups


def warmup_from_df(
    engine: IndicatorEngine,
    strategy: StrategyBase,
    exchange: str,
    df: pl.DataFrame,
) -> Dict[str, Dict[str, Any]]:
    """用历史 K 线预热策略声明的全部指标（每个 (symbol, tf) 只预热一次）

    Returns:
        最后一次 warmup 的结果字典（键含指标名与 ind_key 双键）
    """
    results: Dict[str, Dict[str, Any]] = {}
    for symbol, timeframe in consume_requirements(engine, strategy, exchange):
        results.update(engine.warmup(symbol, exchange, timeframe, df))
    return results


def inject_indicators(
    engine: IndicatorEngine,
    df: pl.DataFrame,
    symbol: str,
    exchange: str,
    timeframe: str,
) -> pl.DataFrame:
    """把该品种/周期全部指标实例的有效序列按 timestamp 左连接注入 df

    预热未覆盖的早期 bar 列为 null（各实例 min_periods 不同，列起点各异）。
    """
    out = df
    for indicator in engine.indicators_for(symbol, exchange, timeframe):
        series = engine.get_series(
            indicator.name, indicator.params, symbol, exchange, timeframe
        )
        if not series:
            continue
        fields = list(series[0]["values"].keys())
        ind_df = pl.DataFrame({
            "timestamp": [item["timestamp"] for item in series],
            **{
                field_col(indicator.name, indicator.params, f): [
                    item["values"][f] for item in series
                ]
                for f in fields
            },
        })
        out = out.join(ind_df, on="timestamp", how="left")
    return out

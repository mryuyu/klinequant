"""策略配置层单测（config/strategy_config.py）

覆盖：yaml 加载 / 默认值 / 校验（重名、缺主周期、空 symbols、空 name）/
symbols 大写归一 / periods 回落 / enabled_strategies / get / 错误包装。
"""
from __future__ import annotations

import textwrap

import pytest

from config.strategy_config import (
    BacktestConfig,
    DefaultsConfig,
    RiskConfig,
    StrategiesConfig,
    StrategyConfigError,
    StrategyInstanceConfig,
    load_strategies_config,
)


def _write(tmp_path, body: str):
    p = tmp_path / "strategies.yaml"
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return str(p)


# ─── 默认值 ───


def test_defaults_are_sane():
    """各子模型缺省值稳定（poll/bar_count/stale_guard/capital 等）。"""
    d = DefaultsConfig()
    assert d.poll_interval == 0.5
    assert d.bar_count == 1000
    assert d.stale_guard == 120.0
    assert d.duration == 0.0
    r = RiskConfig()
    assert r.rules == {}
    assert r.max_daily_loss_pct == 0.0
    assert r.max_total_positions == 0
    bt = BacktestConfig()
    assert bt.initial_capital == 10000.0
    assert bt.history_bars == 3000
    assert bt.account_currency == "USD"
    assert bt.slippage.model == "percentage"
    assert bt.fee.model == "fixed"


def test_empty_yaml_yields_empty_config(tmp_path):
    """空文件 → 全默认、strategies 空列表（不报错）。"""
    cfg = load_strategies_config(_write(tmp_path, ""))
    assert cfg.strategies == []
    assert cfg.enabled_strategies() == []
    assert cfg.defaults.bar_count == 1000


# ─── 加载真实配置 ───


def test_load_repo_strategies_yaml():
    """仓库自带 config/strategies.yaml 可加载，dualma-eur 实例参数正确。"""
    cfg = load_strategies_config("config/strategies.yaml")
    s = cfg.get("dualma-eur")
    assert s is not None
    assert s.module == "strategies.sdk_dual_ma"
    assert s.symbols == ["EURUSD"]
    assert s.period == "1m"
    assert s.periods == ["1m", "15m"]
    assert s.params["fast"] == 7
    assert s.params["slow"] == 25
    assert s.params["trend_period"] == "15m"
    assert s.enabled is True
    assert cfg.risk.max_positions_per_strategy == 3
    assert cfg.backtest.initial_capital == 10000


# ─── 归一与回落 ───


def test_symbols_uppercased_and_periods_default(tmp_path):
    """symbols 统一大写；periods 缺省回落为 [period]。"""
    path = _write(tmp_path, """
        strategies:
          - name: s1
            module: m.s1
            symbols: [eurusd, gbpusd]
            period: 5m
    """)
    cfg = load_strategies_config(path)
    s = cfg.get("s1")
    assert s.symbols == ["EURUSD", "GBPUSD"]
    assert s.periods == ["5m"]


def test_enabled_strategies_filters_disabled(tmp_path):
    """enabled=false 的实例被 enabled_strategies 过滤，但 get 仍可取。"""
    path = _write(tmp_path, """
        strategies:
          - {name: a, module: m.a, symbols: [EURUSD], enabled: true}
          - {name: b, module: m.b, symbols: [GBPUSD], enabled: false}
    """)
    cfg = load_strategies_config(path)
    assert [s.name for s in cfg.enabled_strategies()] == ["a"]
    assert cfg.get("b") is not None
    assert cfg.get("zzz") is None


# ─── 校验失败 → StrategyConfigError ───


def test_duplicate_names_rejected(tmp_path):
    """策略 name 重复 → 校验失败并包装为 StrategyConfigError。"""
    path = _write(tmp_path, """
        strategies:
          - {name: dup, module: m.a, symbols: [EURUSD]}
          - {name: dup, module: m.b, symbols: [GBPUSD]}
    """)
    with pytest.raises(StrategyConfigError):
        load_strategies_config(path)


def test_main_period_must_be_in_periods(tmp_path):
    """主周期 period 不在 periods 中 → 校验失败。"""
    path = _write(tmp_path, """
        strategies:
          - {name: s, module: m.s, symbols: [EURUSD], period: 1m, periods: [15m, 1h]}
    """)
    with pytest.raises(StrategyConfigError):
        load_strategies_config(path)


def test_empty_symbols_rejected(tmp_path):
    """symbols 为空 → 校验失败。"""
    path = _write(tmp_path, """
        strategies:
          - {name: s, module: m.s, symbols: []}
    """)
    with pytest.raises(StrategyConfigError):
        load_strategies_config(path)


def test_empty_name_rejected():
    """name 为空白 → 直接构造即校验失败（pydantic ValidationError）。"""
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        StrategyInstanceConfig(name="  ", module="m.s", symbols=["EURUSD"])


def test_missing_file_raises():
    """文件不存在 → StrategyConfigError。"""
    with pytest.raises(StrategyConfigError):
        load_strategies_config("config/__definitely_not_exists__.yaml")


def test_non_mapping_top_level_raises(tmp_path):
    """顶层非映射（如列表）→ StrategyConfigError。"""
    p = tmp_path / "bad.yaml"
    p.write_text("- just\n- a\n- list\n", encoding="utf-8")
    with pytest.raises(StrategyConfigError):
        load_strategies_config(str(p))


def test_broken_yaml_raises(tmp_path):
    """YAML 语法错误 → StrategyConfigError（不泄漏原始 YAMLError）。"""
    p = tmp_path / "broken.yaml"
    p.write_text("strategies: [ {name: a, module: m.a, symbols: [X] \n", encoding="utf-8")
    with pytest.raises(StrategyConfigError):
        load_strategies_config(str(p))


# ─── 程序化构造（供其他测试复用的形状）───


def test_programmatic_construction():
    """直接构造 StrategiesConfig（回测/多策略测试常用路径）。"""
    cfg = StrategiesConfig(
        strategies=[
            StrategyInstanceConfig(name="p1", module="m.p1", symbols=["AAAUSD"],
                                   period="1m", periods=["1m"], params={"k": 1}),
        ],
        backtest=BacktestConfig(initial_capital=5000),
    )
    assert cfg.get("p1").params == {"k": 1}
    assert cfg.backtest.initial_capital == 5000
    assert cfg.risk.max_total_positions == 0

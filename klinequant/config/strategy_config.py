"""策略配置层（YAML + Pydantic v2）

一份 ``config/strategies.yaml`` 描述多策略运行的全部非凭证配置：
标的列表、周期、风控参数、策略参数、回测参数——不写死在代码里。

设计约束：
  - 账户凭证不在此层：仍走 ``config/accounts.yaml`` + ``resolve_account``
    （一进程一账户，``${ENV_VAR}`` 插值），本配置只引用账户名之外的运行参数；
  - ``initial_capital`` 仅回测用；实盘账户资金以 venue 为准；
  - ``params`` 为自由 dict（策略自定义参数，框架只透传给 StrategyContext）；
  - risk.rules 按 ``create_default_rules`` 参数形状配置；**未在 yaml 出现的
    既有规则装配时默认禁用**（避免 FX 语义不适用规则按默认参数误拒）。

加载入口：``load_strategies_config(path)`` → ``StrategiesConfig``。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from klinequant import PACKAGE_DIR

__all__ = [
    "DefaultsConfig",
    "RiskConfig",
    "SlippageConfig",
    "FeeConfig",
    "BacktestConfig",
    "StrategyInstanceConfig",
    "StrategiesConfig",
    "StrategyConfigError",
    "load_strategies_config",
]


class StrategyConfigError(ValueError):
    """策略配置错误（文件损坏 / 校验失败）。"""


class DefaultsConfig(BaseModel):
    """运行默认值（实盘 LiveRunner / 统一入口脚本消费）。"""

    poll_interval: float = 0.5
    bar_count: int = 1000          # MR_Y_CYCLE min_periods=768，默认需 >=800
    stale_guard: float = 120.0     # R5 断线闸门阈值秒（0=禁用）
    duration: float = 0.0          # 运行时长秒（0=不限时）
    magic: int = 202609            # MT5 执行器实例级回落 magic
    deviation: int = 20            # MT5 滑点容忍 points


class RiskConfig(BaseModel):
    """全局风控参数（策略侧 api.risk_check 消费，不做 send_order 强制拦截）。

    ``rules``：RiskEngine 既有 12 条规则的启停与参数，按 ``create_default_rules``
    参数形状（``{rule_name: {enabled: bool, ...rule_params}}``）；未列出的规则
    装配时默认禁用。``max_daily_loss_pct`` / ``max_total_positions`` /
    ``max_positions_per_strategy`` 为 SDK 轨新增规则参数（risk_rules.py）。
    """

    rules: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    max_daily_loss_pct: float = 0.0        # 日亏损百分比限额（0=禁用）
    max_total_positions: int = 0           # 全局持仓笔数上限（0=禁用）
    max_positions_per_strategy: int = 0    # 单策略持仓笔数上限（0=禁用）


class SlippageConfig(BaseModel):
    model: str = "percentage"
    params: Dict[str, Any] = Field(default_factory=lambda: {"pct": 0.0001})


class FeeConfig(BaseModel):
    model: str = "fixed"
    params: Dict[str, Any] = Field(default_factory=lambda: {"fee_per_trade": 0})


class BacktestConfig(BaseModel):
    """回测参数（仅 --mode backtest 消费）。"""

    initial_capital: float = 10000.0
    history_bars: int = 3000
    account_currency: str = "USD"
    slippage: SlippageConfig = Field(default_factory=SlippageConfig)
    fee: FeeConfig = Field(default_factory=FeeConfig)


class StrategyInstanceConfig(BaseModel):
    """单个策略实例（同一 module 可配多条不同 name/params 的实例）。"""

    name: str                                   # 自定义策略名（≠文件名），全局唯一
    module: str                                 # importlib 导入路径，如 strategies.sdk_dual_ma
    enabled: bool = True
    symbols: List[str] = Field(default_factory=list)
    period: str = "1m"                          # 主/驱动周期
    periods: List[str] = Field(default_factory=list)   # 订阅周期（空=[period]）
    params: Dict[str, Any] = Field(default_factory=dict)
    max_consecutive_errors: int = 20            # 迭代级容错阈值（resilient 包装用）

    @field_validator("symbols", mode="before")
    @classmethod
    def _upper_symbols(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v.upper()]
        if isinstance(v, list):
            return [str(s).upper() for s in v]
        return v

    @model_validator(mode="after")
    def _check(self) -> "StrategyInstanceConfig":
        if not self.name.strip():
            raise ValueError("策略 name 不能为空")
        if not self.symbols:
            raise ValueError(f"策略 {self.name} 的 symbols 不能为空")
        if not self.periods:
            self.periods = [self.period]
        if self.period not in self.periods:
            raise ValueError(
                f"策略 {self.name} 的主周期 {self.period} 必须包含在 periods {self.periods} 中"
            )
        return self


class StrategiesConfig(BaseModel):
    """strategies.yaml 顶层模型。"""

    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    backtest: BacktestConfig = Field(default_factory=BacktestConfig)
    strategies: List[StrategyInstanceConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_unique_names(self) -> "StrategiesConfig":
        names = [s.name for s in self.strategies]
        dup = {n for n in names if names.count(n) > 1}
        if dup:
            raise ValueError(f"策略 name 重复：{sorted(dup)}（name 必须全局唯一）")
        return self

    def enabled_strategies(self) -> List[StrategyInstanceConfig]:
        return [s for s in self.strategies if s.enabled]

    def get(self, name: str) -> Optional[StrategyInstanceConfig]:
        for s in self.strategies:
            if s.name == name:
                return s
        return None


def load_strategies_config(path: str | Path) -> StrategiesConfig:
    """加载并校验 strategies.yaml（Pydantic 校验失败统一包装为 StrategyConfigError）。

    相对路径锚定 PACKAGE_DIR（随包分发的 config/ 目录），与运行 CWD 解耦；
    传入绝对路径则原样使用。
    """
    p = Path(path)
    if not p.is_absolute():
        p = PACKAGE_DIR / p
    if not p.exists():
        raise StrategyConfigError(f"策略配置文件不存在: {p}")
    try:
        with open(p, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise StrategyConfigError(f"策略配置 YAML 解析失败: {e}") from e
    if not isinstance(raw, dict):
        raise StrategyConfigError("strategies.yaml 顶层必须是映射")
    try:
        return StrategiesConfig.model_validate(raw)
    except Exception as e:  # pydantic ValidationError 等
        raise StrategyConfigError(f"策略配置校验失败: {e}") from e

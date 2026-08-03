"""StrategyContext — 策略运行上下文

为策略提供：
    - 参数管理（从配置加载）
    - 日志（策略独立日志）
    - 状态存储（KV 持久化）
    - 运行信息（策略 ID、名称、周期等）

遵循需求文档 §4.6 STR-002。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class StrategyInfo:
    """策略基本信息"""

    strategy_id: str
    name: str
    version: str = "1.0.0"
    author: str = ""
    description: str = ""
    symbols: list = field(default_factory=list)
    timeframes: list = field(default_factory=list)
    parameters: Dict[str, Any] = field(default_factory=dict)


class StrategyContext:
    """策略运行上下文

    每个策略实例拥有独立的 Context，包含：
        - info: 策略基本信息
        - params: 策略参数（可热更新）
        - logger: 策略独立日志器
        - state: KV 状态存储
    """

    def __init__(self, info: StrategyInfo, log_dir: Optional[str] = None):
        self._info = info
        self._params: Dict[str, Any] = dict(info.parameters)
        self._state: Dict[str, Any] = {}
        self._is_running: bool = False

        # 策略独立日志
        self._logger = logging.getLogger(f"strategy.{info.strategy_id}")
        if log_dir:
            handler = logging.FileHandler(
                f"{log_dir}/{info.strategy_id}.log", encoding="utf-8"
            )
            handler.setFormatter(
                logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
            )
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.DEBUG)

    @property
    def info(self) -> StrategyInfo:
        return self._info

    @property
    def strategy_id(self) -> str:
        return self._info.strategy_id

    @property
    def params(self) -> Dict[str, Any]:
        return dict(self._params)

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def is_running(self) -> bool:
        return self._is_running

    def get_param(self, key: str, default: Any = None) -> Any:
        """获取策略参数"""
        return self._params.get(key, default)

    def set_param(self, key: str, value: Any) -> None:
        """设置策略参数（热更新）"""
        self._params[key] = value
        self._logger.info(f"Param updated: {key} = {value}")

    def update_params(self, params: Dict[str, Any]) -> None:
        """批量更新参数"""
        self._params.update(params)
        self._logger.info(f"Params updated: {list(params.keys())}")

    # ─── 状态存储 ───

    def get_state(self, key: str, default: Any = None) -> Any:
        """获取状态值"""
        return self._state.get(key, default)

    def set_state(self, key: str, value: Any) -> None:
        """设置状态值"""
        self._state[key] = value

    def get_all_state(self) -> Dict[str, Any]:
        """获取全部状态"""
        return dict(self._state)

    def load_state(self, state: Dict[str, Any]) -> None:
        """加载状态（从持久化恢复）"""
        self._state.update(state)

    # ─── 生命周期 ───

    def mark_running(self) -> None:
        self._is_running = True

    def mark_stopped(self) -> None:
        self._is_running = False

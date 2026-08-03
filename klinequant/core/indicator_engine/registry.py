"""IndicatorRegistry — 指标注册表

管理所有指标实例的注册、查询、工厂创建：
    - register(cls)：注册指标类
    - create(name, params)：工厂方法创建指标实例
    - list_indicators()：列出所有已注册指标

遵循需求文档 §4.2 IND-009。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Type

from core.indicator_engine.base import IndicatorBase


class IndicatorRegistry:
    """指标注册表（全局单例模式）"""

    def __init__(self):
        self._registry: Dict[str, Type[IndicatorBase]] = {}

    def register(self, indicator_cls: Type[IndicatorBase]) -> Type[IndicatorBase]:
        """注册指标类（可用作装饰器）

        Args:
            indicator_cls: IndicatorBase 子类

        Returns:
            原类（支持装饰器用法）
        """
        # 实例化临时获取 name
        instance = indicator_cls.__new__(indicator_cls)
        indicator_name = instance.name

        if indicator_name in self._registry:
            raise ValueError(f"Indicator '{indicator_name}' already registered")

        self._registry[indicator_name] = indicator_cls
        return indicator_cls

    def create(
        self, name: str, params: Optional[Dict[str, Any]] = None
    ) -> IndicatorBase:
        """工厂方法创建指标实例

        Args:
            name: 指标名称
            params: 指标参数

        Returns:
            IndicatorBase 实例
        """
        if name not in self._registry:
            available = ", ".join(sorted(self._registry.keys()))
            raise KeyError(
                f"Unknown indicator '{name}'. Available: {available}"
            )
        return self._registry[name](params=params)

    def list_indicators(self) -> List[str]:
        """列出所有已注册指标名称"""
        return sorted(self._registry.keys())

    def has(self, name: str) -> bool:
        return name in self._registry

    def __contains__(self, name: str) -> bool:
        return self.has(name)

    def __len__(self) -> int:
        return len(self._registry)


# 全局注册表实例
_global_registry = IndicatorRegistry()


def get_registry() -> IndicatorRegistry:
    """获取全局指标注册表"""
    return _global_registry


def register_indicator(cls: Type[IndicatorBase]) -> Type[IndicatorBase]:
    """装饰器：注册指标类到全局注册表

    用法：
        @register_indicator
        class MyIndicator(IndicatorBase):
            ...
    """
    return _global_registry.register(cls)

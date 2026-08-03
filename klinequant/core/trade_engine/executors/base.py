"""Executor — 订单执行器抽象基类

统一接口：
    - submit_order: 提交订单
    - cancel_order: 撤销订单
    - query_order: 查询订单状态
    - query_positions: 查询持仓
    - query_account: 查询账户

遵循需求文档 §4.4 TRD-001~TRD-003, TRD-009, TRD-012。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any, Dict, List, Optional

from protocol.types import Account, Order, Position


class Executor(ABC):
    """订单执行器抽象基类"""

    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        self._name = name
        self._config = config or {}

    @property
    def name(self) -> str:
        return self._name

    @abstractmethod
    async def submit_order(self, order: Order) -> Order:
        """提交订单到交易所

        Args:
            order: 待提交的订单

        Returns:
            更新后的订单（含 exchange_order_id、新状态）
        """
        ...

    @abstractmethod
    async def cancel_order(self, order: Order) -> Order:
        """撤销订单

        Args:
            order: 待撤销的订单

        Returns:
            更新后的订单
        """
        ...

    @abstractmethod
    async def query_order(self, order: Order) -> Order:
        """查询订单最新状态

        Returns:
            更新后的订单
        """
        ...

    @abstractmethod
    async def query_positions(self, symbols: Optional[List[str]] = None) -> Dict[str, Position]:
        """查询持仓"""
        ...

    @abstractmethod
    async def query_account(self) -> Account:
        """查询账户信息"""
        ...

    async def connect(self) -> None:
        """连接（可选）"""
        pass

    async def disconnect(self) -> None:
        """断开连接（可选）"""
        pass

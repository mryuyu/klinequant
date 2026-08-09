"""StrategyManager — 策略生命周期管理

管理策略的完整生命周期：
    加载 → 初始化 → 运行 → 停止 → 卸载

支持：
    - 策略注册（类注册/文件加载）
    - 多策略并行管理
    - 策略状态监控
    - 策略参数热更新

遵循需求文档 §4.6 STR-004。
"""
from __future__ import annotations

import logging
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Type

from core.strategy_engine.base import StrategyBase
from core.strategy_engine.clients import MarketClient, TradeClient
from core.strategy_engine.context import StrategyContext, StrategyInfo

logger = logging.getLogger(__name__)


class StrategyStatus(str, Enum):
    LOADED = "LOADED"
    INITIALIZED = "INITIALIZED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    ERROR = "ERROR"


class ManagedStrategy:
    """被管理的策略实例"""

    def __init__(
        self,
        strategy: StrategyBase,
        context: StrategyContext,
    ):
        self.strategy = strategy
        self.context = context
        self.status = StrategyStatus.LOADED
        self.started_at: Optional[int] = None
        self.stopped_at: Optional[int] = None
        self.error: Optional[str] = None

    @property
    def strategy_id(self) -> str:
        return self.context.strategy_id


class StrategyManager:
    """策略生命周期管理器"""

    def __init__(self):
        # strategy_id -> ManagedStrategy
        self._strategies: Dict[str, ManagedStrategy] = {}
        # 策略类注册表: name -> class
        self._registry: Dict[str, Type[StrategyBase]] = {}

    @property
    def strategies(self) -> Dict[str, ManagedStrategy]:
        return dict(self._strategies)

    @property
    def running_count(self) -> int:
        return sum(1 for s in self._strategies.values() if s.status == StrategyStatus.RUNNING)

    # ─── 注册 ───

    def register_strategy(self, name: str, cls: Type[StrategyBase]) -> None:
        """注册策略类"""
        self._registry[name] = cls
        logger.info(f"Strategy class registered: {name}")

    def get_registered(self) -> List[str]:
        """获取已注册的策略名称列表"""
        return list(self._registry.keys())

    # ─── 生命周期 ───

    def load_strategy(
        self,
        strategy_id: str,
        cls: Type[StrategyBase],
        info: StrategyInfo,
        trade_client: Optional[TradeClient] = None,
        market_client: Optional[MarketClient] = None,
    ) -> ManagedStrategy:
        """加载策略实例

        Args:
            strategy_id: 策略唯一 ID
            cls: 策略类
            info: 策略信息
            trade_client: 交易客户端
            market_client: 行情客户端

        Returns:
            ManagedStrategy 实例
        """
        if strategy_id in self._strategies:
            raise ValueError(f"Strategy {strategy_id} already loaded")

        ctx = StrategyContext(info)
        tc = trade_client or TradeClient(strategy_id)
        mc = market_client or MarketClient(strategy_id)

        strategy = cls(context=ctx, trade_client=tc, market_client=mc)
        managed = ManagedStrategy(strategy=strategy, context=ctx)
        self._strategies[strategy_id] = managed

        logger.info(f"Strategy loaded: {strategy_id} ({info.name})")
        return managed

    def init_strategy(self, strategy_id: str) -> None:
        """初始化策略"""
        managed = self._get(strategy_id)
        try:
            managed.strategy.on_init()
            managed.status = StrategyStatus.INITIALIZED
            # IND-106：on_init 中的 require_indicators 声明在此收口记录
            # （引擎侧统一预热接线在策略启动链路上消费该声明）
            reqs = managed.strategy.indicator_requirements
            if reqs:
                logger.info(
                    f"Strategy {strategy_id} indicator requirements: {reqs}"
                )
            logger.info(f"Strategy initialized: {strategy_id}")
        except Exception as e:
            managed.status = StrategyStatus.ERROR
            managed.error = str(e)
            logger.error(f"Strategy init failed: {strategy_id}: {e}")
            raise

    def start_strategy(self, strategy_id: str) -> None:
        """启动策略"""
        managed = self._get(strategy_id)
        if managed.status not in (StrategyStatus.INITIALIZED, StrategyStatus.PAUSED):
            raise RuntimeError(
                f"Cannot start strategy in {managed.status} state"
            )
        managed.status = StrategyStatus.RUNNING
        managed.context.mark_running()
        managed.started_at = int(time.time() * 1000)
        logger.info(f"Strategy started: {strategy_id}")

    def stop_strategy(self, strategy_id: str) -> None:
        """停止策略"""
        managed = self._get(strategy_id)
        try:
            managed.strategy.on_stop()
        except Exception as e:
            logger.error(f"Strategy on_stop error: {strategy_id}: {e}")

        managed.status = StrategyStatus.STOPPED
        managed.context.mark_stopped()
        managed.stopped_at = int(time.time() * 1000)
        logger.info(f"Strategy stopped: {strategy_id}")

    def pause_strategy(self, strategy_id: str) -> None:
        """暂停策略"""
        managed = self._get(strategy_id)
        if managed.status != StrategyStatus.RUNNING:
            raise RuntimeError("Can only pause a running strategy")
        managed.status = StrategyStatus.PAUSED
        managed.context.mark_stopped()
        logger.info(f"Strategy paused: {strategy_id}")

    def resume_strategy(self, strategy_id: str) -> None:
        """恢复策略"""
        managed = self._get(strategy_id)
        if managed.status != StrategyStatus.PAUSED:
            raise RuntimeError("Can only resume a paused strategy")
        managed.status = StrategyStatus.RUNNING
        managed.context.mark_running()
        logger.info(f"Strategy resumed: {strategy_id}")

    def unload_strategy(self, strategy_id: str) -> None:
        """卸载策略"""
        managed = self._get(strategy_id)
        if managed.status == StrategyStatus.RUNNING:
            self.stop_strategy(strategy_id)
        del self._strategies[strategy_id]
        logger.info(f"Strategy unloaded: {strategy_id}")

    # ─── 参数热更新 ───

    def update_params(self, strategy_id: str, params: Dict[str, Any]) -> None:
        """热更新策略参数"""
        managed = self._get(strategy_id)
        managed.context.update_params(params)
        logger.info(f"Strategy params updated: {strategy_id}")

    # ─── 查询 ───

    def get_status(self, strategy_id: str) -> StrategyStatus:
        return self._get(strategy_id).status

    def get_all_status(self) -> Dict[str, StrategyStatus]:
        return {sid: s.status for sid, s in self._strategies.items()}

    def _get(self, strategy_id: str) -> ManagedStrategy:
        managed = self._strategies.get(strategy_id)
        if managed is None:
            raise KeyError(f"Strategy not found: {strategy_id}")
        return managed

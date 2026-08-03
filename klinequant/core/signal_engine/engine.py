"""SignalEngine — 信号引擎主循环

整合所有组件：
    - RuleBase（信号规则）
    - 信号冷却期（去重）
    - 信号路由（自动/半自动/告警）
    - 标准化 Signal 生成
    - 订阅 IndicatorEngine 的指标值更新

遵循需求文档 §4.3 SIG-001~SIG-005。
"""
from __future__ import annotations

import logging
import time
import uuid
from collections import defaultdict
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from protocol.types import (
    IndicatorValue,
    Signal,
    SignalDirection,
    SignalStrength,
)
from core.signal_engine.rules.base import RuleBase, RuleResult

logger = logging.getLogger(__name__)


class SignalRoute(str, Enum):
    """信号路由模式"""
    AUTO = "AUTO"          # 自动下单
    SEMI_AUTO = "SEMI"     # 半自动（需确认）
    ALERT_ONLY = "ALERT"   # 仅告警


class SignalEngine:
    """信号引擎

    功能：
        1. 管理信号规则（按 indicator_name + symbol 分组）
        2. 接收指标值更新，评估规则
        3. 信号冷却期（同一规则 + 同方向在冷却期内不重复触发）
        4. 信号路由（AUTO / SEMI_AUTO / ALERT_ONLY）
        5. 生成标准化 Signal 对象
    """

    def __init__(
        self,
        cooldown_seconds: int = 300,
        default_route: SignalRoute = SignalRoute.ALERT_ONLY,
    ):
        """
        Args:
            cooldown_seconds: 信号冷却期（秒），同一规则同方向在此时间内不重复触发
            default_route: 默认信号路由模式
        """
        self._cooldown_seconds = cooldown_seconds
        self._default_route = default_route

        # 规则配置: (indicator_name, symbol) -> List[RuleConfig]
        self._rule_configs: Dict[Tuple[str, str], List[_RuleConfig]] = defaultdict(list)

        # 指标值历史: (indicator_name, symbol) -> (current_values, previous_values)
        self._indicator_history: Dict[Tuple[str, str], Tuple[Optional[Dict], Optional[Dict]]] = {}

        # 冷却记录: cooldown_key -> last_trigger_timestamp
        self._cooldown_map: Dict[str, int] = {}

        # 信号订阅者
        self._signal_subscribers: List[Callable[[Signal], None]] = []

        # 路由配置: rule_name -> SignalRoute
        self._route_overrides: Dict[str, SignalRoute] = {}

        # 策略 ID（用于生成 Signal）
        self._strategy_id: str = "default"

        self._running = False
        self._signal_count = 0

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def signal_count(self) -> int:
        return self._signal_count

    @property
    def cooldown_seconds(self) -> int:
        return self._cooldown_seconds

    def set_strategy_id(self, strategy_id: str) -> None:
        self._strategy_id = strategy_id

    def add_rule(
        self,
        rule: RuleBase,
        indicator_name: str,
        symbol: str,
        route: Optional[SignalRoute] = None,
    ) -> None:
        """添加信号规则

        Args:
            rule: 规则实例
            indicator_name: 关联的指标名称
            symbol: 关联的交易对
            route: 信号路由模式（None 使用默认）
        """
        config = _RuleConfig(
            rule=rule,
            indicator_name=indicator_name,
            symbol=symbol,
            route=route or self._default_route,
        )
        key = (indicator_name, symbol)
        self._rule_configs[key].append(config)

        if route:
            self._route_overrides[rule.name] = route

        logger.info(f"Added rule: {rule.name} for {indicator_name}/{symbol}")

    def subscribe_signals(self, callback: Callable[[Signal], None]) -> None:
        """订阅信号输出"""
        self._signal_subscribers.append(callback)

    def on_indicator_update(self, value: IndicatorValue) -> Optional[Signal]:
        """处理指标值更新（回调函数）

        Args:
            value: 指标值更新

        Returns:
            触发的 Signal（如有）
        """
        key = (value.indicator_name, value.symbol)
        configs = self._rule_configs.get(key, [])

        if not configs:
            return None

        # 更新指标值历史
        current_vals = value.values
        prev_current, _ = self._indicator_history.get(key, (None, None))
        self._indicator_history[key] = (current_vals, prev_current)

        _, previous_vals = self._indicator_history[key]

        # 评估所有关联规则
        for config in configs:
            result = config.rule.evaluate(current_vals, previous_vals)
            if result is None:
                continue

            # 冷却期检查
            cooldown_key = f"{config.rule.name}:{config.symbol}:{result.direction.value}"
            if self._is_in_cooldown(cooldown_key, value.timestamp):
                logger.debug(f"Signal in cooldown: {cooldown_key}")
                continue

            # 生成信号
            signal = self._create_signal(
                rule_result=result,
                symbol=config.symbol,
                timestamp=value.timestamp,
                price=float(value.values.get("close", 0)),
                indicator_values=value.values,
                route=config.route,
            )

            # 记录冷却
            self._cooldown_map[cooldown_key] = value.timestamp

            self._signal_count += 1
            logger.info(
                f"Signal generated: {signal.direction.value} {signal.symbol} "
                f"via {result.rule_name} (strength={signal.strength})"
            )

            # 通知订阅者
            self._notify_subscribers(signal)

            return signal

        return None

    def check_cooldown(self, rule_name: str, symbol: str, direction: SignalDirection, timestamp: int) -> bool:
        """检查指定规则是否在冷却期"""
        cooldown_key = f"{rule_name}:{symbol}:{direction.value}"
        return self._is_in_cooldown(cooldown_key, timestamp)

    def clear_cooldowns(self) -> None:
        """清空所有冷却记录"""
        self._cooldown_map.clear()

    def get_route(self, rule_name: str) -> SignalRoute:
        """获取规则的路由模式"""
        return self._route_overrides.get(rule_name, self._default_route)

    def start(self) -> None:
        self._running = True
        logger.info("SignalEngine started")

    def stop(self) -> None:
        self._running = False
        logger.info("SignalEngine stopped")

    # ─── 内部方法 ───

    def _is_in_cooldown(self, cooldown_key: str, current_timestamp: int) -> bool:
        """检查是否在冷却期"""
        last_trigger = self._cooldown_map.get(cooldown_key)
        if last_trigger is None:
            return False
        elapsed_ms = current_timestamp - last_trigger
        return elapsed_ms < self._cooldown_seconds * 1000

    def _create_signal(
        self,
        rule_result: RuleResult,
        symbol: str,
        timestamp: int,
        price: float,
        indicator_values: Dict[str, Any],
        route: SignalRoute,
    ) -> Signal:
        """创建标准化 Signal"""
        from decimal import Decimal

        # 强度映射
        if rule_result.strength >= 0.8:
            strength = SignalStrength.STRONG
        elif rule_result.strength >= 0.5:
            strength = SignalStrength.MEDIUM
        else:
            strength = SignalStrength.WEAK

        return Signal(
            signal_id=str(uuid.uuid4()),
            strategy_id=self._strategy_id,
            symbol=symbol,
            direction=rule_result.direction,
            strength=strength,
            price=Decimal(str(price)),
            reason=rule_result.reason,
            timestamp=timestamp,
            indicators=indicator_values,
            expires_at=timestamp + self._cooldown_seconds * 1000,
            status="PENDING" if route == SignalRoute.SEMI_AUTO else "CONFIRMED",
        )

    def _notify_subscribers(self, signal: Signal) -> None:
        """通知信号订阅者"""
        for cb in self._signal_subscribers:
            try:
                cb(signal)
            except Exception as e:
                logger.error(f"Signal subscriber error: {e}")


class _RuleConfig:
    """规则配置（内部类）"""

    def __init__(
        self,
        rule: RuleBase,
        indicator_name: str,
        symbol: str,
        route: SignalRoute,
    ):
        self.rule = rule
        self.indicator_name = indicator_name
        self.symbol = symbol
        self.route = route

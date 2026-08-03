"""AlertManager — 告警管理器

统一管理告警规则、告警事件和通知分发：
    - 告警规则：定义触发条件和通知渠道
    - 告警事件：记录所有触发的告警
    - 通知分发：根据规则路由到对应渠道
    - 告警聚合：相同告警在冷却期内不重复发送
    - 告警升级：未处理的告警自动升级

遵循需求文档 §8.1 告警级别定义。
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Callable, Dict, List, Optional, Set

from core.notification.channels import Message, NotificationChannel

logger = logging.getLogger(__name__)


class AlertLevel(IntEnum):
    """告警级别（与需求文档 §8.1 一致）"""
    INFO = 1       # 提示：仅记录日志
    WARNING = 2    # 警告：降级运行，记录日志，告警
    CRITICAL = 3   # 严重：相关模块停止，告警通知
    FATAL = 4      # 致命：系统停止，需人工介入

    @property
    def label(self) -> str:
        return self.name

    @classmethod
    def from_str(cls, s: str) -> "AlertLevel":
        return cls[s.upper()]


@dataclass
class AlertRule:
    """告警规则"""
    name: str
    level: AlertLevel
    description: str = ""
    enabled: bool = True
    # 冷却时间（秒），相同告警在此期间不重复发送
    cooldown_seconds: int = 300
    # 通知渠道名称列表（空 = 所有渠道）
    channels: List[str] = field(default_factory=list)
    # 触发条件（可选，用于自动规则）
    condition: Optional[Callable[[Dict[str, Any]], bool]] = None
    # 自动升级：连续触发 N 次后升级
    escalate_after: int = 0
    escalate_to: Optional[AlertLevel] = None


@dataclass
class AlertEvent:
    """告警事件"""
    alert_id: str
    rule_name: str
    level: AlertLevel
    title: str
    message: str
    source: str  # 来源模块
    timestamp: int
    extra: Dict[str, Any] = field(default_factory=dict)
    notified: bool = False
    acknowledged: bool = False


class AlertManager:
    """告警管理器

    使用方式：
        manager = AlertManager()
        manager.add_channel(DingTalkChannel(webhook_url="..."))
        manager.add_rule(AlertRule(
            name="ws_disconnect",
            level=AlertLevel.CRITICAL,
            description="WebSocket 断线",
        ))

        # 触发告警
        await manager.fire("ws_disconnect", "行情连接断开", source="market_engine")
    """

    def __init__(self, max_history: int = 1000):
        # 渠道
        self._channels: Dict[str, NotificationChannel] = {}
        # 规则
        self._rules: Dict[str, AlertRule] = {}
        # 事件历史
        self._events: List[AlertEvent] = []
        self._max_history = max_history
        # 冷却记录: rule_name -> last_fire_time
        self._cooldowns: Dict[str, float] = {}
        # 连续触发计数: rule_name -> count
        self._trigger_counts: Dict[str, int] = defaultdict(int)
        # 事件回调
        self._callbacks: List[Callable[[AlertEvent], None]] = []
        # 统计
        self._stats: Dict[str, int] = defaultdict(int)
        # 异步队列
        self._queue: Optional[asyncio.Queue] = None
        self._alert_counter = 0

    @property
    def rules(self) -> Dict[str, AlertRule]:
        return dict(self._rules)

    @property
    def events(self) -> List[AlertEvent]:
        return list(self._events)

    @property
    def channels(self) -> Dict[str, NotificationChannel]:
        return dict(self._channels)

    # ─── 渠道管理 ───

    def add_channel(self, channel: NotificationChannel) -> None:
        """添加通知渠道"""
        self._channels[channel.name] = channel
        logger.info(f"Notification channel added: {channel.name}")

    def remove_channel(self, name: str) -> bool:
        """移除通知渠道"""
        if name in self._channels:
            del self._channels[name]
            return True
        return False

    # ─── 规则管理 ───

    def add_rule(self, rule: AlertRule) -> None:
        """添加告警规则"""
        self._rules[rule.name] = rule
        logger.info(f"Alert rule added: {rule.name} ({rule.level.label})")

    def remove_rule(self, name: str) -> bool:
        """移除告警规则"""
        if name in self._rules:
            del self._rules[name]
            return True
        return False

    def enable_rule(self, name: str) -> None:
        """启用规则"""
        if name in self._rules:
            self._rules[name].enabled = True

    def disable_rule(self, name: str) -> None:
        """禁用规则"""
        if name in self._rules:
            self._rules[name].enabled = False

    # ─── 告警触发 ───

    async def fire(
        self,
        rule_name: str,
        message: str,
        source: str = "system",
        title: Optional[str] = None,
        level_override: Optional[AlertLevel] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Optional[AlertEvent]:
        """触发告警

        Args:
            rule_name: 规则名称
            message: 告警消息
            source: 来源模块
            title: 告警标题（默认使用规则描述）
            level_override: 覆盖告警级别
            extra: 附加信息

        Returns:
            AlertEvent 或 None（被冷却过滤）
        """
        rule = self._rules.get(rule_name)

        # 确定级别
        if level_override:
            level = level_override
        elif rule:
            level = rule.level
        else:
            level = AlertLevel.WARNING

        # 检查规则是否启用
        if rule and not rule.enabled:
            return None

        # 冷却检查
        if rule and not self._check_cooldown(rule_name, rule.cooldown_seconds):
            return None

        # 创建事件
        self._alert_counter += 1
        event = AlertEvent(
            alert_id=f"ALT-{self._alert_counter:06d}",
            rule_name=rule_name,
            level=level,
            title=title or (rule.description if rule else rule_name),
            message=message,
            source=source,
            timestamp=int(time.time() * 1000),
            extra=extra or {},
        )

        # 记录事件
        self._events.append(event)
        if len(self._events) > self._max_history:
            self._events = self._events[-self._max_history:]

        # 更新统计
        self._stats[rule_name] += 1
        self._trigger_counts[rule_name] += 1

        # 告警升级检查
        if rule and rule.escalate_after > 0 and rule.escalate_to:
            if self._trigger_counts[rule_name] >= rule.escalate_after:
                event.level = rule.escalate_to
                event.extra["escalated"] = True
                self._trigger_counts[rule_name] = 0
                logger.warning(f"Alert escalated: {rule_name} → {rule.escalate_to.label}")

        # 触发回调
        for cb in self._callbacks:
            try:
                cb(event)
            except Exception as e:
                logger.error(f"Alert callback error: {e}")

        # 发送通知
        await self._notify(event, rule)

        logger.log(
            self._level_to_log(level),
            f"Alert [{level.label}] {event.title}: {message} (source={source})"
        )

        return event

    async def fire_info(self, message: str, source: str = "system", **kwargs) -> Optional[AlertEvent]:
        """触发 INFO 级别告警"""
        return await self.fire("_info", message, source=source, level_override=AlertLevel.INFO, **kwargs)

    async def fire_warning(self, message: str, source: str = "system", **kwargs) -> Optional[AlertEvent]:
        """触发 WARNING 级别告警"""
        return await self.fire("_warning", message, source=source, level_override=AlertLevel.WARNING, **kwargs)

    async def fire_critical(self, message: str, source: str = "system", **kwargs) -> Optional[AlertEvent]:
        """触发 CRITICAL 级别告警"""
        return await self.fire("_critical", message, source=source, level_override=AlertLevel.CRITICAL, **kwargs)

    async def fire_fatal(self, message: str, source: str = "system", **kwargs) -> Optional[AlertEvent]:
        """触发 FATAL 级别告警"""
        return await self.fire("_fatal", message, source=source, level_override=AlertLevel.FATAL, **kwargs)

    # ─── 事件回调 ───

    def on_alert(self, callback: Callable[[AlertEvent], None]) -> None:
        """注册告警回调"""
        self._callbacks.append(callback)

    # ─── 查询 ───

    def get_events(
        self,
        level: Optional[AlertLevel] = None,
        source: Optional[str] = None,
        limit: int = 50,
    ) -> List[AlertEvent]:
        """查询告警事件"""
        events = self._events
        if level:
            events = [e for e in events if e.level == level]
        if source:
            events = [e for e in events if e.source == source]
        return events[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """获取告警统计"""
        level_counts = defaultdict(int)
        for e in self._events:
            level_counts[e.level.label] += 1

        return {
            "total_events": len(self._events),
            "by_level": dict(level_counts),
            "by_rule": dict(self._stats),
            "active_channels": [name for name, ch in self._channels.items() if ch.enabled],
            "rules_count": len(self._rules),
        }

    def acknowledge(self, alert_id: str) -> bool:
        """确认告警"""
        for event in self._events:
            if event.alert_id == alert_id:
                event.acknowledged = True
                return True
        return False

    def reset_counter(self, rule_name: str) -> None:
        """重置触发计数"""
        self._trigger_counts[rule_name] = 0

    # ─── 内部方法 ───

    def _check_cooldown(self, rule_name: str, cooldown_seconds: int) -> bool:
        """检查冷却期"""
        if cooldown_seconds <= 0:
            return True

        now = time.time()
        last_time = self._cooldowns.get(rule_name, 0)

        if now - last_time < cooldown_seconds:
            return False

        self._cooldowns[rule_name] = now
        return True

    async def _notify(self, event: AlertEvent, rule: Optional[AlertRule]) -> None:
        """发送通知到渠道"""
        # INFO 级别默认不通知（仅记录）
        if event.level == AlertLevel.INFO and not rule:
            return

        # 确定目标渠道
        if rule and rule.channels:
            target_channels = [
                self._channels[name]
                for name in rule.channels
                if name in self._channels and self._channels[name].enabled
            ]
        else:
            target_channels = [ch for ch in self._channels.values() if ch.enabled]

        if not target_channels:
            return

        # 构建消息
        msg = Message(
            title=event.title,
            content=event.message,
            level=event.level.label,
            timestamp=event.timestamp,
            extra={**event.extra, "source": event.source, "alert_id": event.alert_id},
        )

        # 发送到所有目标渠道
        for channel in target_channels:
            try:
                success = await channel.send(msg)
                if success:
                    event.notified = True
            except Exception as e:
                logger.error(f"Notify failed via {channel.name}: {e}")

    @staticmethod
    def _level_to_log(level: AlertLevel) -> int:
        """告警级别转日志级别"""
        mapping = {
            AlertLevel.INFO: logging.INFO,
            AlertLevel.WARNING: logging.WARNING,
            AlertLevel.CRITICAL: logging.CRITICAL,
            AlertLevel.FATAL: logging.CRITICAL,
        }
        return mapping.get(level, logging.INFO)

    # ─── 预设规则 ───

    def setup_default_rules(self) -> None:
        """配置默认告警规则"""
        default_rules = [
            AlertRule(
                name="ws_disconnect",
                level=AlertLevel.CRITICAL,
                description="WebSocket 连接断开",
                cooldown_seconds=60,
                escalate_after=3,
                escalate_to=AlertLevel.FATAL,
            ),
            AlertRule(
                name="order_failed",
                level=AlertLevel.WARNING,
                description="订单执行失败",
                cooldown_seconds=30,
            ),
            AlertRule(
                name="risk_triggered",
                level=AlertLevel.CRITICAL,
                description="风控规则触发",
                cooldown_seconds=10,
            ),
            AlertRule(
                name="strategy_crash",
                level=AlertLevel.CRITICAL,
                description="策略崩溃",
                cooldown_seconds=60,
            ),
            AlertRule(
                name="high_drawdown",
                level=AlertLevel.WARNING,
                description="回撤超限",
                cooldown_seconds=300,
            ),
            AlertRule(
                name="api_error",
                level=AlertLevel.WARNING,
                description="交易所 API 错误",
                cooldown_seconds=60,
                escalate_after=5,
                escalate_to=AlertLevel.CRITICAL,
            ),
            AlertRule(
                name="funding_rate_extreme",
                level=AlertLevel.WARNING,
                description="资金费率异常",
                cooldown_seconds=600,
            ),
            AlertRule(
                name="system_resource",
                level=AlertLevel.WARNING,
                description="系统资源告警",
                cooldown_seconds=300,
            ),
        ]

        for rule in default_rules:
            self.add_rule(rule)

        logger.info(f"Default alert rules configured: {len(default_rules)}")

    async def close(self) -> None:
        """关闭所有渠道"""
        for channel in self._channels.values():
            await channel.close()

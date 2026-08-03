"""KlineQuant 内部消息信封定义

所有引擎间通信使用统一消息信封，遵循需求文档 §7.1 ~ §7.2。
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict


# ─────────────────────────────────────────────
# §7.1 消息信封
# ─────────────────────────────────────────────
@dataclass
class Message:
    """引擎间统一消息信封"""

    msg_type: str  # 消息类型，如 "KLINE_UPDATE", "ORDER_SUBMIT"
    source: str  # 发送方模块，如 "market_engine"
    payload: Dict[str, Any]  # 消息体
    target: str = "*"  # 接收方模块，"*" 表示广播
    timestamp: int = 0  # 发送时间 (Unix ms)，0 表示自动填充
    priority: int = 5  # 优先级 0(低) - 9(高)，默认 5
    msg_id: str = ""  # UUID，全局唯一，空表示自动生成
    trace_id: str = ""  # 链路追踪 ID，空表示自动生成

    def __post_init__(self) -> None:
        if not self.msg_id:
            self.msg_id = str(uuid.uuid4())
        if not self.trace_id:
            self.trace_id = str(uuid.uuid4())
        if self.timestamp == 0:
            self.timestamp = int(time.time() * 1000)
        if not 0 <= self.priority <= 9:
            raise ValueError(f"priority must be 0-9, got {self.priority}")


# ─────────────────────────────────────────────
# §7.2 消息类型注册表
# ─────────────────────────────────────────────
class MessageType:
    """消息类型常量，对应需求文档 §7.2 注册表"""

    # 行情相关
    KLINE_UPDATE = "KLINE_UPDATE"  # Market → Indicator / Storage
    KLINE_CLOSED = "KLINE_CLOSED"  # Market → Indicator
    TICK_UPDATE = "TICK_UPDATE"  # Market → (订阅者)

    # 指标相关
    INDICATOR_UPDATE = "INDICATOR_UPDATE"  # Indicator → Signal / Strategy

    # 信号相关
    SIGNAL_GENERATED = "SIGNAL_GENERATED"  # Signal → Trade / Gateway

    # 交易相关
    ORDER_SUBMIT = "ORDER_SUBMIT"  # Trade → Exchange Adapter
    ORDER_UPDATE = "ORDER_UPDATE"  # Exchange Adapter → Trade → Gateway
    POSITION_UPDATE = "POSITION_UPDATE"  # Trade → Gateway / Risk

    # 风控相关
    RISK_CHECK = "RISK_CHECK"  # Trade → Risk
    RISK_RESULT = "RISK_RESULT"  # Risk → Trade

    # 策略相关
    STRATEGY_COMMAND = "STRATEGY_COMMAND"  # Gateway → Strategy Sandbox
    STRATEGY_EVENT = "STRATEGY_EVENT"  # Strategy Sandbox → Gateway

    # 系统
    ALERT = "ALERT"  # Any → Gateway
    HEARTBEAT = "HEARTBEAT"  # Any → Monitor


# 消息方向约束：(合法 source, 合法 target 列表)
_MESSAGE_ROUTES: Dict[str, Dict[str, Any]] = {
    MessageType.KLINE_UPDATE: {
        "source": "market_engine",
        "targets": {"indicator_engine", "storage", "*"},
    },
    MessageType.KLINE_CLOSED: {
        "source": "market_engine",
        "targets": {"indicator_engine", "*"},
    },
    MessageType.TICK_UPDATE: {
        "source": "market_engine",
        "targets": {"*"},
    },
    MessageType.INDICATOR_UPDATE: {
        "source": "indicator_engine",
        "targets": {"signal_engine", "strategy_sandbox", "*"},
    },
    MessageType.SIGNAL_GENERATED: {
        "source": "signal_engine",
        "targets": {"trade_engine", "gateway", "*"},
    },
    MessageType.ORDER_SUBMIT: {
        "source": "trade_engine",
        "targets": {"exchange_adapter"},
    },
    MessageType.ORDER_UPDATE: {
        "source": "exchange_adapter",
        "targets": {"trade_engine", "gateway", "*"},
    },
    MessageType.POSITION_UPDATE: {
        "source": "trade_engine",
        "targets": {"gateway", "risk_engine", "*"},
    },
    MessageType.RISK_CHECK: {
        "source": "trade_engine",
        "targets": {"risk_engine"},
    },
    MessageType.RISK_RESULT: {
        "source": "risk_engine",
        "targets": {"trade_engine"},
    },
    MessageType.STRATEGY_COMMAND: {
        "source": "gateway",
        "targets": {"strategy_sandbox"},
    },
    MessageType.STRATEGY_EVENT: {
        "source": "strategy_sandbox",
        "targets": {"gateway", "*"},
    },
    MessageType.ALERT: {
        "source": "*",
        "targets": {"gateway", "*"},
    },
    MessageType.HEARTBEAT: {
        "source": "*",
        "targets": {"monitor", "*"},
    },
}

# 所有已注册的消息类型集合
REGISTERED_TYPES = frozenset(_MESSAGE_ROUTES.keys())


def validate_message_route(msg_type: str, source: str, target: str) -> bool:
    """校验消息路由是否合法。

    Args:
        msg_type: 消息类型
        source: 发送方
        target: 接收方

    Returns:
        True 表示路由合法
    """
    if msg_type not in _MESSAGE_ROUTES:
        return False
    route = _MESSAGE_ROUTES[msg_type]
    source_ok = route["source"] == "*" or route["source"] == source
    target_ok = target in route["targets"]
    return source_ok and target_ok

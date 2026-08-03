"""Transport 抽象基类

定义引擎间通信的统一接口，支持替换底层实现（ZMQ / WebSocket / gRPC）。
遵循需求文档 §7.3 的传输层抽象设计。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Coroutine

from protocol.messages import Message


# 消息处理回调类型
MessageHandler = Callable[[Message], Coroutine[Any, Any, None]]


class Transport(ABC):
    """传输层抽象基类，所有通信实现必须实现此接口。

    支持两种通信模式：
        - PUB/SUB：一对多广播（行情/指标/信号/订单更新）
        - REQ/REP：一对一请求响应（风控校验/交易命令/策略沙箱）
    """

    @abstractmethod
    async def start(self) -> None:
        """启动传输层，绑定端口并准备接收消息。"""
        ...

    @abstractmethod
    async def stop(self) -> None:
        """停止传输层，释放所有资源。"""
        ...

    # ─── PUB/SUB 广播模式 ───

    @abstractmethod
    async def publish(self, topic: str, message: Message) -> None:
        """发布消息到指定主题（广播给所有订阅者）。

        Args:
            topic: 主题名称，如 "kline", "indicator", "signal"
            message: 要发布的消息
        """
        ...

    @abstractmethod
    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """订阅指定主题的消息。

        Args:
            topic: 主题名称，"*" 表示订阅所有
            handler: 收到消息时的异步回调
        """
        ...

    @abstractmethod
    async def unsubscribe(self, topic: str) -> None:
        """取消订阅指定主题。

        Args:
            topic: 要取消的主题名称
        """
        ...

    # ─── REQ/REP 请求响应模式 ───

    @abstractmethod
    async def request(
        self, target: str, message: Message, timeout: float = 5.0
    ) -> Message:
        """向指定目标发送请求并等待响应。

        Args:
            target: 目标服务标识
            message: 请求消息
            timeout: 超时时间（秒）

        Returns:
            响应消息

        Raises:
            TimeoutError: 超时无响应
        """
        ...

    @abstractmethod
    async def register_handler(self, service_name: str, handler: MessageHandler) -> None:
        """注册请求响应服务处理器。

        Args:
            service_name: 服务名称，如 "risk_engine", "trade_engine"
            handler: 处理请求的异步回调，返回响应消息
        """
        ...

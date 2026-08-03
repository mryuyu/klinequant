"""ZMQ Transport 实现

基于 ZeroMQ 的传输层实现：
    - PUB/SUB：用于行情/指标/信号/订单广播
    - REQ/REP：用于风控校验/交易命令/策略沙箱请求响应

遵循需求文档 §7.3 ~ §7.4。
Windows 适配：绑定 127.0.0.1，使用 zmq.asyncio。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Coroutine, Dict, Optional

import zmq
import zmq.asyncio

from protocol.codec import deserialize_message, serialize_message
from protocol.messages import Message
from protocol.transport.base import MessageHandler, Transport

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# §7.4 ZMQ 端口规划
# ─────────────────────────────────────────────
class PortRegistry:
    """ZMQ 端口注册表，管理端口分配和查询。"""

    # PUB 广播端口
    MARKET_PUB = 5501      # MarketEngine 行情广播
    INDICATOR_PUB = 5502   # IndicatorEngine 指标广播
    SIGNAL_PUB = 5503      # SignalEngine 信号广播
    TRADE_PUB = 5504       # TradeEngine 订单/持仓广播

    # REP 请求响应端口
    RISK_REP = 5510        # RiskEngine 风控请求响应
    TRADE_REP = 5511       # TradeEngine 交易命令

    # 策略沙箱端口范围（每策略一对 REQ/REP）
    STRATEGY_PORT_BASE = 5520
    STRATEGY_PORT_MAX = 5530

    # 主题 → PUB 端口映射
    _TOPIC_PORT_MAP: Dict[str, int] = {
        "kline": MARKET_PUB,
        "tick": MARKET_PUB,
        "indicator": INDICATOR_PUB,
        "signal": SIGNAL_PUB,
        "order": TRADE_PUB,
        "position": TRADE_PUB,
    }

    # 服务名 → REP 端口映射
    _SERVICE_PORT_MAP: Dict[str, int] = {
        "risk_engine": RISK_REP,
        "trade_engine": TRADE_REP,
    }

    _next_strategy_port: int = STRATEGY_PORT_BASE

    @classmethod
    def get_pub_port(cls, topic: str) -> int:
        """获取主题对应的 PUB 端口"""
        return cls._TOPIC_PORT_MAP.get(topic, cls.MARKET_PUB)

    @classmethod
    def get_rep_port(cls, service_name: str) -> int:
        """获取服务对应的 REP 端口"""
        port = cls._SERVICE_PORT_MAP.get(service_name)
        if port is None:
            raise ValueError(f"Unknown service: {service_name}")
        return port

    @classmethod
    def allocate_strategy_port(cls) -> int:
        """为策略沙箱分配下一对可用端口"""
        if cls._next_strategy_port > cls.STRATEGY_PORT_MAX:
            raise RuntimeError("No more strategy ports available (5520-5530 exhausted)")
        port = cls._next_strategy_port
        cls._next_strategy_port += 1
        return port

    @classmethod
    def all_ports(cls) -> list[int]:
        """返回所有已注册端口"""
        ports = [
            cls.MARKET_PUB, cls.INDICATOR_PUB, cls.SIGNAL_PUB, cls.TRADE_PUB,
            cls.RISK_REP, cls.TRADE_REP,
        ]
        ports.extend(range(cls.STRATEGY_PORT_BASE, cls._next_strategy_port))
        return ports

    @classmethod
    def reset(cls) -> None:
        """重置策略端口分配（测试用）"""
        cls._next_strategy_port = cls.STRATEGY_PORT_BASE


# ─────────────────────────────────────────────
# ZMQ PUB/SUB 广播器
# ─────────────────────────────────────────────
class ZmqPublisher:
    """ZMQ PUB 端：绑定端口，广播消息给所有订阅者。"""

    def __init__(self, bind_host: str = "127.0.0.1", port: int = 5501):
        self._bind_addr = f"tcp://{bind_host}:{port}"
        self._context: Optional[zmq.asyncio.Context] = None
        self._socket: Optional[zmq.asyncio.Socket] = None
        self._port = port

    @property
    def port(self) -> int:
        return self._port

    async def start(self) -> None:
        self._context = zmq.asyncio.Context()
        self._socket = self._context.socket(zmq.PUB)
        self._socket.setsockopt(zmq.LINGER, 1000)
        self._socket.bind(self._bind_addr)
        logger.info(f"ZMQ PUB bound to {self._bind_addr}")

    async def stop(self) -> None:
        if self._socket:
            self._socket.close(linger=500)
        if self._context:
            self._context.term()
        logger.info(f"ZMQ PUB stopped ({self._bind_addr})")

    async def publish(self, topic: str, message: Message) -> None:
        """发布消息，topic 作为 ZMQ multipart 的第一帧。"""
        if not self._socket:
            raise RuntimeError("Publisher not started")
        topic_bytes = topic.encode("utf-8")
        payload = serialize_message(message)
        await self._socket.send_multipart([topic_bytes, payload])


class ZmqSubscriber:
    """ZMQ SUB 端：连接 PUB，接收广播消息。"""

    def __init__(self, connect_host: str = "127.0.0.1", port: int = 5501):
        self._connect_addr = f"tcp://{connect_host}:{port}"
        self._context: Optional[zmq.asyncio.Context] = None
        self._socket: Optional[zmq.asyncio.Socket] = None
        self._handlers: Dict[str, MessageHandler] = {}
        self._running = False
        self._recv_task: Optional[asyncio.Task] = None
        self._port = port

    @property
    def port(self) -> int:
        return self._port

    async def start(self) -> None:
        self._context = zmq.asyncio.Context()
        self._socket = self._context.socket(zmq.SUB)
        self._socket.setsockopt(zmq.LINGER, 1000)
        self._socket.connect(self._connect_addr)
        logger.info(f"ZMQ SUB connected to {self._connect_addr}")

    async def stop(self) -> None:
        self._running = False
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
        if self._socket:
            self._socket.close(linger=500)
        if self._context:
            self._context.term()
        logger.info(f"ZMQ SUB stopped ({self._connect_addr})")

    def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """订阅指定主题"""
        if not self._socket:
            raise RuntimeError("Subscriber not started")
        self._handlers[topic] = handler
        # ZMQ SUB 过滤：topic 或空字符串（全部）
        sub_filter = b"" if topic == "*" else topic.encode("utf-8")
        self._socket.setsockopt(zmq.SUBSCRIBE, sub_filter)

    def unsubscribe(self, topic: str) -> None:
        """取消订阅"""
        if not self._socket:
            return
        self._handlers.pop(topic, None)
        sub_filter = b"" if topic == "*" else topic.encode("utf-8")
        self._socket.setsockopt(zmq.UNSUBSCRIBE, sub_filter)

    async def start_receiving(self) -> None:
        """启动后台接收循环"""
        self._running = True
        self._recv_task = asyncio.create_task(self._recv_loop())

    async def _recv_loop(self) -> None:
        """后台接收消息并分发给处理器"""
        while self._running and self._socket:
            try:
                parts = await self._socket.recv_multipart(flags=zmq.NOBLOCK)
                if len(parts) < 2:
                    continue
                topic = parts[0].decode("utf-8")
                message = deserialize_message(parts[1])

                # 精确匹配或通配符
                handler = self._handlers.get(topic) or self._handlers.get("*")
                if handler:
                    try:
                        await handler(message)
                    except Exception:
                        logger.exception(f"Handler error for topic '{topic}'")
            except zmq.Again:
                await asyncio.sleep(0.001)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("ZMQ SUB recv error")
                await asyncio.sleep(0.01)


# ─────────────────────────────────────────────
# ZMQ REQ/REP 请求响应
# ─────────────────────────────────────────────
class ZmqRepServer:
    """ZMQ REP 端：绑定端口，接收请求并返回响应。"""

    def __init__(
        self, service_name: str, bind_host: str = "127.0.0.1", port: int = 5510
    ):
        self._service_name = service_name
        self._bind_addr = f"tcp://{bind_host}:{port}"
        self._context: Optional[zmq.asyncio.Context] = None
        self._socket: Optional[zmq.asyncio.Socket] = None
        self._handler: Optional[MessageHandler] = None
        self._running = False
        self._recv_task: Optional[asyncio.Task] = None
        self._port = port

    @property
    def port(self) -> int:
        return self._port

    @property
    def service_name(self) -> str:
        return self._service_name

    async def start(self) -> None:
        self._context = zmq.asyncio.Context()
        self._socket = self._context.socket(zmq.REP)
        self._socket.setsockopt(zmq.LINGER, 1000)
        self._socket.bind(self._bind_addr)
        logger.info(f"ZMQ REP [{self._service_name}] bound to {self._bind_addr}")

    async def stop(self) -> None:
        self._running = False
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
        if self._socket:
            self._socket.close(linger=500)
        if self._context:
            self._context.term()
        logger.info(f"ZMQ REP [{self._service_name}] stopped")

    def set_handler(self, handler: MessageHandler) -> None:
        self._handler = handler

    async def start_serving(self) -> None:
        """启动后台服务循环"""
        self._running = True
        self._recv_task = asyncio.create_task(self._serve_loop())

    async def _serve_loop(self) -> None:
        while self._running and self._socket:
            try:
                # REP 自动剥离 DEALER 发送的空帧定界符，直接 recv 即可
                data = await self._socket.recv(flags=zmq.NOBLOCK)
                request = deserialize_message(data)

                if self._handler:
                    response = await self._handler(request)
                    await self._socket.send(serialize_message(response))
                else:
                    # 无处理器时返回错误响应
                    error_msg = Message(
                        msg_type="ERROR",
                        source=self._service_name,
                        payload={"error": "No handler registered"},
                    )
                    await self._socket.send(serialize_message(error_msg))
            except zmq.Again:
                await asyncio.sleep(0.001)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception(f"ZMQ REP [{self._service_name}] serve error")
                await asyncio.sleep(0.01)


class ZmqReqClient:
    """ZMQ REQ 端：连接 REP 服务，发送请求并等待响应。

    使用 DEALER socket 替代 REQ socket，避免 REQ 严格状态机导致
    超时后无法复用、多次请求串行阻塞等问题。
    DEALER → REP 通信协议：发送 [b"", data] multipart，REP 自动剥离空帧。
    """

    def __init__(self, connect_host: str = "127.0.0.1", port: int = 5510):
        self._connect_addr = f"tcp://{connect_host}:{port}"
        self._context: Optional[zmq.asyncio.Context] = None
        self._socket: Optional[zmq.asyncio.Socket] = None
        self._port = port

    @property
    def port(self) -> int:
        return self._port

    async def start(self) -> None:
        self._context = zmq.asyncio.Context()
        # DEALER 无 REQ 严格状态机，支持超时后重发
        self._socket = self._context.socket(zmq.DEALER)
        self._socket.setsockopt(zmq.LINGER, 1000)
        self._socket.connect(self._connect_addr)
        logger.info(f"ZMQ DEALER connected to {self._connect_addr}")

    async def stop(self) -> None:
        if self._socket:
            self._socket.close(linger=500)
        if self._context:
            self._context.term()

    async def request(self, message: Message, timeout: float = 5.0) -> Message:
        """发送请求并等待响应"""
        if not self._socket:
            raise RuntimeError("Client not started")

        # DEALER → REP: 发送 [empty_delimiter, data]
        payload = serialize_message(message)
        await self._socket.send_multipart([b"", payload])

        poller = zmq.asyncio.Poller()
        poller.register(self._socket, zmq.POLLIN)

        events = dict(await poller.poll(timeout * 1000))
        if self._socket not in events:
            raise TimeoutError(
                f"No response from {self._connect_addr} within {timeout}s"
            )

        # REP 回复自动前缀空帧 → DEALER 收到 [b"", data]
        parts = await self._socket.recv_multipart()
        return deserialize_message(parts[-1])


# ─────────────────────────────────────────────
# ZmqTransport：统一 Transport 接口实现
# ─────────────────────────────────────────────
class ZmqTransport(Transport):
    """ZMQ 传输层：整合 PUB/SUB 广播 + REQ/REP 请求响应。

    用法示例：
        # 作为 PUB 端（如 MarketEngine）
        transport = ZmqTransport(role="publisher", pub_port=5501)
        await transport.start()
        await transport.publish("kline", kline_message)

        # 作为 SUB 端（如 IndicatorEngine）
        transport = ZmqTransport(role="subscriber", sub_port=5501)
        await transport.start()
        await transport.subscribe("kline", my_handler)

        # 作为 REP 服务端（如 RiskEngine）
        transport = ZmqTransport(role="server", service_name="risk_engine", rep_port=5510)
        await transport.start()
        await transport.register_handler("risk_engine", risk_check_handler)

        # 作为 REQ 客户端（如 TradeEngine 调用 RiskEngine）
        transport = ZmqTransport(role="client", rep_port=5510)
        await transport.start()
        response = await transport.request("risk_engine", check_message)
    """

    def __init__(
        self,
        role: str = "publisher",  # publisher / subscriber / server / client
        bind_host: str = "127.0.0.1",
        pub_port: Optional[int] = None,
        sub_port: Optional[int] = None,
        rep_port: Optional[int] = None,
        service_name: str = "",
    ):
        self._role = role
        self._bind_host = bind_host
        self._publisher: Optional[ZmqPublisher] = None
        self._subscriber: Optional[ZmqSubscriber] = None
        self._rep_server: Optional[ZmqRepServer] = None
        self._req_client: Optional[ZmqReqClient] = None

        if role == "publisher":
            if pub_port is None:
                raise ValueError("pub_port required for publisher role")
            self._publisher = ZmqPublisher(bind_host, pub_port)

        elif role == "subscriber":
            if sub_port is None:
                raise ValueError("sub_port required for subscriber role")
            self._subscriber = ZmqSubscriber(bind_host, sub_port)

        elif role == "server":
            if rep_port is None:
                raise ValueError("rep_port required for server role")
            if not service_name:
                raise ValueError("service_name required for server role")
            self._rep_server = ZmqRepServer(service_name, bind_host, rep_port)

        elif role == "client":
            if rep_port is None:
                raise ValueError("rep_port required for client role")
            self._req_client = ZmqReqClient(bind_host, rep_port)

        else:
            raise ValueError(f"Unknown role: {role}")

    async def start(self) -> None:
        if self._publisher:
            await self._publisher.start()
        if self._subscriber:
            await self._subscriber.start()
        if self._rep_server:
            await self._rep_server.start()
            await self._rep_server.start_serving()
        if self._req_client:
            await self._req_client.start()

    async def stop(self) -> None:
        if self._publisher:
            await self._publisher.stop()
        if self._subscriber:
            await self._subscriber.stop()
        if self._rep_server:
            await self._rep_server.stop()
        if self._req_client:
            await self._req_client.stop()

    async def publish(self, topic: str, message: Message) -> None:
        if not self._publisher:
            raise RuntimeError(f"Cannot publish in '{self._role}' role")
        await self._publisher.publish(topic, message)

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        if not self._subscriber:
            raise RuntimeError(f"Cannot subscribe in '{self._role}' role")
        self._subscriber.subscribe(topic, handler)
        if not self._subscriber._running:
            await self._subscriber.start_receiving()

    async def unsubscribe(self, topic: str) -> None:
        if not self._subscriber:
            raise RuntimeError(f"Cannot unsubscribe in '{self._role}' role")
        self._subscriber.unsubscribe(topic)

    async def request(
        self, target: str, message: Message, timeout: float = 5.0
    ) -> Message:
        if not self._req_client:
            raise RuntimeError(f"Cannot request in '{self._role}' role")
        return await self._req_client.request(message, timeout)

    async def register_handler(self, service_name: str, handler: MessageHandler) -> None:
        if not self._rep_server:
            raise RuntimeError(f"Cannot register handler in '{self._role}' role")
        self._rep_server.set_handler(handler)

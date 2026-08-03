"""ZMQ Transport 通信层单元测试

覆盖 T-T-001 ~ T-T-005：
    T-T-001: Transport 抽象接口完整性
    T-T-002: ZMQ PUB/SUB 发布-接收往返
    T-T-003: ZMQ REQ/REP 请求-响应往返
    T-T-004: 消息类型注册/查找/校验（复用 protocol 层测试）
    T-T-005: 端口绑定冲突检测
"""
from __future__ import annotations

import asyncio
import inspect

import pytest

from protocol.messages import Message
from protocol.transport.base import MessageHandler, Transport
from protocol.transport.zmq_transport import (
    PortRegistry,
    ZmqPublisher,
    ZmqRepServer,
    ZmqReqClient,
    ZmqSubscriber,
    ZmqTransport,
)


# ═══════════════════════════════════════════
# T-T-001: Transport 抽象接口完整性
# ═══════════════════════════════════════════


class TestTransportAbstract:
    """验证 Transport 抽象基类的接口定义"""

    REQUIRED_METHODS = [
        "start", "stop", "publish", "subscribe",
        "unsubscribe", "request", "register_handler",
    ]

    def test_transport_is_abstract(self):
        """Transport 不能直接实例化"""
        with pytest.raises(TypeError):
            Transport()

    def test_all_methods_defined(self):
        """所有必需方法必须存在"""
        for method_name in self.REQUIRED_METHODS:
            assert hasattr(Transport, method_name), f"Missing method: {method_name}"
            assert inspect.iscoroutinefunction(getattr(Transport, method_name))

    def test_zmq_transport_implements_all(self):
        """ZmqTransport 必须实现 Transport 的所有抽象方法"""
        for method_name in self.REQUIRED_METHODS:
            assert hasattr(ZmqTransport, method_name), (
                f"ZmqTransport missing: {method_name}"
            )

    def test_zmq_transport_is_subclass(self):
        """ZmqTransport 是 Transport 的子类"""
        assert issubclass(ZmqTransport, Transport)


# ═══════════════════════════════════════════
# T-T-002: ZMQ PUB/SUB 发布-接收往返
# ═══════════════════════════════════════════


class TestZmqPubSub:
    """验证 PUB/SUB 广播通信"""

    @pytest.mark.asyncio
    async def test_pub_sub_roundtrip(self):
        """发布一条消息，订阅端能正确接收"""
        port = 15601  # 测试用端口，避免冲突
        received = []

        async def handler(msg: Message):
            received.append(msg)

        pub = ZmqPublisher("127.0.0.1", port)
        sub = ZmqSubscriber("127.0.0.1", port)

        try:
            await pub.start()
            await sub.start()
            sub.subscribe("test", handler)
            await sub.start_receiving()

            # 等待 SUB 连接建立
            await asyncio.sleep(0.3)

            msg = Message(
                msg_type="KLINE",
                source="market_engine",
                payload={"symbol": "BTCUSDT", "close": 60000.0},
            )
            await pub.publish("test", msg)

            # 等待接收
            for _ in range(50):
                if received:
                    break
                await asyncio.sleep(0.02)

            assert len(received) == 1
            assert received[0].msg_type == "KLINE"
            assert received[0].source == "market_engine"
            assert received[0].payload["symbol"] == "BTCUSDT"
            assert received[0].payload["close"] == 60000.0
        finally:
            await sub.stop()
            await pub.stop()

    @pytest.mark.asyncio
    async def test_pub_multiple_topics(self):
        """多主题订阅：只收到匹配的消息"""
        port = 15602
        received_kline = []
        received_tick = []

        async def kline_handler(msg: Message):
            received_kline.append(msg)

        async def tick_handler(msg: Message):
            received_tick.append(msg)

        pub = ZmqPublisher("127.0.0.1", port)
        sub = ZmqSubscriber("127.0.0.1", port)

        try:
            await pub.start()
            await sub.start()
            sub.subscribe("kline", kline_handler)
            sub.subscribe("tick", tick_handler)
            await sub.start_receiving()
            await asyncio.sleep(0.3)

            kline_msg = Message(
                msg_type="KLINE", source="market_engine",
                payload={"symbol": "BTCUSDT"},
            )
            tick_msg = Message(
                msg_type="TICK", source="market_engine",
                payload={"symbol": "BTCUSDT", "bid": 60000.0},
            )

            await pub.publish("kline", kline_msg)
            await pub.publish("tick", tick_msg)
            await pub.publish("kline", kline_msg)

            for _ in range(50):
                if len(received_kline) >= 2 and len(received_tick) >= 1:
                    break
                await asyncio.sleep(0.02)

            assert len(received_kline) == 2
            assert len(received_tick) == 1
        finally:
            await sub.stop()
            await pub.stop()

    @pytest.mark.asyncio
    async def test_pub_sub_wildcard(self):
        """通配符 '*' 订阅所有主题"""
        port = 15603
        received = []

        async def handler(msg: Message):
            received.append(msg)

        pub = ZmqPublisher("127.0.0.1", port)
        sub = ZmqSubscriber("127.0.0.1", port)

        try:
            await pub.start()
            await sub.start()
            sub.subscribe("*", handler)
            await sub.start_receiving()
            await asyncio.sleep(0.3)

            msg_a = Message(msg_type="A", source="s", payload={})
            msg_b = Message(msg_type="B", source="s", payload={})
            await pub.publish("topic_a", msg_a)
            await pub.publish("topic_b", msg_b)

            for _ in range(50):
                if len(received) >= 2:
                    break
                await asyncio.sleep(0.02)

            assert len(received) == 2
        finally:
            await sub.stop()
            await pub.stop()

    @pytest.mark.asyncio
    async def test_unsubscribe(self):
        """取消订阅后不再收到消息"""
        port = 15604
        received = []

        async def handler(msg: Message):
            received.append(msg)

        pub = ZmqPublisher("127.0.0.1", port)
        sub = ZmqSubscriber("127.0.0.1", port)

        try:
            await pub.start()
            await sub.start()
            sub.subscribe("test", handler)
            await sub.start_receiving()
            await asyncio.sleep(0.3)

            msg = Message(msg_type="X", source="s", payload={})
            await pub.publish("test", msg)

            for _ in range(30):
                if received:
                    break
                await asyncio.sleep(0.02)

            assert len(received) == 1

            # 取消订阅
            sub.unsubscribe("test")
            await asyncio.sleep(0.1)

            await pub.publish("test", msg)
            await asyncio.sleep(0.2)

            # 不应再收到新消息
            assert len(received) == 1
        finally:
            await sub.stop()
            await pub.stop()


# ═══════════════════════════════════════════
# T-T-003: ZMQ REQ/REP 请求-响应往返
# ═══════════════════════════════════════════


class TestZmqReqRep:
    """验证 REQ/REP 请求响应通信"""

    @pytest.mark.asyncio
    async def test_req_rep_roundtrip(self):
        """发送请求，收到正确响应"""
        port = 15710  # 独立端口段避免冲突

        async def echo_handler(msg: Message) -> Message:
            return Message(
                msg_type="ECHO_RESP",
                source="risk_engine",
                payload={"echo": msg.payload, "approved": True},
            )

        server = ZmqRepServer("risk_engine", "127.0.0.1", port)
        server.set_handler(echo_handler)
        client = ZmqReqClient("127.0.0.1", port)

        try:
            await server.start()
            await server.start_serving()
            await client.start()
            await asyncio.sleep(0.1)

            req = Message(
                msg_type="RISK_CHECK",
                source="trade_engine",
                payload={"symbol": "BTCUSDT", "amount": 1000},
            )
            resp = await client.request(req, timeout=5.0)

            assert resp.msg_type == "ECHO_RESP"
            assert resp.source == "risk_engine"
            assert resp.payload["approved"] is True
            assert resp.payload["echo"]["symbol"] == "BTCUSDT"
            assert resp.payload["echo"]["amount"] == 1000
        finally:
            await client.stop()
            await server.stop()

    @pytest.mark.asyncio
    async def test_req_rep_multiple(self):
        """多次请求-响应"""
        port = 15711

        async def add_handler(msg: Message) -> Message:
            result = msg.payload["a"] + msg.payload["b"]
            return Message(
                msg_type="RESULT", source="calc", payload={"result": result}
            )

        server = ZmqRepServer("calc", "127.0.0.1", port)
        server.set_handler(add_handler)
        client = ZmqReqClient("127.0.0.1", port)

        try:
            await server.start()
            await server.start_serving()
            await client.start()
            await asyncio.sleep(0.1)

            for i in range(5):
                req = Message(
                    msg_type="ADD", source="test",
                    payload={"a": i, "b": i * 10},
                )
                resp = await client.request(req, timeout=3.0)
                assert resp.payload["result"] == i + i * 10
        finally:
            await client.stop()
            await server.stop()

    @pytest.mark.asyncio
    async def test_req_timeout(self):
        """无服务端时请求超时"""
        port = 15712  # 无服务监听

        client = ZmqReqClient("127.0.0.1", port)
        try:
            await client.start()
            req = Message(msg_type="PING", source="test", payload={})
            with pytest.raises(TimeoutError):
                await client.request(req, timeout=0.5)
        finally:
            await client.stop()


# ═══════════════════════════════════════════
# T-T-004: 端口规划验证
# ═══════════════════════════════════════════


class TestPortRegistry:
    """验证端口注册表的端口分配和查询"""

    def setup_method(self):
        PortRegistry.reset()

    def test_pub_port_mapping(self):
        """主题 → PUB 端口映射"""
        assert PortRegistry.get_pub_port("kline") == 5501
        assert PortRegistry.get_pub_port("tick") == 5501
        assert PortRegistry.get_pub_port("indicator") == 5502
        assert PortRegistry.get_pub_port("signal") == 5503
        assert PortRegistry.get_pub_port("order") == 5504
        assert PortRegistry.get_pub_port("position") == 5504

    def test_rep_port_mapping(self):
        """服务 → REP 端口映射"""
        assert PortRegistry.get_rep_port("risk_engine") == 5510
        assert PortRegistry.get_rep_port("trade_engine") == 5511

    def test_unknown_service_raises(self):
        """未知服务抛出 ValueError"""
        with pytest.raises(ValueError, match="Unknown service"):
            PortRegistry.get_rep_port("unknown_service")

    def test_strategy_port_allocation(self):
        """策略沙箱端口分配"""
        PortRegistry.reset()
        p1 = PortRegistry.allocate_strategy_port()
        p2 = PortRegistry.allocate_strategy_port()
        assert p1 == 5520
        assert p2 == 5521

    def test_strategy_port_exhaustion(self):
        """策略端口耗尽抛出 RuntimeError"""
        PortRegistry.reset()
        for _ in range(11):  # 5520~5530 = 11个
            PortRegistry.allocate_strategy_port()
        with pytest.raises(RuntimeError, match="No more strategy ports"):
            PortRegistry.allocate_strategy_port()

    def test_all_ports(self):
        """返回所有已注册端口"""
        PortRegistry.reset()
        ports = PortRegistry.all_ports()
        assert 5501 in ports
        assert 5510 in ports
        assert 5511 in ports
        assert len(ports) >= 6  # 至少 4 PUB + 2 REP

    def test_port_range(self):
        """所有端口在 5501-5530 范围内"""
        for port in PortRegistry.all_ports():
            assert 5501 <= port <= 5530


# ═══════════════════════════════════════════
# T-T-005: ZmqTransport 角色验证
# ═══════════════════════════════════════════


class TestZmqTransport:
    """验证 ZmqTransport 角色参数校验"""

    def test_publisher_role_requires_port(self):
        with pytest.raises(ValueError, match="pub_port"):
            ZmqTransport(role="publisher")

    def test_subscriber_role_requires_port(self):
        with pytest.raises(ValueError, match="sub_port"):
            ZmqTransport(role="subscriber")

    def test_server_role_requires_port_and_name(self):
        with pytest.raises(ValueError, match="rep_port"):
            ZmqTransport(role="server")
        with pytest.raises(ValueError, match="service_name"):
            ZmqTransport(role="server", rep_port=5510)

    def test_client_role_requires_port(self):
        with pytest.raises(ValueError, match="rep_port"):
            ZmqTransport(role="client")

    def test_unknown_role_raises(self):
        with pytest.raises(ValueError, match="Unknown role"):
            ZmqTransport(role="invalid")

    @pytest.mark.asyncio
    async def test_publisher_cannot_subscribe(self):
        """publisher 角色调用 subscribe 应抛出 RuntimeError"""
        t = ZmqTransport(role="publisher", pub_port=15620)
        await t.start()
        try:
            async def dummy(msg): pass
            with pytest.raises(RuntimeError, match="Cannot subscribe"):
                await t.subscribe("test", dummy)
        finally:
            await t.stop()

    def test_client_cannot_publish(self):
        t = ZmqTransport(role="client", rep_port=15621)
        assert t._publisher is None

    @pytest.mark.asyncio
    async def test_transport_pub_sub_integration(self):
        """集成测试：通过 ZmqTransport 接口完成 PUB/SUB 往返"""
        port = 15630
        received = []

        async def handler(msg: Message):
            received.append(msg)

        pub = ZmqTransport(role="publisher", pub_port=port)
        sub = ZmqTransport(role="subscriber", sub_port=port)

        try:
            await pub.start()
            await sub.start()
            await sub.subscribe("test", handler)
            await asyncio.sleep(0.3)

            msg = Message(msg_type="TEST", source="test", payload={"v": 42})
            await pub.publish("test", msg)

            for _ in range(50):
                if received:
                    break
                await asyncio.sleep(0.02)

            assert len(received) == 1
            assert received[0].payload["v"] == 42
        finally:
            await sub.stop()
            await pub.stop()

    @pytest.mark.asyncio
    async def test_transport_req_rep_integration(self):
        """集成测试：通过 ZmqTransport 接口完成 REQ/REP 往返"""
        port = 15631

        async def handler(msg: Message) -> Message:
            return Message(
                msg_type="OK", source="risk_engine",
                payload={"approved": True},
            )

        server = ZmqTransport(
            role="server", rep_port=port, service_name="risk_engine"
        )
        client = ZmqTransport(role="client", rep_port=port)

        try:
            await server.start()
            await server.register_handler("risk_engine", handler)
            await client.start()
            await asyncio.sleep(0.1)

            req = Message(msg_type="CHECK", source="trade", payload={})
            resp = await client.request("risk_engine", req, timeout=3.0)

            assert resp.msg_type == "OK"
            assert resp.payload["approved"] is True
        finally:
            await client.stop()
            await server.stop()

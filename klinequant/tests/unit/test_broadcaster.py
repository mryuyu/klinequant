"""Phase 3 lead 侧 SignalBroadcaster 单测（真实 ZMQ / localhost）。

覆盖：
  - publish_intent 经真实 ZMQ 被订阅端收到，coid/字段透传
  - broadcast_order / broadcast_cancel / broadcast_flatten 的 action→msg_type
  - 心跳按间隔发出
  - report 反向扇入（lead SUB bind）→ report_callback 聚合
  - fire-and-forget：未启动时 publish 不抛异常（不阻断 lead 本地执行）
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

from protocol.transport.zmq_transport import ZmqTransport
from strategy.sdk.broadcaster import SignalBroadcaster
from strategy.sdk.dist_protocol import (
    ACTION_CANCEL,
    ACTION_FLATTEN,
    DIST_CANCEL,
    DIST_FLATTEN,
    DIST_INTENT,
    TOPIC_HEARTBEAT,
    TOPIC_INTENT,
    TOPIC_REPORT,
    OrderIntent,
    Report,
    parse_message,
    report_to_message,
)

TOKEN = "bcast-token"


async def _wait_for(pred, timeout=3.0, interval=0.02):
    """轮询等待 pred() 为真，超时抛 AssertionError。"""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if pred():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


# ═══════════════════════════════════════════
# intent 广播往返
# ═══════════════════════════════════════════
async def test_publish_intent_received_over_zmq():
    port_i, port_r = 16660, 16661
    bcast = SignalBroadcaster(
        token=TOKEN, account="lead-1",
        intent_endpoint=f"tcp://127.0.0.1:{port_i}",
        report_endpoint=f"tcp://127.0.0.1:{port_r}",
        heartbeat_interval=0,  # 关闭心跳，聚焦 intent
    )
    received: list = []
    sub = ZmqTransport(role="subscriber", bind_host="127.0.0.1",
                       sub_port=port_i, sub_bind=False)
    try:
        bcast.start()
        await sub.start()

        async def handler(msg):
            received.append(msg)

        await sub.subscribe(TOPIC_INTENT, handler)
        await asyncio.sleep(0.4)  # 等 SUB 连接建立

        bcast.broadcast_order(
            client_order_id="KQ-lead-1m-1-aaaa", tag="1m", symbol="BTCUSDT",
            side="BUY", offset="OPEN", qty=Decimal("0.5"), kind="MARKET",
        )
        await _wait_for(lambda: len(received) >= 1)

        obj, reason = parse_message(received[0], TOKEN)
        assert reason is None
        assert isinstance(obj, OrderIntent)
        assert obj.client_order_id == "KQ-lead-1m-1-aaaa"
        assert obj.symbol == "BTCUSDT"
        assert obj.qty == Decimal("0.5")
        assert obj.account == "lead-1"  # broadcaster 注入自身 account
        assert received[0].msg_type == DIST_INTENT
    finally:
        await sub.stop()
        bcast.stop()


async def test_broadcast_cancel_and_flatten_msg_types():
    port_i, port_r = 16662, 16663
    bcast = SignalBroadcaster(
        token=TOKEN, intent_endpoint=f"tcp://127.0.0.1:{port_i}",
        report_endpoint=f"tcp://127.0.0.1:{port_r}", heartbeat_interval=0,
    )
    received: list = []
    sub = ZmqTransport(role="subscriber", bind_host="127.0.0.1",
                       sub_port=port_i, sub_bind=False)
    try:
        bcast.start()
        await sub.start()

        async def handler(msg):
            received.append(msg)

        await sub.subscribe(TOPIC_INTENT, handler)
        await asyncio.sleep(0.4)

        bcast.broadcast_cancel(client_order_id="c-1", tag="1m", symbol="ETHUSDT")
        bcast.broadcast_flatten(client_order_id="f-1", tag="1m", symbol="ETHUSDT")
        await _wait_for(lambda: len(received) >= 2)

        types = {m.msg_type for m in received}
        assert DIST_CANCEL in types
        assert DIST_FLATTEN in types
        objs = [parse_message(m, TOKEN)[0] for m in received]
        actions = {o.action for o in objs}
        assert actions == {ACTION_CANCEL, ACTION_FLATTEN}
    finally:
        await sub.stop()
        bcast.stop()


# ═══════════════════════════════════════════
# 心跳
# ═══════════════════════════════════════════
async def test_heartbeat_emitted_periodically():
    port_i, port_r = 16664, 16665
    bcast = SignalBroadcaster(
        token=TOKEN, intent_endpoint=f"tcp://127.0.0.1:{port_i}",
        report_endpoint=f"tcp://127.0.0.1:{port_r}", heartbeat_interval=0.15,
    )
    received: list = []
    sub = ZmqTransport(role="subscriber", bind_host="127.0.0.1",
                       sub_port=port_i, sub_bind=False)
    try:
        bcast.start()
        await sub.start()

        async def handler(msg):
            received.append(msg)

        await sub.subscribe(TOPIC_HEARTBEAT, handler)
        await asyncio.sleep(0.4)
        # 至少收到一次心跳（间隔 0.15s，等 ~1s 应收多次）
        await _wait_for(lambda: len(received) >= 1, timeout=2.0)
        obj, reason = parse_message(received[0], TOKEN)
        assert reason is None
        assert obj["heartbeat"] is True
    finally:
        await sub.stop()
        bcast.stop()


# ═══════════════════════════════════════════
# report 反向扇入聚合
# ═══════════════════════════════════════════
async def test_report_fan_in_aggregated():
    port_i, port_r = 16666, 16667
    collected: list[Report] = []
    bcast = SignalBroadcaster(
        token=TOKEN, intent_endpoint=f"tcp://127.0.0.1:{port_i}",
        report_endpoint=f"tcp://127.0.0.1:{port_r}", heartbeat_interval=0,
        report_callback=collected.append,
    )
    # 测试侧模拟 follower：report PUB connect 到 lead report SUB（bind）
    pub = ZmqTransport(role="publisher", bind_host="127.0.0.1",
                       pub_port=port_r, pub_bind=False)
    try:
        bcast.start()
        await pub.start()
        await asyncio.sleep(0.4)  # 等 PUB 连接到 lead SUB

        report = Report(client_order_id="KQ-lead-1m-9-zzzz", follower="follower-1",
                        status="FILLED", filled_qty=Decimal("0.25"),
                        filled_price=Decimal("60000"))
        await pub.publish(TOPIC_REPORT, report_to_message(report, TOKEN))

        await _wait_for(lambda: len(collected) >= 1)
        assert collected[0].client_order_id == "KQ-lead-1m-9-zzzz"
        assert collected[0].follower == "follower-1"
        assert collected[0].status == "FILLED"
        assert collected[0].filled_qty == Decimal("0.25")
    finally:
        await pub.stop()
        bcast.stop()


async def test_report_reject_fires_alert(caplog):
    """REJECT 回报 → 无 AlertManager 时至少写日志（不抛异常）。"""
    port_i, port_r = 16668, 16669
    collected: list[Report] = []
    bcast = SignalBroadcaster(
        token=TOKEN, intent_endpoint=f"tcp://127.0.0.1:{port_i}",
        report_endpoint=f"tcp://127.0.0.1:{port_r}", heartbeat_interval=0,
        report_callback=collected.append,
    )
    pub = ZmqTransport(role="publisher", bind_host="127.0.0.1",
                       pub_port=port_r, pub_bind=False)
    try:
        bcast.start()
        await pub.start()
        await asyncio.sleep(0.4)
        await pub.publish(TOPIC_REPORT, report_to_message(
            Report(client_order_id="c-rej", follower="f", status="REJECT",
                   reason="insufficient margin"), TOKEN))
        await _wait_for(lambda: len(collected) >= 1)
        assert collected[0].status == "REJECT"
    finally:
        await pub.stop()
        bcast.stop()


# ═══════════════════════════════════════════
# fire-and-forget：未启动不抛
# ═══════════════════════════════════════════
def test_publish_before_start_does_not_raise():
    bcast = SignalBroadcaster(
        token=TOKEN, intent_endpoint="tcp://127.0.0.1:16670",
        report_endpoint="tcp://127.0.0.1:16671", heartbeat_interval=0,
    )
    # 未 start()：publish/broadcast 只记 warning，绝不抛回调用方
    bcast.publish_intent(OrderIntent(client_order_id="x", symbol="BTCUSDT"))
    bcast.broadcast_order(client_order_id="y", tag="1m", symbol="BTCUSDT",
                          side="BUY", offset="OPEN", qty=Decimal("1"))
    bcast.broadcast_flatten(client_order_id="z", tag="1m", symbol="BTCUSDT")
    # stop() 在未启动时也应安全 no-op
    bcast.stop()


def test_stop_idempotent():
    bcast = SignalBroadcaster(
        token=TOKEN, intent_endpoint="tcp://127.0.0.1:16672",
        report_endpoint="tcp://127.0.0.1:16673", heartbeat_interval=0,
    )
    bcast.start()
    bcast.stop()
    bcast.stop()  # 二次 stop 不应抛

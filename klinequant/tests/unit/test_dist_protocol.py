"""Phase 3 分发协议与信任边界单测（dist_protocol）。

覆盖：
  - OrderIntent / Report / heartbeat 与 Message 互转往返（含 Decimal 保真）
  - token 校验（匹配 / 不匹配，常量时比较）
  - 非白名单 msg_type 拒绝
  - action → msg_type 映射
  - parse_endpoint / is_whitelisted
  - msgpack 序列化全链路 Decimal 保真
"""
from __future__ import annotations

from decimal import Decimal

from protocol.codec import deserialize_message, serialize_message
from protocol.messages import Message
from strategy.sdk.dist_protocol import (
    ACTION_CANCEL,
    ACTION_FLATTEN,
    ACTION_ORDER,
    DIST_CANCEL,
    DIST_FLATTEN,
    DIST_INTENT,
    DIST_REPORT,
    TOPIC_INTENT,
    TOPIC_REPORT,
    OrderIntent,
    Report,
    heartbeat_message,
    intent_to_message,
    is_whitelisted,
    parse_endpoint,
    parse_message,
    report_to_message,
)

TOKEN = "s3cr3t-token"


# ═══════════════════════════════════════════
# 端点 / 白名单
# ═══════════════════════════════════════════
def test_parse_endpoint_full():
    assert parse_endpoint("tcp://127.0.0.1:5560") == ("127.0.0.1", 5560)


def test_parse_endpoint_host_port_only():
    assert parse_endpoint("192.168.1.5:6000") == ("192.168.1.5", 6000)


def test_parse_endpoint_missing_host_defaults_loopback():
    assert parse_endpoint(":5561") == ("127.0.0.1", 5561)


def test_is_whitelisted():
    assert is_whitelisted(DIST_INTENT)
    assert is_whitelisted(DIST_CANCEL)
    assert is_whitelisted(DIST_FLATTEN)
    assert is_whitelisted(DIST_REPORT)
    assert not is_whitelisted("KLINE")
    assert not is_whitelisted("EXEC_CODE")
    assert not is_whitelisted("")


# ═══════════════════════════════════════════
# intent 往返 + action→msg_type
# ═══════════════════════════════════════════
def _full_intent() -> OrderIntent:
    return OrderIntent(
        client_order_id="KQ-lead-1m-42-abcd",
        account="crypto-lead",
        tag="1m",
        symbol="BTCUSDT",
        side="BUY",
        offset="OPEN",
        qty=Decimal("0.123"),
        kind="LIMIT",
        price=Decimal("60000.50"),
        stop_price=Decimal("59000.00"),
        sl=Decimal("58000.00"),
        tp=Decimal("65000.00"),
        tif="GTC",
        action=ACTION_ORDER,
        ts=1_700_000_000_000,
        lead_balance=Decimal("10000.25"),
    )


def test_intent_roundtrip_all_fields():
    intent = _full_intent()
    msg = intent_to_message(intent, TOKEN)
    assert msg.msg_type == DIST_INTENT

    obj, reason = parse_message(msg, TOKEN)
    assert reason is None
    assert isinstance(obj, OrderIntent)
    assert obj.client_order_id == intent.client_order_id
    assert obj.account == intent.account
    assert obj.tag == intent.tag
    assert obj.symbol == intent.symbol
    assert obj.side == intent.side
    assert obj.offset == intent.offset
    assert obj.qty == intent.qty
    assert obj.kind == intent.kind
    assert obj.price == intent.price
    assert obj.stop_price == intent.stop_price
    assert obj.sl == intent.sl
    assert obj.tp == intent.tp
    assert obj.tif == intent.tif
    assert obj.action == intent.action
    assert obj.ts == intent.ts
    assert obj.lead_balance == intent.lead_balance


def test_action_to_msg_type_mapping():
    assert intent_to_message(
        OrderIntent(client_order_id="c", action=ACTION_ORDER), TOKEN).msg_type == DIST_INTENT
    assert intent_to_message(
        OrderIntent(client_order_id="c", action=ACTION_CANCEL), TOKEN).msg_type == DIST_CANCEL
    assert intent_to_message(
        OrderIntent(client_order_id="c", action=ACTION_FLATTEN), TOKEN).msg_type == DIST_FLATTEN


def test_cancel_intent_roundtrip():
    intent = OrderIntent(client_order_id="cancel-1", tag="1m", symbol="BTCUSDT",
                         action=ACTION_CANCEL)
    obj, reason = parse_message(intent_to_message(intent, TOKEN), TOKEN)
    assert reason is None
    assert obj.action == ACTION_CANCEL
    assert obj.symbol == "BTCUSDT"


def test_decimal_fidelity_through_msgpack():
    """完整 serialize→deserialize→parse 链路：Decimal 精确保真（无浮点漂移）。"""
    intent = _full_intent()
    wire = serialize_message(intent_to_message(intent, TOKEN))
    restored = deserialize_message(wire)
    obj, reason = parse_message(restored, TOKEN)
    assert reason is None
    assert obj.qty == Decimal("0.123")
    assert obj.price == Decimal("60000.50")
    assert obj.sl == Decimal("58000.00")
    assert obj.tp == Decimal("65000.00")
    assert obj.lead_balance == Decimal("10000.25")


def test_optional_decimals_none_preserved():
    intent = OrderIntent(client_order_id="mkt-1", symbol="ETHUSDT", side="BUY",
                         offset="OPEN", qty=Decimal("1"), kind="MARKET")
    obj, _ = parse_message(intent_to_message(intent, TOKEN), TOKEN)
    assert obj.price is None
    assert obj.stop_price is None
    assert obj.sl is None
    assert obj.tp is None
    assert obj.lead_balance is None


# ═══════════════════════════════════════════
# report 往返
# ═══════════════════════════════════════════
def test_report_roundtrip():
    report = Report(
        client_order_id="KQ-lead-1m-42-abcd",
        follower="crypto-follower-1",
        status="FILLED",
        filled_qty=Decimal("0.0615"),
        filled_price=Decimal("60001.25"),
        reason="",
        ts=1_700_000_000_500,
    )
    msg = report_to_message(report, TOKEN)
    assert msg.msg_type == DIST_REPORT
    obj, reason = parse_message(msg, TOKEN)
    assert reason is None
    assert isinstance(obj, Report)
    assert obj.client_order_id == report.client_order_id
    assert obj.follower == report.follower
    assert obj.status == "FILLED"
    assert obj.filled_qty == report.filled_qty
    assert obj.filled_price == report.filled_price
    assert obj.ts == report.ts


# ═══════════════════════════════════════════
# heartbeat
# ═══════════════════════════════════════════
def test_heartbeat_roundtrip():
    msg = heartbeat_message(TOKEN)
    obj, reason = parse_message(msg, TOKEN)
    assert reason is None
    assert isinstance(obj, dict)
    assert obj["heartbeat"] is True


# ═══════════════════════════════════════════
# 信任边界：token / 白名单
# ═══════════════════════════════════════════
def test_token_mismatch_rejected():
    msg = intent_to_message(_full_intent(), TOKEN)
    obj, reason = parse_message(msg, "wrong-token")
    assert obj is None
    assert reason == "token mismatch"


def test_non_whitelist_msg_type_rejected():
    """非白名单 msg_type（即便 token 正确）→ 拒绝，绝不执行。"""
    evil = Message(msg_type="EXEC_STRATEGY", source="attacker",
                   payload={"token": TOKEN, "code": "<malicious-strategy-code>"})
    obj, reason = parse_message(evil, TOKEN)
    assert obj is None
    assert "non-whitelist" in reason


def test_non_whitelist_before_token_check():
    """白名单校验先于 token：非白名单类型即使 token 错也报 non-whitelist。"""
    evil = Message(msg_type="KLINE", source="x", payload={"token": "bad"})
    obj, reason = parse_message(evil, TOKEN)
    assert obj is None
    assert "non-whitelist" in reason


def test_empty_payload_token_rejected():
    msg = Message(msg_type=DIST_INTENT, source="lead", payload={"client_order_id": "c1"})
    obj, reason = parse_message(msg, TOKEN)
    assert obj is None
    assert reason == "token mismatch"


# ═══════════════════════════════════════════
# topic 常量（broadcaster / agent 共用）
# ═══════════════════════════════════════════
def test_topic_constants_distinct():
    from strategy.sdk.dist_protocol import TOPIC_HEARTBEAT
    assert len({TOPIC_INTENT, TOPIC_HEARTBEAT, TOPIC_REPORT}) == 3

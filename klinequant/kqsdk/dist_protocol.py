"""Phase 3 信号分发协议与信任边界（单一真相源，broadcaster / order_agent 共用）。

设计约束（对齐《SDK 阶段实施规划 v1.3》Phase 3 + 用户 2026-10-08 定案）：
  - **策略代码永不外流**：分发通道只承载白名单指令（order/cancel/flatten/heartbeat/
    执行回报），Agent 端不含任何策略模块；非白名单 ``msg_type`` / token 不匹配的
    消息一律丢弃 + 告警，绝不执行、绝不下发策略代码或参数。
  - **seq 与 intent 身份同源**：幂等主键直接复用 Phase R WAL 生成的 lead
    ``client_order_id``（已内嵌 ``next_seq`` 序列、全局唯一、重启续号），故分发侧
    **不再单独消费 next_seq**（避免与 OrderIdFactory 双递增）。
  - 传输复用 ``protocol.transport.zmq_transport``（PUB/SUB）+ ``protocol.codec``
    （msgpack）；本模块只定义应用层白名单 schema 与 token 校验。

通道拓扑（一对多分发 + 多对一回报）：
  intent/heartbeat：lead PUB **bind** → followers SUB **connect**（标准扇出）。
  report：lead SUB **bind** → followers PUB **connect**（反向扇入，见 zmq bind 选项）。
"""
from __future__ import annotations

import hmac
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from protocol.messages import Message

__all__ = [
    # topics
    "TOPIC_INTENT",
    "TOPIC_HEARTBEAT",
    "TOPIC_REPORT",
    # msg types
    "DIST_INTENT",
    "DIST_CANCEL",
    "DIST_FLATTEN",
    "DIST_HEARTBEAT",
    "DIST_REPORT",
    "WHITELIST",
    "is_whitelisted",
    # actions
    "ACTION_ORDER",
    "ACTION_CANCEL",
    "ACTION_FLATTEN",
    # endpoints
    "DEFAULT_INTENT_ENDPOINT",
    "DEFAULT_REPORT_ENDPOINT",
    "parse_endpoint",
    # payload
    "OrderIntent",
    "Report",
    "intent_to_message",
    "report_to_message",
    "heartbeat_message",
    "parse_message",
]

# ─── ZMQ topics ───
TOPIC_INTENT = "kq.intent"
TOPIC_HEARTBEAT = "kq.heartbeat"
TOPIC_REPORT = "kq.report"

# ─── 白名单 msg_type（信任边界：只有这些类型会被接受）───
DIST_INTENT = "DIST_INTENT"        # lead→follower：下单意图
DIST_CANCEL = "DIST_CANCEL"        # lead→follower：撤挂单
DIST_FLATTEN = "DIST_FLATTEN"      # lead→follower：一键清仓
DIST_HEARTBEAT = "DIST_HEARTBEAT"  # lead→follower：心跳
DIST_REPORT = "DIST_REPORT"        # follower→lead：执行回报

# lead→follower 指令集（Agent 只接受这些）
LEAD_TO_FOLLOWER = frozenset({DIST_INTENT, DIST_CANCEL, DIST_FLATTEN, DIST_HEARTBEAT})
# follower→lead 回报集
FOLLOWER_TO_LEAD = frozenset({DIST_REPORT})
WHITELIST = LEAD_TO_FOLLOWER | FOLLOWER_TO_LEAD


def is_whitelisted(msg_type: str) -> bool:
    """msg_type 是否在分发白名单内（非白名单 → 直接丢弃 + 告警）。"""
    return msg_type in WHITELIST


# ─── intent.action 取值 ───
ACTION_ORDER = "order"
ACTION_CANCEL = "cancel"
ACTION_FLATTEN = "flatten"

# action → msg_type
_ACTION_TO_TYPE = {
    ACTION_ORDER: DIST_INTENT,
    ACTION_CANCEL: DIST_CANCEL,
    ACTION_FLATTEN: DIST_FLATTEN,
}

# ─── 默认端点（避开既有引擎端口 5501-5530；可由 account.extra 覆盖）───
DEFAULT_INTENT_ENDPOINT = "tcp://127.0.0.1:5560"
DEFAULT_REPORT_ENDPOINT = "tcp://127.0.0.1:5561"


def parse_endpoint(endpoint: str) -> tuple[str, int]:
    """``tcp://host:port`` → ``(host, port)``（供 ZmqTransport 的 bind_host/port）。"""
    ep = (endpoint or "").strip()
    if "://" in ep:
        _scheme, _, hostport = ep.partition("://")
    else:
        hostport = ep
    host, _, port_s = hostport.rpartition(":")
    if not host:  # 形如 ":5560" 或缺 host
        host = "127.0.0.1"
    return host, int(port_s)


def _d(value: Decimal | None) -> str | None:
    """Decimal → str（None 保持 None），用于 payload 序列化。"""
    return None if value is None else str(value)


def _dec(value: Any, default: Decimal | None = None) -> Decimal | None:
    """payload str → Decimal（None/空/非法回落 default）。"""
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, ArithmeticError):
        return default


# ─────────────────────────────────────────────
# 分发载荷
# ─────────────────────────────────────────────
@dataclass
class OrderIntent:
    """一条 lead 订单意图（白名单指令）。

    ``client_order_id`` 为 lead 侧 WAL 生成的结构化 coid，是 follower 幂等去重主键。
    ``action`` ∈ order/cancel/flatten；cancel/flatten 只关心 symbol/tag。
    ``lead_balance`` 供 balance-ratio 缩放（None → 回落 fixed scale）。
    """

    client_order_id: str
    account: str = ""
    tag: str = ""
    symbol: str = ""
    side: str = ""
    offset: str = ""
    qty: Decimal = Decimal("0")
    kind: str = "MARKET"
    price: Decimal | None = None
    stop_price: Decimal | None = None
    sl: Decimal | None = None
    tp: Decimal | None = None
    tif: str = ""
    action: str = ACTION_ORDER
    ts: int = 0
    lead_balance: Decimal | None = None


@dataclass
class Report:
    """一条 follower 执行回报（回流 lead 聚合）。"""

    client_order_id: str
    follower: str = ""
    status: str = ""        # FILLED / IN_FLIGHT / DEAD / REJECT / DUPLICATE / SKIPPED
    filled_qty: Decimal = Decimal("0")
    filled_price: Decimal | None = None
    reason: str = ""
    ts: int = 0


# ─────────────────────────────────────────────
# Message 互转（token 塞 payload；Decimal 走字符串）
# ─────────────────────────────────────────────
def _now_ms() -> int:
    return int(time.time() * 1000)


def intent_to_message(intent: OrderIntent, token: str, source: str = "lead") -> Message:
    """OrderIntent → Message（msg_type 按 action 映射到白名单类型）。"""
    msg_type = _ACTION_TO_TYPE.get(intent.action, DIST_INTENT)
    payload: dict[str, Any] = {
        "token": token,
        "client_order_id": intent.client_order_id,
        "account": intent.account,
        "tag": intent.tag,
        "symbol": intent.symbol,
        "side": intent.side,
        "offset": intent.offset,
        "qty": _d(intent.qty),
        "kind": intent.kind,
        "price": _d(intent.price),
        "stop_price": _d(intent.stop_price),
        "sl": _d(intent.sl),
        "tp": _d(intent.tp),
        "tif": intent.tif,
        "action": intent.action,
        "ts": intent.ts or _now_ms(),
        "lead_balance": _d(intent.lead_balance),
    }
    return Message(msg_type=msg_type, source=source, payload=payload)


def report_to_message(report: Report, token: str, source: str = "follower") -> Message:
    """Report → Message（DIST_REPORT）。"""
    payload: dict[str, Any] = {
        "token": token,
        "client_order_id": report.client_order_id,
        "follower": report.follower,
        "status": report.status,
        "filled_qty": _d(report.filled_qty),
        "filled_price": _d(report.filled_price),
        "reason": report.reason,
        "ts": report.ts or _now_ms(),
    }
    return Message(msg_type=DIST_REPORT, source=source, payload=payload)


def heartbeat_message(token: str, source: str = "lead") -> Message:
    """构造心跳 Message（DIST_HEARTBEAT，无订单载荷）。"""
    return Message(
        msg_type=DIST_HEARTBEAT, source=source,
        payload={"token": token, "ts": _now_ms()},
    )


def parse_message(
    msg: Message, expected_token: str
) -> tuple[Any | None, str | None]:
    """校验并解析入站 Message（信任边界执行点）。

    Returns:
        ``(obj, None)``：校验通过，obj 为 OrderIntent / Report / dict(心跳)。
        ``(None, reason)``：被拒（非白名单 / token 不匹配 / 载荷非法），reason 供告警。

    token 用 :func:`hmac.compare_digest` 常量时比较，避免时序侧信道。
    """
    if not is_whitelisted(msg.msg_type):
        return None, f"non-whitelist msg_type={msg.msg_type!r}"

    payload = msg.payload or {}
    got = str(payload.get("token", ""))
    if not hmac.compare_digest(got, str(expected_token or "")):
        return None, "token mismatch"

    if msg.msg_type == DIST_HEARTBEAT:
        return {"heartbeat": True, "ts": payload.get("ts", 0)}, None

    if msg.msg_type == DIST_REPORT:
        return Report(
            client_order_id=str(payload.get("client_order_id", "")),
            follower=str(payload.get("follower", "")),
            status=str(payload.get("status", "")),
            filled_qty=_dec(payload.get("filled_qty"), Decimal("0")) or Decimal("0"),
            filled_price=_dec(payload.get("filled_price")),
            reason=str(payload.get("reason", "")),
            ts=int(payload.get("ts", 0) or 0),
        ), None

    # DIST_INTENT / DIST_CANCEL / DIST_FLATTEN → OrderIntent
    action = str(payload.get("action", ACTION_ORDER))
    return OrderIntent(
        client_order_id=str(payload.get("client_order_id", "")),
        account=str(payload.get("account", "")),
        tag=str(payload.get("tag", "")),
        symbol=str(payload.get("symbol", "")),
        side=str(payload.get("side", "")),
        offset=str(payload.get("offset", "")),
        qty=_dec(payload.get("qty"), Decimal("0")) or Decimal("0"),
        kind=str(payload.get("kind", "MARKET")),
        price=_dec(payload.get("price")),
        stop_price=_dec(payload.get("stop_price")),
        sl=_dec(payload.get("sl")),
        tp=_dec(payload.get("tp")),
        tif=str(payload.get("tif", "")),
        action=action,
        ts=int(payload.get("ts", 0) or 0),
        lead_balance=_dec(payload.get("lead_balance")),
    ), None

"""KlineQuant 消息序列化/反序列化

基于 msgpack 的二进制编解码，支持所有核心数据结构的往返序列化。
遵循需求文档 §7.1 的 Message.serialize / Message.deserialize 接口。
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Type

import msgpack

from protocol.messages import Message
from protocol.types import (
    Account,
    IndicatorValue,
    Kline,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    Signal,
    SignalDirection,
    SignalStrength,
    SymbolInfo,
    Tick,
)


# ─────────────────────────────────────────────
# Decimal / Enum 扩展编码器
# ─────────────────────────────────────────────
def _encode_ext(obj: Any) -> Any:
    """msgpack 默认编码器：处理 Decimal 和 Enum"""
    if isinstance(obj, Decimal):
        return {"__decimal__": str(obj)}
    if hasattr(obj, "value"):  # Enum
        return obj.value
    raise TypeError(f"Object of type {type(obj)} is not msgpack serializable")


def _decode_ext(obj: Dict[str, Any]) -> Any:
    """msgpack object_hook：还原 Decimal"""
    if "__decimal__" in obj:
        return Decimal(obj["__decimal__"])
    return obj


# ─────────────────────────────────────────────
# Message 序列化/反序列化
# ─────────────────────────────────────────────
def serialize_message(msg: Message) -> bytes:
    """将 Message 序列化为 msgpack 二进制字节"""
    data = {
        "msg_id": msg.msg_id,
        "msg_type": msg.msg_type,
        "source": msg.source,
        "target": msg.target,
        "timestamp": msg.timestamp,
        "payload": msg.payload,
        "trace_id": msg.trace_id,
        "priority": msg.priority,
    }
    return msgpack.packb(data, default=_encode_ext, use_bin_type=True)


def deserialize_message(data: bytes) -> Message:
    """从 msgpack 二进制字节反序列化为 Message"""
    raw = msgpack.unpackb(data, object_hook=_decode_ext, raw=False)
    return Message(
        msg_id=raw["msg_id"],
        msg_type=raw["msg_type"],
        source=raw["source"],
        target=raw["target"],
        timestamp=raw["timestamp"],
        payload=raw["payload"],
        trace_id=raw["trace_id"],
        priority=raw["priority"],
    )


# ─────────────────────────────────────────────
# 通用数据结构序列化/反序列化
# ─────────────────────────────────────────────
def serialize_obj(obj: Any) -> bytes:
    """将任意 dataclass 实例序列化为 msgpack 二进制"""
    from dataclasses import asdict

    if hasattr(obj, "__dataclass_fields__"):
        data = {"__type__": type(obj).__name__, **asdict(obj)}
    elif isinstance(obj, dict):
        data = obj
    else:
        raise TypeError(f"Cannot serialize {type(obj)}")
    return msgpack.packb(data, default=_encode_ext, use_bin_type=True)


# 类型名 → 类的映射表
_TYPE_REGISTRY: Dict[str, Type] = {
    "Kline": Kline,
    "Tick": Tick,
    "Order": Order,
    "Position": Position,
    "Signal": Signal,
    "Account": Account,
    "IndicatorValue": IndicatorValue,
    "SymbolInfo": SymbolInfo,
}

# 类型名 → {字段名: 枚举类} 的精确映射
_TYPE_ENUM_FIELDS: Dict[str, Dict[str, Type]] = {
    "Order": {
        "side": OrderSide,
        "order_type": OrderType,
        "status": OrderStatus,
    },
    "Signal": {
        "direction": SignalDirection,
        "strength": SignalStrength,
    },
}


def deserialize_obj(data: bytes) -> Any:
    """从 msgpack 二进制反序列化为 dataclass 实例"""
    raw = msgpack.unpackb(data, object_hook=_decode_ext, raw=False)
    type_name = raw.pop("__type__", None)
    if type_name is None:
        return raw

    cls = _TYPE_REGISTRY.get(type_name)
    if cls is None:
        raise ValueError(f"Unknown type: {type_name}")

    # 还原 Enum 字段（仅针对特定类型）
    enum_fields = _TYPE_ENUM_FIELDS.get(type_name, {})
    for field_name, enum_cls in enum_fields.items():
        if field_name in raw:
            raw[field_name] = enum_cls(raw[field_name])
    # 过滤掉 cls 没有的字段
    valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
    filtered = {k: v for k, v in raw.items() if k in valid_fields}

    return cls(**filtered)


def serialize_list(items: list) -> bytes:
    """序列化 dataclass 列表"""
    from dataclasses import asdict

    data = []
    for obj in items:
        if hasattr(obj, "__dataclass_fields__"):
            data.append({"__type__": type(obj).__name__, **asdict(obj)})
        else:
            data.append(obj)
    return msgpack.packb(data, default=_encode_ext, use_bin_type=True)


def deserialize_list(data: bytes) -> list:
    """反序列化为 dataclass 列表"""
    raw_list = msgpack.unpackb(data, object_hook=_decode_ext, raw=False)
    result = []
    for raw in raw_list:
        type_name = raw.pop("__type__", None)
        if type_name is None:
            result.append(raw)
            continue
        cls = _TYPE_REGISTRY.get(type_name)
        if cls is None:
            result.append(raw)
            continue
        enum_fields = _TYPE_ENUM_FIELDS.get(type_name, {})
        for field_name, enum_cls in enum_fields.items():
            if field_name in raw:
                raw[field_name] = enum_cls(raw[field_name])
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in raw.items() if k in valid_fields}
        result.append(cls(**filtered))
    return result

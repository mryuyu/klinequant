"""protocol 包单元测试

覆盖 P-T-001 ~ P-T-010 全部测试用例。
"""
from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

# 确保项目根目录在 sys.path 中
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

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
from protocol.messages import Message, MessageType, validate_message_route
from protocol.codec import (
    serialize_message,
    deserialize_message,
    serialize_obj,
    deserialize_obj,
    serialize_list,
    deserialize_list,
)


# ═══════════════════════════════════════════════
# P-T-001: Kline 字段校验
# ═══════════════════════════════════════════════
class TestKline:
    def _make_kline(self, **overrides) -> Kline:
        defaults = dict(
            symbol="BTC-USDT",
            exchange="binance",
            timeframe="1h",
            timestamp=1700000000000,
            open=Decimal("42000"),
            high=Decimal("42500"),
            low=Decimal("41800"),
            close=Decimal("42300"),
            volume=Decimal("1234.56"),
            quote_volume=Decimal("51800000"),
            trade_count=5000,
            is_closed=True,
        )
        defaults.update(overrides)
        return Kline(**defaults)

    def test_valid_kline(self):
        k = self._make_kline()
        assert k.symbol == "BTC-USDT"
        assert k.is_closed is True

    def test_high_must_gte_open_close(self):
        with pytest.raises(ValueError, match="high"):
            self._make_kline(high=Decimal("41900"))  # high < close(42300)

    def test_low_must_lte_open_close(self):
        with pytest.raises(ValueError, match="low"):
            self._make_kline(low=Decimal("42100"))  # low > open(42000)

    def test_volume_must_non_negative(self):
        with pytest.raises(ValueError, match="volume"):
            self._make_kline(volume=Decimal("-1"))

    def test_frozen(self):
        k = self._make_kline()
        with pytest.raises(AttributeError):
            k.close = Decimal("99999")  # type: ignore


# ═══════════════════════════════════════════════
# P-T-002: Tick 字段校验
# ═══════════════════════════════════════════════
class TestTick:
    def test_valid_tick(self):
        t = Tick(
            symbol="BTC-USDT",
            exchange="binance",
            timestamp=1700000000000,
            last_price=Decimal("42300"),
            bid_price=Decimal("42299"),
            bid_qty=Decimal("1.5"),
            ask_price=Decimal("42301"),
            ask_qty=Decimal("0.8"),
            volume_24h=Decimal("50000"),
        )
        assert t.symbol == "BTC-USDT"
        assert t.ask_price > t.bid_price

    def test_frozen(self):
        t = Tick(
            symbol="BTC-USDT", exchange="binance", timestamp=0,
            last_price=Decimal("0"), bid_price=Decimal("0"),
            bid_qty=Decimal("0"), ask_price=Decimal("0"),
            ask_qty=Decimal("0"), volume_24h=Decimal("0"),
        )
        with pytest.raises(AttributeError):
            t.last_price = Decimal("1")  # type: ignore


# ═══════════════════════════════════════════════
# P-T-003: Order 状态流转
# ═══════════════════════════════════════════════
class TestOrder:
    def _make_order(self, status: OrderStatus = OrderStatus.PENDING) -> Order:
        return Order(
            order_id="test-001",
            symbol="BTC-USDT",
            exchange="binance",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.01"),
            status=status,
            price=Decimal("42000"),
        )

    def test_valid_transition_pending_to_submitted(self):
        o = self._make_order()
        o.transition_to(OrderStatus.SUBMITTED)
        assert o.status == OrderStatus.SUBMITTED

    def test_valid_transition_submitted_to_filled(self):
        o = self._make_order(OrderStatus.SUBMITTED)
        o.transition_to(OrderStatus.FILLED)
        assert o.status == OrderStatus.FILLED

    def test_valid_transition_submitted_to_partial(self):
        o = self._make_order(OrderStatus.SUBMITTED)
        o.transition_to(OrderStatus.PARTIAL_FILLED)
        assert o.status == OrderStatus.PARTIAL_FILLED

    def test_valid_transition_partial_to_canceling(self):
        o = self._make_order(OrderStatus.PARTIAL_FILLED)
        o.transition_to(OrderStatus.CANCELING)
        assert o.status == OrderStatus.CANCELING

    def test_invalid_transition_filled_to_submitted(self):
        o = self._make_order(OrderStatus.FILLED)
        with pytest.raises(ValueError, match="Invalid transition"):
            o.transition_to(OrderStatus.SUBMITTED)

    def test_invalid_transition_pending_to_filled(self):
        o = self._make_order()
        with pytest.raises(ValueError, match="Invalid transition"):
            o.transition_to(OrderStatus.FILLED)

    def test_can_transition_to(self):
        o = self._make_order()
        assert o.can_transition_to(OrderStatus.SUBMITTED) is True
        assert o.can_transition_to(OrderStatus.FILLED) is False

    def test_pending_to_failed(self):
        o = self._make_order()
        o.transition_to(OrderStatus.FAILED)
        assert o.status == OrderStatus.FAILED


# ═══════════════════════════════════════════════
# P-T-004: Position 字段校验
# ═══════════════════════════════════════════════
class TestPosition:
    def test_valid_position(self):
        p = Position(
            symbol="BTC-USDT",
            exchange="binance",
            side="LONG",
            quantity=Decimal("1.5"),
            avg_entry_price=Decimal("42000"),
            unrealized_pnl=Decimal("500"),
        )
        assert p.side == "LONG"
        assert p.quantity == Decimal("1.5")

    def test_flat_position(self):
        p = Position(
            symbol="BTC-USDT",
            exchange="binance",
            side="FLAT",
            quantity=Decimal("0"),
            avg_entry_price=Decimal("0"),
        )
        assert p.side == "FLAT"
        assert p.quantity == Decimal("0")


# ═══════════════════════════════════════════════
# P-T-005: Signal 字段校验 + 过期判断
# ═══════════════════════════════════════════════
class TestSignal:
    def _make_signal(self, **overrides) -> Signal:
        defaults = dict(
            signal_id="sig-001",
            strategy_id="str-001",
            symbol="BTC-USDT",
            direction=SignalDirection.LONG,
            strength=SignalStrength.STRONG,
            price=Decimal("42000"),
            reason="MA7 crossed above MA25",
            timestamp=1700000000000,
            expires_at=1700003600000,
        )
        defaults.update(overrides)
        return Signal(**defaults)

    def test_valid_signal(self):
        s = self._make_signal()
        assert s.direction == SignalDirection.LONG
        assert s.strength == SignalStrength.STRONG

    def test_not_expired(self):
        s = self._make_signal()
        assert s.is_expired(1700001000000) is False

    def test_expired(self):
        s = self._make_signal()
        assert s.is_expired(1700004000000) is True

    def test_no_expiry(self):
        s = self._make_signal(expires_at=0)
        assert s.is_expired(9999999999999) is False


# ═══════════════════════════════════════════════
# P-T-006: Account 余额字段
# ═══════════════════════════════════════════════
class TestAccount:
    def test_valid_account(self):
        a = Account(
            exchange="binance",
            account_type="SPOT",
            total_balance=Decimal("100000"),
            available_balance=Decimal("80000"),
            frozen_balance=Decimal("20000"),
        )
        assert a.total_balance == Decimal("100000")
        assert a.available_balance + a.frozen_balance == a.total_balance


# ═══════════════════════════════════════════════
# P-T-007: IndicatorValue 嵌套 dict
# ═══════════════════════════════════════════════
class TestIndicatorValue:
    def test_single_value(self):
        iv = IndicatorValue(
            indicator_name="MA",
            symbol="BTC-USDT",
            timeframe="1h",
            timestamp=1700000000000,
            values={"MA": Decimal("42000.5")},
            params={"period": 14},
        )
        assert iv.values["MA"] == Decimal("42000.5")

    def test_nested_value(self):
        iv = IndicatorValue(
            indicator_name="MACD",
            symbol="BTC-USDT",
            timeframe="1h",
            timestamp=1700000000000,
            values={"DIF": Decimal("100"), "DEA": Decimal("80"), "HIST": Decimal("20")},
            params={"fast": 12, "slow": 26, "signal": 9},
        )
        assert iv.values["HIST"] == Decimal("20")
        assert iv.params["fast"] == 12


# ═══════════════════════════════════════════════
# P-T-008: SymbolInfo 精度字段校验
# ═══════════════════════════════════════════════
class TestSymbolInfo:
    def _make_symbol_info(self, **overrides) -> SymbolInfo:
        defaults = dict(
            symbol="BTC-USDT",
            exchange="binance",
            base_currency="BTC",
            quote_currency="USDT",
            price_precision=2,
            qty_precision=6,
            min_qty=Decimal("0.00001"),
            min_notional=Decimal("10"),
            tick_size=Decimal("0.01"),
        )
        defaults.update(overrides)
        return SymbolInfo(**defaults)

    def test_valid_symbol_info(self):
        si = self._make_symbol_info()
        assert si.price_precision == 2
        assert si.min_qty == Decimal("0.00001")

    def test_negative_price_precision(self):
        with pytest.raises(ValueError, match="price_precision"):
            self._make_symbol_info(price_precision=-1)

    def test_negative_qty_precision(self):
        with pytest.raises(ValueError, match="qty_precision"):
            self._make_symbol_info(qty_precision=-1)

    def test_zero_min_qty(self):
        with pytest.raises(ValueError, match="min_qty"):
            self._make_symbol_info(min_qty=Decimal("0"))


# ═══════════════════════════════════════════════
# P-T-009: Message 序列化/反序列化往返一致性
# ═══════════════════════════════════════════════
class TestMessage:
    def test_message_creation(self):
        msg = Message(
            msg_type=MessageType.KLINE_UPDATE,
            source="market_engine",
            payload={"symbol": "BTC-USDT"},
        )
        assert msg.msg_id != ""
        assert msg.trace_id != ""
        assert msg.timestamp > 0
        assert msg.priority == 5

    def test_invalid_priority(self):
        with pytest.raises(ValueError, match="priority"):
            Message(
                msg_type="TEST",
                source="test",
                payload={},
                priority=10,
            )

    def test_serialize_deserialize_roundtrip(self):
        msg = Message(
            msg_type=MessageType.KLINE_UPDATE,
            source="market_engine",
            target="indicator_engine",
            payload={"symbol": "BTC-USDT", "close": "42000.50"},
            priority=7,
        )
        data = serialize_message(msg)
        assert isinstance(data, bytes)

        restored = deserialize_message(data)
        assert restored.msg_id == msg.msg_id
        assert restored.msg_type == msg.msg_type
        assert restored.source == msg.source
        assert restored.target == msg.target
        assert restored.priority == 7
        assert restored.payload["symbol"] == "BTC-USDT"

    def test_message_with_decimal_payload(self):
        msg = Message(
            msg_type=MessageType.ORDER_SUBMIT,
            source="trade_engine",
            payload={"price": Decimal("42000.50"), "qty": Decimal("0.01")},
        )
        data = serialize_message(msg)
        restored = deserialize_message(data)
        assert restored.payload["price"] == Decimal("42000.50")


# ═══════════════════════════════════════════════
# P-T-009 续: 消息路由验证
# ═══════════════════════════════════════════════
class TestMessageRoute:
    def test_valid_route(self):
        assert validate_message_route(
            MessageType.KLINE_UPDATE, "market_engine", "indicator_engine"
        ) is True

    def test_broadcast_route(self):
        assert validate_message_route(
            MessageType.KLINE_UPDATE, "market_engine", "*"
        ) is True

    def test_invalid_source(self):
        assert validate_message_route(
            MessageType.KLINE_UPDATE, "trade_engine", "indicator_engine"
        ) is False

    def test_invalid_target(self):
        assert validate_message_route(
            MessageType.RISK_CHECK, "trade_engine", "gateway"
        ) is False

    def test_unknown_type(self):
        assert validate_message_route(
            "UNKNOWN_TYPE", "any", "any"
        ) is False


# ═══════════════════════════════════════════════
# P-T-010: 数据结构序列化兼容性
# ═══════════════════════════════════════════════
class TestCodec:
    def test_kline_roundtrip(self):
        k = Kline(
            symbol="BTC-USDT", exchange="binance", timeframe="1h",
            timestamp=1700000000000,
            open=Decimal("42000"), high=Decimal("42500"),
            low=Decimal("41800"), close=Decimal("42300"),
            volume=Decimal("1234.56"), quote_volume=Decimal("51800000"),
            trade_count=5000, is_closed=True,
        )
        data = serialize_obj(k)
        restored = deserialize_obj(data)
        assert isinstance(restored, Kline)
        assert restored.symbol == k.symbol
        assert restored.close == Decimal("42300")
        assert restored.is_closed is True

    def test_order_roundtrip(self):
        o = Order(
            order_id="ord-001", symbol="BTC-USDT", exchange="binance",
            side=OrderSide.BUY, order_type=OrderType.LIMIT,
            quantity=Decimal("0.01"), status=OrderStatus.SUBMITTED,
            price=Decimal("42000"), client_order_id="client-001",
        )
        data = serialize_obj(o)
        restored = deserialize_obj(data)
        assert isinstance(restored, Order)
        assert restored.side == OrderSide.BUY
        assert restored.order_type == OrderType.LIMIT
        assert restored.status == OrderStatus.SUBMITTED
        assert restored.price == Decimal("42000")

    def test_signal_roundtrip(self):
        s = Signal(
            signal_id="sig-001", strategy_id="str-001",
            symbol="BTC-USDT", direction=SignalDirection.LONG,
            strength=SignalStrength.STRONG, price=Decimal("42000"),
            reason="test", timestamp=1700000000000,
            indicators={"MA7": Decimal("42000"), "RSI14": Decimal("72")},
        )
        data = serialize_obj(s)
        restored = deserialize_obj(data)
        assert isinstance(restored, Signal)
        assert restored.direction == SignalDirection.LONG
        assert restored.strength == SignalStrength.STRONG

    def test_position_roundtrip(self):
        p = Position(
            symbol="BTC-USDT", exchange="binance", side="LONG",
            quantity=Decimal("1.5"), avg_entry_price=Decimal("42000"),
            unrealized_pnl=Decimal("500"), leverage=1,
        )
        data = serialize_obj(p)
        restored = deserialize_obj(data)
        assert isinstance(restored, Position)
        assert restored.quantity == Decimal("1.5")

    def test_symbol_info_roundtrip(self):
        si = SymbolInfo(
            symbol="BTC-USDT", exchange="binance",
            base_currency="BTC", quote_currency="USDT",
            price_precision=2, qty_precision=6,
            min_qty=Decimal("0.00001"), min_notional=Decimal("10"),
            tick_size=Decimal("0.01"),
        )
        data = serialize_obj(si)
        restored = deserialize_obj(data)
        assert isinstance(restored, SymbolInfo)
        assert restored.tick_size == Decimal("0.01")

    def test_indicator_value_roundtrip(self):
        iv = IndicatorValue(
            indicator_name="MACD", symbol="BTC-USDT", timeframe="1h",
            timestamp=1700000000000,
            values={"DIF": Decimal("100"), "DEA": Decimal("80")},
            params={"fast": 12, "slow": 26},
        )
        data = serialize_obj(iv)
        restored = deserialize_obj(data)
        assert isinstance(restored, IndicatorValue)
        assert restored.values["DIF"] == Decimal("100")

    def test_list_roundtrip(self):
        klines = [
            Kline(
                symbol="BTC-USDT", exchange="binance", timeframe="1m",
                timestamp=1700000000000 + i * 60000,
                open=Decimal("42000"), high=Decimal("42100"),
                low=Decimal("41900"), close=Decimal("42050"),
                volume=Decimal("10"), quote_volume=Decimal("420000"),
                trade_count=100, is_closed=True,
            )
            for i in range(5)
        ]
        data = serialize_list(klines)
        restored = deserialize_list(data)
        assert len(restored) == 5
        assert all(isinstance(k, Kline) for k in restored)
        assert restored[0].timestamp == 1700000000000
        assert restored[4].timestamp == 1700000240000

    def test_unknown_type_raises(self):
        import msgpack
        raw = msgpack.packb({"__type__": "NonExistent"}, use_bin_type=True)
        with pytest.raises(ValueError, match="Unknown type"):
            deserialize_obj(raw)

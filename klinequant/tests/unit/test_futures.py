"""合约支持单元测试

覆盖 P2-002：
    FUT-T-001: 合约数据结构（FundingRate/FuturesPosition/MarginMode）
    FUT-T-002: BinanceFuturesAdapter 连接/订阅/WS 解析
    FUT-T-003: BinanceFuturesExecutor 下单（做多/做空）
    FUT-T-004: 资金费率监控与计算
    FUT-T-005: 资金费率告警
"""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from protocol.types import (
    FundingRate,
    FundingFee,
    FuturesPosition,
    MarginMode,
    ContractType,
    Order,
    OrderSide,
    OrderType,
    OrderStatus,
)
from core.market_engine.adapters.binance_futures import BinanceFuturesAdapter
from core.trade_engine.executors.binance_futures_executor import BinanceFuturesExecutor
from core.trade_engine.funding_monitor import FundingRateMonitor


# ═══════════════════════════════════════════════
# FUT-T-001: 合约数据结构
# ═══════════════════════════════════════════════

class TestFuturesDataStructures:
    def test_funding_rate(self):
        """资金费率数据结构"""
        fr = FundingRate(
            symbol="BTCUSDT",
            exchange="binance_futures",
            funding_rate=Decimal("0.0001"),
            next_funding_time=1700000000000,
            mark_price=Decimal("50000"),
            index_price=Decimal("49999"),
            timestamp=1700000000000,
        )
        assert fr.rate_percent == Decimal("0.01")
        assert fr.is_positive is True

    def test_funding_rate_negative(self):
        """负费率"""
        fr = FundingRate(
            symbol="ETHUSDT",
            exchange="binance_futures",
            funding_rate=Decimal("-0.0005"),
            next_funding_time=1700000000000,
            mark_price=Decimal("3000"),
            index_price=Decimal("3001"),
            timestamp=1700000000000,
        )
        assert fr.is_positive is False
        assert fr.rate_percent == Decimal("-0.05")

    def test_futures_position_long(self):
        """多头持仓"""
        pos = FuturesPosition(
            symbol="BTCUSDT",
            exchange="binance_futures",
            side="LONG",
            quantity=Decimal("1.5"),
            avg_entry_price=Decimal("50000"),
            mark_price=Decimal("51000"),
            liquidation_price=Decimal("45000"),
            unrealized_pnl=Decimal("1500"),
            margin=Decimal("7500"),
            leverage=10,
            margin_mode=MarginMode.CROSS,
        )
        assert pos.notional_value == Decimal("76500")  # 1.5 * 51000
        assert pos.pnl_ratio == Decimal("0.2")  # 1500 / 7500
        assert pos.is_danger is False

    def test_futures_position_short(self):
        """空头持仓"""
        pos = FuturesPosition(
            symbol="BTCUSDT",
            exchange="binance_futures",
            side="SHORT",
            quantity=Decimal("0.5"),
            avg_entry_price=Decimal("50000"),
            mark_price=Decimal("49000"),
            unrealized_pnl=Decimal("500"),
            margin=Decimal("2500"),
            leverage=10,
        )
        assert pos.side == "SHORT"
        assert pos.unrealized_pnl == Decimal("500")

    def test_futures_position_danger(self):
        """接近强平"""
        pos = FuturesPosition(
            symbol="BTCUSDT",
            exchange="binance_futures",
            side="LONG",
            quantity=Decimal("1"),
            avg_entry_price=Decimal("50000"),
            margin_ratio=Decimal("0.85"),
        )
        assert pos.is_danger is True

    def test_margin_mode_enum(self):
        """保证金模式枚举"""
        assert MarginMode.CROSS.value == "CROSS"
        assert MarginMode.ISOLATED.value == "ISOLATED"

    def test_contract_type_enum(self):
        """合约类型枚举"""
        assert ContractType.PERPETUAL.value == "PERPETUAL"
        assert ContractType.QUARTERLY.value == "QUARTERLY"

    def test_funding_fee(self):
        """资金费结算记录"""
        fee = FundingFee(
            symbol="BTCUSDT",
            exchange="binance_futures",
            funding_rate=Decimal("0.0001"),
            position_qty=Decimal("1.5"),
            fee_amount=Decimal("-0.00015"),
            funding_time=1700000000000,
            strategy_id="dual_ma",
        )
        assert fee.fee_amount < Decimal("0")  # 多头 + 正费率 = 支出


# ═══════════════════════════════════════════════
# FUT-T-002: BinanceFuturesAdapter
# ═══════════════════════════════════════════════

class TestBinanceFuturesAdapter:
    @pytest.mark.asyncio
    async def test_connect_disconnect(self):
        """连接/断开"""
        adapter = BinanceFuturesAdapter(config={})
        assert not adapter.is_connected

        await adapter.connect()
        assert adapter.is_connected
        assert adapter.name == "binance_futures"

        await adapter.disconnect()
        assert not adapter.is_connected

    @pytest.mark.asyncio
    async def test_subscribe_kline(self):
        """订阅 K 线"""
        adapter = BinanceFuturesAdapter(config={})
        await adapter.connect()

        cb = AsyncMock()
        await adapter.subscribe_kline("BTCUSDT", "1m", cb)
        assert "btcusdt@kline_1m" in adapter._subscriptions

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_subscribe_funding_rate(self):
        """订阅资金费率"""
        adapter = BinanceFuturesAdapter(config={})
        await adapter.connect()

        await adapter.subscribe_funding_rate("BTCUSDT")
        assert "btcusdt@markPrice@1s" in adapter._subscriptions

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_kline_ws_message(self):
        """K 线 WS 消息解析"""
        adapter = BinanceFuturesAdapter(config={})
        await adapter.connect()

        received = []
        adapter.register_kline_callback(AsyncMock(side_effect=lambda k: received.append(k)))

        msg = {
            "e": "kline",
            "k": {
                "s": "BTCUSDT", "i": "1m", "x": True,
                "t": 1700000000000, "o": "50000", "h": "50100",
                "l": "49900", "c": "50050", "v": "100",
                "T": 1700000059999, "q": "5000000", "n": 500,
                "V": "60", "Q": "3000000",
            },
        }
        await adapter._handle_ws_message(msg)

        assert len(received) == 1
        assert received[0].is_closed is True
        assert received[0].close == Decimal("50050")

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_mark_price_ws_message(self):
        """标记价格/资金费率 WS 消息"""
        adapter = BinanceFuturesAdapter(config={})
        await adapter.connect()

        rates = []
        adapter._funding_callbacks.append(lambda r: rates.append(r))

        msg = {
            "e": "markPriceUpdate",
            "s": "BTCUSDT",
            "p": "50000.5",
            "i": "50000.2",
            "r": "0.0001",
            "T": 1700028800000,
            "E": 1700000000000,
        }
        await adapter._handle_ws_message(msg)

        assert "BTCUSDT" in adapter._funding_rates
        fr = adapter._funding_rates["BTCUSDT"]
        assert fr.funding_rate == Decimal("0.0001")
        assert fr.mark_price == Decimal("50000.5")
        assert len(rates) == 1

        await adapter.disconnect()


# ═══════════════════════════════════════════════
# FUT-T-003: BinanceFuturesExecutor
# ═══════════════════════════════════════════════

class TestBinanceFuturesExecutor:
    @pytest.mark.asyncio
    async def test_submit_long_order(self):
        """做多下单"""
        executor = BinanceFuturesExecutor(
            api_key="key", api_secret="secret"
        )
        await executor.connect()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "orderId": 12345,
            "clientOrderId": "kq_test",
            "status": "NEW",
            "origQty": "0.5",
            "side": "BUY",
        }

        with patch.object(executor._http, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_resp

            order = Order(
                order_id="ORD-F01",
                client_order_id="CL-F01",
                symbol="BTCUSDT",
                exchange="binance_futures",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("0.5"),
                status=OrderStatus.PENDING,
                strategy_id="test",
                created_at=int(time.time() * 1000),
                updated_at=0,
            )
            result = await executor.submit_order(order)
            assert result.status == OrderStatus.SUBMITTED
            assert result.exchange_order_id == "12345"

        await executor.disconnect()

    @pytest.mark.asyncio
    async def test_submit_short_order(self):
        """做空下单"""
        executor = BinanceFuturesExecutor(
            api_key="key", api_secret="secret"
        )
        await executor.connect()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "orderId": 12346,
            "clientOrderId": "kq_short",
            "status": "NEW",
            "origQty": "1.0",
            "side": "SELL",
        }

        with patch.object(executor._http, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_resp

            order = Order(
                order_id="ORD-F02",
                client_order_id="CL-F02",
                symbol="BTCUSDT",
                exchange="binance_futures",
                side=OrderSide.SELL,  # 做空 = SELL
                order_type=OrderType.MARKET,
                quantity=Decimal("1.0"),
                status=OrderStatus.PENDING,
                strategy_id="test",
                created_at=int(time.time() * 1000),
                updated_at=0,
            )
            result = await executor.submit_order(order)
            assert result.status == OrderStatus.SUBMITTED
            assert result.side == OrderSide.SELL

        await executor.disconnect()

    @pytest.mark.asyncio
    async def test_set_leverage(self):
        """设置杠杆"""
        executor = BinanceFuturesExecutor(
            api_key="key", api_secret="secret"
        )
        await executor.connect()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"leverage": 20, "symbol": "BTCUSDT"}

        with patch.object(executor._http, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_resp
            lev = await executor.set_leverage("BTCUSDT", 20)
            assert lev == 20

        await executor.disconnect()


# ═══════════════════════════════════════════════
# FUT-T-004: 资金费率监控与计算
# ═══════════════════════════════════════════════

class TestFundingRateMonitor:
    def test_calculate_funding_fee_long_positive_rate(self):
        """多头 + 正费率 = 支出"""
        monitor = FundingRateMonitor()
        fee = monitor.calculate_funding_fee(
            "BTCUSDT", Decimal("1.5"), Decimal("0.0001")
        )
        # -(1.5 * 0.0001) = -0.00015
        assert fee == Decimal("-0.00015")

    def test_calculate_funding_fee_short_positive_rate(self):
        """空头 + 正费率 = 收入"""
        monitor = FundingRateMonitor()
        fee = monitor.calculate_funding_fee(
            "BTCUSDT", Decimal("-1.5"), Decimal("0.0001")
        )
        # -(-1.5 * 0.0001) = 0.00015
        assert fee == Decimal("0.00015")

    def test_calculate_funding_fee_negative_rate(self):
        """多头 + 负费率 = 收入"""
        monitor = FundingRateMonitor()
        fee = monitor.calculate_funding_fee(
            "BTCUSDT", Decimal("2.0"), Decimal("-0.0003")
        )
        # -(2.0 * -0.0003) = 0.0006
        assert fee == Decimal("0.0006")

    def test_estimate_daily_cost(self):
        """每日成本估算"""
        monitor = FundingRateMonitor()
        rate = FundingRate(
            symbol="BTCUSDT", exchange="binance_futures",
            funding_rate=Decimal("0.0001"),
            next_funding_time=0, mark_price=Decimal("50000"),
            index_price=Decimal("50000"), timestamp=0,
        )
        monitor.update_rate_manually("BTCUSDT", rate)

        daily = monitor.estimate_daily_cost("BTCUSDT", Decimal("1.0"))
        # 1.0 * 50000 * 0.0001 * 3 = 15
        assert daily == Decimal("15")

    def test_rate_statistics(self):
        """费率统计"""
        monitor = FundingRateMonitor()
        for i in range(25):
            rate = FundingRate(
                symbol="BTCUSDT", exchange="binance_futures",
                funding_rate=Decimal("0.0001") + Decimal(str(i * 0.00001)),
                next_funding_time=0, mark_price=Decimal("50000"),
                index_price=Decimal("50000"), timestamp=i * 1000,
            )
            monitor.update_rate_manually("BTCUSDT", rate)

        stats = monitor.get_rate_statistics("BTCUSDT")
        assert stats["count"] == 25
        assert stats["trend"] == "RISING"
        assert stats["mean"] > Decimal("0")

    def test_record_funding_fee(self):
        """记录结算"""
        monitor = FundingRateMonitor()
        record = monitor.record_funding_fee(
            "BTCUSDT", Decimal("1.0"), Decimal("0.0001"),
            1700000000000, "dual_ma",
        )
        assert record.fee_amount == Decimal("-0.0001")
        assert len(monitor.fee_records) == 1


# ═══════════════════════════════════════════════
# FUT-T-005: 资金费率告警
# ═══════════════════════════════════════════════

class TestFundingRateAlerts:
    def test_high_rate_alert(self):
        """高费率告警"""
        alerts = []
        monitor = FundingRateMonitor()
        monitor.add_alert_callback(lambda a: alerts.append(a))

        rate = FundingRate(
            symbol="BTCUSDT", exchange="binance_futures",
            funding_rate=Decimal("0.0015"),  # 0.15% > 0.1% 阈值
            next_funding_time=0, mark_price=Decimal("50000"),
            index_price=Decimal("50000"), timestamp=0,
        )
        monitor.update_rate_manually("BTCUSDT", rate)

        assert len(alerts) == 1
        assert alerts[0]["level"] == "HIGH"

    def test_extreme_rate_alert(self):
        """极端费率告警"""
        alerts = []
        monitor = FundingRateMonitor()
        monitor.add_alert_callback(lambda a: alerts.append(a))

        rate = FundingRate(
            symbol="ETHUSDT", exchange="binance_futures",
            funding_rate=Decimal("0.005"),  # 0.5% > 0.3% 阈值
            next_funding_time=0, mark_price=Decimal("3000"),
            index_price=Decimal("3000"), timestamp=0,
        )
        monitor.update_rate_manually("ETHUSDT", rate)

        assert len(alerts) == 1
        assert alerts[0]["level"] == "EXTREME"

    def test_normal_rate_no_alert(self):
        """正常费率无告警"""
        alerts = []
        monitor = FundingRateMonitor()
        monitor.add_alert_callback(lambda a: alerts.append(a))

        rate = FundingRate(
            symbol="BTCUSDT", exchange="binance_futures",
            funding_rate=Decimal("0.0001"),  # 0.01% 正常
            next_funding_time=0, mark_price=Decimal("50000"),
            index_price=Decimal("50000"), timestamp=0,
        )
        monitor.update_rate_manually("BTCUSDT", rate)

        assert len(alerts) == 0

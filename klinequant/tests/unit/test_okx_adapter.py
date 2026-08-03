"""OKX 适配器单元测试

覆盖 P2-001：
    OKX-T-001: OKX K 线标准化（REST + WS 格式）
    OKX-T-002: OKX 交易对标准化（BTCUSDT ↔ BTC-USDT）
    OKX-T-003: OKX Tick/Ticker 标准化
    OKX-T-004: OKXAdapter 连接/断开/订阅
    OKX-T-005: OKXAdapter WS 消息解析
    OKX-T-006: OKXExecutor 签名与下单
"""
from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.market_engine.okx_normalizer import (
    TIMEFRAME_TO_OKX,
    OKX_TO_TIMEFRAME,
    normalize_okx_kline,
    normalize_okx_klines,
    normalize_okx_trade,
    normalize_okx_ticker,
    normalize_symbol,
    denormalize_symbol,
    timeframe_to_okx_bar,
    okx_bar_to_timeframe,
)
from core.market_engine.adapters.okx import OKXAdapter
from core.trade_engine.executors.okx_executor import OKXExecutor
from protocol.types import Kline, Tick, Order, OrderSide, OrderType, OrderStatus


# ─── 测试辅助 ───

def _make_okx_kline(
    ts: str = "1700000000000",
    o: str = "50000.0",
    h: str = "50100.0",
    l: str = "49900.0",
    c: str = "50050.0",
    vol: str = "100.5",
    vol_ccy: str = "50.2",
    vol_ccy_quote: str = "5025000.0",
    confirm: str = "1",
) -> List[str]:
    """构造 OKX K 线原始数组"""
    return [ts, o, h, l, c, vol, vol_ccy, vol_ccy_quote, confirm]


# ═══════════════════════════════════════════════
# OKX-T-001: K 线标准化
# ═══════════════════════════════════════════════

class TestOKXKlineNormalization:
    def test_basic_kline(self):
        """基本 K 线转换"""
        raw = _make_okx_kline()
        kline = normalize_okx_kline(raw, "BTCUSDT", "1m")

        assert kline.symbol == "BTCUSDT"
        assert kline.exchange == "okx"
        assert kline.timeframe == "1m"
        assert kline.open == Decimal("50000.0")
        assert kline.high == Decimal("50100.0")
        assert kline.low == Decimal("49900.0")
        assert kline.close == Decimal("50050.0")
        assert kline.volume == Decimal("100.5")
        assert kline.quote_volume == Decimal("5025000.0")
        assert kline.is_closed is True
        assert kline.trade_count == 0  # OKX 不提供

    def test_unclosed_kline(self):
        """未收盘 K 线 (confirm=0)"""
        raw = _make_okx_kline(confirm="0")
        kline = normalize_okx_kline(raw, "ETHUSDT", "5m")
        assert kline.is_closed is False

    def test_timestamp_alignment(self):
        """时间戳对齐到周期边界"""
        # 1700000030000 → 对齐到 1m → 1700000020000? 不对
        # 1700000000000 已经对齐到 1m
        raw = _make_okx_kline(ts="1700000030000")
        kline = normalize_okx_kline(raw, "BTCUSDT", "1m")
        # 对齐到 60000 边界: 1700000030000 // 60000 * 60000 = 1700000020000? 
        # 1700000030000 / 60000 = 28333333.83... → floor = 28333333 * 60000 = 1699999980000
        assert kline.timestamp == (1700000030000 // 60000) * 60000

    def test_batch_klines_reversed(self):
        """批量 K 线：OKX 降序 → 升序"""
        raw_list = [
            _make_okx_kline(ts="1700000120000", c="50200", h="50300"),
            _make_okx_kline(ts="1700000060000", c="50100", h="50200"),
            _make_okx_kline(ts="1700000000000", c="50000", h="50100"),
        ]
        klines = normalize_okx_klines(raw_list, "BTCUSDT", "1m")
        assert len(klines) == 3
        # 应该是升序
        assert klines[0].close == Decimal("50000")
        assert klines[1].close == Decimal("50100")
        assert klines[2].close == Decimal("50200")

    def test_invalid_kline_skipped(self):
        """无效 K 线被跳过"""
        raw_list = [
            _make_okx_kline(ts="1700000000000"),
            ["invalid"],  # 太短
            _make_okx_kline(ts="1700000060000"),
        ]
        klines = normalize_okx_klines(raw_list, "BTCUSDT", "1m")
        assert len(klines) == 2


# ═══════════════════════════════════════════════
# OKX-T-002: 交易对标准化
# ═══════════════════════════════════════════════

class TestOKXSymbolNormalization:
    def test_btcusdt_to_okx(self):
        """BTCUSDT → BTC-USDT"""
        assert normalize_symbol("BTCUSDT") == "BTC-USDT"

    def test_ethusdt_to_okx(self):
        """ETHUSDT → ETH-USDT"""
        assert normalize_symbol("ETHUSDT") == "ETH-USDT"

    def test_already_formatted(self):
        """已含 '-' 的格式原样返回"""
        assert normalize_symbol("BTC-USDT") == "BTC-USDT"

    def test_usdc_pair(self):
        """USDC 交易对"""
        assert normalize_symbol("BTCUSDC") == "BTC-USDC"

    def test_denormalize(self):
        """BTC-USDT → BTCUSDT"""
        assert denormalize_symbol("BTC-USDT") == "BTCUSDT"
        assert denormalize_symbol("ETH-USDC") == "ETHUSDC"

    def test_timeframe_mapping(self):
        """周期映射正确性"""
        assert timeframe_to_okx_bar("1m") == "1m"
        assert timeframe_to_okx_bar("1h") == "1H"
        assert timeframe_to_okx_bar("1d") == "1D"
        assert timeframe_to_okx_bar("4h") == "4H"

    def test_timeframe_reverse(self):
        """反向周期映射"""
        assert okx_bar_to_timeframe("1m") == "1m"
        assert okx_bar_to_timeframe("1H") == "1h"
        assert okx_bar_to_timeframe("1D") == "1d"
        assert okx_bar_to_timeframe("1Dutc") == "1d"

    def test_invalid_timeframe(self):
        """无效周期抛异常"""
        with pytest.raises(ValueError):
            timeframe_to_okx_bar("7m")


# ═══════════════════════════════════════════════
# OKX-T-003: Tick/Ticker 标准化
# ═══════════════════════════════════════════════

class TestOKXTickNormalization:
    def test_trade_normalization(self):
        """逐笔成交标准化"""
        data = {
            "instId": "BTC-USDT",
            "tradeId": "12345",
            "px": "50000.5",
            "sz": "0.01",
            "side": "buy",
            "ts": "1700000000000",
        }
        tick = normalize_okx_trade(data)
        assert tick.symbol == "BTCUSDT"
        assert tick.exchange == "okx"
        assert tick.last_price == Decimal("50000.5")
        assert tick.timestamp == 1700000000000
        assert tick.bid_qty == Decimal("0.01")  # buy → bid
        assert tick.ask_qty == Decimal("0")

    def test_trade_sell_side(self):
        """卖出成交"""
        data = {
            "instId": "ETH-USDT",
            "px": "3000",
            "sz": "1.5",
            "side": "sell",
            "ts": "1700000000000",
        }
        tick = normalize_okx_trade(data)
        assert tick.ask_qty == Decimal("1.5")  # sell → ask
        assert tick.bid_qty == Decimal("0")

    def test_ticker_normalization(self):
        """Ticker 标准化"""
        data = {
            "instId": "BTC-USDT",
            "last": "50000",
            "lastSz": "0.1",
            "askPx": "50001",
            "askSz": "2.0",
            "bidPx": "49999",
            "bidSz": "3.0",
            "vol24h": "10000",
            "ts": "1700000000000",
        }
        tick = normalize_okx_ticker(data)
        assert tick.symbol == "BTCUSDT"
        assert tick.last_price == Decimal("50000")
        assert tick.bid_price == Decimal("49999")
        assert tick.ask_price == Decimal("50001")
        assert tick.volume_24h == Decimal("10000")


# ═══════════════════════════════════════════════
# OKX-T-004: OKXAdapter 连接/断开/订阅
# ═══════════════════════════════════════════════

class TestOKXAdapter:
    @pytest.mark.asyncio
    async def test_connect_disconnect(self):
        """连接/断开"""
        adapter = OKXAdapter(config={})
        assert not adapter.is_connected

        await adapter.connect()
        assert adapter.is_connected
        assert adapter.name == "okx"

        await adapter.disconnect()
        assert not adapter.is_connected

    @pytest.mark.asyncio
    async def test_subscribe_kline(self):
        """订阅 K 线"""
        adapter = OKXAdapter(config={})
        await adapter.connect()

        callback = AsyncMock()
        await adapter.subscribe_kline("BTCUSDT", "1m", callback)

        # 验证订阅参数
        assert len(adapter._sub_args) == 1
        assert adapter._sub_args[0] == {"channel": "candle1m", "instId": "BTC-USDT"}

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_subscribe_tick(self):
        """订阅成交"""
        adapter = OKXAdapter(config={})
        await adapter.connect()

        callback = AsyncMock()
        await adapter.subscribe_tick("ETHUSDT", callback)

        assert {"channel": "trades", "instId": "ETH-USDT"} in adapter._sub_args

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_multiple_subscriptions(self):
        """多品种订阅"""
        adapter = OKXAdapter(config={})
        await adapter.connect()

        cb = AsyncMock()
        await adapter.subscribe_kline("BTCUSDT", "1m", cb)
        await adapter.subscribe_kline("ETHUSDT", "5m", cb)
        await adapter.subscribe_kline("BTCUSDT", "1h", cb)

        assert len(adapter._sub_args) == 3

        await adapter.disconnect()


# ═══════════════════════════════════════════════
# OKX-T-005: WS 消息解析
# ═══════════════════════════════════════════════

class TestOKXWSMessageParsing:
    @pytest.mark.asyncio
    async def test_candle_message(self):
        """K 线 WS 消息解析"""
        adapter = OKXAdapter(config={})
        await adapter.connect()

        received: List[Kline] = []
        adapter.register_kline_callback(
            AsyncMock(side_effect=lambda k: received.append(k))
        )

        # 模拟 OKX WS 推送
        msg = {
            "arg": {"channel": "candle1m", "instId": "BTC-USDT"},
            "action": "update",
            "data": [
                ["1700000000000", "50000", "50100", "49900", "50050",
                 "100", "50", "5000000", "1"]
            ],
        }
        await adapter._handle_ws_message(msg)

        assert len(received) == 1
        assert received[0].symbol == "BTCUSDT"
        assert received[0].close == Decimal("50050")
        assert received[0].is_closed is True

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_trades_message(self):
        """成交 WS 消息解析"""
        adapter = OKXAdapter(config={})
        await adapter.connect()

        received: List[Tick] = []
        adapter.register_tick_callback(
            AsyncMock(side_effect=lambda t: received.append(t))
        )

        msg = {
            "arg": {"channel": "trades", "instId": "ETH-USDT"},
            "data": [
                {"instId": "ETH-USDT", "tradeId": "1", "px": "3000",
                 "sz": "1.5", "side": "buy", "ts": "1700000000000"}
            ],
        }
        await adapter._handle_ws_message(msg)

        assert len(received) == 1
        assert received[0].symbol == "ETHUSDT"
        assert received[0].last_price == Decimal("3000")

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_subscribe_event(self):
        """订阅确认事件不报错"""
        adapter = OKXAdapter(config={})
        await adapter.connect()

        msg = {
            "event": "subscribe",
            "arg": {"channel": "candle1m", "instId": "BTC-USDT"},
        }
        # 不应抛异常
        await adapter._handle_ws_message(msg)

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_error_event(self):
        """错误事件不崩溃"""
        adapter = OKXAdapter(config={})
        await adapter.connect()

        msg = {
            "event": "error",
            "code": "30040",
            "msg": "Channel does not exist",
        }
        await adapter._handle_ws_message(msg)

        await adapter.disconnect()


# ═══════════════════════════════════════════════
# OKX-T-006: OKXExecutor 签名与下单
# ═══════════════════════════════════════════════

class TestOKXExecutor:
    def test_signature_generation(self):
        """HMAC SHA256 签名生成"""
        executor = OKXExecutor(
            api_key="test_key",
            api_secret="test_secret",
            passphrase="test_pass",
        )
        sig = executor._sign("2024-01-01T00:00:00.000Z", "GET", "/api/v5/account/balance")
        # 签名应该是 base64 字符串
        assert isinstance(sig, str)
        assert len(sig) > 0

    def test_headers_generation(self):
        """请求头生成"""
        executor = OKXExecutor(
            api_key="my_key",
            api_secret="my_secret",
            passphrase="my_pass",
        )
        headers = executor._headers("GET", "/api/v5/account/balance")
        assert headers["OK-ACCESS-KEY"] == "my_key"
        assert headers["OK-ACCESS-PASSPHRASE"] == "my_pass"
        assert "OK-ACCESS-SIGN" in headers
        assert "OK-ACCESS-TIMESTAMP" in headers
        assert headers["Content-Type"] == "application/json"

    @pytest.mark.asyncio
    async def test_submit_order_success(self):
        """下单成功"""
        executor = OKXExecutor(
            api_key="key", api_secret="secret", passphrase="pass"
        )
        await executor.connect()

        # Mock HTTP 响应
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "code": "0",
            "data": [{"ordId": "12345", "sCode": "0", "sMsg": ""}],
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(executor._http, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response

            order = Order(
                order_id="ORD-001",
                client_order_id="CL-001",
                symbol="BTCUSDT",
                exchange="okx",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("0.01"),
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
    async def test_submit_order_rejected(self):
        """下单被拒绝"""
        executor = OKXExecutor(
            api_key="key", api_secret="secret", passphrase="pass"
        )
        await executor.connect()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "code": "1",
            "msg": "Insufficient balance",
            "data": [{"sCode": "51008", "sMsg": "Insufficient balance"}],
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(executor._http, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response

            order = Order(
                order_id="ORD-002",
                client_order_id="CL-002",
                symbol="BTCUSDT",
                exchange="okx",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("100"),
                price=Decimal("50000"),
                status=OrderStatus.PENDING,
                strategy_id="test",
                created_at=int(time.time() * 1000),
                updated_at=0,
            )
            result = await executor.submit_order(order)

            assert result.status == OrderStatus.REJECTED
            assert "Insufficient" in (result.cancel_reason or "")

        await executor.disconnect()

    @pytest.mark.asyncio
    async def test_query_account(self):
        """查询账户"""
        executor = OKXExecutor(
            api_key="key", api_secret="secret", passphrase="pass"
        )
        await executor.connect()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "code": "0",
            "data": [{
                "totalEq": "10000",
                "details": [
                    {"ccy": "USDT", "availBal": "8000", "frozenBal": "2000"},
                ],
            }],
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(executor._http, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response

            account = await executor.query_account()
            assert account.total_balance == Decimal("10000")
            assert account.available_balance == Decimal("8000")
            assert account.frozen_balance == Decimal("2000")
            assert account.exchange == "okx"

        await executor.disconnect()

    @pytest.mark.asyncio
    async def test_query_positions(self):
        """查询持仓"""
        executor = OKXExecutor(
            api_key="key", api_secret="secret", passphrase="pass",
            td_mode="cross",
        )
        await executor.connect()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "code": "0",
            "data": [{
                "instId": "BTC-USDT",
                "posSide": "net",
                "pos": "0.5",
                "avgPx": "50000",
                "upl": "100",
                "realizedPnl": "50",
                "margin": "5000",
                "lever": "10",
            }],
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(executor._http, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response

            positions = await executor.query_positions()
            assert "BTCUSDT" in positions
            pos = positions["BTCUSDT"]
            assert pos.side == "LONG"
            assert pos.quantity == Decimal("0.5")
            assert pos.leverage == 10
            assert pos.unrealized_pnl == Decimal("100")

        await executor.disconnect()

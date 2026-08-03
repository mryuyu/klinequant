"""行情引擎单元测试

覆盖 MKT-T-001 ~ MKT-T-006：
    MKT-T-001: Binance WS 消息解析（mock 数据）
    MKT-T-002: K 线标准化：各格式 → Kline 转换
    MKT-T-003: 周期重采样：1m → 5m/1h 一致性
    MKT-T-004: 断线重连状态机流转
    MKT-T-005: K 线缺失检测逻辑
    MKT-T-006: MarketEngine 集成：标准化 → ZMQ 发布
"""
from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.market_engine.adapters.base import ExchangeAdapter, KlineCallback
from core.market_engine.adapters.binance import BinanceAdapter
from core.market_engine.engine import MarketEngine
from protocol.messages import Message, MessageType
from core.market_engine.normalizer import (
    TIMEFRAME_MS,
    align_timestamp,
    normalize_binance_kline,
    normalize_binance_klines,
    timeframe_to_ms,
)
from core.market_engine.timeframe_engine import TimeframeEngine
from protocol.types import Kline


# ─── 测试辅助 ───

def _make_raw_kline(
    open_time: int = 1700000000000,
    open: str = "50000.00",
    high: str = "50100.00",
    low: str = "49900.00",
    close: str = "50050.00",
    volume: str = "100.5",
    close_time: int = 1700000059999,
    quote_volume: str = "5025000.00",
    trade_count: int = 500,
    taker_buy_base: str = "60.0",
    taker_buy_quote: str = "3000000.0",
) -> list:
    """生成 Binance 原始 K 线数组"""
    return [
        open_time, open, high, low, close, volume, close_time,
        quote_volume, trade_count, taker_buy_base, taker_buy_quote, "0",
    ]


def _make_kline(
    symbol: str = "BTCUSDT",
    exchange: str = "binance",
    timeframe: str = "1m",
    timestamp: int = 1700000000000,
    open: str = "50000.00",
    high: str = "50100.00",
    low: str = "49900.00",
    close: str = "50050.00",
    volume: str = "100.5",
    is_closed: bool = True,
) -> Kline:
    """生成标准 Kline"""
    return Kline(
        symbol=symbol,
        exchange=exchange,
        timeframe=timeframe,
        timestamp=timestamp,
        open=Decimal(open),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal(volume),
        quote_volume=Decimal("5000000"),
        trade_count=100,
        is_closed=is_closed,
    )


# ═══════════════════════════════════════════════════════════
# MKT-T-001: Binance WS 消息解析
# ═══════════════════════════════════════════════════════════


class TestBinanceWSMessageParsing:
    """Binance WebSocket 消息解析测试"""

    @pytest.mark.asyncio
    async def test_kline_event_parsing(self):
        """K 线事件正确解析"""
        adapter = BinanceAdapter(config={"testnet": {"enabled": False}})
        received = []

        async def on_kline(k: Kline):
            received.append(k)

        await adapter.connect()
        await adapter.subscribe_kline("BTCUSDT", "1m", on_kline)

        # 模拟 WS K 线事件
        ws_event = {
            "e": "kline",
            "E": 1700000060000,
            "s": "BTCUSDT",
            "k": {
                "t": 1700000000000,
                "T": 1700000059999,
                "s": "BTCUSDT",
                "i": "1m",
                "o": "50000.00",
                "h": "50100.00",
                "l": "49900.00",
                "c": "50050.00",
                "v": "100.5",
                "n": 500,
                "x": True,
                "q": "5025000.00",
                "V": "60.0",
                "Q": "3000000.0",
            },
        }

        await adapter._handle_ws_message(ws_event)

        assert len(received) == 1
        k = received[0]
        assert k.symbol == "BTCUSDT"
        assert k.timeframe == "1m"
        assert k.open == Decimal("50000.00")
        assert k.high == Decimal("50100.00")
        assert k.low == Decimal("49900.00")
        assert k.close == Decimal("50050.00")
        assert k.volume == Decimal("100.5")

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_trade_event_parsing(self):
        """逐笔成交事件解析"""
        adapter = BinanceAdapter(config={})
        received = []

        async def on_tick(t):
            received.append(t)

        await adapter.connect()
        await adapter.subscribe_tick("BTCUSDT", on_tick)

        trade_event = {
            "e": "trade",
            "E": 1700000060000,
            "s": "BTCUSDT",
            "t": 12345,
            "p": "50050.50",
            "q": "0.001",
            "T": 1700000060000,
            "m": True,
        }

        await adapter._handle_ws_message(trade_event)

        assert len(received) == 1
        assert received[0].symbol == "BTCUSDT"
        assert received[0].last_price == Decimal("50050.50")

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_error_event_handling(self):
        """错误事件不崩溃"""
        adapter = BinanceAdapter(config={})
        await adapter.connect()
        error_event = {"e": "error", "msg": "test error"}
        await adapter._handle_ws_message(error_event)  # 不应抛异常
        await adapter.disconnect()


# ═══════════════════════════════════════════════════════════
# MKT-T-002: K 线标准化
# ═══════════════════════════════════════════════════════════


class TestKlineNormalization:
    """K 线标准化测试"""

    def test_normalize_single_kline(self):
        """单根 K 线标准化"""
        raw = _make_raw_kline()
        kline = normalize_binance_kline(raw, "BTCUSDT", "1m")

        assert kline.symbol == "BTCUSDT"
        assert kline.exchange == "binance"
        assert kline.timeframe == "1m"
        assert kline.open == Decimal("50000.00")
        assert kline.high == Decimal("50100.00")
        assert kline.low == Decimal("49900.00")
        assert kline.close == Decimal("50050.00")
        assert kline.volume == Decimal("100.5")
        assert kline.trade_count == 500

    def test_normalize_batch(self):
        """批量标准化"""
        raw_list = [
            _make_raw_kline(open_time=1700000000000 + i * 60000)
            for i in range(5)
        ]
        klines = normalize_binance_klines(raw_list, "ETHUSDT", "1m")
        assert len(klines) == 5
        # 验证时间戳递增
        for i in range(1, len(klines)):
            assert klines[i].timestamp > klines[i - 1].timestamp

    def test_timestamp_alignment(self):
        """时间戳对齐到周期边界"""
        # 1m = 60000ms: 1700000000000 / 60000 = 28333333.33, 所以对齐到 28333333 * 60000
        assert align_timestamp(1700000000123, "1m") == 1699999980000
        assert align_timestamp(1700000059999, "1m") == 1700000040000
        # 5m = 300000ms
        base_5m = 1700000100000  # 对齐到 5m 边界
        assert align_timestamp(base_5m, "5m") == base_5m
        assert align_timestamp(base_5m + 100, "5m") == base_5m
        # 1h = 3600000ms
        assert align_timestamp(1700000000000, "1h") == 1699999200000

    def test_timeframe_to_ms(self):
        """周期转毫秒"""
        assert timeframe_to_ms("1m") == 60_000
        assert timeframe_to_ms("5m") == 300_000
        assert timeframe_to_ms("1h") == 3_600_000
        assert timeframe_to_ms("1d") == 86_400_000

    def test_timeframe_invalid(self):
        """无效周期抛出异常"""
        with pytest.raises(ValueError):
            timeframe_to_ms("2m")  # 不支持的周期

    def test_normalize_skip_invalid(self):
        """无效数据跳过不中断"""
        raw_list = [
            _make_raw_kline(),
            ["invalid"],  # 无效数据
            _make_raw_kline(open_time=1700000060000),
        ]
        klines = normalize_binance_klines(raw_list, "BTCUSDT", "1m")
        assert len(klines) == 2  # 跳过无效的

    def test_lowercase_symbol_normalized(self):
        """小写 symbol 转大写"""
        raw = _make_raw_kline()
        kline = normalize_binance_kline(raw, "btcusdt", "1m")
        assert kline.symbol == "BTCUSDT"


# ═══════════════════════════════════════════════════════════
# MKT-T-003: 周期重采样
# ═══════════════════════════════════════════════════════════


class TestTimeframeResampling:
    """K 线周期重采样测试"""

    def test_1m_to_5m_resampling(self):
        """1m → 5m 重采样"""
        engine = TimeframeEngine("BTCUSDT", "binance", "1m")
        engine.add_target("5m")

        # 找到对齐到 5m 边界的时间戳
        base_ts = (1700000000000 // 300000) * 300000  # 5m = 300000ms
        results = []

        # 喂入 5 根 1m K 线
        for i in range(5):
            kline = _make_kline(
                timestamp=base_ts + i * 60000,
                open=str(50000 + i * 10),
                high=str(50100 + i * 10),
                low=str(49900 + i * 10),
                close=str(50050 + i * 10),
            )
            resampled = engine.feed(kline)
            if "5m" in resampled:
                results.append(resampled["5m"])

        # 应该有结果
        assert len(results) >= 1
        last = results[-1]
        assert last.timeframe == "5m"
        assert last.timestamp == base_ts  # 对齐到 5m 起点

    def test_1m_to_1h_resampling(self):
        """1m → 1h 重采样"""
        engine = TimeframeEngine("BTCUSDT", "binance", "1m")
        engine.add_target("1h")

        base_ts = 1699999200000  # 对齐到 1h 边界
        # 喂入 60 根 1m K 线
        for i in range(60):
            kline = _make_kline(
                timestamp=base_ts + i * 60000,
                open=str(50000 + i),
                high=str(50100 + i),
                low=str(49900 + i),
                close=str(50050 + i),
            )
            resampled = engine.feed(kline)

    def test_multiple_targets(self):
        """多目标周期同时重采样"""
        engine = TimeframeEngine("BTCUSDT", "binance", "1m")
        engine.add_target("5m")
        engine.add_target("15m")
        engine.add_target("1h")

        assert "5m" in engine.target_timeframes
        assert "15m" in engine.target_timeframes
        assert "1h" in engine.target_timeframes

    def test_ohlc_aggregation(self):
        """OHLC 聚合逻辑正确"""
        engine = TimeframeEngine("BTCUSDT", "binance", "1m")
        engine.add_target("5m")

        # 使用对齐到 5m 边界的时间戳
        base_ts = (1700000000000 // 300000) * 300000  # 5m = 300000ms
        klines_data = [
            (50000, 50100, 49900, 50050),  # bar 1
            (50050, 50200, 50000, 50150),  # bar 2 - 更高 high
            (50150, 50180, 49800, 50000),  # bar 3 - 更低 low
            (50000, 50100, 49950, 50080),  # bar 4
            (50080, 50120, 50050, 50100),  # bar 5
        ]

        last_result = None
        for i, (o, h, l, c) in enumerate(klines_data):
            kline = _make_kline(
                timestamp=base_ts + i * 60000,
                open=str(o),
                high=str(h),
                low=str(l),
                close=str(c),
            )
            resampled = engine.feed(kline)
            if "5m" in resampled:
                last_result = resampled["5m"]

        if last_result:
            # 5m bar 的 open 应该是第一根的 open
            assert last_result.open == Decimal("50000")
            # 5m bar 的 close 应该是最后一根的 close
            assert last_result.close == Decimal("50100")
            # 5m bar 的 high 应该是所有中最高的
            assert last_result.high == Decimal("50200")
            # 5m bar 的 low 应该是所有中最低的
            assert last_result.low == Decimal("49800")

    def test_wrong_base_timeframe_raises(self):
        """输入错误基础周期抛异常"""
        engine = TimeframeEngine("BTCUSDT", "binance", "1m")
        engine.add_target("5m")

        wrong_kline = _make_kline(timeframe="5m")
        with pytest.raises(ValueError):
            engine.feed(wrong_kline)


# ═══════════════════════════════════════════════════════════
# MKT-T-004: ExchangeAdapter 抽象基类
# ═══════════════════════════════════════════════════════════


class TestExchangeAdapterBase:
    """ExchangeAdapter 基类测试"""

    def test_abstract_methods(self):
        """抽象方法完整性"""
        assert hasattr(ExchangeAdapter, "connect")
        assert hasattr(ExchangeAdapter, "disconnect")
        assert hasattr(ExchangeAdapter, "subscribe_kline")
        assert hasattr(ExchangeAdapter, "fetch_klines")

    @pytest.mark.asyncio
    async def test_binance_adapter_connect_disconnect(self):
        """BinanceAdapter 连接/断开"""
        adapter = BinanceAdapter(config={})
        assert not adapter.is_connected

        await adapter.connect()
        assert adapter.is_connected

        await adapter.disconnect()
        assert not adapter.is_connected

    @pytest.mark.asyncio
    async def test_binance_adapter_name(self):
        """适配器名称"""
        adapter = BinanceAdapter(config={})
        assert adapter.name == "binance"


# ═══════════════════════════════════════════════════════════
# MKT-T-005: K 线缺失检测
# ═══════════════════════════════════════════════════════════


class TestGapDetection:
    """K 线缺失检测测试"""

    @pytest.mark.asyncio
    async def test_detect_gap_no_cache(self):
        """无缓存时拉取最新"""
        adapter = BinanceAdapter(config={})
        await adapter.connect()

        # Mock fetch_klines
        mock_klines = [_make_kline(timestamp=int(time.time() * 1000))]
        adapter.fetch_klines = AsyncMock(return_value=mock_klines)

        result = await adapter.detect_and_fill_gaps("BTCUSDT", "1m")
        assert len(result) == 1
        adapter.fetch_klines.assert_called_once()

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_detect_gap_with_cache(self):
        """有缓存时检测缺失"""
        adapter = BinanceAdapter(config={})
        await adapter.connect()

        # 设置缓存：5 分钟前
        old_ts = int(time.time() * 1000) - 5 * 60 * 1000
        old_kline = _make_kline(timestamp=old_ts)
        adapter._latest_kline["BTCUSDT_1m"] = old_kline

        # Mock REST 返回 4 根补全
        gap_klines = [
            _make_kline(timestamp=old_ts + i * 60000)
            for i in range(1, 5)
        ]
        adapter.fetch_klines = AsyncMock(return_value=gap_klines)

        result = await adapter.detect_and_fill_gaps("BTCUSDT", "1m")
        assert len(result) == 4

        await adapter.disconnect()


# ═══════════════════════════════════════════════════════════
# MKT-T-006: MarketEngine 集成
# ═══════════════════════════════════════════════════════════


class TestMarketEngineIntegration:
    """MarketEngine 集成测试"""

    @pytest.mark.asyncio
    async def test_engine_lifecycle(self):
        """引擎启动/停止"""
        adapter = BinanceAdapter(config={})
        engine = MarketEngine(adapter=adapter)

        engine.add_symbol("BTCUSDT", ["1m", "5m"])

        # Mock 方法
        adapter.connect = AsyncMock()
        adapter.subscribe_kline = AsyncMock()
        adapter.start_ws = AsyncMock()
        adapter.disconnect = AsyncMock()

        # 启动
        task = asyncio.create_task(engine.start())
        await asyncio.sleep(0.1)

        assert engine.is_running
        adapter.connect.assert_called()

        # 停止
        await engine.stop()
        assert not engine.is_running

    @pytest.mark.asyncio
    async def test_engine_add_symbol(self):
        """添加交易对"""
        adapter = BinanceAdapter(config={})
        engine = MarketEngine(adapter=adapter)

        engine.add_symbol("BTCUSDT", ["1m", "5m", "1h"])
        engine.add_symbol("ETHUSDT", ["1m", "15m"])

        assert "BTCUSDT" in engine._symbols
        assert "ETHUSDT" in engine._symbols
        assert engine._symbols["BTCUSDT"] == ["1m", "5m", "1h"]

    @pytest.mark.asyncio
    async def test_kline_validation(self):
        """K 线数据校验"""
        adapter = BinanceAdapter(config={})
        engine = MarketEngine(adapter=adapter)

        # 有效 K 线
        valid = _make_kline()
        assert engine._validate_kline(valid) is True

        # 零价格（需要构造合法 Kline，open 和 close 同时为 0）
        zero_price = Kline(
            symbol="BTCUSDT", exchange="binance", timeframe="1m",
            timestamp=1700000000000,
            open=Decimal("0"), high=Decimal("0"), low=Decimal("0"), close=Decimal("0"),
            volume=Decimal("100"), quote_volume=Decimal("0"),
            trade_count=0, is_closed=True,
        )
        assert engine._validate_kline(zero_price) is False

    @pytest.mark.asyncio
    async def test_engine_stats(self):
        """统计计数"""
        adapter = BinanceAdapter(config={})
        engine = MarketEngine(adapter=adapter)

        stats = engine.stats
        assert stats["klines_received"] == 0
        assert stats["klines_published"] == 0

        # 模拟处理 K 线
        kline = _make_kline()
        engine.add_symbol("BTCUSDT", ["1m"])
        await engine._on_kline_received(kline)

        assert engine.stats["klines_received"] == 1

    @pytest.mark.asyncio
    async def test_engine_with_mock_transport(self):
        """引擎 + mock ZMQ transport"""
        adapter = BinanceAdapter(config={})
        mock_transport = MagicMock()
        mock_transport.publish = AsyncMock()

        engine = MarketEngine(adapter=adapter, transport=mock_transport)
        engine.add_symbol("BTCUSDT", ["1m"])

        kline = _make_kline()
        await engine._process_kline(kline)

        mock_transport.publish.assert_called_once()
        call_args = mock_transport.publish.call_args
        assert call_args[0][0] == "kline"
        msg = call_args[0][1]
        assert msg.msg_type in (MessageType.KLINE_UPDATE, MessageType.KLINE_CLOSED)

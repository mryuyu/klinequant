"""MarketEngine — 行情引擎主循环

整合所有组件：
    - ExchangeAdapter（数据源）
    - TimeframeEngine（周期重采样）
    - ZMQ Publisher（行情广播）
    - KlineRepository（持久化）
    - RedisCacheManager（快照缓存）
    - GracefulShutdown（优雅停机）

遵循需求文档 §4.1 MKT-010~MKT-015。
"""
from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

from config.graceful_shutdown import GracefulShutdown
from core.market_engine.adapters.base import ExchangeAdapter
from core.market_engine.adapters.binance import BinanceAdapter
from core.market_engine.normalizer import TIMEFRAME_MS, timeframe_to_ms
from core.market_engine.timeframe_engine import TimeframeEngine
from protocol.messages import Message, MessageType
from protocol.types import Kline, Tick

logger = logging.getLogger(__name__)


class MarketEngine:
    """行情引擎

    用法：
        engine = MarketEngine(adapter=binance_adapter)
        engine.add_symbol("BTCUSDT", ["1m", "5m", "1h"])
        await engine.start()
    """

    def __init__(
        self,
        adapter: ExchangeAdapter,
        transport: Optional[Any] = None,
        kline_repo: Optional[Any] = None,
        redis_cache: Optional[Any] = None,
    ):
        """
        Args:
            adapter: 交易所适配器
            transport: ZMQ Transport（可选，用于 PUB 广播）
            kline_repo: KlineRepository（可选，用于持久化）
            redis_cache: RedisCacheManager（可选，用于快照缓存）
        """
        self._adapter = adapter
        self._transport = transport
        self._kline_repo = kline_repo
        self._redis = redis_cache

        self._running = False
        self._symbols: Dict[str, List[str]] = {}  # symbol → [intervals]
        self._tf_engines: Dict[str, TimeframeEngine] = {}  # symbol → TimeframeEngine

        # 数据校验状态
        self._prev_kline: Dict[str, Kline] = {}  # "symbol_interval" → last Kline

        # 统计
        self._stats = {
            "klines_received": 0,
            "klines_published": 0,
            "klines_stored": 0,
            "ticks_received": 0,
            "errors": 0,
        }

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def stats(self) -> Dict[str, int]:
        return self._stats.copy()

    # ─── 配置 ───

    def add_symbol(self, symbol: str, intervals: List[str]) -> None:
        """添加交易对和订阅周期。

        Args:
            symbol: 交易对，如 "BTCUSDT"
            intervals: K 线周期列表，如 ["1m", "5m", "1h"]
        """
        self._symbols[symbol.upper()] = intervals

        # 创建周期重采样引擎
        tf_engine = TimeframeEngine(symbol.upper(), self._adapter.name)
        for interval in intervals:
            if interval != "1m":
                tf_engine.add_target(interval)
        self._tf_engines[symbol.upper()] = tf_engine

    # ─── 启动/停止 ───

    async def start(self) -> None:
        """启动行情引擎"""
        if self._running:
            return

        logger.info("MarketEngine starting...")
        self._running = True

        # 1. 连接交易所
        await self._adapter.connect()

        # 2. 启动时检查并补全缺失 K 线
        await self._initial_gap_fill()

        # 3. 订阅所有交易对
        await self._subscribe_all()

        # 4. 启动 WebSocket
        await self._adapter.start_ws()

        logger.info(
            f"MarketEngine started: {len(self._symbols)} symbols, "
            f"adapter={self._adapter.name}"
        )

    async def stop(self) -> None:
        """停止行情引擎"""
        self._running = False
        await self._adapter.disconnect()
        logger.info("MarketEngine stopped")

    async def run_forever(self) -> None:
        """主循环：运行直到收到停机信号"""
        await self.start()

        try:
            while self._running:
                await asyncio.sleep(1)
                # 定期输出统计
                if self._stats["klines_received"] % 100 == 0 and self._stats["klines_received"] > 0:
                    logger.info(f"MarketEngine stats: {self._stats}")
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    # ─── 内部流程 ───

    async def _subscribe_all(self) -> None:
        """订阅所有交易对"""
        for symbol, intervals in self._symbols.items():
            for interval in intervals:
                await self._adapter.subscribe_kline(
                    symbol, interval, self._on_kline_received
                )

    async def _initial_gap_fill(self) -> None:
        """启动时检查并补全缺失 K 线"""
        if not self._kline_repo:
            return

        for symbol, intervals in self._symbols.items():
            for interval in intervals:
                try:
                    # 查询本地最新 K 线
                    latest = await self._kline_repo.get_latest(symbol, self._adapter.name, interval)

                    if latest:
                        # 检测缺失
                        expected_interval = timeframe_to_ms(interval)
                        now_ms = int(time.time() * 1000)
                        missing_start = latest.timestamp + expected_interval

                        if missing_start < now_ms:
                            logger.info(
                                f"Gap detected: {symbol} {interval} "
                                f"from {latest.timestamp}, filling..."
                            )
                            klines = await self._adapter.fetch_klines(
                                symbol, interval,
                                start_time=missing_start,
                                limit=1000,
                            )
                            if klines:
                                await self._kline_repo.save_batch(klines)
                                self._stats["klines_stored"] += len(klines)
                                logger.info(
                                    f"Gap filled: {symbol} {interval} "
                                    f"+{len(klines)} klines"
                                )
                    else:
                        # 本地无数据，拉取初始数据
                        logger.info(
                            f"No local data for {symbol} {interval}, "
                            f"fetching initial klines..."
                        )
                        klines = await self._adapter.fetch_klines(
                            symbol, interval, limit=500
                        )
                        if klines:
                            await self._kline_repo.save_batch(klines)
                            self._stats["klines_stored"] += len(klines)
                            logger.info(
                                f"Initial data: {symbol} {interval} "
                                f"+{len(klines)} klines"
                            )

                except Exception as e:
                    logger.error(f"Gap fill error for {symbol} {interval}: {e}")
                    self._stats["errors"] += 1

    # ─── 数据处理回调 ───

    async def _on_kline_received(self, kline: Kline) -> None:
        """K 线接收回调"""
        self._stats["klines_received"] += 1

        # 数据校验
        if not self._validate_kline(kline):
            return

        # 周期重采样
        symbol = kline.symbol
        tf_engine = self._tf_engines.get(symbol)
        if tf_engine and kline.timeframe == "1m":
            resampled = tf_engine.feed(kline)
            for tf, resampled_kline in resampled.items():
                await self._process_kline(resampled_kline)

        # 处理原始 K 线
        await self._process_kline(kline)

    async def _process_kline(self, kline: Kline) -> None:
        """处理单根 K 线（发布 + 持久化 + 缓存）"""
        # ZMQ 发布
        if self._transport:
            try:
                msg = Message(
                    msg_type=MessageType.KLINE_CLOSED if kline.is_closed else MessageType.KLINE_UPDATE,
                    source="market_engine",
                    payload={
                        "symbol": kline.symbol,
                        "exchange": kline.exchange,
                        "timeframe": kline.timeframe,
                        "timestamp": kline.timestamp,
                        "open": str(kline.open),
                        "high": str(kline.high),
                        "low": str(kline.low),
                        "close": str(kline.close),
                        "volume": str(kline.volume),
                        "is_closed": kline.is_closed,
                    },
                )
                await self._transport.publish("kline", msg)
                self._stats["klines_published"] += 1
            except Exception as e:
                logger.error(f"ZMQ publish error: {e}")
                self._stats["errors"] += 1

        # 收盘时写入 DuckDB
        if kline.is_closed and self._kline_repo:
            try:
                await self._kline_repo.save(kline)
                self._stats["klines_stored"] += 1
            except Exception as e:
                logger.error(f"DB write error: {e}")
                self._stats["errors"] += 1

        # 更新 Redis 快照
        if self._redis:
            try:
                cache_key = f"kline:{kline.symbol}:{kline.exchange}:{kline.timeframe}"
                await self._redis.set(
                    cache_key,
                    {
                        "timestamp": kline.timestamp,
                        "open": str(kline.open),
                        "high": str(kline.high),
                        "low": str(kline.low),
                        "close": str(kline.close),
                        "volume": str(kline.volume),
                    },
                    ttl=120,
                )
            except Exception as e:
                logger.debug(f"Redis cache error: {e}")

    # ─── 数据校验 ───

    def _validate_kline(self, kline: Kline) -> bool:
        """校验 K 线数据完整性"""
        # 价格非零
        if kline.close <= 0 or kline.open <= 0:
            logger.warning(f"Zero price kline: {kline.symbol} {kline.timeframe}")
            return False

        # OHLC 约束
        if kline.high < max(kline.open, kline.close):
            logger.warning(f"Invalid high: {kline.symbol} {kline.timeframe}")
            return False
        if kline.low > min(kline.open, kline.close):
            logger.warning(f"Invalid low: {kline.symbol} {kline.timeframe}")
            return False

        # 时间戳跳跃检测
        cache_key = f"{kline.symbol}_{kline.timeframe}"
        prev = self._prev_kline.get(cache_key)
        if prev is not None:
            expected_interval = timeframe_to_ms(kline.timeframe)
            actual_gap = kline.timestamp - prev.timestamp
            if actual_gap > expected_interval * 2:
                logger.warning(
                    f"Timestamp gap: {kline.symbol} {kline.timeframe} "
                    f"expected {expected_interval}ms, got {actual_gap}ms"
                )
                # 不丢弃，但记录警告

        self._prev_kline[cache_key] = kline
        return True

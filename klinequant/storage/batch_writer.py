"""BatchWriter — DuckDB 批量写入缓冲

K线/Tick/指标等高吞吐场景下，将写入请求缓冲到内存，
达到阈值后一次性 executemany 写入，降低 IO 次数。

支持两种刷新触发：
    1. 缓冲区大小阈值（默认 500 行）
    2. 时间间隔（默认 5 秒）
    3. 手动 flush()

遵循技术文档 §3.2.3 DuckDB 写入性能优化。
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, List, Optional

from storage.duckdb_manager import DuckDBManager

logger = logging.getLogger(__name__)


class BatchWriter:
    """DuckDB 批量写入缓冲器。

    用法：
        writer = BatchWriter(
            table="klines",
            columns=["symbol", "exchange", "timeframe", "timestamp", ...],
            buffer_size=500,
            flush_interval=5.0,
        )
        await writer.start()

        # 写入行
        await writer.write([
            "BTCUSDT", "binance", "1m", 1690000000000, ...
        ])

        # 手动刷新
        await writer.flush()

        # 关闭（会刷新剩余数据）
        await writer.stop()
    """

    def __init__(
        self,
        table: str,
        columns: List[str],
        manager: Optional[DuckDBManager] = None,
        buffer_size: int = 500,
        flush_interval: float = 5.0,
    ):
        self._table = table
        self._columns = columns
        self._manager = manager or DuckDBManager.instance()
        self._buffer_size = buffer_size
        self._flush_interval = flush_interval

        self._buffer: List[List[Any]] = []
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False
        self._total_written = 0

        # 构建 INSERT SQL
        placeholders = ", ".join(["?"] * len(columns))
        col_names = ", ".join(columns)
        self._insert_sql = (
            f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"
        )

    @property
    def table(self) -> str:
        return self._table

    @property
    def buffer_count(self) -> int:
        return len(self._buffer)

    @property
    def total_written(self) -> int:
        return self._total_written

    async def start(self) -> None:
        """启动定时刷新任务"""
        self._running = True
        self._flush_task = asyncio.create_task(self._periodic_flush())
        logger.info(
            f"BatchWriter started: {self._table} "
            f"(buffer={self._buffer_size}, interval={self._flush_interval}s)"
        )

    async def stop(self) -> None:
        """停止并刷新剩余数据"""
        self._running = False
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        # 最终刷新
        await self.flush()
        logger.info(f"BatchWriter stopped: {self._table}, total_written={self._total_written}")

    async def write(self, row: List[Any]) -> None:
        """写入一行数据。缓冲区满时自动刷新。"""
        self._buffer.append(row)
        if len(self._buffer) >= self._buffer_size:
            await self.flush()

    async def write_batch(self, rows: List[List[Any]]) -> None:
        """批量写入多行。缓冲区满时自动刷新。"""
        self._buffer.extend(rows)
        if len(self._buffer) >= self._buffer_size:
            await self.flush()

    async def flush(self) -> None:
        """立即刷新缓冲区到 DuckDB"""
        if not self._buffer:
            return

        batch = self._buffer.copy()
        self._buffer.clear()

        try:
            await self._manager.executemany(self._insert_sql, batch)
            self._total_written += len(batch)
            logger.debug(f"BatchWriter flushed {len(batch)} rows to {self._table}")
        except Exception:
            logger.exception(
                f"BatchWriter flush failed: {len(batch)} rows to {self._table}"
            )
            # 写回缓冲区（重试）
            self._buffer = batch + self._buffer

    async def _periodic_flush(self) -> None:
        """定时刷新任务"""
        while self._running:
            try:
                await asyncio.sleep(self._flush_interval)
                if self._buffer:
                    await self.flush()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("BatchWriter periodic flush error")


# ─────────────────────────────────────────────
# 预定义 Writer 工厂函数
# ─────────────────────────────────────────────

_KLINE_COLUMNS = [
    "symbol", "exchange", "timeframe", "timestamp",
    "open", "high", "low", "close",
    "volume", "quote_volume", "trade_count", "is_closed",
]

_TICK_COLUMNS = [
    "symbol", "exchange", "timestamp",
    "last_price", "bid_price", "bid_qty",
    "ask_price", "ask_qty", "volume_24h",
]

_INDICATOR_COLUMNS = [
    "symbol", "exchange", "timeframe", "timestamp",
    "indicator", "values",
]


def create_kline_writer(
    manager: Optional[DuckDBManager] = None,
    buffer_size: int = 200,
    flush_interval: float = 3.0,
) -> BatchWriter:
    """创建 K 线批量写入器"""
    return BatchWriter(
        table="klines",
        columns=_KLINE_COLUMNS,
        manager=manager,
        buffer_size=buffer_size,
        flush_interval=flush_interval,
    )


def create_tick_writer(
    manager: Optional[DuckDBManager] = None,
    buffer_size: int = 1000,
    flush_interval: float = 2.0,
) -> BatchWriter:
    """创建 Tick 批量写入器"""
    return BatchWriter(
        table="ticks",
        columns=_TICK_COLUMNS,
        manager=manager,
        buffer_size=buffer_size,
        flush_interval=flush_interval,
    )


def create_indicator_writer(
    manager: Optional[DuckDBManager] = None,
    buffer_size: int = 500,
    flush_interval: float = 5.0,
) -> BatchWriter:
    """创建指标值批量写入器"""
    return BatchWriter(
        table="indicator_values",
        columns=_INDICATOR_COLUMNS,
        manager=manager,
        buffer_size=buffer_size,
        flush_interval=flush_interval,
    )

"""TickRepository — Tick 数据 CRUD"""
from __future__ import annotations

from decimal import Decimal
from typing import List

from protocol.types import Tick
from storage.repositories.base import BaseRepository


class TickRepository(BaseRepository):
    """Tick 数据 Repository"""

    _COLUMNS = (
        "symbol", "exchange", "timestamp",
        "last_price", "bid_price", "bid_qty",
        "ask_price", "ask_qty", "volume_24h",
    )

    _INSERT_SQL = (
        "INSERT INTO ticks "
        f"({', '.join(_COLUMNS)}) "
        f"VALUES ({', '.join(['?'] * len(_COLUMNS))})"
    )

    async def save(self, tick: Tick) -> None:
        """保存单条 Tick"""
        await self._manager.execute(self._INSERT_SQL, self._to_row(tick))

    async def save_batch(self, ticks: List[Tick]) -> None:
        """批量保存"""
        if not ticks:
            return
        rows = [self._to_row(t) for t in ticks]
        await self._manager.executemany(self._INSERT_SQL, rows)

    async def query_latest(
        self, symbol: str, exchange: str, limit: int = 100
    ) -> List[Tick]:
        """查询最新 N 条 Tick"""
        sql = (
            "SELECT * FROM ticks WHERE symbol = ? AND exchange = ? "
            "ORDER BY timestamp DESC LIMIT ?"
        )
        rows = await self._manager.fetch_all(sql, [symbol, exchange, limit])
        return [self._from_row(r) for r in reversed(rows)]

    async def query_range(
        self, symbol: str, exchange: str, start_ts: int, end_ts: int
    ) -> List[Tick]:
        """时间范围查询"""
        sql = (
            "SELECT * FROM ticks WHERE symbol = ? AND exchange = ? "
            "AND timestamp >= ? AND timestamp <= ? "
            "ORDER BY timestamp ASC"
        )
        rows = await self._manager.fetch_all(
            sql, [symbol, exchange, start_ts, end_ts]
        )
        return [self._from_row(r) for r in rows]

    # ─── 内部转换 ───

    @staticmethod
    def _to_row(t: Tick) -> list:
        return [
            t.symbol, t.exchange, t.timestamp,
            float(t.last_price), float(t.bid_price), float(t.bid_qty),
            float(t.ask_price), float(t.ask_qty), float(t.volume_24h),
        ]

    @staticmethod
    def _from_row(row: dict) -> Tick:
        return Tick(
            symbol=row["symbol"],
            exchange=row["exchange"],
            timestamp=row["timestamp"],
            last_price=Decimal(str(row["last_price"])),
            bid_price=Decimal(str(row["bid_price"])),
            bid_qty=Decimal(str(row["bid_qty"])),
            ask_price=Decimal(str(row["ask_price"])),
            ask_qty=Decimal(str(row["ask_qty"])),
            volume_24h=Decimal(str(row["volume_24h"])),
        )

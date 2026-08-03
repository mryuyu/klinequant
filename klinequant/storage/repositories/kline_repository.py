"""KlineRepository — K 线数据 CRUD

DuckDB klines 表的高层封装，提供：
    - save / save_batch：单条/批量写入
    - get_klines：按 symbol + timeframe + 时间范围查询
    - get_latest：获取最新一根 K 线
    - get_range：时间范围查询
    - count / delete_range
"""
from __future__ import annotations

from decimal import Decimal
from typing import List, Optional

from protocol.types import Kline
from storage.repositories.base import BaseRepository


class KlineRepository(BaseRepository):
    """K 线数据 Repository"""

    _COLUMNS = (
        "symbol", "exchange", "timeframe", "timestamp",
        "open", "high", "low", "close",
        "volume", "quote_volume", "trade_count", "is_closed",
    )

    _INSERT_SQL = (
        "INSERT OR REPLACE INTO klines "
        f"({', '.join(_COLUMNS)}) "
        f"VALUES ({', '.join(['?'] * len(_COLUMNS))})"
    )

    # ─── 写入 ───

    async def save(self, kline: Kline) -> None:
        """保存单根 K 线（INSERT OR REPLACE）"""
        await self._manager.execute(self._INSERT_SQL, self._to_row(kline))

    async def save_batch(self, klines: List[Kline]) -> None:
        """批量保存 K 线"""
        if not klines:
            return
        rows = [self._to_row(k) for k in klines]
        await self._manager.executemany(self._INSERT_SQL, rows)

    # ─── 查询 ───

    async def get_klines(
        self,
        symbol: str,
        exchange: str,
        timeframe: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        limit: int = 500,
    ) -> List[Kline]:
        """查询 K 线列表"""
        conditions = ["symbol = ?", "exchange = ?", "timeframe = ?"]
        params: list = [symbol, exchange, timeframe]

        if start_ts is not None:
            conditions.append("timestamp >= ?")
            params.append(start_ts)
        if end_ts is not None:
            conditions.append("timestamp <= ?")
            params.append(end_ts)

        where = " AND ".join(conditions)
        sql = (
            f"SELECT * FROM klines WHERE {where} "
            f"ORDER BY timestamp ASC LIMIT ?"
        )
        params.append(limit)

        rows = await self._manager.fetch_all(sql, params)
        return [self._from_row(r) for r in rows]

    async def get_latest(
        self, symbol: str, exchange: str, timeframe: str
    ) -> Optional[Kline]:
        """获取最新一根 K 线"""
        sql = (
            "SELECT * FROM klines "
            "WHERE symbol = ? AND exchange = ? AND timeframe = ? "
            "ORDER BY timestamp DESC LIMIT 1"
        )
        row = await self._manager.fetch_one(sql, [symbol, exchange, timeframe])
        return self._from_row(row) if row else None

    async def get_range(
        self,
        symbol: str,
        exchange: str,
        timeframe: str,
        start_ts: int,
        end_ts: int,
    ) -> List[Kline]:
        """时间范围查询（无 limit）"""
        sql = (
            "SELECT * FROM klines "
            "WHERE symbol = ? AND exchange = ? AND timeframe = ? "
            "AND timestamp >= ? AND timestamp <= ? "
            "ORDER BY timestamp ASC"
        )
        rows = await self._manager.fetch_all(
            sql, [symbol, exchange, timeframe, start_ts, end_ts]
        )
        return [self._from_row(r) for r in rows]

    async def count(
        self,
        symbol: str,
        exchange: str,
        timeframe: str,
    ) -> int:
        """统计 K 线数量"""
        sql = (
            "SELECT COUNT(*) as cnt FROM klines "
            "WHERE symbol = ? AND exchange = ? AND timeframe = ?"
        )
        result = await self._manager.fetch_scalar(sql, [symbol, exchange, timeframe])
        return result or 0

    async def delete_range(
        self,
        symbol: str,
        exchange: str,
        timeframe: str,
        start_ts: int,
        end_ts: int,
    ) -> int:
        """删除时间范围内的 K 线"""
        sql = (
            "DELETE FROM klines "
            "WHERE symbol = ? AND exchange = ? AND timeframe = ? "
            "AND timestamp >= ? AND timestamp <= ?"
        )
        # 先 count 再 delete
        before = len(await self.get_range(symbol, exchange, timeframe, start_ts, end_ts))
        await self._manager.execute(sql, [symbol, exchange, timeframe, start_ts, end_ts])
        return before

    # ─── 内部转换 ───

    @staticmethod
    def _to_row(k: Kline) -> list:
        return [
            k.symbol, k.exchange, k.timeframe, k.timestamp,
            float(k.open), float(k.high), float(k.low), float(k.close),
            float(k.volume), float(k.quote_volume), k.trade_count, k.is_closed,
        ]

    @staticmethod
    def _from_row(row: dict) -> Kline:
        return Kline(
            symbol=row["symbol"],
            exchange=row["exchange"],
            timeframe=row["timeframe"],
            timestamp=row["timestamp"],
            open=Decimal(str(row["open"])),
            high=Decimal(str(row["high"])),
            low=Decimal(str(row["low"])),
            close=Decimal(str(row["close"])),
            volume=Decimal(str(row["volume"])),
            quote_volume=Decimal(str(row["quote_volume"])),
            trade_count=row["trade_count"],
            is_closed=row["is_closed"],
        )

"""FillRepository — 成交记录 CRUD"""
from __future__ import annotations

from decimal import Decimal
from typing import List, Optional

from storage.repositories.base import BaseRepository


class FillRepository(BaseRepository):
    """成交记录 Repository"""

    _INSERT_SQL = """
        INSERT OR REPLACE INTO fills
        (fill_id, order_id, strategy_id, symbol, exchange, side,
         price, quantity, fee, fee_asset, timestamp, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    async def save(
        self, fill_id: str, order_id: str, strategy_id: str,
        symbol: str, exchange: str, side: str,
        price: Decimal, quantity: Decimal, fee: Decimal,
        fee_asset: str, timestamp: int,
    ) -> None:
        """保存成交记录"""
        import time
        now = int(time.time() * 1000)
        await self._manager.execute(self._INSERT_SQL, [
            fill_id, order_id, strategy_id, symbol, exchange, side,
            float(price), float(quantity), float(fee), fee_asset or None,
            timestamp, now,
        ])

    async def save_batch(self, fills: List[dict]) -> None:
        """批量保存成交记录"""
        if not fills:
            return
        import time
        now = int(time.time() * 1000)
        rows = []
        for f in fills:
            rows.append([
                f["fill_id"], f["order_id"], f["strategy_id"],
                f["symbol"], f["exchange"], f["side"],
                float(f["price"]), float(f["quantity"]),
                float(f.get("fee", 0)), f.get("fee_asset"),
                f["timestamp"], now,
            ])
        await self._manager.executemany(self._INSERT_SQL, rows)

    async def query_by_order(self, order_id: str) -> List[dict]:
        """按订单查询成交"""
        return await self._manager.fetch_all(
            "SELECT * FROM fills WHERE order_id = ? ORDER BY timestamp ASC",
            [order_id],
        )

    async def query_by_strategy(
        self, strategy_id: str, limit: int = 200
    ) -> List[dict]:
        """按策略查询成交"""
        return await self._manager.fetch_all(
            "SELECT * FROM fills WHERE strategy_id = ? "
            "ORDER BY timestamp DESC LIMIT ?",
            [strategy_id, limit],
        )

    async def query_by_time_range(
        self, strategy_id: str, start_ts: int, end_ts: int
    ) -> List[dict]:
        """按时间范围查询成交"""
        return await self._manager.fetch_all(
            "SELECT * FROM fills WHERE strategy_id = ? "
            "AND timestamp >= ? AND timestamp <= ? "
            "ORDER BY timestamp ASC",
            [strategy_id, start_ts, end_ts],
        )

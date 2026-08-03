"""OrderRepository — 订单 CRUD

DuckDB orders 表的高层封装：
    - save / get_by_id / update_status / get_open_orders
    - query_by_strategy / query_by_time_range
"""
from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

from protocol.types import Order, OrderSide, OrderStatus, OrderType
from storage.repositories.base import BaseRepository


class OrderRepository(BaseRepository):
    """订单数据 Repository"""

    _INSERT_SQL = """
        INSERT OR REPLACE INTO orders
        (order_id, strategy_id, symbol, exchange, side, order_type, status,
         price, quantity, filled_qty, filled_price, fee, fee_asset,
         leverage, client_order_id, exchange_order_id, created_at, updated_at, extra)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    async def save(self, order: Order) -> None:
        """保存订单"""
        await self._manager.execute(self._INSERT_SQL, self._to_row(order))

    async def save_batch(self, orders: List[Order]) -> None:
        """批量保存"""
        if not orders:
            return
        rows = [self._to_row(o) for o in orders]
        await self._manager.executemany(self._INSERT_SQL, rows)

    async def get_by_id(self, order_id: str) -> Optional[Order]:
        """按 ID 查询"""
        row = await self._manager.fetch_one(
            "SELECT * FROM orders WHERE order_id = ?", [order_id]
        )
        return self._from_row(row) if row else None

    async def update_status(
        self,
        order_id: str,
        new_status: OrderStatus,
        filled_qty: Optional[Decimal] = None,
        avg_fill_price: Optional[Decimal] = None,
    ) -> bool:
        """更新订单状态（含成交数量和均价）"""
        now = int(time.time() * 1000)
        params: list = [new_status.value, now, order_id]
        sets = ["status = ?", "updated_at = ?"]

        if filled_qty is not None:
            sets.append("filled_qty = ?")
            params.insert(2, float(filled_qty))
        if avg_fill_price is not None:
            sets.append("filled_price = ?")
            params.insert(-1, float(avg_fill_price))

        sql = f"UPDATE orders SET {', '.join(sets)} WHERE order_id = ?"
        await self._manager.execute(sql, params)
        return True

    async def get_open_orders(self, strategy_id: Optional[str] = None) -> List[Order]:
        """查询未完成订单（PENDING/SUBMITTED/PARTIAL_FILLED/CANCELING）"""
        open_statuses = [
            OrderStatus.PENDING.value,
            OrderStatus.SUBMITTED.value,
            OrderStatus.PARTIAL_FILLED.value,
            OrderStatus.CANCELING.value,
        ]
        placeholders = ", ".join(["?"] * len(open_statuses))

        if strategy_id:
            sql = (
                f"SELECT * FROM orders WHERE strategy_id = ? AND status IN ({placeholders}) "
                "ORDER BY created_at DESC"
            )
            rows = await self._manager.fetch_all(sql, [strategy_id] + open_statuses)
        else:
            sql = (
                f"SELECT * FROM orders WHERE status IN ({placeholders}) "
                "ORDER BY created_at DESC"
            )
            rows = await self._manager.fetch_all(sql, open_statuses)

        return [self._from_row(r) for r in rows]

    async def query_by_strategy(
        self, strategy_id: str, limit: int = 100
    ) -> List[Order]:
        """按策略查询订单"""
        sql = (
            "SELECT * FROM orders WHERE strategy_id = ? "
            "ORDER BY created_at DESC LIMIT ?"
        )
        rows = await self._manager.fetch_all(sql, [strategy_id, limit])
        return [self._from_row(r) for r in rows]

    # ─── 内部转换 ───

    @staticmethod
    def _to_row(o: Order) -> list:
        extra = json.dumps(o.metadata) if o.metadata else None
        return [
            o.order_id, o.strategy_id, o.symbol, o.exchange,
            o.side.value, o.order_type.value, o.status.value,
            float(o.price) if o.price is not None else None,
            float(o.quantity),
            float(o.filled_quantity),
            float(o.avg_fill_price),
            float(o.fee),
            o.fee_currency or None,
            1,  # leverage default
            o.client_order_id or None,
            o.exchange_order_id or None,
            o.created_at,
            o.updated_at,
            extra,
        ]

    @staticmethod
    def _from_row(row: dict) -> Order:
        extra = {}
        if row.get("extra"):
            try:
                extra = json.loads(row["extra"])
            except (json.JSONDecodeError, TypeError):
                pass

        return Order(
            order_id=row["order_id"],
            symbol=row["symbol"],
            exchange=row["exchange"],
            side=OrderSide(row["side"]),
            order_type=OrderType(row["order_type"]),
            quantity=Decimal(str(row["quantity"])),
            status=OrderStatus(row["status"]),
            exchange_order_id=row.get("exchange_order_id") or "",
            strategy_id=row.get("strategy_id") or "",
            price=Decimal(str(row["price"])) if row.get("price") is not None else None,
            filled_quantity=Decimal(str(row["filled_qty"] or 0)),
            avg_fill_price=Decimal(str(row["filled_price"] or 0)),
            created_at=row.get("created_at", 0) or 0,
            updated_at=row.get("updated_at", 0) or 0,
            fee=Decimal(str(row["fee"] or 0)),
            fee_currency=row.get("fee_asset") or "",
            client_order_id=row.get("client_order_id") or "",
            metadata=extra,
        )

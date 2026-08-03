"""Repository 层单元测试

覆盖 R-T-001 ~ R-T-005：
    R-T-001: KlineRepository save_batch / get_klines / get_latest
    R-T-002: OrderRepository save / get_by_id / update_status / get_open
    R-T-003: FillRepository save / query_by_order
    R-T-004: StrategyRepository CRUD + 状态更新
    R-T-005: RiskLogRepository save / query_by_time_range
"""
from __future__ import annotations

import tempfile
import time
from decimal import Decimal
from pathlib import Path

import pytest

from protocol.types import (
    Kline, Tick, Order, OrderSide, OrderType, OrderStatus,
)
from storage.duckdb_manager import DuckDBManager
from storage.repositories.kline_repository import KlineRepository
from storage.repositories.order_repository import OrderRepository
from storage.repositories.fill_repository import FillRepository
from storage.repositories.strategy_repository import StrategyRepository
from storage.repositories.risk_log_repository import RiskLogRepository
from storage.repositories.tick_repository import TickRepository


# ─── 共享 fixture ───

@pytest.fixture
async def db():
    """每个测试独立 DuckDB 实例"""
    DuckDBManager.reset()
    db_path = Path(tempfile.mktemp(suffix=".duckdb"))
    manager = DuckDBManager.instance(db_path)
    await manager.initialize()
    yield manager
    await manager.close()
    DuckDBManager.reset()


def _make_kline(symbol="BTCUSDT", exchange="binance", tf="1m", ts=1690000000000,
                open_=60000, close=60050):
    return Kline(
        symbol=symbol, exchange=exchange, timeframe=tf, timestamp=ts,
        open=Decimal(str(open_)), high=Decimal(str(open_ + 100)),
        low=Decimal(str(open_ - 100)), close=Decimal(str(close)),
        volume=Decimal("100"), quote_volume=Decimal("6000000"),
        trade_count=500, is_closed=True,
    )


def _make_order(order_id="ord-001", symbol="BTCUSDT", side=OrderSide.BUY,
                status=OrderStatus.PENDING, qty=Decimal("1.0")):
    now = int(time.time() * 1000)
    return Order(
        order_id=order_id, symbol=symbol, exchange="binance",
        side=side, order_type=OrderType.LIMIT, quantity=qty,
        status=status, strategy_id="strat-001",
        price=Decimal("60000"), created_at=now, updated_at=now,
    )


# ═══════════════════════════════════════════
# R-T-001: KlineRepository
# ═══════════════════════════════════════════


class TestKlineRepository:

    @pytest.mark.asyncio
    async def test_save_and_get(self, db):
        repo = KlineRepository(db)
        k = _make_kline()
        await repo.save(k)

        result = await repo.get_klines("BTCUSDT", "binance", "1m")
        assert len(result) == 1
        assert result[0].symbol == "BTCUSDT"
        assert result[0].open == Decimal("60000")
        assert result[0].close == Decimal("60050")

    @pytest.mark.asyncio
    async def test_save_batch(self, db):
        repo = KlineRepository(db)
        klines = [_make_kline(ts=1690000000000 + i * 60000) for i in range(10)]
        await repo.save_batch(klines)

        count = await repo.count("BTCUSDT", "binance", "1m")
        assert count == 10

    @pytest.mark.asyncio
    async def test_get_latest(self, db):
        repo = KlineRepository(db)
        for i in range(5):
            await repo.save(_make_kline(ts=1690000000000 + i * 60000,
                                        close=60000 + i))

        latest = await repo.get_latest("BTCUSDT", "binance", "1m")
        assert latest is not None
        assert latest.timestamp == 1690000000000 + 4 * 60000
        assert latest.close == Decimal("60004")

    @pytest.mark.asyncio
    async def test_get_klines_with_range(self, db):
        repo = KlineRepository(db)
        for i in range(10):
            await repo.save(_make_kline(ts=1690000000000 + i * 60000))

        # 查询 3~7
        result = await repo.get_klines(
            "BTCUSDT", "binance", "1m",
            start_ts=1690000000000 + 3 * 60000,
            end_ts=1690000000000 + 7 * 60000,
        )
        assert len(result) == 5

    @pytest.mark.asyncio
    async def test_get_latest_empty(self, db):
        repo = KlineRepository(db)
        result = await repo.get_latest("NONEXIST", "binance", "1m")
        assert result is None

    @pytest.mark.asyncio
    async def test_upsert_replace(self, db):
        repo = KlineRepository(db)
        k1 = _make_kline(close=60000)
        await repo.save(k1)

        # 相同主键，不同 close → REPLACE
        k2 = _make_kline(close=60100)
        await repo.save(k2)

        count = await repo.count("BTCUSDT", "binance", "1m")
        assert count == 1

        latest = await repo.get_latest("BTCUSDT", "binance", "1m")
        assert latest.close == Decimal("60100")

    @pytest.mark.asyncio
    async def test_save_batch_empty(self, db):
        repo = KlineRepository(db)
        await repo.save_batch([])  # 不应报错


# ═══════════════════════════════════════════
# R-T-002: OrderRepository
# ═══════════════════════════════════════════


class TestOrderRepository:

    @pytest.mark.asyncio
    async def test_save_and_get_by_id(self, db):
        repo = OrderRepository(db)
        order = _make_order()
        await repo.save(order)

        result = await repo.get_by_id("ord-001")
        assert result is not None
        assert result.order_id == "ord-001"
        assert result.symbol == "BTCUSDT"
        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.LIMIT
        assert result.status == OrderStatus.PENDING
        assert result.quantity == Decimal("1.0")

    @pytest.mark.asyncio
    async def test_get_by_id_not_found(self, db):
        repo = OrderRepository(db)
        result = await repo.get_by_id("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_status(self, db):
        repo = OrderRepository(db)
        await repo.save(_make_order())

        await repo.update_status("ord-001", OrderStatus.SUBMITTED)

        order = await repo.get_by_id("ord-001")
        assert order.status == OrderStatus.SUBMITTED

    @pytest.mark.asyncio
    async def test_update_status_with_fill(self, db):
        repo = OrderRepository(db)
        await repo.save(_make_order(status=OrderStatus.SUBMITTED))

        await repo.update_status(
            "ord-001", OrderStatus.PARTIAL_FILLED,
            filled_qty=Decimal("0.5"),
            avg_fill_price=Decimal("60050"),
        )

        order = await repo.get_by_id("ord-001")
        assert order.status == OrderStatus.PARTIAL_FILLED
        assert order.filled_quantity == Decimal("0.5")
        assert order.avg_fill_price == Decimal("60050")

    @pytest.mark.asyncio
    async def test_get_open_orders(self, db):
        repo = OrderRepository(db)
        await repo.save(_make_order("ord-1", status=OrderStatus.PENDING))
        await repo.save(_make_order("ord-2", status=OrderStatus.SUBMITTED))
        await repo.save(_make_order("ord-3", status=OrderStatus.FILLED))

        opens = await repo.get_open_orders()
        assert len(opens) == 2
        ids = {o.order_id for o in opens}
        assert "ord-1" in ids
        assert "ord-2" in ids
        assert "ord-3" not in ids

    @pytest.mark.asyncio
    async def test_get_open_orders_by_strategy(self, db):
        repo = OrderRepository(db)
        o1 = _make_order("ord-a", status=OrderStatus.PENDING)
        o1.strategy_id = "strat-A"
        o2 = _make_order("ord-b", status=OrderStatus.SUBMITTED)
        o2.strategy_id = "strat-B"
        await repo.save(o1)
        await repo.save(o2)

        opens = await repo.get_open_orders(strategy_id="strat-A")
        assert len(opens) == 1
        assert opens[0].order_id == "ord-a"

    @pytest.mark.asyncio
    async def test_save_batch(self, db):
        repo = OrderRepository(db)
        orders = [_make_order(f"ord-{i}") for i in range(5)]
        await repo.save_batch(orders)

        for i in range(5):
            result = await repo.get_by_id(f"ord-{i}")
            assert result is not None


# ═══════════════════════════════════════════
# R-T-003: FillRepository
# ═══════════════════════════════════════════


class TestFillRepository:

    @pytest.mark.asyncio
    async def test_save_and_query_by_order(self, db):
        repo = FillRepository(db)
        await repo.save(
            fill_id="fill-001", order_id="ord-001", strategy_id="strat-001",
            symbol="BTCUSDT", exchange="binance", side="BUY",
            price=Decimal("60000"), quantity=Decimal("0.5"),
            fee=Decimal("0.001"), fee_asset="BTC",
            timestamp=1690000000000,
        )
        await repo.save(
            fill_id="fill-002", order_id="ord-001", strategy_id="strat-001",
            symbol="BTCUSDT", exchange="binance", side="BUY",
            price=Decimal("60100"), quantity=Decimal("0.5"),
            fee=Decimal("0.001"), fee_asset="BTC",
            timestamp=1690000001000,
        )

        fills = await repo.query_by_order("ord-001")
        assert len(fills) == 2
        assert fills[0]["fill_id"] == "fill-001"
        assert fills[1]["fill_id"] == "fill-002"

    @pytest.mark.asyncio
    async def test_query_by_strategy(self, db):
        repo = FillRepository(db)
        for i in range(3):
            await repo.save(
                fill_id=f"fill-{i}", order_id=f"ord-{i}", strategy_id="strat-001",
                symbol="BTCUSDT", exchange="binance", side="BUY",
                price=Decimal("60000"), quantity=Decimal("1"),
                fee=Decimal("0"), fee_asset="USDT",
                timestamp=1690000000000 + i * 1000,
            )

        fills = await repo.query_by_strategy("strat-001")
        assert len(fills) == 3

    @pytest.mark.asyncio
    async def test_query_by_time_range(self, db):
        repo = FillRepository(db)
        for i in range(5):
            await repo.save(
                fill_id=f"fill-{i}", order_id=f"ord-{i}", strategy_id="strat-001",
                symbol="BTCUSDT", exchange="binance", side="BUY",
                price=Decimal("60000"), quantity=Decimal("1"),
                fee=Decimal("0"), fee_asset="USDT",
                timestamp=1690000000000 + i * 1000,
            )

        fills = await repo.query_by_time_range("strat-001", 1690000001000, 1690000003000)
        assert len(fills) == 3


# ═══════════════════════════════════════════
# R-T-004: StrategyRepository
# ═══════════════════════════════════════════


class TestStrategyRepository:

    @pytest.mark.asyncio
    async def test_save_and_get(self, db):
        repo = StrategyRepository(db)
        await repo.save(
            strategy_id="strat-001", name="MA_Cross", version="1.0",
            config={"ma_short": 5, "ma_long": 20},
            symbols=["BTCUSDT", "ETHUSDT"],
        )

        result = await repo.get_by_id("strat-001")
        assert result is not None
        assert result["name"] == "MA_Cross"
        assert result["status"] == "STOPPED"

    @pytest.mark.asyncio
    async def test_update_status(self, db):
        repo = StrategyRepository(db)
        await repo.save("s1", "Test", "1.0", {}, ["BTCUSDT"])
        await repo.update_status("s1", "RUNNING")

        result = await repo.get_by_id("s1")
        assert result["status"] == "RUNNING"

    @pytest.mark.asyncio
    async def test_update_config(self, db):
        repo = StrategyRepository(db)
        await repo.save("s1", "Test", "1.0", {"a": 1}, ["BTCUSDT"])
        await repo.update_config("s1", {"a": 2, "b": 3})

        result = await repo.get_by_id("s1")
        import json
        config = json.loads(result["config"])
        assert config == {"a": 2, "b": 3}

    @pytest.mark.asyncio
    async def test_get_all(self, db):
        repo = StrategyRepository(db)
        await repo.save("s1", "A", "1.0", {}, ["BTC"])
        await repo.save("s2", "B", "1.0", {}, ["ETH"])

        all_strats = await repo.get_all()
        assert len(all_strats) == 2

    @pytest.mark.asyncio
    async def test_delete(self, db):
        repo = StrategyRepository(db)
        await repo.save("s1", "A", "1.0", {}, ["BTC"])
        await repo.delete("s1")

        result = await repo.get_by_id("s1")
        assert result is None


# ═══════════════════════════════════════════
# R-T-005: RiskLogRepository
# ═══════════════════════════════════════════


class TestRiskLogRepository:

    @pytest.mark.asyncio
    async def test_save_and_query(self, db):
        repo = RiskLogRepository(db)
        log_id = await repo.save(
            strategy_id="strat-001",
            rule_name="max_drawdown",
            level="WARN",
            message="Drawdown exceeded 5%",
            context={"drawdown": 0.052, "equity": 10000},
            timestamp=1690000000000,
        )
        assert log_id  # UUID string

        logs = await repo.query_by_strategy("strat-001")
        assert len(logs) == 1
        assert logs[0]["rule_name"] == "max_drawdown"
        assert logs[0]["level"] == "WARN"

    @pytest.mark.asyncio
    async def test_query_by_time_range(self, db):
        repo = RiskLogRepository(db)
        for i in range(5):
            await repo.save(
                strategy_id="strat-001",
                rule_name=f"rule_{i}",
                level="INFO",
                message=f"Event {i}",
                timestamp=1690000000000 + i * 1000,
            )

        logs = await repo.query_by_time_range("strat-001", 1690000001000, 1690000003000)
        assert len(logs) == 3

    @pytest.mark.asyncio
    async def test_query_by_level(self, db):
        repo = RiskLogRepository(db)
        await repo.save("s1", "r1", "INFO", "msg1", timestamp=1690000000000)
        await repo.save("s1", "r2", "WARN", "msg2", timestamp=1690000001000)
        await repo.save("s1", "r3", "ERROR", "msg3", timestamp=1690000002000)

        warns = await repo.query_by_level("s1", "WARN")
        assert len(warns) == 1
        assert warns[0]["rule_name"] == "r2"


# ═══════════════════════════════════════════
# TickRepository
# ═══════════════════════════════════════════


class TestTickRepository:

    @pytest.mark.asyncio
    async def test_save_and_query(self, db):
        repo = TickRepository(db)
        tick = Tick(
            symbol="BTCUSDT", exchange="binance", timestamp=1690000000000,
            last_price=Decimal("60000"), bid_price=Decimal("59999"),
            bid_qty=Decimal("1.5"), ask_price=Decimal("60001"),
            ask_qty=Decimal("2.0"), volume_24h=Decimal("50000"),
        )
        await repo.save(tick)

        ticks = await repo.query_latest("BTCUSDT", "binance", limit=10)
        assert len(ticks) == 1
        assert ticks[0].last_price == Decimal("60000")

    @pytest.mark.asyncio
    async def test_save_batch_and_range(self, db):
        repo = TickRepository(db)
        ticks = [
            Tick(
                symbol="BTCUSDT", exchange="binance",
                timestamp=1690000000000 + i * 100,
                last_price=Decimal("60000"), bid_price=Decimal("59999"),
                bid_qty=Decimal("1"), ask_price=Decimal("60001"),
                ask_qty=Decimal("1"), volume_24h=Decimal("50000"),
            )
            for i in range(10)
        ]
        await repo.save_batch(ticks)

        result = await repo.query_range(
            "BTCUSDT", "binance",
            1690000000300, 1690000000700,
        )
        assert len(result) == 5  # i=3,4,5,6,7

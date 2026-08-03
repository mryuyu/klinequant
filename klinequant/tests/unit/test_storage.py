"""DuckDB + Redis 存储层单元测试

覆盖 DB-T-001 ~ DB-T-005：
    DB-T-001: DuckDBManager 单例 + 写锁互斥
    DB-T-002: DuckDB Schema 迁移脚本幂等性
    DB-T-003: klines 表插入/查询/时间范围筛选
    DB-T-004: BatchWriter 批量写入 + 缓冲刷新
    DB-T-005: RedisCacheManager get/set/delete + TTL 过期
"""
from __future__ import annotations

import asyncio
import json
import tempfile
from decimal import Decimal
from pathlib import Path

import pytest

from storage.duckdb_manager import DuckDBManager
from storage.batch_writer import (
    BatchWriter,
    create_indicator_writer,
    create_kline_writer,
    create_tick_writer,
)
from storage.schema import TABLE_NAMES, run_migrations


# ═══════════════════════════════════════════
# DB-T-001: DuckDBManager 单例 + 写锁互斥
# ═══════════════════════════════════════════


class TestDuckDBManager:
    """验证 DuckDBManager 的单例模式和写锁"""

    def setup_method(self):
        DuckDBManager.reset()

    def teardown_method(self):
        DuckDBManager.reset()

    def test_singleton(self):
        """instance() 返回同一个对象"""
        m1 = DuckDBManager.instance()
        m2 = DuckDBManager.instance()
        assert m1 is m2

    def test_singleton_with_path(self):
        """带路径的 instance() 返回同一个对象"""
        p = Path(tempfile.mktemp(suffix=".duckdb"))
        m1 = DuckDBManager.instance(p)
        m2 = DuckDBManager.instance()
        assert m1 is m2
        assert m1.db_path == p

    def test_reset(self):
        """reset() 后 instance() 返回新对象"""
        m1 = DuckDBManager.instance()
        DuckDBManager.reset()
        m2 = DuckDBManager.instance()
        assert m1 is not m2

    @pytest.mark.asyncio
    async def test_initialize_idempotent(self):
        """initialize() 幂等：多次调用不报错"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        try:
            await manager.initialize()
            assert manager.is_initialized
            await manager.initialize()  # 第二次不应报错
            assert manager.is_initialized
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_write_lock_serializes(self):
        """写锁保证并发 execute 串行执行"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        try:
            await manager.initialize()

            # 创建测试表
            await manager.execute("CREATE TABLE test_lock (id INTEGER, val VARCHAR)")

            # 并发写入 20 条
            tasks = []
            for i in range(20):
                tasks.append(
                    manager.execute(
                        "INSERT INTO test_lock VALUES (?, ?)", [i, f"val_{i}"]
                    )
                )
            await asyncio.gather(*tasks)

            # 验证全部写入成功
            count = await manager.fetch_scalar("SELECT COUNT(*) FROM test_lock")
            assert count == 20
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_fetch_all(self):
        """fetch_all 返回 dict 列表"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        try:
            await manager.initialize()
            await manager.execute("CREATE TABLE test_fetch (id INTEGER, name VARCHAR)")
            await manager.execute("INSERT INTO test_fetch VALUES (?, ?)", [1, "alice"])
            await manager.execute("INSERT INTO test_fetch VALUES (?, ?)", [2, "bob"])

            rows = await manager.fetch_all("SELECT * FROM test_fetch ORDER BY id")
            assert len(rows) == 2
            assert rows[0]["id"] == 1
            assert rows[0]["name"] == "alice"
            assert rows[1]["id"] == 2
            assert rows[1]["name"] == "bob"
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_fetch_one(self):
        """fetch_one 返回单条或 None"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        try:
            await manager.initialize()
            await manager.execute("CREATE TABLE test_one (id INTEGER)")
            await manager.execute("INSERT INTO test_one VALUES (?)", [42])

            row = await manager.fetch_one("SELECT * FROM test_one WHERE id = ?", [42])
            assert row is not None
            assert row["id"] == 42

            none_row = await manager.fetch_one("SELECT * FROM test_one WHERE id = ?", [999])
            assert none_row is None
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_close_and_reinitialize(self):
        """close 后可重新 initialize"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        try:
            await manager.initialize()
            assert manager.is_initialized
            await manager.close()
            assert not manager.is_initialized
        finally:
            pass


# ═══════════════════════════════════════════
# DB-T-002: Schema 迁移脚本幂等性
# ═══════════════════════════════════════════


class TestSchema:
    """验证 Schema 迁移的幂等性和完整性"""

    def setup_method(self):
        DuckDBManager.reset()

    def teardown_method(self):
        DuckDBManager.reset()

    @pytest.mark.asyncio
    async def test_migration_idempotent(self):
        """run_migrations() 多次执行不报错"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        try:
            await manager.initialize()  # 内部已执行一次迁移
            # 手动再执行一次
            await manager._execute_raw(run_migrations())
            # 验证表存在
            tables = await manager.fetch_all(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            )
            table_names = {t["table_name"] for t in tables}
            for name in TABLE_NAMES:
                assert name in table_names, f"Missing table: {name}"
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_all_tables_created(self):
        """所有 10 张表均被创建"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        try:
            await manager.initialize()
            tables = await manager.fetch_all(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            )
            table_names = {t["table_name"] for t in tables}
            assert len(TABLE_NAMES) == 10
            for name in TABLE_NAMES:
                assert name in table_names, f"Missing table: {name}"
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_klines_primary_key(self):
        """klines 表主键约束（symbol + exchange + timeframe + timestamp）"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        try:
            await manager.initialize()
            row = [
                "BTCUSDT", "binance", "1m", 1690000000000,
                60000.0, 60100.0, 59900.0, 60050.0,
                100.0, 6000000.0, 500, True,
            ]
            await manager.execute(
                "INSERT INTO klines VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row + [1690000000000],
            )
            # 重复插入应触发 REPLACE
            row[7] = 60060.0  # 修改 close
            await manager.execute(
                "INSERT OR REPLACE INTO klines VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row + [1690000000000],
            )
            count = await manager.fetch_scalar("SELECT COUNT(*) FROM klines")
            assert count == 1  # 只有一条
            result = await manager.fetch_one("SELECT close FROM klines")
            assert result["close"] == 60060.0  # 更新后的值
        finally:
            await manager.close()


# ═══════════════════════════════════════════
# DB-T-003: klines 表插入/查询/时间范围筛选
# ═══════════════════════════════════════════


class TestKlinesTable:
    """验证 klines 表的 CRUD 操作"""

    def setup_method(self):
        DuckDBManager.reset()

    def teardown_method(self):
        DuckDBManager.reset()

    async def _setup_with_data(self):
        """创建 manager 并插入测试 K 线数据"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        await manager.initialize()

        base_ts = 1690000000000
        for i in range(10):
            await manager.execute(
                "INSERT INTO klines VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    "BTCUSDT", "binance", "1m", base_ts + i * 60000,
                    60000.0 + i, 60100.0 + i, 59900.0 + i, 60050.0 + i,
                    100.0 + i, 6000000.0, 500 + i, i < 8,
                    1690000000000,  # created_at as BIGINT
                ],
            )
        return manager, base_ts

    @pytest.mark.asyncio
    async def test_insert_and_count(self):
        """插入 10 条 K 线，COUNT 为 10"""
        manager, _ = await self._setup_with_data()
        try:
            count = await manager.fetch_scalar("SELECT COUNT(*) FROM klines")
            assert count == 10
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_time_range_query(self):
        """时间范围查询"""
        manager, base_ts = await self._setup_with_data()
        try:
            # 查询第 3~7 根 K 线
            start = base_ts + 2 * 60000
            end = base_ts + 6 * 60000
            rows = await manager.fetch_all(
                "SELECT * FROM klines WHERE timestamp >= ? AND timestamp <= ? ORDER BY timestamp",
                [start, end],
            )
            assert len(rows) == 5
            assert rows[0]["timestamp"] == start
            assert rows[-1]["timestamp"] == end
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_latest_kline(self):
        """查询最新 K 线"""
        manager, base_ts = await self._setup_with_data()
        try:
            row = await manager.fetch_one(
                "SELECT * FROM klines WHERE symbol = ? AND exchange = ? AND timeframe = ? "
                "ORDER BY timestamp DESC LIMIT 1",
                ["BTCUSDT", "binance", "1m"],
            )
            assert row is not None
            assert row["timestamp"] == base_ts + 9 * 60000
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_closed_filter(self):
        """按 is_closed 过滤"""
        manager, _ = await self._setup_with_data()
        try:
            rows = await manager.fetch_all(
                "SELECT * FROM klines WHERE is_closed = TRUE"
            )
            assert len(rows) == 8  # 前 8 根 is_closed=True

            rows_open = await manager.fetch_all(
                "SELECT * FROM klines WHERE is_closed = FALSE"
            )
            assert len(rows_open) == 2
        finally:
            await manager.close()


# ═══════════════════════════════════════════
# DB-T-004: BatchWriter 批量写入 + 缓冲刷新
# ═══════════════════════════════════════════


class TestBatchWriter:
    """验证 BatchWriter 缓冲写入行为"""

    def setup_method(self):
        DuckDBManager.reset()

    def teardown_method(self):
        DuckDBManager.reset()

    @pytest.mark.asyncio
    async def test_write_and_flush(self):
        """写入后手动 flush，数据落库"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        try:
            await manager.initialize()

            writer = BatchWriter(
                table="klines",
                columns=[
                    "symbol", "exchange", "timeframe", "timestamp",
                    "open", "high", "low", "close",
                    "volume", "quote_volume", "trade_count", "is_closed",
                ],
                manager=manager,
                buffer_size=100,
                flush_interval=999,  # 禁用定时刷新
            )

            # 写入 5 行（未达 buffer_size，不自动刷新）
            for i in range(5):
                await writer.write([
                    "BTCUSDT", "binance", "1m", 1690000000000 + i * 60000,
                    60000.0, 60100.0, 59900.0, 60050.0,
                    100.0, 6000000.0, 500, True,
                ])

            assert writer.buffer_count == 5

            # 数据库还没有数据
            count = await manager.fetch_scalar("SELECT COUNT(*) FROM klines")
            assert count == 0

            # 手动 flush
            await writer.flush()
            assert writer.buffer_count == 0
            assert writer.total_written == 5

            count = await manager.fetch_scalar("SELECT COUNT(*) FROM klines")
            assert count == 5
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_auto_flush_on_buffer_full(self):
        """缓冲区满时自动 flush"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        try:
            await manager.initialize()

            writer = BatchWriter(
                table="klines",
                columns=[
                    "symbol", "exchange", "timeframe", "timestamp",
                    "open", "high", "low", "close",
                    "volume", "quote_volume", "trade_count", "is_closed",
                ],
                manager=manager,
                buffer_size=3,  # 小缓冲区触发自动刷新
                flush_interval=999,
            )

            for i in range(7):
                await writer.write([
                    "BTCUSDT", "binance", "1m", 1690000000000 + i * 60000,
                    60000.0, 60100.0, 59900.0, 60050.0,
                    100.0, 6000000.0, 500, True,
                ])

            # 7 行，buffer_size=3 → 自动 flush 2 次（6行），剩余 1 行在缓冲
            assert writer.buffer_count == 1
            assert writer.total_written == 6

            count = await manager.fetch_scalar("SELECT COUNT(*) FROM klines")
            assert count == 6

            # 最终 flush
            await writer.flush()
            assert writer.total_written == 7
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_write_batch(self):
        """批量写入多行"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        try:
            await manager.initialize()

            writer = BatchWriter(
                table="klines",
                columns=[
                    "symbol", "exchange", "timeframe", "timestamp",
                    "open", "high", "low", "close",
                    "volume", "quote_volume", "trade_count", "is_closed",
                ],
                manager=manager,
                buffer_size=100,
                flush_interval=999,
            )

            rows = [
                [
                    "BTCUSDT", "binance", "1m", 1690000000000 + i * 60000,
                    60000.0, 60100.0, 59900.0, 60050.0,
                    100.0, 6000000.0, 500, True,
                ]
                for i in range(10)
            ]
            await writer.write_batch(rows)
            assert writer.buffer_count == 10

            await writer.flush()
            count = await manager.fetch_scalar("SELECT COUNT(*) FROM klines")
            assert count == 10
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_stop_flushes_remaining(self):
        """stop() 会刷新剩余缓冲"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        try:
            await manager.initialize()

            writer = BatchWriter(
                table="klines",
                columns=[
                    "symbol", "exchange", "timeframe", "timestamp",
                    "open", "high", "low", "close",
                    "volume", "quote_volume", "trade_count", "is_closed",
                ],
                manager=manager,
                buffer_size=100,
                flush_interval=999,
            )
            await writer.start()

            for i in range(3):
                await writer.write([
                    "BTCUSDT", "binance", "1m", 1690000000000 + i * 60000,
                    60000.0, 60100.0, 59900.0, 60050.0,
                    100.0, 6000000.0, 500, True,
                ])

            assert writer.buffer_count == 3

            await writer.stop()
            assert writer.buffer_count == 0
            assert writer.total_written == 3

            count = await manager.fetch_scalar("SELECT COUNT(*) FROM klines")
            assert count == 3
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_factory_functions(self):
        """工厂函数创建正确的 writer"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        try:
            kw = create_kline_writer(manager)
            assert kw.table == "klines"
            assert kw.buffer_count == 0

            tw = create_tick_writer(manager)
            assert tw.table == "ticks"

            iw = create_indicator_writer(manager)
            assert iw.table == "indicator_values"
        finally:
            await manager.close()

    @pytest.mark.asyncio
    async def test_flush_empty_buffer_noop(self):
        """空缓冲区 flush 不报错且不执行 SQL"""
        db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        manager = DuckDBManager.instance(db_path)
        try:
            await manager.initialize()
            writer = BatchWriter(
                table="klines",
                columns=[
                    "symbol", "exchange", "timeframe", "timestamp",
                    "open", "high", "low", "close",
                    "volume", "quote_volume", "trade_count", "is_closed",
                ],
                manager=manager,
            )
            await writer.flush()  # 空 flush 不报错
            assert writer.total_written == 0
        finally:
            await manager.close()


# ═══════════════════════════════════════════
# DB-T-005: RedisCacheManager
# ═══════════════════════════════════════════


class TestRedisCacheManager:
    """验证 RedisCacheManager 的 KV / Hash / TTL 操作"""

    @pytest.fixture
    async def cache(self):
        """创建并清理 Redis 缓存"""
        from storage.redis_cache import RedisCacheManager
        cm = RedisCacheManager(key_prefix="kq_test:")
        await cm.initialize()
        # 清理测试键
        await cm.delete_pattern("*")
        yield cm
        await cm.delete_pattern("*")
        await cm.close()

    @pytest.mark.asyncio
    async def test_set_and_get(self, cache):
        """set/get 基本 KV"""
        await cache.set("hello", {"world": 42})
        result = await cache.get("hello")
        assert result == {"world": 42}

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, cache):
        """不存在的 key 返回 None"""
        result = await cache.get("nonexistent_key")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_with_ttl(self, cache):
        """带 TTL 的 set"""
        await cache.set("ttl_key", "value", ttl=2)
        result = await cache.get("ttl_key")
        assert result == "value"

        ttl_val = await cache.ttl("ttl_key")
        assert 0 < ttl_val <= 2

    @pytest.mark.asyncio
    async def test_delete(self, cache):
        """delete 删除键"""
        await cache.set("del_key", "v")
        assert await cache.exists("del_key")

        deleted = await cache.delete("del_key")
        assert deleted == 1
        assert not await cache.exists("del_key")

    @pytest.mark.asyncio
    async def test_hset_hget(self, cache):
        """Hash set/get"""
        await cache.hset("my_hash", "field1", {"a": 1})
        await cache.hset("my_hash", "field2", {"b": 2})

        v1 = await cache.hget("my_hash", "field1")
        assert v1 == {"a": 1}

        v2 = await cache.hget("my_hash", "field2")
        assert v2 == {"b": 2}

        # 不存在的字段
        v3 = await cache.hget("my_hash", "field999")
        assert v3 is None

    @pytest.mark.asyncio
    async def test_hgetall(self, cache):
        """获取整个 Hash"""
        await cache.hset("hash_all", "x", 1)
        await cache.hset("hash_all", "y", 2)

        all_data = await cache.hgetall("hash_all")
        assert len(all_data) == 2
        assert all_data["x"] == 1
        assert all_data["y"] == 2

    @pytest.mark.asyncio
    async def test_hdel(self, cache):
        """删除 Hash 字段"""
        await cache.hset("hash_del", "f1", "v1")
        await cache.hset("hash_del", "f2", "v2")

        deleted = await cache.hdel("hash_del", "f1")
        assert deleted == 1

        all_data = await cache.hgetall("hash_del")
        assert len(all_data) == 1
        assert "f2" in all_data

    @pytest.mark.asyncio
    async def test_mget_mset(self, cache):
        """批量 set/get"""
        await cache.mset({"k1": "v1", "k2": "v2", "k3": "v3"})
        results = await cache.mget(["k1", "k2", "k3", "k4"])
        assert results == ["v1", "v2", "v3", None]

    @pytest.mark.asyncio
    async def test_decimal_serialization(self, cache):
        """Decimal 值可正确序列化/反序列化"""
        data = {"price": Decimal("60000.50"), "qty": Decimal("1.5")}
        await cache.set("decimal_test", data)
        result = await cache.get("decimal_test")
        # Decimal 序列化为字符串
        assert result["price"] == "60000.50"
        assert result["qty"] == "1.5"

    @pytest.mark.asyncio
    async def test_delete_pattern(self, cache):
        """按模式删除"""
        await cache.set("pattern:a", 1)
        await cache.set("pattern:b", 2)
        await cache.set("other:c", 3)

        deleted = await cache.delete_pattern("pattern:*")
        assert deleted == 2

        assert await cache.get("pattern:a") is None
        assert await cache.get("pattern:b") is None
        assert await cache.get("other:c") == 3

"""DuckDBManager — 单例连接管理器

DuckDB 单线程写入限制下的异步封装：
- 单例模式：全局唯一连接
- 写锁：asyncio.Lock 保证串行写入
- 异步接口：run_in_executor 包装同步 API
- 自动建表：启动时执行 Schema 迁移

遵循技术文档 §3.2 DuckDB 规范。
"""
from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb

logger = logging.getLogger(__name__)

# 默认数据库路径（项目根目录下 data/klinequant.duckdb）
_DEFAULT_DB_PATH = Path("data/klinequant.duckdb")


class DuckDBManager:
    """DuckDB 连接管理器（单例 + 写锁 + 异步封装）。

    DuckDB 是嵌入式分析数据库，单进程内只能有一个写连接。
    本类通过单例模式 + asyncio.Lock 确保并发安全。

    用法：
        manager = DuckDBManager.instance()
        await manager.initialize()

        # 写入
        await manager.execute("INSERT INTO klines VALUES (?, ?, ...)", [params])

        # 查询
        rows = await manager.fetch_all("SELECT * FROM klines WHERE symbol = ?", ["BTCUSDT"])
    """

    _instance: Optional[DuckDBManager] = None
    _instance_lock = threading.Lock()

    def __init__(self, db_path: Optional[Path] = None):
        self._db_path = db_path or _DEFAULT_DB_PATH
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self._write_lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="duckdb")
        self._initialized = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    @classmethod
    def instance(cls, db_path: Optional[Path] = None) -> DuckDBManager:
        """获取全局单例。线程安全。"""
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls(db_path)
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """重置单例（仅测试用）。"""
        if cls._instance is not None:
            if cls._instance._conn:
                cls._instance._conn.close()
            cls._instance._executor.shutdown(wait=False)
            cls._instance = None

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    @property
    def db_path(self) -> Path:
        return self._db_path

    async def initialize(self) -> None:
        """初始化连接并创建目录。幂等。"""
        if self._initialized:
            return
        self._loop = asyncio.get_running_loop()

        # 确保目录存在
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        # 同步连接在 executor 中创建
        self._conn = await self._loop.run_in_executor(
            self._executor, self._create_connection
        )

        # 执行 Schema 迁移
        from storage.schema import run_migrations
        await self._execute_raw(run_migrations())

        self._initialized = True
        logger.info(f"DuckDB initialized: {self._db_path}")

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """同步创建 DuckDB 连接"""
        return duckdb.connect(str(self._db_path))

    async def close(self) -> None:
        """关闭连接并释放资源"""
        if self._conn:
            await self._run_sync(self._conn.close)
            self._conn = None
        self._executor.shutdown(wait=False)
        self._initialized = False
        logger.info("DuckDB closed")

    # ─── 异步执行接口 ───

    async def execute(self, sql: str, params: Optional[List[Any]] = None) -> None:
        """执行写操作（INSERT/UPDATE/DELETE），自动获取写锁。"""
        async with self._write_lock:
            await self._execute_with_params(sql, params)

    async def executemany(self, sql: str, params_list: List[List[Any]]) -> None:
        """批量执行写操作，单次锁内完成。"""
        async with self._write_lock:
            await self._run_sync(self._executemany_sync, sql, params_list)

    async def fetch_all(
        self, sql: str, params: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """查询所有结果，返回 dict 列表。"""
        return await self._fetch_with_params(sql, params, fetch_one=False)

    async def fetch_one(
        self, sql: str, params: Optional[List[Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """查询单条结果。"""
        results = await self._fetch_with_params(sql, params, fetch_one=True)
        return results[0] if results else None

    async def fetch_scalar(
        self, sql: str, params: Optional[List[Any]] = None
    ) -> Any:
        """查询标量值（如 COUNT(*)）。"""
        row = await self.fetch_one(sql, params)
        if row:
            return list(row.values())[0]
        return None

    # ─── 内部实现 ───

    async def _execute_raw(self, sql: str) -> None:
        """执行多行 SQL（如建表脚本）"""
        async with self._write_lock:
            await self._run_sync(self._execute_raw_sync, sql)

    def _execute_raw_sync(self, sql: str) -> None:
        assert self._conn is not None
        self._conn.execute(sql)

    async def _execute_with_params(self, sql: str, params: Optional[List[Any]]) -> None:
        await self._run_sync(self._execute_sync, sql, params or [])

    def _execute_sync(self, sql: str, params: List[Any]) -> None:
        assert self._conn is not None
        if params:
            self._conn.execute(sql, params)
        else:
            self._conn.execute(sql)

    def _executemany_sync(self, sql: str, params_list: List[List[Any]]) -> None:
        assert self._conn is not None
        self._conn.executemany(sql, params_list)

    async def _fetch_with_params(
        self, sql: str, params: Optional[List[Any]], fetch_one: bool
    ) -> List[Dict[str, Any]]:
        return await self._run_sync(
            self._fetch_sync, sql, params or [], fetch_one
        )

    def _fetch_sync(
        self, sql: str, params: List[Any], fetch_one: bool
    ) -> List[Dict[str, Any]]:
        assert self._conn is not None
        if params:
            result = self._conn.execute(sql, params)
        else:
            result = self._conn.execute(sql)

        if fetch_one:
            row = result.fetchone()
            if row is None:
                return []
            columns = [desc[0] for desc in result.description]
            return [dict(zip(columns, row))]
        else:
            rows = result.fetchall()
            columns = [desc[0] for desc in result.description]
            return [dict(zip(columns, row)) for row in rows]

    async def _run_sync(self, func, *args) -> Any:
        """在线程池中执行同步函数"""
        if not self._loop:
            self._loop = asyncio.get_running_loop()
        return await self._loop.run_in_executor(self._executor, func, *args)

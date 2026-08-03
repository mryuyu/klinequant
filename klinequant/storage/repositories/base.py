"""Repository 基类

所有 Repository 共享 DuckDBManager 实例，提供统一的初始化和辅助方法。
"""
from __future__ import annotations

from typing import Optional

from storage.duckdb_manager import DuckDBManager


class BaseRepository:
    """Repository 基类"""

    def __init__(self, manager: Optional[DuckDBManager] = None):
        self._manager = manager or DuckDBManager.instance()

    @property
    def manager(self) -> DuckDBManager:
        return self._manager

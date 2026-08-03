"""StrategyRepository — 策略 CRUD"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from storage.repositories.base import BaseRepository


class StrategyRepository(BaseRepository):
    """策略配置 Repository"""

    _INSERT_SQL = """
        INSERT OR REPLACE INTO strategies
        (strategy_id, name, version, config, status, symbols, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    async def save(
        self,
        strategy_id: str,
        name: str,
        version: str,
        config: Dict[str, Any],
        symbols: List[str],
        status: str = "STOPPED",
    ) -> None:
        """保存策略"""
        now = int(time.time() * 1000)
        await self._manager.execute(self._INSERT_SQL, [
            strategy_id, name, version,
            json.dumps(config),
            status,
            json.dumps(symbols),
            now, now,
        ])

    async def get_by_id(self, strategy_id: str) -> Optional[dict]:
        """按 ID 查询"""
        return await self._manager.fetch_one(
            "SELECT * FROM strategies WHERE strategy_id = ?", [strategy_id]
        )

    async def get_all(self) -> List[dict]:
        """查询所有策略"""
        return await self._manager.fetch_all(
            "SELECT * FROM strategies ORDER BY created_at DESC"
        )

    async def update_status(self, strategy_id: str, status: str) -> bool:
        """更新策略状态"""
        now = int(time.time() * 1000)
        await self._manager.execute(
            "UPDATE strategies SET status = ?, updated_at = ? WHERE strategy_id = ?",
            [status, now, strategy_id],
        )
        return True

    async def update_config(
        self, strategy_id: str, config: Dict[str, Any]
    ) -> bool:
        """更新策略配置"""
        now = int(time.time() * 1000)
        await self._manager.execute(
            "UPDATE strategies SET config = ?, updated_at = ? WHERE strategy_id = ?",
            [json.dumps(config), now, strategy_id],
        )
        return True

    async def delete(self, strategy_id: str) -> bool:
        """删除策略"""
        await self._manager.execute(
            "DELETE FROM strategies WHERE strategy_id = ?", [strategy_id]
        )
        return True

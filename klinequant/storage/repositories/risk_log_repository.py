"""RiskLogRepository — 风控日志 CRUD"""
from __future__ import annotations

import json
import time
import uuid
from typing import Any, Dict, List, Optional

from storage.repositories.base import BaseRepository


class RiskLogRepository(BaseRepository):
    """风控日志 Repository"""

    async def save(
        self,
        strategy_id: str,
        rule_name: str,
        level: str,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        timestamp: Optional[int] = None,
    ) -> str:
        """保存风控日志，返回 log_id"""
        log_id = str(uuid.uuid4())
        ts = timestamp or int(time.time() * 1000)
        now = int(time.time() * 1000)
        ctx_json = json.dumps(context) if context else None

        await self._manager.execute(
            "INSERT INTO risk_logs "
            "(log_id, strategy_id, rule_name, level, message, context, timestamp, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [log_id, strategy_id, rule_name, level, message, ctx_json, ts, now],
        )
        return log_id

    async def query_by_strategy(
        self, strategy_id: str, limit: int = 100
    ) -> List[dict]:
        """按策略查询风控日志"""
        return await self._manager.fetch_all(
            "SELECT * FROM risk_logs WHERE strategy_id = ? "
            "ORDER BY timestamp DESC LIMIT ?",
            [strategy_id, limit],
        )

    async def query_by_time_range(
        self,
        strategy_id: str,
        start_ts: int,
        end_ts: int,
    ) -> List[dict]:
        """按时间范围查询"""
        return await self._manager.fetch_all(
            "SELECT * FROM risk_logs WHERE strategy_id = ? "
            "AND timestamp >= ? AND timestamp <= ? "
            "ORDER BY timestamp ASC",
            [strategy_id, start_ts, end_ts],
        )

    async def query_by_level(
        self, strategy_id: str, level: str, limit: int = 50
    ) -> List[dict]:
        """按级别查询"""
        return await self._manager.fetch_all(
            "SELECT * FROM risk_logs WHERE strategy_id = ? AND level = ? "
            "ORDER BY timestamp DESC LIMIT ?",
            [strategy_id, level, limit],
        )

"""审计日志模块

遵循需求文档 §14.4：所有关键操作记录不可篡改的审计日志。

记录类型：
    - LOGIN / LOGOUT：登录/登出
    - ORDER_CREATE / ORDER_CANCEL：下单/撤单
    - STRATEGY_START / STRATEGY_STOP / STRATEGY_PAUSE：策略启停
    - RISK_RULE_CHANGE：风控规则变更
    - CONFIG_CHANGE：配置修改
    - API_KEY_CHANGE：API Key 操作

存储：JSONL 文件（按日轮转）+ 内存环形缓冲（最近 1000 条）
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# 审计日志存储目录
AUDIT_LOG_DIR = Path(os.getenv("AUDIT_LOG_DIR", "logs/audit"))


@dataclass
class AuditEntry:
    """审计日志条目"""

    audit_id: str
    timestamp: int  # ms
    action: str  # 操作类型
    operator: str  # 操作者（user / system / strategy:xxx）
    resource: str  # 资源类型（order / strategy / risk_rule / config / api_key）
    resource_id: str  # 资源标识
    detail: str  # 人类可读描述
    ip: Optional[str] = None
    result: str = "SUCCESS"  # SUCCESS / FAILED
    extra: dict = field(default_factory=dict)


class AuditLogger:
    """审计日志记录器

    - 内存环形缓冲（最近 1000 条，供 API 查询）
    - JSONL 文件持久化（按日轮转，不可篡改追加写入）
    """

    def __init__(self, max_memory: int = 1000, log_dir: Optional[Path] = None):
        self._buffer: deque[AuditEntry] = deque(maxlen=max_memory)
        self._log_dir = log_dir or AUDIT_LOG_DIR
        self._log_dir.mkdir(parents=True, exist_ok=True)

    def log(
        self,
        action: str,
        operator: str,
        resource: str,
        resource_id: str,
        detail: str,
        ip: Optional[str] = None,
        result: str = "SUCCESS",
        extra: Optional[dict] = None,
    ) -> AuditEntry:
        """记录一条审计日志"""
        entry = AuditEntry(
            audit_id=f"AUD-{uuid.uuid4().hex[:12]}",
            timestamp=int(time.time() * 1000),
            action=action,
            operator=operator,
            resource=resource,
            resource_id=resource_id,
            detail=detail,
            ip=ip,
            result=result,
            extra=extra or {},
        )
        self._buffer.append(entry)
        self._persist(entry)
        return entry

    def query(
        self,
        action: Optional[str] = None,
        resource: Optional[str] = None,
        operator: Optional[str] = None,
        limit: int = 50,
    ) -> list[dict]:
        """查询审计日志（从内存缓冲）"""
        results = []
        for entry in reversed(self._buffer):
            if action and entry.action != action:
                continue
            if resource and entry.resource != resource:
                continue
            if operator and entry.operator != operator:
                continue
            results.append(asdict(entry))
            if len(results) >= limit:
                break
        return results

    @property
    def total_buffered(self) -> int:
        return len(self._buffer)

    def _persist(self, entry: AuditEntry) -> None:
        """追加写入 JSONL 文件（按日轮转）"""
        try:
            date_str = time.strftime("%Y-%m-%d", time.localtime(entry.timestamp / 1000))
            file_path = self._log_dir / f"audit_{date_str}.jsonl"
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(asdict(entry), ensure_ascii=False) + "\n")
        except Exception as e:
            logger.error(f"Failed to persist audit log: {e}")


# 全局单例
audit_logger = AuditLogger()

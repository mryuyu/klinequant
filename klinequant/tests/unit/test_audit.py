"""审计日志模块单元测试

覆盖：
    - AuditEntry 数据结构
    - AuditLogger 记录/查询/持久化
    - 过滤功能
    - 环形缓冲上限
"""
import json
import pytest
from pathlib import Path

from gateway.audit import AuditEntry, AuditLogger


@pytest.fixture
def audit(tmp_path):
    """创建临时目录的 AuditLogger"""
    return AuditLogger(max_memory=100, log_dir=tmp_path)


class TestAuditEntry:
    def test_dataclass_fields(self):
        entry = AuditEntry(
            audit_id="AUD-001",
            timestamp=1000,
            action="LOGIN",
            operator="admin",
            resource="auth",
            resource_id="admin",
            detail="登录成功",
        )
        assert entry.audit_id == "AUD-001"
        assert entry.result == "SUCCESS"
        assert entry.ip is None
        assert entry.extra == {}


class TestAuditLogger:
    def test_log_creates_entry(self, audit):
        entry = audit.log(
            action="ORDER_CREATE",
            operator="user",
            resource="order",
            resource_id="ORD-001",
            detail="买入 BTCUSDT",
        )
        assert entry.audit_id.startswith("AUD-")
        assert entry.action == "ORDER_CREATE"
        assert entry.timestamp > 0
        assert audit.total_buffered == 1

    def test_query_returns_reverse_order(self, audit):
        audit.log("A", "u", "r", "1", "first")
        audit.log("B", "u", "r", "2", "second")
        audit.log("C", "u", "r", "3", "third")

        results = audit.query(limit=3)
        assert len(results) == 3
        assert results[0]["detail"] == "third"
        assert results[2]["detail"] == "first"

    def test_query_filter_by_action(self, audit):
        audit.log("LOGIN", "admin", "auth", "admin", "login")
        audit.log("ORDER_CREATE", "user", "order", "o1", "order")
        audit.log("LOGIN", "admin", "auth", "admin", "login2")

        results = audit.query(action="LOGIN")
        assert len(results) == 2
        assert all(r["action"] == "LOGIN" for r in results)

    def test_query_filter_by_resource(self, audit):
        audit.log("X", "u", "strategy", "s1", "d1")
        audit.log("Y", "u", "order", "o1", "d2")

        results = audit.query(resource="strategy")
        assert len(results) == 1
        assert results[0]["resource"] == "strategy"

    def test_query_limit(self, audit):
        for i in range(20):
            audit.log("A", "u", "r", str(i), f"entry {i}")

        results = audit.query(limit=5)
        assert len(results) == 5

    def test_buffer_max_size(self, tmp_path):
        logger = AuditLogger(max_memory=10, log_dir=tmp_path)
        for i in range(20):
            logger.log("A", "u", "r", str(i), f"entry {i}")

        assert logger.total_buffered == 10
        # 最早的 10 条被淘汰
        results = logger.query(limit=10)
        ids = [r["resource_id"] for r in results]
        assert "0" not in ids
        assert "19" in ids

    def test_persist_to_file(self, audit, tmp_path):
        audit.log("LOGIN", "admin", "auth", "admin", "test persist")

        # 找到 JSONL 文件
        files = list(tmp_path.glob("audit_*.jsonl"))
        assert len(files) == 1

        lines = files[0].read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["action"] == "LOGIN"
        assert data["detail"] == "test persist"

    def test_persist_append_mode(self, audit, tmp_path):
        audit.log("A", "u", "r", "1", "first")
        audit.log("B", "u", "r", "2", "second")

        files = list(tmp_path.glob("audit_*.jsonl"))
        lines = files[0].read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2

    def test_failed_result(self, audit):
        entry = audit.log(
            "ORDER_CREATE", "user", "order", "x",
            "下单失败", result="FAILED",
        )
        assert entry.result == "FAILED"
        results = audit.query(limit=1)
        assert results[0]["result"] == "FAILED"

    def test_extra_data(self, audit):
        entry = audit.log(
            "CONFIG_CHANGE", "admin", "config", "exchange",
            "修改交易所配置", extra={"key": "binance", "field": "api_key"},
        )
        assert entry.extra["key"] == "binance"

"""运维监控模块单元测试

测试内容：
    - 健康检查各组件
    - DuckDB 备份/恢复
    - 备份清理策略
"""
import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

# 导入被测模块
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from health_check import (
    CheckResult,
    HealthReport,
    check_disk_space,
    check_duckdb,
    run_health_check,
)
from backup_duckdb import BackupManager


class TestCheckResult:
    def test_dataclass_fields(self):
        result = CheckResult(
            name="test",
            status="ok",
            message="测试通过",
            latency_ms=12.5,
        )
        assert result.name == "test"
        assert result.status == "ok"
        assert result.latency_ms == 12.5
        assert result.details == {}

    def test_with_details(self):
        result = CheckResult(
            name="db",
            status="warn",
            message="警告",
            details={"size": 100},
        )
        assert result.details["size"] == 100


class TestHealthReport:
    def test_to_dict(self):
        report = HealthReport(
            timestamp=1234567890,
            overall="healthy",
            checks=[
                CheckResult(name="a", status="ok", message="ok"),
            ],
            duration_ms=100.5,
        )
        d = report.to_dict()
        assert d["overall"] == "healthy"
        assert len(d["checks"]) == 1
        assert d["duration_ms"] == 100.5


class TestCheckDiskSpace:
    def test_returns_result(self):
        result = check_disk_space()
        assert result.name == "disk_space"
        assert result.status in ("ok", "warn")
        assert "percent" in result.details
        assert "free_gb" in result.details

    def test_status_threshold(self):
        # 模拟高磁盘使用率
        with patch("shutil.disk_usage") as mock_usage:
            mock_usage.return_value = MagicMock(
                total=100 * 1024**3,
                used=95 * 1024**3,
                free=5 * 1024**3,
            )
            result = check_disk_space(Path("C:\\"))
            assert result.status == "warn"
            assert result.details["percent"] == 95.0


class TestCheckDuckDB:
    def test_nonexistent_db(self, tmp_path):
        result = check_duckdb(tmp_path / "nonexistent.duckdb")
        assert result.status == "warn"
        assert "不存在" in result.message

    def test_existing_db(self, tmp_path):
        # 创建测试数据库
        import duckdb
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")
        conn.execute("INSERT INTO test_table VALUES (1, 'hello')")
        conn.close()

        result = check_duckdb(db_path)
        assert result.status == "ok"
        assert result.details["tables"] == 1


class TestRunHealthCheck:
    def test_skip_options(self):
        # 跳过所有外部检查，只检查本地
        report = run_health_check(
            skip_binance=True,
            skip_frontend=True,
        )
        # 应该有 backend_api, duckdb, disk_space
        names = [c.name for c in report.checks]
        assert "binance_api" not in names
        assert "frontend_dev" not in names
        assert "disk_space" in names


class TestBackupManager:
    def test_init_defaults(self):
        manager = BackupManager()
        assert manager.retain_count == 5
        assert "klinequant.duckdb" in str(manager.db_path)

    def test_backup_nonexistent_db(self, tmp_path):
        manager = BackupManager(
            db_path=tmp_path / "nonexistent.duckdb",
            backup_dir=tmp_path / "backups",
        )
        result = manager.create_backup()
        assert result is None

    def test_backup_and_restore(self, tmp_path):
        """完整备份恢复流程测试"""
        import duckdb

        # 创建测试数据库
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE klines (symbol VARCHAR, close DOUBLE)")
        conn.execute("INSERT INTO klines VALUES ('BTCUSDT', 50000.0)")
        conn.execute("INSERT INTO klines VALUES ('ETHUSDT', 3000.0)")
        conn.execute("CREATE TABLE trades (id INT, price DOUBLE)")
        conn.execute("INSERT INTO trades VALUES (1, 49999.0)")
        conn.close()

        # 执行备份
        backup_dir = tmp_path / "backups"
        manager = BackupManager(
            db_path=db_path,
            backup_dir=backup_dir,
            retain_count=3,
        )
        backup_path = manager.create_backup(verify=True)
        assert backup_path is not None
        assert backup_path.exists()

        # 验证 manifest
        manifest = json.loads((backup_path / "manifest.json").read_text())
        assert manifest["total_rows"] == 3
        assert len(manifest["tables"]) == 2

        # 验证 Parquet 文件
        assert (backup_path / "klines.parquet").exists()
        assert (backup_path / "trades.parquet").exists()

        # 删除原数据库
        db_path.unlink()
        assert not db_path.exists()

        # 恢复
        success = manager.restore_backup(backup_path.name)
        assert success is True
        assert db_path.exists()

        # 验证恢复的数据
        conn = duckdb.connect(str(db_path))
        count = conn.execute("SELECT COUNT(*) FROM klines").fetchone()[0]
        assert count == 2
        count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        assert count == 1
        conn.close()

    def test_cleanup_old_backups(self, tmp_path):
        """测试备份清理策略"""
        backup_dir = tmp_path / "backups"
        backup_dir.mkdir()

        # 创建 5 个模拟备份
        for i in range(5):
            d = backup_dir / f"klinequant_2024010{i}_120000"
            d.mkdir()
            (d / "manifest.json").write_text("{}")

        manager = BackupManager(
            db_path=tmp_path / "test.duckdb",
            backup_dir=backup_dir,
            retain_count=3,
        )
        deleted = manager.cleanup_old_backups()
        assert deleted == 2

        # 验证剩余
        remaining = list(backup_dir.iterdir())
        assert len(remaining) == 3

    def test_list_backups(self, tmp_path):
        """测试备份列表"""
        backup_dir = tmp_path / "backups"
        backup_dir.mkdir()

        # 创建带 manifest 的备份
        d = backup_dir / "klinequant_20240101_120000"
        d.mkdir()
        manifest = {
            "created_at": "2024-01-01T12:00:00",
            "tables": [{"name": "klines", "rows": 100}],
            "total_rows": 100,
        }
        (d / "manifest.json").write_text(json.dumps(manifest))
        (d / "klines.parquet").write_bytes(b"dummy")

        manager = BackupManager(
            db_path=tmp_path / "test.duckdb",
            backup_dir=backup_dir,
        )
        backups = manager.list_backups()
        assert len(backups) == 1
        assert backups[0]["name"] == "klinequant_20240101_120000"
        assert backups[0]["total_rows"] == 100

    def test_restore_nonexistent(self, tmp_path):
        """测试恢复不存在的备份"""
        manager = BackupManager(
            db_path=tmp_path / "test.duckdb",
            backup_dir=tmp_path / "backups",
        )
        success = manager.restore_backup("nonexistent_backup")
        assert success is False

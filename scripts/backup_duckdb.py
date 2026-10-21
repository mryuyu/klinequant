#!/usr/bin/env python
"""DuckDB 自动备份脚本

功能：
    - DuckDB 数据库快照备份（EXPORT DATABASE）
    - 保留策略：自动清理过期备份
    - 备份完整性验证
    - 支持定时执行（配合 Windows Task Scheduler）

用法：
    # 单次备份
    python scripts/backup_duckdb.py

    # 指定备份目录
    python scripts/backup_duckdb.py --backup-dir D:/backups

    # 保留最近 7 个备份
    python scripts/backup_duckdb.py --retain 7

    # 定时备份（每 6 小时）
    python scripts/backup_duckdb.py --schedule 6

备份格式：
    backups/klinequant_YYYYMMDD_HHMMSS/
        ├── klines.parquet
        ├── trades.parquet
        ├── ...
        └── manifest.json
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# 默认配置
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "klinequant.duckdb"
DEFAULT_BACKUP_DIR = PROJECT_ROOT / "backups"
DEFAULT_RETAIN_COUNT = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class BackupManager:
    """DuckDB 备份管理器"""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        backup_dir: Optional[Path] = None,
        retain_count: int = DEFAULT_RETAIN_COUNT,
    ):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.backup_dir = backup_dir or DEFAULT_BACKUP_DIR
        self.retain_count = retain_count

    def create_backup(self, verify: bool = True) -> Optional[Path]:
        """创建数据库备份

        Args:
            verify: 是否验证备份完整性

        Returns:
            备份目录路径，失败返回 None
        """
        if not self.db_path.exists():
            logger.warning(f"数据库文件不存在: {self.db_path}")
            return None

        # 生成备份目录名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"klinequant_{timestamp}"
        backup_path = self.backup_dir / backup_name

        logger.info(f"开始备份: {self.db_path} -> {backup_path}")
        start_time = time.perf_counter()

        try:
            import duckdb

            # 使用 DuckDB EXPORT DATABASE 命令
            # 这会导出所有表为 Parquet 格式
            conn = duckdb.connect(str(self.db_path), read_only=True)

            # 获取表列表
            tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
            logger.info(f"发现 {len(tables)} 个表: {tables}")

            # 创建备份目录
            backup_path.mkdir(parents=True, exist_ok=True)

            # 导出每个表
            exported_tables = []
            total_rows = 0
            for table in tables:
                try:
                    # 获取行数
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    # 导出为 Parquet
                    parquet_file = backup_path / f"{table}.parquet"
                    conn.execute(f"COPY {table} TO '{parquet_file}' (FORMAT PARQUET)")
                    exported_tables.append({"name": table, "rows": count})
                    total_rows += count
                    logger.info(f"  导出 {table}: {count} 行")
                except Exception as e:
                    logger.error(f"  导出 {table} 失败: {e}")

            conn.close()

            # 写入 manifest
            duration = time.perf_counter() - start_time
            manifest = {
                "created_at": datetime.now().isoformat(),
                "source_db": str(self.db_path),
                "source_size_mb": round(self.db_path.stat().st_size / (1024 * 1024), 2),
                "tables": exported_tables,
                "total_rows": total_rows,
                "duration_seconds": round(duration, 2),
                "version": "1.0",
            }
            manifest_file = backup_path / "manifest.json"
            manifest_file.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))

            # 验证备份
            if verify:
                if not self._verify_backup(backup_path, exported_tables):
                    logger.error("备份验证失败！")
                    shutil.rmtree(backup_path, ignore_errors=True)
                    return None

            logger.info(f"备份完成: {len(exported_tables)} 表, {total_rows} 行, 耗时 {duration:.1f}s")
            return backup_path

        except ImportError:
            logger.error("duckdb 模块未安装，无法执行备份")
            return None
        except Exception as e:
            logger.error(f"备份失败: {e}")
            # 清理不完整的备份
            if backup_path.exists():
                shutil.rmtree(backup_path, ignore_errors=True)
            return None

    def _verify_backup(self, backup_path: Path, expected_tables: list[dict]) -> bool:
        """验证备份完整性"""
        logger.info("验证备份完整性...")

        # 检查 manifest
        manifest_file = backup_path / "manifest.json"
        if not manifest_file.exists():
            logger.error("manifest.json 不存在")
            return False

        # 检查每个表的 Parquet 文件
        for table_info in expected_tables:
            parquet_file = backup_path / f"{table_info['name']}.parquet"
            if not parquet_file.exists():
                logger.error(f"缺少文件: {parquet_file.name}")
                return False

            # 验证 Parquet 可读
            try:
                import duckdb
                conn = duckdb.connect(":memory:")
                count = conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{parquet_file}')"
                ).fetchone()[0]
                conn.close()

                if count != table_info["rows"]:
                    logger.error(
                        f"行数不匹配: {table_info['name']} "
                        f"期望 {table_info['rows']}, 实际 {count}"
                    )
                    return False
            except Exception as e:
                logger.error(f"验证 {table_info['name']} 失败: {e}")
                return False

        logger.info("备份验证通过 ✓")
        return True

    def cleanup_old_backups(self) -> int:
        """清理过期备份，保留最近 N 个

        Returns:
            删除的备份数量
        """
        if not self.backup_dir.exists():
            return 0

        # 获取所有备份目录（按名称排序 = 按时间排序）
        backups = sorted(
            [d for d in self.backup_dir.iterdir()
             if d.is_dir() and d.name.startswith("klinequant_")],
            key=lambda p: p.name,
            reverse=True,
        )

        deleted = 0
        if len(backups) > self.retain_count:
            for old_backup in backups[self.retain_count:]:
                logger.info(f"删除过期备份: {old_backup.name}")
                shutil.rmtree(old_backup, ignore_errors=True)
                deleted += 1

        return deleted

    def list_backups(self) -> list[dict]:
        """列出所有备份"""
        if not self.backup_dir.exists():
            return []

        backups = []
        for d in sorted(self.backup_dir.iterdir(), reverse=True):
            if d.is_dir() and d.name.startswith("klinequant_"):
                manifest_file = d / "manifest.json"
                info = {"name": d.name, "path": str(d)}
                if manifest_file.exists():
                    try:
                        manifest = json.loads(manifest_file.read_text())
                        info["created_at"] = manifest.get("created_at")
                        info["tables"] = len(manifest.get("tables", []))
                        info["total_rows"] = manifest.get("total_rows", 0)
                        info["size_mb"] = round(
                            sum(f.stat().st_size for f in d.iterdir() if f.is_file()) / (1024 * 1024),
                            2
                        )
                    except Exception:
                        pass
                backups.append(info)
        return backups

    def restore_backup(self, backup_name: str, target_db: Optional[Path] = None) -> bool:
        """从备份恢复数据库

        Args:
            backup_name: 备份目录名（如 klinequant_20240101_120000）
            target_db: 目标数据库路径（默认覆盖原数据库）

        Returns:
            是否成功
        """
        backup_path = self.backup_dir / backup_name
        if not backup_path.exists():
            logger.error(f"备份不存在: {backup_path}")
            return False

        target = target_db or self.db_path
        logger.info(f"开始恢复: {backup_path} -> {target}")

        try:
            import duckdb

            # 如果目标数据库存在，先备份
            if target.exists():
                pre_restore_backup = target.with_suffix(".pre_restore.duckdb")
                shutil.copy2(target, pre_restore_backup)
                logger.info(f"已创建恢复前备份: {pre_restore_backup}")

            # 创建新数据库并导入
            conn = duckdb.connect(str(target))

            manifest_file = backup_path / "manifest.json"
            if manifest_file.exists():
                manifest = json.loads(manifest_file.read_text())
                for table_info in manifest.get("tables", []):
                    table_name = table_info["name"]
                    parquet_file = backup_path / f"{table_name}.parquet"
                    if parquet_file.exists():
                        # 创建表并导入
                        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_file}')")
                        logger.info(f"  恢复 {table_name}: {table_info['rows']} 行")

            conn.close()
            logger.info("恢复完成 ✓")
            return True

        except Exception as e:
            logger.error(f"恢复失败: {e}")
            return False


def run_scheduled(interval_hours: float, manager: BackupManager) -> None:
    """定时备份循环"""
    interval_seconds = interval_hours * 3600
    logger.info(f"启动定时备份，间隔 {interval_hours} 小时")

    while True:
        try:
            backup_path = manager.create_backup()
            if backup_path:
                deleted = manager.cleanup_old_backups()
                if deleted:
                    logger.info(f"已清理 {deleted} 个过期备份")
        except KeyboardInterrupt:
            logger.info("收到中断信号，退出")
            break
        except Exception as e:
            logger.error(f"备份异常: {e}")

        logger.info(f"下次备份: {interval_hours} 小时后")
        time.sleep(interval_seconds)


def main():
    parser = argparse.ArgumentParser(description="KlineQuant DuckDB 备份工具")
    parser.add_argument("--db", type=Path, default=None, help="数据库路径")
    parser.add_argument("--backup-dir", type=Path, default=None, help="备份目录")
    parser.add_argument("--retain", type=int, default=DEFAULT_RETAIN_COUNT, help=f"保留备份数量 (默认 {DEFAULT_RETAIN_COUNT})")
    parser.add_argument("--schedule", type=float, default=None, help="定时备份间隔（小时）")
    parser.add_argument("--list", action="store_true", help="列出所有备份")
    parser.add_argument("--restore", type=str, default=None, help="从指定备份恢复")
    parser.add_argument("--no-verify", action="store_true", help="跳过备份验证")
    parser.add_argument("--cleanup", action="store_true", help="仅清理过期备份")
    args = parser.parse_args()

    manager = BackupManager(
        db_path=args.db,
        backup_dir=args.backup_dir,
        retain_count=args.retain,
    )

    if args.list:
        backups = manager.list_backups()
        if not backups:
            print("暂无备份")
        else:
            print(f"\n{'=' * 60}")
            print(f"  备份列表 ({len(backups)} 个)")
            print(f"{'=' * 60}")
            for b in backups:
                print(f"  {b['name']}")
                if "created_at" in b:
                    print(f"    创建: {b['created_at']}")
                    print(f"    表数: {b.get('tables', '?')}, 行数: {b.get('total_rows', '?')}")
                    print(f"    大小: {b.get('size_mb', '?')} MB")
            print(f"{'=' * 60}\n")
        return

    if args.restore:
        success = manager.restore_backup(args.restore)
        sys.exit(0 if success else 1)

    if args.cleanup:
        deleted = manager.cleanup_old_backups()
        print(f"已清理 {deleted} 个过期备份")
        return

    if args.schedule:
        run_scheduled(args.schedule, manager)
    else:
        # 单次备份
        backup_path = manager.create_backup(verify=not args.no_verify)
        if backup_path:
            deleted = manager.cleanup_old_backups()
            print(f"\n备份成功: {backup_path}")
            if deleted:
                print(f"已清理 {deleted} 个过期备份")
            sys.exit(0)
        else:
            print("\n备份失败")
            sys.exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python
"""健康检查脚本

独立可执行，检查系统各组件健康状态：
    - 后端 API（FastAPI）
    - 前端 Dev Server（Vite）
    - 币安 API 连通性
    - DuckDB 数据库文件
    - 磁盘空间

用法：
    python scripts/health_check.py [--json] [--verbose]

退出码：
    0 = 全部健康
    1 = 存在异常
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# 默认配置
DEFAULT_BACKEND_URL = "http://127.0.0.1:8000"
DEFAULT_FRONTEND_URL = "http://127.0.0.1:5173"
BINANCE_PING_URL = "https://api.binance.com/api/v3/ping"
DUCKDB_PATH = PROJECT_ROOT / "data" / "klinequant.duckdb"
DISK_WARN_THRESHOLD = 90.0  # 磁盘使用率告警阈值


@dataclass
class CheckResult:
    """单项检查结果"""
    name: str
    status: str  # "ok" | "warn" | "error"
    message: str
    latency_ms: Optional[float] = None
    details: dict = field(default_factory=dict)


@dataclass
class HealthReport:
    """健康检查报告"""
    timestamp: int
    overall: str  # "healthy" | "degraded" | "unhealthy"
    checks: list[CheckResult]
    duration_ms: float

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "overall": self.overall,
            "duration_ms": round(self.duration_ms, 2),
            "checks": [asdict(c) for c in self.checks],
        }


def check_backend(base_url: str, timeout: float = 5.0) -> CheckResult:
    """检查后端 API 健康状态"""
    import urllib.request
    import urllib.error

    url = f"{base_url}/api/system/health"
    start = time.perf_counter()
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            latency = (time.perf_counter() - start) * 1000
            if resp.status == 200:
                data = json.loads(resp.read().decode())
                return CheckResult(
                    name="backend_api",
                    status="ok",
                    message=f"API 正常 (v{data.get('version', '?')})",
                    latency_ms=latency,
                    details={
                        "uptime_seconds": data.get("uptime_seconds"),
                        "engines": len(data.get("engines", [])),
                    },
                )
            return CheckResult(
                name="backend_api",
                status="error",
                message=f"HTTP {resp.status}",
                latency_ms=latency,
            )
    except urllib.error.URLError as e:
        latency = (time.perf_counter() - start) * 1000
        return CheckResult(
            name="backend_api",
            status="error",
            message=f"连接失败: {e.reason}",
            latency_ms=latency,
        )
    except Exception as e:
        latency = (time.perf_counter() - start) * 1000
        return CheckResult(
            name="backend_api",
            status="error",
            message=f"异常: {e}",
            latency_ms=latency,
        )


def check_frontend(base_url: str, timeout: float = 5.0) -> CheckResult:
    """检查前端 Dev Server"""
    import urllib.request
    import urllib.error

    start = time.perf_counter()
    try:
        req = urllib.request.Request(base_url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            latency = (time.perf_counter() - start) * 1000
            if resp.status == 200:
                return CheckResult(
                    name="frontend_dev",
                    status="ok",
                    message="Vite Dev Server 正常",
                    latency_ms=latency,
                )
            return CheckResult(
                name="frontend_dev",
                status="warn",
                message=f"HTTP {resp.status}",
                latency_ms=latency,
            )
    except Exception as e:
        latency = (time.perf_counter() - start) * 1000
        return CheckResult(
            name="frontend_dev",
            status="warn",
            message=f"前端未启动或不可达: {e}",
            latency_ms=latency,
        )


def check_binance(timeout: float = 10.0) -> CheckResult:
    """检查币安 API 连通性"""
    import urllib.request
    import urllib.error

    # 使用代理（如果配置）
    proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    start = time.perf_counter()
    try:
        if proxy:
            proxy_handler = urllib.request.ProxyHandler({
                "http": proxy,
                "https": proxy,
            })
            opener = urllib.request.build_opener(proxy_handler)
            resp = opener.open(BINANCE_PING_URL, timeout=timeout)
        else:
            resp = urllib.request.urlopen(BINANCE_PING_URL, timeout=timeout)

        latency = (time.perf_counter() - start) * 1000
        with resp:
            if resp.status == 200:
                return CheckResult(
                    name="binance_api",
                    status="ok",
                    message="币安 API 连通",
                    latency_ms=latency,
                )
        return CheckResult(
            name="binance_api",
            status="warn",
            message=f"HTTP {resp.status}",
            latency_ms=latency,
        )
    except Exception as e:
        latency = (time.perf_counter() - start) * 1000
        return CheckResult(
            name="binance_api",
            status="error",
            message=f"币安 API 不可达: {e}",
            latency_ms=latency,
        )


def check_duckdb(db_path: Optional[Path] = None) -> CheckResult:
    """检查 DuckDB 数据库文件"""
    path = db_path or DUCKDB_PATH
    if not path.exists():
        return CheckResult(
            name="duckdb",
            status="warn",
            message=f"数据库文件不存在: {path}",
            details={"path": str(path)},
        )

    size_mb = path.stat().st_size / (1024 * 1024)
    # 尝试打开验证完整性
    try:
        import duckdb
        conn = duckdb.connect(str(path), read_only=True)
        tables = conn.execute("SHOW TABLES").fetchall()
        conn.close()
        return CheckResult(
            name="duckdb",
            status="ok",
            message=f"数据库正常 ({size_mb:.1f} MB, {len(tables)} 表)",
            details={
                "path": str(path),
                "size_mb": round(size_mb, 2),
                "tables": len(tables),
            },
        )
    except ImportError:
        return CheckResult(
            name="duckdb",
            status="ok",
            message=f"数据库文件存在 ({size_mb:.1f} MB)，duckdb 模块未安装跳过验证",
            details={"path": str(path), "size_mb": round(size_mb, 2)},
        )
    except Exception as e:
        return CheckResult(
            name="duckdb",
            status="error",
            message=f"数据库验证失败: {e}",
            details={"path": str(path)},
        )


def check_disk_space(path: Optional[Path] = None) -> CheckResult:
    """检查磁盘空间"""
    check_path = str(path or PROJECT_ROOT)
    # Windows 使用盘符
    if os.name == "nt":
        check_path = str(Path(check_path).anchor) or "C:\\"

    usage = shutil.disk_usage(check_path)
    percent = (usage.used / usage.total) * 100
    free_gb = usage.free / (1024 ** 3)

    status = "ok" if percent < DISK_WARN_THRESHOLD else "warn"
    return CheckResult(
        name="disk_space",
        status=status,
        message=f"磁盘使用 {percent:.1f}% (剩余 {free_gb:.1f} GB)",
        details={
            "total_gb": round(usage.total / (1024 ** 3), 1),
            "used_gb": round(usage.used / (1024 ** 3), 1),
            "free_gb": round(free_gb, 1),
            "percent": round(percent, 1),
        },
    )


def run_health_check(
    backend_url: str = DEFAULT_BACKEND_URL,
    frontend_url: str = DEFAULT_FRONTEND_URL,
    skip_binance: bool = False,
    skip_frontend: bool = False,
) -> HealthReport:
    """执行完整健康检查"""
    start = time.perf_counter()
    checks: list[CheckResult] = []

    # 1. 后端 API
    checks.append(check_backend(backend_url))

    # 2. 前端（可选）
    if not skip_frontend:
        checks.append(check_frontend(frontend_url))

    # 3. 币安 API（可选）
    if not skip_binance:
        checks.append(check_binance())

    # 4. DuckDB
    checks.append(check_duckdb())

    # 5. 磁盘空间
    checks.append(check_disk_space())

    duration = (time.perf_counter() - start) * 1000

    # 综合状态
    statuses = [c.status for c in checks]
    if "error" in statuses:
        overall = "unhealthy"
    elif "warn" in statuses:
        overall = "degraded"
    else:
        overall = "healthy"

    return HealthReport(
        timestamp=int(time.time() * 1000),
        overall=overall,
        checks=checks,
        duration_ms=duration,
    )


def print_report(report: HealthReport, verbose: bool = False) -> None:
    """打印人类可读的报告"""
    icons = {"ok": "✓", "warn": "⚠", "error": "✗"}
    colors = {"ok": "\033[32m", "warn": "\033[33m", "error": "\033[31m"}
    reset = "\033[0m"

    overall_icon = {"healthy": "✓", "degraded": "⚠", "unhealthy": "✗"}[report.overall]
    print(f"\n{'=' * 50}")
    print(f"  KlineQuant 健康检查  {overall_icon} {report.overall.upper()}")
    print(f"{'=' * 50}\n")

    for c in report.checks:
        icon = icons.get(c.status, "?")
        color = colors.get(c.status, "")
        latency = f" ({c.latency_ms:.0f}ms)" if c.latency_ms else ""
        print(f"  {color}{icon}{reset} {c.name:<15} {c.message}{latency}")
        if verbose and c.details:
            for k, v in c.details.items():
                print(f"      {k}: {v}")

    print(f"\n  耗时: {report.duration_ms:.0f}ms")
    print(f"{'=' * 50}\n")


def main():
    parser = argparse.ArgumentParser(description="KlineQuant 健康检查")
    parser.add_argument("--json", action="store_true", help="输出 JSON 格式")
    parser.add_argument("--verbose", "-v", action="store_true", help="显示详细信息")
    parser.add_argument("--backend", default=DEFAULT_BACKEND_URL, help="后端 URL")
    parser.add_argument("--frontend", default=DEFAULT_FRONTEND_URL, help="前端 URL")
    parser.add_argument("--skip-binance", action="store_true", help="跳过币安检查")
    parser.add_argument("--skip-frontend", action="store_true", help="跳过前端检查")
    args = parser.parse_args()

    report = run_health_check(
        backend_url=args.backend,
        frontend_url=args.frontend,
        skip_binance=args.skip_binance,
        skip_frontend=args.skip_frontend,
    )

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
    else:
        print_report(report, verbose=args.verbose)

    sys.exit(0 if report.overall == "healthy" else 1)


if __name__ == "__main__":
    main()

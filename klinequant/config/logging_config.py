"""loguru 日志配置

统一日志管理：
    - 控制台输出（带颜色）
    - 文件轮转（按大小/时间）
    - 日志级别过滤
    - 结构化日志字段（extra）

遵循技术文档 §4.1 日志规范。
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

from loguru import logger


# 默认日志目录
_DEFAULT_LOG_DIR = Path("logs")


def setup_logging(
    log_dir: Optional[Path] = None,
    level: str = "INFO",
    rotation: str = "10 MB",
    retention: str = "7 days",
    console: bool = True,
    file_enabled: bool = True,
) -> None:
    """初始化 loguru 日志配置。

    Args:
        log_dir: 日志目录，默认项目根目录/logs
        level: 日志级别，默认 INFO
        rotation: 文件轮转大小，默认 10 MB
        retention: 日志保留时间，默认 7 天
        console: 是否启用控制台输出
        file_enabled: 是否启用文件日志
    """
    log_dir = log_dir or _DEFAULT_LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)

    # 移除默认 handler
    logger.remove()

    # 控制台输出（带颜色）
    if console:
        logger.add(
            sys.stderr,
            level=level,
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                "<level>{message}</level>"
            ),
            colorize=True,
        )

    # 文件日志（轮转）
    if file_enabled:
        # 主日志文件
        logger.add(
            log_dir / "klinequant_{time:YYYY-MM-DD}.log",
            level=level,
            format=(
                "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
                "{level: <8} | "
                "{name}:{function}:{line} | "
                "{message}"
            ),
            rotation=rotation,
            retention=retention,
            encoding="utf-8",
        )

        # 错误日志单独文件
        logger.add(
            log_dir / "error_{time:YYYY-MM-DD}.log",
            level="ERROR",
            format=(
                "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
                "{level: <8} | "
                "{name}:{function}:{line} | "
                "{message}"
            ),
            rotation=rotation,
            retention=retention,
            encoding="utf-8",
        )


def get_logger(name: str):
    """获取带模块名绑定的 logger。

    Args:
        name: 模块名，通常传 __name__

    Returns:
        绑定了模块名的 logger 实例
    """
    return logger.bind(name=name)

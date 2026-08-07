"""轻量 .env 加载器

网关此前直接读 os.getenv（依赖启动进程的环境变量）。接入 IG 等多市场源后
凭证统一放 klinequant/.env，此处在网关启动入口加载一次（不覆盖已有环境变量）。
"""
from __future__ import annotations

import os
from pathlib import Path


def load_env() -> None:
    """解析 klinequant/.env 的 KEY=VALUE 行写入 os.environ（已有值不覆盖）"""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.is_file():
        return
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.split("#", 1)[0].strip() if key not in ("IG_PASSWORD",) else value.strip()
            if key and key not in os.environ:
                os.environ[key] = value.strip().strip('"').strip("'")
    except Exception:
        pass

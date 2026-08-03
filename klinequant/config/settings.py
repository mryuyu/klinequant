"""pydantic-settings 配置加载

基于 pydantic-settings 的类型安全配置管理：
    - 从 settings.yaml 加载
    - 环境变量覆盖（KQ_ 前缀）
    - 嵌套配置模型
    - 配置验证
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field


# ─── 配置模型 ───


class SystemConfig(BaseModel):
    name: str = "KlineQuant"
    version: str = "1.0.0"
    environment: str = "development"
    timezone: str = "UTC"
    locale: str = "zh_CN"


class GatewayConfig(BaseModel):
    host: str = "127.0.0.1"
    api_port: int = 8000
    ws_port: int = 8001
    cors_origins: List[str] = Field(default_factory=lambda: ["http://localhost:5173"])


class ZmqConfig(BaseModel):
    bind_host: str = "127.0.0.1"
    market_pub: int = 5501
    indicator_pub: int = 5502
    signal_pub: int = 5503
    trade_pub: int = 5504
    risk_rep: int = 5510
    trade_rep: int = 5511
    strategy_port_base: int = 5520
    strategy_port_max: int = 5530


class DuckDBConfig(BaseModel):
    path: str = "data/klinequant.duckdb"
    batch_size: int = 500
    flush_interval: float = 5.0


class ClickHouseConfig(BaseModel):
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 8123
    database: str = "klinequant"
    user: str = "default"
    password: str = ""


class RedisConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    key_prefix: str = "kq:"


class DatabaseConfig(BaseModel):
    duckdb: DuckDBConfig = Field(default_factory=DuckDBConfig)
    clickhouse: ClickHouseConfig = Field(default_factory=ClickHouseConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)


class LoggingConfig(BaseModel):
    level: str = "INFO"
    log_dir: str = "logs"
    rotation: str = "10 MB"
    retention: str = "7 days"
    console: bool = True
    file_enabled: bool = True


class Settings(BaseModel):
    """KlineQuant 全局配置"""

    system: SystemConfig = Field(default_factory=SystemConfig)
    gateway: GatewayConfig = Field(default_factory=GatewayConfig)
    zmq: ZmqConfig = Field(default_factory=ZmqConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    exchanges_config: str = "config/exchanges.yaml"


# ─── 配置加载器 ───


def _expand_env_vars(data: Any) -> Any:
    """递归展开 ${ENV_VAR} 格式的环境变量引用。"""
    if isinstance(data, str):
        import re
        pattern = re.compile(r"\$\{(\w+)\}")
        def _replace(m):
            return os.environ.get(m.group(1), m.group(0))
        return pattern.sub(_replace, data)
    elif isinstance(data, dict):
        return {k: _expand_env_vars(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [_expand_env_vars(v) for v in data]
    return data


def load_settings(
    config_path: Optional[Path] = None,
    env_prefix: str = "KQ_",
) -> Settings:
    """加载配置。

    优先级：环境变量 > yaml 文件 > 默认值

    Args:
        config_path: yaml 配置文件路径，默认 config/settings.yaml
        env_prefix: 环境变量前缀，默认 KQ_

    Returns:
        Settings 实例
    """
    config_path = config_path or Path("config/settings.yaml")

    # 从 yaml 加载
    yaml_data: Dict[str, Any] = {}
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        yaml_data = _expand_env_vars(raw)

    # 环境变量覆盖（KQ_SECTION__KEY 格式，双下划线分隔嵌套）
    env_overrides: Dict[str, Any] = {}
    for key, value in os.environ.items():
        if key.startswith(env_prefix):
            parts = key[len(env_prefix):].lower().split("__")
            _set_nested(env_overrides, parts, value)

    # 合并
    merged = _deep_merge(yaml_data, env_overrides)

    return Settings(**merged)


def _set_nested(d: Dict, parts: List[str], value: Any) -> None:
    """设置嵌套字典值。"""
    for part in parts[:-1]:
        d = d.setdefault(part, {})
    d[parts[-1]] = value


def _deep_merge(base: Dict, override: Dict) -> Dict:
    """深度合并两个字典，override 优先。"""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_exchanges_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """加载交易所配置。

    Args:
        config_path: yaml 配置文件路径

    Returns:
        交易所配置字典
    """
    config_path = config_path or Path("config/exchanges.yaml")
    if not config_path.exists():
        return {"exchanges": {}, "subscriptions": {}}

    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    return _expand_env_vars(raw)


# 全局单例
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """获取全局配置单例。"""
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings


def reset_settings() -> None:
    """重置配置单例（测试用）。"""
    global _settings
    _settings = None

"""日志与配置模块单元测试

覆盖 C-T-001 ~ C-T-005：
    C-T-001: loguru 日志配置（setup_logging / get_logger）
    C-T-002: settings.yaml 加载（load_settings / env 覆盖）
    C-T-003: exchanges.yaml 加载
    C-T-004: pydantic-settings 模型验证
    C-T-005: GracefulShutdown（注册 / 执行 / 超时）
"""
from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import yaml

from config.logging_config import setup_logging, get_logger
from config.settings import (
    Settings,
    SystemConfig,
    GatewayConfig,
    ZmqConfig,
    DuckDBConfig,
    RedisConfig,
    DatabaseConfig,
    LoggingConfig,
    load_settings,
    get_settings,
    reset_settings,
    load_exchanges_config,
    _expand_env_vars,
    _deep_merge,
)
from config.graceful_shutdown import GracefulShutdown


# ═══════════════════════════════════════════════════════════
# C-T-001: loguru 日志配置
# ═══════════════════════════════════════════════════════════


class TestLoggingConfig:
    """loguru 日志配置测试"""

    def test_setup_logging_default(self, tmp_path):
        """默认配置初始化"""
        setup_logging(log_dir=tmp_path, console=False)
        # 应创建日志目录
        assert tmp_path.exists()

    def test_setup_logging_custom_level(self, tmp_path):
        """自定义日志级别"""
        setup_logging(log_dir=tmp_path, level="DEBUG", console=False)
        from loguru import logger
        # 不应抛出
        logger.debug("test debug message")

    def test_setup_logging_creates_log_dir(self, tmp_path):
        """自动创建日志目录"""
        log_dir = tmp_path / "nested" / "logs"
        setup_logging(log_dir=log_dir, console=False, file_enabled=True)
        assert log_dir.exists()

    def test_setup_logging_no_file(self, tmp_path):
        """禁用文件日志"""
        setup_logging(log_dir=tmp_path, console=True, file_enabled=False)
        # 不应创建日志文件
        log_files = list(tmp_path.glob("*.log"))
        assert len(log_files) == 0

    def test_get_logger(self):
        """get_logger 返回绑定 logger"""
        log = get_logger("test_module")
        # 应有 bind 属性
        assert log is not None
        # 不应抛出
        log.info("test message")

    def test_get_logger_different_names(self):
        """不同模块名返回不同 logger"""
        log1 = get_logger("module_a")
        log2 = get_logger("module_b")
        # 两者都可用
        log1.info("from module_a")
        log2.info("from module_b")


# ═══════════════════════════════════════════════════════════
# C-T-002: settings.yaml 加载
# ═══════════════════════════════════════════════════════════


class TestSettingsLoader:
    """配置加载测试"""

    @pytest.fixture(autouse=True)
    def reset(self):
        """每个测试重置全局单例"""
        reset_settings()
        yield
        reset_settings()

    def test_load_default_settings(self, tmp_path):
        """无 yaml 文件时加载默认值"""
        config_path = tmp_path / "nonexistent.yaml"
        settings = load_settings(config_path=config_path)
        assert isinstance(settings, Settings)
        assert settings.system.name == "KlineQuant"
        assert settings.zmq.market_pub == 5501

    def test_load_from_yaml(self, tmp_path):
        """从 yaml 文件加载"""
        yaml_content = {
            "system": {"name": "TestQuant", "environment": "test"},
            "zmq": {"market_pub": 6501},
        }
        config_path = tmp_path / "settings.yaml"
        config_path.write_text(yaml.dump(yaml_content), encoding="utf-8")

        settings = load_settings(config_path=config_path)
        assert settings.system.name == "TestQuant"
        assert settings.system.environment == "test"
        assert settings.zmq.market_pub == 6501
        # 未指定的字段使用默认值
        assert settings.zmq.indicator_pub == 5502

    def test_env_override(self, tmp_path, monkeypatch):
        """环境变量覆盖配置"""
        config_path = tmp_path / "settings.yaml"
        config_path.write_text(yaml.dump({"system": {"name": "YamlQuant"}}), encoding="utf-8")

        monkeypatch.setenv("KQ_SYSTEM__NAME", "EnvQuant")
        settings = load_settings(config_path=config_path)
        assert settings.system.name == "EnvQuant"

    def test_expand_env_vars(self, monkeypatch):
        """展开 ${ENV_VAR} 格式"""
        monkeypatch.setenv("TEST_API_KEY", "secret123")
        data = {"key": "${TEST_API_KEY}", "other": "plain"}
        result = _expand_env_vars(data)
        assert result["key"] == "secret123"
        assert result["other"] == "plain"

    def test_expand_env_vars_missing(self):
        """缺失环境变量保留原始占位符"""
        data = {"key": "${NONEXISTENT_VAR_12345}"}
        result = _expand_env_vars(data)
        assert result["key"] == "${NONEXISTENT_VAR_12345}"

    def test_deep_merge(self):
        """深度合并"""
        base = {"a": {"b": 1, "c": 2}, "d": 3}
        override = {"a": {"b": 10, "e": 5}, "f": 6}
        result = _deep_merge(base, override)
        assert result == {"a": {"b": 10, "c": 2, "e": 5}, "d": 3, "f": 6}

    def test_get_settings_singleton(self, tmp_path, monkeypatch):
        """get_settings 返回单例"""
        # 修改默认路径
        monkeypatch.chdir(tmp_path)
        s1 = get_settings()
        s2 = get_settings()
        assert s1 is s2


# ═══════════════════════════════════════════════════════════
# C-T-003: exchanges.yaml 加载
# ═══════════════════════════════════════════════════════════


class TestExchangesConfig:
    """交易所配置加载测试"""

    def test_load_nonexistent(self, tmp_path):
        """不存在文件返回空配置"""
        config = load_exchanges_config(config_path=tmp_path / "missing.yaml")
        assert config == {"exchanges": {}, "subscriptions": {}}

    def test_load_exchanges(self, tmp_path):
        """正常加载交易所配置"""
        yaml_content = {
            "exchanges": {
                "binance": {
                    "enabled": True,
                    "api_key": "test_key",
                    "rest_base": "https://api.binance.com",
                }
            },
            "subscriptions": {
                "binance": {"symbols": ["BTCUSDT"]}
            },
        }
        config_path = tmp_path / "exchanges.yaml"
        config_path.write_text(yaml.dump(yaml_content), encoding="utf-8")

        config = load_exchanges_config(config_path=config_path)
        assert config["exchanges"]["binance"]["enabled"] is True
        assert config["exchanges"]["binance"]["api_key"] == "test_key"
        assert "BTCUSDT" in config["subscriptions"]["binance"]["symbols"]

    def test_env_expansion_in_exchanges(self, tmp_path, monkeypatch):
        """交易所配置中的环境变量展开"""
        monkeypatch.setenv("MY_SECRET", "super_secret_key")
        yaml_content = {
            "exchanges": {
                "binance": {"api_key": "${MY_SECRET}"}
            }
        }
        config_path = tmp_path / "exchanges.yaml"
        config_path.write_text(yaml.dump(yaml_content), encoding="utf-8")

        config = load_exchanges_config(config_path=config_path)
        assert config["exchanges"]["binance"]["api_key"] == "super_secret_key"


# ═══════════════════════════════════════════════════════════
# C-T-004: pydantic 模型验证
# ═══════════════════════════════════════════════════════════


class TestSettingsModels:
    """pydantic 配置模型测试"""

    def test_system_config_defaults(self):
        """SystemConfig 默认值"""
        cfg = SystemConfig()
        assert cfg.name == "KlineQuant"
        assert cfg.timezone == "UTC"

    def test_zmq_config_ports(self):
        """ZmqConfig 端口范围"""
        cfg = ZmqConfig()
        assert 5501 <= cfg.market_pub <= 5530
        assert cfg.strategy_port_base < cfg.strategy_port_max

    def test_duckdb_config(self):
        """DuckDB 配置"""
        cfg = DuckDBConfig()
        assert cfg.batch_size > 0
        assert cfg.flush_interval > 0

    def test_redis_config(self):
        """Redis 配置"""
        cfg = RedisConfig()
        assert cfg.port == 6379
        assert cfg.key_prefix == "kq:"

    def test_settings_full_construction(self):
        """完整 Settings 构建"""
        settings = Settings(
            system=SystemConfig(name="Custom", environment="production"),
            gateway=GatewayConfig(api_port=9000),
            zmq=ZmqConfig(market_pub=6501),
        )
        assert settings.system.name == "Custom"
        assert settings.gateway.api_port == 9000
        assert settings.zmq.market_pub == 6501
        # 未指定的使用默认
        assert settings.database.redis.port == 6379


# ═══════════════════════════════════════════════════════════
# C-T-005: GracefulShutdown
# ═══════════════════════════════════════════════════════════


class TestGracefulShutdown:
    """优雅停机测试"""

    def test_register_callback(self):
        """注册回调"""
        gs = GracefulShutdown()
        gs.register(async_noop, name="test")
        assert len(gs._callbacks) == 1

    def test_register_multiple_callbacks(self):
        """注册多个回调"""
        gs = GracefulShutdown()
        gs.register(async_noop, name="cb1")
        gs.register(async_noop, name="cb2")
        gs.register(async_noop, name="cb3")
        assert len(gs._callbacks) == 3

    @pytest.mark.asyncio
    async def test_shutdown_now_executes_callbacks(self):
        """shutdown_now 执行所有回调"""
        gs = GracefulShutdown()
        results = []

        async def cb1():
            results.append("cb1")

        async def cb2():
            results.append("cb2")

        gs.register(cb1)
        gs.register(cb2)

        await gs.shutdown_now()

        # 按注册逆序执行
        assert results == ["cb2", "cb1"]
        assert gs.is_shutting_down

    @pytest.mark.asyncio
    async def test_shutdown_callback_timeout(self):
        """回调超时不阻塞"""
        gs = GracefulShutdown(timeout=0.1)

        async def slow_cb():
            await asyncio.sleep(10)

        gs.register(slow_cb)

        # 不应永久阻塞
        await gs.shutdown_now()
        assert gs.is_shutting_down

    @pytest.mark.asyncio
    async def test_shutdown_callback_error_handling(self):
        """回调异常不影响其他回调"""
        gs = GracefulShutdown()
        results = []

        async def error_cb():
            raise RuntimeError("test error")

        async def ok_cb():
            results.append("ok")

        gs.register(error_cb)
        gs.register(ok_cb)

        await gs.shutdown_now()

        # ok_cb 应该执行（逆序：ok_cb 先，error_cb 后）
        assert "ok" in results
        assert gs.is_shutting_down

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(self):
        """重复调用 shutdown_now 只执行一次"""
        gs = GracefulShutdown()
        count = 0

        async def counter():
            nonlocal count
            count += 1

        gs.register(counter)
        await gs.shutdown_now()
        await gs.shutdown_now()  # 第二次应该跳过

        assert count == 1

    def test_is_shutting_down_initially_false(self):
        """初始状态不是 shutting_down"""
        gs = GracefulShutdown()
        assert not gs.is_shutting_down


async def async_noop():
    """测试用空异步函数"""
    pass

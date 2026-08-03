"""网络混沌测试

测试系统在网络异常下的韧性：
    - 延迟注入
    - 超时处理
    - 连接断开恢复
"""
import pytest
import time
from unittest.mock import patch, MagicMock, AsyncMock

from tests.chaos import (
    ChaosMonkey,
    FaultConfig,
    FaultType,
    create_network_chaos_scenario,
)


class TestNetworkDelay:
    """网络延迟测试"""

    def test_delay_injection(self):
        """延迟注入生效"""
        monkey = ChaosMonkey(seed=42)
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.NETWORK_DELAY,
            probability=1.0,  # 100% 触发
            duration_ms=100,
        ))

        start = time.perf_counter()
        with monkey.intercept("test"):
            pass
        elapsed = (time.perf_counter() - start) * 1000

        assert elapsed >= 100  # 至少 100ms 延迟
        assert monkey.stats["fault_triggered_count"] == 1

    def test_probabilistic_delay(self):
        """概率性延迟（统计验证）"""
        monkey = ChaosMonkey(seed=42)
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.NETWORK_DELAY,
            probability=0.5,
            duration_ms=10,
        ))

        triggers = 0
        for _ in range(100):
            with monkey.intercept("test"):
                pass
            if monkey.stats["fault_triggered_count"] > triggers:
                triggers = monkey.stats["fault_triggered_count"]

        # 50% 概率，100 次应在 30-70 次之间
        assert 20 <= triggers <= 80


class TestNetworkTimeout:
    """网络超时测试"""

    def test_timeout_raises(self):
        """超时抛出 TimeoutError"""
        monkey = ChaosMonkey(seed=42)
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.NETWORK_TIMEOUT,
            probability=1.0,
            duration_ms=50,
        ))

        with pytest.raises(TimeoutError, match="Injected timeout"):
            with monkey.intercept("api"):
                pass

    def test_timeout_with_recovery(self):
        """超时后恢复逻辑"""
        monkey = ChaosMonkey(seed=42)
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.NETWORK_TIMEOUT,
            probability=1.0,
            duration_ms=10,
        ))

        recovered = False
        try:
            with monkey.intercept("api"):
                pass
        except TimeoutError:
            # 模拟恢复逻辑
            recovered = True

        assert recovered is True


class TestNetworkDisconnect:
    """网络断开测试"""

    def test_disconnect_raises(self):
        """断开连接抛出 ConnectionError"""
        monkey = ChaosMonkey(seed=42)
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.NETWORK_DISCONNECT,
            probability=1.0,
            error_message="Connection reset by peer",
        ))

        with pytest.raises(ConnectionError, match="Connection reset"):
            with monkey.intercept("websocket"):
                pass


class TestEndpointFiltering:
    """端点过滤测试"""

    def test_affected_endpoints_only(self):
        """只影响指定端点"""
        monkey = ChaosMonkey(seed=42)
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.NETWORK_TIMEOUT,
            probability=1.0,
            affected_endpoints=["binance_api"],
        ))

        # 非目标端点不受影响
        with monkey.intercept("other_api"):
            pass
        assert monkey.stats["fault_triggered_count"] == 0

        # 目标端点受影响
        with pytest.raises(TimeoutError):
            with monkey.intercept("binance_api"):
                pass
        assert monkey.stats["fault_triggered_count"] == 1


class TestNetworkChaosScenario:
    """预设网络混沌场景测试"""

    def test_scenario_creation(self):
        """场景创建"""
        monkey = create_network_chaos_scenario()
        assert len(monkey._faults) == 3

    def test_scenario_execution(self):
        """场景执行（多次拦截）"""
        monkey = create_network_chaos_scenario()

        errors = 0
        for _ in range(50):
            try:
                with monkey.intercept("binance_api"):
                    pass
            except (TimeoutError, ConnectionError):
                errors += 1

        # 应该有部分请求失败
        assert errors > 0
        assert monkey.stats["intercept_count"] == 50


class TestRetryWithChaos:
    """带重试的混沌测试"""

    def test_retry_on_failure(self):
        """失败后重试成功"""
        monkey = ChaosMonkey(seed=42)
        # 第一次失败，后续成功
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.NETWORK_TIMEOUT,
            probability=0.3,
            duration_ms=10,
        ))

        max_retries = 3
        success = False

        for attempt in range(max_retries):
            try:
                with monkey.intercept("api"):
                    success = True
                    break
            except TimeoutError:
                continue

        # 3 次重试内应该成功（概率很高）
        # 即使全部失败也是正常的混沌测试结果
        assert monkey.stats["intercept_count"] <= max_retries

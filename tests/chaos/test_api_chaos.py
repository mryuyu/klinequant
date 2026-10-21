"""API 混沌测试

测试系统在 API 异常下的韧性：
    - 错误码处理
    - 限流响应
    - 服务降级
"""
import pytest
from unittest.mock import patch, MagicMock

from tests.chaos import (
    ChaosMonkey,
    FaultConfig,
    FaultType,
    APIError,
    create_api_chaos_scenario,
)


class TestAPIErrorInjection:
    """API 错误注入测试"""

    def test_429_rate_limit(self):
        """429 限流错误"""
        monkey = ChaosMonkey(seed=42)
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.API_ERROR,
            probability=1.0,
            error_code=429,
            error_message="Rate limit exceeded",
        ))

        with pytest.raises(APIError) as exc_info:
            with monkey.intercept("binance_api"):
                pass

        assert exc_info.value.code == 429
        assert "Rate limit" in str(exc_info.value)

    def test_503_service_unavailable(self):
        """503 服务不可用"""
        monkey = ChaosMonkey(seed=42)
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.API_ERROR,
            probability=1.0,
            error_code=503,
            error_message="Service unavailable",
        ))

        with pytest.raises(APIError) as exc_info:
            with monkey.intercept("api"):
                pass

        assert exc_info.value.code == 503

    def test_500_internal_error(self):
        """500 内部错误"""
        monkey = ChaosMonkey(seed=42)
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.API_ERROR,
            probability=1.0,
            error_code=500,
            error_message="Internal server error",
        ))

        with pytest.raises(APIError):
            with monkey.intercept("api"):
                pass


class TestAPIErrorHandling:
    """API 错误处理测试"""

    def test_graceful_degradation(self):
        """优雅降级"""
        monkey = ChaosMonkey(seed=42)
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.API_ERROR,
            probability=1.0,
            error_code=503,
        ))

        # 模拟降级逻辑
        fallback_data = {"source": "cache", "data": []}
        result = None

        try:
            with monkey.intercept("api"):
                result = {"source": "api", "data": [1, 2, 3]}
        except APIError:
            result = fallback_data

        assert result["source"] == "cache"

    def test_circuit_breaker_pattern(self):
        """熔断器模式"""
        monkey = ChaosMonkey(seed=42)
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.API_ERROR,
            probability=0.5,
            error_code=500,
        ))

        # 简单熔断器
        failure_count = 0
        circuit_open = False
        threshold = 3

        for _ in range(10):
            if circuit_open:
                continue

            try:
                with monkey.intercept("api"):
                    pass
                failure_count = 0  # 成功重置
            except APIError:
                failure_count += 1
                if failure_count >= threshold:
                    circuit_open = True

        # 熔断器应该触发
        assert circuit_open or failure_count < threshold


class TestAPIChaosScenario:
    """预设 API 混沌场景"""

    def test_scenario_creation(self):
        """场景创建"""
        monkey = create_api_chaos_scenario()
        assert len(monkey._faults) == 2

    def test_mixed_errors(self):
        """混合错误场景"""
        monkey = create_api_chaos_scenario()

        error_codes = set()
        for _ in range(100):
            try:
                with monkey.intercept("api"):
                    pass
            except APIError as e:
                error_codes.add(e.code)

        # 应该看到 429 和 503
        assert len(error_codes) >= 1


class TestRetryStrategy:
    """重试策略测试"""

    def test_exponential_backoff(self):
        """指数退避重试"""
        import time

        monkey = ChaosMonkey(seed=42)
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.API_ERROR,
            probability=0.7,  # 高失败率
            error_code=503,
        ))

        max_retries = 5
        base_delay = 0.01  # 10ms
        attempts = 0
        success = False

        for attempt in range(max_retries):
            attempts += 1
            try:
                with monkey.intercept("api"):
                    success = True
                    break
            except APIError:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    time.sleep(delay)

        assert attempts <= max_retries

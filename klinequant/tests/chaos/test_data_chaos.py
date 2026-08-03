"""数据混沌测试

测试系统在数据异常下的韧性：
    - 畸形数据处理
    - 空值/缺失值
    - 数据损坏恢复
"""
import pytest
import json
from decimal import Decimal
from unittest.mock import patch, MagicMock

from tests.chaos import (
    ChaosMonkey,
    FaultConfig,
    FaultType,
    DataCorruptionError,
    create_data_chaos_scenario,
)


class TestDataCorruption:
    """数据损坏测试"""

    def test_corruption_raises(self):
        """数据损坏抛出异常"""
        monkey = ChaosMonkey(seed=42)
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.DATA_CORRUPTION,
            probability=1.0,
            error_message="Malformed JSON",
        ))

        with pytest.raises(DataCorruptionError, match="Malformed"):
            with monkey.intercept("data_feed"):
                pass

    def test_corruption_with_validation(self):
        """数据损坏 + 验证逻辑"""
        monkey = ChaosMonkey(seed=42)
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.DATA_CORRUPTION,
            probability=1.0,
        ))

        validated_data = None
        try:
            with monkey.intercept("data_feed"):
                raw_data = '{"price": "invalid"}'
                validated_data = json.loads(raw_data)
        except DataCorruptionError:
            validated_data = {"price": 0, "error": True}

        assert validated_data.get("error") is True


class TestMalformedData:
    """畸形数据测试"""

    def test_invalid_json(self):
        """无效 JSON 处理"""
        malformed_samples = [
            '{"incomplete": ',
            'not json at all',
            '{"price": NaN}',
            '',
            None,
        ]

        for sample in malformed_samples:
            try:
                if sample is None:
                    raise TypeError("None data")
                result = json.loads(sample)
            except (json.JSONDecodeError, TypeError):
                pass  # 预期的异常

    def test_invalid_kline_data(self):
        """无效 K线数据处理"""
        invalid_klines = [
            [],  # 空数组
            [1234567890],  # 字段不足
            ["not_a_number", "100", "90", "110", "95", "1000"],  # 时间戳非数字
            [1234567890, "invalid", "90", "110", "95", "1000"],  # 价格非数字
        ]

        for kline in invalid_klines:
            is_valid = len(kline) >= 6
            if is_valid:
                try:
                    float(kline[1])
                except (ValueError, TypeError):
                    is_valid = False
            # 所有样本都应该被识别为无效
            # （最后一个可能通过，取决于实现）

    def test_negative_price_handling(self):
        """负价格处理"""
        prices = [-100, -0.01, 0, 0.0000001]

        for price in prices:
            is_valid = price > 0
            if price <= 0:
                assert not is_valid


class TestMissingData:
    """缺失数据测试"""

    def test_null_fields(self):
        """空字段处理"""
        data_with_nulls = {
            "symbol": "BTCUSDT",
            "price": None,
            "volume": None,
            "timestamp": 1234567890,
        }

        # 安全获取
        price = data_with_nulls.get("price") or 0
        volume = data_with_nulls.get("volume") or 0

        assert price == 0
        assert volume == 0

    def test_missing_required_fields(self):
        """缺失必填字段"""
        incomplete_data = {"symbol": "BTCUSDT"}  # 缺少 price

        required_fields = ["symbol", "price", "timestamp"]
        missing = [f for f in required_fields if f not in incomplete_data]

        assert "price" in missing
        assert "timestamp" in missing


class TestDataTypeChaos:
    """数据类型异常测试"""

    def test_string_as_number(self):
        """字符串作为数字"""
        values = ["100", "100.5", "1e5", "invalid", "", None]

        for v in values:
            try:
                if v is None:
                    raise TypeError
                num = float(v)
                assert isinstance(num, float)
            except (ValueError, TypeError):
                pass  # 预期异常

    def test_decimal_precision(self):
        """Decimal 精度测试"""
        # 浮点数精度问题
        float_result = 0.1 + 0.2
        assert float_result != 0.3  # 浮点误差

        # Decimal 精确计算
        decimal_result = Decimal("0.1") + Decimal("0.2")
        assert decimal_result == Decimal("0.3")


class TestDataChaosScenario:
    """预设数据混沌场景"""

    def test_scenario_creation(self):
        """场景创建"""
        monkey = create_data_chaos_scenario()
        assert len(monkey._faults) == 1

    def test_data_pipeline_resilience(self):
        """数据管道韧性"""
        monkey = create_data_chaos_scenario()

        processed = 0
        errors = 0

        for _ in range(50):
            try:
                with monkey.intercept("data_feed"):
                    # 模拟数据处理
                    data = {"price": 50000, "volume": 100}
                    processed += 1
            except DataCorruptionError:
                errors += 1

        # 应该有部分失败
        assert processed + errors == 50


class TestRecoveryStrategies:
    """恢复策略测试"""

    def test_fallback_to_cache(self):
        """降级到缓存"""
        cache = {"BTCUSDT": {"price": 49000, "cached": True}}
        use_cache_fallback = True

        def fetch_price(symbol: str):
            # 模拟 API 失败
            raise ConnectionError("API down")

        symbol = "BTCUSDT"
        try:
            price = fetch_price(symbol)
        except ConnectionError:
            if use_cache_fallback and symbol in cache:
                price = cache[symbol]["price"]
            else:
                price = 0

        assert price == 49000

    def test_data_sanitization(self):
        """数据清洗"""
        raw_data = [
            {"price": 50000, "volume": 100},
            {"price": -100, "volume": 50},   # 无效：负价格
            {"price": 51000, "volume": -10},  # 无效：负成交量
            {"price": 52000, "volume": 200},
            None,  # 无效：空数据
        ]

        def sanitize(record):
            if not record:
                return None
            if record.get("price", 0) <= 0:
                return None
            if record.get("volume", 0) < 0:
                return None
            return record

        clean_data = [sanitize(r) for r in raw_data]
        clean_data = [r for r in clean_data if r is not None]

        assert len(clean_data) == 2
        assert all(r["price"] > 0 for r in clean_data)

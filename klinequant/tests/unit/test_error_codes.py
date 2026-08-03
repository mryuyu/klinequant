"""统一错误码体系单元测试

覆盖：
    - 错误码常量完整性
    - APIError 异常构建与序列化
    - HTTP 状态码映射
    - 异常处理器注册
"""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from gateway.errors import (
    APIError,
    ErrorCode,
    ErrorSeverity,
    register_error_handlers,
    _CODE_TO_HTTP,
    _CODE_TO_MSG,
)


class TestErrorCodeConstants:
    """错误码常量完整性"""

    def test_system_codes_range(self):
        """系统错误码在 10000-19999"""
        codes = [
            ErrorCode.ENGINE_START_FAILED,
            ErrorCode.CONFIG_INVALID,
            ErrorCode.INTERNAL_ERROR,
            ErrorCode.SERVICE_UNAVAILABLE,
            ErrorCode.TIMEOUT,
        ]
        for c in codes:
            assert 10000 <= c <= 19999

    def test_market_codes_range(self):
        """行情错误码在 20000-29999"""
        codes = [
            ErrorCode.WS_DISCONNECTED,
            ErrorCode.DATA_MISSING,
            ErrorCode.MARKET_DATA_ERROR,
            ErrorCode.INVALID_SYMBOL,
            ErrorCode.INVALID_TIMEFRAME,
        ]
        for c in codes:
            assert 20000 <= c <= 29999

    def test_trade_codes_range(self):
        """交易错误码在 30000-39999"""
        codes = [
            ErrorCode.ORDER_REJECTED,
            ErrorCode.INSUFFICIENT_BALANCE,
            ErrorCode.ORDER_NOT_FOUND,
            ErrorCode.CANCEL_FAILED,
            ErrorCode.TRADE_EXECUTION_ERROR,
            ErrorCode.EXCHANGE_API_ERROR,
        ]
        for c in codes:
            assert 30000 <= c <= 39999

    def test_risk_codes_range(self):
        """风控错误码在 40000-49999"""
        codes = [
            ErrorCode.POSITION_LIMIT_EXCEEDED,
            ErrorCode.LOSS_LIMIT_EXCEEDED,
            ErrorCode.RISK_RULE_VIOLATION,
            ErrorCode.ORDER_FREQUENCY_EXCEEDED,
            ErrorCode.RISK_ENGINE_ERROR,
        ]
        for c in codes:
            assert 40000 <= c <= 49999

    def test_strategy_codes_range(self):
        """策略错误码在 50000-59999"""
        codes = [
            ErrorCode.STRATEGY_CRASHED,
            ErrorCode.STRATEGY_TIMEOUT,
            ErrorCode.STRATEGY_NOT_FOUND,
            ErrorCode.STRATEGY_ALREADY_RUNNING,
            ErrorCode.STRATEGY_LOAD_FAILED,
        ]
        for c in codes:
            assert 50000 <= c <= 59999

    def test_data_codes_range(self):
        """数据错误码在 60000-69999"""
        codes = [
            ErrorCode.DB_CONNECTION_FAILED,
            ErrorCode.DATA_INCONSISTENCY,
            ErrorCode.DATA_FETCH_FAILED,
            ErrorCode.BACKTEST_DATA_ERROR,
        ]
        for c in codes:
            assert 60000 <= c <= 69999

    def test_all_codes_have_http_mapping(self):
        """所有错误码都有 HTTP 状态码映射"""
        all_codes = [
            v for k, v in vars(ErrorCode).items()
            if not k.startswith("_") and isinstance(v, int)
        ]
        for code in all_codes:
            assert code in _CODE_TO_HTTP, f"Missing HTTP mapping for code {code}"

    def test_all_codes_have_message(self):
        """所有错误码都有默认消息"""
        all_codes = [
            v for k, v in vars(ErrorCode).items()
            if not k.startswith("_") and isinstance(v, int)
        ]
        for code in all_codes:
            assert code in _CODE_TO_MSG, f"Missing message for code {code}"


class TestAPIError:
    """APIError 异常类"""

    def test_basic_creation(self):
        """基本创建"""
        err = APIError(ErrorCode.ORDER_REJECTED)
        assert err.code == 30001
        assert err.detail == "下单被拒绝"
        assert err.severity == ErrorSeverity.WARNING
        assert err.http_status == 422

    def test_custom_detail(self):
        """自定义详情"""
        err = APIError(ErrorCode.INSUFFICIENT_BALANCE, detail="BTCUSDT 余额不足")
        assert err.detail == "BTCUSDT 余额不足"

    def test_severity_levels(self):
        """严重级别"""
        fatal = APIError(ErrorCode.DB_CONNECTION_FAILED, severity=ErrorSeverity.FATAL)
        assert fatal.severity == ErrorSeverity.FATAL

        info = APIError(ErrorCode.STRATEGY_NOT_FOUND, severity=ErrorSeverity.INFO)
        assert info.severity == ErrorSeverity.INFO

    def test_to_response_structure(self):
        """响应体结构"""
        err = APIError(
            ErrorCode.POSITION_LIMIT_EXCEEDED,
            detail="BTC 持仓超过 10 BTC",
            severity=ErrorSeverity.CRITICAL,
            data={"current": 12.5, "limit": 10},
        )
        resp = err.to_response()
        assert "error" in resp
        assert resp["error"]["code"] == 40001
        assert resp["error"]["message"] == "BTC 持仓超过 10 BTC"
        assert resp["error"]["severity"] == "CRITICAL"
        assert resp["error"]["data"]["current"] == 12.5
        assert "timestamp" in resp["error"]

    def test_to_response_without_data(self):
        """无附加数据时不包含 data 字段"""
        err = APIError(ErrorCode.TIMEOUT)
        resp = err.to_response()
        assert "data" not in resp["error"]

    def test_http_status_mapping(self):
        """HTTP 状态码映射正确"""
        assert APIError(ErrorCode.CONFIG_INVALID).http_status == 400
        assert APIError(ErrorCode.ORDER_NOT_FOUND).http_status == 404
        assert APIError(ErrorCode.STRATEGY_ALREADY_RUNNING).http_status == 409
        assert APIError(ErrorCode.ORDER_FREQUENCY_EXCEEDED).http_status == 429
        assert APIError(ErrorCode.INTERNAL_ERROR).http_status == 500
        assert APIError(ErrorCode.WS_DISCONNECTED).http_status == 503
        assert APIError(ErrorCode.TIMEOUT).http_status == 504

    def test_exception_inheritance(self):
        """继承自 Exception"""
        err = APIError(ErrorCode.INTERNAL_ERROR)
        assert isinstance(err, Exception)
        assert str(err) == "系统内部错误"


class TestErrorHandlers:
    """FastAPI 异常处理器集成"""

    @pytest.fixture
    def client(self):
        app = FastAPI()
        register_error_handlers(app)

        @app.get("/test-api-error")
        async def raise_api_error():
            raise APIError(
                ErrorCode.STRATEGY_NOT_FOUND,
                detail="策略 dual_ma_v2 不存在",
                severity=ErrorSeverity.WARNING,
            )

        @app.get("/test-unhandled")
        async def raise_unhandled():
            raise ValueError("unexpected")

        return TestClient(app, raise_server_exceptions=False)

    def test_api_error_response(self, client):
        """APIError 返回标准格式"""
        resp = client.get("/test-api-error")
        assert resp.status_code == 404
        body = resp.json()
        assert body["error"]["code"] == 50003
        assert "dual_ma_v2" in body["error"]["message"]
        assert body["error"]["severity"] == "WARNING"

    def test_unhandled_error_response(self, client):
        """未捕获异常返回 10003"""
        resp = client.get("/test-unhandled")
        assert resp.status_code == 500
        body = resp.json()
        assert body["error"]["code"] == 10003
        assert body["error"]["severity"] == "CRITICAL"

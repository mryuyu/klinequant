"""统一错误码体系

遵循需求文档 §8.1 错误码分段 + §8.2 异常分级。

错误码范围：
    10000-19999  系统错误
    20000-29999  行情错误
    30000-39999  交易错误
    40000-49999  风控错误
    50000-59999  策略错误
    60000-69999  数据错误

异常分级：
    FATAL / CRITICAL / WARNING / INFO
"""
from __future__ import annotations

import logging
import time
from enum import Enum
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


# ─── 异常分级（§8.2）───


class ErrorSeverity(str, Enum):
    FATAL = "FATAL"
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"


# ─── 错误码定义（§8.1）───


class ErrorCode:
    """错误码常量"""

    # 系统错误 10000-19999
    ENGINE_START_FAILED = 10001
    CONFIG_INVALID = 10002
    INTERNAL_ERROR = 10003
    SERVICE_UNAVAILABLE = 10004
    TIMEOUT = 10005

    # 行情错误 20000-29999
    WS_DISCONNECTED = 20001
    DATA_MISSING = 20002
    MARKET_DATA_ERROR = 20003
    INVALID_SYMBOL = 20004
    INVALID_TIMEFRAME = 20005

    # 交易错误 30000-39999
    ORDER_REJECTED = 30001
    INSUFFICIENT_BALANCE = 30002
    ORDER_NOT_FOUND = 30003
    CANCEL_FAILED = 30004
    TRADE_EXECUTION_ERROR = 30005
    EXCHANGE_API_ERROR = 30006

    # 风控错误 40000-49999
    POSITION_LIMIT_EXCEEDED = 40001
    LOSS_LIMIT_EXCEEDED = 40002
    RISK_RULE_VIOLATION = 40003
    ORDER_FREQUENCY_EXCEEDED = 40004
    RISK_ENGINE_ERROR = 40005

    # 策略错误 50000-59999
    STRATEGY_CRASHED = 50001
    STRATEGY_TIMEOUT = 50002
    STRATEGY_NOT_FOUND = 50003
    STRATEGY_ALREADY_RUNNING = 50004
    STRATEGY_LOAD_FAILED = 50005

    # 数据错误 60000-69999
    DB_CONNECTION_FAILED = 60001
    DATA_INCONSISTENCY = 60002
    DATA_FETCH_FAILED = 60003
    BACKTEST_DATA_ERROR = 60004


# 错误码 → HTTP 状态码映射
_CODE_TO_HTTP: dict[int, int] = {
    # 系统
    ErrorCode.ENGINE_START_FAILED: 500,
    ErrorCode.CONFIG_INVALID: 400,
    ErrorCode.INTERNAL_ERROR: 500,
    ErrorCode.SERVICE_UNAVAILABLE: 503,
    ErrorCode.TIMEOUT: 504,
    # 行情
    ErrorCode.WS_DISCONNECTED: 503,
    ErrorCode.DATA_MISSING: 404,
    ErrorCode.MARKET_DATA_ERROR: 502,
    ErrorCode.INVALID_SYMBOL: 400,
    ErrorCode.INVALID_TIMEFRAME: 400,
    # 交易
    ErrorCode.ORDER_REJECTED: 422,
    ErrorCode.INSUFFICIENT_BALANCE: 422,
    ErrorCode.ORDER_NOT_FOUND: 404,
    ErrorCode.CANCEL_FAILED: 422,
    ErrorCode.TRADE_EXECUTION_ERROR: 500,
    ErrorCode.EXCHANGE_API_ERROR: 502,
    # 风控
    ErrorCode.POSITION_LIMIT_EXCEEDED: 403,
    ErrorCode.LOSS_LIMIT_EXCEEDED: 403,
    ErrorCode.RISK_RULE_VIOLATION: 403,
    ErrorCode.ORDER_FREQUENCY_EXCEEDED: 429,
    ErrorCode.RISK_ENGINE_ERROR: 500,
    # 策略
    ErrorCode.STRATEGY_CRASHED: 500,
    ErrorCode.STRATEGY_TIMEOUT: 504,
    ErrorCode.STRATEGY_NOT_FOUND: 404,
    ErrorCode.STRATEGY_ALREADY_RUNNING: 409,
    ErrorCode.STRATEGY_LOAD_FAILED: 500,
    # 数据
    ErrorCode.DB_CONNECTION_FAILED: 503,
    ErrorCode.DATA_INCONSISTENCY: 500,
    ErrorCode.DATA_FETCH_FAILED: 502,
    ErrorCode.BACKTEST_DATA_ERROR: 422,
}

# 错误码 → 默认消息
_CODE_TO_MSG: dict[int, str] = {
    ErrorCode.ENGINE_START_FAILED: "引擎启动失败",
    ErrorCode.CONFIG_INVALID: "配置无效",
    ErrorCode.INTERNAL_ERROR: "系统内部错误",
    ErrorCode.SERVICE_UNAVAILABLE: "服务不可用",
    ErrorCode.TIMEOUT: "请求超时",
    ErrorCode.WS_DISCONNECTED: "WebSocket 连接断开",
    ErrorCode.DATA_MISSING: "数据缺失",
    ErrorCode.MARKET_DATA_ERROR: "行情数据获取失败",
    ErrorCode.INVALID_SYMBOL: "无效交易对",
    ErrorCode.INVALID_TIMEFRAME: "无效时间周期",
    ErrorCode.ORDER_REJECTED: "下单被拒绝",
    ErrorCode.INSUFFICIENT_BALANCE: "余额不足",
    ErrorCode.ORDER_NOT_FOUND: "订单不存在",
    ErrorCode.CANCEL_FAILED: "撤单失败",
    ErrorCode.TRADE_EXECUTION_ERROR: "交易执行错误",
    ErrorCode.EXCHANGE_API_ERROR: "交易所 API 错误",
    ErrorCode.POSITION_LIMIT_EXCEEDED: "持仓超限",
    ErrorCode.LOSS_LIMIT_EXCEEDED: "亏损超限",
    ErrorCode.RISK_RULE_VIOLATION: "风控规则触发",
    ErrorCode.ORDER_FREQUENCY_EXCEEDED: "下单频率超限",
    ErrorCode.RISK_ENGINE_ERROR: "风控引擎错误",
    ErrorCode.STRATEGY_CRASHED: "策略崩溃",
    ErrorCode.STRATEGY_TIMEOUT: "策略超时",
    ErrorCode.STRATEGY_NOT_FOUND: "策略不存在",
    ErrorCode.STRATEGY_ALREADY_RUNNING: "策略已在运行",
    ErrorCode.STRATEGY_LOAD_FAILED: "策略加载失败",
    ErrorCode.DB_CONNECTION_FAILED: "数据库连接失败",
    ErrorCode.DATA_INCONSISTENCY: "数据不一致",
    ErrorCode.DATA_FETCH_FAILED: "数据获取失败",
    ErrorCode.BACKTEST_DATA_ERROR: "回测数据错误",
}


# ─── 自定义异常 ───


class APIError(Exception):
    """统一 API 异常

    Usage:
        raise APIError(ErrorCode.ORDER_REJECTED, detail="BTCUSDT 下单被拒: 余额不足")
        raise APIError(ErrorCode.STRATEGY_NOT_FOUND, severity=ErrorSeverity.WARNING)
    """

    def __init__(
        self,
        code: int,
        detail: Optional[str] = None,
        severity: ErrorSeverity = ErrorSeverity.WARNING,
        data: Optional[dict[str, Any]] = None,
    ):
        self.code = code
        self.detail = detail or _CODE_TO_MSG.get(code, "Unknown error")
        self.severity = severity
        self.data = data
        self.timestamp = int(time.time() * 1000)
        super().__init__(self.detail)

    @property
    def http_status(self) -> int:
        return _CODE_TO_HTTP.get(self.code, 500)

    def to_response(self) -> dict[str, Any]:
        """生成标准错误响应体"""
        resp: dict[str, Any] = {
            "error": {
                "code": self.code,
                "message": self.detail,
                "severity": self.severity.value,
                "timestamp": self.timestamp,
            }
        }
        if self.data:
            resp["error"]["data"] = self.data
        return resp


# ─── FastAPI 异常处理器 ───


def register_error_handlers(app: FastAPI) -> None:
    """注册全局异常处理器"""

    @app.exception_handler(APIError)
    async def api_error_handler(request: Request, exc: APIError) -> JSONResponse:
        # 根据严重级别记录日志
        log_msg = f"[{exc.severity.value}] code={exc.code} {exc.detail}"
        if exc.severity == ErrorSeverity.FATAL:
            logger.critical(log_msg)
        elif exc.severity == ErrorSeverity.CRITICAL:
            logger.error(log_msg)
        elif exc.severity == ErrorSeverity.WARNING:
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

        return JSONResponse(
            status_code=exc.http_status,
            content=exc.to_response(),
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        """兜底：未捕获异常统一返回 10003"""
        logger.exception(f"Unhandled error: {exc}")
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": ErrorCode.INTERNAL_ERROR,
                    "message": "系统内部错误",
                    "severity": ErrorSeverity.CRITICAL.value,
                    "timestamp": int(time.time() * 1000),
                }
            },
        )

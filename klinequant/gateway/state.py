"""Gateway 共享应用状态

管理所有引擎实例的生命周期，供各路由模块使用。
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger(__name__)

# 币安配置
BINANCE_REST_BASE = os.getenv("BINANCE_REST_BASE", "https://api.binance.com")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
BINANCE_TESTNET = os.getenv("BINANCE_TESTNET", "false").lower() == "true"
HTTP_PROXY = os.getenv("HTTP_PROXY", "http://127.0.0.1:7897")


class AppState:
    """应用全局状态（单例）"""

    def __init__(self):
        self.start_time = time.time()
        # AlertManager 实例（延迟初始化）
        self._alert_manager = None
        # StrategyManager 实例（延迟初始化）
        self._strategy_manager = None
        # RiskEngine 实例（延迟初始化）
        self._risk_engine = None
        # 币安 REST 客户端
        self._http_client: Optional[httpx.AsyncClient] = None

    @property
    def uptime_seconds(self) -> float:
        return time.time() - self.start_time

    @property
    def alert_manager(self):
        """延迟初始化 AlertManager"""
        if self._alert_manager is None:
            from core.notification.alert_manager import AlertManager
            self._alert_manager = AlertManager()
            self._alert_manager.setup_default_rules()
        return self._alert_manager

    @property
    def strategy_manager(self):
        """延迟初始化 StrategyManager，并注册内置策略"""
        if self._strategy_manager is None:
            from core.strategy_engine.manager import StrategyManager
            from strategies.dual_ma import DualMAStrategy
            self._strategy_manager = StrategyManager()
            self._strategy_manager.register_strategy("dual_ma", DualMAStrategy)
        return self._strategy_manager

    @property
    def risk_engine(self):
        """延迟初始化 RiskEngine（默认 12 条规则）"""
        if self._risk_engine is None:
            from core.risk_engine.engine import RiskEngine
            self._risk_engine = RiskEngine()
            self._risk_engine.start()
        return self._risk_engine

    def get_http_client(self) -> httpx.AsyncClient:
        """获取带代理的 HTTP 客户端（复用连接）"""
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(
                proxy=HTTP_PROXY if HTTP_PROXY else None,
                timeout=10.0,
            )
        return self._http_client

    async def close(self):
        """关闭资源"""
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()


# 全局单例
state = AppState()

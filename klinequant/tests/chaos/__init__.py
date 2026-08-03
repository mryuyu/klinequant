"""混沌测试框架

故障注入测试，验证系统韧性：
    - 网络故障注入（延迟/丢包/断连）
    - 服务故障模拟（API 错误/超时）
    - 数据异常测试（畸形数据/空值）
    - 资源耗尽模拟（内存/磁盘）

用法：
    # 运行混沌测试
    pytest tests/chaos/ -v

    # 单独运行网络故障测试
    pytest tests/chaos/test_network_chaos.py -v

遵循需求文档 §15.3 混沌工程实践。
"""
from __future__ import annotations

import asyncio
import random
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional
from unittest.mock import patch, MagicMock


class FaultType(Enum):
    """故障类型"""
    NETWORK_DELAY = "network_delay"       # 网络延迟
    NETWORK_TIMEOUT = "network_timeout"   # 网络超时
    NETWORK_DISCONNECT = "network_disconnect"  # 网络断开
    API_ERROR = "api_error"               # API 错误
    DATA_CORRUPTION = "data_corruption"   # 数据损坏
    RESOURCE_EXHAUST = "resource_exhaust" # 资源耗尽
    LATENCY_SPIKE = "latency_spike"       # 延迟尖峰


@dataclass
class FaultConfig:
    """故障配置"""
    fault_type: FaultType
    probability: float = 1.0      # 触发概率 (0-1)
    duration_ms: int = 0          # 持续时间（延迟类）
    error_code: int = 500         # 错误码（API 错误类）
    error_message: str = "Injected fault"
    affected_endpoints: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


class ChaosMonkey:
    """混沌猴子 - 故障注入器

    用于测试系统在异常情况下的行为。

    用法：
        monkey = ChaosMonkey()
        monkey.add_fault(FaultConfig(
            fault_type=FaultType.NETWORK_DELAY,
            probability=0.5,
            duration_ms=1000,
        ))

        with monkey.intercept("api.binance.com"):
            # 这里的网络调用可能被注入故障
            response = requests.get("https://api.binance.com/api/v3/klines")
    """

    def __init__(self, seed: Optional[int] = None):
        self._faults: list[FaultConfig] = []
        self._rng = random.Random(seed)
        self._intercept_count = 0
        self._fault_triggered_count = 0
        self._active = False
        self._history: list[dict] = []

    def add_fault(self, config: FaultConfig) -> "ChaosMonkey":
        """添加故障配置"""
        self._faults.append(config)
        return self

    def clear_faults(self) -> None:
        """清除所有故障配置"""
        self._faults.clear()

    @property
    def stats(self) -> dict:
        """获取统计信息"""
        return {
            "intercept_count": self._intercept_count,
            "fault_triggered_count": self._fault_triggered_count,
            "faults_configured": len(self._faults),
            "history": self._history[-10:],  # 最近 10 条
        }

    def should_trigger(self, config: FaultConfig) -> bool:
        """判断是否触发故障"""
        return self._rng.random() < config.probability

    @contextmanager
    def intercept(self, target: str = "*"):
        """拦截上下文

        Args:
            target: 目标标识（如域名、服务名）
        """
        self._active = True
        self._intercept_count += 1

        try:
            # 检查是否有匹配的故障
            for fault in self._faults:
                # 检查端点匹配
                if fault.affected_endpoints and target not in fault.affected_endpoints:
                    continue

                if self.should_trigger(fault):
                    self._fault_triggered_count += 1
                    self._record_trigger(fault, target)
                    self._apply_fault(fault)
                    break

            yield self

        finally:
            self._active = False

    def _record_trigger(self, fault: FaultConfig, target: str) -> None:
        """记录故障触发"""
        self._history.append({
            "timestamp": time.time(),
            "fault_type": fault.fault_type.value,
            "target": target,
            "config": {
                "probability": fault.probability,
                "duration_ms": fault.duration_ms,
                "error_code": fault.error_code,
            },
        })

    def _apply_fault(self, fault: FaultConfig) -> None:
        """应用故障"""
        if fault.fault_type == FaultType.NETWORK_DELAY:
            time.sleep(fault.duration_ms / 1000)

        elif fault.fault_type == FaultType.NETWORK_TIMEOUT:
            raise TimeoutError(f"Injected timeout after {fault.duration_ms}ms")

        elif fault.fault_type == FaultType.NETWORK_DISCONNECT:
            raise ConnectionError(f"Injected disconnect: {fault.error_message}")

        elif fault.fault_type == FaultType.API_ERROR:
            raise APIError(fault.error_code, fault.error_message)

        elif fault.fault_type == FaultType.DATA_CORRUPTION:
            raise DataCorruptionError(fault.error_message)

        elif fault.fault_type == FaultType.RESOURCE_EXHAUST:
            raise MemoryError(fault.error_message)

        elif fault.fault_type == FaultType.LATENCY_SPIKE:
            # 随机延迟尖峰
            spike = self._rng.randint(fault.duration_ms, fault.duration_ms * 3)
            time.sleep(spike / 1000)


class APIError(Exception):
    """API 错误"""
    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super().__init__(f"API Error {code}: {message}")


class DataCorruptionError(Exception):
    """数据损坏错误"""
    pass


# ─── 异步版本 ───

class AsyncChaosMonkey:
    """异步混沌猴子"""

    def __init__(self, seed: Optional[int] = None):
        self._faults: list[FaultConfig] = []
        self._rng = random.Random(seed)
        self._stats = {"intercepts": 0, "triggers": 0}

    def add_fault(self, config: FaultConfig) -> "AsyncChaosMonkey":
        self._faults.append(config)
        return self

    async def intercept(self, target: str = "*") -> None:
        """异步拦截"""
        self._stats["intercepts"] += 1

        for fault in self._faults:
            if fault.affected_endpoints and target not in fault.affected_endpoints:
                continue

            if self._rng.random() < fault.probability:
                self._stats["triggers"] += 1
                await self._apply_fault(fault)
                break

    async def _apply_fault(self, fault: FaultConfig) -> None:
        if fault.fault_type == FaultType.NETWORK_DELAY:
            await asyncio.sleep(fault.duration_ms / 1000)

        elif fault.fault_type == FaultType.NETWORK_TIMEOUT:
            await asyncio.sleep(fault.duration_ms / 1000)
            raise asyncio.TimeoutError(f"Injected timeout: {fault.error_message}")

        elif fault.fault_type == FaultType.NETWORK_DISCONNECT:
            raise ConnectionError(fault.error_message)

        elif fault.fault_type == FaultType.API_ERROR:
            raise APIError(fault.error_code, fault.error_message)


# ─── 测试场景预设 ───

def create_network_chaos_scenario() -> ChaosMonkey:
    """创建网络故障场景"""
    return (
        ChaosMonkey(seed=42)
        .add_fault(FaultConfig(
            fault_type=FaultType.NETWORK_DELAY,
            probability=0.3,
            duration_ms=500,
            affected_endpoints=["binance_api"],
        ))
        .add_fault(FaultConfig(
            fault_type=FaultType.NETWORK_TIMEOUT,
            probability=0.1,
            duration_ms=5000,
            affected_endpoints=["binance_api"],
        ))
        .add_fault(FaultConfig(
            fault_type=FaultType.NETWORK_DISCONNECT,
            probability=0.05,
            error_message="Connection reset by peer",
        ))
    )


def create_api_chaos_scenario() -> ChaosMonkey:
    """创建 API 故障场景"""
    return (
        ChaosMonkey(seed=42)
        .add_fault(FaultConfig(
            fault_type=FaultType.API_ERROR,
            probability=0.2,
            error_code=429,
            error_message="Rate limit exceeded",
        ))
        .add_fault(FaultConfig(
            fault_type=FaultType.API_ERROR,
            probability=0.1,
            error_code=503,
            error_message="Service unavailable",
        ))
    )


def create_data_chaos_scenario() -> ChaosMonkey:
    """创建数据异常场景"""
    return (
        ChaosMonkey(seed=42)
        .add_fault(FaultConfig(
            fault_type=FaultType.DATA_CORRUPTION,
            probability=0.1,
            error_message="Malformed JSON response",
        ))
    )

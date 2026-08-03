"""StrategySandbox — 策略进程沙箱

使用 multiprocessing spawn 模式隔离策略进程：
    - 策略崩溃不影响主进程
    - 策略异常自动捕获并上报
    - Windows 适配（spawn 模式）

遵循需求文档 §4.6 STR-003。
"""
from __future__ import annotations

import logging
import multiprocessing as mp
import traceback
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)


class SandboxStatus(str, Enum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    CRASHED = "CRASHED"


@dataclass
class SandboxResult:
    """沙箱执行结果"""

    strategy_id: str
    status: SandboxStatus
    error: Optional[str] = None
    data: Optional[Dict[str, Any]] = None


def _strategy_worker(
    strategy_id: str,
    target_fn: Callable,
    args: tuple,
    kwargs: dict,
    result_queue: mp.Queue,
) -> None:
    """策略工作进程入口

    在独立进程中执行策略函数，捕获所有异常。
    """
    try:
        result = target_fn(*args, **kwargs)
        result_queue.put(SandboxResult(
            strategy_id=strategy_id,
            status=SandboxStatus.STOPPED,
            data={"result": result},
        ))
    except Exception as e:
        tb = traceback.format_exc()
        result_queue.put(SandboxResult(
            strategy_id=strategy_id,
            status=SandboxStatus.CRASHED,
            error=f"{type(e).__name__}: {e}\n{tb}",
        ))


class StrategySandbox:
    """策略进程沙箱

    在独立进程中运行策略，隔离崩溃风险。
    使用 spawn 模式（Windows 兼容）。
    """

    def __init__(self, strategy_id: str):
        self._strategy_id = strategy_id
        self._process: Optional[mp.Process] = None
        self._result_queue: Optional[mp.Queue] = None
        self._status = SandboxStatus.IDLE

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    @property
    def status(self) -> SandboxStatus:
        return self._status

    @property
    def is_alive(self) -> bool:
        return self._process is not None and self._process.is_alive()

    def start(
        self,
        target_fn: Callable,
        args: tuple = (),
        kwargs: Optional[dict] = None,
    ) -> None:
        """启动策略进程

        Args:
            target_fn: 策略入口函数（必须可 pickle）
            args: 位置参数
            kwargs: 关键字参数
        """
        if self.is_alive:
            logger.warning(f"Sandbox {self._strategy_id} already running")
            return

        ctx = mp.get_context("spawn")
        self._result_queue = ctx.Queue()
        self._process = ctx.Process(
            target=_strategy_worker,
            args=(self._strategy_id, target_fn, args, kwargs or {}, self._result_queue),
            name=f"strategy-{self._strategy_id}",
            daemon=True,
        )
        self._process.start()
        self._status = SandboxStatus.RUNNING
        logger.info(f"Sandbox {self._strategy_id} started (PID={self._process.pid})")

    def stop(self, timeout: float = 5.0) -> SandboxResult:
        """停止策略进程"""
        if self._process is None:
            return SandboxResult(
                strategy_id=self._strategy_id,
                status=SandboxStatus.IDLE,
            )

        if self._process.is_alive():
            self._process.terminate()
            self._process.join(timeout=timeout)
            if self._process.is_alive():
                self._process.kill()
                self._process.join(timeout=1.0)

        self._status = SandboxStatus.STOPPED

        # 尝试获取结果
        result = self._try_get_result()
        if result is None:
            result = SandboxResult(
                strategy_id=self._strategy_id,
                status=SandboxStatus.STOPPED,
            )
        return result

    def poll(self, timeout: float = 0.1) -> Optional[SandboxResult]:
        """非阻塞检查结果"""
        if self._result_queue is None:
            return None
        try:
            result = self._result_queue.get(timeout=timeout)
            self._status = result.status
            return result
        except Exception:
            return None

    def _try_get_result(self) -> Optional[SandboxResult]:
        """尝试从队列获取结果"""
        if self._result_queue is None:
            return None
        try:
            return self._result_queue.get_nowait()
        except Exception:
            return None

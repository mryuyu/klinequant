"""GracefulShutdown — 优雅停机管理

Windows 适配的信号处理：
    - SIGINT (Ctrl+C)
    - SIGBREAK (Ctrl+Break, Windows 特有)
    - 注册多个回调，按注册顺序逆序执行
    - 超时强制退出
"""
from __future__ import annotations

import asyncio
import signal
import sys
from typing import Awaitable, Callable, List, Optional

from loguru import logger


ShutdownCallback = Callable[[], Awaitable[None]]


class GracefulShutdown:
    """优雅停机管理器。

    用法：
        shutdown = GracefulShutdown()
        shutdown.register(my_cleanup_func)
        shutdown.install()  # 注册信号处理器
        # ... 主循环 ...
        # 收到 SIGINT/SIGBREAK 后自动执行回调
    """

    def __init__(self, timeout: float = 10.0):
        """
        Args:
            timeout: 停机超时（秒），超时后强制退出
        """
        self._callbacks: List[ShutdownCallback] = []
        self._timeout = timeout
        self._shutting_down = False
        self._original_handlers: dict = {}

    @property
    def is_shutting_down(self) -> bool:
        return self._shutting_down

    def register(self, callback: ShutdownCallback, name: Optional[str] = None) -> None:
        """注册停机回调。

        Args:
            callback: 异步回调函数
            name: 回调名称（用于日志）
        """
        self._callbacks.append(callback)
        logger.debug(f"Shutdown callback registered: {name or callback.__name__}")

    def install(self) -> None:
        """安装信号处理器。"""
        # SIGINT (Ctrl+C)
        self._original_handlers[signal.SIGINT] = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self._handle_signal)

        # Windows: SIGBREAK (Ctrl+Break)
        if sys.platform == "win32":
            try:
                self._original_handlers[signal.SIGBREAK] = signal.getsignal(signal.SIGBREAK)
                signal.signal(signal.SIGBREAK, self._handle_signal)
            except (AttributeError, ValueError):
                pass

        logger.debug("Graceful shutdown handlers installed")

    def uninstall(self) -> None:
        """恢复原始信号处理器。"""
        for sig, handler in self._original_handlers.items():
            try:
                signal.signal(sig, handler)
            except (OSError, ValueError):
                pass
        self._original_handlers.clear()

    def _handle_signal(self, signum: int, frame) -> None:
        """信号处理入口。"""
        sig_name = signal.Signals(signum).name
        if self._shutting_down:
            logger.warning(f"Received {sig_name} again, forcing exit...")
            sys.exit(1)

        logger.info(f"Received {sig_name}, initiating graceful shutdown...")
        self._shutting_down = True

        # 在事件循环中执行异步回调
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._execute_callbacks())
        except RuntimeError:
            # 没有运行中的事件循环，同步执行
            asyncio.run(self._execute_callbacks())

    async def _execute_callbacks(self) -> None:
        """按注册逆序执行所有回调。"""
        for callback in reversed(self._callbacks):
            name = getattr(callback, "__name__", str(callback))
            try:
                logger.debug(f"Executing shutdown callback: {name}")
                await asyncio.wait_for(callback(), timeout=self._timeout)
            except asyncio.TimeoutError:
                logger.warning(f"Shutdown callback timeout: {name}")
            except Exception as e:
                logger.error(f"Shutdown callback error: {name}: {e}")

        logger.info("Graceful shutdown completed")

    async def shutdown_now(self) -> None:
        """手动触发停机（不依赖信号）。"""
        if not self._shutting_down:
            self._shutting_down = True
            await self._execute_callbacks()

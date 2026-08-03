"""StrategyHotLoader — 策略热加载器

支持运行中新增/更新策略无需重启系统：
    - 目录监控：监控策略文件夹变更（新增/修改/删除）
    - 动态加载：通过 importlib 动态加载策略模块
    - 热替换：保持策略状态，替换策略逻辑
    - 版本管理：记录加载历史，支持回滚
    - 安全校验：加载前验证策略类合法性

遵循需求文档 §4.6 STR-004。
"""
from __future__ import annotations

import hashlib
import importlib
import importlib.util
import inspect
import logging
import sys
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Type

from core.strategy_engine.base import StrategyBase
from core.strategy_engine.context import StrategyInfo
from core.strategy_engine.manager import StrategyManager, StrategyStatus

logger = logging.getLogger(__name__)


class LoadStatus(str, Enum):
    """加载状态"""
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PENDING = "PENDING"
    ROLLED_BACK = "ROLLED_BACK"


@dataclass
class StrategyModuleInfo:
    """策略模块信息"""
    module_name: str
    file_path: str
    class_name: str
    strategy_cls: Type[StrategyBase]
    file_hash: str
    loaded_at: int
    version: int = 1
    status: LoadStatus = LoadStatus.SUCCESS
    error: Optional[str] = None


@dataclass
class HotLoadEvent:
    """热加载事件"""
    event_type: str  # "loaded" / "reloaded" / "unloaded" / "error"
    strategy_name: str
    file_path: str
    timestamp: int
    version: int = 1
    message: str = ""


class StrategyHotLoader:
    """策略热加载器

    功能：
        1. 从指定目录动态加载策略文件
        2. 监控文件变更并自动重载
        3. 保持策略运行状态的热替换
        4. 加载历史记录与回滚

    使用方式：
        loader = StrategyHotLoader(manager, strategy_dir="./strategies")
        loader.load_all()           # 初始加载
        loader.start_watching()     # 启动文件监控
        loader.stop_watching()      # 停止监控
    """

    def __init__(
        self,
        manager: StrategyManager,
        strategy_dir: str = "./strategies",
        watch_interval: float = 2.0,
        auto_reload: bool = True,
    ):
        self._manager = manager
        self._strategy_dir = Path(strategy_dir)
        self._watch_interval = watch_interval
        self._auto_reload = auto_reload

        # module_name -> StrategyModuleInfo
        self._modules: Dict[str, StrategyModuleInfo] = {}
        # module_name -> 历史版本列表
        self._history: Dict[str, List[StrategyModuleInfo]] = {}
        # 事件回调
        self._event_callbacks: List[Callable[[HotLoadEvent], None]] = []
        # 文件 hash 缓存
        self._file_hashes: Dict[str, str] = {}

        # 监控线程
        self._watch_thread: Optional[threading.Thread] = None
        self._watching = False
        self._lock = threading.Lock()

    @property
    def loaded_modules(self) -> Dict[str, StrategyModuleInfo]:
        """已加载的策略模块"""
        return dict(self._modules)

    @property
    def strategy_dir(self) -> Path:
        return self._strategy_dir

    # ─── 事件系统 ───

    def on_event(self, callback: Callable[[HotLoadEvent], None]) -> None:
        """注册事件回调"""
        self._event_callbacks.append(callback)

    def _emit_event(self, event: HotLoadEvent) -> None:
        """触发事件"""
        for cb in self._event_callbacks:
            try:
                cb(event)
            except Exception as e:
                logger.error(f"Event callback error: {e}")

    # ─── 加载 ───

    def load_all(self) -> Dict[str, LoadStatus]:
        """加载目录下所有策略文件

        Returns:
            {文件名: 加载状态}
        """
        results: Dict[str, LoadStatus] = {}
        if not self._strategy_dir.exists():
            logger.warning(f"Strategy dir not found: {self._strategy_dir}")
            return results

        for py_file in self._strategy_dir.glob("*.py"):
            if py_file.name.startswith("_"):
                continue
            status = self.load_file(py_file)
            results[py_file.name] = status

        logger.info(f"Loaded {sum(1 for s in results.values() if s == LoadStatus.SUCCESS)}/{len(results)} strategies")
        return results

    def load_file(self, file_path: Path) -> LoadStatus:
        """加载单个策略文件

        Args:
            file_path: 策略文件路径

        Returns:
            加载状态
        """
        file_path = Path(file_path)
        module_name = file_path.stem

        try:
            # 计算文件 hash
            file_hash = self._compute_hash(file_path)

            # 动态加载模块
            strategy_cls = self._import_strategy_class(file_path)

            if strategy_cls is None:
                raise ValueError(f"No StrategyBase subclass found in {file_path.name}")

            # 验证策略类
            self._validate_strategy_class(strategy_cls)

            # 记录版本
            prev_info = self._modules.get(module_name)
            version = (prev_info.version + 1) if prev_info else 1

            # 创建模块信息
            info = StrategyModuleInfo(
                module_name=module_name,
                file_path=str(file_path),
                class_name=strategy_cls.__name__,
                strategy_cls=strategy_cls,
                file_hash=file_hash,
                loaded_at=int(time.time() * 1000),
                version=version,
                status=LoadStatus.SUCCESS,
            )

            with self._lock:
                # 保存历史
                if module_name not in self._history:
                    self._history[module_name] = []
                if prev_info:
                    self._history[module_name].append(prev_info)

                self._modules[module_name] = info
                self._file_hashes[str(file_path)] = file_hash

            # 注册到 StrategyManager
            self._manager.register_strategy(module_name, strategy_cls)

            # 触发事件
            event_type = "reloaded" if prev_info else "loaded"
            self._emit_event(HotLoadEvent(
                event_type=event_type,
                strategy_name=module_name,
                file_path=str(file_path),
                timestamp=info.loaded_at,
                version=version,
                message=f"{strategy_cls.__name__} v{version}",
            ))

            logger.info(f"Strategy loaded: {module_name} ({strategy_cls.__name__} v{version})")
            return LoadStatus.SUCCESS

        except Exception as e:
            logger.error(f"Failed to load strategy {file_path.name}: {e}")
            self._emit_event(HotLoadEvent(
                event_type="error",
                strategy_name=module_name,
                file_path=str(file_path),
                timestamp=int(time.time() * 1000),
                message=str(e),
            ))
            return LoadStatus.FAILED

    def reload_strategy(self, module_name: str) -> LoadStatus:
        """重新加载指定策略

        如果策略正在运行，会先保存状态，重载后恢复。
        """
        info = self._modules.get(module_name)
        if not info:
            logger.warning(f"Strategy not found: {module_name}")
            return LoadStatus.FAILED

        file_path = Path(info.file_path)
        if not file_path.exists():
            logger.error(f"Strategy file not found: {file_path}")
            return LoadStatus.FAILED

        # 检查是否有运行中的实例需要热替换
        running_state = self._capture_running_state(module_name)

        # 重新加载
        status = self.load_file(file_path)

        # 恢复运行状态
        if status == LoadStatus.SUCCESS and running_state:
            self._restore_running_state(module_name, running_state)

        return status

    def unload_file(self, module_name: str) -> bool:
        """卸载策略模块"""
        with self._lock:
            if module_name not in self._modules:
                return False

            info = self._modules.pop(module_name)
            self._file_hashes.pop(info.file_path, None)

            # 从 manager 中卸载运行中的实例
            for sid, managed in self._manager.strategies.items():
                if managed.context.info.name == module_name:
                    try:
                        self._manager.unload_strategy(sid)
                    except Exception as e:
                        logger.error(f"Error unloading strategy instance {sid}: {e}")

        self._emit_event(HotLoadEvent(
            event_type="unloaded",
            strategy_name=module_name,
            file_path=info.file_path,
            timestamp=int(time.time() * 1000),
            version=info.version,
        ))

        logger.info(f"Strategy unloaded: {module_name}")
        return True

    # ─── 回滚 ───

    def rollback(self, module_name: str, target_version: Optional[int] = None) -> LoadStatus:
        """回滚策略到指定版本（默认上一版本）

        Args:
            module_name: 策略模块名
            target_version: 目标版本号，None 表示上一版本
        """
        history = self._history.get(module_name, [])
        if not history:
            logger.warning(f"No history for {module_name}")
            return LoadStatus.FAILED

        # 找到目标版本
        if target_version is None:
            target = history[-1]
        else:
            target = next((h for h in history if h.version == target_version), None)
            if target is None:
                logger.warning(f"Version {target_version} not found for {module_name}")
                return LoadStatus.FAILED

        # 恢复旧版本
        with self._lock:
            current = self._modules.get(module_name)
            if current:
                history.append(current)
            target.status = LoadStatus.ROLLED_BACK
            self._modules[module_name] = target

        self._manager.register_strategy(module_name, target.strategy_cls)

        self._emit_event(HotLoadEvent(
            event_type="reloaded",
            strategy_name=module_name,
            file_path=target.file_path,
            timestamp=int(time.time() * 1000),
            version=target.version,
            message=f"Rolled back to v{target.version}",
        ))

        logger.info(f"Strategy rolled back: {module_name} → v{target.version}")
        return LoadStatus.SUCCESS

    def get_history(self, module_name: str) -> List[Dict[str, Any]]:
        """获取策略加载历史"""
        history = self._history.get(module_name, [])
        return [
            {
                "version": h.version,
                "class_name": h.class_name,
                "loaded_at": h.loaded_at,
                "status": h.status.value,
                "file_hash": h.file_hash[:8],
            }
            for h in history
        ]

    # ─── 文件监控 ───

    def start_watching(self) -> None:
        """启动文件变更监控（后台线程）"""
        if self._watching:
            return

        self._watching = True
        self._watch_thread = threading.Thread(
            target=self._watch_loop,
            name="strategy-hot-loader",
            daemon=True,
        )
        self._watch_thread.start()
        logger.info(f"Strategy file watcher started: {self._strategy_dir}")

    def stop_watching(self) -> None:
        """停止文件监控"""
        self._watching = False
        if self._watch_thread:
            self._watch_thread.join(timeout=5)
            self._watch_thread = None
        logger.info("Strategy file watcher stopped")

    @property
    def is_watching(self) -> bool:
        return self._watching

    def _watch_loop(self) -> None:
        """文件监控循环"""
        while self._watching:
            try:
                self._check_changes()
            except Exception as e:
                logger.error(f"Watch loop error: {e}")
            time.sleep(self._watch_interval)

    def _check_changes(self) -> None:
        """检查文件变更"""
        if not self._strategy_dir.exists():
            return

        current_files = {
            f.stem: f
            for f in self._strategy_dir.glob("*.py")
            if not f.name.startswith("_")
        }

        # 检测新增和修改
        for name, file_path in current_files.items():
            new_hash = self._compute_hash(file_path)
            old_hash = self._file_hashes.get(str(file_path))

            if old_hash is None:
                # 新文件
                if self._auto_reload:
                    logger.info(f"New strategy detected: {file_path.name}")
                    self.load_file(file_path)
            elif new_hash != old_hash:
                # 文件修改
                if self._auto_reload:
                    logger.info(f"Strategy file changed: {file_path.name}")
                    self.reload_strategy(name)

        # 检测删除
        loaded_names = set(self._modules.keys())
        for name in loaded_names - set(current_files.keys()):
            info = self._modules.get(name)
            if info and not Path(info.file_path).exists():
                if self._auto_reload:
                    logger.info(f"Strategy file removed: {name}")
                    self.unload_file(name)

    # ─── 内部方法 ───

    def _import_strategy_class(self, file_path: Path) -> Optional[Type[StrategyBase]]:
        """从文件动态导入策略类"""
        module_name = f"_hot_strategy_{file_path.stem}_{int(time.time())}"

        spec = importlib.util.spec_from_file_location(module_name, str(file_path))
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot create module spec for {file_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module

        try:
            spec.loader.exec_module(module)
        except Exception as e:
            del sys.modules[module_name]
            raise ImportError(f"Error executing {file_path.name}: {e}")

        # 查找 StrategyBase 子类
        strategy_cls = None
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if (
                issubclass(obj, StrategyBase)
                and obj is not StrategyBase
                and not inspect.isabstract(obj)
            ):
                strategy_cls = obj
                break

        # 清理临时模块（保留引用在 StrategyModuleInfo 中）
        # 不删除 sys.modules 以免类引用失效

        return strategy_cls

    def _validate_strategy_class(self, cls: Type[StrategyBase]) -> None:
        """验证策略类合法性"""
        # 必须实现 on_init 和 on_bar
        if not hasattr(cls, "on_init") or not callable(getattr(cls, "on_init")):
            raise ValueError(f"{cls.__name__} missing on_init method")
        if not hasattr(cls, "on_bar") or not callable(getattr(cls, "on_bar")):
            raise ValueError(f"{cls.__name__} missing on_bar method")

        # 检查 on_bar 签名
        sig = inspect.signature(cls.on_bar)
        params = list(sig.parameters.keys())
        if len(params) < 3:  # self, df, bar_index
            raise ValueError(f"{cls.__name__}.on_bar requires (self, df, bar_index)")

    def _capture_running_state(self, module_name: str) -> Optional[Dict[str, Any]]:
        """捕获运行中策略的状态"""
        for sid, managed in self._manager.strategies.items():
            if managed.context.info.name == module_name:
                if managed.status == StrategyStatus.RUNNING:
                    return {
                        "strategy_id": sid,
                        "state": managed.context.get_all_state(),
                        "params": managed.context.params,
                        "info": managed.context.info,
                    }
        return None

    def _restore_running_state(self, module_name: str, state: Dict[str, Any]) -> None:
        """恢复策略运行状态"""
        try:
            info = self._modules.get(module_name)
            if not info:
                return

            strategy_id = state["strategy_id"]

            # 如果旧实例还在，先卸载
            if strategy_id in self._manager.strategies:
                self._manager.unload_strategy(strategy_id)

            # 用新类重新加载
            new_info = StrategyInfo(
                strategy_id=strategy_id,
                name=module_name,
                parameters=state["params"],
            )
            managed = self._manager.load_strategy(
                strategy_id, info.strategy_cls, new_info
            )
            # 恢复状态
            managed.context.load_state(state["state"])
            self._manager.init_strategy(strategy_id)
            self._manager.start_strategy(strategy_id)

            logger.info(f"Strategy state restored: {strategy_id}")
        except Exception as e:
            logger.error(f"Failed to restore strategy state: {e}")

    @staticmethod
    def _compute_hash(file_path: Path) -> str:
        """计算文件 MD5 hash"""
        content = file_path.read_bytes()
        return hashlib.md5(content).hexdigest()

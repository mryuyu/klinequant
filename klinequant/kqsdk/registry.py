"""策略装载与运行辅助（多策略配置化框架）

三个职责：
  1. ``@strategy(name=...)`` 装饰器：模块内注册策略函数——策略名可自定义，
     不需要与策略文件同名；同一模块可注册多个策略。
  2. ``StrategyContext``：框架注入策略的运行上下文（策略名/参数/风控配置/
     订阅信息/账户名），策略参数全部来自 yaml，不写死在代码里。
  3. ``@resilient`` 迭代级容错包装：策略主循环单次迭代异常 → 打日志后继续
     循环（而不是抛出导致整个策略线程/事件循环退出）；连续异常超阈值才放弃
     本策略（只影响自己，不拖垮其他策略线程）。

装载解析顺序（``resolve_strategy_fn``）：
  ① 全局注册表中与实例 name 同名的注册策略（跨模块按名命中）
  ② 模块注册表中与实例 name 同名的注册策略
  ③ 模块内恰好一个注册策略时直接使用
  ④ 模块级 ``strategy`` 函数（兼容 fx_cross_ema 等旧写法）

策略函数签名双形态兼容：``strategy(api)`` 或 ``strategy(api, ctx)``
（``inspect.signature`` 判定参数个数，旧策略零改动）。
"""
from __future__ import annotations

import functools
import importlib
import inspect
import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from config.strategy_config import RiskConfig

logger = logging.getLogger(__name__)

__all__ = [
    "StrategyContext",
    "StrategyLoadError",
    "strategy",
    "resolve_strategy_fn",
    "invoke_strategy",
    "resilient",
]


class StrategyLoadError(RuntimeError):
    """策略装载失败（模块不存在 / 无策略函数 / 名字不匹配）。"""


@dataclass
class StrategyContext:
    """框架注入策略的运行上下文（yaml 配置的运行时投影）。

    ``name`` 即自定义策略名（≠文件名）：敞口 tag / StateStore key / magic 派生 /
    日志前缀全部用它做身份标识。``params`` 为自由 dict（策略自定义参数）。
    """

    name: str
    params: Dict[str, Any] = field(default_factory=dict)
    risk: RiskConfig = field(default_factory=RiskConfig)
    symbols: List[str] = field(default_factory=list)
    period: str = "1m"
    periods: List[str] = field(default_factory=list)
    account: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)


# ─── 注册表 ───

# 全局注册表：name → (fn, module_name)；@strategy(name=...) 写入
_REGISTRY: Dict[str, Tuple[Callable, str]] = {}
_REGISTRY_LOCK = threading.Lock()


def strategy(name: str):
    """策略注册装饰器：把函数以自定义名注册进全局注册表。

    用法::

        @strategy("my-name")          # 名字可自定义，不需要和文件名一致
        def my_strategy(api, ctx):
            ...

    被装饰函数原样返回（模块内仍可直接引用）；框架按 yaml 实例 name 命中。
    """

    def deco(fn: Callable) -> Callable:
        if not callable(fn):
            raise TypeError("@strategy 只能装饰可调用对象")
        with _REGISTRY_LOCK:
            if name in _REGISTRY:
                prev_mod = _REGISTRY[name][1]
                raise StrategyLoadError(
                    f"策略名 {name!r} 重复注册（{prev_mod} 与 {fn.__module__}）；"
                    "策略名必须全局唯一"
                )
            _REGISTRY[name] = (fn, fn.__module__ or "")
        return fn

    return deco


def registered_strategies() -> Dict[str, str]:
    """当前已注册的策略名 → 模块名（诊断/测试用）。"""
    with _REGISTRY_LOCK:
        return {n: mod for n, (_fn, mod) in _REGISTRY.items()}


def clear_registry() -> None:
    """清空全局注册表（仅单测用）。"""
    with _REGISTRY_LOCK:
        _REGISTRY.clear()


def _module_registry(mod: Any) -> Dict[str, Callable]:
    """模块内注册策略（name → fn）：按 fn.__module__ 归属过滤全局注册表。"""
    mod_name = getattr(mod, "__name__", "")
    with _REGISTRY_LOCK:
        return {
            n: fn for n, (fn, m) in _REGISTRY.items() if m == mod_name
        }


def resolve_strategy_fn(module_path: str, name: str) -> Callable:
    """按 yaml 实例（module + name）解析策略函数。

    解析顺序：全局注册名命中 → 模块注册名命中 → 模块唯一注册策略 →
    模块级 ``strategy`` 函数（兼容旧写法）。全部落空抛 StrategyLoadError。
    """
    with _REGISTRY_LOCK:
        hit = _REGISTRY.get(name)
    if hit is not None:
        return hit[0]

    try:
        mod = importlib.import_module(module_path)
    except ModuleNotFoundError as e:
        raise StrategyLoadError(
            f"策略模块 {module_path} 不存在（实例 {name!r}）"
        ) from e

    in_mod = _module_registry(mod)
    if name in in_mod:
        return in_mod[name]
    if len(in_mod) == 1:
        return next(iter(in_mod.values()))

    fn = getattr(mod, "strategy", None)
    if callable(fn):
        return fn

    raise StrategyLoadError(
        f"模块 {module_path} 中找不到策略 {name!r}：既无 @strategy({name!r}) 注册，"
        "也无模块级 strategy 函数"
    )


def invoke_strategy(fn: Callable, api: Any, ctx: Optional[StrategyContext] = None) -> None:
    """调用策略函数（签名双形态兼容）。

    ``fn(api, ctx)`` 双参形态（ctx 位置参数或关键字均可）→ 传上下文；
    ``fn(api)`` 单参形态（旧写法）→ 只传 api。
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):  # builtin 等不可内省对象，按单参调用
        fn(api)
        return
    n = len(sig.parameters)
    if n >= 2:
        fn(api, ctx)
    else:
        fn(api)


# ─── 迭代级容错（需求：异常打日志后继续循环，单策略异常不干掉整个事件循环）───


def resilient(
    max_consecutive: int = 20,
    backoff: float = 0.0,
    logger_obj: Optional[logging.Logger] = None,
):
    """策略函数迭代级容错包装（装饰在策略函数上，包体 = 整个主循环）。

    语义：
      - wait_update 自身异常 → 记 ERROR（含策略名/品种/traceback）→ 继续下一轮
        循环（不向上抛出，不退出策略线程）；
      - 循环体内部异常（策略代码抛到顶层）→ 记 ERROR → 重新进入策略循环
        （局部变量重置，持久语义量靠 api.state 跨重启/跨重入保留）；
      - **连续**异常超过 ``max_consecutive`` 次（其间任意一次成功迭代即清零）
        → 记 CRITICAL 后结束本策略线程（只影响自己，不影响其他策略/品种线程）；
      - ``backoff`` > 0 时每次异常后 sleep 该秒数（防异常风暴打满 CPU/日志）。

    约定：被装饰函数的循环体形如 ``while api.wait_update(): ...``。实现上策略体
    拿到的是 KqApi 的轻量子类代理（仅覆写 wait_update），不污染原实例。
    """

    log = logger_obj or logger

    def deco(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(api: Any, *args: Any, **kwargs: Any) -> Any:
            sym = getattr(api, "_symbol", "?")
            name = getattr(api, "_strategy_name", "") or getattr(api, "_tag", "")
            state = {"consecutive": 0}

            class _ResilientApi(type(api)):
                """wait_update 容错代理：异常计数续跑，连续超限退出循环。"""

                def wait_update(self, deadline: Any = None) -> bool:  # noqa: D102
                    try:
                        if deadline is None:
                            got = super().wait_update()
                        else:
                            got = super().wait_update(deadline=deadline)
                        state["consecutive"] = 0
                        return bool(got)
                    except Exception:
                        state["consecutive"] += 1
                        log.error(
                            f"[RESILIENT:{name}:{sym}] wait_update 异常 "
                            f"(连续 {state['consecutive']}/{max_consecutive})，续跑",
                            exc_info=True,
                        )
                        return self._degrade()

                def _degrade(self) -> bool:
                    if state["consecutive"] >= max_consecutive:
                        log.critical(
                            f"[RESILIENT:{name}:{sym}] 连续异常达上限 "
                            f"{max_consecutive}，本策略线程退出（不影响其他策略）"
                        )
                        return False
                    if backoff > 0:
                        threading.Event().wait(backoff)
                    return True

            try:
                proxy = _ResilientApi.__new__(_ResilientApi)
                proxy.__dict__ = api.__dict__   # 共享实例状态（同一对象身份语义）
            except Exception:  # api 非常规实例（单测 stub 等）时直接透传
                proxy = api

            # 外层重入循环：策略体内部异常（非 wait_update）也续跑，连续超限才放弃
            while True:
                try:
                    result = fn(proxy, *args, **kwargs)
                    return result          # 正常退出（wait_update 返 False）不重入
                except Exception:
                    state["consecutive"] += 1
                    log.error(
                        f"[RESILIENT:{name}:{sym}] 策略循环体异常 "
                        f"(连续 {state['consecutive']}/{max_consecutive})，重入续跑",
                        exc_info=True,
                    )
                    if state["consecutive"] >= max_consecutive:
                        log.critical(
                            f"[RESILIENT:{name}:{sym}] 连续异常达上限 "
                            f"{max_consecutive}，本策略线程退出（不影响其他策略）"
                        )
                        return None
                    if backoff > 0:
                        threading.Event().wait(backoff)

        return wrapper

    return deco

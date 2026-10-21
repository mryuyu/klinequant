"""策略装载与运行辅助单测（kqsdk/registry.py）

覆盖：@strategy 装饰器注册（含重名拒绝）、resolve_strategy_fn 四级解析
（全局命中 / 模块唯一 / 模块级 strategy 回落 / 落空报错）、invoke_strategy
单双参签名分派、resilient 迭代级容错（wait_update 异常续跑、循环体异常重入、
连续超限退出）。
"""
from __future__ import annotations

import pytest

# 模块级导入：在收集期即触发 @kq_strategy("dualma-eur") 注册，使其早于任何
# 测试夹具的注册表快照——teardown 只清理测试内新增名，不误删真实注册。
import strategies.sdk_dual_ma as _dual_ma_mod  # noqa: F401
from klinequant.kqsdk import registry
from klinequant.kqsdk.registry import (
    StrategyContext,
    StrategyLoadError,
    invoke_strategy,
    registered_strategies,
    resilient,
    resolve_strategy_fn,
    strategy,
)


@pytest.fixture(autouse=True)
def _isolate_registry():
    """只清理测试期间新增的注册名，保留真实策略注册（dualma-eur 等）。"""
    before = set(registry._REGISTRY.keys())
    yield
    with registry._REGISTRY_LOCK:
        for n in list(registry._REGISTRY.keys()):
            if n not in before:
                del registry._REGISTRY[n]


# ─── 装饰器注册 ───


def test_strategy_decorator_registers_by_custom_name():
    """@strategy(name) 以自定义名注册（≠函数名/文件名）。"""

    @strategy("_test_reg_custom")
    def some_fn(api, ctx):
        return None

    regs = registered_strategies()
    assert "_test_reg_custom" in regs
    # 被装饰函数原样返回
    assert some_fn.__name__ == "some_fn"


def test_duplicate_registration_rejected():
    """同名重复注册 → StrategyLoadError（策略名须全局唯一）。"""

    @strategy("_test_reg_dup")
    def s1(api):
        return None

    with pytest.raises(StrategyLoadError):

        @strategy("_test_reg_dup")
        def s2(api):
            return None


def test_decorator_requires_callable():
    """装饰非可调用对象 → TypeError。"""
    with pytest.raises(TypeError):
        strategy("_test_reg_bad")(123)


# ─── resolve_strategy_fn 四级解析 ───


def test_resolve_global_registry_hit():
    """① 全局注册名命中（module_path 可为任意值，跨模块按名命中）。"""

    @strategy("_test_reg_global")
    def s(api, ctx):
        return None

    assert resolve_strategy_fn("nonexistent.module.path", "_test_reg_global") is s


def test_resolve_module_unique_fallback():
    """③ 模块内恰好一个注册策略时，按未匹配的 name 也回落到它。"""
    fn = resolve_strategy_fn("strategies.sdk_dual_ma", "dualma-gbp-not-registered")
    assert fn is _dual_ma_mod.dual_ma


def test_resolve_module_level_strategy_fallback():
    """④ 模块级 strategy 函数回落（兼容 fx_cross_ema 旧写法）。"""
    import strategies.fx_cross_ema as m

    fn = resolve_strategy_fn("strategies.fx_cross_ema", "whatever-name")
    assert fn is m.strategy


def test_resolve_missing_module_raises():
    """模块不存在且全局无命中 → StrategyLoadError。"""
    with pytest.raises(StrategyLoadError):
        resolve_strategy_fn("no.such.module.xyz", "no-such-name")


def test_resolve_no_strategy_in_module_raises():
    """模块存在但既无注册策略也无模块级 strategy → StrategyLoadError。"""
    with pytest.raises(StrategyLoadError):
        resolve_strategy_fn("klinequant.kqsdk.risk_rules", "nope")


# ─── invoke_strategy 签名分派 ───


def test_invoke_two_param_receives_ctx():
    """双参形态 fn(api, ctx) → 传入上下文。"""
    got = {}

    def fn(api, ctx):
        got["api"] = api
        got["ctx"] = ctx

    ctx = StrategyContext(name="x")
    invoke_strategy(fn, "API", ctx)
    assert got == {"api": "API", "ctx": ctx}


def test_invoke_one_param_receives_api_only():
    """单参形态 fn(api)（旧写法）→ 只传 api。"""
    got = {}

    def fn(api):
        got["api"] = api

    invoke_strategy(fn, "API", StrategyContext(name="x"))
    assert got == {"api": "API"}


def test_invoke_without_ctx_defaults_none():
    """不传 ctx 时双参形态收到 None。"""
    got = {}

    def fn(api, ctx):
        got["ctx"] = ctx

    invoke_strategy(fn, "API")
    assert got["ctx"] is None


# ─── resilient 迭代级容错 ───


class _StubApi:
    """最小 KqApi 替身：wait_update 按脚本返回/抛异常。"""

    def __init__(self, wait_results=None, always_raise=False):
        self._symbol = "EURUSD"
        self._strategy_name = "stub"
        self._tag = "stub"
        self._results = list(wait_results or [])
        self._always_raise = always_raise

    def wait_update(self, deadline=None):
        if self._always_raise:
            raise RuntimeError("feed down")
        if not self._results:
            return False
        item = self._results.pop(0)
        if isinstance(item, Exception):
            raise item
        return bool(item)


def test_resilient_wait_update_exception_continues():
    """wait_update 单次异常 → 记日志续跑（不退出循环）。"""
    api = _StubApi([True, RuntimeError("blip"), True, False])
    seen = []

    @resilient(max_consecutive=5)
    def strat(a):
        while a.wait_update():
            seen.append(1)

    strat(api)
    # True(1) → 异常降级为 True(2) → True(3) → False 退出
    assert len(seen) == 3


def test_resilient_consecutive_limit_exits_thread():
    """连续异常达上限 → CRITICAL 后退出本策略线程（不无限循环）。"""
    api = _StubApi(always_raise=True)
    iterations = []

    @resilient(max_consecutive=3)
    def strat(a):
        while a.wait_update():
            iterations.append(1)

    strat(api)
    # 连续 2 次降级续跑后第 3 次达上限退出
    assert len(iterations) == 2


def test_resilient_body_exception_reenters():
    """循环体内部异常 → 重入续跑（局部变量重置，靠 api.state 保留语义）。"""
    api = _StubApi([False])
    calls = {"n": 0}

    @resilient(max_consecutive=5)
    def strat(a):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("boom")
        while a.wait_update():
            pass

    strat(api)
    assert calls["n"] == 3


def test_resilient_body_exception_exceeds_limit():
    """循环体持续异常达上限 → 放弃本策略（返回，不再重入）。"""
    api = _StubApi([])
    calls = {"n": 0}

    @resilient(max_consecutive=2)
    def strat(a):
        calls["n"] += 1
        raise RuntimeError("always")

    strat(api)
    assert calls["n"] == 2


def test_resilient_normal_exit_no_reentry():
    """正常退出（wait_update 返 False）不触发重入。"""
    api = _StubApi([True, True, False])
    calls = {"n": 0}

    @resilient(max_consecutive=5)
    def strat(a):
        calls["n"] += 1
        while a.wait_update():
            pass

    strat(api)
    assert calls["n"] == 1

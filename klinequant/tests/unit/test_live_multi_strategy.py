"""LiveRunner 多策略装配单测（kqsdk/live_runner.py，需求 7/11）

覆盖：from_config 逐策略构造（names / symbols 并集 / periods 并集 / 各自 ctx.params）、
StateStore key = ``{account}:{strategy_name}``、--only 过滤、无 enabled 抛错、
共享 ExposureLedger 按 tag 隔离持仓（全局统一视图 + 策略隔离）、_make_runner 的
单/双参分派与 resilient 崩溃隔离（单策略异常不向外传播）。
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from config.strategy_config import StrategiesConfig, StrategyInstanceConfig
from core.trade_engine.ledger import ExposureLedger
from kqsdk import registry
from kqsdk.live_runner import (
    LiveRunner,
    _fn_takes_ctx,
    _make_runner,
)
from kqsdk.registry import StrategyContext
from kqsdk.registry import strategy as kq_strategy

_MOD = "tests.unit.test_live_multi_strategy"


@kq_strategy("_test_live_a")
def _strat_a(api, ctx):   # 双参形态
    return None


@kq_strategy("_test_live_b")
def _strat_b(api):        # 单参旧形态
    return None


@pytest.fixture(scope="module", autouse=True)
def _cleanup_registry():
    yield
    with registry._REGISTRY_LOCK:
        for n in list(registry._REGISTRY):
            if n.startswith("_test_live_"):
                del registry._REGISTRY[n]


class _FakeBackend:
    """LiveRunner.__init__ 只读 account_name/account_magic/exchange，不调用方法。"""

    exchange = "mt5"
    account_magic = None

    def __init__(self, account_name="fx-demo"):
        self.account_name = account_name


def _cfg_two() -> StrategiesConfig:
    return StrategiesConfig(strategies=[
        StrategyInstanceConfig(
            name="_test_live_a", module=_MOD, symbols=["EURUSD"],
            period="1m", periods=["1m", "15m"], params={"fast": 7, "slow": 25},
        ),
        StrategyInstanceConfig(
            name="_test_live_b", module=_MOD, symbols=["GBPUSD", "USDJPY"],
            period="5m", periods=["5m"], params={"n": 10},
        ),
    ])


# ─── from_config 构造 ───


def test_from_config_builds_instances():
    """逐策略构造实例：names / ctx.params / account 各自独立。"""
    runner = LiveRunner.from_config(_cfg_two(), _FakeBackend(), account_name="fx-demo")
    strats = runner._strategies
    assert [s.name for s in strats] == ["_test_live_a", "_test_live_b"]
    assert strats[0].ctx.params == {"fast": 7, "slow": 25}
    assert strats[1].ctx.params == {"n": 10}
    assert strats[0].ctx.name == "_test_live_a"
    assert strats[0].ctx.account == "fx-demo"
    assert strats[0].ctx.risk is runner._risk_cfg


def test_from_config_symbol_and_period_union():
    """订阅 = 全策略 symbols × periods 并集（去重、保序）。"""
    runner = LiveRunner.from_config(_cfg_two(), _FakeBackend())
    assert runner._symbols == ["EURUSD", "GBPUSD", "USDJPY"]
    assert runner._periods == ["1m", "15m", "5m"]
    assert runner._symbol == "EURUSD"
    # 每策略仍只持有自己的 symbols/periods
    assert runner._strategies[0].symbols == ["EURUSD"]
    assert runner._strategies[0].periods == ["1m", "15m"]
    assert runner._strategies[1].symbols == ["GBPUSD", "USDJPY"]


def test_from_config_only_filter():
    """--only 只保留指定策略名。"""
    runner = LiveRunner.from_config(_cfg_two(), _FakeBackend(), only=["_test_live_b"])
    assert [s.name for s in runner._strategies] == ["_test_live_b"]


def test_from_config_disabled_excluded():
    """enabled=False 的实例被排除。"""
    cfg = StrategiesConfig(strategies=[
        StrategyInstanceConfig(name="_test_live_a", module=_MOD,
                               symbols=["EURUSD"], enabled=False),
        StrategyInstanceConfig(name="_test_live_b", module=_MOD, symbols=["GBPUSD"]),
    ])
    runner = LiveRunner.from_config(cfg, _FakeBackend())
    assert [s.name for s in runner._strategies] == ["_test_live_b"]


def test_from_config_no_enabled_raises():
    """无可运行策略（全禁用 / only 不匹配）→ ValueError。"""
    cfg = StrategiesConfig(strategies=[
        StrategyInstanceConfig(name="_test_live_a", module=_MOD,
                               symbols=["EURUSD"], enabled=False),
    ])
    with pytest.raises(ValueError):
        LiveRunner.from_config(cfg, _FakeBackend())
    with pytest.raises(ValueError):
        LiveRunner.from_config(_cfg_two(), _FakeBackend(), only=["nonexistent"])


# ─── StateStore key 按策略名 ───


def test_state_key_per_strategy_name():
    """R4 状态 key = ``{account}:{strategy_name}``（同策略全品种共享一份）。"""
    runner = LiveRunner.from_config(_cfg_two(), _FakeBackend(), account_name="fx-demo")
    a, b = runner._strategies
    assert runner._state_key(a) == "fx-demo:_test_live_a"
    assert runner._state_key(b) == "fx-demo:_test_live_b"


def test_state_key_default_account():
    """账户名空 → key 回落 ``default:{name}``。"""
    runner = LiveRunner.from_config(_cfg_two(), _FakeBackend(account_name=""),
                                    account_name="")
    assert runner._state_key(runner._strategies[0]) == "default:_test_live_a"


# ─── 共享 ledger 按 tag 隔离（全局统一视图 + 策略隔离）───


def test_shared_ledger_isolates_by_tag():
    """同一 symbol 不同策略 tag 各自独立记账；net_position 为全局净持仓。"""
    ledger = ExposureLedger()
    ledger.sync_from_venue("EURUSD", "_test_live_a", Decimal("0.5"), Decimal("1.10"))
    ledger.sync_from_venue("EURUSD", "_test_live_b", Decimal("-0.3"), Decimal("1.12"))
    assert ledger.position("EURUSD", "_test_live_a").volume == Decimal("0.5")
    assert ledger.position("EURUSD", "_test_live_b").volume == Decimal("-0.3")
    assert ledger.net_position("EURUSD") == Decimal("0.2")   # 全局统一视图
    assert {p.tag for p in ledger.all_positions()} == {"_test_live_a", "_test_live_b"}


# ─── _make_runner：签名分派 + resilient 崩溃隔离 ───


def test_fn_takes_ctx_detection():
    assert _fn_takes_ctx(lambda api, ctx: None) is True
    assert _fn_takes_ctx(lambda api: None) is False


def test_make_runner_double_param_dispatch():
    """双参形态 → 传 (api, ctx)。"""
    calls = []

    def fn(api, ctx):
        calls.append((api, ctx.name))

    bound = _make_runner(fn, StrategyContext(name="x"), resilient_on=False, max_errors=0)
    bound("API")
    assert calls == [("API", "x")]


def test_make_runner_single_param_legacy():
    """单参旧形态 → 只传 api（ctx 不注入）。"""
    calls = []

    def fn(api):
        calls.append(api)

    bound = _make_runner(fn, StrategyContext(name="x"), resilient_on=False, max_errors=0)
    bound("API")
    assert calls == ["API"]


class _StubApi:
    _symbol = "EURUSD"
    _strategy_name = "crashy"
    _tag = "crashy"

    def wait_update(self, deadline=None):
        return False


def test_make_runner_resilient_contains_crash():
    """resilient_on=True：策略体持续崩溃被容错包装吞掉（不向外传播，不拖垮进程）。"""

    def fn(api, ctx):
        raise RuntimeError("boom")

    bound = _make_runner(fn, StrategyContext(name="crashy"),
                         resilient_on=True, max_errors=2)
    # 不应抛出（连续超限后静默退出本策略线程）
    assert bound(_StubApi()) is None

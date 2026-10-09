"""Phase M4 多周期记账隔离单元测试。

覆盖《SDK 阶段实施规划 v1.3》M4 验收点（拓扑 Z：一个 KqApi 绑定一个 symbol、
线程内访问该 symbol 多 period）：
  - position(symbol, period) 只返回本周期 tag(``{base}:{period}``) 的仓（各自独立记账）
  - 交易所净持仓 = 各周期 tag 持仓之和
  - send_order(period=...) 按 (account, ``{base}:{period}``) 派生独立 magic
    （多周期同账户必然多 magic，venue 侧按 magic 隔离）
  - period=None 完全等价旧口径（base tag / base magic），后向兼容既有单周期策略
  - LiveRunner._reconcile_targets 单周期回落 base、多周期逐 period 派生对账目标
"""
from decimal import Decimal

from core.trade_engine.executors.mt5_executor import SubmitResult
from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import UnifiedResolver, derive_magic
from protocol.types import Offset, OrderSide, SymbolInfo
from strategy.sdk.api import KqApi
from strategy.sdk.live_runner import LiveRunner

# ─── 常量 ───

ACCT = "acct"
BASE = "strat"


# ─── 脚手架 ───


def _fx_spec(symbol="EURUSD"):
    return SymbolInfo(
        symbol=symbol, market_type="FX",
        pip_size=Decimal("0.0001"), tick_size=Decimal("0.00001"),
        qty_step=Decimal("0.01"), min_qty=Decimal("0.01"), qty_max=Decimal("200"),
        can_short=True,
    )


class _NullFeed:
    """最小 feed 桩：send_order 不触发行情（stale_threshold=None 时不查心跳）。"""

    def now_ms(self):
        return 0

    def latest_tick(self, symbol):
        return None

    def latest_bars(self, symbol, period, count):
        return []

    def wait_update(self, deadline=None):
        return False

    def is_changing(self, obj, field=None):
        return False


class _FillExec:
    """submit 恒返 FILLED（filled_qty=spec.qty）；记录 spec 以校验 tag/magic 派生。"""

    def __init__(self):
        self.submitted = []

    def submit(self, spec):
        self.submitted.append(spec)
        return SubmitResult(
            success=True, status="FILLED", order_ticket=len(self.submitted),
            filled_qty=spec.qty, filled_price=Decimal("1.1000"), comment="ok",
        )

    def cancel(self, order_ticket, symbol, magic=None):
        return True

    def query_positions(self, symbol="", magic=None):
        return []

    def query_account(self):
        return None

    def query_orders(self, symbol="", magic=None):
        return []


class _FakeBackend:
    """仅供 LiveRunner 构造（_reconcile_targets 不触发 backend）。"""


def _make_api(executor, *, magic=None, periods=None):
    return KqApi(
        symbol="EURUSD", period="1h", tag=BASE,
        specs={"EURUSD": _fx_spec()}, ledger=ExposureLedger(),
        resolver=UnifiedResolver(), executor=executor, feed=_NullFeed(),
        account_name=ACCT, magic=magic, periods=periods,
    )


def _make_runner(*, period="1h", periods=None, magic=None):
    return LiveRunner(
        _FakeBackend(), symbols=["EURUSD"], period=period,
        strategy_fn=lambda api: None, tag=BASE, periods=periods,
        account_name=ACCT, magic=magic,
    )


# ═══════════════════════════════════════════
# 记账隔离：position(period) 只返回本周期仓
# ═══════════════════════════════════════════


def test_position_isolated_by_period():
    """两周期各自开仓 → position(period=...) 只返回本周期 tag 的持仓。"""
    ex = _FillExec()
    api = _make_api(ex, periods=["1h", "15m"])
    api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"), period="1h")
    api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.20"), period="15m")

    assert api.position(period="1h").volume == Decimal("0.10")
    assert api.position(period="15m").volume == Decimal("0.20")
    # base tag（period=None）无仓 → 与两周期仓完全隔离
    assert api.position(period=None).volume == Decimal("0")


def test_net_position_is_sum_of_periods():
    """交易所净持仓 = 各周期 tag 持仓之和（ledger 按 (symbol,tag) 键控天然分离）。"""
    ex = _FillExec()
    api = _make_api(ex, periods=["1h", "15m"])
    api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"), period="1h")
    api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.20"), period="15m")

    net = (
        api.position(period="1h").volume
        + api.position(period="15m").volume
    )
    assert net == Decimal("0.30")
    # 直接查 ledger：两个独立 tag 各存一份，互不覆盖
    assert api._ledger.position("EURUSD", f"{BASE}:1h").volume == Decimal("0.10")
    assert api._ledger.position("EURUSD", f"{BASE}:15m").volume == Decimal("0.20")


def test_magic_derived_per_period():
    """多周期同账户必然多 magic：各周期按 (account, `{base}:{period}`) 独立派生。"""
    ex = _FillExec()
    api = _make_api(ex, periods=["1h", "15m"])
    api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"), period="1h")
    api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.20"), period="15m")

    m1h = derive_magic(ACCT, f"{BASE}:1h")
    m15m = derive_magic(ACCT, f"{BASE}:15m")
    assert m1h != m15m
    assert ex.submitted[0].tag == f"{BASE}:1h"
    assert ex.submitted[0].magic == m1h
    assert ex.submitted[1].tag == f"{BASE}:15m"
    assert ex.submitted[1].magic == m15m


# ═══════════════════════════════════════════
# 后向兼容：period=None 等价旧口径
# ═══════════════════════════════════════════


def test_period_none_uses_base_tag_and_magic():
    """period=None → base tag(``strat``) + base magic，与 M4 之前完全一致。"""
    ex = _FillExec()
    api = _make_api(ex, periods=["1h", "15m"])
    api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"))   # 不传 period

    assert ex.submitted[0].tag == BASE
    assert ex.submitted[0].magic == derive_magic(ACCT, BASE)
    assert api.position().volume == Decimal("0.10")               # period=None 回落
    # base 仓与周期仓互不干扰
    assert api.position(period="1h").volume == Decimal("0")


def test_tag_for_and_magic_for():
    """_tag_for / _magic_for 派生规则 + 显式 magic 覆盖优先。"""
    api = _make_api(_FillExec(), periods=["1h", "15m"])
    assert api._tag_for(None) == BASE
    assert api._tag_for("1h") == f"{BASE}:1h"
    assert api._magic_for(None) == derive_magic(ACCT, BASE)
    assert api._magic_for("15m") == derive_magic(ACCT, f"{BASE}:15m")

    # 显式 magic 覆盖：period=None 与显式 period 均沿用覆盖值
    api_ov = _make_api(_FillExec(), magic=4242, periods=["1h", "15m"])
    assert api_ov._magic_for(None) == 4242
    assert api_ov._magic_for("1h") == 4242


# ═══════════════════════════════════════════
# LiveRunner 对账目标：单周期回落 / 多周期逐 period
# ═══════════════════════════════════════════


def test_reconcile_targets_single_period():
    """单周期 → [(base tag, base magic)]（完全等价旧口径，后向兼容）。

    _FakeBackend 无 account_magic → self._magic=None（回落执行器实例级），与 M4 之前一致。
    """
    runner = _make_runner(period="1h")
    assert runner._multi_period is False
    assert runner._reconcile_targets() == [(BASE, None)]
    # 显式 magic 覆盖时沿用覆盖值
    runner_ov = _make_runner(period="1h", magic=4242)
    assert runner_ov._reconcile_targets() == [(BASE, 4242)]


def test_reconcile_targets_multi_period():
    """多周期 → 逐 period 返回 (`{base}:{period}`, 派生 magic)，各周期独立对账。"""
    runner = _make_runner(period="1h", periods=["1h", "15m"])
    assert runner._multi_period is True
    assert runner._reconcile_targets() == [
        (f"{BASE}:1h", derive_magic(ACCT, f"{BASE}:1h")),
        (f"{BASE}:15m", derive_magic(ACCT, f"{BASE}:15m")),
    ]

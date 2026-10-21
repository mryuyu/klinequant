"""api.risk_check 端到端单测（kqsdk/api.py + RiskEngine 接线，需求 7）

覆盖：日亏损限额（金额 max_daily_loss 与百分比 daily_loss_pct「先到先拒」）、
全局持仓笔数 / 单策略持仓笔数规则（OPEN 拦、CLOSE 放行）、市价单从 feed 取
last_price、风控内核异常 fail-closed、无 RiskEngine 注入时本地降级检查。
"""
from __future__ import annotations

from decimal import Decimal

from config.strategy_config import RiskConfig
from core.risk_engine.rules.base import RiskCheckResult
from core.trade_engine.ledger import ExposureLedger
from kqsdk.api import KqApi
from kqsdk.risk_adapter import RiskDataProvider, SdkRiskAdapter, assemble_risk
from protocol.types import Offset, OrderSide, Position, SymbolInfo


def _spec(symbol="EURUSD", *, min_qty="0.01") -> SymbolInfo:
    return SymbolInfo(
        symbol=symbol, market_type="FX", pip_size=Decimal("0.0001"),
        qty_step=Decimal("0.01"), min_qty=Decimal(min_qty), qty_max=Decimal("200"),
        contract_multiplier=Decimal("100000"),
    )


def _pos(symbol, tag, effective, realized="0") -> Position:
    eff = Decimal(effective)
    return Position(
        symbol=symbol, tag=tag,
        side="LONG" if eff > 0 else ("SHORT" if eff < 0 else "FLAT"),
        quantity=eff, volume=eff, effective=eff, realized_pnl=Decimal(realized),
    )


class _FakeLedger:
    def __init__(self, positions=None):
        self._positions = list(positions or [])

    def all_positions(self):
        return list(self._positions)


class _FakeTick:
    def __init__(self, price):
        self.last_price = Decimal(price)


class _FakeFeed:
    def __init__(self, tick_price=None):
        self._tp = tick_price

    def now_ms(self):
        return 0

    def latest_tick(self, symbol):
        return _FakeTick(self._tp) if self._tp else None

    def latest_bars(self, symbol, period, count):
        return []

    def wait_update(self, deadline=None):
        return False

    def is_changing(self, obj, field=None):
        return False


class _FakeExecutor:
    def __init__(self, balance="10000"):
        self._b = balance

    def query_account(self):
        return {"balance": self._b, "profit": "0", "margin_free": self._b, "margin": "0"}

    def query_positions(self, symbol=""):
        return []

    def query_orders(self, symbol=""):
        return []

    def submit(self, spec):
        raise AssertionError("risk_check 不应触及 executor.submit")

    def cancel(self, *a, **k):
        return True


class _CaptureEngine:
    def __init__(self):
        self.ctx = None

    def check_order(self, ctx):
        self.ctx = ctx
        return RiskCheckResult.ok("all_rules")


class _BoomAdapter:
    def check(self, **kwargs):
        raise RuntimeError("engine down")


def _make_api(*, risk_adapter=None, tag="1m", tick_price=None, balance="10000",
              min_qty="0.01"):
    specs = {s: _spec(s, min_qty=min_qty) for s in ("EURUSD", "GBPUSD", "USDJPY")}
    return KqApi(
        symbol="EURUSD", period="1m", tag=tag, specs=specs,
        ledger=ExposureLedger(), resolver=None, executor=_FakeExecutor(balance),
        feed=_FakeFeed(tick_price), risk_adapter=risk_adapter,
    )


def _adapter(cfg, positions=None, equity="10000"):
    return assemble_risk(cfg, _FakeLedger(positions), lambda: Decimal(equity), symbols=[])


# ─── 无 RiskEngine 注入：本地降级 ───


def test_local_degradation_without_adapter():
    """risk_adapter=None → 本地检查：qty>0 且 >= min_qty 才放行。"""
    api = _make_api(risk_adapter=None)
    assert api.risk_check("EURUSD", OrderSide.BUY, Offset.OPEN, Decimal("0.5")).passed
    assert not api.risk_check("EURUSD", OrderSide.BUY, Offset.OPEN, Decimal("0")).passed
    # 0.001 < min_qty 0.01 → 拒
    assert not api.risk_check("EURUSD", OrderSide.BUY, Offset.OPEN, Decimal("0.001")).passed


# ─── 日亏损限额（金额 / 百分比 / 先到先拒）───


def test_daily_loss_amount_rejects():
    """既有 max_daily_loss（绝对金额）：日亏超限拒绝。"""
    equity = {"v": Decimal("10000")}
    adapter = assemble_risk(
        RiskConfig(rules={"max_daily_loss": {"enabled": True, "max_loss": 200}}),
        _FakeLedger([]), lambda: equity["v"], symbols=[],
    )
    api = _make_api(risk_adapter=adapter)
    api.risk_check("EURUSD", OrderSide.BUY, Offset.OPEN, Decimal("0.1"))   # 建立日基线
    equity["v"] = Decimal("9700")   # 日亏 300 > 200
    res = api.risk_check("EURUSD", OrderSide.BUY, Offset.OPEN, Decimal("0.1"))
    assert not res.passed
    assert res.rule_name == "max_daily_loss"


def test_daily_loss_pct_rejects():
    """SDK daily_loss_pct（百分比）：日亏占比超限拒绝。"""
    equity = {"v": Decimal("10000")}
    adapter = assemble_risk(
        RiskConfig(max_daily_loss_pct=2.0), _FakeLedger([]), lambda: equity["v"], symbols=[],
    )
    api = _make_api(risk_adapter=adapter)
    api.risk_check("EURUSD", OrderSide.BUY, Offset.OPEN, Decimal("0.1"))
    equity["v"] = Decimal("9700")   # -3% < -2%
    res = api.risk_check("EURUSD", OrderSide.BUY, Offset.OPEN, Decimal("0.1"))
    assert not res.passed
    assert res.rule_name == "daily_loss_pct"


def test_daily_loss_amount_and_pct_first_to_reject():
    """金额与百分比同时启用 → 规则列表顺序「先到先拒」（既有金额规则在前）。"""
    equity = {"v": Decimal("10000")}
    adapter = assemble_risk(
        RiskConfig(rules={"max_daily_loss": {"enabled": True, "max_loss": 200}},
                   max_daily_loss_pct=1.0),
        _FakeLedger([]), lambda: equity["v"], symbols=[],
    )
    api = _make_api(risk_adapter=adapter)
    api.risk_check("EURUSD", OrderSide.BUY, Offset.OPEN, Decimal("0.1"))
    equity["v"] = Decimal("9700")   # 同时触发金额(-300>200)与 pct(-3%>1%)
    res = api.risk_check("EURUSD", OrderSide.BUY, Offset.OPEN, Decimal("0.1"))
    assert not res.passed
    assert res.rule_name == "max_daily_loss"   # legacy 规则先于 SDK 规则


# ─── 持仓笔数规则 ───


def test_max_total_positions_blocks_open_allows_close():
    """全局持仓笔数：OPEN 达上限拒绝，CLOSE 放行（只减风险）。"""
    adapter = _adapter(
        RiskConfig(max_total_positions=2),
        positions=[_pos("EURUSD", "s1", "0.5"), _pos("GBPUSD", "s2", "0.3")],
    )
    api = _make_api(risk_adapter=adapter)
    res = api.risk_check("USDJPY", OrderSide.BUY, Offset.OPEN, Decimal("0.1"))
    assert not res.passed
    assert res.rule_name == "max_total_positions"
    # CLOSE 放行
    assert api.risk_check("EURUSD", OrderSide.SELL, Offset.CLOSE, Decimal("0.5")).passed


def test_max_positions_per_strategy_blocks_open():
    """单策略持仓笔数：本策略达上限拒绝新开。"""
    adapter = _adapter(
        RiskConfig(max_positions_per_strategy=1),
        positions=[_pos("EURUSD", "s1", "0.5")],
    )
    api = _make_api(risk_adapter=adapter, tag="s1")
    res = api.risk_check("GBPUSD", OrderSide.BUY, Offset.OPEN, Decimal("0.1"))
    assert not res.passed
    assert res.rule_name == "max_positions_per_strategy"


def test_risk_check_passes_within_limits():
    """各限额均未触及 → 放行。"""
    adapter = _adapter(
        RiskConfig(max_total_positions=5, max_positions_per_strategy=3),
        positions=[_pos("EURUSD", "s1", "0.5")],
    )
    api = _make_api(risk_adapter=adapter, tag="s1")
    assert api.risk_check("GBPUSD", OrderSide.BUY, Offset.OPEN, Decimal("0.1")).passed


# ─── 市价单取价 / fail-closed ───


def test_market_order_pulls_last_price_from_feed():
    """price=None（市价）→ 从 feed.latest_tick 取 last_price 供规则估算名义值。"""
    engine = _CaptureEngine()
    provider = RiskDataProvider(_FakeLedger([]), lambda: Decimal("10000"))
    api = _make_api(risk_adapter=SdkRiskAdapter(engine, provider), tick_price="1.2345")
    api.risk_check("EURUSD", OrderSide.BUY, Offset.OPEN, Decimal("0.1"))
    assert engine.ctx.extra["last_price"] == Decimal("1.2345")


def test_adapter_exception_fail_closed():
    """风控内核异常 → fail-closed 拒绝（只影响本次决策，不抛出）。"""
    api = _make_api(risk_adapter=_BoomAdapter())
    res = api.risk_check("EURUSD", OrderSide.BUY, Offset.OPEN, Decimal("0.1"))
    assert not res.passed
    assert res.rule_name == "risk_adapter"
    assert res.level == "CRITICAL"

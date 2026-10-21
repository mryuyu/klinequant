"""资金管理 calc_qty 单测（kqsdk/api.py，需求 5）

覆盖：固定金额 / 权益百分比、risk_amount 优先、交叉盘换算钩子 profit_calc
（含失败回落名义计算）、qty_step 向下取整、[min_qty, qty_max] 钳制、
不足 min_qty 返回 0、stop<=0 / risk<=0 / 缺风险参数 的边界。
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from core.trade_engine.ledger import ExposureLedger
from kqsdk.api import KqApi
from protocol.types import SymbolInfo


def _spec(symbol="EURUSD", *, contract_multiplier="100000", qty_step="0.01",
          min_qty="0.01", qty_max="200", pip="0.0001") -> SymbolInfo:
    return SymbolInfo(
        symbol=symbol, market_type="FX", pip_size=Decimal(pip),
        qty_step=Decimal(qty_step), min_qty=Decimal(min_qty),
        qty_max=Decimal(qty_max), contract_multiplier=Decimal(contract_multiplier),
    )


class _FakeExecutor:
    def __init__(self, balance="0", profit="0"):
        self._balance = balance
        self._profit = profit

    def query_account(self):
        return {"balance": self._balance, "profit": self._profit,
                "margin_free": self._balance, "margin": "0"}

    def query_positions(self, symbol=""):
        return []

    def query_orders(self, symbol=""):
        return []

    def submit(self, spec):
        raise AssertionError("calc_qty 不应触及 executor.submit")

    def cancel(self, *a, **k):
        return True


class _FakeFeed:
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


def _make_api(*, balance="0", profit_calc=None, specs=None):
    specs = specs or {"EURUSD": _spec()}
    return KqApi(
        symbol="EURUSD", period="1m", tag="1m", specs=specs,
        ledger=ExposureLedger(), resolver=None,
        executor=_FakeExecutor(balance), feed=_FakeFeed(),
        profit_calc=profit_calc,
    )


# ─── 风险额来源 ───


def test_calc_qty_fixed_amount_nominal():
    """固定金额 + 名义换算：risk/loss_per_unit，按 step 取整。"""
    api = _make_api()
    # loss_per_unit = 0.0010 × 100000 = 100；qty = 100 / 100 = 1
    qty = api.calc_qty(risk_amount=100, stop_distance=Decimal("0.0010"))
    assert qty == Decimal("1")


def test_calc_qty_risk_pct_of_equity():
    """百分比：risk = 权益 × pct / 100。"""
    api = _make_api(balance="10000")
    # risk = 10000 × 1% = 100 → qty = 1
    qty = api.calc_qty(risk_pct=1.0, stop_distance=Decimal("0.0010"))
    assert qty == Decimal("1")


def test_calc_qty_risk_amount_takes_priority():
    """risk_amount 与 risk_pct 同时给出 → risk_amount 优先。"""
    api = _make_api(balance="10000")
    qty = api.calc_qty(risk_amount=100, risk_pct=50,
                       stop_distance=Decimal("0.0010"))
    assert qty == Decimal("1")   # 用 100 而非 5000


def test_calc_qty_no_risk_param_raises():
    """两个风险参数都缺 → ValueError。"""
    api = _make_api()
    with pytest.raises(ValueError):
        api.calc_qty(stop_distance=Decimal("0.0010"))


# ─── 交叉盘换算钩子 ───


def test_calc_qty_profit_calc_hook_used():
    """注入 profit_calc → 用其精确换算（含交叉盘）而非名义。"""
    calls = []

    def pc(sym, delta, vol):
        calls.append((sym, delta, vol))
        return Decimal("50")   # 每单位损失 50（账户币）

    api = _make_api(profit_calc=pc)
    qty = api.calc_qty(risk_amount=100, stop_distance=Decimal("0.0010"))
    assert qty == Decimal("2")   # 100 / 50
    assert calls and calls[0][0] == "EURUSD" and calls[0][2] == Decimal("1")


def test_calc_qty_profit_calc_failure_falls_back():
    """profit_calc 抛异常 → 告警并回落 spec 名义计算。"""

    def pc(sym, delta, vol):
        raise RuntimeError("mt5 down")

    api = _make_api(profit_calc=pc)
    qty = api.calc_qty(risk_amount=100, stop_distance=Decimal("0.0010"))
    assert qty == Decimal("1")   # 回落名义 = 100 / (0.0010×100000)


# ─── 取整与钳制 ───


def test_calc_qty_step_round_down():
    """按 qty_step 向下取整（不四舍五入、不向上）。"""
    specs = {"EURUSD": _spec(contract_multiplier="100")}
    api = _make_api(specs=specs)
    # loss_per_unit = 1 × 100 = 100；raw = 123.4 / 100 = 1.234 → 1.23
    qty = api.calc_qty(risk_amount="123.4", stop_distance=Decimal("1"))
    assert qty == Decimal("1.23")


def test_calc_qty_clamp_to_max():
    """超过 qty_max → 钳制到上限。"""
    specs = {"EURUSD": _spec(contract_multiplier="1", qty_max="200")}
    api = _make_api(specs=specs)
    qty = api.calc_qty(risk_amount=10_000_000, stop_distance=Decimal("1"))
    assert qty == Decimal("200")


def test_calc_qty_below_min_returns_zero():
    """不足 min_qty → 返回 0（拒开，仓位太小无意义）。"""
    api = _make_api()
    # loss_per_unit = 1 × 100000 = 100000；raw = 1 / 100000 = 1e-5 → step 取整为 0
    qty = api.calc_qty(risk_amount=1, stop_distance=Decimal("1"))
    assert qty == Decimal("0")


# ─── 非法输入边界 ───


def test_calc_qty_zero_stop_returns_zero():
    """stop_distance=0 → 返回 0（避免除零）。"""
    api = _make_api()
    assert api.calc_qty(risk_amount=100, stop_distance=Decimal("0")) == Decimal("0")


def test_calc_qty_negative_stop_returns_zero():
    """stop_distance<0 → 返回 0。"""
    api = _make_api()
    assert api.calc_qty(risk_amount=100, stop_distance=Decimal("-1")) == Decimal("0")


def test_calc_qty_zero_risk_returns_zero():
    """风险额<=0 → 返回 0。"""
    api = _make_api()
    assert api.calc_qty(risk_amount=0, stop_distance=Decimal("0.0010")) == Decimal("0")


def test_calc_qty_routes_named_symbol_spec():
    """显式 symbol → 用该品种各自 spec（不同 contract_multiplier 不混用）。"""
    specs = {
        "EURUSD": _spec("EURUSD", contract_multiplier="100000"),
        "USDJPY": _spec("USDJPY", contract_multiplier="1000", pip="0.01"),
    }
    api = _make_api(specs=specs)
    # EURUSD: 100 / (0.0010×100000=100) = 1
    assert api.calc_qty("EURUSD", risk_amount=100,
                        stop_distance=Decimal("0.0010")) == Decimal("1")
    # USDJPY: 100 / (0.01×1000=10) = 10
    assert api.calc_qty("USDJPY", risk_amount=100,
                        stop_distance=Decimal("0.01")) == Decimal("10")

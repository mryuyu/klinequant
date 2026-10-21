"""RiskEngine 接线适配层单测（kqsdk/risk_adapter.py）

覆盖：SdkRiskAdapter 的 Order 壳适配（市价单 price=None + extra.last_price 回落、
限价单用 price）、RiskDataProvider 全局状态供给（持仓按 symbol 聚合、全局/单策略
笔数、strategy_pnl 按 tag 增量、连续亏损负增量计数、日盈亏 UTC 跨日重置）、
build_risk_rules 装配（未列既有规则默认禁用）、共享实例加锁并发串行化。
"""
from __future__ import annotations

import threading
from decimal import Decimal

import pytest

from config.strategy_config import RiskConfig
from core.risk_engine.rules.base import RiskCheckResult
from kqsdk.risk_adapter import (
    RiskDataProvider,
    SdkRiskAdapter,
    assemble_risk,
    build_risk_rules,
)
from protocol.types import Offset, OrderKind, OrderSide, OrderType, Position


def _pos(symbol, tag, effective, realized="0", avg="0") -> Position:
    eff = Decimal(effective)
    return Position(
        symbol=symbol, tag=tag,
        side="LONG" if eff > 0 else ("SHORT" if eff < 0 else "FLAT"),
        quantity=eff, volume=eff, effective=eff,
        realized_pnl=Decimal(realized), avg_entry_price=Decimal(avg),
    )


class _FakeLedger:
    """RiskDataProvider 只消费 all_positions()。"""

    def __init__(self, positions=None):
        self._positions = list(positions or [])

    def all_positions(self):
        return list(self._positions)

    def set_positions(self, positions):
        self._positions = list(positions)


class _CaptureEngine:
    """假 RiskEngine：捕获 RiskContext，返回预设结果。"""

    def __init__(self, result=None):
        self.ctx = None
        self._result = result or RiskCheckResult.ok("all_rules")

    def check_order(self, ctx):
        self.ctx = ctx
        return self._result


def _provider(positions=None, equity="10000", clock=None):
    ledger = _FakeLedger(positions)
    kw = {"clock": clock} if clock is not None else {}
    return RiskDataProvider(ledger, lambda: Decimal(equity), **kw), ledger


# ─── RiskDataProvider：持仓视图与笔数 ───


def test_snapshot_positions_aggregate_by_symbol():
    """全 tag 按 symbol 聚合净持仓（全局统一视图）。"""
    provider, _ = _provider([
        _pos("EURUSD", "s1", "0.5"),
        _pos("EURUSD", "s2", "-0.2"),
        _pos("GBPUSD", "s1", "0.3"),
    ])
    snap = provider.snapshot(tag="s1", offset=Offset.OPEN)
    pos = snap["positions"]
    assert pos["EURUSD"].volume == Decimal("0.3")   # 0.5 + (-0.2)
    assert pos["EURUSD"].side == "LONG"
    assert pos["GBPUSD"].volume == Decimal("0.3")


def test_snapshot_position_counts():
    """全局笔数 = effective≠0 的 (symbol,tag)；单策略按 tag 前缀归集。"""
    provider, _ = _provider([
        _pos("EURUSD", "s1", "0.5"),
        _pos("GBPUSD", "s1:15m", "0.3"),   # 基名仍 s1
        _pos("USDJPY", "s2", "0.1"),
        _pos("XXXUSD", "s3", "0"),          # effective 0 → 不计
    ])
    extra = provider.snapshot(tag="s1", offset=Offset.OPEN)["extra"]
    assert extra["total_positions"] == 3
    assert extra["strategy_positions"]["s1"] == 2
    assert extra["strategy_positions"]["s2"] == 1
    assert extra["offset"] == "OPEN"


# ─── RiskDataProvider：策略盈亏与连续亏损 ───


def test_strategy_pnl_increment_by_tag():
    """strategy_pnl = 当前累计 realized_pnl − 日起始基线（按 tag）。"""
    provider, ledger = _provider([_pos("EURUSD", "s1", "0.5", realized="0")])
    assert provider.snapshot(tag="s1", offset=Offset.OPEN)["strategy_pnl"]["s1"] == Decimal("0")
    ledger.set_positions([_pos("EURUSD", "s1", "0.5", realized="30")])
    assert provider.snapshot(tag="s1", offset=Offset.OPEN)["strategy_pnl"]["s1"] == Decimal("30")


def test_consecutive_losses_counts_negative_deltas():
    """连续亏损 = realized_pnl 负向增量步数（正向增量清零）。"""
    provider, ledger = _provider([_pos("EURUSD", "s1", "0.5", realized="0")])
    provider.snapshot(tag="s1", offset=Offset.OPEN)   # 建立追踪基线
    ledger.set_positions([_pos("EURUSD", "s1", "0.5", realized="-10")])
    assert provider.snapshot(tag="s1", offset=Offset.OPEN)["strategy_consecutive_losses"]["s1"] == 1
    ledger.set_positions([_pos("EURUSD", "s1", "0.5", realized="-25")])
    assert provider.snapshot(tag="s1", offset=Offset.OPEN)["strategy_consecutive_losses"]["s1"] == 2
    ledger.set_positions([_pos("EURUSD", "s1", "0.5", realized="-5")])
    assert provider.snapshot(tag="s1", offset=Offset.OPEN)["strategy_consecutive_losses"]["s1"] == 0


# ─── RiskDataProvider：日盈亏与 UTC 跨日重置 ───


def test_daily_pnl_and_utc_day_roll():
    """日盈亏 = 权益 − 日起始快照；跨 UTC 日自动重置基线（假时钟驱动）。"""
    equity = {"v": Decimal("10000")}
    clock = {"t": 1_700_000_000.0}
    provider = RiskDataProvider(
        _FakeLedger([]), lambda: equity["v"], clock=lambda: clock["t"],
    )
    assert provider.snapshot(tag="s1", offset=Offset.OPEN)["daily_pnl"] == Decimal("0")
    equity["v"] = Decimal("9800")
    snap = provider.snapshot(tag="s1", offset=Offset.OPEN)
    assert snap["daily_pnl"] == Decimal("-200")
    assert float(snap["extra"]["daily_pnl_pct"]) == pytest.approx(-2.0, abs=1e-6)
    # 跨到下一个 UTC 日 → 基线重置为当前权益
    clock["t"] += 86_400 * 2
    assert provider.snapshot(tag="s1", offset=Offset.OPEN)["daily_pnl"] == Decimal("0")


# ─── SdkRiskAdapter：Order 壳适配 ───


def test_adapter_market_order_uses_last_price():
    """市价单：order_type=MARKET、price=None，last_price 落 extra（规则估算名义值）。"""
    provider, _ = _provider([])
    engine = _CaptureEngine()
    adapter = SdkRiskAdapter(engine, provider)
    res = adapter.check(
        symbol="EURUSD", tag="s1:1m", side=OrderSide.BUY, offset=Offset.OPEN,
        qty=Decimal("0.5"), kind=OrderKind.MARKET, last_price=Decimal("1.1234"),
    )
    assert res.passed
    order = engine.ctx.order
    assert order.order_id == ""
    assert order.symbol == "EURUSD"
    assert order.strategy_id == "s1:1m"
    assert order.order_type == OrderType.MARKET
    assert order.price is None
    assert order.quantity == Decimal("0.5")
    assert engine.ctx.extra["last_price"] == Decimal("1.1234")


def test_adapter_limit_order_uses_price():
    """限价单：order_type=LIMIT、price 落 order；last_price 不再进 extra。"""
    provider, _ = _provider([])
    engine = _CaptureEngine()
    adapter = SdkRiskAdapter(engine, provider)
    adapter.check(
        symbol="EURUSD", tag="s1", side=OrderSide.SELL, offset=Offset.CLOSE,
        qty=Decimal("0.2"), kind=OrderKind.LIMIT,
        price=Decimal("1.10"), last_price=Decimal("1.1234"),
    )
    order = engine.ctx.order
    assert order.order_type == OrderType.LIMIT
    assert order.price == Decimal("1.10")
    assert "last_price" not in engine.ctx.extra
    assert engine.ctx.extra["offset"] == "CLOSE"


def test_adapter_propagates_rejection():
    """引擎拒绝结果原样透传给调用方（策略侧据 passed 决策）。"""
    provider, _ = _provider([])
    engine = _CaptureEngine(result=RiskCheckResult.reject("max_total_positions", "too many"))
    adapter = SdkRiskAdapter(engine, provider)
    res = adapter.check(
        symbol="EURUSD", tag="s1", side=OrderSide.BUY, offset=Offset.OPEN,
        qty=Decimal("0.1"),
    )
    assert not res.passed
    assert res.rule_name == "max_total_positions"


# ─── build_risk_rules：YAML 装配 ───


def test_build_risk_rules_disables_unlisted_legacy():
    """未在 yaml 出现的既有规则默认禁用；列出的启用并更新参数。"""
    cfg = RiskConfig(
        rules={"max_daily_loss": {"enabled": True, "max_loss": 200}},
        max_total_positions=5,
    )
    by_name = {r.name: r for r in build_risk_rules(cfg)}
    assert by_name["max_daily_loss"].enabled is True
    assert by_name["max_daily_loss"].params["max_loss"] == 200
    # 未列出的既有规则禁用（FX 语义不适用规则不按默认参数误拒）
    assert by_name["order_frequency"].enabled is False
    assert by_name["max_order_amount"].enabled is False
    # SDK 新增规则在场且参数正确
    assert by_name["max_total_positions"].params["max_positions"] == 5
    assert "daily_loss_pct" in by_name
    assert "max_positions_per_strategy" in by_name


def test_build_risk_rules_explicit_disable():
    """yaml 显式 enabled=false 的既有规则被禁用。"""
    cfg = RiskConfig(rules={"night_trading": {"enabled": False}})
    by_name = {r.name: r for r in build_risk_rules(cfg)}
    assert by_name["night_trading"].enabled is False


# ─── 共享实例加锁并发 ───


def test_shared_adapter_serializes_concurrent_checks():
    """多策略线程共享一个 adapter：加锁串行化，统计无丢失。"""
    adapter = assemble_risk(RiskConfig(), _FakeLedger([]),
                            lambda: Decimal("10000"), symbols=[])
    results: list[bool] = []
    lock = threading.Lock()

    def worker():
        for _ in range(20):
            r = adapter.check(
                symbol="EURUSD", tag="s1", side=OrderSide.BUY,
                offset=Offset.OPEN, qty=Decimal("0.1"), last_price=Decimal("1.1"),
            )
            with lock:
                results.append(r.passed)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(results) == 160
    assert all(results)   # 空配置 → 既有规则全禁用、SDK 规则参数 0 恒放行
    assert adapter.engine.stats["total_checks"] == 160

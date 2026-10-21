"""多策略组合回测单测（kqsdk/backtest_runner.py + backtest_pump.py）

覆盖：Gate 的 off-by-one ack / 幂等终止 / drain、GatedFeed 读操作委托真实 feed、
run_portfolio 确定性 lockstep（同输入两次回放结果一致）、组合报表 + 分策略归属
（trade.strategy = 策略名）、反 look-ahead（每步可见 bar 数 == 已揭示步数）。

回测组件用注入的 bars/specs，不依赖 MT5。两策略各用**不同品种**——BacktestExecutor
按 symbol 净持仓，同品种会被 netting 合并（故组合归属测试须品种隔离）。
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from config.strategy_config import (
    BacktestConfig,
    StrategiesConfig,
    StrategyInstanceConfig,
)
from core.trade_engine.spec_loader import load_spec_from_mt5_dict
from kqsdk import registry
from kqsdk.backtest_feed import BacktestDataFeed
from kqsdk.backtest_pump import Gate, GatedFeed
from kqsdk.backtest_runner import BacktestRunner, PortfolioReport
from kqsdk.registry import strategy as kq_strategy
from protocol.types import Offset, OrderSide

_MOD = "tests.unit.test_backtest_multi"
BASE_TS = 1_600_000_000_000
M1 = 60_000
_INFO = {
    "digits": 5, "point": 0.00001, "trade_contract_size": 100000,
    "volume_step": 0.01, "volume_min": 0.01, "volume_max": 200,
    "trade_mode": 4, "spread": 0,
    "currency_base": "EUR", "currency_profit": "USD", "margin_currency": "USD",
}

# 反 look-ahead 记录（模块级，测试前清空；策略在工作线程内追加）
_LOOKAHEAD: list[tuple[int, int]] = []


def _spec(symbol: str):
    return load_spec_from_mt5_dict(_INFO, symbol)


def _series(symbol: str, n: int, base: float = 100.0):
    """n 根 1m bar，价格随序号递增（确定性趋势，便于断言盈亏与归属）。"""
    return [
        {
            "symbol": symbol, "period": "1m",
            "timestamp": BASE_TS + i * M1,
            "open": base + i, "high": base + i + 0.5,
            "low": base + i - 0.5, "close": base + i, "volume": 1000.0,
        }
        for i in range(n)
    ]


def _open_once(api, side: OrderSide):
    """第 3 根新 bar 开仓一次，持有到末尾（run_portfolio 的 flatten 平仓成 Trade）。"""
    sym = api.symbol_info().symbol
    last_ts = None
    n = 0
    opened = False
    while api.wait_update():
        bars = api.klines(count=50)
        if len(bars) < 2:
            continue
        ts = int(bars[-1]["timestamp"])
        if ts == last_ts:
            continue
        last_ts = ts
        n += 1
        if not opened and n >= 3:
            api.send_order(side, Offset.OPEN, Decimal("0.01"), symbol=sym)
            opened = True


@kq_strategy("_test_bt_a")
def _bt_a(api, ctx):
    _open_once(api, OrderSide.BUY)


@kq_strategy("_test_bt_b")
def _bt_b(api, ctx):
    _open_once(api, OrderSide.SELL)


@kq_strategy("_test_bt_lookahead")
def _bt_lookahead(api, ctx):
    step = 0
    while api.wait_update():
        step += 1
        _LOOKAHEAD.append((step, len(api.klines(count=1000))))


@pytest.fixture(scope="module", autouse=True)
def _cleanup_registry():
    yield
    with registry._REGISTRY_LOCK:
        for n in list(registry._REGISTRY):
            if n.startswith("_test_bt_"):
                del registry._REGISTRY[n]


def _cfg(strategies, capital=10000):
    return StrategiesConfig(
        strategies=strategies,
        backtest=BacktestConfig(initial_capital=capital),
    )


def _two_symbol_cfg():
    return _cfg([
        StrategyInstanceConfig(name="_test_bt_a", module=_MOD, symbols=["AAAUSD"],
                               period="1m", periods=["1m"]),
        StrategyInstanceConfig(name="_test_bt_b", module=_MOD, symbols=["BBBUSD"],
                               period="1m", periods=["1m"]),
    ])


def _run_two(n_bars=10) -> PortfolioReport:
    cfg = _two_symbol_cfg()
    runner = BacktestRunner.from_config(
        cfg,
        bars_by_symbol={"AAAUSD": _series("AAAUSD", n_bars),
                        "BBBUSD": _series("BBBUSD", n_bars)},
        specs={"AAAUSD": _spec("AAAUSD"), "BBBUSD": _spec("BBBUSD")},
    )
    return runner.run_portfolio()


# ─── Gate / GatedFeed 原语 ───


def test_gate_offby_one_ack_and_idempotent_terminate():
    """publish→next_step 取事件；terminate 后幂等返回 False；drain 安全收尾。"""
    g = Gate("t")
    g.publish(0, {"v": 0})
    assert g.next_step() == (True, {"v": 0})
    g.publish(1, {"v": 1})
    assert g.next_step() == (True, {"v": 1})   # 内部 ack 上一步
    g.terminate()
    assert g.next_step() == (False, {})
    assert g.next_step() == (False, {})        # 幂等：已终止立即返回
    g.drain()                                  # 不阻塞、瞬间返回


def test_gated_feed_delegates_reads_to_real_feed():
    """GatedFeed：wait_* 经 gate，读操作委托真实 feed。"""
    real = BacktestDataFeed({"AAAUSD": {"1m": _series("AAAUSD", 5)}}, periods=["1m"])
    g = Gate("g")
    gf = GatedFeed(real, g)
    g.publish(0, {"x": 1})
    assert gf.wait_and_snapshot() == (True, {"x": 1})
    assert gf.now_ms() == real.now_ms()
    assert gf.seconds_since_update() == 0.0   # 回测无断线
    bars = gf.latest_bars("AAAUSD", "1m", 10)
    assert isinstance(bars, list)


# ─── 组合回测：确定性 + 归属 ───


def test_portfolio_deterministic_replay():
    """同输入两次回放：步数/成交数/组合权益曲线/最终权益完全一致（可复现）。"""
    r1 = _run_two()
    r2 = _run_two()
    assert r1.n_steps == r2.n_steps > 0
    assert r1.combined.total_trades == r2.combined.total_trades
    assert r1.combined_equity_curve == r2.combined_equity_curve
    assert abs(r1.combined.final_equity - r2.combined.final_equity) < 1e-9


def test_portfolio_per_strategy_attribution():
    """组合报表按策略名拆分 trades；每笔 trade.strategy 归属正确。"""
    rep = _run_two()
    assert set(rep.strategies.keys()) == {"_test_bt_a", "_test_bt_b"}
    a = rep.strategies["_test_bt_a"]
    b = rep.strategies["_test_bt_b"]
    assert a.symbols == ["AAAUSD"]
    assert b.symbols == ["BBBUSD"]
    assert a.trades and all(t.strategy == "_test_bt_a" for t in a.trades)
    assert b.trades and all(t.strategy == "_test_bt_b" for t in b.trades)
    # 组合成交 = 各策略成交之和（品种隔离，无 netting 合并）
    assert rep.combined.total_trades == len(a.trades) + len(b.trades)


def test_portfolio_strategy_pnl_sign_matches_side():
    """价格递增序列：BUY 策略已实现盈亏 > 0，SELL 策略 < 0（归属与方向自洽）。"""
    rep = _run_two()
    a_pnl = sum(float(t.pnl) for t in rep.strategies["_test_bt_a"].trades)
    b_pnl = sum(float(t.pnl) for t in rep.strategies["_test_bt_b"].trades)
    assert a_pnl > 0
    assert b_pnl < 0


def test_portfolio_only_filter():
    """--only 只回测指定策略（组合仅含该策略成交）。"""
    cfg = _two_symbol_cfg()
    runner = BacktestRunner.from_config(
        cfg,
        bars_by_symbol={"AAAUSD": _series("AAAUSD", 10), "BBBUSD": _series("BBBUSD", 10)},
        specs={"AAAUSD": _spec("AAAUSD"), "BBBUSD": _spec("BBBUSD")},
        only=["_test_bt_a"],
    )
    rep = runner.run_portfolio()
    assert set(rep.strategies.keys()) == {"_test_bt_a"}


def test_portfolio_no_enabled_raises():
    """无可回测策略 → ValueError。"""
    cfg = _cfg([
        StrategyInstanceConfig(name="_test_bt_a", module=_MOD,
                               symbols=["AAAUSD"], enabled=False),
    ])
    with pytest.raises(ValueError):
        BacktestRunner.from_config(
            cfg, bars_by_symbol={"AAAUSD": _series("AAAUSD", 5)},
            specs={"AAAUSD": _spec("AAAUSD")},
        )


# ─── 反 look-ahead ───


def test_no_lookahead_visible_bars_match_revealed_steps():
    """单策略单品种：第 k 步恰可见 k 根 bar（绝不提前看到未来 bar）。"""
    _LOOKAHEAD.clear()
    n = 8
    cfg = _cfg([
        StrategyInstanceConfig(name="_test_bt_lookahead", module=_MOD,
                               symbols=["AAAUSD"], period="1m", periods=["1m"]),
    ])
    runner = BacktestRunner.from_config(
        cfg, bars_by_symbol={"AAAUSD": _series("AAAUSD", n)},
        specs={"AAAUSD": _spec("AAAUSD")},
    )
    runner.run_portfolio()
    assert _LOOKAHEAD == [(k, k) for k in range(1, n + 1)]

"""FX 同构回测路由单测（gateway/routers/backtest_fx.py）

覆盖：
  - 纯函数：策略扫描/加载、report/trade 序列化、inf/nan 归 None
  - 集成：_run_fx_backtest_sync 用 fake BacktestRunner（跳过 MT5）验证 results 结构
  - 端点：/strategies、/run、/result（COMPLETED/PENDING/404）

不依赖 MT5：集成测试 monkeypatch BacktestRunner 与 _load_strategy。
"""
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.backtest_engine.performance import PerformanceReport, Trade
from gateway.routers import backtest_fx
from gateway.routers.backtest_fx import (
    FxBacktestRunRequest,
    _f,
    _list_strategies,
    _load_strategy,
    _run_fx_backtest_sync,
    _serialize_report,
    _serialize_trade,
)
from strategy.sdk.backtest_runner import BacktestReport, SymbolBacktestResult


@pytest.fixture
def client():
    """最小 app 只挂 backtest_fx 路由（避开 create_app 的数据源 bootstrap）。"""
    app = FastAPI()
    app.include_router(backtest_fx.router)
    return TestClient(app)


# ─── 纯函数 ───


def test_list_strategies_includes_fx():
    names = _list_strategies()
    assert "fx_simple_test" in names
    assert "fx_macd_demo" in names


def test_load_strategy_returns_callable():
    assert callable(_load_strategy("fx_simple_test"))


def test_load_strategy_invalid_raises():
    with pytest.raises(Exception):
        _load_strategy("nonexistent_strategy_xyz")


def test_f_helper_inf_nan_to_none():
    assert _f(None) is None
    assert _f(float("inf")) is None
    assert _f(float("nan")) is None
    assert _f(Decimal("1.5")) == 1.5


def test_serialize_report_inf_becomes_none():
    rep = PerformanceReport(
        total_return=0.1, profit_factor=float("inf"), sharpe_ratio=float("nan")
    )
    d = _serialize_report(rep)
    assert d["total_return"] == 0.1
    assert d["profit_factor"] is None   # inf → None（JSON 安全）
    assert d["sharpe_ratio"] is None    # nan → None
    assert d["total_trades"] == 0


def test_serialize_trade_fields():
    t = Trade(
        symbol="EURUSD", side="SHORT", entry_price=Decimal("1.2"),
        exit_price=Decimal("1.1"), quantity=Decimal("0.01"),
        entry_time=100, exit_time=200, pnl=Decimal("10"),
        fee=Decimal("0.7"), bars_held=5,
    )
    d = _serialize_trade(t)
    assert d["side"] == "SHORT"
    assert d["entry_price"] == 1.2
    assert d["pnl"] == 10.0
    assert d["bars_held"] == 5


# ─── 集成：fake runner 跳过 MT5 ───


class _FakeRunner:
    """接受路由传参，run() 返回预构造 BacktestReport（含 inf profit_factor）。"""

    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def run(self):
        sym = self._kwargs["symbols"][0]
        rep = PerformanceReport(
            total_return=0.05, annual_return=0.5, sharpe_ratio=1.2,
            sortino_ratio=1.5, max_drawdown=0.03, calmar_ratio=2.0,
            win_rate=0.6, profit_factor=float("inf"), avg_win=10.0,
            avg_loss=5.0, total_trades=1, total_fees=0.5,
            initial_capital=10000.0, final_equity=10500.0,
        )
        trade = Trade(
            symbol=sym, side="LONG", entry_price=Decimal("1.1"),
            exit_price=Decimal("1.15"), quantity=Decimal("0.01"),
            entry_time=1, exit_time=2, pnl=Decimal("50"),
            fee=Decimal("0.5"), bars_held=1,
        )
        res = SymbolBacktestResult(
            symbol=sym, report=rep, trades=[trade],
            equity_curve=[10000.0, 10250.0, 10500.0], n_bars=3,
        )
        return BacktestReport(results={sym: res}, period="1m", initial_capital=10000.0)


def test_run_fx_sync_serializes_results(monkeypatch):
    monkeypatch.setattr(backtest_fx, "_load_strategy", lambda name: (lambda api: None))
    monkeypatch.setattr("strategy.sdk.backtest_runner.BacktestRunner", _FakeRunner)

    body = FxBacktestRunRequest(
        symbols=["EURUSD"], period="1m", strategy="fx_simple_test",
        history_bars=100, capital=10000.0,
    )
    tid = "fxbt_sync1"
    backtest_fx._tasks[tid] = {"task_id": tid, "status": "PENDING"}
    _run_fx_backtest_sync(tid, body)

    task = backtest_fx._tasks[tid]
    assert task["status"] == "COMPLETED"
    assert task["n_symbols"] == 1
    r = task["results"]["EURUSD"]
    assert r["report"]["total_return"] == 0.05
    assert r["report"]["profit_factor"] is None  # inf → None
    assert r["n_bars"] == 3
    assert r["equity_curve"] == [10000.0, 10250.0, 10500.0]
    assert r["trades"][0]["side"] == "LONG"
    assert r["trades"][0]["pnl"] == 50.0


def test_run_fx_sync_failure_marks_failed(monkeypatch):
    def _boom(name):
        raise ValueError("no such strategy")

    monkeypatch.setattr(backtest_fx, "_load_strategy", _boom)
    body = FxBacktestRunRequest(symbols=["EURUSD"], strategy="bad")
    tid = "fxbt_fail1"
    backtest_fx._tasks[tid] = {"task_id": tid, "status": "PENDING"}
    _run_fx_backtest_sync(tid, body)
    assert backtest_fx._tasks[tid]["status"] == "FAILED"
    assert "no such strategy" in backtest_fx._tasks[tid]["error"]


# ─── 端点 ───


def test_strategies_endpoint(client):
    resp = client.get("/api/backtest/fx/strategies")
    assert resp.status_code == 200
    assert "fx_simple_test" in resp.json()["strategies"]


def test_run_endpoint_returns_task_id(client, monkeypatch):
    monkeypatch.setattr(backtest_fx, "_run_fx_backtest_sync", lambda tid, body: None)
    resp = client.post("/api/backtest/fx/run", json={
        "symbols": ["EURUSD"], "period": "1m",
        "strategy": "fx_simple_test", "history_bars": 100, "capital": 10000,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "PENDING"
    assert data["task_id"].startswith("fxbt_")


def test_run_endpoint_empty_symbols_400(client):
    resp = client.post("/api/backtest/fx/run", json={
        "symbols": [], "period": "1m", "strategy": "fx_simple_test",
    })
    assert resp.status_code == 400


def test_result_endpoint_completed(client):
    tid = "fxbt_res1"
    backtest_fx._tasks[tid] = {
        "task_id": tid, "status": "COMPLETED", "strategy": "s",
        "symbols": ["EURUSD"], "period": "1m", "history_bars": 100,
        "capital": 10000.0, "n_symbols": 1,
        "results": {"EURUSD": {"report": {"total_return": 0.05},
                               "equity_curve": [1.0], "trades": [], "n_bars": 3}},
        "created_at": 0, "completed_at": 1,
    }
    resp = client.get(f"/api/backtest/fx/result/{tid}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "COMPLETED"
    assert body["results"]["EURUSD"]["n_bars"] == 3


def test_result_endpoint_pending(client):
    tid = "fxbt_pend1"
    backtest_fx._tasks[tid] = {"task_id": tid, "status": "RUNNING"}
    resp = client.get(f"/api/backtest/fx/result/{tid}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "RUNNING"


def test_result_endpoint_404(client):
    resp = client.get("/api/backtest/fx/result/nonexist_xyz")
    assert resp.status_code == 404

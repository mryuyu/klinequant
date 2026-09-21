"""FX 同构回测路由

复用 strategy/sdk 的 BacktestRunner（回测/实盘同构：同一份 strategy(api: KqApi)，
MT5 历史数据驱动），把绩效报告 / 资金曲线 / 交易明细暴露给前端 BacktestView。

与旧 backtest.py（币安 REST + BacktestEngine + dual_ma 回调式）并存、互不影响：
旧路由面向加密货币 dual_ma，本路由面向外汇 strategy(api) 同构回测。

API：
    GET  /api/backtest/fx/strategies      — 可用策略列表（strategies/ 下含 strategy(api) 的模块）
    POST /api/backtest/fx/run             — 提交 FX 回测任务（线程池异步执行）
    GET  /api/backtest/fx/result/{task_id} — 回测结果（逐品种 report/equity_curve/trades）
"""
from __future__ import annotations

import asyncio
import importlib
import logging
import math
import time
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/backtest/fx", tags=["backtest-fx"])
logger = logging.getLogger(__name__)

# 回测任务内存存储（进程级，重启即失）
_tasks: Dict[str, dict] = {}

# strategies/ 目录（klinequant/strategies）
_STRATEGIES_DIR = Path(__file__).resolve().parents[2] / "strategies"


class FxBacktestRunRequest(BaseModel):
    symbols: List[str] = Field(default_factory=lambda: ["EURUSD"])
    period: str = "1m"             # 1m/5m/15m/30m/1h/4h/1d
    strategy: str = "fx_simple_test"
    history_bars: int = 1500       # 从 MT5 拉取的历史 bar 数
    capital: float = 10000.0
    magic: int = 202609
    account_currency: str = "USD"
    slippage: float = 0.0001       # percentage 滑点（pct）
    fee: float = 0.0               # 每笔固定手续费（quote 单位）


def _f(v: Any) -> Optional[float]:
    """float 化并把 inf/nan 归 None（JSON 不能表示 Infinity/NaN）。"""
    if v is None:
        return None
    fv = float(v)
    return fv if math.isfinite(fv) else None


def _list_strategies() -> List[str]:
    """扫描 strategies/ 下含 `def strategy(` 的模块名（不 import，避免副作用）。"""
    names: List[str] = []
    if not _STRATEGIES_DIR.is_dir():
        return names
    for p in sorted(_STRATEGIES_DIR.glob("*.py")):
        if p.name.startswith("__"):
            continue
        try:
            if "def strategy(" in p.read_text(encoding="utf-8"):
                names.append(p.stem)
        except Exception:
            continue
    return names


def _load_strategy(name: str):
    """importlib 加载 strategies.{name}.strategy（同 run_fx_backtest.py）。"""
    mod = importlib.import_module(f"strategies.{name}")
    fn = getattr(mod, "strategy", None)
    if not callable(fn):
        raise ValueError(f"strategies.{name} 无可调用的 strategy(api) 函数")
    return fn


def _serialize_report(rep: Any) -> Dict[str, Any]:
    """PerformanceReport → 前端友好 dict（字段名与旧 backtest 路由对齐）。"""
    return {
        "total_return": _f(rep.total_return),
        "annual_return": _f(rep.annual_return),
        "sharpe_ratio": _f(rep.sharpe_ratio),
        "sortino_ratio": _f(rep.sortino_ratio),
        "max_drawdown": _f(rep.max_drawdown),
        "calmar_ratio": _f(rep.calmar_ratio),
        "win_rate": _f(rep.win_rate),
        "profit_factor": _f(rep.profit_factor),
        "total_trades": int(rep.total_trades),
        "avg_win": _f(rep.avg_win),
        "avg_loss": _f(rep.avg_loss),
        "total_fees": _f(rep.total_fees),
        "initial_capital": _f(rep.initial_capital),
        "final_equity": _f(rep.final_equity),
    }


def _serialize_trade(t: Any) -> Dict[str, Any]:
    """Trade（一开一平）→ dict（与旧 backtest 路由 trades 同结构）。"""
    return {
        "symbol": t.symbol,
        "side": t.side,
        "entry_price": float(t.entry_price),
        "exit_price": float(t.exit_price),
        "quantity": float(t.quantity),
        "entry_time": int(t.entry_time),
        "exit_time": int(t.exit_time),
        "pnl": float(t.pnl),
        "fee": float(t.fee),
        "bars_held": int(t.bars_held),
    }


def _run_fx_backtest_sync(task_id: str, body: FxBacktestRunRequest):
    """线程池内同步执行 BacktestRunner（内部起 MT5 子进程拉历史 + 回放）。"""
    from strategy.sdk.backtest_runner import BacktestRunner

    try:
        _tasks[task_id]["status"] = "RUNNING"
        strategy_fn = _load_strategy(body.strategy)
        runner = BacktestRunner(
            symbols=body.symbols,
            period=body.period,
            strategy_fn=strategy_fn,
            history_bars=body.history_bars,
            initial_capital=Decimal(str(body.capital)),
            slippage_model="percentage",
            slippage_params={"pct": Decimal(str(body.slippage))},
            fee_model="fixed",
            fee_params={"fee_per_trade": Decimal(str(body.fee))},
            magic=body.magic,
            account_currency=body.account_currency,
        )
        report = runner.run()

        results: Dict[str, Any] = {}
        for sym, r in report.results.items():
            results[sym] = {
                "report": _serialize_report(r.report),
                "equity_curve": [float(x) for x in r.equity_curve],
                "trades": [_serialize_trade(t) for t in r.trades],
                "n_bars": r.n_bars,
            }

        _tasks[task_id].update({
            "status": "COMPLETED",
            "completed_at": int(time.time() * 1000),
            "n_symbols": report.results and len(report.results) or 0,
            "results": results,
        })
    except Exception as e:  # noqa: BLE001
        logger.error(f"[FX-BT] task {task_id} failed: {e}", exc_info=True)
        _tasks[task_id].update({
            "status": "FAILED",
            "error": str(e),
            "completed_at": int(time.time() * 1000),
        })


@router.get("/strategies")
async def list_strategies():
    """可用 FX 策略列表。"""
    return {"strategies": _list_strategies()}


@router.post("/run")
async def run_fx_backtest(body: FxBacktestRunRequest):
    """提交 FX 同构回测任务（异步执行，轮询 /result 取结果）。"""
    if not body.symbols:
        raise HTTPException(status_code=400, detail="symbols 不能为空")

    task_id = f"fxbt_{uuid.uuid4().hex[:8]}"
    _tasks[task_id] = {
        "task_id": task_id,
        "strategy": body.strategy,
        "symbols": body.symbols,
        "period": body.period,
        "history_bars": body.history_bars,
        "capital": body.capital,
        "status": "PENDING",
        "created_at": int(time.time() * 1000),
    }

    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, _run_fx_backtest_sync, task_id, body)

    return {
        "task_id": task_id,
        "status": "PENDING",
        "strategy": body.strategy,
        "symbols": body.symbols,
    }


@router.get("/result/{task_id}")
async def get_fx_result(task_id: str):
    """获取 FX 回测结果（逐品种 report + equity_curve + trades）。"""
    task = _tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")

    if task["status"] != "COMPLETED":
        return {
            "task_id": task_id,
            "status": task["status"],
            "error": task.get("error"),
        }

    return {
        "task_id": task_id,
        "status": "COMPLETED",
        "strategy": task["strategy"],
        "symbols": task["symbols"],
        "period": task["period"],
        "history_bars": task["history_bars"],
        "n_symbols": task.get("n_symbols", 0),
        "results": task["results"],
        "created_at": task["created_at"],
        "completed_at": task.get("completed_at"),
    }

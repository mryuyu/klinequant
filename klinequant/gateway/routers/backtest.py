"""回测路由

API：
    POST /api/backtest/run — 提交回测任务（异步执行）
    GET  /api/backtest/tasks — 回测任务列表
    GET  /api/backtest/result/{task_id} — 回测结果（绩效报告）
    GET  /api/backtest/tasks/{task_id}/trades — 回测交易明细

接入 BacktestEngine 真实实例，遵循需求文档 §6.1.4 GW-006。
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from decimal import Decimal
from typing import Optional

import polars as pl
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from gateway.state import HTTP_PROXY, BINANCE_REST_BASE, state

router = APIRouter(prefix="/api/backtest", tags=["backtest"])
logger = logging.getLogger(__name__)

# 回测任务内存存储
_tasks: dict[str, dict] = {}


class BacktestRunRequest(BaseModel):
    strategy_type: str = "dual_ma"
    symbol: str = "BTCUSDT"
    timeframe: str = "1h"
    limit: int = 500  # K 线数量
    initial_capital: float = 100000
    parameters: dict = {}
    slippage_model: str = "percentage"  # fixed / percentage / volume
    slippage_value: float = 0.001
    fee_model: str = "percentage"  # fixed / percentage / tiered
    fee_value: float = 0.001


def _build_strategy_fn(strategy_type: str, parameters: dict):
    """根据策略类型构建回测策略回调函数"""
    if strategy_type == "dual_ma":
        fast = parameters.get("fast_period", 7)
        slow = parameters.get("slow_period", 25)

        def dual_ma_strategy(df: pl.DataFrame, bar_index: int) -> Optional[str]:
            if bar_index < slow:
                return None
            closes = df["close"].to_list()
            ma_fast = sum(closes[bar_index - fast + 1: bar_index + 1]) / fast
            ma_slow = sum(closes[bar_index - slow + 1: bar_index + 1]) / slow
            # 金叉/死叉检测
            if bar_index < slow + 1:
                return None
            prev_closes = closes[:bar_index]
            prev_ma_fast = sum(prev_closes[-fast:]) / fast
            prev_ma_slow = sum(prev_closes[-slow:]) / slow
            if prev_ma_fast <= prev_ma_slow and ma_fast > ma_slow:
                return "LONG"
            if prev_ma_fast >= prev_ma_slow and ma_fast < ma_slow:
                return "CLOSE"
            return None

        return dual_ma_strategy

    # 默认：不产生信号
    def noop_strategy(df: pl.DataFrame, bar_index: int) -> Optional[str]:
        return None

    return noop_strategy


async def _fetch_klines(symbol: str, timeframe: str, limit: int) -> pl.DataFrame:
    """从币安 REST API 获取历史 K 线"""
    client = state.get_http_client()
    resp = await client.get(
        f"{BINANCE_REST_BASE}/api/v3/klines",
        params={"symbol": symbol, "interval": timeframe, "limit": limit},
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Binance API error: {resp.status_code}")

    raw = resp.json()
    if not raw:
        raise HTTPException(status_code=404, detail="No kline data returned")

    # 转换为 DataFrame
    rows = []
    for k in raw:
        rows.append({
            "timestamp": int(k[0]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
        })
    return pl.DataFrame(rows)


def _run_backtest_sync(task_id: str, body: BacktestRunRequest, data: pl.DataFrame):
    """同步执行回测（在线程池中运行）"""
    from core.backtest_engine.engine import BacktestConfig, BacktestEngine

    try:
        _tasks[task_id]["status"] = "RUNNING"

        # 构建配置
        bars_per_year_map = {
            "1m": 525600, "5m": 105120, "15m": 35040, "30m": 17520,
            "1h": 8760, "4h": 2190, "1d": 365,
        }
        # 构建滑点/手续费参数字典（与引擎工厂函数对齐）
        slippage_params: dict = {}
        if body.slippage_model == "percentage":
            slippage_params = {"pct": Decimal(str(body.slippage_value))}
        elif body.slippage_model == "fixed":
            slippage_params = {"ticks": Decimal(str(body.slippage_value))}
        elif body.slippage_model == "volume_based":
            slippage_params = {"impact_factor": Decimal(str(body.slippage_value))}

        fee_params: dict = {}
        if body.fee_model == "percentage":
            fee_params = {"rate": Decimal(str(body.fee_value))}
        elif body.fee_model == "fixed":
            fee_params = {"fee_per_trade": Decimal(str(body.fee_value))}
        elif body.fee_model == "tiered":
            fee_params = {
                "maker_rate": Decimal(str(body.fee_value)),
                "taker_rate": Decimal(str(body.fee_value)),
            }

        config = BacktestConfig(
            symbol=body.symbol,
            timeframe=body.timeframe,
            initial_capital=Decimal(str(body.initial_capital)),
            bars_per_year=bars_per_year_map.get(body.timeframe, 8760),
            slippage_model=body.slippage_model,
            slippage_params=slippage_params,
            fee_model=body.fee_model,
            fee_params=fee_params,
        )

        engine = BacktestEngine(config)
        strategy_fn = _build_strategy_fn(body.strategy_type, body.parameters)
        result = engine.run(data, strategy_fn)

        # 存储结果
        report = result.report
        _tasks[task_id].update({
            "status": "COMPLETED",
            "completed_at": int(time.time() * 1000),
            "report": {
                "total_return": report.total_return,
                "annual_return": report.annual_return,
                "sharpe_ratio": report.sharpe_ratio,
                "sortino_ratio": report.sortino_ratio,
                "max_drawdown": report.max_drawdown,
                "max_drawdown_duration": report.max_drawdown_duration,
                "calmar_ratio": report.calmar_ratio,
                "win_rate": report.win_rate,
                "profit_factor": report.profit_factor,
                "total_trades": report.total_trades,
                "avg_win": report.avg_win,
                "avg_loss": report.avg_loss,
                "max_consecutive_wins": report.max_consecutive_wins,
                "max_consecutive_losses": report.max_consecutive_losses,
                "total_fees": report.total_fees,
                "initial_capital": report.initial_capital,
                "final_equity": report.final_equity,
            },
            "equity_curve": result.equity_curve,
            "trades": [
                {
                    "symbol": t.symbol,
                    "side": t.side,
                    "entry_price": float(t.entry_price),
                    "exit_price": float(t.exit_price),
                    "quantity": float(t.quantity),
                    "entry_time": t.entry_time,
                    "exit_time": t.exit_time,
                    "pnl": float(t.pnl),
                    "fee": float(t.fee),
                    "bars_held": t.bars_held,
                }
                for t in result.trades
            ],
            "duration_ms": result.duration_ms,
        })

    except Exception as e:
        logger.error(f"Backtest {task_id} failed: {e}")
        _tasks[task_id].update({
            "status": "FAILED",
            "error": str(e),
            "completed_at": int(time.time() * 1000),
        })


@router.post("/run")
async def run_backtest(body: BacktestRunRequest):
    """提交回测任务"""
    task_id = f"bt_{uuid.uuid4().hex[:8]}"

    # 获取历史 K 线
    try:
        data = await _fetch_klines(body.symbol, body.timeframe, body.limit)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch klines: {e}")

    # 创建任务记录
    _tasks[task_id] = {
        "task_id": task_id,
        "strategy_type": body.strategy_type,
        "symbol": body.symbol,
        "timeframe": body.timeframe,
        "parameters": body.parameters,
        "initial_capital": body.initial_capital,
        "status": "PENDING",
        "created_at": int(time.time() * 1000),
        "bars": len(data),
    }

    # 在线程池中执行回测（避免阻塞事件循环）
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, _run_backtest_sync, task_id, body, data)

    return {
        "task_id": task_id,
        "status": "PENDING",
        "strategy": body.strategy_type,
        "symbol": body.symbol,
        "bars": len(data),
    }


@router.get("/tasks")
async def list_tasks(limit: int = Query(20, ge=1, le=100)):
    """获取回测任务列表"""
    tasks = sorted(_tasks.values(), key=lambda t: t["created_at"], reverse=True)
    # 返回列表时不包含大体量字段
    summary = []
    for t in tasks[:limit]:
        summary.append({
            "task_id": t["task_id"],
            "strategy_type": t["strategy_type"],
            "symbol": t["symbol"],
            "timeframe": t["timeframe"],
            "status": t["status"],
            "created_at": t["created_at"],
            "bars": t.get("bars", 0),
            "duration_ms": t.get("duration_ms"),
        })
    return {"tasks": summary, "total": len(_tasks)}


@router.get("/result/{task_id}")
async def get_result(task_id: str):
    """获取回测结果（绩效报告 + 资金曲线）"""
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
        "strategy_type": task["strategy_type"],
        "symbol": task["symbol"],
        "timeframe": task["timeframe"],
        "parameters": task["parameters"],
        "report": task["report"],
        "equity_curve": task["equity_curve"],
        "duration_ms": task["duration_ms"],
        "created_at": task["created_at"],
        "completed_at": task.get("completed_at"),
    }


@router.get("/tasks/{task_id}/trades")
async def get_trades(task_id: str, limit: int = Query(100, ge=1, le=1000)):
    """获取回测交易明细"""
    task = _tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")

    if task["status"] != "COMPLETED":
        raise HTTPException(status_code=409, detail=f"Task not completed: {task['status']}")

    trades = task.get("trades", [])
    return {
        "task_id": task_id,
        "trades": trades[:limit],
        "total": len(trades),
    }

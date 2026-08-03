"""策略路由

API：
    GET    /api/strategies — 策略列表
    POST   /api/strategies — 创建（加载）策略
    GET    /api/strategies/{id} — 策略详情
    PUT    /api/strategies/{id}/params — 更新策略参数
    POST   /api/strategies/{id}/start — 启动策略
    POST   /api/strategies/{id}/stop — 停止策略
    POST   /api/strategies/{id}/pause — 暂停策略
    DELETE /api/strategies/{id} — 卸载策略
    GET    /api/strategies/{id}/logs — 策略日志
    GET    /api/strategies/registered — 已注册策略类列表

接入 StrategyManager 真实实例，遵循需求文档 §6.1.2 GW-004。
"""
from __future__ import annotations

import logging
import time
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from gateway.audit import audit_logger
from gateway.state import state

router = APIRouter(prefix="/api/strategies", tags=["strategies"])
logger = logging.getLogger(__name__)

# 策略日志内存缓存（strategy_id -> list[dict]）
_strategy_logs: dict[str, list[dict]] = {}
MAX_LOG_LINES = 500


def _append_log(strategy_id: str, level: str, message: str):
    """记录策略操作日志"""
    if strategy_id not in _strategy_logs:
        _strategy_logs[strategy_id] = []
    _strategy_logs[strategy_id].append({
        "timestamp": int(time.time() * 1000),
        "level": level,
        "message": message,
    })
    if len(_strategy_logs[strategy_id]) > MAX_LOG_LINES:
        _strategy_logs[strategy_id] = _strategy_logs[strategy_id][-MAX_LOG_LINES:]


class StrategyCreate(BaseModel):
    name: str
    strategy_type: str = "dual_ma"
    symbols: list = ["BTCUSDT"]
    timeframes: list = ["1h"]
    parameters: dict = {}


class StrategyParamsUpdate(BaseModel):
    parameters: dict


@router.get("")
async def list_strategies():
    """获取策略列表"""
    mgr = state.strategy_manager
    result = []
    for sid, managed in mgr.strategies.items():
        info = managed.context.info
        result.append({
            "strategy_id": sid,
            "name": info.name,
            "strategy_type": info.description or "custom",
            "status": managed.status.value,
            "symbols": info.symbols,
            "timeframes": info.timeframes,
            "parameters": managed.context.params,
            "started_at": managed.started_at,
            "stopped_at": managed.stopped_at,
            "error": managed.error,
        })
    return {"strategies": result, "total": len(result)}


@router.get("/registered")
async def list_registered():
    """获取已注册的策略类列表"""
    mgr = state.strategy_manager
    return {"registered": mgr.get_registered()}


@router.post("")
async def create_strategy(body: StrategyCreate):
    """创建（加载）策略实例"""
    mgr = state.strategy_manager
    registered = mgr.get_registered()
    if body.strategy_type not in registered:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown strategy type '{body.strategy_type}'. Available: {registered}",
        )

    strategy_id = f"str_{uuid.uuid4().hex[:8]}"
    from core.strategy_engine.context import StrategyInfo
    info = StrategyInfo(
        strategy_id=strategy_id,
        name=body.name,
        description=body.strategy_type,
        symbols=body.symbols,
        timeframes=body.timeframes,
        parameters=body.parameters,
    )

    try:
        cls = mgr._registry[body.strategy_type]
        mgr.load_strategy(strategy_id, cls, info)
        mgr.init_strategy(strategy_id)
        _append_log(strategy_id, "INFO", f"Strategy created and initialized: {body.name}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load strategy: {e}")

    return {
        "strategy_id": strategy_id,
        "name": body.name,
        "status": "INITIALIZED",
    }


@router.get("/{strategy_id}")
async def get_strategy(strategy_id: str):
    """获取策略详情"""
    mgr = state.strategy_manager
    managed = mgr.strategies.get(strategy_id)
    if not managed:
        raise HTTPException(status_code=404, detail=f"Strategy not found: {strategy_id}")

    info = managed.context.info
    return {
        "strategy_id": strategy_id,
        "name": info.name,
        "strategy_type": info.description or "custom",
        "version": info.version,
        "status": managed.status.value,
        "symbols": info.symbols,
        "timeframes": info.timeframes,
        "parameters": managed.context.params,
        "state": managed.context.get_all_state(),
        "started_at": managed.started_at,
        "stopped_at": managed.stopped_at,
        "error": managed.error,
    }


@router.put("/{strategy_id}/params")
async def update_strategy_params(strategy_id: str, body: StrategyParamsUpdate):
    """更新策略参数（热更新）"""
    mgr = state.strategy_manager
    try:
        mgr.update_params(strategy_id, body.parameters)
        _append_log(strategy_id, "INFO", f"Params updated: {list(body.parameters.keys())}")
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Strategy not found: {strategy_id}")

    return {"strategy_id": strategy_id, "updated": True, "parameters": body.parameters}


@router.post("/{strategy_id}/start")
async def start_strategy(strategy_id: str):
    """启动策略"""
    mgr = state.strategy_manager
    try:
        mgr.start_strategy(strategy_id)
        _append_log(strategy_id, "INFO", "Strategy started")
        audit_logger.log(
            action="STRATEGY_START", operator="user", resource="strategy",
            resource_id=strategy_id, detail=f"策略 {strategy_id} 已启动",
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Strategy not found: {strategy_id}")
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))

    return {"strategy_id": strategy_id, "status": "RUNNING"}


@router.post("/{strategy_id}/stop")
async def stop_strategy(strategy_id: str):
    """停止策略"""
    mgr = state.strategy_manager
    try:
        mgr.stop_strategy(strategy_id)
        _append_log(strategy_id, "INFO", "Strategy stopped")
        audit_logger.log(
            action="STRATEGY_STOP", operator="user", resource="strategy",
            resource_id=strategy_id, detail=f"策略 {strategy_id} 已停止",
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Strategy not found: {strategy_id}")

    return {"strategy_id": strategy_id, "status": "STOPPED"}


@router.post("/{strategy_id}/pause")
async def pause_strategy(strategy_id: str):
    """暂停策略"""
    mgr = state.strategy_manager
    try:
        mgr.pause_strategy(strategy_id)
        _append_log(strategy_id, "INFO", "Strategy paused")
        audit_logger.log(
            action="STRATEGY_PAUSE", operator="user", resource="strategy",
            resource_id=strategy_id, detail=f"策略 {strategy_id} 已暂停",
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Strategy not found: {strategy_id}")
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))

    return {"strategy_id": strategy_id, "status": "PAUSED"}


@router.delete("/{strategy_id}")
async def unload_strategy(strategy_id: str):
    """卸载策略"""
    mgr = state.strategy_manager
    try:
        mgr.unload_strategy(strategy_id)
        _append_log(strategy_id, "INFO", "Strategy unloaded")
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Strategy not found: {strategy_id}")

    return {"strategy_id": strategy_id, "unloaded": True}


@router.get("/{strategy_id}/logs")
async def get_strategy_logs(
    strategy_id: str,
    limit: int = Query(100, ge=1, le=500),
):
    """获取策略日志"""
    logs = _strategy_logs.get(strategy_id, [])
    return {
        "strategy_id": strategy_id,
        "logs": logs[-limit:],
        "total": len(logs),
    }

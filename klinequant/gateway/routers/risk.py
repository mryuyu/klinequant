"""风控路由

API：
    GET  /api/risk/rules — 风控规则列表
    PATCH /api/risk/rules/{name} — 更新规则（启用/禁用/参数）
    GET  /api/risk/stats — 风控统计
    GET  /api/risk/logs — 风控日志

接入 RiskEngine 真实实例，遵循需求文档 §9。
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from gateway.audit import audit_logger
from gateway.state import state

router = APIRouter(prefix="/api/risk", tags=["risk"])
logger = logging.getLogger(__name__)

# 风控日志内存缓存
_risk_logs: list[dict] = []
MAX_RISK_LOGS = 500


class RuleUpdate(BaseModel):
    enabled: Optional[bool] = None
    params: Optional[dict] = None


@router.get("/rules")
async def list_rules():
    """获取风控规则列表"""
    engine = state.risk_engine
    rules = []
    for r in engine.rules:
        rules.append({
            "name": r.name,
            "enabled": r.enabled,
            "params": r.params,
        })
    return {"rules": rules, "total": len(rules)}


@router.patch("/rules/{rule_name}")
async def update_rule(rule_name: str, body: RuleUpdate):
    """更新风控规则（启用/禁用/参数热更新）"""
    engine = state.risk_engine

    if body.enabled is not None:
        ok = engine.enable_rule(rule_name, body.enabled)
        if not ok:
            raise HTTPException(status_code=404, detail=f"Rule not found: {rule_name}")
        audit_logger.log(
            action="RISK_RULE_CHANGE", operator="user", resource="risk_rule",
            resource_id=rule_name,
            detail=f"规则 {rule_name} {'启用' if body.enabled else '禁用'}",
        )

    if body.params:
        ok = engine.update_rule_params(rule_name, body.params)
        if not ok:
            raise HTTPException(status_code=404, detail=f"Rule not found: {rule_name}")
        audit_logger.log(
            action="RISK_RULE_CHANGE", operator="user", resource="risk_rule",
            resource_id=rule_name,
            detail=f"规则 {rule_name} 参数更新: {list(body.params.keys())}",
        )

    rule = engine.get_rule(rule_name)
    return {
        "name": rule_name,
        "enabled": rule.enabled if rule else None,
        "params": rule.params if rule else None,
        "updated": True,
    }


@router.get("/stats")
async def get_stats():
    """获取风控统计"""
    engine = state.risk_engine
    stats = engine.stats
    return {
        "running": engine.is_running,
        "fail_closed": engine.fail_closed,
        "total_checks": stats["total_checks"],
        "total_passed": stats["total_passed"],
        "total_rejected": stats["total_rejected"],
        "rules_count": len(engine.rules),
        "rules_enabled": sum(1 for r in engine.rules if r.enabled),
    }


@router.get("/logs")
async def get_risk_logs(limit: int = Query(50, ge=1, le=500)):
    """获取风控日志"""
    return {"logs": _risk_logs[-limit:], "total": len(_risk_logs)}

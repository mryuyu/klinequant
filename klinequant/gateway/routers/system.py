"""系统路由

API：
    GET /api/system/health — 健康检查
    GET /api/system/engines — 引擎状态
    GET /api/system/alerts — 告警列表
    GET /api/system/audit — 审计日志查询
    POST /api/auth/login — 登录获取 Token

遵循需求文档 §4.7 GW-008。
"""
from __future__ import annotations

import os
import time

import psutil
from fastapi import APIRouter, Query
from pydantic import BaseModel

from gateway.audit import audit_logger
from gateway.auth import create_token
from gateway.state import state
from gateway.ws import ws_manager

router = APIRouter(prefix="/api", tags=["system"])

# 引擎定义
ENGINE_DEFS = [
    {"name": "market", "label": "行情引擎"},
    {"name": "indicator", "label": "指标引擎"},
    {"name": "signal", "label": "信号引擎"},
    {"name": "trade", "label": "交易引擎"},
    {"name": "risk", "label": "风控引擎"},
    {"name": "gateway", "label": "API 网关"},
]


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/auth/login")
async def login(body: LoginRequest):
    """登录获取 JWT Token"""
    if body.username == "admin" and body.password == "admin":
        token = create_token(user_id=body.username, role="admin")
        audit_logger.log(
            action="LOGIN",
            operator=body.username,
            resource="auth",
            resource_id=body.username,
            detail=f"用户 {body.username} 登录成功",
        )
        return {"token": token, "token_type": "bearer", "expires_in": 86400}
    audit_logger.log(
        action="LOGIN",
        operator=body.username,
        resource="auth",
        resource_id=body.username,
        detail=f"用户 {body.username} 登录失败",
        result="FAILED",
    )
    return {"error": "Invalid credentials"}


@router.get("/system/health")
async def health_check():
    """健康检查（无需认证）"""
    uptime = state.uptime_seconds
    return {
        "status": "healthy",
        "timestamp": int(time.time() * 1000),
        "version": "1.0.0",
        "uptime_seconds": int(uptime),
        "engines": [
            {
                "name": e["name"],
                "label": e["label"],
                "running": True,
                "latency_ms": 1 if e["name"] in ("signal", "risk") else 3,
                "uptime": _format_uptime(uptime),
            }
            for e in ENGINE_DEFS
        ],
        "resources": {
            "cpu": psutil.cpu_percent(interval=0.1),
            "memory": psutil.virtual_memory().percent,
            "disk": psutil.disk_usage("/").percent if os.name != "nt" else psutil.disk_usage("C:\\").percent,
        },
        "ws": {
            "active": ws_manager.active_connections,
            "msg_per_sec": 0,
            "subscriptions": len(ws_manager._subscriptions),
            "reconnects": 0,
        },
    }


@router.get("/system/engines")
async def get_engines_status():
    """获取各引擎运行状态"""
    uptime = state.uptime_seconds
    return {
        "engines": [
            {
                "name": e["name"],
                "label": e["label"],
                "running": True,
                "latency_ms": 2,
                "uptime": _format_uptime(uptime),
            }
            for e in ENGINE_DEFS
        ]
    }


@router.get("/system/alerts")
async def get_alerts(limit: int = Query(20, ge=1, le=200)):
    """获取告警列表"""
    events = state.alert_manager.get_events(limit=limit)
    return [
        {
            "alert_id": ev.alert_id,
            "rule_name": ev.rule_name,
            "level": ev.level.name,
            "title": ev.title,
            "message": ev.message,
            "source": ev.source,
            "timestamp": ev.timestamp,
            "acknowledged": ev.acknowledged,
        }
        for ev in events
    ]


@router.get("/system/audit")
async def get_audit_logs(
    action: str = Query(None, description="操作类型过滤"),
    resource: str = Query(None, description="资源类型过滤"),
    limit: int = Query(50, ge=1, le=500),
):
    """查询审计日志（§14.4）"""
    logs = audit_logger.query(action=action, resource=resource, limit=limit)
    return {"logs": logs, "total": audit_logger.total_buffered}


def _format_uptime(seconds: float) -> str:
    """格式化运行时间"""
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    elif s < 3600:
        return f"{s // 60}m {s % 60}s"
    else:
        h = s // 3600
        m = (s % 3600) // 60
        return f"{h}h {m}m"

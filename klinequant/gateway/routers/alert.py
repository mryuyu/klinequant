"""告警路由

API：
    GET    /api/alerts/events — 告警事件列表
    POST   /api/alerts/events/{id}/ack — 确认告警
    GET    /api/alerts/rules — 告警规则列表
    POST   /api/alerts/rules — 创建告警规则
    PATCH  /api/alerts/rules/{name} — 更新规则
    GET    /api/alerts/channels — 通知渠道列表
    POST   /api/alerts/channels — 添加通知渠道
    DELETE /api/alerts/channels/{name} — 删除渠道
    POST   /api/alerts/test — 发送测试告警

遵循需求文档 §4.7 GW-008 / FE-008。
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from gateway.state import state

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


# ─── 请求模型 ───

class RuleCreate(BaseModel):
    name: str
    level: str = "WARNING"
    description: str = ""
    enabled: bool = True
    cooldown_seconds: int = 300
    channels: list = []
    escalate_after: int = 0
    escalate_to: Optional[str] = None


class RuleUpdate(BaseModel):
    enabled: Optional[bool] = None
    cooldown_seconds: Optional[int] = None
    level: Optional[str] = None
    description: Optional[str] = None


class ChannelCreate(BaseModel):
    type: str  # dingtalk / feishu / telegram / webhook
    name: str
    webhook_url: str
    secret: Optional[str] = None
    enabled: bool = True


class TestAlertRequest(BaseModel):
    message: str = "这是一条测试告警"
    level: str = "INFO"


# ─── 告警事件 ───

@router.get("/events")
async def list_events(
    level: Optional[str] = None,
    source: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
):
    """获取告警事件列表"""
    mgr = state.alert_manager
    events = mgr.get_events(
        level=level.upper() if level else None,
        source=source,
        limit=limit,
    )
    return [
        {
            "alert_id": ev.alert_id,
            "rule_name": ev.rule_name,
            "level": ev.level.name,
            "title": ev.title,
            "message": ev.message,
            "source": ev.source,
            "timestamp": ev.timestamp,
            "notified": ev.notified,
            "acknowledged": ev.acknowledged,
        }
        for ev in events
    ]


@router.post("/events/{alert_id}/ack")
async def acknowledge_event(alert_id: str):
    """确认告警"""
    mgr = state.alert_manager
    ok = mgr.acknowledge(alert_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Alert not found")
    return {"alert_id": alert_id, "acknowledged": True}


# ─── 告警规则 ───

@router.get("/rules")
async def list_rules():
    """获取告警规则列表"""
    mgr = state.alert_manager
    rules = mgr.rules
    return [
        {
            "name": r.name,
            "level": r.level.name,
            "description": r.description,
            "enabled": r.enabled,
            "cooldown_seconds": r.cooldown_seconds,
            "channels": r.channels,
            "escalate_after": r.escalate_after,
            "escalate_to": r.escalate_to.name if r.escalate_to else None,
        }
        for r in rules.values()
    ]


@router.post("/rules")
async def create_rule(body: RuleCreate):
    """创建告警规则"""
    from core.notification.alert_manager import AlertLevel, AlertRule

    mgr = state.alert_manager
    if body.name in mgr.rules:
        raise HTTPException(status_code=409, detail="Rule already exists")

    escalate_to = AlertLevel.from_str(body.escalate_to) if body.escalate_to else None
    rule = AlertRule(
        name=body.name,
        level=AlertLevel.from_str(body.level),
        description=body.description,
        enabled=body.enabled,
        cooldown_seconds=body.cooldown_seconds,
        channels=body.channels,
        escalate_after=body.escalate_after,
        escalate_to=escalate_to,
    )
    mgr.add_rule(rule)
    return {"name": body.name, "created": True}


@router.patch("/rules/{rule_name}")
async def update_rule(rule_name: str, body: RuleUpdate):
    """更新告警规则"""
    from core.notification.alert_manager import AlertLevel

    mgr = state.alert_manager
    rules = mgr.rules
    if rule_name not in rules:
        raise HTTPException(status_code=404, detail="Rule not found")

    rule = rules[rule_name]
    if body.enabled is not None:
        rule.enabled = body.enabled
    if body.cooldown_seconds is not None:
        rule.cooldown_seconds = body.cooldown_seconds
    if body.level is not None:
        rule.level = AlertLevel.from_str(body.level)
    if body.description is not None:
        rule.description = body.description

    # 重新注册（因为 rules 属性返回副本）
    mgr._rules[rule_name] = rule
    return {"name": rule_name, "updated": True}


# ─── 通知渠道 ───

@router.get("/channels")
async def list_channels():
    """获取通知渠道列表"""
    mgr = state.alert_manager
    channels = mgr.channels
    return [
        {
            "name": ch.name,
            "type": ch.__class__.__name__.replace("Channel", "").lower(),
            "enabled": ch.enabled,
        }
        for ch in channels.values()
    ]


@router.post("/channels")
async def add_channel(body: ChannelCreate):
    """添加通知渠道"""
    from core.notification.channels import (
        DingTalkChannel,
        FeishuChannel,
        TelegramChannel,
        WebhookChannel,
    )

    mgr = state.alert_manager
    channel_map = {
        "dingtalk": DingTalkChannel,
        "feishu": FeishuChannel,
        "telegram": TelegramChannel,
        "webhook": WebhookChannel,
    }

    cls = channel_map.get(body.type.lower())
    if cls is None:
        raise HTTPException(status_code=400, detail=f"Unknown channel type: {body.type}")

    try:
        if body.type.lower() == "dingtalk":
            ch = cls(webhook_url=body.webhook_url, secret=body.secret or "")
        elif body.type.lower() == "feishu":
            ch = cls(webhook_url=body.webhook_url, secret=body.secret or "")
        elif body.type.lower() == "telegram":
            # Telegram: webhook_url = bot_token, secret = chat_id
            ch = cls(bot_token=body.webhook_url, chat_id=body.secret or "")
        else:
            ch = cls(webhook_url=body.webhook_url)
        ch.enabled = body.enabled
        mgr.add_channel(ch)
        return {"name": ch.name, "created": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/channels/{channel_name}")
async def remove_channel(channel_name: str):
    """删除通知渠道"""
    mgr = state.alert_manager
    ok = mgr.remove_channel(channel_name)
    if not ok:
        raise HTTPException(status_code=404, detail="Channel not found")
    return {"name": channel_name, "deleted": True}


# ─── 测试 ───

@router.post("/test")
async def send_test_alert(body: TestAlertRequest):
    """发送测试告警"""
    from core.notification.alert_manager import AlertLevel

    mgr = state.alert_manager
    level = AlertLevel.from_str(body.level)
    event = await mgr.fire(
        rule_name="test_alert",
        message=body.message,
        source="gateway",
        title="测试告警",
        level_override=level,
    )
    if event:
        return {"alert_id": event.alert_id, "fired": True}
    return {"fired": False, "reason": "cooldown or disabled"}

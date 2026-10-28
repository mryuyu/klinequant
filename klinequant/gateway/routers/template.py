"""图表模版路由（前端「模版管理」持久化后端）

背景：模版原存于浏览器 localStorage（kq-templates），重装浏览器/换设备即丢失。
改为落盘到项目内 data/state/chart_templates.json，跨浏览器/设备持久（该目录已 gitignore，仅本地）。

API：
    GET /api/chart-templates — 返回全部模版 {templates: [{name, ts, data}]}
    PUT /api/chart-templates — 整体覆盖写入 {templates: [...]}（前端持完整数组）

存储契约：单一 JSON 文件承载整个模版数组；写入为「读全量→改→写全量」的原子替换，
前端负责增删改后提交完整列表，后端不解析 data 内部结构（前端布局/指标/配色自描述）。
"""
from __future__ import annotations

import json
import logging
import threading

from fastapi import APIRouter, Body

from klinequant import PROJECT_ROOT

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/chart-templates", tags=["template"])

# 可变数据统一置于 PROJECT_ROOT/data/state（与策略状态 db 同目录约定）
TPL_FILE = PROJECT_ROOT / "data" / "state" / "chart_templates.json"
_lock = threading.Lock()   # 单文件读改写互斥，防并发写覆盖（本地单用户，开销可忽略）


def _read() -> list:
    """读取模版数组；文件缺失/损坏回退空列表（不抛异常，前端按空态处理）"""
    try:
        if not TPL_FILE.is_file():
            return []
        obj = json.loads(TPL_FILE.read_text(encoding="utf-8"))
        return obj.get("templates", []) if isinstance(obj, dict) else []
    except Exception as e:
        logger.error(f"chart templates read failed: {e}")
        return []


def _write(templates: list) -> bool:
    """原子写入：先落临时文件再 replace，防写入中断损坏既有存档"""
    try:
        TPL_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = TPL_FILE.with_suffix(".json.tmp")
        text = json.dumps({"templates": templates}, ensure_ascii=False, indent=2)
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(TPL_FILE)
        return True
    except Exception as e:
        logger.error(f"chart templates write failed: {e}")
        return False


@router.get("")
async def get_templates():
    """全部模版（前端弹窗打开时拉取）"""
    with _lock:
        return {"templates": _read()}


@router.put("")
async def put_templates(payload: dict = Body(...)):
    """整体覆盖写入模版数组（前端保存/删除后提交完整列表）"""
    templates = payload.get("templates") if isinstance(payload, dict) else None
    if not isinstance(templates, list):
        return {"ok": False, "error": "templates 字段缺失或非数组"}
    with _lock:
        ok = _write(templates)
    return {"ok": ok, "count": len(templates)}

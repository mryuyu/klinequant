"""指标路由（IND-102）

API：
    GET /api/indicator/meta    — 已注册指标元数据（字段/值域/预热根数/默认参数）
    GET /api/indicator/history — 指标历史序列（后端引擎统一计算，剔除预热段的有效序列）

实时增量走 WS 主题 indicators.{exchange}.{symbol}.{tf}（由 indicator_service.on_bar 推送）。
计算契约 key = (指标名, 参数组合)：同指标多参数实例天然并存。
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import APIRouter, Query

from gateway.indicator_service import ensure_warmed
from gateway.market_sources.manager import market_manager
from gateway.state import state

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/indicator", tags=["indicator"])


@router.get("/meta")
async def get_meta():
    """已注册指标元数据（前端指标选择器/弹窗渲染源，display_meta 契约）"""
    engine = state.indicator_engine
    items = []
    for name in engine.registry.list_indicators():
        try:
            inst = engine.registry.create(name, None)
        except Exception as e:
            logger.warning(f"Indicator meta skipped {name}: {e}")
            continue
        items.append({
            "name": name,
            "min_periods": inst.min_periods,
            "display_meta": inst.display_meta,
            "default_params": inst.default_params,
        })
    return {"indicators": items}


@router.get("/history")
async def get_history(
    symbol: str = Query("BTCUSDT", description="交易对"),
    timeframe: str = Query("1h", description="K线周期"),
    indicator: str = Query("MACD", description="指标名（注册表名称，大写）"),
    params: Optional[str] = Query(None, description="参数 JSON，如 {\"fast_period\":12}"),
    limit: int = Query(300, ge=1, le=5000, description="返回根数（显示需求，上限对齐前端 klineCount 与预热深度 _MAX_WARMUP_TOTAL）"),
    exchange: Optional[str] = Query(None, description="市场源，缺省默认所"),
):
    """指标历史序列：注册+预热（拉取深度=显示需求+预热根数）后返回有效序列"""
    ex = (exchange or market_manager.default_exchange()).strip().lower()
    name = indicator.strip().upper()

    try:
        p = json.loads(params) if params else {}
    except json.JSONDecodeError:
        logger.warning(f"Indicator params invalid: {params}")
        p = {}

    empty = {
        "indicator": name, "params": p, "symbol": symbol.upper(),
        "timeframe": timeframe, "exchange": ex,
        "warmed": False, "count": 0, "data": [],
    }
    if market_manager.get(ex) is None:
        logger.error(f"Indicator history: unknown market source {ex}")
        return empty

    try:
        ind = await ensure_warmed(ex, symbol.upper(), timeframe, name, p, limit)
    except KeyError:
        logger.error(f"Indicator history: unknown indicator {name}")
        return empty

    engine = state.indicator_engine
    data = engine.get_series(name, p, symbol.upper(), ex, timeframe, limit=limit)
    return {
        "indicator": name,
        "params": p,
        "symbol": symbol.upper(),
        "timeframe": timeframe,
        "exchange": ex,
        "warmed": ind.is_warmed_up,
        "count": len(data),
        "data": data,
    }

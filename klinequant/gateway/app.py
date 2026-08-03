"""FastAPI 应用骨架

功能：
    - CORS 配置
    - 路由注册
    - WebSocket 端点
    - 启动/关闭事件

遵循需求文档 §4.7 GW-001。
"""
from __future__ import annotations

import logging

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from gateway.errors import register_error_handlers
from gateway.routers import alert, backtest, market, risk, strategy, system, trade
from gateway.ws import ws_manager

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    """创建 FastAPI 应用实例"""
    app = FastAPI(
        title="KlineQuant API Gateway",
        description="量化交易系统 API 网关",
        version="1.0.0",
    )

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # API 路径规范化（§6.1）：/api/v1/xxx → /api/xxx 兼容重写
    class APIVersionRewrite(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next):
            if request.url.path.startswith("/api/v1/"):
                # 重写路径：去掉 /v1 段
                new_path = request.url.path.replace("/api/v1/", "/api/", 1)
                request.scope["path"] = new_path
            return await call_next(request)

    app.add_middleware(APIVersionRewrite)

    # 统一错误码异常处理器（§8.1/8.2）
    register_error_handlers(app)

    # 注册路由
    app.include_router(system.router)
    app.include_router(market.router)
    app.include_router(strategy.router)
    app.include_router(trade.router)
    app.include_router(backtest.router)
    app.include_router(alert.router)
    app.include_router(risk.router)

    # WebSocket 端点
    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        ws_id = await ws_manager.connect(websocket)
        try:
            while True:
                data = await websocket.receive_text()
                await ws_manager.handle_message(ws_id, data)
        except WebSocketDisconnect:
            ws_manager.disconnect(ws_id)
        except Exception as e:
            logger.error(f"WS error: {ws_id}: {e}")
            ws_manager.disconnect(ws_id)

    @app.on_event("startup")
    async def on_startup():
        logger.info("KlineQuant Gateway starting...")
        # 启动 WS K线推送后台任务
        import asyncio
        from gateway.ws_kline import start_kline_broadcaster
        asyncio.create_task(start_kline_broadcaster())

    @app.on_event("shutdown")
    async def on_shutdown():
        logger.info("KlineQuant Gateway shutting down...")
        from gateway.state import state
        await state.close()

    return app


# 应用实例
app = create_app()

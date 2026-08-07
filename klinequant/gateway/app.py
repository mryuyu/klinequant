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
import os

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from gateway.env import load_env

load_env()  # 凭证等环境变量从 klinequant/.env 加载（须在路由/插件导入前）

from gateway.errors import register_error_handlers
from gateway.market_sources import bootstrap_sources, market_manager
from gateway.routers import alert, backtest, market, risk, strategy, system, trade
from gateway.ws import ws_manager

logger = logging.getLogger(__name__)

# 前端页面目录（lc-live.html 为默认首页与唯一迭代基线）：klinequant/../frontend/mockup
FRONTEND_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "..", "frontend", "mockup")
)


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

    # 前端页面：根路径重定向到 /static/lc-live.html（保证页面内相对路径 lwc.js 正确解析），
    # /static 挂载整个 mockup 目录
    index_file = os.path.join(FRONTEND_DIR, "lc-live.html")
    if os.path.isfile(index_file):
        @app.get("/", include_in_schema=False)
        async def index():
            return RedirectResponse("/static/lc-live.html")

        app.mount("/static", StaticFiles(directory=FRONTEND_DIR, html=True), name="static")
    else:
        logger.warning(f"Frontend dir not found, UI disabled: {FRONTEND_DIR}")

        @app.get("/", include_in_schema=False)
        async def index_missing():
            return RedirectResponse("/docs")

    @app.on_event("startup")
    async def on_startup():
        logger.info("KlineQuant Gateway starting...")
        # 市场源插件框架：注册启用的插件（KQ_MARKET_SOURCES）并启动订阅分发
        bootstrap_sources()
        await market_manager.start()

    @app.on_event("shutdown")
    async def on_shutdown():
        logger.info("KlineQuant Gateway shutting down...")
        from gateway.state import state
        await state.close()

    return app


# 应用实例
app = create_app()

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
import time

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
from gateway.routers import alert, backtest, indicator, market, risk, strategy, system, trade
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
    app.include_router(indicator.router)
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
        # 市场源插件后台初始化：部分源登录（如 ths）可达分钟级，同步等待会阻塞
        # startup 事件、uvicorn 拒接连接致首页打不开；改后台任务，页面/接口先行就绪，
        # 源未就绪时 /api/market/sources 返回空、路由返回空数据，前端自适配（刷新重试）
        import asyncio

        # 事件循环看门狗：独立线程心跳探活，循环被同步调用堵住 >10s 时 dump 全线程栈，
        # 让下次复现直接暴露阻塞点（2026-08-31 曾发生 4 分钟全量静默无栈可查）
        import faulthandler
        import threading

        faulthandler.enable()   # C 扩展（thsdk/MT5）段错误时也留栈到 stderr

        loop = asyncio.get_running_loop()
        # asyncio debug 默认关闭（2026-08-31 实证：每个 Future/回调都抓全栈 +
        # linecache.checkcache 文件 stat，大页拉取高并发时事件循环线程自身被拖垮，
        # 停摆 18~47s 导致前端 WS 静默看门狗误判断连）；排障时 GATEWAY_ASYNCIO_DEBUG=1 重开。
        # 看门狗 + faulthandler 保持常开（开销可忽略），阻塞时仍能 dump 全线程栈。
        if os.getenv("GATEWAY_ASYNCIO_DEBUG", "0") == "1":
            loop.set_debug(True)   # 慢回调告警（>slow_callback_duration 记录协程来源）
        loop.slow_callback_duration = 0.5   # 多请求并发时调度延迟可达 0.3~0.5s 属正常，只看更高量级
        hb = {"at": loop.time()}

        def _poke():
            hb["at"] = loop.time()

        def _watch():
            while True:
                time.sleep(5)
                stalled = loop.time() - hb["at"]
                if stalled > 10.0:
                    logger.warning(
                        "EVENT LOOP BLOCKED %.0fs — dumping all thread stacks", stalled
                    )
                    faulthandler.dump_traceback(all_threads=True)
                try:
                    loop.call_soon_threadsafe(_poke)
                except RuntimeError:
                    return   # 循环已关闭（退出中）

        app.state._watchdog = threading.Thread(
            target=_watch, name="loop-watchdog", daemon=True
        )
        app.state._watchdog.start()

        async def _bootstrap():
            try:
                # 源构造/登录为同步阻塞型（均已带超时护栏），移出事件循环防卡全部请求
                await asyncio.to_thread(bootstrap_sources)
                await market_manager.start()
                logger.info("Market sources ready")
            except Exception:
                logger.exception("Market source bootstrap failed")

        app.state.bootstrap_task = asyncio.create_task(_bootstrap())

    @app.on_event("shutdown")
    async def on_shutdown():
        logger.info("KlineQuant Gateway shutting down...")
        boot = getattr(app.state, "bootstrap_task", None)
        if boot is not None and not boot.done():
            boot.cancel()   # 初始化未完成即退出：取消后台任务防悬挂
        from gateway.state import state
        await state.close()

    return app


# 应用实例
app = create_app()

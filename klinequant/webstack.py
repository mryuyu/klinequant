"""Web 栈启动器 — 运行模式态①/态③ 共享（详见《KlineQuant 迭代计划》六·六节）。

态①（纯行情看板 + 回测工作台，无策略）：
    python -m webstack            # 前台起 gateway（托管 lc-live.html + Vue /app + API）并开浏览器

态③（策略研究带 GUI，--webgui，WEBGUI-L1）：
    from webstack import start_in_background
    start_in_background()         # 后台线程起同一 gateway，策略进程继续在主线程跑 LiveRunner

设计要点：
    - gateway 单进程即“整个 web 栈”：/ → lc-live.html，/app → Vue dist，/api → 后端，/ws → 推送。
    - 前端是否启动由“是否调用本启动器”决定，而非浏览器反向拉起（浏览器无法启动死掉的后端）。
    - dist 未构建时 gateway 自动跳过 /app（见 gateway/app.py），本启动器不依赖 dist 是否存在。
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
import urllib.request
import webbrowser

logger = logging.getLogger("webstack")

ROOT = os.path.dirname(os.path.abspath(__file__))          # klinequant/
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
APP_IMPORT = "gateway.app:app"

if ROOT not in sys.path:                                    # 确保 gateway.app 可被 uvicorn 导入
    sys.path.insert(0, ROOT)


def _wait_ready(host: str, port: int, timeout: float = 30.0) -> bool:
    """轮询 /api/system/health 直到就绪或超时（仅用标准库，无额外依赖）。"""
    url = f"http://{host}:{port}/api/system/health"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.5)
    return False


def _open_browser_when_ready(host: str, port: int, path: str, timeout: float = 30.0) -> None:
    if _wait_ready(host, port, timeout):
        url = f"http://{host}:{port}{path}"
        logger.info("opening browser: %s", url)
        try:
            webbrowser.open(url)
        except Exception:
            logger.warning("无法自动打开浏览器，请手动访问 %s", url)
    else:
        logger.warning("gateway 未在 %.0fs 内就绪，跳过自动开浏览器", timeout)


def run_foreground(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    *,
    open_browser: bool = True,
    open_path: str = "/app/",
) -> None:
    """态①：前台运行 gateway（阻塞），就绪后自动开浏览器。Ctrl+C 退出。"""
    import uvicorn

    if open_browser:
        threading.Thread(
            target=_open_browser_when_ready, args=(host, port, open_path), daemon=True
        ).start()

    print(f"[webstack] gateway 启动中：")
    print(f"    lc-live.html  ->  http://{host}:{port}/")
    print(f"    Vue 正式版     ->  http://{host}:{port}{open_path}")
    print(f"    API           ->  http://{host}:{port}/api/  |  WS /ws")
    uvicorn.run(APP_IMPORT, host=host, port=port, log_level="info")


def start_in_background(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    *,
    open_browser: bool = True,
    open_path: str = "/app/",
):
    """态③（WEBGUI-L1）：后台线程运行 gateway，返回 (server, thread)。

    uvicorn.Server 默认在 install_signal_handlers 里注册 SIGINT/SIGTERM，而信号只能在
    主线程注册；策略进程的主线程要留给 LiveRunner，故这里用 _NoSignalServer 覆写为空操作。
    退出时设 server.should_exit = True 让 gateway 优雅关停。
    """
    import uvicorn

    class _NoSignalServer(uvicorn.Server):
        def install_signal_handlers(self) -> None:
            pass  # 后台线程不注册信号，交给策略主进程统一管理

    config = uvicorn.Config(APP_IMPORT, host=host, port=port, log_level="warning")
    server = _NoSignalServer(config)
    thread = threading.Thread(target=server.run, name="webstack-gateway", daemon=True)
    thread.start()

    if open_browser:
        threading.Thread(
            target=_open_browser_when_ready, args=(host, port, open_path), daemon=True
        ).start()
    return server, thread


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="KlineQuant web 栈启动器（态① 纯看板 / 回测）")
    p.add_argument("--host", default=DEFAULT_HOST)
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--no-browser", action="store_true", help="不自动打开浏览器")
    p.add_argument(
        "--open", dest="open_path", default="/app/",
        help="就绪后打开的路径（默认 /app/ Vue 正式版；填 / 则打开 lc-live.html）",
    )
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s"
    )
    run_foreground(
        args.host, args.port, open_browser=not args.no_browser, open_path=args.open_path
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""pytest 全局配置"""
import sys
import asyncio
from pathlib import Path

# manual 目录为手动调试/验证脚本（需网络/代理），不纳入 pytest 自动收集
collect_ignore_glob = ["manual/*"]

# Windows: ZMQ asyncio 需要 SelectorEventLoop（支持 add_reader）
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 项目根路径
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

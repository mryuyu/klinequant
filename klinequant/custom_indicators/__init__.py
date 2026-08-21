"""自定义指标目录 — def 式指标（IND-110）

约定：每个 .py 文件一个（或一组）指标，内部用 @pyindicator 注册；
本包被导入时自动加载目录下所有非下划线开头的模块。

接入方式：网关启动时 import custom_indicators 即完成注册，
前端通过 /api/indicator/meta 自动发现，无需改动前端代码。
"""
import importlib
import logging
import pkgutil

logger = logging.getLogger(__name__)

for _m in pkgutil.iter_modules(__path__):
    if _m.name.startswith("_"):
        continue
    try:
        importlib.import_module(f"{__name__}.{_m.name}")
    except Exception as e:
        logger.error(f"Custom indicator module {_m.name} load failed: {e}")

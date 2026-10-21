"""KlineQuant 量化交易平台顶层包。

外部用法::

    from klinequant import kqsdk          # 策略 SDK 门面
    import klinequant.gateway.app          # Web 网关（按需）

路径锚点
--------
PACKAGE_DIR
    本包所在目录（只读资源随包分发：``klinequant/config/*.yaml``）。
PROJECT_ROOT
    项目运行根目录，存放可变数据（``data/`` ``logs/`` ``.env`` ``secrets``）。
    默认取本包的父目录（git-clone 模型下即仓库根）；可用环境变量
    ``KLINEQUANT_HOME`` 显式覆盖，使数据目录脱离代码位置。

子包采用惰性暴露（PEP 562 ``__getattr__``），``import klinequant`` 本身不会
拉入 polars/fastapi/duckdb 等重依赖，只有真正访问 ``klinequant.kqsdk`` 等
属性时才导入对应子包。
"""
from __future__ import annotations

import os
from pathlib import Path

__version__ = "1.0.0"

PACKAGE_DIR: Path = Path(__file__).resolve().parent
PROJECT_ROOT: Path = Path(
    os.environ.get("KLINEQUANT_HOME") or PACKAGE_DIR.parent
).resolve()

# 可惰性导入的子包白名单
_LAZY_SUBMODULES = frozenset(
    {
        "config",
        "core",
        "custom_indicators",
        "gateway",
        "kqsdk",
        "protocol",
        "storage",
        "webstack",
    }
)

__all__ = [
    "__version__",
    "PACKAGE_DIR",
    "PROJECT_ROOT",
    "kqsdk",
]


def __getattr__(name: str):  # noqa: D105 - PEP 562 惰性子包暴露
    if name in _LAZY_SUBMODULES:
        import importlib

        module = importlib.import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:  # noqa: D105 - 让 dir()/补全看到惰性子包
    return sorted(set(globals()) | _LAZY_SUBMODULES)

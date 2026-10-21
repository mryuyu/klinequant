# ruff: noqa: E501
"""ZLINE — ZLine 变色趋势线（TDX 公式移植，IND-110 def 式）

通达信原版（存档）：
    zLine:EMA(EMA(((C/EMA(EMA(EMA((3*C+O+H+L)/6,5),5),5))*(3*C+O+H+L)/6),8),8);
    PartLine(zLine>ref(zLine,1),zLine),ColorMagenta,linethick2;
    PartLine(zLine<ref(zLine,1),zLine),ColorCyan,linethick2;

语义：一条连续 zLine，按「与前一根比较」的方向分段变色——
上升段（zLine > 前值）洋红 #ff00ff，下降段（zLine < 前值）青色 #00ffff，线宽 2px。
前端以 plot="dirline" + dir_colors 声明，由 DirLinePrimitive 逐段自绘（lwc 无原生分段变色线，
且该 lwc 构建会丢弃 whitespace 断点，故不采用「双字段互斥 null + 断点」方案）。
公式内权重 (3C+O+H+L)/6 为硬编码常量；两级 EMA 周期开放为用户可调参数：
e_period（原版 5，三重 EMA 平滑基数）、z_period（原版 8，两重 EMA 输出平滑）。
"""
from klinequant.core.indicator_engine.graph import ema, pyindicator


@pyindicator(
    name="ZLINE",
    pane="main",
    range="price",
    desc="ZLine 变色趋势线（上升洋红 / 下降青色）",
    style=[
        # 单字段连续 zLine；方向分段着色：[上升 ColorMagenta, 下降 ColorCyan]，linethick2
        {"plot": "dirline", "dir_colors": ["#ff00ff", "#00ffff"], "line_width": 2},
    ],
)
def zline(open, high, low, close, e_period=5, z_period=8):
    p = (3 * close + open + high + low) / 6
    e = ema(ema(ema(p, e_period), e_period), e_period)
    mid = (close / e) * (3 * close + open + high + low) / 6
    z = ema(ema(mid, z_period), z_period)
    return {"ZLINE": z}

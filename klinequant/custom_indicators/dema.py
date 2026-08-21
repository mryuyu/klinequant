"""DEMA — 双重指数移动平均（def 式自定义指标样例，主图叠加）

公式：DEMA = 2*EMA(close) - EMA(EMA(close))，滞后小于普通 EMA。
展示主图叠加（pane='main'）与单输出字段缺省命名（返回单个 Node）。
"""
from core.indicator_engine.graph import ema, pyindicator


@pyindicator(name="DEMA", pane="main", range="price",
             desc="双重指数移动平均")
def dema(close, period=20):
    e1 = ema(close, period)
    e2 = ema(e1, period)
    return 2 * e1 - e2
